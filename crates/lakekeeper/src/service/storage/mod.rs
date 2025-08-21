#![allow(clippy::match_wildcard_for_single_variants)]

pub(crate) mod az;
pub mod error;
pub(crate) mod gcs;
pub mod s3;

#[cfg(test)]
use std::{collections::HashMap, str::FromStr};

pub use az::{AdlsProfile, AzCredential};
pub(crate) use error::ValidationError;
use error::{CredentialsError, TableConfigError, UpdateError};
use futures::StreamExt;
pub use gcs::{GcsCredential, GcsProfile, GcsServiceKey};
use iceberg::io::FileIO;
use iceberg_ext::{catalog::rest::ErrorModel, configs::table::TableProperties};
use lakekeeper_io::{
    s3::S3Location, InvalidLocationError, LakekeeperStorage, Location, StorageBackend,
};
pub use s3::{S3Credential, S3Flavor, S3Profile};
use serde::{Deserialize, Serialize};

use super::{secrets::SecretInStorage, NamespaceId, TableId};
use crate::{
    api::{
        iceberg::v1::DataAccess, management::v1::warehouse::TabularDeleteProfile, CatalogConfig,
    },
    catalog::{compression_codec::CompressionCodec, io::list_location},
    request_metadata::RequestMetadata,
    service::{
        storage::error::{IcebergFileIoError, UnexpectedStorageType},
        tabular_idents::TabularId,
    },
    WarehouseId, CONFIG,
};

/// Storage profile for a warehouse.
#[derive(
    Debug, Clone, Eq, PartialEq, Serialize, Deserialize, derive_more::From, utoipa::ToSchema,
)]
#[serde(tag = "type", rename_all = "kebab-case")]
#[allow(clippy::unsafe_derive_deserialize)]
// tokio::join! uses unsafe code internally.
// This is no problem since our constructor does not enforce any invariants relevant to the unsafe code. Deserialize is even the primary way of constructing `StorageProfile` since it is received via REST.
pub enum StorageProfile {
    /// Azure storage profile
    #[serde(rename = "adls", alias = "azdls")]
    #[schema(title = "StorageProfileAdls")]
    Adls(AdlsProfile),
    /// S3 storage profile
    #[serde(rename = "s3")]
    #[schema(title = "StorageProfileS3")]
    S3(S3Profile),
    #[serde(rename = "gcs")]
    #[schema(title = "StorageProfileGcs")]
    Gcs(GcsProfile),
    #[cfg(test)]
    Memory(MemoryProfile),
}

#[cfg(test)]
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    utoipa::ToSchema,
    typed_builder::TypedBuilder,
)]
#[serde(rename_all = "kebab-case")]
pub struct MemoryProfile {
    /// Base location for the local profile
    base_location: String,
}

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum StoragePermissions {
    Read,
    ReadWrite,
    ReadWriteDelete,
}

#[derive(Debug)]
pub struct TableConfig {
    pub(crate) creds: TableProperties,
    pub(crate) config: TableProperties,
}

impl StorageProfile {
    #[must_use]
    pub fn generate_catalog_config(
        &self,
        warehouse_id: WarehouseId,
        request_metadata: &RequestMetadata,
        delete_profile: TabularDeleteProfile,
    ) -> CatalogConfig {
        match self {
            StorageProfile::S3(profile) => {
                profile.generate_catalog_config(warehouse_id, request_metadata, delete_profile)
            }
            StorageProfile::Adls(prof) => prof.generate_catalog_config(warehouse_id),
            StorageProfile::Gcs(prof) => prof.generate_catalog_config(warehouse_id),
            #[cfg(test)]
            StorageProfile::Memory(_) => CatalogConfig {
                overrides: HashMap::new(),
                defaults: HashMap::new(),
                endpoints: crate::api::iceberg::supported_endpoints().to_vec(),
            },
        }
    }

    /// Update this profile with the other profile.
    /// Fails if this is an incompatible update, such as changing the location.
    ///
    /// # Errors
    /// Fails if the profiles are not compatible, typically because the location changed
    pub fn update_with(self, other: Self) -> Result<Self, UpdateError> {
        match (self, other) {
            (StorageProfile::S3(this_profile), StorageProfile::S3(other_profile)) => {
                this_profile.update_with(other_profile).map(Into::into)
            }
            (StorageProfile::Adls(this_profile), StorageProfile::Adls(other_profile)) => {
                this_profile.update_with(other_profile).map(Into::into)
            }
            (StorageProfile::Gcs(this_profile), StorageProfile::Gcs(other_profile)) => {
                this_profile.update_with(other_profile).map(Into::into)
            }
            #[cfg(test)]
            (StorageProfile::Memory(_this_profile), StorageProfile::Memory(_other_profile)) => {
                unimplemented!("Local profile update not implemented")
            }
            (this_profile, other_profile) => Err(UpdateError::IncompatibleProfiles(
                this_profile.storage_type().to_string(),
                other_profile.storage_type().to_string(),
            )),
        }
    }

    /// Create a new file IO instance for the storage profile.
    ///
    /// # Errors
    /// Fails if the underlying storage profile's file IO creation fails.
    pub async fn file_io(
        &self,
        secret: Option<&StorageCredential>,
    ) -> Result<StorageBackend, CredentialsError> {
        match self {
            StorageProfile::S3(profile) => profile
                .lakekeeper_io(
                    secret
                        .map(|s| s.try_to_s3())
                        .transpose()
                        .map_err(CredentialsError::from)?,
                )
                .await
                .map(Into::into),
            StorageProfile::Adls(profile) => profile
                .lakekeeper_io(
                    secret
                        .map(|s| s.try_to_az())
                        .ok_or_else(|| CredentialsError::MissingCredential("adls".to_string()))?
                        .map_err(CredentialsError::from)?,
                )
                .map(Into::into),
            StorageProfile::Gcs(prof) => prof
                .lakekeeper_io(
                    secret
                        .map(|s| s.try_to_gcs())
                        .ok_or_else(|| CredentialsError::MissingCredential("gcs".to_string()))?
                        .map_err(CredentialsError::from)?,
                )
                .await
                .map(Into::into),
            #[cfg(test)]
            StorageProfile::Memory(_) => Ok(StorageBackend::Memory(
                lakekeeper_io::memory::MemoryStorage::new(),
            )),
        }
    }

    /// Get the base location of this Storage Profiles
    ///
    /// # Errors
    /// Can fail for un-normalized profiles.
    pub fn base_location(&self) -> Result<Location, InvalidLocationError> {
        match self {
            StorageProfile::S3(profile) => profile.base_location().map(S3Location::into_location),
            StorageProfile::Adls(profile) => profile.base_location(),
            StorageProfile::Gcs(profile) => profile.base_location(),
            #[cfg(test)]
            StorageProfile::Memory(profile) => Ok(Location::from_str(&profile.base_location)
                .map_err(|_| {
                    InvalidLocationError::new(
                        profile.base_location.clone(),
                        "Invalid base location for memory profile".to_string(),
                    )
                })?),
        }
    }

    /// Get the default location for the namespace.
    ///
    /// # Errors
    /// Fails if the `key_prefix` is not valid for S3 URLs.
    pub fn default_namespace_location(
        &self,
        namespace_id: NamespaceId,
    ) -> Result<Location, ValidationError> {
        let mut base_location: Location = self.base_location()?;
        base_location
            .without_trailing_slash()
            .push(&namespace_id.to_string());
        Ok(base_location)
    }

    #[must_use]
    pub fn storage_type(&self) -> &'static str {
        match self {
            StorageProfile::S3(_) => "s3",
            StorageProfile::Adls(_) => "adls",
            StorageProfile::Gcs(_) => "gcs",
            #[cfg(test)]
            StorageProfile::Memory(_) => "memory",
        }
    }

    /// Generate the table config for the storage profile.
    ///
    /// # Errors
    /// Fails if the underlying storage profile's generation fails.
    #[allow(clippy::too_many_arguments)]
    pub async fn generate_table_config(
        &self,
        data_access: DataAccess,
        secret: Option<&StorageCredential>,
        table_location: &Location,
        storage_permissions: StoragePermissions,
        request_metadata: &RequestMetadata,
        warehouse_id: WarehouseId,
        tabular_id: TabularId,
    ) -> Result<TableConfig, TableConfigError> {
        match self {
            StorageProfile::S3(profile) => {
                profile
                    .generate_table_config(
                        data_access,
                        secret
                            .map(|s| s.try_to_s3())
                            .transpose()
                            .map_err(CredentialsError::from)?,
                        table_location,
                        storage_permissions,
                        request_metadata,
                        warehouse_id,
                        tabular_id,
                    )
                    .await
            }
            StorageProfile::Adls(profile) => {
                profile
                    .generate_table_config(
                        data_access,
                        table_location,
                        secret
                            .ok_or_else(|| CredentialsError::MissingCredential("adls".to_string()))?
                            .try_to_az()
                            .map_err(CredentialsError::from)?,
                        storage_permissions,
                    )
                    .await
            }
            StorageProfile::Gcs(profile) => {
                profile
                    .generate_table_config(
                        data_access,
                        secret
                            .map(|s| s.try_to_gcs())
                            .transpose()
                            .map_err(CredentialsError::from)?
                            .ok_or_else(|| {
                                CredentialsError::MissingCredential("gcs".to_string())
                            })?,
                        table_location,
                        storage_permissions,
                    )
                    .await
            }
            #[cfg(test)]
            StorageProfile::Memory(_) => Ok(TableConfig {
                creds: TableProperties::default(),
                config: TableProperties::default(),
            }),
        }
    }

    /// Try to normalize the storage profile.
    /// Fails if some validation fails. This does not check physical filesystem access.
    ///
    /// # Errors
    /// Fails if the underlying storage profile's normalization fails.
    pub fn normalize(
        &mut self,
        credential: Option<&StorageCredential>,
    ) -> Result<(), ValidationError> {
        // ------------- Common validations -------------
        // Test if we can generate a default namespace location
        let ns_location = self.default_namespace_location(NamespaceId::new_random())?;
        self.default_tabular_location(&ns_location, TableId::new_random().into());

        // ------------- Profile specific validations -------------
        match self {
            StorageProfile::S3(profile) => profile.normalize(
                credential
                    .map(|s| s.try_to_s3())
                    .transpose()
                    .map_err(CredentialsError::from)?,
            ),
            StorageProfile::Adls(prof) => prof.normalize(),
            StorageProfile::Gcs(profile) => profile.normalize(),
            #[cfg(test)]
            StorageProfile::Memory(_) => Ok(()),
        }
    }

    /// Validate physical access
    ///
    /// If location is not provided, a dummy table location is used.
    ///
    /// # Errors
    /// Fails if a file cannot be written and deleted.
    #[allow(clippy::too_many_lines)]
    pub async fn validate_access(
        &self,
        credential: Option<&StorageCredential>,
        location: Option<&Location>,
        request_metadata: &RequestMetadata,
    ) -> Result<(), ValidationError> {
        if CONFIG.skip_storage_validation {
            tracing::debug!("Storage validation is disabled, skipping validation of credentials.");
            return Ok(());
        }

        let io = self.file_io(credential).await?;

        let ns_id = NamespaceId::new_random();
        let table_id = TableId::new_random();
        let ns_location = self.default_namespace_location(ns_id)?;
        let test_location = location.map_or_else(
            || self.default_tabular_location(&ns_location, table_id.into()),
            std::borrow::ToOwned::to_owned,
        );
        tracing::debug!("Validating direct read/write access to {test_location}");

        // Test vended-credentials access
        let test_vended_credentials = match self {
            StorageProfile::S3(profile) => profile.sts_enabled,
            StorageProfile::Adls(_) => true,
            StorageProfile::Gcs(_) => true,
            #[cfg(test)]
            StorageProfile::Memory(_) => false,
        };

        // Run both validations in parallel
        let direct_validation = self.validate_read_write_lakekeeper(&io, &test_location);
        let vended_validation = async {
            if test_vended_credentials {
                self.validate_vended_credentials_access(
                    credential,
                    &test_location,
                    request_metadata,
                )
                .await?;
            }
            Ok::<(), ValidationError>(())
        };

        let (direct_result, vended_result) = tokio::join!(direct_validation, vended_validation);

        direct_result?;
        vended_result?;

        tracing::debug!("Cleanup started");
        io.remove_all(&test_location).await?;
        tracing::debug!("Cleanup finished");

        match is_empty(&io, &test_location).await {
            Err(ValidationError::IoOperationFailed(io_error)) => {
                tracing::info!(
                    ?io_error,
                    "Error while checking location is empty: {io_error}"
                );
                Err(ValidationError::IoOperationFailed(io_error))
            }
            Ok(false) => Err(InvalidLocationError::new(
                test_location.to_string(),
                "Files are left after remove_all on test location".to_string(),
            )
            .into()),
            Ok(true) => {
                tracing::debug!("Location is empty");
                Ok(Ok(()))
            }
            Err(other) => {
                tracing::info!("Unrecoverable error: {other:?}");
                Ok(Err(other))
            }
        }??;
        tracing::debug!("Access validation finished");
        Ok(())
    }

    /// Validate access with vended credentials
    ///
    /// # Errors
    /// Fails if a file cannot be written and deleted using vended credentials.
    async fn validate_vended_credentials_access(
        &self,
        credential: Option<&StorageCredential>,
        test_location: &Location,
        request_metadata: &RequestMetadata,
    ) -> Result<(), ValidationError> {
        tracing::debug!("Validating vended credentials access to: {test_location}");

        let tbl_config = self
            .generate_table_config(
                DataAccess {
                    remote_signing: false,
                    vended_credentials: true,
                },
                credential,
                test_location,
                StoragePermissions::ReadWriteDelete,
                // The following arguments are used only for generating the remote signing configuration
                // and are not used in the vended credentials case.
                request_metadata,
                WarehouseId::new_random(),
                TableId::new_random().into(),
            )
            .await?;

        match &self {
            StorageProfile::S3(_) => {
                tracing::debug!("Getting s3 file io from table config for vended credentials.");
                let sts_file_io = s3::get_file_io_from_table_config(&tbl_config.config)?;
                tracing::debug!(
                    "Validating read/write access to: {test_location} using vended credentials"
                );
                self.validate_read_write_iceberg(&sts_file_io, test_location, true)
                    .await?;
            }
            StorageProfile::Adls(_) => {
                tracing::debug!("Validating adls vended credentials access to: {test_location}");
                let sts_file_io = az::get_file_io_from_table_config(&tbl_config.config)?;
                self.validate_read_write_iceberg(&sts_file_io, test_location, true)
                    .await?;
            }
            StorageProfile::Gcs(_) => {
                tracing::debug!("Getting gcs file io from table config for vended credentials.");
                let sts_file_io = gcs::get_file_io_from_table_config(&tbl_config.config)?;
                tracing::debug!("Validating gcs vended credentials access to: {test_location}");
                self.validate_read_write_iceberg(&sts_file_io, test_location, true)
                    .await?;
            }
            #[cfg(test)]
            StorageProfile::Memory(_) => {
                unreachable!("Local profile does not support vended credentials access validation")
            }
        }

        Ok(())
    }

    async fn validate_read_write_lakekeeper(
        &self,
        io: &impl LakekeeperStorage,
        test_location: &Location,
    ) -> Result<(), ValidationError> {
        let compression_codec = CompressionCodec::Gzip;

        let metadata_location = self.default_metadata_location(
            test_location,
            &compression_codec,
            uuid::Uuid::now_v7(),
            0,
        );
        let mut test_file_write = metadata_location.parent();
        test_file_write.push("test");
        println!("Test file write location: {test_file_write}");
        let mut test_file_write = test_file_write.parent();
        test_file_write.push("test");
        tracing::debug!("Validating access to: {}", test_file_write);

        // Test write
        crate::catalog::io::write_file(io, &test_file_write, "test", compression_codec)
            .await
            .map_err(|e| {
                tracing::info!("Error while writing file: {e:?}");
                ValidationError::from(e)
            })?;

        // Test read
        let _ = crate::catalog::io::read_file(io, &test_file_write, compression_codec)
            .await
            .map_err(|e| {
                tracing::info!("Error while reading file: {e:?}");
                ValidationError::from(e)
            })?;

        // Test delete
        crate::catalog::io::delete_file(io, &test_file_write)
            .await
            .map_err(|e| {
                tracing::info!("Error while deleting file: {e:?}");
                ValidationError::from(e)
            })?;

        tracing::debug!(
            "Successfully wrote, read and deleted file at: {}",
            test_file_write
        );

        Ok(())
    }

    async fn validate_read_write_iceberg(
        &self,
        file_io: &FileIO,
        test_location: &Location,
        is_vended_credentials: bool,
    ) -> Result<(), ValidationError> {
        let compression_codec = CompressionCodec::Gzip;

        let mut test_file_write = self.default_metadata_location(
            test_location,
            &compression_codec,
            uuid::Uuid::now_v7(),
            0,
        );
        if is_vended_credentials {
            let f = test_file_write
                .path()
                .and_then(|s| s.split('/').next_back())
                .unwrap_or("missing")
                .to_string();
            test_file_write.pop().push("vended").push(&f);
            tracing::debug!(
                "Validating vended credential access to: {}",
                test_file_write
            );
        } else {
            test_file_write.pop().push("test");
            tracing::debug!("Validating access to: {}", test_file_write);
        }

        // Test write
        file_io
            .new_output(&test_file_write)
            .map_err(IcebergFileIoError::IcebergError)?
            .write("test".into())
            .await
            .map_err(IcebergFileIoError::IcebergError)?;

        // Test read
        file_io
            .new_input(&test_file_write)
            .map_err(IcebergFileIoError::IcebergError)?
            .read()
            .await
            .map_err(IcebergFileIoError::IcebergError)?;

        // Test delete
        file_io
            .delete(&test_file_write)
            .await
            .map_err(IcebergFileIoError::IcebergError)?;

        tracing::debug!(
            "Successfully wrote, read and deleted file at `{test_file_write}` with Iceberg FileIO and vended credentials."
        );

        Ok(())
    }

    /// Try to convert the storage profile into an S3 profile.
    ///
    /// # Errors
    /// Fails if the profile is not an S3 profile.
    pub fn try_into_s3(self) -> Result<S3Profile, UnexpectedStorageType> {
        match self {
            Self::S3(profile) => Ok(profile),
            _ => Err(UnexpectedStorageType {
                is: self.storage_type(),
                to: "s3",
            }),
        }
    }

    /// Try to convert the storage profile into an Az profile.
    ///
    /// # Errors
    /// Fails if the profile is not an Az profile.
    pub fn try_into_az(self) -> Result<AdlsProfile, UnexpectedStorageType> {
        match self {
            Self::Adls(profile) => Ok(profile),
            _ => Err(UnexpectedStorageType {
                is: self.storage_type(),
                to: "adls",
            }),
        }
    }

    #[must_use]
    /// Check whether the location of this storage profile is overlapping
    /// with the given storage profile.
    /// This check is only an indication and does not guarantee no overlap.
    pub fn is_overlapping_location(&self, other: &StorageProfile) -> bool {
        match (self, other) {
            (StorageProfile::S3(profile), StorageProfile::S3(other_profile)) => {
                profile.is_overlapping_location(other_profile)
            }
            (StorageProfile::Adls(profile), StorageProfile::Adls(other_profile)) => {
                profile.is_overlapping_location(other_profile)
            }
            (StorageProfile::Gcs(profile), StorageProfile::Gcs(other_profile)) => {
                profile.is_overlapping_location(other_profile)
            }
            _ => false,
        }
    }

    #[must_use]
    /// Check whether the location is allowed for the storage profile.
    ///
    /// Allowed locations are sublocation of the base location.
    pub fn is_allowed_location(&self, other: &Location) -> bool {
        let Some(mut base_location) = self.base_location().ok() else {
            return false;
        };

        if let StorageProfile::S3(profile) = self {
            // For s3 locations we allow optionally in addition to s3:// prefixes
            // also s3a:// and other custom variants.
            let other_scheme = other.scheme();
            if !profile.is_allowed_schema(other_scheme) {
                tracing::debug!("Scheme {other_scheme} is not allowed for S3 profile.",);
                return false;
            }
            if other_scheme != base_location.scheme() {
                base_location.set_scheme_unchecked_mut(other_scheme);
            }
        }

        if let StorageProfile::Adls(profile) = self {
            let other_scheme = other.scheme();
            if !profile.is_allowed_schema(other_scheme) {
                tracing::debug!("Scheme {other_scheme} is not allowed for ADLS profile.",);
                return false;
            }
            if other_scheme != base_location.scheme() {
                base_location.set_scheme_unchecked_mut(other_scheme);
            }
        }

        base_location.with_trailing_slash();
        if other == &base_location {
            return false;
        }

        other.is_sublocation_of(&base_location)
    }

    /// Require that the location is allowed for the storage profile.
    ///
    /// # Errors
    /// Fails if the provided location is not a sublocation of the base location.
    pub fn require_allowed_location(&self, other: &Location) -> Result<(), ErrorModel> {
        if !self.is_allowed_location(other) {
            let base_location = self
                .base_location()
                .ok()
                .map_or(String::new(), |l| l.to_string());
            return Err(ErrorModel::bad_request(
                format!("Provided location {other} is not a valid sublocation of the storage profile {base_location}."),
                "InvalidLocation",
                None,
            ));
        }
        Ok(())
    }
}

pub trait StorageLocations {
    /// Get the default tabular location for the storage profile.
    fn default_tabular_location(
        &self,
        namespace_location: &Location,
        table_id: TabularId,
    ) -> Location {
        let mut l = namespace_location.clone();
        l.without_trailing_slash().push(&table_id.to_string());
        l
    }

    #[must_use]
    /// Get the default metadata location for the storage profile.
    fn default_metadata_location(
        &self,
        table_location: &Location,
        compression_codec: &CompressionCodec,
        metadata_id: uuid::Uuid,
        metadata_count: usize,
    ) -> Location {
        let filename_extension_compression = compression_codec.as_file_extension();
        let filename = format!(
            "{metadata_count:05}-{metadata_id}{filename_extension_compression}.metadata.json",
        );
        let mut l = table_location.clone();

        l.without_trailing_slash().extend(&["metadata", &filename]);
        l
    }
}

impl StorageLocations for StorageProfile {}
impl StorageLocations for S3Profile {}
impl StorageLocations for AdlsProfile {}

#[cfg(test)]
impl Default for MemoryProfile {
    fn default() -> Self {
        Self {
            base_location: <Location as std::str::FromStr>::from_str(
                format!("memory://test-{}", uuid::Uuid::new_v4()).as_str(),
            )
            .expect("Failed to create temporary directory location")
            .to_string(),
        }
    }
}

/// Storage secret for a warehouse.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, derive_more::From, utoipa::ToSchema)]
#[serde(tag = "type")]
pub enum StorageCredential {
    /// Credentials for S3 storage
    ///
    /// Example payload in the code-snippet below:
    ///
    /// ```
    /// use lakekeeper::service::storage::StorageCredential;
    /// let cred: StorageCredential = serde_json::from_str(r#"{
    ///     "type": "s3",
    ///     "credential-type": "access-key",
    ///     "aws-access-key-id": "minio-root-user",
    ///     "aws-secret-access-key": "minio-root-password"
    ///   }"#).unwrap();
    /// ```
    #[serde(rename = "s3")]
    #[schema(title = "StorageCredentialS3")]
    S3(S3Credential),
    /// Credentials for Az storage
    ///
    /// Example payload:
    ///
    /// ```
    /// use lakekeeper::service::storage::StorageCredential;
    /// let cred: StorageCredential = serde_json::from_str(r#"{
    ///     "type": "az",
    ///     "credential-type": "client-credentials",
    ///     "client-id": "...",
    ///     "client-secret": "...",
    ///     "tenant-id": "..."
    ///   }"#).unwrap();
    /// ```
    #[serde(rename = "az")]
    #[schema(title = "StorageCredentialAz")]
    Az(AzCredential),
    /// Credentials for GCS storage
    ///
    /// Example payload in the code-snippet below:
    ///
    /// ```
    /// use lakekeeper::service::storage::StorageCredential;
    /// let cred: StorageCredential = serde_json::from_str(r#"{
    ///     "type": "gcs",
    ///     "credential-type": "service-account-key",
    ///     "key": {
    ///       "type": "service_account",
    ///       "project_id": "example-project-1234",
    ///       "private_key_id": "....",
    ///       "private_key": "-----BEGIN PRIVATE KEY-----\n.....\n-----END PRIVATE KEY-----\n",
    ///       "client_email": "abc@example-project-1234.iam.gserviceaccount.com",
    ///       "client_id": "123456789012345678901",
    ///       "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    ///       "token_uri": "https://oauth2.googleapis.com/token",
    ///       "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    ///       "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/abc%example-project-1234.iam.gserviceaccount.com",
    ///       "universe_domain": "googleapis.com"
    ///     }
    /// }"#).unwrap();
    /// ```
    ///
    #[serde(rename = "gcs")]
    #[schema(title = "StorageCredentialGcs")]
    Gcs(GcsCredential),
}

impl SecretInStorage for StorageCredential {}

impl StorageCredential {
    #[must_use]
    pub fn storage_type(&self) -> &'static str {
        match self {
            StorageCredential::S3(_) => "s3",
            StorageCredential::Az(_) => "adls",
            StorageCredential::Gcs(_) => "gcs",
        }
    }

    /// Try to convert the credential into an S3 credential.
    ///
    /// # Errors
    /// Fails if the credential is not an S3 credential.
    pub fn try_to_s3(&self) -> Result<&S3Credential, UnexpectedStorageType> {
        match self {
            Self::S3(profile) => Ok(profile),
            _ => Err(UnexpectedStorageType {
                is: self.storage_type(),
                to: "s3",
            }),
        }
    }

    /// Try to convert the credential into an Az credential.
    ///
    /// # Errors
    /// Fails if the credential is not an Az credential.
    pub fn try_to_az(&self) -> Result<&AzCredential, UnexpectedStorageType> {
        match self {
            Self::Az(profile) => Ok(profile),
            _ => Err(UnexpectedStorageType {
                is: self.storage_type(),
                to: "adls",
            }),
        }
    }

    /// Try to convert the credential into an Gcs credential.
    ///
    ///  # Errors
    /// Fails if the credential is not an Gcs credential.
    pub fn try_to_gcs(&self) -> Result<&GcsCredential, UnexpectedStorageType> {
        match self {
            Self::Gcs(profile) => Ok(profile),
            _ => Err(UnexpectedStorageType {
                is: self.storage_type(),
                to: "gcs",
            }),
        }
    }
}

/// Split a location into a filesystem prefix and the path.
/// Splits at "://"
///
/// # Errors
/// Fails if the location does not contain "://"
pub fn split_location(location: &str) -> Result<(&str, &str), ErrorModel> {
    let mut split = location.splitn(2, "://");
    let prefix = split.next().ok_or_else(|| {
        ErrorModel::internal(
            format!("Unexpected location: {location}"),
            "UnexpectedLocationFormat",
            None,
        )
    })?;
    let path = split.next().ok_or_else(|| {
        ErrorModel::internal(
            format!("Unexpected location. Expected at least one `://`. Got: {location}"),
            "UnexpectedLocationFormat",
            None,
        )
    })?;
    Ok((prefix, path))
}

#[must_use]
pub fn join_location(prefix: &str, path: &str) -> String {
    format!("{prefix}://{path}")
}

pub(crate) async fn is_empty(
    io: &impl LakekeeperStorage,
    location: &Location,
) -> Result<bool, ValidationError> {
    tracing::debug!("Checking location is empty: {location}");

    let mut entry_stream = list_location(io, location, Some(1)).await.map_err(|e| {
        tracing::debug!("Initializing list location failed: {e}");
        ValidationError::from(e)
    })?;
    while let Some(entries) = entry_stream.next().await {
        let entries = entries.map_err(|e| {
            tracing::debug!("Stream batch failed: {e}");
            ValidationError::from(Box::new(e))
        })?;

        if !entries.is_empty() {
            tracing::debug!("Location `{location}` is not empty, entries: {entries:?}",);
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, str::FromStr};

    use iceberg::spec::{PartitionSpec, Schema, SortOrder, TableMetadata, TableMetadataBuilder};
    use needs_env_var::needs_env_var;

    use super::{
        s3::{S3AwsSystemIdentityCredential, S3CloudflareR2Credential},
        *,
    };
    use crate::{
        catalog::io::{delete_file, read_metadata_file, write_file},
        service::storage::s3::S3AccessKeyCredential,
    };

    #[test]
    fn test_split_location() {
        let location = "abfss://";
        let (prefix, path) = split_location(location).unwrap();
        assert_eq!(prefix, "abfss");
        assert_eq!(path, "");
        assert_eq!(join_location(prefix, path), location);

        let location = "abfss://foo/bar";
        let (prefix, path) = split_location(location).unwrap();
        assert_eq!(prefix, "abfss");
        assert_eq!(path, "foo/bar");
        assert_eq!(join_location(prefix, path), location);
    }

    #[test]
    fn test_default_locations() {
        let profile = StorageProfile::S3(
            S3Profile::builder()
                .bucket("my-bucket".to_string())
                .endpoint("http://localhost:9000".parse().unwrap())
                .region("us-east-1".to_string())
                .key_prefix("subfolder".to_string())
                .sts_enabled(false)
                .flavor(S3Flavor::Aws)
                .build(),
        );

        let target_location = "s3://my-bucket/subfolder/00000000-0000-0000-0000-000000000001/00000000-0000-0000-0000-000000000002";

        let namespace_id: NamespaceId = uuid::uuid!("00000000-0000-0000-0000-000000000001").into();
        let namespace_location = profile.default_namespace_location(namespace_id).unwrap();
        let table_id = TabularId::View(uuid::uuid!("00000000-0000-0000-0000-000000000002"));
        let table_location = profile.default_tabular_location(&namespace_location, table_id);
        assert_eq!(table_location.to_string(), target_location);

        let mut namespace_location_without_slash = namespace_location.clone();
        namespace_location_without_slash.without_trailing_slash();
        let table_location =
            profile.default_tabular_location(&namespace_location_without_slash, table_id);
        assert!(!namespace_location_without_slash.to_string().ends_with('/'));
        assert_eq!(table_location.to_string(), target_location);
    }

    #[test]
    fn test_redact_s3_access_key() {
        let secrets: StorageCredential = S3Credential::AccessKey(S3AccessKeyCredential {
            aws_access_key_id: "
                AKIAIOSFODNN7EXAMPLE
            "
            .to_string(),
            aws_secret_access_key: "
                wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
            "
            .to_string(),
            external_id: Some("abctnFEMI".to_string()),
        })
        .into();

        let debug_print = format!("{secrets:?}");
        assert!(!debug_print.contains("tnFEMI"));
    }

    #[test]
    fn test_redact_s3_external_id() {
        let secrets: StorageCredential =
            S3Credential::AwsSystemIdentity(S3AwsSystemIdentityCredential {
                external_id: Some("abctnFEMI".to_string()),
            })
            .into();
        let debug_print = format!("{secrets:?}");
        assert!(!debug_print.contains("tnFEMI"));
    }

    #[test]
    fn test_redact_cloudflare() {
        let secrets: StorageCredential = S3Credential::CloudflareR2(S3CloudflareR2Credential {
            access_key_id: "def".to_string(),
            secret_access_key: "abc".to_string(),
            token: "abc".to_string(),
            account_id: "hij".to_string(),
        })
        .into();
        let debug_print = format!("{secrets:?}");
        assert!(!debug_print.contains("abc"));
    }

    #[test]
    fn test_s3_profile_de_from_v1() {
        let value = serde_json::json!({
            "type": "s3",
            "bucket": "my-bucket",
            "endpoint": "http://localhost:9000",
            "region": "us-east-1",
            "sts-enabled": false,
        });

        let profile: StorageProfile = serde_json::from_value(value).unwrap();
        assert_eq!(
            profile,
            StorageProfile::S3(
                S3Profile::builder()
                    .bucket("my-bucket".to_string())
                    .endpoint("http://localhost:9000".parse().unwrap())
                    .region("us-east-1".to_string())
                    .sts_enabled(false)
                    .flavor(S3Flavor::Aws)
                    .build()
            )
        );
    }

    #[test]
    fn test_s3_secret_de_from_v1() {
        let value = serde_json::json!({
            "type": "s3",
            "credential-type": "access-key",
            "aws-access-key-id": "AKIAIOSFODNN7EXAMPLE",
            "aws-secret-access-key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        });

        let secret: StorageCredential = serde_json::from_value(value).unwrap();
        assert_eq!(
            secret,
            StorageCredential::S3(S3Credential::AccessKey(S3AccessKeyCredential {
                aws_access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
                aws_secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
                external_id: None
            }))
        );
    }

    #[test]
    fn test_is_allowed_location_s3() {
        let profile = StorageProfile::S3(
            S3Profile::builder()
                .bucket("my.bucket".to_string())
                .endpoint("http://localhost:9000".parse().unwrap())
                .region("us-east-1".to_string())
                .sts_enabled(false)
                .flavor(S3Flavor::Aws)
                .key_prefix("my/subpath".to_string())
                .build(),
        );

        let cases = vec![
            ("s3://my.bucket/my/subpath/ns-id", true),
            ("s3://my.bucket/my/subpath/ns-id/", true),
            ("s3://my.bucket/my/subpath/ns-id/tbl-id", true),
            ("s3://my.bucket/my/subpath/ns-id/tbl-id/", true),
            ("s3://other.bucket/my/subpath/ns-id/tbl-id/", false),
            ("s3://my.bucket/other/subpath/ns-id/tbl-id/", false),
            // Exact path should not be accepted
            ("s3://my.bucket/my/subpath", false),
            ("s3://my.bucket/my/subpath/", false),
        ];

        for (sublocation, expected_result) in cases {
            let sublocation = Location::from_str(sublocation).unwrap();
            assert_eq!(
                profile.is_allowed_location(&sublocation),
                expected_result,
                "Base Location: {}, Maybe sublocation: {sublocation}",
                profile.base_location().unwrap(),
            );
        }
    }

    #[test]
    fn test_is_allowed_location_wasbs() {
        let profile = StorageProfile::Adls(AdlsProfile {
            filesystem: "filesystem".to_string(),
            key_prefix: Some("test_prefix".to_string()),
            account_name: "account".to_string(),
            authority_host: None,
            host: None,
            sas_token_validity_seconds: None,
            allow_alternative_protocols: true,
        });

        let cases = vec![
            (
                "abfss://filesystem@account.dfs.core.windows.net/test_prefix/ns/t",
                true,
            ),
            (
                "wasbs://filesystem@account.dfs.core.windows.net/test_prefix/ns/t",
                true,
            ),
        ];

        for (sublocation, expected_result) in cases {
            let sublocation = Location::from_str(sublocation).unwrap();
            assert_eq!(
                profile.is_allowed_location(&sublocation),
                expected_result,
                "Base Location: {}, Maybe sublocation: {sublocation}",
                profile.base_location().unwrap(),
            );
        }
    }

    #[tokio::test]
    #[needs_env_var::needs_env_var(TEST_AZURE = 1)]
    async fn test_vended_az() {
        for (cred, _typ) in [
            (az::test::azure_tests::client_creds(), "client-credentials"),
            (az::test::azure_tests::shared_key(), "shared-key"),
        ] {
            let mut profile: StorageProfile = az::test::azure_tests::azure_profile().into();
            let cred: StorageCredential = cred.into();
            test_profile_vended_creds(&cred, &mut profile).await;
            test_profile_io(&cred, &mut profile).await;
        }
    }

    #[tokio::test]
    #[needs_env_var::needs_env_var(TEST_GCS = 1)]
    async fn test_vended_gcs() {
        let key_prefix = Some(format!("test_prefix-{}", uuid::Uuid::now_v7()));
        let cred: StorageCredential = std::env::var("LAKEKEEPER_TEST__GCS_CREDENTIAL")
            .map(|s| GcsCredential::ServiceAccountKey {
                key: serde_json::from_str::<GcsServiceKey>(&s).unwrap(),
            })
            .map_err(|_| ())
            .expect("Missing cred")
            .into();
        let bucket = std::env::var("LAKEKEEPER_TEST__GCS_BUCKET").expect("Missing bucket");
        let mut profile: StorageProfile = GcsProfile {
            bucket,
            key_prefix: key_prefix.clone(),
        }
        .into();

        test_profile_vended_creds(&cred, &mut profile).await;
        test_profile_io(&cred, &mut profile).await;
    }

    #[needs_env_var(TEST_AWS = 1)]
    #[test]
    fn test_vended_aws() {
        crate::test::test_block_on(
            async {
                let key_prefix = format!("test_prefix-{}", uuid::Uuid::now_v7());
                let bucket = std::env::var("AWS_S3_BUCKET").unwrap();
                let region = std::env::var("AWS_S3_REGION").unwrap();
                let sts_role_arn = std::env::var("AWS_S3_STS_ROLE_ARN").unwrap();
                let cred: StorageCredential = S3Credential::AccessKey(S3AccessKeyCredential {
                    aws_access_key_id: std::env::var("AWS_S3_ACCESS_KEY_ID").unwrap(),
                    aws_secret_access_key: std::env::var("AWS_S3_SECRET_ACCESS_KEY").unwrap(),
                    external_id: None,
                })
                .into();

                let mut profile: StorageProfile = S3Profile::builder()
                    .bucket(bucket)
                    .key_prefix(key_prefix.clone())
                    .region(region)
                    .sts_role_arn(sts_role_arn)
                    .sts_enabled(true)
                    .flavor(S3Flavor::Aws)
                    .build()
                    .into();

                test_profile_vended_creds(&cred, &mut profile).await;
                test_profile_io(&cred, &mut profile).await;
            },
            true,
        );
    }

    #[needs_env_var(TEST_AWS = 1)]
    #[tokio::test]
    // #[tracing_test::traced_test]
    async fn test_validate_aws() {
        use super::s3::test::aws::get_storage_profile;

        let (profile, credential) = get_storage_profile();
        let profile: StorageProfile = profile.into();
        let cred: StorageCredential = credential.into();
        Box::pin(profile.validate_access(
            Some(&cred),
            None,
            &RequestMetadata::new_unauthenticated(),
        ))
        .await
        .expect("Failed to validate access");
    }

    #[needs_env_var::needs_env_var(TEST_MINIO = 1)]
    #[test]
    fn test_vended_s3_compat() {
        use super::s3::test::s3_compat::storage_profile;

        crate::test::test_block_on(
            async {
                let key_prefix = format!("test_prefix-{}", uuid::Uuid::now_v7());
                let (profile, cred) = storage_profile(&key_prefix);
                let mut profile: StorageProfile = profile.into();
                let cred: StorageCredential = cred.into();

                test_profile_vended_creds(&cred, &mut profile).await;
                test_profile_io(&cred, &mut profile).await;
            },
            true,
        );
    }

    #[allow(dead_code)]
    fn generate_table_metadata() -> TableMetadata {
        TableMetadataBuilder::new(
            Schema::builder().build().expect("Failed to build schema"),
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            format!("test-table-{}", uuid::Uuid::now_v7(),),
            iceberg::spec::FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .expect("Failed to build table metadata")
        .metadata
    }

    #[allow(dead_code, clippy::too_many_lines)]
    async fn test_profile_io(cred: &StorageCredential, profile: &mut StorageProfile) {
        profile
            .normalize(Some(cred))
            .expect("Failed to normalize profile");
        let base_location = profile
            .base_location()
            .expect("Failed to get base location");
        let table_location = base_location.clone();
        let mut metadata_location = table_location.clone();
        metadata_location
            .without_trailing_slash()
            .push("test.gz.metadata.json");

        let io = profile.file_io(Some(cred)).await.unwrap();

        let m = generate_table_metadata();

        write_file(&io, &metadata_location, m.clone(), CompressionCodec::Gzip)
            .await
            .unwrap();
        let read_metadata = read_metadata_file(&io, &metadata_location)
            .await
            .expect("Failed to read metadata file");
        assert_eq!(read_metadata, m);
        delete_file(&io, &metadata_location)
            .await
            .expect("Failed to delete metadata file");
        // Check that the location is empty
        assert!(
            is_empty(&io, &table_location).await.unwrap(),
            "Location should be empty after delete"
        );
    }

    #[allow(dead_code, clippy::too_many_lines)]
    async fn test_profile_vended_creds(cred: &StorageCredential, profile: &mut StorageProfile) {
        profile
            .normalize(Some(cred))
            .expect("Failed to normalize profile");
        let base_location = profile
            .base_location()
            .expect("Failed to get base location");
        let mut table_location1 = base_location.clone();
        table_location1.without_trailing_slash().push("test");
        let mut table_location2 = base_location.clone();
        table_location2.without_trailing_slash().push("test2");

        let config1 = profile
            .generate_table_config(
                DataAccess {
                    vended_credentials: true,
                    remote_signing: false,
                },
                Some(cred),
                &table_location1,
                StoragePermissions::ReadWriteDelete,
                &RequestMetadata::new_unauthenticated(),
                WarehouseId::new_random(),
                TableId::new_random().into(),
            )
            .await
            .unwrap();

        let config2 = profile
            .generate_table_config(
                DataAccess {
                    vended_credentials: true,
                    remote_signing: false,
                },
                Some(cred),
                &table_location2,
                StoragePermissions::ReadWriteDelete,
                &RequestMetadata::new_unauthenticated(),
                WarehouseId::new_random(),
                TableId::new_random().into(),
            )
            .await
            .unwrap();
        let (downscoped1, downscoped2) = match profile {
            StorageProfile::Adls(_) => {
                let downscoped1 = az::get_file_io_from_table_config(&config1.config).unwrap();
                let downscoped2 = az::get_file_io_from_table_config(&config2.config).unwrap();
                (downscoped1, downscoped2)
            }
            StorageProfile::S3(_) => {
                let downscoped1 = s3::get_file_io_from_table_config(&config1.config).unwrap();
                let downscoped2 = s3::get_file_io_from_table_config(&config2.config).unwrap();
                (downscoped1, downscoped2)
            }
            StorageProfile::Gcs(_) => {
                let downscoped1 = gcs::get_file_io_from_table_config(&config1.config).unwrap();
                let downscoped2 = gcs::get_file_io_from_table_config(&config2.config).unwrap();
                (downscoped1, downscoped2)
            }
            StorageProfile::Memory(_) => {
                unreachable!("Local storage does not support vended credentials")
            }
        };
        // can read & write in own locations
        let test_file1 = table_location1.cloning_push("test.txt");
        let test_file2 = table_location2.cloning_push("test.txt");

        downscoped1
            .new_output(&test_file1)
            .unwrap()
            .write("test content 1".into())
            .await
            .unwrap();

        downscoped2
            .new_output(&test_file2)
            .unwrap()
            .write("test content 2".into())
            .await
            .unwrap();

        let input1 = downscoped1
            .new_input(&test_file1)
            .unwrap()
            .read()
            .await
            .unwrap();
        assert_eq!(input1, "test content 1");

        let input2 = downscoped2
            .new_input(&test_file2)
            .unwrap()
            .read()
            .await
            .unwrap();
        assert_eq!(input2, "test content 2");

        // cannot read across locations
        let _ = downscoped1
            .new_input(&test_file2)
            .unwrap()
            .read()
            .await
            .unwrap_err();
        let _ = downscoped2
            .new_input(&test_file1)
            .unwrap()
            .read()
            .await
            .unwrap_err();

        // cannot write across locations
        let _ = downscoped1
            .new_output(&test_file2)
            .unwrap()
            .write("this-should-fail".into())
            .await
            .unwrap_err();

        let _ = downscoped2
            .new_output(&test_file1)
            .unwrap()
            .write("this-should-fail".into())
            .await
            .unwrap_err();

        // cannot delete across locations
        downscoped1.delete(&test_file2).await.unwrap_err();
        downscoped2.delete(&test_file1).await.unwrap_err();

        // can delete in own locations
        downscoped1.delete(&test_file1).await.unwrap();
        downscoped2.delete(&test_file2).await.unwrap();

        // cleanup
        profile
            .file_io(Some(cred))
            .await
            .unwrap()
            .remove_all(base_location.as_str())
            .await
            .unwrap();
    }

    #[test]
    fn test_memory_profile_serde() {
        let profile = MemoryProfile::default();
        let serialized = serde_json::to_string(&profile).unwrap();
        assert!(serialized.contains("memory://"));
        assert!(serialized.contains("base-location"));
        let deserialized: MemoryProfile = serde_json::from_str(&serialized).unwrap();
        assert_eq!(profile, deserialized);
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_local_profile_validate_access() {
        let profile: StorageProfile = MemoryProfile::default().into();
        let cred: Option<StorageCredential> = None;
        let request_metadata = RequestMetadata::new_unauthenticated();

        Box::pin(profile.validate_access(cred.as_ref(), None, &request_metadata))
            .await
            .unwrap();
    }
}
