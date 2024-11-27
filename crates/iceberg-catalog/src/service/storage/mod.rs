#![allow(clippy::match_wildcard_for_single_variants)]

mod az;
mod error;
mod gcs;
mod s3;

use super::{secrets::SecretInStorage, NamespaceIdentUuid, TableIdentUuid};
use crate::api::{iceberg::v1::DataAccess, CatalogConfig};
use crate::catalog::compression_codec::CompressionCodec;
use crate::catalog::io::list_location;
use crate::service::tabular_idents::TabularIdentUuid;
use crate::WarehouseIdent;
pub use az::{AdlsLocation, AdlsProfile, AzCredential};
pub(crate) use error::ValidationError;
use error::{ConversionError, CredentialsError, FileIoError, TableConfigError, UpdateError};
use futures::StreamExt;
pub use gcs::{GcsCredential, GcsProfile, GcsServiceKey};
use iceberg::io::FileIO;
use iceberg_ext::configs::table::TableProperties;
use iceberg_ext::configs::Location;
pub use s3::{S3Credential, S3Flavor, S3Location, S3Profile};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Storage profile for a warehouse.
#[derive(
    Debug, Clone, Eq, PartialEq, Serialize, Deserialize, derive_more::From, utoipa::ToSchema,
)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum StorageProfile {
    /// Azure storage profile
    #[serde(rename = "adls", alias = "azdls")]
    #[schema(title = "StorageProfileAdls")]
    Adls(AdlsProfile),
    /// S3 storage profile
    #[serde(rename = "s3")]
    #[schema(title = "StorageProfileS3")]
    S3(S3Profile),
    #[cfg(test)]
    #[serde(rename = "test")]
    Test(TestProfile),
    #[serde(rename = "gcs")]
    #[schema(title = "StorageProfileGcs")]
    Gcs(GcsProfile),
}

#[derive(Debug, Clone, strum_macros::Display)]
pub enum StorageType {
    #[strum(serialize = "s3")]
    S3,
    #[strum(serialize = "adls")]
    Adls,
    #[cfg(test)]
    #[strum(serialize = "test")]
    Test,
    #[strum(serialize = "gcs")]
    Gcs,
}

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum StoragePermissions {
    Read,
    ReadWrite,
    ReadWriteDelete,
}

impl StorageProfile {
    #[must_use]
    pub fn generate_catalog_config(&self, warehouse_id: WarehouseIdent) -> CatalogConfig {
        match self {
            StorageProfile::S3(profile) => profile.generate_catalog_config(warehouse_id),
            #[cfg(test)]
            StorageProfile::Test(_) => {
                use std::collections::HashMap;
                CatalogConfig {
                    overrides: HashMap::default(),
                    defaults: HashMap::default(),
                }
            }
            StorageProfile::Adls(prof) => prof.generate_catalog_config(warehouse_id),
            StorageProfile::Gcs(prof) => prof.generate_catalog_config(warehouse_id),
        }
    }

    /// Check if the profile can be updated with the other profile.
    /// This function should fail if the new profile might point to a different location
    /// and thus existing data might be lost.
    ///
    /// # Errors
    /// Fails if the profiles are not compatible, typically because the location changed
    pub fn can_be_updated_with(&self, other: &Self) -> Result<(), UpdateError> {
        match (self, other) {
            (StorageProfile::S3(this_profile), StorageProfile::S3(other_profile)) => {
                this_profile.can_be_updated_with(other_profile)
            }
            (StorageProfile::Adls(this_profile), StorageProfile::Adls(other_profile)) => {
                this_profile.can_be_updated_with(other_profile)
            }
            #[cfg(test)]
            (StorageProfile::Test(_), _) => Ok(()),
            #[cfg(test)]
            (_, StorageProfile::Test(_)) => Ok(()),
            (_, _) => Err(UpdateError::IncompatibleProfiles(
                self.storage_type().to_string(),
                other.storage_type().to_string(),
            )),
        }
    }

    /// Create a new file IO instance for the storage profile.
    ///
    /// # Errors
    /// Fails if the underlying storage profile's file IO creation fails.
    pub fn file_io(
        &self,
        secret: Option<&StorageCredential>,
    ) -> Result<iceberg::io::FileIO, FileIoError> {
        match self {
            StorageProfile::S3(profile) => profile.file_io(
                secret
                    .map(|s| s.try_to_s3())
                    .transpose()?
                    .map(Into::into)
                    .as_ref(),
            ),
            StorageProfile::Adls(prof) => prof.file_io(secret.map(|s| s.try_to_az()).transpose()?),
            #[cfg(test)]
            StorageProfile::Test(_) => Ok(iceberg::io::FileIOBuilder::new("file").build()?),
            StorageProfile::Gcs(prof) => {
                Ok(prof.file_io(secret.map(|s| s.try_into_gcs()).transpose()?)?)
            }
        }
    }

    /// Get the base location of this Storage Profiles
    ///
    /// # Errors
    /// Can fail for un-normalized profiles.
    pub fn base_location(&self) -> Result<Location, ValidationError> {
        match self {
            StorageProfile::S3(profile) => profile.base_location().map(Into::into),
            StorageProfile::Adls(profile) => profile.base_location(),
            StorageProfile::Gcs(profile) => profile.base_location(),
            #[cfg(test)]
            StorageProfile::Test(profile) => {
                std::str::FromStr::from_str(&format!("file://tmp/{}", profile.base_location))
                    .map_err(|_| ValidationError::InvalidLocation {
                        reason: "Invalid namespace location".to_string(),
                        location: "file://tmp/".to_string(),
                        source: None,
                        storage_type: self.storage_type(),
                    })
            }
        }
    }

    /// Get the default location for the namespace.
    ///
    /// # Errors
    /// Fails if the `key_prefix` is not valid for S3 URLs.
    pub fn default_namespace_location(
        &self,
        namespace_id: NamespaceIdentUuid,
    ) -> Result<Location, ValidationError> {
        let mut base_location: Location = self.base_location()?;
        base_location
            .without_trailing_slash()
            .push(&namespace_id.to_string());
        Ok(base_location)
    }

    #[must_use]
    pub fn storage_type(&self) -> StorageType {
        match self {
            StorageProfile::S3(_) => StorageType::S3,
            #[cfg(test)]
            StorageProfile::Test(_) => StorageType::Test,
            StorageProfile::Adls(_) => StorageType::Adls,
            StorageProfile::Gcs(_) => StorageType::Gcs,
        }
    }

    /// Generate the table config for the storage profile.
    ///
    /// # Errors
    /// Fails if the underlying storage profile's generation fails.
    pub async fn generate_table_config(
        &self,
        data_access: &DataAccess,
        secret: Option<&StorageCredential>,
        table_location: &Location,
        storage_permissions: StoragePermissions,
    ) -> Result<TableProperties, TableConfigError> {
        match self {
            StorageProfile::S3(profile) => {
                profile
                    .generate_table_config(
                        data_access,
                        secret.map(|s| s.try_to_s3()).transpose()?,
                        table_location,
                        storage_permissions,
                    )
                    .await
            }
            StorageProfile::Adls(profile) => {
                profile
                    .generate_table_config(
                        data_access,
                        table_location,
                        secret
                            .ok_or_else(|| {
                                CredentialsError::MissingCredential(self.storage_type())
                            })?
                            .try_to_az()?,
                        storage_permissions,
                    )
                    .await
            }
            #[cfg(test)]
            StorageProfile::Test(_) => Ok(TableProperties::default()),
            StorageProfile::Gcs(profile) => {
                profile
                    .generate_table_config(
                        data_access,
                        secret.map(|s| s.try_into_gcs()).transpose()?,
                        table_location,
                        storage_permissions,
                    )
                    .await
            }
        }
    }

    /// Try to normalize the storage profile.
    /// Fails if some validation fails. This does not check physical filesystem access.
    ///
    /// # Errors
    /// Fails if the underlying storage profile's normalization fails.
    pub fn normalize(&mut self) -> Result<(), ValidationError> {
        // ------------- Common validations -------------
        // Test if we can generate a default namespace location
        let ns_location = self.default_namespace_location(NamespaceIdentUuid::default())?;
        self.default_tabular_location(&ns_location, TableIdentUuid::default().into());

        // ------------- Profile specific validations -------------
        match self {
            StorageProfile::S3(profile) => profile.normalize(),
            StorageProfile::Adls(prof) => prof.normalize(),
            #[cfg(test)]
            StorageProfile::Test(_) => Ok(()),
            StorageProfile::Gcs(profile) => profile.normalize(),
        }
    }

    /// Validate physical access
    ///
    /// If location is not provided, a dummy table location is used.
    ///
    /// # Errors
    /// Fails if a file cannot be written and deleted.
    pub async fn validate_access(
        &self,
        credential: Option<&StorageCredential>,
        location: Option<&Location>,
    ) -> Result<(), ValidationError> {
        let file_io = self.file_io(credential)?;

        let ns_id = NamespaceIdentUuid::default();
        let table_id = TableIdentUuid::default();
        let ns_location = self.default_namespace_location(ns_id)?;
        let test_location = location.map_or_else(
            || self.default_tabular_location(&ns_location, table_id.into()),
            std::borrow::ToOwned::to_owned,
        );
        // Validate direct read/write access
        self.validate_read_write(&file_io, &test_location, false)
            .await?;

        // Test vended-credentials access
        let test_vended_credentials = match self {
            StorageProfile::S3(profile) => profile.sts_enabled,
            StorageProfile::Adls(_) => true,
            StorageProfile::Gcs(_) => true,
            #[cfg(test)]
            StorageProfile::Test(_) => false,
        };

        if test_vended_credentials {
            let tbl_config = self
                .generate_table_config(
                    &DataAccess {
                        remote_signing: false,
                        vended_credentials: true,
                    },
                    credential,
                    &test_location,
                    StoragePermissions::ReadWriteDelete,
                )
                .await?;
            match &self {
                StorageProfile::S3(_) => {
                    let sts_file_io = s3::get_file_io_from_table_config(&tbl_config)?;
                    self.validate_read_write(&sts_file_io, &test_location, true)
                        .await?;
                }
                StorageProfile::Adls(_) => {
                    az::validate_vended_credentials(&tbl_config, &test_location, self).await?;
                }
                #[cfg(test)]
                StorageProfile::Test(_) => {}
                StorageProfile::Gcs(_) => {
                    let sts_file_io = gcs::get_file_io_from_table_config(&tbl_config)?;
                    self.validate_read_write(&sts_file_io, &test_location, true)
                        .await?;
                }
            }
        }
        tracing::info!("Cleanup started");
        // Cleanup
        crate::catalog::io::remove_all(&file_io, &test_location)
            .await
            .map_err(|e| ValidationError::IoOperationFailed(e, Box::new(self.clone())))?;

        tracing::info!("Cleanup finished");

        check_location_is_empty(&file_io, &test_location, self, || {
            ValidationError::InvalidLocation {
                reason: "Files are left after remove_all on test location".to_string(),
                source: None,
                location: test_location.to_string(),
                storage_type: self.storage_type(),
            }
        })
        .await?;
        tracing::info!("checked location is empty");
        Ok(())
    }

    async fn validate_read_write(
        &self,
        file_io: &iceberg::io::FileIO,
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
                .url()
                .path()
                .split('/')
                .next_back()
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
        crate::catalog::io::write_metadata_file(
            &test_file_write,
            "test",
            compression_codec,
            file_io,
        )
        .await
        .map_err(|e| ValidationError::IoOperationFailed(e, Box::new(self.clone())))?;

        // Test read
        let _ = crate::catalog::io::read_file(file_io, &test_file_write)
            .await
            .map_err(|e| ValidationError::IoOperationFailed(e, Box::new(self.clone())))?;

        // Test delete
        crate::catalog::io::delete_file(file_io, &test_file_write)
            .await
            .map_err(|e| ValidationError::IoOperationFailed(e, Box::new(self.clone())))?;

        tracing::debug!(
            "Successfully wrote, read and deleted file at: {}",
            test_file_write
        );

        Ok(())
    }

    /// Try to convert the storage profile into an S3 profile.
    ///
    /// # Errors
    /// Fails if the profile is not an S3 profile.
    pub fn try_into_s3(self) -> Result<S3Profile, ConversionError> {
        match self {
            Self::S3(profile) => Ok(profile),
            _ => Err(ConversionError {
                is: self.storage_type(),
                to: StorageType::S3,
            }),
        }
    }

    /// Try to convert the storage profile into an Az profile.
    ///
    /// # Errors
    /// Fails if the profile is not an Az profile.
    pub fn try_into_az(self) -> Result<AdlsProfile, ConversionError> {
        match self {
            Self::Adls(profile) => Ok(profile),
            _ => Err(ConversionError {
                is: self.storage_type(),
                to: StorageType::Adls,
            }),
        }
    }

    #[must_use]
    pub fn is_allowed_location(&self, other: &Location) -> bool {
        let base_location = self.base_location().ok();

        if let Some(mut base_location) = base_location {
            base_location.with_trailing_slash();
            if other == &base_location {
                return false;
            }

            other.is_sublocation_of(&base_location)
        } else {
            false
        }
    }
}

pub trait StorageLocations {
    /// Get the default tabular location for the storage profile.
    fn default_tabular_location(
        &self,
        namespace_location: &Location,
        table_id: TabularIdentUuid,
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

#[derive(Debug, Eq, Clone, PartialEq, Serialize, Deserialize)]
pub struct TestProfile {
    base_location: Uuid,
}

impl Default for TestProfile {
    fn default() -> Self {
        Self {
            base_location: Uuid::now_v7(),
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
    /// use iceberg_catalog::service::storage::StorageCredential;
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
    /// use iceberg_catalog::service::storage::StorageCredential;
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
    /// use iceberg_catalog::service::storage::StorageCredential;
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
    pub fn storage_type(&self) -> StorageType {
        match self {
            StorageCredential::S3(_) => StorageType::S3,
            StorageCredential::Az(_) => StorageType::Adls,
            StorageCredential::Gcs(_) => StorageType::Gcs,
        }
    }

    /// Try to convert the credential into an S3 credential.
    ///
    /// # Errors
    /// Fails if the credential is not an S3 credential.
    pub fn try_to_s3(&self) -> Result<&S3Credential, CredentialsError> {
        match self {
            Self::S3(profile) => Ok(profile),
            _ => Err(ConversionError {
                is: self.storage_type(),
                to: StorageType::S3,
            }
            .into()),
        }
    }

    /// Try to convert the credential into an Az credential.
    ///
    /// # Errors
    /// Fails if the credential is not an Az credential.
    pub fn try_to_az(&self) -> Result<&AzCredential, CredentialsError> {
        match self {
            Self::Az(profile) => Ok(profile),
            _ => Err(ConversionError {
                is: self.storage_type(),
                to: StorageType::Adls,
            }
            .into()),
        }
    }

    /// Try to convert the credential into an Gcs credential.
    ///
    ///  # Errors
    /// Fails if the credential is not an Gcs credential.
    pub fn try_into_gcs(&self) -> Result<&GcsCredential, CredentialsError> {
        match self {
            Self::Gcs(profile) => Ok(profile),
            _ => Err(ConversionError {
                is: self.storage_type(),
                to: StorageType::Gcs,
            }
            .into()),
        }
    }
}

pub mod path_utils {
    use lazy_regex::regex;

    /// Reduce the scheme string to only the path.
    #[must_use]
    pub fn reduce_scheme_string(path: &str, only_path: bool) -> String {
        let re = regex!("^(?<protocol>abfss?://)[^/@]+@[^/]+(?<path>/.+)");
        if let Some(caps) = re.captures(path) {
            let mut metadata_location = String::new();
            if only_path {
                caps.expand("$path", &mut metadata_location);
            } else {
                caps.expand("$protocol$path", &mut metadata_location);
            }
            return metadata_location;
        };
        path.to_string()
    }
}

pub(crate) async fn check_location_is_empty(
    file_io: &FileIO,
    location: &Location,
    storage_profile: &StorageProfile,
    error_fn: impl FnOnce() -> ValidationError,
) -> Result<(), ValidationError> {
    tracing::info!("Checking location is empty: {location}");

    let mut entry_stream = list_location(file_io, location, Some(1))
        .await
        .map_err(|e| ValidationError::IoOperationFailed(e, Box::new(storage_profile.clone())))?;
    while let Some(entries) = entry_stream.next().await {
        let entries = entries.map_err(|e| {
            ValidationError::IoOperationFailed(e, Box::new(storage_profile.clone()))
        })?;

        if !entries.is_empty() {
            tracing::debug!("Location is not empty: {location}, entries: {entries:?}",);
            let er = error_fn();
            return Err(er);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use needs_env_var::needs_env_var;
    use std::str::FromStr;

    #[test]
    fn test_reduce_scheme_string() {
        let path = "abfss://filesystem@dfs.windows.net/path/_test";
        let reduced_path = path_utils::reduce_scheme_string(path, true);
        assert_eq!(reduced_path, "/path/_test");

        let reduced_path = path_utils::reduce_scheme_string(path, false);
        // ToDo Tobi: Is this correct?
        assert_eq!(reduced_path, "abfss:///path/_test");
    }

    #[test]
    fn test_default_locations() {
        let profile = StorageProfile::S3(S3Profile {
            bucket: "my-bucket".to_string(),
            endpoint: Some("http://localhost:9000".parse().unwrap()),
            region: "us-east-1".to_string(),
            assume_role_arn: None,
            path_style_access: None,
            key_prefix: Some("subfolder".to_string()),
            sts_role_arn: None,
            sts_enabled: false,
            flavor: S3Flavor::Aws,
        });

        let target_location = "s3://my-bucket/subfolder/00000000-0000-0000-0000-000000000001/00000000-0000-0000-0000-000000000002";

        let namespace_id: NamespaceIdentUuid =
            uuid::uuid!("00000000-0000-0000-0000-000000000001").into();
        let namespace_location = profile.default_namespace_location(namespace_id).unwrap();
        let table_id = TabularIdentUuid::View(uuid::uuid!("00000000-0000-0000-0000-000000000002"));
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
    fn test_redact() {
        let secrets: StorageCredential = S3Credential::AccessKey {
            aws_access_key_id: "
                AKIAIOSFODNN7EXAMPLE
            "
            .to_string(),
            aws_secret_access_key: "
                wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
            "
            .to_string(),
        }
        .into();

        let debug_print = format!("{secrets:?}");
        assert!(!debug_print.contains("tnFEMI"));
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
            StorageProfile::S3(S3Profile {
                bucket: "my-bucket".to_string(),
                endpoint: Some("http://localhost:9000".parse().unwrap()),
                region: "us-east-1".to_string(),
                assume_role_arn: None,
                path_style_access: None,
                key_prefix: None,
                sts_role_arn: None,
                sts_enabled: false,
                flavor: S3Flavor::Aws,
            })
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
            StorageCredential::S3(S3Credential::AccessKey {
                aws_access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
                aws_secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()
            })
        );
    }

    #[test]
    fn test_is_allowed_location() {
        let profile = StorageProfile::S3(S3Profile {
            bucket: "my.bucket".to_string(),
            endpoint: Some("http://localhost:9000".parse().unwrap()),
            region: "us-east-1".to_string(),
            assume_role_arn: None,
            path_style_access: None,
            key_prefix: Some("my/subpath".to_string()),
            sts_role_arn: None,
            sts_enabled: false,
            flavor: S3Flavor::Aws,
        });

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

    // TODO: add vended azure test here once opendal supports sas

    #[tokio::test]
    #[needs_env_var::needs_env_var(TEST_GCS = 1)]
    async fn test_vended_gcs() {
        let key_prefix = Some(format!("test_prefix-{}", uuid::Uuid::now_v7()));

        {
            let cred: StorageCredential = std::env::var("GCS_CREDENTIAL")
                .map(|s| GcsCredential::ServiceAccountKey {
                    key: serde_json::from_str::<GcsServiceKey>(&s).unwrap(),
                })
                .map_err(|_| ())
                .expect("Missing cred")
                .into();
            let bucket = std::env::var("GCS_BUCKET").expect("Missing bucket");
            let mut profile: StorageProfile = GcsProfile {
                bucket,
                key_prefix: key_prefix.clone(),
            }
            .into();

            test_profile(&cred, &mut profile).await;
        }
    }

    #[needs_env_var(TEST_AWS = 1)]
    #[test]
    fn test_vended_aws() {
        crate::test::test_block_on(
            async {
                let key_prefix = Some(format!("test_prefix-{}", uuid::Uuid::now_v7()));
                let bucket = std::env::var("AWS_S3_BUCKET").unwrap();
                let region = std::env::var("AWS_S3_REGION").unwrap();
                let sts_role_arn = std::env::var("AWS_S3_STS_ROLE_ARN").unwrap();
                let cred: StorageCredential = S3Credential::AccessKey {
                    aws_access_key_id: std::env::var("AWS_S3_ACCESS_KEY_ID").unwrap(),
                    aws_secret_access_key: std::env::var("AWS_S3_SECRET_ACCESS_KEY").unwrap(),
                }
                .into();

                let mut profile: StorageProfile = S3Profile {
                    bucket,
                    key_prefix: key_prefix.clone(),
                    assume_role_arn: None,
                    endpoint: None,
                    region,
                    path_style_access: Some(true),
                    sts_role_arn: Some(sts_role_arn),
                    flavor: S3Flavor::Aws,
                    sts_enabled: true,
                }
                .into();

                test_profile(&cred, &mut profile).await;
            },
            true,
        );
    }

    #[needs_env_var::needs_env_var(TEST_MINIO = 1)]
    #[test]
    fn test_vended_minio() {
        crate::test::test_block_on(
            async {
                let key_prefix = Some(format!("test_prefix-{}", uuid::Uuid::now_v7()));
                let bucket = std::env::var("LAKEKEEPER_TEST__S3_BUCKET").unwrap();
                let region = std::env::var("LAKEKEEPER_TEST__S3_REGION").unwrap_or("local".into());
                let aws_access_key_id = std::env::var("LAKEKEEPER_TEST__S3_ACCESS_KEY").unwrap();
                let aws_secret_access_key =
                    std::env::var("LAKEKEEPER_TEST__S3_SECRET_KEY").unwrap();
                let endpoint = std::env::var("LAKEKEEPER_TEST__S3_ENDPOINT").unwrap();

                let cred: StorageCredential = S3Credential::AccessKey {
                    aws_access_key_id,
                    aws_secret_access_key,
                }
                .into();

                let mut profile = S3Profile {
                    bucket,
                    key_prefix: key_prefix.clone(),
                    assume_role_arn: None,
                    endpoint: Some(endpoint.parse().unwrap()),
                    region,
                    path_style_access: Some(true),
                    sts_role_arn: None,
                    flavor: S3Flavor::Minio,
                    sts_enabled: true,
                }
                .into();

                test_profile(&cred, &mut profile).await;
            },
            true,
        );
    }

    #[allow(dead_code, clippy::too_many_lines)]
    async fn test_profile(cred: &StorageCredential, profile: &mut StorageProfile) {
        profile.normalize().expect("Failed to normalize profile");
        let base_location = profile
            .base_location()
            .expect("Failed to get base location");
        let mut table_location1 = base_location.clone();
        table_location1.push("test");
        let mut table_location2 = base_location.clone();
        table_location2.push("test2");

        let config1 = profile
            .generate_table_config(
                &DataAccess {
                    vended_credentials: true,
                    remote_signing: false,
                },
                Some(cred),
                &table_location1,
                StoragePermissions::ReadWriteDelete,
            )
            .await
            .unwrap();

        let config2 = profile
            .generate_table_config(
                &DataAccess {
                    vended_credentials: true,
                    remote_signing: false,
                },
                Some(cred),
                &table_location2,
                StoragePermissions::ReadWriteDelete,
            )
            .await
            .unwrap();
        let (downscoped1, downscoped2) = match profile {
            StorageProfile::Test(_) | StorageProfile::Adls(_) => {
                unimplemented!("Not supported")
            }
            StorageProfile::S3(_) => {
                let downscoped1 = s3::get_file_io_from_table_config(&config1).unwrap();
                let downscoped2 = s3::get_file_io_from_table_config(&config2).unwrap();
                (downscoped1, downscoped2)
            }
            StorageProfile::Gcs(_) => {
                let downscoped1 = gcs::get_file_io_from_table_config(&config1).unwrap();
                let downscoped2 = gcs::get_file_io_from_table_config(&config2).unwrap();
                (downscoped1, downscoped2)
            }
        };
        // can read & write in own locations
        let test_file1 = table_location1.cloning_push("test.txt");
        let test_file2 = table_location2.cloning_push("test.txt");

        let output = downscoped1.new_output(test_file1.as_str()).unwrap();
        output.write(b"test".to_vec().into()).await.unwrap();

        let output2 = downscoped2.new_output(test_file2.as_str()).unwrap();
        output2.write(b"test2".to_vec().into()).await.unwrap();

        let input1 = downscoped1
            .new_input(test_file1.as_str())
            .unwrap()
            .read()
            .await
            .unwrap();
        assert_eq!(input1.as_ref(), b"test");
        let input2 = downscoped2
            .new_input(test_file2.as_str())
            .unwrap()
            .read()
            .await
            .unwrap();
        assert_eq!(input2.as_ref(), b"test2");

        // cannot read across locations
        let _ = downscoped1
            .new_input(test_file2.as_str())
            .unwrap()
            .read()
            .await
            .unwrap_err();
        let _ = downscoped2
            .new_input(test_file1.as_str())
            .unwrap()
            .read()
            .await
            .unwrap_err();

        // cannot write across locations
        let _ = downscoped1
            .new_output(
                table_location2
                    .cloning_push("this-should-fail.txt")
                    .as_str(),
            )
            .unwrap()
            .write(b"this-fails".to_vec().into())
            .await
            .unwrap_err();

        let _ = downscoped2
            .new_output(
                table_location1
                    .cloning_push("this-should-fail.txt")
                    .as_str(),
            )
            .unwrap()
            .write(b"this-fails".to_vec().into())
            .await
            .unwrap_err();

        // cannot delete across locations
        downscoped1.delete(test_file2.as_str()).await.unwrap_err();
        downscoped2.delete(test_file1.as_str()).await.unwrap_err();

        // can delete in own locations
        downscoped1.delete(test_file1.as_str()).await.unwrap();
        downscoped2.delete(test_file2.as_str()).await.unwrap();

        // cleanup
        profile
            .file_io(Some(cred))
            .unwrap()
            .remove_all(base_location.as_str())
            .await
            .unwrap();
    }
}
