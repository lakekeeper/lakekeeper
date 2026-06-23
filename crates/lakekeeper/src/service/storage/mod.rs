#![allow(clippy::match_wildcard_for_single_variants)]

pub(crate) mod az;
mod cache;
pub mod error;
pub(crate) mod gcs;
pub mod s3;
pub mod storage_layout;

use std::{
    collections::HashMap,
    str::FromStr as _,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub use az::{AzCredential, EndpointMode, GenericAdlsProfile, OneLakeProfile, TopLevelFolder};
pub(crate) use error::ValidationError;
use error::{CredentialsError, TableConfigError, UpdateError};
use futures::StreamExt;
pub use gcs::{GcsCredential, GcsProfile, GcsServiceKey};
use iceberg::{NamespaceIdent, TableIdent};
use iceberg_ext::{catalog::rest::ErrorModel, configs::table::TableProperties};
use lakekeeper_io::{
    InvalidLocationError, LakekeeperStorage, Location, LocationParseError, StorageBackend,
    s3::S3Location,
};
pub use s3::{S3Credential, S3Flavor, S3Profile};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{NamespaceId, TableId, secrets::SecretInStorage};
use crate::{
    CONFIG, WarehouseId,
    api::{
        CatalogConfig,
        iceberg::v1::{DataAccess, tables::DataAccessMode},
        management::v1::warehouse::TabularDeleteProfile,
    },
    request_metadata::RequestMetadata,
    server::{compression_codec::CompressionCodec, io::list_location},
    service::{
        BasicTabularInfo, NamespaceVersion, TabularId, TabularInfo, WarehouseVersion,
        storage::{
            error::UnexpectedStorageType,
            storage_layout::{
                DEFAULT_LAYOUT, NamespaceNameContext, NamespacePath, StorageLayout,
                TabularNameContext,
            },
        },
    },
};

/// Storage profile for a warehouse.
#[derive(Debug, Hash, Clone, Eq, PartialEq, Serialize, Deserialize, derive_more::From)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(tag = "type", rename_all = "kebab-case")]
#[allow(clippy::unsafe_derive_deserialize)]
// tokio::join! uses unsafe code internally.
// This is no problem since our constructor does not enforce any invariants relevant to the unsafe code. Deserialize is even the primary way of constructing `StorageProfile` since it is received via REST.
pub enum StorageProfile {
    /// Generic Azure Data Lake Storage Gen2 profile. Speaks ADLS Gen2 against
    /// any storage account.
    #[serde(rename = "adls", alias = "azdls")]
    #[cfg_attr(feature = "open-api", schema(title = "StorageProfileAdls"))]
    Adls(GenericAdlsProfile),
    /// `OneLake` (Microsoft Fabric) profile. Knows how to construct `OneLake`
    /// URLs from workspace + lakehouse IDs and how to derive the
    /// workspace-private-link endpoint host.
    #[serde(rename = "onelake")]
    #[cfg_attr(feature = "open-api", schema(title = "StorageProfileOneLake"))]
    OneLake(OneLakeProfile),
    /// S3 storage profile
    #[serde(rename = "s3")]
    #[cfg_attr(feature = "open-api", schema(title = "StorageProfileS3"))]
    S3(S3Profile),
    #[serde(rename = "gcs")]
    #[cfg_attr(feature = "open-api", schema(title = "StorageProfileGcs"))]
    Gcs(GcsProfile),
    #[cfg(feature = "test-utils")]
    Memory(MemoryProfile),
}

/// Storage profile for a warehouse.
#[derive(Debug, Hash, Copy, Clone, Eq, PartialEq, derive_more::From)]
enum StorageProfileBorrowed<'a> {
    Adls(&'a GenericAdlsProfile),
    OneLake(&'a OneLakeProfile),
    S3(&'a S3Profile),
    Gcs(&'a GcsProfile),
    #[cfg(feature = "test-utils")]
    Memory(&'a MemoryProfile),
}

#[cfg(feature = "test-utils")]
#[derive(
    Debug, Hash, Clone, PartialEq, Eq, Serialize, Deserialize, typed_builder::TypedBuilder,
)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct MemoryProfile {
    /// Base location for the local profile
    base_location: String,
    /// Storage layout for namespace and tabular paths.
    #[serde(default)]
    #[builder(default, setter(strip_option))]
    pub storage_layout: Option<StorageLayout>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Copy, strum_macros::Display)]
pub enum StoragePermissions {
    Read,
    ReadWrite,
    ReadWriteDelete,
}

#[derive(Debug)]
pub struct TableConfig {
    pub(crate) creds: TableProperties,
    pub(crate) config: TableProperties,
    /// Actual expiry (epoch ms) of the vended credentials in [`Self::creds`], or
    /// `None` if none expire. Set wherever a backend vends an expiring
    /// credential; the source for the `loadTable` `ETag`'s revalidation point.
    pub(crate) credentials_expiration_ms: Option<i64>,
}

/// Half of a credential's remaining lifetime, capped at 1h — the window during
/// which the STC cache keeps serving it ([`cache`]) and during which a
/// conditional `loadTable` may still answer `304`.
pub(crate) fn credential_serve_window(remaining: Duration) -> Duration {
    (remaining / 2).min(Duration::from_hours(1))
}

/// Current time in epoch ms. Fails closed to `i64::MAX` for an unknowable clock
/// (pre-1970 / overflow): as a "now" this makes freshness checks reload rather
/// than serve a possibly-stale `304`.
pub(crate) fn now_epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_millis()).ok())
        .unwrap_or(i64::MAX)
}

/// Absolute time (epoch ms) until which a conditional `loadTable` may answer
/// `304` for a credential expiring at `expiry_ms`: `now + credential_serve_window`,
/// clamped to never exceed `expiry_ms` so a bogus clock can't push it past the
/// real expiry. Always reflects the credential the client actually holds.
#[must_use]
pub(crate) fn credential_revalidate_after_ms(expiry_ms: i64) -> i64 {
    revalidate_after_at(expiry_ms, now_epoch_ms())
}

pub(crate) fn revalidate_after_at(expiry_ms: i64, now_ms: i64) -> i64 {
    let remaining =
        Duration::from_millis(u64::try_from(expiry_ms.saturating_sub(now_ms)).unwrap_or(0));
    let window = credential_serve_window(remaining);
    let revalidate_after = now_ms
        .saturating_add(i64::try_from(window.as_millis()).unwrap_or(i64::MAX))
        .min(expiry_ms);
    // Load-bearing safety invariant: a 304 is served only while `now <
    // revalidate_after`, so this must never reach `expiry_ms` or a 304 could hand
    // back an expired credential. Enforced by the `.min(expiry_ms)` clamp above.
    debug_assert!(revalidate_after <= expiry_ms);
    revalidate_after
}

#[derive(Debug, Hash, Clone, Eq, PartialEq)]
pub struct ShortTermCredentialsRequest {
    pub table_location: Location,
    pub storage_permissions: StoragePermissions,
    pub warehouse_id: WarehouseId,
    pub tabular_id: TabularId,
}

impl std::fmt::Display for ShortTermCredentialsRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Short Term Credentials Request for table {} at location `{}` with permissions {} in warehouse {}",
            self.tabular_id, self.table_location, self.storage_permissions, self.warehouse_id,
        )
    }
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
            StorageProfile::OneLake(prof) => prof.generate_catalog_config(warehouse_id),
            StorageProfile::Gcs(prof) => prof.generate_catalog_config(warehouse_id),
            #[cfg(feature = "test-utils")]
            StorageProfile::Memory(_) => CatalogConfig {
                overrides: std::collections::HashMap::new(),
                defaults: std::collections::HashMap::new(),
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
            (StorageProfile::OneLake(this_profile), StorageProfile::OneLake(other_profile)) => {
                this_profile.update_with(other_profile).map(Into::into)
            }
            (StorageProfile::Gcs(this_profile), StorageProfile::Gcs(other_profile)) => {
                this_profile.update_with(other_profile).map(Into::into)
            }
            #[cfg(feature = "test-utils")]
            (StorageProfile::Memory(_this_profile), StorageProfile::Memory(other_profile)) => {
                Ok(other_profile.into())
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
                .await
                .map(Into::into),
            StorageProfile::OneLake(profile) => profile
                .lakekeeper_io(
                    secret
                        .map(|s| s.try_to_az())
                        .ok_or_else(|| CredentialsError::MissingCredential("onelake".to_string()))?
                        .map_err(CredentialsError::from)?,
                )
                .await
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
            #[cfg(feature = "test-utils")]
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
            StorageProfile::OneLake(profile) => profile.base_location(),
            StorageProfile::Gcs(profile) => profile.base_location(),
            #[cfg(feature = "test-utils")]
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
        namespace_path: &NamespacePath,
    ) -> Result<Location, ValidationError> {
        let mut base_location: Location = self.base_location()?;
        base_location.without_trailing_slash();
        let layout = self.layout().unwrap_or_else(|| &DEFAULT_LAYOUT);

        let segments = layout.render_namespace_path(namespace_path);
        base_location.extend(segments);
        Ok(base_location)
    }

    #[must_use]
    pub fn storage_type(&self) -> &'static str {
        match self {
            StorageProfile::S3(_) => "s3",
            StorageProfile::Adls(_) => "adls",
            StorageProfile::OneLake(_) => "onelake",
            StorageProfile::Gcs(_) => "gcs",
            #[cfg(feature = "test-utils")]
            StorageProfile::Memory(_) => "memory",
        }
    }

    /// Whether [`Self::generate_table_config`] for `data_access` may vend
    /// credentials that expire. Gates the conditional-`loadTable` 304 path for
    /// the cases where the client's echoed `ETag` carries no revalidation point
    /// (metadata-only / wildcard).
    ///
    /// Conservative: may return `true` when the concrete response ends up without
    /// credentials (only forgoes the 304 fast-path); never `false` while expiring
    /// credentials are vended.
    #[must_use]
    pub fn vends_expiring_credentials(&self, data_access: DataAccessMode) -> bool {
        if !data_access.provide_credentials() {
            return false;
        }
        match self {
            // Real backends vend expiring credentials — a new one should land here.
            StorageProfile::S3(_)
            | StorageProfile::Adls(_)
            | StorageProfile::OneLake(_)
            | StorageProfile::Gcs(_) => true,
            // The in-memory test profile never vends credentials.
            #[cfg(feature = "test-utils")]
            StorageProfile::Memory(_) => false,
        }
    }

    /// Generate the table config for the storage profile.
    ///
    /// # Errors
    /// Fails if the underlying storage profile's generation fails.
    #[allow(clippy::too_many_arguments)]
    pub async fn generate_table_config(
        &self,
        data_access: DataAccessMode,
        secret: Option<&StorageCredential>,
        table_location: &Location,
        storage_permissions: StoragePermissions,
        request_metadata: &RequestMetadata,
        tabular_info: &impl BasicTabularInfo,
    ) -> Result<TableConfig, TableConfigError> {
        let stc_request = ShortTermCredentialsRequest {
            table_location: table_location.clone(),
            storage_permissions,
            warehouse_id: tabular_info.warehouse_id(),
            tabular_id: tabular_info.tabular_id(),
        };

        match self {
            StorageProfile::S3(profile) => {
                profile
                    .generate_table_config(
                        data_access,
                        secret
                            .map(|s| s.try_to_s3())
                            .transpose()
                            .map_err(CredentialsError::from)?,
                        stc_request,
                        tabular_info,
                        request_metadata,
                    )
                    .await
            }
            StorageProfile::Adls(profile) => {
                profile
                    .generate_table_config(
                        data_access,
                        secret
                            .ok_or_else(|| CredentialsError::MissingCredential("adls".to_string()))?
                            .try_to_az()
                            .map_err(CredentialsError::from)?,
                        stc_request,
                        tabular_info,
                        request_metadata,
                    )
                    .await
            }
            StorageProfile::OneLake(profile) => {
                profile
                    .generate_table_config(
                        data_access,
                        secret
                            .ok_or_else(|| {
                                CredentialsError::MissingCredential("onelake".to_string())
                            })?
                            .try_to_az()
                            .map_err(CredentialsError::from)?,
                        stc_request,
                        tabular_info,
                        request_metadata,
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
                        &stc_request,
                        tabular_info,
                        request_metadata,
                    )
                    .await
            }
            #[cfg(feature = "test-utils")]
            StorageProfile::Memory(_) => Ok(TableConfig {
                creds: TableProperties::default(),
                config: TableProperties::default(),
                credentials_expiration_ms: None,
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
        let namespace_path = NamespacePath::new(vec![NamespaceNameContext {
            name: "test_namespace".to_string(),
            uuid: Uuid::now_v7(),
        }]);
        let ns_location = self.default_namespace_location(&namespace_path)?;
        let tabular_name_context = TabularNameContext {
            name: "test_tabular".to_string(),
            uuid: Uuid::now_v7(),
        };
        let _ = self.default_tabular_location(&ns_location, &tabular_name_context);

        // ------------- Profile specific validations -------------
        match self {
            StorageProfile::S3(profile) => profile.normalize(
                credential
                    .map(|s| s.try_to_s3())
                    .transpose()
                    .map_err(CredentialsError::from)?,
            ),
            StorageProfile::Adls(prof) => prof.normalize(),
            StorageProfile::OneLake(prof) => prof.normalize(
                credential
                    .map(|s| s.try_to_az())
                    .transpose()
                    .map_err(CredentialsError::from)?,
            ),
            StorageProfile::Gcs(profile) => profile.normalize(),
            #[cfg(feature = "test-utils")]
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

        let namespace_path = NamespacePath::new(vec![NamespaceNameContext {
            name: "test_namespace".to_string(),
            uuid: Uuid::now_v7(),
        }]);
        let tabular_name_context = TabularNameContext {
            name: "test_tabular".to_string(),
            uuid: Uuid::now_v7(),
        };
        let ns_location = self.default_namespace_location(&namespace_path)?;
        let test_location = location.map_or_else(
            || self.default_tabular_location(&ns_location, &tabular_name_context),
            std::borrow::ToOwned::to_owned,
        );
        tracing::debug!("Validating direct read/write access to {test_location}");

        // Test vended-credentials access
        let test_vended_credentials = match self {
            StorageProfile::S3(profile) => profile.sts_enabled,
            StorageProfile::Adls(profile) => profile.sas_enabled,
            StorageProfile::OneLake(profile) => profile.sas_enabled,
            StorageProfile::Gcs(profile) => profile.sts_enabled,
            #[cfg(feature = "test-utils")]
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
        let validation_err = match (direct_result, vended_result) {
            (Ok(()), Ok(())) => None,
            (Err(e), Ok(()) | Err(_)) | (Ok(()), Err(e)) => Some(e),
        };
        tracing::debug!("Cleanup started");
        if let Err(e) = io.remove_all(test_location.as_str()).await {
            tracing::warn!("Cleanup failed after validation: {e}");
        } else {
            tracing::debug!("Cleanup finished");
        }
        if let Some(e) = validation_err {
            return Err(e);
        }

        match is_empty(&io, &test_location).await {
            Err(ValidationError::IoOperationFailed(io_error)) => {
                tracing::info!("Error while checking location is empty: {io_error}");
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

        // Create a sub-location for testing vended credentials access
        let mut sub_location = test_location.clone();
        sub_location.without_trailing_slash().push("vended-test");

        let tabular_info = TabularInfo {
            warehouse_id: WarehouseId::new_random(),
            namespace_id: NamespaceId::new_random(),
            namespace_version: NamespaceVersion::new(0),
            warehouse_version: WarehouseVersion::new(0),
            tabular_ident: TableIdent::new(
                NamespaceIdent::new("vended-test".to_string()),
                "tbl".to_string(),
            ),
            tabular_id: TableId::new_random(),
            location: test_location.clone(),
            metadata_location: None,
            protected: false,
            properties: HashMap::new(),
            updated_at: None,
        };

        let tbl_config = self
            .generate_table_config(
                DataAccess {
                    remote_signing: false,
                    vended_credentials: true,
                }
                .into(),
                credential,
                &sub_location,
                StoragePermissions::ReadWriteDelete,
                // The following arguments are used only for generating the remote signing configuration
                // and are not used in the vended credentials case.
                request_metadata,
                &tabular_info,
            )
            .await?;

        let sts_storage: StorageBackend = match &self {
            StorageProfile::S3(_) => {
                tracing::debug!("Building S3 storage from vended credentials.");
                s3::lakekeeper_io_from_vended_table_config(&tbl_config.config)
                    .await?
                    .into()
            }
            StorageProfile::Adls(profile) => {
                tracing::debug!("Building ADLS storage from vended credentials.");
                profile
                    .lakekeeper_io_from_vended_table_config(&tbl_config.config)
                    .await?
                    .into()
            }
            StorageProfile::OneLake(profile) => {
                tracing::debug!("Building `OneLake` storage from vended credentials.");
                profile
                    .lakekeeper_io_from_vended_table_config(&tbl_config.config)
                    .await?
                    .into()
            }
            StorageProfile::Gcs(_) => {
                tracing::debug!("Building GCS storage from vended credentials.");
                gcs::lakekeeper_io_from_vended_table_config(&tbl_config.config)
                    .await?
                    .into()
            }
            #[cfg(feature = "test-utils")]
            StorageProfile::Memory(_) => {
                unreachable!("Local profile does not support vended credentials access validation")
            }
        };

        tracing::debug!(
            "Validating read/write access to sub-location: {sub_location} and forbidden access to parent location: {test_location} using vended credentials"
        );

        // Run both validations in parallel
        let read_write_validation =
            self.validate_read_write_lakekeeper(&sts_storage, &sub_location);
        let no_write_validation =
            self.validate_no_write_access_lakekeeper(&sts_storage, test_location);

        let (read_write_result, no_write_result) =
            tokio::join!(read_write_validation, no_write_validation);

        // If both validations failed, surface both — the no-write failure means
        // downscoped credentials were over-permissive, which is a security signal
        // that should not be hidden by an unrelated read/write failure.
        if let (Err(rw), Err(nw)) = (&read_write_result, &no_write_result) {
            tracing::warn!(
                "Both vended-credentials validations failed. Read/write: {rw:?}. No-write: {nw:?}"
            );
        }
        read_write_result?;
        no_write_result?;

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
        let mut test_file_write = test_file_write.parent();
        test_file_write.push("test");
        tracing::debug!("Validating access to: {}", test_file_write);

        // Test write
        crate::server::io::write_file(io, &test_file_write, "test", compression_codec)
            .await
            .map_err(|e| {
                tracing::info!("Error while writing file: {e:?}");
                ValidationError::from(e)
            })?;

        // Test read
        let _ = crate::server::io::read_file(io, &test_file_write, compression_codec)
            .await
            .map_err(|e| {
                tracing::info!("Error while reading file: {e:?}");
                ValidationError::from(e)
            })?;

        // Test delete
        crate::server::io::delete_file(io, &test_file_write)
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

    /// Validate that we cannot write to a location with the given credentials.
    ///
    /// # Errors
    /// Fails if we can write to the location (which should not be allowed).
    async fn validate_no_write_access_lakekeeper(
        &self,
        io: &impl LakekeeperStorage,
        test_location: &Location,
    ) -> Result<(), ValidationError> {
        let compression_codec = CompressionCodec::Gzip;

        let mut test_file_write = self.default_metadata_location(
            test_location,
            &compression_codec,
            uuid::Uuid::now_v7(),
            0,
        );
        test_file_write.pop().push("forbidden-write-test");

        tracing::debug!(
            "Validating that write access is denied to: {}",
            test_file_write
        );

        match crate::server::io::write_file(
            io,
            &test_file_write,
            "forbidden-content",
            compression_codec,
        )
        .await
        {
            Ok(()) => {
                // Should not have been able to write — try to clean up the rogue file.
                if let Err(e) = crate::server::io::delete_file(io, &test_file_write).await {
                    tracing::warn!(
                        "Failed to delete rogue validation file at {test_file_write} (creds were over-permissive on write but cleanup failed): {e:?}"
                    );
                }
                return Err(CredentialsError::ShortTermCredential {
                    reason: "Downscoped credentials allow write access to parent location."
                        .to_string(),
                    source: None,
                }
                .into());
            }
            Err(e) => {
                tracing::debug!(
                    "Write correctly failed for forbidden location: {test_file_write} ({e:?})"
                );
            }
        }

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

    /// Try to convert the storage profile into a generic ADLS profile.
    ///
    /// # Errors
    /// Fails if the profile is not a generic ADLS profile.
    pub fn try_into_generic_adls(self) -> Result<GenericAdlsProfile, UnexpectedStorageType> {
        match self {
            Self::Adls(profile) => Ok(profile),
            _ => Err(UnexpectedStorageType {
                is: self.storage_type(),
                to: "adls",
            }),
        }
    }

    /// Try to convert the storage profile into a `OneLake` profile.
    ///
    /// # Errors
    /// Fails if the profile is not a `OneLake` profile.
    pub fn try_into_onelake(self) -> Result<OneLakeProfile, UnexpectedStorageType> {
        match self {
            Self::OneLake(profile) => Ok(profile),
            _ => Err(UnexpectedStorageType {
                is: self.storage_type(),
                to: "onelake",
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
            (StorageProfile::OneLake(profile), StorageProfile::OneLake(other_profile)) => {
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

        if let StorageProfile::OneLake(profile) = self {
            let other_scheme = other.scheme();
            if !profile.is_allowed_schema(other_scheme) {
                tracing::debug!("Scheme {other_scheme} is not allowed for `OneLake` profile.",);
                return false;
            }
            // OneLake collapses any `%XX` escape in the blob path to its
            // decoded character somewhere in its request-handling pipeline.
            // The user-delegation-key-signed canonical never matches the
            // collapsed form, so vended SAS for such paths fails with
            // `401 Access token validation failed`. Reject up-front rather
            // than create unreachable tables.
            for seg in other.path_segments() {
                if seg.contains('%') {
                    tracing::debug!(
                        "OneLake path segment `{seg}` contains `%` which OneLake \
                         silently collapses, breaking vended-credentials access. \
                         Reject up-front."
                    );
                    return false;
                }
            }
            // Base location is always `abfss://` for OneLake; no scheme rewrite needed.
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
                format!(
                    "Provided location {other} is not a valid sublocation of the storage profile {base_location}."
                ),
                "InvalidLocation",
                None,
            ));
        }
        Ok(())
    }

    #[must_use]
    /// Get the default metadata location for the storage profile.
    pub fn default_metadata_location(
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

    /// Get the default tabular location for the storage profile.
    #[must_use]
    pub fn default_tabular_location(
        &self,
        namespace_location: &Location,
        tabular_name_context: &TabularNameContext,
    ) -> Location {
        let mut location = namespace_location.clone();

        let layout = self.layout().unwrap_or_else(|| &DEFAULT_LAYOUT);

        let segment = layout.render_tabular_segment(tabular_name_context);
        location.without_trailing_slash().push(&segment);
        location
    }

    #[must_use]
    pub fn layout(&self) -> Option<&StorageLayout> {
        match self {
            StorageProfile::S3(profile) => profile.storage_layout.as_ref(),
            StorageProfile::Adls(profile) => profile.storage_layout.as_ref(),
            StorageProfile::OneLake(profile) => profile.storage_layout.as_ref(),
            StorageProfile::Gcs(profile) => profile.storage_layout.as_ref(),
            #[cfg(feature = "test-utils")]
            StorageProfile::Memory(profile) => profile.storage_layout.as_ref(),
        }
    }
}

#[cfg(feature = "test-utils")]
impl Default for MemoryProfile {
    fn default() -> Self {
        Self {
            base_location: <Location as std::str::FromStr>::from_str(
                format!("memory://test-{}", uuid::Uuid::new_v4()).as_str(),
            )
            .expect("Failed to create temporary directory location")
            .to_string(),
            storage_layout: None,
        }
    }
}

/// Storage secret for a warehouse.
#[derive(Debug, Hash, Clone, PartialEq, Eq, Serialize, Deserialize, derive_more::From)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
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
    ///     "access-key-id": "minio-root-user",
    ///     "secret-access-key": "minio-root-password"
    ///   }"#).unwrap();
    /// ```
    #[serde(rename = "s3")]
    #[cfg_attr(feature = "open-api", schema(title = "StorageCredentialS3"))]
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
    #[cfg_attr(feature = "open-api", schema(title = "StorageCredentialAz"))]
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
    #[cfg_attr(feature = "open-api", schema(title = "StorageCredentialGcs"))]
    Gcs(GcsCredential),
}

/// The type of storage credential configured for a warehouse, without secret values.
///
/// This is returned in API responses so clients know which credential type
/// was selected (e.g. to restore radio button state in the UI).
#[derive(Debug, Hash, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(tag = "type", content = "credential-type", rename_all = "kebab-case")]
pub enum StorageCredentialType {
    /// S3 credential type
    #[serde(rename = "s3")]
    S3(S3CredentialType),
    /// Azure credential type
    #[serde(rename = "az")]
    Az(AzCredentialType),
    /// GCS credential type
    #[serde(rename = "gcs")]
    Gcs(GcsCredentialType),
}

/// The type of S3 credential.
#[derive(Debug, Hash, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub enum S3CredentialType {
    AccessKey,
    AwsSystemIdentity,
    CloudflareR2,
}

/// The type of Azure credential.
#[derive(Debug, Hash, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub enum AzCredentialType {
    ClientCredentials,
    SharedAccessKey,
    AzureSystemIdentity,
}

/// The type of GCS credential.
#[derive(Debug, Hash, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub enum GcsCredentialType {
    ServiceAccountKey,
    GcpSystemIdentity,
}

impl StorageCredential {
    /// Returns the credential type discriminant without secret values.
    #[must_use]
    pub fn credential_type(&self) -> StorageCredentialType {
        match self {
            StorageCredential::S3(s3) => StorageCredentialType::S3(match s3 {
                S3Credential::AccessKey(_) => S3CredentialType::AccessKey,
                S3Credential::AwsSystemIdentity(_) => S3CredentialType::AwsSystemIdentity,
                S3Credential::CloudflareR2(_) => S3CredentialType::CloudflareR2,
            }),
            StorageCredential::Az(az) => StorageCredentialType::Az(match az {
                AzCredential::ClientCredentials { .. } => AzCredentialType::ClientCredentials,
                AzCredential::SharedAccessKey { .. } => AzCredentialType::SharedAccessKey,
                AzCredential::AzureSystemIdentity {} => AzCredentialType::AzureSystemIdentity,
            }),
            StorageCredential::Gcs(gcs) => StorageCredentialType::Gcs(match gcs {
                GcsCredential::ServiceAccountKey { .. } => GcsCredentialType::ServiceAccountKey,
                GcsCredential::GcpSystemIdentity {} => GcsCredentialType::GcpSystemIdentity,
            }),
        }
    }
}

#[derive(Debug, Hash, Copy, Clone, PartialEq, derive_more::From)]
enum StorageCredentialBorrowed<'a> {
    S3(&'a S3Credential),
    Az(&'a AzCredential),
    Gcs(&'a GcsCredential),
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

pub fn join_location(
    prefix: &str,
    path: &str,
) -> std::result::Result<Location, LocationParseError> {
    Location::from_str(&format!("{prefix}://{path}"))
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

    use super::{
        s3::{S3AwsSystemIdentityCredential, S3CloudflareR2Credential, test::test_block_on},
        *,
    };
    use crate::{
        server::io::{delete_file, read_metadata_file, write_file},
        service::{
            TableInfo,
            storage::{
                s3::S3AccessKeyCredential,
                storage_layout::{NamespaceNameContext, TabularNameContext},
            },
        },
    };

    #[test]
    fn test_split_location() {
        // Minimal authority-only Location — `abfss://` (no host) is now
        // rejected by the validator since an empty host is never useful and
        // backend-aliasing safety requires a non-empty authority.
        let location = Location::from_str("abfss://host").unwrap();
        let prefix = location.scheme();
        let full_path = location.authority_and_path();
        assert_eq!(prefix, "abfss");
        assert_eq!(full_path, "host");
        assert_eq!(join_location(prefix, full_path).unwrap(), location);

        let location = Location::from_str("abfss://foo/bar").unwrap();
        let prefix = location.scheme();
        let full_path = location.authority_and_path();
        assert_eq!(prefix, "abfss");
        assert_eq!(full_path, "foo/bar");
        assert_eq!(join_location(prefix, full_path).unwrap(), location);
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

        let ns_uuid = uuid::uuid!("00000000-0000-0000-0000-000000000001");
        let tabular_uuid = uuid::uuid!("00000000-0000-0000-0000-000000000002");
        let tabular_name = "test-table";

        let namespace_path = NamespacePath::new(vec![NamespaceNameContext {
            name: "ns".to_string(),
            uuid: ns_uuid,
        }]);

        let tabular_name_context = TabularNameContext {
            name: tabular_name.to_string(),
            uuid: tabular_uuid,
        };

        // Default layout is flat: no namespace directory under the base location.
        let target_location = format!("s3://my-bucket/subfolder/{tabular_uuid}");

        let namespace_location = profile.default_namespace_location(&namespace_path).unwrap();
        let tabular_location =
            profile.default_tabular_location(&namespace_location, &tabular_name_context);
        assert_eq!(tabular_location.to_string(), target_location);

        let mut namespace_location_without_slash = namespace_location.clone();
        namespace_location_without_slash.without_trailing_slash();
        let tabular_location_trailing = profile
            .default_tabular_location(&namespace_location_without_slash, &tabular_name_context);
        assert!(!namespace_location_without_slash.to_string().ends_with('/'));
        assert_eq!(tabular_location_trailing.to_string(), target_location);
    }

    #[test]
    fn test_redact_s3_access_key() {
        let secrets: StorageCredential = S3Credential::AccessKey(S3AccessKeyCredential {
            access_key_id: "
                AKIAIOSFODNN7EXAMPLE
            "
            .to_string(),
            secret_access_key: "
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
                access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
                secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
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
        let profile = StorageProfile::Adls(GenericAdlsProfile {
            filesystem: "filesystem".to_string(),
            key_prefix: Some("test_prefix".to_string()),
            account_name: "account".to_string(),
            authority_host: None,
            host: None,
            sas_token_validity_seconds: None,
            allow_alternative_protocols: true,
            sas_enabled: true,
            storage_layout: None,
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

    #[test]
    fn test_is_allowed_location_onelake_rejects_percent_in_segments() {
        use az::{EndpointMode, OneLakeProfile, TopLevelFolder};
        use uuid::Uuid;
        let profile = StorageProfile::OneLake(OneLakeProfile {
            workspace_id: Uuid::parse_str("0388d6cb-27fd-4dc5-948b-32ab7aab9577").unwrap(),
            lakehouse_id: Uuid::parse_str("eb2b7644-2ae4-43ed-ad08-8cc295ffa7ac").unwrap(),
            directory_rel_path: Some("test_prefix".to_string()),
            top_level_folder: TopLevelFolder::default(),
            endpoint_mode: EndpointMode::Default,
            sas_token_validity_seconds: None,
            sas_enabled: true,
            authority_host: None,
            storage_layout: None,
        });
        let base = "abfss://0388d6cb-27fd-4dc5-948b-32ab7aab9577@onelake.dfs.fabric.microsoft.com/eb2b7644-2ae4-43ed-ad08-8cc295ffa7ac/Files/test_prefix";
        let cases = vec![
            // Vanilla sub-locations are allowed.
            (format!("{base}/ns/t"), true),
            (format!("{base}/ns/t/metadata/00000.gz.metadata.json"), true),
            // Any `%` in a segment is rejected (OneLake collapses `%XX`).
            (format!("{base}/ns/%3F/data"), false),
            (format!("{base}/ns/%22/data"), false),
            (format!("{base}/ns/%41bc/data"), false),
            (format!("{base}/ns/has%percent/data"), false),
            // Raw URL-safe special chars are NOT blocked (they don't go
            // through a `%` encoding on the wire).
            (format!("{base}/ns/star*name/data"), true),
            (format!("{base}/ns/dollar$name/data"), true),
        ];
        for (sublocation, expected_result) in cases {
            let loc = Location::from_str(&sublocation).unwrap();
            assert_eq!(
                profile.is_allowed_location(&loc),
                expected_result,
                "sublocation={sublocation}",
            );
        }
    }

    mod azure_integration_tests {
        use super::*;

        #[tokio::test]
        async fn test_vended_az() {
            for (cred, _typ) in [
                (
                    super::az::test::azure_integration_tests::client_creds(),
                    "client-credentials",
                ),
                (
                    super::az::test::azure_integration_tests::shared_key(),
                    "shared-key",
                ),
            ] {
                let mut profile: StorageProfile =
                    az::test::azure_integration_tests::azure_profile().into();
                let cred: StorageCredential = cred.into();
                test_profile_vended_creds(&cred, &mut profile).await;
                test_profile_io(&cred, &mut profile).await;
            }
        }
    }

    mod onelake_integration_tests {
        use super::*;

        #[tokio::test]
        #[ignore = "live OneLake test; opt in with --ignored (see \
                    az::onelake_integration_tests module docs)"]
        async fn test_vended_onelake() {
            let cred: StorageCredential =
                super::az::test::onelake_integration_tests::client_creds().into();
            let mut profile: StorageProfile =
                az::test::onelake_integration_tests::onelake_profile().into();
            test_profile_vended_creds(&cred, &mut profile).await;
            test_profile_io(&cred, &mut profile).await;
        }
    }

    mod gcs_integration_tests {
        use super::*;

        #[tokio::test]
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
                sts_enabled: true,
                storage_layout: None,
            }
            .into();

            test_profile_vended_creds(&cred, &mut profile).await;
            test_profile_io(&cred, &mut profile).await;
        }
    }

    mod aws_integration_tests {
        use super::*;

        #[test]
        fn test_vended_aws() {
            test_block_on(
                async {
                    let key_prefix = format!("test_prefix-{}", uuid::Uuid::now_v7());
                    let bucket = std::env::var("LAKEKEEPER_TEST__AWS_S3_BUCKET").unwrap();
                    let region = std::env::var("LAKEKEEPER_TEST__AWS_S3_REGION").unwrap();
                    let sts_role_arn =
                        std::env::var("LAKEKEEPER_TEST__AWS_S3_STS_ROLE_ARN").unwrap();
                    let cred: StorageCredential = S3Credential::AccessKey(S3AccessKeyCredential {
                        access_key_id: std::env::var("LAKEKEEPER_TEST__AWS_S3_ACCESS_KEY_ID")
                            .unwrap(),
                        secret_access_key: std::env::var(
                            "LAKEKEEPER_TEST__AWS_S3_SECRET_ACCESS_KEY",
                        )
                        .unwrap(),
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

        #[tokio::test]
        // #[tracing_test::traced_test]
        async fn test_validate_aws() {
            use crate::service::storage::s3::test::aws_integration_tests::get_storage_profile;

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
    }

    mod minio_integration_tests {
        use super::*;

        #[test]
        fn test_vended_s3_compat() {
            use super::super::s3::test::minio_integration_tests::storage_profile;

            test_block_on(
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
    }

    fn generate_table_metadata() -> TableMetadata {
        TableMetadataBuilder::new(
            Schema::builder().build().expect("Failed to build schema"),
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            format!("test-table-{}", uuid::Uuid::now_v7()),
            iceberg::spec::FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .expect("Failed to build table metadata")
        .metadata
    }

    #[allow(clippy::too_many_lines)]
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

    #[allow(clippy::too_many_lines)]
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
                }
                .into(),
                Some(cred),
                &table_location1,
                StoragePermissions::ReadWriteDelete,
                &RequestMetadata::new_unauthenticated(),
                &TableInfo::new_random(WarehouseId::new_random()),
            )
            .await
            .unwrap();

        let config2 = profile
            .generate_table_config(
                DataAccess {
                    vended_credentials: true,
                    remote_signing: false,
                }
                .into(),
                Some(cred),
                &table_location2,
                StoragePermissions::ReadWriteDelete,
                &RequestMetadata::new_unauthenticated(),
                &TableInfo::new_random(WarehouseId::new_random()),
            )
            .await
            .unwrap();
        let (downscoped1, downscoped2): (StorageBackend, StorageBackend) = match profile {
            StorageProfile::Adls(p) => (
                p.lakekeeper_io_from_vended_table_config(&config1.config)
                    .await
                    .unwrap()
                    .into(),
                p.lakekeeper_io_from_vended_table_config(&config2.config)
                    .await
                    .unwrap()
                    .into(),
            ),
            StorageProfile::OneLake(p) => (
                p.lakekeeper_io_from_vended_table_config(&config1.config)
                    .await
                    .unwrap()
                    .into(),
                p.lakekeeper_io_from_vended_table_config(&config2.config)
                    .await
                    .unwrap()
                    .into(),
            ),
            StorageProfile::S3(_) => (
                s3::lakekeeper_io_from_vended_table_config(&config1.config)
                    .await
                    .unwrap()
                    .into(),
                s3::lakekeeper_io_from_vended_table_config(&config2.config)
                    .await
                    .unwrap()
                    .into(),
            ),
            StorageProfile::Gcs(_) => (
                gcs::lakekeeper_io_from_vended_table_config(&config1.config)
                    .await
                    .unwrap()
                    .into(),
                gcs::lakekeeper_io_from_vended_table_config(&config2.config)
                    .await
                    .unwrap()
                    .into(),
            ),
            StorageProfile::Memory(_) => {
                unreachable!("Local storage does not support vended credentials")
            }
        };
        // can read & write in own locations
        let test_file1 = table_location1.cloning_push("test.txt");
        let test_file2 = table_location2.cloning_push("test.txt");

        downscoped1
            .write(
                test_file1.as_str(),
                bytes::Bytes::from_static(b"test content 1"),
            )
            .await
            .unwrap();
        downscoped2
            .write(
                test_file2.as_str(),
                bytes::Bytes::from_static(b"test content 2"),
            )
            .await
            .unwrap();

        let input1 = downscoped1.read(test_file1.as_str()).await.unwrap();
        assert_eq!(input1.as_ref(), b"test content 1");

        let input2 = downscoped2.read(test_file2.as_str()).await.unwrap();
        assert_eq!(input2.as_ref(), b"test content 2");

        // cannot read across locations
        let _ = downscoped1.read(test_file2.as_str()).await.unwrap_err();
        let _ = downscoped2.read(test_file1.as_str()).await.unwrap_err();

        // cannot write across locations
        let _ = downscoped1
            .write(
                test_file2.as_str(),
                bytes::Bytes::from_static(b"this-should-fail"),
            )
            .await
            .unwrap_err();
        let _ = downscoped2
            .write(
                test_file1.as_str(),
                bytes::Bytes::from_static(b"this-should-fail"),
            )
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

#[cfg(all(test, feature = "test-utils"))]
mod vends_expiring_credentials_tests {
    use super::{MemoryProfile, StorageProfile};
    use crate::api::iceberg::v1::{DataAccess, tables::DataAccessMode};

    #[test]
    fn memory_profile_never_vends_expiring_credentials() {
        let profile = StorageProfile::Memory(MemoryProfile::default());
        // The in-memory test profile vends no credentials, so the conditional
        // `loadTable` 304 fast-path must stay available for it regardless of the
        // requested access mode.
        assert!(!profile.vends_expiring_credentials(DataAccessMode::default()));
        assert!(
            !profile.vends_expiring_credentials(DataAccessMode::ServerDelegated(DataAccess {
                vended_credentials: true,
                remote_signing: false,
            }))
        );
        assert!(!profile.vends_expiring_credentials(DataAccessMode::ClientManaged));
    }
}

#[cfg(test)]
mod revalidate_after_tests {
    use super::revalidate_after_at;

    const NOW: i64 = 1_750_000_000_000;

    #[test]
    fn revalidate_after_is_half_remaining_and_before_expiry() {
        // 10-min credential → revalidate at +5 min, always before expiry.
        let expiry = NOW + 600_000;
        let reval = revalidate_after_at(expiry, NOW);
        assert_eq!(reval, NOW + 300_000);
        assert!(reval < expiry);
        // Capped at 1h: a 4h credential revalidates at +1h, not +2h.
        assert_eq!(
            revalidate_after_at(NOW + 4 * 3_600_000, NOW),
            NOW + 3_600_000
        );
        // Already expired → clamped to expiry, so the check never serves a 304.
        assert_eq!(revalidate_after_at(NOW - 1, NOW), NOW - 1);
        // Bogus clock (pre-1970 → now == i64::MAX) must not poison the result
        // with a far-future revalidation point: clamp to the real expiry.
        assert_eq!(revalidate_after_at(expiry, i64::MAX), expiry);
    }
}
