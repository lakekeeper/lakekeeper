use std::str::FromStr;

use azure_storage::CloudLocation;
use iceberg_ext::configs::table::TableProperties;
use lakekeeper_io::{
    InvalidLocationError, Location,
    adls::{
        AdlsStorage, AzureSettings, normalize_host, validate_filesystem_name,
        validate_storage_account_name,
    },
};
use serde::{Deserialize, Serialize};
use url::Url;

use super::{
    AdlsTableConfigContext, AzCredential, DEFAULT_GENERIC_ADLS_HOST,
    MAX_GENERIC_ADLS_SAS_TOKEN_VALIDITY_SECONDS, SasMintContext, adls_catalog_config,
    adls_lakekeeper_io, generate_adls_table_config, iceberg_expiration_property_key,
    iceberg_sas_property_key, key_prefix_overlaps, lakekeeper_io_from_vended_adls_table_config,
    validate_sas_token_validity_seconds,
};
use crate::{
    WarehouseId,
    api::{CatalogConfig, RequestMetadata, Result, iceberg::v1::tables::DataAccessMode},
    service::{
        BasicTabularInfo,
        storage::{
            ShortTermCredentialsRequest, TableConfig,
            cache::STCCacheKey,
            error::{
                CredentialsError, InvalidProfileError, TableConfigError, UpdateError,
                ValidationError,
            },
            storage_layout::StorageLayout,
        },
    },
};

pub(crate) const ALTERNATIVE_PROTOCOLS: [&str; 1] = ["wasbs"];

/// Storage profile for a generic Azure Data Lake Storage Gen2 account.
///
/// This profile speaks ADLS Gen2 against any storage account (including
/// Microsoft Fabric / `OneLake`, if you configure `account_name = "onelake"`,
/// `host = "dfs.fabric.microsoft.com"`, and a `key_prefix` like
/// `<lakehouse>/Files/<sub>`). Lakekeeper offers `OneLakeProfile` as a
/// convenience layer that knows how to compute those values from
/// workspace + lakehouse IDs and that supports `OneLake`'s private-link endpoint
/// pattern.
#[derive(Debug, Hash, Eq, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
// Preserve the OpenAPI schema name for back-compat with existing clients.
#[cfg_attr(feature = "open-api", schema(as = AdlsProfile))]
#[serde(rename_all = "kebab-case")]
pub struct GenericAdlsProfile {
    /// Name of the adls filesystem, in blobstorage also known as container.
    pub filesystem: String,
    /// Subpath in the filesystem to use.
    pub key_prefix: Option<String>,
    /// Name of the azure storage account.
    pub account_name: String,
    /// The authority host to use for authentication. Default: `https://login.microsoftonline.com`.
    pub authority_host: Option<Url>,
    /// The host to use for the storage account. Default: `dfs.core.windows.net`.
    pub host: Option<String>,
    /// The validity of the sas token in seconds. Default: 3600. Max: 7 days.
    pub sas_token_validity_seconds: Option<u64>,
    /// Allow alternative protocols such as `wasbs://` in locations.
    /// This is disabled by default. We do not recommend to use this setting
    /// except for migration of old tables via the register endpoint.
    #[serde(default)]
    pub allow_alternative_protocols: bool,
    /// Enable SAS (Shared Access Signature) token generation for Azure Data Lake Storage.
    /// When disabled, clients cannot use vended credentials for this storage profile.
    /// Defaults to true.
    #[serde(default = "super::default_true")]
    pub sas_enabled: bool,
    /// Storage layout for namespace and tabular paths.
    #[serde(default)]
    pub storage_layout: Option<StorageLayout>,
}

impl GenericAdlsProfile {
    /// Check if an Azure variant is allowed.
    /// By default, only `abfss` is allowed.
    /// If `allow_alternative_protocols` is set, `wasbs` is also allowed.
    #[must_use]
    pub fn is_allowed_schema(&self, schema: &str) -> bool {
        schema == "abfss"
            || (self.allow_alternative_protocols && ALTERNATIVE_PROTOCOLS.contains(&schema))
    }

    /// Validate the Azure storage profile.
    ///
    /// # Errors
    /// - Fails if the filesystem name is invalid.
    /// - Fails if the key prefix is too long or invalid.
    /// - Fails if the account name is invalid.
    /// - Fails if the endpoint suffix is invalid.
    /// - Fails if the SAS-token TTL is 0 or above the 7-day max.
    pub(crate) fn normalize(&mut self) -> Result<(), ValidationError> {
        validate_sas_token_validity_seconds(
            self.sas_token_validity_seconds,
            MAX_GENERIC_ADLS_SAS_TOKEN_VALIDITY_SECONDS,
        )?;
        validate_filesystem_name(&self.filesystem)?;
        self.host = self.host.take().map(normalize_host).transpose()?.flatten();
        self.normalize_key_prefix()?;
        validate_storage_account_name(&self.account_name)?;

        Ok(())
    }

    /// Update the storage profile with another profile.
    /// `filesystem`, `key_prefix`, `authority_host` and `host` must be the same.
    /// We enforce this to avoid issues by accidentally changing the bucket or region
    /// of a warehouse, after which all tables would not be accessible anymore.
    /// Changing an endpoint might still result in an invalid profile, but we allow it.
    ///
    /// # Errors
    /// Fails if the `bucket`, `region` or `key_prefix` is different.
    pub fn update_with(self, mut other: Self) -> Result<Self, UpdateError> {
        if self.filesystem != other.filesystem {
            return Err(UpdateError::ImmutableField("filesystem".to_string()));
        }

        if self.key_prefix != other.key_prefix {
            return Err(UpdateError::ImmutableField("key_prefix".to_string()));
        }

        if self.authority_host != other.authority_host {
            return Err(UpdateError::ImmutableField("authority_host".to_string()));
        }

        if self.host != other.host {
            return Err(UpdateError::ImmutableField("host".to_string()));
        }

        if other.storage_layout.is_none() {
            other.storage_layout = self.storage_layout;
        }

        Ok(other)
    }

    #[allow(clippy::unused_self)]
    #[must_use]
    pub fn generate_catalog_config(&self, _: WarehouseId) -> CatalogConfig {
        adls_catalog_config()
    }

    /// Base Location for this storage profile.
    ///
    /// # Errors
    /// Can fail for un-normalized profiles
    pub fn base_location(&self) -> Result<Location, InvalidLocationError> {
        let location = if let Some(key_prefix) = &self.key_prefix {
            format!(
                "abfss://{}@{}.{}/{}/",
                self.filesystem,
                self.account_name,
                self.endpoint_host(),
                key_prefix.trim_matches('/')
            )
        } else {
            format!(
                "abfss://{}@{}.{}/",
                self.filesystem,
                self.account_name,
                self.endpoint_host().trim_end_matches('/'),
            )
        };
        Location::from_str(&location).map_err(|e| {
            InvalidLocationError::new(
                location,
                format!("Failed to create base location for storage profile: {e}"),
            )
        })
    }

    fn endpoint_host(&self) -> &str {
        self.host.as_deref().unwrap_or(DEFAULT_GENERIC_ADLS_HOST)
    }

    fn cloud_location(&self) -> CloudLocation {
        if let Some(host) = &self.host {
            CloudLocation::Custom {
                account: self.account_name.clone(),
                uri: format!("https://{}.{}", self.account_name, host),
            }
        } else {
            CloudLocation::Public {
                account: self.account_name.clone(),
            }
        }
    }

    #[must_use]
    pub(super) fn azure_settings(&self) -> AzureSettings {
        AzureSettings {
            authority_host: self.authority_host.clone(),
            cloud_location: self.cloud_location(),
        }
    }

    /// Get the Lakekeeper IO for this storage profile.
    ///
    /// # Errors
    /// - If system identity is requested but not enabled in the configuration.
    /// - If the client could not be initialized.
    pub async fn lakekeeper_io(
        &self,
        credential: &AzCredential,
    ) -> Result<AdlsStorage, CredentialsError> {
        adls_lakekeeper_io(self.azure_settings(), credential).await
    }

    /// Build an `AdlsStorage` client from previously-vended credentials.
    pub(in crate::service::storage) async fn lakekeeper_io_from_vended_table_config(
        &self,
        config: &TableProperties,
    ) -> Result<AdlsStorage, CredentialsError> {
        lakekeeper_io_from_vended_adls_table_config(
            self.azure_settings(),
            &self.iceberg_sas_property_key(),
            config,
        )
        .await
    }

    /// Generate the table configuration for Azure Datalake Storage Gen2.
    ///
    /// # Errors
    /// Fails if sas token cannot be generated.
    pub async fn generate_table_config(
        &self,
        data_access: DataAccessMode,
        credential: &AzCredential,
        stc_request: ShortTermCredentialsRequest,
        tabular_info: &impl BasicTabularInfo,
        request_metadata: &RequestMetadata,
    ) -> Result<TableConfig, TableConfigError> {
        if !data_access.provide_credentials() || !self.sas_enabled {
            tracing::debug!(
                "Not providing ADLS SAS credentials - provide_credentials: {}, sas_enabled: {}",
                data_access.provide_credentials(),
                self.sas_enabled
            );
            return Ok(TableConfig {
                creds: TableProperties::default(),
                config: TableProperties::default(),
                credentials_expiration_ms: None,
            });
        }

        let cache_key = STCCacheKey::new(stc_request.clone(), self.into(), Some(credential.into()));
        let settings = self.azure_settings();
        generate_adls_table_config(AdlsTableConfigContext {
            cache_key,
            sas_mint: SasMintContext {
                account_name: &self.account_name,
                filesystem: &self.filesystem,
                user_ttl: self.sas_token_validity_seconds,
                settings: &settings,
            },
            credential,
            stc_request,
            sas_property_key: self.iceberg_sas_property_key(),
            sas_expires_at_property_key: self.iceberg_sas_expires_at_property_key(),
            tabular_info,
            request_metadata,
            extra_config: vec![],
        })
        .await
    }

    #[must_use]
    pub(super) fn iceberg_sas_property_key(&self) -> String {
        iceberg_sas_property_key(&self.account_name, self.endpoint_host())
    }

    fn iceberg_sas_expires_at_property_key(&self) -> String {
        iceberg_expiration_property_key(&self.account_name, self.endpoint_host())
    }

    fn normalize_key_prefix(&mut self) -> Result<(), ValidationError> {
        if let Some(key_prefix) = self.key_prefix.as_mut() {
            *key_prefix = key_prefix.trim_matches('/').to_string();
        }

        if let Some(key_prefix) = self.key_prefix.as_ref()
            && key_prefix.is_empty()
        {
            self.key_prefix = None;
        }

        // Azure supports a max of 1024 chars and we need some buffer for tables.
        if let Some(key_prefix) = &self.key_prefix
            && key_prefix.len() > 512
        {
            return Err(InvalidProfileError {
                source: None,
                reason: "Storage Profile `key-prefix` must be less than 512 characters."
                    .to_string(),
                entity: "key-prefix".to_string(),
            }
            .into());
        }

        Ok(())
    }

    /// Check whether the location of this storage profile is overlapping
    /// with the given storage profile.
    #[must_use]
    pub fn is_overlapping_location(&self, other: &Self) -> bool {
        if self.filesystem != other.filesystem
            || self.account_name != other.account_name
            || self.host != other.host
            || self.authority_host != other.authority_host
        {
            return false;
        }
        key_prefix_overlaps(self.key_prefix.as_deref(), other.key_prefix.as_deref())
    }
}
