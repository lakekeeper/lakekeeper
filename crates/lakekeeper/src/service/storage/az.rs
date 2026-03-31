use std::{
    collections::HashMap,
    fmt,
    str::FromStr,
    sync::{Arc, LazyLock},
    time::{Duration, Instant},
};

use azure_core::{
    credentials::{Secret, TokenCredential},
    hmac::hmac_sha256,
};
use iceberg::io::ADLS_AUTHORITY_HOST;
use iceberg_ext::configs::table::{TableProperties, adls, creds, custom};
use lakekeeper_io::{
    InvalidLocationError, Location,
    adls::{
        AdlsLocation, AdlsStorage, AzureAuth, AzureClientCredentialsAuth, AzureSettings,
        AzureSharedAccessKeyAuth, normalize_host, validate_account_name, validate_filesystem_name,
    },
};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use url::Url;
use veil::Redact;

use crate::{
    CONFIG, WarehouseId,
    api::{
        CatalogConfig, RequestMetadata, Result,
        iceberg::{supported_endpoints, v1::tables::DataAccessMode},
    },
    request_metadata::UserAgent,
    service::{
        BasicTabularInfo,
        storage::{
            ShortTermCredentialsRequest, StoragePermissions, TableConfig,
            cache::{
                STCCacheKey, STCCacheValue, ShortTermCredential, get_stc_from_cache,
                insert_stc_into_cache,
            },
            error::{
                CredentialsError, IcebergFileIoError, InvalidProfileError, TableConfigError,
                UpdateError, ValidationError,
            },
            storage_layout::StorageLayout,
        },
    },
};

#[derive(Debug, Hash, Eq, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct AdlsProfile {
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
    /// The validity of the sas token in seconds. Default: 3600.
    pub sas_token_validity_seconds: Option<u64>,
    /// Allow alternative protocols such as `wasbs://` in locations.
    /// This is disabled by default. We do not recommend to use this setting
    /// except for migration of old tables via the register endpoint.
    #[serde(default)]
    pub allow_alternative_protocols: bool,
    /// Enable SAS (Shared Access Signature) token generation for Azure Data Lake Storage.
    /// When disabled, clients cannot use vended credentials for this storage profile.
    /// Defaults to true.
    #[serde(default = "default_true")]
    pub sas_enabled: bool,
    /// Storage layout for namespace and tabular paths.
    #[serde(default)]
    pub storage_layout: Option<StorageLayout>,
}

fn default_true() -> bool {
    true
}

const DEFAULT_HOST: &str = "dfs.core.windows.net";
static DEFAULT_AUTHORITY_HOST: LazyLock<Url> = LazyLock::new(|| {
    Url::parse("https://login.microsoftonline.com").expect("Default authority host is a valid URL")
});

const MAX_SAS_TOKEN_VALIDITY_SECONDS: u64 = 7 * 24 * 60 * 60;
const MAX_SAS_TOKEN_VALIDITY_SECONDS_I64: i64 = 7 * 24 * 60 * 60;
const SAS_TOKEN_DEFAULT_VALIDITY_SECONDS: u64 = 3600;

pub(crate) const ALTERNATIVE_PROTOCOLS: [&str; 1] = ["wasbs"];

impl AdlsProfile {
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
    pub(super) fn normalize(&mut self) -> Result<(), ValidationError> {
        if let Some(sas_validity) = self.sas_token_validity_seconds {
            // 7 days in seconds
            if sas_validity > MAX_SAS_TOKEN_VALIDITY_SECONDS {
                return Err(InvalidProfileError {
                    source: None,
                    reason: "SAS token can be only valid up to a week.".to_string(),
                    entity: "sas-token-validity-seconds".to_string(),
                }
                .into());
            }
        }
        validate_filesystem_name(&self.filesystem)?;
        self.host = self.host.take().map(normalize_host).transpose()?.flatten();
        self.normalize_key_prefix()?;
        validate_account_name(&self.account_name)?;

        Ok(())
    }

    /// Update the storage profile with another profile.
    /// `key_prefix`, `region` and `bucket` must be the same.
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

    // may change..
    #[allow(clippy::unused_self)]
    #[must_use]
    pub fn generate_catalog_config(&self, _: WarehouseId) -> CatalogConfig {
        CatalogConfig {
            defaults: HashMap::default(),
            overrides: HashMap::default(),
            endpoints: supported_endpoints().to_vec(),
        }
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
                self.host.as_deref().unwrap_or(DEFAULT_HOST),
                key_prefix.trim_matches('/')
            )
        } else {
            format!(
                "abfss://{}@{}.{}/",
                self.filesystem,
                self.account_name,
                self.host
                    .as_deref()
                    .unwrap_or(DEFAULT_HOST)
                    .trim_end_matches('/'),
            )
        };
        let location = Location::from_str(&location).map_err(|e| {
            InvalidLocationError::new(
                location,
                format!("Failed to create base location for storage profile: {e}"),
            )
        })?;

        Ok(location)
    }

    fn cloud_location(&self) -> lakekeeper_io::adls::CloudLocation {
        if let Some(host) = &self.host {
            lakekeeper_io::adls::CloudLocation::Custom {
                account: self.account_name.clone(),
                uri: host.clone(),
            }
        } else {
            lakekeeper_io::adls::CloudLocation::Public {
                account: self.account_name.clone(),
            }
        }
    }

    fn azure_settings(&self) -> AzureSettings {
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
        let azure_auth = AzureAuth::try_from(credential.clone())?;
        self.azure_settings()
            .get_storage_client(&azure_auth)
            .await
            .map_err(Into::into)
    }

    /// Generate the table configuration for Azure Datalake Storage Gen2.
    ///
    /// # Errors
    /// Fails if sas token cannot be generated.
    #[allow(clippy::too_many_lines)]
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
                "Not providing Azure SAS credentials - provide_credentials: {}, sas_enabled: {}",
                data_access.provide_credentials(),
                self.sas_enabled
            );
            return Ok(TableConfig {
                creds: TableProperties::default(),
                config: TableProperties::default(),
            });
        }

        let cache_key = STCCacheKey::new(stc_request.clone(), self.into(), Some(credential.into()));
        let cached_result = self.load_sas_token_from_cache(&cache_key).await;

        let (sas, expiration) = if let Some((sas_token, exp)) = cached_result {
            (sas_token, exp)
        } else {
            let (sas_token_start, requested_sas_token_end) = self.sas_token_validity_period();
            tracing::debug!(
                "Generating SAS token with requested validity - start: {}, end: {}",
                sas_token_start,
                requested_sas_token_end
            );
            let (sas, expiration) = match credential {
                AzCredential::ClientCredentials { .. } => {
                    let azure_auth = AzureAuth::try_from(credential.clone())?;
                    let token_cred = self
                        .azure_settings()
                        .get_token_credential(&azure_auth)
                        .await
                        .map_err(CredentialsError::from)?;
                    self.sas_via_delegation_key(
                        sas_token_start,
                        requested_sas_token_end,
                        &stc_request,
                        token_cred,
                    )
                    .await?
                }
                AzCredential::SharedAccessKey { key } => {
                    let sas = self.sas(
                        &stc_request,
                        requested_sas_token_end,
                        &SasSigningKey::SharedKey(Secret::new(key.clone())),
                    )?;
                    (sas, requested_sas_token_end)
                }
                AzCredential::AzureSystemIdentity {} => {
                    let azure_auth = AzureAuth::try_from(credential.clone())?;
                    let token_cred = self
                        .azure_settings()
                        .get_token_credential(&azure_auth)
                        .await
                        .map_err(CredentialsError::from)?;
                    self.sas_via_delegation_key(
                        sas_token_start,
                        requested_sas_token_end,
                        &stc_request,
                        token_cred,
                    )
                    .await
                    .map_err(|e| {
                        tracing::debug!("Failed to get azure system identity token: {e}",);
                        CredentialsError::ShortTermCredential {
                            reason: "Failed to get azure system identity token".to_string(),
                            source: Some(Box::new(e)),
                        }
                    })?
                }
            };

            if CONFIG.cache.stc.enabled {
                let sts_validity_duration = Duration::from_secs(
                    self.sas_token_validity_seconds
                        .unwrap_or(SAS_TOKEN_DEFAULT_VALIDITY_SECONDS),
                );
                let valid_until = Instant::now().checked_add(sts_validity_duration);
                let cache_value = STCCacheValue::new(
                    ShortTermCredential::Adls {
                        sas_token: sas.clone(),
                        expiration,
                    },
                    valid_until,
                );
                insert_stc_into_cache(cache_key, cache_value).await;
            }

            (sas, expiration)
        };

        let mut creds = TableProperties::default();

        let expiration_ms = expiration.unix_timestamp().saturating_mul(1000);
        creds.insert(&creds::ExpirationTimeMs(expiration_ms));
        creds.insert(&custom::CustomConfig {
            key: self.iceberg_sas_property_key(),
            value: sas,
        });
        creds.insert(&adls::RefreshClientCredentialsEndpoint(
            request_metadata.refresh_client_credentials_endpoint_for_table(
                tabular_info.warehouse_id(),
                tabular_info.tabular_ident(),
            ),
        ));

        // Note: PyIceberg versions up to 0.10.0 have a bug parsing ADLS vended credentials.
        // The client incorrectly extracts the account name from *any* property starting with
        // "adls.sas-token", including "adls.sas-token-expires-at-ms.*", which breaks endpoint detection.
        let pyiceberg_version = match request_metadata.user_agent() {
            Some(UserAgent::PyIceberg { version }) => Some(version),
            _ => None,
        };
        let can_handle_sas_expiry_key = pyiceberg_version
            .as_ref()
            .and_then(|v| semver::Version::parse(v).ok())
            .is_some_and(|v| v > semver::Version::new(0, 10, 0));
        if pyiceberg_version.is_none() || can_handle_sas_expiry_key {
            creds.insert(&custom::CustomConfig {
                key: self.iceberg_sas_expires_at_property_key(),
                value: expiration_ms.to_string(),
            });
        } else {
            tracing::debug!(
                "Not inserting '{}' property for PyIceberg version {:?} due to known parsing issues.",
                self.iceberg_sas_expires_at_property_key(),
                pyiceberg_version,
            );
        }

        Ok(TableConfig {
            // Due to backwards compat reasons we still return creds within config too
            config: creds.clone(),
            creds,
        })
    }

    fn sas_token_validity_period(&self) -> (OffsetDateTime, OffsetDateTime) {
        // allow for some clock drift
        let start = OffsetDateTime::now_utc() - time::Duration::minutes(1);

        // Add 1 minutes to validity to account for clock drift
        let validity = self
            .sas_token_validity_seconds
            .unwrap_or(SAS_TOKEN_DEFAULT_VALIDITY_SECONDS)
            + 60;

        let clamped_validity_seconds = i64::try_from(validity)
            .unwrap_or(MAX_SAS_TOKEN_VALIDITY_SECONDS_I64)
            .clamp(120, MAX_SAS_TOKEN_VALIDITY_SECONDS_I64);

        let end = start.saturating_add(time::Duration::seconds(clamped_validity_seconds));

        (start, end)
    }

    async fn load_sas_token_from_cache(
        &self,
        cache_key: &STCCacheKey,
    ) -> Option<(String, OffsetDateTime)> {
        let stc_request = &cache_key.request;
        if CONFIG.cache.stc.enabled {
            if let Some(STCCacheValue {
                credentials:
                    ShortTermCredential::Adls {
                        sas_token,
                        expiration,
                    },
                ..
            }) = get_stc_from_cache(cache_key).await
            {
                tracing::debug!("Using cached short term credentials for request: {stc_request}");
                return Some((sas_token, expiration));
            }
            tracing::debug!(
                "No cached SAS token found for request: {stc_request}, fetching new credentials"
            );
        } else {
            tracing::debug!(
                "STC caching disabled, fetching new SAS token for request: {stc_request}"
            );
        }

        None
    }

    async fn sas_via_delegation_key(
        &self,
        sas_token_start: OffsetDateTime,
        sas_token_end: OffsetDateTime,
        stc_request: &ShortTermCredentialsRequest,
        token_cred: Arc<dyn TokenCredential>,
    ) -> Result<(String, OffsetDateTime), CredentialsError> {
        tracing::debug!(
            "Requesting user delegation key from azure for sas token generation - Valid from {sas_token_start} to {sas_token_end}",
        );

        // Get a Bearer token scoped to Azure Storage
        let access_token = token_cred
            .get_token(&["https://storage.azure.com/.default"], None)
            .await
            .map_err(|e| CredentialsError::ShortTermCredential {
                reason: "Failed to get Azure access token for user delegation key request."
                    .to_string(),
                source: Some(Box::new(e)),
            })?;

        // Build the user delegation key endpoint
        let blob_endpoint = self.cloud_location().blob_endpoint();
        let url = format!("{blob_endpoint}?restype=service&comp=userdelegationkey");

        // Build the XML request body
        let start_str = format_iso8601(sas_token_start);
        let end_str = format_iso8601(sas_token_end);
        let body = format!(
            r#"<?xml version="1.0" encoding="utf-8"?>
<KeyInfo>
  <Start>{start_str}</Start>
  <Expiry>{end_str}</Expiry>
</KeyInfo>"#
        );

        let response = reqwest::Client::new()
            .post(&url)
            .bearer_auth(access_token.token.secret())
            .header("x-ms-version", SERVICE_SAS_VERSION)
            .header("Content-Type", "application/xml")
            .body(body)
            .send()
            .await
            .map_err(|e| CredentialsError::ShortTermCredential {
                reason: "HTTP request for user delegation key failed.".to_string(),
                source: Some(Box::new(e)),
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(CredentialsError::ShortTermCredential {
                reason: format!(
                    "User delegation key request returned HTTP {}: {body}",
                    status.as_u16()
                ),
                source: None,
            });
        }

        let xml_body =
            response
                .text()
                .await
                .map_err(|e| CredentialsError::ShortTermCredential {
                    reason: "Failed to read user delegation key response body.".to_string(),
                    source: Some(Box::new(e)),
                })?;

        let key = parse_user_delegation_key(&xml_body, sas_token_end).map_err(|reason| {
            CredentialsError::ShortTermCredential {
                reason,
                source: None,
            }
        })?;

        tracing::debug!(
            "Successfully obtained user delegation key from azure for sas token generation - Valid until {}",
            key.signed_expiry
        );

        let signed_expiry = key.signed_expiry;
        let sas = self.sas(
            stc_request,
            signed_expiry,
            &SasSigningKey::UserDelegation(key),
        )?;
        Ok((sas, signed_expiry))
    }

    fn sas(
        &self,
        stc_request: &ShortTermCredentialsRequest,
        signed_expiry: OffsetDateTime,
        key: &SasSigningKey,
    ) -> Result<String, CredentialsError> {
        let path = reduce_scheme_string(stc_request.table_location.as_ref());
        let rootless_path = path.trim_start_matches('/').trim_end_matches('/');
        let depth = rootless_path.split('/').count();

        let canonical_resource = format!(
            "/blob/{}/{}/{}",
            self.account_name.as_str(),
            self.filesystem.as_str(),
            rootless_path
        );

        tracing::debug!(
            "Generating SAS token for resource `{canonical_resource}` with permissions {} valid until {signed_expiry}",
            stc_request.storage_permissions
        );

        let permissions = permissions_string(stc_request.storage_permissions);
        let sas_token =
            build_sas_token(key, &canonical_resource, &permissions, signed_expiry, depth).map_err(
                |e| CredentialsError::ShortTermCredential {
                    reason: format!("Error building Azure SAS token: {e}"),
                    source: Some(Box::new(e)),
                },
            )?;

        Ok(sas_token)
    }

    fn iceberg_sas_property_key(&self) -> String {
        iceberg_sas_property_key(
            &self.account_name,
            self.host.as_deref().unwrap_or(DEFAULT_HOST),
        )
    }

    fn iceberg_sas_expires_at_property_key(&self) -> String {
        iceberg_expiration_property_key(
            &self.account_name,
            self.host.as_deref().unwrap_or(DEFAULT_HOST),
        )
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

    #[must_use]
    /// Check whether the location of this storage profile is overlapping
    /// with the given storage profile.
    pub fn is_overlapping_location(&self, other: &Self) -> bool {
        // Different filesystem, account_name, or host means no overlap
        if self.filesystem != other.filesystem
            || self.account_name != other.account_name
            || self.host != other.host
            || self.authority_host != other.authority_host
        {
            return false;
        }

        // If key prefixes are identical, they overlap
        if self.key_prefix == other.key_prefix {
            return true;
        }

        match (&self.key_prefix, &other.key_prefix) {
            // Both have Some key_prefix values - check if one is a prefix of the other
            (Some(key_prefix), Some(other_key_prefix)) => {
                let kp1 = format!("{key_prefix}/");
                let kp2 = format!("{other_key_prefix}/");
                kp1.starts_with(&kp2) || kp2.starts_with(&kp1)
            }
            // If either has no key prefix, it can access the entire filesystem
            (None, _) | (_, None) => true,
        }
    }
}

/// Removes the hostname and user from the path.
/// Keeps only the path and optionally the scheme.
#[must_use]
pub(crate) fn reduce_scheme_string(path: &str) -> String {
    AdlsLocation::try_from_str(path, true)
        .map(|l| format!("/{}", l.blob_name().clone().trim_start_matches('/')))
        .unwrap_or(path.to_string())
}

#[derive(Redact, Hash, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(tag = "credential-type", rename_all = "kebab-case")]
pub enum AzCredential {
    #[serde(rename_all = "kebab-case")]
    #[cfg_attr(feature = "open-api", schema(title = "AzCredentialClientCredentials"))]
    ClientCredentials {
        client_id: String,
        tenant_id: String,
        #[redact(partial)]
        client_secret: String,
    },
    #[serde(rename_all = "kebab-case")]
    #[cfg_attr(feature = "open-api", schema(title = "AzCredentialSharedAccessKey"))]
    SharedAccessKey {
        #[redact]
        key: String,
    },
    #[serde(rename_all = "kebab-case")]
    #[cfg_attr(feature = "open-api", schema(title = "AzCredentialManagedIdentity"))]
    AzureSystemIdentity {},
}

fn permissions_string(permissions: StoragePermissions) -> String {
    match permissions {
        StoragePermissions::Read => "rl".to_string(),
        StoragePermissions::ReadWrite => "racwl".to_string(),
        StoragePermissions::ReadWriteDelete => "racwdxyl".to_string(),
    }
}

fn iceberg_sas_property_key(account_name: &str, endpoint_suffix: &str) -> String {
    format!("adls.sas-token.{account_name}.{endpoint_suffix}")
}

fn iceberg_expiration_property_key(account_name: &str, endpoint_suffix: &str) -> String {
    format!("adls.sas-token-expires-at-ms.{account_name}.{endpoint_suffix}")
}

pub(super) fn get_file_io_from_table_config(
    config: &TableProperties,
) -> Result<iceberg::io::FileIO, IcebergFileIoError> {
    // Add Authority host if not present
    let mut config = config.inner().clone();

    let sas_token_prefix = "adls.sas-token.";
    // Iceberg Rust cannot parse tokens of form "<sas_token_prefix><storage_account_name>.<endpoint_suffix>=<sas_token>"
    // https://github.com/apache/iceberg-rust/issues/1442
    let mut sas_token = None;
    for (key, value) in &config {
        if key.starts_with(sas_token_prefix) {
            sas_token = Some(value.clone());
            break;
        }
    }
    if let Some(sas_token) = sas_token {
        config.remove(sas_token_prefix);
        config.insert("adls.sas-token".to_string(), sas_token);
    }

    if !config.contains_key(ADLS_AUTHORITY_HOST) {
        config.insert(
            ADLS_AUTHORITY_HOST.to_string(),
            DEFAULT_AUTHORITY_HOST.to_string(),
        );
    }
    Ok(iceberg::io::FileIOBuilder::new("abfss")
        .with_props(config)
        .build()?)
}

impl TryFrom<AzCredential> for AzureAuth {
    type Error = CredentialsError;

    fn try_from(cred: AzCredential) -> Result<Self, Self::Error> {
        if !CONFIG.enable_azure_system_credentials
            && matches!(cred, AzCredential::AzureSystemIdentity {})
        {
            return Err(CredentialsError::Misconfiguration(
                "Azure System identity credentials are disabled in this Lakekeeper deployment."
                    .to_string(),
            ));
        }

        Ok(match cred {
            AzCredential::ClientCredentials {
                client_id,
                tenant_id,
                client_secret,
            } => AzureClientCredentialsAuth {
                client_id,
                tenant_id,
                client_secret,
            }
            .into(),
            AzCredential::SharedAccessKey { key } => AzureSharedAccessKeyAuth { key }.into(),
            AzCredential::AzureSystemIdentity {} => AzureAuth::AzureSystemIdentity,
        })
    }
}

// ---------------------------------------------------------------------------
// SAS token generation helpers
// ---------------------------------------------------------------------------
//
// NOTE: The `build_sas_token`, `parse_user_delegation_key`, and
// `sas_via_delegation_key` functions below implement SAS URL creation
// manually because `azure_storage_blob` 0.10 does not yet provide this
// capability.  Once https://github.com/Azure/azure-sdk-for-rust/issues/3330
// is resolved and the SDK exposes a SAS-generation API, these helpers (and
// the raw `reqwest` call in `sas_via_delegation_key`) can be replaced with
// the upstream implementation.

/// Azure Storage SAS API version used for token signing.
const SERVICE_SAS_VERSION: &str = "2022-11-02";

/// Signing key for a SAS token – either a raw shared account key or a
/// user-delegation key obtained from the Azure Storage REST API.
enum SasSigningKey {
    SharedKey(Secret),
    UserDelegation(UserDelegationKey),
}

/// A user-delegation key as returned by the Azure Blob Storage
/// `Get User Delegation Key` REST operation.
struct UserDelegationKey {
    /// Object-id of the Azure AD principal that requested the key.
    signed_oid: String,
    /// Tenant-id of the Azure AD principal.
    signed_tid: String,
    /// Start of the key's validity period.
    signed_start: OffsetDateTime,
    /// End of the key's validity period.
    signed_expiry: OffsetDateTime,
    /// Service (`b` = Blob).
    signed_service: String,
    /// SAS version the key was issued for.
    signed_version: String,
    /// The actual signing key material (base64-encoded).
    value: Secret,
}

/// A simple error type for SAS-building failures so that the error can be
/// boxed and stored inside `CredentialsError::ShortTermCredential`.
#[derive(Debug)]
struct SasError(String);

impl fmt::Display for SasError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SasError {}

/// Format an [`OffsetDateTime`] as `YYYY-MM-DDTHH:MM:SSZ` (ISO 8601 / RFC 3339
/// truncated to seconds), which is what Azure SAS tokens require.
fn format_iso8601(dt: OffsetDateTime) -> String {
    // OffsetDateTime::format is fallible but with a static format string it
    // will never panic in practice.
    let fmt = time::format_description::parse("[year]-[month]-[day]T[hour]:[minute]:[second]Z")
        .expect("static format string is valid");
    dt.to_offset(time::UtcOffset::UTC)
        .format(&fmt)
        .expect("formatting OffsetDateTime failed")
}

/// Build a Service SAS token URL query-string for a blob or directory
/// resource.
///
/// `depth` is the number of path components in the resource path (used for
/// the `sdd` / signed directory depth parameter for hierarchical namespace).
///
/// # Errors
/// Returns [`SasError`] if HMAC signing fails (e.g. bad base64 key).
fn build_sas_token(
    key: &SasSigningKey,
    canonical_resource: &str,
    permissions: &str,
    expiry: OffsetDateTime,
    depth: usize,
) -> Result<String, SasError> {
    // Resource type: "d" for directory, "b" for blob.
    // A path with depth >= 1 is treated as a directory prefix.
    let resource = "d";
    let expiry_str = format_iso8601(expiry);
    let version = SERVICE_SAS_VERSION;

    // Build the string-to-sign.  The exact field order must match the Azure
    // documentation for service-SAS version 2022-11-02.
    let string_to_sign = match key {
        SasSigningKey::SharedKey(_) => {
            // SharedKey SAS string-to-sign (16 fields, joined by \n):
            // signedPermissions, signedStart, signedExpiry, canonicalizedResource,
            // signedIdentifier, signedIP, signedProtocol, signedVersion,
            // signedResource, signedSnapshotTime, signedEncryptionScope,
            // rscc, rscd, rsce, rscl, rsct
            [
                permissions, // sp
                "",          // st  (no start time)
                &expiry_str, // se
                canonical_resource,
                "",       // si  (no stored access policy)
                "",       // sip (no IP restriction)
                "",       // spr (no protocol restriction)
                version,  // sv
                resource, // sr
                "",       // snapshot time
                "",       // signed encryption scope
                "",       // rscc
                "",       // rscd
                "",       // rsce
                "",       // rscl
                "",       // rsct
            ]
            .join("\n")
        }
        SasSigningKey::UserDelegation(udk) => {
            // User-delegation SAS string-to-sign (24 fields, joined by \n):
            // signedPermissions, signedStart, signedExpiry, canonicalizedResource,
            // signedKeyObjectId, signedKeyTenantId, signedKeyStart, signedKeyExpiry,
            // signedKeyService, signedKeyVersion,
            // signedAuthorizedUserObjectId, signedUnauthorizedUserObjectId,
            // signedCorrelationId,
            // signedIP, signedProtocol, signedVersion, signedResource,
            // signedSnapshotTime, signedEncryptionScope,
            // rscc, rscd, rsce, rscl, rsct
            let key_start = format_iso8601(udk.signed_start);
            let key_expiry = format_iso8601(udk.signed_expiry);
            [
                permissions, // sp
                "",          // st
                &expiry_str, // se
                canonical_resource,
                &udk.signed_oid,     // skoid
                &udk.signed_tid,     // sktid
                &key_start,          // skt
                &key_expiry,         // ske
                &udk.signed_service, // sks
                &udk.signed_version, // skv
                "",                  // saoid (authorised user OID)
                "",                  // suoid (unauthorised user OID)
                "",                  // scid (correlation id)
                "",                  // sip
                "",                  // spr
                version,             // sv
                resource,            // sr
                "",                  // snapshot time
                "",                  // signed encryption scope
                "",                  // rscc
                "",                  // rscd
                "",                  // rsce
                "",                  // rscl
                "",                  // rsct
            ]
            .join("\n")
        }
    };

    let signing_key = match key {
        SasSigningKey::SharedKey(secret) => secret,
        SasSigningKey::UserDelegation(udk) => &udk.value,
    };

    let signature = hmac_sha256(&string_to_sign, signing_key)
        .map_err(|e| SasError(format!("HMAC-SHA256 signing failed: {e}")))?;

    let depth_param = if depth > 0 { depth } else { 1 };

    // Build the query string.
    let mut params: Vec<(&str, String)> = Vec::new();
    if let SasSigningKey::UserDelegation(udk) = key {
        params.push(("skoid", udk.signed_oid.clone()));
        params.push(("sktid", udk.signed_tid.clone()));
        params.push(("skt", format_iso8601(udk.signed_start)));
        params.push(("ske", format_iso8601(udk.signed_expiry)));
        params.push(("sks", udk.signed_service.clone()));
        params.push(("skv", udk.signed_version.clone()));
    }
    params.push(("sv", version.to_string()));
    params.push(("sp", permissions.to_string()));
    params.push(("sr", resource.to_string()));
    params.push(("se", expiry_str));
    params.push(("sdd", depth_param.to_string()));
    params.push(("sig", signature));

    let token = params
        .iter()
        .map(|(k, v)| format!("{k}={}", urlencoding::encode(v)))
        .collect::<Vec<_>>()
        .join("&");

    Ok(token)
}

/// Parse an Azure `UserDelegationKey` XML response body.
///
/// `fallback_expiry` is used as the `signed_expiry` if the XML field is
/// absent or cannot be parsed.
///
/// Returns `Err(String)` with a human-readable error description on failure.
#[allow(clippy::similar_names)]
fn parse_user_delegation_key(
    xml: &str,
    fallback_expiry: OffsetDateTime,
) -> Result<UserDelegationKey, String> {
    fn extract(xml: &str, tag: &str) -> Option<String> {
        let open = format!("<{tag}>");
        let close = format!("</{tag}>");
        let start = xml.find(&open)? + open.len();
        let end = xml.find(&close)?;
        if end < start {
            return None;
        }
        Some(xml[start..end].trim().to_string())
    }

    fn parse_dt(s: &str, fallback: OffsetDateTime) -> OffsetDateTime {
        // Azure returns ISO 8601 like "2022-08-22T15:11:43.0000000Z"
        OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339).unwrap_or(fallback)
    }

    let signed_oid = extract(xml, "SignedOid")
        .ok_or_else(|| "Missing <SignedOid> in user delegation key response".to_string())?;
    let signed_tid = extract(xml, "SignedTid")
        .ok_or_else(|| "Missing <SignedTid> in user delegation key response".to_string())?;
    let signed_service = extract(xml, "SignedService").unwrap_or_else(|| "b".to_string());
    let signed_version =
        extract(xml, "SignedVersion").unwrap_or_else(|| SERVICE_SAS_VERSION.to_string());
    let value = extract(xml, "Value")
        .ok_or_else(|| "Missing <Value> in user delegation key response".to_string())?;

    let signed_start =
        extract(xml, "SignedStart").map_or(fallback_expiry, |s| parse_dt(&s, fallback_expiry));
    let signed_expiry =
        extract(xml, "SignedExpiry").map_or(fallback_expiry, |s| parse_dt(&s, fallback_expiry));

    Ok(UserDelegationKey {
        signed_oid,
        signed_tid,
        signed_start,
        signed_expiry,
        signed_service,
        signed_version,
        value: Secret::new(value),
    })
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::service::storage::{
        AdlsProfile, StorageProfile,
        az::DEFAULT_AUTHORITY_HOST,
        storage_layout::{NamespaceNameContext, NamespacePath, TabularNameContext},
    };

    #[test]
    fn test_reduce_scheme_string() {
        // Test abfss protocol
        let path = "abfss://filesystem@dfs.windows.net/path/_test";
        let reduced_path = reduce_scheme_string(path);
        assert_eq!(reduced_path, "/path/_test");

        // Test wasbs protocol
        let wasbs_path = "wasbs://filesystem@account.windows.net/path/to/data";
        let reduced_wasbs_path = reduce_scheme_string(wasbs_path);
        assert_eq!(reduced_wasbs_path, "/path/to/data");

        // Test a non-matching path
        let non_matching = "http://example.com/path";
        assert_eq!(reduce_scheme_string(non_matching), non_matching);
    }

    pub(crate) mod azure_integration_tests {
        use crate::{
            api::RequestMetadata,
            service::storage::{AdlsProfile, AzCredential, StorageCredential, StorageProfile},
        };

        pub(crate) fn azure_profile() -> AdlsProfile {
            let account_name = std::env::var("LAKEKEEPER_TEST__AZURE_STORAGE_ACCOUNT_NAME")
                .expect("LAKEKEEPER_TEST__AZURE_STORAGE_ACCOUNT_NAME to be set");
            let filesystem = std::env::var("LAKEKEEPER_TEST__AZURE_STORAGE_FILESYSTEM")
                .expect("LAKEKEEPER_TEST__AZURE_STORAGE_FILESYSTEM to be set");

            let key_prefix = format!("test-{}", uuid::Uuid::now_v7());
            AdlsProfile {
                filesystem,
                key_prefix: Some(key_prefix.clone()),
                account_name,
                authority_host: None,
                host: None,
                sas_token_validity_seconds: None,
                allow_alternative_protocols: false,
                sas_enabled: true,
                storage_layout: None,
            }
        }

        pub(crate) fn client_creds() -> AzCredential {
            let client_id = std::env::var("LAKEKEEPER_TEST__AZURE_CLIENT_ID")
                .expect("LAKEKEEPER_TEST__AZURE_CLIENT_ID to be set");
            let client_secret = std::env::var("LAKEKEEPER_TEST__AZURE_CLIENT_SECRET")
                .expect("LAKEKEEPER_TEST__AZURE_CLIENT_SECRET to be set");
            let tenant_id = std::env::var("LAKEKEEPER_TEST__AZURE_TENANT_ID")
                .expect("LAKEKEEPER_TEST__AZURE_TENANT_ID to be set");

            AzCredential::ClientCredentials {
                client_id,
                client_secret,
                tenant_id,
            }
        }

        pub(crate) fn shared_key() -> AzCredential {
            let key = std::env::var("LAKEKEEPER_TEST__AZURE_STORAGE_SHARED_KEY")
                .expect("LAKEKEEPER_TEST__AZURE_STORAGE_SHARED_KEY to be set");
            AzCredential::SharedAccessKey { key }
        }

        #[tokio::test]
        async fn test_can_validate_adls() {
            for (cred, typ) in [
                (client_creds(), "client-creds"),
                (shared_key(), "shared-key"),
            ] {
                let prof = azure_profile();
                let mut prof: StorageProfile = prof.into();
                prof.normalize(Some(&cred.clone().into()))
                    .expect("failed to validate profile");
                let cred: StorageCredential = cred.into();
                Box::pin(prof.validate_access(
                    Some(&cred),
                    None,
                    &RequestMetadata::new_unauthenticated(),
                ))
                .await
                .unwrap_or_else(|e| panic!("Failed to validate '{typ}' due to '{e:?}'"));
            }
        }

        mod azure_system_credentials_integration_tests {
            use super::*;

            #[tokio::test]
            async fn test_system_identity_can_validate() {
                let prof = azure_profile();
                let mut prof: StorageProfile = prof.into();
                prof.normalize(None).expect("failed to validate profile");
                let cred = AzCredential::AzureSystemIdentity {};
                let cred: StorageCredential = cred.into();
                Box::pin(prof.validate_access(
                    Some(&cred),
                    None,
                    &RequestMetadata::new_unauthenticated(),
                ))
                .await
                .unwrap_or_else(|e| panic!("Failed to validate system identity due to '{e:?}'"));
            }
        }
    }

    #[test]
    fn test_default_authority() {
        assert_eq!(
            DEFAULT_AUTHORITY_HOST.as_str(),
            "https://login.microsoftonline.com/"
        );
    }

    #[test]
    fn test_default_adls_locations() {
        let profile = AdlsProfile {
            filesystem: "filesystem".to_string(),
            key_prefix: Some("test_prefix".to_string()),
            account_name: "account".to_string(),
            authority_host: None,
            host: None,
            sas_token_validity_seconds: None,
            allow_alternative_protocols: false,
            sas_enabled: true,
            storage_layout: None,
        };

        let sp: StorageProfile = profile.clone().into();

        let namespace_uuid = uuid::Uuid::now_v7();
        let tabular_uuid = uuid::Uuid::now_v7();
        let namespace_path = NamespacePath::new(vec![NamespaceNameContext {
            name: "test_ns".to_string(),
            uuid: namespace_uuid,
        }]);
        let tabular_name_context = TabularNameContext {
            name: "test_tabular".to_string(),
            uuid: tabular_uuid,
        };
        let namespace_location = sp.default_namespace_location(&namespace_path).unwrap();

        let location = sp.default_tabular_location(&namespace_location, &tabular_name_context);
        assert_eq!(
            location.to_string(),
            format!(
                "abfss://filesystem@account.dfs.core.windows.net/test_prefix/{namespace_uuid}/{tabular_uuid}"
            )
        );

        let mut profile = profile.clone();
        profile.key_prefix = None;
        profile.host = Some("blob.com".to_string());
        let sp: StorageProfile = profile.into();

        let namespace_location = sp.default_namespace_location(&namespace_path).unwrap();
        let location = sp.default_tabular_location(&namespace_location, &tabular_name_context);
        assert_eq!(
            location.to_string(),
            format!("abfss://filesystem@account.blob.com/{namespace_uuid}/{tabular_uuid}")
        );
    }

    #[test]
    fn test_allow_alternative_protocols() {
        let profile = AdlsProfile {
            filesystem: "filesystem".to_string(),
            key_prefix: Some("test_prefix".to_string()),
            account_name: "account".to_string(),
            authority_host: None,
            host: None,
            sas_token_validity_seconds: None,
            allow_alternative_protocols: true,
            sas_enabled: true,
            storage_layout: None,
        };

        assert!(
            profile.is_allowed_schema("abfss"),
            "abfss should be allowed"
        );
        assert!(
            profile.is_allowed_schema("wasbs"),
            "wasbs should be allowed with flag set"
        );

        let profile = AdlsProfile {
            filesystem: "filesystem".to_string(),
            key_prefix: Some("test_prefix".to_string()),
            account_name: "account".to_string(),
            authority_host: None,
            host: None,
            sas_token_validity_seconds: None,
            allow_alternative_protocols: false,
            sas_enabled: true,
            storage_layout: None,
        };

        assert!(
            profile.is_allowed_schema("abfss"),
            "abfss should always be allowed"
        );
        assert!(
            !profile.is_allowed_schema("wasbs"),
            "wasbs should not be allowed with flag unset"
        );
    }
}

#[cfg(test)]
mod is_overlapping_location_tests {
    use super::*;

    fn create_profile(
        filesystem: &str,
        account_name: &str,
        host: Option<&str>,
        authority_host: Option<&str>,
        key_prefix: Option<&str>,
    ) -> AdlsProfile {
        AdlsProfile {
            filesystem: filesystem.to_string(),
            account_name: account_name.to_string(),
            host: host.map(ToString::to_string),
            authority_host: authority_host.map(|url| url.parse().unwrap()),
            key_prefix: key_prefix.map(ToString::to_string),
            sas_token_validity_seconds: None,
            allow_alternative_protocols: false,
            sas_enabled: true,
            storage_layout: None,
        }
    }

    #[test]
    fn test_non_overlapping_different_filesystem() {
        let profile1 = create_profile("filesystem1", "account", None, None, Some("prefix"));
        let profile2 = create_profile("filesystem2", "account", None, None, Some("prefix"));

        assert!(!profile1.is_overlapping_location(&profile2));
    }

    #[test]
    fn test_non_overlapping_different_account_name() {
        let profile1 = create_profile("filesystem", "account1", None, None, Some("prefix"));
        let profile2 = create_profile("filesystem", "account2", None, None, Some("prefix"));

        assert!(!profile1.is_overlapping_location(&profile2));
    }

    #[test]
    fn test_non_overlapping_different_host() {
        let profile1 = create_profile("filesystem", "account", Some("host1"), None, Some("prefix"));
        let profile2 = create_profile("filesystem", "account", Some("host2"), None, Some("prefix"));

        assert!(!profile1.is_overlapping_location(&profile2));
    }

    #[test]
    fn test_non_overlapping_different_authority_host() {
        let profile1 = create_profile(
            "filesystem",
            "account",
            None,
            Some("https://login1.example.com"),
            Some("prefix"),
        );
        let profile2 = create_profile(
            "filesystem",
            "account",
            None,
            Some("https://login2.example.com"),
            Some("prefix"),
        );

        assert!(!profile1.is_overlapping_location(&profile2));
    }

    #[test]
    fn test_overlapping_identical_key_prefix() {
        let profile1 = create_profile("filesystem", "account", None, None, Some("prefix"));
        let profile2 = create_profile("filesystem", "account", None, None, Some("prefix"));

        assert!(profile1.is_overlapping_location(&profile2));
    }

    #[test]
    fn test_overlapping_one_prefix_of_other() {
        let profile1 = create_profile("filesystem", "account", None, None, Some("prefix"));
        let profile2 = create_profile("filesystem", "account", None, None, Some("prefix/subpath"));

        assert!(profile1.is_overlapping_location(&profile2));
        assert!(profile2.is_overlapping_location(&profile1)); // Test symmetry
    }

    #[test]
    fn test_overlapping_no_key_prefix() {
        let profile1 = create_profile("filesystem", "account", None, None, None);
        let profile2 = create_profile("filesystem", "account", None, None, Some("prefix"));

        assert!(profile1.is_overlapping_location(&profile2));
        assert!(profile2.is_overlapping_location(&profile1)); // Test symmetry
    }

    #[test]
    fn test_non_overlapping_unrelated_key_prefixes() {
        let profile1 = create_profile("filesystem", "account", None, None, Some("prefix1"));
        let profile2 = create_profile("filesystem", "account", None, None, Some("prefix2"));

        // These don't overlap as neither is a prefix of the other
        assert!(!profile1.is_overlapping_location(&profile2));
    }

    #[test]
    fn test_overlapping_both_no_key_prefix() {
        let profile1 = create_profile("filesystem", "account", None, None, None);
        let profile2 = create_profile("filesystem", "account", None, None, None);

        assert!(profile1.is_overlapping_location(&profile2));
    }

    #[test]
    fn test_complex_key_prefix_scenarios() {
        // Prefix with similar characters but not a prefix relationship
        let profile1 = create_profile("filesystem", "account", None, None, Some("prefix"));
        let profile2 = create_profile("filesystem", "account", None, None, Some("prefix-extra"));

        // Not overlapping since "prefix" is not a prefix of "prefix-extra"
        assert!(!profile1.is_overlapping_location(&profile2));

        // Actual prefix case
        let profile3 = create_profile("filesystem", "account", None, None, Some("prefix"));
        let profile4 = create_profile("filesystem", "account", None, None, Some("prefix/sub"));

        assert!(profile3.is_overlapping_location(&profile4));
    }
}
