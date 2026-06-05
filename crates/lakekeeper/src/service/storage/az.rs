use std::{
    collections::HashMap,
    str::FromStr,
    sync::LazyLock,
    time::{Duration, Instant},
};

use azure_core::{FixedRetryOptions, RetryOptions, TransportOptions};
use azure_identity::{
    DefaultAzureCredential, DefaultAzureCredentialBuilder, TokenCredentialOptions,
};
use azure_storage::{
    CloudLocation, StorageCredentials,
    prelude::{BlobSasPermissions, BlobSignedResource},
    shared_access_signature::{
        SasToken,
        service_sas::{BlobSharedAccessSignature, SasKey},
    },
};
use azure_storage_blobs::prelude::{BlobServiceClient, ClientBuilder};
use iceberg_ext::configs::table::{TableProperties, adls, creds, custom};
use lakekeeper_io::{
    InvalidLocationError, Location,
    adls::{
        AdlsLocation, AdlsStorage, AzureAuth, AzureClientCredentialsAuth, AzureCloud,
        AzureSettings, AzureSharedAccessKeyAuth, DEFAULT_DFS_HOST, normalize_host,
        validate_account_name, validate_filesystem_name,
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
                CredentialsError, InvalidProfileError, TableConfigError, UpdateError,
                ValidationError,
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
    /// **Deprecated.** No longer accepted on profile create / update.
    /// Existing profiles continue to work with a deprecation warning. A
    /// future release will reject the field on load and discard the value.
    #[cfg_attr(feature = "open-api", schema(deprecated = true))]
    pub authority_host: Option<Url>,
    /// **Deprecated.** No longer accepted on profile create / update.
    /// Existing profiles continue to work with a deprecation warning. A
    /// future release will reject the field on load and discard the value.
    #[cfg_attr(feature = "open-api", schema(deprecated = true))]
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

static DEFAULT_AUTHORITY_HOST: LazyLock<Url> = LazyLock::new(|| {
    Url::parse("https://login.microsoftonline.com").expect("Default authority host is a valid URL")
});

/// Shared `reqwest` client for legacy Azure SDK requests. The legacy SDK
/// wants an `Arc<dyn HttpClient>`; we wrap a single `reqwest::Client` so the
/// vending path doesn't open a fresh TCP pool per request.
static HTTP_CLIENT_ARC: LazyLock<std::sync::Arc<reqwest::Client>> =
    LazyLock::new(|| std::sync::Arc::new(reqwest::Client::new()));

static DEFAULT_LEGACY_CLIENT_OPTIONS: LazyLock<azure_core::ClientOptions> = LazyLock::new(|| {
    azure_core::ClientOptions::default().retry(RetryOptions::fixed(
        FixedRetryOptions::default()
            .max_retries(3u32)
            .max_total_elapsed(std::time::Duration::from_secs(5)),
    ))
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
    /// - Fails if `host` or `authority_host` is set (both deprecated).
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
        // Reject new profiles that set the deprecated `host` /
        // `authority_host` fields. Existing rows already in the database
        // still deserialize — this guard only fires when an operator
        // creates or updates a profile via the management API. See
        // `azure_settings` for the back-compat warn-on-load path. A future
        // release will graduate this behavior to "reject on load and
        // discard the value".
        if self.host.is_some() {
            return Err(InvalidProfileError {
                source: None,
                reason: "The `host` field is deprecated and no longer accepted for \
                         new profiles."
                    .to_string(),
                entity: "host".to_string(),
            }
            .into());
        }
        if self.authority_host.is_some() {
            return Err(InvalidProfileError {
                source: None,
                reason: "The `authority_host` field is deprecated and no longer \
                         accepted for new profiles."
                    .to_string(),
                entity: "authority_host".to_string(),
            }
            .into());
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
    /// `host` and `authority_host` are not immutability-checked here — they
    /// are deprecated and `normalize` rejects any non-None value on update,
    /// so the only way past that gate is a `None`-ward update (the
    /// migration path off the deprecated fields). Without this relaxation
    /// an operator with a legacy profile would be unable to set the fields
    /// to `None`.
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
                self.host.as_deref().unwrap_or(DEFAULT_DFS_HOST),
                key_prefix.trim_matches('/')
            )
        } else {
            format!(
                "abfss://{}@{}.{}/",
                self.filesystem,
                self.account_name,
                self.host
                    .as_deref()
                    .unwrap_or(DEFAULT_DFS_HOST)
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

    /// Build the legacy `CloudLocation` from this profile. Used only for
    /// SAS minting via the deprecated `azure_storage_blobs` SDK (vending
    /// path). The data-plane `AdlsStorage` now uses [`AzureHost`].
    fn cloud_location(&self) -> CloudLocation {
        if let Some(host) = &self.host {
            CloudLocation::Custom {
                account: self.account_name.clone(),
                uri: host.clone(),
            }
        } else {
            CloudLocation::Public {
                account: self.account_name.clone(),
            }
        }
    }

    /// Build the new `AzureSettings` (object_store-based) used to construct
    /// [`AdlsStorage`].
    ///
    /// `AdlsProfile.host` is a host *suffix* — every `abfss://` URL is built
    /// as `{account_name}.{host}` (see `base_location`). `AzureCloud::Custom`
    /// expects the *full* blob host (its `endpoint_url` prepends `https://`
    /// and rewrites `.dfs.` → `.blob.` on whatever string it carries), so we
    /// compose the full host here. Skipping the prefix routes PUTs at
    /// `https://{host}` with the account name missing — for a private-link
    /// profile (`host = privatelink.dfs.core.windows.net`) that lands at
    /// `https://privatelink.blob.core.windows.net/...` which does not resolve.
    fn azure_settings(&self) -> AzureSettings {
        // Back-compat path: existing rows persisted before the `host` /
        // `authority_host` deprecation continue to drive endpoint and
        // authority selection. New profiles cannot reach this branch
        // because `normalize` rejects them on create / update. A future
        // release will discard these values entirely; the deprecation is
        // surfaced via the OpenAPI schema, the field doc comments, and the
        // release notes rather than via runtime logging (this method is
        // called per request, and a runtime warn would either spam or
        // require per-account state to deduplicate).
        let cloud = match &self.host {
            Some(h) => AzureCloud::Custom(format!("{}.{}", self.account_name, h)),
            None => AzureCloud::Public,
        };
        AzureSettings {
            authority_host: self.authority_host.clone(),
            account_name: self.account_name.clone(),
            cloud,
        }
    }

    // Kept `async` for symmetry with sibling backends' `lakekeeper_io`
    // chain; the body itself is currently synchronous because the legacy
    // SDK builds are sync.
    #[allow(clippy::unused_async)]
    async fn blob_service_client(
        &self,
        credential: &AzCredential,
    ) -> Result<BlobServiceClient, CredentialsError> {
        let azure_auth = AzureAuth::try_from(credential.clone())?;
        let storage_credentials = legacy_storage_credentials(
            &azure_auth,
            &self.account_name,
            self.authority_host.clone(),
        )?;

        Ok(
            ClientBuilder::with_location(self.cloud_location(), storage_credentials)
                .transport(TransportOptions::new(HTTP_CLIENT_ARC.clone()))
                .client_options(DEFAULT_LEGACY_CLIENT_OPTIONS.clone())
                .blob_service_client(),
        )
    }

    /// Get the Lakekeeper IO for this storage profile.
    ///
    /// # Errors
    /// - If system identity is requested but not enabled in the configuration.
    /// - If the client could not be initialized.
    // Kept `async` for symmetry with sibling backends' `lakekeeper_io`
    // — `S3Profile::lakekeeper_io` and `GcsProfile::lakekeeper_io` both
    // await internally. ADLS construction is now synchronous.
    #[allow(clippy::unused_async)]
    pub async fn lakekeeper_io(
        &self,
        credential: &AzCredential,
    ) -> Result<AdlsStorage, CredentialsError> {
        let azure_auth = AzureAuth::try_from(credential.clone())?;
        Ok(self.azure_settings().get_storage_client(&azure_auth))
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
                    let client = self.blob_service_client(credential).await?;
                    self.sas_via_delegation_key(
                        sas_token_start,
                        requested_sas_token_end,
                        &stc_request,
                        client,
                    )
                    .await?
                }
                AzCredential::SharedAccessKey { key } => {
                    let sas = self.sas(
                        &stc_request,
                        requested_sas_token_end,
                        azure_core::auth::Secret::new(key.clone()),
                    )?;
                    (sas, requested_sas_token_end)
                }
                AzCredential::AzureSystemIdentity {} => {
                    let client = self.blob_service_client(credential).await?;
                    self.sas_via_delegation_key(
                        sas_token_start,
                        requested_sas_token_end,
                        &stc_request,
                        client,
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
        client: BlobServiceClient,
    ) -> Result<(String, OffsetDateTime), CredentialsError> {
        tracing::debug!(
            "Requesting user delegation key from azure for sas token generation - Valid from {sas_token_start} to {sas_token_end}",
        );
        let delegation_key = client
            .get_user_deligation_key(sas_token_start, sas_token_end)
            .await
            .map_err(|e| CredentialsError::ShortTermCredential {
                reason: "Error getting azure user delegation key.".to_string(),
                source: Some(Box::new(e)),
            })?;
        let signed_expiry = delegation_key.user_deligation_key.signed_expiry;

        tracing::debug!(
            "Successfully obtained user delegation key from azure for sas token generation - Valid from {} until {signed_expiry}",
            delegation_key.user_deligation_key.signed_start
        );

        let key = delegation_key.user_deligation_key;

        let sas = self.sas(stc_request, signed_expiry, key)?;
        Ok((sas, signed_expiry))
    }

    fn sas(
        &self,
        stc_request: &ShortTermCredentialsRequest,
        signed_expiry: OffsetDateTime,
        key: impl Into<SasKey>,
    ) -> Result<String, CredentialsError> {
        let path = reduce_scheme_string(stc_request.table_location.as_ref()).map_err(|e| {
            CredentialsError::ShortTermCredential {
                reason: format!("Invalid ADLS location for SAS signing: {e}"),
                source: Some(Box::new(e)),
            }
        })?;
        let rootless_path = path.trim_start_matches('/').trim_end_matches('/');
        let depth = rootless_path.split('/').count();

        // Azure recomputes the canonical-resource by URL-decoding the request
        // URL path. Hand-rolling the canonical with the encoded form (e.g.
        // literal `%3F` or `%20`) produces a signature mismatch.
        let decoded_path = percent_encoding::percent_decode_str(rootless_path).decode_utf8_lossy();
        let canonical_resource = format!(
            "/blob/{}/{}/{}",
            self.account_name.as_str(),
            self.filesystem.as_str(),
            decoded_path
        );

        tracing::debug!(
            "Generationg SAS token for resource `{canonical_resource}` with permissions {} valid until {signed_expiry}",
            stc_request.storage_permissions
        );

        let sas = BlobSharedAccessSignature::new(
            key,
            canonical_resource,
            stc_request.storage_permissions.into(),
            signed_expiry,
            BlobSignedResource::Directory,
        )
        .signed_directory_depth(depth);

        sas.token()
            .map_err(|e| CredentialsError::ShortTermCredential {
                reason: "Error getting azure sas token.".to_string(),
                source: Some(Box::new(e)),
            })
    }

    fn iceberg_sas_property_key(&self) -> String {
        iceberg_sas_property_key(
            &self.account_name,
            self.host.as_deref().unwrap_or(DEFAULT_DFS_HOST),
        )
    }

    fn iceberg_sas_expires_at_property_key(&self) -> String {
        iceberg_expiration_property_key(
            &self.account_name,
            self.host.as_deref().unwrap_or(DEFAULT_DFS_HOST),
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
/// Reduce an ADLS URL to its rooted blob path (`/<blob_name>`).
pub(crate) fn reduce_scheme_string(path: &str) -> Result<String, InvalidLocationError> {
    let l = AdlsLocation::try_from_str(path, true)?;
    Ok(format!("/{}", l.blob_name().trim_start_matches('/')))
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

impl From<StoragePermissions> for BlobSasPermissions {
    fn from(value: StoragePermissions) -> Self {
        match value {
            StoragePermissions::Read => BlobSasPermissions {
                read: true,
                list: true,
                ..Default::default()
            },
            StoragePermissions::ReadWrite => BlobSasPermissions {
                read: true,
                write: true,
                tags: true,
                add: true,
                list: true,
                ..Default::default()
            },
            StoragePermissions::ReadWriteDelete => BlobSasPermissions {
                read: true,
                write: true,
                tags: true,
                add: true,
                delete: true,
                list: true,
                delete_version: true,
                permanent_delete: true,
                ..Default::default()
            },
        }
    }
}

fn iceberg_sas_property_key(account_name: &str, endpoint_suffix: &str) -> String {
    format!("adls.sas-token.{account_name}.{endpoint_suffix}")
}

fn iceberg_expiration_property_key(account_name: &str, endpoint_suffix: &str) -> String {
    format!("adls.sas-token-expires-at-ms.{account_name}.{endpoint_suffix}")
}

/// Build an `AdlsStorage` client from vended-credentials properties.
///
/// Reads the SAS token (under the profile's account/endpoint-specific key)
/// from the iceberg-format `TableProperties` previously produced by
/// `generate_table_config`.
///
/// `profile` is required because the SAS-token property key embeds the storage
/// account name and endpoint host (`adls.sas-token.<account>.<endpoint>`); it
/// cannot be derived from `TableProperties` alone. Do not "normalize" this
/// signature with the S3/GCS counterparts — removing `profile` will break the
/// key lookup.
// Kept `async` for symmetry with the sibling backends' counterparts
// (`s3::lakekeeper_io_from_vended_table_config`,
// `gcs::lakekeeper_io_from_vended_table_config`), which the dispatch in
// `service/storage/mod.rs` calls in a uniform `.await` pattern.
#[allow(clippy::unused_async)]
pub(super) async fn lakekeeper_io_from_vended_table_config(
    profile: &AdlsProfile,
    config: &TableProperties,
) -> Result<AdlsStorage, CredentialsError> {
    let sas_key = profile.iceberg_sas_property_key();
    let sas_token =
        config
            .get_custom_prop(&sas_key)
            .ok_or_else(|| CredentialsError::ShortTermCredential {
                reason: format!(
                    "ADLS vended credentials are missing SAS token at key '{sas_key}'."
                ),
                source: None,
            })?;
    let auth = AzureAuth::Sas(lakekeeper_io::adls::AzureSasAuth { sas_token });
    Ok(profile.azure_settings().get_storage_client(&auth))
}

/// Build `azure_storage::StorageCredentials` for the legacy
/// `BlobServiceClient` from a Lakekeeper `AzureAuth`. The data-plane
/// `AdlsStorage` does not use this — it uses `object_store`'s own credential
/// providers — but the SAS-minting vending path still needs to call
/// `get_user_delegation_key` through the deprecated SDK until vending is
/// migrated in a follow-up task.
fn legacy_storage_credentials(
    auth: &AzureAuth,
    account_name: &str,
    authority_host: Option<Url>,
) -> Result<StorageCredentials, CredentialsError> {
    Ok(match auth {
        AzureAuth::ClientCredentials(AzureClientCredentialsAuth {
            tenant_id,
            client_id,
            client_secret,
        }) => {
            let client_credential = azure_identity::ClientSecretCredential::new(
                HTTP_CLIENT_ARC.clone(),
                authority_host.unwrap_or_else(|| DEFAULT_AUTHORITY_HOST.clone()),
                tenant_id.clone(),
                client_id.clone(),
                client_secret.clone(),
            );
            StorageCredentials::token_credential(std::sync::Arc::new(client_credential))
        }
        AzureAuth::SharedAccessKey(AzureSharedAccessKeyAuth { key }) => {
            StorageCredentials::access_key(account_name, key.clone())
        }
        AzureAuth::AzureSystemIdentity => {
            let authority_host_str = authority_host
                .as_ref()
                .map_or(DEFAULT_AUTHORITY_HOST.to_string(), ToString::to_string);
            let mut options = TokenCredentialOptions::default();
            options.set_authority_host(authority_host_str);
            let credential: std::sync::Arc<DefaultAzureCredential> = std::sync::Arc::new(
                DefaultAzureCredentialBuilder::new()
                    .with_options(options)
                    .build()
                    .map_err(|e| CredentialsError::ShortTermCredential {
                        reason: format!("Failed to build DefaultAzureCredential: {e}"),
                        source: Some(Box::new(e)),
                    })?,
            );
            StorageCredentials::token_credential(credential)
        }
        AzureAuth::Sas(lakekeeper_io::adls::AzureSasAuth { sas_token }) => {
            StorageCredentials::sas_token(sas_token).map_err(|e| {
                CredentialsError::Misconfiguration(format!("Invalid SAS token: {e}"))
            })?
        }
    })
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
        let path = "abfss://filesystem@dfs.windows.net/path/_test";
        assert_eq!(reduce_scheme_string(path).unwrap(), "/path/_test");

        let wasbs_path = "wasbs://filesystem@account.windows.net/path/to/data";
        assert_eq!(reduce_scheme_string(wasbs_path).unwrap(), "/path/to/data");

        // Non-ADLS scheme must error rather than silently pass through.
        let non_matching = "http://example.com/path";
        assert!(reduce_scheme_string(non_matching).is_err());
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

    /// Back-compat regression: when an existing row carries a non-None
    /// `host` suffix, `azure_settings` must compose the full host as
    /// `{account_name}.{host}` (the `account_name` is concatenated
    /// separately for `abfss://` URLs but `AzureCloud::Custom` needs the
    /// full blob host, since its `endpoint_url` translates `.dfs.` →
    /// `.blob.` and prepends `https://`). An earlier bug passed the suffix
    /// through verbatim, dropping the account name on the wire. Pin the
    /// composition so the back-compat path cannot silently regress while
    /// `host` is still being honored. This test bypasses `normalize` (which
    /// now rejects non-None `host`); it models the on-disk shape of a row
    /// persisted before the deprecation.
    #[test]
    fn azure_settings_composes_full_host_back_compat() {
        let profile = AdlsProfile {
            filesystem: "test".to_string(),
            key_prefix: None,
            account_name: "sbrvakpe".to_string(),
            authority_host: None,
            host: Some("custom.example.net".to_string()),
            sas_token_validity_seconds: None,
            allow_alternative_protocols: false,
            sas_enabled: true,
            storage_layout: None,
        };
        assert_eq!(
            profile.azure_settings().cloud,
            AzureCloud::Custom("sbrvakpe.custom.example.net".to_string()),
        );

        let mut public_profile = profile.clone();
        public_profile.host = None;
        assert_eq!(public_profile.azure_settings().cloud, AzureCloud::Public);
    }

    /// `normalize` rejects non-None `host` on profile create / update.
    /// Existing rows that already have a value bypass `normalize` (they
    /// deserialize directly) and go through the back-compat warn path in
    /// `azure_settings` — see [`azure_settings_composes_full_host_back_compat`].
    #[test]
    fn normalize_rejects_deprecated_host() {
        let mut profile = AdlsProfile {
            filesystem: "filesystem".to_string(),
            key_prefix: None,
            account_name: "account".to_string(),
            authority_host: None,
            host: Some("custom.example.net".to_string()),
            sas_token_validity_seconds: None,
            allow_alternative_protocols: false,
            sas_enabled: true,
            storage_layout: None,
        };
        let err = profile
            .normalize()
            .expect_err("non-None host must be rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("`host`"),
            "error must call out the `host` field: got {msg}",
        );
    }

    /// Symmetric to [`normalize_rejects_deprecated_host`] for
    /// `authority_host`.
    #[test]
    fn normalize_rejects_deprecated_authority_host() {
        let mut profile = AdlsProfile {
            filesystem: "filesystem".to_string(),
            key_prefix: None,
            account_name: "account".to_string(),
            authority_host: Some(
                Url::parse("https://login.microsoftonline.us").expect("valid url"),
            ),
            host: None,
            sas_token_validity_seconds: None,
            allow_alternative_protocols: false,
            sas_enabled: true,
            storage_layout: None,
        };
        let err = profile
            .normalize()
            .expect_err("non-None authority_host must be rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("`authority_host`"),
            "error must call out the `authority_host` field: got {msg}",
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
