use std::{
    collections::HashMap,
    str::FromStr,
    sync::{Arc, LazyLock},
};

use azure_core::{FixedRetryOptions, RetryOptions, TransportOptions};
use azure_identity::{
    DefaultAzureCredential, DefaultAzureCredentialBuilder, TokenCredentialOptions,
};
use azure_storage::{
    prelude::{BlobSasPermissions, BlobSignedResource},
    shared_access_signature::{
        service_sas::{BlobSharedAccessSignature, SasKey},
        SasToken,
    },
    StorageCredentials,
};
use azure_storage_blobs::prelude::BlobServiceClient;
use iceberg::io::AzdlsConfigKeys;
use iceberg_ext::configs::{
    table::{custom, TableProperties},
    Location,
};
use lazy_regex::Regex;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use url::{Host, Url};
use veil::Redact;

use crate::{
    api::{
        iceberg::{supported_endpoints, v1::DataAccess},
        CatalogConfig, Result,
    },
    service::storage::{
        error::{CredentialsError, FileIoError, TableConfigError, UpdateError, ValidationError},
        StoragePermissions, StorageType, TableConfig,
    },
    WarehouseIdent, CONFIG,
};

#[derive(Debug, Eq, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct AdlsProfile {
    /// Name of the adls filesystem, in blobstorage also known as container.
    pub filesystem: String,
    /// Subpath in the filesystem to use.
    /// The same prefix can be used for multiple warehouses.
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
}

const DEFAULT_HOST: &str = "dfs.core.windows.net";
static DEFAULT_AUTHORITY_HOST: LazyLock<Url> = LazyLock::new(|| {
    Url::parse("https://login.microsoftonline.com").expect("Default authority host is a valid URL")
});

static HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(reqwest::Client::new);
static HTTP_CLIENT_ARC: LazyLock<Arc<reqwest::Client>> =
    LazyLock::new(|| Arc::new(HTTP_CLIENT.clone()));

const MAX_SAS_TOKEN_VALIDITY_SECONDS: u64 = 7 * 24 * 60 * 60;
const MAX_SAS_TOKEN_VALIDITY_SECONDS_I64: i64 = 7 * 24 * 60 * 60;

pub(crate) const ALTERNATIVE_PROTOCOLS: [&str; 1] = ["wasbs"];
static ADLS_PATH_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new("^(?<protocol>(abfss|wasbs)?://)[^/@]+@[^/]+(?<path>/.+)")
        .expect("ADLS path regex is valid")
});

static SYSTEM_IDENTITY_CACHE: LazyLock<moka::sync::Cache<String, Arc<DefaultAzureCredential>>> =
    LazyLock::new(|| moka::sync::Cache::builder().max_capacity(1000).build());

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
                return Err(ValidationError::InvalidProfile {
                    source: None,
                    reason: "SAS token can be only valid up to a week.".to_string(),
                    entity: "sas-token-validity-seconds".to_string(),
                });
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
    pub fn update_with(self, other: Self) -> Result<Self, UpdateError> {
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

        Ok(other)
    }

    // may change..
    #[allow(clippy::unused_self)]
    #[must_use]
    pub fn generate_catalog_config(&self, _: WarehouseIdent) -> CatalogConfig {
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
    pub fn base_location(&self) -> Result<Location, ValidationError> {
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
        let location =
            Location::from_str(&location).map_err(|e| ValidationError::InvalidLocation {
                source: Some(Box::new(e)),
                reason: "Failed to create location for storage profile.".to_string(),
                storage_type: StorageType::Adls,
                location,
            })?;

        Ok(location)
    }

    /// Generate the table configuration for Azure Datalake Storage Gen2.
    ///
    /// # Errors
    /// Fails if sas token cannot be generated.
    pub async fn generate_table_config(
        &self,
        _: DataAccess,
        table_location: &Location,
        credential: &AzCredential,
        permissions: StoragePermissions,
    ) -> Result<TableConfig, TableConfigError> {
        let sas = match credential {
            AzCredential::ClientCredentials {
                client_id,
                tenant_id,
                client_secret,
            } => {
                let token = azure_identity::ClientSecretCredential::new(
                    HTTP_CLIENT_ARC.clone(),
                    self.authority_host
                        .clone()
                        .unwrap_or(DEFAULT_AUTHORITY_HOST.clone()),
                    tenant_id.clone(),
                    client_id.clone(),
                    client_secret.clone(),
                );

                self.sas_via_delegation_key(
                    table_location,
                    StorageCredentials::token_credential(Arc::new(token)),
                    permissions,
                )
                .await?
            }
            AzCredential::SharedAccessKey { key } => self.sas(
                table_location,
                permissions,
                OffsetDateTime::now_utc()
                    .saturating_sub(time::Duration::minutes(5))
                    .saturating_add(time::Duration::days(7)),
                azure_core::auth::Secret::new(key.to_string()),
            )?,
            AzCredential::AzureSystemIdentity {} => {
                let identity: Arc<DefaultAzureCredential> = self.get_system_identity()?;
                self.sas_via_delegation_key(
                    table_location,
                    StorageCredentials::token_credential(identity),
                    permissions,
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

        let mut creds = TableProperties::default();

        creds.insert(&custom::CustomConfig {
            key: self.iceberg_sas_property_key(),
            value: sas,
        });

        Ok(TableConfig {
            // Due to backwards compat reasons we still return creds within config too
            config: creds.clone(),
            creds,
        })
    }

    fn get_system_identity(&self) -> Result<Arc<DefaultAzureCredential>, CredentialsError> {
        if !CONFIG.enable_azure_system_credentials {
            return Err(CredentialsError::Misconfiguration(
                "Azure System identity credentials are disabled in this Lakekeeper deployment."
                    .to_string(),
            ));
        }

        let authority_host_str = (self.authority_host.as_ref()).map_or(
            DEFAULT_AUTHORITY_HOST.clone().to_string(),
            std::string::ToString::to_string,
        );

        SYSTEM_IDENTITY_CACHE
            .try_get_with(authority_host_str.clone(), || {
                let mut options = TokenCredentialOptions::default();
                options.set_authority_host(authority_host_str);
                DefaultAzureCredentialBuilder::new()
                    .with_options(options)
                    .build()
                    .map(Arc::new)
            })
            .map_err(|e| {
                tracing::error!("Failed to get Azure system identity: {e}");
                CredentialsError::ShortTermCredential {
                    reason: "Failed to get Azure system identity".to_string(),
                    source: Some(Box::new(e)),
                }
            })
    }

    /// Create a new `FileIO` instance for Adls.
    ///
    /// # Errors
    /// Fails if the `FileIO` instance cannot be created.
    pub async fn file_io(
        &self,
        credential: &AzCredential,
    ) -> Result<iceberg::io::FileIO, FileIoError> {
        let mut builder = iceberg::io::FileIOBuilder::new("azdls").with_client(HTTP_CLIENT.clone());

        builder = builder
            .with_prop(
                AzdlsConfigKeys::Endpoint,
                format!(
                    "https://{}.{}",
                    self.account_name,
                    self.host.as_deref().unwrap_or(DEFAULT_HOST)
                ),
            )
            .with_prop(AzdlsConfigKeys::AccountName, self.account_name.clone())
            .with_prop(AzdlsConfigKeys::Filesystem, self.filesystem.clone())
            .with_client(HTTP_CLIENT.clone());

        if let Some(authority_host) = &self.authority_host {
            builder = builder.with_prop(AzdlsConfigKeys::AuthorityHost, authority_host.to_string());
        }

        match credential {
            AzCredential::ClientCredentials {
                client_id,
                tenant_id,
                client_secret,
            } => {
                builder = builder
                    .with_prop(
                        AzdlsConfigKeys::ClientSecret.to_string(),
                        client_secret.to_string(),
                    )
                    .with_prop(AzdlsConfigKeys::ClientId, client_id.to_string())
                    .with_prop(AzdlsConfigKeys::TenantId, tenant_id.to_string());
            }
            AzCredential::SharedAccessKey { key } => {
                builder = builder.with_prop(AzdlsConfigKeys::AccountKey, key.to_string());
            }
            AzCredential::AzureSystemIdentity {} => {
                // ToDo: Use azure_identity to get token, then pass it to FileIO.
                // As of writing this is not supported in OpenDAL and iceberg-rust.
                if !CONFIG.enable_azure_system_credentials {
                    return Err(CredentialsError::Misconfiguration(
                        "Azure System identity credentials are disabled in this Lakekeeper deployment.".to_string(),
                    ).into());
                }
                let table_config = self
                    .generate_table_config(
                        DataAccess {
                            vended_credentials: true,
                            remote_signing: false,
                        },
                        self.base_location()
                            .map_err(|e| CredentialsError::ShortTermCredential {
                                reason: "Failed to get base location for storage profile"
                                    .to_string(),
                                source: Some(Box::new(e)),
                            })?
                            .without_trailing_slash(),
                        credential,
                        StoragePermissions::ReadWriteDelete,
                    )
                    .await
                    .map_err(|e| CredentialsError::ShortTermCredential {
                        reason: e.to_string(),
                        source: Some(Box::new(e)),
                    })?;

                builder = builder
                    .with_props(table_config.config.inner())
                    .with_prop(AzdlsConfigKeys::Filesystem, self.filesystem.to_string());
            }
        }

        Ok(builder.build()?)
    }

    async fn sas_via_delegation_key(
        &self,
        path: &Location,
        cred: StorageCredentials,
        permissions: StoragePermissions,
    ) -> Result<String, CredentialsError> {
        let client = blob_service_client(self.account_name.as_str(), cred);

        // allow for some clock drift
        let start = time::OffsetDateTime::now_utc() - time::Duration::minutes(5);
        let max_validity_seconds = MAX_SAS_TOKEN_VALIDITY_SECONDS_I64;
        // account for the 5 minutes offset from above
        let sas_token_validity_seconds = self.sas_token_validity_seconds.unwrap_or(3600) + 300;
        let clamped_validity_seconds = i64::try_from(sas_token_validity_seconds)
            .unwrap_or(max_validity_seconds)
            .clamp(0, max_validity_seconds);

        let delegation_key = client
            .get_user_deligation_key(
                start,
                start
                    .checked_add(time::Duration::seconds(clamped_validity_seconds))
                    .ok_or(CredentialsError::ShortTermCredential {
                        reason: format!(
                            "SAS expiry overflow: Cannot issue a token valid for {clamped_validity_seconds} seconds",
                        )
                            .to_string(),
                        source: None,
                    })?,
            )
            .await
            .map_err(|e| CredentialsError::ShortTermCredential {
                reason: "Error getting azure user delegation key.".to_string(),
                source: Some(Box::new(e)),
            })?;
        let signed_expiry = delegation_key.user_deligation_key.signed_expiry;
        let key = delegation_key.user_deligation_key.clone();

        self.sas(path, permissions, signed_expiry, key)
    }

    fn sas(
        &self,
        path: &Location,
        permissions: StoragePermissions,
        signed_expiry: OffsetDateTime,
        key: impl Into<SasKey>,
    ) -> Result<String, CredentialsError> {
        let path = reduce_scheme_string(&path.to_string(), true);
        let rootless_path = path.trim_start_matches('/');
        let depth = rootless_path.split('/').count();
        let canonical_resource = format!(
            "/blob/{}/{}/{}",
            self.account_name.as_str(),
            self.filesystem.as_str(),
            rootless_path
        );

        println!("canonical_resource: {canonical_resource:?}");

        let sas = BlobSharedAccessSignature::new(
            key,
            canonical_resource,
            permissions.into(),
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
            self.host.as_ref().unwrap_or(&DEFAULT_HOST.to_string()),
        )
    }

    fn normalize_key_prefix(&mut self) -> Result<(), ValidationError> {
        if let Some(key_prefix) = self.key_prefix.as_mut() {
            *key_prefix = key_prefix.trim_matches('/').to_string();
        }

        if let Some(key_prefix) = self.key_prefix.as_ref() {
            if key_prefix.is_empty() {
                self.key_prefix = None;
            }
        }

        // Azure supports a max of 1024 chars and we need some buffer for tables.
        if let Some(key_prefix) = &self.key_prefix {
            if key_prefix.len() > 512 {
                return Err(ValidationError::InvalidProfile {
                    source: None,

                    reason: "Storage Profile `key-prefix` must be less than 512 characters."
                        .to_string(),
                    entity: "key-prefix".to_string(),
                });
            }
            for key in key_prefix.split('/') {
                validate_path_segment(key)?;
            }
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

#[derive(Debug, Clone, PartialEq)]
pub struct AdlsLocation {
    account_name: String,
    filesystem: String,
    endpoint_suffix: String,
    key: Vec<String>,
    // Redundant, but useful for failsafe access
    location: Location,
    custom_prefix: Option<String>,
}

impl AdlsLocation {
    /// Create a new [`AdlsLocation`] from the given parameters.
    ///
    /// # Errors
    /// Fails if validation of account name, filesystem name or key fails.
    pub fn new(
        account_name: String,
        filesystem: String,
        host: String,
        key: Vec<String>,
        custom_prefix: Option<String>,
    ) -> Result<Self, ValidationError> {
        validate_filesystem_name(&filesystem)?;
        validate_account_name(&account_name)?;
        for k in &key {
            validate_path_segment(k)?;
        }

        let endpoint_suffix = normalize_host(host)?.unwrap_or(DEFAULT_HOST.to_string());

        let location = format!("abfss://{filesystem}@{account_name}.{endpoint_suffix}",);
        let mut location =
            Location::from_str(&location).map_err(|e| ValidationError::InvalidLocation {
                source: Some(Box::new(e)),
                reason: "Invalid adls location".to_string(),
                storage_type: StorageType::Adls,
                location,
            })?;
        if !key.is_empty() {
            location.without_trailing_slash().extend(key.iter());
        }

        Ok(Self {
            account_name,
            filesystem,
            endpoint_suffix,
            key,
            location,
            custom_prefix,
        })
    }

    #[must_use]
    pub fn location(&self) -> &Location {
        &self.location
    }

    #[must_use]
    pub fn account_name(&self) -> &str {
        &self.account_name
    }

    #[must_use]
    pub fn filesystem(&self) -> &str {
        &self.filesystem
    }

    #[must_use]
    pub fn endpoint_suffix(&self) -> &str {
        &self.endpoint_suffix
    }

    /// Create a new `AdlsLocation` from a Location.
    ///
    /// If `allow_variants` is set to true, `wasbs://` schemes are allowed.
    ///
    /// # Errors
    /// - Fails if the location is not a valid ADLS location
    pub fn try_from_location(
        location: &Location,
        allow_variants: bool,
    ) -> Result<Self, ValidationError> {
        let schema = location.url().scheme();
        let is_custom_variant = ALTERNATIVE_PROTOCOLS.contains(&schema);

        // Protocol must be abfss or wasbs (if allowed)
        if schema != "abfss" && !(allow_variants && is_custom_variant) {
            let reason = if allow_variants {
                format!(
                    "ADLS location must use abfss or wasbs protocol. Found: {}",
                    location.url().scheme()
                )
            } else {
                format!(
                    "ADLS location must use abfss protocol. Found: {}",
                    location.url().scheme()
                )
            };

            return Err(ValidationError::InvalidLocation {
                reason,
                location: location.to_string(),
                source: None,
                storage_type: StorageType::Adls,
            });
        }

        let filesystem = location.url().username().to_string();
        let host = location
            .url()
            .host_str()
            .ok_or_else(|| ValidationError::InvalidLocation {
                reason: "ADLS location has no host specified".to_string(),
                location: location.to_string(),
                source: None,
                storage_type: StorageType::Adls,
            })?
            .to_string();
        // Host: account_name.endpoint_suffix
        let (account_name, endpoint_suffix) =
            host.split_once('.')
                .ok_or_else(|| ValidationError::InvalidLocation {
                    reason: "ADLS location host must be in the format <account_name>.<endpoint>. Specified location has no point (.)"
                        .to_string(),
                    location: location.to_string(),
                    source: None,
                    storage_type: StorageType::Adls,
                })?;

        let key: Vec<String> = location
            .url()
            .path_segments()
            .map_or(Vec::new(), |segments| {
                segments.map(std::string::ToString::to_string).collect()
            });

        let custom_prefix = if is_custom_variant {
            Some(schema.to_string())
        } else {
            None
        };

        Self::new(
            account_name.to_string(),
            filesystem,
            endpoint_suffix.to_string(),
            key,
            custom_prefix,
        )
    }

    #[cfg(test)]
    /// Always returns `abfss://` prefixed location.
    pub(crate) fn into_normalized_location(self) -> Location {
        self.location
    }
}

impl From<AdlsLocation> for Location {
    fn from(location: AdlsLocation) -> Location {
        location.location
    }
}

/// Removes the hostname and user from the path.
/// Keeps only the path and optionally the scheme.
#[must_use]
pub(crate) fn reduce_scheme_string(path: &str, only_path: bool) -> String {
    if let Some(caps) = ADLS_PATH_PATTERN.captures(path) {
        let mut location = String::new();
        if only_path {
            caps.expand("$path", &mut location);
        } else {
            caps.expand("$protocol$path", &mut location);
        }
        return location;
    }
    path.to_string()
}

#[derive(Redact, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(tag = "credential-type", rename_all = "kebab-case")]
pub enum AzCredential {
    #[serde(rename_all = "kebab-case")]
    #[schema(title = "AzCredentialClientCredentials")]
    ClientCredentials {
        client_id: String,
        tenant_id: String,
        #[redact(partial)]
        client_secret: String,
    },
    #[serde(rename_all = "kebab-case")]
    #[schema(title = "AzCredentialSharedAccessKey")]
    SharedAccessKey {
        #[redact]
        key: String,
    },
    #[serde(rename_all = "kebab-case")]
    #[schema(title = "AzCredentialManagedIdentity")]
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

pub(super) fn get_file_io_from_table_config(
    config: &TableProperties,
    file_system: String,
) -> Result<iceberg::io::FileIO, FileIoError> {
    Ok(iceberg::io::FileIOBuilder::new("azdls")
        .with_client(HTTP_CLIENT.clone())
        .with_props(config.inner())
        .with_prop(AzdlsConfigKeys::Filesystem, file_system)
        .build()?)
}

fn blob_service_client(account_name: &str, cred: StorageCredentials) -> BlobServiceClient {
    azure_storage_blobs::prelude::BlobServiceClient::builder(account_name, cred)
        .transport(TransportOptions::new(HTTP_CLIENT_ARC.clone()))
        .client_options(
            azure_core::ClientOptions::default().retry(RetryOptions::fixed(
                FixedRetryOptions::default()
                    .max_retries(3u32)
                    .max_total_elapsed(std::time::Duration::from_secs(5)),
            )),
        )
        .blob_service_client()
}

// https://learn.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata
fn validate_filesystem_name(container: &str) -> Result<(), ValidationError> {
    if container.is_empty() {
        return Err(ValidationError::InvalidProfile {
            source: None,
            reason: "`filesystem` must not be empty.".to_string(),
            entity: "FilesystemName".to_string(),
        });
    }

    // Container names must not contain consecutive hyphens.
    if container.contains("--") {
        return Err(ValidationError::InvalidProfile {
            source: None,
            reason: "Filesystem name must not contain consecutive hyphens.".to_string(),
            entity: "FilesystemName".to_string(),
        });
    }

    let container = container.chars().collect::<Vec<char>>();
    // Container names must be between 3 (min) and 63 (max) characters long.
    if container.len() < 3 || container.len() > 63 {
        return Err(ValidationError::InvalidProfile {
            source: None,
            reason: "`filesystem` must be between 3 and 63 characters long.".to_string(),
            entity: "FilesystemName".to_string(),
        });
    }

    // Container names can consist only of lowercase letters, numbers, and hyphens (-).
    if !container
        .iter()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == '-')
    {
        return Err(ValidationError::InvalidProfile {
            source: None,
            reason:
                "Filesystem name can consist only of lowercase letters, numbers, and hyphens (-)."
                    .to_string(),
            entity: "FilesystemName".to_string(),
        });
    }

    // Container names must begin and end with a letter or number.
    // Unwrap will not fail as the length is already checked.
    if !container.first().is_some_and(char::is_ascii_alphanumeric)
        || !container.last().is_some_and(char::is_ascii_alphanumeric)
    {
        return Err(ValidationError::InvalidProfile {
            source: None,
            reason: "Filesystem name must begin and end with a letter or number.".to_string(),
            entity: "FilesystemName".to_string(),
        });
    }

    Ok(())
}

// https://learn.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata
fn validate_path_segment(path_segment: &str) -> Result<(), ValidationError> {
    if path_segment.contains(|c: char| {
        c == ' '
            || c == '!'
            || c == '*'
            || c == '\''
            || c == '('
            || c == ')'
            || c == ';'
            || c == ':'
            || c == '@'
            || c == '&'
            || c == '='
            || c == '+'
            || c == '$'
            || c == ','
            || c == '/'
            || c == '?'
            || c == '%'
            || c == '#'
            || c == '['
            || c == ']'
    }) {
        return Err(ValidationError::InvalidProfile {
            source: None,
            reason:
                "Directory path contains reserved URL characters that are not properly escaped."
                    .to_string(),
            entity: "DirectoryPath".to_string(),
        });
    }

    // Check if the directory name ends with a dot (.), a backslash (\), or a combination of these.
    if path_segment.ends_with('.') || path_segment.ends_with('\\') {
        return Err(ValidationError::InvalidProfile {
            source: None,
            reason: format!(
                "Directory: '{path_segment}' must not end with a dot (.), or a backslash (\\)."
            ),
            entity: "DirectoryPath".to_string(),
        });
    }

    Ok(())
}

fn normalize_host(host: String) -> Result<Option<String>, ValidationError> {
    // If endpoint suffix is Some(""), set it to None.
    if host.is_empty() {
        Ok(None)
    } else {
        // Endpoint suffix must not contain slashes.
        if host.contains('/') {
            return Err(ValidationError::InvalidProfile {
                source: None,
                reason: "`endpoint_suffix` must not contain slashes.".to_string(),
                entity: "EndpointSuffix".to_string(),
            });
        }

        // Endpoint suffix must be a valid hostname
        if Host::parse(&host).is_err() {
            return Err(ValidationError::InvalidProfile {
                source: None,
                reason: "`endpoint_suffix` must be a valid hostname.".to_string(),
                entity: "EndpointSuffix".to_string(),
            });
        }

        Ok(Some(host))
    }
}

// Storage account names must be between 3 and 24 characters in length
// and may contain numbers and lowercase letters only.
fn validate_account_name(account_name: &str) -> Result<(), ValidationError> {
    if account_name.len() < 3 || account_name.len() > 24 {
        return Err(ValidationError::InvalidProfile {
            source: None,
            reason: "`account_name` must be between 3 and 24 characters long.".to_string(),
            entity: "AccountName".to_string(),
        });
    }

    if !account_name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
    {
        return Err(ValidationError::InvalidProfile {
            source: None,
            reason: "`account_name` must contain only lowercase letters and numbers.".to_string(),
            entity: "AccountName".to_string(),
        });
    }

    Ok(())
}

#[cfg(test)]
pub(crate) mod test {
    use std::str::FromStr;

    use iceberg_ext::configs::Location;
    use needs_env_var::needs_env_var;

    use crate::service::{
        storage::{
            az::{
                normalize_host, reduce_scheme_string, validate_account_name,
                validate_filesystem_name, validate_path_segment, DEFAULT_AUTHORITY_HOST,
            },
            AdlsLocation, AdlsProfile, StorageLocations, StorageProfile,
        },
        tabular_idents::TabularIdentUuid,
        NamespaceIdentUuid,
    };

    #[test]
    fn test_reduce_scheme_string() {
        // Test abfss protocol
        let path = "abfss://filesystem@dfs.windows.net/path/_test";
        let reduced_path = reduce_scheme_string(path, true);
        assert_eq!(reduced_path, "/path/_test");

        let reduced_path = reduce_scheme_string(path, false);
        assert_eq!(reduced_path, "abfss:///path/_test");

        // Test wasbs protocol
        let wasbs_path = "wasbs://filesystem@account.windows.net/path/to/data";
        let reduced_wasbs_path = reduce_scheme_string(wasbs_path, true);
        assert_eq!(reduced_wasbs_path, "/path/to/data");

        let reduced_wasbs_path = reduce_scheme_string(wasbs_path, false);
        assert_eq!(reduced_wasbs_path, "wasbs:///path/to/data");

        // Test a non-matching path
        let non_matching = "http://example.com/path";
        assert_eq!(reduce_scheme_string(non_matching, true), non_matching);
        assert_eq!(reduce_scheme_string(non_matching, false), non_matching);
    }

    #[needs_env_var(TEST_AZURE = 1)]
    pub(crate) mod azure_tests {
        use crate::service::storage::{
            AdlsProfile, AzCredential, StorageCredential, StorageProfile,
        };

        pub(crate) fn azure_profile() -> AdlsProfile {
            let account_name = std::env::var("AZURE_STORAGE_ACCOUNT_NAME").unwrap();
            let filesystem = std::env::var("AZURE_STORAGE_FILESYSTEM").unwrap();

            let key_prefix = format!("test-{}", uuid::Uuid::now_v7());
            AdlsProfile {
                filesystem,
                key_prefix: Some(key_prefix.to_string()),
                account_name,
                authority_host: None,
                host: None,
                sas_token_validity_seconds: None,
                allow_alternative_protocols: false,
            }
        }

        pub(crate) fn client_creds() -> AzCredential {
            let client_id = std::env::var("AZURE_CLIENT_ID").unwrap();
            let client_secret = std::env::var("AZURE_CLIENT_SECRET").unwrap();
            let tenant_id = std::env::var("AZURE_TENANT_ID").unwrap();

            AzCredential::ClientCredentials {
                client_id,
                client_secret,
                tenant_id,
            }
        }

        pub(crate) fn shared_key() -> AzCredential {
            let key = std::env::var("AZURE_STORAGE_SHARED_KEY").unwrap();
            AzCredential::SharedAccessKey { key }
        }

        #[tokio::test]
        async fn test_can_validate() {
            let prof = azure_profile();
            let mut prof: StorageProfile = prof.into();
            prof.normalize().expect("failed to validate profile");
            for (cred, typ) in [
                (client_creds(), "client-creds"),
                (shared_key(), "shared-key"),
            ] {
                let cred: StorageCredential = cred.into();
                prof.validate_access(Some(&cred), None)
                    .await
                    .unwrap_or_else(|e| panic!("Failed to validate '{typ}' due to '{e:?}'"));
            }
        }

        #[tokio::test]
        #[needs_env_var::needs_env_var(LAKEKEEPER_TEST__ENABLE_AZURE_SYSTEM_CREDENTIALS = 1)]
        async fn test_system_identity_can_validate() {
            let prof = azure_profile();
            let mut prof: StorageProfile = prof.into();
            prof.normalize().expect("failed to validate profile");
            let cred = AzCredential::AzureSystemIdentity {};
            let cred: StorageCredential = cred.into();
            prof.validate_access(Some(&cred), None)
                .await
                .unwrap_or_else(|e| panic!("Failed to validate system identity due to '{e:?}'"));
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
    fn test_validate_endpoint_suffix() {
        assert_eq!(
            normalize_host("dfs.core.windows.net".to_string()).unwrap(),
            Some("dfs.core.windows.net".to_string())
        );
        assert!(normalize_host(String::new()).unwrap().is_none());
    }

    #[test]
    fn test_valid_account_names() {
        for name in &["abc", "a1b2c3", "abc123", "123abc"] {
            assert!(validate_account_name(name).is_ok(), "{}", name);
        }
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
        };

        let sp: StorageProfile = profile.clone().into();

        let namespace_id = NamespaceIdentUuid::from(uuid::Uuid::now_v7());
        let table_id = TabularIdentUuid::Table(uuid::Uuid::now_v7());
        let namespace_location = sp.default_namespace_location(namespace_id).unwrap();

        let location = sp.default_tabular_location(&namespace_location, table_id);
        assert_eq!(
            location.to_string(),
            format!("abfss://filesystem@account.dfs.core.windows.net/test_prefix/{namespace_id}/{table_id}")
        );

        let mut profile = profile.clone();
        profile.key_prefix = None;
        profile.host = Some("blob.com".to_string());
        let sp: StorageProfile = profile.into();

        let namespace_location = sp.default_namespace_location(namespace_id).unwrap();
        let location = sp.default_tabular_location(&namespace_location, table_id);
        assert_eq!(
            location.to_string(),
            format!("abfss://filesystem@account.blob.com/{namespace_id}/{table_id}")
        );
    }

    #[test]
    fn test_parse_adls_location() {
        let cases = vec![
            (
                "abfss://filesystem@account0name.foo.com",
                "account0name",
                "filesystem",
                "foo.com",
                vec![],
            ),
            (
                "abfss://filesystem@account0name.dfs.core.windows.net/one",
                "account0name",
                "filesystem",
                "dfs.core.windows.net",
                vec!["one"],
            ),
            (
                "abfss://filesystem@account0name.foo.com/one",
                "account0name",
                "filesystem",
                "foo.com",
                vec!["one"],
            ),
        ];

        for (location_str, account_name, filesystem, endpoint_suffix, key) in cases {
            let adls_location =
                AdlsLocation::try_from_location(&Location::from_str(location_str).unwrap(), false)
                    .unwrap();
            assert_eq!(adls_location.account_name(), account_name);
            assert_eq!(adls_location.filesystem(), filesystem);
            assert_eq!(adls_location.endpoint_suffix(), endpoint_suffix);
            assert_eq!(adls_location.key, key);
            // Roundtrip
            assert_eq!(
                adls_location.into_normalized_location().to_string(),
                location_str
            );
        }
    }

    #[test]
    fn test_invalid_adls_location() {
        let cases = vec![
            "abfss://filesystem@account_name",
            "abfss://filesystem@account_name.example.com./foo",
            "s3://filesystem@account_name.dfs.core.windows/foo",
            "abfss://account_name.dfs.core.windows/foo",
        ];

        for location in cases {
            let location = Location::from_str(location).unwrap();
            let parsed_location = AdlsLocation::try_from_location(&location, false);
            assert!(parsed_location.is_err(), "{parsed_location:?}");
        }
    }

    #[test]
    fn test_invalid_account_names() {
        for name in &["Abc", "abc!", "abc.def", "abc_def", "abc/def"] {
            assert!(validate_account_name(name).is_err(), "{}", name);
        }
    }

    #[test]
    fn test_valid_container_names() {
        for name in &[
            "abc", "a1b2c3", "a-b-c", "1-2-3", "a1-b2-c3", "abc123", "123abc",
        ] {
            assert!(validate_filesystem_name(name).is_ok(), "{}", name);
        }
    }

    #[test]
    fn test_invalid_container_length() {
        assert!(validate_filesystem_name("ab").is_err(), "ab");
        assert!(
            validate_filesystem_name(&"a".repeat(64)).is_err(),
            "64 character long string"
        );
    }

    #[test]
    fn test_invalid_container_characters() {
        for name in &[
            "Abc",     // Uppercase letter
            "abc!",    // Special character
            "abc.def", // Dot character
            "abc_def", // Underscore character
        ] {
            assert!(validate_filesystem_name(name).is_err(), "{}", name);
        }
    }

    #[test]
    fn test_invalid_start_end() {
        for name in &[
            "-abc",   // Starts with hyphen
            "abc-",   // Ends with hyphen
            "-abc-",  // Starts and ends with hyphen
            "1-2-3-", // Ends with hyphen
        ] {
            assert!(validate_filesystem_name(name).is_err(), "{}", name);
        }
    }

    #[test]
    fn test_consecutive_hyphens_container_name() {
        for name in &[
            "a--b", // Consecutive hyphens
            "1--2", // Consecutive hyphens
            "a--1", // Consecutive hyphens
        ] {
            assert!(validate_filesystem_name(name).is_err(), "{}", name);
        }
    }

    #[test]
    fn test_valid_directory_paths() {
        for path in &[
            "valid/path",
            "another/valid/path",
            "valid/path/with123",
            "valid/path/with-dash",
            "valid/path/with_underscore",
        ] {
            for segment in path.split('/') {
                assert!(validate_path_segment(segment).is_ok(), "{}", segment);
            }
        }
    }

    #[test]
    fn test_path_reserved_characters() {
        for path in &[
            " path", "path!", "path*", "path'", "path(", "path)", "path;", "path:", "path@",
            "path&", "path=", "path+", "path$", "path,", "path?", "path%", "path#", "path[",
            "path]",
        ] {
            for segment in path.split('/') {
                assert!(validate_path_segment(segment).is_err(), "{}", segment);
            }
        }
    }

    #[test]
    fn test_path_ending_characters() {
        for path in &["path.", "path\\", "path/.", "path/\\"] {
            assert!(validate_path_segment(path).is_err(), "{}", path);
        }
    }

    #[test]
    fn test_normalize_wasbs_location() {
        let location =
            Location::from_str("wasbs://filesystem@account0name.foo.com/path/to/data").unwrap();

        let location = AdlsLocation::try_from_location(&location, true).unwrap();
        assert_eq!(
            location.into_normalized_location().to_string(),
            "abfss://filesystem@account0name.foo.com/path/to/data",
        );
    }

    #[test]
    fn test_parse_wasbs_location() {
        let location = "wasbs://filesystem@account0name.foo.com/path/to/data";

        // Test with allow_variants = true
        let result = AdlsLocation::try_from_location(&Location::from_str(location).unwrap(), true);

        assert!(result.is_ok(), "Should parse with allow_variants = true");
        let adls_location = result.unwrap();

        // Check that it was normalized to abfss
        assert_eq!(
            adls_location.location().url().scheme(),
            "abfss",
            "Protocol should be normalized to abfss"
        );

        // Check that other properties were preserved
        assert_eq!(adls_location.account_name(), "account0name");
        assert_eq!(adls_location.filesystem(), "filesystem");
        assert_eq!(adls_location.endpoint_suffix(), "foo.com");
        assert_eq!(adls_location.key, vec!["path", "to", "data"]);

        // Test with allow_variants = false
        let result = AdlsLocation::try_from_location(&Location::from_str(location).unwrap(), false);
        assert!(result.is_err(), "Should fail with allow_variants = false");
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
