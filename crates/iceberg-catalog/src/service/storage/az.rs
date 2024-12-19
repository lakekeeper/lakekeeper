use crate::WarehouseIdent;

use crate::api::{iceberg::v1::DataAccess, CatalogConfig, Result};
use crate::catalog::io::IoError;
use crate::service::storage::error::{
    CredentialsError, FileIoError, TableConfigError, UpdateError, ValidationError,
};
use crate::service::storage::path_utils::reduce_scheme_string;
use crate::service::storage::{StoragePermissions, StorageProfile, StorageType, TableConfig};
use azure_storage::prelude::{BlobSasPermissions, BlobSignedResource};
use azure_storage::shared_access_signature::service_sas::BlobSharedAccessSignature;
use azure_storage::shared_access_signature::SasToken;
use azure_storage::StorageCredentials;
use futures::StreamExt;

use crate::api::iceberg::supported_endpoints;
use azure_core::{FixedRetryOptions, RetryOptions, TransportOptions};
use azure_storage_blobs::prelude::BlobServiceClient;
use iceberg::io::AzdlsConfigKeys;
use iceberg_ext::configs::{
    table::{custom, TableProperties},
    Location,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, OnceLock};
use url::{Host, Url};
use veil::Redact;

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
}

const DEFAULT_HOST: &str = "dfs.core.windows.net";
lazy_static::lazy_static! {
    static ref DEFAULT_AUTHORITY_HOST: Url = Url::parse("https://login.microsoftonline.com").expect("Default authority host is a valid URL");
}

static STS_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

impl AdlsProfile {
    /// Validate the Azure storage profile.
    ///
    /// # Errors
    /// - Fails if the filesystem name is invalid.
    /// - Fails if the key prefix is too long or invalid.
    /// - Fails if the account name is invalid.
    /// - Fails if the endpoint suffix is invalid.
    pub(super) fn normalize(&mut self) -> Result<(), ValidationError> {
        validate_filesystem_name(&self.filesystem)?;
        self.host = self.host.take().map(normalize_host).transpose()?.flatten();
        self.normalize_key_prefix()?;
        validate_account_name(&self.account_name)?;

        Ok(())
    }

    /// Check if the profile can be updated with the other profile.
    /// `key_prefix`, `region` and `bucket` must be the same.
    /// We enforce this to avoid issues by accidentally changing the bucket or region
    /// of a warehouse, after which all tables would not be accessible anymore.
    /// Changing an endpoint might still result in an invalid profile, but we allow it.
    ///
    /// # Errors
    /// Fails if the `bucket`, `region` or `key_prefix` is different.
    pub fn can_be_updated_with(&self, other: &Self) -> Result<(), UpdateError> {
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

        Ok(())
    }

    // may change..
    #[allow(clippy::unused_self)]
    #[must_use]
    pub fn generate_catalog_config(&self, _: WarehouseIdent) -> CatalogConfig {
        CatalogConfig {
            defaults: HashMap::default(),
            overrides: HashMap::default(),
            endpoints: supported_endpoints(),
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
        _: &DataAccess,
        table_location: &Location,
        creds: &AzCredential,
        permissions: StoragePermissions,
    ) -> Result<TableConfig, TableConfigError> {
        let AzCredential::ClientCredentials {
            client_id,
            tenant_id,
            client_secret,
        } = creds;
        let http_client = STS_CLIENT.get_or_init(reqwest::Client::new).clone();
        let token = azure_identity::ClientSecretCredential::new(
            Arc::new(http_client),
            self.authority_host
                .clone()
                .unwrap_or(DEFAULT_AUTHORITY_HOST.clone()),
            tenant_id.clone(),
            client_id.clone(),
            client_secret.clone(),
        );
        let cred = azure_storage::StorageCredentials::token_credential(Arc::new(token));
        let mut creds = TableProperties::default();

        let sas = self
            .get_sas_token(table_location, cred, permissions)
            .await?;

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

    /// Create a new `FileIO` instance for Adls.
    ///
    /// # Errors
    /// Fails if the `FileIO` instance cannot be created.
    pub fn file_io(
        &self,
        credential: Option<&AzCredential>,
    ) -> Result<iceberg::io::FileIO, FileIoError> {
        let mut builder = iceberg::io::FileIOBuilder::new("azdls")
            .with_client(STS_CLIENT.get_or_init(reqwest::Client::new).clone());

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
            .with_prop(AzdlsConfigKeys::Filesystem, self.filesystem.clone());

        if let Some(credential) = credential {
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
                    if let Some(authority_host) = &self.authority_host {
                        builder = builder
                            .with_prop(AzdlsConfigKeys::AuthorityHost, authority_host.to_string());
                    }
                }
            }
        }

        Ok(builder.build()?)
    }

    async fn get_sas_token(
        &self,
        path: &Location,
        cred: StorageCredentials,
        permissions: StoragePermissions,
    ) -> Result<String, CredentialsError> {
        let client = blob_service_client(self.account_name.as_str(), cred);

        let start = time::OffsetDateTime::now_utc();
        let max_validity_seconds = i64::MAX;
        let sas_token_validity_seconds = self.sas_token_validity_seconds.unwrap_or(3600);
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
        let path = reduce_scheme_string(&path.to_string(), true);
        let rootless_path = path.trim_start_matches('/');
        let depth = rootless_path.split('/').count();
        let canonical_resource = format!(
            "/blob/{}/{}/{}",
            self.account_name.as_str(),
            self.filesystem.as_str(),
            rootless_path
        );

        let sas = BlobSharedAccessSignature::new(
            delegation_key.user_deligation_key.clone(),
            canonical_resource,
            permissions.into(),
            delegation_key.user_deligation_key.signed_expiry,
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

                    reason: "Storage Profile `key_prefix` must be less than 512 characters."
                        .to_string(),
                    entity: "KeyPrefix".to_string(),
                });
            }
            for key in key_prefix.split('/') {
                validate_path_segment(key)?;
            }
        }

        Ok(())
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
        })
    }

    #[must_use]
    fn iceberg_sas_property_key(&self) -> String {
        iceberg_sas_property_key(&self.account_name, &self.endpoint_suffix)
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
}

impl std::fmt::Display for AdlsLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.location.fmt(f)
    }
}

impl TryFrom<Location> for AdlsLocation {
    type Error = ValidationError;

    fn try_from(location: Location) -> Result<Self, Self::Error> {
        if location.url().scheme() != "abfss" {
            return Err(ValidationError::InvalidLocation {
                reason: "ADLS locations must use abfss protocol.".to_string(),
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

        Self::new(
            account_name.to_string(),
            filesystem,
            endpoint_suffix.to_string(),
            key,
        )
    }
}

impl FromStr for AdlsLocation {
    type Err = ValidationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let location = Location::from_str(s).map_err(|e| ValidationError::InvalidLocation {
            reason: format!("Invalid Location: {e}"),
            location: s.to_string(),
            source: Some(Box::new(e)),
            storage_type: StorageType::Adls,
        })?;

        Self::try_from(location)
    }
}

impl From<AdlsLocation> for Location {
    fn from(location: AdlsLocation) -> Location {
        location.location
    }
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

fn blob_service_client(account_name: &str, cred: StorageCredentials) -> BlobServiceClient {
    azure_storage_blobs::prelude::BlobServiceClient::builder(account_name, cred)
        .transport(TransportOptions::new(Arc::new(
            STS_CLIENT.get_or_init(reqwest::Client::new).clone(),
        )))
        .client_options(
            azure_core::ClientOptions::default().retry(RetryOptions::fixed(
                FixedRetryOptions::default()
                    .max_retries(3u32)
                    .max_total_elapsed(std::time::Duration::from_secs(5)),
            )),
        )
        .blob_service_client()
}

// This function should not use any information available in the profile, thus
// we don't give it a `&self` reference. We just pass it in to attach in the error.
pub(super) async fn validate_vended_credentials(
    table_config: &TableProperties,
    table_location: &Location,
    profile_for_error: &StorageProfile,
) -> Result<(), ValidationError> {
    let table_location = AdlsLocation::try_from(table_location.clone())?;
    let mut file_location = table_location.location.clone();
    file_location.without_trailing_slash().push("test-file");

    // TODO: replace with iceberg_rust's FileIO + opendal once sas is available
    let cred = azure_storage::StorageCredentials::sas_token(
        table_config
            .get_custom_prop(&table_location.iceberg_sas_property_key())
            .ok_or(CredentialsError::ShortTermCredential {
                reason: "Couldn't find sas token in table config.".to_string(),
                source: None,
            })?,
    )
    .map_err(|e| CredentialsError::ShortTermCredential {
        reason: "Error creating azure sas token.".to_string(),
        source: Some(Box::new(e)),
    })?;
    let client = blob_service_client(&table_location.account_name, cred);

    let container = client.container_client(table_location.filesystem.as_str());
    let blob_client = container.blob_client(reduce_scheme_string(&file_location.to_string(), true));
    blob_client
        .put_block_blob("Lakekeeper IOTest")
        .content_type("text/plain")
        .await
        .map_err(|e| {
            ValidationError::IoOperationFailed(
                IoError::FileWrite(Box::new(e)),
                Box::new(profile_for_error.clone()),
            )
        })?;

    let mut get = blob_client.get().into_stream();
    while let Some(n) = get.next().await {
        if let Err(e) = n {
            return Err(ValidationError::IoOperationFailed(
                IoError::FileRead(Box::new(e)),
                Box::new(profile_for_error.clone()),
            ));
        }
    }

    Ok(())
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
        };

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
mod test {
    use crate::service::{
        storage::StorageLocations, tabular_idents::TabularIdentUuid, NamespaceIdentUuid,
    };

    use super::*;
    use needs_env_var::needs_env_var;

    #[needs_env_var(TEST_AZURE = 1)]
    mod azure_tests {
        use crate::service::storage::{AdlsProfile, AzCredential};
        use crate::service::storage::{StorageCredential, StorageProfile};

        #[tokio::test]
        async fn test_can_validate() {
            let account_name = std::env::var("AZURE_STORAGE_ACCOUNT_NAME").unwrap();
            let client_id = std::env::var("AZURE_CLIENT_ID").unwrap();
            let client_secret = std::env::var("AZURE_CLIENT_SECRET").unwrap();
            let tenant_id = std::env::var("AZURE_TENANT_ID").unwrap();
            let filesystem = std::env::var("AZURE_STORAGE_FILESYSTEM").unwrap();
            let key_prefix = vec!['b'; 512].into_iter().collect::<String>();
            let prof = AdlsProfile {
                filesystem,
                key_prefix: Some(key_prefix.to_string()),
                account_name,
                authority_host: None,
                host: None,
                sas_token_validity_seconds: None,
            };
            let mut prof: StorageProfile = prof.into();

            let cred: StorageCredential = AzCredential::ClientCredentials {
                client_id,
                client_secret,
                tenant_id,
            }
            .into();

            prof.normalize().expect("failed to validate profile");
            prof.validate_access(Some(&cred), None).await.unwrap();
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
            let adls_location = AdlsLocation::from_str(location_str).unwrap();
            assert_eq!(adls_location.account_name(), account_name);
            assert_eq!(adls_location.filesystem(), filesystem);
            assert_eq!(adls_location.endpoint_suffix(), endpoint_suffix);
            assert_eq!(adls_location.key, key);
            // Roundtrip
            assert_eq!(adls_location.to_string(), location_str);
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
            let parsed_location = AdlsLocation::from_str(location);
            assert!(parsed_location.is_err(), "{}", parsed_location.unwrap());
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
}
