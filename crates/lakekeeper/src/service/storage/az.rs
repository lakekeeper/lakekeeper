use std::{collections::HashMap, str::FromStr, sync::LazyLock};

use azure_storage::{
    prelude::{BlobSasPermissions, BlobSignedResource},
    shared_access_signature::{
        service_sas::{BlobSharedAccessSignature, SasKey},
        SasToken,
    },
    CloudLocation,
};
use azure_storage_blobs::prelude::BlobServiceClient;
use iceberg::io::ADLS_AUTHORITY_HOST;
use iceberg_ext::configs::table::{custom, TableProperties};
use lakekeeper_io::{
    adls::{
        normalize_host, validate_account_name, validate_filesystem_name, AdlsLocation, AdlsStorage,
        AzureAuth, AzureClientCredentialsAuth, AzureSettings, AzureSharedAccessKeyAuth,
    },
    InvalidLocationError, Location,
};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use url::Url;
use veil::Redact;

use crate::{
    api::{
        iceberg::{supported_endpoints, v1::tables::DataAccessMode},
        CatalogConfig, Result,
    },
    service::storage::{
        error::{
            CredentialsError, IcebergFileIoError, InvalidProfileError, TableConfigError,
            UpdateError, ValidationError,
        },
        StoragePermissions, TableConfig,
    },
    WarehouseId, CONFIG,
};

#[derive(Debug, Eq, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
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
}

const DEFAULT_HOST: &str = "dfs.core.windows.net";
static DEFAULT_AUTHORITY_HOST: LazyLock<Url> = LazyLock::new(|| {
    Url::parse("https://login.microsoftonline.com").expect("Default authority host is a valid URL")
});

const MAX_SAS_TOKEN_VALIDITY_SECONDS: u64 = 7 * 24 * 60 * 60;
const MAX_SAS_TOKEN_VALIDITY_SECONDS_I64: i64 = 7 * 24 * 60 * 60;

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

    fn azure_settings(&self) -> AzureSettings {
        AzureSettings {
            authority_host: self.authority_host.clone(),
            cloud_location: self.cloud_location(),
        }
    }

    fn blob_service_client(
        &self,
        credential: &AzCredential,
    ) -> Result<BlobServiceClient, CredentialsError> {
        let azure_auth = AzureAuth::try_from(credential.clone())?;

        self.azure_settings()
            .get_blob_service_client(&azure_auth)
            .map_err(Into::into)
    }

    /// Get the Lakekeeper IO for this storage profile.
    ///
    /// # Errors
    /// - If system identity is requested but not enabled in the configuration.
    /// - If the client could not be initialized.
    pub fn lakekeeper_io(
        &self,
        credential: &AzCredential,
    ) -> Result<AdlsStorage, CredentialsError> {
        let azure_auth = AzureAuth::try_from(credential.clone())?;
        self.azure_settings()
            .get_storage_client(&azure_auth)
            .map_err(Into::into)
    }

    /// Generate the table configuration for Azure Datalake Storage Gen2.
    ///
    /// # Errors
    /// Fails if sas token cannot be generated.
    pub async fn generate_table_config(
        &self,
        data_access: DataAccessMode,
        table_location: &Location,
        credential: &AzCredential,
        permissions: StoragePermissions,
    ) -> Result<TableConfig, TableConfigError> {
        if matches!(data_access, DataAccessMode::ClientManaged) {
            return Ok(TableConfig {
                creds: TableProperties::default(),
                config: TableProperties::default(),
            });
        }

        let sas = match credential {
            AzCredential::ClientCredentials { .. } => {
                let client = self.blob_service_client(credential)?;
                self.sas_via_delegation_key(table_location, client, permissions)
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
                let client = self.blob_service_client(credential)?;
                self.sas_via_delegation_key(table_location, client, permissions)
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

    async fn sas_via_delegation_key(
        &self,
        path: &Location,
        client: BlobServiceClient,
        permissions: StoragePermissions,
    ) -> Result<String, CredentialsError> {
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
        let path = reduce_scheme_string(path.as_ref());
        let rootless_path = path.trim_start_matches('/').trim_end_matches('/');
        let depth = rootless_path.split('/').count();

        let canonical_resource = format!(
            "/blob/{}/{}/{}",
            self.account_name.as_str(),
            self.filesystem.as_str(),
            rootless_path
        );

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
            self.host.as_deref().unwrap_or(DEFAULT_HOST),
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
                return Err(InvalidProfileError {
                    source: None,

                    reason: "Storage Profile `key-prefix` must be less than 512 characters."
                        .to_string(),
                    entity: "key-prefix".to_string(),
                }
                .into());
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

/// Removes the hostname and user from the path.
/// Keeps only the path and optionally the scheme.
#[must_use]
pub(crate) fn reduce_scheme_string(path: &str) -> String {
    AdlsLocation::try_from_str(path, true)
        .map(|l| format!("/{}", l.blob_name().to_string().trim_start_matches('/')))
        .unwrap_or(path.to_string())
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
) -> Result<iceberg::io::FileIO, IcebergFileIoError> {
    // Add Authority host if not present
    let mut config = config.inner().clone();

    let sas_token_prefix = "adls.sas-token.";
    // Iceberg Rust cannot parse tokens of form "<sas_token_prefix><storage_account_name>.<endpoint_suffix>=<sas_token>"
    // https://github.com/apache/iceberg-rust/issues/1442
    let mut sas_token = None;
    for (key, value) in &config {
        if key.starts_with(sas_token_prefix) {
            sas_token = Some(value.to_string());
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

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::service::{
        storage::{az::DEFAULT_AUTHORITY_HOST, AdlsProfile, StorageLocations, StorageProfile},
        tabular_idents::TabularId,
        NamespaceId,
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
                key_prefix: Some(key_prefix.to_string()),
                account_name,
                authority_host: None,
                host: None,
                sas_token_validity_seconds: None,
                allow_alternative_protocols: false,
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
        };

        let sp: StorageProfile = profile.clone().into();

        let namespace_id = NamespaceId::from(uuid::Uuid::now_v7());
        let table_id = TabularId::Table(uuid::Uuid::now_v7());
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
