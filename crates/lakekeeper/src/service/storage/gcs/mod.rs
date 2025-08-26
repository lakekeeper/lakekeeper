#![allow(clippy::module_name_repetitions)]

use std::{
    collections::HashMap,
    str::FromStr,
    sync::{Arc, LazyLock},
};

use google_cloud_auth::{
    token::DefaultTokenSourceProvider, token_source::TokenSource as GCloudAuthTokenSource,
};
use google_cloud_token::{TokenSource as GCloudTokenSource, TokenSourceProvider as _};
use iceberg_ext::configs::table::{gcs, TableProperties};
use lakekeeper_io::{
    gcs::{validate_bucket_name, CredentialsFile, GCSSettings, GcsAuth, GcsStorage},
    InvalidLocationError, Location,
};
use serde::{Deserialize, Serialize};
use url::Url;
use veil::Redact;

use crate::{
    api::{
        iceberg::{supported_endpoints, v1::tables::DataAccessMode},
        CatalogConfig,
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

mod sts;

static HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(reqwest::Client::new);
const STS_URL_STR: &str = "https://sts.googleapis.com/v1/token";
static STS_URL: LazyLock<Url> = LazyLock::new(|| {
    STS_URL_STR
        .parse::<Url>()
        .expect("failed to parse a constant to a url")
});
const GOOGLE_CLOUD_PLATFORM_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

#[derive(Debug, Eq, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct GcsProfile {
    /// Name of the GCS bucket
    pub bucket: String,
    /// Subpath in the bucket to use.
    pub key_prefix: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(tag = "credential-type", rename_all = "kebab-case")]
/// GCS Credentials
///
/// Currently only supports Service Account Key
/// Example of a key:
/// ```json
///     {
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
/// ```
pub enum GcsCredential {
    /// Service Account Key
    ///
    /// The key is the JSON object obtained when creating a service account key in the GCP console.
    #[schema(title = "GcsCredentialServiceAccountKey")]
    ServiceAccountKey { key: GcsServiceKey },

    /// GCP System Identity
    ///
    /// Use the service account that the application is running as.
    /// This can be a Compute Engine default service account or a user-assigned service account.
    #[schema(title = "GcsCredentialSystemIdentity")]
    GcpSystemIdentity {},
}

#[derive(Redact, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct GcsServiceKey {
    pub r#type: String,
    pub project_id: String,
    pub private_key_id: String,
    #[redact(partial)]
    pub private_key: String,
    pub client_email: String,
    pub client_id: String,
    pub auth_uri: String,
    pub token_uri: String,
    pub auth_provider_x509_cert_url: String,
    pub client_x509_cert_url: String,
    pub universe_domain: String,
}

impl From<GcsServiceKey> for CredentialsFile {
    fn from(key: GcsServiceKey) -> Self {
        let GcsServiceKey {
            r#type,
            project_id,
            private_key_id,
            private_key,
            client_email,
            client_id,
            auth_uri,
            token_uri,
            auth_provider_x509_cert_url: _,
            client_x509_cert_url: _,
            universe_domain: _,
        } = key;

        CredentialsFile {
            tp: r#type,
            client_email: Some(client_email),
            private_key_id: Some(private_key_id),
            private_key: Some(private_key),
            auth_uri: Some(auth_uri),
            token_uri: Some(token_uri),
            project_id: Some(project_id),
            client_secret: None,
            client_id: Some(client_id),
            refresh_token: None,
            audience: None,
            subject_token_type: None,
            token_url_external: None,
            token_info_url: None,
            service_account_impersonation_url: None,
            service_account_impersonation: None,
            delegates: None,
            credential_source: None,
            quota_project_id: None,
            workforce_pool_user_project: None,
        }
    }
}

pub(crate) enum TokenSource {
    GAuth(Arc<dyn GCloudAuthTokenSource>),
    Token(Arc<dyn GCloudTokenSource>),
}

impl TokenSource {
    pub(crate) async fn token(&self) -> Result<String, String> {
        match self {
            TokenSource::GAuth(ts) => ts
                .token()
                .await
                .map(|t| t.access_token)
                .map_err(|e| e.to_string()),
            TokenSource::Token(ts) => ts.token().await.map_err(|e| e.to_string()),
        }
        .map(|t| t.trim_start_matches("Bearer ").to_string())
    }
}

impl GcsProfile {
    /// Create a new GCS storage client.
    ///
    /// # Errors
    /// Fails if the client cannot be initialized
    pub async fn lakekeeper_io(
        &self,
        credential: &GcsCredential,
    ) -> Result<GcsStorage, CredentialsError> {
        let gcs_auth = GcsAuth::try_from(credential.clone())?;
        let settings = GCSSettings {};
        settings
            .get_storage_client(&gcs_auth)
            .await
            .map_err(Into::into)
    }

    /// Validate the GCS profile.
    ///
    /// # Errors
    /// - Fails if the bucket name is invalid.
    /// - Fails if the key prefix is too long.
    pub(super) fn normalize(&mut self) -> Result<(), ValidationError> {
        validate_bucket_name(&self.bucket)?;
        self.normalize_key_prefix()?;

        Ok(())
    }

    /// Validate the GCS profile with credentials.
    /// # Errors
    /// - Fails if the bucket or key prefix changed
    pub fn update_with(self, other: Self) -> Result<Self, UpdateError> {
        if self.bucket != other.bucket {
            return Err(UpdateError::ImmutableField("bucket".to_string()));
        }

        if self.key_prefix != other.key_prefix {
            return Err(UpdateError::ImmutableField("key_prefix".to_string()));
        }

        Ok(other)
    }

    /// Check if the profile can be updated with the other profile.
    /// `key_prefix` and `bucket` must be the same.
    /// We enforce this to avoid issues by accidentally changing the bucket of a warehouse,
    /// after which all tables would not be accessible anymore.
    ///
    /// # Errors
    /// Fails if the `bucket` or `key_prefix` is different.
    pub fn can_be_updated_with(&self, other: &Self) -> Result<(), UpdateError> {
        if self.bucket != other.bucket {
            return Err(UpdateError::ImmutableField("bucket".to_string()));
        }

        if self.key_prefix != other.key_prefix {
            return Err(UpdateError::ImmutableField("key_prefix".to_string()));
        }

        Ok(())
    }

    #[must_use]
    #[allow(clippy::unused_self)]
    pub fn generate_catalog_config(&self, _: WarehouseId) -> CatalogConfig {
        CatalogConfig {
            defaults: HashMap::with_capacity(0),
            overrides: HashMap::with_capacity(0),
            endpoints: supported_endpoints().to_vec(),
        }
    }

    /// Base Location for this storage profile.
    ///
    /// # Errors
    /// Can fail for un-normalized profiles
    pub fn base_location(&self) -> Result<Location, InvalidLocationError> {
        let prefix: Vec<String> = self
            .key_prefix
            .as_ref()
            .map(|s| s.split('/').map(std::borrow::ToOwned::to_owned).collect())
            .unwrap_or_default();
        Location::from_str(&format!("gs://{}/", self.bucket))
            .map(|mut l| {
                l.extend(prefix.iter());
                l
            })
            .map_err(|e| {
                InvalidLocationError::new(
                    format!("gs://{}/", self.bucket),
                    format!("Failed to create base location for GCS profile: {e}"),
                )
            })
    }

    async fn get_token_source(
        &self,
        credential: &GcsCredential,
    ) -> Result<(TokenSource, Option<String>), CredentialsError> {
        let config = google_cloud_auth::project::Config::default()
            .with_scopes(&[GOOGLE_CLOUD_PLATFORM_SCOPE]);

        Ok(match credential {
            GcsCredential::ServiceAccountKey { key } => {
                let source = google_cloud_auth::project::create_token_source_from_credentials(
                    &key.into(),
                    &config,
                )
                .await
                .map_err(|e| {
                    tracing::error!(
                        "Failed to create gcp token source from credentials: {:?}",
                        e
                    );
                    CredentialsError::Misconfiguration(
                        "Failed to create gcp token source from credentials".to_string(),
                    )
                })?;
                (
                    TokenSource::GAuth(source.into()),
                    Some(key.project_id.clone()),
                )
            }
            GcsCredential::GcpSystemIdentity {} => {
                if !CONFIG.enable_gcp_system_credentials {
                    return Err(CredentialsError::Misconfiguration(
                        "GCP System identity credentials are disabled in this Lakekeeper deployment."
                            .to_string(),
                    ));
                }
                let tsp = DefaultTokenSourceProvider::new(config).await.map_err(|e| {
                    tracing::error!(
                        "Failed to create gcp token source from system identity: {:?}",
                        e
                    );
                    CredentialsError::Misconfiguration(
                        "Failed to create gcp token source from system identity".to_string(),
                    )
                })?;
                (TokenSource::Token(tsp.token_source()), tsp.project_id)
            }
        })
    }

    /// Generate the table configuration for GCS.
    pub(crate) async fn generate_table_config(
        &self,
        data_access: DataAccessMode,
        cred: &GcsCredential,
        table_location: &Location,
        storage_permissions: StoragePermissions,
    ) -> Result<TableConfig, TableConfigError> {
        let mut table_properties = TableProperties::default();

        if matches!(data_access, DataAccessMode::ClientManaged) {
            return Ok(TableConfig {
                creds: table_properties.clone(),
                config: table_properties,
            });
        }

        let (source, project_id) = self.get_token_source(cred).await?;
        let token = sts::downscope(
            source,
            &self.bucket,
            table_location.clone(),
            storage_permissions,
        )
        .await?;

        table_properties.insert(&gcs::Token(token.access_token));
        if let Some(ref project_id) = project_id {
            table_properties.insert(&gcs::ProjectId(project_id.clone()));
        }

        if let Some(expiry) = token.expires_in {
            table_properties.insert(&gcs::TokenExpiresAt(
                (std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
                    + (expiry * 1000) as u128)
                    .to_string(),
            ));
        }

        Ok(TableConfig {
            // Due to backwards compat reasons we still return creds within config too
            config: table_properties.clone(),
            creds: table_properties,
        })
    }

    fn normalize_key_prefix(&mut self) -> Result<(), ValidationError> {
        if let Some(key_prefix) = self.key_prefix.as_mut() {
            *key_prefix = key_prefix.trim_matches('/').to_string();
            if key_prefix.starts_with(".well-known/acme-challenge/") {
                return Err(InvalidProfileError {
                    source: None,
                    reason: "Storage Profile `key_prefix` cannot start with `.well-known/acme-challenge/`.".to_string(),
                    entity: "key_prefix".to_string(),
                }.into());
            }
        }

        if let Some(key_prefix) = self.key_prefix.as_ref() {
            if key_prefix.is_empty() {
                self.key_prefix = None;
            }
        }

        // GCS supports a max of 1024 chars and we need some buffer for tables.
        if let Some(key_prefix) = self.key_prefix.as_ref() {
            if key_prefix.len() > 896 {
                return Err(InvalidProfileError {
                    source: None,
                    reason: "Storage Profile `key_prefix` must be less than 896 characters."
                        .to_string(),
                    entity: "key_prefix".to_string(),
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
        // Different bucket means no overlap
        if self.bucket != other.bucket {
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
            // If either has no key prefix, it can access the entire bucket
            (None, _) | (_, None) => true,
        }
    }
}

pub(super) fn get_file_io_from_table_config(
    config: &TableProperties,
) -> Result<iceberg::io::FileIO, IcebergFileIoError> {
    Ok(iceberg::io::FileIOBuilder::new("gcs")
        .with_props(config.inner())
        .build()?)
}

impl TryFrom<GcsCredential> for GcsAuth {
    type Error = CredentialsError;

    fn try_from(credential: GcsCredential) -> Result<Self, Self::Error> {
        if !CONFIG.enable_gcp_system_credentials
            && matches!(credential, GcsCredential::GcpSystemIdentity {})
        {
            return Err(CredentialsError::Misconfiguration(
                "GCP System identity credentials are disabled in this Lakekeeper deployment."
                    .to_string(),
            ));
        }

        Ok(match credential {
            GcsCredential::ServiceAccountKey { key } => {
                GcsAuth::CredentialsFile { file: key.into() }
            }
            GcsCredential::GcpSystemIdentity {} => GcsAuth::GcpSystemIdentity {},
        })
    }
}

#[cfg(test)]
pub(crate) mod test {
    use needs_env_var::needs_env_var;

    #[needs_env_var(TEST_GCS = 1)]
    pub(crate) mod cloud_tests {
        use crate::{
            api::RequestMetadata,
            service::storage::{
                gcs::{GcsCredential, GcsProfile, GcsServiceKey},
                StorageCredential, StorageProfile,
            },
        };

        pub(crate) fn get_storage_profile() -> (GcsProfile, GcsCredential) {
            let bucket = std::env::var("LAKEKEEPER_TEST__GCS_BUCKET").expect("Missing GCS_BUCKET");
            let key =
                std::env::var("LAKEKEEPER_TEST__GCS_CREDENTIAL").expect("Missing GCS_CREDENTIAL");
            let key: GcsServiceKey = serde_json::from_str(&key).unwrap();
            let cred = GcsCredential::ServiceAccountKey { key };
            let profile = GcsProfile {
                bucket,
                key_prefix: Some(format!("test_prefix/{}", uuid::Uuid::now_v7())),
            };
            (profile, cred)
        }

        #[tokio::test]
        async fn test_can_validate() {
            let (profile, cred) = get_storage_profile();

            let cred: StorageCredential = cred.into();
            let s = &serde_json::to_string(&cred).unwrap();
            serde_json::from_str::<StorageCredential>(s).expect("json roundtrip failed");

            let mut profile: StorageProfile = profile.into();

            profile
                .normalize(Some(&cred))
                .expect("Failed to normalize profile");
            Box::pin(profile.validate_access(
                Some(&cred),
                None,
                &RequestMetadata::new_unauthenticated(),
            ))
            .await
            .unwrap();
        }

        #[tokio::test]
        #[needs_env_var::needs_env_var(LAKEKEEPER_TEST__ENABLE_GCP_SYSTEM_CREDENTIALS = 1)]
        async fn test_system_identity_can_validate() {
            let (profile, _credential) = get_storage_profile();
            let mut profile: StorageProfile = profile.into();
            profile.normalize().expect("failed to validate profile");
            let credential = GcsCredential::GcpSystemIdentity {};
            let credential: StorageCredential = credential.into();
            profile
                .validate_access(Some(&credential), None)
                .await
                .unwrap_or_else(|e| panic!("Failed to validate system identity due to '{e:?}'"));
        }
    }

    #[needs_env_var(TEST_GCS_HNS = 1)]
    pub(crate) mod gcs_hns_tests {
        use crate::{
            api::RequestMetadata,
            service::storage::{
                gcs::{GcsCredential, GcsProfile, GcsServiceKey},
                StorageCredential, StorageProfile,
            },
        };

        pub(crate) fn get_storage_profile() -> (GcsProfile, GcsCredential) {
            let bucket =
                std::env::var("LAKEKEEPER_TEST__GCS_HNS_BUCKET").expect("Missing GCS_HNS_BUCKET");
            let key =
                std::env::var("LAKEKEEPER_TEST__GCS_CREDENTIAL").expect("Missing GCS_CREDENTIAL");
            let key: GcsServiceKey = serde_json::from_str(&key).unwrap();
            let cred = GcsCredential::ServiceAccountKey { key };
            let profile = GcsProfile {
                bucket,
                key_prefix: Some(format!("test_prefix/{}", uuid::Uuid::now_v7())),
            };
            (profile, cred)
        }

        #[tokio::test]
        async fn test_can_validate() {
            let (profile, cred) = get_storage_profile();

            let cred: StorageCredential = cred.into();
            let s = &serde_json::to_string(&cred).unwrap();
            serde_json::from_str::<StorageCredential>(s).expect("json roundtrip failed");

            let mut profile: StorageProfile = profile.into();

            profile
                .normalize(Some(&cred))
                .expect("Failed to normalize profile");
            Box::pin(profile.validate_access(
                Some(&cred),
                None,
                &RequestMetadata::new_unauthenticated(),
            ))
            .await
            .unwrap();
        }
    }
}

#[cfg(test)]
mod is_overlapping_location_tests {
    use super::*;

    fn create_profile(bucket: &str, key_prefix: Option<&str>) -> GcsProfile {
        GcsProfile {
            bucket: bucket.to_string(),
            key_prefix: key_prefix.map(ToString::to_string),
        }
    }

    #[test]
    fn test_non_overlapping_different_bucket() {
        let profile1 = create_profile("bucket1", Some("prefix"));
        let profile2 = create_profile("bucket2", Some("prefix"));

        assert!(!profile1.is_overlapping_location(&profile2));
    }

    #[test]
    fn test_overlapping_identical_key_prefix() {
        let profile1 = create_profile("bucket1", Some("prefix"));
        let profile2 = create_profile("bucket1", Some("prefix"));

        assert!(profile1.is_overlapping_location(&profile2));
    }

    #[test]
    fn test_overlapping_one_prefix_of_other() {
        let profile1 = create_profile("bucket1", Some("prefix"));
        let profile2 = create_profile("bucket1", Some("prefix/subpath"));

        assert!(profile1.is_overlapping_location(&profile2));
        assert!(profile2.is_overlapping_location(&profile1)); // Test symmetry
    }

    #[test]
    fn test_overlapping_no_key_prefix() {
        let profile1 = create_profile("bucket1", None);
        let profile2 = create_profile("bucket1", Some("prefix"));

        assert!(profile1.is_overlapping_location(&profile2));
        assert!(profile2.is_overlapping_location(&profile1)); // Test symmetry
    }

    #[test]
    fn test_non_overlapping_unrelated_key_prefixes() {
        let profile1 = create_profile("bucket1", Some("prefix1"));
        let profile2 = create_profile("bucket1", Some("prefix2"));

        // These don't overlap as neither is a prefix of the other
        assert!(!profile1.is_overlapping_location(&profile2));
    }

    #[test]
    fn test_overlapping_both_no_key_prefix() {
        let profile1 = create_profile("bucket1", None);
        let profile2 = create_profile("bucket1", None);

        assert!(profile1.is_overlapping_location(&profile2));
    }

    #[test]
    fn test_complex_key_prefix_scenarios() {
        // Prefix with similar characters but not a prefix relationship
        let profile1 = create_profile("bucket1", Some("prefix"));
        let profile2 = create_profile("bucket1", Some("prefix-extra"));

        // Not overlapping since "prefix" is not a prefix of "prefix-extra"
        assert!(!profile1.is_overlapping_location(&profile2));

        // Actual prefix case
        let profile3 = create_profile("bucket1", Some("prefix"));
        let profile4 = create_profile("bucket1", Some("prefix/sub"));

        assert!(profile3.is_overlapping_location(&profile4));
    }
}
