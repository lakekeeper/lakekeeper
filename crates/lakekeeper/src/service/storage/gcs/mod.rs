#![allow(clippy::module_name_repetitions)]

use std::{
    collections::HashMap,
    str::FromStr,
    time::{Duration, Instant},
};

use iceberg_ext::configs::table::{TableProperties, creds, gcs};
use lakekeeper_io::{
    InvalidLocationError, Location,
    gcs::{
        GCSSettings, GcsAccessToken, GcsAuth, GcsServiceAccountKey, GcsStorage,
        system_identity_project_id, validate_bucket_name,
    },
};
use serde::{Deserialize, Serialize};
pub(super) use sts::STSResponse;
use veil::Redact;

use crate::{
    CONFIG, WarehouseId,
    api::{
        CatalogConfig, RequestMetadata,
        iceberg::{supported_endpoints, v1::tables::DataAccessMode},
    },
    service::{
        BasicTabularInfo,
        storage::{
            ShortTermCredentialsRequest, TableConfig,
            cache::{CachedStc, CredentialCacheType, GCS_STC_CACHE, STCCacheKey, get_or_load_stc},
            error::{
                CredentialsError, InvalidProfileError, TableConfigError, UpdateError,
                ValidationError,
            },
            storage_layout::StorageLayout,
        },
    },
};

mod sts;

const STS_URL: &str = "https://sts.googleapis.com/v1/token";

#[derive(Debug, Hash, Eq, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct GcsProfile {
    /// Name of the GCS bucket
    pub bucket: String,
    /// Subpath in the bucket to use.
    pub key_prefix: Option<String>,
    /// Enable STS (Security Token Service) downscoped token generation for GCS.
    /// When disabled, clients cannot use vended credentials for this storage profile.
    /// Defaults to true.
    #[serde(default = "default_true")]
    pub sts_enabled: bool,
    /// Storage layout for namespace and tabular paths.
    #[serde(default)]
    pub storage_layout: Option<StorageLayout>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Hash, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
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
    #[cfg_attr(feature = "open-api", schema(title = "GcsCredentialServiceAccountKey"))]
    ServiceAccountKey { key: GcsServiceKey },

    /// GCP System Identity
    ///
    /// Use the service account that the application is running as.
    /// This can be a Compute Engine default service account or a user-assigned service account.
    #[cfg_attr(feature = "open-api", schema(title = "GcsCredentialSystemIdentity"))]
    GcpSystemIdentity {},
}

#[derive(Redact, Hash, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
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

impl From<&GcsServiceKey> for GcsServiceAccountKey {
    fn from(key: &GcsServiceKey) -> Self {
        let GcsServiceKey {
            r#type,
            project_id,
            private_key_id,
            private_key,
            client_email,
            client_id,
            auth_uri,
            token_uri,
            auth_provider_x509_cert_url,
            client_x509_cert_url,
            universe_domain,
        } = key;

        GcsServiceAccountKey::new(serde_json::json!({
            "type": r#type,
            "project_id": project_id,
            "private_key_id": private_key_id,
            "private_key": private_key,
            "client_email": client_email,
            "client_id": client_id,
            "auth_uri": auth_uri,
            "token_uri": token_uri,
            "auth_provider_x509_cert_url": auth_provider_x509_cert_url,
            "client_x509_cert_url": client_x509_cert_url,
            "universe_domain": universe_domain,
        }))
    }
}

#[derive(Clone, Debug)]
pub(super) struct CachedSTSResponse {
    pub(super) token: STSResponse,
    pub(super) project_id: Option<String>,
    pub(super) expires_at_system_time: Option<std::time::SystemTime>,
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
    pub fn update_with(self, mut other: Self) -> Result<Self, UpdateError> {
        if self.bucket != other.bucket {
            return Err(UpdateError::ImmutableField("bucket".to_string()));
        }

        if self.key_prefix != other.key_prefix {
            return Err(UpdateError::ImmutableField("key_prefix".to_string()));
        }

        if other.storage_layout.is_none() {
            other.storage_layout = self.storage_layout;
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
            defaults: HashMap::new(),
            overrides: HashMap::new(),
            endpoints: supported_endpoints().to_vec(),
            idempotency_key_lifetime: None,
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

    /// Access token of `credential`, used as the subject token of the STS downscoping,
    /// and the project it belongs to.
    async fn get_subject_token(
        credential: &GcsCredential,
    ) -> Result<(GcsAccessToken, Option<String>), TableConfigError> {
        let auth = GcsAuth::try_from(credential.clone())?;
        let token = auth.access_token().await.map_err(|e| {
            tracing::error!("Failed to get a gcp access token: {e:?}");
            subject_token_error(e.is_transient())
        })?;
        let project_id = match credential {
            GcsCredential::ServiceAccountKey { key } => Some(key.project_id.clone()),
            GcsCredential::GcpSystemIdentity {} => system_identity_project_id().await,
        };
        Ok((token, project_id))
    }

    /// Generate the table configuration for GCS.
    pub(crate) async fn generate_table_config(
        &self,
        data_access: DataAccessMode,
        credential: &GcsCredential,
        stc_request: &ShortTermCredentialsRequest,
        tabular_info: &impl BasicTabularInfo,
        request_metadata: &RequestMetadata,
    ) -> Result<TableConfig, TableConfigError> {
        let mut table_properties = TableProperties::default();

        if !data_access.provide_credentials() || !self.sts_enabled {
            tracing::debug!(
                "Not providing GCS credentials - provide_credentials: {}, sts_enabled: {}",
                data_access.provide_credentials(),
                self.sts_enabled
            );
            return Ok(TableConfig {
                creds: table_properties.clone(),
                config: table_properties,
                credentials_expiration_ms: None,
                remote_signing: None,
            });
        }

        let cache_key = STCCacheKey::new(stc_request.clone(), self.into(), Some(credential.into()));

        // Single-flight read-through: concurrent identical requests coalesce onto
        // one STS downscope (the most expensive miss in the system) per cache key.
        // The typed cache returns the `CachedSTSResponse` directly — no variant check.
        let response = get_or_load_stc(
            &GCS_STC_CACHE,
            CredentialCacheType::ShortTermCredentials,
            cache_key,
            || async {
                let (subject_token, project_id) = Self::get_subject_token(credential).await?;
                let token = sts::downscope(&subject_token.token, &self.bucket, stc_request).await?;

                let sts_validity_duration = vended_token_validity(
                    token.expires_in,
                    subject_token.expires_at,
                    std::time::SystemTime::now(),
                );

                let expires_at_system_time =
                    std::time::SystemTime::now().checked_add(sts_validity_duration);
                if expires_at_system_time.is_none() {
                    tracing::warn!(
                        "Calculated expiry time for STS token overflowed. Valid duration: {sts_validity_duration:?}",
                    );
                }

                let token = CachedSTSResponse {
                    token,
                    project_id,
                    expires_at_system_time,
                };
                Ok::<_, TableConfigError>(CachedStc::new(
                    token,
                    Instant::now().checked_add(sts_validity_duration),
                ))
            },
        )
        .await?;

        table_properties.insert(&gcs::Token(response.token.access_token));
        if let Some(project_id) = response.project_id {
            table_properties.insert(&gcs::ProjectId(project_id));
        }

        let mut credentials_expiration_ms: Option<i64> = None;
        if let Some(expiry) = response.expires_at_system_time {
            match expiry.duration_since(std::time::UNIX_EPOCH) {
                Ok(expiry_since_epoch) => {
                    table_properties.insert(&gcs::TokenExpiresAt(
                        expiry_since_epoch.as_millis().to_string(),
                    ));
                    match i64::try_from(expiry_since_epoch.as_millis()) {
                        Ok(expiration) => {
                            table_properties.insert(&creds::ExpirationTimeMs(expiration));
                            credentials_expiration_ms = Some(expiration);
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Calculated expiry time for STS token is outside of valid range: {e:?}. SystemTime: {expiry:?}.",
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "Calculated expiry time for STS token is before UNIX_EPOCH: {e:?}. SystemTime: {expiry:?}",
                    );
                }
            }
        }

        table_properties.insert(&gcs::RefreshCredentialsEndpoint(
            request_metadata.refresh_client_credentials_endpoint_for_table(
                tabular_info.warehouse_id(),
                tabular_info.tabular_ident(),
            ),
        ));

        Ok(TableConfig {
            // Due to backwards compat reasons we still return creds within config too
            config: table_properties.clone(),
            creds: table_properties,
            credentials_expiration_ms,
            remote_signing: None,
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

        if let Some(key_prefix) = self.key_prefix.as_ref()
            && key_prefix.is_empty()
        {
            self.key_prefix = None;
        }

        // GCS supports a max of 1024 chars and we need some buffer for tables.
        if let Some(key_prefix) = self.key_prefix.as_ref()
            && key_prefix.len() > 896
        {
            return Err(InvalidProfileError {
                source: None,
                reason: "Storage Profile `key_prefix` must be less than 896 characters."
                    .to_string(),
                entity: "key_prefix".to_string(),
            }
            .into());
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

/// Validity assumed for a downscoped token when neither STS nor the subject token name one.
const DEFAULT_VENDED_TOKEN_VALIDITY: Duration = Duration::from_secs(3600);

/// How long a downscoped token is valid. STS omits the expiry when the subject token is an
/// access token: the downscoped token then expires with the subject token.
fn vended_token_validity(
    sts_expires_in: Option<usize>,
    subject_expires_at: Option<std::time::SystemTime>,
    now: std::time::SystemTime,
) -> Duration {
    let from_sts = sts_expires_in.map(|secs| Duration::from_secs(secs as u64));
    let from_subject =
        subject_expires_at.map(|expires_at| expires_at.duration_since(now).unwrap_or_default());
    match (from_sts, from_subject) {
        (Some(sts), Some(subject)) => sts.min(subject),
        (Some(validity), None) | (None, Some(validity)) => validity,
        (None, None) => DEFAULT_VENDED_TOKEN_VALIDITY,
    }
}

/// Failures that may pass, such as an unavailable token endpoint, are a failed dependency.
fn subject_token_error(transient: bool) -> TableConfigError {
    if transient {
        TableConfigError::FailedDependency("Failed to get a gcp access token".to_string())
    } else {
        CredentialsError::Misconfiguration("Failed to get a gcp access token".to_string()).into()
    }
}

/// Build a `GcsStorage` client from vended-credentials properties.
///
/// Reads the downscoped `OAuth2` access token from the iceberg-format
/// `TableProperties` previously produced by `generate_table_config`.
pub(super) async fn lakekeeper_io_from_vended_table_config(
    config: &TableProperties,
) -> Result<GcsStorage, CredentialsError> {
    let access_token = config.get_prop_opt::<gcs::Token>().ok_or_else(|| {
        CredentialsError::ShortTermCredential {
            reason: "GCS vended credentials are missing the OAuth2 access token.".to_string(),
            source: None,
        }
    })?;
    let auth = GcsAuth::BearerToken(lakekeeper_io::gcs::GcsBearerTokenAuth { access_token });
    GCSSettings {}
        .get_storage_client(&auth)
        .await
        .map_err(Into::into)
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
                GcsAuth::ServiceAccountKey { key: (&key).into() }
            }
            GcsCredential::GcpSystemIdentity {} => GcsAuth::GcpSystemIdentity {},
        })
    }
}

#[cfg(test)]
pub(crate) mod test {
    pub(crate) mod gcs_integration_tests {
        use crate::{
            api::RequestMetadata,
            service::storage::{
                StorageCredential, StorageProfile,
                gcs::{GcsCredential, GcsProfile, GcsServiceKey},
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
                sts_enabled: true,
                storage_layout: None,
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

        mod gcp_system_credentials_integration_tests {
            use super::*;

            #[tokio::test]
            async fn test_system_identity_can_validate() {
                let (profile, credential) = get_storage_profile();
                let mut profile: StorageProfile = profile.into();
                let credential: StorageCredential = credential.into();
                profile
                    .normalize(Some(&credential))
                    .expect("failed to validate profile");
                let credential = GcsCredential::GcpSystemIdentity {};
                let credential: StorageCredential = credential.into();
                Box::pin(profile.validate_access(
                    Some(&credential),
                    None,
                    &RequestMetadata::new_unauthenticated(),
                ))
                .await
                .unwrap_or_else(|e| panic!("Failed to validate system identity due to '{e:?}'"));
            }
        }
    }

    pub(crate) mod gcs_hns_integration_tests {
        use crate::{
            api::RequestMetadata,
            service::storage::{
                StorageCredential, StorageProfile,
                gcs::{GcsCredential, GcsProfile, GcsServiceKey},
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
                sts_enabled: true,
                storage_layout: None,
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
            sts_enabled: true,
            storage_layout: None,
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

#[cfg(test)]
mod stored_credential_tests {
    use super::*;
    use crate::service::storage::StorageCredential;

    // Shape of a GCS credential as stored by released versions.
    const STORED_SERVICE_ACCOUNT_KEY: &str = r#"{
        "type": "gcs",
        "credential-type": "service-account-key",
        "key": {
            "type": "service_account",
            "project_id": "example-project-0000",
            "private_key_id": "test-key-id",
            "private_key": "test-private-key",
            "client_email": "example-sa@example-project-0000.iam.gserviceaccount.com",
            "client_id": "000000000000000000000",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/example-sa%40example-project-0000.iam.gserviceaccount.com",
            "universe_domain": "googleapis.com"
        }
    }"#;

    #[test]
    fn stored_service_account_key_still_deserializes() {
        let credential: StorageCredential =
            serde_json::from_str(STORED_SERVICE_ACCOUNT_KEY).unwrap();
        let StorageCredential::Gcs(credential) = credential else {
            panic!("expected a GCS credential, got {credential:?}");
        };
        let GcsCredential::ServiceAccountKey { key } = &credential else {
            panic!("expected a service account key, got {credential:?}");
        };
        assert_eq!(key.project_id, "example-project-0000");
        assert_eq!(
            key.client_email,
            "example-sa@example-project-0000.iam.gserviceaccount.com"
        );

        let auth = GcsAuth::try_from(credential).unwrap();
        let GcsAuth::ServiceAccountKey { key } = auth else {
            panic!("expected a service account key auth, got {auth:?}");
        };
        assert_eq!(key.project_id(), Some("example-project-0000"));
    }

    #[test]
    fn service_key_to_account_key_roundtrip() {
        let key = GcsServiceKey {
            r#type: "service_account".to_string(),
            project_id: "example-project-0000".to_string(),
            private_key_id: "0000".to_string(),
            private_key: "test-private-key".to_string(),
            client_email: "example-sa@example-project-0000.iam.gserviceaccount.com".to_string(),
            client_id: "000000000000000000000".to_string(),
            auth_uri: "https://accounts.google.com/o/oauth2/auth".to_string(),
            token_uri: "https://oauth2.googleapis.com/token".to_string(),
            auth_provider_x509_cert_url: "https://www.googleapis.com/oauth2/v1/certs".to_string(),
            client_x509_cert_url: "https://www.googleapis.com/robot/v1/metadata/x509/example-sa"
                .to_string(),
            universe_domain: "googleapis.com".to_string(),
        };
        let account_key = GcsServiceAccountKey::from(&key);
        assert_eq!(account_key.project_id(), Some("example-project-0000"));

        let json = serde_json::to_value(&account_key).unwrap();
        assert_eq!(json, serde_json::to_value(&key).unwrap());
    }

    #[tokio::test]
    async fn permanent_access_token_error_is_misconfiguration() {
        let mut key: GcsServiceKey = serde_json::from_value(
            serde_json::from_str::<serde_json::Value>(STORED_SERVICE_ACCOUNT_KEY).unwrap()["key"]
                .clone(),
        )
        .unwrap();
        key.universe_domain = "example.com".to_string();

        let err = GcsProfile::get_subject_token(&GcsCredential::ServiceAccountKey { key })
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                TableConfigError::Credentials(CredentialsError::Misconfiguration(_))
            ),
            "{err:?}"
        );
    }

    #[test]
    fn subject_token_error_by_transience() {
        assert!(matches!(
            subject_token_error(true),
            TableConfigError::FailedDependency(_)
        ));
        assert!(matches!(
            subject_token_error(false),
            TableConfigError::Credentials(CredentialsError::Misconfiguration(_))
        ));
    }

    #[test]
    fn vended_token_validity_takes_shortest_known() {
        let now = std::time::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let in_secs = |secs| Some(now + Duration::from_secs(secs));
        assert_eq!(
            vended_token_validity(Some(3600), in_secs(300), now),
            Duration::from_secs(300)
        );
        assert_eq!(
            vended_token_validity(Some(600), in_secs(3599), now),
            Duration::from_secs(600)
        );
        assert_eq!(
            vended_token_validity(None, in_secs(1800), now),
            Duration::from_mins(30)
        );
        assert_eq!(
            vended_token_validity(Some(900), None, now),
            Duration::from_mins(15)
        );
        assert_eq!(
            vended_token_validity(None, None, now),
            Duration::from_secs(3600)
        );
        assert_eq!(
            vended_token_validity(None, Some(now - Duration::from_secs(1)), now),
            Duration::ZERO
        );
    }

    #[test]
    fn stored_system_identity_still_deserializes() {
        let credential: StorageCredential =
            serde_json::from_str(r#"{"type": "gcs", "credential-type": "gcp-system-identity"}"#)
                .unwrap();
        assert_eq!(
            credential,
            StorageCredential::Gcs(GcsCredential::GcpSystemIdentity {})
        );
    }
}
