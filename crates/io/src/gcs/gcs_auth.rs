use std::{path::PathBuf, time::SystemTime};

use google_cloud_auth::{
    credentials::{
        self, CacheableResource, Credentials, CredentialsProvider, EntityTag, service_account,
    },
    errors::CredentialsError,
};
use http::{Extensions, HeaderMap, HeaderValue, header::AUTHORIZATION};
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;
use veil::Redact;

use crate::{
    InitializeClientError,
    gcs::gcs_access_token::{
        GcsAccessToken, TOKEN_HTTP_CLIENT, exchange_service_account_key, metadata_server_token,
        token_endpoint,
    },
};

pub(crate) const CLOUD_PLATFORM_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";
const ADC_ENV_VAR: &str = "GOOGLE_APPLICATION_CREDENTIALS";
const METADATA_HOST_ENV_VAR: &str = "GCE_METADATA_HOST";
const DEFAULT_METADATA_HOST: &str = "metadata.google.internal";
static SYSTEM_IDENTITY_PROJECT_ID: OnceCell<String> = OnceCell::const_new();

/// GCS Credentials
#[derive(Clone, PartialEq, Debug)]
pub enum GcsAuth {
    /// Service Account Key
    ///
    /// The key is the JSON object obtained when creating a service account key in the GCP console.
    ServiceAccountKey { key: GcsServiceAccountKey },

    /// GCP System Identity
    ///
    /// Use the identity that the application is running as, found via Application Default
    /// Credentials: the file in `GOOGLE_APPLICATION_CREDENTIALS`, the gcloud default credentials
    /// file, or the metadata server (Compute Engine default or user-assigned service account).
    GcpSystemIdentity {},

    /// Static `OAuth2` bearer token. Used with downscoped tokens vended via STS.
    BearerToken(GcsBearerTokenAuth),
}

#[derive(Redact, Clone, PartialEq, Eq)]
pub struct GcsBearerTokenAuth {
    #[redact(partial)]
    pub access_token: String,
}

/// Service account key as the JSON object obtained when creating a key in the GCP console.
///
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
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "serde_json::Value", into = "serde_json::Value")]
pub struct GcsServiceAccountKey(serde_json::Value);

impl From<serde_json::Value> for GcsServiceAccountKey {
    fn from(json: serde_json::Value) -> Self {
        Self::new(json)
    }
}

impl From<GcsServiceAccountKey> for serde_json::Value {
    fn from(key: GcsServiceAccountKey) -> Self {
        key.0
    }
}

impl GcsServiceAccountKey {
    #[must_use]
    pub fn new(mut json: serde_json::Value) -> Self {
        // An empty `universe_domain` means the default. The SDK would take it as another
        // universe and refuse to build a client.
        if let Some(object) = json.as_object_mut()
            && object
                .get("universe_domain")
                .and_then(serde_json::Value::as_str)
                == Some("")
        {
            object.remove("universe_domain");
        }
        Self(json)
    }

    #[must_use]
    pub fn project_id(&self) -> Option<&str> {
        self.str_field("project_id")
    }

    pub(crate) fn str_field(&self, name: &str) -> Option<&str> {
        self.0.get(name).and_then(serde_json::Value::as_str)
    }

    pub(crate) fn json(&self) -> &serde_json::Value {
        &self.0
    }

    /// The SDK reads only keys in PKCS#8 format, the format of keys that Google creates.
    pub(crate) fn check_format(&self) -> Result<(), &'static str> {
        if self
            .str_field("private_key")
            .is_some_and(|private_key| private_key.contains("RSA PRIVATE KEY"))
        {
            return Err(PKCS1_KEY_UNSUPPORTED);
        }
        Ok(())
    }
}

impl std::fmt::Debug for GcsServiceAccountKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcsServiceAccountKey")
            .field("type", &self.str_field("type"))
            .field("project_id", &self.project_id())
            .field("client_email", &self.str_field("client_email"))
            .finish_non_exhaustive()
    }
}

/// Failure to obtain an access token from [`GcsAuth`].
#[derive(Debug, thiserror::Error)]
#[error("Failed to obtain a GCS access token: {reason}")]
pub struct GcsAccessTokenError {
    reason: &'static str,
    transient: bool,
    #[source]
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}

impl GcsAccessTokenError {
    /// A failure that repeats until the configuration changes.
    pub(crate) fn permanent(
        reason: &'static str,
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ) -> Self {
        Self {
            reason,
            transient: false,
            source,
        }
    }

    /// A failure of the network or of Google's services that may succeed when repeated.
    pub(crate) fn transient(
        reason: &'static str,
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ) -> Self {
        Self {
            reason,
            transient: true,
            source,
        }
    }

    /// Whether repeating the request may succeed.
    #[must_use]
    pub fn is_transient(&self) -> bool {
        self.transient
    }
}

impl GcsAuth {
    /// Credentials for the GCS clients.
    pub(crate) fn credentials(&self) -> Result<Credentials, InitializeClientError> {
        match self {
            GcsAuth::ServiceAccountKey { key } => {
                key.check_format().map_err(invalid_configuration)?;
                service_account::Builder::new(key.0.clone())
                    .build()
                    .map_err(|e| InitializeClientError {
                        reason: "Failed to load the GCS service account key".to_string(),
                        source: Some(Box::new(e)),
                    })
            }
            GcsAuth::GcpSystemIdentity {} => {
                adc_environment().map_err(invalid_configuration)?;
                credentials::Builder::default()
                    .with_scopes([CLOUD_PLATFORM_SCOPE])
                    .build()
                    .map_err(|e| InitializeClientError {
                        reason: "Failed to load the GCP system identity".to_string(),
                        source: Some(Box::new(e)),
                    })
            }
            GcsAuth::BearerToken(GcsBearerTokenAuth { access_token }) => {
                StaticBearerCredentials::new(access_token).map(Credentials::from)
            }
        }
    }

    /// `OAuth2` access token with the `cloud-platform` scope.
    ///
    /// Google STS accepts it as subject token, and a downscoped token expires with it, so the
    /// expiry is returned where it is known:
    /// - Service account keys, also as Application Default Credentials file, are exchanged
    ///   at the Google token endpoint.
    /// - Without a credentials file, the token comes from the metadata server.
    /// - Other credentials files get a new token from the SDK on every call, with no expiry.
    ///
    /// # Errors
    /// Fails if the credentials cannot be loaded or the token endpoint rejects them.
    pub async fn access_token(&self) -> Result<GcsAccessToken, GcsAccessTokenError> {
        match self {
            GcsAuth::ServiceAccountKey { key } => service_account_access_token(key).await,
            GcsAuth::GcpSystemIdentity {} => {
                adc_environment().map_err(|reason| GcsAccessTokenError::permanent(reason, None))?;
                match read_adc_file().await? {
                    Some(json) if is_service_account_key(&json) => {
                        service_account_access_token(&GcsServiceAccountKey::new(json)).await
                    }
                    Some(_) => adc_file_access_token().await,
                    None => metadata_server_token(&metadata_host(), SystemTime::now()).await,
                }
            }
            GcsAuth::BearerToken(GcsBearerTokenAuth { access_token }) => Ok(GcsAccessToken {
                token: access_token.clone(),
                expires_at: None,
            }),
        }
    }
}

async fn service_account_access_token(
    key: &GcsServiceAccountKey,
) -> Result<GcsAccessToken, GcsAccessTokenError> {
    key.check_format()
        .map_err(|reason| GcsAccessTokenError::permanent(reason, None))?;
    exchange_service_account_key(key, token_endpoint(key)?, SystemTime::now()).await
}

/// Token of an Application Default Credentials file that is not a service account key.
async fn adc_file_access_token() -> Result<GcsAccessToken, GcsAccessTokenError> {
    let credentials = credentials::Builder::default()
        .with_scopes([CLOUD_PLATFORM_SCOPE])
        .build_access_token_credentials()
        .map_err(|e| {
            GcsAccessTokenError::permanent(
                "failed to load the GCP system identity",
                Some(Box::new(e)),
            )
        })?;
    let token = credentials.access_token().await.map_err(|e| {
        let reason = "the token request failed";
        if e.is_transient() {
            GcsAccessTokenError::transient(reason, Some(Box::new(e)))
        } else {
            GcsAccessTokenError::permanent(reason, Some(Box::new(e)))
        }
    })?;
    Ok(GcsAccessToken {
        token: token.token,
        expires_at: None,
    })
}

fn metadata_host() -> String {
    std::env::var(METADATA_HOST_ENV_VAR).unwrap_or_else(|_| DEFAULT_METADATA_HOST.to_string())
}

const PKCS1_KEY_UNSUPPORTED: &str = "service account keys in PKCS#1 format (BEGIN RSA PRIVATE KEY) are not supported, convert the key to PKCS#8 (BEGIN PRIVATE KEY)";
const CREDENTIALS_JSON_ENV_VAR: &str = "GOOGLE_APPLICATION_CREDENTIALS_JSON";
const CREDENTIALS_JSON_UNSUPPORTED: &str = "GOOGLE_APPLICATION_CREDENTIALS_JSON is not supported, write the key to a file and set GOOGLE_APPLICATION_CREDENTIALS to its path";

fn invalid_configuration(reason: &'static str) -> InitializeClientError {
    InitializeClientError {
        reason: reason.to_string(),
        source: None,
    }
}

/// Application Default Credentials ignore `GOOGLE_APPLICATION_CREDENTIALS_JSON`. Without this
/// check, an identity from another source would be used where the variable names a key.
fn adc_environment() -> Result<(), &'static str> {
    check_adc_environment(std::env::var_os(CREDENTIALS_JSON_ENV_VAR).is_some())
}

fn check_adc_environment(credentials_json_set: bool) -> Result<(), &'static str> {
    if credentials_json_set {
        return Err(CREDENTIALS_JSON_UNSUPPORTED);
    }
    Ok(())
}

fn is_service_account_key(json: &serde_json::Value) -> bool {
    json.get("type").and_then(serde_json::Value::as_str) == Some("service_account")
}

/// Content of the Application Default Credentials file, `None` if there is none and the
/// metadata server provides the identity.
async fn read_adc_file() -> Result<Option<serde_json::Value>, GcsAccessTokenError> {
    let Some(path) = adc_file_path() else {
        return Ok(None);
    };
    let content = tokio::fs::read(&path).await.map_err(|e| {
        GcsAccessTokenError::permanent(
            "failed to read the Application Default Credentials file",
            Some(Box::new(e)),
        )
    })?;
    serde_json::from_slice(&content).map(Some).map_err(|e| {
        GcsAccessTokenError::permanent(
            "failed to parse the Application Default Credentials file",
            Some(Box::new(e)),
        )
    })
}

/// Project of the GCP system identity.
///
/// Read from the Application Default Credentials file (`project_id`, else `quota_project_id`),
/// else from the metadata server. Only a found project is cached, so a failing metadata
/// server is asked again on the next call.
pub async fn system_identity_project_id() -> Option<String> {
    SYSTEM_IDENTITY_PROJECT_ID
        .get_or_try_init(|| async { lookup_system_identity_project_id().await.ok_or(()) })
        .await
        .ok()
        .cloned()
}

async fn lookup_system_identity_project_id() -> Option<String> {
    match read_adc_file().await {
        Ok(Some(json)) => project_id_from_adc_json(&json),
        Ok(None) => metadata_server_project_id().await,
        Err(e) => {
            tracing::debug!(error = %e, "Could not read the GCP credentials file");
            None
        }
    }
}

/// The file set in `GOOGLE_APPLICATION_CREDENTIALS`, else the gcloud default credentials file
/// if it exists.
fn adc_file_path() -> Option<PathBuf> {
    if let Some(path) = std::env::var_os(ADC_ENV_VAR) {
        return Some(PathBuf::from(path));
    }
    let config_dir = if cfg!(windows) {
        PathBuf::from(std::env::var_os("APPDATA")?)
    } else {
        PathBuf::from(std::env::var_os("HOME")?).join(".config")
    };
    let path = config_dir
        .join("gcloud")
        .join("application_default_credentials.json");
    path.is_file().then_some(path)
}

fn project_id_from_adc_json(json: &serde_json::Value) -> Option<String> {
    ["project_id", "quota_project_id"]
        .iter()
        .find_map(|key| {
            json.get(key)
                .and_then(serde_json::Value::as_str)
                .filter(|project_id| !project_id.is_empty())
        })
        .map(ToString::to_string)
}

async fn metadata_server_project_id() -> Option<String> {
    let host = metadata_host();
    let response = TOKEN_HTTP_CLIENT
        .get(format!(
            "http://{host}/computeMetadata/v1/project/project-id"
        ))
        .header("Metadata-Flavor", "Google")
        .send()
        .await
        .and_then(reqwest::Response::error_for_status);
    match response {
        Ok(response) => response
            .text()
            .await
            .ok()
            .map(|project_id| project_id.trim().to_string())
            .filter(|project_id| !project_id.is_empty()),
        Err(e) => {
            tracing::debug!(error = %e, "Could not read the project from the GCP metadata server");
            None
        }
    }
}

/// Sends a fixed `OAuth2` bearer token.
#[derive(Debug)]
struct StaticBearerCredentials {
    headers: HeaderMap,
    entity_tag: EntityTag,
}

impl StaticBearerCredentials {
    fn new(access_token: &str) -> Result<Self, InitializeClientError> {
        let mut value = HeaderValue::try_from(format!("Bearer {access_token}")).map_err(|e| {
            InitializeClientError {
                reason: "GCS access token is not a valid HTTP header value".to_string(),
                source: Some(Box::new(e)),
            }
        })?;
        value.set_sensitive(true);
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, value);
        Ok(Self {
            headers,
            entity_tag: EntityTag::new(),
        })
    }
}

impl CredentialsProvider for StaticBearerCredentials {
    fn headers(
        &self,
        extensions: Extensions,
    ) -> impl Future<Output = Result<CacheableResource<HeaderMap>, CredentialsError>> + Send {
        let headers = match extensions.get::<EntityTag>() {
            Some(tag) if *tag == self.entity_tag => CacheableResource::NotModified,
            _ => CacheableResource::New {
                entity_tag: self.entity_tag.clone(),
                data: self.headers.clone(),
            },
        };
        std::future::ready(Ok(headers))
    }

    fn universe_domain(&self) -> impl Future<Output = Option<String>> + Send {
        std::future::ready(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_key() -> GcsServiceAccountKey {
        GcsServiceAccountKey::new(serde_json::json!({
            "type": "service_account",
            "project_id": "example-project-0000",
            "private_key_id": "test-key-id",
            "private_key": "test-private-key-secret",
            "client_email": "example-sa@example-project-0000.iam.gserviceaccount.com",
            "client_id": "000000000000000000000",
            "token_uri": "https://oauth2.googleapis.com/token",
        }))
    }

    fn bearer(token: &str) -> GcsAuth {
        GcsAuth::BearerToken(GcsBearerTokenAuth {
            access_token: token.to_string(),
        })
    }

    #[test]
    fn service_account_key_debug_redacts_private_key() {
        assert_eq!(
            format!("{:?}", GcsAuth::ServiceAccountKey { key: sample_key() }),
            r#"ServiceAccountKey { key: GcsServiceAccountKey { type: Some("service_account"), project_id: Some("example-project-0000"), client_email: Some("example-sa@example-project-0000.iam.gserviceaccount.com"), .. } }"#
        );
    }

    #[test]
    fn service_account_key_project_id() {
        assert_eq!(sample_key().project_id(), Some("example-project-0000"));
    }

    #[test]
    fn service_account_key_deserializes_from_json() {
        let key: GcsServiceAccountKey =
            serde_json::from_str(r#"{"type":"service_account","project_id":"p1"}"#).unwrap();
        assert_eq!(key.project_id(), Some("p1"));
    }

    #[test]
    fn empty_universe_domain_is_the_default() {
        let json = serde_json::json!({"type": "service_account", "universe_domain": ""});
        let deserialized: GcsServiceAccountKey = serde_json::from_value(json.clone()).unwrap();
        for key in [GcsServiceAccountKey::new(json), deserialized] {
            assert_eq!(key.json(), &serde_json::json!({"type": "service_account"}));
        }
    }

    fn pkcs1_key() -> GcsServiceAccountKey {
        let mut json = sample_key().json().clone();
        json["private_key"] = format!(
            "-----BEGIN {0}-----\ntest\n-----END {0}-----\n",
            "RSA PRIVATE KEY"
        )
        .into();
        GcsServiceAccountKey::new(json)
    }

    #[test]
    fn pkcs1_key_credentials_name_the_format() {
        assert_eq!(
            GcsAuth::ServiceAccountKey { key: pkcs1_key() }
                .credentials()
                .unwrap_err()
                .reason,
            PKCS1_KEY_UNSUPPORTED
        );
    }

    #[tokio::test]
    async fn pkcs1_key_access_token_names_the_format() {
        let err = GcsAuth::ServiceAccountKey { key: pkcs1_key() }
            .access_token()
            .await
            .unwrap_err();
        assert!(!err.is_transient());
        assert_eq!(
            err.to_string(),
            format!("Failed to obtain a GCS access token: {PKCS1_KEY_UNSUPPORTED}")
        );
    }

    #[test]
    fn credentials_json_variable_is_rejected() {
        assert_eq!(
            check_adc_environment(true),
            Err(CREDENTIALS_JSON_UNSUPPORTED)
        );
        assert_eq!(check_adc_environment(false), Ok(()));
    }

    #[tokio::test]
    async fn bearer_credentials_header() {
        let credentials = bearer("tok").credentials().unwrap();
        let CacheableResource::New { data, entity_tag } =
            credentials.headers(Extensions::new()).await.unwrap()
        else {
            panic!("expected new headers");
        };
        assert_eq!(data.get(AUTHORIZATION).unwrap(), "Bearer tok");
        assert_eq!(data.len(), 1);

        let mut extensions = Extensions::new();
        extensions.insert(entity_tag);
        assert_eq!(
            credentials.headers(extensions).await.unwrap(),
            CacheableResource::NotModified
        );
    }

    #[test]
    fn bearer_credentials_reject_invalid_header() {
        assert_eq!(
            bearer("tok\n").credentials().unwrap_err().reason,
            "GCS access token is not a valid HTTP header value"
        );
    }

    #[tokio::test]
    async fn bearer_access_token_is_bare() {
        assert_eq!(
            bearer("tok").access_token().await.unwrap(),
            GcsAccessToken {
                token: "tok".to_string(),
                expires_at: None,
            }
        );
    }

    #[test]
    fn service_account_key_detected_by_type() {
        assert!(is_service_account_key(
            &serde_json::json!({"type": "service_account"})
        ));
        assert!(!is_service_account_key(
            &serde_json::json!({"type": "authorized_user"})
        ));
        assert!(!is_service_account_key(
            &serde_json::json!({"type": "external_account"})
        ));
        assert!(!is_service_account_key(&serde_json::json!({})));
    }

    #[tokio::test]
    async fn service_account_access_token_rejects_other_universe_domain() {
        let key = GcsServiceAccountKey::new(serde_json::json!({
            "type": "service_account",
            "universe_domain": "example.com",
        }));
        let err = GcsAuth::ServiceAccountKey { key }
            .access_token()
            .await
            .unwrap_err();
        assert!(!err.is_transient());
        assert_eq!(
            err.reason,
            "service account keys outside the googleapis.com universe domain are not supported for credential vending"
        );
    }

    #[test]
    fn project_id_from_adc_json_prefers_project_id() {
        assert_eq!(
            project_id_from_adc_json(
                &serde_json::json!({"project_id": "p1", "quota_project_id": "q1"})
            ),
            Some("p1".to_string())
        );
        assert_eq!(
            project_id_from_adc_json(&serde_json::json!({"quota_project_id": "q1"})),
            Some("q1".to_string())
        );
        assert_eq!(
            project_id_from_adc_json(&serde_json::json!({"project_id": ""})),
            None
        );
        assert_eq!(
            project_id_from_adc_json(
                &serde_json::json!({"project_id": "", "quota_project_id": "q1"})
            ),
            Some("q1".to_string())
        );
        assert_eq!(project_id_from_adc_json(&serde_json::json!({})), None);
    }
}
