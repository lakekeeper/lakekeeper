use std::sync::{LazyLock, OnceLock};

use google_cloud_auth::credentials::CredentialsFile;
use iceberg_ext::configs::Location;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::service::storage::{error::TableConfigError, gcs::GcsServiceKey, StoragePermissions};

static STS_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
const STS_URL_STR: &str = "https://sts.googleapis.com/v1/token";
static STS_URL: LazyLock<Url> = LazyLock::new(|| {
    STS_URL_STR
        .parse::<Url>()
        .expect("failed to parse a constant to a url")
});
const GOOGLE_CLOUD_PLATFORM_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

pub(crate) async fn downscope(
    cred: impl Into<CredentialsFile>,
    bucket: &str,
    table_location: Location,
    storage_permissions: StoragePermissions,
) -> Result<STSResponse, TableConfigError> {
    let client = STS_CLIENT.get_or_init(reqwest::Client::new);
    let sts_url = STS_URL.clone();

    let c =
        google_cloud_auth::project::Config::default().with_scopes(&[GOOGLE_CLOUD_PLATFORM_SCOPE]);
    let source = google_cloud_auth::project::create_token_source_from_credentials(&cred.into(), &c)
        .await
        .map_err(|e| {
            tracing::error!(
                "Failed to create gcp token source from credentials: {:?}",
                e
            );
            TableConfigError::FailedDependency(
                "Failed to create gcp token source from credentials".to_string(),
            )
        })?;
    let token = source.token().await.map_err(|e| {
        tracing::error!("Failed to get token from token source: {:?}", e);
        TableConfigError::FailedDependency("Failed to get gcp token from token source".to_string())
    })?;

    client
        .post(sts_url)
        .header("Content-Type", "application/json")
        .json(&STSRequest::from_token_and_options(
            &token.access_token,
            &Options::from_location_and_permissions(bucket, &table_location, storage_permissions),
        )?)
        .send()
        .await
        .map_err(|e| {
            tracing::error!("Failed to send downscoping request: {:?}", e);
            TableConfigError::FailedDependency("Failed to send downscoping request".to_string())
        })?
        .json::<STSResponse>()
        .await
        .map_err(|e| {
            tracing::error!("Failed to parse downscoping response: {:?}", e);
            TableConfigError::FailedDependency("Failed to downscope.".to_string())
        })
}

#[derive(Deserialize, veil::Redact)]
pub(crate) struct STSResponse {
    #[redact(partial)]
    pub(crate) access_token: String,
    pub(crate) expires_in: Option<usize>,
    token_type: String,
}

#[derive(Serialize)]
struct STSRequest {
    // urn:ietf:params:oauth:grant-type:token-exchange
    pub grant_type: String,
    /// The full resource name of the identity provider; for example:
    /// //iam.googleapis.com/projects/<project-number>/locations/global/workloadIdentityPools/<pool-id>/providers/<provider-id>
    /// for workload identity pool providers, or
    /// //iam.googleapis.com/locations/global/workforcePools/<pool-id>/providers/<provider-id> for
    /// workforce pool providers. Required when exchanging an external credential for a Google
    /// access token.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audience: Option<String>,
    /// The OAuth 2.0 scopes to include on the resulting access token, formatted as a list of space-
    /// delimited, case-sensitive strings. Required when exchanging an external credential for a
    /// Google access token.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    // urn:ietf:params:oauth:token-type:access_token
    pub requested_token_type: String,
    pub subject_token: String,
    pub subject_token_type: String,
    // serialized json string
    pub options: String,
}

impl STSRequest {
    fn from_token_and_options(token: &str, options: &Options) -> Result<Self, TableConfigError> {
        let op = serde_json::to_string(options).map_err(|e| {
            TableConfigError::Internal("Failed to serialize options".to_string(), Some(Box::new(e)))
        })?;
        Ok(Self {
            grant_type: "urn:ietf:params:oauth:grant-type:token-exchange".to_string(),
            audience: None,
            scope: None,
            requested_token_type: "urn:ietf:params:oauth:token-type:access_token".to_string(),
            subject_token: token.to_string(),
            subject_token_type: "urn:ietf:params:oauth:token-type:access_token".to_string(),
            // A string with JSON-format Credential Access Boundary, encoded with percent encoding.
            options: percent_encoding::utf8_percent_encode(&op, percent_encoding::NON_ALPHANUMERIC)
                .to_string(),
        })
    }
}

#[derive(Serialize, Deserialize)]
struct Options {
    #[serde(rename = "accessBoundary")]
    access_boundary: AccessBoundary,
}

impl Options {
    fn from_location_and_permissions(
        bucket: &str,
        table_location: &Location,
        storage_permissions: StoragePermissions,
    ) -> Self {
        let mut table_location = table_location.clone();
        table_location.with_trailing_slash();
        let prefixless_location = table_location
            .as_str()
            .replace(&format!("gs://{bucket}/"), "");
        Options {
            access_boundary: AccessBoundary {
                access_boundary_rules: vec![AccessBoundaryRule {
                    available_resource: format!(
                        "//storage.googleapis.com/projects/_/buckets/{bucket}",
                    ),
                    available_permissions: match storage_permissions {
                        StoragePermissions::Read => vec!["inRole:roles/storage.objectViewer".to_string()],
                        StoragePermissions::ReadWrite => vec!["inRole:roles/storage.objectViewer".to_string(),
                                                              "inRole:roles/storage.objectCreator".to_string()],
                        StoragePermissions::ReadWriteDelete => vec!["inRole:roles/storage.objectUser".to_string()]
                    },
                    availability_condition: AvailabilityCondition {
                        title: "obj-prefixes".to_string(),
                        // need the getAttribute to allow Listing operations
                        expression: format!(
                            "resource.name.startsWith('projects/_/buckets/{bucket}/objects/{prefixless_location}') || api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith('{prefixless_location}')",
                        ),
                    },
                }],
            },
        }
    }
}

#[derive(Serialize, Deserialize)]
struct AccessBoundary {
    #[serde(rename = "accessBoundaryRules")]
    access_boundary_rules: Vec<AccessBoundaryRule>,
}

#[derive(Serialize, Deserialize)]
struct AccessBoundaryRule {
    #[serde(rename = "availableResource")]
    available_resource: String,
    #[serde(rename = "availablePermissions")]
    available_permissions: Vec<String>,
    availability_condition: AvailabilityCondition,
}

#[derive(Serialize, Debug, Deserialize)]
struct AvailabilityCondition {
    title: String,
    expression: String,
}

impl From<&GcsServiceKey> for CredentialsFile {
    fn from(
        GcsServiceKey {
            r#type: tp,
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
        }: &GcsServiceKey,
    ) -> Self {
        Self {
            tp: tp.to_string(),
            client_email: Some(client_email.clone()),
            private_key_id: Some(private_key_id.clone()),
            private_key: Some(private_key.clone()),
            auth_uri: Some(auth_uri.clone()),
            token_uri: Some(token_uri.clone()),
            project_id: Some(project_id.clone()),
            client_secret: None,
            client_id: Some(client_id.clone()),
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
