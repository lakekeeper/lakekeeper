use google_cloud_auth::credentials::CredentialsFile;
use iceberg_ext::configs::Location;
use serde::{Deserialize, Serialize};

use super::{TokenSource, HTTP_CLIENT, STS_URL};
use crate::service::storage::{error::TableConfigError, gcs::GcsServiceKey, StoragePermissions};

pub(crate) async fn downscope(
    token_source: TokenSource,
    bucket: &str,
    table_location: Location,
    storage_permissions: StoragePermissions,
) -> Result<STSResponse, TableConfigError> {
    let token = token_source.token().await.map_err(|e| {
        tracing::error!("Failed to get token from token source: {:?}", e);
        TableConfigError::FailedDependency("Failed to get gcp token from token source".to_string())
    })?;

    let sts_request = &STSRequest::from_token_and_options(
        &token,
        &Options::from_location_and_permissions(bucket, &table_location, storage_permissions),
    )?;

    let response = HTTP_CLIENT
        .clone()
        .post(STS_URL.clone())
        .json(&sts_request)
        .send()
        .await
        .map_err(|e| {
            tracing::error!("Failed to send downscoping request: {:?}", e);
            TableConfigError::FailedDependency("Failed to send downscoping request".to_string())
        })?
        .json::<serde_json::Value>()
        .await
        .map_err(|e| {
            tracing::error!(
                "Downscoping did not return a JSON body: {e:?}. Request: {sts_request:?}",
            );
            TableConfigError::FailedDependency("Failed to downscope.".to_string())
        })?;

    serde_json::from_value(response.clone()).map_err(|e| {
        tracing::error!(
            "Failed to parse downscoping response: {e:?}. Received Body: {response}. Request: {sts_request:?}",
        );
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

#[derive(Serialize, veil::Redact)]
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
    #[redact(partial)]
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
                        StoragePermissions::Read => {
                            vec!["inRole:roles/storage.objectViewer".to_string()]
                        }
                        StoragePermissions::ReadWrite => vec![
                            "inRole:roles/storage.objectViewer".to_string(),
                            "inRole:roles/storage.objectCreator".to_string(),
                        ],
                        StoragePermissions::ReadWriteDelete => vec![
                            "inRole:roles/storage.objectUser".to_string(),
                        ],
                    },
                    availability_condition: AvailabilityCondition {
                        title: "obj-prefixes".to_string(),
                        // need the getAttribute to allow Listing operations
                        expression: format!(
                            "resource.name.startsWith('projects/_/buckets/{bucket}/objects/{prefixless_location}') || resource.name.startsWith('projects/_/buckets/{bucket}/folders/{prefixless_location}') || api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith('{prefixless_location}')",
                        ),
                    }.into(),
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
    #[serde(skip_serializing_if = "Option::is_none")]
    availability_condition: Option<AvailabilityCondition>,
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
