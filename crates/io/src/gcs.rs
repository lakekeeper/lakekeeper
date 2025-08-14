mod gcs_error;
mod gcs_location;
mod gcs_storage;

use std::sync::LazyLock;

pub use gcs_location::{validate_bucket_name, GcsLocation, InvalidGCSBucketName};
pub use gcs_storage::GcsStorage;
pub use google_cloud_storage::client::google_cloud_auth::credentials::CredentialsFile;
use google_cloud_storage::client::{Client, ClientConfig};
use reqwest_middleware::ClientBuilder;
use reqwest_retry::{policies::ExponentialBackoff, Jitter, RetryTransientMiddleware};

use crate::InitializeClientError;

static HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(reqwest::Client::new);

#[derive(Debug, Eq, Clone, PartialEq, typed_builder::TypedBuilder)]
pub struct GCSSettings {}

#[derive(Clone, PartialEq)]
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
pub enum GcsAuth {
    /// Service Account Key
    ///
    /// The key is the JSON object obtained when creating a service account key in the GCP console.
    CredentialsFile { file: CredentialsFile },

    /// GCP System Identity
    ///
    /// Use the service account that the application is running as.
    /// This can be a Compute Engine default service account or a user-assigned service account.
    GcpSystemIdentity {},
}

impl GCSSettings {
    /// Create a new GCS client with the provided authentication method.
    ///
    /// # Errors
    /// If the client cannot be initialized, an `InitializeClientError` is returned.
    pub async fn get_storage_client(
        &self,
        auth: &GcsAuth,
    ) -> Result<GcsStorage, InitializeClientError> {
        let client = self.get_gcs_storage_client(auth).await?;
        Ok(GcsStorage::new(client))
    }

    async fn get_gcs_storage_client(
        &self,
        auth: &GcsAuth,
    ) -> Result<Client, InitializeClientError> {
        let retry_policy = ExponentialBackoff::builder()
            .base(2)
            .jitter(Jitter::Full)
            .build_with_max_retries(3);
        let mid_client = ClientBuilder::new(HTTP_CLIENT.clone())
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();

        let config = ClientConfig {
            http: Some(mid_client),
            ..ClientConfig::default()
        };

        let config = match auth {
            GcsAuth::GcpSystemIdentity {} => {
                config
                    .with_auth()
                    .await
                    .map_err(|e| InitializeClientError {
                        reason: format!(
                            "Failed to initialize GCS client with system identity: {e}"
                        ),
                        source: Some(e.into()),
                    })?
            }
            GcsAuth::CredentialsFile { file } => config
                .with_credentials(file.clone())
                .await
                .map_err(|e| InitializeClientError {
                    reason: format!("Failed to initialize GCS client with credentials file: {e}"),
                    source: Some(e.into()),
                })?,
        };

        Ok(Client::new(config))
    }
}

impl std::fmt::Debug for GcsAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GcsAuth::CredentialsFile { file } => f
                .debug_struct("GcsCredential::CredentialsFile")
                .field("type", &file.tp)
                .field("client_email", &file.client_email)
                .field("project_id", &file.project_id)
                .field("auth_uri", &file.auth_uri)
                .field("token_uri", &file.token_uri)
                .field("audience", &file.audience)
                .field("subject_token_type", &file.subject_token_type)
                .field("token_url_external", &file.token_url_external)
                .field("token_info_url", &file.token_info_url)
                .field(
                    "service_account_impersonation_url",
                    &file.service_account_impersonation_url,
                )
                .field("quota_project_id", &file.quota_project_id)
                .field(
                    "workforce_pool_user_project",
                    &file.workforce_pool_user_project,
                )
                .field(
                    "private_key_id",
                    &file.private_key_id.as_ref().map(|_| "<redacted>"),
                )
                .field(
                    "private_key",
                    &file.private_key.as_ref().map(|_| "<redacted>"),
                )
                .field(
                    "client_secret",
                    &file.client_secret.as_ref().map(|_| "<redacted>"),
                )
                .field("client_id", &file.client_id.as_ref().map(|_| "<redacted>"))
                .field(
                    "refresh_token",
                    &file.refresh_token.as_ref().map(|_| "<redacted>"),
                )
                .field(
                    "service_account_impersonation",
                    &file
                        .service_account_impersonation
                        .as_ref()
                        .map(|_| "<redacted>"),
                )
                .field("delegates", &file.delegates.as_ref().map(|_| "<redacted>"))
                .field(
                    "credential_source",
                    &file.credential_source.as_ref().map(|_| "<redacted>"),
                )
                .finish(),
            GcsAuth::GcpSystemIdentity {} => {
                f.debug_struct("GcsCredential::GcpSystemIdentity").finish()
            }
        }
    }
}
