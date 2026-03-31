use std::{
    sync::{Arc, LazyLock},
    time::Duration,
};

use azure_core::{
    credentials::TokenCredential,
    http::{ClientOptions, ExponentialRetryOptions, HttpClient, RetryOptions, Transport},
};
use azure_identity::{
    ClientSecretCredential, ClientSecretCredentialOptions, ManagedIdentityCredential,
    WorkloadIdentityCredential,
};
use azure_storage_blob::{BlobServiceClient, BlobServiceClientOptions};
use url::Url;
use veil::Redact;

/// A single shared `reqwest 0.13` client injected into every `BlobServiceClient`.
///
/// `typespec_client_core` (used by the Azure SDK) implements `HttpClient` for
/// `reqwest 0.13`.  `reqwest::Client` (0.12, used for S3/GCS) is a *different*
/// crate version and does **not** satisfy that trait.  We therefore alias the
/// 0.13 crate as `reqwest-0-13` in `Cargo.toml` (see `storage-adls` feature)
/// and use it exclusively here for Azure blob transport.
///
/// `reqwest::Client` is cheaply `Clone`-able (it wraps an `Arc` internally),
/// so sharing one instance reuses the same connection pool and TLS context
/// across all Azure blob operations instead of allocating a fresh one on every
/// `BlobServiceClient::new()` call.
static AZURE_BLOB_HTTP_CLIENT: LazyLock<Arc<dyn HttpClient>> =
    LazyLock::new(|| Arc::new(reqwest_0_13::Client::new()));

mod adls_error;
mod adls_location;
mod adls_storage;
pub(crate) mod shared_key_policy;

pub use adls_location::{
    AdlsLocation, InvalidADLSAccountName, InvalidADLSFilesystemName, InvalidADLSHost,
    InvalidADLSPathSegment, normalize_host, validate_account_name, validate_filesystem_name,
};
pub use adls_storage::AdlsStorage;

use crate::error::InitializeClientError;

const DEFAULT_HOST: &str = "dfs.core.windows.net";
const DEFAULT_BLOB_HOST: &str = "blob.core.windows.net";

static DEFAULT_AUTHORITY_HOST: LazyLock<Url> = LazyLock::new(|| {
    Url::parse("https://login.microsoftonline.com").expect("Default authority host is a valid URL")
});

pub(crate) const ADLS_CUSTOM_SCHEMES: [&str; 1] = ["wasbs"];

static SYSTEM_IDENTITY_CACHE: LazyLock<moka::future::Cache<String, Arc<dyn TokenCredential>>> =
    LazyLock::new(|| {
        moka::future::Cache::builder()
            .max_capacity(1000)
            .time_to_live(Duration::from_secs(30 * 60))
            .build()
    });

#[derive(Debug, Clone, PartialEq, Eq, derive_more::From)]
pub enum AzureAuth {
    ClientCredentials(AzureClientCredentialsAuth),
    SharedAccessKey(AzureSharedAccessKeyAuth),
    AzureSystemIdentity,
}

#[derive(Redact, Clone, PartialEq, Eq, typed_builder::TypedBuilder)]
pub struct AzureSharedAccessKeyAuth {
    #[redact(partial)]
    pub key: String,
}

#[derive(Redact, Clone, PartialEq, Eq, typed_builder::TypedBuilder)]
pub struct AzureClientCredentialsAuth {
    pub client_id: String,
    pub tenant_id: String,
    #[redact(partial)]
    pub client_secret: String,
}

/// A simple enum to represent the Azure cloud location, replacing the old `CloudLocation` type.
#[derive(Debug, Clone)]
pub enum CloudLocation {
    /// The public Azure cloud. The endpoint is `https://<account>.blob.core.windows.net/`.
    Public { account: String },
    /// A custom Azure cloud with a custom URI suffix.
    Custom { account: String, uri: String },
}

impl CloudLocation {
    #[must_use]
    pub fn account(&self) -> &str {
        match self {
            CloudLocation::Public { account } | CloudLocation::Custom { account, .. } => account,
        }
    }

    /// Returns the blob service endpoint URL string.
    #[must_use]
    pub fn blob_endpoint(&self) -> String {
        match self {
            CloudLocation::Public { account } => {
                format!("https://{account}.{DEFAULT_BLOB_HOST}/")
            }
            CloudLocation::Custom { account, uri } => {
                // uri may be just "blob.example.com" or "https://account.blob.example.com/"
                if uri.starts_with("http://") || uri.starts_with("https://") {
                    // Already a full URI
                    if uri.ends_with('/') {
                        uri.clone()
                    } else {
                        format!("{uri}/")
                    }
                } else {
                    // It's a host suffix like "dfs.core.windows.net" or "blob.example.com"
                    // Replace "dfs." prefix with "blob." for the blob endpoint
                    let blob_host = if let Some(stripped) = uri.strip_prefix("dfs.") {
                        format!("blob.{stripped}")
                    } else {
                        uri.clone()
                    };
                    format!("https://{account}.{blob_host}/")
                }
            }
        }
    }
}

#[derive(Debug, Clone, typed_builder::TypedBuilder)]
pub struct AzureSettings {
    // -------- Azure Settings for multiple services --------
    /// The authority host to use for authentication. Example: `https://login.microsoftonline.com`.
    #[builder(default)]
    pub authority_host: Option<Url>,
    // Contains the account name and possibly a custom URI
    pub cloud_location: CloudLocation,
}

impl AzureSettings {
    /// Returns [`BlobServiceClientOptions`] with an explicit exponential retry policy and a
    /// shared HTTP transport.
    ///
    /// The new Azure SDK defaults to exponential retry already, but we make it explicit so
    /// the configuration is visible and easy to adjust.
    ///
    /// All `BlobServiceClient` instances share a single `reqwest::Client` (via `AZURE_BLOB_HTTP_CLIENT`)
    /// so they also share the same connection pool and TLS context rather than allocating a fresh
    /// one on every call.
    fn default_service_client_options() -> BlobServiceClientOptions {
        BlobServiceClientOptions {
            client_options: ClientOptions {
                retry: RetryOptions::exponential(ExponentialRetryOptions {
                    max_retries: 8,
                    initial_delay: time::Duration::milliseconds(200),
                    max_delay: time::Duration::seconds(30),
                    max_total_elapsed: time::Duration::seconds(60),
                }),
                transport: Some(Transport::new(AZURE_BLOB_HTTP_CLIENT.clone())),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Creates a new [`AzureSettings`] instance.
    ///
    /// # Errors
    /// - If system identity cannot be retrieved or initialized.
    pub async fn get_storage_client(
        &self,
        cred: &AzureAuth,
    ) -> Result<AdlsStorage, InitializeClientError> {
        let client = self.get_blob_service_client(cred).await?;
        Ok(AdlsStorage::new(client, self.cloud_location.clone()))
    }

    /// Returns a [`BlobServiceClient`] for the Azure Storage account.
    ///
    /// # Errors
    /// - If system identity cannot be retrieved or initialized.
    pub async fn get_blob_service_client(
        &self,
        cred: &AzureAuth,
    ) -> Result<BlobServiceClient, InitializeClientError> {
        let credential: Option<Arc<dyn TokenCredential>> = match cred {
            AzureAuth::ClientCredentials(AzureClientCredentialsAuth {
                tenant_id,
                client_id,
                client_secret,
            }) => {
                let authority_host = self.authority_host.as_ref().map_or_else(
                    || DEFAULT_AUTHORITY_HOST.as_str().to_string(),
                    |u| u.as_str().to_string(),
                );

                let options = if authority_host == DEFAULT_AUTHORITY_HOST.as_str() {
                    None
                } else {
                    // Set custom authority host via cloud configuration
                    use azure_core::cloud::{CloudConfiguration, CustomConfiguration};
                    let mut custom_config = CustomConfiguration::default();
                    custom_config.authority_host = authority_host;
                    let client_opts = azure_core::http::ClientOptions {
                        cloud: Some(Arc::new(CloudConfiguration::Custom(custom_config))),
                        ..Default::default()
                    };
                    Some(ClientSecretCredentialOptions {
                        client_options: client_opts,
                    })
                };

                let cred = ClientSecretCredential::new(
                    tenant_id,
                    client_id.clone(),
                    client_secret.clone().into(),
                    options,
                )
                .map_err(|e| InitializeClientError {
                    reason: format!("Failed to create Azure ClientSecretCredential: {e}"),
                    source: Some(Box::new(e)),
                })?;
                Some(cred)
            }
            AzureAuth::SharedAccessKey(AzureSharedAccessKeyAuth { key }) => {
                use azure_core::{credentials::Secret, http::policies::Policy};
                use shared_key_policy::SharedKeyAuthorizationPolicy;

                let policy = Arc::new(SharedKeyAuthorizationPolicy::new(
                    self.cloud_location.account().to_string(),
                    Secret::new(key.clone()),
                ));
                let mut options = Self::default_service_client_options();
                options
                    .client_options
                    .per_try_policies
                    .push(policy as Arc<dyn Policy>);
                let endpoint = self.cloud_location.blob_endpoint();
                return BlobServiceClient::new(&endpoint, None, Some(options)).map_err(|e| {
                    InitializeClientError {
                        reason: format!(
                            "Failed to create Azure BlobServiceClient for endpoint '{endpoint}': {e}"
                        ),
                        source: Some(Box::new(e)),
                    }
                });
            }
            AzureAuth::AzureSystemIdentity => {
                let identity = self.get_system_identity().await?;
                Some(identity)
            }
        };

        let endpoint = self.cloud_location.blob_endpoint();
        BlobServiceClient::new(
            &endpoint,
            credential,
            Some(Self::default_service_client_options()),
        )
        .map_err(|e| InitializeClientError {
            reason: format!(
                "Failed to create Azure BlobServiceClient for endpoint '{endpoint}': {e}"
            ),
            source: Some(Box::new(e)),
        })
    }

    /// Returns a [`TokenCredential`] for `ClientCredentials` or `AzureSystemIdentity` auth.
    ///
    /// # Errors
    /// - If the credential cannot be created or the system identity is unavailable.
    pub async fn get_token_credential(
        &self,
        cred: &AzureAuth,
    ) -> Result<Arc<dyn TokenCredential>, InitializeClientError> {
        match cred {
            AzureAuth::ClientCredentials(AzureClientCredentialsAuth {
                tenant_id,
                client_id,
                client_secret,
            }) => {
                let authority_host = self.authority_host.as_ref().map_or_else(
                    || DEFAULT_AUTHORITY_HOST.as_str().to_string(),
                    |u| u.as_str().to_string(),
                );

                let options = if authority_host == DEFAULT_AUTHORITY_HOST.as_str() {
                    None
                } else {
                    use azure_core::cloud::{CloudConfiguration, CustomConfiguration};
                    let mut custom_config = CustomConfiguration::default();
                    custom_config.authority_host = authority_host;
                    let client_opts = azure_core::http::ClientOptions {
                        cloud: Some(Arc::new(CloudConfiguration::Custom(custom_config))),
                        ..Default::default()
                    };
                    Some(ClientSecretCredentialOptions {
                        client_options: client_opts,
                    })
                };

                let cred = ClientSecretCredential::new(
                    tenant_id,
                    client_id.clone(),
                    client_secret.clone().into(),
                    options,
                )
                .map_err(|e| InitializeClientError {
                    reason: format!("Failed to create Azure ClientSecretCredential: {e}"),
                    source: Some(Box::new(e)),
                })?;
                Ok(cred as Arc<dyn TokenCredential>)
            }
            AzureAuth::AzureSystemIdentity => self.get_system_identity().await,
            AzureAuth::SharedAccessKey(_) => Err(InitializeClientError {
                reason: "SharedAccessKey does not support token credential access".to_string(),
                source: None,
            }),
        }
    }

    async fn get_system_identity(&self) -> Result<Arc<dyn TokenCredential>, InitializeClientError> {
        let authority_host_str = self.authority_host.as_ref().map_or(
            DEFAULT_AUTHORITY_HOST.as_str().to_string(),
            ToString::to_string,
        );
        let cache_key = format!("{}::{}", authority_host_str, self.cloud_location.account());

        SYSTEM_IDENTITY_CACHE
            .try_get_with(cache_key.clone(), async move {
                // Try WorkloadIdentityCredential first (Kubernetes), fall back to ManagedIdentityCredential
                let cred: Arc<dyn TokenCredential> = match WorkloadIdentityCredential::new(None) {
                    Ok(wic) => wic,
                    Err(_) => {
                        ManagedIdentityCredential::new(None).map_err(|e| InitializeClientError {
                            reason: format!("Failed to create ManagedIdentityCredential: {e}"),
                            source: Some(Box::new(e)),
                        })?
                    }
                };
                Ok::<Arc<dyn TokenCredential>, InitializeClientError>(cred)
            })
            .await
            .map_err(|e| {
                tracing::error!("Failed to get Azure system identity: {e}");
                InitializeClientError {
                    reason: format!("Failed to get Azure system identity: {e}"),
                    source: Some(Box::new(e)),
                }
            })
    }
}
