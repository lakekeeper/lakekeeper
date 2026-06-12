use std::{
    sync::{Arc, LazyLock},
    time::Duration,
};

use azure_core::{
    FixedRetryOptions, RetryOptions, TransportOptions,
    auth::{AccessToken, TokenCredential},
};
use azure_identity::{
    DefaultAzureCredential, DefaultAzureCredentialBuilder, TokenCredentialOptions,
};
pub use azure_storage::CloudLocation;
use azure_storage::StorageCredentials;
use azure_storage_blobs::prelude::{BlobServiceClient, ClientBuilder};
use azure_storage_datalake::prelude::{DataLakeClient, DataLakeClientBuilder};
use url::Url;
use veil::Redact;

mod adls_error;
mod adls_location;
mod adls_storage;

pub use adls_location::{
    AdlsLocation, InvalidADLSAccountName, InvalidADLSFilesystemName, InvalidADLSHost,
    InvalidADLSPathSegment, normalize_host, validate_adls_host_account, validate_filesystem_name,
    validate_storage_account_name,
};
pub use adls_storage::AdlsStorage;

use crate::InitializeClientError;

/// Wraps a [`TokenCredential`] to retry transient failures when acquiring a
/// bearer token. The Azure storage data-plane retry policy ([`RetryOptions`])
/// does not cover the credential/token endpoint, so a single transient connect
/// timeout to the OAuth endpoint (e.g. SNAT/ephemeral-port exhaustion under
/// high concurrency) would otherwise fail the operation, unretried.
#[derive(Debug)]
struct RetryingTokenCredential {
    inner: Arc<dyn TokenCredential>,
}

impl RetryingTokenCredential {
    fn new(inner: Arc<dyn TokenCredential>) -> Arc<Self> {
        Arc::new(Self { inner })
    }
}

#[async_trait::async_trait]
impl TokenCredential for RetryingTokenCredential {
    async fn get_token(&self, scopes: &[&str]) -> azure_core::Result<AccessToken> {
        tryhard::retry_fn(|| self.inner.get_token(scopes))
            .retries(3)
            .exponential_backoff(Duration::from_millis(200))
            .max_delay(Duration::from_secs(10))
            .await
    }

    async fn clear_cache(&self) -> azure_core::Result<()> {
        self.inner.clear_cache().await
    }
}

const DEFAULT_HOST: &str = "dfs.core.windows.net";
static DEFAULT_AUTHORITY_HOST: LazyLock<Url> = LazyLock::new(|| {
    Url::parse("https://login.microsoftonline.com").expect("Default authority host is a valid URL")
});
/// Bounds how long a single connect attempt may hang before it is treated as a
/// failure. Without this, reqwest falls back to the OS default, where a stalled
/// TCP connect runs for tens of seconds before surfacing `ETIMEDOUT`
/// (`os error 110`). That blows the entire retry budget on a single attempt, so
/// transient connect failures (e.g. SNAT/ephemeral-port exhaustion against
/// Azure) never get retried. Keep this comfortably below `RETRY_MAX_TOTAL_ELAPSED`
/// so several attempts fit inside the budget.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Total wall-clock budget for retries. Must exceed `CONNECT_TIMEOUT` by enough
/// to allow a few retries, otherwise a single slow connect expires the policy
/// before any retry happens. The retry clock starts after the first attempt completes, so this
/// budget covers the retries, not the initial request.
const RETRY_MAX_TOTAL_ELAPSED: Duration = Duration::from_secs(30);

static DEFAULT_CLIENT_OPTIONS: LazyLock<azure_core::ClientOptions> = LazyLock::new(|| {
    azure_core::ClientOptions::default().retry(RetryOptions::fixed(
        FixedRetryOptions::default()
            .max_retries(3u32)
            .max_total_elapsed(RETRY_MAX_TOTAL_ELAPSED),
    ))
});

static HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .build()
        // Only fails if the TLS backend or system DNS config can't be
        // initialized — i.e. the environment is fundamentally broken. The
        // default `reqwest::Client::new()` would panic on the same condition.
        .expect("Failed to build ADLS HTTP client")
});
// Reqwest client is already cheap to clone. We keep this `HTTP_CLIENT_ARC` because the Azure SDK requires an `Arc<dyn HttpClient>`.
static HTTP_CLIENT_ARC: LazyLock<Arc<reqwest::Client>> =
    LazyLock::new(|| Arc::new(HTTP_CLIENT.clone()));

pub(crate) const ADLS_CUSTOM_SCHEMES: [&str; 1] = ["wasbs"];

static SYSTEM_IDENTITY_CACHE: LazyLock<moka::future::Cache<String, Arc<DefaultAzureCredential>>> =
    LazyLock::new(|| {
        moka::future::Cache::builder()
            .max_capacity(1000)
            .time_to_live(Duration::from_mins(30))
            .build()
    });

#[derive(Debug, Clone, PartialEq, Eq, derive_more::From)]
pub enum AzureAuth {
    ClientCredentials(AzureClientCredentialsAuth),
    SharedAccessKey(AzureSharedAccessKeyAuth),
    AzureSystemIdentity,
    /// SAS (Shared Access Signature) token. Used with downscoped credentials vended via SAS delegation.
    Sas(AzureSasAuth),
}

#[derive(Redact, Clone, PartialEq, Eq, typed_builder::TypedBuilder)]
pub struct AzureSharedAccessKeyAuth {
    #[redact(partial)]
    pub key: String,
}

#[derive(Redact, Clone, PartialEq, Eq, typed_builder::TypedBuilder)]
pub struct AzureSasAuth {
    #[redact(partial)]
    pub sas_token: String,
}

#[derive(Redact, Clone, PartialEq, Eq, typed_builder::TypedBuilder)]
pub struct AzureClientCredentialsAuth {
    pub client_id: String,
    pub tenant_id: String,
    #[redact(partial)]
    pub client_secret: String,
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
    /// Creates a new [`AzureSettings`] instance.
    ///
    /// # Errors
    /// - If system identity cannot be retrieved or initialized.
    pub async fn get_storage_client(
        &self,
        cred: &AzureAuth,
    ) -> Result<AdlsStorage, InitializeClientError> {
        let client = self.get_datalake_client(cred).await?;
        Ok(AdlsStorage::new(client, self.cloud_location.clone()))
    }

    /// Returns the Azure Storage credentials based on the provided authentication method.
    ///
    /// # Errors
    /// - If system identity cannot be retrieved or initialized.
    pub async fn get_azure_storage_credentials(
        &self,
        cred: &AzureAuth,
    ) -> Result<StorageCredentials, InitializeClientError> {
        let account_name = self.cloud_location.account();

        Ok(match cred {
            AzureAuth::ClientCredentials(AzureClientCredentialsAuth {
                tenant_id,
                client_id,
                client_secret,
            }) => {
                let azure_auth = azure_identity::ClientSecretCredential::new(
                    HTTP_CLIENT_ARC.clone(),
                    self.authority_host
                        .clone()
                        .unwrap_or(DEFAULT_AUTHORITY_HOST.clone()),
                    tenant_id.clone(),
                    client_id.clone(),
                    client_secret.clone(),
                );

                StorageCredentials::token_credential(RetryingTokenCredential::new(Arc::new(
                    azure_auth,
                )))
            }
            AzureAuth::SharedAccessKey(AzureSharedAccessKeyAuth { key }) => {
                StorageCredentials::access_key(account_name, key.clone())
            }
            AzureAuth::AzureSystemIdentity => {
                let identity: Arc<DefaultAzureCredential> = self.get_system_identity().await?;
                StorageCredentials::token_credential(RetryingTokenCredential::new(identity))
            }
            AzureAuth::Sas(AzureSasAuth { sas_token }) => StorageCredentials::sas_token(sas_token)
                .map_err(|e| InitializeClientError {
                    reason: format!("Invalid Azure SAS token: {e}"),
                    source: Some(Box::new(e)),
                })?,
        })
    }

    /// Returns a [`DataLakeClient`] for the Azure Storage account.
    ///
    /// # Errors
    /// - If system identity cannot be retrieved or initialized.
    pub async fn get_datalake_client(
        &self,
        cred: &AzureAuth,
    ) -> Result<DataLakeClient, InitializeClientError> {
        let azure_storage_cred = self.get_azure_storage_credentials(cred).await?;

        Ok(
            DataLakeClientBuilder::with_location(self.cloud_location.clone(), azure_storage_cred)
                .transport(TransportOptions::new(HTTP_CLIENT_ARC.clone()))
                .client_options(DEFAULT_CLIENT_OPTIONS.clone())
                .build(),
        )
    }

    /// Returns a [`BlobServiceClient`] for the Azure Storage account.
    ///
    /// # Errors
    /// - If system identity cannot be retrieved or initialized.
    pub async fn get_blob_service_client(
        &self,
        cred: &AzureAuth,
    ) -> Result<BlobServiceClient, InitializeClientError> {
        let azure_storage_cred = self.get_azure_storage_credentials(cred).await?;

        Ok(
            ClientBuilder::with_location(self.cloud_location.clone(), azure_storage_cred)
                .transport(TransportOptions::new(HTTP_CLIENT_ARC.clone()))
                .client_options(DEFAULT_CLIENT_OPTIONS.clone())
                .blob_service_client(),
        )
    }

    async fn get_system_identity(
        &self,
    ) -> Result<Arc<DefaultAzureCredential>, InitializeClientError> {
        let authority_host_str = self
            .authority_host
            .as_ref()
            .map_or(DEFAULT_AUTHORITY_HOST.to_string(), ToString::to_string);
        let cache_key = format!("{}::{}", authority_host_str, self.cloud_location.account());

        SYSTEM_IDENTITY_CACHE
            .try_get_with(cache_key.clone(), async move {
                let mut options = TokenCredentialOptions::default();
                options.set_authority_host(authority_host_str);
                DefaultAzureCredentialBuilder::new()
                    .with_options(options)
                    .build()
                    .map(Arc::new)
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
