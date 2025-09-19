use std::{
    sync::{Arc, LazyLock},
    time::Duration,
};

use azure_core::{FixedRetryOptions, RetryOptions, TransportOptions};
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
    normalize_host, validate_account_name, validate_filesystem_name, AdlsLocation,
    InvalidADLSAccountName, InvalidADLSFilesystemName, InvalidADLSHost, InvalidADLSPathSegment,
};
pub use adls_storage::AdlsStorage;

use crate::error::InitializeClientError;

const DEFAULT_HOST: &str = "dfs.core.windows.net";
static DEFAULT_AUTHORITY_HOST: LazyLock<Url> = LazyLock::new(|| {
    Url::parse("https://login.microsoftonline.com").expect("Default authority host is a valid URL")
});
static DEFAULT_CLIENT_OPTIONS: LazyLock<azure_core::ClientOptions> = LazyLock::new(|| {
    azure_core::ClientOptions::default().retry(RetryOptions::fixed(
        FixedRetryOptions::default()
            .max_retries(3u32)
            .max_total_elapsed(std::time::Duration::from_secs(5)),
    ))
});

static HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(reqwest::Client::new);
// Reqwest client is already cheap to clone. We keep this `HTTP_CLIENT_ARC` because the Azure SDK requires an `Arc<dyn HttpClient>`.
static HTTP_CLIENT_ARC: LazyLock<Arc<reqwest::Client>> =
    LazyLock::new(|| Arc::new(HTTP_CLIENT.clone()));

pub(crate) const ADLS_CUSTOM_SCHEMES: [&str; 1] = ["wasbs"];

static SYSTEM_IDENTITY_CACHE: LazyLock<moka::sync::Cache<String, Arc<DefaultAzureCredential>>> =
    LazyLock::new(|| {
        moka::sync::Cache::builder()
            .max_capacity(1000)
            .time_to_live(Duration::from_secs(30 * 60))
            .build()
    });

#[derive(Debug, Clone, PartialEq, derive_more::From)]
pub enum AzureAuth {
    ClientCredentials(AzureClientCredentialsAuth),
    SharedAccessKey(AzureSharedAccessKeyAuth),
    AzureSystemIdentity,
}

#[derive(Redact, Clone, PartialEq, typed_builder::TypedBuilder)]
pub struct AzureSharedAccessKeyAuth {
    #[redact(partial)]
    pub key: String,
}

#[derive(Redact, Clone, PartialEq, typed_builder::TypedBuilder)]
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
    pub fn get_storage_client(
        &self,
        cred: &AzureAuth,
    ) -> Result<AdlsStorage, InitializeClientError> {
        let client = self.get_datalake_client(cred)?;
        Ok(AdlsStorage::new(client, self.cloud_location.clone()))
    }

    /// Returns the Azure Storage credentials based on the provided authentication method.
    ///
    /// # Errors
    /// - If system identity cannot be retrieved or initialized.
    pub fn get_azure_storage_credentials(
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

                StorageCredentials::token_credential(Arc::new(azure_auth))
            }
            AzureAuth::SharedAccessKey(AzureSharedAccessKeyAuth { key }) => {
                StorageCredentials::access_key(account_name, key.clone())
            }
            AzureAuth::AzureSystemIdentity => {
                let identity: Arc<DefaultAzureCredential> = self.get_system_identity()?;
                StorageCredentials::token_credential(identity)
            }
        })
    }

    /// Returns a [`DataLakeClient`] for the Azure Storage account.
    ///
    /// # Errors
    /// - If system identity cannot be retrieved or initialized.
    pub fn get_datalake_client(
        &self,
        cred: &AzureAuth,
    ) -> Result<DataLakeClient, InitializeClientError> {
        let azure_storage_cred = self.get_azure_storage_credentials(cred)?;

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
    pub fn get_blob_service_client(
        &self,
        cred: &AzureAuth,
    ) -> Result<BlobServiceClient, InitializeClientError> {
        let azure_storage_cred = self.get_azure_storage_credentials(cred)?;

        Ok(
            ClientBuilder::with_location(self.cloud_location.clone(), azure_storage_cred)
                .transport(TransportOptions::new(HTTP_CLIENT_ARC.clone()))
                .client_options(DEFAULT_CLIENT_OPTIONS.clone())
                .blob_service_client(),
        )
    }

    fn get_system_identity(&self) -> Result<Arc<DefaultAzureCredential>, InitializeClientError> {
        let authority_host_str = self.authority_host.as_ref().map_or(
            DEFAULT_AUTHORITY_HOST.as_str().to_string(),
            ToString::to_string,
        );
        let cache_key = format!("{}::{}", authority_host_str, self.cloud_location.account());

        SYSTEM_IDENTITY_CACHE
            .try_get_with(cache_key.clone(), || {
                let mut options = TokenCredentialOptions::default();
                options.set_authority_host(authority_host_str);
                DefaultAzureCredentialBuilder::new()
                    .with_options(options)
                    .build()
                    .map(Arc::new)
            })
            .map_err(|e| {
                tracing::error!("Failed to get Azure system identity: {e}");
                InitializeClientError {
                    reason: format!("Failed to get Azure system identity: {e}"),
                    source: Some(Box::new(e)),
                }
            })
    }
}
