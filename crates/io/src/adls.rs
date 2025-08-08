use std::sync::{Arc, LazyLock};

use url::Url;
use veil::Redact;

mod adls_error;
mod adls_location;
mod adls_storage;

pub use adls_location::{
    normalize_host, validate_account_name, AdlsLocation, InvalidADLSAccountName,
    InvalidADLSFilesystemName, InvalidADLSHost, InvalidADLSPathSegment,
};
pub use adls_storage::AdlsStorage;

const DEFAULT_HOST: &str = "dfs.core.windows.net";
// static DEFAULT_AUTHORITY_HOST: LazyLock<Url> = LazyLock::new(|| {
//     Url::parse("https://login.microsoftonline.com").expect("Default authority host is a valid URL")
// });

// static HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(reqwest::Client::new);
// static HTTP_CLIENT_ARC: LazyLock<Arc<reqwest::Client>> =
//     LazyLock::new(|| Arc::new(HTTP_CLIENT.clone()));

// const MAX_SAS_TOKEN_VALIDITY_SECONDS: u64 = 7 * 24 * 60 * 60;
// const MAX_SAS_TOKEN_VALIDITY_SECONDS_I64: i64 = 7 * 24 * 60 * 60;

pub(crate) const ADLS_CUSTOM_SCHEMES: [&str; 1] = ["wasbs"];
// static ADLS_PATH_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
//     Regex::new("^(?<protocol>(abfss|wasbs)?://)[^/@]+@[^/]+(?<path>/.+)")
//         .expect("ADLS path regex is valid")
// });

// static SYSTEM_IDENTITY_CACHE: LazyLock<moka::sync::Cache<String, Arc<DefaultAzureCredential>>> =
//     LazyLock::new(|| moka::sync::Cache::builder().max_capacity(1000).build());

#[derive(Debug, Clone, PartialEq, derive_more::From)]
pub enum AzureAuth {
    ClientCredentials(AzureClientCredentialsAuth),
    SharedAccessKey(AzureSharedAccessKeyAuth),
    AzureSystemIdentity {},
}

#[derive(Redact, Clone, PartialEq)]
pub struct AzureSharedAccessKeyAuth {
    #[redact(partial)]
    pub key: String,
}

#[derive(Redact, Clone, PartialEq)]
pub struct AzureClientCredentialsAuth {
    pub client_id: String,
    pub tenant_id: String,
    #[redact(partial)]
    pub client_secret: String,
}

#[derive(Debug, Eq, Clone, PartialEq, typed_builder::TypedBuilder)]
pub struct AzureSettings {
    // -------- Azure Settings for multiple services --------
    /// The authority host to use for authentication. Example: `https://login.microsoftonline.com`.
    #[builder(default, setter(strip_option))]
    pub authority_host: Option<Url>,
    /// The host to use for the storage account. Example: `dfs.core.windows.net`.
    #[builder(default, setter(strip_option))]
    pub host: Option<String>,
    // // -------- ADLS Gen 2 specific settings --------
    // /// Name of the adls filesystem, in blobstorage also known as container.
    // pub filesystem: String,
    // /// Name of the azure storage account.
    // pub account_name: String,
}
