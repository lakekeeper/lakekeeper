//! Azure Data Lake Storage Gen2 backend built on `object_store`.
//!
//! See the surrounding module files for the layering:
//! * [`adls_location`] — URL parsing and validation (SDK-independent)
//! * [`credentials`] — `AzureAuth` → `MicrosoftAzureBuilder` shim
//! * [`adls_storage`] — `LakekeeperStorage` impl over `MicrosoftAzure`
//! * [`adls_writer`] — multipart writer with abort-on-Drop
//! * [`adls_error`] — `object_store::Error` → `IOError` mapping
use std::sync::Arc;

use url::Url;
use veil::Redact;

mod adls_error;
mod adls_location;
mod adls_storage;
mod adls_writer;
mod credentials;

pub use adls_location::{
    AdlsLocation, InvalidADLSAccountName, InvalidADLSFilesystemName, InvalidADLSHost,
    InvalidADLSPathSegment, normalize_host, validate_account_name, validate_filesystem_name,
};
pub use adls_storage::AdlsStorage;

use crate::adls::credentials::ResolvedCredential;

pub(crate) const ADLS_CUSTOM_SCHEMES: [&str; 1] = ["wasbs"];

/// Default DFS host suffix. `object_store` makes blob-endpoint requests
/// regardless of which form the URL takes (DFS or blob), but our public URL
/// scheme — and the iceberg-client property contracts — remain DFS-shaped.
pub const DEFAULT_DFS_HOST: &str = "dfs.core.windows.net";

/// Azure cloud selector for the data-plane endpoint.
///
/// The upper crate today only ever produces `Public` (no host override) or
/// `Custom` (operator-supplied host, which is how sovereign clouds, regional
/// rollouts, and private-link endpoints are all routed). The enum is kept —
/// rather than collapsed into a single `endpoint: Option<String>` — because
/// `Custom` is the natural home for the DFS→blob host translation that
/// `object_store::MicrosoftAzure` requires, and the two-variant shape lets a
/// future first-class sovereign-cloud variant (Azure Germany, etc.) be added
/// non-breakingly thanks to `#[non_exhaustive]`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AzureCloud {
    /// Standard public cloud (`blob.core.windows.net`).
    Public,
    /// Operator-supplied host. The string is the full host (no scheme, no
    /// path), e.g. `account.privatelink.dfs.core.windows.net` for private
    /// link or `account.blob.core.chinacloudapi.cn` for Azure China.
    Custom(String),
}

impl AzureCloud {
    /// Build the full HTTPS blob-endpoint URL `object_store` should connect
    /// to, or `None` for the public cloud (the builder synthesises
    /// `https://{account}.blob.core.windows.net` itself in that case).
    /// `object_store::MicrosoftAzure` always speaks blob endpoints, so any
    /// caller-supplied DFS host gets translated to its blob equivalent here.
    ///
    /// # Why not `MicrosoftAzureBuilder::with_url`?
    ///
    /// `object_store` ships a URL parser
    /// ([`MicrosoftAzureBuilder::with_url`](https://docs.rs/object_store/0.13.2/object_store/azure/struct.MicrosoftAzureBuilder.html#method.with_url)),
    /// but it only recognises a fixed set of well-known suffixes —
    /// `dfs.core.windows.net`, `dfs.fabric.microsoft.com`, and their `blob.`
    /// twins — and its account-name validator rejects any value containing
    /// a `.`. A private-endpoint URL such as
    /// `abfss://fs@acct.privatelink.dfs.core.windows.net/...` strips the
    /// known suffix to `acct.privatelink`, which contains a dot, so
    /// `with_url` returns `UrlNotRecognised` and never sets the account or
    /// endpoint. Sovereign clouds (`*.chinacloudapi.cn`,
    /// `*.usgovcloudapi.net`) hit the same wall.
    ///
    /// Because Lakekeeper's storage profiles must support private link,
    /// we do the DFS→blob translation ourselves and pass
    /// `account`, `container`, and `endpoint` to the builder explicitly.
    pub(crate) fn endpoint_url(&self) -> Option<String> {
        match self {
            Self::Public => None,
            Self::Custom(host) => {
                // Translate dfs → blob if the caller passed a DFS-shaped host
                // (e.g. `acct.privatelink.dfs.core.windows.net`).
                let blob_host = host.replacen(".dfs.", ".blob.", 1);
                Some(format!("https://{blob_host}"))
            }
        }
    }
}

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
    /// The authority host to use for authentication. Example: `https://login.microsoftonline.com`.
    #[builder(default)]
    pub authority_host: Option<Url>,
    /// The Azure storage account name (e.g. `mystorageacct`).
    pub account_name: String,
    /// The sovereign cloud to target. Defaults to [`AzureCloud::Public`].
    #[builder(default = AzureCloud::Public)]
    pub cloud: AzureCloud,
}

impl AzureSettings {
    /// Build an [`AdlsStorage`] for this account using the given credentials.
    ///
    /// # Caveats — SAS credentials and delete
    ///
    /// `object_store::MicrosoftAzure` implements deletes only via
    /// `delete_stream`, which routes through the container-level Blob Batch
    /// endpoint (`POST {container}?restype=container&comp=batch`) — even
    /// for a single blob. Directory-scoped SAS tokens are not authorised
    /// for that endpoint and Azure rejects them with
    /// `Signed Directory Depth Invalid`.
    ///
    /// Consequence: a storage handle built with [`AzureAuth::Sas`] from a
    /// directory-scoped SAS can `read` and `write` but **cannot delete**.
    /// For delete-bearing flows (cleanup, garbage collection), use a
    /// catalog-credentialed handle (`ClientCredentials`, `SharedAccessKey`,
    /// or `AzureSystemIdentity`), whose authorisation extends to the batch
    /// endpoint.
    ///
    /// # Errors
    /// - If the system-identity probe fails (rare — the probe itself is
    ///   in-process; failures here would indicate broken environment).
    /// - If a [`AzureAuth::Sas`] token cannot be parsed by `object_store`.
    #[must_use]
    pub fn get_storage_client(&self, cred: &AzureAuth) -> AdlsStorage {
        AdlsStorage::new(Arc::new(adls_storage::AdlsClientConfig {
            account_name: self.account_name.clone(),
            authority_host: self.authority_host.clone(),
            cloud: self.cloud.clone(),
            credential: ResolvedCredential::from_auth(cred),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_url_public_defers_to_object_store() {
        // `Public` returns None so the builder picks its own default
        // (`https://{account}.blob.core.windows.net`).
        assert_eq!(AzureCloud::Public.endpoint_url(), None);
    }

    #[test]
    fn endpoint_url_custom_passes_through() {
        assert_eq!(
            AzureCloud::Custom("acct.privatelink.example.com".to_string()).endpoint_url(),
            Some("https://acct.privatelink.example.com".to_string())
        );
    }

    #[test]
    fn endpoint_url_custom_translates_dfs_to_blob() {
        // Pinned in code: DFS-shaped private-endpoint hosts get rewritten
        // to the blob equivalent because `object_store` only speaks blob.
        assert_eq!(
            AzureCloud::Custom("acct.privatelink.dfs.core.windows.net".to_string()).endpoint_url(),
            Some("https://acct.privatelink.blob.core.windows.net".to_string())
        );
    }

    #[test]
    fn endpoint_url_custom_does_not_translate_non_dfs() {
        // The `.replacen(".dfs.", ".blob.", 1)` must only match a literal
        // `.dfs.` infix. A bare "dfs" elsewhere in the host must stay.
        assert_eq!(
            AzureCloud::Custom("dfsproxy.example.com".to_string()).endpoint_url(),
            Some("https://dfsproxy.example.com".to_string())
        );
    }
}
