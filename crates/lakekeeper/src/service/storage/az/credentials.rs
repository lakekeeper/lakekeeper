use lakekeeper_io::adls::{AzureAuth, AzureClientCredentialsAuth, AzureSharedAccessKeyAuth};
use serde::{Deserialize, Serialize};
use veil::Redact;

use crate::{CONFIG, api::Result, service::storage::error::CredentialsError};

#[derive(Redact, Hash, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(tag = "credential-type", rename_all = "kebab-case")]
pub enum AzCredential {
    #[serde(rename_all = "kebab-case")]
    #[cfg_attr(feature = "open-api", schema(title = "AzCredentialClientCredentials"))]
    ClientCredentials {
        client_id: String,
        tenant_id: String,
        #[redact(partial)]
        client_secret: String,
    },
    #[serde(rename_all = "kebab-case")]
    #[cfg_attr(feature = "open-api", schema(title = "AzCredentialSharedAccessKey"))]
    SharedAccessKey {
        #[redact]
        key: String,
    },
    #[serde(rename_all = "kebab-case")]
    #[cfg_attr(feature = "open-api", schema(title = "AzCredentialManagedIdentity"))]
    AzureSystemIdentity {},
}

impl TryFrom<AzCredential> for AzureAuth {
    type Error = CredentialsError;

    fn try_from(cred: AzCredential) -> Result<Self, Self::Error> {
        if !CONFIG.enable_azure_system_credentials
            && matches!(cred, AzCredential::AzureSystemIdentity {})
        {
            return Err(CredentialsError::Misconfiguration(
                "Azure System identity credentials are disabled in this Lakekeeper deployment."
                    .to_string(),
            ));
        }

        Ok(match cred {
            AzCredential::ClientCredentials {
                client_id,
                tenant_id,
                client_secret,
            } => AzureClientCredentialsAuth {
                client_id,
                tenant_id,
                client_secret,
            }
            .into(),
            AzCredential::SharedAccessKey { key } => AzureSharedAccessKeyAuth { key }.into(),
            AzCredential::AzureSystemIdentity {} => AzureAuth::AzureSystemIdentity,
        })
    }
}
