use crate::{service::authz::ErrorModel, AuthZBackend, CONFIG};

pub(super) mod allow_all;

#[cfg(feature = "authz-openfga")]
pub mod openfga;

/// Get the default authorizer from the configuration
///
/// # Errors
/// Default model is not obtainable, i.e. if the model is not found in openfga
// Return error model here to convert it into anyhow in bin. IcebergErrorResponse does
// not implement StdError
pub async fn get_default_authorizer_from_config() -> Result<BuiltInAuthorizers, ErrorModel> {
    match &CONFIG.authz_backend {
        AuthZBackend::AllowAll => Ok(allow_all::AllowAllAuthorizer.into()),
        #[cfg(feature = "authz-openfga")]
        AuthZBackend::OpenFGA => Ok(openfga::new_authorizer_from_config().await?.into()),
    }
}

/// Migrate the default authorizer to a new model version.
///
/// # Errors
/// Migration fails - for details check the documentation of the configured
/// Authorizer implementation
pub async fn migrate_default_authorizer() -> std::result::Result<(), ErrorModel> {
    match &CONFIG.authz_backend {
        AuthZBackend::AllowAll => Ok(()),
        #[cfg(feature = "authz-openfga")]
        AuthZBackend::OpenFGA => {
            let client = openfga::new_client_from_config().await?;
            let store_name = None;
            openfga::migrate(&client, store_name).await?;
            Ok(())
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, strum_macros::Display, strum_macros::AsRefStr, strum_macros::EnumString,
)]
#[strum(serialize_all = "snake_case")]
pub enum FgaType {
    User,
    Role,
    Server,
    Project,
    Warehouse,
    Namespace,
    Table,
    View,
    ModelVersion,
    AuthModelId,
}

#[derive(Debug, Clone)]
pub enum BuiltInAuthorizers {
    AllowAll(allow_all::AllowAllAuthorizer),
    #[cfg(feature = "authz-openfga")]
    OpenFGA(openfga::OpenFGAAuthorizer),
}

impl From<allow_all::AllowAllAuthorizer> for BuiltInAuthorizers {
    fn from(authorizer: allow_all::AllowAllAuthorizer) -> Self {
        Self::AllowAll(authorizer)
    }
}

#[cfg(feature = "authz-openfga")]
impl From<openfga::OpenFGAAuthorizer> for BuiltInAuthorizers {
    fn from(authorizer: openfga::OpenFGAAuthorizer) -> Self {
        Self::OpenFGA(authorizer)
    }
}
