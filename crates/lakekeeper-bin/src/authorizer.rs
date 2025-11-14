use lakekeeper::service::{authz::AllowAllAuthorizer, ServerId};
use lakekeeper_authz_openfga::{
    migrate as openfga_migrate, OpenFGAAuthorizer, CONFIG as OPENFGA_CONFIG,
};

#[derive(Debug)]
pub(crate) enum AuthorizerEnum {
    AllowAll(AllowAllAuthorizer),
    OpenFGA(Box<OpenFGAAuthorizer>),
}

impl AuthorizerEnum {
    pub(crate) async fn init_from_env(server_id: ServerId) -> anyhow::Result<Self> {
        if OPENFGA_CONFIG.is_openfga_enabled() {
            Ok(AuthorizerEnum::OpenFGA(Box::new(
                lakekeeper_authz_openfga::new_authorizer_from_default_config(server_id).await?,
            )))
        } else {
            Ok(AuthorizerEnum::AllowAll(AllowAllAuthorizer { server_id }))
        }
    }
}

pub(crate) async fn migrate(server_id: ServerId) -> anyhow::Result<()> {
    if OPENFGA_CONFIG.is_openfga_enabled() {
        let client = lakekeeper_authz_openfga::new_client_from_default_config().await?;
        // Passing None here will use the store name from the config
        let store_name_override = None;
        openfga_migrate(&client, store_name_override, server_id).await?;
    }
    Ok(())
}
