use std::{sync::Arc, vec};

use lakekeeper::{
    implementations::{get_default_catalog_from_config, postgres::PostgresBackend},
    limes::{Authenticator, AuthenticatorEnum},
    serve::{ServeConfiguration, serve},
    service::{
        CatalogStore, SecretStore,
        authn::{BuiltInAuthenticators, get_default_authenticator_from_config},
        authz::Authorizer,
        endpoint_statistics::EndpointStatisticsSink,
        events::get_default_cloud_event_backends_from_config,
    },
    tracing,
};

use crate::authorizer::AuthorizerEnum;
#[cfg(feature = "ui")]
use crate::ui;

pub(crate) async fn serve_default(bind_addr: std::net::SocketAddr) -> anyhow::Result<()> {
    let (catalog, secrets, stats) = get_default_catalog_from_config().await?;
    let server_id = <PostgresBackend as CatalogStore>::get_server_info(catalog.clone())
        .await?
        .server_id();
    let authorizer = AuthorizerEnum::init_from_env(server_id).await?;
    let stats = vec![stats];

    match authorizer {
        AuthorizerEnum::AllowAll(authz) => {
            tracing::info!("Using AllowAll authorizer");
            serve_with_authn::<PostgresBackend, _, _>(bind_addr, secrets, catalog, authz, stats)
                .await
        }
        AuthorizerEnum::OpenFGA(authz) => {
            tracing::info!("Using OpenFGA authorizer");
            serve_with_authn::<PostgresBackend, _, _>(bind_addr, secrets, catalog, *authz, stats)
                .await
        }
    }
}

async fn serve_with_authn<C: CatalogStore, S: SecretStore, A: Authorizer>(
    bind: std::net::SocketAddr,
    secret: S,
    catalog: C::State,
    authz: A,
    stats: Vec<Arc<dyn EndpointStatisticsSink + 'static>>,
) -> anyhow::Result<()> {
    let authentication = get_default_authenticator_from_config().await?;

    match authentication {
        None => {
            serve_inner::<C, _, _, AuthenticatorEnum>(bind, secret, catalog, authz, None, stats)
                .await
        }
        Some(BuiltInAuthenticators::Chain(authn)) => {
            serve_inner::<C, _, _, _>(bind, secret, catalog, authz, Some(authn), stats).await
        }
        Some(BuiltInAuthenticators::Single(authn)) => {
            serve_inner::<C, _, _, _>(bind, secret, catalog, authz, Some(authn), stats).await
        }
    }
}

async fn serve_inner<C: CatalogStore, S: SecretStore, A: Authorizer, N: Authenticator + 'static>(
    bind: std::net::SocketAddr,
    secrets: S,
    catalog: C::State,
    authorizer: A,
    authenticator: Option<N>,
    stats: Vec<Arc<dyn EndpointStatisticsSink + 'static>>,
) -> anyhow::Result<()> {
    let cloud_event_sinks = get_default_cloud_event_backends_from_config().await?;

    let config = ServeConfiguration::<C, _, _, _>::builder()
        .bind_addr(bind)
        .secrets_state(secrets)
        .catalog_state(catalog)
        .authorizer(authorizer)
        .authenticator(authenticator)
        .stats(stats)
        .modify_router_fn(Some(add_ui_routes))
        .cloud_event_sinks(cloud_event_sinks)
        .build();

    serve(config).await
}

fn add_ui_routes(router: lakekeeper::axum::Router) -> lakekeeper::axum::Router {
    #[cfg(feature = "ui")]
    {
        let ui_router = ui::get_ui_router();
        router.merge(ui_router)
    }

    #[cfg(not(feature = "ui"))]
    router
}
