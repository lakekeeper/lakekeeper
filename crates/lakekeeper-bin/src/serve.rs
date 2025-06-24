use std::{sync::Arc, vec};

#[cfg(feature = "ui")]
use axum::routing::get;
use lakekeeper::{
    implementations::{get_default_catalog_from_config, postgres::PostgresCatalog},
    serve::{serve, ServeConfiguration},
    service::{
        authn::{get_default_authenticator_from_config, BuiltInAuthenticators},
        authz::{
            implementations::{get_default_authorizer_from_config, BuiltInAuthorizers},
            Authorizer,
        },
        endpoint_statistics::EndpointStatisticsSink,
        event_publisher::get_default_cloud_event_backends_from_config,
        Catalog, SecretStore,
    },
};
use limes::{Authenticator, AuthenticatorEnum};

#[cfg(feature = "ui")]
use crate::ui;

pub(crate) async fn serve_default(bind_addr: std::net::SocketAddr) -> anyhow::Result<()> {
    let (catalog, secrets, stats) = get_default_catalog_from_config().await?;
    let authorizer = get_default_authorizer_from_config().await?;
    let stats = vec![stats];

    match authorizer {
        BuiltInAuthorizers::AllowAll(authz) => {
            serve_with_authn::<PostgresCatalog, _, _>(bind_addr, secrets, catalog, authz, stats)
                .await
        }
        BuiltInAuthorizers::OpenFGA(authz) => {
            serve_with_authn::<PostgresCatalog, _, _>(bind_addr, secrets, catalog, authz, stats)
                .await
        }
    }
}

async fn serve_with_authn<C: Catalog, S: SecretStore, A: Authorizer>(
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

async fn serve_inner<C: Catalog, S: SecretStore, A: Authorizer, N: Authenticator + 'static>(
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

fn add_ui_routes(router: axum::Router) -> axum::Router {
    #[cfg(feature = "ui")]
    let router = router
        .route("/ui", get(redirect_to_ui))
        .route("/", get(redirect_to_ui))
        .route("/ui/index.html", get(redirect_to_ui))
        .route("/ui/", get(ui::index_handler))
        .route("/ui/favicon.ico", get(ui::favicon_handler))
        .route("/ui/assets/{*file}", get(ui::static_handler))
        .route("/ui/{*file}", get(ui::index_handler));
    #[cfg(not(feature = "ui"))]
    let router = router;

    router
}

#[cfg(feature = "ui")]
async fn redirect_to_ui(headers: axum::http::HeaderMap) -> axum::response::Redirect {
    if let Some(prefix) = lakekeeper::determine_forwarded_prefix(&headers) {
        axum::response::Redirect::permanent(format!("/{prefix}/ui/").as_str())
    } else {
        axum::response::Redirect::permanent("/ui/")
    }
}
