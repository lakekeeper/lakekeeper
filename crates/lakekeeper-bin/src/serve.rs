use std::sync::Arc;

use lakekeeper::{
    SecretBackend,
    limes::{Authenticator, AuthenticatorEnum},
    serve::{ServeConfiguration, serve},
    service::{
        CatalogStore, SecretStore,
        authn::{BuiltInAuthenticators, get_default_authenticator_from_config},
        authz::Authorizer,
        endpoint_statistics::EndpointStatisticsSink,
        events::{EventDispatcher, get_default_cloud_event_backends_from_config},
    },
    tracing,
};
use lakekeeper_storage_postgres::{
    CatalogState, PostgresBackend, PostgresStatisticsSink, SecretsState as PgSecretsState,
    get_reader_pool, get_writer_pool,
};

#[cfg(feature = "ui")]
use crate::ui;
use crate::{authorizer::AuthorizerEnum, secrets::SecretsEnum};

pub(crate) async fn serve_default(bind_addr: std::net::SocketAddr) -> anyhow::Result<()> {
    let (catalog, secrets, stats) = get_default_catalog_from_config().await?;
    let server_id = <PostgresBackend as CatalogStore>::get_server_info(catalog.clone())
        .await?
        .server_id();
    // Events implement interior mutability.
    let events = EventDispatcher::new(vec![]);
    let authorizer = AuthorizerEnum::init_from_env(server_id).await?;
    let stats = vec![stats];

    match authorizer {
        AuthorizerEnum::AllowAll(authz) => {
            tracing::info!("Using AllowAll authorizer");
            serve_with_authn::<PostgresBackend, _, _>(
                bind_addr, secrets, catalog, authz, stats, events,
            )
            .await
        }
        AuthorizerEnum::OpenFGA(authz) => {
            tracing::info!("Using OpenFGA authorizer");
            serve_with_authn::<PostgresBackend, _, _>(
                bind_addr, secrets, catalog, *authz, stats, events,
            )
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
    events: EventDispatcher,
) -> anyhow::Result<()> {
    // Use the upstream config-driven authenticator
    // Supports both single-provider (OPENID_PROVIDER_URI) and multi-provider (OPENID_PROVIDERS) modes
    let authentication = get_default_authenticator_from_config().await?;

    match authentication {
        None => {
            serve_inner::<C, _, _, AuthenticatorEnum>(
                bind, secret, catalog, authz, None, stats, events,
            )
            .await
        }
        Some(BuiltInAuthenticators::Chain(authn)) => {
            serve_inner::<C, _, _, _>(bind, secret, catalog, authz, Some(authn), stats, events)
                .await
        }
        Some(BuiltInAuthenticators::Single(authn)) => {
            serve_inner::<C, _, _, _>(bind, secret, catalog, authz, Some(authn), stats, events)
                .await
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
    events: EventDispatcher,
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
        .event_dispatcher(Some(events))
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

/// Build the default catalog state + secrets backend + endpoint statistics
/// sink from the binary's runtime configuration.
async fn get_default_catalog_from_config() -> anyhow::Result<(
    CatalogState,
    SecretsEnum,
    Arc<dyn EndpointStatisticsSink + 'static>,
)> {
    use lakekeeper_storage_postgres::config::{CONFIG as PG_CONFIG, DEFAULT_ENCRYPTION_KEY};

    if lakekeeper::CONFIG.secret_backend == SecretBackend::Postgres
        && PG_CONFIG.pg_encryption_key == DEFAULT_ENCRYPTION_KEY
    {
        tracing::warn!(
            "THIS IS UNSAFE! Using default encryption key for secrets in postgres, \
             please set a proper key using LAKEKEEPER__PG_ENCRYPTION_KEY environment variable."
        );
    }

    let read_pool = get_reader_pool(
        PG_CONFIG
            .to_pool_opts()
            .max_connections(PG_CONFIG.pg_read_pool_connections),
    )
    .await?;
    let write_pool = get_writer_pool(
        PG_CONFIG
            .to_pool_opts()
            .max_connections(PG_CONFIG.pg_write_pool_connections),
    )
    .await?;

    let catalog_state = CatalogState::from_pools(read_pool.clone(), write_pool.clone());

    let secrets_state: SecretsEnum = match lakekeeper::CONFIG.secret_backend {
        SecretBackend::KV2 => lakekeeper_secrets_kv2::SecretsState::from_config(
            lakekeeper_secrets_kv2::config::CONFIG
                .kv2
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Need vault config to use vault as backend"))?,
        )
        .await?
        .into(),
        SecretBackend::Postgres => {
            PgSecretsState::from_pools(read_pool.clone(), write_pool.clone()).into()
        }
    };

    let stats_sink = Arc::new(PostgresStatisticsSink::new(
        catalog_state.read_pool(),
        catalog_state.write_pool(),
    ));

    Ok((catalog_state, secrets_state, stats_sink))
}
