use std::{sync::Arc, vec};

use anyhow::anyhow;
#[cfg(feature = "ui")]
use axum::routing::get;
use lakekeeper::{
    api::router::{new_full_router, serve as service_serve, RouterArgs},
    implementations::{get_default_catalog_from_config, postgres::PostgresCatalog},
    service::{
        authn::{get_default_authenticator_from_config, BuiltInAuthenticators},
        authz::{
            implementations::{get_default_authorizer_from_config, BuiltInAuthorizers},
            Authorizer,
        },
        contract_verification::ContractVerifiers,
        endpoint_hooks::EndpointHookCollection,
        endpoint_statistics::{
            EndpointStatisticsMessage, EndpointStatisticsSink, EndpointStatisticsTracker, FlushMode,
        },
        event_publisher::{
            get_default_cloud_event_backends_from_config, CloudEventsMessage, CloudEventsPublisher,
            CloudEventsPublisherBackgroundTask,
        },
        health::ServiceHealthProvider,
        task_queue::TaskQueueRegistry,
        Catalog, EndpointStatisticsTrackerTx, SecretStore, StartupValidationData,
    },
    CONFIG,
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

pub(crate) async fn serve_with_authn<C: Catalog, S: SecretStore, A: Authorizer>(
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
    serve::<C, _, _, _>(
        bind,
        secrets,
        catalog,
        authorizer,
        authenticator,
        stats,
        ContractVerifiers::new(vec![]),
        Some(add_ui_routes),
    )
    .await
}

fn add_ui_routes(router: axum::Router) -> axum::Router {
    #[cfg(feature = "ui")]
    let router = router
        .route(
            "/ui",
            get(|| async { axum::response::Redirect::permanent("/ui/") }),
        )
        .route(
            "/",
            get(|| async { axum::response::Redirect::permanent("/ui/") }),
        )
        .route(
            "/ui/index.html",
            get(|| async { axum::response::Redirect::permanent("/ui/") }),
        )
        .route("/ui/", get(ui::index_handler))
        .route("/ui/favicon.ico", get(ui::favicon_handler))
        .route("/ui/assets/{*file}", get(ui::static_handler))
        .route("/ui/{*file}", get(ui::index_handler));
    #[cfg(not(feature = "ui"))]
    let router = router;

    router
}

pub(crate) async fn serve<C: Catalog, S: SecretStore, A: Authorizer, N: Authenticator + 'static>(
    bind_addr: std::net::SocketAddr,
    secrets_state: S,
    catalog_state: C::State,
    authorizer: A,
    authenticator: Option<N>,
    stats: Vec<Arc<dyn EndpointStatisticsSink + 'static>>,
    contract_verification: ContractVerifiers,
    modify_router_fn: Option<fn(axum::Router) -> axum::Router>,
) -> Result<(), anyhow::Error> {
    let validation_data = C::get_server_info(catalog_state.clone()).await?;
    match validation_data {
        StartupValidationData::NotBootstrapped => {
            tracing::info!("The catalog is not bootstrapped. Bootstrapping sets the initial administrator. Please open the Web-UI after startup or call the bootstrap endpoint directly.");
        }
        StartupValidationData::Bootstrapped {
            server_id,
            terms_accepted,
        } => {
            if !terms_accepted {
                return Err(anyhow!(
                    "The terms of service have not been accepted on bootstrap."
                ));
            }
            if server_id != CONFIG.server_id {
                return Err(anyhow!(
                    "The server ID during bootstrap {} does not match the server ID in the configuration {}.", server_id, CONFIG.server_id
                ));
            }
            tracing::info!("The catalog is bootstrapped. Server ID: {server_id}");
        }
    }

    let health_provider = ServiceHealthProvider::new(
        vec![
            ("catalog", Arc::new(catalog_state.clone())),
            ("secrets", Arc::new(secrets_state.clone())),
            ("auth", Arc::new(authorizer.clone())),
        ],
        CONFIG.health_check_frequency_seconds,
        CONFIG.health_check_jitter_millis,
    );
    health_provider.spawn_update_heath_checks().await;

    let listener = tokio::net::TcpListener::bind(bind_addr)
        .await
        .map_err(|e| anyhow!(e).context(format!("Failed to bind to address: {bind_addr}")))?;

    // Cloud events publisher setup
    let (cloud_events_tx, cloud_events_rx) = tokio::sync::mpsc::channel(1000);
    let cloud_event_sinks = get_default_cloud_event_backends_from_config().await?;
    let cloud_events_background_task: CloudEventsPublisherBackgroundTask =
        CloudEventsPublisherBackgroundTask {
            source: cloud_events_rx,
            sinks: cloud_event_sinks,
        };

    // Metrics server
    let (layer, metrics_future) = lakekeeper::metrics::get_axum_layer_and_install_recorder(
        CONFIG.metrics_port,
    )
    .map_err(|e| {
        anyhow!(e).context(format!(
            "Failed to start metrics server on port: {}",
            CONFIG.metrics_port
        ))
    })?;

    // Endpoint stats
    let (endpoint_statistics_tx, endpoint_statistics_rx) = tokio::sync::mpsc::channel(1000);
    let tracker = EndpointStatisticsTracker::new(
        endpoint_statistics_rx,
        stats,
        CONFIG.endpoint_stat_flush_interval,
        FlushMode::Automatic,
    );
    let endpoint_statistics_tracker_tx = EndpointStatisticsTrackerTx::new(endpoint_statistics_tx);
    let hooks = EndpointHookCollection::new(vec![Arc::new(CloudEventsPublisher::new(
        cloud_events_tx.clone(),
    ))]);
    let mut task_queue_registry = TaskQueueRegistry::new();
    task_queue_registry.register_built_in_queues::<C, _, _>(
        catalog_state.clone(),
        secrets_state.clone(),
        authorizer.clone(),
        CONFIG.task_poll_interval,
    );

    let mut router = new_full_router::<C, _, _, _>(RouterArgs {
        authenticator: authenticator.clone(),
        authorizer: authorizer.clone(),
        catalog_state: catalog_state.clone(),
        secrets_state: secrets_state.clone(),
        table_change_checkers: contract_verification,
        service_health_provider: health_provider,
        cors_origins: CONFIG.allow_origin.as_deref(),
        metrics_layer: Some(layer),
        endpoint_statistics_tracker_tx: endpoint_statistics_tracker_tx.clone(),
        hooks,
        registered_task_queues: task_queue_registry.registered_task_queues(),
    })?;

    if let Some(modify_router_fn) = modify_router_fn {
        router = modify_router_fn(router);
    }

    let publisher_handle = tokio::task::spawn(async move {
        match cloud_events_background_task.publish().await {
            Ok(_) => tracing::info!("Exiting publisher task"),
            Err(e) => tracing::error!("Publisher task failed: {e}"),
        };
    });
    let stats_handle = tokio::task::spawn(tracker.run());

    let task_runner = task_queue_registry.task_queues_runner();
    tokio::select!(
        _ = task_runner.run_queue_workers(true) => tracing::error!("Task queues failed."),
        err = service_serve(listener, router) => tracing::error!("Service failed: {err:?}"),
        _ = metrics_future => tracing::error!("Metrics server failed"),
    );

    tracing::debug!("Sending shutdown signal to event publisher.");
    endpoint_statistics_tracker_tx
        .send(EndpointStatisticsMessage::Shutdown)
        .await?;
    cloud_events_tx.send(CloudEventsMessage::Shutdown).await?;

    // Wait for queues to finish processing
    publisher_handle.await?;
    stats_handle.await?;
    Ok(())
}
