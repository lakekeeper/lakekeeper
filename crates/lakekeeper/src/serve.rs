use std::{sync::Arc, vec};

use anyhow::anyhow;
use futures::stream::{FuturesUnordered, StreamExt};
use limes::{Authenticator, AuthenticatorEnum};

use crate::{
    api::router::{new_full_router, serve as service_serve, RouterArgs},
    service::{
        authz::{AllowAllAuthorizer, Authorizer},
        contract_verification::ContractVerifiers,
        endpoint_hooks::EndpointHookCollection,
        endpoint_statistics::{
            EndpointStatisticsMessage, EndpointStatisticsSink, EndpointStatisticsTracker, FlushMode,
        },
        event_publisher::{
            CloudEventBackend, CloudEventsMessage, CloudEventsPublisher,
            CloudEventsPublisherBackgroundTask,
        },
        health::ServiceHealthProvider,
        task_queue::TaskQueueRegistry,
        Catalog, EndpointStatisticsTrackerTx, SecretStore, ServerInfo,
    },
    CONFIG,
};

#[derive(Debug, typed_builder::TypedBuilder)]
pub struct ServeConfiguration<
    C: Catalog,
    S: SecretStore,
    A: Authorizer = AllowAllAuthorizer,
    N: Authenticator + 'static = AuthenticatorEnum,
> {
    /// The address to bind the service to
    pub bind_addr: std::net::SocketAddr,
    /// The secret store state
    pub secrets_state: S,
    /// The catalog state
    pub catalog_state: C::State,
    /// The authorizer to use for access control
    pub authorizer: A,
    #[builder(default)]
    /// The authenticator to use for authentication
    pub authenticator: Option<N>,
    #[builder(default)]
    /// A list of statistics sinks to collect endpoint statistics
    pub stats: Vec<Arc<dyn EndpointStatisticsSink + 'static>>,
    #[builder(default)]
    /// Contract verifiers that can prohibit invalid table changes
    pub contract_verification: ContractVerifiers,
    #[builder(default)]
    /// A function to modify the router before serving
    pub modify_router_fn: Option<fn(axum::Router) -> axum::Router>,
    /// Cloud events sinks / publishers
    #[builder(default)]
    pub cloud_event_sinks: Vec<Arc<dyn CloudEventBackend + Send + Sync + 'static>>,
    /// Enable built-in queue workers
    #[builder(default = true)]
    pub enable_built_in_task_queues: bool,
    /// Additional task queues to run. Tuples of type:
    #[builder(default)]
    pub register_additional_task_queues_fn: Option<fn(&mut TaskQueueRegistry)>,
    /// Additional endpoint hooks to register.
    /// Emitting cloud events is always registered.
    #[builder(default)]
    pub additional_endpoint_hooks: Option<EndpointHookCollection>,
    /// Additional background services / futures to await.
    /// If any of these futures fail, the service will gracefully shut down and exit.
    #[builder(default)]
    pub additional_background_services: Vec<tokio::task::JoinHandle<()>>,
}

/// Starts the service with the provided configuration.
///
/// # Errors
/// - If the service cannot bind to the specified address.
/// - If the server is bootstrapped but the server ID does not match the configuration.
/// - If the terms of service have not been accepted during bootstrap.
#[allow(clippy::too_many_lines)]
pub async fn serve<C: Catalog, S: SecretStore, A: Authorizer, N: Authenticator + 'static>(
    config: ServeConfiguration<C, S, A, N>,
) -> anyhow::Result<()> {
    let ServeConfiguration {
        bind_addr,
        secrets_state,
        catalog_state,
        authorizer,
        authenticator,
        stats,
        contract_verification,
        modify_router_fn,
        cloud_event_sinks,
        enable_built_in_task_queues: enable_built_in_queues,
        register_additional_task_queues_fn,
        additional_endpoint_hooks,
        additional_background_services,
    } = config;

    let cancellation_token = tokio_util::sync::CancellationToken::new();
    let listener = tokio::net::TcpListener::bind(bind_addr)
        .await
        .map_err(|e| anyhow!(e).context(format!("Failed to bind to address: {bind_addr}")))?;

    // Validate ServerInfo, exit if ServerID does not match or terms are not accepted
    let server_info = C::get_server_info(catalog_state.clone()).await?;
    validate_server_info(&server_info)?;

    // Health checks
    let health_provider = ServiceHealthProvider::new(
        vec![
            ("catalog", Arc::new(catalog_state.clone())),
            ("secrets", Arc::new(secrets_state.clone())),
            ("auth", Arc::new(authorizer.clone())),
        ],
        CONFIG.health_check_frequency_seconds,
    );

    // Cloud events publisher setup
    let (cloud_events_tx, cloud_events_rx) = tokio::sync::mpsc::channel(1000);
    let cloud_events_background_task = CloudEventsPublisherBackgroundTask {
        source: cloud_events_rx,
        sinks: cloud_event_sinks,
    };

    // Metrics server
    let (layer, metrics_future) =
        crate::metrics::get_axum_layer_and_install_recorder(CONFIG.metrics_port).map_err(|e| {
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

    // Endpoint Hooks
    let mut hooks = additional_endpoint_hooks.unwrap_or(EndpointHookCollection::new(vec![]));
    hooks.append(Arc::new(CloudEventsPublisher::new(cloud_events_tx.clone())));

    // Task queues
    let mut task_queue_registry = TaskQueueRegistry::new();
    if enable_built_in_queues {
        task_queue_registry.register_built_in_queues::<C, _, _>(
            catalog_state.clone(),
            secrets_state.clone(),
            authorizer.clone(),
            CONFIG.task_poll_interval,
        );
    }
    if let Some(register_fn) = register_additional_task_queues_fn {
        register_fn(&mut task_queue_registry);
    }

    // Router
    let mut router = new_full_router::<C, _, _, _>(RouterArgs {
        authenticator: authenticator.clone(),
        authorizer: authorizer.clone(),
        catalog_state: catalog_state.clone(),
        secrets_state: secrets_state.clone(),
        table_change_checkers: contract_verification,
        service_health_provider: health_provider.clone(),
        cors_origins: CONFIG.allow_origin.as_deref(),
        metrics_layer: Some(layer),
        endpoint_statistics_tracker_tx: endpoint_statistics_tracker_tx.clone(),
        hooks,
        registered_task_queues: task_queue_registry.registered_task_queues(),
    })?;

    if let Some(modify_router_fn) = modify_router_fn {
        router = modify_router_fn(router);
    }

    // Launch background services
    let health_handles = health_provider
        .spawn_update_heath_checks(cancellation_token.clone())
        .await;
    let publisher_handle = tokio::task::spawn(async move {
        match cloud_events_background_task.publish().await {
            Ok(()) => tracing::info!("Exiting publisher task"),
            Err(e) => tracing::error!("Publisher task failed: {e}"),
        }
    });
    let stats_handle = tokio::task::spawn(tracker.run());

    let task_runner = task_queue_registry.task_queues_runner();

    // Convert health handles Vec into FuturesUnordered for concurrent monitoring
    let mut health_handles_stream = FuturesUnordered::new();
    for handle in health_handles {
        health_handles_stream.push(handle);
    }

    // Execute additional background services
    let mut additional_services_futures = FuturesUnordered::new();
    for service in additional_background_services {
        additional_services_futures.push(service);
    }

    tokio::select!(
        () = task_runner.run_queue_workers(true) => tracing::error!("Task queues failed."),
        err = service_serve(listener, router) => tracing::error!("Service failed: {err:?}"),
        _ = metrics_future => tracing::error!("Metrics server failed"),
        Some(_) = health_handles_stream.next() => tracing::error!("Health check thread failed."),
        Some(_) = additional_services_futures.next() => tracing::error!("An additional background service finished unexpectedly."),
    );

    tracing::debug!("Sending shutdown signal to threads");
    cancellation_token.cancel();
    endpoint_statistics_tracker_tx
        .send(EndpointStatisticsMessage::Shutdown)
        .await?;
    cloud_events_tx.send(CloudEventsMessage::Shutdown).await?;

    // Wait for queues to finish processing
    publisher_handle.await?;
    stats_handle.await?;
    Ok(())
}

fn validate_server_info(server_info: &ServerInfo) -> anyhow::Result<()> {
    match server_info {
        ServerInfo::NotBootstrapped => {
            tracing::info!("The catalog is not bootstrapped. Bootstrapping sets the initial administrator. Please open the Web-UI after startup or call the bootstrap endpoint directly.");
            Ok(())
        }
        ServerInfo::Bootstrapped {
            server_id,
            terms_accepted,
        } => {
            if !terms_accepted {
                Err(anyhow!(
                    "The terms of service have not been accepted on bootstrap."
                ))
            } else if *server_id != CONFIG.server_id {
                Err(anyhow!(
                    "The server ID during bootstrap {} does not match the server ID in the configuration {}.",
                    server_id, CONFIG.server_id
                ))
            } else {
                tracing::info!("The catalog is bootstrapped. Server ID: {server_id}");
                Ok(())
            }
        }
    }
}
