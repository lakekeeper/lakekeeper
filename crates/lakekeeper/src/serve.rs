use std::{collections::HashMap, sync::Arc, vec};

use anyhow::anyhow;
use limes::{Authenticator, AuthenticatorEnum};
use tokio::task::{AbortHandle, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::{
    api::{
        router::{new_full_router, serve as service_serve, RouterArgs},
        shutdown_signal,
    },
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

/// Type alias for a function that registers additional background services.
///
/// # Arguments
/// - `JoinSet`: A set of tasks which should be used to spawn the background service.
/// - `CancellationToken`: A token to signal cancellation of the background service.
///
/// # Returns
/// - `Vec<(String, tokio::task::AbortHandle)>`: A vector of tuples containing the name of the service and its associated abort handle.
pub type RegisterBackgroundServiceFn =
    fn(&mut JoinSet<Result<(), anyhow::Error>>, CancellationToken) -> Vec<(String, AbortHandle)>;

/// Helper function to process the result of a service task completion
fn handle_service_completion(
    result: Result<(tokio::task::Id, Result<(), anyhow::Error>), tokio::task::JoinError>,
    service_abort_handles: &mut HashMap<tokio::task::Id, String>,
    during_shutdown: bool,
) {
    match result {
        Ok((id, task_result)) => {
            let task_name = service_abort_handles
                .remove(&id)
                .unwrap_or_else(|| format!("Unknown Service with ID {id}"));
            match task_result {
                Ok(()) => {
                    if during_shutdown {
                        tracing::info!("Service '{task_name}' finished gracefully during shutdown");
                    } else {
                        tracing::info!("Service '{task_name}' finished successfully but was supposed to run indefinitely");
                    }
                }
                Err(e) => {
                    if during_shutdown {
                        tracing::warn!(
                            "Service '{task_name}' exited with error during shutdown: {e}"
                        );
                    } else {
                        tracing::error!("Service '{task_name}' exited with error: {e}");
                    }
                }
            }
        }
        Err(join_err) => {
            if during_shutdown {
                tracing::warn!("Service join error during shutdown: {join_err}");
            } else {
                tracing::error!("Service join error: {join_err}");
            }
        }
    }
}

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
    #[builder(default)]
    pub additional_background_services: Vec<RegisterBackgroundServiceFn>,
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

    let cancellation_token = CancellationToken::new();
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
    let (layer, metrics_future) = crate::metrics::get_axum_layer_and_install_recorder(
        CONFIG.metrics_port,
        cancellation_token.clone(),
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

    // ---- Launch background services ----
    // Strings are name of the service, used for logging
    let mut service_futures = JoinSet::<Result<(), anyhow::Error>>::new();
    let mut service_ids = HashMap::new();

    // Sigint / Sigterm handler:
    let cancellation_token_clone = cancellation_token.clone();
    let shutdown_signal_handle = service_futures.spawn(async move {
        shutdown_signal(cancellation_token_clone).await;
        Err(anyhow!("Shutdown signal received"))
    });
    service_ids.insert(
        shutdown_signal_handle.id(),
        "Shutdown Signal Handler".to_string(),
    );

    // Metrics server:
    let metrics_handle = service_futures.spawn(async move {
        metrics_future
            .await
            .map_err(|e| anyhow!(e).context("Metrics Services exited with error"))
    });
    service_ids.insert(metrics_handle.id(), "Metrics Server".to_string());

    // Periodic health checks:
    let health_abort_handles =
        health_provider.spawn_update_health_checks(&mut service_futures, &cancellation_token);
    for (service_name, abort_handle) in health_abort_handles {
        service_ids.insert(abort_handle.id(), service_name);
    }

    // Cloud events publisher:
    let ce_abort_handle = service_futures.spawn(async move {
        cloud_events_background_task
            .publish()
            .await
            .map_err(|e| anyhow!(e).context("Event publisher exited with error"))
    });
    service_ids.insert(ce_abort_handle.id(), "Event Publisher".to_string());

    // Endpoint statistics tracker:
    let tracker_abort_handle = service_futures.spawn(async move {
        tracker.run().await;
        Ok(())
    });
    service_ids.insert(
        tracker_abort_handle.id(),
        "Endpoint Statistics Tracker".to_string(),
    );

    // Execute additional background services:
    for additional_service_register_fn in additional_background_services {
        let abort_handles =
            additional_service_register_fn(&mut service_futures, cancellation_token.clone());
        for (service_name, abort_handle) in abort_handles {
            tracing::info!("Spawned background service: {service_name}");
            service_ids.insert(abort_handle.id(), service_name);
        }
    }

    // Task Queues:
    let task_runner = task_queue_registry.task_queues_runner(cancellation_token.clone());
    if task_queue_registry.is_empty() {
        tracing::info!("No task queues registered, skipping task queue worker startup");
    } else {
        let task_abort_handle = service_futures.spawn(async move {
            task_runner.run_queue_workers(true).await;
            Ok(())
        });
        service_ids.insert(task_abort_handle.id(), "Task Worker Monitor".to_string());
    }

    // HTTP Server / Axum:
    let cancellation_token_clone = cancellation_token.clone();
    let axum_abort_handle = service_futures.spawn(async move {
        service_serve(listener, router, cancellation_token_clone)
            .await
            .map_err(|e| anyhow!(e).context("Axum server exited with error"))
    });
    service_ids.insert(axum_abort_handle.id(), "Axum Server".to_string());

    tracing::info!("All background services started. Lakekeeper is now running.");
    match service_futures.join_next_with_id().await {
        Some(result) => {
            handle_service_completion(result, &mut service_ids, false);
        }
        None => {
            tracing::error!("No services were started, exiting.");
        }
    }

    tracing::debug!("Sending shutdown signal to threads");
    cancellation_token.cancel();

    endpoint_statistics_tracker_tx
        .send(EndpointStatisticsMessage::Shutdown)
        .await?;
    cloud_events_tx.send(CloudEventsMessage::Shutdown).await?;

    // Wait for remaining tasks to finish, wait at most 20 seconds, then print which tasks are still running
    // but exit anyway. Report progress every 5 seconds.
    let shutdown_timeout_secs = 20;
    let report_interval_secs = 5;
    let start_time = std::time::Instant::now();

    tracing::info!("Waiting up to {shutdown_timeout_secs} seconds for {} background services to finish gracefully", service_ids.len());

    let timeout = tokio::time::timeout(
        std::time::Duration::from_secs(shutdown_timeout_secs),
        async {
            let mut last_report = std::time::Instant::now();

            while let Some(result) = service_futures.join_next_with_id().await {
                handle_service_completion(result, &mut service_ids, true);

                // Report progress every 5 seconds
                if last_report.elapsed() >= std::time::Duration::from_secs(report_interval_secs) {
                    let elapsed = start_time.elapsed().as_secs();
                    let remaining = shutdown_timeout_secs.saturating_sub(elapsed);
                    let running_services = service_ids.values().cloned().collect::<Vec<_>>();

                    if !running_services.is_empty() {
                        tracing::info!(
                        "Shutdown progress: {} seconds elapsed, {} seconds remaining. Still waiting for {} services: {:?}",
                        elapsed,
                        remaining,
                        running_services.len(),
                        running_services
                    );
                    }
                    last_report = std::time::Instant::now();
                }
            }
        },
    );

    if let Ok(()) = timeout.await {
        let elapsed = start_time.elapsed().as_secs();
        tracing::info!(
            "All background services finished gracefully within {} seconds",
            elapsed
        );
    } else {
        let running_services = service_ids.values().cloned().collect::<Vec<_>>();

        tracing::warn!(
            "Timeout reached after {} seconds waiting for background services to finish. Still running services: {:?}",
            shutdown_timeout_secs,
            running_services
        );
        // Abort all remaining tasks
        service_futures.abort_all();

        // Give a brief moment for abort to take effect
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        tracing::info!("Aborted all remaining background services");
    }

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
