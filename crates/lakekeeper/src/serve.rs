use std::{collections::HashMap, sync::Arc, vec};

use anyhow::anyhow;
use futures::future::BoxFuture;
use limes::{Authenticator, AuthenticatorEnum};
use tokio::task::{AbortHandle, JoinSet};

use crate::{
    CONFIG, CancellationToken,
    api::{
        ApiContext,
        management::v1::{
            extension::ArcManagementApiExtension,
            server::{APACHE_LICENSE_STATUS, BuildInfo, DEFAULT_BUILD_INFO, LicenseStatus},
        },
        router::{RouterArgs, new_full_router, new_health_router, serve as service_serve},
    },
    metrics::MetricsServer,
    service::{
        CatalogStore, EndpointStatisticsTrackerTx, RoleProviderId, SecretStore, ServerInfo, State,
        admission::AdmissionGates,
        authz::{AllowAllAuthorizer, Authorizer, ConfiguredInstanceAdmins},
        contract_verification::ContractVerifiers,
        endpoint_statistics::{
            EndpointStatisticsMessage, EndpointStatisticsSink, EndpointStatisticsTracker, FlushMode,
        },
        events::{
            CloudEventBackend, CloudEventsMessage, CloudEventsPublisher,
            CloudEventsPublisherBackgroundTask, EventDispatcher,
            backends::audit::AuditEventListener,
        },
        health::ServiceHealthProvider,
        tasks::{
            TaskQueueRegistry,
            task_drain::{
                ArcTaskRunnerControl, DrainSettings, stop_signals, wait_for_stop_and_drain,
            },
        },
    },
};

/// The service whose end stops the process: it waits for a stop signal or timer and drains the
/// task workers.
struct StopService {
    id: tokio::task::Id,
    task_control: ArcTaskRunnerControl,
}

/// Type alias for a function that registers additional background services.
///
/// # Arguments
/// - `JoinSet`: A collection which should be used to spawn the background service.
/// - `CancellationToken`: A token to signal cancellation of the background service.
///
/// # Returns
/// - `Vec<(String, tokio::task::AbortHandle)>`: A vector of tuples containing the name of the service and its associated abort handle.
pub type RegisterBackgroundServiceFn<A, C, S> = Box<
    dyn FnOnce(
        &mut JoinSet<Result<(), anyhow::Error>>,
        CancellationToken,
        ApiContext<State<A, C, S>>,
    ) -> BoxFuture<'_, anyhow::Result<Vec<(String, AbortHandle)>>>,
>;

pub type RegisterTaskQueueFn<A, C, S> = Box<
    dyn FnOnce(
        TaskQueueRegistry,
        ApiContext<State<A, C, S>>,
    ) -> BoxFuture<'static, anyhow::Result<()>>,
>;

/// Helper function to process the result of a service task completion
fn log_service_completion<H: ::std::hash::BuildHasher>(
    result: &Result<(tokio::task::Id, Result<(), anyhow::Error>), tokio::task::JoinError>,
    service_abort_handles: &mut HashMap<tokio::task::Id, String, H>,
    during_shutdown: bool,
) -> String {
    match result {
        Ok((id, task_result)) => {
            let task_name = service_abort_handles
                .remove(id)
                .unwrap_or_else(|| format!("Unknown Service with ID {id}"));
            match task_result {
                Ok(()) => {
                    if during_shutdown {
                        let msg =
                            format!("Service '{task_name}' finished gracefully during shutdown");
                        tracing::info!("{msg}");
                        msg
                    } else {
                        let msg = format!(
                            "Service '{task_name}' finished successfully but was supposed to run indefinitely"
                        );
                        tracing::info!("{msg}");
                        msg
                    }
                }
                Err(e) => {
                    if during_shutdown {
                        let msg =
                            format!("Service '{task_name}' exited with error during shutdown: {e}");
                        tracing::warn!("{msg}");
                        msg
                    } else {
                        let msg = format!("Service '{task_name}' exited with error: {e}");
                        tracing::error!("{msg}");
                        msg
                    }
                }
            }
        }
        Err(join_err) => {
            if during_shutdown {
                let msg = format!("Service join error during shutdown: {join_err}");
                tracing::warn!("{msg}");
                msg
            } else {
                let msg = format!("Service join error: {join_err}");
                tracing::error!("{msg}");
                msg
            }
        }
    }
}

#[derive(derive_more::Debug, typed_builder::TypedBuilder)]
pub struct ServeConfiguration<
    C: CatalogStore,
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
    /// Post-authentication admission gates. Empty by default (admits every
    /// request). Downstream binaries may register gates that reject already
    /// authenticated principals before they reach any handler — e.g. an
    /// external control-plane permission check.
    pub admission_gates: AdmissionGates,
    #[builder(default)]
    /// A function to modify the router before serving
    pub modify_router_fn: Option<fn(axum::Router) -> axum::Router>,
    /// Routes added to the management API. They are authenticated and counted in endpoint
    /// statistics like Lakekeeper's own routes; each handler authorizes and audits its request.
    #[builder(default)]
    pub management_api_extensions: Vec<ArcManagementApiExtension<A, C, S>>,
    /// Cloud events sinks / publishers.
    ///
    /// The cloud-event publisher is registered only when this is non-empty.
    /// Listeners added through `event_dispatcher` are unaffected.
    #[builder(default)]
    pub cloud_event_sinks: Vec<Arc<dyn CloudEventBackend + Send + Sync + 'static>>,
    /// Enable built-in queue workers
    #[builder(default = true)]
    pub enable_built_in_task_queues: bool,
    /// Additional task queues to run. Tuples of type:
    #[builder(default)]
    #[debug("Vec with {} functions", register_additional_task_queues_fn.len())]
    pub register_additional_task_queues_fn: Vec<RegisterTaskQueueFn<A, C, S>>,
    /// Additional event listeners to register.
    /// Emitting cloud events is always registered.
    #[builder(default)]
    pub event_dispatcher: Option<EventDispatcher>,
    /// Additional background services / futures to await.
    #[builder(default)]
    #[debug("Vec with {} functions", register_additional_background_services_fn.len())]
    pub register_additional_background_services_fn: Vec<RegisterBackgroundServiceFn<A, C, S>>,
    /// License Status
    #[builder(default)]
    pub license_status: Option<&'static LicenseStatus>,
    /// Build-time information (commit SHAs, enterprise version, bundled console).
    /// Defaults to empty values; downstream binaries should inject their own.
    #[builder(default)]
    pub build_info: Option<&'static BuildInfo>,
    /// Catalog-managed system roles installed into the process-wide
    /// registry for the duration of this `serve` call. Drives the seed
    /// in `create_project`. Pass an empty `Vec` for OSS (no system roles
    /// seeded); downstream binaries pass their
    /// full spec list — the same one the binary passes to
    /// `run_post_migration_hooks` so both subcommands agree.
    #[builder(default)]
    pub system_roles: Vec<crate::service::SystemRoleSpec>,
    /// Receivers of the cache change-log rows of the cache domains that extensions own.
    /// They are called while `cache.invalidation.enabled` is on.
    #[builder(default)]
    pub cache_change_receivers: Vec<crate::service::cache_invalidation::ArcCacheChangeReceiver>,
}

/// Starts the service with the provided configuration.
///
/// # Errors
/// - If the service cannot bind to the specified address.
/// - If the terms of service have not been accepted during bootstrap.
/// - If a cache change receiver names a domain of Lakekeeper, or two name the same domain.
pub async fn serve<C: CatalogStore, S: SecretStore, A: Authorizer, N: Authenticator + 'static>(
    mut config: ServeConfiguration<C, S, A, N>,
) -> anyhow::Result<()> {
    // Install the system role registry for this process. Driven by the
    // binary; OSS passes an empty Vec, downstream binaries pass their
    // spec list. Drives the seed in `create_project` for the lifetime
    // of this serve call.
    if let Err(rejected) =
        crate::service::install_system_role_registry(std::mem::take(&mut config.system_roles))
    {
        // Logged at ERROR by the installer; second install in the same
        // process indicates a programming error in the host binary, but
        // we don't escalate here.
        let _ = rejected;
    }

    let drain_settings = DrainSettings::from_config();
    let cancellation_token = CancellationToken::new();

    // Validate Authenticators and propagate their IDP IDs to the authorizer
    if let Some(authenticator) = &config.authenticator {
        let idp_ids = validate_authenticator_idp_ids(authenticator)?;
        config.authorizer.set_registered_idp_ids(idp_ids);
    }
    let config = config; // Make config immutable for our sanity

    log_instance_admins();

    // Strings are name of the service, used for logging
    let mut service_futures = JoinSet::<Result<(), anyhow::Error>>::new();
    let mut service_ids = HashMap::new();

    // Sigint / Sigterm handler, idle exit and max lifetime:
    let stop_service = spawn_stop_service(
        &mut service_futures,
        drain_settings,
        cancellation_token.clone(),
    );
    service_ids.insert(stop_service.id, "Shutdown Handler".to_string());

    // Endpoint statistics TX
    let (endpoint_statistics_tx, endpoint_statistics_rx) = tokio::sync::mpsc::channel(1000);
    let endpoint_statistics_tracker_tx = EndpointStatisticsTrackerTx::new(endpoint_statistics_tx);

    // Cloud Events TX
    let (cloud_events_tx, cloud_events_rx) = tokio::sync::mpsc::channel(1000);

    // ------------- Serve -------------
    let serving_result = serve_inner(
        config,
        cancellation_token.clone(),
        &mut service_futures,
        &mut service_ids,
        cloud_events_tx.clone(),
        cloud_events_rx,
        endpoint_statistics_tracker_tx.clone(),
        endpoint_statistics_rx,
        stop_service,
    )
    .await;

    // Handle shutdown if serve_inner returned (e.g. due to error)
    if let Some(err) = serving_result.as_ref().err() {
        tracing::error!("Service error: {err}. Initiating shutdown of background services.");
    } else {
        tracing::info!("Service is shutting down gracefully.");
    }

    cancellation_token.cancel();

    endpoint_statistics_tracker_tx
        .send(EndpointStatisticsMessage::Shutdown)
        .await?;
    cloud_events_tx.send(CloudEventsMessage::Shutdown).await?;

    wait_for_services_to_stop(&mut service_futures, &mut service_ids).await;

    serving_result
}

/// Waits for the background services to finish after cancellation, at most 20 seconds, then
/// aborts the rest. Reports progress every 5 seconds.
async fn wait_for_services_to_stop<H: ::std::hash::BuildHasher>(
    service_futures: &mut JoinSet<Result<(), anyhow::Error>>,
    service_ids: &mut HashMap<tokio::task::Id, String, H>,
) {
    let shutdown_timeout_secs = 20;
    let report_interval_secs = 5;
    let start_time = std::time::Instant::now();

    tracing::info!(
        "Waiting up to {shutdown_timeout_secs} seconds for {} background services to finish gracefully",
        service_ids.len()
    );

    let timeout = tokio::time::timeout(
        std::time::Duration::from_secs(shutdown_timeout_secs),
        async {
            let mut last_report = std::time::Instant::now();

            while let Some(result) = service_futures.join_next_with_id().await {
                log_service_completion(&result, service_ids, true);

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
}

fn spawn_stop_service(
    service_futures: &mut JoinSet<Result<(), anyhow::Error>>,
    drain_settings: DrainSettings,
    cancellation_token: CancellationToken,
) -> StopService {
    let task_control = ArcTaskRunnerControl::default();
    let control = Arc::clone(&task_control);
    let mut signals = stop_signals();
    let handle = service_futures.spawn(async move {
        wait_for_stop_and_drain(&control, drain_settings, &mut signals, &cancellation_token).await;
        Ok(())
    });
    StopService {
        id: handle.id(),
        task_control,
    }
}

#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
async fn serve_inner<
    C: CatalogStore,
    S: SecretStore,
    A: Authorizer,
    N: Authenticator + 'static,
    H: ::std::hash::BuildHasher + 'static,
>(
    config: ServeConfiguration<C, S, A, N>,
    cancellation_token: CancellationToken,
    service_futures: &mut JoinSet<Result<(), anyhow::Error>>,
    service_ids: &mut HashMap<tokio::task::Id, String, H>,
    cloud_events_tx: tokio::sync::mpsc::Sender<CloudEventsMessage>,
    cloud_events_rx: tokio::sync::mpsc::Receiver<CloudEventsMessage>,
    endpoint_statistics_tracker_tx: EndpointStatisticsTrackerTx,
    endpoint_statistics_rx: tokio::sync::mpsc::Receiver<EndpointStatisticsMessage>,
    stop_service: StopService,
) -> anyhow::Result<()> {
    let ServeConfiguration {
        bind_addr,
        secrets_state,
        catalog_state,
        authorizer,
        authenticator,
        stats,
        contract_verification,
        admission_gates,
        modify_router_fn,
        management_api_extensions,
        cloud_event_sinks,
        enable_built_in_task_queues: enable_built_in_queues,
        register_additional_task_queues_fn,
        event_dispatcher: additional_event_dispatcher,
        register_additional_background_services_fn: additional_background_services,
        license_status,
        build_info,
        system_roles: _,
        cache_change_receivers,
    } = config;
    let cache_change_receivers =
        crate::service::cache_invalidation::CacheChangeReceivers::new(cache_change_receivers)?;

    let license_status = license_status.unwrap_or(&APACHE_LICENSE_STATUS);
    let build_info = build_info.unwrap_or(&DEFAULT_BUILD_INFO);

    // The listener always binds: in headless worker mode
    // (`LAKEKEEPER__SERVE_HTTP_API=false`) it serves only `/health` (see the
    // router selection below) so liveness/readiness probes still have an
    // endpoint.
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
    let has_cloud_event_sinks = !cloud_event_sinks.is_empty();
    let cloud_events_background_task = CloudEventsPublisherBackgroundTask {
        source: cloud_events_rx,
        sinks: cloud_event_sinks,
    };

    // Metrics server
    let (layer, metrics_server) =
        crate::metrics::get_axum_layer_and_install_recorder(CONFIG.metrics.port).map_err(|e| {
            anyhow!(e).context(format!(
                "Failed to start metrics server on port: {}",
                CONFIG.metrics.port
            ))
        })?;

    // Endpoint stats
    let tracker = EndpointStatisticsTracker::new(
        endpoint_statistics_rx,
        stats,
        CONFIG.endpoint_stat_flush_interval,
        FlushMode::Automatic,
    );

    // Event system setup
    let dispatcher = additional_event_dispatcher.unwrap_or(EventDispatcher::new(vec![]));
    // Registering the publisher with no sinks configured is pure waste that is
    // paid per commit: the listener runs `serde_json::to_value` over the whole
    // commit request (megabytes, once `max_request_body_size` is raised), queues
    // it, and the background task then builds a full `CloudEvent` from it —
    // only to drop it because there is nowhere to send it. Skip the listener
    // entirely instead; nothing observable changes.
    if has_cloud_event_sinks {
        dispatcher
            .append(Arc::new(CloudEventsPublisher::new(cloud_events_tx.clone())))
            .await;
    } else {
        tracing::info!(
            "No cloud-event sinks configured, not registering the cloud-events publisher"
        );
    }
    register_built_in_listeners::<C>(&dispatcher, &catalog_state).await;

    // Task queues
    let task_queue_registry = TaskQueueRegistry::new();
    // In read-only maintenance mode we don't start built-in queue workers:
    // the operator drains writes before running schema migrations, and
    // workers would otherwise tick against a half-migrated DB.
    let skip_built_in_queues_for_maintenance = CONFIG.maintenance_mode.is_read_only();
    if skip_built_in_queues_for_maintenance {
        tracing::info!(
            "Maintenance mode is read-only: skipping built-in task queue worker registration."
        );
    }
    if enable_built_in_queues && !skip_built_in_queues_for_maintenance {
        task_queue_registry
            .register_built_in_queues::<C, _, _>(
                catalog_state.clone(),
                secrets_state.clone(),
                authorizer.clone(),
                CONFIG.task_poll_interval,
            )
            .await;
    }

    // Register additional task queues if provided
    // Registered task queues have interior mutability. A later registration of a task
    // affects the state of all previously registered tasks.
    let registered_task_queues = task_queue_registry.registered_task_queues();
    let state = ApiContext {
        v1_state: State::<_, C, _> {
            authz: authorizer,
            catalog: catalog_state,
            secrets: secrets_state,
            contract_verifiers: contract_verification,
            registered_task_queues,
            events: dispatcher,
            license_status,
            build_info,
        },
    };

    for register_fn in register_additional_task_queues_fn {
        register_fn(task_queue_registry.clone(), state.clone()).await?;
    }

    // Number of task-queue workers that will actually run. Queues register even
    // with zero workers, so this is distinct from "any queues registered".
    let total_task_workers = task_queue_registry.total_workers().await;
    // A headless host with neither task-queue workers nor background services
    // does nothing: no HTTP API, no task processing, yet `/health` reports 200.
    // Fail fast: such a process would only pretend to be healthy. A host that
    // registers background services (even with no task workers) still does work,
    // so it is allowed to start.
    if total_task_workers == 0
        && additional_background_services.is_empty()
        && !CONFIG.serve_http_api
    {
        return Err(anyhow!(
            "Headless host has no work to do: serve_http_api is disabled, no task-queue workers \
             are configured (all TASK_*_WORKERS=0 or skipped by read-only maintenance), and no \
             background services are registered. Refusing to start a process that would do \
             nothing. Enable at least one TASK_*_WORKERS, register a background service, or set \
             LAKEKEEPER__SERVE_HTTP_API=true."
        ));
    }

    // Router: the full catalog API when serving it, otherwise a minimal
    // `/health`-only router for headless worker deployments. Building the full
    // router generates the OpenAPI document and installs CORS/auth layers, so a
    // worker skips it entirely; the per-request metrics `layer` is dropped with
    // it (a worker serves no API requests to record). The metrics server itself
    // still runs, spawned below.
    let router = if CONFIG.serve_http_api {
        let router = new_full_router::<C, _, _, _>(RouterArgs {
            authenticator: authenticator.clone(),
            state: state.clone(),
            service_health_provider: health_provider.clone(),
            cors_origins: CONFIG.allow_origin.as_deref(),
            metrics_layer: Some(layer),
            endpoint_statistics_tracker_tx: endpoint_statistics_tracker_tx.clone(),
            instance_admin_membership: Arc::new(ConfiguredInstanceAdmins::from_config()),
            admission_gates,
            management_api_extensions,
        })
        .await?;
        modify_router_fn.unwrap_or(std::convert::identity)(router)
    } else {
        // Headless: the host announces the mode at startup (see the binary's
        // `serve`). Serve only `/health` for probes.
        drop(layer);
        new_health_router(health_provider.clone())
    };

    // ---- Launch background services ----
    // Metrics server:
    let metrics_handle = service_futures.spawn(serve_metrics(
        metrics_server,
        &state.v1_state,
        cancellation_token.clone(),
    ));
    service_ids.insert(metrics_handle.id(), "Metrics Server".to_string());

    // Periodic health checks:
    let health_abort_handles =
        health_provider.spawn_update_health_checks(service_futures, &cancellation_token);
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

    // Cache invalidation: reading and pruning the change log.
    for (service_name, task) in crate::service::cache_invalidation::background_tasks::<C>(
        state.v1_state.catalog.clone(),
        cache_change_receivers,
        &cancellation_token,
    )
    .await
    {
        let abort_handle = service_futures.spawn(async move {
            task.await;
            Ok(())
        });
        service_ids.insert(abort_handle.id(), service_name.to_string());
    }

    // Execute additional background services:
    for additional_service_register_fn in additional_background_services {
        let abort_handles = additional_service_register_fn(
            service_futures,
            cancellation_token.clone(),
            state.clone(),
        )
        .await?;
        for (service_name, abort_handle) in abort_handles {
            tracing::info!("Spawned background service: {service_name}");
            service_ids.insert(abort_handle.id(), service_name);
        }
    }

    // Task Queues:
    let task_runner = task_queue_registry
        .task_queues_runner(cancellation_token.clone())
        .await
        .with_control(stop_service.task_control);
    if total_task_workers == 0 {
        // Queues can be registered with zero workers (every TASK_*_WORKERS=0) or
        // skipped entirely (read-only maintenance). `run_queue_workers` returns
        // immediately in that case, so don't spawn the monitor — otherwise its
        // early `Ok(())` is treated as a service that should run forever exiting,
        // which shuts the whole process down. The headless no-work case already
        // failed fast above, so this is an API-only pod.
        tracing::info!(
            "No task-queue workers configured; serving the HTTP API without a task worker monitor."
        );
    } else {
        let task_abort_handle = service_futures.spawn(async move {
            task_runner.run_queue_workers(true).await;
            Ok(())
        });
        service_ids.insert(task_abort_handle.id(), "Task Worker Monitor".to_string());
    }

    // Writes, so skipped in read-only maintenance. Runs also without task workers.
    if !skip_built_in_queues_for_maintenance {
        let materializer_catalog_state = state.v1_state.catalog.clone();
        let materializer_cancellation_token = cancellation_token.clone();
        let materializer_abort_handle = service_futures.spawn(async move {
            crate::service::tasks::task_schedule::task_schedule_materializer::<C>(
                materializer_catalog_state,
                CONFIG.task_poll_interval,
                materializer_cancellation_token,
            )
            .await;
            Ok(())
        });
        service_ids.insert(
            materializer_abort_handle.id(),
            "Task Schedule Materializer".to_string(),
        );
    }

    // HTTP Server / Axum. Serves the full API, or just `/health` in headless
    // worker mode.
    let cancellation_token_clone = cancellation_token.clone();
    let axum_abort_handle = service_futures.spawn(async move {
        service_serve(listener, router, cancellation_token_clone)
            .await
            .map_err(|e| anyhow!(e).context("Axum server exited with error"))
    });
    service_ids.insert(axum_abort_handle.id(), "Axum Server".to_string());

    tracing::info!("All background services started. Lakekeeper is now running.");
    if let Some(result) = service_futures.join_next_with_id().await {
        let msg = log_service_completion(&result, service_ids, false);
        match result {
            Ok((id, res)) => {
                if id == stop_service.id || cancellation_token.is_cancelled() {
                    Ok(())
                } else {
                    match res {
                        Ok(()) => Err(anyhow!(msg)),
                        Err(e) => Err(anyhow!(e).context(msg)),
                    }
                }
            }
            Err(e) => Err(anyhow!(e).context("Failed to join on a background service")),
        }
    } else {
        tracing::error!("No services were started, exiting.");
        Ok(())
    }
}

/// Serves the metrics port, with the task queue demand route. A process that serves the HTTP API
/// also samples the oldest due task of each task queue.
fn serve_metrics<A: Authorizer, C: CatalogStore, S: SecretStore>(
    metrics_server: MetricsServer,
    state: &State<A, C, S>,
    cancellation_token: CancellationToken,
) -> impl Future<Output = anyhow::Result<()>> + use<A, C, S> {
    let demand = crate::api::task_queue_demand::QueueDemand::new::<C>(
        state.catalog.clone(),
        state.registered_task_queues.clone(),
    );
    // Headless workers come and go with the autoscaler, so they do not add series or queries.
    let sampler = CONFIG
        .serve_http_api
        .then(|| demand.clone().sample_oldest_due_age());
    let metrics_future = metrics_server.serve(demand.router(), cancellation_token);
    async move {
        let sampler = sampler.map(tokio::spawn);
        let result = metrics_future.await;
        if let Some(sampler) = sampler {
            sampler.abort();
        }
        result.map_err(|e| anyhow!(e).context("Metrics Services exited with error"))
    }
}

/// Registers the cache and audit listeners that the configuration turns on.
async fn register_built_in_listeners<C: CatalogStore>(
    dispatcher: &EventDispatcher,
    catalog_state: &C::State,
) {
    if CONFIG.cache.warehouse.enabled {
        tracing::info!("Warehouse cache is enabled, registering warehouse cache event listener");
        dispatcher
            .append(Arc::new(
                crate::service::warehouse_cache::WarehouseCacheEventListener {},
            ))
            .await;
    } else {
        tracing::info!("Warehouse cache is disabled");
    }
    if CONFIG.cache.namespace.enabled {
        tracing::info!("Namespace cache is enabled, registering namespace cache event listener");
        dispatcher
            .append(Arc::new(
                crate::service::namespace_cache::NamespaceCacheEventListener {},
            ))
            .await;
    } else {
        tracing::info!("Namespace cache is disabled");
    }
    if CONFIG.cache.role.enabled {
        tracing::info!("Role cache is enabled, registering role cache event listener");
        dispatcher
            .append(Arc::new(
                crate::service::role_cache::RoleCacheEventListener {},
            ))
            .await;
    } else {
        tracing::info!("Role cache is disabled");
    }
    if CONFIG.cache.user.enabled {
        tracing::info!("User cache is enabled, registering user cache event listener");
        dispatcher
            .append(Arc::new(
                crate::service::user_cache::UserCacheEventListener {},
            ))
            .await;
    } else {
        tracing::info!("User cache is disabled");
    }
    if CONFIG.audit.tracing.enabled {
        tracing::info!("Audit tracing is enabled, registering audit event listener");
        if CONFIG.audit.tracing.include_user_email {
            tracing::info!("Audit records carry user emails");
        }
        dispatcher
            .append(Arc::new(AuditEventListener::with_catalog(Arc::new(
                crate::service::events::CatalogStoreReader::<C>::new(catalog_state.clone()),
            ))))
            .await;
    } else {
        tracing::info!("Audit tracing is disabled");
    }
}

fn validate_server_info(server_info: &ServerInfo) -> anyhow::Result<()> {
    if server_info.is_open_for_bootstrap() {
        if CONFIG.serve_http_api {
            tracing::info!(
                "The catalog is open for bootstrap. Bootstrapping sets the initial administrator. Please open the Web-UI after startup or call the bootstrap endpoint directly."
            );
        } else {
            tracing::info!(
                "The catalog is open for bootstrap. This is a headless worker and serves no bootstrap endpoint; bootstrap via an instance running the HTTP API (Web-UI or the bootstrap endpoint)."
            );
        }
    } else {
        tracing::info!("The catalog is not open for bootstrap.");
        if !server_info.terms_accepted() {
            tracing::error!("The terms of service have not been accepted.");
            return Err(anyhow!(
                "Server ID {}.The terms of service have not been accepted.",
                server_info.server_id()
            ));
        }
        tracing::info!(
            "Server ID {}. The terms of service have been accepted.",
            server_info.server_id()
        );
    }

    Ok(())
}

/// Log the configured instance admins at startup. Count at INFO; individual
/// IDs at DEBUG (they are deployment-config PII — `IdP` subjects).
fn log_instance_admins() {
    let n = CONFIG.instance_admins.len();
    if n == 0 {
        return;
    }
    tracing::info!(
        "Configured {n} instance admin(s) via LAKEKEEPER__INSTANCE_ADMINS. \
         These principals bypass authorization for all control-plane actions \
         (but not for CatalogTableAction::ReadData / WriteData)."
    );
    for admin in &CONFIG.instance_admins {
        tracing::debug!("Instance admin: {admin}");
    }
}

fn validate_authenticator_idp_ids(
    authenticator: &impl Authenticator,
) -> anyhow::Result<Arc<[RoleProviderId]>> {
    let idp_ids = authenticator.idp_ids();
    if idp_ids.is_empty() {
        anyhow::bail!(
            "Authenticator returned an empty list of IdP IDs. At least one IdP ID is required if authentication is enabled. All IdP IDs must be non-empty strings."
        );
    }
    let mut result = Vec::with_capacity(idp_ids.len());
    for idp_id in idp_ids {
        let Some(idp_id) = idp_id else {
            return Err(anyhow!(
                "Authenticator returned an empty IdP ID. All IdP IDs must be non-empty strings."
            ));
        };
        let role_provider_id = RoleProviderId::try_new(idp_id).map_err(|e| {
            anyhow!(
                "Invalid IdP ID '{idp_id}' in authenticator configuration: {e}. All IdP IDs must consist of lowercase letters, digits, or hyphens."
            )
        })?;
        result.push(role_provider_id);
    }
    Ok(result.into())
}
