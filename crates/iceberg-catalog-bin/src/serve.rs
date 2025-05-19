use std::sync::Arc;

use anyhow::{anyhow, Error};
#[cfg(feature = "ui")]
use axum::routing::get;
use iceberg_catalog::{
    api::router::{new_full_router, serve as service_serve, RouterArgs},
    implementations::{
        postgres::{
            endpoint_statistics::PostgresStatisticsSink,
            task_queues::{TabularExpirationQueue, TabularPurgeQueue},
            CatalogState, PostgresCatalog, ReadWrite,
        },
        Secrets,
    },
    service::{
        authz::{
            implementations::{get_default_authorizer_from_config, Authorizers},
            Authorizer,
        },
        contract_verification::ContractVerifiers,
        endpoint_hooks::EndpointHookCollection,
        endpoint_statistics::{EndpointStatisticsMessage, EndpointStatisticsTracker, FlushMode},
        event_publisher::{
            kafka::{KafkaBackend, KafkaConfig},
            nats::NatsBackend,
            CloudEventBackend, CloudEventsMessage, CloudEventsPublisher,
            CloudEventsPublisherBackgroundTask, TracingPublisher,
        },
        health::ServiceHealthProvider,
        task_queue::TaskQueues,
        Catalog, EndpointStatisticsTrackerTx, StartupValidationData,
    },
    SecretBackend, CONFIG,
};
use limes::{Authenticator, AuthenticatorEnum};
use reqwest::Url;

const OIDC_IDP_ID: &str = "oidc";
const K8S_IDP_ID: &str = "kubernetes";

#[cfg(feature = "ui")]
use crate::ui;

pub(crate) async fn serve(bind_addr: std::net::SocketAddr) -> Result<(), anyhow::Error> {
    let read_pool = iceberg_catalog::implementations::postgres::get_reader_pool(
        CONFIG
            .to_pool_opts()
            .max_connections(CONFIG.pg_read_pool_connections),
    )
    .await?;
    let write_pool = iceberg_catalog::implementations::postgres::get_writer_pool(
        CONFIG
            .to_pool_opts()
            .max_connections(CONFIG.pg_write_pool_connections),
    )
    .await?;

    let catalog_state = CatalogState::from_pools(read_pool.clone(), write_pool.clone());

    let validation_data = PostgresCatalog::get_server_info(catalog_state.clone()).await?;
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

    let secrets_state: Secrets = match CONFIG.secret_backend {
        SecretBackend::KV2 => iceberg_catalog::implementations::kv2::SecretsState::from_config(
            CONFIG
                .kv2
                .as_ref()
                .ok_or_else(|| anyhow!("Need vault config to use vault as backend"))?,
        )
        .await?
        .into(),
        SecretBackend::Postgres => {
            iceberg_catalog::implementations::postgres::SecretsState::from_pools(
                read_pool.clone(),
                write_pool.clone(),
            )
            .into()
        }
    };
    let authorizer = get_default_authorizer_from_config().await?;

    let health_provider = ServiceHealthProvider::new(
        vec![
            ("catalog", Arc::new(catalog_state.clone())),
            ("secrets", Arc::new(secrets_state.clone())),
            ("auth", Arc::new(authorizer.clone())),
        ],
        CONFIG.health_check_frequency_seconds,
        CONFIG.health_check_jitter_millis,
    );
    health_provider.spawn_health_checks().await;

    let queues = TaskQueues::new(
        Arc::new(TabularExpirationQueue::from_config(
            ReadWrite::from_pools(read_pool.clone(), write_pool.clone()),
            CONFIG.queue_config.clone(),
        )?),
        Arc::new(TabularPurgeQueue::from_config(
            ReadWrite::from_pools(read_pool.clone(), write_pool.clone()),
            CONFIG.queue_config.clone(),
        )?),
    );

    let listener = tokio::net::TcpListener::bind(bind_addr)
        .await
        .map_err(|e| anyhow!(e).context(format!("Failed to bind to address: {bind_addr}")))?;
    match authorizer {
        Authorizers::AllowAll(a) => {
            serve_with_authn(
                a,
                catalog_state,
                secrets_state,
                queues,
                health_provider,
                listener,
            )
            .await?
        }
        Authorizers::OpenFGA(a) => {
            serve_with_authn(
                a,
                catalog_state,
                secrets_state,
                queues,
                health_provider,
                listener,
            )
            .await?
        }
    }

    Ok(())
}

async fn serve_with_authn<A: Authorizer>(
    authorizer: A,
    catalog_state: CatalogState,
    secrets_state: Secrets,
    queues: TaskQueues,
    health_provider: ServiceHealthProvider,
    listener: tokio::net::TcpListener,
) -> Result<(), anyhow::Error> {
    let authn_k8s_audience = if CONFIG.enable_kubernetes_authentication {
        Some(
            limes::kubernetes::KubernetesAuthenticator::try_new_with_default_client(
                Some(K8S_IDP_ID),
                CONFIG
                    .kubernetes_authentication_audience
                    .clone()
                    .unwrap_or_default(),
            )
            .await
            .inspect_err(|e| tracing::error!("Failed to create K8s authorizer: {e}"))
            .inspect(|v| tracing::info!("K8s authorizer created {:?}", v))?,
        )
    } else {
        tracing::info!("Running without Kubernetes authentication.");
        None
    };
    let authn_k8s_legacy = if CONFIG.enable_kubernetes_authentication
        && CONFIG.kubernetes_authentication_accept_legacy_serviceaccount
    {
        let mut authenticator =
            limes::kubernetes::KubernetesAuthenticator::try_new_with_default_client(
                Some(K8S_IDP_ID),
                vec![],
            )
            .await
            .inspect_err(|e| tracing::error!("Failed to create K8s authorizer: {e}"))?;
        authenticator.set_issuers(vec!["kubernetes/serviceaccount".to_string()]);
        tracing::info!(
            "K8s authorizer for legacy service account tokens created {:?}",
            authenticator
        );

        Some(authenticator)
    } else {
        tracing::info!("Running without Kubernetes authentication for legacy service accounts.");
        None
    };

    let authn_oidc = if let Some(uri) = CONFIG.openid_provider_uri.clone() {
        let mut authenticator = limes::jwks::JWKSWebAuthenticator::new(
            uri.as_ref(),
            Some(std::time::Duration::from_secs(3600)),
        )
        .await?
        .set_idp_id(OIDC_IDP_ID);
        if let Some(aud) = &CONFIG.openid_audience {
            tracing::debug!("Setting accepted audiences: {aud:?}");
            authenticator = authenticator.set_accepted_audiences(aud.clone());
        }
        if let Some(iss) = &CONFIG.openid_additional_issuers {
            tracing::debug!("Setting openid_additional_issuers: {iss:?}");
            authenticator = authenticator.add_additional_issuers(iss.clone());
        }
        if let Some(scope) = &CONFIG.openid_scope {
            tracing::debug!("Setting openid_scope: {}", scope);
            authenticator = authenticator.set_scope(scope.clone());
        }
        if let Some(subject_claim) = &CONFIG.openid_subject_claim {
            tracing::debug!("Setting openid_subject_claim: {}", subject_claim);
            authenticator = authenticator.with_subject_claim(subject_claim.clone());
        } else {
            // "oid" should be used for entra-id, as the `sub` is different between applications.
            // We prefer oid here by default as no other IdP sets this field (that we know of) and
            // we can provide an out-of-the-box experience for users.
            // Nevertheless, we document this behavior in the docs and recommend as part of the
            // `production` checklist to set the claim explicitly.
            tracing::debug!("Defaulting openid_subject_claim to: oid, sub");
            authenticator =
                authenticator.with_subject_claims(vec!["oid".to_string(), "sub".to_string()]);
        }
        tracing::info!("Running with OIDC authentication.");
        Some(authenticator)
    } else {
        tracing::info!("Running without OIDC authentication.");
        None
    };

    let authn_k8s = authn_k8s_audience.map(AuthenticatorEnum::from);
    let authn_k8s_legacy = authn_k8s_legacy.map(AuthenticatorEnum::from);
    let authn_oidc = authn_oidc.map(AuthenticatorEnum::from);
    match (authn_k8s, authn_oidc, authn_k8s_legacy) {
        (Some(k8s), Some(oidc), Some(authn_k8s_legacy)) => {
            let authenticator = limes::AuthenticatorChain::<AuthenticatorEnum>::builder()
                .add_authenticator(oidc)
                .add_authenticator(k8s)
                .add_authenticator(authn_k8s_legacy)
                .build();
            serve_inner(
                authorizer,
                Some(authenticator),
                catalog_state,
                secrets_state,
                queues,
                health_provider,
                listener,
            )
            .await
        }
        (None, Some(auth1), Some(auth2))
        | (Some(auth1), None, Some(auth2))
        // OIDC has priority over k8s if specified
        | (Some(auth2), Some(auth1), None) => {
            let authenticator = limes::AuthenticatorChain::<AuthenticatorEnum>::builder()
                .add_authenticator(auth1)
                .add_authenticator(auth2)
                .build();
            serve_inner(
                authorizer,
                Some(authenticator),
                catalog_state,
                secrets_state,
                queues,
                health_provider,
                listener,
            )
            .await
        }
        (Some(auth), None, None) | (None, Some(auth), None) | (None, None, Some(auth)) => {
            serve_inner(
                authorizer,
                Some(auth),
                catalog_state,
                secrets_state,
                queues,
                health_provider,
                listener,
            )
            .await
        }
        (None, None, None) => {
            tracing::warn!("Authentication is disabled. This is not suitable for production!");
            serve_inner(
                authorizer,
                None::<AuthenticatorEnum>,
                catalog_state,
                secrets_state,
                queues,
                health_provider,
                listener,
            )
            .await
        }
    }
}

/// Helper function to remove redundant code from matching different implementations
#[allow(clippy::too_many_arguments)]
async fn serve_inner<A: Authorizer, N: Authenticator + 'static>(
    authorizer: A,
    authenticator: Option<N>,
    catalog_state: CatalogState,
    secrets_state: Secrets,
    queues: TaskQueues,
    health_provider: ServiceHealthProvider,
    listener: tokio::net::TcpListener,
) -> Result<(), anyhow::Error> {
    let (cloud_events_tx, cloud_events_rx) = tokio::sync::mpsc::channel(1000);

    let mut cloud_event_sinks = vec![];

    if let Some(nat_addr) = &CONFIG.nats_address {
        let nats_publisher = build_nats_client(nat_addr).await?;
        cloud_event_sinks
            .push(Arc::new(nats_publisher) as Arc<dyn CloudEventBackend + Sync + Send>);
    } else {
        tracing::info!("Running without NATS publisher.");
    };

    if let (Some(kafka_config), Some(kafka_topic)) = (&CONFIG.kafka_config, &CONFIG.kafka_topic) {
        let kafka_publisher = build_kafka_producer(kafka_config, kafka_topic)?;
        cloud_event_sinks
            .push(Arc::new(kafka_publisher) as Arc<dyn CloudEventBackend + Sync + Send>);
    } else {
        tracing::info!("Running without Kafka publisher.");
    }

    if let Some(true) = &CONFIG.log_cloudevents {
        let tracing_publisher = TracingPublisher;
        cloud_event_sinks
            .push(Arc::new(tracing_publisher) as Arc<dyn CloudEventBackend + Sync + Send>);
        tracing::info!("Logging Cloudevents.");
    } else {
        tracing::info!("Running without logging Cloudevents.");
    }

    if cloud_event_sinks.is_empty() {
        tracing::info!("Running without publisher.");
    }

    let x: CloudEventsPublisherBackgroundTask = CloudEventsPublisherBackgroundTask {
        source: cloud_events_rx,
        sinks: cloud_event_sinks,
    };

    let (layer, metrics_future) =
        iceberg_catalog::metrics::get_axum_layer_and_install_recorder(CONFIG.metrics_port)
            .map_err(|e| {
                anyhow!(e).context(format!(
                    "Failed to start metrics server on port: {}",
                    CONFIG.metrics_port
                ))
            })?;

    let (endpoint_statistics_tx, endpoint_statistics_rx) = tokio::sync::mpsc::channel(1000);

    let tracker = EndpointStatisticsTracker::new(
        endpoint_statistics_rx,
        vec![Arc::new(PostgresStatisticsSink::new(
            catalog_state.write_pool(),
        ))],
        CONFIG.endpoint_stat_flush_interval,
        FlushMode::Automatic,
    );

    let endpoint_statistics_tracker_tx = EndpointStatisticsTrackerTx::new(endpoint_statistics_tx);
    let hooks = EndpointHookCollection::new(vec![Arc::new(CloudEventsPublisher::new(
        cloud_events_tx.clone(),
    ))]);

    let router = new_full_router::<PostgresCatalog, _, Secrets, _>(RouterArgs {
        authenticator: authenticator.clone(),
        authorizer: authorizer.clone(),
        catalog_state: catalog_state.clone(),
        secrets_state: secrets_state.clone(),
        queues: queues.clone(),
        table_change_checkers: ContractVerifiers::new(vec![]),
        service_health_provider: health_provider,
        cors_origins: CONFIG.allow_origin.as_deref(),
        metrics_layer: Some(layer),
        endpoint_statistics_tracker_tx: endpoint_statistics_tracker_tx.clone(),
        hooks,
    })?;

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

    let publisher_handle = tokio::task::spawn(async move {
        match x.publish().await {
            Ok(_) => tracing::info!("Exiting publisher task"),
            Err(e) => tracing::error!("Publisher task failed: {e}"),
        };
    });
    let stats_handle = tokio::task::spawn(tracker.run());

    tokio::select!(
        _ = queues.spawn_queues::<PostgresCatalog, _, _>(catalog_state, secrets_state, authorizer) => tracing::error!("Tabular queue task failed"),
        err = service_serve(listener, router) => tracing::error!("Service failed: {err:?}"),
        _ = metrics_future => tracing::error!("Metrics server failed"),
    );

    tracing::debug!("Sending shutdown signal to event publisher.");
    endpoint_statistics_tracker_tx
        .send(EndpointStatisticsMessage::Shutdown)
        .await?;
    cloud_events_tx.send(CloudEventsMessage::Shutdown).await?;
    publisher_handle.await?;
    stats_handle.await?;
    Ok(())
}

async fn build_nats_client(nat_addr: &Url) -> Result<NatsBackend, Error> {
    tracing::info!("Running with nats publisher, connecting to: {nat_addr}");
    let builder = async_nats::ConnectOptions::new();

    let builder = if let Some(file) = &CONFIG.nats_creds_file {
        builder.credentials_file(file).await?
    } else {
        builder
    };

    let builder = if let (Some(user), Some(pw)) = (&CONFIG.nats_user, &CONFIG.nats_password) {
        builder.user_and_password(user.clone(), pw.clone())
    } else {
        builder
    };

    let builder = if let Some(token) = &CONFIG.nats_token {
        builder.token(token.clone())
    } else {
        builder
    };

    let nats_publisher = NatsBackend {
        client: builder.connect(nat_addr.to_string()).await?,
        topic: CONFIG
            .nats_topic
            .clone()
            .ok_or(anyhow::anyhow!("Missing nats topic."))?,
    };
    Ok(nats_publisher)
}

fn build_kafka_producer(
    kafka_config: &KafkaConfig,
    topic: &String,
) -> anyhow::Result<KafkaBackend> {
    if !(kafka_config.conf.contains_key("bootstrap.servers")
        || kafka_config.conf.contains_key("metadata.broker.list"))
    {
        return Err(anyhow::anyhow!(
            "Kafka config map does not contain 'bootstrap.servers' or 'metadata.broker.list'. You need to provide either of those, in addition with any other parameters you need."
        ));
    }
    let mut producer_client_config = rdkafka::ClientConfig::new();
    for (key, value) in kafka_config.conf.iter() {
        producer_client_config.set(key, value);
    }
    if let Some(sasl_password) = kafka_config.sasl_password.clone() {
        producer_client_config.set("sasl.password", sasl_password);
    }
    if let Some(sasl_oauthbearer_client_secret) =
        kafka_config.sasl_oauthbearer_client_secret.clone()
    {
        producer_client_config.set(
            "sasl.oauthbearer.client.secret",
            sasl_oauthbearer_client_secret,
        );
    }
    if let Some(ssl_key_password) = kafka_config.ssl_key_password.clone() {
        producer_client_config.set("ssl.key.password", ssl_key_password);
    }
    if let Some(ssl_keystore_password) = kafka_config.ssl_keystore_password.clone() {
        producer_client_config.set("ssl.keystore.password", ssl_keystore_password);
    }
    let producer = producer_client_config.create()?;
    let kafka_backend = KafkaBackend {
        producer,
        topic: topic.clone(),
    };
    let kafka_brokers = kafka_config
        .conf
        .get("bootstrap.servers")
        .or(kafka_config.conf.get("metadata.broker.list"))
        .unwrap();
    tracing::info!(
        "Running with kafka publisher, initial brokers are: {}. Topic: {}.",
        kafka_brokers,
        topic
    );
    Ok(kafka_backend)
}
