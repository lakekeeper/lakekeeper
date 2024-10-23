use anyhow::{anyhow, Error};
use iceberg_catalog::api::router::{new_full_router, serve as service_serve};
use iceberg_catalog::implementations::postgres::{CatalogState, PostgresCatalog, ReadWrite};
use iceberg_catalog::implementations::{AllowAllAuthState, AllowAllAuthZHandler};
use iceberg_catalog::service::contract_verification::ContractVerifiers;
use iceberg_catalog::service::event_publisher::{
    CloudEventBackend, CloudEventsPublisher, CloudEventsPublisherBackgroundTask, KafkaBackend,
    KafkaConfig, Message, NatsBackend,
};
use iceberg_catalog::service::health::ServiceHealthProvider;
use iceberg_catalog::service::secrets::Secrets;
use iceberg_catalog::service::token_verification::Verifier;
use iceberg_catalog::{SecretBackend, CONFIG};
use reqwest::Url;

use iceberg_catalog::implementations::postgres::task_queues::{
    TabularExpirationQueue, TabularPurgeQueue,
};
use iceberg_catalog::service::task_queue::TaskQueues;
use std::sync::Arc;

pub(crate) async fn serve(bind_addr: std::net::SocketAddr) -> Result<(), anyhow::Error> {
    let read_pool =
        iceberg_catalog::implementations::postgres::get_reader_pool(CONFIG.to_pool_opts()).await?;
    let write_pool =
        iceberg_catalog::implementations::postgres::get_writer_pool(CONFIG.to_pool_opts()).await?;

    let catalog_state = CatalogState::from_pools(read_pool.clone(), write_pool.clone());
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
    let auth_state = AllowAllAuthState;

    let health_provider = ServiceHealthProvider::new(
        vec![
            ("catalog", Arc::new(catalog_state.clone())),
            ("secrets", Arc::new(secrets_state.clone())),
            ("auth", Arc::new(auth_state.clone())),
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

    let mut cloud_event_sinks = vec![];

    if let Some(nat_addr) = &CONFIG.nats_address {
        let nats_publisher = build_nats_client(nat_addr).await?;
        cloud_event_sinks
            .push(Arc::new(nats_publisher) as Arc<dyn CloudEventBackend + Sync + Send>);
    }
    if let (Some(kafka_config), Some(kafka_topic)) = (&CONFIG.kafka_config, &CONFIG.kafka_topic) {
        let kafka_publisher = build_kafka_producer(kafka_config, kafka_topic, &CONFIG.kafka_key)?;
        cloud_event_sinks
            .push(Arc::new(kafka_publisher) as Arc<dyn CloudEventBackend + Sync + Send>);
    }

    if cloud_event_sinks.is_empty() {
        tracing::info!("Running without publisher.");
    };

    let (tx, rx) = tokio::sync::mpsc::channel(1000);

    let x = CloudEventsPublisherBackgroundTask {
        source: rx,
        sinks: cloud_event_sinks,
    };
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;

    let metrics_layer =
        iceberg_catalog::metrics::get_axum_layer_and_install_recorder(CONFIG.metrics_port)?;

    let router = new_full_router::<
        PostgresCatalog,
        PostgresCatalog,
        AllowAllAuthZHandler,
        AllowAllAuthZHandler,
        Secrets,
    >(
        auth_state,
        catalog_state.clone(),
        secrets_state.clone(),
        queues.clone(),
        CloudEventsPublisher::new(tx.clone()),
        ContractVerifiers::new(vec![]),
        if let Some(uri) = CONFIG.openid_provider_uri.clone() {
            Some(Verifier::new(uri).await?)
        } else {
            None
        },
        health_provider,
        Some(metrics_layer),
    );

    let publisher_handle = tokio::task::spawn(async move {
        match x.publish().await {
            Ok(_) => tracing::info!("Exiting publisher task"),
            Err(e) => tracing::error!("Publisher task failed: {e}"),
        };
    });

    tokio::select!(
        _ = queues.spawn_queues::<PostgresCatalog, _>(catalog_state, secrets_state) => tracing::error!("Tabular queue task failed"),
        err = service_serve(listener, router) => tracing::error!("Service failed: {err:?}"),
    );

    tracing::debug!("Sending shutdown signal to event publisher.");
    tx.send(Message::Shutdown).await?;
    publisher_handle.await?;

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
    key: &Option<String>,
) -> Result<KafkaBackend, Error> {
    if !(kafka_config.conf.contains_key("bootstrap.servers")
        || kafka_config.conf.contains_key("metadata.broker.list"))
    {
        return Err(anyhow!(
            "Kafka config map does not conain 'bootstrap.servers' or 'metadata.broker.list'. You need to provide either of those, in addition with any other parameters you need."
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
    producer_client_config.set(
        "enable.idempotence",
        kafka_config.enable_idempotence.clone(),
    );
    let producer = producer_client_config.create()?;
    let kafka_backend = KafkaBackend {
        producer,
        topic: topic.clone(),
        key: key.clone().unwrap_or_else(|| "".into()),
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
