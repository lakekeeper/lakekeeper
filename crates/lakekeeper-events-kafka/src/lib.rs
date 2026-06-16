//! Kafka cloud-events publisher for Lakekeeper.
//!
//! Implements [`lakekeeper::service::events::CloudEventBackend`] for an
//! rdkafka `FutureProducer`. Configured via env vars under the same
//! `LAKEKEEPER__` / `ICEBERG_REST__` prefix as core; the
//! [`config::CONFIG`] static aggregates the Kafka-specific fields.

pub mod config;
pub(crate) mod vendor;

use std::time::Duration;

use async_trait::async_trait;
use cloudevents::Event;
use lakekeeper::service::events::CloudEventBackend;
use rdkafka::producer::{FutureProducer, FutureRecord, future_producer::Delivery};

use crate::{
    config::CONFIG,
    vendor::cloudevents::binding::rdkafka::{FutureRecordExt, MessageRecord},
};

/// Creates a Kafka publisher from the crate's configuration.
/// Returns `None` if the Kafka config or topic is not set.
///
/// # Errors
/// - If the Kafka producer cannot be created from the crate's configuration.
pub fn build_kafka_publisher_from_config() -> anyhow::Result<Option<KafkaBackend>> {
    let (Some(config), Some(topic)) = (&CONFIG.kafka_config, &CONFIG.kafka_topic) else {
        tracing::info!("Kafka config or topic not set. Events are not published to Kafka.");
        return Ok(None);
    };

    if topic.trim().is_empty() {
        tracing::info!(
            "Kafka topic is empty or contains only whitespace. Events are not published to Kafka."
        );
        return Ok(None);
    }

    if !(config.conf.contains_key("bootstrap.servers")
        || config.conf.contains_key("metadata.broker.list"))
    {
        tracing::info!(
            "Kafka config does not contain `bootstrap.servers` or `metadata.broker.list`. Events are not published to Kafka."
        );
        return Ok(None);
    }

    let mut producer_client_config = rdkafka::ClientConfig::new();
    for (key, value) in &config.conf {
        tracing::debug!("Configuring Kafka producer client config: {key}");
        producer_client_config.set(key, value);
    }

    if let Some(sasl_password) = config.sasl_password.clone() {
        tracing::debug!("Configuring Kafka producer client config: sasl.password");
        producer_client_config.set("sasl.password", sasl_password);
    }

    if let Some(sasl_oauthbearer_client_secret) = config.sasl_oauthbearer_client_secret.clone() {
        tracing::debug!("Configuring Kafka producer client config: sasl.oauthbearer.client.secret");
        producer_client_config.set(
            "sasl.oauthbearer.client.secret",
            sasl_oauthbearer_client_secret,
        );
    }

    if let Some(ssl_key_password) = config.ssl_key_password.clone() {
        tracing::debug!("Configuring Kafka producer client config: ssl.key.password");
        producer_client_config.set("ssl.key.password", ssl_key_password);
    }

    if let Some(ssl_keystore_password) = config.ssl_keystore_password.clone() {
        tracing::debug!("Configuring Kafka producer client config: ssl.keystore.password");
        producer_client_config.set("ssl.keystore.password", ssl_keystore_password);
    }

    let producer = match producer_client_config.create() {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(error = %e, "Failed to create Kafka producer");
            return Err(anyhow::anyhow!(e).context("Failed to create Kafka producer"));
        }
    };

    let kafka_backend = KafkaBackend::builder()
        .producer(producer)
        .topic(topic.clone())
        .build();

    let kafka_brokers = config
        .conf
        .get("bootstrap.servers")
        .map(String::as_str)
        .or(config.conf.get("metadata.broker.list").map(String::as_str))
        .unwrap_or("<unknown>");
    tracing::info!(
        "Publishing events to Kafka topic {topic}, initial brokers are: {kafka_brokers}",
    );
    Ok(Some(kafka_backend))
}

#[derive(typed_builder::TypedBuilder)]
pub struct KafkaBackend {
    pub producer: FutureProducer,
    pub topic: String,
}

impl std::fmt::Debug for KafkaBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaBackend")
            .field("topic", &self.topic)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl CloudEventBackend for KafkaBackend {
    async fn publish(&self, event: Event) -> anyhow::Result<()> {
        let key: String = match event.extension("tabular-id") {
            Some(extension_value) => extension_value.to_string(),
            None => String::new(),
        };
        let message_record = MessageRecord::from_event(event)?;
        let delivery_status = self
            .producer
            .send(
                FutureRecord::to(&self.topic)
                    .message_record(&message_record)
                    .key(&key[..]),
                Duration::from_secs(1),
            )
            .await;

        match delivery_status {
            Ok(Delivery {
                partition,
                offset,
                timestamp,
            }) => {
                tracing::debug!(
                    "CloudEvents event sent via kafka to topic: {}, partition: {partition}, offset: {offset}, timestamp: {timestamp:?}",
                    &self.topic,
                );
                Ok(())
            }
            Err((e, _)) => Err(anyhow::anyhow!(e)),
        }
    }

    fn name(&self) -> &'static str {
        "kafka-publisher"
    }
}
