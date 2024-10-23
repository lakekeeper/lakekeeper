pub(crate) mod vendor;

use crate::service::tabular_idents::TabularIdentUuid;
use async_trait::async_trait;
use cloudevents::Event;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::{sync::Arc, time::Duration};
use uuid::Uuid;
use veil::Redact;

#[cfg(feature = "kafka")]
use crate::service::event_publisher::vendor::cloudevents::binding::rdkafka::{
    FutureRecordExt, MessageRecord,
};
#[cfg(feature = "kafka")]
use rdkafka::producer::{FutureProducer, FutureRecord};

#[derive(Debug, Clone)]
pub struct CloudEventsPublisher {
    tx: tokio::sync::mpsc::Sender<Message>,
    timeout: tokio::time::Duration,
}

impl CloudEventsPublisher {
    #[must_use]
    pub fn new(tx: tokio::sync::mpsc::Sender<Message>) -> Self {
        Self::new_with_timeout(tx, tokio::time::Duration::from_millis(50))
    }

    #[must_use]
    pub fn new_with_timeout(
        tx: tokio::sync::mpsc::Sender<Message>,
        timeout: tokio::time::Duration,
    ) -> Self {
        Self { tx, timeout }
    }

    /// # Errors
    ///
    /// Returns an error if the event cannot be sent to the channel due to capacity / timeout.
    pub async fn publish(
        &self,
        id: Uuid,
        typ: &str,
        data: serde_json::Value,
        metadata: EventMetadata,
    ) -> anyhow::Result<()> {
        self.tx
            .send_timeout(
                Message::Event(Payload {
                    id,
                    typ: typ.to_string(),
                    data,
                    metadata,
                }),
                self.timeout,
            )
            .await
            .map_err(|e| {
                tracing::warn!("Failed to emit event with id: '{}' due to: '{}'.", id, e);
                e
            })?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct EventMetadata {
    pub tabular_id: TabularIdentUuid,
    pub warehouse_id: Uuid,
    pub name: String,
    pub namespace: String,
    pub prefix: String,
    pub num_events: usize,
    pub sequence_number: usize,
    pub trace_id: Uuid,
}

#[derive(Debug)]
pub struct Payload {
    pub id: Uuid,
    pub typ: String,
    pub data: serde_json::Value,
    pub metadata: EventMetadata,
}

#[derive(Debug)]
pub enum Message {
    Event(Payload),
    Shutdown,
}

#[derive(Debug)]
pub struct CloudEventsPublisherBackgroundTask {
    pub source: tokio::sync::mpsc::Receiver<Message>,
    pub sinks: Vec<Arc<dyn CloudEventBackend + Sync + Send>>,
}

impl CloudEventsPublisherBackgroundTask {
    /// # Errors
    /// Returns an error if the `Event` cannot be built from the data passed into this function
    pub async fn publish(mut self) -> anyhow::Result<()> {
        while let Some(Message::Event(Payload {
            id,
            typ,
            data,
            metadata,
        })) = self.source.recv().await
        {
            use cloudevents::{EventBuilder, EventBuilderV10};

            let event_builder = EventBuilderV10::new()
                .id(id.to_string())
                .source(format!(
                    "uri:iceberg-catalog-service:{}",
                    hostname::get()
                        .map(|os| os.to_string_lossy().to_string())
                        .unwrap_or("hostname-unavailable".into())
                ))
                .ty(typ)
                .data("application/json", data);

            let EventMetadata {
                tabular_id,
                warehouse_id,
                name,
                namespace,
                prefix,
                num_events,
                sequence_number,
                trace_id,
            } = metadata;
            // TODO: this could be more elegant with a proc macro to give us IntoIter for EventMetadata
            let event = event_builder
                .extension("tabular-type", tabular_id.typ_str())
                .extension("tabular-id", tabular_id.to_string())
                .extension("warehouse-id", warehouse_id.to_string())
                .extension("name", name.to_string())
                .extension("namespace", namespace.to_string())
                .extension("prefix", prefix.to_string())
                // TODO: decide what to do with these numbers, likely they are never anywhere close to
                // saturating the respective int types, so probably a non-issue. Still we are converting
                // the numbers to_string here to avoid usize -> i64 which is what EventBuilderV10
                // uses to represent integers. The CloudEvents spec states i32 would be the correct int
                // type.
                .extension("num-events", num_events.to_string())
                .extension("sequence-number", sequence_number.to_string())
                // Implement distributed tracing: https://github.com/lakekeeper/lakekeeper/issues/63
                .extension("trace-id", trace_id.to_string())
                .build()?;

            for sink in &self.sinks {
                if let Err(e) = sink.publish(event.clone()).await {
                    tracing::warn!(
                        "Failed to emit event with id: '{}' on sink: '{}' due to: '{}'.",
                        id,
                        sink.name(),
                        e
                    );
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
pub trait CloudEventBackend: Debug {
    async fn publish(&self, event: Event) -> anyhow::Result<()>;
    fn name(&self) -> &str;
}

#[cfg(feature = "nats")]
#[derive(Debug)]
pub struct NatsBackend {
    pub client: async_nats::Client,
    pub topic: String,
}

#[cfg(feature = "nats")]
#[async_trait]
impl CloudEventBackend for NatsBackend {
    async fn publish(&self, event: Event) -> anyhow::Result<()> {
        Ok(self
            .client
            .publish(self.topic.clone(), serde_json::to_vec(&event)?.into())
            .await?)
    }

    fn name(&self) -> &'static str {
        "nats-publisher"
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Redact)]
pub struct KafkaConfig {
    #[serde(alias = "sasl.password")]
    #[redact]
    pub sasl_password: Option<String>,
    #[serde(alias = "sasl.oauthbearer.client.secret")]
    #[redact]
    pub sasl_oauthbearer_client_secret: Option<String>,
    #[serde(alias = "ssl.key.password")]
    #[redact]
    pub ssl_key_password: Option<String>,
    #[serde(alias = "ssl.keystore.password")]
    #[redact]
    pub ssl_keystore_password: Option<String>,
    #[serde(
        alias = "enable.idempotence",
        default = "KafkaConfig::enable_idempotence_default"
    )]
    pub enable_idempotence: String,
    #[serde(flatten)]
    pub conf: HashMap<String, String>,
}

impl KafkaConfig {
    fn enable_idempotence_default() -> String {
        "true".to_string()
    }
}

#[cfg(feature = "kafka")]
pub struct KafkaBackend {
    pub producer: FutureProducer,
    pub topic: String,
    pub key: String,
}

#[cfg(feature = "kafka")]
impl std::fmt::Debug for KafkaBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaBackend")
            // TODO:: Debug output for FutureProducer
            .field("topic", &self.topic)
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "kafka")]
#[async_trait]
impl CloudEventBackend for KafkaBackend {
    async fn publish(&self, event: Event) -> anyhow::Result<()> {
        let message_record = MessageRecord::from_event(event)?;
        let delivery_status = self
            .producer
            .send(
                FutureRecord::to(&self.topic)
                    .message_record(&message_record)
                    .key(&self.key[..]),
                Duration::from_secs(1),
            )
            .await;

        match delivery_status {
            Ok((partition, offset)) => {
                tracing::debug!("CloudEvents event send via kafka to topic: {} and partition: {} with offset: {}", &self.topic, partition, offset);
                Ok(())
            }
            Err((e, _)) => Err(anyhow::anyhow!(e)),
        }
    }

    fn name(&self) -> &'static str {
        "kafka-publisher"
    }
}

#[derive(Clone, Debug)]
pub struct TracingPublisher;

#[async_trait::async_trait]
impl CloudEventBackend for TracingPublisher {
    async fn publish(&self, event: Event) -> anyhow::Result<()> {
        let data = serde_json::to_string(&event).unwrap_or("Serialization failed".to_string());
        tracing::info!("Received event: {data}'");
        Ok(())
    }

    fn name(&self) -> &'static str {
        "tracing-publisher"
    }
}
