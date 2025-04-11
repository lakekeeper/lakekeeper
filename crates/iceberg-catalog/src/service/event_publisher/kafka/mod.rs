pub(crate) mod vendor;

use std::{collections::HashMap, time::Duration};

use async_trait::async_trait;
use cloudevents::Event;
use rdkafka::producer::{future_producer::Delivery, FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use veil::Redact;

use super::CloudEventBackend;
use crate::service::event_publisher::kafka::vendor::cloudevents::binding::rdkafka::{
    FutureRecordExt, MessageRecord,
};

#[derive(Clone, Serialize, Deserialize, PartialEq, Redact)]
pub struct KafkaConfig {
    #[serde(rename = "sasl.password")]
    #[redact]
    pub sasl_password: Option<String>,
    #[serde(rename = "sasl.oauthbearer.client.secret")]
    #[redact]
    pub sasl_oauthbearer_client_secret: Option<String>,
    #[serde(rename = "ssl.key.password")]
    #[redact]
    pub ssl_key_password: Option<String>,
    #[serde(rename = "ssl.keystore.password")]
    #[redact]
    pub ssl_keystore_password: Option<String>,
    #[serde(flatten)]
    pub conf: HashMap<String, String>,
}

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
