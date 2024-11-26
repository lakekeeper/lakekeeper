use axum::async_trait;
use cloudevents::Event;

use super::CloudEventBackend;

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
