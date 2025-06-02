use async_trait::async_trait;
use cloudevents::Event;

use super::CloudEventBackend;
use crate::CONFIG;

/// Generate a NATS publisher from the crates configuration.
/// Returns `None` if the NATS address or topic is not set.
///
/// # Errors
/// - If NATs is configured but the connection or authentication fails.
pub async fn build_nats_publisher_from_config() -> anyhow::Result<Option<NatsBackend>> {
    let (Some(nats_addr), Some(nats_topic)) =
        (CONFIG.nats_address.clone(), CONFIG.nats_topic.clone())
    else {
        tracing::info!("NATS address or topic not set. Events are not published to NATS.");
        return Ok(None);
    };

    let builder = async_nats::ConnectOptions::new();

    let builder = if let Some(file) = &CONFIG.nats_creds_file {
        tracing::debug!(
            "Connecting to NATS at {nats_addr} with credentials file: {}",
            file.to_string_lossy()
        );
        builder.credentials_file(file).await?
    } else {
        builder
    };

    let builder = if let (Some(user), Some(pw)) = (&CONFIG.nats_user, &CONFIG.nats_password) {
        tracing::debug!("Connecting to NATS at {nats_addr} with user: {user}");
        builder.user_and_password(user.clone(), pw.clone())
    } else {
        builder
    };

    let builder = if let Some(token) = &CONFIG.nats_token {
        tracing::debug!("Connecting to NATS at {nats_addr} with token");
        builder.token(token.clone())
    } else {
        builder
    };

    let nats_publisher = NatsBackend {
        client: builder.connect(nats_addr.to_string()).await.map_err(|e| {
            anyhow::anyhow!(e).context(format!("Failed to connect to NATS at {nats_addr}"))
        })?,
        topic: nats_topic.clone(),
    };

    tracing::info!("Publishing events to NATS topic {nats_topic}, NATS address is: {nats_addr}");
    Ok(Some(nats_publisher))
}

#[derive(Debug)]
pub struct NatsBackend {
    pub client: async_nats::Client,
    pub topic: String,
}

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
