//! NATS cloud-events publisher for Lakekeeper.
//!
//! Implements [`lakekeeper::service::events::CloudEventBackend`] for a NATS
//! client. Configured via env vars under the same `LAKEKEEPER__` /
//! `ICEBERG_REST__` prefix as core; the [`config::CONFIG`] static aggregates
//! the NATS-specific fields.

pub mod config;

use async_trait::async_trait;
use cloudevents::Event;
use lakekeeper::service::events::CloudEventBackend;

use crate::config::CONFIG;

/// Generate a NATS publisher from the crate's configuration.
/// Returns `None` if the NATS address or topic is not set.
///
/// # Errors
/// - If NATS is configured but the connection or authentication fails.
pub async fn build_nats_publisher_from_config() -> anyhow::Result<Option<NatsBackend>> {
    let (Some(nats_addr), Some(nats_topic)) =
        (CONFIG.nats_address.clone(), CONFIG.nats_topic.clone())
    else {
        tracing::info!("NATS address or topic not set. Events are not published to NATS.");
        return Ok(None);
    };

    if nats_topic.trim().is_empty() {
        tracing::info!("NATS topic is empty. Events are not published to NATS.");
        return Ok(None);
    }

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

    let client = builder.connect(nats_addr.to_string()).await.map_err(|e| {
        anyhow::anyhow!(e).context(format!("Failed to connect to NATS at {nats_addr}"))
    })?;
    let nats_publisher = NatsBackend::builder()
        .client(client)
        .topic(nats_topic.clone())
        .build();

    tracing::info!("Publishing events to NATS topic {nats_topic}, NATS address is: {nats_addr}");
    Ok(Some(nats_publisher))
}

#[derive(Debug, typed_builder::TypedBuilder)]
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
