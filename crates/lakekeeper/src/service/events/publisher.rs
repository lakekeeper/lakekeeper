use std::{
    fmt::{Debug, Display},
    sync::{Arc, LazyLock},
};

use anyhow::Context;
use async_trait::async_trait;
use cloudevents::Event;
use iceberg::TableIdent;
use uuid::Uuid;

use super::{dispatch::EventListener, types};
use crate::{
    CONFIG,
    api::RequestMetadata,
    api::iceberg::{
        types::Prefix,
        v1::{NamespaceParameters, TableParameters},
    },
    server::tables::maybe_body_to_json,
    service::{TableId, TabularId, WarehouseId},
};

/// Cached hostname for CloudEvents source URI. Resolved once at first access.
static HOSTNAME: LazyLock<String> = LazyLock::new(|| {
    hostname::get().map_or_else(
        |_| "hostname-unavailable".into(),
        |os| os.to_string_lossy().to_string(),
    )
});

/// Serializes the actor from request metadata to a JSON string.
///
/// # Errors
/// Returns an error if the actor cannot be serialized.
fn serialize_actor(request_metadata: &RequestMetadata) -> anyhow::Result<String> {
    serde_json::to_string(request_metadata.actor())
        .map_err(|e| anyhow::anyhow!(e).context("Failed to serialize actor"))
}

/// Builds the default cloud event backends from the configuration.
///
/// # Errors
/// If the publisher cannot be built from the configuration.
#[allow(clippy::unused_async)]
pub async fn get_default_cloud_event_backends_from_config()
-> anyhow::Result<Vec<Arc<dyn CloudEventBackend + Sync + Send>>> {
    let mut cloud_event_sinks = vec![];

    #[cfg(feature = "nats")]
    if let Some(nats_publisher) = super::backends::nats::build_nats_publisher_from_config().await? {
        cloud_event_sinks
            .push(Arc::new(nats_publisher) as Arc<dyn CloudEventBackend + Sync + Send>);
    }
    #[cfg(feature = "kafka")]
    if let Some(kafka_publisher) = super::backends::kafka::build_kafka_publisher_from_config()? {
        cloud_event_sinks
            .push(Arc::new(kafka_publisher) as Arc<dyn CloudEventBackend + Sync + Send>);
    }

    if let Some(true) = &CONFIG.log_cloudevents {
        let tracing_publisher = TracingPublisher;
        cloud_event_sinks
            .push(Arc::new(tracing_publisher) as Arc<dyn CloudEventBackend + Sync + Send>);
        tracing::info!("Logging Cloudevents to Console.");
    } else {
        tracing::info!("Running without logging Cloudevents.");
    }

    if cloud_event_sinks.is_empty() {
        tracing::info!("Running without publisher.");
    }

    Ok(cloud_event_sinks)
}

#[async_trait::async_trait]
impl EventListener for CloudEventsPublisher {
    async fn transaction_committed(
        &self,
        event: types::CommitTransactionEvent,
    ) -> anyhow::Result<()> {
        let types::CommitTransactionEvent {
            warehouse_id,
            request,
            commits: _commits,
            table_ident_to_id_fn,
            request_metadata,
        } = event;
        let estimated = request.table_changes.len();
        let mut events = Vec::with_capacity(estimated);
        let mut event_table_ids: Vec<(TableIdent, TableId)> = Vec::with_capacity(estimated);
        for commit_table_request in &request.table_changes {
            if let Some(id) = &commit_table_request.identifier
                && let Some(uuid) = (*table_ident_to_id_fn)(id)
            {
                events.push(maybe_body_to_json(commit_table_request));
                event_table_ids.push((id.clone(), uuid));
            }
        }
        let number_of_events = events.len();
        let mut futs = Vec::with_capacity(number_of_events);
        for (event_sequence_number, (body, (table_ident, table_id))) in
            events.into_iter().zip(event_table_ids).enumerate()
        {
            futs.push(self.publish(
                Uuid::now_v7(),
                "updateTable",
                body,
                EventMetadata {
                    tabular_id: TabularId::Table(table_id),
                    warehouse_id,
                    name: table_ident.name,
                    namespace: table_ident.namespace.to_url_string(),
                    prefix: String::new(),
                    num_events: number_of_events,
                    sequence_number: event_sequence_number,
                    trace_id: request_metadata.request_id(),
                    actor: serialize_actor(&request_metadata)?,
                },
            ));
        }
        futures::future::try_join_all(futs)
            .await
            .context("Failed to publish `updateTable` event")?;
        Ok(())
    }

    async fn table_dropped(&self, event: types::DropTableEvent) -> anyhow::Result<()> {
        let types::DropTableEvent {
            warehouse_id,
            parameters: TableParameters { prefix, table },
            drop_params: _drop_params,
            table_id: table_ident_uuid,
            request_metadata,
        } = event;
        self.publish(
            Uuid::now_v7(),
            "dropTable",
            serde_json::Value::Null,
            EventMetadata {
                tabular_id: TabularId::Table(table_ident_uuid),
                warehouse_id,
                name: table.name,
                namespace: table.namespace.to_url_string(),
                prefix: prefix.map(Prefix::into_string).unwrap_or_default(),
                num_events: 1,
                sequence_number: 0,
                trace_id: request_metadata.request_id(),
                actor: serialize_actor(&request_metadata)?,
            },
        )
        .await
        .context("Failed to publish `dropTable` event")?;
        Ok(())
    }

    async fn table_registered(&self, event: types::RegisterTableEvent) -> anyhow::Result<()> {
        let types::RegisterTableEvent {
            warehouse_id,
            parameters: NamespaceParameters { prefix, namespace },
            request,
            metadata,
            metadata_location: _metadata_location,
            request_metadata,
        } = event;
        self.publish(
            Uuid::now_v7(),
            "registerTable",
            serde_json::Value::Null,
            EventMetadata {
                tabular_id: TabularId::Table(metadata.uuid().into()),
                warehouse_id,
                name: request.name.clone(),
                namespace: namespace.to_url_string(),
                prefix: prefix.map(Prefix::into_string).unwrap_or_default(),
                num_events: 1,
                sequence_number: 0,
                trace_id: request_metadata.request_id(),
                actor: serialize_actor(&request_metadata)?,
            },
        )
        .await
        .context("Failed to publish `registerTable` event")?;
        Ok(())
    }

    async fn table_created(&self, event: types::CreateTableEvent) -> anyhow::Result<()> {
        let types::CreateTableEvent {
            warehouse_id,
            parameters: NamespaceParameters { prefix, namespace },
            request,
            metadata,
            metadata_location: _metadata_location,
            data_access: _data_access,
            request_metadata,
        } = event;
        self.publish(
            Uuid::now_v7(),
            "createTable",
            serde_json::Value::Null,
            EventMetadata {
                tabular_id: TabularId::Table(metadata.uuid().into()),
                warehouse_id,
                name: request.name.clone(),
                namespace: namespace.to_url_string(),
                prefix: prefix.map(Prefix::into_string).unwrap_or_default(),
                num_events: 1,
                sequence_number: 0,
                trace_id: request_metadata.request_id(),
                actor: serialize_actor(&request_metadata)?,
            },
        )
        .await
        .context("Failed to publish `createTable` event")?;
        Ok(())
    }

    async fn table_renamed(&self, event: types::RenameTableEvent) -> anyhow::Result<()> {
        let types::RenameTableEvent {
            warehouse_id,
            table_id: table_ident_uuid,
            request,
            request_metadata,
        } = event;
        self.publish(
            Uuid::now_v7(),
            "renameTable",
            serde_json::Value::Null,
            EventMetadata {
                tabular_id: TabularId::Table(table_ident_uuid),
                warehouse_id,
                name: request.source.name.clone(),
                namespace: request.source.namespace.to_url_string(),
                prefix: String::new(),
                num_events: 1,
                sequence_number: 0,
                trace_id: request_metadata.request_id(),
                actor: serialize_actor(&request_metadata)?,
            },
        )
        .await
        .context("Failed to publish `renameTable` event")?;
        Ok(())
    }

    async fn view_created(&self, event: types::CreateViewEvent) -> anyhow::Result<()> {
        let types::CreateViewEvent {
            warehouse_id,
            parameters,
            request,
            metadata,
            metadata_location: _metadata_location,
            data_access: _data_access,
            request_metadata,
        } = event;
        self.publish(
            Uuid::now_v7(),
            "createView",
            maybe_body_to_json(&request),
            EventMetadata {
                tabular_id: TabularId::View(metadata.uuid().into()),
                warehouse_id,
                name: request.name.clone(),
                namespace: parameters.namespace.to_url_string(),
                prefix: parameters
                    .prefix
                    .map(Prefix::into_string)
                    .unwrap_or_default(),
                num_events: 1,
                sequence_number: 0,
                trace_id: request_metadata.request_id(),
                actor: serialize_actor(&request_metadata)?,
            },
        )
        .await
        .context("Failed to publish `createView` event")?;
        Ok(())
    }

    async fn view_committed(&self, event: types::CommitViewEvent) -> anyhow::Result<()> {
        let types::CommitViewEvent {
            warehouse_id,
            parameters,
            request,
            view_commit: metadata,
            data_access: _data_access,
            request_metadata,
        } = event;
        self.publish(
            Uuid::now_v7(),
            "updateView",
            maybe_body_to_json(request),
            EventMetadata {
                tabular_id: TabularId::View(metadata.new_metadata.uuid().into()),
                warehouse_id,
                name: parameters.view.name,
                namespace: parameters.view.namespace.to_url_string(),
                prefix: parameters
                    .prefix
                    .map(Prefix::into_string)
                    .unwrap_or_default(),
                num_events: 1,
                sequence_number: 0,
                trace_id: request_metadata.request_id(),
                actor: serialize_actor(&request_metadata)?,
            },
        )
        .await
        .context("Failed to publish `updateView` event")?;
        Ok(())
    }

    async fn view_dropped(&self, event: types::DropViewEvent) -> anyhow::Result<()> {
        let types::DropViewEvent {
            warehouse_id,
            parameters,
            drop_params: _drop_params,
            view_id: view_ident_uuid,
            request_metadata,
        } = event;
        self.publish(
            Uuid::now_v7(),
            "dropView",
            serde_json::Value::Null,
            EventMetadata {
                tabular_id: TabularId::View(view_ident_uuid),
                warehouse_id,
                name: parameters.view.name,
                namespace: parameters.view.namespace.to_url_string(),
                prefix: parameters
                    .prefix
                    .map(Prefix::into_string)
                    .unwrap_or_default(),
                num_events: 1,
                sequence_number: 0,
                trace_id: request_metadata.request_id(),
                actor: serialize_actor(&request_metadata)?,
            },
        )
        .await
        .context("Failed to publish `dropView` event")?;
        Ok(())
    }

    async fn view_renamed(&self, event: types::RenameViewEvent) -> anyhow::Result<()> {
        let types::RenameViewEvent {
            warehouse_id,
            view_id: view_ident_uuid,
            request,
            request_metadata,
        } = event;
        self.publish(
            Uuid::now_v7(),
            "renameView",
            serde_json::Value::Null,
            EventMetadata {
                tabular_id: TabularId::View(view_ident_uuid),
                warehouse_id,
                name: request.source.name.clone(),
                namespace: request.source.namespace.to_url_string(),
                prefix: String::new(),
                num_events: 1,
                sequence_number: 0,
                trace_id: request_metadata.request_id(),
                actor: serialize_actor(&request_metadata)?,
            },
        )
        .await
        .context("Failed to publish `renameView` event")?;
        Ok(())
    }

    async fn tabular_undropped(&self, event: types::UndropTabularEvent) -> anyhow::Result<()> {
        let types::UndropTabularEvent {
            warehouse_id,
            request: _request,
            responses,
            request_metadata,
        } = event;
        let num_tabulars = responses.len();
        let mut futs = Vec::with_capacity(responses.len());
        for (idx, tabular_info) in responses.iter().enumerate() {
            futs.push(self.publish(
                Uuid::now_v7(),
                "undropTabulars",
                serde_json::Value::Null,
                EventMetadata {
                    tabular_id: tabular_info.tabular_id(),
                    warehouse_id,
                    name: tabular_info.tabular_ident().name.clone(),
                    namespace: tabular_info.tabular_ident().namespace.to_url_string(),
                    prefix: String::new(),
                    num_events: num_tabulars,
                    sequence_number: idx,
                    trace_id: request_metadata.request_id(),
                    actor: serialize_actor(&request_metadata)?,
                },
            ));
        }
        futures::future::try_join_all(futs)
            .await
            .map_err(|e| {
                tracing::error!("Failed to publish event: {e}");
                e
            })
            .context("Failed to publish `undropTabulars` event")?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct CloudEventsPublisher {
    tx: tokio::sync::mpsc::Sender<CloudEventsMessage>,
    timeout: tokio::time::Duration,
}

impl Display for CloudEventsPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CloudEventsPublisher")
    }
}

impl CloudEventsPublisher {
    #[must_use]
    pub fn new(tx: tokio::sync::mpsc::Sender<CloudEventsMessage>) -> Self {
        Self::new_with_timeout(tx, tokio::time::Duration::from_millis(50))
    }

    #[must_use]
    pub fn new_with_timeout(
        tx: tokio::sync::mpsc::Sender<CloudEventsMessage>,
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
                CloudEventsMessage::Event(Payload {
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
    pub tabular_id: TabularId,
    pub warehouse_id: WarehouseId,
    pub name: String,
    pub namespace: String,
    pub prefix: String,
    pub num_events: usize,
    pub sequence_number: usize,
    pub trace_id: Uuid,
    pub actor: String,
}

#[derive(Debug)]
pub struct Payload {
    pub id: Uuid,
    pub typ: String,
    pub data: serde_json::Value,
    pub metadata: EventMetadata,
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum CloudEventsMessage {
    Event(Payload),
    Shutdown,
}

#[derive(Debug)]
pub struct CloudEventsPublisherBackgroundTask {
    pub source: tokio::sync::mpsc::Receiver<CloudEventsMessage>,
    pub sinks: Vec<Arc<dyn CloudEventBackend + Sync + Send>>,
}

impl CloudEventsPublisherBackgroundTask {
    /// # Errors
    /// Returns an error if the `Event` cannot be built from the data passed into this function
    pub async fn publish(mut self) -> anyhow::Result<()> {
        while let Some(CloudEventsMessage::Event(Payload {
            id,
            typ,
            data,
            metadata,
        })) = self.source.recv().await
        {
            use cloudevents::{EventBuilder, EventBuilderV10};

            let event_builder = EventBuilderV10::new()
                .id(id.to_string())
                .source(format!("uri:iceberg-catalog-service:{}", &*HOSTNAME))
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
                actor,
            } = metadata;
            // TODO: this could be more elegant with a proc macro to give us IntoIter for EventMetadata
            let event = match event_builder
                .extension("tabular-type", tabular_id.typ_str())
                .extension("tabular-id", tabular_id.to_string())
                .extension("warehouse-id", warehouse_id.to_string())
                .extension("name", name.clone())
                .extension("namespace", namespace.clone())
                .extension("prefix", prefix.clone())
                .extension("num-events", i64::try_from(num_events).unwrap_or(i64::MAX))
                .extension(
                    "sequence-number",
                    i64::try_from(sequence_number).unwrap_or(i64::MAX),
                )
                // Implement distributed tracing: https://github.com/lakekeeper/lakekeeper/issues/63
                .extension("trace-id", trace_id.to_string())
                .extension("actor", actor)
                .build()
            {
                Ok(event) => event,
                Err(e) => {
                    tracing::warn!("Failed to build CloudEvent with id '{id}': {e}");
                    continue;
                }
            };

            let publish_futures = self.sinks.iter().map(|sink| {
                let event = event.clone();
                async move {
                    if let Err(e) = sink.publish(event).await {
                        tracing::warn!(
                            "Failed to emit event with id: '{}' on sink: '{}' due to: '{}'.",
                            id,
                            sink.name(),
                            e
                        );
                    }
                    Ok::<_, anyhow::Error>(())
                }
            });

            // Run all publish operations concurrently
            futures::future::join_all(publish_futures).await;
        }

        Ok(())
    }
}

#[async_trait]
pub trait CloudEventBackend: Debug {
    async fn publish(&self, event: Event) -> anyhow::Result<()>;
    fn name(&self) -> &str;
}

#[derive(Clone, Debug)]
pub struct TracingPublisher;

#[async_trait::async_trait]
impl CloudEventBackend for TracingPublisher {
    async fn publish(&self, event: Event) -> anyhow::Result<()> {
        let data =
            serde_json::to_value(&event).unwrap_or(serde_json::json!("Event serialization failed"));
        tracing::info!(event=%data, "CloudEvent");
        Ok(())
    }

    fn name(&self) -> &'static str {
        "tracing-publisher"
    }
}
