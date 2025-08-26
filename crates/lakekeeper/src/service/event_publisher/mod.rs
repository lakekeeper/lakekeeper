use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use anyhow::Context;
use async_trait::async_trait;
use cloudevents::Event;
use iceberg::{
    spec::{TableMetadata, ViewMetadata},
    TableIdent,
};
use iceberg_ext::catalog::rest::{
    CommitTransactionRequest, CommitViewRequest, CreateTableRequest, CreateViewRequest,
    RegisterTableRequest, RenameTableRequest,
};
use lakekeeper_io::Location;
use uuid::Uuid;

use super::{TableId, UndropTabularResponse, ViewId, WarehouseId};
use crate::{
    api::{
        iceberg::{
            types::{DropParams, Prefix},
            v1::{DataAccess, NamespaceParameters, TableParameters, ViewParameters},
        },
        management::v1::warehouse::UndropTabularsRequest,
        RequestMetadata,
    },
    catalog::tables::{maybe_body_to_json, CommitContext},
    service::{
        endpoint_hooks::{EndpointHook, TableIdentToIdFn, ViewCommit},
        tabular_idents::TabularId,
    },
    CONFIG,
};

#[cfg(feature = "kafka")]
pub mod kafka;
#[cfg(feature = "nats")]
pub mod nats;

/// Builds the default cloud event backends from the configuration.
///
/// # Errors
/// If the publisher cannot be built from the configuration.
pub async fn get_default_cloud_event_backends_from_config(
) -> anyhow::Result<Vec<Arc<dyn CloudEventBackend + Sync + Send>>> {
    let mut cloud_event_sinks = vec![];

    #[cfg(feature = "nats")]
    if let Some(nats_publisher) = nats::build_nats_publisher_from_config().await? {
        cloud_event_sinks
            .push(Arc::new(nats_publisher) as Arc<dyn CloudEventBackend + Sync + Send>);
    }
    #[cfg(feature = "kafka")]
    if let Some(kafka_publisher) = kafka::build_kafka_publisher_from_config()? {
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
impl EndpointHook for CloudEventsPublisher {
    async fn commit_transaction(
        &self,
        warehouse_id: WarehouseId,
        request: Arc<CommitTransactionRequest>,
        _commits: Arc<Vec<CommitContext>>,
        table_ident_to_id_fn: &TableIdentToIdFn,
        request_metadata: Arc<RequestMetadata>,
    ) -> anyhow::Result<()> {
        let estimated = request.table_changes.len();
        let mut events = Vec::with_capacity(estimated);
        let mut event_table_ids: Vec<(TableIdent, TableId)> = Vec::with_capacity(estimated);
        for commit_table_request in &request.table_changes {
            if let Some(id) = &commit_table_request.identifier {
                if let Some(uuid) = table_ident_to_id_fn(id) {
                    events.push(maybe_body_to_json(commit_table_request));
                    event_table_ids.push((id.clone(), uuid));
                }
            }
        }
        let number_of_events = events.len();
        let mut futs = Vec::with_capacity(number_of_events);
        for (event_sequence_number, (body, (table_ident, table_id))) in
            events.into_iter().zip(event_table_ids).enumerate()
        {
            futs.push(
                self.publish(
                    Uuid::now_v7(),
                    "updateTable",
                    body,
                    EventMetadata {
                        tabular_id: TabularId::Table(*table_id),
                        warehouse_id,
                        name: table_ident.name,
                        namespace: table_ident.namespace.to_url_string(),
                        prefix: String::new(),
                        num_events: number_of_events,
                        sequence_number: event_sequence_number,
                        trace_id: request_metadata.request_id(),
                        actor: serde_json::to_string(request_metadata.actor())
                            .map_err(|e| anyhow::anyhow!(e).context("Failed to serialize actor"))?,
                    },
                ),
            );
        }
        futures::future::try_join_all(futs)
            .await
            .context("Failed to publish `updateTable` event")?;
        Ok(())
    }

    async fn drop_table(
        &self,
        warehouse_id: WarehouseId,
        TableParameters { prefix, table }: TableParameters,
        _drop_params: DropParams,
        table_ident_uuid: TableId,
        request_metadata: Arc<RequestMetadata>,
    ) -> anyhow::Result<()> {
        self.publish(
            Uuid::now_v7(),
            "dropTable",
            serde_json::Value::Null,
            EventMetadata {
                tabular_id: TabularId::Table(*table_ident_uuid),
                warehouse_id,
                name: table.name,
                namespace: table.namespace.to_url_string(),
                prefix: prefix.map(Prefix::into_string).unwrap_or_default(),
                num_events: 1,
                sequence_number: 0,
                trace_id: request_metadata.request_id(),
                actor: serde_json::to_string(request_metadata.actor())
                    .map_err(|e| anyhow::anyhow!(e).context("Failed to serialize actor"))?,
            },
        )
        .await
        .context("Failed to publish `dropTable` event")?;
        Ok(())
    }
    async fn register_table(
        &self,
        warehouse_id: WarehouseId,
        NamespaceParameters { prefix, namespace }: NamespaceParameters,
        request: Arc<RegisterTableRequest>,
        metadata: Arc<TableMetadata>,
        _metadata_location: Arc<Location>,
        request_metadata: Arc<RequestMetadata>,
    ) -> anyhow::Result<()> {
        self.publish(
            Uuid::now_v7(),
            "registerTable",
            serde_json::Value::Null,
            EventMetadata {
                tabular_id: TabularId::Table(metadata.uuid()),
                warehouse_id,
                name: request.name.clone(),
                namespace: namespace.to_url_string(),
                prefix: prefix.map(Prefix::into_string).unwrap_or_default(),
                num_events: 1,
                sequence_number: 0,
                trace_id: request_metadata.request_id(),
                actor: serde_json::to_string(request_metadata.actor())
                    .map_err(|e| anyhow::anyhow!(e).context("Failed to serialize actor"))?,
            },
        )
        .await
        .context("Failed to publish `registerTable` event")?;
        Ok(())
    }

    async fn create_table(
        &self,
        warehouse_id: WarehouseId,
        NamespaceParameters { prefix, namespace }: NamespaceParameters,
        request: Arc<CreateTableRequest>,
        metadata: Arc<TableMetadata>,
        _metadata_location: Option<Arc<Location>>,
        _data_access: DataAccess,
        request_metadata: Arc<RequestMetadata>,
    ) -> anyhow::Result<()> {
        self.publish(
            Uuid::now_v7(),
            "createTable",
            serde_json::Value::Null,
            EventMetadata {
                tabular_id: TabularId::Table(metadata.uuid()),
                warehouse_id,
                name: request.name.clone(),
                namespace: namespace.to_url_string(),
                prefix: prefix.map(Prefix::into_string).unwrap_or_default(),
                num_events: 1,
                sequence_number: 0,
                trace_id: request_metadata.request_id(),
                actor: serde_json::to_string(request_metadata.actor())
                    .map_err(|e| anyhow::anyhow!(e).context("Failed to serialize actor"))?,
            },
        )
        .await
        .context("Failed to publish `createTable` event")?;
        Ok(())
    }

    async fn rename_table(
        &self,
        warehouse_id: WarehouseId,
        table_ident_uuid: TableId,
        request: Arc<RenameTableRequest>,
        request_metadata: Arc<RequestMetadata>,
    ) -> anyhow::Result<()> {
        self.publish(
            Uuid::now_v7(),
            "renameTable",
            serde_json::Value::Null,
            EventMetadata {
                tabular_id: TabularId::Table(*table_ident_uuid),
                warehouse_id,
                name: request.source.name.clone(),
                namespace: request.source.namespace.to_url_string(),
                prefix: String::new(),
                num_events: 1,
                sequence_number: 0,
                trace_id: request_metadata.request_id(),
                actor: serde_json::to_string(request_metadata.actor())
                    .map_err(|e| anyhow::anyhow!(e).context("Failed to serialize actor"))?,
            },
        )
        .await
        .context("Failed to publish `renameTable` event")?;
        Ok(())
    }

    async fn create_view(
        &self,
        warehouse_id: WarehouseId,
        parameters: NamespaceParameters,
        request: Arc<CreateViewRequest>,
        metadata: Arc<ViewMetadata>,
        _metadata_location: Arc<Location>,
        _data_access: DataAccess,
        request_metadata: Arc<RequestMetadata>,
    ) -> anyhow::Result<()> {
        self.publish(
            Uuid::now_v7(),
            "createView",
            maybe_body_to_json(&request),
            EventMetadata {
                tabular_id: TabularId::View(metadata.uuid()),
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
                actor: serde_json::to_string(request_metadata.actor())
                    .map_err(|e| anyhow::anyhow!(e).context("Failed to serialize actor"))?,
            },
        )
        .await
        .context("Failed to publish `createView` event")?;
        Ok(())
    }

    async fn commit_view(
        &self,
        warehouse_id: WarehouseId,
        parameters: ViewParameters,
        request: Arc<CommitViewRequest>,
        metadata: Arc<ViewCommit>,
        _data_access: DataAccess,
        request_metadata: Arc<RequestMetadata>,
    ) -> anyhow::Result<()> {
        self.publish(
            Uuid::now_v7(),
            "updateView",
            maybe_body_to_json(request),
            EventMetadata {
                tabular_id: TabularId::View(metadata.new_metadata.uuid()),
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
                actor: serde_json::to_string(request_metadata.actor())
                    .map_err(|e| anyhow::anyhow!(e).context("Failed to serialize actor"))?,
            },
        )
        .await
        .context("Failed to publish `updateView` event")?;
        Ok(())
    }

    async fn drop_view(
        &self,
        warehouse_id: WarehouseId,
        parameters: ViewParameters,
        _drop_params: DropParams,
        view_ident_uuid: ViewId,
        request_metadata: Arc<RequestMetadata>,
    ) -> anyhow::Result<()> {
        self.publish(
            Uuid::now_v7(),
            "dropView",
            serde_json::Value::Null,
            EventMetadata {
                tabular_id: TabularId::View(*view_ident_uuid),
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
                actor: serde_json::to_string(request_metadata.actor())
                    .map_err(|e| anyhow::anyhow!(e).context("Failed to serialize actor"))?,
            },
        )
        .await
        .context("Failed to publish `dropView` event")?;
        Ok(())
    }

    async fn rename_view(
        &self,
        warehouse_id: WarehouseId,
        view_ident_uuid: ViewId,
        request: Arc<RenameTableRequest>,
        request_metadata: Arc<RequestMetadata>,
    ) -> anyhow::Result<()> {
        self.publish(
            Uuid::now_v7(),
            "renameView",
            serde_json::Value::Null,
            EventMetadata {
                tabular_id: TabularId::View(*view_ident_uuid),
                warehouse_id,
                name: request.source.name.clone(),
                namespace: request.source.namespace.to_url_string(),
                prefix: String::new(),
                num_events: 1,
                sequence_number: 0,
                trace_id: request_metadata.request_id(),
                actor: serde_json::to_string(request_metadata.actor())
                    .map_err(|e| anyhow::anyhow!(e).context("Failed to serialize actor"))?,
            },
        )
        .await
        .context("Failed to publish `renameView` event")?;
        Ok(())
    }

    async fn undrop_tabular(
        &self,
        warehouse_id: WarehouseId,
        _request: Arc<UndropTabularsRequest>,
        responses: Arc<Vec<UndropTabularResponse>>,
        request_metadata: Arc<RequestMetadata>,
    ) -> anyhow::Result<()> {
        let num_tabulars = responses.len();
        let mut futs = Vec::with_capacity(responses.len());
        for (idx, utr) in responses.iter().enumerate() {
            futs.push(
                self.publish(
                    Uuid::now_v7(),
                    "undropTabulars",
                    serde_json::Value::Null,
                    EventMetadata {
                        tabular_id: TabularId::from(utr.table_ident),
                        warehouse_id,
                        name: utr.name.clone(),
                        namespace: utr.namespace.to_url_string(),
                        prefix: String::new(),
                        num_events: num_tabulars,
                        sequence_number: idx,
                        trace_id: request_metadata.request_id(),
                        actor: serde_json::to_string(request_metadata.actor())
                            .map_err(|e| anyhow::anyhow!(e).context("Failed to serialize actor"))?,
                    },
                ),
            );
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
                actor,
            } = metadata;
            // TODO: this could be more elegant with a proc macro to give us IntoIter for EventMetadata
            let event = event_builder
                .extension("tabular-type", tabular_id.typ_str())
                .extension("tabular-id", tabular_id.to_string())
                .extension("warehouse-id", warehouse_id.to_string())
                .extension("name", name.to_string())
                .extension("namespace", namespace.to_string())
                .extension("prefix", prefix.to_string())
                .extension("num-events", i64::try_from(num_events).unwrap_or(i64::MAX))
                .extension(
                    "sequence-number",
                    i64::try_from(sequence_number).unwrap_or(i64::MAX),
                )
                // Implement distributed tracing: https://github.com/lakekeeper/lakekeeper/issues/63
                .extension("trace-id", trace_id.to_string())
                .extension("actor", actor)
                .build()?;

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
