#![allow(clippy::module_name_repetitions)]

use std::{
    collections::HashMap,
    fmt::Debug,
    str::FromStr,
    sync::{atomic::AtomicI64, Arc},
    time::Duration,
};

#[cfg(feature = "router")]
use axum::{
    extract::{Path, Query, Request, State},
    middleware::Next,
    response::Response,
};
use http::StatusCode;
use tracing::Instrument;
use uuid::Uuid;

use crate::{api::endpoints::Endpoint, request_metadata::RequestMetadata, ProjectId, WarehouseId};

#[cfg(feature = "router")]
/// Middleware for tracking endpoint statistics.
///
/// This middleware forwards information about the called endpoint to the receiver of
/// `EndpointStatisticsTrackerTx`.
pub(crate) async fn endpoint_statistics_middleware_fn(
    State(tracker): State<EndpointStatisticsTrackerTx>,
    Path(path_params): Path<HashMap<String, String>>,
    Query(query_params): Query<HashMap<String, String>>,
    request: Request,
    next: Next,
) -> Response {
    let request_metadata = request.extensions().get::<RequestMetadata>().cloned();

    let response = next.run(request).await;

    if let Some(request_metadata) = request_metadata {
        if let Err(e) = tracker
            .0
            .send(EndpointStatisticsMessage::EndpointCalled {
                request_metadata,
                response_status: response.status(),
                path_params,
                query_params,
            })
            .await
        {
            tracing::error!("Failed to send endpoint statistics message: {}", e);
        };
    } else {
        tracing::error!(?path_params, "No request metadata found.");
    }

    response
}

/// Sender for the endpoint statistics tracker.
#[derive(Debug, Clone)]
pub struct EndpointStatisticsTrackerTx(tokio::sync::mpsc::Sender<EndpointStatisticsMessage>);

impl EndpointStatisticsTrackerTx {
    #[must_use]
    pub fn new(tx: tokio::sync::mpsc::Sender<EndpointStatisticsMessage>) -> Self {
        Self(tx)
    }

    /// Send a message to the endpoint statistics tracker.
    ///
    /// # Errors
    /// If the receiver has been dropped.
    pub async fn send(
        &self,
        msg: EndpointStatisticsMessage,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<EndpointStatisticsMessage>> {
        self.0.send(msg).await
    }
}

#[derive(Debug)]
pub enum EndpointStatisticsMessage {
    EndpointCalled {
        request_metadata: RequestMetadata,
        response_status: StatusCode,
        path_params: HashMap<String, String>,
        query_params: HashMap<String, String>,
    },
    Flush,
    Shutdown,
}

#[derive(Debug, Default)]
pub struct ProjectStatistics {
    stats: HashMap<EndpointIdentifier, AtomicI64>,
}

impl ProjectStatistics {
    #[must_use]
    pub fn into_consumable(self) -> HashMap<EndpointIdentifier, i64> {
        self.stats
            .into_iter()
            .map(|(k, v)| (k, v.load(std::sync::atomic::Ordering::Relaxed)))
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EndpointIdentifier {
    pub uri: Endpoint,
    pub status_code: StatusCode,
    pub warehouse: Option<WarehouseId>,
    // probably only relevant for config calls
    pub warehouse_name: Option<String>,
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum FlushMode {
    Automatic,
    #[cfg(test)]
    Manual,
}

#[derive(Debug)]
pub struct EndpointStatisticsTracker {
    rcv: tokio::sync::mpsc::Receiver<EndpointStatisticsMessage>,
    endpoint_statistics: HashMap<ProjectId, ProjectStatistics>,
    statistic_sinks: Vec<Arc<dyn EndpointStatisticsSink>>,
    flush_interval: Duration,
    flush_mode: FlushMode,
}

#[derive(Debug)]
enum LoopState {
    Continue,
    Break,
}

impl EndpointStatisticsTracker {
    #[must_use]
    pub fn new(
        rcv: tokio::sync::mpsc::Receiver<EndpointStatisticsMessage>,
        stat_sinks: Vec<Arc<dyn EndpointStatisticsSink>>,
        flush_interval: Duration,
        flush_mode: FlushMode,
    ) -> Self {
        Self {
            rcv,
            endpoint_statistics: HashMap::new(),
            statistic_sinks: stat_sinks,
            flush_interval,
            flush_mode,
        }
    }

    pub async fn run(mut self) {
        let mut last_update = tokio::time::Instant::now();
        loop {
            let span =
                tracing::info_span!("endpoint_statistics_tracker", iteration_id=%Uuid::now_v7());

            match self.loop_tick(&mut last_update).instrument(span).await {
                LoopState::Continue => {}
                LoopState::Break => {
                    self.close().await;
                    break;
                }
            }
        }
    }

    async fn loop_tick(&mut self, last_update: &mut tokio::time::Instant) -> LoopState {
        if matches!(self.flush_mode, FlushMode::Automatic)
            && last_update.elapsed() > self.flush_interval
        {
            tracing::debug!(
                "Flushing stats after: {}ms",
                last_update.elapsed().as_millis()
            );
            self.flush_storage().await;
            *last_update = tokio::time::Instant::now();
        }

        let msg = if matches!(self.flush_mode, FlushMode::Automatic) {
            let Ok(msg) = tokio::time::timeout(self.flush_interval, self.rcv.recv()).await else {
                tracing::debug!("No message received, continuing.");
                return LoopState::Continue;
            };
            msg
        } else {
            self.rcv.recv().await
        };

        let Some(msg) = msg else {
            tracing::info!("Channel closed, breaking rcv loop.");
            return LoopState::Break;
        };

        match msg {
            EndpointStatisticsMessage::EndpointCalled {
                request_metadata,
                response_status,
                path_params,
                query_params,
            } => {
                self.process_endpoint_called(
                    &request_metadata,
                    response_status,
                    &path_params,
                    &query_params,
                );
                LoopState::Continue
            }
            EndpointStatisticsMessage::Shutdown => {
                tracing::info!("Received shutdown message, breaking rcv loop.");
                LoopState::Break
            }
            EndpointStatisticsMessage::Flush => {
                tracing::info!("Received flush message, flushing sinks.");
                self.flush_storage().await;
                LoopState::Continue
            }
        }
    }

    async fn close(mut self) {
        tracing::info!("Shutting down endpoint statistics tracker, flushing storage.");
        self.rcv.close();
        tracing::trace!("Channel closed, flushing stats.");
        self.flush_storage().await;
        tracing::trace!("Endpoint Statistics Tracker done.");
    }

    async fn flush_storage(&mut self) {
        let mut stats = HashMap::new();
        std::mem::swap(&mut stats, &mut self.endpoint_statistics);

        let s: HashMap<ProjectId, HashMap<EndpointIdentifier, i64>> = stats
            .into_iter()
            .map(|(k, v)| (k, v.into_consumable()))
            .collect();

        for sink in &self.statistic_sinks {
            tracing::debug!("Sinking stats for '{}'", sink.sink_id());
            if let Err(e) = sink.consume_endpoint_statistics(s.clone()).await {
                tracing::error!(
                    "Failed to consume stats for sink '{}' due to: {}",
                    sink.sink_id(),
                    e.error
                );
            };
        }
    }

    fn process_endpoint_called(
        &mut self,
        request_metadata: &RequestMetadata,
        response_status: StatusCode,
        path_params: &HashMap<String, String>,
        query_params: &HashMap<String, String>,
    ) {
        let warehouse = Self::maybe_get_warehouse_ident(path_params);

        let Some(matched_path) = request_metadata.matched_path() else {
            tracing::trace!("No path matched.");
            return;
        };

        let Some(uri) =
            Endpoint::from_method_and_matched_path(request_metadata.request_method(), matched_path)
        else {
            tracing::error!(
                            "Could not parse endpoint from matched path: '{} {}'. This is likely a bug which will affect the statistics collection.",
                            request_metadata.request_method(),
                            matched_path
                        );
            return;
        };
        let Some(project) = request_metadata.preferred_project_id() else {
            tracing::debug!("No project specified, request not counted.");
            return;
        };

        self.endpoint_statistics
            .entry(project)
            .or_default()
            .stats
            .entry(EndpointIdentifier {
                warehouse,
                uri,
                status_code: response_status,
                warehouse_name: query_params.get("warehouse").cloned(),
            })
            .or_insert_with(|| AtomicI64::new(0))
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn maybe_get_warehouse_ident(path_params: &HashMap<String, String>) -> Option<WarehouseId> {
        path_params
            .get("warehouse_id")
            .map(|s| WarehouseId::from_str(s.as_str()))
            .transpose()
            .inspect_err(|e| tracing::debug!("Could not parse warehouse: {}", e.error))
            .ok()
            .flatten()
            .or(path_params
                .get("prefix")
                .map(|s| Uuid::from_str(s.as_str()))
                .transpose()
                .inspect_err(|e| tracing::debug!("Could not parse prefix: {}", e))
                .ok()
                .flatten()
                .map(WarehouseId::from))
    }
}

// E.g. postgres consumer which populates some postgres tables
#[async_trait::async_trait]
pub trait EndpointStatisticsSink: Debug + Send + Sync + 'static {
    async fn consume_endpoint_statistics(
        &self,
        stats: HashMap<ProjectId, HashMap<EndpointIdentifier, i64>>,
    ) -> crate::api::Result<()>;

    fn sink_id(&self) -> &'static str;
}
