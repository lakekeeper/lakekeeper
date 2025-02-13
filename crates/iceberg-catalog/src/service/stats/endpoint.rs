#![allow(clippy::module_name_repetitions)]

use std::{
    collections::HashMap,
    fmt::Debug,
    str::FromStr,
    sync::{atomic::AtomicI64, Arc},
    time::Duration,
};

use crate::{
    api::endpoints::Endpoints, request_metadata::RequestMetadata, ProjectIdent, WarehouseIdent,
};
use axum::extract::Query;
use axum::{
    extract::{Path, Request, State},
    middleware::Next,
    response::Response,
};
use http::StatusCode;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct TrackerTx(tokio::sync::mpsc::Sender<Message>);

impl TrackerTx {
    #[must_use]
    pub fn new(tx: tokio::sync::mpsc::Sender<Message>) -> Self {
        Self(tx)
    }
}

// TODO: We're aggregating endpoint statistics per warehouse, which means we'll have to somehow
//       extract the warehouse id from the request. That's no fun
pub(crate) async fn stats_middleware_fn(
    State(tracker): State<TrackerTx>,
    Path(path_params): Path<HashMap<String, String>>,
    Query(query_params): Query<HashMap<String, String>>,
    request: Request,
    next: Next,
) -> Response {
    let rm = request
        .extensions()
        .get::<RequestMetadata>()
        .unwrap()
        .clone();

    let response = next.run(request).await;
    tracker
        .0
        .send(Message::EndpointCalled {
            request_metadata: rm,
            response_status: response.status(),
            path_params,
            query_params,
        })
        .await
        .unwrap();

    response
}

#[derive(Debug)]
pub enum Message {
    EndpointCalled {
        request_metadata: RequestMetadata,
        response_status: StatusCode,
        path_params: HashMap<String, String>,
        query_params: HashMap<String, String>,
    },
}

#[derive(Debug, Default)]
pub struct ProjectStats {
    stats: HashMap<EndpointIdentifier, AtomicI64>,
}

impl ProjectStats {
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
    pub uri: Endpoints,
    pub status_code: StatusCode,
    pub warehouse: Option<WarehouseIdent>,
    // probably only relevant for config calls
    pub warehouse_name: Option<String>,
}

#[derive(Debug)]
pub struct Tracker {
    rcv: tokio::sync::mpsc::Receiver<Message>,
    endpoint_stats: HashMap<Option<ProjectIdent>, ProjectStats>,
    stat_sinks: Vec<Arc<dyn StatsSink>>,
}

impl Tracker {
    #[must_use]
    pub fn new(
        rcv: tokio::sync::mpsc::Receiver<Message>,
        stat_sinks: Vec<Arc<dyn StatsSink>>,
    ) -> Self {
        Self {
            rcv,
            endpoint_stats: HashMap::new(),
            stat_sinks,
        }
    }

    async fn recv_with_timeout(&mut self) -> Option<Message> {
        tokio::select! {
            msg = self.rcv.recv() => msg,
            _ = tokio::time::sleep(Duration::from_secs(15)) => None,
        }
    }

    pub async fn run(mut self) {
        let mut last_update = tokio::time::Instant::now();
        loop {
            tracing::debug!(
                "Checking if we should consume stats, elapsed: {}",
                last_update.elapsed().as_millis()
            );
            if last_update.elapsed() > Duration::from_secs(15) {
                tracing::debug!("Consuming stats");
                self.consume_stats().await;
                last_update = tokio::time::Instant::now();
            }

            let Some(msg) = self.recv_with_timeout().await else {
                tracing::debug!("No message received, continuing.");
                continue;
            };
            tracing::debug!("Received message: {:?}", msg);
            match msg {
                Message::EndpointCalled {
                    request_metadata,
                    response_status,
                    path_params,
                    query_params,
                } => {
                    let project_id = request_metadata.project_id();

                    // TODO: use authz to check if project is accessible

                    let warehouse = dbg!(&path_params)
                        .get("warehouse_id")
                        .map(|s| WarehouseIdent::from_str(s.as_str()))
                        .transpose()
                        .ok()
                        .flatten()
                        .or(path_params
                            .get("prefix")
                            .map(|s| Uuid::from_str(s.as_str()))
                            .transpose()
                            .inspect_err(|e| tracing::debug!("Could not parse prefix: {}", e))
                            .ok()
                            .flatten()
                            .map(WarehouseIdent::from));
                    let Some(mp) = request_metadata.matched_path.as_ref() else {
                        tracing::error!("No path matched.");
                        continue;
                    };

                    let Some(uri) = Endpoints::from_method_and_matched_path(
                        &request_metadata.request_method,
                        mp.as_str(),
                    ) else {
                        tracing::error!(
                            "Could not parse endpoint from matched path: '{}'.",
                            mp.as_str()
                        );
                        continue;
                    };

                    self.endpoint_stats
                        .entry(project_id)
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
            }
        }
    }

    async fn consume_stats(&mut self) {
        let mut stats = HashMap::new();
        std::mem::swap(&mut stats, &mut self.endpoint_stats);
        tracing::debug!("Consuming stats: {:?}", stats);
        let s: HashMap<Option<ProjectIdent>, HashMap<EndpointIdentifier, i64>> = stats
            .into_iter()
            .map(|(k, v)| (k, v.into_consumable()))
            .collect();
        tracing::debug!("Converted stats: {:?}", s);
        for sink in &self.stat_sinks {
            tracing::debug!("Sinking stats");
            sink.consume_endpoint_stats(s.clone()).await;
        }
    }
}

// E.g. postgres consumer which populates some postgres tables
#[async_trait::async_trait]
pub trait StatsSink: Debug + Send + Sync + 'static {
    async fn consume_endpoint_stats(
        &self,
        stats: HashMap<Option<ProjectIdent>, HashMap<EndpointIdentifier, i64>>,
    );
}
