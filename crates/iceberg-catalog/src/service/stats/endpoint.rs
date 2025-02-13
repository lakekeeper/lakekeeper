#![allow(clippy::module_name_repetitions)]

use std::{
    collections::HashMap,
    fmt::Debug,
    str::FromStr,
    sync::{atomic::AtomicI64, Arc},
    time::Duration,
};

use axum::{
    extract::{Path, Request, State},
    middleware::Next,
    response::Response,
};
use http::StatusCode;

use crate::{
    api::endpoints::Endpoints, request_metadata::RequestMetadata, ProjectIdent, WarehouseIdent,
};

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
    Path(params): Path<HashMap<String, String>>,
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
            path_params: params,
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
    pub warehouse: Option<WarehouseIdentOrPrefix>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WarehouseIdentOrPrefix {
    Ident(WarehouseIdent),
    Prefix(String),
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

    pub async fn run(mut self) {
        let mut last_update = tokio::time::Instant::now();
        while let Some(msg) = self.rcv.recv().await {
            tracing::debug!("Received message: {:?}", msg);
            match msg {
                Message::EndpointCalled {
                    request_metadata,
                    response_status,
                    path_params,
                } => {
                    let project_id = request_metadata.project_id();

                    // TODO: use authz to check if project is accessible

                    let warehouse = dbg!(&path_params)
                        .get("warehouse_id")
                        .map(|s| WarehouseIdent::from_str(s.as_str()))
                        .transpose()
                        .ok()
                        .flatten()
                        .map(WarehouseIdentOrPrefix::Ident)
                        .or(path_params
                            .get("prefix")
                            .map(ToString::to_string)
                            .map(WarehouseIdentOrPrefix::Prefix));
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
                        })
                        .or_insert_with(|| AtomicI64::new(0))
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
            if last_update.elapsed() > Duration::from_secs(300) {
                self.consume_stats().await;
                last_update = tokio::time::Instant::now();
            }
        }
    }

    async fn consume_stats(&mut self) {
        let mut stats = HashMap::new();
        std::mem::swap(&mut stats, &mut self.endpoint_stats);
        let s: HashMap<Option<ProjectIdent>, HashMap<EndpointIdentifier, i64>> = stats
            .into_iter()
            .map(|(k, v)| (k, v.into_consumable()))
            .collect();
        for sink in &self.stat_sinks {
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
