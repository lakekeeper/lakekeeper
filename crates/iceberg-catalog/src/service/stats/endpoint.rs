#![allow(clippy::module_name_repetitions)]

use crate::api::iceberg::supported_endpoints;
use crate::request_metadata::RequestMetadata;
use crate::service::authz::{Authorizer, CatalogProjectAction};
use crate::service::{Catalog, SecretStore, State as ServiceState};
use crate::{ProjectIdent, WarehouseIdent};
use axum::extract::{Path, Request, State};
use axum::middleware::Next;
use axum::response::Response;
use http::{Method, StatusCode};
use std::collections::HashMap;
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use std::time::Duration;

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
        response_status: http::StatusCode,
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
pub enum Uri {
    Matched(String),
    Unmatched(String),
}

impl Uri {
    pub(crate) fn into_pair(self) -> (Option<String>, Option<String>) {
        match self {
            Uri::Matched(s) => (Some(s), None),
            Uri::Unmatched(s) => (None, Some(s)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EndpointIdentifier {
    pub uri: Uri,
    pub status_code: StatusCode,
    pub method: Method,
    pub warehouse: Option<WarehouseIdentOrPrefix>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WarehouseIdentOrPrefix {
    Ident(WarehouseIdent),
    Prefix(String),
}

#[derive(Debug)]
pub struct Tracker<A>
where
    A: Authorizer + Clone,
{
    rcv: tokio::sync::mpsc::Receiver<Message>,
    endpoint_stats: HashMap<Option<ProjectIdent>, ProjectStats>,
    stat_sinks: Vec<Arc<dyn StatsSink>>,
    state: A,
}

impl<A> Tracker<A>
where
    A: Authorizer + Clone,
{
    pub fn new(
        rcv: tokio::sync::mpsc::Receiver<Message>,
        state: A,
        stat_sinks: Vec<Arc<dyn StatsSink>>,
    ) -> Self {
        Self {
            rcv,
            endpoint_stats: HashMap::new(),
            stat_sinks,
            state,
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

                    let whi = dbg!(&path_params)
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

                    self.endpoint_stats
                        .entry(project_id)
                        .or_default()
                        .stats
                        .entry(EndpointIdentifier {
                            warehouse: dbg!(whi),
                            uri: request_metadata
                                .matched_path
                                .as_ref()
                                .map(|s| s.as_str().to_string())
                                .map(Uri::Matched)
                                .unwrap_or(Uri::Unmatched(request_metadata.uri.clone())),
                            status_code: response_status,
                            method: request_metadata.request_method,
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
