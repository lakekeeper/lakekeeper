// #![allow(clippy::module_name_repetitions)]
//
// use crate::request_metadata::RequestMetadata;
// use axum::extract::{Request, State};
// use axum::middleware::Next;
// use axum::response::Response;
// use axum::Extension;
// use axum_extra::headers::authorization::Bearer;
// use axum_extra::headers::Authorization;
// use axum_extra::TypedHeader;
// use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
// use std::collections::HashMap;
// use std::sync::atomic::AtomicI64;
// use std::sync::Arc;
// use std::time::Duration;
// use uuid::Uuid;
//
// #[derive(Debug, Clone)]
// pub(crate) struct TrackerTx(tokio::sync::mpsc::Sender<Message>);
//
// // TODO: We're aggregating endpoint statistics per warehouse, which means we'll have to somehow
// //       extract the warehouse id from the request. That's no fun
// pub(crate) async fn stats_middleware_fn(
//     State(tracker): State<TrackerTx>,
//     request: Request,
//     next: Next,
// ) -> Response {
//     let endpoint = request.uri().path().to_string();
//     let response = next.run(request).await;
//     // TODO: are we only interested in non 5xx? or all?
//     if !response.status().is_server_error() {
//         if let Err(err) = tracker
//             .0
//             .send(Message::IncrementEndpoint { endpoint })
//             .await
//         {
//             tracing::error!("Failed to send stats message: {:?}", err);
//         };
//     }
//     response
// }
//
// enum Message {
//     IncrementEndpoint { endpoint: String },
// }
//
// struct Tracker {
//     rcv: tokio::sync::mpsc::Receiver<Message>,
//     endpoint_stats: HashMap<String, AtomicI64>,
//     stat_sinks: Vec<Arc<dyn StatsSink>>,
// }
//
// impl Tracker {
//     async fn run(mut self) {
//         let mut last_update = tokio::time::Instant::now();
//         while let Some(msg) = self.rcv.recv().await {
//             match msg {
//                 Message::IncrementEndpoint { endpoint } => {
//                     self.endpoint_stats
//                         .entry(endpoint)
//                         .or_insert_with(|| AtomicI64::new(0))
//                         .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
//                 }
//             }
//             if last_update.elapsed() > Duration::from_secs(300) {
//                 self.consume_stats().await;
//                 last_update = tokio::time::Instant::now();
//             }
//         }
//     }
//
//     async fn consume_stats(&mut self) {
//         let mut stats = HashMap::new();
//         std::mem::swap(&mut stats, &mut self.endpoint_stats);
//         let stats = stats
//             .into_iter()
//             .map(|(k, v)| (k, v.load(std::sync::atomic::Ordering::Relaxed)))
//             .collect::<HashMap<String, i64>>();
//         let stats_id = Uuid::now_v7();
//         for sink in &self.stat_sinks {
//             sink.consume_endpoint_stats(stats_id, stats.clone()).await;
//         }
//     }
// }
//
// // E.g. postgres consumer which populates some postgres tables
// #[async_trait::async_trait]
// pub trait StatsSink: Send + Sync + 'static {
//     async fn consume_endpoint_stats(&self, stats_id: Uuid, stats: HashMap<String, i64>);
// }
