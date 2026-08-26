//! Regression test for unbounded Prometheus cardinality on unmatched routes.
//!
//! `metrics::set_global_recorder` can only succeed once per process, so this
//! lives in its own integration-test binary.

use axum::{Router, routing::get};
use axum_prometheus::{metrics, metrics_exporter_prometheus::PrometheusBuilder};
use lakekeeper::metrics::{UNMATCHED_ENDPOINT_LABEL, build_metric_layer};
use tower::ServiceExt;

/// Build a router shaped like the real one: nested sub-routers plus a top-level
/// route, with the metric layer applied last.
fn app(layer: axum_prometheus::PrometheusMetricLayer<'static>) -> Router {
    let catalog = Router::new().route(
        "/{prefix}/namespaces/{namespace}/tables/{table}",
        get(|| async { "ok" }),
    );
    Router::new()
        .nest("/catalog/v1", catalog)
        .route("/health", get(|| async { "ok" }))
        .layer(layer)
}

async fn call(app: &Router, uri: &str) {
    let request = http::Request::builder()
        .uri(uri)
        .body(axum::body::Body::empty())
        .unwrap();
    let _ = app.clone().oneshot(request).await.unwrap();
}

fn endpoint_labels(rendered: &str) -> Vec<String> {
    rendered
        .lines()
        .filter(|line| line.starts_with("axum_http_requests_total"))
        .filter_map(|line| {
            let start = line.find("endpoint=\"")? + "endpoint=\"".len();
            let rest = &line[start..];
            Some(rest[..rest.find('"')?].to_string())
        })
        .collect()
}

#[tokio::test]
async fn unmatched_paths_share_one_endpoint_label() {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let scrape = handle.clone();
    metrics::set_global_recorder(recorder).expect("no other recorder in this test binary");

    // Deliberately the production constructor: building an equivalent layer
    // here would leave this test green if the real one stopped applying the
    // policy.
    let app = app(build_metric_layer(handle));

    // Matched routes must keep their templated label — the whole point of the
    // metric is per-route latency, and collapsing those would be a regression.
    call(&app, "/catalog/v1/wh/namespaces/ns/tables/orders").await;
    call(&app, "/catalog/v1/wh/namespaces/other/tables/customers").await;
    call(&app, "/health").await;

    // Every shape a client can malform a URL into, plus outright junk. Each of
    // these used to mint 16 permanent series carrying the table name.
    for i in 0..200 {
        call(&app, &format!("/v1/wh/namespaces/ns/tables/table_{i}")).await;
        call(
            &app,
            &format!("/catalog/v1/wh/namespaces/ns/tables/table_{i}/"),
        )
        .await;
        call(
            &app,
            &format!("/catalog/v1//wh/namespaces/ns/tables/table_{i}"),
        )
        .await;
        call(&app, &format!("/junk/{i}")).await;
    }

    let rendered = scrape.render();
    let labels = endpoint_labels(&rendered);

    let template = "/catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}";
    assert!(
        labels.iter().any(|l| l == template),
        "matched routes must stay templated, got {labels:?}"
    );
    assert!(labels.iter().any(|l| l == "/health"));
    assert!(
        labels.iter().any(|l| l == UNMATCHED_ENDPOINT_LABEL),
        "unmatched requests must be recorded under `{UNMATCHED_ENDPOINT_LABEL}`, got {labels:?}"
    );

    // 800 distinct unmatched paths must not add 800 labels.
    let unexpected: Vec<_> = labels
        .iter()
        .filter(|l| l.contains("table_") || l.contains("/junk/"))
        .collect();
    assert!(
        unexpected.is_empty(),
        "raw request paths leaked into the `endpoint` label: {unexpected:?}"
    );
    assert!(
        labels.len() <= 4,
        "endpoint label cardinality must be bounded by the route table, got {} labels: {labels:?}",
        labels.len()
    );
}
