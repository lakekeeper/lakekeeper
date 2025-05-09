use axum::extract::State;
use axum::response::sse::Event;
use axum::response::{Response, Sse};
use axum::{routing::get, Json, Router};
use futures::Stream;
use http::{HeaderMap, StatusCode};
use rmcp::model::ClientJsonRpcMessage;
use rmcp::transport::common::axum::DEFAULT_AUTO_PING_INTERVAL;
use rmcp::transport::streamable_http_server::axum::{
    delete_handler, get_handler, post_handler, App, StreamableHttpServerConfig,
};
use rmcp::transport::StreamableHttpServer;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

// Does this matter?
pub(crate) const BIND_ADDRESS: &str = "127.0.0.1:8181";

pub(crate) fn get_mcp_router_and_server(
) -> Result<(StreamableHttpServer, Router<App>), std::io::Error> {
    let addr = BIND_ADDRESS.parse::<SocketAddr>().unwrap();

    let config = StreamableHttpServerConfig {
        bind: addr,
        ct: CancellationToken::new(),
        sse_keep_alive: Some(Duration::from_secs(15)),
        path: "/".to_string(),
    };

    let (app, transport_rx) = App::new(config.sse_keep_alive.unwrap_or(DEFAULT_AUTO_PING_INTERVAL));

    let sse_router = Router::new()
        .route(
            &config.path,
            get(get_handler).post(post_handler).delete(delete_handler),
        )
        .with_state(app);

    let sse_server = StreamableHttpServer {
        transport_rx,
        config,
    };
    Ok((sse_server, sse_router))
}
