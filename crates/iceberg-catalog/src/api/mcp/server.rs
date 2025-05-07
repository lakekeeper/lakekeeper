use axum::Router;
use rmcp::transport::sse_server::{SseServer, SseServerConfig};
const BIND_ADDRESS: &str = "127.0.0.1:8001";

pub(crate) fn get_mcp_router_and_server() -> Result<(SseServer, Router), std::io::Error> {
    let config = SseServerConfig {
        bind: BIND_ADDRESS.parse().expect("invalid bind address"),
        sse_path: "/sse".to_string(),
        post_path: "/message".to_string(),
        ct: tokio_util::sync::CancellationToken::new(),
        sse_keep_alive: None,
    };

    let (sse_server, router) = SseServer::new(config);
    Ok((sse_server, router))
}
