#![warn(
    missing_debug_implementations,
    rust_2018_idioms,
    unreachable_pub,
    clippy::pedantic
)]
#![allow(
    clippy::module_name_repetitions,
    clippy::large_enum_variant,
    clippy::missing_errors_doc
)]
#![forbid(unsafe_code)]
mod config;
pub mod server;
pub mod service;
pub use config::{AuthZBackend, CONFIG, DEFAULT_PROJECT_ID, PgSslMode, SecretBackend};
pub use service::{ProjectId, SecretId, WarehouseId};

#[cfg(feature = "router")]
#[cfg_attr(docsrs, doc(cfg(feature = "router")))]
pub mod serve;

pub mod implementations;
pub mod utils;

pub mod api;
mod request_metadata;

pub use async_trait;
pub use axum;
pub use iceberg;
pub use limes;
#[cfg(feature = "kafka")]
#[cfg_attr(docsrs, doc(cfg(feature = "kafka")))]
pub use rdkafka;
pub use request_metadata::{
    TokenRoles, X_FORWARDED_HOST_HEADER, X_FORWARDED_PORT_HEADER, X_FORWARDED_PREFIX_HEADER,
    X_FORWARDED_PROTO_HEADER, X_PROJECT_ID_HEADER_NAME, X_REQUEST_ID_HEADER_NAME,
    determine_base_uri, determine_forwarded_prefix,
};
#[cfg(feature = "sqlx")]
pub use sqlx;
pub use tokio;
pub use tokio_util::sync::CancellationToken;
#[cfg(feature = "router")]
#[cfg_attr(docsrs, doc(cfg(feature = "router")))]
pub use tower;
#[cfg(feature = "router")]
#[cfg_attr(docsrs, doc(cfg(feature = "router")))]
pub use tower_http;
#[cfg(feature = "open-api")]
#[cfg_attr(docsrs, doc(cfg(feature = "open-api")))]
pub use utoipa;

#[cfg(feature = "router")]
#[cfg_attr(docsrs, doc(cfg(feature = "router")))]
pub mod metrics;
#[cfg(feature = "router")]
#[cfg_attr(docsrs, doc(cfg(feature = "router")))]
pub mod request_tracing;

pub use tracing;
#[cfg(any(test, all(feature = "test-utils", feature = "sqlx-postgres")))]
pub mod tests;

pub(crate) type XXHashSet<T> = std::collections::HashSet<T, xxhash_rust::xxh3::Xxh3Builder>;
