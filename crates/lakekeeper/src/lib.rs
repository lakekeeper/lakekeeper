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
pub use config::{
    AuthZBackend, CONFIG, DEFAULT_PROJECT_ID, KubernetesSubjectSource, MatchedEngines,
    SecretBackend, TrinoEngineConfig, TrustedEngine,
};
pub use service::{ProjectId, SecretId, WarehouseId};

#[cfg(feature = "router")]
#[cfg_attr(docsrs, doc(cfg(feature = "router")))]
pub mod serve;

pub mod utils;

pub mod api;
mod request_metadata;

pub use async_trait;
pub use axum;
pub use axum_extra;
pub use iceberg;
pub use limes;
pub use request_metadata::{
    TokenRoles, X_FORWARDED_HOST_HEADER, X_FORWARDED_PORT_HEADER, X_FORWARDED_PREFIX_HEADER,
    X_FORWARDED_PROTO_HEADER, X_PROJECT_ID_HEADER_NAME, X_REQUEST_ID_HEADER_NAME,
    determine_base_uri, determine_forwarded_prefix,
};
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

/// Exists only while `open-api` is **off**, so an authorizer crate can detect the one
/// feature combination it cannot otherwise diagnose.
///
/// `Authorizer::api_doc` is required only under this crate's `open-api`, while an
/// authorizer implements it under its *own* `open-api`. Cargo features propagate
/// downward only, so enabling ours does not enable theirs, and that build fails with
/// "missing `api_doc`" pointing at an implementation that is plainly present — its help
/// text even suggests writing the method that already exists. An authorizer crate that
/// imports this under `cfg(not(feature = "open-api"))` fails instead on the name below,
/// which says what to do.
#[cfg(not(feature = "open-api"))]
#[doc(hidden)]
pub mod enable_the_open_api_feature_of_your_authorizer_crate_too {
    /// Name this from an authorizer crate in a type position — a `use` of the module
    /// would warn as unused in the build where it resolves.
    #[derive(Debug)]
    pub struct Marker;
}

#[cfg(feature = "router")]
#[cfg_attr(docsrs, doc(cfg(feature = "router")))]
pub mod metrics;
#[cfg(feature = "router")]
#[cfg_attr(docsrs, doc(cfg(feature = "router")))]
pub mod request_tracing;

pub use tracing;

pub type XXHashSet<T> = std::collections::HashSet<T, xxhash_rust::xxh3::Xxh3Builder>;
