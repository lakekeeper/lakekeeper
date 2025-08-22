#![warn(
    missing_debug_implementations,
    rust_2018_idioms,
    unreachable_pub,
    clippy::pedantic
)]
#![allow(clippy::module_name_repetitions, clippy::large_enum_variant)]
#![forbid(unsafe_code)]
pub mod catalog;
mod config;
pub mod service;
pub use config::{AuthZBackend, OpenFGAAuth, PgSslMode, SecretBackend, CONFIG, DEFAULT_PROJECT_ID};
pub use service::{ProjectId, SecretIdent, WarehouseId};

#[cfg(feature = "router")]
#[cfg_attr(docsrs, doc(cfg(feature = "router")))]
pub mod serve;

pub mod implementations;
pub(crate) mod utils;

pub mod api;
mod request_metadata;

pub use axum;
pub use iceberg;
pub use limes;
#[cfg(feature = "kafka")]
#[cfg_attr(docsrs, doc(cfg(feature = "kafka")))]
pub use rdkafka;
pub use request_metadata::{
    determine_base_uri, determine_forwarded_prefix, X_FORWARDED_HOST_HEADER,
    X_FORWARDED_PORT_HEADER, X_FORWARDED_PREFIX_HEADER, X_FORWARDED_PROTO_HEADER,
};
pub use tokio;
#[cfg(feature = "router")]
#[cfg_attr(docsrs, doc(cfg(feature = "router")))]
pub use tower;
#[cfg(feature = "router")]
#[cfg_attr(docsrs, doc(cfg(feature = "router")))]
pub use tower_http;
pub use utoipa;

#[cfg(feature = "router")]
#[cfg_attr(docsrs, doc(cfg(feature = "router")))]
pub mod metrics;
#[cfg(feature = "router")]
#[cfg_attr(docsrs, doc(cfg(feature = "router")))]
pub mod tracing;

#[cfg(test)]
pub mod tests;

#[cfg(test)]
pub mod test {
    use std::{future::Future, sync::LazyLock};

    use tokio::runtime::Runtime;

    #[allow(dead_code)]
    static COMMON_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to start Tokio runtime")
    });

    #[track_caller]
    #[allow(dead_code)]
    pub(crate) fn test_block_on<F: Future>(f: F, common_runtime: bool) -> F::Output {
        {
            if common_runtime {
                return COMMON_RUNTIME.block_on(f);
            }
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to start Tokio runtime")
                .block_on(f)
        }
    }
}
