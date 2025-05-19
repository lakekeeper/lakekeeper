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
pub use config::{AuthZBackend, OpenFGAAuth, SecretBackend, CONFIG, DEFAULT_PROJECT_ID};
pub use service::{ProjectId, SecretIdent, WarehouseId};

pub mod implementations;
pub(crate) mod utils;

pub mod api;
mod request_metadata;

pub use axum;
pub use limes;
#[cfg(feature = "kafka")]
pub use rdkafka;
pub(crate) use request_metadata::{
    X_FORWARDED_HOST_HEADER, X_FORWARDED_PORT_HEADER, X_FORWARDED_PROTO_HEADER,
};
pub use tokio;
pub use utoipa;

#[cfg(feature = "router")]
pub mod metrics;
mod retry;
#[cfg(feature = "router")]
pub(crate) mod tracing;

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
