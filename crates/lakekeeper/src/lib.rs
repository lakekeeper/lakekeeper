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

// Structured logs reach the wire through `tracing::field::valuable`, which exists only
// under this cfg. Without it the crate fails to build anyway, but with a confusing
// "cannot find function `valuable`" rather than an actionable message. The cfg is set
// repo-wide in `.cargo/config.toml` and in every CI workflow.
#[cfg(not(tracing_unstable))]
compile_error!(
    "this crate must be built with RUSTFLAGS=\"--cfg tracing_unstable\" (see \
     .cargo/config.toml). Structured log values are emitted via \
     tracing::field::valuable, which is gated behind that cfg."
);
// `#[audit_part]` expands to `::lakekeeper::...` paths so that it works the same in this crate
// and in crates built on it.
extern crate self as lakekeeper;

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
    TokenRoles, X_BREAK_GLASS_HEADER_NAME, X_FORWARDED_HOST_HEADER, X_FORWARDED_PORT_HEADER,
    X_FORWARDED_PREFIX_HEADER, X_FORWARDED_PROTO_HEADER, X_PROJECT_ID_HEADER_NAME,
    X_REQUEST_ID_HEADER_NAME, determine_base_uri, determine_forwarded_prefix,
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

/// The audit log's surface for crates that define audit types: the attribute, the emitter
/// trait, the registry, and the shared value types.
pub mod audit {
    pub use lakekeeper_audit_macros::audit_part;

    // What an emitting crate needs and nothing else. Lakekeeper's own record parts —
    // `EntityRecord`, `DecisionRecord`, the subject records — are reachable at
    // `service::events::backends::audit` but are not re-exported here: another emitter never
    // builds one. It supplies an actor, a vocabulary and a context of its own, and the shape
    // assembles the rest.
    pub use crate::service::events::backends::audit::{
        AUDIT_TARGET, ActorRecord, AnyWireStr, AuditEmitter, AuditJson, AuditPart, Kind,
        OperationRecord, Registration, WireKey, WireName, WireStr, enabled, is_emitter_name,
        is_major_minor, warn_on_retired_audit_filter,
    };
    #[cfg(any(test, feature = "test-utils"))]
    pub use crate::service::events::backends::audit::{schema, validate};
}

/// Re-exports the `#[audit_part]` expansion relies on. Not part of the public API.
#[doc(hidden)]
pub mod __private {
    pub use inventory;
    pub use schemars;
    pub use serde;
}

crate::declare_audit_emitter!(
    Lakekeeper,
    name = "lakekeeper",
    format = crate::service::events::backends::audit::AUDIT_FORMAT
);
