#![warn(
    missing_debug_implementations,
    rust_2018_idioms,
    unreachable_pub,
    clippy::pedantic
)]
#![allow(clippy::module_name_repetitions, clippy::large_enum_variant)]
#![forbid(unsafe_code)]

pub mod api;

pub mod catalog;
mod config;
pub mod service;
pub use service::{ProjectIdent, SecretIdent, WarehouseIdent};

pub use config::{SecretBackend, CONFIG};

pub mod implementations;

mod request_metadata;

#[cfg(feature = "router")]
pub mod metrics;
#[cfg(feature = "router")]
pub(crate) mod tracing;
