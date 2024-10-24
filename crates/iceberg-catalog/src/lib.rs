#![warn(
    missing_debug_implementations,
    rust_2018_idioms,
    unreachable_pub,
    clippy::pedantic
)]
#![allow(clippy::module_name_repetitions)]
#![forbid(unsafe_code)]

pub mod catalog;
mod config;
pub mod service;
pub use service::{ProjectIdent, SecretIdent, WarehouseIdent};

pub use config::{AuthZBackend, OpenFGAAuth, SecretBackend, CONFIG};

pub mod implementations;

mod request_metadata;

pub mod api;

#[cfg(feature = "router")]
pub mod metrics;
#[cfg(feature = "router")]
pub(crate) mod tracing;
