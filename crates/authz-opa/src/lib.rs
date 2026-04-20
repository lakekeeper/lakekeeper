#![warn(
    missing_debug_implementations,
    rust_2018_idioms,
    unreachable_pub,
    clippy::pedantic
)]
#![allow(clippy::module_name_repetitions, clippy::missing_errors_doc)]
#![forbid(unsafe_code)]

use std::sync::LazyLock;

pub use authorizer::OpaAuthorizer;
pub use client::new_authorizer_from_default_config;
pub use config::{CONFIG, OpaConfig};
pub(crate) use error::OpaError;

mod authorizer;
mod client;
mod config;
pub mod error;
mod health;
mod input;

pub(crate) static AUTH_CONFIG: LazyLock<OpaConfig> = LazyLock::new(|| {
    CONFIG
        .opa
        .clone()
        .expect("OPA Authorization method called but OpaConfig not found")
});
