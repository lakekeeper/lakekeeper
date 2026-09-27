#![warn(
    missing_debug_implementations,
    rust_2018_idioms,
    unreachable_pub,
    clippy::pedantic
)]
#![forbid(unsafe_code)]
#![allow(clippy::module_name_repetitions)]

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

pub mod catalog;
pub mod configs;
pub mod spec;
pub mod validation;

pub use iceberg::{
    NamespaceIdent, TableCommit, TableCreation, TableIdent, TableRequirement, TableUpdate,
};

#[macro_use]
extern crate serde_derive;
