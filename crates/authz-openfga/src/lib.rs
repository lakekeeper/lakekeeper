#![warn(
    missing_debug_implementations,
    rust_2018_idioms,
    unreachable_pub,
    clippy::pedantic
)]
#![allow(clippy::module_name_repetitions, clippy::missing_errors_doc)]
#![forbid(unsafe_code)]

use std::{str::FromStr as _, sync::LazyLock};

pub use authorizer::OpenFGAAuthorizer;
pub use client::{
    BearerOpenFGAAuthorizer, ClientCredentialsOpenFGAAuthorizer, UnauthenticatedOpenFGAAuthorizer,
    new_authorizer_from_default_config, new_client_from_default_config,
};
pub(crate) use error::{OpenFGAError, OpenFGAResult};
use openfga_client::migration::AuthorizationModelVersion;

mod api;
mod authorizer;
mod check;
mod client;
mod config;
mod entities;
pub mod error;
mod health;
mod migration;
mod models;
mod relations;

pub use config::CONFIG;
pub use migration::migrate;

const MAX_TUPLES_PER_WRITE: i32 = 100;

static AUTH_CONFIG: LazyLock<crate::config::OpenFGAConfig> = LazyLock::new(|| {
    CONFIG
        .openfga
        .clone()
        .expect("OpenFGA Authorization method called but OpenFGAConfig not found")
});

static CONFIGURED_MODEL_VERSION: LazyLock<Option<AuthorizationModelVersion>> = LazyLock::new(
    || {
        AUTH_CONFIG
        .authorization_model_version
        .as_ref()
        .filter(|v| !v.is_empty())
        .map(|v| {
            AuthorizationModelVersion::from_str(v).unwrap_or_else(|_| {
                panic!(
                    "Failed to parse OpenFGA authorization model version from config. Got {v}, expected <major>.<minor>"
                )
            })
        })
    },
);

#[derive(
    Debug, Clone, PartialEq, strum_macros::Display, strum_macros::AsRefStr, strum_macros::EnumString,
)]
#[strum(serialize_all = "snake_case")]
pub enum FgaType {
    User,
    Role,
    Server,
    Project,
    Warehouse,
    Namespace,
    #[strum(serialize = "lakekeeper_table")]
    Table,
    #[strum(serialize = "lakekeeper_view")]
    View,
    ModelVersion,
    AuthModelId,
}
