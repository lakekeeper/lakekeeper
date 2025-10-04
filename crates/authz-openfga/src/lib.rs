#![warn(
    missing_debug_implementations,
    rust_2018_idioms,
    unreachable_pub,
    clippy::pedantic
)]
#![allow(clippy::module_name_repetitions, clippy::large_enum_variant)]
#![forbid(unsafe_code)]

// use std::{
//     collections::HashSet,
//     fmt::Debug,
//     str::FromStr,
//     sync::{Arc, LazyLock},
// };

// use futures::future::try_join_all;
// use lakekeeper::{axum::Router, service::health::Health};
// use openfga_client::{
//     client::{
//         batch_check_single_result::CheckResult, BatchCheckItem, CheckRequestTupleKey,
//         ConsistencyPreference, ReadRequestTupleKey, ReadResponse, Tuple, TupleKey,
//         TupleKeyWithoutCondition,
//     },
//     migration::AuthorizationModelVersion,
//     tonic,
// };

// use lakekeeper::{
//     service::{
//         authn::Actor,
//         authz::{
//             Authorizer, CatalogNamespaceAction, CatalogProjectAction, CatalogServerAction,
//             CatalogTableAction, CatalogViewAction, CatalogWarehouseAction, ListProjectsResponse,
//         },
//         NamespaceId, ServerId, TableId,
//     },
//     ProjectId, WarehouseId, CONFIG,
// };

// mod api;
// mod authorizer;
// mod check;
// mod client;
// mod error;
// mod health;
// mod migration;

// mod relations;

// pub(crate) use client::{new_authorizer_from_config, new_client_from_config};
// pub use client::{
//     BearerOpenFGAAuthorizer, ClientCredentialsOpenFGAAuthorizer, UnauthenticatedOpenFGAAuthorizer,
// };
// use entities::{OpenFgaEntity, ParseOpenFgaEntity as _};
// pub(crate) use error::{OpenFGAError, OpenFGAResult};
// use lakekeeper::tokio::sync::RwLock;
// use lakekeeper::utoipa::OpenApi;
// pub(crate) use migration::migrate;
// pub(crate) use models::{OpenFgaType, RoleAssignee};
// use openfga_client::client::BasicOpenFgaClient;
// use relations::{
//     NamespaceRelation, ProjectRelation, RoleRelation, ServerRelation, TableRelation, ViewRelation,
//     WarehouseRelation,
// };

// mod authorizer;
mod config;
mod entities;
mod error;
mod models;

use std::{str::FromStr as _, sync::LazyLock};

pub use config::CONFIG;
use openfga_client::migration::AuthorizationModelVersion;

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
