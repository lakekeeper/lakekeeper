mod config;
mod error;
pub(crate) mod io;
mod metrics;
pub(crate) mod namespace;
#[cfg(feature = "s3-signer")]
mod s3_signer;
mod tables;
mod views;

pub use config::Server as ConfigServer;
pub use namespace::{MAX_NAMESPACE_DEPTH, UNSUPPORTED_NAMESPACE_PROPERTIES};

use crate::api::{iceberg::v1::Prefix, ErrorModel, Result};
use crate::service::WarehouseStatus;
use crate::{
    service::{auth::AuthZHandler, secrets::SecretStore, Catalog},
    WarehouseIdent,
};
use http::StatusCode;
use std::marker::PhantomData;

#[derive(Clone, Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct CatalogServer<C: Catalog, A: AuthZHandler, S: SecretStore> {
    auth_handler: PhantomData<A>,
    config_server: PhantomData<C>,
    secret_store: PhantomData<S>,
}

fn require_warehouse_id(prefix: Option<Prefix>) -> Result<WarehouseIdent> {
    prefix
        .ok_or(
            ErrorModel::builder()
                .code(http::StatusCode::BAD_REQUEST.into())
                .message(
                    "No prefix specified. The warehouse-id must be provided as prefix in the URL."
                        .to_string(),
                )
                .r#type("NoPrefixProvided".to_string())
                .build(),
        )?
        .try_into()
}

fn require_no_location_specified(location: &Option<String>) -> Result<()> {
    if location.is_some() {
        return Err(ErrorModel::builder()
            .code(StatusCode::BAD_REQUEST.into())
            .message("Specifying a Table `location` is not supported. Location is managed by the Catalog.".to_string())
            .r#type("LocationNotSupported".to_string())
            .build()
            .into());
    }
    Ok(())
}

fn require_active_warehouse(status: WarehouseStatus) -> Result<()> {
    if status != WarehouseStatus::Active {
        return Err(ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message("Warehouse is not active".to_string())
            .r#type("WarehouseNotActive".to_string())
            .build()
            .into());
    }
    Ok(())
}
