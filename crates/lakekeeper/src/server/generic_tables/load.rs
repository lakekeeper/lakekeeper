use std::str::FromStr as _;

use iceberg_ext::catalog::rest::StorageCredential;
use lakekeeper_io::Location;

use crate::{
    api::{
        ApiContext, ErrorModel,
        iceberg::v1::DataAccessMode,
        v1::generic_tables::{GenericTableData, GenericTableParameters, LoadGenericTableResponse},
    },
    request_metadata::RequestMetadata,
    server::{maybe_get_secret, require_warehouse_id},
    service::{
        CatalogNamespaceOps, CatalogStore, CatalogWarehouseOps, GenericTableTabularBridge, Result,
        SecretStore, State, Transaction, authz::Authorizer, storage::StoragePermissions,
    },
};

pub(super) async fn load_generic_table<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: GenericTableParameters,
    state: ApiContext<State<A, C, S>>,
    data_access: impl Into<DataAccessMode>,
    request_metadata: RequestMetadata,
) -> Result<LoadGenericTableResponse> {
    let data_access = data_access.into();

    let GenericTableParameters {
        prefix,
        namespace,
        table_name,
    } = parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;

    let warehouse = C::get_active_warehouse_by_id(warehouse_id, state.v1_state.catalog.clone())
        .await?
        .ok_or_else(|| {
            ErrorModel::not_found("Warehouse not found".to_string(), "WarehouseNotFound", None)
        })?;

    let ns = C::get_namespace(
        warehouse_id,
        namespace.clone(),
        state.v1_state.catalog.clone(),
    )
    .await?
    .ok_or_else(|| {
        ErrorModel::not_found("Namespace not found".to_string(), "NamespaceNotFound", None)
    })?;

    let namespace_id = ns.namespace.namespace.namespace_id;

    let mut t = C::Transaction::begin_read(state.v1_state.catalog.clone()).await?;
    let info = C::load_generic_table_impl(warehouse_id, namespace_id, &table_name, t.transaction())
        .await?;
    t.commit().await?;

    let table_location = Location::from_str(&info.base_location).map_err(|e| {
        ErrorModel::internal(
            format!("Failed to parse base location: {e}"),
            "InvalidLocation",
            Some(Box::new(e)),
        )
    })?;

    let bridge = GenericTableTabularBridge::from_info(&info);

    let storage_secret =
        maybe_get_secret(warehouse.storage_secret_id, &state.v1_state.secrets).await?;
    let storage_secret_ref = storage_secret.as_deref();

    // TODO: derive from authz checks (see load_table.rs)
    let storage_permissions = StoragePermissions::ReadWriteDelete;

    let table_config = warehouse
        .storage_profile
        .generate_table_config(
            data_access,
            storage_secret_ref,
            &table_location,
            storage_permissions,
            &request_metadata,
            &bridge,
        )
        .await?;

    let storage_credentials = (!table_config.creds.inner().is_empty()).then(|| {
        vec![StorageCredential {
            prefix: info.base_location.clone(),
            config: table_config.creds.clone().into(),
        }]
    });

    Ok(LoadGenericTableResponse {
        table: GenericTableData {
            name: info.name,
            format: info.format,
            base_location: info.base_location,
            doc: info.doc,
            properties: info.properties,
            schema: info.schema,
            statistics: info.statistics,
        },
        config: Some(table_config.config.into()),
        storage_credentials,
    })
}
