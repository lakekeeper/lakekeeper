use http::StatusCode;
use iceberg::TableIdent;
use uuid::Uuid;

use crate::{
    api::{
        ApiContext, ErrorModel,
        endpoints::EndpointFlat,
        iceberg::v1::namespace::NamespaceParameters,
        v1::generic_tables::{
            CreateGenericTableRequest, GenericTableData, GenericTableParameters,
            LoadGenericTableResponse,
        },
    },
    request_metadata::RequestMetadata,
    server::{require_warehouse_id, tabular::determine_tabular_location},
    service::{
        CatalogGenericTableOps, CatalogIdempotencyOps, CatalogNamespaceOps, CatalogStore,
        CatalogWarehouseOps, GenericTableCreation, GenericTableId, Result, SecretStore, State,
        TabularId, Transaction, authz::Authorizer, idempotency::IdempotencyInfo,
    },
};

fn validate_create_request(request: &CreateGenericTableRequest) -> Result<()> {
    if request.name.is_empty() {
        return Err(ErrorModel::bad_request(
            "Generic table name cannot be empty",
            "InvalidName",
            None,
        )
        .into());
    }
    if request.name.contains('+') {
        return Err(ErrorModel::bad_request(
            "Generic table name cannot contain '+' character.",
            "InvalidName",
            None,
        )
        .into());
    }
    if request.format.as_str().is_empty() {
        return Err(ErrorModel::bad_request(
            "Generic table format cannot be empty",
            "InvalidFormat",
            None,
        )
        .into());
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
pub(super) async fn create_generic_table<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: NamespaceParameters,
    request: CreateGenericTableRequest,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<LoadGenericTableResponse> {
    let NamespaceParameters { namespace, prefix } = &parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    validate_create_request(&request)?;

    // ------------------- IDEMPOTENCY CHECK -------------------
    let idempotency_key = request_metadata.idempotency_key().copied();
    if let Some(ref key) = idempotency_key {
        let check =
            C::check_idempotency_key(warehouse_id, key, state.v1_state.catalog.clone()).await?;
        if check.is_replay() {
            return super::load::load_generic_table::<C, A, S>(
                GenericTableParameters {
                    prefix: prefix.clone(),
                    namespace: namespace.clone(),
                    table_name: request.name.clone(),
                },
                state,
                crate::api::iceberg::v1::DataAccessMode::ClientManaged,
                request_metadata,
            )
            .await;
        }
    }

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

    let generic_table_id = GenericTableId::from(Uuid::now_v7());
    let tabular_id = TabularId::GenericTable(generic_table_id);
    let table_ident = TableIdent::new(namespace.clone(), request.name.clone());

    let location = determine_tabular_location(
        &ns,
        request.base_location.clone(),
        tabular_id,
        &table_ident,
        &warehouse.storage_profile,
    )?;

    let creation = GenericTableCreation {
        generic_table_id,
        namespace_id,
        warehouse_id: warehouse.warehouse_id,
        name: request.name.clone(),
        format: request.format.clone(),
        location,
        doc: request.doc.clone(),
        schema: request.schema.clone(),
        statistics: request.statistics.clone(),
        properties: request.properties.clone(),
    };

    let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
    let info = C::create_generic_table(creation, t.transaction()).await?;

    // Insert idempotency key in the same transaction.
    if let Some(ref key) = idempotency_key
        && !C::try_insert_idempotency_key(
            warehouse_id,
            &IdempotencyInfo::builder()
                .key(*key)
                .endpoint(EndpointFlat::GenericTableV1CreateGenericTable)
                .http_status(StatusCode::OK)
                .build(),
            t.transaction(),
        )
        .await?
    {
        t.rollback()
            .await
            .inspect_err(|e| tracing::warn!("Rollback after idempotency conflict: {e}"))
            .ok();
        return Err(ErrorModel::request_in_progress().into());
    }
    t.commit().await?;

    state
        .v1_state
        .authz
        .create_generic_table(
            &request_metadata,
            warehouse.warehouse_id,
            info.generic_table_id,
            namespace_id,
        )
        .await?;

    Ok(LoadGenericTableResponse {
        table: GenericTableData {
            name: info.name,
            format: info.format,
            base_location: info.location.to_string(),
            doc: info.doc,
            properties: info.properties,
            schema: info.schema,
            statistics: info.statistics,
        },
        config: None,
        storage_credentials: None,
    })
}
