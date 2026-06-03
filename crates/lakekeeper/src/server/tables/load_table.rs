use std::{collections::HashMap, sync::Arc};

use http::StatusCode;
use iceberg_ext::catalog::rest::{ETag, StorageCredential, create_etag};

use crate::{
    WarehouseId,
    api::iceberg::v1::{
        ApiContext, LoadTableResult, LoadTableResultOrNotModified, Result, TableIdent,
        TableParameters,
        tables::{LoadTableFilters, LoadTableRequest},
    },
    request_metadata::RequestMetadata,
    server::{
        maybe_get_secret, require_warehouse_id,
        tables::{authorize_load_table, parse_location, validate_table_or_view_ident},
    },
    service::{
        AuthZTableInfo as _, CachePolicy, CatalogStore, CatalogTableOps, CatalogWarehouseOps,
        LoadTableResponse as CatalogLoadTableResult, State, TableId, TableIdentOrId, TabularInfo,
        TabularListFlags, TabularNotFound, Transaction, WarehouseStatus,
        authz::{Authorizer, AuthzWarehouseOps, CatalogTableAction},
        events::{
            APIEventContext,
            context::{ResolvedTable, authz_to_error_no_audit},
        },
        secrets::SecretStore,
    },
};

fn get_etag(table_info: &TabularInfo<TableId>) -> Option<ETag> {
    table_info
        .metadata_location
        .as_ref()
        .map(lakekeeper_io::Location::as_str)
        .map(create_etag)
}

fn etag_already_present(etags: &[ETag], etag: &ETag) -> bool {
    etags.iter().any(|e| e == etag || e == &ETag::from("*"))
}

/// Load a table from the catalog.
///
/// # Panics
/// May panic if internal invariants are violated (e.g., an entry expected to
/// exist in a pre-resolved map is missing).
#[allow(clippy::too_many_lines)]
pub async fn load_table<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: TableParameters,
    request: LoadTableRequest,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<LoadTableResultOrNotModified> {
    let LoadTableRequest {
        data_access,
        filters,
        etags,
        referenced_by,
    } = request;

    // ------------------- VALIDATIONS -------------------
    let TableParameters { prefix, table } = parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    // It is important to throw a 404 if a table cannot be found,
    // because spark might check if `table`.`branch` exists, which should return 404.
    // Only then will it treat it as a branch.
    if let Err(mut e) = validate_table_or_view_ident(&table) {
        if e.error.r#type == *"NamespaceDepthExceeded" {
            e.error.code = StatusCode::NOT_FOUND.into();
        }
        return Err(e);
    }

    // ------------------- AUTHZ -------------------
    let authorizer = state.v1_state.authz;
    let catalog_state = state.v1_state.catalog;

    let event_ctx = APIEventContext::for_table(
        Arc::new(request_metadata.clone()),
        state.v1_state.events,
        warehouse_id,
        table.clone(),
        CatalogTableAction::GetMetadata,
    );

    let (event_ctx, (warehouse, table_info, storage_permissions)) = event_ctx.emit_authz(
        authorize_load_table::<C, A>(
            &request_metadata,
            table,
            warehouse_id,
            TabularListFlags::active(),
            authorizer.clone(),
            catalog_state.clone(),
            referenced_by.as_deref(),
        )
        .await,
    )?;

    let mut event_ctx = event_ctx.resolve(ResolvedTable {
        warehouse,
        table: Arc::new(table_info),
        storage_permissions,
    });

    // ------------------- ETAG CHECK -------------------
    let etag = get_etag(&event_ctx.resolved().table);
    if let Some(etag_value) = etag
        .as_ref()
        .map(|e| e.as_str().trim_matches('"'))
        .map(ETag::from)
        && etag_already_present(&etags, &etag_value)
    {
        return Ok(LoadTableResultOrNotModified::NotModifiedResponse(
            etag.unwrap(),
        ));
    }

    // ------------------- BUSINESS LOGIC -------------------
    let mut t = C::Transaction::begin_read(catalog_state.clone()).await?;
    let CatalogLoadTableResult {
        table_id: _,
        namespace_id: _,
        table_metadata,
        metadata_location,
        warehouse_version,
    } = load_table_inner::<C>(
        warehouse_id,
        event_ctx.resolved().table.table_id(),
        event_ctx.resolved().table.table_ident(),
        false,
        &filters,
        &mut t,
    )
    .await?;
    t.commit().await?;

    // Refetch warehouse if version is stale
    if event_ctx.resolved().warehouse.version < warehouse_version {
        let warehouse = C::get_warehouse_by_id_cache_aware(
            warehouse_id,
            WarehouseStatus::active(),
            CachePolicy::RequireMinimumVersion(*warehouse_version),
            catalog_state.clone(),
        )
        .await;
        let fresh_warehouse = authorizer
            .require_warehouse_presence(warehouse_id, warehouse)
            .map_err(authz_to_error_no_audit)?;
        event_ctx.resolved_mut().warehouse = fresh_warehouse;
    }
    let warehouse = &event_ctx.resolved().warehouse;

    let table_location =
        parse_location(table_metadata.location(), StatusCode::INTERNAL_SERVER_ERROR)?;

    let storage_config = if let Some(storage_permissions) = storage_permissions {
        let storage_secret =
            maybe_get_secret(warehouse.storage_secret_id, &state.v1_state.secrets).await?;
        let storage_secret_ref = storage_secret.as_deref();
        Some(
            warehouse
                .storage_profile
                .generate_table_config(
                    data_access,
                    storage_secret_ref,
                    &table_location,
                    storage_permissions,
                    &request_metadata,
                    &*event_ctx.resolved().table,
                )
                .await?,
        )
    } else {
        None
    };

    let storage_credentials = storage_config.as_ref().and_then(|c| {
        (!c.creds.inner().is_empty()).then(|| {
            vec![StorageCredential {
                prefix: table_location.to_string(),
                config: c.creds.clone().into(),
            }]
        })
    });

    let metadata_ref = Arc::new(table_metadata);
    let metadata_location_ref = metadata_location.map(Arc::new);

    event_ctx.emit_table_loaded_async(metadata_ref.clone(), metadata_location_ref.clone());

    let load_table_result = LoadTableResult {
        metadata_location: metadata_location_ref.as_ref().map(ToString::to_string),
        metadata: metadata_ref,
        config: storage_config.map(|c| c.config.into()),
        storage_credentials,
    };

    Ok(LoadTableResultOrNotModified::LoadTableResult(
        load_table_result,
    ))
}

/// Load a table from the catalog, ensuring that it is not staged
///
/// # Errors
/// Returns an error if the table is staged, if it cannot be found, or if a DB error occurs.
async fn load_table_inner<C: CatalogStore>(
    warehouse_id: WarehouseId,
    table_id: TableId,
    table_ident: &TableIdent,
    include_deleted: bool,
    load_table_filters: &LoadTableFilters,
    t: &mut C::Transaction,
) -> Result<CatalogLoadTableResult> {
    let mut metadatas = C::load_tables(
        warehouse_id,
        [table_id],
        include_deleted,
        load_table_filters,
        t.transaction(),
    )
    .await?
    .into_iter()
    .map(|r| (r.table_id, r))
    .collect::<HashMap<_, _>>();
    let result = metadatas.remove(&table_id).ok_or_else(|| {
        TabularNotFound::new(warehouse_id, TableIdentOrId::from(table_ident.clone()))
            .append_detail("Table metadata not returned from table load".to_string())
    })?;
    if !metadatas.is_empty() {
        tracing::error!(
            "Unexpected extra table metadatas returned when loading table {:?} in warehouse {:?}: {:?}",
            table_ident,
            warehouse_id,
            metadatas.keys()
        );
    }
    require_not_staged(
        warehouse_id,
        table_ident.clone(),
        result.metadata_location.as_ref(),
    )?;
    Ok(result)
}

fn require_not_staged<T>(
    warehouse_id: WarehouseId,
    table_ident: impl Into<TableIdentOrId>,
    metadata_location: Option<&T>,
) -> std::result::Result<(), TabularNotFound> {
    if metadata_location.is_none() {
        return Err(TabularNotFound::new(warehouse_id, table_ident.into())
            .append_detail("Table is in staged state; operation requires active table"));
    }

    Ok(())
}
