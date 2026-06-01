use std::{collections::BTreeMap, sync::Arc};

use http::StatusCode;

use crate::{
    WarehouseId,
    api::{
        data::v1::generic_tables::{RenameGenericTableRequest, RenameGenericTableTarget},
        endpoints::EndpointFlat,
        iceberg::v1::{ApiContext, ErrorModel, Prefix, Result, TableIdent},
    },
    request_metadata::RequestMetadata,
    server::{require_warehouse_id, tables::validate_table_or_view_ident},
    service::{
        CatalogGenericTableOps, CatalogIdempotencyOps, CatalogNamespaceOps, CatalogStore,
        CatalogTabularOps, CatalogWarehouseOps, GenericTableInfo, LoadGenericTableError,
        NamespaceHierarchy, ResolvedWarehouse, SecretStore, State, TabularId, Transaction,
        authz::{
            AuthZCannotSeeGenericTable, AuthZError, AuthZGenericTableOps, Authorizer,
            AuthzNamespaceOps, AuthzWarehouseOps, CatalogGenericTableAction,
            CatalogNamespaceAction, RequireGenericTableActionError,
            refresh_warehouse_and_namespace_if_needed,
        },
        events::{APIEventContext, context::ResolvedGenericTable},
        idempotency::IdempotencyInfo,
    },
};

pub(super) async fn rename_generic_table<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    prefix: Option<Prefix>,
    request: RenameGenericTableRequest,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<()> {
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    let to_table_ident = |t: RenameGenericTableTarget, code: &'static str| -> Result<TableIdent> {
        t.try_into().map_err(|e: iceberg::Error| {
            ErrorModel::bad_request(format!("Invalid {code}: {e}"), code, None).into()
        })
    };
    let source = to_table_ident(request.source.clone(), "InvalidSourceIdent")?;
    let destination = to_table_ident(request.destination.clone(), "InvalidDestinationIdent")?;
    validate_table_or_view_ident(&source)?;
    validate_table_or_view_ident(&destination)?;

    let idempotency_key = request_metadata.idempotency_key().copied();
    if let Some(ref key) = idempotency_key {
        let check =
            C::check_idempotency_key(warehouse_id, key, state.v1_state.catalog.clone()).await?;
        if check.is_replay() {
            return Ok(());
        }
    }

    let authorizer = &state.v1_state.authz;

    let event_ctx = APIEventContext::for_generic_table(
        Arc::new(request_metadata.clone()),
        state.v1_state.events.clone(),
        warehouse_id,
        source.clone(),
        CatalogGenericTableAction::Rename,
    );

    let authz_result = authorize_rename_generic_table::<C, A>(
        &request_metadata,
        warehouse_id,
        &source,
        &destination,
        authorizer,
        state.v1_state.catalog.clone(),
    )
    .await;

    let (event_ctx, (warehouse, destination_namespace, source_info)) =
        event_ctx.emit_authz(authz_result)?;

    let source_id = source_info.generic_table_id;
    let event_ctx = event_ctx.resolve(ResolvedGenericTable {
        warehouse: warehouse.clone(),
        generic_table: Arc::new(source_info),
        storage_permissions: None,
    });

    if source == destination {
        return Ok(());
    }

    let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
    C::rename_tabular(
        warehouse_id,
        TabularId::GenericTable(source_id),
        &source,
        &destination,
        t.transaction(),
    )
    .await?;

    if let Some(ref key) = idempotency_key
        && !C::try_insert_idempotency_key(
            warehouse_id,
            &IdempotencyInfo::builder()
                .key(*key)
                .endpoint(EndpointFlat::GenericTableV1RenameGenericTable)
                .http_status(StatusCode::NO_CONTENT)
                .build(),
            t.transaction(),
        )
        .await?
    {
        t.rollback()
            .await
            .inspect_err(|e| {
                tracing::warn!("Rollback failed after idempotency conflict: {e}");
            })
            .ok();
        return Err(ErrorModel::request_in_progress().into());
    }

    t.commit().await?;

    event_ctx.emit_generic_table_renamed_async(destination_namespace.namespace, Arc::new(request));

    Ok(())
}

async fn authorize_rename_generic_table<C: CatalogStore, A: Authorizer + Clone>(
    request_metadata: &RequestMetadata,
    warehouse_id: WarehouseId,
    source: &TableIdent,
    destination: &TableIdent,
    authorizer: &A,
    catalog_state: C::State,
) -> std::result::Result<(Arc<ResolvedWarehouse>, NamespaceHierarchy, GenericTableInfo), AuthZError>
{
    let (warehouse, destination_namespace, source_namespace) = tokio::join!(
        C::get_active_warehouse_by_id(warehouse_id, catalog_state.clone()),
        C::get_namespace(warehouse_id, &destination.namespace, catalog_state.clone()),
        C::get_namespace(warehouse_id, &source.namespace, catalog_state.clone()),
    );

    let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;
    let source_namespace = authorizer.require_namespace_presence(
        warehouse_id,
        source.namespace.clone(),
        source_namespace,
    )?;

    let source_namespace_id = source_namespace.namespace.namespace_id();

    let mut t = C::Transaction::begin_read(catalog_state.clone())
        .await
        .map_err(super::iceberg_err_to_authz)?;
    let source_info = match C::load_generic_table(
        warehouse_id,
        source_namespace_id,
        &source.name,
        t.transaction(),
    )
    .await
    {
        Ok(info) => info,
        Err(LoadGenericTableError::GenericTableNotFound(_)) => {
            return Err(
                AuthZCannotSeeGenericTable::new_not_found(warehouse_id, source.clone()).into(),
            );
        }
        Err(e) => return Err(super::iceberg_err_to_authz(e)),
    };
    t.commit().await.map_err(super::iceberg_err_to_authz)?;

    let (warehouse, source_namespace) = refresh_warehouse_and_namespace_if_needed::<C, _, _>(
        &warehouse,
        source_namespace,
        &source_info,
        AuthZCannotSeeGenericTable::new_not_found(warehouse_id, source.clone()),
        authorizer,
        catalog_state,
    )
    .await?;

    let create_action = CatalogNamespaceAction::CreateGenericTable {
        name: Some(destination.name.clone()),
        generic_table_id: Some(source_info.generic_table_id),
        format: Some(source_info.format.to_string()),
        base_location: Some(source_info.location.to_string()),
        properties: Arc::new(
            source_info
                .properties
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<BTreeMap<_, _>>(),
        ),
    };

    let user_provided_namespace = &destination.namespace;
    let (destination_namespace, source_info) = tokio::join!(
        authorizer.require_namespace_action(
            request_metadata,
            &warehouse,
            user_provided_namespace.clone(),
            destination_namespace,
            create_action,
        ),
        authorizer.require_generic_table_action(
            request_metadata,
            &warehouse,
            &source_namespace,
            source.clone(),
            Ok::<_, RequireGenericTableActionError>(Some(source_info)),
            CatalogGenericTableAction::Rename,
        ),
    );

    let destination_namespace = destination_namespace?;
    let source_info = source_info?;

    Ok((warehouse, destination_namespace, source_info))
}
