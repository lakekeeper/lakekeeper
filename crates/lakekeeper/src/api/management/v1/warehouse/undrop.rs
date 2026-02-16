use std::sync::Arc;

use crate::{
    WarehouseId,
    request_metadata::RequestMetadata,
    service::{
        CachePolicy, CatalogNamespaceOps, CatalogStore, CatalogTabularOps, CatalogWarehouseOps,
        ResolvedWarehouse, TabularId, TabularListFlags, ViewOrTableInfo, WarehouseStatus,
        authz::{
            AuthZCannotSeeNamespace, AuthZCannotSeeTable, AuthZCannotSeeView,
            AuthZCannotUseWarehouseId, AuthZError, AuthZTableOps, AuthZWarehouseActionForbidden,
            Authorizer, AuthzWarehouseOps, CatalogTableAction, CatalogViewAction,
            CatalogWarehouseAction, RequireTableActionError, RequireWarehouseActionError,
        },
        require_namespace_for_tabular,
    },
};

pub(crate) async fn require_undrop_permissions<A: Authorizer, C: CatalogStore>(
    warehouse_id: WarehouseId,
    request: &[TabularId],
    authorizer: &A,
    catalog_state: C::State,
    request_metadata: &RequestMetadata,
) -> Result<Arc<ResolvedWarehouse>, AuthZError> {
    let warehouse = C::get_warehouse_by_id_cache_aware(
        warehouse_id,
        WarehouseStatus::active(),
        CachePolicy::Skip,
        catalog_state.clone(),
    )
    .await;
    let warehouse = authorizer
        .require_warehouse_action(
            request_metadata,
            warehouse_id,
            warehouse,
            CatalogWarehouseAction::Use,
        )
        .await?;

    let warehouse_id = warehouse.warehouse_id;
    let tabulars = C::get_tabular_infos_by_id(
        warehouse_id,
        request,
        TabularListFlags {
            include_active: true,
            include_deleted: true,
            include_staged: false,
        },
        catalog_state.clone(),
    )
    .await
    .map_err(RequireTableActionError::from)?;

    let found_tabulars = tabulars
        .iter()
        .map(|t| (t.tabular_id(), t))
        .collect::<std::collections::HashMap<_, _>>();

    let missing = request
        .iter()
        .filter(|id| !found_tabulars.contains_key(id))
        .collect::<Vec<_>>();
    if let Some(id) = missing.first() {
        match **id {
            TabularId::Table(id) => {
                return Err(AuthZCannotSeeTable::new_not_found(warehouse_id, id).into());
            }
            TabularId::View(id) => {
                return Err(AuthZCannotSeeView::new_not_found(warehouse_id, id).into());
            }
        }
    }

    let namespaces = C::get_namespaces_by_id(
        warehouse_id,
        &tabulars
            .iter()
            .map(ViewOrTableInfo::namespace_id)
            .collect::<Vec<_>>(),
        catalog_state,
    )
    .await
    .map_err(RequireTableActionError::from)?;

    let actions = tabulars
        .iter()
        .map(|t| {
            Ok::<_, AuthZCannotSeeNamespace>((
                require_namespace_for_tabular(&namespaces, t)?,
                t.as_action_request(CatalogViewAction::Undrop, CatalogTableAction::Undrop),
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    authorizer
        .require_tabular_actions(request_metadata, &warehouse, &namespaces, &actions)
        .await?;
    Ok(warehouse)
}

#[derive(Debug)]
pub(super) struct AuthorizeListSoftDeletedTabularsResponse {
    pub(super) warehouse: Arc<ResolvedWarehouse>,
    pub(super) can_list_everything: bool,
}

pub(super) async fn authorize_list_soft_deleted_tabulars<C: CatalogStore, A: Authorizer>(
    request_metadata: &RequestMetadata,
    warehouse_id: WarehouseId,
    authorizer: &A,
    catalog: C::State,
) -> Result<AuthorizeListSoftDeletedTabularsResponse, RequireWarehouseActionError> {
    let warehouse = C::get_active_warehouse_by_id(warehouse_id, catalog).await;
    let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;

    let [can_use, can_list_deleted_tabulars, can_list_everything] = authorizer
        .are_allowed_warehouse_actions_arr(
            request_metadata,
            None,
            &[
                (&warehouse, CatalogWarehouseAction::Use),
                (&warehouse, CatalogWarehouseAction::ListDeletedTabulars),
                (&warehouse, CatalogWarehouseAction::ListEverything),
            ],
        )
        .await?
        .into_inner();

    if !can_use {
        return Err(AuthZCannotUseWarehouseId::new_access_denied(warehouse_id).into());
    }
    if !can_list_deleted_tabulars {
        return Err(AuthZWarehouseActionForbidden::new(
            warehouse_id,
            &CatalogWarehouseAction::ListDeletedTabulars,
        )
        .into());
    }

    Ok(AuthorizeListSoftDeletedTabularsResponse {
        warehouse,
        can_list_everything,
    })
}
