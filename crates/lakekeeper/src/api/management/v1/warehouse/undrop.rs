use iceberg_ext::catalog::rest::ErrorModel;

use crate::{
    api::{self, management::v1::warehouse::UndropTabularsRequest},
    request_metadata::RequestMetadata,
    service::{
        authz::{
            AuthZCannotSeeTable, AuthZCannotSeeView, AuthZTableOps, Authorizer, CatalogTableAction,
            CatalogViewAction, RequireTableActionError,
        },
        require_namespace_for_tabular, CatalogNamespaceOps, CatalogStore, CatalogTabularOps,
        ResolvedWarehouse, TabularId, TabularListFlags, ViewOrTableInfo,
    },
};

pub(crate) async fn require_undrop_permissions<A: Authorizer, C: CatalogStore>(
    warehouse: &ResolvedWarehouse,
    request: &UndropTabularsRequest,
    authorizer: &A,
    catalog_state: C::State,
    request_metadata: &RequestMetadata,
) -> api::Result<()> {
    let warehouse_id = warehouse.warehouse_id;
    let tabulars = C::get_tabular_infos_by_id(
        warehouse_id,
        &request.targets,
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
        .targets
        .iter()
        .filter(|id| !found_tabulars.contains_key(id))
        .collect::<Vec<_>>();
    if let Some(id) = missing.first() {
        match **id {
            TabularId::Table(id) => {
                return Err(AuthZCannotSeeTable::new(warehouse_id, id).into());
            }
            TabularId::View(id) => {
                return Err(AuthZCannotSeeView::new(warehouse_id, id).into());
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
    .await?;

    let actions = tabulars
        .iter()
        .map(|t| {
            Ok::<_, ErrorModel>((
                require_namespace_for_tabular(&namespaces, t)?,
                t.as_action_request(CatalogViewAction::Undrop, CatalogTableAction::Undrop),
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    authorizer
        .require_tabular_actions(request_metadata, warehouse, &namespaces, &actions)
        .await?;
    Ok(())
}
