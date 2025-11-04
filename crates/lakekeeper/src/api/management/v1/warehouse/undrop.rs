use crate::{
    api::{self, management::v1::warehouse::UndropTabularsRequest},
    request_metadata::RequestMetadata,
    service::{
        authz::{
            AuthZCannotSeeTable, AuthZCannotSeeView, AuthZTableOps, Authorizer, CatalogTableAction,
            CatalogViewAction, RequireTableActionError,
        },
        CatalogStore, CatalogTabularOps, ResolvedWarehouse, TabularId, TabularListFlags,
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
        catalog_state,
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

    let actions = tabulars
        .iter()
        .map(|t| t.as_action_request(CatalogViewAction::CanUndrop, CatalogTableAction::CanUndrop))
        .collect::<Vec<_>>();
    authorizer
        .require_tabular_actions(request_metadata, &actions)
        .await?;
    Ok(())
}
