use crate::{
    api::{management::v1::ProtectionResponse, ApiContext},
    request_metadata::RequestMetadata,
    service::{
        authz::{Authorizer, CatalogViewAction, CatalogWarehouseAction},
        Catalog, Result, SecretStore, State, TabularIdentUuid, Transaction, ViewIdentUuid,
    },
    WarehouseIdent,
};

pub(crate) async fn set_protect_view<C: Catalog, A: Authorizer + Clone, S: SecretStore>(
    view_id: ViewIdentUuid,
    warehouse_id: WarehouseIdent,
    protect: bool,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<ProtectionResponse> {
    // ------------------- AUTHZ -------------------
    let authorizer = state.v1_state.authz;
    authorizer
        .require_warehouse_action(
            &request_metadata,
            warehouse_id,
            CatalogWarehouseAction::CanUse,
        )
        .await?;
    let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;

    let view_id: ViewIdentUuid = authorizer
        .require_view_action(
            &request_metadata,
            Ok(Some(view_id)),
            CatalogViewAction::CanDrop,
        )
        .await?;

    // ------------------- BUSINESS LOGIC -------------------
    let status =
        C::set_tabular_protected(TabularIdentUuid::View(*view_id), protect, t.transaction())
            .await?;
    t.commit().await?;
    Ok(status)
}
