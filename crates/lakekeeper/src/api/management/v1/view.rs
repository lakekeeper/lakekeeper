use super::{ApiServer, ProtectionResponse};
use crate::{
    api::{ApiContext, RequestMetadata, Result},
    service::{
        authz::{AuthZViewOps, Authorizer, CatalogViewAction},
        CatalogStore, CatalogTabularOps, SecretStore, State, TabularId, TabularListFlags,
        Transaction, ViewId,
    },
    WarehouseId,
};

impl<C: CatalogStore, A: Authorizer + Clone, S: SecretStore> ViewManagementService<C, A, S>
    for ApiServer<C, A, S>
{
}

#[async_trait::async_trait]
pub trait ViewManagementService<C: CatalogStore, A: Authorizer, S: SecretStore>
where
    Self: Send + Sync + 'static,
{
    async fn set_view_protection(
        view_id: ViewId,
        warehouse_id: WarehouseId,
        protected: bool,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ProtectionResponse> {
        // ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz;
        let state_catalog = state.v1_state.catalog;

        let (_warehouse, _namespace, _view) = authorizer
            .load_and_authorize_view_operation::<C>(
                &request_metadata,
                warehouse_id,
                view_id,
                TabularListFlags::all(),
                CatalogViewAction::CanDrop,
                state_catalog.clone(),
            )
            .await?;

        // ------------------- BUSINESS LOGIC -------------------
        let mut t = C::Transaction::begin_write(state_catalog).await?;
        let status = C::set_tabular_protected(
            warehouse_id,
            TabularId::View(view_id),
            protected,
            t.transaction(),
        )
        .await?;
        t.commit().await?;
        Ok(ProtectionResponse {
            protected: status.protected(),
            updated_at: status.updated_at(),
        })
    }

    async fn get_view_protection(
        view_id: ViewId,
        warehouse_id: WarehouseId,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ProtectionResponse> {
        let authorizer = state.v1_state.authz.clone();

        let state_catalog = state.v1_state.catalog;

        let (_warehouse, _namespace, view) = authorizer
            .load_and_authorize_view_operation::<C>(
                &request_metadata,
                warehouse_id,
                view_id,
                TabularListFlags::all(),
                CatalogViewAction::CanGetMetadata,
                state_catalog.clone(),
            )
            .await?;

        Ok(ProtectionResponse {
            protected: view.protected,
            updated_at: view.updated_at,
        })
    }
}
