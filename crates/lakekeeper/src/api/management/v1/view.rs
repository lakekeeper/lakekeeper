use super::{ApiServer, ProtectionResponse};
use crate::{
    api::{ApiContext, RequestMetadata, Result},
    service::{
        authz::{AuthZViewOps, Authorizer, CatalogViewAction},
        CatalogStore, CatalogTabularOps, SecretStore, State, TabularId, TabularListFlags,
        Transaction, ViewId, ViewOrTableInfo,
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
        let view = C::get_view_info(
            warehouse_id,
            view_id,
            TabularListFlags::all(),
            state.v1_state.catalog.clone(),
        )
        .await;
        authorizer
            .require_view_action(
                &request_metadata,
                warehouse_id,
                view_id,
                view,
                CatalogViewAction::CanDrop,
            )
            .await?;

        // ------------------- BUSINESS LOGIC -------------------
        let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
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

        let view = C::get_view_info(
            warehouse_id,
            view_id,
            TabularListFlags::all(),
            state.v1_state.catalog.clone(),
        )
        .await;

        let view = ViewOrTableInfo::View(
            authorizer
                .require_view_action(
                    &request_metadata,
                    warehouse_id,
                    view_id,
                    view,
                    CatalogViewAction::CanGetMetadata,
                )
                .await?,
        );
        Ok(ProtectionResponse {
            protected: view.protected(),
            updated_at: view.updated_at(),
        })
    }
}
