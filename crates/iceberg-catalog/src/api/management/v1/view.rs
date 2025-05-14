use super::{ApiServer, ProtectionResponse};
use crate::{
    api::{ApiContext, RequestMetadata, Result},
    service::{
        authz::{Authorizer, CatalogViewAction},
        Catalog, SecretStore, State, TabularId, Transaction, ViewId,
    },
    WarehouseId,
};

impl<C: Catalog, A: Authorizer + Clone, S: SecretStore> ViewManagementService<C, A, S>
    for ApiServer<C, A, S>
{
}

#[async_trait::async_trait]
pub trait ViewManagementService<C: Catalog, A: Authorizer, S: SecretStore>
where
    Self: Send + Sync + 'static,
{
    async fn set_view_protection(
        view_id: ViewId,
        _warehouse_id: WarehouseId,
        protected: bool,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ProtectionResponse> {
        // ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz;

        let view_id: ViewId = authorizer
            .require_view_action(
                &request_metadata,
                Ok(Some(view_id)),
                CatalogViewAction::CanDrop,
            )
            .await?;

        // ------------------- BUSINESS LOGIC -------------------
        let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
        let status =
            C::set_tabular_protected(TabularId::View(*view_id), protected, t.transaction()).await?;
        t.commit().await?;
        Ok(status)
    }

    async fn get_view_protection(
        view_id: ViewId,
        _warehouse_id: WarehouseId,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ProtectionResponse> {
        //  ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz.clone();
        let mut t = C::Transaction::begin_read(state.v1_state.catalog.clone()).await?;

        authorizer
            .require_view_action(
                &request_metadata,
                Ok(Some(view_id)),
                CatalogViewAction::CanGetMetadata,
            )
            .await?;
        let status = C::get_tabular_protected(view_id.into(), t.transaction()).await?;
        t.commit().await?;
        Ok(status)
    }
}
