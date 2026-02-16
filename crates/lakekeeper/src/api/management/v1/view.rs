use std::sync::Arc;

use super::{ApiServer, ProtectionResponse};
use crate::{
    WarehouseId,
    api::{ApiContext, RequestMetadata, Result},
    service::{
        CatalogStore, CatalogTabularOps, SecretStore, State, TabularId, TabularListFlags,
        Transaction, ViewId,
        authz::{AuthZViewOps, Authorizer, CatalogViewAction},
        events::APIEventContext,
    },
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

        let event_ctx = APIEventContext::for_view(
            Arc::new(request_metadata),
            state.v1_state.events.clone(),
            warehouse_id,
            view_id,
            CatalogViewAction::SetProtection,
        );

        let authz_result = authorizer
            .load_and_authorize_view_operation::<C>(
                event_ctx.request_metadata(),
                event_ctx.user_provided_entity(),
                TabularListFlags::all(),
                event_ctx.action().clone(),
                state_catalog.clone(),
            )
            .await;
        let (_event_ctx, _view) = event_ctx.emit_authz(authz_result)?;

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
        // ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz;
        let state_catalog = state.v1_state.catalog;

        let event_ctx = APIEventContext::for_view(
            Arc::new(request_metadata),
            state.v1_state.events.clone(),
            warehouse_id,
            view_id,
            CatalogViewAction::GetMetadata,
        );

        let authz_result = authorizer
            .load_and_authorize_view_operation::<C>(
                event_ctx.request_metadata(),
                event_ctx.user_provided_entity(),
                TabularListFlags::all(),
                event_ctx.action().clone(),
                state_catalog.clone(),
            )
            .await;
        let (_event_ctx, (_warehouse, _namespace, view)) = event_ctx.emit_authz(authz_result)?;

        Ok(ProtectionResponse {
            protected: view.protected,
            updated_at: view.updated_at,
        })
    }
}
