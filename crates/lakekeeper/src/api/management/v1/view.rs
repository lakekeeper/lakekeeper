use super::{ApiServer, ProtectionResponse};
use crate::{
    WarehouseId,
    api::{ApiContext, RequestMetadata, Result},
    logging::audit::{AuditContext, events::{AuthorizationDeniedEvent, SetViewProtectionEvent}},
    service::{
        CatalogStore, CatalogTabularOps, SecretStore, State, TabularId, TabularListFlags,
        Transaction, ViewId,
        authz::{AuthZViewOps, Authorizer, CatalogViewAction},
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

        let (_warehouse, _namespace, _view) = authorizer
            .load_and_authorize_view_operation::<C>(
                &request_metadata,
                warehouse_id,
                view_id,
                TabularListFlags::all(),
                CatalogViewAction::SetProtection,
                state_catalog.clone(),
            )
            .await
            .inspect_err(|e| {
                request_metadata.log_audit(AuthorizationDeniedEvent {
                    action: "set_view_protection".to_string(),
                    error: e.to_string(),
                });
            })?;

        request_metadata.log_audit(SetViewProtectionEvent {
            view_id,
            warehouse_id,
            protected,
        });

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
                CatalogViewAction::GetMetadata,
                state_catalog.clone(),
            )
            .await
            .inspect_err(|e| {
                request_metadata.log_audit(AuthorizationDeniedEvent {
                    action: "get_view_protection".to_string(),
                    error: e.to_string(),
                });
            })?;

        Ok(ProtectionResponse {
            protected: view.protected,
            updated_at: view.updated_at,
        })
    }
}
