use super::{ApiServer, ProtectionResponse};
use crate::{
    api::{ApiContext, RequestMetadata, Result},
    service::{
        authz::{
            AuthZViewOps, Authorizer, AuthzNamespaceOps, AuthzWarehouseOps, CatalogViewAction,
            RequireViewActionError,
        },
        CatalogNamespaceOps, CatalogStore, CatalogTabularOps, CatalogWarehouseOps, SecretStore,
        State, TabularId, TabularListFlags, Transaction, ViewId,
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

        let (warehouse, view) = tokio::join!(
            C::get_active_warehouse_by_id(warehouse_id, state_catalog.clone()),
            C::get_view_info(
                warehouse_id,
                view_id,
                TabularListFlags::all(),
                state_catalog.clone(),
            )
        );
        let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;
        let view = authorizer.require_view_presence(warehouse_id, view_id, view)?;
        let namespace = C::get_namespace(
            warehouse_id,
            view.tabular_ident.namespace.clone(),
            state_catalog.clone(),
        )
        .await;
        let namespace = authorizer.require_namespace_presence(
            warehouse_id,
            view.tabular_ident.namespace.clone(),
            namespace,
        )?;

        authorizer
            .require_view_action(
                &request_metadata,
                &warehouse,
                &namespace,
                view_id,
                Ok::<_, RequireViewActionError>(Some(view)),
                CatalogViewAction::CanDrop,
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

        let (warehouse, view) = tokio::join!(
            C::get_active_warehouse_by_id(warehouse_id, state_catalog.clone()),
            C::get_view_info(
                warehouse_id,
                view_id,
                TabularListFlags::all(),
                state_catalog.clone(),
            )
        );
        let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;
        let view = authorizer.require_view_presence(warehouse_id, view_id, view)?;
        let namespace = C::get_namespace(
            warehouse_id,
            view.tabular_ident.namespace.clone(),
            state_catalog.clone(),
        )
        .await;
        let namespace = authorizer.require_namespace_presence(
            warehouse_id,
            view.tabular_ident.namespace.clone(),
            namespace,
        )?;

        let view = authorizer
            .require_view_action(
                &request_metadata,
                &warehouse,
                &namespace,
                view_id,
                Ok::<_, RequireViewActionError>(Some(view)),
                CatalogViewAction::CanGetMetadata,
            )
            .await?;

        Ok(ProtectionResponse {
            protected: view.protected,
            updated_at: view.updated_at,
        })
    }
}
