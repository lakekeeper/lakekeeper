use std::sync::Arc;

use super::{ApiServer, ProtectionResponse};
use crate::{
    WarehouseId,
    api::{ApiContext, RequestMetadata, Result},
    service::{
        CatalogStore, CatalogTabularOps, SecretStore, State, TableId, TabularId, TabularListFlags,
        Transaction,
        authz::{AuthZTableOps, Authorizer, CatalogTableAction},
        events::APIEventContext,
    },
};

impl<C: CatalogStore, A: Authorizer + Clone, S: SecretStore> TableManagementService<C, A, S>
    for ApiServer<C, A, S>
{
}

#[async_trait::async_trait]
pub trait TableManagementService<C: CatalogStore, A: Authorizer, S: SecretStore>
where
    Self: Send + Sync + 'static,
{
    async fn set_table_protection(
        table_id: TableId,
        warehouse_id: WarehouseId,
        protected: bool,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ProtectionResponse> {
        // ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz;
        let state_catalog = state.v1_state.catalog.clone();

        let event_ctx = APIEventContext::for_table(
            Arc::new(request_metadata),
            state.v1_state.events.clone(),
            warehouse_id,
            table_id,
            CatalogTableAction::SetProtection,
        );

        let authz_result = authorizer
            .load_and_authorize_table_operation::<C>(
                event_ctx.request_metadata(),
                event_ctx.user_provided_entity(),
                TabularListFlags::all(),
                event_ctx.action().clone(),
                state_catalog.clone(),
            )
            .await;
        let (_event_ctx, _table) = event_ctx.emit_authz(authz_result)?;
        // ------------------- BUSINESS LOGIC -------------------
        let mut t = C::Transaction::begin_write(state_catalog).await?;
        let status = C::set_tabular_protected(
            warehouse_id,
            TabularId::Table(table_id),
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

    async fn get_table_protection(
        table_id: TableId,
        warehouse_id: WarehouseId,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ProtectionResponse> {
        // ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz;

        let event_ctx = APIEventContext::for_table(
            Arc::new(request_metadata.clone()),
            state.v1_state.events.clone(),
            warehouse_id,
            table_id,
            CatalogTableAction::GetMetadata,
        );

        let authz_result = authorizer
            .load_and_authorize_table_operation::<C>(
                event_ctx.request_metadata(),
                event_ctx.user_provided_entity(),
                TabularListFlags::all(),
                event_ctx.action().clone(),
                state.v1_state.catalog,
            )
            .await;
        let (_event_ctx, (_, _, table)) = event_ctx.emit_authz(authz_result)?;

        Ok(ProtectionResponse {
            protected: table.protected,
            updated_at: table.updated_at,
        })
    }
}
