use super::{ApiServer, ProtectionResponse};
use crate::{
    WarehouseId,
    api::{ApiContext, RequestMetadata, Result},
    logging::audit::{
        AuditContext,
        events::{AuthorizationDeniedEvent, SetTableProtectionEvent},
    },
    service::{
        CatalogStore, CatalogTabularOps, SecretStore, State, TableId, TabularId, TabularListFlags,
        Transaction,
        authz::{AuthZTableOps, Authorizer, CatalogTableAction},
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

        let (_warehouse, _namespace, _table) = authorizer
            .load_and_authorize_table_operation::<C>(
                &request_metadata,
                warehouse_id,
                table_id,
                TabularListFlags::all(),
                CatalogTableAction::SetProtection,
                state_catalog.clone(),
            )
            .await
            .inspect_err(|e| {
                request_metadata.log_audit(AuthorizationDeniedEvent {
                    action: "set_table_protection".to_string(),
                    error: e.to_string(),
                });
            })?;

        request_metadata.log_audit(SetTableProtectionEvent {
            warehouse_id,
            table_id,
            protected,
        });

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

        let (_warehouse, _namespace, table) = authorizer
            .load_and_authorize_table_operation::<C>(
                &request_metadata,
                warehouse_id,
                table_id,
                TabularListFlags::all(),
                CatalogTableAction::GetMetadata,
                state.v1_state.catalog,
            )
            .await
            .inspect_err(|e| {
                request_metadata.log_audit(AuthorizationDeniedEvent {
                    action: "get_table_protection".to_string(),
                    error: e.to_string(),
                });
            })?;

        Ok(ProtectionResponse {
            protected: table.protected,
            updated_at: table.updated_at,
        })
    }
}
