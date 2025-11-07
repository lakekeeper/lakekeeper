use super::{ApiServer, ProtectionResponse};
use crate::{
    api::{ApiContext, RequestMetadata, Result},
    service::{
        authz::{
            AuthZTableOps, Authorizer, AuthzNamespaceOps, AuthzWarehouseOps, CatalogTableAction,
            RequireTableActionError,
        },
        CatalogNamespaceOps, CatalogStore, CatalogTabularOps, CatalogWarehouseOps, SecretStore,
        State, TableId, TabularId, TabularListFlags, Transaction,
    },
    WarehouseId,
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
        let state_catalog = state.v1_state.catalog;

        let (warehouse, table) = tokio::join!(
            C::get_active_warehouse_by_id(warehouse_id, state_catalog.clone()),
            C::get_table_info(
                warehouse_id,
                table_id,
                TabularListFlags::all(),
                state_catalog.clone(),
            )
        );
        let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;
        let table = authorizer.require_table_presence(warehouse_id, table_id, table)?;
        let namespace = C::get_namespace(
            warehouse_id,
            table.tabular_ident.namespace.clone(),
            state_catalog.clone(),
        )
        .await;
        let namespace = authorizer.require_namespace_presence(
            warehouse_id,
            table.tabular_ident.namespace.clone(),
            namespace,
        )?;

        authorizer
            .require_table_action(
                &request_metadata,
                &warehouse,
                &namespace,
                table_id,
                Ok::<_, RequireTableActionError>(Some(table)),
                CatalogTableAction::CanDrop,
            )
            .await?;

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
        //  ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz.clone();

        let state_catalog = state.v1_state.catalog;

        let (warehouse, table) = tokio::join!(
            C::get_active_warehouse_by_id(warehouse_id, state_catalog.clone()),
            C::get_table_info(
                warehouse_id,
                table_id,
                TabularListFlags::all(),
                state_catalog.clone(),
            )
        );
        let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;
        let table = authorizer.require_table_presence(warehouse_id, table_id, table)?;
        let namespace = C::get_namespace(
            warehouse_id,
            table.tabular_ident.namespace.clone(),
            state_catalog.clone(),
        )
        .await;
        let namespace = authorizer.require_namespace_presence(
            warehouse_id,
            table.tabular_ident.namespace.clone(),
            namespace,
        )?;

        let table = authorizer
            .require_table_action(
                &request_metadata,
                &warehouse,
                &namespace,
                table_id,
                Ok::<_, RequireTableActionError>(Some(table)),
                CatalogTableAction::CanGetMetadata,
            )
            .await?;

        Ok(ProtectionResponse {
            protected: table.protected,
            updated_at: table.updated_at,
        })
    }
}
