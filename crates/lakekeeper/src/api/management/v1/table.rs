use super::{ApiServer, ProtectionResponse};
use crate::{
    api::{ApiContext, RequestMetadata, Result},
    service::{
        authz::{AuthZTableOps, Authorizer, CatalogTableAction},
        CatalogStore, CatalogTabularOps, SecretStore, State, TableId, TabularId, TabularListFlags,
        Transaction,
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
        let table = C::get_table_info(
            warehouse_id,
            table_id,
            TabularListFlags::all(),
            state.v1_state.catalog.clone(),
        )
        .await;
        authorizer
            .require_table_action(
                &request_metadata,
                warehouse_id,
                table_id,
                table,
                CatalogTableAction::CanDrop,
            )
            .await?;

        let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
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

        let info = C::get_table_info(
            warehouse_id,
            table_id,
            TabularListFlags::all(),
            state.v1_state.catalog,
        )
        .await;

        let info = authorizer
            .require_table_action(
                &request_metadata,
                warehouse_id,
                table_id,
                info,
                CatalogTableAction::CanGetMetadata,
            )
            .await?;

        Ok(ProtectionResponse {
            protected: info.protected,
            updated_at: info.updated_at,
        })
    }
}
