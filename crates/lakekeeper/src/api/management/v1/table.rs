use super::{ApiServer, ProtectionResponse};
use crate::{
    api::{ApiContext, RequestMetadata, Result},
    service::{
        authz::{Authorizer, CatalogTableAction},
        Catalog, SecretStore, State, TableId, TabularId, Transaction,
    },
    WarehouseId,
};

impl<C: Catalog, A: Authorizer + Clone, S: SecretStore> TableManagementService<C, A, S>
    for ApiServer<C, A, S>
{
}

#[async_trait::async_trait]
pub trait TableManagementService<C: Catalog, A: Authorizer, S: SecretStore>
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
        let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;

        authorizer
            .require_table_action(
                &request_metadata,
                Ok(Some(table_id)),
                CatalogTableAction::CanDrop,
            )
            .await?;

        let status = C::set_tabular_protected(
            warehouse_id,
            TabularId::Table(*table_id),
            protected,
            t.transaction(),
        )
        .await?;
        t.commit().await?;
        Ok(status)
    }

    async fn get_table_protection(
        table_id: TableId,
        warehouse_id: WarehouseId,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ProtectionResponse> {
        //  ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz.clone();
        let mut t = C::Transaction::begin_read(state.v1_state.catalog.clone()).await?;

        authorizer
            .require_table_action(
                &request_metadata,
                Ok(Some(table_id)),
                CatalogTableAction::CanGetMetadata,
            )
            .await?;
        let status =
            C::get_tabular_protected(warehouse_id, table_id.into(), t.transaction()).await?;
        t.commit().await?;
        Ok(status)
    }
}
