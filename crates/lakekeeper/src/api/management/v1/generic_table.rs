use std::sync::Arc;

use super::{ApiServer, ProtectionResponse};
use crate::{
    WarehouseId,
    api::{ApiContext, RequestMetadata, Result},
    service::{
        CatalogStore, CatalogTabularOps, GenericTableId, SecretStore, State, TabularId,
        TabularListFlags, Transaction,
        authz::{AuthZGenericTableOps, Authorizer, CatalogGenericTableAction},
        events::{APIEventContext, context::UserProvidedGenericTable},
    },
};

impl<C: CatalogStore, A: Authorizer + Clone, S: SecretStore> GenericTableManagementService<C, A, S>
    for ApiServer<C, A, S>
{
}

#[async_trait::async_trait]
pub trait GenericTableManagementService<C: CatalogStore, A: Authorizer, S: SecretStore>
where
    Self: Send + Sync + 'static,
{
    async fn set_generic_table_protection(
        generic_table_id: GenericTableId,
        warehouse_id: WarehouseId,
        protected: bool,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ProtectionResponse> {
        // ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz;
        let state_catalog = state.v1_state.catalog.clone();

        let event_ctx = APIEventContext::for_generic_table(
            Arc::new(request_metadata),
            state.v1_state.events.clone(),
            warehouse_id,
            generic_table_id,
            CatalogGenericTableAction::SetProtection,
        );

        let authz_result = authorize_set_or_get::<C, A>(
            &authorizer,
            event_ctx.request_metadata(),
            warehouse_id,
            generic_table_id,
            event_ctx.action().clone(),
            state_catalog.clone(),
        )
        .await;
        let (_event_ctx, _info) = event_ctx.emit_authz(authz_result)?;

        // ------------------- BUSINESS LOGIC -------------------
        let mut t = C::Transaction::begin_write(state_catalog).await?;
        let status = C::set_tabular_protected(
            warehouse_id,
            TabularId::GenericTable(generic_table_id),
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

    async fn get_generic_table_protection(
        generic_table_id: GenericTableId,
        warehouse_id: WarehouseId,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ProtectionResponse> {
        // ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz;

        let event_ctx = APIEventContext::for_generic_table(
            Arc::new(request_metadata),
            state.v1_state.events.clone(),
            warehouse_id,
            generic_table_id,
            CatalogGenericTableAction::GetMetadata,
        );

        let authz_result = authorize_set_or_get::<C, A>(
            &authorizer,
            event_ctx.request_metadata(),
            warehouse_id,
            generic_table_id,
            event_ctx.action().clone(),
            state.v1_state.catalog,
        )
        .await;
        let (_event_ctx, info) = event_ctx.emit_authz(authz_result)?;

        Ok(ProtectionResponse {
            protected: info.protected,
            updated_at: info.updated_at,
        })
    }
}

async fn authorize_set_or_get<C, A>(
    authorizer: &A,
    request_metadata: &RequestMetadata,
    warehouse_id: WarehouseId,
    generic_table_id: GenericTableId,
    action: CatalogGenericTableAction,
    catalog_state: C::State,
) -> std::result::Result<crate::service::GenericTabularInfo, crate::service::authz::AuthZError>
where
    C: CatalogStore,
    A: Authorizer + Clone,
{
    let (_warehouse, _namespace, info) = authorizer
        .load_and_authorize_generic_table_operation::<C>(
            request_metadata,
            &UserProvidedGenericTable::new(warehouse_id, generic_table_id),
            TabularListFlags::all(),
            action,
            catalog_state,
        )
        .await?;

    Ok(info)
}
