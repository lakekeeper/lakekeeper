use std::sync::Arc;

use super::{ApiServer, ProtectionResponse};
use crate::{
    WarehouseId,
    api::{ApiContext, RequestMetadata, Result},
    service::{
        CatalogStore, CatalogTabularOps, DatasetId, DatasetTabularInfo, SecretStore, State,
        TabularId, TabularListFlags, Transaction,
        authz::{AuthZDatasetOps, Authorizer, CatalogDatasetAction},
        events::{APIEventContext, context::UserProvidedDataset},
    },
};

impl<C: CatalogStore, A: Authorizer + Clone, S: SecretStore> DatasetManagementService<C, A, S>
    for ApiServer<C, A, S>
{
}

#[async_trait::async_trait]
pub trait DatasetManagementService<C: CatalogStore, A: Authorizer, S: SecretStore>
where
    Self: Send + Sync + 'static,
{
    async fn set_dataset_protection(
        dataset_id: DatasetId,
        warehouse_id: WarehouseId,
        protected: bool,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ProtectionResponse> {
        // ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz;
        let state_catalog = state.v1_state.catalog.clone();

        let event_ctx = APIEventContext::for_dataset(
            Arc::new(request_metadata),
            state.v1_state.events.clone(),
            warehouse_id,
            dataset_id,
            CatalogDatasetAction::SetProtection,
        );

        let authz_result = authorize_set_or_get::<C, A>(
            &authorizer,
            event_ctx.request_metadata(),
            warehouse_id,
            dataset_id,
            event_ctx.action().clone(),
            state_catalog.clone(),
        )
        .await;
        let (_event_ctx, _info) = event_ctx.emit_authz(authz_result)?;

        // ------------------- BUSINESS LOGIC -------------------
        let mut t = C::Transaction::begin_write(state_catalog).await?;
        let status = C::set_tabular_protected(
            warehouse_id,
            TabularId::Dataset(dataset_id),
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

    async fn get_dataset_protection(
        dataset_id: DatasetId,
        warehouse_id: WarehouseId,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ProtectionResponse> {
        // ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz;

        let event_ctx = APIEventContext::for_dataset(
            Arc::new(request_metadata),
            state.v1_state.events.clone(),
            warehouse_id,
            dataset_id,
            CatalogDatasetAction::GetMetadata,
        );

        let authz_result = authorize_set_or_get::<C, A>(
            &authorizer,
            event_ctx.request_metadata(),
            warehouse_id,
            dataset_id,
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
    dataset_id: DatasetId,
    action: CatalogDatasetAction,
    catalog_state: C::State,
) -> std::result::Result<DatasetTabularInfo, crate::service::authz::AuthZError>
where
    C: CatalogStore,
    A: Authorizer + Clone,
{
    // `all()`: protection must stay settable on a soft-deleted dataset, or it
    // could not be lifted to undrop one.
    let (_warehouse, _namespace, info) = authorizer
        .load_and_authorize_dataset_operation::<C>(
            request_metadata,
            &UserProvidedDataset::new(warehouse_id, dataset_id),
            TabularListFlags::all(),
            action,
            catalog_state,
        )
        .await?;

    Ok(info)
}
