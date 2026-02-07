use std::sync::Arc;

use super::{ApiServer, ProtectionResponse};
use crate::{
    WarehouseId,
    api::{ApiContext, RequestMetadata, Result},
    service::{
        CachePolicy, CatalogNamespaceOps, CatalogStore, NamespaceId, SecretStore, State,
        Transaction,
        authz::{Authorizer, AuthzNamespaceOps, CatalogNamespaceAction},
        events::SetNamespaceProtectionEvent,
    },
};

impl<C: CatalogStore, A: Authorizer + Clone, S: SecretStore> NamespaceManagementService<C, A, S>
    for ApiServer<C, A, S>
{
}

#[async_trait::async_trait]
pub trait NamespaceManagementService<C: CatalogStore, A: Authorizer, S: SecretStore>
where
    Self: Send + Sync + 'static,
{
    async fn set_namespace_protection(
        namespace_id: NamespaceId,
        warehouse_id: WarehouseId,
        protected_request: bool,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ProtectionResponse> {
        //  ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz.clone();

        let (_warehouse, _namespace) = authorizer
            .load_and_authorize_namespace_action::<C>(
                &request_metadata,
                warehouse_id,
                namespace_id,
                CatalogNamespaceAction::SetProtection,
                CachePolicy::Skip,
                state.v1_state.catalog.clone(),
            )
            .await?;

        let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
        tracing::debug!(
            "Setting protection status for namespace: {:?} to {protected_request}",
            namespace_id
        );
        let status = C::set_namespace_protected(
            warehouse_id,
            namespace_id,
            protected_request,
            t.transaction(),
        )
        .await?;
        t.commit().await?;

        let protected = status.namespace.protected;
        let updated_at = status.namespace.updated_at;

        state
            .v1_state
            .events
            .namespace_protection_set(SetNamespaceProtectionEvent {
                requested_protected: protected_request,
                updated_namespace: status,
                request_metadata: Arc::new(request_metadata),
            })
            .await;

        let protection_response = ProtectionResponse {
            protected,
            updated_at,
        };
        Ok(protection_response)
    }

    async fn get_namespace_protection(
        namespace_id: NamespaceId,
        warehouse_id: WarehouseId,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ProtectionResponse> {
        let authorizer = state.v1_state.authz.clone();

        let (_warehouse, namespace) = authorizer
            .load_and_authorize_namespace_action::<C>(
                &request_metadata,
                warehouse_id,
                namespace_id,
                CatalogNamespaceAction::GetMetadata,
                CachePolicy::Skip,
                state.v1_state.catalog,
            )
            .await?;

        Ok(ProtectionResponse {
            protected: namespace.is_protected(),
            updated_at: namespace.updated_at(),
        })
    }
}
