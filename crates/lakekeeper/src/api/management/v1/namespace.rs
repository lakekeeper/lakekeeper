use super::{ApiServer, ProtectionResponse};
use crate::{
    api::{ApiContext, RequestMetadata, Result},
    service::{
        authz::{Authorizer, AuthzNamespaceOps, CatalogNamespaceAction},
        CatalogNamespaceOps, CatalogStore, NamespaceId, SecretStore, State, Transaction,
    },
    WarehouseId,
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
        protected: bool,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ProtectionResponse> {
        //  ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz.clone();

        let namespace =
            C::get_namespace(warehouse_id, namespace_id, state.v1_state.catalog.clone()).await;

        authorizer
            .require_namespace_action(
                &request_metadata,
                warehouse_id,
                namespace_id,
                namespace,
                CatalogNamespaceAction::CanDelete,
            )
            .await?;

        let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
        tracing::debug!(
            "Setting protection status for namespace: {:?} to {protected}",
            namespace_id
        );
        let status =
            C::set_namespace_protected(warehouse_id, namespace_id, protected, t.transaction())
                .await?;
        t.commit().await?;
        let protection_response = ProtectionResponse {
            protected: status.protected,
            updated_at: status.updated_at,
        };
        Ok(protection_response)
    }

    async fn get_namespace_protection(
        namespace_id: NamespaceId,
        warehouse_id: WarehouseId,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ProtectionResponse> {
        //  ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz.clone();

        let namespace =
            C::get_namespace(warehouse_id, namespace_id, state.v1_state.catalog.clone()).await;

        let namespace = authorizer
            .require_namespace_action(
                &request_metadata,
                warehouse_id,
                namespace_id,
                namespace,
                CatalogNamespaceAction::CanGetMetadata,
            )
            .await?;

        Ok(ProtectionResponse {
            protected: namespace.is_protected(),
            updated_at: namespace.updated_at(),
        })
    }
}
