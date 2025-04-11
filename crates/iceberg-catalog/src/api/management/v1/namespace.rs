use super::{ApiServer, ProtectionResponse};
use crate::{
    api::{ApiContext, RequestMetadata, Result},
    service::{
        authz::{Authorizer, CatalogNamespaceAction},
        Catalog, NamespaceIdentUuid, SecretStore, State, Transaction,
    },
    WarehouseIdent,
};

impl<C: Catalog, A: Authorizer + Clone, S: SecretStore> NamespaceManagementService<C, A, S>
    for ApiServer<C, A, S>
{
}

#[async_trait::async_trait]
pub trait NamespaceManagementService<C: Catalog, A: Authorizer, S: SecretStore>
where
    Self: Send + Sync + 'static,
{
    async fn set_namespace_protection(
        namespace_id: NamespaceIdentUuid,
        _warehouse_id: WarehouseIdent,
        protected: bool,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ProtectionResponse> {
        //  ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz.clone();
        let mut t = C::Transaction::begin_write(state.v1_state.catalog.clone()).await?;

        authorizer
            .require_namespace_action(
                &request_metadata,
                Ok(Some(namespace_id)),
                CatalogNamespaceAction::CanDelete,
            )
            .await?;
        tracing::debug!(
            "Setting protection status for namespace: {:?} to {protected}",
            namespace_id
        );
        let status = C::set_namespace_protected(namespace_id, protected, t.transaction()).await?;
        t.commit().await?;
        Ok(status)
    }

    async fn get_namespace_protection(
        namespace_id: NamespaceIdentUuid,
        _warehouse_id: WarehouseIdent,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ProtectionResponse> {
        //  ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz.clone();
        let mut t = C::Transaction::begin_read(state.v1_state.catalog.clone()).await?;

        authorizer
            .require_namespace_action(
                &request_metadata,
                Ok(Some(namespace_id)),
                CatalogNamespaceAction::CanGetMetadata,
            )
            .await?;
        let status = C::get_namespace_protected(namespace_id, t.transaction()).await?;
        t.commit().await?;
        Ok(status)
    }
}
