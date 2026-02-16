use std::sync::Arc;

use super::{ApiServer, ProtectionResponse};
use crate::{
    WarehouseId,
    api::{ApiContext, RequestMetadata, Result},
    service::{
        CachePolicy, CatalogNamespaceOps, CatalogStore, NamespaceId, SecretStore, State,
        Transaction,
        authz::{Authorizer, AuthzNamespaceOps, CatalogNamespaceAction},
        events::{APIEventContext, context::ResolvedNamespace},
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
        let authorizer = state.v1_state.authz;
        let state_catalog = state.v1_state.catalog.clone();

        let event_ctx = APIEventContext::for_namespace(
            Arc::new(request_metadata),
            state.v1_state.events.clone(),
            warehouse_id,
            namespace_id,
            CatalogNamespaceAction::SetProtection,
        );

        let authz_result = authorizer
            .load_and_authorize_namespace_action::<C>(
                event_ctx.request_metadata(),
                event_ctx.user_provided_entity().clone(),
                event_ctx.action().clone(),
                CachePolicy::Skip,
                state_catalog.clone(),
            )
            .await;
        let (event_ctx, (warehouse, namespace)) = event_ctx.emit_authz(authz_result)?;
        let event_ctx = event_ctx.resolve(ResolvedNamespace {
            warehouse,
            namespace: namespace.namespace,
        });

        // ------------------- BUSINESS LOGIC -------------------
        let mut t = C::Transaction::begin_write(state_catalog).await?;
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

        event_ctx.emit_namespace_protection_set(protected_request, status.clone());

        let protected = status.namespace.protected;
        let updated_at = status.namespace.updated_at;

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
        // ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz;

        let event_ctx = APIEventContext::for_namespace(
            Arc::new(request_metadata),
            state.v1_state.events.clone(),
            warehouse_id,
            namespace_id,
            CatalogNamespaceAction::GetMetadata,
        );

        let authz_result = authorizer
            .load_and_authorize_namespace_action::<C>(
                event_ctx.request_metadata(),
                event_ctx.user_provided_entity().clone(),
                event_ctx.action().clone(),
                CachePolicy::Skip,
                state.v1_state.catalog,
            )
            .await;
        let (_event_ctx, (_warehouse, namespace)) = event_ctx.emit_authz(authz_result)?;

        Ok(ProtectionResponse {
            protected: namespace.is_protected(),
            updated_at: namespace.updated_at(),
        })
    }
}
