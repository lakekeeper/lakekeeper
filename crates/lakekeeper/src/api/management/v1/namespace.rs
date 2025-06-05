use utoipa::ToSchema;
use uuid::Uuid;

use super::{ApiServer, ProtectionResponse};
use crate::{
    api::{ApiContext, RequestMetadata, Result},
    service::{
        authz::{Authorizer, CatalogNamespaceAction, CatalogWarehouseAction, NamespaceParent},
        Catalog, MoveNamespaceParent, NamespaceId, SecretStore, State, Transaction,
    },
    WarehouseId,
};

impl<C: Catalog, A: Authorizer + Clone, S: SecretStore> NamespaceManagementService<C, A, S>
    for ApiServer<C, A, S>
{
}

#[derive(Debug, Clone, serde::Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub enum MoveNamespaceRequest {
    Namespace { id: Uuid, new_name: Option<String> },
    Warehouse { new_name: Option<String> },
}

#[async_trait::async_trait]
pub trait NamespaceManagementService<C: Catalog, A: Authorizer, S: SecretStore>
where
    Self: Send + Sync + 'static,
{
    async fn set_namespace_protection(
        namespace_id: NamespaceId,
        _warehouse_id: WarehouseId,
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
        namespace_id: NamespaceId,
        _warehouse_id: WarehouseId,
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

    async fn move_namespace(
        namespace_id: NamespaceId,
        warehouse_id: WarehouseId,
        request: MoveNamespaceRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        let authorizer = state.v1_state.authz.clone();
        let mut t = C::Transaction::begin_write(state.v1_state.catalog.clone()).await?;
        authorizer
            .require_namespace_action(
                &request_metadata,
                Ok(Some(namespace_id)),
                CatalogNamespaceAction::CanDelete,
            )
            .await?;
        let (new_parent, new_name) = match request {
            MoveNamespaceRequest::Namespace { id, new_name } => {
                authorizer
                    .require_namespace_action(
                        &request_metadata,
                        Ok(Some(id.into())),
                        CatalogNamespaceAction::CanCreateNamespace,
                    )
                    .await?;
                (MoveNamespaceParent::Namespace(id.into()), new_name)
            }
            MoveNamespaceRequest::Warehouse { new_name } => {
                authorizer
                    .require_warehouse_action(
                        &request_metadata,
                        warehouse_id,
                        CatalogWarehouseAction::CanCreateNamespace,
                    )
                    .await?;
                (MoveNamespaceParent::Warehouse(warehouse_id), new_name)
            }
        };
        let move_namespace_response = C::move_namespace(
            warehouse_id,
            namespace_id,
            Some(new_parent),
            new_name,
            t.transaction(),
        )
        .await?;
        match move_namespace_response {
            crate::service::MoveNamespaceResponse::NoOp
            | crate::service::MoveNamespaceResponse::Rename => (),
            crate::service::MoveNamespaceResponse::Move {
                old_parent_namespace_id,
            } => {
                authorizer
                    .move_namespace(
                        &request_metadata,
                        namespace_id,
                        match new_parent {
                            MoveNamespaceParent::Namespace(id) => NamespaceParent::Namespace(id),
                            MoveNamespaceParent::Warehouse(id) => NamespaceParent::Warehouse(id),
                        },
                        match old_parent_namespace_id {
                            Some(old_parent_namespace_id) => {
                                NamespaceParent::Namespace(old_parent_namespace_id)
                            }
                            None => NamespaceParent::Warehouse(warehouse_id),
                        },
                    )
                    .await?;
            }
        }

        t.commit().await?;
        Ok(())
    }
}
