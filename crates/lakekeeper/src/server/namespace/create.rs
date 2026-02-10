use std::{collections::BTreeMap, sync::Arc};

use iceberg::NamespaceIdent;

use crate::{
    WarehouseId,
    api::RequestMetadata,
    service::{
        CatalogNamespaceOps, CatalogStore, CatalogWarehouseOps, NamespaceHierarchy,
        ResolvedWarehouse,
        authz::{
            Authorizer, AuthzNamespaceOps, AuthzWarehouseOps, CatalogNamespaceAction,
            CatalogWarehouseAction, FetchWarehouseNamespaceTabularError,
        },
    },
};

pub(super) async fn authorize_namespace_create<C: CatalogStore, A: Authorizer>(
    authorizer: &A,
    request_metadata: &RequestMetadata,
    warehouse_id: WarehouseId,
    parent: Option<&NamespaceIdent>,
    catalog_state: C::State,
    properties: Arc<BTreeMap<String, String>>,
) -> Result<(Arc<ResolvedWarehouse>, Option<NamespaceHierarchy>), FetchWarehouseNamespaceTabularError>
{
    let warehouse = C::get_active_warehouse_by_id(warehouse_id, catalog_state.clone()).await;
    let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;

    Ok(if let Some(namespace_parent) = parent {
        let parent_namespace = C::get_namespace(
            warehouse_id,
            namespace_parent.clone(),
            catalog_state.clone(),
        )
        .await;
        let parent_namespace = authorizer
            .require_namespace_action(
                request_metadata,
                &warehouse,
                namespace_parent.clone(),
                parent_namespace,
                CatalogNamespaceAction::CreateNamespace { properties },
            )
            .await?;
        (warehouse, Some(parent_namespace))
    } else {
        let warehouse = authorizer
            .require_warehouse_action(
                request_metadata,
                warehouse_id,
                Ok(Some(warehouse)),
                CatalogWarehouseAction::CreateNamespace { properties },
            )
            .await?;
        (warehouse, None)
    })
}
