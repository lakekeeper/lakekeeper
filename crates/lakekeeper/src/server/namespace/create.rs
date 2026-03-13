use std::{collections::BTreeMap, sync::Arc};

use iceberg::NamespaceIdent;

use crate::{
    WarehouseId,
    api::RequestMetadata,
    service::{
        CatalogNamespaceOps, CatalogStore, CatalogWarehouseOps, NamespaceHierarchy,
        ResolvedWarehouse,
        authz::{
            AuthZError, Authorizer, AuthzNamespaceOps, AuthzWarehouseOps, CatalogNamespaceAction,
            CatalogWarehouseAction,
        },
    },
};

pub(super) async fn authorize_namespace_create<C: CatalogStore, A: Authorizer>(
    authorizer: &A,
    request_metadata: &RequestMetadata,
    warehouse_id: WarehouseId,
    new_namespace: &NamespaceIdent,
    catalog_state: C::State,
    properties: Arc<BTreeMap<String, String>>,
) -> Result<(Arc<ResolvedWarehouse>, Option<NamespaceHierarchy>), AuthZError> {
    let warehouse = C::get_active_warehouse_by_id(warehouse_id, catalog_state.clone()).await;
    let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;
    let name = new_namespace.as_ref().last().cloned().unwrap_or_default();

    Ok(if let Some(parent) = new_namespace.parent() {
        let parent_namespace =
            C::get_namespace(warehouse_id, parent.clone(), catalog_state.clone()).await;
        let parent_namespace = authorizer
            .require_namespace_action(
                request_metadata,
                &warehouse,
                parent.clone(),
                parent_namespace,
                CatalogNamespaceAction::CreateNamespace {
                    name: Some(name),
                    properties,
                },
            )
            .await?;
        (warehouse, Some(parent_namespace))
    } else {
        let warehouse = authorizer
            .require_warehouse_action(
                request_metadata,
                warehouse_id,
                Ok(Some(warehouse)),
                CatalogWarehouseAction::CreateNamespace {
                    name: Some(name),
                    properties,
                },
            )
            .await?;
        (warehouse, None)
    })
}
