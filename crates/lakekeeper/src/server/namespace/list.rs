use std::sync::Arc;

use iceberg::NamespaceIdent;

use crate::{
    WarehouseId,
    api::RequestMetadata,
    service::{
        CatalogNamespaceOps, CatalogStore, CatalogWarehouseOps, NamespaceHierarchy,
        ResolvedWarehouse,
        authz::{
            AuthZCannotUseWarehouseId, AuthZWarehouseActionForbidden, Authorizer,
            AuthzNamespaceOps, AuthzWarehouseOps, CatalogNamespaceAction, CatalogWarehouseAction,
            AuthZError, RequireWarehouseActionError,
        },
    },
};

pub(super) async fn authorize_namespace_list<C: CatalogStore, A: Authorizer>(
    authorizer: A,
    request_metadata: &RequestMetadata,
    warehouse_id: WarehouseId,
    parent: Option<&NamespaceIdent>,
    catalog_state: C::State,
) -> Result<
    (bool, Arc<ResolvedWarehouse>, Option<NamespaceHierarchy>),
    AuthZError,
> {
    let warehouse = C::get_active_warehouse_by_id(warehouse_id, catalog_state.clone()).await;
    let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;

    let [can_use_warehouse, can_list_namespaces, can_list_everything] = authorizer
        .are_allowed_warehouse_actions_arr(
            request_metadata,
            None,
            &[
                (&warehouse, CatalogWarehouseAction::Use),
                (&warehouse, CatalogWarehouseAction::ListNamespaces),
                (&warehouse, CatalogWarehouseAction::ListEverything),
            ],
        )
        .await?
        .into_inner();

    if !can_use_warehouse {
        return Err(RequireWarehouseActionError::from(
            AuthZCannotUseWarehouseId::new_access_denied(warehouse_id),
        )
        .into());
    }
    if !can_list_namespaces {
        return Err(
            RequireWarehouseActionError::from(AuthZWarehouseActionForbidden::new(
                warehouse_id,
                &CatalogWarehouseAction::ListNamespaces,
            ))
            .into(),
        );
    }

    let mut can_list_everything = can_list_everything;
    let (warehouse, parent_namespace) = if let Some(parent_ident) = parent {
        let parent_namespace =
            C::get_namespace(warehouse_id, parent_ident.clone(), catalog_state.clone()).await;

        let parent_namespace = authorizer
            .require_namespace_action(
                request_metadata,
                &warehouse,
                parent_ident,
                parent_namespace,
                CatalogNamespaceAction::ListNamespaces,
            )
            .await?;
        // Rely on short-circuit of `||` to query `namespace:can_list_everything` only if not
        // `warehouse:can_list_everything`.
        can_list_everything = can_list_everything
            || authorizer
                .is_allowed_namespace_action(
                    request_metadata,
                    None,
                    &warehouse,
                    &parent_namespace.parents,
                    &parent_namespace.namespace,
                    CatalogNamespaceAction::ListEverything,
                )
                .await?
                .into_inner();

        (warehouse, Some(parent_namespace))
    } else {
        (warehouse, None)
    };

    Ok((can_list_everything, warehouse, parent_namespace))
}
