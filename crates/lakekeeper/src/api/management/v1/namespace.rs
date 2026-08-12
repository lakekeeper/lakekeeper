use std::sync::Arc;

use iceberg_ext::catalog::rest::ErrorModel;
use serde::{Deserialize, Serialize};

use super::{ApiServer, ProtectionResponse};
use crate::{
    CONFIG, WarehouseId,
    api::{ApiContext, RequestMetadata, Result},
    server::namespace::validate_namespace_ident_creation,
    service::{
        CachePolicy, CatalogNamespaceOps, CatalogStore, CatalogWarehouseOps, NamespaceHierarchy,
        NamespaceId, NamespaceIdent, ResolvedWarehouse, SecretStore, State, Transaction,
        authz::{
            Authorizer, AuthzNamespaceOps, AuthzWarehouseOps, CatalogNamespaceAction,
            CatalogWarehouseAction, NamespaceParent,
        },
        events::{APIEventContext, context::ResolvedNamespace},
    },
};

impl<C: CatalogStore, A: Authorizer + Clone, S: SecretStore> NamespaceManagementService<C, A, S>
    for ApiServer<C, A, S>
{
}

/// The action a move requests on the namespace being moved.
///
/// Single definition, used both to build the request's [`APIEventContext`] and to perform the
/// authorization check, so the audited action and the checked action cannot diverge — and the
/// `destination` carried in the action is necessarily the one being authorized.
fn move_namespace_action(destination: &NamespaceIdent, force: bool) -> CatalogNamespaceAction {
    CatalogNamespaceAction::Move {
        destination: Arc::new(destination.as_ref().clone()),
        force,
    }
}

/// Authorize a namespace move: `Move` on the namespace, plus `CreateNamespace` **and**
/// `AcceptMovedNamespace` on the destination parent.
///
/// Both checks live in one function so a single `emit_authz` covers them — the destination
/// decision is as much a part of the audit trail as the source one.
///
/// Deliberately does not accept the action as a parameter: it derives it from `destination`
/// and `force` via [`move_namespace_action`], so a caller cannot authorize a move against
/// some other namespace action, nor pass an action whose `destination` disagrees with the
/// destination actually being checked.
///
/// Returns the destination as a [`NamespaceParent`] so the caller can hand it to the
/// authorizer's hierarchy hook without resolving it a second time.
async fn authorize_namespace_move<C: CatalogStore, A: Authorizer>(
    authorizer: &A,
    request_metadata: &RequestMetadata,
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    destination: &NamespaceIdent,
    force: bool,
    catalog_state: C::State,
) -> std::result::Result<
    (Arc<ResolvedWarehouse>, NamespaceHierarchy, NamespaceParent),
    crate::service::authz::AuthZError,
> {
    let action = move_namespace_action(destination, force);
    let warehouse = C::get_active_warehouse_by_id(warehouse_id, catalog_state.clone()).await;
    let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;

    // Cold path: read authoritative state rather than a possibly-lagging cached copy.
    let namespace = C::get_namespace_cache_aware(
        warehouse_id,
        namespace_id,
        CachePolicy::Skip,
        catalog_state.clone(),
    )
    .await;
    let namespace = authorizer
        .require_namespace_action(
            request_metadata,
            &warehouse,
            namespace_id,
            namespace,
            action,
        )
        .await?;

    // Two checks at the destination, not one.
    //
    // `CreateNamespace` answers "may a child be added here" — the structural question, and
    // the same one `create_namespace` asks. `AcceptMovedNamespace` answers "may grants be
    // issued here", which `create` does not imply: an inbound move carries existing contents
    // and their direct grants, so allowing it on `create` alone would let a namespace be
    // populated and granted under a permissive parent and then moved into a `managed_access`
    // subtree — issuing grants there that the actor could never have issued directly.
    //
    // Net rule: the actor must be able to grant at *both* ends.
    //
    // `None` parent means the warehouse root, mirroring `authorize_namespace_create`. The
    // root needs the same treatment: a `managed_access` warehouse is equally a destination
    // whose grants are meant to be centrally controlled.
    let destination_name = destination.as_ref().last().cloned().unwrap_or_default();
    let source_path = Arc::new(namespace.namespace_ident().as_ref().clone());
    let new_parent = if let Some(destination_parent) = destination.parent() {
        let parent_namespace = C::get_namespace_cache_aware(
            warehouse_id,
            destination_parent.clone(),
            CachePolicy::Skip,
            catalog_state,
        )
        .await;
        let parent_namespace = authorizer
            .require_namespace_action(
                request_metadata,
                &warehouse,
                destination_parent.clone(),
                parent_namespace,
                CatalogNamespaceAction::CreateNamespace {
                    name: Some(destination_name),
                    properties: Arc::new(std::collections::BTreeMap::new()),
                },
            )
            .await?;
        authorizer
            .require_namespace_action(
                request_metadata,
                &warehouse,
                destination_parent,
                Ok(Some(parent_namespace.clone())),
                CatalogNamespaceAction::AcceptMovedNamespace {
                    source: source_path,
                },
            )
            .await?;
        NamespaceParent::Namespace(parent_namespace.namespace_id())
    } else {
        authorizer
            .require_warehouse_action(
                request_metadata,
                warehouse_id,
                Ok(Some(warehouse.clone())),
                CatalogWarehouseAction::CreateNamespace {
                    name: Some(destination_name),
                    properties: Arc::new(std::collections::BTreeMap::new()),
                },
            )
            .await?;
        authorizer
            .require_warehouse_action(
                request_metadata,
                warehouse_id,
                Ok(Some(warehouse.clone())),
                CatalogWarehouseAction::AcceptMovedNamespace {
                    source: source_path,
                },
            )
            .await?;
        NamespaceParent::Warehouse(warehouse_id)
    };

    Ok((warehouse, namespace, new_parent))
}

/// Request to move a namespace to a new location in the hierarchy.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
pub struct MoveNamespaceRequest {
    /// Full new path of the namespace, including its new name as the last element.
    ///
    /// The preceding elements identify the new parent; an empty list moves the namespace to
    /// the warehouse root. Renaming in place is expressed by keeping the same parent and
    /// changing only the last element. Mirrors the `destination` of the Iceberg
    /// rename-table request.
    ///
    /// A destination equal to the namespace's current path succeeds without changing
    /// anything, so retrying a request that already went through is safe.
    #[cfg_attr(feature = "open-api", schema(value_type = Vec<String>))]
    pub destination: NamespaceIdent,
    /// Move the namespace even if it is protected.
    #[serde(default)]
    pub force: bool,
}

/// The namespace after a successful move.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
pub struct MoveNamespaceResponse {
    /// The namespace's new path.
    #[cfg_attr(feature = "open-api", schema(value_type = Vec<String>))]
    pub namespace: NamespaceIdent,
    /// Unchanged by the move; returned so callers can confirm identity.
    #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
    pub namespace_id: NamespaceId,
    /// Id of the new parent namespace, or `null` if the namespace is now top-level.
    #[cfg_attr(feature = "open-api", schema(value_type = Option<uuid::Uuid>))]
    pub parent_namespace_id: Option<NamespaceId>,
}

impl axum::response::IntoResponse for MoveNamespaceResponse {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::OK, axum::Json(self)).into_response()
    }
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

    /// Move a namespace to `request.destination`, re-parenting and/or renaming it.
    ///
    /// Requires `move` on the namespace itself, plus both `create_namespace` and
    /// `accept_moved_namespace` on the destination parent (or on the warehouse, when moving
    /// to the root) — grant authority at both ends. See [`authorize_namespace_move`].
    async fn move_namespace(
        namespace_id: NamespaceId,
        warehouse_id: WarehouseId,
        request: MoveNamespaceRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<MoveNamespaceResponse> {
        // ------------------- VALIDATIONS -------------------
        // Same input rules as creating a namespace at this path: the storage layer trusts
        // the caller for these, exactly as `create_namespace_impl` does.
        let MoveNamespaceRequest { destination, force } = request;
        validate_namespace_ident_creation(&destination)?;
        // `validate_namespace_ident_creation` passes a zero-length ident vacuously — its
        // depth, dot and empty-part checks all hold trivially for no elements — and
        // `NamespaceIdent` derives `Deserialize` over a `Vec<String>`, so `[]` reaches us
        // from the wire. Reject it here rather than indexing into it below.
        let Some(first_segment) = destination.as_ref().first() else {
            return Err(ErrorModel::bad_request(
                "Destination namespace must not be empty.",
                "NamespaceEmpty",
                None,
            )
            .into());
        };
        if CONFIG
            .reserved_namespaces
            .contains(&first_segment.to_lowercase())
        {
            tracing::debug!("Denying move to reserved namespace: '{first_segment}'");
            return Err(ErrorModel::bad_request(
                "Namespace is reserved for internal use.",
                "ReservedNamespace",
                None,
            )
            .into());
        }

        // ------------------- AUTHZ -------------------
        // Before opening the write transaction: the authorizer may read the catalog on a
        // cache miss, and doing that inside an open transaction would check out a second
        // pool connection.
        let authorizer = state.v1_state.authz.clone();
        let state_catalog = state.v1_state.catalog.clone();

        let event_ctx = APIEventContext::for_namespace(
            Arc::new(request_metadata),
            state.v1_state.events.clone(),
            warehouse_id,
            namespace_id,
            move_namespace_action(&destination, force),
        );

        // Both decisions are produced before emitting, so a denial at *either* end is
        // recorded by the single `emit_authz` below rather than escaping unaudited.
        let authz_result = authorize_namespace_move::<C, A>(
            &authorizer,
            event_ctx.request_metadata(),
            warehouse_id,
            namespace_id,
            &destination,
            force,
            state_catalog.clone(),
        )
        .await;
        let (event_ctx, (warehouse, namespace, new_parent)) = event_ctx.emit_authz(authz_result)?;

        // ------------------- STORAGE LAYOUT -------------------
        // A namespace's physical location is frozen at creation. Under layouts that derive
        // it from the ancestor chain or the name, moving would leave later-created children
        // in unrelated places, so refuse rather than silently fragment the layout.
        let previous_ident = namespace.namespace_ident().clone();
        let renamed = previous_ident.as_ref().last() != destination.as_ref().last();
        let reparented = previous_ident.parent() != destination.parent();
        if let Some(layout) = warehouse.storage_profile.layout()
            && layout.move_desyncs_location(renamed, reparented)
        {
            return Err(ErrorModel::bad_request(
                "Namespaces cannot be moved in this warehouse: its storage layout derives \
                 physical locations from namespace names or from the namespace hierarchy, so \
                 moving would place newly created child namespaces outside the moved \
                 namespace's location.",
                "StorageLayoutForbidsNamespaceMove",
                None,
            )
            .into());
        }

        let event_ctx = event_ctx.resolve(ResolvedNamespace {
            warehouse,
            namespace: namespace.namespace.clone(),
        });

        // ------------------- BUSINESS LOGIC -------------------
        let mut t = C::Transaction::begin_write(state_catalog).await?;
        let moved = C::move_namespace(
            warehouse_id,
            namespace_id,
            &destination,
            force,
            t.transaction(),
        )
        .await?;
        t.commit().await?;

        let response = MoveNamespaceResponse {
            namespace: moved.namespace.canonical_ident().clone(),
            namespace_id: moved.namespace.namespace_id(),
            parent_namespace_id: moved.namespace.parent_namespaces_id(),
        };

        // ------------------- POST-COMMIT -------------------
        // Only now that the catalog has committed. Re-pointing the authorization hierarchy
        // beforehand would grant the destination's principals access to a namespace that may
        // still roll back.
        //
        // The trade-off is that a failure here cannot be surfaced to the caller — the move
        // did happen — so authorization is left lagging the catalog: the namespace's
        // contents keep inheriting from the *old* parent, and only gain the new parent's
        // grants once the write succeeds. Permissive in the stale direction, which is why
        // this ordering is still the safer one.
        //
        // Nothing repairs that automatically. `lakekeeper openfga reconcile` is an operator
        // CLI command, not a background task, and its default `add-missing` mode is purely
        // additive — removing the contradicted old-parent edge needs
        // `--mode add-and-delete-drift`. The same exposure exists at the `delete_namespace`
        // / `delete_table` / `delete_view` hooks, which drop their failures the same way.
        if moved.changed_parent() {
            let old_parent = moved
                .previous_parent
                .map_or(NamespaceParent::Warehouse(warehouse_id), |parent| {
                    NamespaceParent::Namespace(parent)
                });
            authorizer
                .move_namespace(
                    event_ctx.request_metadata(),
                    namespace_id,
                    new_parent,
                    old_parent,
                )
                .await
                .inspect_err(|e| {
                    tracing::error!(?e, "Failed to move namespace in authorizer: {}", e.error);
                })
                .ok();
        }

        // Invalidates the pre-move path in this replica's namespace cache, among others.
        event_ctx.emit_namespace_moved_async(moved);

        Ok(response)
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
