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
async fn authorize_namespace_move<C: CatalogStore, A: Authorizer>(
    authorizer: &A,
    request_metadata: &RequestMetadata,
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    destination: &NamespaceIdent,
    force: bool,
    catalog_state: C::State,
) -> std::result::Result<
    (Arc<ResolvedWarehouse>, NamespaceHierarchy),
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
    if let Some(destination_parent) = destination.parent() {
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
                Ok(Some(parent_namespace)),
                CatalogNamespaceAction::AcceptMovedNamespace {
                    source: source_path,
                },
            )
            .await?;
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
    }

    Ok((warehouse, namespace))
}

/// Input validation for a move destination, matching what creating a namespace at that path
/// would require. The storage layer trusts the caller for these, as `create_namespace_impl`
/// does.
fn validate_move_destination(destination: &NamespaceIdent) -> Result<()> {
    validate_namespace_ident_creation(destination)?;
    // `validate_namespace_ident_creation` passes a zero-length ident vacuously — its depth,
    // dot and empty-part checks all hold trivially for no elements — and `NamespaceIdent`
    // derives `Deserialize` over a `Vec<String>`, so `[]` reaches us from the wire. Reject it
    // here rather than letting a caller index into it.
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
    Ok(())
}

/// Refuse the move when the warehouse's storage layout would end up disagreeing with the
/// namespace hierarchy.
///
/// A namespace's physical location is frozen at creation, so a move never relocates existing
/// data. But under a layout that derives locations from the ancestor chain or from namespace
/// names, a *later-created* child would compute its path from the new chain and land outside
/// the moved namespace — fragmenting the layout silently. See
/// [`StorageLayout::move_desyncs_location`](crate::service::storage::storage_layout::StorageLayout::move_desyncs_location).
fn ensure_storage_layout_permits_move(
    warehouse: &ResolvedWarehouse,
    previous_ident: &NamespaceIdent,
    destination: &NamespaceIdent,
) -> Result<()> {
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
    Ok(())
}

/// Request to move a namespace to a new location in the hierarchy.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
pub struct MoveNamespaceRequest {
    /// Full new path of the namespace, including its new name as the last element.
    ///
    /// The preceding elements identify the new parent; a single-element destination moves the
    /// namespace to the warehouse root. Must not be empty. Renaming in place is expressed by
    /// keeping the same parent and changing only the last element. Mirrors the `destination`
    /// of Iceberg REST rename-table request.
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
    /// to the root) — grant authority on top of the ordinary write privilege at each end.
    /// See [`authorize_namespace_move`].
    ///
    /// # Authorization-hierarchy ordering
    ///
    /// The hierarchy is re-pointed *around* the commit: detach the old parent before it,
    /// attach the new one after.
    ///
    /// The alternative — attach first, detach second, both after the commit — has failure
    /// modes that leave the namespace reachable from *two* parents at once, so its contents
    /// silently keep inheriting permissions from where they used to live. That is invisible:
    /// no error reaches the caller, the catalog looks correct, and the surplus grant appears
    /// in no assignment listing.
    ///
    /// Detaching first trades that for a brief window in which the namespace inherits from
    /// *neither* parent — principals whose access arrives through the parent get 403s for one
    /// authorizer round-trip plus the commit. Direct grants and ownership on the namespace
    /// itself are separate relations and unaffected.
    ///
    /// The window is worth it because every failure mode then leaves the authorizer
    /// *missing* an edge rather than holding a surplus one:
    ///
    /// | Failure | Resulting state |
    /// |---|---|
    /// | detach (pre-commit) | nothing committed; both systems unchanged; request fails |
    /// | commit | old edge re-attached to compensate; request fails |
    /// | attach-new (post-commit) | namespace has no parent: access lost, never leaked |
    ///
    /// Missing edges are also the repairable kind: `lakekeeper openfga reconcile` fixes them
    /// in its **default** additive mode, whereas removing a surplus edge needs
    /// `--mode add-and-delete-drift`. So the surviving failure is both fail-closed and
    /// repairable by the command an operator reaches for first.
    ///
    /// Retrying the request does *not* repair a failed post-commit attach: the catalog has
    /// already moved, so the storage layer reports a no-op and the hooks are skipped.
    /// Reconciliation is the repair path, not retry.
    async fn move_namespace(
        namespace_id: NamespaceId,
        warehouse_id: WarehouseId,
        request: MoveNamespaceRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<MoveNamespaceResponse> {
        // ------------------- VALIDATIONS -------------------
        let MoveNamespaceRequest { destination, force } = request;
        validate_move_destination(&destination)?;

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
        let (event_ctx, (warehouse, namespace)) = event_ctx.emit_authz(authz_result)?;

        // ------------------- STORAGE LAYOUT -------------------
        ensure_storage_layout_permits_move(&warehouse, namespace.namespace_ident(), &destination)?;

        let event_ctx = event_ctx.resolve(ResolvedNamespace {
            warehouse,
            namespace: namespace.namespace.clone(),
        });

        // ------------------- BUSINESS LOGIC -------------------
        // Detach-then-commit-then-attach; see this method's docs for why that order.
        let mut t = C::Transaction::begin_write(state_catalog).await?;
        let moved = C::move_namespace(
            warehouse_id,
            namespace_id,
            &destination,
            force,
            t.transaction(),
        )
        .await?;

        let reparented = moved.changed_parent();
        let old_parent = moved
            .previous_parent
            .map_or(NamespaceParent::Warehouse(warehouse_id), |parent| {
                NamespaceParent::Namespace(parent)
            });
        let new_parent = moved
            .namespace
            .parent_namespaces_id()
            .map_or(NamespaceParent::Warehouse(warehouse_id), |parent| {
                NamespaceParent::Namespace(parent)
            });

        // Pre-commit: retire the old edge. Hard error — nothing is committed yet, so
        // failing here leaves both systems as they were. Mirrors `create_namespace`, which
        // likewise writes to the authorizer inside the open transaction.
        if reparented {
            authorizer
                .detach_namespace_parent(
                    event_ctx.request_metadata(),
                    namespace_id,
                    old_parent.clone(),
                )
                .await?;
        }

        if let Err(err) = t.commit().await {
            // The move did not happen, so put the old edge back. Best effort: if this also
            // fails the namespace is left parentless, which is fail-closed and repairable
            // by an additive reconcile.
            if reparented {
                authorizer
                    .attach_namespace_parent(event_ctx.request_metadata(), namespace_id, old_parent)
                    .await
                    .inspect_err(|e| {
                        tracing::error!(
                            ?e,
                            "Failed to restore namespace parent in authorizer after a failed \
                             commit: {}",
                            e.error
                        );
                    })
                    .ok();
            }
            return Err(err);
        }

        let response = MoveNamespaceResponse {
            namespace: moved.namespace.canonical_ident().clone(),
            namespace_id: moved.namespace.namespace_id(),
            parent_namespace_id: moved.namespace.parent_namespaces_id(),
        };

        // Post-commit: publish the new edge, now that the catalog has accepted the move.
        // Its failure cannot be reported — the move happened — so it is logged and left to
        // reconciliation, per the contract on `attach_namespace_parent`.
        if reparented {
            authorizer
                .attach_namespace_parent(event_ctx.request_metadata(), namespace_id, new_parent)
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
