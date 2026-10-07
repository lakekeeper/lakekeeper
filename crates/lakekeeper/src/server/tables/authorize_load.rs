use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use iceberg::{NamespaceIdent, TableIdent};
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};

use crate::{
    WarehouseId,
    api::iceberg::types::ReferencingView,
    config::{MatchedEngines, SecurityModel},
    request_metadata::RequestMetadata,
    service::{
        Actor, AuthZTabularInfo as _, CatalogBackendError, CatalogGetNamespaceError,
        CatalogGetWarehouseByIdError, CatalogNamespaceOps, CatalogStore, CatalogTabularOps,
        CatalogWarehouseOps, GenericTabularInfo, GetTabularInfoError, NamespaceHierarchy,
        NamespaceId, NamespaceWithParent, ResolveTasksError, ResolvedWarehouse,
        TabularIdentBorrowed, TabularIdentOwned, TabularInfo, TabularListFlags, UserId, ViewInfo,
        ViewOrTableInfo,
        authz::{
            ActionOnGenericTable, ActionOnTable, ActionOnTableOrView, ActionOnView,
            AuthZCannotSeeNamespace, AuthZError, AuthZTableOps, AuthZViewOps,
            AuthorizationCountMismatch, Authorizer, AuthzBadRequest,
            BackendUnavailableOrCountMismatch, CatalogGenericTableAction, CatalogTableAction,
            CatalogViewAction, RequireTableActionError, RequireViewActionError, UserOrRole,
        },
    },
};

pub(crate) type TabularAuthzAction<'a> = (
    &'a NamespaceWithParent,
    ActionOnTableOrView<
        'a,
        'a,
        TabularInfo<crate::service::TableId>,
        ViewInfo,
        CatalogTableAction,
        CatalogViewAction,
        GenericTabularInfo,
        CatalogGenericTableAction,
    >,
);

#[derive(thiserror::Error, Debug)]
#[error("Referenced-by chain of {depth} views exceeds the maximum depth of {max_depth} views")]
pub(crate) struct ReferencedByDepthExceeded {
    pub(crate) depth: usize,
    pub(crate) max_depth: usize,
}

impl From<ReferencedByDepthExceeded> for IcebergErrorResponse {
    fn from(value: ReferencedByDepthExceeded) -> Self {
        let typ = "ReferencedByDepthExceeded";
        let boxed = Box::new(value);
        let message = boxed.to_string();

        ErrorModel::bad_request(message, typ, Some(boxed)).into()
    }
}

#[derive(thiserror::Error, Debug)]
#[error("A referenced-by chain must not contain the object being loaded")]
pub(crate) struct ReferencedByContainsTarget;

impl From<ReferencedByContainsTarget> for IcebergErrorResponse {
    fn from(value: ReferencedByContainsTarget) -> Self {
        let typ = "ReferencedByContainsTarget";
        let boxed = Box::new(value);
        let message = boxed.to_string();

        ErrorModel::bad_request(message, typ, Some(boxed)).into()
    }
}

/// Bounds the client-supplied `referenced_by` chain to `max_depth` views.
///
/// Every entry widens the authorization work for the request: it is resolved,
/// authorized and sorted alongside the target tabular. The bound is checked on
/// the raw supplied list, before [`effective_referenced_by`] narrows it — so it
/// is a property of the request, not of engine trust, and an untrusted caller
/// whose chain would have been ignored is rejected just the same. Deduplication
/// happens downstream; the client controls the raw depth, so that is what is
/// capped.
///
/// `max_depth` is taken as an argument rather than read from the global config
/// so that callers pass `CONFIG.referenced_by.max_nesting_depth` explicitly and
/// the wiring is observable in tests.
pub(crate) fn validate_referenced_by_depth(
    referenced_by: Option<&[ReferencingView]>,
    max_depth: usize,
) -> Result<(), ReferencedByDepthExceeded> {
    let depth = referenced_by.map_or(0, <[ReferencingView]>::len);
    if depth > max_depth {
        return Err(ReferencedByDepthExceeded { depth, max_depth });
    }
    Ok(())
}

/// Validates a client-supplied `referenced_by` chain: its depth, then the
/// identifier of every entry, then that no entry names `target`, the object
/// being loaded. Nothing can reference itself; the chain authorization relies
/// on the target being its last entry.
///
/// Entries are held to the same identifier rules as the target of the request
/// (`MAX_NAMESPACE_DEPTH`, no `.` in namespace parts, no empty parts or name).
/// Without this, the cap would bound the *number* of entries but not their
/// *size*: a chain of legal length whose entries carry deeply nested namespace
/// idents still expands in the parent-path CTE of `get_namespaces_by_name`.
/// Nothing legitimate is rejected — namespace creation enforces the same rules,
/// so an entry that fails here cannot name a view that exists.
///
/// Depth is checked first, so the per-entry loop is itself bounded.
pub(crate) fn validate_referenced_by(
    referenced_by: Option<&[ReferencingView]>,
    target: &TableIdent,
    max_depth: usize,
) -> crate::api::Result<()> {
    validate_referenced_by_depth(referenced_by, max_depth)?;
    let views = referenced_by.unwrap_or(&[]);
    for view in views {
        super::validate_table_or_view_ident(view.as_ref())?;
    }
    if views.iter().any(|view| view.as_ref() == target) {
        return Err(ReferencedByContainsTarget.into());
    }
    Ok(())
}

/// Filters `referenced_by` based on engine presence. Without a trusted engine
/// we cannot determine the DEFINER/INVOKER security model, so the parameter
/// is ignored.
pub(crate) fn effective_referenced_by<'a>(
    referenced_by: Option<&'a [ReferencingView]>,
    engines: &MatchedEngines,
) -> Option<&'a [ReferencingView]> {
    if referenced_by.is_some() && !engines.is_trusted() {
        tracing::debug!(
            "referenced-by parameter ignored: no trusted engine configured for this request"
        );
    }
    referenced_by.filter(|_| engines.is_trusted())
}

pub(crate) fn get_relevant_namespaces_to_authorize_load_tabular<'a>(
    tabular: &TabularIdentBorrowed<'a>,
    referenced_by: Option<&'a [ReferencingView]>,
) -> HashSet<NamespaceIdent> {
    let views = referenced_by.unwrap_or(&[]);
    let mut results = HashSet::with_capacity(views.len() + 1);
    results.insert(tabular.as_table_ident().namespace().clone());
    for view in views {
        results.insert(view.as_ref().namespace().clone());
    }
    results
}

pub(crate) fn get_relevant_tabulars_to_authorize_load_tabular<'a>(
    tabular: TabularIdentBorrowed<'a>,
    referenced_by: Option<&'a [ReferencingView]>,
) -> HashSet<TabularIdentOwned> {
    let views = referenced_by.unwrap_or(&[]);
    let mut results = HashSet::with_capacity(views.len() + 1);
    results.insert(tabular.into());
    for view in views {
        results.insert(TabularIdentBorrowed::View(view).into());
    }
    results
}

#[derive(Debug)]
pub(crate) struct AuthorizeLoadTabularObjects {
    pub(crate) warehouse: Result<Option<Arc<ResolvedWarehouse>>, CatalogGetWarehouseByIdError>,
    pub(crate) namespaces:
        Result<HashMap<NamespaceId, NamespaceWithParent>, CatalogGetNamespaceError>,
    pub(crate) tabulars: Result<HashMap<TableIdent, ViewOrTableInfo>, GetTabularInfoError>,
}

pub(crate) async fn load_objects_to_authorize_load_tabular<C: CatalogStore>(
    warehouse_id: WarehouseId,
    namespaces: Vec<NamespaceIdent>,
    tabulars: Vec<TabularIdentOwned>,
    list_flags: TabularListFlags,
    state: C::State,
) -> AuthorizeLoadTabularObjects {
    let ns_refs: Vec<_> = namespaces.iter().collect();
    let tab_refs: Vec<_> = tabulars.iter().map(|t| t.as_borrowed()).collect();
    let (warehouse, ns, tabs) = tokio::join!(
        C::get_active_warehouse_by_id(warehouse_id, state.clone()),
        C::get_namespaces_by_ident(warehouse_id, &ns_refs, state.clone()),
        C::get_tabular_infos_by_ident(warehouse_id, &tab_refs, list_flags, state),
    );

    AuthorizeLoadTabularObjects {
        warehouse,
        namespaces: ns,
        tabulars: tabs,
    }
}

pub(crate) fn check_required_tabulars<A: Authorizer>(
    warehouse_id: WarehouseId,
    user_provided_tabulars: HashSet<TabularIdentOwned>,
    tabulars: Result<HashMap<TableIdent, ViewOrTableInfo>, GetTabularInfoError>,
    authorizer: &A,
) -> Result<HashMap<TableIdent, ViewOrTableInfo>, AuthZError> {
    let tabulars = tabulars.map_err(|e| {
        ResolveTasksError::CatalogBackendError(CatalogBackendError::new_unexpected(e))
    })?;

    for user_provided_tabular in user_provided_tabulars {
        match user_provided_tabular {
            TabularIdentOwned::Table(table_ident) => {
                let table = tabulars
                    .get(&table_ident)
                    .and_then(|info| info.clone().into_table_info());
                authorizer.require_table_presence(
                    warehouse_id,
                    table_ident,
                    Ok::<_, RequireTableActionError>(table),
                )?;
            }
            TabularIdentOwned::View(view_ident) => {
                let view = tabulars
                    .get(&view_ident)
                    .and_then(|info| info.clone().into_view_info());
                authorizer.require_view_presence(
                    warehouse_id,
                    view_ident,
                    Ok::<_, RequireViewActionError>(view),
                )?;
            }
            TabularIdentOwned::GenericTable(_) => {
                // Generic tables are handled via dedicated endpoints.
            }
        }
    }

    Ok(tabulars)
}

pub(crate) fn check_required_namespaces(
    warehouse_id: WarehouseId,
    user_provided_namespaces: &HashSet<NamespaceIdent>,
    namespaces: Result<HashMap<NamespaceId, NamespaceWithParent>, CatalogGetNamespaceError>,
) -> Result<HashMap<NamespaceId, NamespaceWithParent>, AuthZError> {
    let namespaces = namespaces.map_err(|e| {
        ResolveTasksError::CatalogBackendError(CatalogBackendError::new_unexpected(e))
    })?;

    let namespace_idents: HashSet<NamespaceIdent> = namespaces
        .values()
        .map(NamespaceWithParent::namespace_ident)
        .cloned()
        .collect();

    let missing_namespaces = user_provided_namespaces
        .difference(&namespace_idents)
        .collect::<Vec<_>>();
    if let Some(missing_namespace) = missing_namespaces.first() {
        return Err(
            AuthZCannotSeeNamespace::new_not_found(warehouse_id, *missing_namespace).into(),
        );
    }

    Ok(namespaces)
}

pub(crate) fn sort_tabulars_for_authorize_load_tabular(
    tabular_infos: &HashMap<TableIdent, ViewOrTableInfo>,
    referenced_by: Option<&[ReferencingView]>,
    tabular: &TableIdent,
) -> Vec<ViewOrTableInfo> {
    let capacity = referenced_by.map_or(0, <[ReferencingView]>::len) + 1;
    let mut results = Vec::with_capacity(capacity);

    if let Some(referencing_views) = referenced_by {
        for referencing_view in referencing_views {
            if let Some(info) = tabular_infos.get(referencing_view.as_ref()) {
                results.push(info.clone());
            } else {
                debug_assert!(
                    false,
                    "Referencing view {:?} not found in tabular_infos — should have been caught by check_required_tabulars",
                    referencing_view.as_ref()
                );
            }
        }
    }

    if let Some(info) = tabular_infos.get(tabular) {
        results.push(info.clone());
    }

    results
}

pub(crate) fn add_namespace_to_tabulars_for_authorize_load_tabular(
    warehouse_id: WarehouseId,
    tabulars: Vec<ViewOrTableInfo>,
    namespaces: &HashMap<NamespaceId, NamespaceHierarchy>,
) -> Result<Vec<(ViewOrTableInfo, NamespaceHierarchy)>, AuthZError> {
    tabulars
        .into_iter()
        .map(|tabular| {
            let namespace_id = tabular.namespace_id();
            namespaces
                .get(&namespace_id)
                .map(|namespace| (tabular, namespace.clone()))
                .ok_or_else(|| {
                    AuthZCannotSeeNamespace::new_not_found(warehouse_id, namespace_id).into()
                })
        })
        .collect()
}

/// Resolve DEFINER owners and assign the current user for each tabular in the chain.
///
/// For each view with a DEFINER security model, the owner is resolved to an `Actor`
/// and becomes the `current_user` for subsequent tabulars. INVOKER views inherit the
/// current user unchanged.
///
/// When no trusted engine is present, only the base tabular (last entry) is returned
/// with the request actor.
/// Result entry from [`resolve_users_for_authorize_load_tabular`].
#[derive(Debug)]
pub struct ResolvedTabular {
    pub tabular: ViewOrTableInfo,
    pub user: Option<UserOrRole>,
    /// True if this tabular is accessed via delegated execution (downstream of a DEFINER view).
    pub is_delegated_execution: bool,
    pub namespace: NamespaceHierarchy,
}

/// Resolve users for each tabular in the authorization chain.
///
/// `token_idp_id` is the `IdP` of the requesting token — used to construct
/// owner `UserId`s for DEFINER views. This comes from the token, not from
/// engine config, because the owner string was set by that same `IdP`.
pub(crate) fn resolve_users_for_authorize_load_tabular(
    tabulars: &[(ViewOrTableInfo, NamespaceHierarchy)],
    actor: &Actor,
    engines: &MatchedEngines,
    token_idp_id: Option<&str>,
) -> Result<Vec<ResolvedTabular>, AuthZError> {
    if !engines.is_trusted() {
        // Without an engine, only authorize the base tabular (last in sorted order).
        return Ok(tabulars
            .last()
            .map(|(tabular, namespace)| ResolvedTabular {
                tabular: tabular.clone(),
                user: actor.to_user_or_role(),
                is_delegated_execution: false,
                namespace: namespace.clone(),
            })
            .into_iter()
            .collect());
    }

    let mut current_user: Actor = actor.clone();
    let mut delegated = false;
    let mut owners_cache: HashMap<String, Actor> = HashMap::new();
    let mut result = Vec::with_capacity(tabulars.len());

    for (tabular, namespace) in tabulars {
        result.push(ResolvedTabular {
            tabular: tabular.clone(),
            user: current_user.to_user_or_role(),
            is_delegated_execution: delegated,
            namespace: namespace.clone(),
        });
        // Only views have a security model. Tables and generic tables can only
        // appear as the last entry (the target) — all referenced-by entries are
        // looked up as TabularIdentBorrowed::View and the DB filters by type.
        // This function is shared between the iceberg-table load path (target
        // is a Table) and the generic-table credentials path (target is a
        // GenericTable), so both must short-circuit the security-model check.
        // Otherwise a generic table whose properties happen to match an
        // engine's owner-property key would be misread as DEFINER and trigger
        // delegated execution.
        //
        // Exhaustive match: a future variant of ViewOrTableInfo must make an
        // explicit decision here.
        match tabular {
            ViewOrTableInfo::Table(_) | ViewOrTableInfo::GenericTable(_) => {
                debug_assert!(
                    tabulars
                        .last()
                        .is_some_and(|(t, _)| std::ptr::eq(t, tabular)),
                    "Table or generic table appeared as intermediate entry in authorization chain"
                );
                continue;
            }
            ViewOrTableInfo::View(_) => {}
        }
        match engines
            .determine_security_model(tabular.properties())
            .map_err(|e| AuthZError::from(AuthzBadRequest::new(e.to_string())))?
        {
            SecurityModel::Invoker => {}
            SecurityModel::Definer(owner) => {
                current_user = if let Some(cached) = owners_cache.get(&owner) {
                    cached.clone()
                } else {
                    let idp_id = token_idp_id.ok_or_else(|| {
                        AuthZError::from(AuthzBadRequest::new(
                            "DEFINER view requires token with IdP ID".to_string(),
                        ))
                    })?;
                    let subject = limes::Subject::new(Some(idp_id.to_string()), owner.clone());
                    let user_id = UserId::try_new(subject).map_err(|e| {
                        AuthZError::from(AuthzBadRequest::new(format!(
                            "Invalid owner '{owner}' in DEFINER view property: {e}"
                        )))
                    })?;
                    let owner_actor = Actor::Principal(user_id);
                    owners_cache.insert(owner, owner_actor.clone());
                    owner_actor
                };
                delegated = true;
            }
        }
    }

    Ok(result)
}

/// Emits every authz action that *might* matter for an entry in the resolved
/// chain. Consumers pick which results they care about based on the entry's
/// role in their operation (target vs. intermediate).
///
/// `target` is the tabular being loaded; every other entry is an intermediate
/// referenced-by view. The role is decided by **ident**, not slice position —
/// the same key `interpret_authz_results_for_load_view` uses to consume these
/// results — so the two sides can never disagree about which entry is the
/// target, regardless of how the chain is ordered or assembled.
///
/// - **Table** (only ever the target) → `GetMetadata` + `ReadData` +
///   `WriteData`. `loadTable` uses all three: `GetMetadata` to gate presence,
///   `ReadData` / `WriteData` to decide which storage-credential scope to
///   return.
/// - **Target view** → `GetMetadata` only. `loadView` consults `GetMetadata`
///   and discards everything else, so emitting `Select` here would make the
///   authorizer evaluate (and log) a decision that never gates anything.
/// - **Intermediate view** (a DEFINER referenced-by view) → `GetMetadata` +
///   `Select`. `Select` is the data-plane check that gates DEFINER chain
///   traversal past the control-plane bypass; intermediate consumers enforce
///   denial on it.
#[must_use]
pub(crate) fn build_actions_from_sorted_tabulars_for_authorize_load_tabular<'a>(
    tabulars: &'a [ResolvedTabular],
    target: &TableIdent,
) -> Vec<TabularAuthzAction<'a>> {
    tabulars
        .iter()
        .flat_map(|resolved| {
            let is_target = resolved.tabular.tabular_ident() == target;
            let is_delegated_execution = resolved.is_delegated_execution;
            let user = resolved.user.as_ref();
            let tabular = &resolved.tabular;
            let namespace = &resolved.namespace;
            match tabular {
                ViewOrTableInfo::Table(info) => vec![
                    CatalogTableAction::GetMetadata,
                    CatalogTableAction::ReadData,
                    CatalogTableAction::WriteData,
                ]
                .into_iter()
                .map(|action| {
                    (
                        &namespace.namespace,
                        ActionOnTableOrView::Table(ActionOnTable {
                            info,
                            action,
                            user,
                            is_delegated_execution,
                        }),
                    )
                })
                .collect::<Vec<_>>(),
                ViewOrTableInfo::View(info) => {
                    // Target view: only `GetMetadata` is ever consulted (by
                    // `loadView`). Intermediate views additionally enforce
                    // `Select` for DEFINER chain traversal.
                    let view_actions = if is_target {
                        vec![CatalogViewAction::GetMetadata]
                    } else {
                        vec![CatalogViewAction::GetMetadata, CatalogViewAction::Select]
                    };
                    view_actions
                        .into_iter()
                        .map(|action| {
                            (
                                &namespace.namespace,
                                ActionOnTableOrView::View(ActionOnView {
                                    info,
                                    action,
                                    user,
                                    is_delegated_execution,
                                }),
                            )
                        })
                        .collect::<Vec<_>>()
                }
                ViewOrTableInfo::GenericTable(info) => vec![
                    CatalogGenericTableAction::GetMetadata,
                    CatalogGenericTableAction::ReadData,
                    CatalogGenericTableAction::WriteData,
                ]
                .into_iter()
                .map(|action| {
                    (
                        &namespace.namespace,
                        ActionOnTableOrView::GenericTable(ActionOnGenericTable {
                            info,
                            action,
                            user,
                            is_delegated_execution,
                        }),
                    )
                })
                .collect::<Vec<_>>(),
            }
        })
        .collect()
}

/// Who an action is decided for. Delegated execution is part of the key, so a
/// DEFINER view owned by the caller still starts a new segment.
fn acting_principal<'a>(action: &TabularAuthzAction<'a>) -> (Option<&'a UserOrRole>, bool) {
    match &action.1 {
        ActionOnTableOrView::Table(a) => (a.user, a.is_delegated_execution),
        ActionOnTableOrView::View(a) => (a.user, a.is_delegated_execution),
        ActionOnTableOrView::GenericTable(a) => (a.user, a.is_delegated_execution),
    }
}

/// Splits `actions` into contiguous runs decided for the same principal.
fn principal_segments<'s, 'a>(
    actions: &'s [TabularAuthzAction<'a>],
) -> impl Iterator<Item = &'s [TabularAuthzAction<'a>]> {
    actions.chunk_by(|a, b| acting_principal(a) == acting_principal(b))
}

/// A refused check on a view fails every load route: each consumer of
/// [`build_actions_from_sorted_tabulars_for_authorize_load_tabular`] returns its
/// error at the first refused intermediate view. A target view is the last
/// entry ([`validate_referenced_by`] keeps it out of the chain), so no segment
/// follows its refusal.
fn refusal_ends_request(action: &TabularAuthzAction<'_>) -> bool {
    match &action.1 {
        ActionOnTableOrView::View(_) => true,
        ActionOnTableOrView::Table(_) | ActionOnTableOrView::GenericTable(_) => false,
    }
}

/// Decisions for a load chain, one per action, in action order.
///
/// Entries from `decided` on were not sent to the authorizer, because a refused
/// view before them ended the request, and read as refused. Every consumer
/// returns at that refusal and never reads them; [`Self::iter`] asserts this in
/// debug builds.
#[derive(Debug)]
pub(crate) struct LoadChainDecisions {
    allowed: Vec<bool>,
    decided: usize,
}

impl LoadChainDecisions {
    pub(crate) fn len(&self) -> usize {
        self.allowed.len()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = bool> + '_ {
        self.allowed.iter().enumerate().map(|(index, &allowed)| {
            debug_assert!(
                index < self.decided,
                "a load-chain consumer read entry {index}, which the authorizer never decided"
            );
            allowed
        })
    }
}

/// Every entry decided, for tests of the consumers.
#[cfg(test)]
impl From<Vec<bool>> for LoadChainDecisions {
    fn from(allowed: Vec<bool>) -> Self {
        let decided = allowed.len();
        Self { allowed, decided }
    }
}

/// Decides a load chain one principal at a time, in chain order.
///
/// A segment that refuses a view ends the request, so later segments are not
/// sent to the authorizer: a refused caller never causes a lookup of a DEFINER
/// owner. A chain without DEFINER views is one segment and one call.
pub(crate) async fn are_allowed_load_chain_actions<A: Authorizer>(
    authorizer: &A,
    metadata: &RequestMetadata,
    warehouse: &ResolvedWarehouse,
    namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
    actions: &[TabularAuthzAction<'_>],
) -> Result<LoadChainDecisions, AuthZError> {
    let mut results = Vec::with_capacity(actions.len());
    for segment in principal_segments(actions) {
        let decided = authorizer
            .are_allowed_tabular_actions_vec(metadata, warehouse, namespaces, segment)
            .await?
            .into_allowed();
        if decided.len() != segment.len() {
            return Err(
                BackendUnavailableOrCountMismatch::from(AuthorizationCountMismatch::new(
                    segment.len(),
                    decided.len(),
                    "load_chain_segment",
                ))
                .into(),
            );
        }
        let ends_request = segment
            .iter()
            .zip(&decided)
            .any(|(action, allowed)| !allowed && refusal_ends_request(action));
        results.extend(decided);
        if ends_request {
            break;
        }
    }
    let decided = results.len();
    results.resize(actions.len(), false);
    Ok(LoadChainDecisions {
        allowed: results,
        decided,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use http::StatusCode;
    use iceberg::{NamespaceIdent, TableIdent};

    use super::*;
    use crate::{
        WarehouseId,
        config::{MatchedEngines, TrinoEngineConfig, TrustedEngine},
        server::{namespace::MAX_NAMESPACE_DEPTH, tables::interpret_authz_results_for_load_table},
        service::{
            AuthZTableInfo as _, AuthZViewInfo as _, BasicTabularInfo as _, TableInfo,
            authz::{
                AuthZCannotSeeTable, AuthZCannotSeeView,
                tests::{HidingAuthorizer, RecordedTabularCheck},
            },
            events::AuthorizationFailureSource as _,
            storage::StoragePermissions,
        },
    };

    fn referencing_views(n: usize) -> Vec<ReferencingView> {
        (0..n)
            .map(|i| {
                ReferencingView::new(TableIdent::new(
                    NamespaceIdent::new("ns".to_string()),
                    format!("view_{i}"),
                ))
            })
            .collect()
    }

    #[test]
    fn test_validate_referenced_by_depth_accepts_none_and_empty() {
        validate_referenced_by_depth(None, 0).unwrap();
        validate_referenced_by_depth(Some(&[]), 0).unwrap();
    }

    /// The limit governing the check is the one passed in, not a hardcoded
    /// value: each case uses a different bound and the accepted depth tracks it.
    #[test]
    fn test_validate_referenced_by_depth_boundary() {
        for max_depth in [0, 1, 3, 10] {
            let at_limit = referencing_views(max_depth);
            validate_referenced_by_depth(Some(&at_limit), max_depth)
                .unwrap_or_else(|_| panic!("a chain of exactly {max_depth} is accepted"));

            let over_limit = referencing_views(max_depth + 1);
            let err = validate_referenced_by_depth(Some(&over_limit), max_depth)
                .expect_err("one over the limit is rejected");
            assert_eq!(err.depth, max_depth + 1);
            assert_eq!(err.max_depth, max_depth);

            let response = IcebergErrorResponse::from(err);
            assert_eq!(response.error.code, StatusCode::BAD_REQUEST.as_u16());
            assert_eq!(response.error.r#type, "ReferencedByDepthExceeded");
            // The rendered message carries both numbers, not just the fields.
            assert_eq!(
                response.error.message,
                format!(
                    "Referenced-by chain of {} views exceeds the maximum depth of {max_depth} views",
                    max_depth + 1
                )
            );
        }
    }

    fn unrelated_target() -> TableIdent {
        TableIdent::new(NamespaceIdent::new("ns".to_string()), "target".to_string())
    }

    fn deep_view(depth: usize) -> ReferencingView {
        let parts: Vec<String> = (0..depth).map(|i| format!("ns{i}")).collect();
        ReferencingView::new(TableIdent::new(
            NamespaceIdent::from_vec(parts).unwrap(),
            "v".to_string(),
        ))
    }

    /// Entries are held to the same identifier rules as the request target, so
    /// the cap bounds the size of a chain and not merely its length.
    #[test]
    fn test_validate_referenced_by_rejects_oversized_entry_idents() {
        let ok = [deep_view(MAX_NAMESPACE_DEPTH as usize)];
        validate_referenced_by(Some(&ok), &unrelated_target(), 10)
            .expect("an entry at the namespace limit is accepted");

        let too_deep = [deep_view(MAX_NAMESPACE_DEPTH as usize + 1)];
        let err = validate_referenced_by(Some(&too_deep), &unrelated_target(), 10)
            .expect_err("an entry past the namespace limit is rejected");
        assert_eq!(err.error.code, StatusCode::BAD_REQUEST.as_u16());
        assert_eq!(err.error.r#type, "NamespaceDepthExceeded");
    }

    /// Depth is checked before the per-entry loop, so an over-deep chain of
    /// oversized entries reports the chain error and never walks the entries.
    #[test]
    fn test_validate_referenced_by_checks_depth_before_entries() {
        let over: Vec<ReferencingView> = (0..11)
            .map(|_| deep_view(MAX_NAMESPACE_DEPTH as usize + 1))
            .collect();
        let err =
            validate_referenced_by(Some(&over), &unrelated_target(), 10).expect_err("rejected");
        assert_eq!(err.error.r#type, "ReferencedByDepthExceeded");
    }

    /// The object being loaded cannot reference itself, wherever it appears in
    /// the chain.
    #[test]
    fn test_validate_referenced_by_rejects_the_target() {
        let target = unrelated_target();
        let other = ReferencingView::new(TableIdent::new(
            NamespaceIdent::new("ns".to_string()),
            "other".to_string(),
        ));
        let chains = [
            vec![ReferencingView::new(target.clone())],
            vec![other.clone(), ReferencingView::new(target.clone())],
            vec![ReferencingView::new(target.clone()), other.clone()],
        ];
        for chain in chains {
            let err = validate_referenced_by(Some(&chain), &target, 10)
                .expect_err("a chain naming the target is rejected");
            assert_eq!(err.error.code, StatusCode::BAD_REQUEST.as_u16());
            assert_eq!(err.error.r#type, "ReferencedByContainsTarget");
            assert_eq!(
                err.error.message,
                "A referenced-by chain must not contain the object being loaded"
            );
        }
        validate_referenced_by(Some(&[other]), &target, 10).expect("other views are accepted");
        validate_referenced_by(None, &target, 10).expect("no chain is accepted");
    }

    /// Depth is checked first, so an over-deep chain that also names the target
    /// reports its depth.
    #[test]
    fn test_validate_referenced_by_checks_depth_before_the_target() {
        let target = TableIdent::new(NamespaceIdent::new("ns".to_string()), "view_0".to_string());
        let over_limit = referencing_views(11);
        let err = validate_referenced_by(Some(&over_limit), &target, 10).expect_err("rejected");
        assert_eq!(err.error.r#type, "ReferencedByDepthExceeded");
    }

    /// The bound is checked on the raw client-supplied list, before
    /// [`effective_referenced_by`] narrows it — so an untrusted caller, whose
    /// chain would otherwise be ignored entirely, is rejected just the same.
    #[test]
    fn test_validate_referenced_by_depth_ignores_engine_trust() {
        let engines = MatchedEngines::default();
        assert!(!engines.is_trusted());

        let over_limit = referencing_views(11);
        assert!(effective_referenced_by(Some(&over_limit), &engines).is_none());
        validate_referenced_by_depth(Some(&over_limit), 10)
            .expect_err("rejected even though the chain would be ignored");
    }

    #[test]
    fn test_get_relevant_namespaces_to_authorize_load_tabular_contains_base_tabular_namespace() {
        let namespace_ident = NamespaceIdent::from_strs(vec!["ns_a", "ns_b"])
            .expect("NamespaceIdent should be able to be build");
        let table_ident = TableIdent::new(namespace_ident.clone(), "table".to_string());
        let table = TabularIdentBorrowed::Table(&table_ident);
        let namespaces = get_relevant_namespaces_to_authorize_load_tabular(&table, None);
        assert!(namespaces.contains(&namespace_ident));
    }

    #[test]
    fn test_get_relevant_namespaces_to_authorize_load_tabular_contains_all_namespaces_of_referencing_views()
     {
        let namespace_a = NamespaceIdent::from_strs(vec!["ns_a"]).unwrap();
        let view_a = TableIdent::new(namespace_a.clone(), "view_a".to_string());

        let namespace_b = NamespaceIdent::from_strs(vec!["ns_b"]).unwrap();
        let view_b = TableIdent::new(namespace_b.clone(), "view_b".to_string());

        let referencing_views = vec![ReferencingView::new(view_a), ReferencingView::new(view_b)];

        let namespace_c = NamespaceIdent::from_strs(vec!["ns_c"]).unwrap();
        let table_ident = TableIdent::new(namespace_c.clone(), "table".to_string());
        let table = TabularIdentBorrowed::Table(&table_ident);

        let namespaces =
            get_relevant_namespaces_to_authorize_load_tabular(&table, Some(&referencing_views));

        assert_eq!(namespaces.len(), 3);
        assert!(namespaces.contains(&namespace_a));
        assert!(namespaces.contains(&namespace_b));
        assert!(namespaces.contains(&namespace_c));
    }

    #[test]
    fn test_get_relevant_namespaces_to_authorize_load_tabular_contains_no_duplicates() {
        let namespace = NamespaceIdent::from_strs(vec!["ns"]).unwrap();

        let view_a = TableIdent::new(namespace.clone(), "view_a".to_string());
        let view_b = TableIdent::new(namespace.clone(), "view_b".to_string());
        let referencing_views = vec![ReferencingView::new(view_a), ReferencingView::new(view_b)];

        let table_ident = TableIdent::new(namespace.clone(), "table".to_string());
        let table = TabularIdentBorrowed::Table(&table_ident);

        let namespaces =
            get_relevant_namespaces_to_authorize_load_tabular(&table, Some(&referencing_views));

        assert_eq!(namespaces.len(), 1);
        assert!(namespaces.contains(&namespace));
    }

    #[test]
    fn test_get_relevant_namespaces_to_authorize_load_tabular_empty_referenced_by_returns_only_base()
     {
        let namespace = NamespaceIdent::from_strs(vec!["ns"]).unwrap();
        let table_ident = TableIdent::new(namespace.clone(), "table".to_string());
        let table = TabularIdentBorrowed::Table(&table_ident);

        let namespaces = get_relevant_namespaces_to_authorize_load_tabular(&table, None);

        assert_eq!(namespaces.len(), 1);
        assert!(namespaces.contains(&namespace));
    }

    #[test]
    fn test_get_relevant_tabulars_to_authorize_load_tabular_contains_base_tabular() {
        let namespace = NamespaceIdent::from_strs(vec!["ns"]).unwrap();
        let table_ident = TableIdent::new(namespace, "table".to_string());
        let table = TabularIdentBorrowed::Table(&table_ident);

        let tabulars = get_relevant_tabulars_to_authorize_load_tabular(table.clone(), None);

        assert_eq!(tabulars.len(), 1);
        assert!(tabulars.contains(&table.into()));
    }

    #[test]
    fn test_get_relevant_tabulars_to_authorize_load_tabular_contains_all_referencing_views() {
        let namespace_a = NamespaceIdent::from_strs(vec!["ns_a"]).unwrap();
        let view_a = TableIdent::new(namespace_a.clone(), "view_a".to_string());

        let namespace_b = NamespaceIdent::from_strs(vec!["ns_b"]).unwrap();
        let view_b = TableIdent::new(namespace_b.clone(), "view_b".to_string());

        let referencing_views = vec![
            ReferencingView::new(view_a.clone()),
            ReferencingView::new(view_b.clone()),
        ];

        let namespace_c = NamespaceIdent::from_strs(vec!["ns_c"]).unwrap();
        let table_ident = TableIdent::new(namespace_c.clone(), "table".to_string());
        let table = TabularIdentBorrowed::Table(&table_ident);

        let tabulars = get_relevant_tabulars_to_authorize_load_tabular(
            table.clone(),
            Some(&referencing_views),
        );

        assert_eq!(tabulars.len(), 3);
        assert!(tabulars.contains(&TabularIdentOwned::View(view_a)));
        assert!(tabulars.contains(&TabularIdentOwned::View(view_b)));
        assert!(tabulars.contains(&table.into()));
    }

    #[test]
    fn test_get_relevant_tabulars_to_authorize_load_tabular_empty_referenced_by_returns_only_base()
    {
        let namespace = NamespaceIdent::from_strs(vec!["ns"]).unwrap();
        let table_ident = TableIdent::new(namespace.clone(), "table".to_string());
        let table = TabularIdentBorrowed::Table(&table_ident);

        let tabulars = get_relevant_tabulars_to_authorize_load_tabular(table.clone(), None);

        assert_eq!(tabulars.len(), 1);
        assert!(tabulars.contains(&table.into()));
    }

    #[test]
    fn test_sort_tabulars_for_authorize_load_tabular_should_contain_table_when_only_table_is_given()
    {
        let warehouse_id = WarehouseId::new_random();
        let table = TableInfo::new_random(warehouse_id);

        let referenced_by = None;

        let tabulars: HashMap<TableIdent, ViewOrTableInfo> =
            vec![(table.tabular_ident.clone(), table.clone().into())]
                .into_iter()
                .collect();

        let sorted_tabulars = sort_tabulars_for_authorize_load_tabular(
            &tabulars,
            referenced_by,
            &table.tabular_ident,
        );

        assert_eq!(sorted_tabulars.len(), 1);
    }

    #[test]
    fn test_sort_tabulars_for_authorize_load_tabular_should_contain_referencing_views_in_order_before_tabular()
     {
        let warehouse_id = WarehouseId::new_random();
        let table = TableInfo::new_random(warehouse_id);

        let view_1 = ViewInfo::new_random(warehouse_id);
        let view_2 = ViewInfo::new_random(warehouse_id);

        let referenced_by = vec![
            ReferencingView::new(view_1.clone().tabular_ident),
            ReferencingView::new(view_2.clone().tabular_ident),
        ];

        let tabulars: HashMap<TableIdent, ViewOrTableInfo> = vec![
            (view_1.tabular_ident.clone(), view_1.clone().into()),
            (view_2.tabular_ident.clone(), view_2.clone().into()),
            (table.tabular_ident.clone(), table.clone().into()),
        ]
        .into_iter()
        .collect();

        let sorted_tabulars = sort_tabulars_for_authorize_load_tabular(
            &tabulars,
            Some(&referenced_by),
            &table.tabular_ident,
        );

        assert_eq!(sorted_tabulars.len(), 3);

        assert_eq!(sorted_tabulars[0], view_1.into());
        assert_eq!(sorted_tabulars[1], view_2.into());
        assert_eq!(sorted_tabulars[2], table.into());
    }

    #[test]
    fn test_add_namespace_to_tabulars_for_authorize_load_tabular_adds_namespace_when_given_single_table_and_correct_namespace()
     {
        let warehouse_id = WarehouseId::new_random();

        let table = TableInfo::new_random(warehouse_id);
        let tabulars = vec![table.clone().into()];

        let namespace = NamespaceHierarchy::new_with_id(warehouse_id, table.namespace_id);
        let mut namespaces = HashMap::new();
        namespaces.insert(namespace.namespace_id(), namespace.clone());

        let tabulars_with_namespaces = add_namespace_to_tabulars_for_authorize_load_tabular(
            warehouse_id,
            tabulars,
            &namespaces,
        )
        .unwrap();

        assert_eq!(tabulars_with_namespaces.len(), 1);
        assert_eq!(tabulars_with_namespaces[0], (table.into(), namespace));
    }

    #[test]
    fn test_add_namespace_to_tabulars_for_authorize_load_tabular_adds_namespace_when_given_single_table_and_multiple_namespaces()
     {
        let warehouse_id = WarehouseId::new_random();

        let table = TableInfo::new_random(warehouse_id);
        let tabulars = vec![table.clone().into()];

        let namespace_1 = NamespaceHierarchy::new_with_id(warehouse_id, NamespaceId::new_random());
        let namespace_2 = NamespaceHierarchy::new_with_id(warehouse_id, table.namespace_id);
        let mut namespaces = HashMap::new();
        namespaces.insert(namespace_1.namespace_id(), namespace_1);
        namespaces.insert(namespace_2.namespace_id(), namespace_2.clone());

        let tabulars_with_namespaces = add_namespace_to_tabulars_for_authorize_load_tabular(
            warehouse_id,
            tabulars,
            &namespaces,
        )
        .unwrap();

        assert_eq!(tabulars_with_namespaces.len(), 1);
        assert_eq!(tabulars_with_namespaces[0], (table.into(), namespace_2));
    }

    #[test]
    fn test_add_namespace_to_tabulars_for_authorize_load_tabular_adds_namespaces_when_given_multiple_tabulars_and_multiple_namespaces()
     {
        let warehouse_id = WarehouseId::new_random();

        let table = TableInfo::new_random(warehouse_id);
        let view = ViewInfo::new_random(warehouse_id);
        let tabulars = vec![view.clone().into(), table.clone().into()];

        let namespace_1 = NamespaceHierarchy::new_with_id(warehouse_id, table.namespace_id);
        let namespace_2 = NamespaceHierarchy::new_with_id(warehouse_id, view.namespace_id);
        let mut namespaces = HashMap::new();
        namespaces.insert(namespace_1.namespace_id(), namespace_1.clone());
        namespaces.insert(namespace_2.namespace_id(), namespace_2.clone());

        let tabulars_with_namespaces = add_namespace_to_tabulars_for_authorize_load_tabular(
            warehouse_id,
            tabulars,
            &namespaces,
        )
        .unwrap();

        assert_eq!(tabulars_with_namespaces.len(), 2);
        assert_eq!(tabulars_with_namespaces[0], (view.into(), namespace_2));
        assert_eq!(tabulars_with_namespaces[1], (table.into(), namespace_1));
    }

    #[test]
    fn test_resolve_users_for_authorize_load_tabular_returns_empty_list_if_no_tabular_given() {
        let actor = Actor::Principal(UserId::new_unchecked("test", "test"));

        let tabulars = resolve_users_for_authorize_load_tabular(
            &Vec::new(),
            &actor,
            &MatchedEngines::default(),
            None,
        )
        .unwrap();

        assert!(tabulars.is_empty());
    }

    #[test]
    fn test_resolve_users_for_authorize_load_tabular_adds_request_user_if_only_tabular_is_defined()
    {
        let warehouse_id = WarehouseId::new_random();

        let actor = Actor::Principal(UserId::new_unchecked("test", "test"));

        let table = TableInfo::new_random(warehouse_id);
        let namespace = NamespaceHierarchy::new_with_id(warehouse_id, table.namespace_id);
        let tabulars = vec![(table.clone().into(), namespace.clone())];

        let tabulars = resolve_users_for_authorize_load_tabular(
            &tabulars,
            &actor,
            &MatchedEngines::default(),
            None,
        )
        .unwrap();

        assert_eq!(tabulars[0].tabular, ViewOrTableInfo::from(table));
        assert_eq!(tabulars[0].user, actor.to_user_or_role());
        assert!(!tabulars[0].is_delegated_execution);
        assert_eq!(tabulars[0].namespace, namespace);
    }

    #[test]
    fn test_resolve_users_for_authorize_load_tabular_adds_request_user_if_all_views_are_invoker() {
        let warehouse_id = WarehouseId::new_random();

        let owner_property = "trino.run-as-owner".to_string();
        let engines = MatchedEngines::single(TrustedEngine::Trino(TrinoEngineConfig {
            owner_property: owner_property.clone(),
            identities: HashMap::new(),
        }));

        let actor = Actor::Principal(UserId::new_unchecked("test", "test"));

        let view_1 = ViewInfo::new_random(warehouse_id);
        let view_1_namespace = NamespaceHierarchy::new_with_id(warehouse_id, view_1.namespace_id);

        let view_2 = ViewInfo::new_random(warehouse_id);
        let view_2_namespace = NamespaceHierarchy::new_with_id(warehouse_id, view_2.namespace_id);

        let table = TableInfo::new_random(warehouse_id);
        let table_namespace = NamespaceHierarchy::new_with_id(warehouse_id, table.namespace_id);

        let tabulars = vec![
            (view_1.clone().into(), view_1_namespace.clone()),
            (view_2.clone().into(), view_2_namespace.clone()),
            (table.clone().into(), table_namespace.clone()),
        ];

        let tabulars =
            resolve_users_for_authorize_load_tabular(&tabulars, &actor, &engines, Some("test"))
                .unwrap();

        assert_eq!(tabulars.len(), 3);
        assert_eq!(tabulars[0].tabular, ViewOrTableInfo::from(view_1));
        assert_eq!(tabulars[0].user, actor.to_user_or_role());
        assert!(!tabulars[0].is_delegated_execution);
        assert_eq!(tabulars[0].namespace, view_1_namespace);

        assert_eq!(tabulars[1].tabular, ViewOrTableInfo::from(view_2));
        assert_eq!(tabulars[1].user, actor.to_user_or_role());
        assert!(!tabulars[1].is_delegated_execution);
        assert_eq!(tabulars[1].namespace, view_2_namespace);

        assert_eq!(tabulars[2].tabular, ViewOrTableInfo::from(table));
        assert_eq!(tabulars[2].user, actor.to_user_or_role());
        assert!(!tabulars[2].is_delegated_execution);
        assert_eq!(tabulars[2].namespace, table_namespace);
    }

    #[test]
    fn test_resolve_users_for_authorize_load_tabular_changes_to_view_owner_if_a_views_is_definer() {
        let warehouse_id = WarehouseId::new_random();

        let owner_property = "trino.run-as-owner".to_string();
        let engines = MatchedEngines::single(TrustedEngine::Trino(TrinoEngineConfig {
            owner_property: owner_property.clone(),
            identities: HashMap::new(),
        }));

        let actor_test_name = "test";
        let actor_test = Actor::Principal(UserId::new_unchecked("test", actor_test_name));

        let actor_trino_name = "trino";
        let actor_trino = Actor::Principal(UserId::new_unchecked("test", actor_trino_name));

        let actor_peter_name = "peter";
        let actor_peter = Actor::Principal(UserId::new_unchecked("test", actor_peter_name));

        let mut view_1 = ViewInfo::new_random(warehouse_id);
        view_1
            .properties
            .insert(owner_property.clone(), actor_trino_name.to_string());
        let view_1_namespace = NamespaceHierarchy::new_with_id(warehouse_id, view_1.namespace_id);

        let view_2 = ViewInfo::new_random(warehouse_id);
        let view_2_namespace = NamespaceHierarchy::new_with_id(warehouse_id, view_2.namespace_id);

        let mut view_3 = ViewInfo::new_random(warehouse_id);
        view_3
            .properties
            .insert(owner_property, actor_peter_name.to_string());
        let view_3_namespace = NamespaceHierarchy::new_with_id(warehouse_id, view_3.namespace_id);

        let view_4 = ViewInfo::new_random(warehouse_id);
        let view_4_namespace = NamespaceHierarchy::new_with_id(warehouse_id, view_4.namespace_id);

        let table = TableInfo::new_random(warehouse_id);
        let table_namespace = NamespaceHierarchy::new_with_id(warehouse_id, table.namespace_id);

        let tabulars = vec![
            (view_1.clone().into(), view_1_namespace.clone()),
            (view_2.clone().into(), view_2_namespace.clone()),
            (view_3.clone().into(), view_3_namespace.clone()),
            (view_4.clone().into(), view_4_namespace.clone()),
            (table.clone().into(), table_namespace.clone()),
        ];

        let tabulars = resolve_users_for_authorize_load_tabular(
            &tabulars,
            &actor_test,
            &engines,
            Some("test"),
        )
        .unwrap();

        assert_eq!(tabulars.len(), 5);
        assert_eq!(tabulars[0].tabular, ViewOrTableInfo::from(view_1));
        assert_eq!(tabulars[0].user, actor_test.to_user_or_role());
        assert!(!tabulars[0].is_delegated_execution);
        assert_eq!(tabulars[0].namespace, view_1_namespace);

        assert_eq!(tabulars[1].tabular, ViewOrTableInfo::from(view_2));
        assert_eq!(tabulars[1].user, actor_trino.to_user_or_role());
        assert!(tabulars[1].is_delegated_execution);
        assert_eq!(tabulars[1].namespace, view_2_namespace);

        assert_eq!(tabulars[2].tabular, ViewOrTableInfo::from(view_3));
        assert_eq!(tabulars[2].user, actor_trino.to_user_or_role());
        assert!(tabulars[2].is_delegated_execution);
        assert_eq!(tabulars[2].namespace, view_3_namespace);

        assert_eq!(tabulars[3].tabular, ViewOrTableInfo::from(view_4));
        assert_eq!(tabulars[3].user, actor_peter.to_user_or_role());
        assert!(tabulars[3].is_delegated_execution);
        assert_eq!(tabulars[3].namespace, view_4_namespace);

        assert_eq!(tabulars[4].tabular, ViewOrTableInfo::from(table));
        assert_eq!(tabulars[4].user, actor_peter.to_user_or_role());
        assert!(tabulars[4].is_delegated_execution);
        assert_eq!(tabulars[4].namespace, table_namespace);
    }

    #[test]
    fn test_resolve_users_for_authorize_load_tabular_returns_only_tabular_with_owner_if_no_trusted_engine_is_given()
     {
        let warehouse_id = WarehouseId::new_random();

        let owner_property = "trino.run-as-owner".to_string();
        let idp_id = "test";

        let actor_test_name = "test";
        let actor_test = Actor::Principal(UserId::new_unchecked(idp_id, actor_test_name));

        let actor_trino_name = "trino";

        let actor_peter_name = "peter";

        let mut view_1 = ViewInfo::new_random(warehouse_id);
        view_1
            .properties
            .insert(owner_property.clone(), actor_trino_name.to_string());
        let view_1_namespace = NamespaceHierarchy::new_with_id(warehouse_id, view_1.namespace_id);

        let view_2 = ViewInfo::new_random(warehouse_id);
        let view_2_namespace = NamespaceHierarchy::new_with_id(warehouse_id, view_2.namespace_id);

        let mut view_3 = ViewInfo::new_random(warehouse_id);
        view_3
            .properties
            .insert(owner_property, actor_peter_name.to_string());
        let view_3_namespace = NamespaceHierarchy::new_with_id(warehouse_id, view_3.namespace_id);

        let view_4 = ViewInfo::new_random(warehouse_id);
        let view_4_namespace = NamespaceHierarchy::new_with_id(warehouse_id, view_4.namespace_id);

        let table = TableInfo::new_random(warehouse_id);
        let table_namespace = NamespaceHierarchy::new_with_id(warehouse_id, table.namespace_id);

        let tabulars = vec![
            (view_1.clone().into(), view_1_namespace.clone()),
            (view_2.clone().into(), view_2_namespace.clone()),
            (view_3.clone().into(), view_3_namespace.clone()),
            (view_4.clone().into(), view_4_namespace.clone()),
            (table.clone().into(), table_namespace.clone()),
        ];

        let tabulars = resolve_users_for_authorize_load_tabular(
            &tabulars,
            &actor_test,
            &MatchedEngines::default(),
            None,
        )
        .unwrap();

        assert_eq!(tabulars.len(), 1);
        assert_eq!(tabulars[0].tabular, ViewOrTableInfo::from(table));
        assert_eq!(tabulars[0].user, actor_test.to_user_or_role());
        assert!(!tabulars[0].is_delegated_execution);
        assert_eq!(tabulars[0].namespace, table_namespace);
    }

    // ---- build_actions tests ----

    #[test]
    fn test_build_actions_single_table_produces_three_actions() {
        let warehouse_id = WarehouseId::new_random();
        let table = TableInfo::new_random(warehouse_id);
        let namespace = NamespaceHierarchy::new_with_id(warehouse_id, table.namespace_id);
        let actor = Actor::Principal(UserId::new_unchecked("test", "user"));

        let target = table.tabular_ident.clone();
        let tabulars = vec![ResolvedTabular {
            tabular: ViewOrTableInfo::Table(table),
            user: actor.to_user_or_role(),
            is_delegated_execution: false,
            namespace,
        }];

        let actions =
            build_actions_from_sorted_tabulars_for_authorize_load_tabular(&tabulars, &target);

        assert_eq!(actions.len(), 3);
        assert!(
            actions
                .iter()
                .all(|(_, a)| matches!(a, ActionOnTableOrView::Table(_)))
        );
    }

    #[test]
    fn test_build_actions_target_view_produces_only_get_metadata() {
        let warehouse_id = WarehouseId::new_random();
        let view = ViewInfo::new_random(warehouse_id);
        let namespace = NamespaceHierarchy::new_with_id(warehouse_id, view.namespace_id);
        let actor = Actor::Principal(UserId::new_unchecked("test", "user"));

        // A single view is the target. loadView only consults `GetMetadata`,
        // so `Select` must not be emitted — evaluating it would produce a
        // discarded authorization decision.
        let target = view.tabular_ident.clone();
        let tabulars = vec![ResolvedTabular {
            tabular: ViewOrTableInfo::View(view),
            user: actor.to_user_or_role(),
            is_delegated_execution: false,
            namespace,
        }];

        let actions =
            build_actions_from_sorted_tabulars_for_authorize_load_tabular(&tabulars, &target);

        let emitted: Vec<_> = actions
            .iter()
            .filter_map(|(_, a)| match a {
                ActionOnTableOrView::View(v) => Some(v.action.clone()),
                ActionOnTableOrView::Table(_) | ActionOnTableOrView::GenericTable(_) => None,
            })
            .collect();
        assert_eq!(emitted, vec![CatalogViewAction::GetMetadata]);
    }

    #[test]
    fn test_build_actions_intermediate_view_includes_select() {
        let warehouse_id = WarehouseId::new_random();
        let intermediate = ViewInfo::new_random(warehouse_id);
        let target = ViewInfo::new_random(warehouse_id);
        let intermediate_ns =
            NamespaceHierarchy::new_with_id(warehouse_id, intermediate.namespace_id);
        let target_ns = NamespaceHierarchy::new_with_id(warehouse_id, target.namespace_id);
        let actor = Actor::Principal(UserId::new_unchecked("test", "user"));

        // Chain of two views: the first is an intermediate referenced-by view,
        // the second is the target.
        let target_ident = target.tabular_ident.clone();
        let tabulars = vec![
            ResolvedTabular {
                tabular: ViewOrTableInfo::View(intermediate),
                user: actor.to_user_or_role(),
                is_delegated_execution: false,
                namespace: intermediate_ns,
            },
            ResolvedTabular {
                tabular: ViewOrTableInfo::View(target),
                user: actor.to_user_or_role(),
                is_delegated_execution: false,
                namespace: target_ns,
            },
        ];

        let actions =
            build_actions_from_sorted_tabulars_for_authorize_load_tabular(&tabulars, &target_ident);

        let emitted: Vec<_> = actions
            .iter()
            .filter_map(|(_, a)| match a {
                ActionOnTableOrView::View(v) => Some(v.action.clone()),
                ActionOnTableOrView::Table(_) | ActionOnTableOrView::GenericTable(_) => None,
            })
            .collect();
        // Intermediate view: GetMetadata + Select. Target view: GetMetadata only.
        assert_eq!(
            emitted,
            vec![
                CatalogViewAction::GetMetadata,
                CatalogViewAction::Select,
                CatalogViewAction::GetMetadata,
            ]
        );
    }

    #[test]
    fn test_build_actions_definer_view_has_delegated_flag() {
        let warehouse_id = WarehouseId::new_random();

        let mut view = ViewInfo::new_random(warehouse_id);
        view.properties
            .insert("trino.run-as-owner".to_string(), "alice".to_string());
        let namespace = NamespaceHierarchy::new_with_id(warehouse_id, view.namespace_id);
        let actor = Actor::Principal(UserId::new_unchecked("test", "user"));

        let target = view.tabular_ident.clone();
        let tabulars = vec![ResolvedTabular {
            tabular: ViewOrTableInfo::View(view),
            user: actor.to_user_or_role(),
            is_delegated_execution: true,
            namespace,
        }];

        let actions =
            build_actions_from_sorted_tabulars_for_authorize_load_tabular(&tabulars, &target);

        assert_eq!(actions.len(), 1);
        for (_, a) in &actions {
            match a {
                ActionOnTableOrView::View(v) => assert!(v.is_delegated_execution),
                ActionOnTableOrView::Table(_) | ActionOnTableOrView::GenericTable(_) => {
                    panic!("expected view action")
                }
            }
        }
    }

    #[test]
    fn test_build_actions_invoker_view_has_no_delegated_flag() {
        let warehouse_id = WarehouseId::new_random();

        let view = ViewInfo::new_random(warehouse_id);
        let namespace = NamespaceHierarchy::new_with_id(warehouse_id, view.namespace_id);
        let actor = Actor::Principal(UserId::new_unchecked("test", "user"));

        let target = view.tabular_ident.clone();
        let tabulars = vec![ResolvedTabular {
            tabular: ViewOrTableInfo::View(view),
            user: actor.to_user_or_role(),
            is_delegated_execution: false,
            namespace,
        }];

        let actions =
            build_actions_from_sorted_tabulars_for_authorize_load_tabular(&tabulars, &target);

        assert_eq!(actions.len(), 1);
        for (_, a) in &actions {
            match a {
                ActionOnTableOrView::View(v) => assert!(!v.is_delegated_execution),
                ActionOnTableOrView::Table(_) | ActionOnTableOrView::GenericTable(_) => {
                    panic!("expected view action")
                }
            }
        }
    }

    // ---- are_allowed_load_chain_actions tests ----

    const OWNER_PROPERTY: &str = "trino.run-as-owner";

    fn principal(name: &str) -> UserOrRole {
        UserOrRole::User(UserId::new_unchecked("test", name))
    }

    fn caller_metadata() -> RequestMetadata {
        RequestMetadata::test_user(UserId::new_unchecked("test", "caller"))
    }

    struct Chain {
        warehouse: ResolvedWarehouse,
        views: Vec<ViewInfo>,
        target: TableIdent,
        resolved: Vec<ResolvedTabular>,
    }

    impl Chain {
        fn view_key(&self, i: usize) -> String {
            format!(
                "view:{}/{}",
                self.warehouse.warehouse_id,
                self.views[i].view_id()
            )
        }

        fn target_key(&self) -> String {
            match &self.resolved.last().expect("chain has a target").tabular {
                ViewOrTableInfo::Table(t) => {
                    format!("table:{}/{}", self.warehouse.warehouse_id, t.table_id())
                }
                ViewOrTableInfo::View(v) => {
                    format!("view:{}/{}", self.warehouse.warehouse_id, v.view_id())
                }
                ViewOrTableInfo::GenericTable(_) => unreachable!("tests build no generic tables"),
            }
        }

        async fn decide(&self, authz: &HidingAuthorizer) -> Result<LoadChainDecisions, AuthZError> {
            let actions = build_actions_from_sorted_tabulars_for_authorize_load_tabular(
                &self.resolved,
                &self.target,
            );
            are_allowed_load_chain_actions(
                authz,
                &caller_metadata(),
                &self.warehouse,
                &HashMap::new(),
                &actions,
            )
            .await
        }

        fn segment_lengths(&self) -> Vec<usize> {
            let actions = build_actions_from_sorted_tabulars_for_authorize_load_tabular(
                &self.resolved,
                &self.target,
            );
            principal_segments(&actions).map(<[_]>::len).collect()
        }

        async fn load_table(
            &self,
            authz: &HidingAuthorizer,
        ) -> Result<(TableInfo, Option<StoragePermissions>), AuthZError> {
            let actions = build_actions_from_sorted_tabulars_for_authorize_load_tabular(
                &self.resolved,
                &self.target,
            );
            let results = are_allowed_load_chain_actions(
                authz,
                &caller_metadata(),
                &self.warehouse,
                &HashMap::new(),
                &actions,
            )
            .await?;
            interpret_authz_results_for_load_table(
                &actions,
                &results,
                self.warehouse.warehouse_id,
                &self.target,
            )
        }
    }

    /// `owners[i]` makes view `i` a DEFINER view owned by that principal; the
    /// chain ends in `target`.
    fn chain_to(owners: &[Option<&str>], target: ViewOrTableInfo) -> Chain {
        let warehouse_id = target.warehouse_id();
        let views: Vec<ViewInfo> = owners
            .iter()
            .map(|owner| {
                let mut view = ViewInfo::new_random(warehouse_id);
                if let Some(owner) = owner {
                    view.properties
                        .insert(OWNER_PROPERTY.to_string(), (*owner).to_string());
                }
                view
            })
            .collect();
        let target_ident = target.tabular_ident().clone();
        let sorted: Vec<(ViewOrTableInfo, NamespaceHierarchy)> = views
            .iter()
            .cloned()
            .map(ViewOrTableInfo::from)
            .chain(std::iter::once(target))
            .map(|tabular| {
                let namespace =
                    NamespaceHierarchy::new_with_id(warehouse_id, tabular.namespace_id());
                (tabular, namespace)
            })
            .collect();
        let engines = MatchedEngines::single(TrustedEngine::Trino(TrinoEngineConfig {
            owner_property: OWNER_PROPERTY.to_string(),
            identities: HashMap::new(),
        }));
        let resolved = resolve_users_for_authorize_load_tabular(
            &sorted,
            caller_metadata().actor(),
            &engines,
            Some("test"),
        )
        .unwrap();
        Chain {
            warehouse: ResolvedWarehouse::new_with_id(warehouse_id),
            views,
            target: target_ident,
            resolved,
        }
    }

    fn chain(owners: &[Option<&str>]) -> (Chain, TableInfo) {
        let table = TableInfo::new_random(WarehouseId::new_random());
        (chain_to(owners, table.clone().into()), table)
    }

    fn check(object: &str, action: &str, user: Option<&str>) -> RecordedTabularCheck {
        RecordedTabularCheck {
            object: object.to_string(),
            action: action.to_string(),
            user: user.map(principal),
            is_delegated_execution: user.is_some(),
        }
    }

    fn view_checks(object: &str, user: Option<&str>) -> Vec<RecordedTabularCheck> {
        vec![
            check(object, "GetMetadata", user),
            check(object, "Select", user),
        ]
    }

    fn table_checks(object: &str, user: Option<&str>) -> Vec<RecordedTabularCheck> {
        vec![
            check(object, "GetMetadata", user),
            check(object, "ReadData", user),
            check(object, "WriteData", user),
        ]
    }

    #[test]
    fn test_principal_segments_invoker_chain_is_one_segment() {
        let (chain, _) = chain(&[None, None]);
        let actions = build_actions_from_sorted_tabulars_for_authorize_load_tabular(
            &chain.resolved,
            &chain.target,
        );
        let lengths: Vec<usize> = principal_segments(&actions).map(<[_]>::len).collect();
        assert_eq!(lengths, vec![7]);
    }

    /// view1 is decided as the caller, view2 and view3 as `owner_b`, the table as `owner_c`.
    #[test]
    fn test_principal_segments_split_at_each_definer_switch() {
        let (chain, _) = chain(&[Some("owner_b"), None, Some("owner_c")]);
        let actions = build_actions_from_sorted_tabulars_for_authorize_load_tabular(
            &chain.resolved,
            &chain.target,
        );
        let lengths: Vec<usize> = principal_segments(&actions).map(<[_]>::len).collect();
        assert_eq!(lengths, vec![2, 4, 3]);
    }

    /// Two DEFINER views of the same owner hand over to one principal: no extra call.
    #[test]
    fn test_principal_segments_same_owner_twice_is_one_segment() {
        let (chain, _) = chain(&[Some("owner_b"), Some("owner_b")]);
        let actions = build_actions_from_sorted_tabulars_for_authorize_load_tabular(
            &chain.resolved,
            &chain.target,
        );
        let lengths: Vec<usize> = principal_segments(&actions).map(<[_]>::len).collect();
        assert_eq!(lengths, vec![2, 5]);
    }

    #[tokio::test]
    async fn test_refused_caller_never_sends_owner_checks() {
        let (chain, _) = chain(&[Some("owner_b")]);
        let authz = HidingAuthorizer::new();
        authz.hide_for_user(&principal("caller"), &chain.view_key(0));

        let decisions = chain.decide(&authz).await.unwrap();
        assert_eq!(decisions.allowed, vec![false, false, false, false, false]);
        assert_eq!(decisions.decided, 2);
        let err = chain.load_table(&authz).await.unwrap_err();
        let AuthZError::AuthZCannotSeeView(err) = err else {
            panic!("expected AuthZCannotSeeView, got {err:?}");
        };
        assert_eq!(
            err,
            AuthZCannotSeeView::new_forbidden(
                chain.warehouse.warehouse_id,
                chain.views[0].tabular_ident.clone()
            )
            .with_delegated_execution(false)
        );
        // Both runs sent the caller's view checks and nothing for owner_b.
        let caller_batch = view_checks(&chain.view_key(0), None);
        assert_eq!(
            authz.tabular_checks(),
            vec![caller_batch.clone(), caller_batch]
        );
    }

    #[tokio::test]
    async fn test_refused_owner_is_decided_after_the_caller() {
        let (chain, table) = chain(&[Some("owner_b")]);
        let authz = HidingAuthorizer::new();
        authz.hide_for_user(&principal("owner_b"), &chain.target_key());

        let err = chain.load_table(&authz).await.unwrap_err();
        let AuthZError::AuthZCannotSeeTable(err) = err else {
            panic!("expected AuthZCannotSeeTable, got {err:?}");
        };
        assert_eq!(
            err,
            AuthZCannotSeeTable::new_forbidden(chain.warehouse.warehouse_id, table.tabular_ident)
                .with_delegated_execution(true)
        );
        assert_eq!(
            authz.tabular_checks(),
            vec![
                view_checks(&chain.view_key(0), None),
                table_checks(&chain.target_key(), Some("owner_b")),
            ]
        );
    }

    #[tokio::test]
    async fn test_all_allowed_returns_table_and_storage_permissions() {
        let (chain, table) = chain(&[Some("owner_b"), None, Some("owner_c")]);
        let authz = HidingAuthorizer::new();

        let (info, permissions) = chain.load_table(&authz).await.unwrap();
        assert_eq!(info.tabular_id, table.tabular_id);
        assert_eq!(permissions, Some(StoragePermissions::ReadWriteDelete));
        assert_eq!(
            authz.tabular_checks(),
            vec![
                view_checks(&chain.view_key(0), None),
                [
                    view_checks(&chain.view_key(1), Some("owner_b")),
                    view_checks(&chain.view_key(2), Some("owner_b")),
                ]
                .concat(),
                table_checks(&chain.target_key(), Some("owner_c")),
            ]
        );
    }

    /// The owner's `ReadData`/`WriteData` only shape storage permissions.
    #[tokio::test]
    async fn test_owner_read_only_narrows_storage_permissions() {
        let (chain, table) = chain(&[Some("owner_b")]);
        let authz = HidingAuthorizer::new();
        authz.block_action(&format!("table:{:?}", CatalogTableAction::WriteData));

        let (info, permissions) = chain.load_table(&authz).await.unwrap();
        assert_eq!(info.tabular_id, table.tabular_id);
        assert_eq!(permissions, Some(StoragePermissions::Read));
    }

    /// One call asks about tables before views; a split chain would ask the
    /// caller's view first.
    #[tokio::test]
    async fn test_invoker_chain_is_decided_in_one_call() {
        let (chain, table) = chain(&[None]);
        let authz = HidingAuthorizer::new();

        let (info, permissions) = chain.load_table(&authz).await.unwrap();
        assert_eq!(info.tabular_id, table.tabular_id);
        assert_eq!(permissions, Some(StoragePermissions::ReadWriteDelete));
        assert_eq!(
            authz.tabular_checks(),
            vec![
                table_checks(&chain.target_key(), None),
                view_checks(&chain.view_key(0), None),
            ]
        );
    }

    #[tokio::test]
    async fn test_failing_owner_lookup_is_not_reached_when_caller_refused() {
        let (chain, _) = chain(&[Some("owner_b")]);
        let authz = HidingAuthorizer::new();
        authz.fail_tabular_checks_for_user(&principal("owner_b"));
        authz.hide_for_user(&principal("caller"), &chain.view_key(0));

        let err = chain.load_table(&authz).await.unwrap_err();
        let model = err.into_error_model();
        let expected = AuthZError::from(
            AuthZCannotSeeView::new_forbidden(
                chain.warehouse.warehouse_id,
                chain.views[0].tabular_ident.clone(),
            )
            .with_delegated_execution(false),
        )
        .into_error_model();
        assert_eq!(model.code, StatusCode::NOT_FOUND.as_u16());
        assert_eq!(model.code, expected.code);
        assert_eq!(model.r#type, expected.r#type);
        assert_eq!(model.message, expected.message);
        assert_eq!(
            authz.tabular_checks(),
            vec![view_checks(&chain.view_key(0), None)]
        );
    }

    /// Counterpart: once the caller may traverse, the owner's failing lookup surfaces.
    #[tokio::test]
    async fn test_failing_owner_lookup_surfaces_when_caller_allowed() {
        let (chain, _) = chain(&[Some("owner_b")]);
        let authz = HidingAuthorizer::new();
        authz.fail_tabular_checks_for_user(&principal("owner_b"));

        let model = chain
            .load_table(&authz)
            .await
            .unwrap_err()
            .into_error_model();
        assert_injected_backend_failure(&model);
        assert_eq!(
            authz.tabular_checks(),
            vec![
                view_checks(&chain.view_key(0), None),
                table_checks(&chain.target_key(), Some("owner_b")),
            ]
        );
    }

    fn assert_injected_backend_failure(model: &ErrorModel) {
        assert_eq!(model.code, StatusCode::SERVICE_UNAVAILABLE.as_u16());
        assert_eq!(model.r#type, "AuthorizationBackendError");
        assert_eq!(model.message, "Authorization service is unavailable");
        assert_eq!(
            model.source.as_ref().map(ToString::to_string).as_deref(),
            Some("Tabular checks for this principal fail in the test authorizer")
        );
    }

    /// `owner_b` refuses the second DEFINER view, so `owner_c`, who would decide
    /// the table, is never asked.
    #[tokio::test]
    async fn test_refused_later_segment_ends_the_chain() {
        let (chain, _) = chain(&[Some("owner_b"), Some("owner_c")]);
        let authz = HidingAuthorizer::new();
        authz.hide_for_user(&principal("owner_b"), &chain.view_key(1));

        let decisions = chain.decide(&authz).await.unwrap();
        assert_eq!(
            decisions.allowed,
            vec![true, true, false, false, false, false, false]
        );
        assert_eq!(decisions.decided, 4);
        let err = chain.load_table(&authz).await.unwrap_err();
        let AuthZError::AuthZCannotSeeView(err) = err else {
            panic!("expected AuthZCannotSeeView, got {err:?}");
        };
        assert_eq!(
            err,
            AuthZCannotSeeView::new_forbidden(
                chain.warehouse.warehouse_id,
                chain.views[1].tabular_ident.clone()
            )
            .with_delegated_execution(true)
        );
        // Both runs sent the caller's and owner_b's view checks, nothing for owner_c.
        let batches = vec![
            view_checks(&chain.view_key(0), None),
            view_checks(&chain.view_key(1), Some("owner_b")),
        ];
        assert_eq!(authz.tabular_checks(), [batches.clone(), batches].concat());
    }

    /// Counterpart of [`test_refused_later_segment_ends_the_chain`]: a lookup of
    /// `owner_c` that would fail is never reached, so the response is the refusal.
    #[tokio::test]
    async fn test_failing_lookup_after_a_refused_later_segment_is_not_reached() {
        let (chain, _) = chain(&[Some("owner_b"), Some("owner_c")]);
        let authz = HidingAuthorizer::new();
        authz.fail_tabular_checks_for_user(&principal("owner_c"));
        authz.hide_for_user(&principal("owner_b"), &chain.view_key(1));

        let model = chain
            .load_table(&authz)
            .await
            .unwrap_err()
            .into_error_model();
        let expected = AuthZError::from(
            AuthZCannotSeeView::new_forbidden(
                chain.warehouse.warehouse_id,
                chain.views[1].tabular_ident.clone(),
            )
            .with_delegated_execution(true),
        )
        .into_error_model();
        assert_eq!(model.code, StatusCode::NOT_FOUND.as_u16());
        assert_eq!(model.r#type, expected.r#type);
        assert_eq!(model.message, expected.message);
        assert_eq!(model.stack, expected.stack);
        assert_eq!(
            authz.tabular_checks(),
            vec![
                view_checks(&chain.view_key(0), None),
                view_checks(&chain.view_key(1), Some("owner_b")),
            ]
        );
    }

    /// A DEFINER view owned by the caller hands over to the caller in delegated
    /// execution, which is decided in a call of its own.
    #[tokio::test]
    async fn test_caller_owned_definer_view_starts_a_segment() {
        let (chain, table) = chain(&[Some("caller")]);
        assert_eq!(chain.segment_lengths(), vec![2, 3]);

        let authz = HidingAuthorizer::new();
        let (info, permissions) = chain.load_table(&authz).await.unwrap();
        assert_eq!(info.tabular_id, table.tabular_id);
        assert_eq!(permissions, Some(StoragePermissions::ReadWriteDelete));
        // The authorizer asks about the caller as itself, still in delegated execution.
        let delegated_to_caller = |action: &str| RecordedTabularCheck {
            object: chain.target_key(),
            action: action.to_string(),
            user: None,
            is_delegated_execution: true,
        };
        assert_eq!(
            authz.tabular_checks(),
            vec![
                view_checks(&chain.view_key(0), None),
                vec![
                    delegated_to_caller("GetMetadata"),
                    delegated_to_caller("ReadData"),
                    delegated_to_caller("WriteData"),
                ],
            ]
        );
    }

    /// An owner that comes back after another owner is decided once per run.
    #[tokio::test]
    async fn test_non_contiguous_owner_is_decided_per_segment() {
        let (chain, table) = chain(&[Some("owner_b"), Some("owner_c"), Some("owner_b")]);
        assert_eq!(chain.segment_lengths(), vec![2, 2, 2, 3]);

        let authz = HidingAuthorizer::new();
        let (info, permissions) = chain.load_table(&authz).await.unwrap();
        assert_eq!(info.tabular_id, table.tabular_id);
        assert_eq!(permissions, Some(StoragePermissions::ReadWriteDelete));
        assert_eq!(
            authz.tabular_checks(),
            vec![
                view_checks(&chain.view_key(0), None),
                view_checks(&chain.view_key(1), Some("owner_b")),
                view_checks(&chain.view_key(2), Some("owner_c")),
                table_checks(&chain.target_key(), Some("owner_b")),
            ]
        );
    }

    /// Reading an entry the authorizer never decided is a consumer bug.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "which the authorizer never decided")]
    fn test_reading_an_undecided_entry_panics_in_debug_builds() {
        let decisions = LoadChainDecisions {
            allowed: vec![false, false],
            decided: 1,
        };
        let _ = decisions.iter().count();
    }
}
