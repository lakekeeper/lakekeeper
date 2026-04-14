use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use iceberg::{NamespaceIdent, TableIdent};

use crate::{
    WarehouseId,
    api::iceberg::types::ReferencingView,
    config::{MatchedEngines, SecurityModel},
    service::{
        Actor, AuthZTabularInfo as _, CatalogBackendError, CatalogGetNamespaceError,
        CatalogGetWarehouseByIdError, CatalogNamespaceOps, CatalogStore, CatalogTabularOps,
        CatalogWarehouseOps, GetTabularInfoError, NamespaceHierarchy, NamespaceId,
        NamespaceWithParent, ResolveTasksError, ResolvedWarehouse, TabularIdentBorrowed,
        TabularIdentOwned, TabularInfo, TabularListFlags, UserId, ViewInfo, ViewOrTableInfo,
        authz::{
            ActionOnTable, ActionOnTableOrView, ActionOnView, AuthZCannotSeeNamespace, AuthZError,
            AuthZTableOps, AuthZViewOps, Authorizer, AuthzBadRequest, CatalogTableAction,
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
    >,
);

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
pub(crate) struct ResolvedTabular {
    pub(crate) tabular: ViewOrTableInfo,
    pub(crate) user: Option<UserOrRole>,
    /// True if this tabular is accessed via delegated execution (downstream of a DEFINER view).
    pub(crate) is_delegated_execution: bool,
    pub(crate) namespace: NamespaceHierarchy,
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
        // Only views have a security model. Tables can only appear as the
        // last entry (the target) because all referenced-by entries are looked
        // up as TabularIdentBorrowed::View and the DB filters by type.
        if matches!(tabular, ViewOrTableInfo::Table(_)) {
            debug_assert!(
                tabulars
                    .last()
                    .is_some_and(|(t, _)| std::ptr::eq(t, tabular)),
                "Table appeared as intermediate entry in authorization chain"
            );
            continue;
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

pub(crate) fn build_actions_from_sorted_tabulars_for_authorize_load_tabular(
    tabulars: &[ResolvedTabular],
) -> Vec<TabularAuthzAction<'_>> {
    tabulars
        .iter()
        .flat_map(|resolved| {
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
                ViewOrTableInfo::View(info) => vec![(
                    &namespace.namespace,
                    ActionOnTableOrView::View(ActionOnView {
                        info,
                        action: CatalogViewAction::GetMetadata,
                        user,
                        is_delegated_execution,
                    }),
                )],
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use iceberg::{NamespaceIdent, TableIdent};

    use super::*;
    use crate::{
        WarehouseId,
        config::{MatchedEngines, TrinoEngineConfig, TrustedEngine},
        service::TableInfo,
    };

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

        let tabulars = vec![ResolvedTabular {
            tabular: ViewOrTableInfo::Table(table),
            user: actor.to_user_or_role(),
            is_delegated_execution: false,
            namespace,
        }];

        let actions = build_actions_from_sorted_tabulars_for_authorize_load_tabular(&tabulars);

        assert_eq!(actions.len(), 3);
        assert!(
            actions
                .iter()
                .all(|(_, a)| matches!(a, ActionOnTableOrView::Table(_)))
        );
    }

    #[test]
    fn test_build_actions_single_view_produces_one_action() {
        let warehouse_id = WarehouseId::new_random();
        let view = ViewInfo::new_random(warehouse_id);
        let namespace = NamespaceHierarchy::new_with_id(warehouse_id, view.namespace_id);
        let actor = Actor::Principal(UserId::new_unchecked("test", "user"));

        let tabulars = vec![ResolvedTabular {
            tabular: ViewOrTableInfo::View(view),
            user: actor.to_user_or_role(),
            is_delegated_execution: false,
            namespace,
        }];

        let actions = build_actions_from_sorted_tabulars_for_authorize_load_tabular(&tabulars);

        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0].1, ActionOnTableOrView::View(_)));
    }

    #[test]
    fn test_build_actions_definer_view_has_delegated_flag() {
        let warehouse_id = WarehouseId::new_random();

        let mut view = ViewInfo::new_random(warehouse_id);
        view.properties
            .insert("trino.run-as-owner".to_string(), "alice".to_string());
        let namespace = NamespaceHierarchy::new_with_id(warehouse_id, view.namespace_id);
        let actor = Actor::Principal(UserId::new_unchecked("test", "user"));

        let tabulars = vec![ResolvedTabular {
            tabular: ViewOrTableInfo::View(view),
            user: actor.to_user_or_role(),
            is_delegated_execution: true,
            namespace,
        }];

        let actions = build_actions_from_sorted_tabulars_for_authorize_load_tabular(&tabulars);

        assert_eq!(actions.len(), 1);
        match &actions[0].1 {
            ActionOnTableOrView::View(v) => assert!(v.is_delegated_execution),
            ActionOnTableOrView::Table(_) => panic!("expected view action"),
        }
    }

    #[test]
    fn test_build_actions_invoker_view_has_no_delegated_flag() {
        let warehouse_id = WarehouseId::new_random();

        let view = ViewInfo::new_random(warehouse_id);
        let namespace = NamespaceHierarchy::new_with_id(warehouse_id, view.namespace_id);
        let actor = Actor::Principal(UserId::new_unchecked("test", "user"));

        let tabulars = vec![ResolvedTabular {
            tabular: ViewOrTableInfo::View(view),
            user: actor.to_user_or_role(),
            is_delegated_execution: false,
            namespace,
        }];

        let actions = build_actions_from_sorted_tabulars_for_authorize_load_tabular(&tabulars);

        assert_eq!(actions.len(), 1);
        match &actions[0].1 {
            ActionOnTableOrView::View(v) => assert!(!v.is_delegated_execution),
            ActionOnTableOrView::Table(_) => panic!("expected view action"),
        }
    }
}
