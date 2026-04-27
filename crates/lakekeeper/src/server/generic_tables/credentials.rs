use std::{collections::HashMap, sync::Arc};

use iceberg_ext::catalog::rest::StorageCredential;

use crate::{
    WarehouseId,
    api::{
        ApiContext,
        iceberg::{
            types::ReferencingView,
            v1::{DataAccess, Result},
        },
        v1::generic_tables::{
            GenericTableParameters, LoadGenericTableCredentialsRequest,
            LoadGenericTableCredentialsResponse,
        },
    },
    request_metadata::RequestMetadata,
    server::{
        maybe_get_secret, require_warehouse_id,
        tables::authorize_load::{
            AuthorizeLoadTabularObjects, TabularAuthzAction,
            add_namespace_to_tabulars_for_authorize_load_tabular,
            build_actions_from_sorted_tabulars_for_authorize_load_tabular,
            check_required_namespaces, check_required_tabulars, effective_referenced_by,
            get_relevant_namespaces_to_authorize_load_tabular,
            get_relevant_tabulars_to_authorize_load_tabular,
            load_objects_to_authorize_load_tabular, resolve_users_for_authorize_load_tabular,
            sort_tabulars_for_authorize_load_tabular,
        },
    },
    service::{
        CatalogStore, GenericTabularInfo, NamespaceHierarchy, ResolvedWarehouse, SecretStore,
        State, TabularIdentBorrowed, TabularListFlags,
        authz::{
            ActionOnTableOrView, AuthZCannotSeeGenericTable, AuthZCannotSeeView, AuthZError,
            AuthZTableOps, AuthorizationCountMismatch, Authorizer, AuthzWarehouseOps,
            BackendUnavailableOrCountMismatch, CatalogGenericTableAction,
        },
        build_namespace_hierarchy,
        events::{APIEventContext, context::ResolvedNamespace},
        storage::StoragePermissions,
    },
};

pub(super) async fn load_generic_table_credentials<
    C: CatalogStore,
    A: Authorizer + Clone,
    S: SecretStore,
>(
    parameters: GenericTableParameters,
    request: LoadGenericTableCredentialsRequest,
    data_access: DataAccess,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<LoadGenericTableCredentialsResponse> {
    let LoadGenericTableCredentialsRequest { referenced_by } = request;

    let GenericTableParameters {
        prefix,
        namespace,
        table_name,
    } = parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;

    let table_ident = iceberg::TableIdent::new(namespace.clone(), table_name.clone());

    let event_ctx = APIEventContext::for_generic_table(
        Arc::new(request_metadata.clone()),
        state.v1_state.events.clone(),
        warehouse_id,
        table_ident.clone(),
        CatalogGenericTableAction::ReadData,
    );

    let authz_result = match authorize_load_generic_table::<C, A>(
        event_ctx.request_metadata(),
        table_ident.clone(),
        warehouse_id,
        TabularListFlags::active(),
        state.v1_state.authz.clone(),
        state.v1_state.catalog.clone(),
        referenced_by.as_deref(),
    )
    .await
    {
        Err(e) => Err(e),
        Ok((_, _, _, None)) => {
            Err(AuthZCannotSeeGenericTable::new_forbidden(warehouse_id, table_ident.clone()).into())
        }
        Ok((a, b, c, Some(d))) => Ok((a, b, c, d)),
    };

    let (event_ctx, (warehouse, ns_hierarchy, gt_info, storage_permissions)) =
        event_ctx.emit_authz(authz_result)?;

    let _event_ctx = event_ctx.resolve(ResolvedNamespace {
        warehouse: warehouse.clone(),
        namespace: ns_hierarchy.namespace.clone(),
    });

    let storage_secret =
        maybe_get_secret(warehouse.storage_secret_id, &state.v1_state.secrets).await?;
    let storage_secret_ref = storage_secret.as_deref();

    let storage_config = warehouse
        .storage_profile
        .generate_table_config(
            data_access.into(),
            storage_secret_ref,
            &gt_info.location,
            storage_permissions,
            &request_metadata,
            &gt_info,
        )
        .await?;

    let storage_credentials = if storage_config.creds.inner().is_empty() {
        vec![]
    } else {
        vec![StorageCredential {
            prefix: gt_info.location.to_string(),
            config: storage_config.creds.into(),
        }]
    };

    Ok(LoadGenericTableCredentialsResponse {
        storage_credentials,
    })
}

async fn authorize_load_generic_table<C: CatalogStore, A: Authorizer + Clone>(
    request_metadata: &RequestMetadata,
    table: iceberg::TableIdent,
    warehouse_id: WarehouseId,
    list_flags: TabularListFlags,
    authorizer: A,
    state: C::State,
    referenced_by: Option<&[ReferencingView]>,
) -> Result<
    (
        Arc<ResolvedWarehouse>,
        NamespaceHierarchy,
        GenericTabularInfo,
        Option<StoragePermissions>,
    ),
    AuthZError,
> {
    let engines = request_metadata.engines();
    let referenced_by = effective_referenced_by(referenced_by, engines);

    // 1. Collect all relevant namespace idents
    let user_provided_namespaces = get_relevant_namespaces_to_authorize_load_tabular(
        &TabularIdentBorrowed::GenericTable(&table),
        referenced_by,
    );

    // 2. Collect all relevant tabular idents
    let user_provided_tabulars = get_relevant_tabulars_to_authorize_load_tabular(
        TabularIdentBorrowed::GenericTable(&table),
        referenced_by,
    );

    // 3. Load objects concurrently
    let AuthorizeLoadTabularObjects {
        warehouse,
        namespaces,
        tabulars,
    } = load_objects_to_authorize_load_tabular::<C>(
        warehouse_id,
        user_provided_namespaces.clone().into_iter().collect(),
        user_provided_tabulars.clone().into_iter().collect(),
        list_flags,
        state.clone(),
    )
    .await;

    // 4. Check objects presence
    let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;
    let tabulars =
        check_required_tabulars(warehouse_id, user_provided_tabulars, tabulars, &authorizer)?;
    let namespaces =
        check_required_namespaces(warehouse_id, &user_provided_namespaces, namespaces)?;

    // 5. Build NamespaceHierarchy
    let namespaces_with_hierarchy = namespaces
        .iter()
        .map(|(namespace_id, namespace)| {
            (
                *namespace_id,
                build_namespace_hierarchy(namespace, &namespaces),
            )
        })
        .collect::<HashMap<_, _>>();

    // 6. Sort tabulars
    let sorted_tabulars =
        sort_tabulars_for_authorize_load_tabular(&tabulars, referenced_by, &table);

    // 7. Connect tabular with namespaces
    let sorted_tabulars = add_namespace_to_tabulars_for_authorize_load_tabular(
        warehouse_id,
        sorted_tabulars,
        &namespaces_with_hierarchy,
    )?;

    // 8. Resolve owners and assign current user for each tabular in the chain.
    let token_idp_id = request_metadata
        .authentication()
        .and_then(|a| a.subject().idp_id())
        .map(String::as_str);
    let sorted_tabulars_with_full_info = resolve_users_for_authorize_load_tabular(
        &sorted_tabulars,
        request_metadata.actor(),
        engines,
        token_idp_id,
    )?;

    // 9. Build actions and authorize in batch.
    let actions = build_actions_from_sorted_tabulars_for_authorize_load_tabular(
        &sorted_tabulars_with_full_info,
    );
    let authz_results = authorizer
        .are_allowed_tabular_actions_vec(request_metadata, &warehouse, &namespaces, &actions)
        .await?
        .into_inner();

    // 10. Interpret results.
    let target_ns = sorted_tabulars_with_full_info
        .last()
        .map(|r| r.namespace.clone())
        .ok_or_else(|| AuthZCannotSeeGenericTable::new_not_found(warehouse_id, table.clone()))?;
    let (gt_info, storage_permissions) = interpret_authz_results_for_load_generic_table(
        &actions,
        &authz_results,
        warehouse_id,
        &table,
    )?;

    Ok((warehouse, target_ns, gt_info, storage_permissions))
}

fn interpret_authz_results_for_load_generic_table(
    actions: &[TabularAuthzAction<'_>],
    authz_results: &[bool],
    warehouse_id: WarehouseId,
    table: &iceberg::TableIdent,
) -> Result<(GenericTabularInfo, Option<StoragePermissions>), AuthZError> {
    if actions.len() != authz_results.len() {
        return Err(
            BackendUnavailableOrCountMismatch::from(AuthorizationCountMismatch::new(
                actions.len(),
                authz_results.len(),
                "load_generic_table",
            ))
            .into(),
        );
    }

    let mut gt_info: Option<GenericTabularInfo> = None;
    let mut gt_is_delegated = false;
    let mut can_get_metadata = None;
    let mut can_read = None;
    let mut can_write = None;

    for ((_ns, action), &allowed) in actions.iter().zip(authz_results) {
        match action {
            ActionOnTableOrView::GenericTable(gt_action) => {
                if let Some(existing) = &gt_info {
                    if existing.tabular_id != gt_action.info.tabular_id {
                        return Err(BackendUnavailableOrCountMismatch::from(
                            AuthorizationCountMismatch::new(1, 2, "generic_tables_in_chain"),
                        )
                        .into());
                    }
                } else {
                    gt_info = Some(gt_action.info.clone());
                    gt_is_delegated = gt_action.is_delegated_execution;
                }
                match &gt_action.action {
                    CatalogGenericTableAction::GetMetadata => can_get_metadata = Some(allowed),
                    CatalogGenericTableAction::ReadData => can_read = Some(allowed),
                    CatalogGenericTableAction::WriteData => can_write = Some(allowed),
                    _ => {}
                }
            }
            ActionOnTableOrView::View(view_action) => {
                if !allowed {
                    return Err(AuthZCannotSeeView::new_forbidden(
                        warehouse_id,
                        view_action.info.tabular_ident.clone(),
                    )
                    .with_delegated_execution(view_action.is_delegated_execution)
                    .into());
                }
            }
            ActionOnTableOrView::Table(_) => {
                // Unreachable: target is a generic table; chain intermediates are views only.
                debug_assert!(
                    false,
                    "Table action in load_generic_table authorization chain"
                );
            }
        }
    }

    let gt_info = gt_info
        .ok_or_else(|| AuthZCannotSeeGenericTable::new_not_found(warehouse_id, table.clone()))?;

    if !can_get_metadata.unwrap_or(false) {
        return Err(
            AuthZCannotSeeGenericTable::new_forbidden(warehouse_id, table.clone())
                .with_delegated_execution(gt_is_delegated)
                .into(),
        );
    }

    let storage_permissions = if can_write.unwrap_or(false) {
        Some(StoragePermissions::ReadWriteDelete)
    } else if can_read.unwrap_or(false) {
        Some(StoragePermissions::Read)
    } else {
        None
    };

    Ok((gt_info, storage_permissions))
}
