use std::{collections::HashMap, str::FromStr as _, sync::Arc};

use iceberg::TableIdent;
use iceberg_ext::catalog::rest::LoadViewResult;
use lakekeeper_io::Location;

use crate::{
    WarehouseId,
    api::{
        ApiContext,
        iceberg::v1::{ViewParameters, views::LoadViewRequest},
    },
    request_metadata::RequestMetadata,
    server::{
        require_warehouse_id,
        tables::{
            add_namespace_to_tabulars_for_authorize_load_tabular,
            build_actions_from_sorted_tabulars_for_authorize_load_tabular,
            check_required_namespaces, check_required_tabulars, effective_referenced_by,
            get_relevant_namespaces_to_authorize_load_tabular,
            get_relevant_tabulars_to_authorize_load_tabular,
            load_objects_to_authorize_load_tabular, resolve_users_for_authorize_load_tabular,
            sort_tabulars_for_authorize_load_tabular, validate_table_or_view_ident,
        },
    },
    service::{
        AuthZViewInfo as _, CatalogStore, CatalogViewOps, InternalParseLocationError,
        ResolvedWarehouse, Result, SecretStore, State, TabularIdentBorrowed, TabularListFlags,
        Transaction, ViewInfo,
        authz::{
            ActionOnTableOrView, AuthZCannotSeeView, AuthZError, AuthZTableOps,
            AuthorizationCountMismatch, Authorizer, AuthzWarehouseOps,
            BackendUnavailableOrCountMismatch, CatalogViewAction,
        },
        build_namespace_hierarchy,
        events::{APIEventContext, context::ResolvedView},
        storage::StoragePermissions,
    },
};

pub(crate) async fn load_view<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: ViewParameters,
    request: LoadViewRequest,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<LoadViewResult> {
    let data_access = request.data_access;
    // ------------------- VALIDATIONS -------------------
    let ViewParameters { prefix, view } = parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    match validate_table_or_view_ident(&view) {
        Ok(()) => {}
        Err(e) => {
            if e.error.r#type != *"NamespaceDepthExceeded" {
                return Err(e);
            }
        }
    }

    // ------------------- AUTHZ -------------------
    let authorizer = state.v1_state.authz;
    let catalog_state = state.v1_state.catalog;

    let event_ctx = APIEventContext::for_view(
        Arc::new(request_metadata.clone()),
        state.v1_state.events,
        warehouse_id,
        view.clone(),
        CatalogViewAction::GetMetadata,
    );

    let authz_result = authorize_load_view::<C, _>(
        &request_metadata,
        warehouse_id,
        &view,
        &authorizer,
        catalog_state.clone(),
        request.referenced_by.as_deref(),
    )
    .await;
    let (event_ctx, (warehouse, view_info, storage_permissions)) =
        event_ctx.emit_authz(authz_result)?;

    let event_ctx = event_ctx.resolve(ResolvedView {
        warehouse,
        view: Arc::new(view_info),
    });

    let view_id = event_ctx.resolved().view.view_id();
    // ------------------- BUSINESS LOGIC -------------------
    let mut t = C::Transaction::begin_read(catalog_state).await?;
    let view = C::load_view(warehouse_id, view_id, false, t.transaction()).await?;
    t.commit().await?;

    let view_location =
        Location::from_str(view.metadata.location()).map_err(InternalParseLocationError::from)?;

    let warehouse = &event_ctx.resolved().warehouse;
    let storage_secret = if let Some(secret_id) = warehouse.storage_secret_id {
        Some(
            state
                .v1_state
                .secrets
                .require_storage_secret_by_id(secret_id)
                .await?
                .secret,
        )
    } else {
        None
    };
    let storage_secret_ref = storage_secret.as_deref();

    let access = warehouse
        .storage_profile
        .generate_table_config(
            data_access,
            storage_secret_ref,
            &view_location,
            storage_permissions.unwrap_or(StoragePermissions::Read),
            &request_metadata,
            &*event_ctx.resolved().view,
        )
        .await?;

    let metadata_ref = view.metadata;
    let metadata_location_ref = Arc::new(view.metadata_location);

    event_ctx.emit_view_loaded_async(metadata_ref.clone(), metadata_location_ref.clone());

    let load_table_result = LoadViewResult {
        metadata_location: metadata_location_ref.to_string(),
        metadata: metadata_ref,
        config: Some(access.config.into()),
    };

    Ok(load_table_result)
}

use crate::api::iceberg::types::ReferencingView;

async fn authorize_load_view<C: CatalogStore, A: Authorizer + Clone>(
    request_metadata: &RequestMetadata,
    warehouse_id: WarehouseId,
    view: &TableIdent,
    authorizer: &A,
    state: C::State,
    referenced_by: Option<&[ReferencingView]>,
) -> Result<(Arc<ResolvedWarehouse>, ViewInfo, Option<StoragePermissions>), AuthZError> {
    let engines = request_metadata.engines();
    let list_flags = TabularListFlags::active();
    let referenced_by = effective_referenced_by(referenced_by, engines);

    // 1. Collect all relevant namespace idents
    let user_provided_namespaces = get_relevant_namespaces_to_authorize_load_tabular(
        &TabularIdentBorrowed::View(view),
        referenced_by,
    );

    // 2. Collect all relevant tabular idents
    let user_provided_tabulars = get_relevant_tabulars_to_authorize_load_tabular(
        TabularIdentBorrowed::View(view),
        referenced_by,
    );

    // 3. Load objects concurrently
    let objects = load_objects_to_authorize_load_tabular::<C>(
        warehouse_id,
        user_provided_namespaces.clone().into_iter().collect(),
        user_provided_tabulars.clone().into_iter().collect(),
        list_flags,
        state,
    )
    .await;

    // 4. Check objects presence
    let warehouse = authorizer.require_warehouse_presence(warehouse_id, objects.warehouse)?;
    let tabulars = check_required_tabulars(
        warehouse_id,
        user_provided_tabulars,
        objects.tabulars,
        authorizer,
    )?;
    let namespaces =
        check_required_namespaces(warehouse_id, &user_provided_namespaces, objects.namespaces)?;

    // 5. Build NamespaceHierarchy
    let namespaces_with_hierarchy: HashMap<_, _> = namespaces
        .iter()
        .map(|(namespace_id, namespace)| {
            (
                *namespace_id,
                build_namespace_hierarchy(namespace, &namespaces),
            )
        })
        .collect();

    // 6. Sort tabulars
    let sorted_tabulars = sort_tabulars_for_authorize_load_tabular(&tabulars, referenced_by, view);

    // 7. Connect with namespaces
    let sorted_tabulars = add_namespace_to_tabulars_for_authorize_load_tabular(
        warehouse_id,
        sorted_tabulars,
        &namespaces_with_hierarchy,
    )?;

    // 8. Resolve owners and assign current user
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

    // 9. Build actions and check all authorizations in batch
    let actions = build_actions_from_sorted_tabulars_for_authorize_load_tabular(
        &sorted_tabulars_with_full_info,
    );
    let authz_results = authorizer
        .are_allowed_tabular_actions_vec(request_metadata, &warehouse, &namespaces, &actions)
        .await?
        .into_inner();

    // 10. Interpret authorization results
    let (view_info, storage_permissions) =
        interpret_authz_results_for_load_view(&actions, &authz_results, warehouse_id, view)?;

    Ok((warehouse, view_info, storage_permissions))
}

fn interpret_authz_results_for_load_view(
    actions: &[crate::server::tables::TabularAuthzAction<'_>],
    authz_results: &[bool],
    warehouse_id: WarehouseId,
    view: &TableIdent,
) -> Result<(ViewInfo, Option<StoragePermissions>), AuthZError> {
    if actions.len() != authz_results.len() {
        return Err(
            BackendUnavailableOrCountMismatch::from(AuthorizationCountMismatch::new(
                actions.len(),
                authz_results.len(),
                "load_view",
            ))
            .into(),
        );
    }

    let mut target_view_info: Option<ViewInfo> = None;
    let mut target_is_delegated = false;
    let mut can_get_metadata = false;

    for ((_ns, action), &allowed) in actions.iter().zip(authz_results) {
        match action {
            ActionOnTableOrView::View(view_action) => {
                // The target view is the last view in the chain.
                // All views produce GetMetadata actions; the target view is identified
                // by matching its ident.
                if view_action.info.tabular_ident == *view {
                    target_view_info = Some(view_action.info.clone());
                    target_is_delegated = view_action.is_delegated_execution;
                    if matches!(view_action.action, CatalogViewAction::GetMetadata) {
                        can_get_metadata = allowed;
                    }
                } else if !allowed {
                    return Err(AuthZCannotSeeView::new_forbidden(
                        warehouse_id,
                        view_action.info.tabular_ident.clone(),
                    )
                    .with_delegated_execution(view_action.is_delegated_execution)
                    .into());
                }
            }
            ActionOnTableOrView::Table(_) => {
                // Unreachable: all referenced-by entries are looked up as views
                // (TabularIdentBorrowed::View) and the target is also a view,
                // so the DB type filter prevents tables from appearing here.
                debug_assert!(false, "Table action in loadView authorization chain");
            }
        }
    }

    let view_info = target_view_info
        .ok_or_else(|| AuthZCannotSeeView::new_not_found(warehouse_id, view.clone()))?;

    if !can_get_metadata {
        return Err(
            AuthZCannotSeeView::new_forbidden(warehouse_id, view.clone())
                .with_delegated_execution(target_is_delegated)
                .into(),
        );
    }

    // Views loaded via loadView always get read storage permissions
    Ok((view_info, Some(StoragePermissions::Read)))
}

#[cfg(test)]
pub(crate) mod test {
    use iceberg_ext::catalog::rest::LoadViewResult;
    use sqlx::PgPool;

    use crate::{
        api::iceberg::v1::{
            ViewParameters,
            views::{LoadViewRequest, ViewService},
        },
        implementations::postgres::{PostgresBackend, SecretsState},
        server::CatalogServer,
        service::{Result, State, authz::AllowAllAuthorizer},
    };

    pub(crate) async fn load_view(
        api_context: crate::api::ApiContext<
            State<AllowAllAuthorizer, PostgresBackend, SecretsState>,
        >,
        view: ViewParameters,
    ) -> Result<LoadViewResult> {
        <CatalogServer<PostgresBackend, AllowAllAuthorizer, SecretsState> as ViewService<
            State<AllowAllAuthorizer, PostgresBackend, SecretsState>,
        >>::load_view(
            view,
            LoadViewRequest::default(),
            api_context,
            crate::tests::random_request_metadata(),
        )
        .await
    }

    #[sqlx::test]
    async fn test_load_view(pool: PgPool) {
        let (ctx, namespace, whi, _) = crate::server::views::test::setup(pool, None).await;

        let view_name = "my-view";
        let rq = crate::tests::create_view_request(Some(view_name), None);
        let prefix = whi.to_string();
        Box::pin(crate::server::views::create::test::create_view(
            ctx.clone(),
            namespace.clone(),
            rq,
            Some(prefix.clone()),
        ))
        .await
        .expect("create_view should succeed");

        let mut view_ns = namespace.inner();
        view_ns.push(view_name.into());
        let view_ident = iceberg::TableIdent::from_strs(view_ns).unwrap();

        let loaded_view = load_view(
            ctx,
            ViewParameters {
                prefix: Some(crate::api::iceberg::types::Prefix(prefix)),
                view: view_ident,
            },
        )
        .await
        .expect("load_view should succeed");

        assert_eq!(loaded_view.metadata.current_version().schema_id(), 0);
    }
}
