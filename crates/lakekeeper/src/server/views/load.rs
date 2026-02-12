use std::{collections::BTreeMap, str::FromStr as _, sync::Arc};

use iceberg::TableIdent;
use iceberg_ext::catalog::rest::LoadViewResult;
use lakekeeper_io::Location;

use crate::{
    WarehouseId,
    api::{
        ApiContext,
        iceberg::v1::{DataAccessMode, ViewParameters},
    },
    request_metadata::RequestMetadata,
    server::{require_warehouse_id, tables::validate_table_or_view_ident},
    service::{
        AuthZViewInfo, CatalogNamespaceOps, CatalogStore, CatalogTabularOps, CatalogViewOps,
        CatalogWarehouseOps, InternalParseLocationError, ResolvedWarehouse, Result, SecretStore,
        State, TabularListFlags, Transaction, ViewInfo,
        authz::{
            AuthZCannotSeeView, AuthZViewOps, Authorizer, AuthzNamespaceOps, AuthzWarehouseOps,
            CatalogViewAction, AuthZError,
            refresh_warehouse_and_namespace_if_needed,
        },
        events::{APIEventContext, context::ResolvedView},
        storage::StoragePermissions,
    },
};

pub(crate) async fn load_view<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: ViewParameters,
    state: ApiContext<State<A, C, S>>,
    data_access: impl Into<DataAccessMode>,
    request_metadata: RequestMetadata,
) -> Result<LoadViewResult> {
    let data_access = data_access.into();
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

async fn authorize_load_view<C: CatalogStore, A: Authorizer + Clone>(
    request_metadata: &RequestMetadata,
    warehouse_id: WarehouseId,
    view: &TableIdent,
    authorizer: &A,
    state: C::State,
) -> Result<
    (Arc<ResolvedWarehouse>, ViewInfo, Option<StoragePermissions>),
    AuthZError,
> {
    let (warehouse, namespace, view_info) = tokio::join!(
        C::get_active_warehouse_by_id(warehouse_id, state.clone()),
        C::get_namespace(warehouse_id, view.namespace.clone(), state.clone()),
        C::get_view_info(
            warehouse_id,
            view.clone(),
            TabularListFlags::active(),
            state.clone()
        )
    );
    let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;
    let view_info = authorizer.require_view_presence(warehouse_id, view.clone(), view_info)?;
    let namespace =
        authorizer.require_namespace_presence(warehouse_id, view.namespace.clone(), namespace)?;

    let (warehouse, namespace) = refresh_warehouse_and_namespace_if_needed::<C, _, _>(
        &warehouse,
        namespace,
        &view_info,
        AuthZCannotSeeView::new_not_found(warehouse_id, view.clone()),
        authorizer,
        state.clone(),
    )
    .await?;

    let [can_load, can_write] = authorizer
        .are_allowed_view_actions_arr(
            &request_metadata,
            None,
            &warehouse,
            &namespace,
            &view_info,
            &[
                CatalogViewAction::GetMetadata,
                CatalogViewAction::Commit {
                    updated_properties: Arc::new(BTreeMap::default()),
                    removed_properties: Arc::new(Vec::default()),
                },
            ],
        )
        .await?
        .into_inner();

    if !can_load {
        return Err(AuthZCannotSeeView::new_forbidden(warehouse_id, view.clone()).into());
    }

    let storage_permissions = if can_write {
        Some(StoragePermissions::ReadWriteDelete)
    } else {
        Some(StoragePermissions::Read)
    };
    Ok((warehouse, view_info, storage_permissions))
}

#[cfg(test)]
pub(crate) mod test {
    use iceberg::TableIdent;
    use iceberg_ext::catalog::rest::{CreateViewRequest, LoadViewResult};
    use sqlx::PgPool;

    use crate::{
        api::{
            ApiContext,
            iceberg::v1::{DataAccess, Prefix, ViewParameters, views},
        },
        implementations::postgres::{PostgresBackend, secrets::SecretsState},
        server::{
            CatalogServer,
            views::{create::test::create_view, test::setup},
        },
        service::{State, authz::AllowAllAuthorizer},
        tests::create_view_request,
    };

    pub(crate) async fn load_view(
        api_context: ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>,
        params: ViewParameters,
    ) -> crate::api::Result<LoadViewResult> {
        <CatalogServer<PostgresBackend, AllowAllAuthorizer, SecretsState> as views::ViewService<
            State<AllowAllAuthorizer, PostgresBackend, SecretsState>,
        >>::load_view(
            params,
            api_context,
            DataAccess {
                vended_credentials: true,
                remote_signing: false,
            },
            crate::request_metadata::RequestMetadata::new_unauthenticated(),
        )
        .await
    }

    #[sqlx::test]
    async fn test_load_view(pool: PgPool) {
        let (api_context, namespace, whi, _) = setup(pool, None).await;

        let view_name = "my-view";
        let rq: CreateViewRequest = create_view_request(Some(view_name), None);

        let prefix = &whi.to_string();
        let created_view = Box::pin(create_view(
            api_context.clone(),
            namespace.clone(),
            rq,
            Some(prefix.into()),
        ))
        .await
        .unwrap();
        let mut table_ident = namespace.clone().inner();
        table_ident.push(view_name.into());

        let loaded_view = load_view(
            api_context,
            ViewParameters {
                prefix: Some(Prefix(prefix.clone())),
                view: TableIdent::from_strs(table_ident).unwrap(),
            },
        )
        .await
        .expect("View should be loadable");
        assert_eq!(loaded_view.metadata, created_view.metadata);
    }
}
