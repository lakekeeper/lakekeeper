use std::str::FromStr as _;

use iceberg_ext::catalog::rest::LoadViewResult;
use lakekeeper_io::Location;

use crate::{
    api::{
        iceberg::v1::{DataAccessMode, ViewParameters},
        ApiContext,
    },
    request_metadata::RequestMetadata,
    server::{
        require_warehouse_id,
        tables::{require_active_warehouse, validate_table_or_view_ident},
    },
    service::{
        authz::{
            AuthZCannotSeeView, AuthZViewOps, Authorizer, CatalogViewAction, RequireViewActionError,
        },
        storage::{StorageCredential, StoragePermissions},
        AuthZViewInfo as _, CachePolicy, CatalogStore, CatalogTabularOps, CatalogViewOps,
        CatalogWarehouseOps, InternalParseLocationError, Result, SecretStore, State, Transaction,
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

    let view_info = C::get_view_info(
        warehouse_id,
        view.clone(),
        crate::service::TabularListFlags::active(),
        state.v1_state.catalog.clone(),
    )
    .await
    .map_err(RequireViewActionError::from)?
    .ok_or_else(|| AuthZCannotSeeView::new(warehouse_id, view.clone()))?;

    let view_id = view_info.view_id();

    let [can_load, can_write] = authorizer
        .are_allowed_view_actions_arr(
            &request_metadata,
            &view_info,
            &[
                CatalogViewAction::CanGetMetadata,
                CatalogViewAction::CanCommit,
            ],
        )
        .await?
        .into_inner();

    if !can_load {
        return Err(AuthZCannotSeeView::new(warehouse_id, view.clone()).into());
    }
    // ------------------- BUSINESS LOGIC -------------------
    let mut t = C::Transaction::begin_read(state.v1_state.catalog.clone()).await?;
    let view = C::load_view(warehouse_id, view_id, false, t.transaction()).await?;
    t.commit().await?;

    let warehouse = C::require_warehouse_by_id_cache_aware(
        warehouse_id,
        CachePolicy::RequireMinimumVersion(*view.warehouse_version),
        state.v1_state.catalog,
    )
    .await?;
    require_active_warehouse(warehouse.status)?;

    let view_location =
        Location::from_str(view.metadata.location()).map_err(InternalParseLocationError::from)?;

    let storage_secret: Option<StorageCredential> =
        if let Some(secret_id) = warehouse.storage_secret_id {
            Some(
                state
                    .v1_state
                    .secrets
                    .get_secret_by_id(secret_id)
                    .await?
                    .secret,
            )
        } else {
            None
        };

    let storage_permissions = if can_write {
        StoragePermissions::ReadWriteDelete
    } else {
        StoragePermissions::Read
    };

    let access = warehouse
        .storage_profile
        .generate_table_config(
            data_access,
            storage_secret.as_ref(),
            &view_location,
            storage_permissions,
            &request_metadata,
            warehouse_id,
            view_id.into(),
        )
        .await?;
    let load_table_result = LoadViewResult {
        metadata_location: view.metadata_location.to_string(),
        metadata: view.metadata,
        config: Some(access.config.into()),
    };

    Ok(load_table_result)
}

#[cfg(test)]
pub(crate) mod test {
    use iceberg::TableIdent;
    use iceberg_ext::catalog::rest::{CreateViewRequest, LoadViewResult};
    use sqlx::PgPool;

    use crate::{
        api::{
            iceberg::v1::{views, DataAccess, Prefix, ViewParameters},
            ApiContext,
        },
        implementations::postgres::{secrets::SecretsState, PostgresBackend},
        server::{
            views::{create::test::create_view, test::setup},
            CatalogServer,
        },
        service::{authz::AllowAllAuthorizer, State},
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
        let (api_context, namespace, whi) = setup(pool, None).await;

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
