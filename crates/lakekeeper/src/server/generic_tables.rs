mod create;
mod drop;
mod list;
mod load;

use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    api::{
        ApiContext,
        iceberg::v1::{DataAccessMode, namespace::NamespaceParameters},
        v1::generic_tables::{
            CreateGenericTableRequest, GenericTableParameters, GenericTableService,
            ListGenericTablesQuery, ListGenericTablesResponse, LoadGenericTableResponse,
        },
    },
    request_metadata::RequestMetadata,
    server::CatalogServer,
    service::{
        CatalogGenericTableOps, CatalogNamespaceOps, CatalogStore, CatalogWarehouseOps,
        GenericTableInfo, NamespaceHierarchy, ResolvedWarehouse, Result, SecretStore, State,
        Transaction, WarehouseId,
        authz::{
            AuthZCannotSeeGenericTable, AuthZError, AuthZGenericTableOps, Authorizer,
            AuthzNamespaceOps, AuthzWarehouseOps, refresh_warehouse_and_namespace_if_needed,
        },
    },
};

/// Fetches and authorizes a generic table operation in one call.
///
/// Combines: parallel warehouse + namespace fetch, generic table load,
/// TOCTOU-safe namespace validation, and authorization.
async fn load_and_authorize_generic_table_operation<C: CatalogStore, A: Authorizer + Clone>(
    authorizer: &A,
    request_metadata: &RequestMetadata,
    warehouse_id: WarehouseId,
    namespace: iceberg::NamespaceIdent,
    table_name: &str,
    action: impl Into<A::GenericTableAction> + Send,
    catalog_state: C::State,
) -> std::result::Result<(Arc<ResolvedWarehouse>, NamespaceHierarchy, GenericTableInfo), AuthZError>
{
    // Fetch warehouse and namespace in parallel
    let (warehouse_result, namespace_result) = tokio::join!(
        C::get_active_warehouse_by_id(warehouse_id, catalog_state.clone()),
        C::get_namespace(warehouse_id, namespace.clone(), catalog_state.clone()),
    );

    let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse_result)?;
    let namespace_hierarchy =
        authorizer.require_namespace_presence(warehouse_id, namespace.clone(), namespace_result)?;

    let namespace_id = namespace_hierarchy.namespace.namespace_id();

    let table_ident = iceberg::TableIdent::new(namespace.clone(), table_name.to_string());

    // Fetch generic table
    let mut t = C::Transaction::begin_read(catalog_state.clone())
        .await
        .map_err(iceberg_err_to_authz)?;
    let info = match C::load_generic_table(warehouse_id, namespace_id, table_name, t.transaction())
        .await
    {
        Ok(info) => info,
        Err(crate::service::LoadGenericTableError::GenericTableNotFound(_)) => {
            return Err(
                AuthZCannotSeeGenericTable::new_not_found(warehouse_id, table_ident).into(),
            );
        }
        Err(e) => return Err(iceberg_err_to_authz(e)),
    };
    t.commit().await.map_err(iceberg_err_to_authz)?;

    // TOCTOU-safe namespace validation
    let (warehouse, namespace_hierarchy) = refresh_warehouse_and_namespace_if_needed::<C, _, _>(
        &warehouse,
        namespace_hierarchy,
        &info,
        AuthZCannotSeeGenericTable::new_not_found(warehouse_id, table_ident.clone()),
        authorizer,
        catalog_state,
    )
    .await?;

    // Authorization check
    let info = authorizer
        .require_generic_table_action(
            request_metadata,
            &warehouse,
            &namespace_hierarchy,
            table_ident,
            Ok::<_, crate::service::authz::RequireGenericTableActionError>(Some(info)),
            action,
        )
        .await?;

    Ok((warehouse, namespace_hierarchy, info))
}

/// Convert a catalog error into `AuthZError` via `CatalogBackendError`.
fn iceberg_err_to_authz(e: impl Into<crate::service::IcebergErrorResponse>) -> AuthZError {
    let err_model = iceberg_ext::catalog::rest::ErrorModel::from(e.into());
    AuthZError::RequireGenericTableActionError(
        crate::service::authz::RequireGenericTableActionError::CatalogBackendError(
            crate::service::CatalogBackendError::new_unexpected(err_model),
        ),
    )
}

#[async_trait]
impl<C: CatalogStore, A: Authorizer + Clone, S: SecretStore> GenericTableService<State<A, C, S>>
    for CatalogServer<C, A, S>
{
    async fn create_generic_table(
        parameters: NamespaceParameters,
        request: CreateGenericTableRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadGenericTableResponse> {
        create::create_generic_table::<C, A, S>(parameters, request, state, request_metadata).await
    }

    async fn load_generic_table(
        parameters: GenericTableParameters,
        state: ApiContext<State<A, C, S>>,
        data_access: impl Into<DataAccessMode> + Send,
        request_metadata: RequestMetadata,
    ) -> Result<LoadGenericTableResponse> {
        load::load_generic_table::<C, A, S>(parameters, state, data_access, request_metadata).await
    }

    async fn list_generic_tables(
        parameters: NamespaceParameters,
        query: ListGenericTablesQuery,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ListGenericTablesResponse> {
        list::list_generic_tables::<C, A, S>(parameters, query, state, request_metadata).await
    }

    async fn drop_generic_table(
        parameters: GenericTableParameters,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        drop::drop_generic_table::<C, A, S>(parameters, state, request_metadata).await
    }
}

#[cfg(test)]
pub(crate) mod test {
    use http::StatusCode;
    use iceberg::NamespaceIdent;
    use sqlx::PgPool;

    use crate::{
        api::{
            ApiContext,
            iceberg::v1::{DataAccessMode, namespace::NamespaceParameters},
            v1::generic_tables::{
                CreateGenericTableRequest, GenericTableParameters, GenericTableService as _,
                ListGenericTablesQuery,
            },
        },
        implementations::postgres::{
            PostgresBackend, SecretsState, namespace::tests::initialize_namespace,
            warehouse::test::initialize_warehouse,
        },
        request_metadata::RequestMetadata,
        server::CatalogServer,
        service::{
            GenericTableFormat, State,
            authz::AllowAllAuthorizer,
            storage::{MemoryProfile, StorageProfile},
        },
        tests::random_request_metadata,
    };

    type Ctx = ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>;

    async fn setup(pool: PgPool) -> (Ctx, NamespaceIdent, crate::WarehouseId) {
        let api_context = crate::tests::get_api_context(&pool, AllowAllAuthorizer::default()).await;
        let state = api_context.v1_state.catalog.clone();
        let (_project_id, warehouse_id) = initialize_warehouse(
            state.clone(),
            Some(StorageProfile::Memory(MemoryProfile::default())),
            None,
            None,
            true,
        )
        .await;

        let namespace = initialize_namespace(
            state,
            warehouse_id,
            &NamespaceIdent::new(uuid::Uuid::now_v7().to_string()),
            None,
        )
        .await
        .namespace_ident()
        .clone();

        (api_context, namespace, warehouse_id)
    }

    fn create_request(name: &str) -> CreateGenericTableRequest {
        CreateGenericTableRequest {
            name: name.to_string(),
            format: GenericTableFormat::Unknown("lance".to_string()),
            base_location: None,
            doc: Some("test doc".to_string()),
            properties: Default::default(),
            schema: None,
            statistics: None,
        }
    }

    #[sqlx::test]
    async fn test_create_generic_table(pool: PgPool) {
        let (ctx, namespace, whi) = setup(pool).await;
        let prefix = whi.to_string();

        let result = CatalogServer::create_generic_table(
            NamespaceParameters {
                prefix: Some(prefix.into()),
                namespace: namespace.clone(),
            },
            create_request("my-gt"),
            ctx,
            random_request_metadata(),
        )
        .await
        .unwrap();

        assert_eq!(result.table.name, "my-gt");
        assert_eq!(
            result.table.format,
            GenericTableFormat::Unknown("lance".to_string())
        );
        assert_eq!(result.table.doc, Some("test doc".to_string()));
    }

    #[sqlx::test]
    async fn test_create_duplicate_fails(pool: PgPool) {
        let (ctx, namespace, whi) = setup(pool).await;
        let prefix = whi.to_string();
        let params = NamespaceParameters {
            prefix: Some(prefix.into()),
            namespace: namespace.clone(),
        };

        CatalogServer::create_generic_table(
            params.clone(),
            create_request("dup-gt"),
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        let err = CatalogServer::create_generic_table(
            params,
            create_request("dup-gt"),
            ctx,
            random_request_metadata(),
        )
        .await
        .expect_err("duplicate should fail");

        assert_eq!(err.error.code, StatusCode::CONFLICT);
    }

    #[sqlx::test]
    async fn test_create_empty_name_fails(pool: PgPool) {
        let (ctx, namespace, whi) = setup(pool).await;

        let err = CatalogServer::create_generic_table(
            NamespaceParameters {
                prefix: Some(whi.to_string().into()),
                namespace,
            },
            create_request(""),
            ctx,
            random_request_metadata(),
        )
        .await
        .expect_err("empty name should fail");

        assert_eq!(err.error.code, StatusCode::BAD_REQUEST);
    }

    #[sqlx::test]
    async fn test_load_generic_table(pool: PgPool) {
        let (ctx, namespace, whi) = setup(pool).await;
        let prefix = whi.to_string();

        let created = CatalogServer::create_generic_table(
            NamespaceParameters {
                prefix: Some(prefix.clone().into()),
                namespace: namespace.clone(),
            },
            create_request("load-gt"),
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        let loaded = CatalogServer::load_generic_table(
            GenericTableParameters {
                prefix: Some(prefix.into()),
                namespace,
                table_name: "load-gt".to_string(),
            },
            ctx,
            DataAccessMode::ClientManaged,
            random_request_metadata(),
        )
        .await
        .unwrap();

        assert_eq!(loaded.table.name, "load-gt");
        assert_eq!(loaded.table.base_location, created.table.base_location);
    }

    #[sqlx::test]
    async fn test_load_not_found(pool: PgPool) {
        let (ctx, namespace, whi) = setup(pool).await;

        let err = CatalogServer::load_generic_table(
            GenericTableParameters {
                prefix: Some(whi.to_string().into()),
                namespace,
                table_name: "does-not-exist".to_string(),
            },
            ctx,
            DataAccessMode::ClientManaged,
            random_request_metadata(),
        )
        .await
        .expect_err("should not exist");

        assert_eq!(err.error.code, StatusCode::NOT_FOUND);
    }

    #[sqlx::test]
    async fn test_list_generic_tables(pool: PgPool) {
        let (ctx, namespace, whi) = setup(pool).await;
        let prefix = whi.to_string();
        let params = NamespaceParameters {
            prefix: Some(prefix.clone().into()),
            namespace: namespace.clone(),
        };

        // Empty initially
        let list = CatalogServer::list_generic_tables(
            params.clone(),
            ListGenericTablesQuery::default(),
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();
        assert!(list.identifiers.is_empty());

        // Create two
        CatalogServer::create_generic_table(
            params.clone(),
            create_request("gt-a"),
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();
        CatalogServer::create_generic_table(
            params.clone(),
            create_request("gt-b"),
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        let list = CatalogServer::list_generic_tables(
            params,
            ListGenericTablesQuery::default(),
            ctx,
            random_request_metadata(),
        )
        .await
        .unwrap();

        assert_eq!(list.identifiers.len(), 2);
        let names: Vec<&str> = list.identifiers.iter().map(|i| i.name.as_str()).collect();
        assert!(names.contains(&"gt-a"));
        assert!(names.contains(&"gt-b"));
    }

    #[sqlx::test]
    async fn test_drop_generic_table(pool: PgPool) {
        let (ctx, namespace, whi) = setup(pool).await;
        let prefix = whi.to_string();
        let ns_params = NamespaceParameters {
            prefix: Some(prefix.clone().into()),
            namespace: namespace.clone(),
        };

        CatalogServer::create_generic_table(
            ns_params.clone(),
            create_request("drop-gt"),
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        CatalogServer::drop_generic_table(
            GenericTableParameters {
                prefix: Some(prefix.clone().into()),
                namespace: namespace.clone(),
                table_name: "drop-gt".to_string(),
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        // Verify gone from list
        let list = CatalogServer::list_generic_tables(
            ns_params,
            ListGenericTablesQuery::default(),
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();
        assert!(list.identifiers.is_empty());

        // Verify load fails
        let err = CatalogServer::load_generic_table(
            GenericTableParameters {
                prefix: Some(prefix.into()),
                namespace,
                table_name: "drop-gt".to_string(),
            },
            ctx,
            DataAccessMode::ClientManaged,
            random_request_metadata(),
        )
        .await
        .expect_err("should be gone");
        assert_eq!(err.error.code, StatusCode::NOT_FOUND);
    }

    #[sqlx::test]
    async fn test_drop_not_found(pool: PgPool) {
        let (ctx, namespace, whi) = setup(pool).await;

        let err = CatalogServer::drop_generic_table(
            GenericTableParameters {
                prefix: Some(whi.to_string().into()),
                namespace,
                table_name: "ghost".to_string(),
            },
            ctx,
            random_request_metadata(),
        )
        .await
        .expect_err("should not exist");

        assert_eq!(err.error.code, StatusCode::NOT_FOUND);
    }
}
