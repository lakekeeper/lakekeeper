mod commit;
pub(crate) mod create;
mod drop;
mod exists;
mod list;
mod load;
mod rename;

use std::str::FromStr;

#[allow(unused_imports)]
pub(crate) use exists::authorized_view_ident_to_id;
use iceberg_ext::catalog::rest::{ErrorModel, ViewUpdate};
use lakekeeper_io::Location;

use super::{tables::validate_table_properties, CatalogServer};
use crate::{
    api::iceberg::{
        types::DropParams,
        v1::{
            ApiContext, CommitViewRequest, CreateViewRequest, DataAccessMode, ListTablesQuery,
            ListTablesResponse, LoadViewResult, NamespaceParameters, Prefix, RenameTableRequest,
            Result, ViewParameters,
        },
    },
    request_metadata::RequestMetadata,
    service::{authz::Authorizer, Catalog, SecretStore, State},
};

#[async_trait::async_trait]
impl<C: Catalog, A: Authorizer + Clone, S: SecretStore>
    crate::api::iceberg::v1::views::ViewService<State<A, C, S>> for CatalogServer<C, A, S>
{
    /// List all view identifiers underneath a given namespace
    async fn list_views(
        parameters: NamespaceParameters,
        query: ListTablesQuery,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ListTablesResponse> {
        list::list_views(parameters, query, state, request_metadata).await
    }

    /// Create a view in the given namespace
    async fn create_view(
        parameters: NamespaceParameters,
        request: CreateViewRequest,
        state: ApiContext<State<A, C, S>>,
        data_access: impl Into<DataAccessMode> + Send,
        request_metadata: RequestMetadata,
    ) -> Result<LoadViewResult> {
        create::create_view(parameters, request, state, data_access, request_metadata).await
    }

    /// Load a view from the catalog
    async fn load_view(
        parameters: ViewParameters,
        state: ApiContext<State<A, C, S>>,
        data_access: impl Into<DataAccessMode> + Send,
        request_metadata: RequestMetadata,
    ) -> Result<LoadViewResult> {
        load::load_view(parameters, state, data_access, request_metadata).await
    }

    /// Commit updates to a view
    async fn commit_view(
        parameters: ViewParameters,
        request: CommitViewRequest,
        state: ApiContext<State<A, C, S>>,
        data_access: impl Into<DataAccessMode> + Send,
        request_metadata: RequestMetadata,
    ) -> Result<LoadViewResult> {
        commit::commit_view(parameters, request, state, data_access, request_metadata).await
    }

    /// Drop a view from the catalog
    async fn drop_view(
        parameters: ViewParameters,
        drop_params: DropParams,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        drop::drop_view(parameters, drop_params, state, request_metadata).await
    }

    /// Check if a view exists
    async fn view_exists(
        parameters: ViewParameters,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        exists::view_exists(parameters, state, request_metadata).await
    }

    /// Rename a view
    async fn rename_view(
        prefix: Option<Prefix>,
        request: RenameTableRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        rename::rename_view(prefix, request, state, request_metadata).await
    }
}

fn validate_view_properties<'a, I>(properties: I) -> Result<()>
where
    I: IntoIterator<Item = &'a String>,
{
    validate_table_properties(properties)
}

fn validate_view_updates(updates: &Vec<ViewUpdate>) -> Result<()> {
    for update in updates {
        match update {
            ViewUpdate::SetProperties { updates } => {
                validate_view_properties(updates.keys())?;
            }
            ViewUpdate::RemoveProperties { removals } => {
                validate_view_properties(removals.iter())?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn parse_view_location(location: &str) -> Result<Location> {
    Ok(Location::from_str(location).map_err(|e| {
        ErrorModel::internal(
            format!("Invalid view location in DB: {e}"),
            "InvalidViewLocation",
            Some(Box::new(e)),
        )
    })?)
}

#[cfg(test)]
mod test {
    use iceberg::{NamespaceIdent, TableIdent};
    use lakekeeper_io::Location;
    use sqlx::PgPool;
    use uuid::Uuid;

    use crate::{
        api::{
            iceberg::v1::{views::ViewService, DropParams, PaginationQuery, ViewParameters},
            management::v1::warehouse::TabularDeleteProfile,
            ApiContext, RequestMetadata,
        },
        catalog::{
            test::tabular_test_multi_warehouse_setup, views::validate_view_properties,
            CatalogServer,
        },
        implementations::postgres::{
            namespace::tests::initialize_namespace, tabular::view::tests::view_request,
            warehouse::test::initialize_warehouse, PostgresCatalog, SecretsState,
        },
        service::{
            authz::AllowAllAuthorizer,
            storage::{MemoryProfile, StorageProfile},
            Catalog, State, ViewId,
        },
        WarehouseId,
    };

    pub(crate) async fn setup(
        pool: PgPool,
        namespace_name: Option<Vec<String>>,
    ) -> (
        ApiContext<State<AllowAllAuthorizer, PostgresCatalog, SecretsState>>,
        NamespaceIdent,
        WarehouseId,
    ) {
        let api_context = crate::tests::get_api_context(&pool, AllowAllAuthorizer::default()).await;
        let state = api_context.v1_state.catalog.clone();
        let warehouse_id = initialize_warehouse(
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
            &NamespaceIdent::from_vec(namespace_name.unwrap_or(vec![Uuid::now_v7().to_string()]))
                .unwrap(),
            None,
        )
        .await
        .1
        .namespace;
        (api_context, namespace, warehouse_id)
    }

    // Returns a random view location and matching location for its metadata.
    fn new_random_location() -> (Location, Location) {
        let id = Uuid::now_v7();
        (
            format!("s3://my_bucket/my_table/metadata/{id}")
                .parse()
                .unwrap(),
            format!(
                "s3://my_bucket/my_table/metadata/{id}/metadata-{}.gz.json",
                Uuid::now_v7()
            )
            .parse()
            .unwrap(),
        )
    }

    #[test]
    fn test_mixed_case_properties() {
        let properties = ["a".to_string(), "B".to_string()];
        assert!(validate_view_properties(properties.iter()).is_ok());
    }

    async fn assert_view_exists(
        ctx: ApiContext<State<AllowAllAuthorizer, PostgresCatalog, SecretsState>>,
        warehouse_id: WarehouseId,
        view_id: ViewId,
        namespace: &NamespaceIdent,
        include_deleted: bool,
        expected_num_views: usize,
        assert_msg: &str,
    ) {
        let mut read_tx = ctx.v1_state.catalog.read_pool().begin().await.unwrap();
        let views = PostgresCatalog::list_views(
            warehouse_id,
            namespace,
            include_deleted,
            &mut read_tx,
            PaginationQuery::empty(),
        )
        .await
        .unwrap();
        assert_eq!(
            views.len(),
            expected_num_views,
            "unexpected number of views"
        );
        assert!(views.get(&view_id).is_some(), "{assert_msg}");
        read_tx.commit().await.unwrap();
    }

    async fn assert_view_doesnt_exist(
        ctx: ApiContext<State<AllowAllAuthorizer, PostgresCatalog, SecretsState>>,
        warehouse_id: WarehouseId,
        view_id: ViewId,
        namespace: &NamespaceIdent,
        include_deleted: bool,
        expected_num_views: usize,
        assert_msg: &str,
    ) {
        let mut read_tx = ctx.v1_state.catalog.read_pool().begin().await.unwrap();
        let views = PostgresCatalog::list_views(
            warehouse_id,
            namespace,
            include_deleted,
            &mut read_tx,
            PaginationQuery::empty(),
        )
        .await
        .unwrap();
        assert_eq!(
            views.len(),
            expected_num_views,
            "unexpected number of views"
        );
        assert!(views.get(&view_id).is_none(), "{assert_msg}");
        read_tx.commit().await.unwrap();
    }

    // Reasons for using a mix of PostgresCatalog and CatalogServer:
    //
    // - PostgresCatalog: required for specifying id of table to be created
    // - CatalogServer: required for taking TabularDeleteProfile into account
    #[sqlx::test]
    async fn test_reuse_view_ids_hard_delete(pool: PgPool) {
        let delete_profile = TabularDeleteProfile::Hard {};
        let (ctx, mut wh_ns_data, _base_loc) =
            tabular_test_multi_warehouse_setup(pool.clone(), 3, delete_profile).await;

        let v_id = ViewId::new_random();
        let v_name = "v1".to_string();

        // Create views with the same table ID across different warehouses.
        for (wh_id, ns_id, ns_params) in &wh_ns_data {
            let (location, meta_location) = new_random_location();
            let meta = view_request(Some(*v_id), &location);
            let ident = TableIdent {
                namespace: ns_params.namespace.clone(),
                name: v_name.clone(),
            };
            let mut tx = ctx.v1_state.catalog.write_pool().begin().await.unwrap();
            PostgresCatalog::create_view(
                *wh_id,
                *ns_id,
                &ident,
                meta,
                &meta_location,
                &location,
                &mut tx,
            )
            .await
            .expect("Should create view");
            tx.commit().await.unwrap();

            // Verify view creation.
            assert_view_exists(
                ctx.clone(),
                *wh_id,
                v_id,
                &ns_params.namespace,
                false,
                1,
                "view should be created",
            )
            .await;
        }

        // Hard delete one of the views.
        let deleted_view_data = wh_ns_data.pop().unwrap();
        CatalogServer::drop_view(
            ViewParameters {
                prefix: deleted_view_data.2.prefix.clone(),
                view: TableIdent {
                    namespace: deleted_view_data.2.namespace.clone(),
                    name: v_name.clone(),
                },
            },
            DropParams {
                purge_requested: false,
                force: false,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        // Deleted view cannot be accessed anymore.
        assert_view_doesnt_exist(
            ctx.clone(),
            deleted_view_data.0,
            v_id,
            &deleted_view_data.2.namespace,
            true,
            0,
            "view should be deleted",
        )
        .await;

        // Views in other warehouses are still there.
        assert!(!wh_ns_data.is_empty());
        for (wh_id, _ns_id, ns_params) in &wh_ns_data {
            assert_view_exists(
                ctx.clone(),
                *wh_id,
                v_id,
                &ns_params.namespace,
                false,
                1,
                "view should still exist",
            )
            .await;
        }

        // As the delete was hard, the view can be recreated in the warehouse.
        let (location, meta_location) = new_random_location();
        let meta = view_request(Some(*v_id), &location);
        let ident = TableIdent {
            namespace: deleted_view_data.2.namespace.clone(),
            name: v_name.clone(),
        };
        let mut tx = ctx.v1_state.catalog.write_pool().begin().await.unwrap();
        PostgresCatalog::create_view(
            deleted_view_data.0,
            deleted_view_data.1,
            &ident,
            meta,
            &meta_location,
            &location,
            &mut tx,
        )
        .await
        .expect("Should create view");
        tx.commit().await.unwrap();

        assert_view_exists(
            ctx.clone(),
            deleted_view_data.0,
            v_id,
            &deleted_view_data.2.namespace,
            false,
            1,
            "view should be recreated",
        )
        .await;
    }

    // Reasons for using a mix of PostgresCatalog and CatalogServer:
    //
    // - PostgresCatalog: required for specifying id of table to be created
    // - CatalogServer: required for taking TabularDeleteProfile into account
    #[sqlx::test]
    async fn test_reuse_view_ids_soft_delete(pool: PgPool) {
        let delete_profile = TabularDeleteProfile::Soft {
            expiration_seconds: chrono::Duration::seconds(10),
        };
        let (ctx, mut wh_ns_data, _base_loc) =
            tabular_test_multi_warehouse_setup(pool.clone(), 3, delete_profile).await;

        let v_id = ViewId::new_random();
        let v_name = "v1".to_string();

        // Create views with the same table ID across different warehouses.
        for (wh_id, ns_id, ns_params) in &wh_ns_data {
            let (location, meta_location) = new_random_location();
            let meta = view_request(Some(*v_id), &location);
            let ident = TableIdent {
                namespace: ns_params.namespace.clone(),
                name: v_name.clone(),
            };
            let mut tx = ctx.v1_state.catalog.write_pool().begin().await.unwrap();
            PostgresCatalog::create_view(
                *wh_id,
                *ns_id,
                &ident,
                meta,
                &meta_location,
                &location,
                &mut tx,
            )
            .await
            .expect("Should create view");
            tx.commit().await.unwrap();

            // Verify view creation.
            assert_view_exists(
                ctx.clone(),
                *wh_id,
                v_id,
                &ns_params.namespace,
                false,
                1,
                "view should be created",
            )
            .await;
        }

        // Soft delete one of the views.
        let deleted_view_data = wh_ns_data.pop().unwrap();
        CatalogServer::drop_view(
            ViewParameters {
                prefix: deleted_view_data.2.prefix.clone(),
                view: TableIdent {
                    namespace: deleted_view_data.2.namespace.clone(),
                    name: v_name.clone(),
                },
            },
            DropParams {
                purge_requested: false,
                force: false,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        // Whether the view is still available depends on query params.
        assert_view_doesnt_exist(
            ctx.clone(),
            deleted_view_data.0,
            v_id,
            &deleted_view_data.2.namespace,
            false,
            0,
            "soft deleted view should not be shown",
        )
        .await;
        assert_view_exists(
            ctx.clone(),
            deleted_view_data.0,
            v_id,
            &deleted_view_data.2.namespace,
            true,
            1,
            "soft deleted view should be shown",
        )
        .await;

        // Views in other warehouses are still there.
        assert!(!wh_ns_data.is_empty());
        for (wh_id, _ns_id, ns_params) in &wh_ns_data {
            assert_view_exists(
                ctx.clone(),
                *wh_id,
                v_id,
                &ns_params.namespace,
                false,
                1,
                "view should still exist",
            )
            .await;
        }
    }
}
