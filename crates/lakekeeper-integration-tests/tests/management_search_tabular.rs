//! Tabular search through the management API.
use lakekeeper::{
    WarehouseId,
    api::{
        ApiContext,
        management::v1::{
            ApiServer as ManagementApiServer,
            tabular::{SearchTabularRequest, TabularManagementService as _},
            warehouse::TabularDeleteProfile,
        },
    },
    service::{State, authz::AllowAllAuthorizer},
};
use lakekeeper_integration_tests::{
    create_ns, create_table, memory_io_profile, random_request_metadata, setup,
};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;

type TestApiContext = ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>;

/// A warehouse with the namespace `sales`, returning the context and the warehouse id.
async fn sales_warehouse(pool: PgPool) -> (TestApiContext, WarehouseId) {
    let (ctx, warehouse) = setup(
        pool,
        memory_io_profile(),
        None,
        AllowAllAuthorizer::default(),
        TabularDeleteProfile::Hard {},
        None,
        1,
        None,
    )
    .await;
    create_ns(
        ctx.clone(),
        warehouse.warehouse_id.to_string(),
        "sales".to_string(),
    )
    .await;
    (ctx, warehouse.warehouse_id)
}

/// Search `warehouse_id` for `term`, returning namespace, name and distance of each result.
async fn search(
    ctx: TestApiContext,
    warehouse_id: WarehouseId,
    term: &str,
) -> Vec<(Vec<String>, String, Option<f32>)> {
    ManagementApiServer::<PostgresBackend, AllowAllAuthorizer, SecretsState>::search_tabular(
        warehouse_id,
        ctx,
        random_request_metadata(),
        SearchTabularRequest {
            search: term.to_string(),
        },
    )
    .await
    .unwrap()
    .tabulars
    .into_iter()
    .map(|t| (t.namespace_name, t.tabular_name, t.distance))
    .collect()
}

#[sqlx::test]
async fn test_search_tabular_by_path_of_a_data_file(pool: PgPool) {
    let (ctx, warehouse_id) = sales_warehouse(pool).await;
    let prefix = warehouse_id.to_string();
    let orders = create_table(ctx.clone(), &prefix, "sales", "orders", false)
        .await
        .unwrap();
    create_table(ctx.clone(), &prefix, "sales", "customers", false)
        .await
        .unwrap();

    let data_file = format!(
        "{}/data/00000-0-0195f1c2-7a4e-7c3b-9d1f-2b6c8e4a0f13-00001.parquet",
        orders.metadata.location().trim_end_matches('/')
    );
    // A path copied from object storage is usually longer than the 64 characters a name search keeps
    assert!(data_file.chars().count() > 64, "{data_file}");

    assert_eq!(
        search(ctx, warehouse_id, &data_file).await,
        vec![(vec!["sales".to_string()], "orders".to_string(), None)]
    );
}

#[sqlx::test]
async fn test_search_tabular_by_path_outside_every_table(pool: PgPool) {
    let (ctx, warehouse_id) = sales_warehouse(pool).await;
    let prefix = warehouse_id.to_string();
    let orders = create_table(ctx.clone(), &prefix, "sales", "orders", false)
        .await
        .unwrap();

    // The directory holding `orders` is inside the warehouse, but no table lives under `other`.
    let orders_location = orders.metadata.location().trim_end_matches('/');
    let parent = orders_location
        .rsplit_once('/')
        .map(|(parent, _)| parent)
        .unwrap();
    let path = format!("{parent}/other/data/orders.parquet");

    assert_eq!(search(ctx, warehouse_id, &path).await, vec![]);
}

#[sqlx::test]
async fn test_search_tabular_by_name_that_looks_like_a_url(pool: PgPool) {
    let (ctx, warehouse_id) = sales_warehouse(pool).await;
    create_table(
        ctx.clone(),
        warehouse_id.to_string(),
        "sales",
        "http://feed",
        false,
    )
    .await
    .unwrap();

    let found = search(ctx, warehouse_id, "http://feed").await;

    let names = found
        .iter()
        .map(|(namespace, name, _)| (namespace.clone(), name.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(names, vec![(vec!["sales".to_string()], "http://feed")]);
    // Found by the name search, which ranks by distance; a location match carries none.
    assert!(found[0].2.is_some(), "{found:?}");
}
