use iceberg::NamespaceIdent;
use iceberg_ext::catalog::rest::CreateNamespaceRequest;
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    api::{
        iceberg::{
            types::Prefix,
            v1::{
                NamespaceParameters,
                namespace::{NamespaceDropFlags, NamespaceService},
            },
        },
        management::v1::{
            ApiServer, DeleteWarehouseQuery,
            warehouse::{CreateWarehouseRequest, Service, TabularDeleteProfile},
        },
    },
    server::CatalogServer,
    service::authz::AllowAllAuthorizer,
    tests::{random_request_metadata, spawn_build_in_queues},
};

#[sqlx::test]
async fn test_cannot_drop_warehouse_before_purge_tasks_completed(pool: PgPool) {
    let storage_profile = crate::tests::memory_io_profile();
    let authorizer = AllowAllAuthorizer::default();

    let (api_context, _) = crate::tests::setup(
        pool.clone(),
        storage_profile.clone(),
        None,
        authorizer,
        TabularDeleteProfile::default(),
        None,
        1,
        None,
    )
    .await;

    // Create a warehouse
    let warehouse_name = format!("test_warehouse_{}", Uuid::now_v7());
    let warehouse = ApiServer::create_warehouse(
        CreateWarehouseRequest::builder()
            .warehouse_name(warehouse_name.clone())
            .storage_profile(storage_profile)
            .build(),
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    // Create namespace
    let ns_name = NamespaceIdent::new(format!("test_namespace_{}", Uuid::now_v7()));
    let prefix = Some(Prefix(warehouse.warehouse_id().to_string()));
    let _ = CatalogServer::create_namespace(
        prefix.clone(),
        CreateNamespaceRequest {
            namespace: ns_name.clone(),
            properties: None,
        },
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    // Create tables
    for i in 0..2 {
        let table_name = format!("table_{i}");
        let _ = crate::tests::create_table(
            api_context.clone(),
            &warehouse.warehouse_id().to_string(),
            &ns_name.to_string(),
            &table_name,
            false,
        )
        .await
        .unwrap();
    }

    // Delete namespace recursively with purge
    CatalogServer::drop_namespace(
        NamespaceParameters {
            prefix: prefix.clone(),
            namespace: ns_name.clone(),
        },
        NamespaceDropFlags::builder().recursive().purge().build(),
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    // Drop warehouse - this should fail due to purge tasks
    ApiServer::delete_warehouse(
        warehouse.warehouse_id(),
        DeleteWarehouseQuery::builder().build(),
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .expect_err("Warehouse deletion should fail due to purge tasks");

    // Spawn task queue workers
    let cancellation_token = crate::CancellationToken::new();
    let queues_handle = spawn_build_in_queues(
        &api_context,
        Some(std::time::Duration::from_secs(1)),
        cancellation_token.clone(),
    )
    .await;

    // Drop warehouse — poll until purge tasks complete to avoid flakiness
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match ApiServer::delete_warehouse(
            warehouse.warehouse_id(),
            DeleteWarehouseQuery::builder().build(),
            api_context.clone(),
            random_request_metadata(),
        )
        .await
        {
            Ok(()) => break,
            Err(_e) if std::time::Instant::now() < deadline => {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            Err(e) => panic!("Warehouse deletion did not complete within 5s: {e:?}"),
        }
    }
    cancellation_token.cancel();
    queues_handle.await.unwrap();
}
