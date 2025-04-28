use iceberg::NamespaceIdent;
use iceberg_ext::catalog::rest::CreateNamespaceRequest;
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    api::{
        iceberg::{
            types::Prefix,
            v1::{
                namespace::{NamespaceDropFlags, NamespaceService},
                NamespaceParameters,
            },
        },
        management::v1::{
            bootstrap::{BootstrapRequest, Service as _},
            warehouse::{CreateWarehouseRequest, Service},
            ApiServer, DeleteWarehouseQuery,
        },
    },
    catalog::CatalogServer,
    service::{authz::AllowAllAuthorizer, task_queue::TaskQueueConfig},
    tests::{get_api_context, random_request_metadata, spawn_drop_queues},
};

#[sqlx::test]
async fn test_cannot_drop_warehouse_before_purge_tasks_completed(pool: PgPool) {
    let storage_profile = crate::tests::test_io_profile();
    let authorizer = AllowAllAuthorizer {};
    let q_config = TaskQueueConfig {
        max_retries: 1,
        max_age: chrono::Duration::seconds(60),
        poll_interval: std::time::Duration::from_secs(1),
        num_workers: 2,
    };
    let api_context = get_api_context(&pool, authorizer, Some(q_config));

    // Bootstrap
    ApiServer::bootstrap(
        api_context.clone(),
        random_request_metadata(),
        BootstrapRequest::builder().accept_terms_of_use().build(),
    )
    .await
    .unwrap();

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
    let prefix = Some(Prefix(warehouse.warehouse_id.to_string()));
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
            &warehouse.warehouse_id.to_string(),
            &ns_name.to_string(),
            &table_name,
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
        warehouse.warehouse_id,
        DeleteWarehouseQuery::builder().build(),
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .expect_err("Warehouse deletion should fail due to purge tasks");

    // Spawn task queue workers
    spawn_drop_queues(&api_context);

    // Wait for tables to be dropped
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    // Drop warehouse - this should succeed now
    ApiServer::delete_warehouse(
        warehouse.warehouse_id,
        DeleteWarehouseQuery::builder().build(),
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .expect("Warehouse deletion should succeed after purge tasks are completed");
}
