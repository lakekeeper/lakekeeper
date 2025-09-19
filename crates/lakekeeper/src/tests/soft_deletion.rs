use std::collections::HashMap;

use futures::future::join_all;
use iceberg::{NamespaceIdent, TableIdent};
use iceberg_ext::catalog::rest::CreateNamespaceRequest;
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    api::{
        iceberg::{
            types::Prefix,
            v1::{
                namespace::NamespaceService, tables::TablesService, DataAccessMode, DropParams,
                ListTablesQuery, NamespaceParameters, TableParameters,
            },
        },
        management::v1::{
            bootstrap::{BootstrapRequest, Service as _},
            tasks::{ListTasksRequest, Service as _, TaskStatus},
            warehouse::{
                CreateWarehouseRequest, ListDeletedTabularsQuery, Service, TabularDeleteProfile,
                UndropTabularsRequest,
            },
            ApiServer,
        },
    },
    catalog::{CatalogServer, NAMESPACE_ID_PROPERTY},
    service::{
        authz::AllowAllAuthorizer,
        task_queue::tabular_expiration_queue::QUEUE_NAME as EXPIRATION_QUEUE_NAME, NamespaceId,
        TabularId,
    },
    tests::{get_api_context, random_request_metadata},
};

#[sqlx::test]
async fn test_soft_deletion(pool: PgPool) {
    let storage_profile = crate::tests::memory_io_profile();
    let authorizer = AllowAllAuthorizer::default();

    let api_context = get_api_context(&pool, authorizer).await;

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
            .delete_profile(TabularDeleteProfile::Soft {
                expiration_seconds: chrono::Duration::seconds(300),
            })
            .build(),
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    // Create namespace
    let ns_ident = NamespaceIdent::new(format!("test_namespace_{}", Uuid::now_v7()));
    let prefix = Some(Prefix(warehouse.warehouse_id.to_string()));
    let create_ns_response = CatalogServer::create_namespace(
        prefix.clone(),
        CreateNamespaceRequest {
            namespace: ns_ident.clone(),
            properties: None,
        },
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let ns_id = NamespaceId::from(
        uuid::Uuid::parse_str(
            create_ns_response
                .properties
                .unwrap()
                .get(NAMESPACE_ID_PROPERTY)
                .unwrap(),
        )
        .unwrap(),
    );

    // Create tables in parallel
    let create_futs = (0..20).map(|i| {
        let api_context = api_context.clone();
        let warehouse_id = warehouse.warehouse_id.to_string();
        let ns_name = ns_ident.to_string();
        let table_name = format!("table_{i}");
        tokio::spawn(async move {
            (
                table_name.clone(),
                crate::tests::create_table(
                    api_context,
                    &warehouse_id,
                    &ns_name,
                    &table_name,
                    false,
                )
                .await
                .unwrap()
                .metadata
                .uuid(),
            )
        })
    });
    let table_name_to_uuid = join_all(create_futs)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect::<HashMap<_, _>>();

    // Delete half of the tables in parallel
    let delete_futs = (0..10).map(|i| {
        let api_context = api_context.clone();
        let warehouse_id = warehouse.warehouse_id.to_string();
        let ns_ident_clone = ns_ident.clone();
        let table_name = format!("table_{i}");
        let table_parameters = TableParameters {
            prefix: Some(Prefix(warehouse_id.clone())),
            table: TableIdent::new(ns_ident_clone, table_name),
        };
        tokio::spawn(async move {
            CatalogServer::drop_table(
                table_parameters,
                DropParams {
                    purge_requested: true,
                    force: false,
                },
                api_context,
                random_request_metadata(),
            )
            .await
            .unwrap();
        })
    });
    let drops = join_all(delete_futs).await;
    for j in drops {
        j.expect("drop_table task panicked");
    }

    // Verify that half of the tables are dropped
    let tables = CatalogServer::list_tables(
        NamespaceParameters {
            prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
            namespace: ns_ident.clone(),
        },
        ListTablesQuery {
            return_uuids: true,
            ..Default::default()
        },
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let identifiers = tables.identifiers;
    assert_eq!(identifiers.len(), 10);
    for i in 10..20 {
        let table_name = format!("table_{i}");
        assert!(identifiers.contains(&TableIdent::new(ns_ident.clone(), table_name)));
    }
    for i in 0..10 {
        let table_name = format!("table_{i}");
        assert!(!identifiers.contains(&TableIdent::new(ns_ident.clone(), table_name)));
    }

    // List tasks and check that expiration tasks are enqueued
    let tasks = ApiServer::list_tasks(
        warehouse.warehouse_id,
        ListTasksRequest {
            status: Some(vec![TaskStatus::Scheduled]),
            queue_name: Some(vec![EXPIRATION_QUEUE_NAME.clone()]),
            ..Default::default()
        },
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap()
    .tasks;

    assert_eq!(tasks.len(), 10);
    for task in tasks {
        assert_eq!(&task.queue_name, &*EXPIRATION_QUEUE_NAME);
        assert_eq!(task.status, TaskStatus::Scheduled);
    }

    // List deleted tabulars
    let deleted_tabulars = ApiServer::list_soft_deleted_tabulars(
        warehouse.warehouse_id,
        ListDeletedTabularsQuery {
            namespace_id: Some(ns_id),
            ..Default::default()
        },
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap()
    .tabulars;

    assert_eq!(deleted_tabulars.len(), 10);
    for i in 0..10 {
        let table_name = format!("table_{i}");
        assert!(deleted_tabulars.iter().any(|t| { t.name == table_name }));
    }

    // Un-delete one of the deleted tables
    let undrop_table_name = "table_4";
    let undrop_table_id = TabularId::Table(*table_name_to_uuid.get(undrop_table_name).unwrap());

    ApiServer::undrop_tabulars(
        warehouse.warehouse_id,
        random_request_metadata(),
        UndropTabularsRequest {
            targets: vec![undrop_table_id],
        },
        api_context.clone(),
    )
    .await
    .unwrap();

    // Verify we can load the table
    let table = CatalogServer::load_table(
        TableParameters {
            prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
            table: TableIdent::new(ns_ident.clone(), undrop_table_name.to_string()),
        },
        DataAccessMode::ClientManaged,
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert_eq!(table.metadata.uuid(), *undrop_table_id);

    // Verify listing tables shows the undropped table
    let tables = CatalogServer::list_tables(
        NamespaceParameters {
            prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
            namespace: ns_ident.clone(),
        },
        ListTablesQuery {
            ..Default::default()
        },
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let identifiers = tables.identifiers;
    assert_eq!(identifiers.len(), 11);
    assert!(identifiers.contains(&TableIdent::new(
        ns_ident.clone(),
        undrop_table_name.to_string()
    )));

    // List deleted tabulars, should now be 1 less
    let deleted_tabulars = ApiServer::list_soft_deleted_tabulars(
        warehouse.warehouse_id,
        ListDeletedTabularsQuery {
            namespace_id: Some(ns_id),
            ..Default::default()
        },
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap()
    .tabulars;
    assert_eq!(deleted_tabulars.len(), 9);
}
