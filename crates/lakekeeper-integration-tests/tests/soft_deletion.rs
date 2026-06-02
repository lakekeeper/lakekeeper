use std::collections::HashMap;

use futures::future::join_all;
use iceberg::{NamespaceIdent, TableIdent};
use iceberg_ext::catalog::rest::CreateNamespaceRequest;
use lakekeeper::{
    api::{
        iceberg::{
            types::Prefix,
            v1::{
                DropParams, ListTablesQuery, LoadTableResultOrNotModified, NamespaceParameters,
                TableParameters,
                namespace::NamespaceService,
                tables::{LoadTableRequest, TablesService},
            },
        },
        management::v1::{
            ApiServer,
            tasks::{ListTasksRequest, Service as _, TaskStatus},
            warehouse::{
                ListDeletedTabularsQuery, Service, TabularDeleteProfile, UndropTabularsRequest,
            },
        },
    },
    server::{CatalogServer, NAMESPACE_ID_PROPERTY},
    service::{
        NamespaceId, TabularId, authz::AllowAllAuthorizer,
        tasks::tabular_expiration_queue::QUEUE_NAME as EXPIRATION_QUEUE_NAME,
    },
};
use lakekeeper_integration_tests::random_request_metadata;
use sqlx::PgPool;
use uuid::Uuid;

#[sqlx::test]
async fn test_soft_deletion(pool: PgPool) {
    let storage_profile = lakekeeper_integration_tests::memory_io_profile();
    let authorizer = AllowAllAuthorizer::default();

    let (api_context, warehouse) = lakekeeper_integration_tests::setup(
        pool.clone(),
        storage_profile.clone(),
        None,
        authorizer,
        TabularDeleteProfile::Soft {
            expiration_seconds: chrono::Duration::seconds(300),
        },
        None,
        1,
        None,
    )
    .await;

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
                lakekeeper_integration_tests::create_table(
                    api_context.clone(),
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
    let undrop_table_id =
        TabularId::Table((*table_name_to_uuid.get(undrop_table_name).unwrap()).into());

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
        LoadTableRequest::builder().build(),
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let LoadTableResultOrNotModified::LoadTableResult(table) = table else {
        panic!("Expected LoadTableResult, got NotModified");
    };

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

#[sqlx::test]
async fn test_soft_delete_and_undrop_generic_table(pool: PgPool) {
    use lakekeeper::api::{
        data::v1::generic_tables::{
            GenericTableParameters, GenericTableService as _, ListGenericTablesQuery,
        },
        iceberg::types::DropParams,
    };

    let storage_profile = lakekeeper_integration_tests::memory_io_profile();
    let authorizer = AllowAllAuthorizer::default();

    let (api_context, warehouse) = lakekeeper_integration_tests::setup(
        pool.clone(),
        storage_profile,
        None,
        authorizer,
        TabularDeleteProfile::Soft {
            expiration_seconds: chrono::Duration::seconds(300),
        },
        None,
        1,
        None,
    )
    .await;

    let prefix = warehouse.warehouse_id.to_string();
    let ns_name = format!("test_namespace_{}", Uuid::now_v7());

    lakekeeper_integration_tests::create_ns(api_context.clone(), prefix.clone(), ns_name.clone())
        .await;

    let gt_name = "my_gt";
    lakekeeper_integration_tests::create_generic_table(
        api_context.clone(),
        prefix.clone(),
        ns_name.clone(),
        gt_name,
    )
    .await
    .unwrap();

    let listed = CatalogServer::list_generic_tables(
        NamespaceParameters {
            prefix: Some(Prefix(prefix.clone())),
            namespace: NamespaceIdent::new(ns_name.clone()),
        },
        ListGenericTablesQuery::default(),
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let gt_id = listed
        .identifiers
        .iter()
        .find(|i| i.name == gt_name)
        .and_then(|i| i.id)
        .expect("generic table id should be returned by list");

    // Soft-delete via drop
    CatalogServer::drop_generic_table(
        GenericTableParameters {
            prefix: Some(Prefix(prefix.clone())),
            namespace: NamespaceIdent::new(ns_name.clone()),
            table_name: gt_name.to_string(),
        },
        DropParams {
            purge_requested: false,
            force: false,
        },
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    // Active list excludes the dropped generic table.
    let listed = CatalogServer::list_generic_tables(
        NamespaceParameters {
            prefix: Some(Prefix(prefix.clone())),
            namespace: NamespaceIdent::new(ns_name.clone()),
        },
        ListGenericTablesQuery::default(),
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert!(!listed.identifiers.iter().any(|i| i.name == gt_name));

    // The dropped GT appears in the soft-deleted listing.
    let deleted = ApiServer::list_soft_deleted_tabulars(
        warehouse.warehouse_id,
        ListDeletedTabularsQuery::default(),
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap()
    .tabulars;
    assert!(
        deleted.iter().any(|t| t.name == gt_name && t.id == *gt_id),
        "expected dropped generic table in soft-deleted listing: {deleted:?}",
    );

    // Undrop: the GT is listable again, with the same id.
    ApiServer::undrop_tabulars(
        warehouse.warehouse_id,
        random_request_metadata(),
        UndropTabularsRequest {
            targets: vec![TabularId::GenericTable(gt_id)],
        },
        api_context.clone(),
    )
    .await
    .unwrap();

    let listed = CatalogServer::list_generic_tables(
        NamespaceParameters {
            prefix: Some(Prefix(prefix.clone())),
            namespace: NamespaceIdent::new(ns_name.clone()),
        },
        ListGenericTablesQuery::default(),
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert!(
        listed
            .identifiers
            .iter()
            .any(|i| i.name == gt_name && i.id == Some(gt_id)),
        "undropped generic table should reappear in list with the same id",
    );
}
