// Extracted from crates/lakekeeper/src/api/management/v1/check.rs.
// VAK-437 split.

use std::{collections::BTreeMap, sync::Arc};

use lakekeeper::{
    api::{
        RequestMetadata,
        iceberg::{
            types::Prefix,
            v1::{DataAccess, NamespaceParameters, tables::TablesService},
        },
        management::v1::{
            check::{
                CatalogActionCheckItem, CatalogActionCheckOperation,
                CatalogActionsBatchCheckRequest, NamespaceIdentOrUuid, RoleAssignee,
                TabularIdentOrUuid, UserOrRole, check_internal,
            },
            warehouse::TabularDeleteProfile,
        },
    },
    server::CatalogServer,
    service::{
        CatalogNamespaceOps, NamespaceIdent, UserId,
        authz::{
            CatalogGenericTableAction, CatalogNamespaceAction, CatalogServerAction,
            CatalogTableAction, CatalogWarehouseAction, tests::HidingAuthorizer,
        },
    },
};
use lakekeeper_integration_tests::{create_generic_table, create_table_request};
use lakekeeper_storage_postgres::{CatalogState, PostgresBackend};

#[sqlx::test]
async fn test_check_internal_basic_permissions(pool: sqlx::PgPool) {
    let prof = lakekeeper_integration_tests::memory_io_profile();
    let authz = HidingAuthorizer::new();

    let (api_context, test_warehouse) = lakekeeper_integration_tests::setup_simple(
        pool.clone(),
        prof,
        None,
        authz.clone(),
        TabularDeleteProfile::Hard {},
        None,
    )
    .await;

    let metadata = RequestMetadata::new_unauthenticated();

    // Create a namespace
    let ns_name = "test_namespace";
    let create_ns_resp = lakekeeper_integration_tests::create_ns(
        api_context.clone(),
        test_warehouse.warehouse_id.to_string(),
        ns_name.to_string(),
    )
    .await;

    let ns_params = NamespaceParameters {
        prefix: Some(Prefix(test_warehouse.warehouse_id.to_string())),
        namespace: create_ns_resp.namespace.clone(),
    };

    // Create a table
    let table_name = "test_table";
    let create_table_resp = CatalogServer::create_table(
        ns_params.clone(),
        create_table_request(Some(table_name.to_string()), None),
        DataAccess::not_specified(),
        api_context.clone(),
        metadata.clone(),
    )
    .await
    .unwrap();

    let table_id = create_table_resp.metadata.uuid();

    // Get the namespace ID from the catalog
    let catalog_state = CatalogState::from_pools(pool.clone(), pool.clone());
    let namespace_hierarchy = PostgresBackend::get_namespace(
        test_warehouse.warehouse_id,
        create_ns_resp.namespace.clone(),
        catalog_state.clone(),
    )
    .await
    .unwrap()
    .unwrap();
    let namespace_id = namespace_hierarchy.namespace_id();

    // Test 1: Check server action (should be allowed by default)
    let request = CatalogActionsBatchCheckRequest {
        checks: vec![CatalogActionCheckItem {
            id: Some("server-check-1".to_string()),
            identity: None,
            operation: CatalogActionCheckOperation::Server {
                action: CatalogServerAction::CreateProject {
                    name: None,
                    project_id: None,
                },
            },
        }],
        error_on_not_found: false,
    };

    let response = check_internal(api_context.clone(), metadata.clone(), request)
        .await
        .unwrap();

    assert_eq!(response.results.len(), 1);
    assert_eq!(response.results[0].id, Some("server-check-1".to_string()));
    assert!(response.results[0].allowed);

    // Test 2: Check warehouse action by ID
    let request = CatalogActionsBatchCheckRequest {
        checks: vec![CatalogActionCheckItem {
            id: Some("warehouse-check-1".to_string()),
            identity: None,
            operation: CatalogActionCheckOperation::Warehouse {
                action: CatalogWarehouseAction::Use,
                warehouse_id: test_warehouse.warehouse_id,
            },
        }],
        error_on_not_found: false,
    };

    let response = check_internal(api_context.clone(), metadata.clone(), request)
        .await
        .unwrap();

    assert_eq!(response.results.len(), 1);
    assert_eq!(
        response.results[0].id,
        Some("warehouse-check-1".to_string())
    );
    assert!(response.results[0].allowed);

    // Test 3: Check namespace action by ID
    let request = CatalogActionsBatchCheckRequest {
        checks: vec![CatalogActionCheckItem {
            id: Some("namespace-check-1".to_string()),
            identity: None,
            operation: CatalogActionCheckOperation::Namespace {
                action: CatalogNamespaceAction::CreateTable {
                    name: None,
                    table_id: None,
                    properties: Arc::default(),
                },
                namespace: NamespaceIdentOrUuid::Id {
                    namespace_id,
                    warehouse_id: test_warehouse.warehouse_id,
                },
            },
        }],
        error_on_not_found: false,
    };

    let response = check_internal(api_context.clone(), metadata.clone(), request)
        .await
        .unwrap();

    assert_eq!(response.results.len(), 1);
    assert_eq!(
        response.results[0].id,
        Some("namespace-check-1".to_string())
    );
    assert!(response.results[0].allowed);

    // Test 4: Check namespace action by name
    let request = CatalogActionsBatchCheckRequest {
        checks: vec![CatalogActionCheckItem {
            id: Some("namespace-check-2".to_string()),
            identity: None,
            operation: CatalogActionCheckOperation::Namespace {
                action: CatalogNamespaceAction::CreateTable {
                    name: None,
                    table_id: None,
                    properties: Arc::default(),
                },
                namespace: NamespaceIdentOrUuid::Name {
                    namespace: create_ns_resp.namespace.clone(),
                    warehouse_id: test_warehouse.warehouse_id,
                },
            },
        }],
        error_on_not_found: false,
    };

    let response = check_internal(api_context.clone(), metadata.clone(), request)
        .await
        .unwrap();

    assert_eq!(response.results.len(), 1);
    assert_eq!(
        response.results[0].id,
        Some("namespace-check-2".to_string())
    );
    assert!(response.results[0].allowed);

    // Test 5: Check table action by ID
    let request = CatalogActionsBatchCheckRequest {
        checks: vec![CatalogActionCheckItem {
            id: Some("table-check-1".to_string()),
            identity: None,
            operation: CatalogActionCheckOperation::Table {
                action: CatalogTableAction::ReadData,
                table: TabularIdentOrUuid::IdInWarehouse {
                    warehouse_id: test_warehouse.warehouse_id,
                    table_id,
                },
            },
        }],
        error_on_not_found: false,
    };

    let response = check_internal(api_context.clone(), metadata.clone(), request)
        .await
        .unwrap();

    assert_eq!(response.results.len(), 1);
    assert_eq!(response.results[0].id, Some("table-check-1".to_string()));
    assert!(response.results[0].allowed);

    // Test 6: Check table action by name
    let request = CatalogActionsBatchCheckRequest {
        checks: vec![CatalogActionCheckItem {
            id: Some("table-check-2".to_string()),
            identity: None,
            operation: CatalogActionCheckOperation::Table {
                action: CatalogTableAction::ReadData,
                table: TabularIdentOrUuid::Name {
                    namespace: create_ns_resp.namespace.clone(),
                    table: table_name.to_string(),
                    warehouse_id: test_warehouse.warehouse_id,
                },
            },
        }],
        error_on_not_found: false,
    };

    let response = check_internal(api_context.clone(), metadata.clone(), request)
        .await
        .unwrap();

    assert_eq!(response.results.len(), 1);
    assert_eq!(response.results[0].id, Some("table-check-2".to_string()));
    assert!(response.results[0].allowed);

    // Test 7: Batch check with multiple operations
    let request = CatalogActionsBatchCheckRequest {
        checks: vec![
            CatalogActionCheckItem {
                id: Some("batch-1".to_string()),
                identity: None,
                operation: CatalogActionCheckOperation::Server {
                    action: CatalogServerAction::CreateProject {
                        name: None,
                        project_id: None,
                    },
                },
            },
            CatalogActionCheckItem {
                id: Some("batch-2".to_string()),
                identity: None,
                operation: CatalogActionCheckOperation::Warehouse {
                    action: CatalogWarehouseAction::Use,
                    warehouse_id: test_warehouse.warehouse_id,
                },
            },
            CatalogActionCheckItem {
                id: Some("batch-3".to_string()),
                identity: None,
                operation: CatalogActionCheckOperation::Table {
                    action: CatalogTableAction::ReadData,
                    table: TabularIdentOrUuid::IdInWarehouse {
                        warehouse_id: test_warehouse.warehouse_id,
                        table_id,
                    },
                },
            },
        ],
        error_on_not_found: false,
    };

    let response = check_internal(api_context.clone(), metadata.clone(), request)
        .await
        .unwrap();

    assert_eq!(response.results.len(), 3);
    assert!(response.results.iter().all(|r| r.allowed));
    assert_eq!(response.results[0].id, Some("batch-1".to_string()));
    assert_eq!(response.results[1].id, Some("batch-2".to_string()));
    assert_eq!(response.results[2].id, Some("batch-3".to_string()));
}

#[sqlx::test]
async fn test_check_internal_hidden_warehouse(pool: sqlx::PgPool) {
    let prof = lakekeeper_integration_tests::memory_io_profile();
    let authz = HidingAuthorizer::new();

    let (api_context, test_warehouse) = lakekeeper_integration_tests::setup_simple(
        pool.clone(),
        prof,
        None,
        authz.clone(),
        TabularDeleteProfile::Hard {},
        None,
    )
    .await;

    let metadata = RequestMetadata::new_unauthenticated();

    // First verify warehouse is accessible
    let request = CatalogActionsBatchCheckRequest {
        checks: vec![CatalogActionCheckItem {
            id: Some("visible-warehouse".to_string()),
            identity: None,
            operation: CatalogActionCheckOperation::Warehouse {
                action: CatalogWarehouseAction::Use,
                warehouse_id: test_warehouse.warehouse_id,
            },
        }],
        error_on_not_found: false,
    };

    let response = check_internal(api_context.clone(), metadata.clone(), request)
        .await
        .unwrap();

    assert_eq!(response.results.len(), 1);
    assert!(response.results[0].allowed); // Should be allowed initially

    // Now hide the warehouse
    authz.hide(&format!("warehouse:{}", test_warehouse.warehouse_id));

    // Check warehouse action again - should now be denied
    let request = CatalogActionsBatchCheckRequest {
        checks: vec![CatalogActionCheckItem {
            id: Some("hidden-warehouse".to_string()),
            identity: None,
            operation: CatalogActionCheckOperation::Warehouse {
                action: CatalogWarehouseAction::Use,
                warehouse_id: test_warehouse.warehouse_id,
            },
        }],
        error_on_not_found: false,
    };

    let response = check_internal(api_context.clone(), metadata.clone(), request)
        .await
        .unwrap();

    assert_eq!(response.results.len(), 1);
    assert_eq!(response.results[0].id, Some("hidden-warehouse".to_string()));
    assert!(!response.results[0].allowed); // Should now be denied
}

#[sqlx::test]
async fn test_check_internal_hidden_namespace(pool: sqlx::PgPool) {
    let prof = lakekeeper_integration_tests::memory_io_profile();
    let authz = HidingAuthorizer::new();

    let (api_context, test_warehouse) = lakekeeper_integration_tests::setup_simple(
        pool.clone(),
        prof,
        None,
        authz.clone(),
        TabularDeleteProfile::Hard {},
        None,
    )
    .await;

    let metadata = RequestMetadata::new_unauthenticated();

    // Create a namespace
    let create_ns_resp = lakekeeper_integration_tests::create_ns(
        api_context.clone(),
        test_warehouse.warehouse_id.to_string(),
        "test_namespace".to_string(),
    )
    .await;

    // Get the namespace ID
    let catalog_state = CatalogState::from_pools(pool.clone(), pool.clone());
    let namespace_hierarchy = PostgresBackend::get_namespace(
        test_warehouse.warehouse_id,
        create_ns_resp.namespace.clone(),
        catalog_state.clone(),
    )
    .await
    .unwrap()
    .unwrap();
    let namespace_id = namespace_hierarchy.namespace_id();

    // First verify namespace is accessible by ID
    let request = CatalogActionsBatchCheckRequest {
        checks: vec![CatalogActionCheckItem {
            id: Some("visible-namespace-id".to_string()),
            identity: None,
            operation: CatalogActionCheckOperation::Namespace {
                action: CatalogNamespaceAction::CreateTable {
                    name: None,
                    table_id: None,
                    properties: Arc::default(),
                },
                namespace: NamespaceIdentOrUuid::Id {
                    namespace_id,
                    warehouse_id: test_warehouse.warehouse_id,
                },
            },
        }],
        error_on_not_found: false,
    };

    let response = check_internal(api_context.clone(), metadata.clone(), request)
        .await
        .unwrap();

    assert_eq!(response.results.len(), 1);
    assert!(response.results[0].allowed); // Should be allowed initially

    // Verify namespace is accessible by name
    let request = CatalogActionsBatchCheckRequest {
        checks: vec![CatalogActionCheckItem {
            id: Some("visible-namespace-name".to_string()),
            identity: None,
            operation: CatalogActionCheckOperation::Namespace {
                action: CatalogNamespaceAction::CreateTable {
                    name: None,
                    table_id: None,
                    properties: Arc::default(),
                },
                namespace: NamespaceIdentOrUuid::Name {
                    namespace: create_ns_resp.namespace.clone(),
                    warehouse_id: test_warehouse.warehouse_id,
                },
            },
        }],
        error_on_not_found: false,
    };

    let response = check_internal(api_context.clone(), metadata.clone(), request)
        .await
        .unwrap();

    assert_eq!(response.results.len(), 1);
    assert!(response.results[0].allowed); // Should be allowed initially

    // Now hide the namespace
    authz.hide(&format!("namespace:{namespace_id}"));

    // Check namespace action by ID - should now be denied
    let request = CatalogActionsBatchCheckRequest {
        checks: vec![CatalogActionCheckItem {
            id: Some("hidden-namespace-id".to_string()),
            identity: None,
            operation: CatalogActionCheckOperation::Namespace {
                action: CatalogNamespaceAction::CreateTable {
                    name: None,
                    table_id: None,
                    properties: Arc::default(),
                },
                namespace: NamespaceIdentOrUuid::Id {
                    namespace_id,
                    warehouse_id: test_warehouse.warehouse_id,
                },
            },
        }],
        error_on_not_found: false,
    };

    let response = check_internal(api_context.clone(), metadata.clone(), request)
        .await
        .unwrap();

    assert_eq!(response.results.len(), 1);
    assert!(!response.results[0].allowed); // Should now be denied

    // Check namespace action by name - should also be denied
    let request = CatalogActionsBatchCheckRequest {
        checks: vec![CatalogActionCheckItem {
            id: Some("hidden-namespace-name".to_string()),
            identity: None,
            operation: CatalogActionCheckOperation::Namespace {
                action: CatalogNamespaceAction::CreateTable {
                    name: None,
                    table_id: None,
                    properties: Arc::default(),
                },
                namespace: NamespaceIdentOrUuid::Name {
                    namespace: create_ns_resp.namespace.clone(),
                    warehouse_id: test_warehouse.warehouse_id,
                },
            },
        }],
        error_on_not_found: false,
    };

    let response = check_internal(api_context.clone(), metadata.clone(), request)
        .await
        .unwrap();

    assert_eq!(response.results.len(), 1);
    assert!(!response.results[0].allowed); // Should now be denied
}

#[sqlx::test]
async fn test_check_internal_hidden_table(pool: sqlx::PgPool) {
    let prof = lakekeeper_integration_tests::memory_io_profile();
    let authz = HidingAuthorizer::new();

    let (api_context, test_warehouse) = lakekeeper_integration_tests::setup_simple(
        pool.clone(),
        prof,
        None,
        authz.clone(),
        TabularDeleteProfile::Hard {},
        None,
    )
    .await;

    let metadata = RequestMetadata::new_unauthenticated();

    // Create a namespace
    let create_ns_resp = lakekeeper_integration_tests::create_ns(
        api_context.clone(),
        test_warehouse.warehouse_id.to_string(),
        "test_namespace".to_string(),
    )
    .await;

    let ns_params = NamespaceParameters {
        prefix: Some(Prefix(test_warehouse.warehouse_id.to_string())),
        namespace: create_ns_resp.namespace.clone(),
    };

    // Create a table
    let table_name = "test_table";
    let create_table_resp = CatalogServer::create_table(
        ns_params.clone(),
        create_table_request(Some(table_name.to_string()), None),
        DataAccess::not_specified(),
        api_context.clone(),
        metadata.clone(),
    )
    .await
    .unwrap();

    let table_id = create_table_resp.metadata.uuid();

    // First verify table is accessible by ID
    let request = CatalogActionsBatchCheckRequest {
        checks: vec![CatalogActionCheckItem {
            id: Some("visible-table-id".to_string()),
            identity: None,
            operation: CatalogActionCheckOperation::Table {
                action: CatalogTableAction::ReadData,
                table: TabularIdentOrUuid::IdInWarehouse {
                    warehouse_id: test_warehouse.warehouse_id,
                    table_id,
                },
            },
        }],
        error_on_not_found: false,
    };

    let response = check_internal(api_context.clone(), metadata.clone(), request)
        .await
        .unwrap();

    assert_eq!(response.results.len(), 1);
    assert!(response.results[0].allowed); // Should be allowed initially

    // Verify table is accessible by name
    let request = CatalogActionsBatchCheckRequest {
        checks: vec![CatalogActionCheckItem {
            id: Some("visible-table-name".to_string()),
            identity: None,
            operation: CatalogActionCheckOperation::Table {
                action: CatalogTableAction::ReadData,
                table: TabularIdentOrUuid::Name {
                    namespace: create_ns_resp.namespace.clone(),
                    table: table_name.to_string(),
                    warehouse_id: test_warehouse.warehouse_id,
                },
            },
        }],
        error_on_not_found: false,
    };

    let response = check_internal(api_context.clone(), metadata.clone(), request)
        .await
        .unwrap();

    assert_eq!(response.results.len(), 1);
    assert!(response.results[0].allowed); // Should be allowed initially

    // Now hide the table
    authz.hide(&format!(
        "table:{}/{}",
        test_warehouse.warehouse_id, table_id
    ));

    // Check table action by ID - should now be denied
    let request = CatalogActionsBatchCheckRequest {
        checks: vec![CatalogActionCheckItem {
            id: Some("hidden-table-id".to_string()),
            identity: None,
            operation: CatalogActionCheckOperation::Table {
                action: CatalogTableAction::ReadData,
                table: TabularIdentOrUuid::IdInWarehouse {
                    warehouse_id: test_warehouse.warehouse_id,
                    table_id,
                },
            },
        }],
        error_on_not_found: false,
    };

    let response = check_internal(api_context.clone(), metadata.clone(), request)
        .await
        .unwrap();

    assert_eq!(response.results.len(), 1);
    assert!(!response.results[0].allowed); // Should now be denied

    // Check table action by name - should also be denied
    let request = CatalogActionsBatchCheckRequest {
        checks: vec![CatalogActionCheckItem {
            id: Some("hidden-table-name".to_string()),
            identity: None,
            operation: CatalogActionCheckOperation::Table {
                action: CatalogTableAction::ReadData,
                table: TabularIdentOrUuid::Name {
                    namespace: create_ns_resp.namespace.clone(),
                    table: table_name.to_string(),
                    warehouse_id: test_warehouse.warehouse_id,
                },
            },
        }],
        error_on_not_found: false,
    };

    let response = check_internal(api_context.clone(), metadata.clone(), request)
        .await
        .unwrap();

    assert_eq!(response.results.len(), 1);
    assert!(!response.results[0].allowed); // Should now be denied
}

#[sqlx::test]
async fn test_check_internal_mixed_visibility(pool: sqlx::PgPool) {
    let prof = lakekeeper_integration_tests::memory_io_profile();
    let authz = HidingAuthorizer::new();

    let (api_context, test_warehouse) = lakekeeper_integration_tests::setup_simple(
        pool.clone(),
        prof,
        None,
        authz.clone(),
        TabularDeleteProfile::Hard {},
        None,
    )
    .await;

    let metadata = RequestMetadata::new_unauthenticated();

    // Create two namespaces
    let ns1_resp = lakekeeper_integration_tests::create_ns(
        api_context.clone(),
        test_warehouse.warehouse_id.to_string(),
        "visible_ns".to_string(),
    )
    .await;

    let ns2_resp = lakekeeper_integration_tests::create_ns(
        api_context.clone(),
        test_warehouse.warehouse_id.to_string(),
        "hidden_ns".to_string(),
    )
    .await;

    // Get namespace IDs
    let catalog_state = CatalogState::from_pools(pool.clone(), pool.clone());
    let ns1_hierarchy = PostgresBackend::get_namespace(
        test_warehouse.warehouse_id,
        ns1_resp.namespace.clone(),
        catalog_state.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    let ns2_hierarchy = PostgresBackend::get_namespace(
        test_warehouse.warehouse_id,
        ns2_resp.namespace.clone(),
        catalog_state.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    // Hide the second namespace
    authz.hide(&format!("namespace:{}", ns2_hierarchy.namespace_id()));

    // Batch check with mixed visibility
    let request = CatalogActionsBatchCheckRequest {
        checks: vec![
            CatalogActionCheckItem {
                id: Some("visible".to_string()),
                identity: None,
                operation: CatalogActionCheckOperation::Namespace {
                    action: CatalogNamespaceAction::CreateTable {
                        name: None,
                        table_id: None,
                        properties: Arc::default(),
                    },
                    namespace: NamespaceIdentOrUuid::Id {
                        namespace_id: ns1_hierarchy.namespace_id(),
                        warehouse_id: test_warehouse.warehouse_id,
                    },
                },
            },
            CatalogActionCheckItem {
                id: Some("hidden".to_string()),
                identity: None,
                operation: CatalogActionCheckOperation::Namespace {
                    action: CatalogNamespaceAction::CreateTable {
                        name: None,
                        table_id: None,
                        properties: Arc::default(),
                    },
                    namespace: NamespaceIdentOrUuid::Id {
                        namespace_id: ns2_hierarchy.namespace_id(),
                        warehouse_id: test_warehouse.warehouse_id,
                    },
                },
            },
        ],
        error_on_not_found: false,
    };

    let response = check_internal(api_context.clone(), metadata.clone(), request)
        .await
        .unwrap();

    assert_eq!(response.results.len(), 2);
    assert_eq!(response.results[0].id, Some("visible".to_string()));
    assert!(response.results[0].allowed); // Visible namespace should be allowed
    assert_eq!(response.results[1].id, Some("hidden".to_string()));
    assert!(!response.results[1].allowed); // Hidden namespace should be denied
}

#[sqlx::test]
async fn test_check_internal_error_on_not_found(pool: sqlx::PgPool) {
    let prof = lakekeeper_integration_tests::memory_io_profile();
    let authz = HidingAuthorizer::new();

    let (api_context, test_warehouse) = lakekeeper_integration_tests::setup_simple(
        pool.clone(),
        prof,
        None,
        authz.clone(),
        TabularDeleteProfile::Hard {},
        None,
    )
    .await;

    let metadata = RequestMetadata::new_unauthenticated();

    // Check a non-existent table with error_on_not_found = false
    let non_existent_table_id = uuid::Uuid::now_v7();
    let request = CatalogActionsBatchCheckRequest {
        checks: vec![CatalogActionCheckItem {
            id: Some("not-found-no-error".to_string()),
            identity: None,
            operation: CatalogActionCheckOperation::Table {
                action: CatalogTableAction::ReadData,
                table: TabularIdentOrUuid::IdInWarehouse {
                    warehouse_id: test_warehouse.warehouse_id,
                    table_id: non_existent_table_id,
                },
            },
        }],
        error_on_not_found: false,
    };

    let response = check_internal(api_context.clone(), metadata.clone(), request)
        .await
        .unwrap();

    assert_eq!(response.results.len(), 1);
    assert!(!response.results[0].allowed); // Should be denied but not error

    // Check a non-existent table with error_on_not_found = true
    let request = CatalogActionsBatchCheckRequest {
        checks: vec![CatalogActionCheckItem {
            id: Some("not-found-with-error".to_string()),
            identity: None,
            operation: CatalogActionCheckOperation::Table {
                action: CatalogTableAction::ReadData,
                table: TabularIdentOrUuid::IdInWarehouse {
                    warehouse_id: test_warehouse.warehouse_id,
                    table_id: non_existent_table_id,
                },
            },
        }],
        error_on_not_found: true,
    };

    let result = check_internal(api_context.clone(), metadata.clone(), request).await;
    assert!(result.is_err()); // Should return an error
}

#[sqlx::test]
async fn test_check_internal_no_id_defaults_to_index(pool: sqlx::PgPool) {
    let prof = lakekeeper_integration_tests::memory_io_profile();
    let authz = HidingAuthorizer::new();

    let (api_context, test_warehouse) = lakekeeper_integration_tests::setup_simple(
        pool.clone(),
        prof,
        None,
        authz.clone(),
        TabularDeleteProfile::Hard {},
        None,
    )
    .await;

    let metadata = RequestMetadata::new_unauthenticated();

    // Check without providing IDs - should use None
    let request = CatalogActionsBatchCheckRequest {
        checks: vec![
            CatalogActionCheckItem {
                id: None,
                identity: None,
                operation: CatalogActionCheckOperation::Server {
                    action: CatalogServerAction::CreateProject {
                        name: None,
                        project_id: None,
                    },
                },
            },
            CatalogActionCheckItem {
                id: None,
                identity: None,
                operation: CatalogActionCheckOperation::Warehouse {
                    action: CatalogWarehouseAction::Use,
                    warehouse_id: test_warehouse.warehouse_id,
                },
            },
        ],
        error_on_not_found: false,
    };

    let response = check_internal(api_context.clone(), metadata.clone(), request)
        .await
        .unwrap();

    assert_eq!(response.results.len(), 2);
    assert_eq!(response.results[0].id, None);
    assert_eq!(response.results[1].id, None);
    assert!(response.results[0].allowed);
    assert!(response.results[1].allowed);
}

#[sqlx::test]
async fn test_check_internal_max_checks_limit(pool: sqlx::PgPool) {
    let prof = lakekeeper_integration_tests::memory_io_profile();
    let authz = HidingAuthorizer::new();

    let (api_context, _test_warehouse) = lakekeeper_integration_tests::setup_simple(
        pool.clone(),
        prof,
        None,
        authz.clone(),
        TabularDeleteProfile::Hard {},
        None,
    )
    .await;

    let metadata = RequestMetadata::new_unauthenticated();

    // Create more than MAX_CHECKS (1000) checks
    let checks = (0..1001)
        .map(|i| CatalogActionCheckItem {
            id: Some(format!("check-{i}")),
            identity: None,
            operation: CatalogActionCheckOperation::Server {
                action: CatalogServerAction::CreateProject {
                    name: None,
                    project_id: None,
                },
            },
        })
        .collect();

    let request = CatalogActionsBatchCheckRequest {
        checks,
        error_on_not_found: false,
    };

    let result = check_internal(api_context.clone(), metadata.clone(), request).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.r#type, "TooManyChecks");
}

#[test]
fn test_table_action_update_property_serde() {
    // Trino may send non-string properties (such as int `format_version`) to OPA, which then forwards to lakekeeper.
    let expected = serde_json::json!({
        "checks":[
            {
                "identity":{
                    "user":"oidc~9410d0bf-4487-4177-a34f-af364cac0a59"
                },
                "operation":{
                    "table":{
                        "action":{
                            "action":"commit",
                            "removed_properties":[],
                            "updated_properties":{"format_version":2}
                        },
                        "namespace":["test_set_properties_trino"],
                        "table":"my_table",
                        "warehouse-id":"e2c21690-dce9-11f0-9036-c3bdc0f3ba79"
                    }
                }
            }],
            "error-on-not-found":false});
    let action = CatalogTableAction::Commit {
        removed_properties: Arc::new(vec![]),
        updated_properties: Arc::new({
            let mut map = BTreeMap::new();
            map.insert("format_version".to_string(), "2".to_string());
            map
        }),
    };
    let item = CatalogActionCheckItem {
        id: None,
        identity: Some(UserOrRole::User(UserId::new_unchecked(
            "oidc",
            "9410d0bf-4487-4177-a34f-af364cac0a59",
        ))),
        operation: CatalogActionCheckOperation::Table {
            action,
            table: TabularIdentOrUuid::Name {
                namespace: NamespaceIdent::from_strs(vec!["test_set_properties_trino".to_string()])
                    .unwrap(),
                table: "my_table".to_string(),
                warehouse_id: uuid::Uuid::parse_str("e2c21690-dce9-11f0-9036-c3bdc0f3ba79")
                    .unwrap()
                    .into(),
            },
        },
    };
    let request = CatalogActionsBatchCheckRequest {
        checks: vec![item],
        error_on_not_found: false,
    };
    let deserialized: CatalogActionsBatchCheckRequest = serde_json::from_value(expected).unwrap();
    assert_eq!(request, deserialized);
}

#[sqlx::test]
async fn test_check_internal_role_based_identity(pool: sqlx::PgPool) {
    use lakekeeper::api::management::v1::{
        ApiServer,
        role::{CreateRoleRequest, Service as RoleService},
    };

    let prof = lakekeeper_integration_tests::memory_io_profile();
    let authz = HidingAuthorizer::new();

    let (api_context, test_warehouse) = lakekeeper_integration_tests::setup_simple(
        pool.clone(),
        prof,
        None,
        authz.clone(),
        TabularDeleteProfile::Hard {},
        None,
    )
    .await;

    let metadata = RequestMetadata::new_unauthenticated();

    // Create a role in the project so fetch_identity_roles can resolve it.
    let role = ApiServer::<PostgresBackend, _, _>::create_role(
        CreateRoleRequest {
            name: "test-role".to_string(),
            description: None,
            project_id: Some((*test_warehouse.project_id).clone()),
            provider_id: None,
            source_id: None,
        },
        api_context.clone(),
        metadata.clone(),
    )
    .await
    .unwrap();
    let role_id = role.id;

    // Test 1: server action with role identity — exercises fetch_identity_roles +
    // resolve_identity end-to-end.
    let request = CatalogActionsBatchCheckRequest {
        checks: vec![CatalogActionCheckItem {
            id: Some("role-server-check".to_string()),
            identity: Some(UserOrRole::Role(RoleAssignee::from_role(role_id))),
            operation: CatalogActionCheckOperation::Server {
                action: CatalogServerAction::CreateProject {
                    name: None,
                    project_id: None,
                },
            },
        }],
        error_on_not_found: false,
    };

    let response = check_internal(api_context.clone(), metadata.clone(), request)
        .await
        .unwrap();

    assert_eq!(response.results.len(), 1);
    assert_eq!(
        response.results[0].id,
        Some("role-server-check".to_string())
    );
    assert!(response.results[0].allowed);

    // Test 2: warehouse action with role identity — allowed before hiding.
    let request = CatalogActionsBatchCheckRequest {
        checks: vec![CatalogActionCheckItem {
            id: Some("role-warehouse-allow".to_string()),
            identity: Some(UserOrRole::Role(RoleAssignee::from_role(role_id))),
            operation: CatalogActionCheckOperation::Warehouse {
                action: CatalogWarehouseAction::Use,
                warehouse_id: test_warehouse.warehouse_id,
            },
        }],
        error_on_not_found: false,
    };

    let response = check_internal(api_context.clone(), metadata.clone(), request)
        .await
        .unwrap();

    assert_eq!(response.results.len(), 1);
    assert_eq!(
        response.results[0].id,
        Some("role-warehouse-allow".to_string())
    );
    assert!(response.results[0].allowed);

    // Test 3: hide the warehouse, then recheck with role identity — should be denied.
    authz.hide(&format!("warehouse:{}", test_warehouse.warehouse_id));

    let request = CatalogActionsBatchCheckRequest {
        checks: vec![CatalogActionCheckItem {
            id: Some("role-warehouse-deny".to_string()),
            identity: Some(UserOrRole::Role(RoleAssignee::from_role(role_id))),
            operation: CatalogActionCheckOperation::Warehouse {
                action: CatalogWarehouseAction::Use,
                warehouse_id: test_warehouse.warehouse_id,
            },
        }],
        error_on_not_found: false,
    };

    let response = check_internal(api_context.clone(), metadata.clone(), request)
        .await
        .unwrap();

    assert_eq!(response.results.len(), 1);
    assert_eq!(
        response.results[0].id,
        Some("role-warehouse-deny".to_string())
    );
    assert!(!response.results[0].allowed);

    // Test 4: batch mixing role identity with no-identity on the same server action —
    // both must succeed, confirming prefetch deduplication works.
    let request = CatalogActionsBatchCheckRequest {
        checks: vec![
            CatalogActionCheckItem {
                id: Some("batch-role".to_string()),
                identity: Some(UserOrRole::Role(RoleAssignee::from_role(role_id))),
                operation: CatalogActionCheckOperation::Server {
                    action: CatalogServerAction::CreateProject {
                        name: None,
                        project_id: None,
                    },
                },
            },
            CatalogActionCheckItem {
                id: Some("batch-no-identity".to_string()),
                identity: None,
                operation: CatalogActionCheckOperation::Server {
                    action: CatalogServerAction::CreateProject {
                        name: None,
                        project_id: None,
                    },
                },
            },
        ],
        error_on_not_found: false,
    };

    let response = check_internal(api_context.clone(), metadata.clone(), request)
        .await
        .unwrap();

    assert_eq!(response.results.len(), 2);
    assert!(response.results.iter().all(|r| r.allowed));
    assert_eq!(response.results[0].id, Some("batch-role".to_string()));
    assert_eq!(
        response.results[1].id,
        Some("batch-no-identity".to_string())
    );
}

#[sqlx::test]
async fn test_check_internal_generic_table_operation(pool: sqlx::PgPool) {
    use lakekeeper::{
        api::data::v1::generic_tables::{GenericTableService as _, ListGenericTablesQuery},
        service::authz::AllowAllAuthorizer,
    };

    let (api_context, test_warehouse) = lakekeeper_integration_tests::setup_simple(
        pool.clone(),
        lakekeeper_integration_tests::memory_io_profile(),
        None,
        AllowAllAuthorizer::default(),
        TabularDeleteProfile::Hard {},
        None,
    )
    .await;
    let metadata = RequestMetadata::new_unauthenticated();
    let prefix = test_warehouse.warehouse_id.to_string();
    let ns_name = "gt_check_ns";
    lakekeeper_integration_tests::create_ns(
        api_context.clone(),
        prefix.clone(),
        ns_name.to_string(),
    )
    .await;

    let gt_name = "my-gt";
    create_generic_table(api_context.clone(), prefix.clone(), ns_name, gt_name)
        .await
        .unwrap();

    let listed = CatalogServer::list_generic_tables(
        NamespaceParameters {
            prefix: Some(Prefix(prefix.clone())),
            namespace: NamespaceIdent::new(ns_name.to_string()),
        },
        ListGenericTablesQuery::default(),
        api_context.clone(),
        metadata.clone(),
    )
    .await
    .unwrap();
    let gt_id = listed
        .identifiers
        .iter()
        .find(|i| i.name == gt_name)
        .and_then(|i| i.id)
        .expect("generic table id");

    let request = CatalogActionsBatchCheckRequest {
        checks: vec![
            CatalogActionCheckItem {
                id: Some("by-name".to_string()),
                identity: None,
                operation: CatalogActionCheckOperation::GenericTable {
                    action: CatalogGenericTableAction::Drop,
                    generic_table: TabularIdentOrUuid::Name {
                        namespace: NamespaceIdent::new(ns_name.to_string()),
                        table: gt_name.to_string(),
                        warehouse_id: test_warehouse.warehouse_id,
                    },
                },
            },
            CatalogActionCheckItem {
                id: Some("by-id".to_string()),
                identity: None,
                operation: CatalogActionCheckOperation::GenericTable {
                    action: CatalogGenericTableAction::ReadData,
                    generic_table: TabularIdentOrUuid::IdInWarehouse {
                        warehouse_id: test_warehouse.warehouse_id,
                        table_id: *gt_id,
                    },
                },
            },
        ],
        error_on_not_found: true,
    };

    let response = check_internal(api_context, metadata, request)
        .await
        .unwrap();
    assert_eq!(response.results.len(), 2);
    assert!(response.results.iter().all(|r| r.allowed));
    assert_eq!(response.results[0].id, Some("by-name".to_string()));
    assert_eq!(response.results[1].id, Some("by-id".to_string()));
}
