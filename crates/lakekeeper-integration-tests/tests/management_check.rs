// Extracted from crates/lakekeeper/src/api/management/v1/check.rs.
// VAK-437 split.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use iceberg::TableUpdate;
use iceberg_ext::catalog::rest::{CommitTableRequest, ErrorModel};
use lakekeeper::{
    WarehouseId,
    api::{
        ApiContext, RequestMetadata,
        data::v1::generic_tables::{
            GenericTableParameters, GenericTableService as _, ListGenericTablesQuery,
        },
        iceberg::{
            types::Prefix,
            v1::{
                DataAccess, DropParams, NamespaceParameters, TableParameters, ViewParameters,
                tables::TablesService, views::ViewService,
            },
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
        CatalogNamespaceOps, NamespaceIdent, State, UserId,
        authz::{
            self, CatalogGenericTableAction, CatalogNamespaceAction, CatalogServerAction,
            CatalogTableAction, CatalogViewAction, CatalogWarehouseAction, TableUpdateKind,
            tests::HidingAuthorizer,
        },
    },
};
use lakekeeper_integration_tests::{
    create_generic_table, create_table_request, create_view_request,
};
use lakekeeper_storage_postgres::{CatalogState, PostgresBackend, SecretsState};

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
        target_refs: Arc::default(),
        update_kinds: Arc::default(),
        commit_facts: authz::CommitFacts::default(),
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
                    action: CatalogGenericTableAction::Drop {
                        force: false,
                        purge: false,
                    },
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

type HidingContext = ApiContext<State<HidingAuthorizer, PostgresBackend, SecretsState>>;

#[derive(Debug, Clone, Copy)]
enum TabularKind {
    Table,
    View,
    GenericTable,
}

/// A warehouse with soft deletion and one namespace, in which `name` was created and
/// dropped three times as `kind`.
struct DroppedTabulars {
    api_context: HidingContext,
    authz: HidingAuthorizer,
    warehouse_id: WarehouseId,
    namespace: NamespaceIdent,
    kind: TabularKind,
    name: &'static str,
    dropped_ids: Vec<uuid::Uuid>,
}

impl DroppedTabulars {
    async fn new(pool: sqlx::PgPool, kind: TabularKind, name: &'static str) -> Self {
        let authz = HidingAuthorizer::new();
        let (api_context, test_warehouse) = lakekeeper_integration_tests::setup_simple(
            pool,
            lakekeeper_integration_tests::memory_io_profile(),
            None,
            authz.clone(),
            TabularDeleteProfile::Soft {
                expiration_seconds: chrono::Duration::seconds(3600),
            },
            None,
        )
        .await;
        let warehouse_id = test_warehouse.warehouse_id;
        let namespace = lakekeeper_integration_tests::create_ns(
            api_context.clone(),
            warehouse_id.to_string(),
            "test_namespace".to_string(),
        )
        .await
        .namespace;

        let mut setup = Self {
            api_context,
            authz,
            warehouse_id,
            namespace,
            kind,
            name,
            dropped_ids: Vec::new(),
        };
        for _ in 0..3 {
            let id = setup.create().await;
            setup.drop().await;
            setup.dropped_ids.push(id);
        }
        setup
    }

    fn prefix(&self) -> Option<Prefix> {
        Some(Prefix(self.warehouse_id.to_string()))
    }

    fn ident(&self) -> iceberg::TableIdent {
        iceberg::TableIdent::new(self.namespace.clone(), self.name.to_string())
    }

    async fn create(&self) -> uuid::Uuid {
        let parameters = NamespaceParameters {
            prefix: self.prefix(),
            namespace: self.namespace.clone(),
        };
        let metadata = RequestMetadata::new_unauthenticated();
        match self.kind {
            TabularKind::Table => CatalogServer::create_table(
                parameters,
                create_table_request(Some(self.name.to_string()), None),
                DataAccess::not_specified(),
                self.api_context.clone(),
                metadata,
            )
            .await
            .unwrap()
            .metadata
            .uuid(),
            TabularKind::View => CatalogServer::create_view(
                parameters,
                create_view_request(Some(self.name), None),
                self.api_context.clone(),
                DataAccess::not_specified(),
                metadata,
            )
            .await
            .unwrap()
            .metadata
            .uuid(),
            TabularKind::GenericTable => {
                create_generic_table(
                    self.api_context.clone(),
                    self.warehouse_id.to_string(),
                    self.namespace_name(),
                    self.name,
                )
                .await
                .unwrap();
                self.live_generic_table_id().await
            }
        }
    }

    fn namespace_name(&self) -> String {
        self.namespace.as_ref()[0].clone()
    }

    /// Generic-table responses carry no id; the listing names the live generic table.
    async fn live_generic_table_id(&self) -> uuid::Uuid {
        let listed = CatalogServer::list_generic_tables(
            NamespaceParameters {
                prefix: self.prefix(),
                namespace: self.namespace.clone(),
            },
            ListGenericTablesQuery::default(),
            self.api_context.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        let ids = listed
            .identifiers
            .iter()
            .filter(|ident| ident.name == self.name)
            .map(|ident| ident.id.expect("generic table id"))
            .collect::<Vec<_>>();
        assert_eq!(ids.len(), 1);
        *ids[0]
    }

    async fn drop(&self) {
        let drop_params = DropParams {
            purge_requested: false,
            force: false,
        };
        let metadata = RequestMetadata::new_unauthenticated();
        match self.kind {
            TabularKind::Table => CatalogServer::drop_table(
                TableParameters {
                    prefix: self.prefix(),
                    table: self.ident(),
                },
                drop_params,
                self.api_context.clone(),
                metadata,
            )
            .await
            .unwrap(),
            TabularKind::View => CatalogServer::drop_view(
                ViewParameters {
                    prefix: self.prefix(),
                    view: self.ident(),
                },
                drop_params,
                self.api_context.clone(),
                metadata,
            )
            .await
            .unwrap(),
            TabularKind::GenericTable => CatalogServer::drop_generic_table(
                GenericTableParameters {
                    prefix: self.prefix(),
                    namespace: self.namespace.clone(),
                    table_name: self.name.to_string(),
                },
                drop_params,
                self.api_context.clone(),
                metadata,
            )
            .await
            .unwrap(),
        }
    }

    fn authz_object(&self, id: uuid::Uuid) -> String {
        match self.kind {
            TabularKind::Table => format!("table:{}/{id}", self.warehouse_id),
            TabularKind::View => format!("view:{}/{id}", self.warehouse_id),
            TabularKind::GenericTable => format!("generic_table:{}/{id}", self.warehouse_id),
        }
    }

    fn check_by_name(&self, identity: Option<UserOrRole>) -> CatalogActionCheckItem {
        self.check_by_other_name(self.name, identity)
    }

    fn check_by_other_name(
        &self,
        name: &str,
        identity: Option<UserOrRole>,
    ) -> CatalogActionCheckItem {
        let tabular = TabularIdentOrUuid::Name {
            namespace: self.namespace.clone(),
            table: name.to_string(),
            warehouse_id: self.warehouse_id,
        };
        let operation = match self.kind {
            TabularKind::Table => CatalogActionCheckOperation::Table {
                action: CatalogTableAction::ReadData,
                table: tabular,
            },
            TabularKind::View => CatalogActionCheckOperation::View {
                action: CatalogViewAction::Select,
                view: tabular,
            },
            TabularKind::GenericTable => CatalogActionCheckOperation::GenericTable {
                action: CatalogGenericTableAction::ReadData,
                generic_table: tabular,
            },
        };
        CatalogActionCheckItem {
            id: None,
            identity,
            operation,
        }
    }

    async fn check(
        &self,
        checks: Vec<CatalogActionCheckItem>,
        error_on_not_found: bool,
    ) -> Result<Vec<bool>, ErrorModel> {
        check_internal(
            self.api_context.clone(),
            RequestMetadata::new_unauthenticated(),
            CatalogActionsBatchCheckRequest {
                checks,
                error_on_not_found,
            },
        )
        .await
        .map(|response| response.results.iter().map(|r| r.allowed).collect())
    }
}

/// A by-name check is decided on the live tabular only. Each user is blind to one side:
/// a check decided on a dropped tabular would allow the first user and deny the second.
async fn assert_by_name_check_decides_on_live(pool: sqlx::PgPool, kind: TabularKind) {
    let setup = DroppedTabulars::new(pool, kind, "shared_name").await;
    let live_id = setup.create().await;
    assert!(!setup.dropped_ids.contains(&live_id));

    let blind_to_live = UserId::new_unchecked("oidc", "blind-to-live");
    let blind_to_dropped = UserId::new_unchecked("oidc", "blind-to-dropped");
    setup.authz.hide_for_user(
        &authz::UserOrRole::User(blind_to_live.clone()),
        &setup.authz_object(live_id),
    );
    for id in &setup.dropped_ids {
        setup.authz.hide_for_user(
            &authz::UserOrRole::User(blind_to_dropped.clone()),
            &setup.authz_object(*id),
        );
    }

    let allowed = setup
        .check(
            vec![
                setup.check_by_name(Some(UserOrRole::User(blind_to_live))),
                setup.check_by_name(Some(UserOrRole::User(blind_to_dropped))),
            ],
            true,
        )
        .await
        .unwrap();
    assert_eq!(allowed, vec![false, true]);
}

/// A name whose tabulars were all dropped addresses nothing.
async fn assert_by_name_check_ignores_dropped(
    pool: sqlx::PgPool,
    kind: TabularKind,
    not_found_type: &str,
) {
    let setup = DroppedTabulars::new(pool, kind, "dropped_name").await;

    let err = setup
        .check(vec![setup.check_by_name(None)], true)
        .await
        .unwrap_err();
    assert_eq!(err.code, 404);
    assert_eq!(err.r#type, not_found_type);

    let allowed = setup
        .check(vec![setup.check_by_name(None)], false)
        .await
        .unwrap();
    assert_eq!(allowed, vec![false]);
}

#[sqlx::test]
async fn test_check_internal_by_name_decides_on_live_table(pool: sqlx::PgPool) {
    assert_by_name_check_decides_on_live(pool, TabularKind::Table).await;
}

#[sqlx::test]
async fn test_check_internal_by_name_decides_on_live_view(pool: sqlx::PgPool) {
    assert_by_name_check_decides_on_live(pool, TabularKind::View).await;
}

#[sqlx::test]
async fn test_check_internal_by_name_ignores_dropped_table(pool: sqlx::PgPool) {
    assert_by_name_check_ignores_dropped(pool, TabularKind::Table, "NoSuchTableException").await;
}

#[sqlx::test]
async fn test_check_internal_by_name_ignores_dropped_view(pool: sqlx::PgPool) {
    assert_by_name_check_ignores_dropped(pool, TabularKind::View, "NoSuchViewException").await;
}

#[sqlx::test]
async fn test_check_internal_by_name_decides_on_live_generic_table(pool: sqlx::PgPool) {
    assert_by_name_check_decides_on_live(pool, TabularKind::GenericTable).await;
}

#[sqlx::test]
async fn test_check_internal_by_name_ignores_dropped_generic_table(pool: sqlx::PgPool) {
    assert_by_name_check_ignores_dropped(
        pool,
        TabularKind::GenericTable,
        "NoSuchGenericTableException",
    )
    .await;
}

/// With `error_on_not_found`, a by-name check tells every caller whether the name is
/// taken, before any authorization. The Trino bridge relies on it to tell a free name
/// apart from a denied check.
async fn assert_by_name_check_reports_free_names(
    pool: sqlx::PgPool,
    kind: TabularKind,
    not_found_type: &str,
    read_grants: &str,
) {
    let setup = DroppedTabulars::new(pool, kind, "taken_name").await;
    setup.create().await;
    let other_user = UserOrRole::User(UserId::new_unchecked("oidc", "other-user"));
    let free_name = || setup.check_by_other_name("free_name", Some(other_user.clone()));
    let taken_name = || setup.check_by_name(Some(other_user.clone()));

    let err = setup.check(vec![free_name()], true).await.unwrap_err();
    assert_eq!((err.code, err.r#type.as_str()), (404, not_found_type));
    assert_eq!(
        setup.check(vec![taken_name()], true).await.unwrap(),
        vec![true]
    );

    // From here on, the caller may not inspect the permissions of other principals.
    setup.authz.block_action(read_grants);
    let err = setup.check(vec![taken_name()], true).await.unwrap_err();
    assert_eq!(
        (err.code, err.r#type.as_str()),
        (403, "CannotInspectPermissions")
    );
    let err = setup.check(vec![free_name()], true).await.unwrap_err();
    assert_eq!((err.code, err.r#type.as_str()), (404, not_found_type));
}

#[sqlx::test]
async fn test_check_internal_by_name_reports_free_table_names(pool: sqlx::PgPool) {
    assert_by_name_check_reports_free_names(
        pool,
        TabularKind::Table,
        "NoSuchTableException",
        "table:ReadGrants",
    )
    .await;
}

#[sqlx::test]
async fn test_check_internal_by_name_reports_free_view_names(pool: sqlx::PgPool) {
    assert_by_name_check_reports_free_names(
        pool,
        TabularKind::View,
        "NoSuchViewException",
        "view:ReadGrants",
    )
    .await;
}

#[sqlx::test]
async fn test_check_internal_by_name_reports_free_generic_table_names(pool: sqlx::PgPool) {
    assert_by_name_check_reports_free_names(
        pool,
        TabularKind::GenericTable,
        "NoSuchGenericTableException",
        "generic_table:ReadGrants",
    )
    .await;
}

/// A staged table that was not committed yet is addressed by its id only.
#[sqlx::test]
async fn test_check_internal_by_name_ignores_staged_table(pool: sqlx::PgPool) {
    let setup = DroppedTabulars::new(pool, TabularKind::Table, "staged_name").await;
    let mut request = create_table_request(Some(setup.name.to_string()), None);
    request.stage_create = Some(true);
    let staged_id = CatalogServer::create_table(
        NamespaceParameters {
            prefix: setup.prefix(),
            namespace: setup.namespace.clone(),
        },
        request,
        DataAccess::not_specified(),
        setup.api_context.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap()
    .metadata
    .uuid();

    let err = setup
        .check(vec![setup.check_by_name(None)], true)
        .await
        .unwrap_err();
    assert_eq!(err.code, 404);
    assert_eq!(err.r#type, "NoSuchTableException");

    let by_id = CatalogActionCheckItem {
        id: None,
        identity: None,
        operation: CatalogActionCheckOperation::Table {
            action: CatalogTableAction::ReadData,
            table: TabularIdentOrUuid::IdInWarehouse {
                warehouse_id: setup.warehouse_id,
                table_id: staged_id,
            },
        },
    };
    assert_eq!(setup.check(vec![by_id], true).await.unwrap(), vec![true]);
}

/// A namespace with a live table `live` and a staged table `staged`, and an authorizer that
/// allows everything until a test blocks something.
struct StagedAndLive {
    api_context: HidingContext,
    authz: HidingAuthorizer,
    warehouse_id: WarehouseId,
    namespace: NamespaceIdent,
    ids: HashMap<&'static str, uuid::Uuid>,
}

impl StagedAndLive {
    async fn new(pool: sqlx::PgPool) -> Self {
        let authz = HidingAuthorizer::new();
        let (api_context, test_warehouse) = lakekeeper_integration_tests::setup_simple(
            pool,
            lakekeeper_integration_tests::memory_io_profile(),
            None,
            authz.clone(),
            TabularDeleteProfile::Hard {},
            None,
        )
        .await;
        let warehouse_id = test_warehouse.warehouse_id;
        let namespace = lakekeeper_integration_tests::create_ns(
            api_context.clone(),
            warehouse_id.to_string(),
            "test_namespace".to_string(),
        )
        .await
        .namespace;
        let mut ids = HashMap::new();
        for (name, stage_create) in [("live", false), ("staged", true)] {
            let mut request = create_table_request(Some(name.to_string()), None);
            request.stage_create = Some(stage_create);
            let id = CatalogServer::create_table(
                NamespaceParameters {
                    prefix: Some(Prefix(warehouse_id.to_string())),
                    namespace: namespace.clone(),
                },
                request,
                DataAccess::not_specified(),
                api_context.clone(),
                RequestMetadata::new_unauthenticated(),
            )
            .await
            .unwrap()
            .metadata
            .uuid();
            ids.insert(name, id);
        }
        Self {
            api_context,
            authz,
            warehouse_id,
            namespace,
            ids,
        }
    }

    /// Batch-check of a commit check on `table`, addressed by id.
    async fn check(&self, table: &str, updates: &[TableUpdate]) -> bool {
        let updated_properties = updates
            .iter()
            .filter_map(|u| match u {
                TableUpdate::SetProperties { updates } => Some(updates.clone()),
                _ => None,
            })
            .flatten()
            .collect();
        let action = CatalogTableAction::Commit {
            updated_properties: Arc::new(updated_properties),
            removed_properties: Arc::default(),
            target_refs: Arc::default(),
            update_kinds: Arc::new(updates.iter().map(TableUpdateKind::from).collect()),
            commit_facts: authz::CommitFacts::default(),
        };
        let results = check_internal(
            self.api_context.clone(),
            RequestMetadata::new_unauthenticated(),
            CatalogActionsBatchCheckRequest {
                checks: vec![CatalogActionCheckItem {
                    id: None,
                    identity: None,
                    operation: CatalogActionCheckOperation::Table {
                        action,
                        table: TabularIdentOrUuid::IdInWarehouse {
                            warehouse_id: self.warehouse_id,
                            table_id: self.ids[table],
                        },
                    },
                }],
                error_on_not_found: true,
            },
        )
        .await
        .unwrap()
        .results;
        assert_eq!(results.len(), 1);
        results[0].allowed
    }

    /// The server's answer to a commit request with `updates` to `table`: allowed, or the
    /// HTTP status of the refusal.
    async fn commit(&self, table: &str, updates: Vec<TableUpdate>) -> Result<(), u16> {
        let ident = iceberg::TableIdent::new(self.namespace.clone(), table.to_string());
        CatalogServer::commit_table(
            TableParameters {
                prefix: Some(Prefix(self.warehouse_id.to_string())),
                table: ident.clone(),
            },
            CommitTableRequest {
                identifier: Some(ident),
                requirements: vec![],
                updates,
            },
            self.api_context.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .map(|_| ())
        .map_err(|e| e.error.code)
    }
}

fn set_owner() -> TableUpdate {
    TableUpdate::SetProperties {
        updates: HashMap::from([("owner".to_string(), "team".to_string())]),
    }
}

/// A commit check on a staged table asks the authorizer what the server asks for a commit
/// request with the same content. This authorizer allows a commit only if it sets a property.
#[sqlx::test]
async fn test_check_internal_commit_on_staged_table_decides_as_the_server(pool: sqlx::PgPool) {
    let setup = StagedAndLive::new(pool).await;
    setup
        .authz
        .block_action("table:Commit { updated_properties: {}");
    let remove = TableUpdate::RemoveProperties {
        removals: vec!["owner".to_string()],
    };

    assert!(!setup.check("staged", std::slice::from_ref(&remove)).await);
    assert_eq!(setup.commit("staged", vec![remove]).await, Err(403));

    assert!(setup.check("staged", &[set_owner()]).await);
    assert_eq!(setup.commit("staged", vec![set_owner()]).await, Ok(()));
}

/// With an authorizer that allows data commits only, as a policy on the data-commit context
/// can, batch-check and the server agree: a data commit to a live table is allowed, and the
/// same commit to a staged table is refused, because it finishes the table's creation.
#[sqlx::test]
async fn test_check_internal_data_commit_decides_as_the_server(pool: sqlx::PgPool) {
    let setup = StagedAndLive::new(pool).await;
    setup.authz.block_structural_commits();
    // A data commit that changes nothing on a live table.
    let data_commit = [TableUpdate::RemoveStatistics { snapshot_id: 1 }];

    assert!(setup.check("live", &data_commit).await);
    assert_eq!(setup.commit("live", data_commit.to_vec()).await, Ok(()));

    assert!(!setup.check("staged", &data_commit).await);
    assert_eq!(setup.commit("staged", data_commit.to_vec()).await, Err(403));

    assert!(!setup.check("live", &[set_owner()]).await);
    assert_eq!(setup.commit("live", vec![set_owner()]).await, Err(403));
}
