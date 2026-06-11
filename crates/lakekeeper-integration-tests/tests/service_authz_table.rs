// Extracted from crates/lakekeeper/src/service/authz/table.rs.
// Original location was `#[cfg(any())] mod tests` (VAK-437 split).

use iceberg::{NamespaceIdent, TableIdent};
use lakekeeper::{
    api::ApiContext,
    service::{
        CatalogGenericTableOps, CatalogNamespaceOps, CatalogStore, CatalogTabularOps,
        CatalogWarehouseOps, GenericTabularInfo, NamespaceWithParent, TableInfo,
        TabularIdentBorrowed, TabularListFlags, Transaction, ViewInfo,
        authz::{
            ActionOnGenericTable, ActionOnTable, ActionOnTableOrView, ActionOnView,
            AuthZGenericTableOps, AuthZTableOps, CatalogGenericTableAction, CatalogTableAction,
            CatalogViewAction, tests::HidingAuthorizer,
        },
    },
};
use lakekeeper_integration_tests::{
    SetupTestCatalog, create_generic_table, create_ns, create_table, create_view, memory_io_profile,
};
use lakekeeper_storage_postgres::PostgresBackend;
use sqlx::PgPool;

/// Fully-specified tabular action type for tests where not all enum variants are present.
type TestTabularAction<'a> = ActionOnTableOrView<
    'a,
    'a,
    TableInfo,
    ViewInfo,
    CatalogTableAction,
    CatalogViewAction,
    GenericTabularInfo,
    CatalogGenericTableAction,
>;

#[sqlx::test]
async fn test_are_allowed_tabular_actions_vec_all_allowed(pool: PgPool) {
    let authz = HidingAuthorizer::new();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool)
        .authorizer(authz.clone())
        .storage_profile(memory_io_profile())
        .build()
        .setup()
        .await;

    // Create a namespace, table, and view
    let prefix = warehouse_resp.warehouse_id.to_string();
    let ns = create_ns(ctx.clone(), prefix.clone(), "test_ns".to_string()).await;
    let _table = create_table(ctx.clone(), &prefix, "test_ns", "t1", false)
        .await
        .unwrap();
    let _view = create_view(ctx.clone(), &prefix, "test_ns", "v1", None)
        .await
        .unwrap();

    // Construct table identifiers
    let table_ident = TableIdent::new(NamespaceIdent::new("test_ns".to_string()), "t1".to_string());
    let view_ident = TableIdent::new(NamespaceIdent::new("test_ns".to_string()), "v1".to_string());

    // Load tabular info
    let tabulars = vec![
        TabularIdentBorrowed::Table(&table_ident),
        TabularIdentBorrowed::View(&view_ident),
    ];
    let infos = PostgresBackend::get_tabular_infos_by_ident(
        warehouse_resp.warehouse_id,
        &tabulars,
        TabularListFlags::active(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();

    let table_info = infos
        .get(&table_ident)
        .unwrap()
        .clone()
        .into_table_info()
        .unwrap();
    let view_info = infos
        .get(&view_ident)
        .unwrap()
        .clone()
        .into_view_info()
        .unwrap();

    // Get namespace hierarchy
    let warehouse = PostgresBackend::get_active_warehouse_by_id(
        warehouse_resp.warehouse_id,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();
    let ns_hierarchy = PostgresBackend::get_namespace(
        warehouse_resp.warehouse_id,
        &ns.namespace,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    // Test with both table and view actions
    let actions: Vec<(&NamespaceWithParent, TestTabularAction<'_>)> = vec![
        (
            &ns_hierarchy.namespace,
            ActionOnTableOrView::Table(ActionOnTable {
                info: &table_info,
                action: CatalogTableAction::GetMetadata,
                user: None,
                is_delegated_execution: false,
            }),
        ),
        (
            &ns_hierarchy.namespace,
            ActionOnTableOrView::View(ActionOnView {
                info: &view_info,
                action: CatalogViewAction::GetMetadata,
                user: None,
                is_delegated_execution: false,
            }),
        ),
    ];

    let parents = ns_hierarchy
        .parents
        .iter()
        .map(|ns| (ns.namespace_id(), ns.clone()))
        .collect();
    let result = authz
        .are_allowed_tabular_actions_vec(
            &lakekeeper_integration_tests::random_request_metadata(),
            &warehouse,
            &parents,
            &actions,
        )
        .await
        .unwrap()
        .into_inner();

    assert_eq!(result, vec![true, true]);
}

#[sqlx::test]
async fn test_are_allowed_tabular_actions_vec_hidden_table(pool: PgPool) {
    let authz = HidingAuthorizer::new();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool)
        .authorizer(authz.clone())
        .storage_profile(memory_io_profile())
        .build()
        .setup()
        .await;

    let prefix = warehouse_resp.warehouse_id.to_string();
    let ns = create_ns(ctx.clone(), prefix.clone(), "test_ns".to_string()).await;
    let _table = create_table(ctx.clone(), &prefix, "test_ns", "t1", false)
        .await
        .unwrap();
    let _view = create_view(ctx.clone(), &prefix, "test_ns", "v1", None)
        .await
        .unwrap();

    let table_ident = TableIdent::new(NamespaceIdent::new("test_ns".to_string()), "t1".to_string());
    let view_ident = TableIdent::new(NamespaceIdent::new("test_ns".to_string()), "v1".to_string());

    let tabulars = vec![
        TabularIdentBorrowed::Table(&table_ident),
        TabularIdentBorrowed::View(&view_ident),
    ];
    let infos = PostgresBackend::get_tabular_infos_by_ident(
        warehouse_resp.warehouse_id,
        &tabulars,
        TabularListFlags::active(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();

    let table_info = infos
        .get(&table_ident)
        .unwrap()
        .clone()
        .into_table_info()
        .unwrap();
    let view_info = infos
        .get(&view_ident)
        .unwrap()
        .clone()
        .into_view_info()
        .unwrap();

    // Hide the table
    authz.hide(&format!(
        "table:{}/{}",
        warehouse_resp.warehouse_id, table_info.tabular_id
    ));

    let warehouse = PostgresBackend::get_active_warehouse_by_id(
        warehouse_resp.warehouse_id,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();
    let ns_hierarchy = PostgresBackend::get_namespace(
        warehouse_resp.warehouse_id,
        &ns.namespace,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();
    let parent_namespaces = ns_hierarchy
        .parents
        .iter()
        .map(|ns| (ns.namespace_id(), ns.clone()))
        .collect();

    let actions: Vec<(&NamespaceWithParent, TestTabularAction<'_>)> = vec![
        (
            &ns_hierarchy.namespace,
            ActionOnTableOrView::Table(ActionOnTable {
                info: &table_info,
                action: CatalogTableAction::GetMetadata,
                user: None,
                is_delegated_execution: false,
            }),
        ),
        (
            &ns_hierarchy.namespace,
            ActionOnTableOrView::View(ActionOnView {
                info: &view_info,
                action: CatalogViewAction::GetMetadata,
                user: None,
                is_delegated_execution: false,
            }),
        ),
    ];

    let result = authz
        .are_allowed_tabular_actions_vec(
            &lakekeeper_integration_tests::random_request_metadata(),
            &warehouse,
            &parent_namespaces,
            &actions,
        )
        .await
        .unwrap()
        .into_inner();

    // Table is hidden, view is visible
    assert_eq!(result, vec![false, true]);
}

#[sqlx::test]
async fn test_are_allowed_tabular_actions_vec_mixed_order(pool: PgPool) {
    let authz = HidingAuthorizer::new();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool)
        .authorizer(authz.clone())
        .storage_profile(memory_io_profile())
        .build()
        .setup()
        .await;

    let prefix = warehouse_resp.warehouse_id.to_string();
    let ns = create_ns(ctx.clone(), prefix.clone(), "test_ns".to_string()).await;
    let _table1 = create_table(ctx.clone(), &prefix, "test_ns", "t1", false)
        .await
        .unwrap();
    let _view1 = create_view(ctx.clone(), &prefix, "test_ns", "v1", None)
        .await
        .unwrap();
    let _table2 = create_table(ctx.clone(), &prefix, "test_ns", "t2", false)
        .await
        .unwrap();

    let table1_ident =
        TableIdent::new(NamespaceIdent::new("test_ns".to_string()), "t1".to_string());
    let view1_ident = TableIdent::new(NamespaceIdent::new("test_ns".to_string()), "v1".to_string());
    let table2_ident =
        TableIdent::new(NamespaceIdent::new("test_ns".to_string()), "t2".to_string());

    let tabulars = vec![
        TabularIdentBorrowed::Table(&table1_ident),
        TabularIdentBorrowed::View(&view1_ident),
        TabularIdentBorrowed::Table(&table2_ident),
    ];
    let infos = PostgresBackend::get_tabular_infos_by_ident(
        warehouse_resp.warehouse_id,
        &tabulars,
        TabularListFlags::active(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();

    let table1_info = infos
        .get(&table1_ident)
        .unwrap()
        .clone()
        .into_table_info()
        .unwrap();
    let view1_info = infos
        .get(&view1_ident)
        .unwrap()
        .clone()
        .into_view_info()
        .unwrap();
    let table2_info = infos
        .get(&table2_ident)
        .unwrap()
        .clone()
        .into_table_info()
        .unwrap();

    // Hide table2 and block view action
    authz.hide(&format!(
        "table:{}/{}",
        warehouse_resp.warehouse_id, table2_info.tabular_id
    ));
    authz.block_action(&format!("view:{:?}", CatalogViewAction::Drop));

    let warehouse = PostgresBackend::get_active_warehouse_by_id(
        warehouse_resp.warehouse_id,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();
    let ns_hierarchy = PostgresBackend::get_namespace(
        warehouse_resp.warehouse_id,
        &ns.namespace,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();
    let parent_namespaces = ns_hierarchy
        .parents
        .iter()
        .map(|ns| (ns.namespace_id(), ns.clone()))
        .collect();

    // Mix tables and views in different order
    let actions: Vec<(&NamespaceWithParent, TestTabularAction<'_>)> = vec![
        (
            &ns_hierarchy.namespace,
            ActionOnTableOrView::Table(ActionOnTable {
                info: &table1_info,
                action: CatalogTableAction::GetMetadata,
                user: None,
                is_delegated_execution: false,
            }),
        ),
        (
            &ns_hierarchy.namespace,
            ActionOnTableOrView::View(ActionOnView {
                info: &view1_info,
                action: CatalogViewAction::Drop,
                user: None,
                is_delegated_execution: false,
            }), // Blocked
        ),
        (
            &ns_hierarchy.namespace,
            ActionOnTableOrView::Table(ActionOnTable {
                info: &table2_info,
                action: CatalogTableAction::ReadData,
                user: None,
                is_delegated_execution: false,
            }), // Hidden
        ),
        (
            &ns_hierarchy.namespace,
            ActionOnTableOrView::View(ActionOnView {
                info: &view1_info,
                action: CatalogViewAction::GetMetadata,
                user: None,
                is_delegated_execution: false,
            }), // Allowed
        ),
    ];

    let result = authz
        .are_allowed_tabular_actions_vec(
            &lakekeeper_integration_tests::random_request_metadata(),
            &warehouse,
            &parent_namespaces,
            &actions,
        )
        .await
        .unwrap()
        .into_inner();

    // Expected: table1 allowed, view1 drop blocked, table2 hidden, view1 get allowed
    assert_eq!(result, vec![true, false, false, true]);
}

/// Helper to load generic table info + warehouse + namespace hierarchy for tests.
async fn load_generic_table_test_ctx(
    ctx: &ApiContext<
        lakekeeper::service::State<
            HidingAuthorizer,
            PostgresBackend,
            lakekeeper_storage_postgres::SecretsState,
        >,
    >,
    warehouse_id: lakekeeper::service::WarehouseId,
    ns: &iceberg_ext::catalog::rest::CreateNamespaceResponse,
    gt_name: &str,
) -> (
    std::sync::Arc<lakekeeper::service::ResolvedWarehouse>,
    lakekeeper::service::NamespaceHierarchy,
    lakekeeper::service::GenericTableInfo,
) {
    let ns_id = lakekeeper::service::NamespaceId::from(
        ns.properties
            .as_ref()
            .unwrap()
            .get("namespace_id")
            .unwrap()
            .parse::<uuid::Uuid>()
            .unwrap(),
    );
    let mut t =
        <PostgresBackend as CatalogStore>::Transaction::begin_read(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    let gt_info =
        PostgresBackend::load_generic_table(warehouse_id, ns_id, gt_name, t.transaction())
            .await
            .unwrap();
    t.commit().await.unwrap();

    let warehouse =
        PostgresBackend::get_active_warehouse_by_id(warehouse_id, ctx.v1_state.catalog.clone())
            .await
            .unwrap()
            .unwrap();
    let ns_hierarchy =
        PostgresBackend::get_namespace(warehouse_id, &ns.namespace, ctx.v1_state.catalog.clone())
            .await
            .unwrap()
            .unwrap();
    (warehouse, ns_hierarchy, gt_info)
}

#[sqlx::test]
async fn test_generic_table_actions_all_allowed(pool: PgPool) {
    let authz = HidingAuthorizer::new();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool)
        .authorizer(authz.clone())
        .storage_profile(memory_io_profile())
        .build()
        .setup()
        .await;

    let prefix = warehouse_resp.warehouse_id.to_string();
    let ns = create_ns(ctx.clone(), prefix.clone(), "test_ns".to_string()).await;
    let _gt = create_generic_table(ctx.clone(), &prefix, "test_ns", "gt1")
        .await
        .unwrap();

    let (warehouse, ns_hierarchy, gt_info) =
        load_generic_table_test_ctx(&ctx, warehouse_resp.warehouse_id, &ns, "gt1").await;
    let parents = ns_hierarchy
        .parents
        .iter()
        .map(|ns| (ns.namespace_id(), ns.clone()))
        .collect();

    let make = |action| {
        (
            &ns_hierarchy.namespace,
            ActionOnGenericTable {
                info: &gt_info,
                action,
                user: None,
                is_delegated_execution: false,
            },
        )
    };
    let result = authz
        .are_allowed_generic_table_actions_vec(
            &lakekeeper_integration_tests::random_request_metadata(),
            &warehouse,
            &parents,
            &[
                make(CatalogGenericTableAction::GetMetadata),
                make(CatalogGenericTableAction::ReadData),
                make(CatalogGenericTableAction::WriteData),
                make(CatalogGenericTableAction::Drop),
                make(CatalogGenericTableAction::IncludeInList),
            ],
        )
        .await
        .unwrap()
        .into_inner();

    assert_eq!(result, vec![true, true, true, true, true]);
}

#[sqlx::test]
async fn test_generic_table_actions_hidden(pool: PgPool) {
    let authz = HidingAuthorizer::new();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool)
        .authorizer(authz.clone())
        .storage_profile(memory_io_profile())
        .build()
        .setup()
        .await;

    let prefix = warehouse_resp.warehouse_id.to_string();
    let ns = create_ns(ctx.clone(), prefix.clone(), "test_ns".to_string()).await;
    let _gt = create_generic_table(ctx.clone(), &prefix, "test_ns", "gt1")
        .await
        .unwrap();

    let (warehouse, ns_hierarchy, gt_info) =
        load_generic_table_test_ctx(&ctx, warehouse_resp.warehouse_id, &ns, "gt1").await;

    // Hide the generic table
    authz.hide(&format!(
        "generic_table:{}/{}",
        warehouse_resp.warehouse_id, gt_info.generic_table_id
    ));

    let parents = ns_hierarchy
        .parents
        .iter()
        .map(|ns| (ns.namespace_id(), ns.clone()))
        .collect();

    let make = |action| {
        (
            &ns_hierarchy.namespace,
            ActionOnGenericTable {
                info: &gt_info,
                action,
                user: None,
                is_delegated_execution: false,
            },
        )
    };
    let result = authz
        .are_allowed_generic_table_actions_vec(
            &lakekeeper_integration_tests::random_request_metadata(),
            &warehouse,
            &parents,
            &[
                make(CatalogGenericTableAction::GetMetadata),
                make(CatalogGenericTableAction::Drop),
            ],
        )
        .await
        .unwrap()
        .into_inner();

    // Both denied — generic table is hidden
    assert_eq!(result, vec![false, false]);
}

#[sqlx::test]
async fn test_generic_table_actions_blocked(pool: PgPool) {
    let authz = HidingAuthorizer::new();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool)
        .authorizer(authz.clone())
        .storage_profile(memory_io_profile())
        .build()
        .setup()
        .await;

    let prefix = warehouse_resp.warehouse_id.to_string();
    let ns = create_ns(ctx.clone(), prefix.clone(), "test_ns".to_string()).await;
    let _gt = create_generic_table(ctx.clone(), &prefix, "test_ns", "gt1")
        .await
        .unwrap();

    let (warehouse, ns_hierarchy, gt_info) =
        load_generic_table_test_ctx(&ctx, warehouse_resp.warehouse_id, &ns, "gt1").await;

    // Block Drop but not GetMetadata
    authz.block_action(&format!(
        "generic_table:{:?}",
        CatalogGenericTableAction::Drop
    ));

    let parents = ns_hierarchy
        .parents
        .iter()
        .map(|ns| (ns.namespace_id(), ns.clone()))
        .collect();

    let make = |action| {
        (
            &ns_hierarchy.namespace,
            ActionOnGenericTable {
                info: &gt_info,
                action,
                user: None,
                is_delegated_execution: false,
            },
        )
    };
    let result = authz
        .are_allowed_generic_table_actions_vec(
            &lakekeeper_integration_tests::random_request_metadata(),
            &warehouse,
            &parents,
            &[
                make(CatalogGenericTableAction::GetMetadata),
                make(CatalogGenericTableAction::Drop),
            ],
        )
        .await
        .unwrap()
        .into_inner();

    // GetMetadata allowed, Drop blocked
    assert_eq!(result, vec![true, false]);
}

/// Verify that a hidden generic table is denied in the batch tabular authz path
/// (`are_allowed_tabular_actions_vec`), not auto-allowed.
#[sqlx::test]
async fn test_tabular_batch_with_hidden_generic_table(pool: PgPool) {
    let authz = HidingAuthorizer::new();
    let (ctx, warehouse_resp) = SetupTestCatalog::builder()
        .pool(pool)
        .authorizer(authz.clone())
        .storage_profile(memory_io_profile())
        .build()
        .setup()
        .await;

    let prefix = warehouse_resp.warehouse_id.to_string();
    let ns = create_ns(ctx.clone(), prefix.clone(), "test_ns".to_string()).await;
    let _table = create_table(ctx.clone(), &prefix, "test_ns", "t1", false)
        .await
        .unwrap();
    let _gt = create_generic_table(ctx.clone(), &prefix, "test_ns", "gt1")
        .await
        .unwrap();

    // Load table via tabular path
    let table_ident = TableIdent::new(NamespaceIdent::new("test_ns".to_string()), "t1".to_string());
    let tabulars = vec![TabularIdentBorrowed::Table(&table_ident)];
    let infos = PostgresBackend::get_tabular_infos_by_ident(
        warehouse_resp.warehouse_id,
        &tabulars,
        TabularListFlags::active(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    let table_info = infos
        .get(&table_ident)
        .unwrap()
        .clone()
        .into_table_info()
        .unwrap();

    let gt_ident = TableIdent::new(
        NamespaceIdent::new("test_ns".to_string()),
        "gt1".to_string(),
    );
    let gt_infos = PostgresBackend::get_tabular_infos_by_ident(
        warehouse_resp.warehouse_id,
        &[TabularIdentBorrowed::GenericTable(&gt_ident)],
        TabularListFlags::active(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    let gt_info = gt_infos
        .get(&gt_ident)
        .unwrap()
        .clone()
        .into_generic_table_info()
        .unwrap();

    // Hide the generic table
    authz.hide(&format!(
        "generic_table:{}/{}",
        warehouse_resp.warehouse_id, gt_info.tabular_id
    ));

    let warehouse = PostgresBackend::get_active_warehouse_by_id(
        warehouse_resp.warehouse_id,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();
    let ns_hierarchy = PostgresBackend::get_namespace(
        warehouse_resp.warehouse_id,
        &ns.namespace,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();
    let parent_namespaces = ns_hierarchy
        .parents
        .iter()
        .map(|n| (n.namespace_id(), n.clone()))
        .collect();

    let actions: Vec<(&NamespaceWithParent, TestTabularAction<'_>)> = vec![
        (
            &ns_hierarchy.namespace,
            ActionOnTableOrView::Table(ActionOnTable {
                info: &table_info,
                action: CatalogTableAction::GetMetadata,
                user: None,
                is_delegated_execution: false,
            }),
        ),
        (
            &ns_hierarchy.namespace,
            ActionOnTableOrView::GenericTable(ActionOnGenericTable {
                info: &gt_info,
                action: CatalogGenericTableAction::IncludeInList,
                user: None,
                is_delegated_execution: false,
            }),
        ),
    ];

    let result = authz
        .are_allowed_tabular_actions_vec(
            &lakekeeper_integration_tests::random_request_metadata(),
            &warehouse,
            &parent_namespaces,
            &actions,
        )
        .await
        .unwrap()
        .into_inner();

    // table: allowed, generic table: hidden → denied
    assert_eq!(result, vec![true, false]);
}
