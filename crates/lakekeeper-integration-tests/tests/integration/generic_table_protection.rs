//! Generic table protection tests.
//!
//! Cover: set/get protection round-trip, drop blocked when protected,
//! drop with `force=true` bypasses protection.
use http::StatusCode;
use iceberg::NamespaceIdent;
use lakekeeper::{
    api::{
        ApiContext,
        data::v1::generic_tables::{
            GenericTableParameters, GenericTableService as _, ListGenericTablesQuery,
        },
        iceberg::{
            types::{DropParams, Prefix},
            v1::{DataAccessMode, namespace::NamespaceParameters},
        },
        management::v1::{ApiServer, generic_table::GenericTableManagementService as _},
    },
    server::CatalogServer,
    service::{GenericTableId, State, authz::AllowAllAuthorizer},
};
use lakekeeper_integration_tests::{
    create_generic_table, create_ns, memory_io_profile, random_request_metadata, setup,
};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;
use uuid::Uuid;

type TestApiContext = ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>;

async fn make_ns_with_profile(
    pool: PgPool,
    delete_profile: lakekeeper::api::management::v1::warehouse::TabularDeleteProfile,
) -> (TestApiContext, String, String, lakekeeper::WarehouseId) {
    let storage_profile = memory_io_profile();
    let authorizer = AllowAllAuthorizer::default();
    let (ctx, warehouse) = setup(
        pool,
        storage_profile,
        None,
        authorizer,
        delete_profile,
        None,
        1,
        None,
    )
    .await;
    let prefix = warehouse.warehouse_id.to_string();
    let ns_name = format!("ns_{}", Uuid::now_v7());
    create_ns(ctx.clone(), prefix.clone(), ns_name.clone()).await;
    (ctx, prefix, ns_name, warehouse.warehouse_id)
}

async fn make_ns(pool: PgPool) -> (TestApiContext, String, String, lakekeeper::WarehouseId) {
    make_ns_with_profile(
        pool,
        lakekeeper::api::management::v1::warehouse::TabularDeleteProfile::Hard {},
    )
    .await
}

async fn drop_via_catalog_api(
    ctx: TestApiContext,
    prefix: &str,
    ns_name: &str,
    name: &str,
    force: bool,
) -> lakekeeper::api::Result<()> {
    CatalogServer::drop_generic_table(
        GenericTableParameters {
            prefix: Some(Prefix(prefix.to_string())),
            namespace: NamespaceIdent::new(ns_name.to_string()),
            table_name: name.to_string(),
        },
        DropParams {
            purge_requested: true,
            force,
        },
        ctx,
        random_request_metadata(),
    )
    .await
}

#[sqlx::test]
async fn test_set_get_protection_round_trip(pool: PgPool) {
    let (ctx, prefix, ns_name, warehouse_id) = make_ns(pool).await;
    let name = "gt_protect";

    let created = create_generic_table(ctx.clone(), prefix.clone(), ns_name.clone(), name)
        .await
        .unwrap();
    // Default is unprotected.
    assert!(!created.table.protected);

    // No id in response yet — use the load to get it via metadata read.
    let load = CatalogServer::load_generic_table(
        GenericTableParameters {
            prefix: Some(Prefix(prefix.clone())),
            namespace: NamespaceIdent::new(ns_name.clone()),
            table_name: name.to_string(),
        },
        ctx.clone(),
        DataAccessMode::default(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert!(!load.table.protected);

    // Resolve generic_table_id via a search-style helper-less path: query the listing.
    let list = CatalogServer::list_generic_tables(
        NamespaceParameters {
            prefix: Some(Prefix(prefix.clone())),
            namespace: NamespaceIdent::new(ns_name.clone()),
        },
        ListGenericTablesQuery::default(),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let id = list
        .identifiers
        .iter()
        .find(|i| i.name == name)
        .and_then(|i| i.id)
        .expect("generic table id present in list response");
    let gt_id = GenericTableId::from(*id);

    // Set protected = true
    let resp = ApiServer::set_generic_table_protection(
        gt_id,
        warehouse_id,
        true,
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert!(resp.protected);
    assert!(resp.updated_at.is_some());

    // Get returns protected = true
    let resp = ApiServer::get_generic_table_protection(
        gt_id,
        warehouse_id,
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert!(resp.protected);

    // Load also reflects protected = true
    let load = CatalogServer::load_generic_table(
        GenericTableParameters {
            prefix: Some(Prefix(prefix.clone())),
            namespace: NamespaceIdent::new(ns_name.clone()),
            table_name: name.to_string(),
        },
        ctx.clone(),
        DataAccessMode::default(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert!(load.table.protected);

    // Flip back
    let resp = ApiServer::set_generic_table_protection(
        gt_id,
        warehouse_id,
        false,
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert!(!resp.protected);
}

#[sqlx::test]
async fn test_drop_protected_generic_table_without_force_fails(pool: PgPool) {
    let (ctx, prefix, ns_name, warehouse_id) = make_ns(pool).await;
    let name = "gt_protect_drop";

    create_generic_table(ctx.clone(), prefix.clone(), ns_name.clone(), name)
        .await
        .unwrap();

    let list = CatalogServer::list_generic_tables(
        NamespaceParameters {
            prefix: Some(Prefix(prefix.clone())),
            namespace: NamespaceIdent::new(ns_name.clone()),
        },
        ListGenericTablesQuery::default(),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let gt_id = GenericTableId::from(
        *list
            .identifiers
            .iter()
            .find(|i| i.name == name)
            .and_then(|i| i.id)
            .unwrap(),
    );

    ApiServer::set_generic_table_protection(
        gt_id,
        warehouse_id,
        true,
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let err = drop_via_catalog_api(ctx.clone(), &prefix, &ns_name, name, false)
        .await
        .expect_err("drop should be blocked by protection");
    assert_eq!(
        err.error.code,
        StatusCode::CONFLICT,
        "expected CONFLICT, got: {err:?}"
    );

    // With force, the drop succeeds.
    drop_via_catalog_api(ctx, &prefix, &ns_name, name, true)
        .await
        .unwrap();
}

// Exercises the `mark_tabular_as_deleted` SQL guard (soft-delete path) — a
// separate code branch from `drop_tabular` (hard-delete path) tested above.
// Both branches share the `(NOT protected) OR force` predicate but each has
// its own statement.
#[sqlx::test]
async fn test_soft_delete_protected_generic_table_without_force_fails(pool: PgPool) {
    let (ctx, prefix, ns_name, warehouse_id) = make_ns_with_profile(
        pool,
        lakekeeper::api::management::v1::warehouse::TabularDeleteProfile::Soft {
            expiration_seconds: chrono::Duration::seconds(60),
        },
    )
    .await;
    let name = "gt_soft_protect";

    create_generic_table(ctx.clone(), prefix.clone(), ns_name.clone(), name)
        .await
        .unwrap();

    let list = CatalogServer::list_generic_tables(
        NamespaceParameters {
            prefix: Some(Prefix(prefix.clone())),
            namespace: NamespaceIdent::new(ns_name.clone()),
        },
        ListGenericTablesQuery::default(),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let gt_id = GenericTableId::from(
        *list
            .identifiers
            .iter()
            .find(|i| i.name == name)
            .and_then(|i| i.id)
            .unwrap(),
    );

    ApiServer::set_generic_table_protection(
        gt_id,
        warehouse_id,
        true,
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let err = drop_via_catalog_api(ctx.clone(), &prefix, &ns_name, name, false)
        .await
        .expect_err("soft-delete drop should be blocked by protection");
    assert_eq!(
        err.error.code,
        StatusCode::CONFLICT,
        "expected CONFLICT, got: {err:?}"
    );

    // With force, the soft-delete succeeds.
    drop_via_catalog_api(ctx, &prefix, &ns_name, name, true)
        .await
        .unwrap();
}
