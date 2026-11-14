//! Cross-type name-collision tests for generic tables.
//!
//! Iceberg tables, views, and generic tables share the same namespace; the
//! `tabular(namespace_id, name)` UNIQUE constraint must prevent a generic
//! table from taking a name already held by an Iceberg table or view (and
//! vice versa). These tests pin that behaviour at the API layer.
//!
//! Each test follows the same shape: create one tabular, then attempt to
//! create another of a different kind with the same name and assert the
//! second call fails with `409 CONFLICT`.
use http::StatusCode;
use iceberg::{NamespaceIdent, TableIdent};
use lakekeeper::{
    api::{
        ApiContext,
        iceberg::{
            types::{DropParams, Prefix},
            v1::{TableParameters, tables::TablesService as _},
        },
        management::v1::warehouse::TabularDeleteProfile,
    },
    server::CatalogServer,
    service::{State, authz::AllowAllAuthorizer},
};
use lakekeeper_integration_tests::{
    create_generic_table, create_ns, create_table, create_view, memory_io_profile,
    random_request_metadata, setup,
};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;
use uuid::Uuid;

type TestApiContext = ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>;

async fn make_ns(pool: PgPool) -> (TestApiContext, String, String) {
    let storage_profile = memory_io_profile();
    let authorizer = AllowAllAuthorizer::default();
    let (ctx, warehouse) = setup(
        pool,
        storage_profile,
        None,
        authorizer,
        TabularDeleteProfile::Hard {},
        None,
        1,
        None,
    )
    .await;
    let prefix = warehouse.warehouse_id.to_string();
    let ns_name = format!("ns_{}", Uuid::now_v7());
    create_ns(ctx.clone(), prefix.clone(), ns_name.clone()).await;
    (ctx, prefix, ns_name)
}

#[sqlx::test]
async fn test_iceberg_table_blocks_generic_table_with_same_name(pool: PgPool) {
    let (ctx, prefix, ns_name) = make_ns(pool).await;
    let name = "collide";

    create_table(
        ctx.clone(),
        prefix.clone(),
        ns_name.clone(),
        name.to_string(),
        false,
    )
    .await
    .unwrap();

    let err = create_generic_table(ctx, prefix, ns_name, name.to_string())
        .await
        .expect_err("generic table create must fail when iceberg table holds the name");
    assert_eq!(
        err.error.code,
        StatusCode::CONFLICT,
        "expected CONFLICT, got: {err:?}"
    );
}

#[sqlx::test]
async fn test_generic_table_blocks_iceberg_table_with_same_name(pool: PgPool) {
    let (ctx, prefix, ns_name) = make_ns(pool).await;
    let name = "collide";

    create_generic_table(
        ctx.clone(),
        prefix.clone(),
        ns_name.clone(),
        name.to_string(),
    )
    .await
    .unwrap();

    let err = create_table(ctx, prefix, ns_name, name.to_string(), false)
        .await
        .expect_err("iceberg table create must fail when generic table holds the name");
    assert_eq!(
        err.error.code,
        StatusCode::CONFLICT,
        "expected CONFLICT, got: {err:?}"
    );
}

#[sqlx::test]
async fn test_view_blocks_generic_table_with_same_name(pool: PgPool) {
    let (ctx, prefix, ns_name) = make_ns(pool).await;
    let name = "collide";

    create_view(ctx.clone(), &prefix, &ns_name, name, None)
        .await
        .unwrap();

    let err = create_generic_table(ctx, prefix, ns_name, name.to_string())
        .await
        .expect_err("generic table create must fail when view holds the name");
    assert_eq!(
        err.error.code,
        StatusCode::CONFLICT,
        "expected CONFLICT, got: {err:?}"
    );
}

#[sqlx::test]
async fn test_generic_table_blocks_view_with_same_name(pool: PgPool) {
    let (ctx, prefix, ns_name) = make_ns(pool).await;
    let name = "collide";

    create_generic_table(
        ctx.clone(),
        prefix.clone(),
        ns_name.clone(),
        name.to_string(),
    )
    .await
    .unwrap();

    let err = create_view(ctx, &prefix, &ns_name, name, None)
        .await
        .expect_err("view create must fail when generic table holds the name");
    assert_eq!(
        err.error.code,
        StatusCode::CONFLICT,
        "expected CONFLICT, got: {err:?}"
    );
}

/// After hard-dropping the prior tabular, the name should be reusable by a
/// different kind. Catches over-eager unique constraints that linger past
/// deletion.
#[sqlx::test]
async fn test_name_reusable_after_hard_drop(pool: PgPool) {
    let (ctx, prefix, ns_name) = make_ns(pool).await;
    let name = "reuse";

    create_table(
        ctx.clone(),
        prefix.clone(),
        ns_name.clone(),
        name.to_string(),
        false,
    )
    .await
    .unwrap();
    CatalogServer::drop_table(
        TableParameters {
            prefix: Some(Prefix(prefix.clone())),
            table: TableIdent::new(NamespaceIdent::new(ns_name.clone()), name.to_string()),
        },
        DropParams {
            purge_requested: false,
            force: true,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    create_generic_table(ctx, prefix, ns_name, name.to_string())
        .await
        .expect("name should be reusable after hard drop");
}
