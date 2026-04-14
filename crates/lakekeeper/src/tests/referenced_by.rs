use std::collections::HashMap;

use iceberg::{NamespaceIdent, TableIdent};
use sqlx::PgPool;

use crate::{
    WarehouseId,
    api::{
        ApiContext,
        iceberg::{
            types::{Prefix, ReferencingView},
            v1::{
                NamespaceParameters, TableParameters, ViewParameters,
                tables::{DataAccessMode, LoadTableRequest, TablesService as _},
                views::{LoadViewRequest, ViewService as _},
            },
        },
    },
    config::{MatchedEngines, TrinoEngineConfig, TrustedEngine},
    implementations::postgres::{PostgresBackend, SecretsState},
    request_metadata::RequestMetadata,
    server::CatalogServer,
    service::{
        AuthZTableInfo as _, AuthZViewInfo as _, CatalogTabularOps as _, State, TabularListFlags,
        UserId,
        authz::{AllowAllAuthorizer, Authorizer, UserOrRole, tests::HidingAuthorizer},
    },
    tests::{SetupTestCatalog, create_view_request, random_request_metadata},
};

type Server<A> = CatalogServer<PostgresBackend, A, SecretsState>;

const ENGINE_IDP: &str = "test-idp";

fn trino_engine() -> TrustedEngine {
    TrustedEngine::Trino(TrinoEngineConfig {
        owner_property: "trino.run-as-owner".to_string(),
        identities: HashMap::new(),
    })
}

fn matched_engines() -> MatchedEngines {
    MatchedEngines::single(trino_engine())
}

fn request_with_engine() -> RequestMetadata {
    let mut m = RequestMetadata::test_user(UserId::new_unchecked(ENGINE_IDP, "engine-user"));
    m.set_engines(matched_engines());
    m
}

fn request_as_user(name: &str) -> RequestMetadata {
    let mut m = RequestMetadata::test_user(UserId::new_unchecked(ENGINE_IDP, name));
    m.set_engines(matched_engines());
    m
}

fn user(name: &str) -> UserOrRole {
    UserOrRole::User(UserId::new_unchecked(ENGINE_IDP, name))
}

fn table_ident(ns_name: &str, name: &str) -> TableIdent {
    TableIdent::new(NamespaceIdent::new(ns_name.to_string()), name.to_string())
}

fn prefix(wh: &crate::tests::TestWarehouseResponse) -> Prefix {
    Prefix(wh.warehouse_id.to_string())
}

fn ns_params(wh: &crate::tests::TestWarehouseResponse, ns_name: &str) -> NamespaceParameters {
    NamespaceParameters {
        prefix: Some(prefix(wh)),
        namespace: NamespaceIdent::new(ns_name.to_string()),
    }
}

fn referenced_by(views: &[TableIdent]) -> Vec<ReferencingView> {
    views.iter().cloned().map(ReferencingView::new).collect()
}

async fn setup_ns_and_table<A: Authorizer>(
    ctx: &ApiContext<State<A, PostgresBackend, SecretsState>>,
    wh: &crate::tests::TestWarehouseResponse,
) {
    let p = wh.warehouse_id.to_string();
    crate::tests::create_ns(ctx.clone(), p.clone(), "ns".into()).await;
    crate::tests::create_table(ctx.clone(), &p, "ns", "my_table", false)
        .await
        .unwrap();
}

async fn create_invoker_view<A: Authorizer>(
    ctx: &ApiContext<State<A, PostgresBackend, SecretsState>>,
    wh: &crate::tests::TestWarehouseResponse,
    name: &str,
) {
    Server::create_view(
        ns_params(wh, "ns"),
        create_view_request(Some(name), None),
        ctx.clone(),
        DataAccessMode::ClientManaged,
        random_request_metadata(),
    )
    .await
    .unwrap();
}

async fn create_definer_view<A: Authorizer>(
    ctx: &ApiContext<State<A, PostgresBackend, SecretsState>>,
    wh: &crate::tests::TestWarehouseResponse,
    name: &str,
    owner: &str,
) {
    let mut rq = create_view_request(Some(name), None);
    rq.properties
        .insert("trino.run-as-owner".to_string(), owner.to_string());
    Server::create_view(
        ns_params(wh, "ns"),
        rq,
        ctx.clone(),
        DataAccessMode::ClientManaged,
        request_with_engine(),
    )
    .await
    .unwrap();
}

async fn table_object_key(
    ctx: &ApiContext<State<impl Authorizer, PostgresBackend, SecretsState>>,
    whi: WarehouseId,
    table: &TableIdent,
) -> String {
    let info = PostgresBackend::get_table_info(
        whi,
        table.clone(),
        TabularListFlags::active(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .expect("table should exist");
    format!("table:{}/{}", whi, info.table_id())
}

async fn view_object_key(
    ctx: &ApiContext<State<impl Authorizer, PostgresBackend, SecretsState>>,
    whi: WarehouseId,
    view: &TableIdent,
) -> String {
    let info = PostgresBackend::get_view_info(
        whi,
        view.clone(),
        TabularListFlags::active(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .expect("view should exist");
    format!("view:{}/{}", whi, info.view_id())
}

// ---- Basic tests ----

#[sqlx::test]
async fn test_load_table_with_referenced_by_succeeds(pool: PgPool) {
    let (ctx, wh) = SetupTestCatalog::builder()
        .pool(pool)
        .authorizer(AllowAllAuthorizer::default())
        .build()
        .setup()
        .await;

    setup_ns_and_table(&ctx, &wh).await;
    create_invoker_view(&ctx, &wh, "my_view").await;

    let result = Server::load_table(
        TableParameters {
            prefix: Some(prefix(&wh)),
            table: table_ident("ns", "my_table"),
        },
        LoadTableRequest::builder()
            .referenced_by(Some(referenced_by(&[table_ident("ns", "my_view")])))
            .build(),
        ctx.clone(),
        request_with_engine(),
    )
    .await;

    assert!(
        result.is_ok(),
        "load_table with referenced_by should succeed: {result:?}"
    );
}

#[sqlx::test]
async fn test_load_table_referenced_by_without_engine_ignores_views(pool: PgPool) {
    let (ctx, wh) = SetupTestCatalog::builder()
        .pool(pool)
        .authorizer(AllowAllAuthorizer::default())
        .build()
        .setup()
        .await;

    setup_ns_and_table(&ctx, &wh).await;

    let result = Server::load_table(
        TableParameters {
            prefix: Some(prefix(&wh)),
            table: table_ident("ns", "my_table"),
        },
        LoadTableRequest::builder()
            .referenced_by(Some(referenced_by(&[table_ident(
                "ns",
                "nonexistent_view",
            )])))
            .build(),
        ctx.clone(),
        random_request_metadata(),
    )
    .await;

    assert!(
        result.is_ok(),
        "without engine, referenced_by should be ignored: {result:?}"
    );
}

#[sqlx::test]
async fn test_load_table_referenced_by_with_engine_rejects_missing_view(pool: PgPool) {
    let (ctx, wh) = SetupTestCatalog::builder()
        .pool(pool)
        .authorizer(AllowAllAuthorizer::default())
        .build()
        .setup()
        .await;

    setup_ns_and_table(&ctx, &wh).await;

    let result = Server::<AllowAllAuthorizer>::load_table(
        TableParameters {
            prefix: Some(prefix(&wh)),
            table: table_ident("ns", "my_table"),
        },
        LoadTableRequest::builder()
            .referenced_by(Some(referenced_by(&[table_ident(
                "ns",
                "nonexistent_view",
            )])))
            .build(),
        ctx.clone(),
        request_with_engine(),
    )
    .await;

    assert!(
        result.is_err(),
        "with engine, nonexistent view in referenced_by should fail: {result:?}"
    );
}

#[sqlx::test]
async fn test_load_table_with_chained_views(pool: PgPool) {
    let (ctx, wh) = SetupTestCatalog::builder()
        .pool(pool)
        .authorizer(AllowAllAuthorizer::default())
        .build()
        .setup()
        .await;

    setup_ns_and_table(&ctx, &wh).await;
    create_invoker_view(&ctx, &wh, "inner_view").await;
    create_invoker_view(&ctx, &wh, "outer_view").await;

    let result = Server::load_table(
        TableParameters {
            prefix: Some(prefix(&wh)),
            table: table_ident("ns", "my_table"),
        },
        LoadTableRequest::builder()
            .referenced_by(Some(referenced_by(&[
                table_ident("ns", "outer_view"),
                table_ident("ns", "inner_view"),
            ])))
            .build(),
        ctx.clone(),
        request_with_engine(),
    )
    .await;

    assert!(
        result.is_ok(),
        "load_table through chained views should succeed: {result:?}"
    );
}

#[sqlx::test]
async fn test_load_view_with_referenced_by(pool: PgPool) {
    let (ctx, wh) = SetupTestCatalog::builder()
        .pool(pool)
        .authorizer(AllowAllAuthorizer::default())
        .build()
        .setup()
        .await;

    let p = wh.warehouse_id.to_string();
    crate::tests::create_ns(ctx.clone(), p, "ns".into()).await;
    create_invoker_view(&ctx, &wh, "outer_view").await;
    create_invoker_view(&ctx, &wh, "inner_view").await;

    let result = Server::load_view(
        ViewParameters {
            prefix: Some(prefix(&wh)),
            view: table_ident("ns", "inner_view"),
        },
        LoadViewRequest {
            data_access: DataAccessMode::ClientManaged,
            referenced_by: Some(referenced_by(&[table_ident("ns", "outer_view")])),
        },
        ctx.clone(),
        request_with_engine(),
    )
    .await;

    assert!(
        result.is_ok(),
        "load_view with referenced_by should succeed: {result:?}"
    );
}

// ---- Hiding tests (global) ----

#[sqlx::test]
async fn test_load_table_hidden_view_in_chain_denied(pool: PgPool) {
    let authz = HidingAuthorizer::new();
    let (ctx, wh) = SetupTestCatalog::builder()
        .pool(pool)
        .authorizer(authz.clone())
        .build()
        .setup()
        .await;
    let whi = wh.warehouse_id;

    setup_ns_and_table(&ctx, &wh).await;
    create_invoker_view(&ctx, &wh, "my_view").await;

    let key = view_object_key(&ctx, whi, &table_ident("ns", "my_view")).await;
    authz.hide(&key);

    let result = Server::load_table(
        TableParameters {
            prefix: Some(prefix(&wh)),
            table: table_ident("ns", "my_table"),
        },
        LoadTableRequest::builder()
            .referenced_by(Some(referenced_by(&[table_ident("ns", "my_view")])))
            .build(),
        ctx.clone(),
        request_with_engine(),
    )
    .await;

    assert!(
        result.is_err(),
        "load_table should fail when view in chain is hidden"
    );
}

#[sqlx::test]
async fn test_load_table_hidden_table_denied(pool: PgPool) {
    let authz = HidingAuthorizer::new();
    let (ctx, wh) = SetupTestCatalog::builder()
        .pool(pool)
        .authorizer(authz.clone())
        .build()
        .setup()
        .await;
    let whi = wh.warehouse_id;

    setup_ns_and_table(&ctx, &wh).await;
    create_invoker_view(&ctx, &wh, "my_view").await;

    let key = table_object_key(&ctx, whi, &table_ident("ns", "my_table")).await;
    authz.hide(&key);

    let result = Server::load_table(
        TableParameters {
            prefix: Some(prefix(&wh)),
            table: table_ident("ns", "my_table"),
        },
        LoadTableRequest::builder()
            .referenced_by(Some(referenced_by(&[table_ident("ns", "my_view")])))
            .build(),
        ctx.clone(),
        request_with_engine(),
    )
    .await;

    assert!(
        result.is_err(),
        "load_table should fail when table is hidden"
    );
}

// ---- DEFINER delegation tests ----

/// User A cannot access the table directly, but succeeds through a DEFINER view
/// because owner B's permissions are used for the table check.
#[sqlx::test]
async fn test_definer_delegates_table_access_to_owner(pool: PgPool) {
    let authz = HidingAuthorizer::new();
    let (ctx, wh) = SetupTestCatalog::builder()
        .pool(pool)
        .authorizer(authz.clone())
        .build()
        .setup()
        .await;
    let whi = wh.warehouse_id;

    setup_ns_and_table(&ctx, &wh).await;
    create_definer_view(&ctx, &wh, "definer_view", "owner_b").await;

    // User A cannot access the table directly
    let table_key = table_object_key(&ctx, whi, &table_ident("ns", "my_table")).await;
    authz.hide_for_user(&user("user_a"), &table_key);

    // But can access it through the DEFINER view (owner_b has access)
    let result = Server::load_table(
        TableParameters {
            prefix: Some(prefix(&wh)),
            table: table_ident("ns", "my_table"),
        },
        LoadTableRequest::builder()
            .referenced_by(Some(referenced_by(&[table_ident("ns", "definer_view")])))
            .build(),
        ctx.clone(),
        request_as_user("user_a"),
    )
    .await;

    assert!(
        result.is_ok(),
        "user_a should access table via DEFINER view: {result:?}"
    );
}

/// When the DEFINER view owner loses table access, load fails.
#[sqlx::test]
async fn test_definer_fails_when_owner_loses_table_access(pool: PgPool) {
    let authz = HidingAuthorizer::new();
    let (ctx, wh) = SetupTestCatalog::builder()
        .pool(pool)
        .authorizer(authz.clone())
        .build()
        .setup()
        .await;
    let whi = wh.warehouse_id;

    setup_ns_and_table(&ctx, &wh).await;
    create_definer_view(&ctx, &wh, "definer_view", "owner_b").await;

    // Revoke owner_b's access to the table
    let table_key = table_object_key(&ctx, whi, &table_ident("ns", "my_table")).await;
    authz.hide_for_user(&user("owner_b"), &table_key);

    let result = Server::load_table(
        TableParameters {
            prefix: Some(prefix(&wh)),
            table: table_ident("ns", "my_table"),
        },
        LoadTableRequest::builder()
            .referenced_by(Some(referenced_by(&[table_ident("ns", "definer_view")])))
            .build(),
        ctx.clone(),
        request_as_user("user_a"),
    )
    .await;

    assert!(
        result.is_err(),
        "should fail when DEFINER owner loses table access"
    );
}

// ---- Chain with DEFINER + INVOKER user switching ----

/// Chain: `User_A` → View1(DEFINER, owner=B) → View2(INVOKER) → View3(DEFINER, owner=C) → Table
///
/// Authorization checks:
///   View1: checked as `User_A`
///   View2: checked as B (switched by View1's DEFINER)
///   View3: checked as B (INVOKER inherits)
///   Table: checked as C (switched by View3's DEFINER)
///
/// We hide the table for `user_a` AND `owner_b` to prove that only `owner_c`
/// (the final DEFINER owner) is used for the table check.
#[sqlx::test]
async fn test_chain_definer_invoker_definer_succeeds(pool: PgPool) {
    let authz = HidingAuthorizer::new();
    let (ctx, wh) = SetupTestCatalog::builder()
        .pool(pool)
        .authorizer(authz.clone())
        .build()
        .setup()
        .await;
    let whi = wh.warehouse_id;

    setup_ns_and_table(&ctx, &wh).await;
    create_definer_view(&ctx, &wh, "view1_definer_b", "owner_b").await;
    create_invoker_view(&ctx, &wh, "view2_invoker").await;
    create_definer_view(&ctx, &wh, "view3_definer_c", "owner_c").await;

    // Neither user_a nor owner_b can access the table directly —
    // only owner_c can, proving the DEFINER chain switches correctly.
    let table_key = table_object_key(&ctx, whi, &table_ident("ns", "my_table")).await;
    authz.hide_for_user(&user("user_a"), &table_key);
    authz.hide_for_user(&user("owner_b"), &table_key);

    let result = Server::load_table(
        TableParameters {
            prefix: Some(prefix(&wh)),
            table: table_ident("ns", "my_table"),
        },
        LoadTableRequest::builder()
            .referenced_by(Some(referenced_by(&[
                table_ident("ns", "view1_definer_b"),
                table_ident("ns", "view2_invoker"),
                table_ident("ns", "view3_definer_c"),
            ])))
            .build(),
        ctx.clone(),
        request_as_user("user_a"),
    )
    .await;

    assert!(
        result.is_ok(),
        "only owner_c should be checked for table: {result:?}"
    );
}

/// Same chain, but `owner_b` loses access to view2 → fails
/// (view2 is checked as `owner_b` because view1 is DEFINER)
#[sqlx::test]
async fn test_chain_fails_when_definer_owner_loses_mid_chain_access(pool: PgPool) {
    let authz = HidingAuthorizer::new();
    let (ctx, wh) = SetupTestCatalog::builder()
        .pool(pool)
        .authorizer(authz.clone())
        .build()
        .setup()
        .await;
    let whi = wh.warehouse_id;

    setup_ns_and_table(&ctx, &wh).await;
    create_definer_view(&ctx, &wh, "view1_definer_b", "owner_b").await;
    create_invoker_view(&ctx, &wh, "view2_invoker").await;
    create_definer_view(&ctx, &wh, "view3_definer_c", "owner_c").await;

    // owner_b can't see view2 → chain breaks at view2
    let view2_key = view_object_key(&ctx, whi, &table_ident("ns", "view2_invoker")).await;
    authz.hide_for_user(&user("owner_b"), &view2_key);

    let result = Server::load_table(
        TableParameters {
            prefix: Some(prefix(&wh)),
            table: table_ident("ns", "my_table"),
        },
        LoadTableRequest::builder()
            .referenced_by(Some(referenced_by(&[
                table_ident("ns", "view1_definer_b"),
                table_ident("ns", "view2_invoker"),
                table_ident("ns", "view3_definer_c"),
            ])))
            .build(),
        ctx.clone(),
        request_as_user("user_a"),
    )
    .await;

    assert!(
        result.is_err(),
        "should fail when owner_b can't access view2"
    );
}

/// Same chain, but `owner_c` loses access to table → fails
/// (table is checked as `owner_c` because view3 is DEFINER)
#[sqlx::test]
async fn test_chain_fails_when_final_definer_owner_loses_table_access(pool: PgPool) {
    let authz = HidingAuthorizer::new();
    let (ctx, wh) = SetupTestCatalog::builder()
        .pool(pool)
        .authorizer(authz.clone())
        .build()
        .setup()
        .await;
    let whi = wh.warehouse_id;

    setup_ns_and_table(&ctx, &wh).await;
    create_definer_view(&ctx, &wh, "view1_definer_b", "owner_b").await;
    create_invoker_view(&ctx, &wh, "view2_invoker").await;
    create_definer_view(&ctx, &wh, "view3_definer_c", "owner_c").await;

    // owner_c can't see the table → chain breaks at table
    let table_key = table_object_key(&ctx, whi, &table_ident("ns", "my_table")).await;
    authz.hide_for_user(&user("owner_c"), &table_key);

    let result = Server::load_table(
        TableParameters {
            prefix: Some(prefix(&wh)),
            table: table_ident("ns", "my_table"),
        },
        LoadTableRequest::builder()
            .referenced_by(Some(referenced_by(&[
                table_ident("ns", "view1_definer_b"),
                table_ident("ns", "view2_invoker"),
                table_ident("ns", "view3_definer_c"),
            ])))
            .build(),
        ctx.clone(),
        request_as_user("user_a"),
    )
    .await;

    assert!(
        result.is_err(),
        "should fail when owner_c can't access table"
    );
}

/// INVOKER view checks the calling user (`user_a`), not any owner.
/// If `user_a` can't see view2 (INVOKER, no chain switch), load fails —
/// proving INVOKER doesn't delegate.
#[sqlx::test]
async fn test_invoker_view_checks_calling_user_not_owner(pool: PgPool) {
    let authz = HidingAuthorizer::new();
    let (ctx, wh) = SetupTestCatalog::builder()
        .pool(pool)
        .authorizer(authz.clone())
        .build()
        .setup()
        .await;
    let whi = wh.warehouse_id;

    setup_ns_and_table(&ctx, &wh).await;
    create_invoker_view(&ctx, &wh, "invoker_view").await;

    // Hide the invoker view for user_a
    let view_key = view_object_key(&ctx, whi, &table_ident("ns", "invoker_view")).await;
    authz.hide_for_user(&user("user_a"), &view_key);

    let result = Server::load_table(
        TableParameters {
            prefix: Some(prefix(&wh)),
            table: table_ident("ns", "my_table"),
        },
        LoadTableRequest::builder()
            .referenced_by(Some(referenced_by(&[table_ident("ns", "invoker_view")])))
            .build(),
        ctx.clone(),
        request_as_user("user_a"),
    )
    .await;

    assert!(
        result.is_err(),
        "INVOKER view should check user_a's permissions"
    );
}

/// Same chain, but `user_a` loses access to view1 → fails
/// (view1 is checked as `user_a`, the entry point)
#[sqlx::test]
async fn test_chain_fails_when_user_loses_entry_point_access(pool: PgPool) {
    let authz = HidingAuthorizer::new();
    let (ctx, wh) = SetupTestCatalog::builder()
        .pool(pool)
        .authorizer(authz.clone())
        .build()
        .setup()
        .await;
    let whi = wh.warehouse_id;

    setup_ns_and_table(&ctx, &wh).await;
    create_definer_view(&ctx, &wh, "view1_definer_b", "owner_b").await;
    create_invoker_view(&ctx, &wh, "view2_invoker").await;
    create_definer_view(&ctx, &wh, "view3_definer_c", "owner_c").await;

    // user_a can't see view1 → chain breaks at entry point
    let view1_key = view_object_key(&ctx, whi, &table_ident("ns", "view1_definer_b")).await;
    authz.hide_for_user(&user("user_a"), &view1_key);

    let result = Server::load_table(
        TableParameters {
            prefix: Some(prefix(&wh)),
            table: table_ident("ns", "my_table"),
        },
        LoadTableRequest::builder()
            .referenced_by(Some(referenced_by(&[
                table_ident("ns", "view1_definer_b"),
                table_ident("ns", "view2_invoker"),
                table_ident("ns", "view3_definer_c"),
            ])))
            .build(),
        ctx.clone(),
        request_as_user("user_a"),
    )
    .await;

    assert!(
        result.is_err(),
        "should fail when user_a can't access view1"
    );
}
