use std::collections::HashMap;

use iceberg::{NamespaceIdent, TableIdent};
use sqlx::PgPool;

use crate::{
    WarehouseId,
    api::{
        ApiContext, Result,
        iceberg::{
            types::{Prefix, ReferencingView},
            v1::{
                DataAccess, NamespaceParameters, tables::DataAccessMode, views::ViewService as _,
            },
        },
        v1::generic_tables::{
            GenericTableParameters, GenericTableService as _, LoadGenericTableCredentialsRequest,
            LoadGenericTableCredentialsResponse,
        },
    },
    config::{MatchedEngines, TrinoEngineConfig, TrustedEngine},
    implementations::postgres::{PostgresBackend, SecretsState},
    request_metadata::RequestMetadata,
    server::CatalogServer,
    service::{
        AuthZViewInfo as _, CatalogGenericTableOps as _, CatalogNamespaceOps as _, CatalogStore,
        CatalogTabularOps as _, State, TabularListFlags, Transaction as _, UserId,
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

fn request_as_instance_admin(name: &str) -> RequestMetadata {
    let mut m = RequestMetadata::test_instance_admin(UserId::new_unchecked(ENGINE_IDP, name));
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

fn gt_params(
    wh: &crate::tests::TestWarehouseResponse,
    ns_name: &str,
    name: &str,
) -> GenericTableParameters {
    GenericTableParameters {
        prefix: Some(prefix(wh)),
        namespace: NamespaceIdent::new(ns_name.to_string()),
        table_name: name.to_string(),
    }
}

async fn setup_ns_and_generic_table<A: Authorizer>(
    ctx: &ApiContext<State<A, PostgresBackend, SecretsState>>,
    wh: &crate::tests::TestWarehouseResponse,
) {
    let p = wh.warehouse_id.to_string();
    crate::tests::create_ns(ctx.clone(), p.clone(), "ns".into()).await;
    crate::tests::create_generic_table(ctx.clone(), p, "ns", "my_gt")
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

async fn generic_table_object_key(
    ctx: &ApiContext<State<impl Authorizer, PostgresBackend, SecretsState>>,
    whi: WarehouseId,
    table: &TableIdent,
) -> String {
    let ns =
        PostgresBackend::get_namespace(whi, table.namespace.clone(), ctx.v1_state.catalog.clone())
            .await
            .unwrap()
            .expect("namespace should exist");
    let mut t =
        <PostgresBackend as CatalogStore>::Transaction::begin_read(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    let info = PostgresBackend::load_generic_table(
        whi,
        ns.namespace.namespace_id(),
        &table.name,
        t.transaction(),
    )
    .await
    .expect("generic table should exist");
    t.commit().await.unwrap();
    format!("generic_table:{}/{}", whi, info.generic_table_id)
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

async fn load_credentials<A: Authorizer + Clone>(
    ctx: &ApiContext<State<A, PostgresBackend, SecretsState>>,
    wh: &crate::tests::TestWarehouseResponse,
    gt_name: &str,
    refs: Option<Vec<TableIdent>>,
    request_metadata: RequestMetadata,
) -> Result<LoadGenericTableCredentialsResponse> {
    Server::load_generic_table_credentials(
        gt_params(wh, "ns", gt_name),
        LoadGenericTableCredentialsRequest {
            referenced_by: refs.map(|v| referenced_by(&v)),
        },
        DataAccess::not_specified(),
        ctx.clone(),
        request_metadata,
    )
    .await
}

// ---- Basic tests ----

#[sqlx::test]
async fn test_load_generic_table_credentials_with_referenced_by_succeeds(pool: PgPool) {
    let (ctx, wh) = SetupTestCatalog::builder()
        .pool(pool)
        .authorizer(AllowAllAuthorizer::default())
        .build()
        .setup()
        .await;

    setup_ns_and_generic_table(&ctx, &wh).await;
    create_invoker_view(&ctx, &wh, "my_view").await;

    let result = load_credentials(
        &ctx,
        &wh,
        "my_gt",
        Some(vec![table_ident("ns", "my_view")]),
        request_with_engine(),
    )
    .await;

    assert!(
        result.is_ok(),
        "load_generic_table_credentials with referenced_by should succeed: {result:?}"
    );
}

#[sqlx::test]
async fn test_load_generic_table_credentials_referenced_by_without_engine_ignores_views(
    pool: PgPool,
) {
    let (ctx, wh) = SetupTestCatalog::builder()
        .pool(pool)
        .authorizer(AllowAllAuthorizer::default())
        .build()
        .setup()
        .await;

    setup_ns_and_generic_table(&ctx, &wh).await;

    // Without an engine, referenced_by is dropped — even a nonexistent view
    // shouldn't cause a failure.
    let result = load_credentials(
        &ctx,
        &wh,
        "my_gt",
        Some(vec![table_ident("ns", "nonexistent_view")]),
        random_request_metadata(),
    )
    .await;

    assert!(
        result.is_ok(),
        "without engine, referenced_by should be ignored: {result:?}"
    );
}

#[sqlx::test]
async fn test_load_generic_table_credentials_with_engine_rejects_missing_view(pool: PgPool) {
    let (ctx, wh) = SetupTestCatalog::builder()
        .pool(pool)
        .authorizer(AllowAllAuthorizer::default())
        .build()
        .setup()
        .await;

    setup_ns_and_generic_table(&ctx, &wh).await;

    let result = load_credentials(
        &ctx,
        &wh,
        "my_gt",
        Some(vec![table_ident("ns", "nonexistent_view")]),
        request_with_engine(),
    )
    .await;

    assert!(
        result.is_err(),
        "with engine, nonexistent view in referenced_by should fail: {result:?}"
    );
}

// ---- Hiding tests (global) ----

#[sqlx::test]
async fn test_load_generic_table_credentials_hidden_view_in_chain_denied(pool: PgPool) {
    let authz = HidingAuthorizer::new();
    let (ctx, wh) = SetupTestCatalog::builder()
        .pool(pool)
        .authorizer(authz.clone())
        .build()
        .setup()
        .await;
    let whi = wh.warehouse_id;

    setup_ns_and_generic_table(&ctx, &wh).await;
    create_invoker_view(&ctx, &wh, "my_view").await;

    let key = view_object_key(&ctx, whi, &table_ident("ns", "my_view")).await;
    authz.hide(&key);

    let result = load_credentials(
        &ctx,
        &wh,
        "my_gt",
        Some(vec![table_ident("ns", "my_view")]),
        request_with_engine(),
    )
    .await;

    assert!(
        result.is_err(),
        "load_generic_table_credentials should fail when view in chain is hidden"
    );
}

#[sqlx::test]
async fn test_load_generic_table_credentials_hidden_generic_table_denied(pool: PgPool) {
    let authz = HidingAuthorizer::new();
    let (ctx, wh) = SetupTestCatalog::builder()
        .pool(pool)
        .authorizer(authz.clone())
        .build()
        .setup()
        .await;
    let whi = wh.warehouse_id;

    setup_ns_and_generic_table(&ctx, &wh).await;
    create_invoker_view(&ctx, &wh, "my_view").await;

    let key = generic_table_object_key(&ctx, whi, &table_ident("ns", "my_gt")).await;
    authz.hide(&key);

    let result = load_credentials(
        &ctx,
        &wh,
        "my_gt",
        Some(vec![table_ident("ns", "my_view")]),
        request_with_engine(),
    )
    .await;

    assert!(
        result.is_err(),
        "load_generic_table_credentials should fail when generic table is hidden"
    );
}

// ---- DEFINER delegation ----

/// User A cannot access the generic table directly, but succeeds through a
/// DEFINER view because owner B's permissions are used for the GT check.
#[sqlx::test]
async fn test_definer_delegates_generic_table_access_to_owner(pool: PgPool) {
    let authz = HidingAuthorizer::new();
    let (ctx, wh) = SetupTestCatalog::builder()
        .pool(pool)
        .authorizer(authz.clone())
        .build()
        .setup()
        .await;
    let whi = wh.warehouse_id;

    setup_ns_and_generic_table(&ctx, &wh).await;
    create_definer_view(&ctx, &wh, "definer_view", "owner_b").await;

    let gt_key = generic_table_object_key(&ctx, whi, &table_ident("ns", "my_gt")).await;
    authz.hide_for_user(&user("user_a"), &gt_key);

    let result = load_credentials(
        &ctx,
        &wh,
        "my_gt",
        Some(vec![table_ident("ns", "definer_view")]),
        request_as_user("user_a"),
    )
    .await;

    assert!(
        result.is_ok(),
        "user_a should access generic table via DEFINER view: {result:?}"
    );
}

/// When the DEFINER view owner loses generic-table access, load fails.
#[sqlx::test]
async fn test_definer_fails_when_owner_loses_generic_table_access(pool: PgPool) {
    let authz = HidingAuthorizer::new();
    let (ctx, wh) = SetupTestCatalog::builder()
        .pool(pool)
        .authorizer(authz.clone())
        .build()
        .setup()
        .await;
    let whi = wh.warehouse_id;

    setup_ns_and_generic_table(&ctx, &wh).await;
    create_definer_view(&ctx, &wh, "definer_view", "owner_b").await;

    let gt_key = generic_table_object_key(&ctx, whi, &table_ident("ns", "my_gt")).await;
    authz.hide_for_user(&user("owner_b"), &gt_key);

    let result = load_credentials(
        &ctx,
        &wh,
        "my_gt",
        Some(vec![table_ident("ns", "definer_view")]),
        request_as_user("user_a"),
    )
    .await;

    assert!(
        result.is_err(),
        "should fail when DEFINER owner loses generic-table access"
    );
}

/// Chain: `User_A` → View1(DEFINER, owner=B) → View2(INVOKER) → View3(DEFINER, owner=C) → `GenericTable`
///
/// Authorization checks:
///   View1: as `User_A`
///   View2: as B (DEFINER switches)
///   View3: as B (INVOKER inherits)
///   GT:    as C (DEFINER switches at View3)
///
/// We hide the GT for `user_a` AND `owner_b` — only `owner_c` (final DEFINER owner)
/// must be used for the GT check.
#[sqlx::test]
async fn test_chain_definer_invoker_definer_succeeds_for_generic_table(pool: PgPool) {
    let authz = HidingAuthorizer::new();
    let (ctx, wh) = SetupTestCatalog::builder()
        .pool(pool)
        .authorizer(authz.clone())
        .build()
        .setup()
        .await;
    let whi = wh.warehouse_id;

    setup_ns_and_generic_table(&ctx, &wh).await;
    create_definer_view(&ctx, &wh, "view1_definer_b", "owner_b").await;
    create_invoker_view(&ctx, &wh, "view2_invoker").await;
    create_definer_view(&ctx, &wh, "view3_definer_c", "owner_c").await;

    let gt_key = generic_table_object_key(&ctx, whi, &table_ident("ns", "my_gt")).await;
    authz.hide_for_user(&user("user_a"), &gt_key);
    authz.hide_for_user(&user("owner_b"), &gt_key);

    let result = load_credentials(
        &ctx,
        &wh,
        "my_gt",
        Some(vec![
            table_ident("ns", "view1_definer_b"),
            table_ident("ns", "view2_invoker"),
            table_ident("ns", "view3_definer_c"),
        ]),
        request_as_user("user_a"),
    )
    .await;

    assert!(
        result.is_ok(),
        "only owner_c should be checked for the generic table: {result:?}"
    );
}

/// INVOKER view checks the calling user (`user_a`), not any owner.
#[sqlx::test]
async fn test_invoker_view_checks_calling_user_for_generic_table(pool: PgPool) {
    let authz = HidingAuthorizer::new();
    let (ctx, wh) = SetupTestCatalog::builder()
        .pool(pool)
        .authorizer(authz.clone())
        .build()
        .setup()
        .await;
    let whi = wh.warehouse_id;

    setup_ns_and_generic_table(&ctx, &wh).await;
    create_invoker_view(&ctx, &wh, "invoker_view").await;

    let view_key = view_object_key(&ctx, whi, &table_ident("ns", "invoker_view")).await;
    authz.hide_for_user(&user("user_a"), &view_key);

    let result = load_credentials(
        &ctx,
        &wh,
        "my_gt",
        Some(vec![table_ident("ns", "invoker_view")]),
        request_as_user("user_a"),
    )
    .await;

    assert!(
        result.is_err(),
        "INVOKER view should check user_a's permissions"
    );
}

/// Instance admins bypass control-plane authz on generic tables, but MUST NOT
/// traverse a DEFINER view they lack `Select` on into the owner's context.
/// Regression guard for the data-plane carve-out.
#[sqlx::test]
async fn test_instance_admin_cannot_traverse_definer_chain_to_generic_table(pool: PgPool) {
    let authz = HidingAuthorizer::new();
    let (ctx, wh) = SetupTestCatalog::builder()
        .pool(pool)
        .authorizer(authz.clone())
        .build()
        .setup()
        .await;
    let whi = wh.warehouse_id;

    setup_ns_and_generic_table(&ctx, &wh).await;
    create_definer_view(&ctx, &wh, "definer_view", "owner_b").await;

    let view_key = view_object_key(&ctx, whi, &table_ident("ns", "definer_view")).await;
    authz.hide_for_user(&user("admin"), &view_key);

    let result = load_credentials(
        &ctx,
        &wh,
        "my_gt",
        Some(vec![table_ident("ns", "definer_view")]),
        request_as_instance_admin("admin"),
    )
    .await;

    assert!(
        result.is_err(),
        "instance admin with `Select` denied on DEFINER entry view must not traverse into owner's context for generic tables",
    );
}
