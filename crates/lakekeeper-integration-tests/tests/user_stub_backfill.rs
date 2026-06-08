//! Story 3a — first-login backfill of role-provider placeholder-stub `users` rows.
//!
//! Role-provider sync stubs a row (`name = "Nameless User with id <id>"`,
//! `last_updated_with = RoleProvider`) for an unknown user so that #1824's
//! "assignment requires an existing user" contract is satisfiable before login.
//! `maybe_register_user` (the `GET /v1/config` first-touch hook) must backfill
//! that placeholder from the token on first login — but must NOT overwrite a row
//! that already carries a real, human-set identity.

use lakekeeper::{
    api::{
        RequestMetadata,
        iceberg::v1::config::{GetConfigQueryParams, Service as _},
        management::v1::user::{UserLastUpdatedWith, UserType},
    },
    server::CatalogServer,
    service::{CatalogStore, Transaction, UserId, authz::AllowAllAuthorizer},
};
use lakekeeper_integration_tests::{SetupTestCatalog, memory_io_profile};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;

type Ctx = lakekeeper::api::ApiContext<
    lakekeeper::service::State<AllowAllAuthorizer, PostgresBackend, SecretsState>,
>;

async fn setup(pool: PgPool) -> (Ctx, std::sync::Arc<lakekeeper::ProjectId>, String) {
    let (ctx, warehouse) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;
    (ctx, warehouse.project_id, warehouse.warehouse_name)
}

async fn seed_user(
    ctx: &Ctx,
    user_id: &UserId,
    name: &str,
    last_updated_with: UserLastUpdatedWith,
) {
    let mut tx =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    PostgresBackend::create_or_update_user(
        user_id,
        name,
        None,
        last_updated_with,
        UserType::Human,
        tx.transaction(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

async fn get_user_row(ctx: &Ctx, user_id: &UserId) -> lakekeeper::api::management::v1::user::User {
    let listed = PostgresBackend::list_user(
        Some(vec![user_id.clone()]),
        None,
        lakekeeper::api::iceberg::v1::PaginationQuery::new_with_page_size(1),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    listed.users.into_iter().next().expect("user row exists")
}

fn config_query(project: &lakekeeper::ProjectId, warehouse_name: &str) -> GetConfigQueryParams {
    GetConfigQueryParams {
        warehouse: Some(format!("{project}/{warehouse_name}")),
    }
}

/// A role-provider placeholder stub is backfilled from the token on first login.
#[sqlx::test]
async fn first_login_backfills_role_provider_stub(pool: PgPool) {
    let (ctx, project_id, warehouse_name) = setup(pool).await;
    let alice = UserId::new_unchecked("oidc", "alice");

    // Stub exactly as role-provider sync writes it.
    seed_user(
        &ctx,
        &alice,
        &format!("Nameless User with id {alice}"),
        UserLastUpdatedWith::RoleProvider,
    )
    .await;

    // First login (the `GET /v1/config` first-touch hook), token name "Test User".
    CatalogServer::get_config(
        config_query(&project_id, &warehouse_name),
        ctx.clone(),
        RequestMetadata::test_user(alice.clone()),
    )
    .await
    .unwrap();

    let row = get_user_row(&ctx, &alice).await;
    assert_eq!(row.name, "Test User");
    assert_eq!(
        row.last_updated_with,
        UserLastUpdatedWith::ConfigCallCreation
    );
}

/// A row with a real, human-set name (non-`RoleProvider`) is never overwritten by
/// a login, even if the token's name differs.
#[sqlx::test]
async fn first_login_does_not_overwrite_real_name(pool: PgPool) {
    let (ctx, project_id, warehouse_name) = setup(pool).await;
    let bob = UserId::new_unchecked("oidc", "bob");

    seed_user(&ctx, &bob, "Real Name", UserLastUpdatedWith::CreateEndpoint).await;

    CatalogServer::get_config(
        config_query(&project_id, &warehouse_name),
        ctx.clone(),
        RequestMetadata::test_user(bob.clone()),
    )
    .await
    .unwrap();

    let row = get_user_row(&ctx, &bob).await;
    assert_eq!(row.name, "Real Name");
    assert_eq!(row.last_updated_with, UserLastUpdatedWith::CreateEndpoint);
}

/// A `RoleProvider` row with a real (non-placeholder) name — e.g. a SCIM-synced
/// "Alice Smith" — is NOT a stub and must be left untouched by login.
#[sqlx::test]
async fn first_login_does_not_overwrite_real_role_provider_name(pool: PgPool) {
    let (ctx, project_id, warehouse_name) = setup(pool).await;
    let carol = UserId::new_unchecked("oidc", "carol");

    seed_user(
        &ctx,
        &carol,
        "Carol Synced",
        UserLastUpdatedWith::RoleProvider,
    )
    .await;

    CatalogServer::get_config(
        config_query(&project_id, &warehouse_name),
        ctx.clone(),
        RequestMetadata::test_user(carol.clone()),
    )
    .await
    .unwrap();

    let row = get_user_row(&ctx, &carol).await;
    assert_eq!(row.name, "Carol Synced");
    assert_eq!(row.last_updated_with, UserLastUpdatedWith::RoleProvider);
}
