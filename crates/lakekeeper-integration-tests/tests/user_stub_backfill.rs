//! Story 3a — first-login backfill of role-provider placeholder-stub `users` rows.
//!
//! Role-provider sync stubs a row (`name IS NULL`, `last_updated_with =
//! RoleProvider`) for an unknown user so that #1824's "assignment requires an
//! existing user" contract is satisfiable before login. The NULL name renders as
//! a `"Nameless User with id <id>"` placeholder at read time.
//! `maybe_register_user` (the `GET /v1/config` first-touch hook) must backfill
//! that stub from the token on first login — but must NOT overwrite a row that
//! already carries a real, human-set identity (the `WHERE name IS NULL` guard).

use lakekeeper::{
    api::{
        RequestMetadata,
        iceberg::v1::config::{GetConfigQueryParams, Service as _},
        management::v1::user::{UserLastUpdatedWith, UserType},
    },
    server::CatalogServer,
    service::{
        CatalogRoleAssignmentOps, CatalogStore, CatalogUserRoleAssignmentUser, RoleProviderId,
        Transaction, UserId, UserUpsertMode, authz::AllowAllAuthorizer,
    },
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
        UserUpsertMode::Overwrite,
        tx.transaction(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

/// Create a role-provider stub the way production does: drive the real
/// role-provider sync with a nameless user (optionally carrying a synced email)
/// and no role assignments. The sync upserts the user with a NULL name
/// (`last_updated_with = role-provider`) — no raw SQL, so the test follows the
/// real write path instead of duplicating the schema.
async fn seed_null_stub(
    ctx: &Ctx,
    project_id: &lakekeeper::ProjectId,
    user_id: &UserId,
    email: Option<&str>,
) {
    let user_id = std::sync::Arc::new(user_id.clone());
    let provider = RoleProviderId::new_unchecked("ldap");
    let mut tx =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    PostgresBackend::sync_user_role_assignments_by_provider(
        CatalogUserRoleAssignmentUser {
            user_id: &user_id,
            name: None,
            email,
            user_type: None,
            updated_with: UserLastUpdatedWith::RoleProvider,
        },
        project_id,
        &provider,
        &[],
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

/// A role-provider NULL-name stub renders as the placeholder at read time,
/// before any login backfills it.
#[sqlx::test]
async fn null_stub_renders_placeholder_name(pool: PgPool) {
    let (ctx, project_id, _warehouse_name) = setup(pool.clone()).await;
    let alice = UserId::new_unchecked("oidc", "alice");

    seed_null_stub(&ctx, &project_id, &alice, None).await;

    let row = get_user_row(&ctx, &alice).await;
    assert_eq!(row.name, format!("Nameless User with id {alice}"));
    assert_eq!(row.last_updated_with, UserLastUpdatedWith::RoleProvider);
}

/// A role-provider stub (NULL name) is backfilled from the token on first login.
#[sqlx::test]
async fn first_login_backfills_role_provider_stub(pool: PgPool) {
    let (ctx, project_id, warehouse_name) = setup(pool.clone()).await;
    let alice = UserId::new_unchecked("oidc", "alice");

    // Stub exactly as role-provider sync writes it: NULL name.
    seed_null_stub(&ctx, &project_id, &alice, None).await;

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

/// The `token_provides_name` gate: a nameless token must NOT backfill a NULL
/// stub, because doing so would flip the row to `ConfigCallCreation` with a
/// still-placeholder name and lock out a later name-bearing login or SCIM
/// full-sync. The stub must survive unchanged (still `RoleProvider`, still
/// NULL → placeholder render).
#[sqlx::test]
async fn nameless_token_does_not_backfill_stub(pool: PgPool) {
    let (ctx, project_id, warehouse_name) = setup(pool.clone()).await;
    let dave = UserId::new_unchecked("oidc", "dave");

    seed_null_stub(&ctx, &project_id, &dave, None).await;

    // First login with a token that carries no name claim.
    CatalogServer::get_config(
        config_query(&project_id, &warehouse_name),
        ctx.clone(),
        RequestMetadata::test_user_without_name(dave.clone()),
    )
    .await
    .unwrap();

    let row = get_user_row(&ctx, &dave).await;
    assert_eq!(row.name, format!("Nameless User with id {dave}"));
    assert_eq!(row.last_updated_with, UserLastUpdatedWith::RoleProvider);
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

/// A role-provider stub that already carries a synced email must keep it when
/// first login backfills the name from a token with no email claim — the backfill
/// adds the name, it must not clear the email.
#[sqlx::test]
async fn first_login_keeps_existing_provider_email(pool: PgPool) {
    let (ctx, project_id, warehouse_name) = setup(pool.clone()).await;
    let erin = UserId::new_unchecked("oidc", "erin");

    // Stub with a provider-synced email but no name yet.
    seed_null_stub(&ctx, &project_id, &erin, Some("erin@example.com")).await;

    // First login: the token has a name ("Test User") but no email claim.
    CatalogServer::get_config(
        config_query(&project_id, &warehouse_name),
        ctx.clone(),
        RequestMetadata::test_user(erin.clone()),
    )
    .await
    .unwrap();

    let row = get_user_row(&ctx, &erin).await;
    assert_eq!(row.name, "Test User");
    assert_eq!(row.email.as_deref(), Some("erin@example.com"));
}
