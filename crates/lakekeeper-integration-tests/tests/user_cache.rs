//! The user cache: `UserId → email`, with absence cached too, kept current by the user
//! lifecycle events. The cache is a process-wide static, so every test uses ids of its
//! own.

use std::{sync::Arc, time::Duration};

use lakekeeper::{
    api::{
        RequestMetadata,
        management::v1::{
            ApiServer,
            user::{
                CreateUserRequest, Service as _, UpdateUserRequest, UserLastUpdatedWith, UserType,
            },
        },
    },
    service::{
        CatalogStore, Transaction, UserId, UserUpsertMode,
        authz::AllowAllAuthorizer,
        user_cache::{UserCacheEventListener, UserEmail, user_emails},
    },
};
use lakekeeper_integration_tests::{SetupTestCatalog, memory_io_profile};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;

type Ctx = lakekeeper::api::ApiContext<
    lakekeeper::service::State<AllowAllAuthorizer, PostgresBackend, SecretsState>,
>;
type Api = ApiServer<PostgresBackend, AllowAllAuthorizer, SecretsState>;

async fn setup(pool: PgPool) -> Ctx {
    let (ctx, _) = SetupTestCatalog::builder()
        .pool(pool)
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;
    ctx.v1_state
        .events
        .append(Arc::new(UserCacheEventListener))
        .await;
    ctx
}

async fn email_of(ctx: &Ctx, user_id: &UserId) -> UserEmail {
    user_emails::<PostgresBackend>(std::slice::from_ref(user_id), ctx.v1_state.catalog.clone())
        .await
        .unwrap()
        .remove(user_id)
        .unwrap()
}

/// Writes the row straight to the database, with no event: the cache cannot learn of it
/// except by reading.
async fn write_silently(ctx: &Ctx, user_id: &UserId, email: Option<&str>) {
    let mut t =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    PostgresBackend::create_or_update_user(
        user_id,
        "Silent",
        email,
        UserLastUpdatedWith::CreateEndpoint,
        UserType::Application,
        UserUpsertMode::Overwrite,
        t.transaction(),
    )
    .await
    .unwrap();
    t.commit().await.unwrap();
}

fn admin() -> RequestMetadata {
    RequestMetadata::test_user(UserId::new_unchecked("oidc", "cache-admin"))
}

/// Events are dispatched on a spawned task; wait until the cache shows `expected`.
async fn eventually(ctx: &Ctx, user_id: &UserId, expected: UserEmail) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while email_of(ctx, user_id).await != expected {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("the cache never showed {expected:?} for {user_id}"));
}

/// Absence is cached: once read, a user that appears without an event stays unknown,
/// which shows the second lookup read nothing.
#[sqlx::test]
async fn an_unknown_user_is_cached_as_unknown(pool: PgPool) {
    let ctx = setup(pool).await;
    let user = UserId::new_unchecked("oidc", "cache-unknown");

    assert_eq!(email_of(&ctx, &user).await, UserEmail::NoUser);
    write_silently(&ctx, &user, Some("late@example.com")).await;
    assert_eq!(email_of(&ctx, &user).await, UserEmail::NoUser);
}

/// A user without an email is cached as such, the same way.
#[sqlx::test]
async fn a_user_without_email_is_cached_as_such(pool: PgPool) {
    let ctx = setup(pool).await;
    let user = UserId::new_unchecked("oidc", "cache-no-email");
    write_silently(&ctx, &user, None).await;

    assert_eq!(email_of(&ctx, &user).await, UserEmail::NoEmail);
    write_silently(&ctx, &user, Some("late@example.com")).await;
    assert_eq!(email_of(&ctx, &user).await, UserEmail::NoEmail);
}

/// A user created after a negative was cached is served by the next lookup, through
/// the event, not the TTL; an update and a delete likewise.
#[sqlx::test]
async fn lifecycle_events_replace_the_cached_answer(pool: PgPool) {
    let ctx = setup(pool).await;
    let user = UserId::new_unchecked("oidc", "cache-lifecycle");
    assert_eq!(email_of(&ctx, &user).await, UserEmail::NoUser);

    Api::create_user(
        ctx.clone(),
        admin(),
        CreateUserRequest {
            update_if_exists: false,
            name: Some("Erin".to_string()),
            email: Some("erin@example.com".to_string()),
            user_type: Some(UserType::Human),
            id: Some(user.clone()),
        },
    )
    .await
    .unwrap();
    eventually(&ctx, &user, UserEmail::Email(Arc::from("erin@example.com"))).await;

    Api::update_user(
        ctx.clone(),
        admin(),
        user.clone(),
        UpdateUserRequest {
            name: "Erin".to_string(),
            email: None,
            user_type: UserType::Human,
        },
    )
    .await
    .unwrap();
    eventually(&ctx, &user, UserEmail::NoEmail).await;

    Api::delete_user(ctx.clone(), admin(), user.clone())
        .await
        .unwrap();
    eventually(&ctx, &user, UserEmail::NoUser).await;
}

/// Several ids resolve in one call, each to its own answer, and the answers are cached.
#[sqlx::test]
async fn several_ids_resolve_together(pool: PgPool) {
    let ctx = setup(pool).await;
    let with_email = UserId::new_unchecked("oidc", "cache-batch-email");
    let without = UserId::new_unchecked("oidc", "cache-batch-none");
    let unknown = UserId::new_unchecked("oidc", "cache-batch-unknown");
    write_silently(&ctx, &with_email, Some("f@example.com")).await;
    write_silently(&ctx, &without, None).await;

    let ids = [
        with_email.clone(),
        without.clone(),
        unknown.clone(),
        with_email.clone(),
    ];
    let emails = user_emails::<PostgresBackend>(&ids, ctx.v1_state.catalog.clone())
        .await
        .unwrap();
    assert_eq!(emails.len(), 3);
    assert_eq!(
        emails[&with_email],
        UserEmail::Email(Arc::from("f@example.com"))
    );
    assert_eq!(emails[&without], UserEmail::NoEmail);
    assert_eq!(emails[&unknown], UserEmail::NoUser);

    // Cached: a silent change is not seen.
    write_silently(&ctx, &unknown, Some("late@example.com")).await;
    assert_eq!(email_of(&ctx, &unknown).await, UserEmail::NoUser);
}

/// More ids than one statement reads: every chunk's answers come back, each under its own id.
#[sqlx::test]
async fn ids_beyond_one_read_resolve_together(pool: PgPool) {
    let ctx = setup(pool).await;
    let first = UserId::new_unchecked("oidc", "cache-chunk-first");
    let last = UserId::new_unchecked("oidc", "cache-chunk-last");
    write_silently(&ctx, &first, Some("first@example.com")).await;
    write_silently(&ctx, &last, Some("last@example.com")).await;

    let mut ids = vec![first.clone()];
    ids.extend((0..1_000).map(|i| UserId::new_unchecked("oidc", &format!("cache-chunk-{i}"))));
    ids.push(last.clone());
    let emails = user_emails::<PostgresBackend>(&ids, ctx.v1_state.catalog.clone())
        .await
        .unwrap();
    assert_eq!(emails.len(), ids.len());
    assert_eq!(
        emails[&first],
        UserEmail::Email(Arc::from("first@example.com"))
    );
    assert_eq!(
        emails[&last],
        UserEmail::Email(Arc::from("last@example.com"))
    );
    assert!(
        ids[1..ids.len() - 1]
            .iter()
            .all(|id| emails[id] == UserEmail::NoUser)
    );
}
