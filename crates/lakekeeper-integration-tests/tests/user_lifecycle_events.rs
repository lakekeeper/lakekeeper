//! The user lifecycle events: every write path that creates, changes or deletes a user
//! fires exactly one event after commit, carrying the row as of the write, and a write
//! that changes nothing fires none.

use std::{sync::Arc, time::Duration};

use lakekeeper::{
    api::{
        RequestMetadata,
        iceberg::v1::config::{GetConfigQueryParams, Service as _},
        management::v1::{
            ApiServer,
            user::{
                CreateUserRequest, Service as _, UpdateUserRequest, UserLastUpdatedWith, UserType,
            },
        },
    },
    server::CatalogServer,
    service::{
        CatalogRoleAssignmentOps as _, CatalogRoleForAssignment, CatalogUserRoleAssignmentUser,
        RoleIdent, RoleProviderId, SyncFor, UserId,
        authz::AllowAllAuthorizer,
        events::{EventListener, UserCreatedEvent, UserDeletedEvent, UserUpdatedEvent},
    },
};
use lakekeeper_integration_tests::{SetupTestCatalog, memory_io_profile};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

type Ctx = lakekeeper::api::ApiContext<
    lakekeeper::service::State<AllowAllAuthorizer, PostgresBackend, SecretsState>,
>;
type Api = ApiServer<PostgresBackend, AllowAllAuthorizer, SecretsState>;

#[derive(Debug)]
enum Seen {
    Created(UserCreatedEvent),
    Updated(UserUpdatedEvent),
    Deleted(UserDeletedEvent),
}

#[derive(Debug)]
struct UserEventCapture(UnboundedSender<Seen>);

impl std::fmt::Display for UserEventCapture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UserEventCapture")
    }
}

#[async_trait::async_trait]
impl EventListener for UserEventCapture {
    async fn user_created(&self, event: UserCreatedEvent) -> anyhow::Result<()> {
        let _ = self.0.send(Seen::Created(event));
        Ok(())
    }
    async fn user_updated(&self, event: UserUpdatedEvent) -> anyhow::Result<()> {
        let _ = self.0.send(Seen::Updated(event));
        Ok(())
    }
    async fn user_deleted(&self, event: UserDeletedEvent) -> anyhow::Result<()> {
        let _ = self.0.send(Seen::Deleted(event));
        Ok(())
    }
}

struct Fixture {
    ctx: Ctx,
    project_id: Arc<lakekeeper::ProjectId>,
    warehouse_name: String,
    events: UnboundedReceiver<Seen>,
}

impl Fixture {
    async fn new(pool: PgPool) -> Self {
        let (ctx, warehouse) = SetupTestCatalog::builder()
            .pool(pool)
            .storage_profile(memory_io_profile())
            .authorizer(AllowAllAuthorizer::default())
            .number_of_warehouses(1)
            .build()
            .setup()
            .await;
        let (sender, events) = unbounded_channel();
        ctx.v1_state
            .events
            .append(Arc::new(UserEventCapture(sender)))
            .await;
        Self {
            ctx,
            project_id: warehouse.project_id,
            warehouse_name: warehouse.warehouse_name,
            events,
        }
    }

    /// The next event; dispatch is spawned, so it may arrive after the call returns.
    async fn next(&mut self) -> Seen {
        tokio::time::timeout(Duration::from_secs(5), self.events.recv())
            .await
            .expect("an event within five seconds")
            .expect("the capture outlives dispatch")
    }

    /// No further event arrives.
    async fn none(&mut self) {
        let next = tokio::time::timeout(Duration::from_millis(300), self.events.recv()).await;
        assert!(next.is_err(), "unexpected event: {next:?}");
    }

    async fn create(&self, id: &UserId, name: &str, email: Option<&str>, update_if_exists: bool) {
        Api::create_user(
            self.ctx.clone(),
            admin(),
            CreateUserRequest {
                update_if_exists,
                name: Some(name.to_string()),
                email: email.map(ToString::to_string),
                user_type: Some(UserType::Human),
                id: Some(id.clone()),
            },
        )
        .await
        .unwrap();
    }

    async fn update(
        &self,
        id: &UserId,
        name: &str,
        email: Option<&str>,
    ) -> lakekeeper::api::Result<()> {
        Api::update_user(
            self.ctx.clone(),
            admin(),
            id.clone(),
            UpdateUserRequest {
                name: name.to_string(),
                email: email.map(ToString::to_string),
                user_type: UserType::Human,
            },
        )
        .await
    }

    async fn config(&self, metadata: RequestMetadata) {
        CatalogServer::get_config(
            GetConfigQueryParams {
                warehouse: Some(format!("{}/{}", self.project_id, self.warehouse_name)),
            },
            self.ctx.clone(),
            metadata,
        )
        .await
        .unwrap();
    }
}

fn admin() -> RequestMetadata {
    RequestMetadata::test_user(UserId::new_unchecked("oidc", "admin"))
}

#[sqlx::test]
async fn create_update_and_delete_each_fire_one_event(pool: PgPool) {
    let mut f = Fixture::new(pool).await;
    let bob = UserId::new_unchecked("oidc", "bob");

    f.create(&bob, "Bob", Some("bob@example.com"), false).await;
    let Seen::Created(created) = f.next().await else {
        panic!("expected a created event")
    };
    assert_eq!(created.user.id, bob);
    assert_eq!(created.user.email.as_deref(), Some("bob@example.com"));
    assert!(created.request_metadata.is_some());
    f.none().await;

    f.update(&bob, "Bob", Some("bob@new.example.com"))
        .await
        .unwrap();
    let Seen::Updated(updated) = f.next().await else {
        panic!("expected an updated event")
    };
    assert_eq!(updated.user.email.as_deref(), Some("bob@new.example.com"));
    assert_eq!(
        updated.previous.as_ref().and_then(|p| p.email.as_deref()),
        Some("bob@example.com")
    );
    f.none().await;

    Api::delete_user(f.ctx.clone(), admin(), bob.clone())
        .await
        .unwrap();
    let Seen::Deleted(deleted) = f.next().await else {
        panic!("expected a deleted event")
    };
    // The row as it was, not the scrubbed tombstone.
    assert_eq!(deleted.user.name, "Bob");
    assert_eq!(deleted.user.email.as_deref(), Some("bob@new.example.com"));
    f.none().await;
}

#[sqlx::test]
async fn writes_that_change_nothing_or_fail_fire_nothing(pool: PgPool) {
    let mut f = Fixture::new(pool).await;
    let bob = UserId::new_unchecked("oidc", "bob");
    f.create(&bob, "Bob", None, false).await;
    let _ = f.next().await;

    // Same values through create-or-update and update.
    f.create(&bob, "Bob", None, true).await;
    f.update(&bob, "Bob", None).await.unwrap();
    f.none().await;

    // A create of an existing user is rejected (409); an update of a missing one too (404).
    Api::create_user(
        f.ctx.clone(),
        admin(),
        CreateUserRequest {
            update_if_exists: false,
            name: Some("Other".to_string()),
            email: None,
            user_type: Some(UserType::Human),
            id: Some(bob.clone()),
        },
    )
    .await
    .unwrap_err();
    f.update(&UserId::new_unchecked("oidc", "nobody"), "Nobody", None)
        .await
        .unwrap_err();
    f.none().await;
}

#[sqlx::test]
async fn self_provisioning_fires_once(pool: PgPool) {
    let mut f = Fixture::new(pool).await;
    let alice = UserId::new_unchecked("oidc", "alice");

    f.config(RequestMetadata::test_user(alice.clone())).await;
    let Seen::Created(created) = f.next().await else {
        panic!("expected a created event")
    };
    assert_eq!(created.user.id, alice);

    f.config(RequestMetadata::test_user(alice.clone())).await;
    f.none().await;
}

#[sqlx::test]
async fn role_provider_syncs_fire_for_the_users_they_write(pool: PgPool) {
    let mut f = Fixture::new(pool).await;
    let carol = Arc::new(UserId::new_unchecked("oidc", "carol"));
    let provider = RoleProviderId::new_unchecked("ldap");
    let ident = Arc::new(RoleIdent::new_unchecked(provider.as_str(), "analysts"));
    let ctx = f.ctx.clone();
    let project_id = Arc::clone(&f.project_id);
    let sync = |email: Option<&'static str>| {
        let ctx = ctx.clone();
        let carol = Arc::clone(&carol);
        let provider = provider.clone();
        let ident = Arc::clone(&ident);
        let project_id = Arc::clone(&project_id);
        async move {
            PostgresBackend::sync_user_role_assignments(
                CatalogUserRoleAssignmentUser {
                    user_id: &carol,
                    name: Some("Carol"),
                    email,
                    user_type: None,
                    updated_with: UserLastUpdatedWith::RoleProvider,
                },
                SyncFor::OtherUser,
                &project_id,
                &provider,
                &[CatalogRoleForAssignment {
                    ident: &ident,
                    name: None,
                    description: None,
                }],
                ctx.v1_state.catalog.clone(),
                &ctx.v1_state.events,
            )
            .await
            .unwrap();
        }
    };

    sync(Some("carol@example.com")).await;
    let Seen::Created(created) = f.next().await else {
        panic!("expected a created event")
    };
    assert_eq!(created.user.id, *carol);
    assert!(created.request_metadata.is_none());

    // The same sync again changes nothing.
    sync(Some("carol@example.com")).await;
    f.none().await;

    sync(Some("carol@new.example.com")).await;
    let Seen::Updated(updated) = f.next().await else {
        panic!("expected an updated event")
    };
    assert_eq!(updated.user.email.as_deref(), Some("carol@new.example.com"));

    // A role-members sync writes its member users too.
    let dave = Arc::new(UserId::new_unchecked("oidc", "dave"));
    PostgresBackend::sync_role_members(
        &f.project_id,
        &CatalogRoleForAssignment {
            ident: &ident,
            name: None,
            description: None,
        },
        &[CatalogUserRoleAssignmentUser {
            user_id: &dave,
            name: Some("Dave"),
            email: None,
            user_type: None,
            updated_with: UserLastUpdatedWith::RoleProvider,
        }],
        f.ctx.v1_state.catalog.clone(),
        &f.ctx.v1_state.events,
    )
    .await
    .unwrap();
    let Seen::Created(created) = f.next().await else {
        panic!("expected a created event")
    };
    assert_eq!(created.user.id, *dave);
    f.none().await;
}
