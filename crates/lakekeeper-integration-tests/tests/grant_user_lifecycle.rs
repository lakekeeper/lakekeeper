//! Deleting a user must take their grants with them.
//!
//! Users are soft-deleted, so no foreign key cascades their grants away, and the
//! same `UserId` comes back on re-login. Without explicit cleanup the returning
//! account would silently regain every privilege it previously held.

use lakekeeper::{
    api::{
        ApiContext, RequestMetadata, RequestMetadataTestBuilder,
        management::v1::{
            ApiServer,
            user::{Service as _, UserLastUpdatedWith, UserType},
        },
    },
    service::{
        CatalogGrantOps, CatalogStore, State, Transaction, UserId, UserUpsertMode,
        authz::{AllowAllAuthorizer, GrantFilter, GrantResource, GrantSpec, UserOrRoleId},
    },
};
use lakekeeper_integration_tests::{SetupTestCatalog, memory_io_profile};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;

type Ctx = ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>;

async fn provision_user(ctx: &Ctx, user_id: &UserId, name: &str) {
    let mut tx =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    PostgresBackend::create_or_update_user(
        user_id,
        name,
        None,
        UserLastUpdatedWith::RoleProvider,
        UserType::Human,
        UserUpsertMode::Overwrite,
        tx.transaction(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

#[sqlx::test]
async fn deleting_a_user_revokes_their_grants(pool: PgPool) {
    let (ctx, warehouse) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;
    let metadata: RequestMetadata = RequestMetadataTestBuilder::builder()
        .project_id(Some(warehouse.project_id.clone()))
        .build();

    let alice = UserId::try_from("oidc~alice").unwrap();
    let bob = UserId::try_from("oidc~bob").unwrap();
    provision_user(&ctx, &alice, "Alice").await;
    provision_user(&ctx, &bob, "Bob").await;

    let grant = |user: &UserId| GrantSpec {
        principal: UserOrRoleId::User(user.clone()),
        resource: GrantResource::Warehouse(warehouse.warehouse_id),
        privilege: "get_metadata".to_string(),
    };
    PostgresBackend::apply_grants(
        &[grant(&alice), grant(&bob)],
        &[],
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();

    let filter = GrantFilter::on(GrantResource::Warehouse(warehouse.warehouse_id), None);
    let list = || async {
        PostgresBackend::list_grants(
            &filter,
            lakekeeper::api::iceberg::v1::PaginationQuery::new(
                lakekeeper::api::iceberg::v1::PageToken::Empty,
                None,
            ),
            ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap()
        .grants
    };
    assert_eq!(list().await.len(), 2);

    ApiServer::<PostgresBackend, AllowAllAuthorizer, SecretsState>::delete_user(
        ctx.clone(),
        metadata,
        alice.clone(),
    )
    .await
    .unwrap();

    // Only Alice's grant is gone; Bob is untouched.
    let remaining = list().await;
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].principal, UserOrRoleId::User(bob.clone()));

    // Re-provisioning the same id must not resurrect the grant.
    provision_user(&ctx, &alice, "Alice").await;
    let remaining = list().await;
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].principal, UserOrRoleId::User(bob));
}

/// Records the grant events a request produced.
#[derive(Debug)]
struct GrantEventCapture(
    tokio::sync::mpsc::UnboundedSender<lakekeeper::service::events::GrantsChangedEvent>,
);

impl std::fmt::Display for GrantEventCapture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GrantEventCapture")
    }
}

#[async_trait::async_trait]
impl lakekeeper::service::events::EventListener for GrantEventCapture {
    async fn grants_changed(
        &self,
        event: lakekeeper::service::events::GrantsChangedEvent,
    ) -> anyhow::Result<()> {
        let _ = self.0.send(event);
        Ok(())
    }
}

/// A user delete whose request is dropped while it commits still clears the user's
/// cached role assignments and announces the revoked grants.
#[sqlx::test]
async fn a_user_delete_dropped_while_committing_still_announces_and_updates_the_cache(
    pool: PgPool,
) {
    use lakekeeper::service::CatalogRoleAssignmentOps as _;
    use lakekeeper_integration_tests::{CommitGate, eventually};

    let (ctx, warehouse) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;
    let metadata: RequestMetadata = RequestMetadataTestBuilder::builder()
        .project_id(Some(warehouse.project_id.clone()))
        .build();
    let alice = UserId::try_from("oidc~dropped-user-delete-0001").unwrap();
    provision_user(&ctx, &alice, "Alice").await;
    let grant = GrantSpec {
        principal: UserOrRoleId::User(alice.clone()),
        resource: GrantResource::Warehouse(warehouse.warehouse_id),
        privilege: "get_metadata".to_string(),
    };
    PostgresBackend::apply_grants(
        std::slice::from_ref(&grant),
        &[],
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    let (sender, mut events) = tokio::sync::mpsc::unbounded_channel();
    ctx.v1_state
        .events
        .append(std::sync::Arc::new(GrantEventCapture(sender)))
        .await;
    let warmed =
        PostgresBackend::list_role_assignments_for_user(&alice, ctx.v1_state.catalog.clone())
            .await
            .unwrap();

    let gate = CommitGate::install(&pool, "users", "UPDATE").await;
    let request = tokio::spawn(
        ApiServer::<PostgresBackend, AllowAllAuthorizer, SecretsState>::delete_user(
            ctx.clone(),
            metadata,
            alice.clone(),
        ),
    );
    gate.wait_for_a_held_commit().await;
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    gate.release().await;

    let announced = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let event = events.recv().await.expect("the capture outlives dispatch");
            if event.removed.contains(&grant) {
                break event;
            }
        }
    })
    .await
    .expect("the revoked grant is announced");
    assert!(announced.created.is_empty());
    eventually("the user's cached assignments are reloaded", || async {
        let served =
            PostgresBackend::list_role_assignments_for_user(&alice, ctx.v1_state.catalog.clone())
                .await
                .unwrap();
        !std::sync::Arc::ptr_eq(&served, &warmed)
    })
    .await;
}
