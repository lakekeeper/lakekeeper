//! Behavioral tests for the role-membership management API (`/role/{id}/members`,
//! `/role/{id}/member-of`, `/user/{id}/roles`) on the catalog arm (AllowAll +
//! Postgres). Exact assertions throughout.

use lakekeeper::{
    ProjectId,
    api::{
        ApiContext, RequestMetadata, RequestMetadataTestBuilder,
        management::v1::{
            ApiServer,
            role_membership::{
                AddRoleMembersRequest, RoleMember, RoleMemberRef, RoleMemberType, Service as _,
            },
            user::{UserLastUpdatedWith, UserType},
        },
    },
    service::{
        CatalogCreateRoleRequest, CatalogRoleOps, CatalogStore, RoleId, RoleProviderId,
        RoleSourceId, State, Transaction, UserId, UserUpsertMode, authz::AllowAllAuthorizer,
    },
};
use lakekeeper_integration_tests::{SetupTestCatalog, memory_io_profile};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;

type Ctx = ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>;

async fn setup(pool: PgPool) -> (Ctx, std::sync::Arc<ProjectId>) {
    let (ctx, warehouse) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;
    (ctx, warehouse.project_id)
}

fn metadata(project_id: &ProjectId) -> RequestMetadata {
    RequestMetadataTestBuilder::builder()
        .project_id(Some(project_id.clone().into()))
        .build()
}

/// Create a catalog-managed (`lakekeeper` provider) role directly in the DB.
async fn make_role(ctx: &Ctx, project_id: &ProjectId, name: &str, source_id: &str) -> RoleId {
    let provider = RoleProviderId::try_new("lakekeeper").unwrap();
    let sid = RoleSourceId::try_new(source_id).unwrap();
    let mut tx =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    let role = PostgresBackend::create_role(
        project_id,
        CatalogCreateRoleRequest::builder()
            .role_id(RoleId::new_random())
            .role_name(name)
            .source_id(&sid)
            .provider_id(&provider)
            .build(),
        tx.transaction(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    role.id()
}

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

fn user_member(user_id: &UserId) -> RoleMemberRef {
    RoleMemberRef::User {
        id: user_id.clone(),
    }
}

fn role_member(role_id: RoleId) -> RoleMemberRef {
    RoleMemberRef::Role { id: role_id }
}

/// True if a response member ([`RoleMember`]) names the same principal as the
/// given request identity ([`RoleMemberRef`]) — identity only, ignoring the
/// hydrated display fields asserted separately where they matter.
fn same(m: &RoleMember, r: &RoleMemberRef) -> bool {
    match (m, r) {
        (RoleMember::User(u), RoleMemberRef::User { id }) => &u.id == id,
        (RoleMember::Role(rm), RoleMemberRef::Role { id }) => &rm.id == id,
        _ => false,
    }
}

/// The kind of a response member, for `?type=`-style assertions.
fn kind(m: &RoleMember) -> RoleMemberType {
    match m {
        RoleMember::User(_) => RoleMemberType::User,
        RoleMember::Role(_) => RoleMemberType::Role,
    }
}

/// The member's principal id rendered as a string (`UserId`/`RoleId` `Display`),
/// for set comparisons that only care about identity.
fn member_id_string(m: &RoleMember) -> String {
    match m {
        RoleMember::User(u) => u.id.to_string(),
        RoleMember::Role(rm) => rm.id.to_string(),
    }
}

// ==================== add + list ====================

#[sqlx::test]
async fn add_and_list_user_member(pool: PgPool) {
    let (ctx, project_id) = setup(pool).await;
    let role = make_role(&ctx, &project_id, "R", "r-src").await;
    let alice = UserId::new_unchecked("oidc", "alice");
    provision_user(&ctx, &alice, "Alice").await;

    let resp = ApiServer::add_role_members(
        ctx.clone(),
        metadata(&project_id),
        role,
        AddRoleMembersRequest {
            members: vec![user_member(&alice)],
        },
    )
    .await
    .unwrap();
    // Returns new state: the requested member, confirmed (an un-hydrated
    // identity reference — no display fields, no timestamps).
    assert_eq!(resp.members, vec![user_member(&alice)]);

    let page =
        ApiServer::list_role_members(ctx.clone(), metadata(&project_id), role, list_query(None))
            .await
            .unwrap();
    assert_eq!(page.members.len(), 1);
    // The catalog arm hydrates the user's display identity in one JOIN: Alice's
    // name, no email, type Human.
    let RoleMember::User(u) = &page.members[0] else {
        panic!("expected a user member, got {:?}", page.members[0]);
    };
    assert_eq!(u.id, alice);
    assert_eq!(u.name.as_deref(), Some("Alice"));
    assert_eq!(u.email, None);
    assert_eq!(u.user_type, Some(UserType::Human));
    // Note: a non-empty keyset page always carries a token; emptiness signals the
    // end (see `list_members_pagination`). So we don't assert `None` here.
}

#[sqlx::test]
async fn add_role_member_and_member_of(pool: PgPool) {
    let (ctx, project_id) = setup(pool).await;
    let parent = make_role(&ctx, &project_id, "parent", "p-src").await;
    let child = make_role(&ctx, &project_id, "child", "c-src").await;

    let resp = ApiServer::add_role_members(
        ctx.clone(),
        metadata(&project_id),
        parent,
        AddRoleMembersRequest {
            members: vec![role_member(child)],
        },
    )
    .await
    .unwrap();
    // The add response echoes the role member identity.
    assert_eq!(resp.members, vec![role_member(child)]);

    // The child appears as a member of the parent, hydrated with its catalog name.
    let members =
        ApiServer::list_role_members(ctx.clone(), metadata(&project_id), parent, list_query(None))
            .await
            .unwrap();
    assert_eq!(members.members.len(), 1);
    let RoleMember::Role(rm) = &members.members[0] else {
        panic!("expected a role member, got {:?}", members.members[0]);
    };
    assert_eq!(rm.id, child);
    assert_eq!(rm.name, "child");

    // The parent appears in the child's member-of listing, hydrated by name.
    let parents =
        ApiServer::list_role_member_of(ctx.clone(), metadata(&project_id), child, page_query())
            .await
            .unwrap();
    assert_eq!(parents.roles.len(), 1);
    assert_eq!(parents.roles[0].id, parent);
    // The catalog reader embeds the parent role's display name.
    assert_eq!(parents.roles[0].name, "parent");
}

#[sqlx::test]
async fn batch_add_mixed(pool: PgPool) {
    let (ctx, project_id) = setup(pool).await;
    let role = make_role(&ctx, &project_id, "R", "r-src").await;
    let member_role = make_role(&ctx, &project_id, "M", "m-src").await;
    let alice = UserId::new_unchecked("oidc", "alice");
    provision_user(&ctx, &alice, "Alice").await;

    let resp = ApiServer::add_role_members(
        ctx.clone(),
        metadata(&project_id),
        role,
        AddRoleMembersRequest {
            members: vec![user_member(&alice), role_member(member_role)],
        },
    )
    .await
    .unwrap();
    assert_eq!(resp.members.len(), 2);

    let page =
        ApiServer::list_role_members(ctx.clone(), metadata(&project_id), role, list_query(None))
            .await
            .unwrap();
    assert_eq!(page.members.len(), 2);
    let kinds: Vec<RoleMemberType> = page.members.iter().map(kind).collect();
    assert!(kinds.contains(&RoleMemberType::User));
    assert!(kinds.contains(&RoleMemberType::Role));
}

// ==================== error / atomicity ====================

#[sqlx::test]
async fn add_unknown_user_404_and_atomic(pool: PgPool) {
    let (ctx, project_id) = setup(pool).await;
    let role = make_role(&ctx, &project_id, "R", "r-src").await;
    let member_role = make_role(&ctx, &project_id, "M", "m-src").await;
    let ghost = UserId::new_unchecked("oidc", "ghost"); // never provisioned

    // Batch with a valid role member AND an unknown user must fail wholesale.
    let err = ApiServer::add_role_members(
        ctx.clone(),
        metadata(&project_id),
        role,
        AddRoleMembersRequest {
            members: vec![role_member(member_role), user_member(&ghost)],
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.r#type, "RoleAssignmentUserNotFound");
    assert_eq!(err.error.code, http::StatusCode::NOT_FOUND.as_u16());

    // Atomic: the valid role member must NOT have been persisted.
    let page =
        ApiServer::list_role_members(ctx.clone(), metadata(&project_id), role, list_query(None))
            .await
            .unwrap();
    assert_eq!(page.members.len(), 0);
}

#[sqlx::test]
async fn add_nonexistent_role_member_404(pool: PgPool) {
    let (ctx, project_id) = setup(pool).await;
    let role = make_role(&ctx, &project_id, "R", "r-src").await;

    let err = ApiServer::add_role_members(
        ctx.clone(),
        metadata(&project_id),
        role,
        AddRoleMembersRequest {
            members: vec![role_member(RoleId::new_random())],
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.r#type, "RoleNotFoundInProject");
    assert_eq!(err.error.code, http::StatusCode::NOT_FOUND.as_u16());
}

#[sqlx::test]
async fn add_cycle_409(pool: PgPool) {
    let (ctx, project_id) = setup(pool).await;
    let a = make_role(&ctx, &project_id, "A", "a-src").await;
    let b = make_role(&ctx, &project_id, "B", "b-src").await;

    // A contains B.
    ApiServer::add_role_members(
        ctx.clone(),
        metadata(&project_id),
        a,
        AddRoleMembersRequest {
            members: vec![role_member(b)],
        },
    )
    .await
    .unwrap();

    // Adding A as a member of B would close a cycle B→A→B.
    let err = ApiServer::add_role_members(
        ctx.clone(),
        metadata(&project_id),
        b,
        AddRoleMembersRequest {
            members: vec![role_member(a)],
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.r#type, "RoleMembershipCycle");
    assert_eq!(err.error.code, http::StatusCode::CONFLICT.as_u16());
}

// ==================== idempotency / remove ====================

#[sqlx::test]
async fn idempotent_add_user(pool: PgPool) {
    let (ctx, project_id) = setup(pool).await;
    let role = make_role(&ctx, &project_id, "R", "r-src").await;
    let alice = UserId::new_unchecked("oidc", "alice");
    provision_user(&ctx, &alice, "Alice").await;

    for _ in 0..2 {
        let resp = ApiServer::add_role_members(
            ctx.clone(),
            metadata(&project_id),
            role,
            AddRoleMembersRequest {
                members: vec![user_member(&alice)],
            },
        )
        .await
        .unwrap();
        assert_eq!(resp.members.len(), 1);
    }

    let page =
        ApiServer::list_role_members(ctx.clone(), metadata(&project_id), role, list_query(None))
            .await
            .unwrap();
    assert_eq!(page.members.len(), 1);
}

#[sqlx::test]
async fn remove_member_and_remove_absent(pool: PgPool) {
    let (ctx, project_id) = setup(pool).await;
    let role = make_role(&ctx, &project_id, "R", "r-src").await;
    let alice = UserId::new_unchecked("oidc", "alice");
    provision_user(&ctx, &alice, "Alice").await;

    ApiServer::add_role_members(
        ctx.clone(),
        metadata(&project_id),
        role,
        AddRoleMembersRequest {
            members: vec![user_member(&alice)],
        },
    )
    .await
    .unwrap();

    // Remove the present member.
    ApiServer::remove_role_member(
        ctx.clone(),
        metadata(&project_id),
        role,
        RoleMemberType::User,
        alice.to_string(),
    )
    .await
    .unwrap();

    let page =
        ApiServer::list_role_members(ctx.clone(), metadata(&project_id), role, list_query(None))
            .await
            .unwrap();
    assert_eq!(page.members.len(), 0);

    // Removing again (absent) is a no-op and still succeeds.
    ApiServer::remove_role_member(
        ctx.clone(),
        metadata(&project_id),
        role,
        RoleMemberType::User,
        alice.to_string(),
    )
    .await
    .unwrap();
}

// ==================== filtering / pagination / user roles ====================

#[sqlx::test]
async fn list_members_type_filter(pool: PgPool) {
    let (ctx, project_id) = setup(pool).await;
    let role = make_role(&ctx, &project_id, "R", "r-src").await;
    let member_role = make_role(&ctx, &project_id, "M", "m-src").await;
    let alice = UserId::new_unchecked("oidc", "alice");
    provision_user(&ctx, &alice, "Alice").await;

    ApiServer::add_role_members(
        ctx.clone(),
        metadata(&project_id),
        role,
        AddRoleMembersRequest {
            members: vec![user_member(&alice), role_member(member_role)],
        },
    )
    .await
    .unwrap();

    let users = ApiServer::list_role_members(
        ctx.clone(),
        metadata(&project_id),
        role,
        list_query(Some(RoleMemberType::User)),
    )
    .await
    .unwrap();
    assert_eq!(users.members.len(), 1);
    assert!(same(&users.members[0], &user_member(&alice)));

    let roles = ApiServer::list_role_members(
        ctx.clone(),
        metadata(&project_id),
        role,
        list_query(Some(RoleMemberType::Role)),
    )
    .await
    .unwrap();
    assert_eq!(roles.members.len(), 1);
    assert!(same(&roles.members[0], &role_member(member_role)));
}

#[sqlx::test]
async fn list_members_pagination(pool: PgPool) {
    let (ctx, project_id) = setup(pool).await;
    let role = make_role(&ctx, &project_id, "R", "r-src").await;
    let alice = UserId::new_unchecked("oidc", "alice");
    let bob = UserId::new_unchecked("oidc", "bob");
    provision_user(&ctx, &alice, "Alice").await;
    provision_user(&ctx, &bob, "Bob").await;

    ApiServer::add_role_members(
        ctx.clone(),
        metadata(&project_id),
        role,
        AddRoleMembersRequest {
            members: vec![user_member(&alice), user_member(&bob)],
        },
    )
    .await
    .unwrap();

    // Drain with page_size = 1; every member appears exactly once.
    let mut seen = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let query = lakekeeper::api::management::v1::role_membership::ListMembersQuery {
            r#type: None,
            page_token: token.clone(),
            page_size: Some(1),
        };
        let page = ApiServer::list_role_members(ctx.clone(), metadata(&project_id), role, query)
            .await
            .unwrap();
        if page.members.is_empty() {
            assert_eq!(page.next_page_token, None);
            break;
        }
        assert_eq!(page.members.len(), 1);
        seen.push(member_id_string(&page.members[0]));
        token = page.next_page_token;
        assert!(token.is_some());
    }
    seen.sort();
    let mut expected = vec![alice.to_string(), bob.to_string()];
    expected.sort();
    assert_eq!(seen, expected);
}

#[sqlx::test]
async fn list_user_roles(pool: PgPool) {
    let (ctx, project_id) = setup(pool).await;
    let r1 = make_role(&ctx, &project_id, "R1", "r1-src").await;
    let r2 = make_role(&ctx, &project_id, "R2", "r2-src").await;
    let alice = UserId::new_unchecked("oidc", "alice");
    provision_user(&ctx, &alice, "Alice").await;

    for role in [r1, r2] {
        ApiServer::add_role_members(
            ctx.clone(),
            metadata(&project_id),
            role,
            AddRoleMembersRequest {
                members: vec![user_member(&alice)],
            },
        )
        .await
        .unwrap();
    }

    let page = ApiServer::list_user_roles(
        ctx.clone(),
        metadata(&project_id),
        alice.clone(),
        page_query(),
    )
    .await
    .unwrap();
    assert_eq!(page.roles.len(), 2);
    let mut ids: Vec<RoleId> = page.roles.iter().map(|r| r.id).collect();
    ids.sort();
    let mut expected = vec![r1, r2];
    expected.sort();
    assert_eq!(ids, expected);
    // Each row embeds the role's display name (R1/R2), sourced from `role.name`.
    let names: std::collections::HashSet<String> =
        page.roles.iter().map(|r| r.name.clone()).collect();
    assert_eq!(
        names,
        ["R1".to_string(), "R2".to_string()].into_iter().collect()
    );
}

/// A user that was never provisioned → 404 (the catalog reader returns `None`).
#[sqlx::test]
async fn list_user_roles_unknown_user_404(pool: PgPool) {
    let (ctx, project_id) = setup(pool).await;
    let ghost = UserId::new_unchecked("oidc", "ghost");

    let err = ApiServer::list_user_roles(ctx.clone(), metadata(&project_id), ghost, page_query())
        .await
        .unwrap_err();
    assert_eq!(err.error.r#type, "UserNotFound");
    assert_eq!(err.error.code, http::StatusCode::NOT_FOUND.as_u16());
}

/// A provisioned user with zero role assignments → 200 with an empty page, NOT a
/// 404 — the reader returns `Some(empty)`, distinguishing "exists, no roles" from
/// "no such user" (previous test).
#[sqlx::test]
async fn list_user_roles_existing_user_no_roles_returns_empty(pool: PgPool) {
    let (ctx, project_id) = setup(pool).await;
    let alice = UserId::new_unchecked("oidc", "alice");
    provision_user(&ctx, &alice, "Alice").await;

    let page = ApiServer::list_user_roles(ctx.clone(), metadata(&project_id), alice, page_query())
        .await
        .unwrap();
    assert_eq!(page.roles.len(), 0);
    assert_eq!(page.next_page_token, None);
}

/// The mixed-batch wrapper is genuinely atomic: when a later member fails, an
/// EARLIER successful insert is rolled back. The wrapper inserts users before role
/// edges, so a valid user + a cycle-inducing role member exercises real rollback
/// (unlike a first-op failure, which never reaches the second insert).
#[sqlx::test]
async fn batch_add_rolls_back_user_on_cycle(pool: PgPool) {
    let (ctx, project_id) = setup(pool).await;
    let a = make_role(&ctx, &project_id, "A", "a-src").await;
    let m = make_role(&ctx, &project_id, "M", "m-src").await;
    let alice = UserId::new_unchecked("oidc", "alice");
    provision_user(&ctx, &alice, "Alice").await;

    // A is a member of M, so adding M as a member of A would close a cycle.
    ApiServer::add_role_members(
        ctx.clone(),
        metadata(&project_id),
        m,
        AddRoleMembersRequest {
            members: vec![role_member(a)],
        },
    )
    .await
    .unwrap();

    // Batch into A: [alice (valid, inserted first), role M (cycle, fails second)].
    let err = ApiServer::add_role_members(
        ctx.clone(),
        metadata(&project_id),
        a,
        AddRoleMembersRequest {
            members: vec![user_member(&alice), role_member(m)],
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.r#type, "RoleMembershipCycle");
    assert_eq!(err.error.code, http::StatusCode::CONFLICT.as_u16());

    // Rollback: alice's earlier (successful) assignment to A was rolled back.
    let page =
        ApiServer::list_role_members(ctx.clone(), metadata(&project_id), a, list_query(None))
            .await
            .unwrap();
    assert_eq!(page.members.len(), 0);
}

/// A member repeated in one request is deduplicated in both persistence and the
/// echoed response.
#[sqlx::test]
async fn add_dedups_repeated_member(pool: PgPool) {
    let (ctx, project_id) = setup(pool).await;
    let role = make_role(&ctx, &project_id, "R", "r-src").await;
    let member_role = make_role(&ctx, &project_id, "M", "m-src").await;

    let resp = ApiServer::add_role_members(
        ctx.clone(),
        metadata(&project_id),
        role,
        AddRoleMembersRequest {
            members: vec![role_member(member_role), role_member(member_role)],
        },
    )
    .await
    .unwrap();
    assert_eq!(resp.members, vec![role_member(member_role)]);

    let page =
        ApiServer::list_role_members(ctx.clone(), metadata(&project_id), role, list_query(None))
            .await
            .unwrap();
    assert_eq!(page.members.len(), 1);
}

// ── query helpers ──────────────────────────────────────────────────────────

fn list_query(
    r#type: Option<RoleMemberType>,
) -> lakekeeper::api::management::v1::role_membership::ListMembersQuery {
    lakekeeper::api::management::v1::role_membership::ListMembersQuery {
        r#type,
        page_token: None,
        page_size: None,
    }
}

fn page_query() -> lakekeeper::api::management::v1::role_membership::ListRolesPageQuery {
    lakekeeper::api::management::v1::role_membership::ListRolesPageQuery {
        page_token: None,
        page_size: None,
    }
}

// ==================== transitive ====================

/// Nested fixture A ⊃ B ⊃ user U, plus user V direct on A. Transitive members of
/// A are the role B, the direct user V, and the via-B user U; direct members are
/// only B and V.
#[sqlx::test]
async fn transitive_role_members(pool: PgPool) {
    let (ctx, project_id) = setup(pool).await;
    let a = make_role(&ctx, &project_id, "A", "a-src").await;
    let b = make_role(&ctx, &project_id, "B", "b-src").await;
    let u = UserId::new_unchecked("oidc", "u");
    let v = UserId::new_unchecked("oidc", "v");
    provision_user(&ctx, &u, "U").await;
    provision_user(&ctx, &v, "V").await;

    // B ∈ A and V ∈ A (direct); U ∈ B.
    ApiServer::add_role_members(
        ctx.clone(),
        metadata(&project_id),
        a,
        AddRoleMembersRequest {
            members: vec![role_member(b), user_member(&v)],
        },
    )
    .await
    .unwrap();
    ApiServer::add_role_members(
        ctx.clone(),
        metadata(&project_id),
        b,
        AddRoleMembersRequest {
            members: vec![user_member(&u)],
        },
    )
    .await
    .unwrap();

    // Direct members of A: B and V only (U is not direct).
    let direct =
        ApiServer::list_role_members(ctx.clone(), metadata(&project_id), a, list_query(None))
            .await
            .unwrap();
    assert_eq!(direct.members.len(), 2);

    // Transitive members of A: role B, user V (direct), user U (via B).
    let page = ApiServer::list_role_transitive_members(
        ctx.clone(),
        metadata(&project_id),
        a,
        list_query(None),
    )
    .await
    .unwrap();
    let roles: std::collections::HashSet<String> = page
        .members
        .iter()
        .filter(|m| kind(m) == RoleMemberType::Role)
        .map(member_id_string)
        .collect();
    let users: std::collections::HashSet<String> = page
        .members
        .iter()
        .filter(|m| kind(m) == RoleMemberType::User)
        .map(member_id_string)
        .collect();
    assert_eq!(roles, [b.to_string()].into_iter().collect());
    assert_eq!(users, [u.to_string(), v.to_string()].into_iter().collect());

    // Role member B carries its display name (the direct listing's name contract
    // holds on the transitive listing too).
    let b_name = page
        .members
        .iter()
        .find_map(|m| match m {
            RoleMember::Role(rm) if rm.id == b => Some(rm.name.clone()),
            _ => None,
        })
        .expect("role B present among transitive members");
    assert_eq!(b_name, "B");
    // User members are hydrated with their catalog names (U/V) on the transitive
    // listing too — the catalog JOIN hydrates user members in one query.
    let user_names: std::collections::HashSet<Option<String>> = page
        .members
        .iter()
        .filter_map(|m| match m {
            RoleMember::User(uu) => Some(uu.name.clone()),
            RoleMember::Role(_) => None,
        })
        .collect();
    assert_eq!(
        user_names,
        [Some("U".to_string()), Some("V".to_string())]
            .into_iter()
            .collect()
    );

    // The `?type=` filter composes on the transitive endpoint.
    let only_users = ApiServer::list_role_transitive_members(
        ctx.clone(),
        metadata(&project_id),
        a,
        list_query(Some(RoleMemberType::User)),
    )
    .await
    .unwrap();
    assert!(
        only_users
            .members
            .iter()
            .all(|m| kind(m) == RoleMemberType::User)
    );
    let only_user_ids: std::collections::HashSet<String> =
        only_users.members.iter().map(member_id_string).collect();
    assert_eq!(
        only_user_ids,
        [u.to_string(), v.to_string()].into_iter().collect()
    );
}

/// Regression for transitive-member keyset stability under edge churn. A role's
/// page key is its own immutable `created_at`, NOT the minimum incoming-edge
/// timestamp — so deleting the edge that *held* the minimum mid-pagination must not
/// shift the role's key forward and re-emit it on a later page.
///
/// Fixture: R ⊃ C, R ⊃ D, C ⊃ D — D is reachable via two parents (C and R). Edges
/// are added C→D, R→C, R→D, so the C→D edge is the earliest of all; pre-fix that
/// made D's key the smallest, sorting D first. We drain R's transitive ROLE members
/// one per page and remove C→D after the first page (D stays reachable via R→D).
/// Pre-fix, D's key jumps from the C→D edge time to the R→D edge time and D is
/// returned a second time; with the immutable key each member appears exactly once.
#[sqlx::test]
async fn transitive_role_members_keyset_stable_under_edge_removal(pool: PgPool) {
    let (ctx, project_id) = setup(pool).await;
    // Roles created in order, so role.created_at is R < C < D (the immutable keys).
    let r = make_role(&ctx, &project_id, "R", "r-src").await;
    let c = make_role(&ctx, &project_id, "C", "c-src").await;
    let d = make_role(&ctx, &project_id, "D", "d-src").await;

    // Edge order sets the pre-fix MIN(edge.created_at): C→D earliest, then R→C, R→D.
    for (parent, member) in [(c, d), (r, c), (r, d)] {
        ApiServer::add_role_members(
            ctx.clone(),
            metadata(&project_id),
            parent,
            AddRoleMembersRequest {
                members: vec![role_member(member)],
            },
        )
        .await
        .unwrap();
    }

    // Drain R's transitive role members one page at a time; after the first page,
    // remove C→D (the edge holding D's pre-fix minimum key).
    let mut seen: Vec<String> = Vec::new();
    let mut token: Option<String> = None;
    let mut removed_edge = false;
    loop {
        let page = ApiServer::list_role_transitive_members(
            ctx.clone(),
            metadata(&project_id),
            r,
            lakekeeper::api::management::v1::role_membership::ListMembersQuery {
                r#type: Some(RoleMemberType::Role),
                page_token: token.clone(),
                page_size: Some(1),
            },
        )
        .await
        .unwrap();
        seen.extend(page.members.iter().map(member_id_string));
        match page.next_page_token {
            Some(next) => {
                token = Some(next);
                if !removed_edge {
                    ApiServer::remove_role_member(
                        ctx.clone(),
                        metadata(&project_id),
                        c,
                        RoleMemberType::Role,
                        d.to_string(),
                    )
                    .await
                    .unwrap();
                    removed_edge = true;
                }
            }
            None => break,
        }
    }

    // Each transitive role member is returned exactly once; the set is exactly {C, D}.
    let mut unique: std::collections::HashSet<String> = std::collections::HashSet::new();
    for id in &seen {
        assert!(
            unique.insert(id.clone()),
            "member {id} was returned more than once: {seen:?}"
        );
    }
    assert_eq!(unique, [c.to_string(), d.to_string()].into_iter().collect());
}

/// Same fixture from the user's side: U is directly assigned only to B, but its
/// effective (transitive) roles are B and A. Names are embedded.
#[sqlx::test]
async fn transitive_user_roles(pool: PgPool) {
    let (ctx, project_id) = setup(pool).await;
    let a = make_role(&ctx, &project_id, "A", "a-src").await;
    let b = make_role(&ctx, &project_id, "B", "b-src").await;
    let u = UserId::new_unchecked("oidc", "u");
    provision_user(&ctx, &u, "U").await;

    // B ∈ A ; U ∈ B.
    ApiServer::add_role_members(
        ctx.clone(),
        metadata(&project_id),
        a,
        AddRoleMembersRequest {
            members: vec![role_member(b)],
        },
    )
    .await
    .unwrap();
    ApiServer::add_role_members(
        ctx.clone(),
        metadata(&project_id),
        b,
        AddRoleMembersRequest {
            members: vec![user_member(&u)],
        },
    )
    .await
    .unwrap();

    // Direct: U is only in B.
    let direct =
        ApiServer::list_user_roles(ctx.clone(), metadata(&project_id), u.clone(), page_query())
            .await
            .unwrap();
    let direct_ids: Vec<RoleId> = direct.roles.iter().map(|r| r.id).collect();
    assert_eq!(direct_ids, vec![b]);

    // Transitive: U effectively holds B and A.
    let page = ApiServer::list_user_transitive_roles(
        ctx.clone(),
        metadata(&project_id),
        u.clone(),
        page_query(),
    )
    .await
    .unwrap();
    let mut ids: Vec<RoleId> = page.roles.iter().map(|r| r.id).collect();
    ids.sort();
    let mut expected = vec![a, b];
    expected.sort();
    assert_eq!(ids, expected);
    let names: std::collections::HashSet<String> =
        page.roles.iter().map(|r| r.name.clone()).collect();
    assert_eq!(
        names,
        ["A".to_string(), "B".to_string()].into_iter().collect()
    );
}

/// The transitive user-roles reader keeps the direct reader's unknown-user
/// contract: a never-provisioned user → 404 (catalog reader returns `None`).
#[sqlx::test]
async fn transitive_user_roles_unknown_user_404(pool: PgPool) {
    let (ctx, project_id) = setup(pool).await;
    let ghost = UserId::new_unchecked("oidc", "ghost");

    let err = ApiServer::list_user_transitive_roles(
        ctx.clone(),
        metadata(&project_id),
        ghost,
        page_query(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.r#type, "UserNotFound");
    assert_eq!(err.error.code, http::StatusCode::NOT_FOUND.as_u16());
}

/// Ancestor walk: C ∈ B ∈ A. C is directly a member only of B, but its transitive
/// member-of set (the roles it effectively belongs to) is B and A. Names are
/// embedded.
#[sqlx::test]
async fn transitive_role_member_of(pool: PgPool) {
    let (ctx, project_id) = setup(pool).await;
    let a = make_role(&ctx, &project_id, "A", "a-src").await;
    let b = make_role(&ctx, &project_id, "B", "b-src").await;
    let c = make_role(&ctx, &project_id, "C", "c-src").await;

    // B ∈ A ; C ∈ B.
    ApiServer::add_role_members(
        ctx.clone(),
        metadata(&project_id),
        a,
        AddRoleMembersRequest {
            members: vec![role_member(b)],
        },
    )
    .await
    .unwrap();
    ApiServer::add_role_members(
        ctx.clone(),
        metadata(&project_id),
        b,
        AddRoleMembersRequest {
            members: vec![role_member(c)],
        },
    )
    .await
    .unwrap();

    // Direct: C is a member only of B.
    let direct =
        ApiServer::list_role_member_of(ctx.clone(), metadata(&project_id), c, page_query())
            .await
            .unwrap();
    let direct_ids: Vec<RoleId> = direct.roles.iter().map(|r| r.id).collect();
    assert_eq!(direct_ids, vec![b]);

    // Transitive: C effectively belongs to B and A.
    let page = ApiServer::list_role_transitive_member_of(
        ctx.clone(),
        metadata(&project_id),
        c,
        page_query(),
    )
    .await
    .unwrap();
    let mut ids: Vec<RoleId> = page.roles.iter().map(|r| r.id).collect();
    ids.sort();
    let mut expected = vec![a, b];
    expected.sort();
    assert_eq!(ids, expected);
    let names: std::collections::HashSet<String> =
        page.roles.iter().map(|r| r.name.clone()).collect();
    assert_eq!(
        names,
        ["A".to_string(), "B".to_string()].into_iter().collect()
    );
}

// ==================== source-system rebind invalidates closures ====================

/// Rebinding a role's source system changes its `RoleIdent`, which is cached per
/// row in every assignee's USER_ASSIGNMENTS closure and in the role's ROLE_MEMBERS
/// entry. The handler must evict both (mirroring `delete_role`), or external
/// authorizers evaluate the stale ident until TTL. Behavioral assertion: after the
/// rebind, the cached reads reflect the NEW ident (and return a fresh `Arc`),
/// proving eviction. The eviction is synchronous (post-commit in the handler), so
/// no sleep is needed.
#[sqlx::test]
async fn source_system_rebind_evicts_user_assignment_and_member_closures(pool: PgPool) {
    use lakekeeper::{
        api::management::v1::role::{Service as _, UpdateRoleSourceSystemRequest},
        service::CatalogRoleAssignmentOps as _,
    };

    let (ctx, project_id) = setup(pool).await;
    let role = make_role(&ctx, &project_id, "rebind-role", "old-src").await;
    let alice = UserId::new_unchecked("oidc", "alice");
    provision_user(&ctx, &alice, "Alice").await;

    // Alice is a direct member of the role → she effectively holds it.
    ApiServer::add_role_members(
        ctx.clone(),
        metadata(&project_id),
        role,
        AddRoleMembersRequest {
            members: vec![user_member(&alice)],
        },
    )
    .await
    .unwrap();

    // Warm both closures so they cache the OLD ident ("old-src").
    let warmed_user =
        PostgresBackend::list_role_assignments_for_user(&alice, ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    assert_eq!(
        warmed_user
            .roles
            .iter()
            .find(|r| r.role_id == role)
            .expect("alice has the role")
            .role_ident
            .source_id(),
        &RoleSourceId::try_new("old-src").unwrap(),
    );
    let warmed_members =
        PostgresBackend::list_role_assignments_for_role(role, ctx.v1_state.catalog.clone())
            .await
            .unwrap()
            .expect("role has a member list");
    assert_eq!(
        warmed_members.role_ident.source_id(),
        &RoleSourceId::try_new("old-src").unwrap(),
    );

    // Rebind the role's source system (changes its RoleIdent's source_id).
    ApiServer::update_role_source_system(
        ctx.clone(),
        metadata(&project_id),
        role,
        UpdateRoleSourceSystemRequest {
            provider_id: RoleProviderId::try_new("lakekeeper").unwrap(),
            source_id: RoleSourceId::try_new("new-src").unwrap(),
        },
    )
    .await
    .unwrap();

    // USER_ASSIGNMENTS closure must reflect the rebound ident, not the stale one,
    // and be a fresh Arc (i.e. it was evicted and reloaded).
    let after_user =
        PostgresBackend::list_role_assignments_for_user(&alice, ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    assert_eq!(
        after_user
            .roles
            .iter()
            .find(|r| r.role_id == role)
            .expect("alice still has the role")
            .role_ident
            .source_id(),
        &RoleSourceId::try_new("new-src").unwrap(),
        "user-assignments closure served the stale ident after a source-system rebind",
    );
    assert!(
        !std::sync::Arc::ptr_eq(&warmed_user, &after_user),
        "user-assignments entry was not evicted by the rebind",
    );

    // ROLE_MEMBERS entry must likewise reflect the rebound ident.
    let after_members =
        PostgresBackend::list_role_assignments_for_role(role, ctx.v1_state.catalog.clone())
            .await
            .unwrap()
            .expect("role still has a member list");
    assert_eq!(
        after_members.role_ident.source_id(),
        &RoleSourceId::try_new("new-src").unwrap(),
        "role-members entry served the stale ident after a source-system rebind",
    );
}
