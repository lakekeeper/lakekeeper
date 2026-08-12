//! Behavioral tests for the grants management API on the catalog arm (Postgres).
//! Most run under `AllowAllAuthorizer`; the final section switches to a denying
//! authorizer to pin what each gate refuses. Exact assertions throughout.

use iceberg::{
    NamespaceIdent,
    spec::{NestedField, PrimitiveType, Schema, Type, UnboundPartitionSpec},
};
use iceberg_ext::catalog::rest::CreateTableRequest;
use lakekeeper::{
    ProjectId,
    api::{
        ApiContext, RequestMetadata, RequestMetadataTestBuilder,
        data::v1::generic_tables::{GenericTableService as _, ListGenericTablesQuery},
        iceberg::{
            types::Prefix,
            v1::{
                CreateNamespaceRequest, NamespaceParameters, PageToken, PaginationQuery,
                namespace::NamespaceService as _,
                tables::{DataAccess, TablesService as _},
            },
        },
        management::v1::{
            ApiServer,
            check::UserOrRole,
            grant::{
                ApplyGrantsRequest, GetGrantAccessQuery, GrantEntry, GrantResourceResponse,
                ListGrantsQuery, Service as _,
            },
            role::{CreateRoleRequest, Service as _},
            tag::{CreateTagDefinitionRequest, Service as _},
            user::{UserLastUpdatedWith, UserType},
            warehouse::Service as _,
        },
    },
    server::CatalogServer,
    service::{
        CatalogGrantOps as _, CatalogNamespaceOps as _, CatalogStore, RoleId, State, TagScope,
        TagValueKind, Transaction, UserId, UserUpsertMode,
        authn::Actor,
        authz::{
            AllowAllAuthorizer, GrantFilter, GrantResource, GrantSpec, ResourceType, UserOrRoleId,
            tests::HidingAuthorizer,
        },
    },
};
use lakekeeper_integration_tests::{
    SetupTestCatalog, create_generic_table, create_view, memory_io_profile, random_request_metadata,
};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;

type Ctx = ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>;
type Server = ApiServer<PostgresBackend, AllowAllAuthorizer, SecretsState>;

struct Fixture {
    ctx: Ctx,
    metadata: RequestMetadata,
    warehouse_id: lakekeeper::WarehouseId,
    project_id: ProjectId,
    alice: UserId,
}

async fn setup(pool: PgPool) -> Fixture {
    let (ctx, warehouse) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;
    let metadata = RequestMetadataTestBuilder::builder()
        .project_id(Some(warehouse.project_id.clone()))
        .build();
    let alice = UserId::try_from("oidc~alice").unwrap();
    provision_user(&ctx, &alice).await;
    Fixture {
        ctx,
        metadata,
        warehouse_id: warehouse.warehouse_id,
        project_id: (*warehouse.project_id).clone(),
        alice,
    }
}

async fn provision_user<A: lakekeeper::service::authz::Authorizer>(
    ctx: &ApiContext<State<A, PostgresBackend, SecretsState>>,
    user_id: &UserId,
) {
    let mut tx =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    PostgresBackend::create_or_update_user(
        user_id,
        "Test User",
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

fn entry(privilege: &str, user: &UserId) -> GrantEntry {
    GrantEntry {
        privilege: privilege.to_string(),
        principal: UserOrRole::User(user.clone()),
    }
}

fn writes(entries: Vec<GrantEntry>) -> ApplyGrantsRequest {
    ApplyGrantsRequest {
        writes: entries,
        deletes: vec![],
    }
}

fn deletes(entries: Vec<GrantEntry>) -> ApplyGrantsRequest {
    ApplyGrantsRequest {
        writes: vec![],
        deletes: entries,
    }
}

fn no_pagination() -> PaginationQuery {
    PaginationQuery::new(PageToken::Empty, None)
}

/// The project-wide listing requires a principal, so every call to it names one.
fn for_user(user: &UserId) -> ListGrantsQuery {
    ListGrantsQuery {
        principal_user: Some(user.clone()),
        principal_role: None,
    }
}

/// A role in the request's project, so it is a legal grant principal.
async fn create_role_in_project(ctx: &Ctx, metadata: &RequestMetadata) -> RoleId {
    let created = Server::create_role(
        CreateRoleRequest {
            name: "grant_role".to_string(),
            description: None,
            project_id: None,
            provider_id: None,
            source_id: None,
        },
        ctx.clone(),
        metadata.clone(),
    )
    .await
    .unwrap();
    created.id
}

async fn create_namespace_in(ctx: &Ctx, warehouse_id: lakekeeper::WarehouseId, name: &str) {
    let prefix: Prefix = warehouse_id.to_string().into();
    CatalogServer::create_namespace(
        Some(prefix),
        CreateNamespaceRequest {
            namespace: NamespaceIdent::new(name.to_string()),
            properties: None,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
}

async fn create_table_returning_id(
    ctx: &Ctx,
    warehouse_id: lakekeeper::WarehouseId,
    ns: &str,
    name: &str,
) -> lakekeeper::service::TableId {
    create_namespace_in(ctx, warehouse_id, ns).await;
    let prefix: Prefix = warehouse_id.to_string().into();
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .unwrap();
    let created = CatalogServer::create_table(
        NamespaceParameters {
            namespace: NamespaceIdent::new(ns.to_string()),
            prefix: Some(prefix),
        },
        CreateTableRequest {
            name: name.to_string(),
            location: None,
            schema,
            partition_spec: Some(UnboundPartitionSpec::builder().build()),
            write_order: None,
            stage_create: Some(false),
            properties: None,
        },
        DataAccess::not_specified(),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    created.metadata.uuid().into()
}

/// The namespace level writes a resource shape no other level does: `warehouse_id` and
/// `namespace_id` together, guarded by a composite foreign key whose MATCH SIMPLE
/// semantics would skip entirely if either column were null. Round-trip it for real.
#[sqlx::test]
async fn apply_and_list_namespace_grants(pool: PgPool) {
    let f = setup(pool).await;
    create_namespace_in(&f.ctx, f.warehouse_id, "grant_nsg").await;
    let namespace_id = PostgresBackend::get_namespace(
        f.warehouse_id,
        NamespaceIdent::new("grant_nsg".to_string()),
        f.ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap()
    .namespace_id();

    Server::apply_namespace_grants(
        f.warehouse_id,
        namespace_id,
        f.ctx.clone(),
        f.metadata.clone(),
        writes(vec![entry("get_metadata", &f.alice)]),
    )
    .await
    .unwrap();

    let page = Server::list_namespace_grants(
        f.warehouse_id,
        namespace_id,
        f.ctx.clone(),
        f.metadata.clone(),
        ListGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants.len(), 1);
    assert_eq!(
        page.grants[0].resource,
        GrantResourceResponse::Namespace {
            warehouse_id: f.warehouse_id,
            namespace_id
        }
    );

    // Also reachable through the project roll-up, which joins namespaces separately.
    let all = Server::list_grants(
        f.ctx.clone(),
        f.metadata.clone(),
        for_user(&f.alice),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(all.grants.len(), 1);

    Server::apply_namespace_grants(
        f.warehouse_id,
        namespace_id,
        f.ctx.clone(),
        f.metadata.clone(),
        deletes(vec![entry("get_metadata", &f.alice)]),
    )
    .await
    .unwrap();
    let page = Server::list_namespace_grants(
        f.warehouse_id,
        namespace_id,
        f.ctx.clone(),
        f.metadata.clone(),
        ListGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants, Vec::new());
}

/// Tag grants are the only level whose cross-project masking comes from the fetch being
/// project-scoped rather than from an explicit check, and the only one keyed solely by
/// `tag_definition_id`.
#[sqlx::test]
async fn apply_and_list_tag_grants(pool: PgPool) {
    let f = setup(pool).await;
    let definition = Server::create_tag_definition(
        CreateTagDefinitionRequest::builder()
            .name("pii".to_string())
            .scope(vec![TagScope::Table])
            .value_kind(TagValueKind::Marker)
            .allowed_values(None)
            .build(),
        f.ctx.clone(),
        f.metadata.clone(),
    )
    .await
    .unwrap();

    Server::apply_tag_grants(
        definition.id,
        f.ctx.clone(),
        f.metadata.clone(),
        writes(vec![entry("apply", &f.alice)]),
    )
    .await
    .unwrap();

    let page = Server::list_tag_grants(
        definition.id,
        f.ctx.clone(),
        f.metadata.clone(),
        ListGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants.len(), 1);
    assert_eq!(
        page.grants[0].resource,
        GrantResourceResponse::Tag {
            tag_definition_id: definition.id
        }
    );

    Server::apply_tag_grants(
        definition.id,
        f.ctx.clone(),
        f.metadata.clone(),
        deletes(vec![entry("apply", &f.alice)]),
    )
    .await
    .unwrap();
    let page = Server::list_tag_grants(
        definition.id,
        f.ctx.clone(),
        f.metadata.clone(),
        ListGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants, Vec::new());
}

/// Generic tables share the collapsed `tabular` storage kind with tables and views, so
/// this exercises the third branch of the kind guard.
#[sqlx::test]
async fn apply_and_list_generic_table_grants(pool: PgPool) {
    let f = setup(pool).await;
    create_namespace_in(&f.ctx, f.warehouse_id, "grant_gt").await;
    create_generic_table(f.ctx.clone(), f.warehouse_id.to_string(), "grant_gt", "gt1")
        .await
        .unwrap();
    let listed = CatalogServer::list_generic_tables(
        NamespaceParameters {
            prefix: Some(f.warehouse_id.to_string().into()),
            namespace: NamespaceIdent::new("grant_gt".to_string()),
        },
        ListGenericTablesQuery::default(),
        f.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let generic_table_id = listed
        .identifiers
        .iter()
        .find(|i| i.name == "gt1")
        .and_then(|i| i.id)
        .unwrap();

    Server::apply_generic_table_grants(
        f.warehouse_id,
        generic_table_id,
        f.ctx.clone(),
        f.metadata.clone(),
        writes(vec![entry("get_metadata", &f.alice)]),
    )
    .await
    .unwrap();

    let page = Server::list_generic_table_grants(
        f.warehouse_id,
        generic_table_id,
        f.ctx.clone(),
        f.metadata.clone(),
        ListGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants.len(), 1);
    assert_eq!(
        page.grants[0].resource,
        GrantResourceResponse::GenericTable {
            warehouse_id: f.warehouse_id,
            generic_table_id
        }
    );

    // Addressing the same id as a table must not reach it: the stored kind differs.
    let as_table: lakekeeper::service::TableId = (*generic_table_id).into();
    let err = Server::list_table_grants(
        f.warehouse_id,
        as_table,
        f.ctx.clone(),
        f.metadata.clone(),
        ListGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 404);
}

/// A server grant must be readable back through its own endpoint, and must not appear
/// in the project roll-up — it belongs to no project.
#[sqlx::test]
async fn apply_and_list_server_grants(pool: PgPool) {
    let f = setup(pool).await;
    Server::apply_server_grants(
        f.ctx.clone(),
        f.metadata.clone(),
        writes(vec![entry("list_users", &f.alice)]),
    )
    .await
    .unwrap();

    let page = Server::list_server_grants(
        f.ctx.clone(),
        f.metadata.clone(),
        ListGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants.len(), 1);
    assert_eq!(page.grants[0].resource, GrantResourceResponse::Server);

    // The project-scoped listing excludes them even for the principal who holds them.
    let all = Server::list_grants(
        f.ctx.clone(),
        f.metadata.clone(),
        for_user(&f.alice),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(all.grants, Vec::new());
}

/// The project level has its own resource listing, distinct from the project-wide
/// roll-up that reports grants on everything inside the project.
#[sqlx::test]
async fn apply_and_list_project_grants(pool: PgPool) {
    let f = setup(pool).await;
    Server::apply_project_grants(
        f.ctx.clone(),
        f.metadata.clone(),
        writes(vec![entry("get_metadata", &f.alice)]),
    )
    .await
    .unwrap();

    let page = Server::list_project_grants(
        f.ctx.clone(),
        f.metadata.clone(),
        ListGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants.len(), 1);
    assert_eq!(
        page.grants[0].resource,
        GrantResourceResponse::Project {
            project_id: f.project_id.clone()
        }
    );
}

/// The project-wide listing resolves each grant's project through a different join per
/// resource kind, so a listing that only joined warehouses would silently drop the
/// others. Server grants belong to no project and must not appear.
#[sqlx::test]
async fn the_project_listing_covers_every_level(pool: PgPool) {
    let f = setup(pool).await;
    let table_id = create_table_returning_id(&f.ctx, f.warehouse_id, "grant_ns", "t1").await;

    Server::apply_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        writes(vec![entry("get_metadata", &f.alice)]),
    )
    .await
    .unwrap();
    Server::apply_table_grants(
        f.warehouse_id,
        table_id,
        f.ctx.clone(),
        f.metadata.clone(),
        writes(vec![entry("get_metadata", &f.alice)]),
    )
    .await
    .unwrap();
    Server::apply_project_grants(
        f.ctx.clone(),
        f.metadata.clone(),
        writes(vec![entry("get_metadata", &f.alice)]),
    )
    .await
    .unwrap();
    Server::apply_server_grants(
        f.ctx.clone(),
        f.metadata.clone(),
        writes(vec![entry("list_users", &f.alice)]),
    )
    .await
    .unwrap();

    let page = Server::list_grants(
        f.ctx.clone(),
        f.metadata.clone(),
        for_user(&f.alice),
        no_pagination(),
    )
    .await
    .unwrap();
    let mut kinds: Vec<String> = page
        .grants
        .iter()
        .map(|g| match &g.resource {
            GrantResourceResponse::Server => "server".to_string(),
            GrantResourceResponse::Project { .. } => "project".to_string(),
            GrantResourceResponse::Warehouse { .. } => "warehouse".to_string(),
            GrantResourceResponse::Table { .. } => "table".to_string(),
            other => format!("{other:?}"),
        })
        .collect();
    kinds.sort();
    // The server grant is deliberately absent: it has no project to be listed under.
    assert_eq!(kinds, vec!["project", "table", "warehouse"]);
}

/// The listing answers for one principal, so a request that names none is a client
/// error rather than a licence to read the whole project. Refused before the
/// authorization check, like the two-principals rejection: there is no listing to
/// authorize yet.
#[sqlx::test]
async fn the_project_listing_requires_a_principal(pool: PgPool) {
    let f = setup(pool).await;
    let err = Server::list_grants(
        f.ctx.clone(),
        f.metadata.clone(),
        ListGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 400);
    assert_eq!(err.error.r#type, "MissingGrantPrincipal");
}

/// Grants are reported at the layer they are held. A grant a role holds must not
/// surface under a user who has that role — resolution is the check endpoints' job.
#[sqlx::test]
async fn a_role_held_grant_is_not_listed_under_its_members(pool: PgPool) {
    let f = setup(pool).await;
    let role_id = create_role_in_project(&f.ctx, &f.metadata).await;

    Server::apply_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        writes(vec![GrantEntry {
            privilege: "get_metadata".to_string(),
            principal: UserOrRole::Role(role_id.into_api_assignee()),
        }]),
    )
    .await
    .unwrap();

    let by_role = Server::list_grants(
        f.ctx.clone(),
        f.metadata.clone(),
        ListGrantsQuery {
            principal_user: None,
            principal_role: Some(role_id),
        },
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(by_role.grants.len(), 1);

    let by_user = Server::list_grants(
        f.ctx.clone(),
        f.metadata.clone(),
        ListGrantsQuery {
            principal_user: Some(f.alice.clone()),
            principal_role: None,
        },
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(by_user.grants, Vec::new());
}

/// The self-listing allowance must actually be reachable, and must be scoped to the
/// caller's own grants. Needs an authenticated actor: the default test metadata is
/// anonymous, whose `user_id()` is `None`, so `is_self` could never be true there.
#[sqlx::test]
async fn listing_your_own_grants_is_scoped_to_you(pool: PgPool) {
    let f = setup(pool).await;
    let bob = UserId::try_from("oidc~bob").unwrap();
    provision_user(&f.ctx, &bob).await;

    Server::apply_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        writes(vec![
            entry("get_metadata", &f.alice),
            entry("list_namespaces", &bob),
        ]),
    )
    .await
    .unwrap();

    // Alice, authenticated as herself, asking about herself.
    let as_alice = RequestMetadataTestBuilder::builder()
        .actor(Actor::Principal(f.alice.clone()))
        .project_id(Some(f.project_id.clone().into()))
        .build();
    let page = Server::list_grants(
        f.ctx.clone(),
        as_alice,
        ListGrantsQuery {
            principal_user: Some(f.alice.clone()),
            principal_role: None,
        },
        no_pagination(),
    )
    .await
    .unwrap();
    // Only her own grant — the self path must not widen the filter to the project.
    assert_eq!(page.grants.len(), 1);
    assert_eq!(page.grants[0].privilege, "get_metadata");
    assert_eq!(page.grants[0].principal, UserOrRole::User(f.alice.clone()));

    // Asking about someone else goes through the gate. Under AllowAll it is permitted,
    // so this pins the filter rather than the denial: bob's grant, not alice's.
    let as_alice = RequestMetadataTestBuilder::builder()
        .actor(Actor::Principal(f.alice.clone()))
        .project_id(Some(f.project_id.clone().into()))
        .build();
    let page = Server::list_grants(
        f.ctx.clone(),
        as_alice,
        ListGrantsQuery {
            principal_user: Some(bob.clone()),
            principal_role: None,
        },
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants.len(), 1);
    assert_eq!(page.grants[0].principal, UserOrRole::User(bob));
}

/// Naming both a user and a role is ambiguous, not a merge.
#[sqlx::test]
async fn naming_two_principals_in_a_listing_is_rejected(pool: PgPool) {
    let f = setup(pool).await;
    let err = Server::list_grants(
        f.ctx.clone(),
        f.metadata.clone(),
        ListGrantsQuery {
            principal_user: Some(f.alice.clone()),
            principal_role: Some(RoleId::new_random()),
        },
        no_pagination(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 400);
    assert_eq!(err.error.r#type, "AmbiguousGrantPrincipal");
}

/// Each tabular kind gets its own path and its own `ResourceType`, so each needs its
/// own round trip: a grant applied through one must come back from that same one,
/// labelled with the right resource.
#[sqlx::test]
async fn apply_and_list_table_grants(pool: PgPool) {
    let f = setup(pool).await;
    let table_id = create_table_returning_id(&f.ctx, f.warehouse_id, "grant_ns", "t1").await;

    Server::apply_table_grants(
        f.warehouse_id,
        table_id,
        f.ctx.clone(),
        f.metadata.clone(),
        writes(vec![entry("get_metadata", &f.alice)]),
    )
    .await
    .unwrap();

    let page = Server::list_table_grants(
        f.warehouse_id,
        table_id,
        f.ctx.clone(),
        f.metadata.clone(),
        ListGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants.len(), 1);
    assert_eq!(page.grants[0].privilege, "get_metadata");
    assert_eq!(
        page.grants[0].resource,
        GrantResourceResponse::Table {
            warehouse_id: f.warehouse_id,
            table_id
        }
    );

    // The warehouse listing must not report it: grants do not inherit, and the
    // resource listing is the direct layer only.
    let warehouse_page = Server::list_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        ListGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(warehouse_page.grants, Vec::new());

    Server::apply_table_grants(
        f.warehouse_id,
        table_id,
        f.ctx.clone(),
        f.metadata.clone(),
        deletes(vec![entry("get_metadata", &f.alice)]),
    )
    .await
    .unwrap();
    let page = Server::list_table_grants(
        f.warehouse_id,
        table_id,
        f.ctx.clone(),
        f.metadata.clone(),
        ListGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants, Vec::new());
}

#[sqlx::test]
async fn apply_and_list_view_grants(pool: PgPool) {
    let f = setup(pool).await;
    create_namespace_in(&f.ctx, f.warehouse_id, "grant_vns").await;
    let loaded = create_view(
        f.ctx.clone(),
        &f.warehouse_id.to_string(),
        "grant_vns",
        "v1",
        None,
    )
    .await
    .unwrap();
    let view_id: lakekeeper::service::ViewId = loaded.metadata.uuid().into();

    Server::apply_view_grants(
        f.warehouse_id,
        view_id,
        f.ctx.clone(),
        f.metadata.clone(),
        writes(vec![entry("get_metadata", &f.alice)]),
    )
    .await
    .unwrap();

    let page = Server::list_view_grants(
        f.warehouse_id,
        view_id,
        f.ctx.clone(),
        f.metadata.clone(),
        ListGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants.len(), 1);
    assert_eq!(
        page.grants[0].resource,
        GrantResourceResponse::View {
            warehouse_id: f.warehouse_id,
            view_id
        }
    );
}

/// A table's id must not resolve as a view. This is refused at the API layer, before
/// the store's own kind guard is reached — so it pins the masked not-found, not the
/// store behaviour (that is `a_grant_on_a_tabular_takes_the_tabulars_own_kind`).
#[sqlx::test]
async fn a_table_id_does_not_resolve_as_a_view(pool: PgPool) {
    let f = setup(pool).await;
    let table_id = create_table_returning_id(&f.ctx, f.warehouse_id, "grant_ns", "t1").await;
    Server::apply_table_grants(
        f.warehouse_id,
        table_id,
        f.ctx.clone(),
        f.metadata.clone(),
        writes(vec![entry("get_metadata", &f.alice)]),
    )
    .await
    .unwrap();

    // Same uuid, addressed as a view.
    let as_view: lakekeeper::service::ViewId = (*table_id).into();
    let err = Server::list_view_grants(
        f.warehouse_id,
        as_view,
        f.ctx.clone(),
        f.metadata.clone(),
        ListGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 404);
    // Pinned so an unrelated 404 (missing warehouse, missing namespace) cannot pass.
    assert_eq!(err.error.r#type, "NoSuchViewException");

    // Writing through the wrong path is refused the same way.
    let err = Server::apply_view_grants(
        f.warehouse_id,
        as_view,
        f.ctx.clone(),
        f.metadata.clone(),
        writes(vec![entry("get_metadata", &f.alice)]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 404);
}

#[sqlx::test]
async fn apply_list_and_revoke_warehouse_grants(pool: PgPool) {
    let f = setup(pool).await;

    Server::apply_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        writes(vec![entry("get_metadata", &f.alice)]),
    )
    .await
    .unwrap();

    let page = Server::list_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        ListGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants.len(), 1);
    let grant = &page.grants[0];
    assert_eq!(grant.privilege, "get_metadata");
    assert_eq!(grant.principal, UserOrRole::User(f.alice.clone()));
    assert_eq!(
        grant.resource,
        GrantResourceResponse::Warehouse {
            warehouse_id: f.warehouse_id
        }
    );
    assert!(grant.recognized);
    let created_at = grant
        .created_at
        .expect("the catalog store records a creation time");

    // Re-applying is idempotent. Asserted against state rather than a reported delta:
    // still exactly one grant, and an unchanged `created_at` proves the second apply
    // did not rewrite the row rather than merely ending in the same shape.
    Server::apply_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        writes(vec![entry("get_metadata", &f.alice)]),
    )
    .await
    .unwrap();
    let page = Server::list_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        ListGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants.len(), 1);
    assert_eq!(page.grants[0].created_at, Some(created_at));

    // Revoking removes it; revoking again is not an error.
    for _ in 0..2 {
        Server::apply_warehouse_grants(
            f.warehouse_id,
            f.ctx.clone(),
            f.metadata.clone(),
            deletes(vec![entry("get_metadata", &f.alice)]),
        )
        .await
        .unwrap();
    }
    let page = Server::list_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        ListGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants, Vec::new());
}

#[sqlx::test]
async fn a_privilege_outside_the_vocabulary_is_rejected_on_write(pool: PgPool) {
    let f = setup(pool).await;
    let err = Server::apply_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        writes(vec![entry("not_a_privilege", &f.alice)]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 400);
    assert_eq!(err.error.r#type, "InvalidGrantPrivilege");
}

/// `read_grants` gates reading grants; it is not itself something to hand out.
#[sqlx::test]
async fn the_grant_read_gate_is_not_a_grantable_privilege(pool: PgPool) {
    let f = setup(pool).await;
    let err = Server::apply_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        writes(vec![entry("read_grants", &f.alice)]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 400);
    assert_eq!(err.error.r#type, "InvalidGrantPrivilege");
}

/// The vocabulary must be discoverable: a privilege name is authorizer-specific and a
/// wrong guess is a 400, so every resource type reports its grantable privileges. The
/// read gate is enforceable but never grantable, so it must not appear.
#[sqlx::test]
async fn grantable_privileges_publishes_the_whole_vocabulary(pool: PgPool) {
    let f = setup(pool).await;
    let published = Server::get_grantable_privileges(f.ctx.clone(), f.metadata.clone())
        .await
        .unwrap();

    // Every resource type is keyed, so "nothing grantable here" is distinguishable
    // from "unknown resource type".
    let mut keys: Vec<&str> = published.privileges.keys().copied().collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec![
            "generic-table",
            "namespace",
            "project",
            "server",
            "table",
            "tag-definition",
            "view",
            "warehouse",
        ]
    );

    let warehouse: Vec<&str> = published.privileges["warehouse"]
        .iter()
        .map(|p| p.name.as_str())
        .collect();
    assert!(
        warehouse.contains(&"get_metadata"),
        "warehouse vocabulary must publish `get_metadata`, got {warehouse:?}"
    );
    assert!(
        !warehouse.contains(&"read_grants"),
        "`read_grants` gates reading grants and is not grantable, got {warehouse:?}"
    );
    // Each descriptor is self-describing, so a flattened client keeps the mapping.
    for descriptor in published.privileges["warehouse"] {
        assert_eq!(descriptor.resource_type, ResourceType::Warehouse);
    }
}

/// Discovery must not be stricter than the endpoints it describes: the vocabulary is
/// static configuration, so it needs no project header and no principal.
#[sqlx::test]
async fn grantable_privileges_needs_no_project_or_principal(pool: PgPool) {
    let f = setup(pool).await;
    let published =
        Server::get_grantable_privileges(f.ctx.clone(), RequestMetadata::new_unauthenticated())
            .await
            .unwrap();
    assert_eq!(published.privileges.len(), 8);
}

/// A revoke is never checked against the vocabulary: a privilege that has left it
/// must stay revocable, or its grants would be stuck forever.
#[sqlx::test]
async fn an_unrecognized_privilege_can_still_be_revoked(pool: PgPool) {
    let f = setup(pool).await;
    // Plant a grant the current vocabulary does not accept, as an authorizer switch
    // or a vocabulary change would leave behind.
    sqlx::query(
        "INSERT INTO grant_assignment \
         (principal_type, user_id, resource_type, privilege, warehouse_id) \
         VALUES ('user', $1, 'warehouse', 'legacy_privilege', $2)",
    )
    .bind(f.alice.to_string())
    .bind(*f.warehouse_id)
    .execute(&f.ctx.v1_state.catalog.write_pool())
    .await
    .unwrap();

    let page = Server::list_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        ListGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants.len(), 1);
    assert_eq!(page.grants[0].privilege, "legacy_privilege");
    // Surfaced, but flagged as enforcing nothing.
    assert!(!page.grants[0].recognized);

    Server::apply_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        deletes(vec![entry("legacy_privilege", &f.alice)]),
    )
    .await
    .unwrap();
    let page = Server::list_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        ListGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants, Vec::new());
}

#[sqlx::test]
async fn a_contradictory_diff_is_rejected(pool: PgPool) {
    let f = setup(pool).await;
    let err = Server::apply_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        ApplyGrantsRequest {
            writes: vec![entry("get_metadata", &f.alice)],
            deletes: vec![entry("get_metadata", &f.alice)],
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 400);
    assert_eq!(err.error.r#type, "ContradictoryGrantDiff");
}

#[sqlx::test]
async fn a_repeated_entry_is_rejected(pool: PgPool) {
    let f = setup(pool).await;
    let err = Server::apply_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        writes(vec![
            entry("get_metadata", &f.alice),
            entry("get_metadata", &f.alice),
        ]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 400);
    assert_eq!(err.error.r#type, "DuplicateGrantEntry");
}

#[sqlx::test]
async fn an_empty_diff_is_rejected(pool: PgPool) {
    let f = setup(pool).await;
    let err = Server::apply_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        writes(vec![]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 400);
    assert_eq!(err.error.r#type, "EmptyGrantDiff");
}

#[sqlx::test]
async fn an_oversized_diff_is_rejected(pool: PgPool) {
    let f = setup(pool).await;
    // One past the cap, which matches the largest batch an authorizer-managed store
    // can apply atomically.
    let entries = (0..=100)
        .map(|i| {
            let user = UserId::try_from(format!("oidc~user{i}").as_str()).unwrap();
            entry("get_metadata", &user)
        })
        .collect();
    let err = Server::apply_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        writes(entries),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 400);
    assert_eq!(err.error.r#type, "GrantDiffTooLarge");
}

/// Role ids are global while roles are project-scoped, so a grant to a role from
/// another project must be refused rather than silently stored.
#[sqlx::test]
async fn a_role_outside_the_project_cannot_be_granted(pool: PgPool) {
    let f = setup(pool).await;
    let err = Server::apply_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        writes(vec![GrantEntry {
            privilege: "get_metadata".to_string(),
            principal: UserOrRole::Role(RoleId::new_random().into_api_assignee()),
        }]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 400);
    assert_eq!(err.error.r#type, "GrantRoleNotInProject");
}

#[sqlx::test]
async fn granting_to_an_unprovisioned_user_is_refused(pool: PgPool) {
    let f = setup(pool).await;
    let ghost = UserId::try_from("oidc~ghost").unwrap();
    let err = Server::apply_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        writes(vec![entry("get_metadata", &ghost)]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 400);
    assert_eq!(err.error.r#type, "GrantUserNotFound");
    assert_eq!(err.error.message, "User `oidc~ghost` does not exist");
}

/// A warehouse id from another project must read as absent — never as a denial that
/// would confirm the warehouse exists.
#[sqlx::test]
async fn a_warehouse_in_another_project_is_not_found(pool: PgPool) {
    let f = setup(pool).await;
    let other_project = RequestMetadataTestBuilder::builder()
        .project_id(Some(std::sync::Arc::new(ProjectId::new_random())))
        .build();

    let err = Server::list_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        other_project.clone(),
        ListGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 404);

    let err = Server::apply_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        other_project,
        writes(vec![entry("get_metadata", &f.alice)]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 404);
}

/// A per-resource listing narrowed to one principal returns that principal's grants and
/// no one else's, for a user and for a role. Narrowing has to happen in the store rather
/// than after paging, or a page could come back empty while a continuation token still
/// promised more.
#[sqlx::test]
async fn a_resource_listing_narrows_to_one_principal(pool: PgPool) {
    let f = setup(pool).await;
    let bob = UserId::try_from("oidc~bob").unwrap();
    provision_user(&f.ctx, &bob).await;
    let role_id = create_role_in_project(&f.ctx, &f.metadata).await;

    Server::apply_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        writes(vec![
            entry("get_metadata", &f.alice),
            entry("list_namespaces", &bob),
            GrantEntry {
                privilege: "use".to_string(),
                principal: UserOrRole::Role(role_id.into_api_assignee()),
            },
        ]),
    )
    .await
    .unwrap();

    let listed = async |query: ListGrantsQuery| {
        let page = Server::list_warehouse_grants(
            f.warehouse_id,
            f.ctx.clone(),
            f.metadata.clone(),
            query,
            no_pagination(),
        )
        .await
        .unwrap();
        page.grants
            .into_iter()
            .map(|g| (g.principal, g.privilege))
            .collect::<Vec<_>>()
    };

    assert_eq!(
        listed(ListGrantsQuery {
            principal_user: Some(f.alice.clone()),
            principal_role: None,
        })
        .await,
        vec![(
            UserOrRole::User(f.alice.clone()),
            "get_metadata".to_string()
        )]
    );
    assert_eq!(
        listed(ListGrantsQuery {
            principal_user: None,
            principal_role: Some(role_id),
        })
        .await,
        vec![(
            UserOrRole::Role(role_id.into_api_assignee()),
            "use".to_string()
        )]
    );
    assert_eq!(listed(ListGrantsQuery::default()).await.len(), 3);
}

/// Naming both principals is refused on a per-resource listing with the same error the
/// rest of the grant surface uses, rather than one of them silently winning.
#[sqlx::test]
async fn a_resource_listing_rejects_two_principals(pool: PgPool) {
    let f = setup(pool).await;
    let role_id = create_role_in_project(&f.ctx, &f.metadata).await;

    let err = Server::list_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        ListGrantsQuery {
            principal_user: Some(f.alice.clone()),
            principal_role: Some(role_id),
        },
        no_pagination(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 400);
    assert_eq!(err.error.r#type, "AmbiguousGrantPrincipal");
}

// ---------------------------------------------------------------------------
// The same handlers under an authorizer that refuses
// ---------------------------------------------------------------------------
//
// Everything above runs under `AllowAllAuthorizer`, which proves the surface works
// but never that a gate closes. These use `HidingAuthorizer`, whose per-action and
// per-object denials are switchable, so each test pins the *difference* the gate
// makes: same request, same data, one denial toggled.
//
// `HidingAuthorizer` deliberately opts out of the grant surface, so its
// `grantable_privileges` is empty and `are_allowed_grants` denies — which is itself
// the production posture of any authorizer that has not implemented grants.

type DenyCtx = ApiContext<State<HidingAuthorizer, PostgresBackend, SecretsState>>;
type DenyServer = ApiServer<PostgresBackend, HidingAuthorizer, SecretsState>;

struct DenyFixture {
    ctx: DenyCtx,
    authorizer: HidingAuthorizer,
    warehouse_id: lakekeeper::WarehouseId,
    project_id: ProjectId,
    alice: UserId,
    bob: UserId,
}

/// Sets up under `HidingAuthorizer` and seeds one grant per user directly through the
/// store: the apply API is unusable here (empty vocabulary), and seeding through the
/// store is what lets a read gate be tested against real rows.
async fn setup_denying(pool: PgPool) -> DenyFixture {
    let authorizer = HidingAuthorizer::new();
    let (ctx, warehouse) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(authorizer.clone())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let alice = UserId::try_from("oidc~alice").unwrap();
    let bob = UserId::try_from("oidc~bob").unwrap();
    provision_user(&ctx, &alice).await;
    provision_user(&ctx, &bob).await;

    let resource = GrantResource::Warehouse(warehouse.warehouse_id);
    let seed = |principal: UserOrRoleId, privilege: &str| GrantSpec {
        principal,
        resource: resource.clone(),
        privilege: privilege.to_string(),
    };
    PostgresBackend::apply_grants(
        &[
            seed(UserOrRoleId::User(alice.clone()), "get_metadata"),
            seed(UserOrRoleId::User(bob.clone()), "list_namespaces"),
        ],
        &[],
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();

    DenyFixture {
        ctx,
        authorizer,
        warehouse_id: warehouse.warehouse_id,
        project_id: (*warehouse.project_id).clone(),
        alice,
        bob,
    }
}

fn as_principal(user: &UserId, project_id: &ProjectId) -> RequestMetadata {
    RequestMetadataTestBuilder::builder()
        .actor(Actor::Principal(user.clone()))
        .project_id(Some(project_id.clone().into()))
        .build()
}

/// Blocking `read_grants` on the warehouse must turn the listing into a denial — not
/// into a listing that quietly returns the rows anyway.
#[sqlx::test]
async fn blocking_read_grants_denies_the_warehouse_listing(pool: PgPool) {
    let f = setup_denying(pool).await;
    let metadata = as_principal(&f.alice, &f.project_id);

    // Control: the gate is open, so the seeded rows are visible.
    let page = DenyServer::list_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        metadata.clone(),
        ListGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants.len(), 2);

    f.authorizer.block_action("warehouse:ReadGrants");

    let err = DenyServer::list_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        metadata,
        ListGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 403);
    assert_eq!(err.error.r#type, "WarehouseActionForbidden");
    assert_eq!(
        err.error.message,
        format!(
            "Warehouse action `read_grants` forbidden on warehouse `{}`",
            f.warehouse_id
        )
    );
}

/// A warehouse the caller cannot see must read as absent, so the response never
/// confirms that the warehouse exists. Note this is a *different* answer from the
/// blocked-action case above: invisible is 404, visible-but-forbidden is 403.
#[sqlx::test]
async fn an_invisible_warehouse_hides_its_grants(pool: PgPool) {
    let f = setup_denying(pool).await;
    f.authorizer.hide(&format!("warehouse:{}", f.warehouse_id));

    let err = DenyServer::list_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        as_principal(&f.alice, &f.project_id),
        ListGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 404);
    assert_eq!(err.error.r#type, "NoSuchWarehouseException");
}

/// The self-read allowance: asking about yourself skips the project gate, asking
/// about anyone else does not. Both halves matter — an allowance that never applies
/// is dead code, one that applies too broadly leaks another principal's access.
#[sqlx::test]
async fn your_own_grants_need_no_authority_but_another_principals_do(pool: PgPool) {
    let f = setup_denying(pool).await;
    f.authorizer.block_action("project:ReadGrants");

    let page = DenyServer::list_grants(
        f.ctx.clone(),
        as_principal(&f.alice, &f.project_id),
        ListGrantsQuery {
            principal_user: Some(f.alice.clone()),
            principal_role: None,
        },
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants.len(), 1);
    assert_eq!(page.grants[0].privilege, "get_metadata");
    assert_eq!(page.grants[0].principal, UserOrRole::User(f.alice.clone()));

    let err = DenyServer::list_grants(
        f.ctx.clone(),
        as_principal(&f.alice, &f.project_id),
        ListGrantsQuery {
            principal_user: Some(f.bob.clone()),
            principal_role: None,
        },
        no_pagination(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 403);
    assert_eq!(err.error.r#type, "ProjectActionForbidden");

    // Naming nobody is refused outright rather than falling back to the gate, so a
    // caller who omits the principal never learns whether they would have been allowed.
    let err = DenyServer::list_grants(
        f.ctx.clone(),
        as_principal(&f.alice, &f.project_id),
        ListGrantsQuery {
            principal_user: None,
            principal_role: None,
        },
        no_pagination(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 400);
    assert_eq!(err.error.r#type, "MissingGrantPrincipal");
}

/// The same self-read allowance on a per-resource listing: a user with no grant-read
/// authority on the warehouse can still see their own grants there, which is what lets a
/// console show someone what they hold and what they must go and ask for.
#[sqlx::test]
async fn your_own_grants_on_a_resource_need_no_grant_read_authority(pool: PgPool) {
    let f = setup_denying(pool).await;
    f.authorizer.block_action("warehouse:ReadGrants");

    let page = DenyServer::list_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        as_principal(&f.alice, &f.project_id),
        ListGrantsQuery {
            principal_user: Some(f.alice.clone()),
            principal_role: None,
        },
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants.len(), 1);
    assert_eq!(page.grants[0].privilege, "get_metadata");
    assert_eq!(page.grants[0].principal, UserOrRole::User(f.alice.clone()));

    // Another principal is not self, so the gate applies.
    let err = DenyServer::list_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        as_principal(&f.alice, &f.project_id),
        ListGrantsQuery {
            principal_user: Some(f.bob.clone()),
            principal_role: None,
        },
        no_pagination(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 403);
    assert_eq!(err.error.r#type, "WarehouseActionForbidden");
}

/// Narrowing to a role is never a self-read, even for a member. A grant a role holds is
/// the role's, so reading it needs the same authority as reading anyone else's. The role
/// need not exist for this to hold: the gate runs before the principal is looked at.
#[sqlx::test]
async fn narrowing_to_a_role_is_never_a_self_read(pool: PgPool) {
    let f = setup_denying(pool).await;
    f.authorizer.block_action("warehouse:ReadGrants");

    let err = DenyServer::list_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        as_principal(&f.alice, &f.project_id),
        ListGrantsQuery {
            principal_user: None,
            principal_role: Some(RoleId::new_random()),
        },
        no_pagination(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 403);
    assert_eq!(err.error.r#type, "WarehouseActionForbidden");
}

/// The self-read allowance is not ungated: it still requires being allowed to see the
/// resource, so a warehouse the caller cannot see stays absent even when they only ask
/// about their own grants on it. Without this the endpoint would confirm that any
/// warehouse id exists.
#[sqlx::test]
async fn a_self_listing_still_requires_seeing_the_resource(pool: PgPool) {
    let f = setup_denying(pool).await;
    let self_query = || ListGrantsQuery {
        principal_user: Some(f.alice.clone()),
        principal_role: None,
    };

    // Blocking the can-see action alone is enough to refuse the self listing.
    f.authorizer.block_action("warehouse:IncludeInList");
    let err = DenyServer::list_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        as_principal(&f.alice, &f.project_id),
        self_query(),
        no_pagination(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 403);
    assert_eq!(err.error.r#type, "WarehouseActionForbidden");
    assert_eq!(
        err.error.message,
        format!(
            "Warehouse action `include_in_list` forbidden on warehouse `{}`",
            f.warehouse_id
        )
    );

    // A hidden warehouse reads as absent rather than forbidden, as on every other path.
    f.authorizer.hide(&format!("warehouse:{}", f.warehouse_id));
    let err = DenyServer::list_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        as_principal(&f.alice, &f.project_id),
        self_query(),
        no_pagination(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 404);
    assert_eq!(err.error.r#type, "NoSuchWarehouseException");
}

/// Server grants belong to no project, so the project-scoped listing excludes them and
/// the server listing is the only way to reach them. Reading your own must therefore work
/// without server-level authority, or nobody could ever see their own server grants.
#[sqlx::test]
async fn your_own_server_grants_are_readable_without_authority(pool: PgPool) {
    let f = setup_denying(pool).await;
    PostgresBackend::apply_grants(
        &[
            GrantSpec {
                principal: UserOrRoleId::User(f.alice.clone()),
                resource: GrantResource::Server,
                privilege: "operator".to_string(),
            },
            GrantSpec {
                principal: UserOrRoleId::User(f.bob.clone()),
                resource: GrantResource::Server,
                privilege: "admin".to_string(),
            },
        ],
        &[],
        f.ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    f.authorizer.block_action("server:ReadGrants");

    let page = DenyServer::list_server_grants(
        f.ctx.clone(),
        as_principal(&f.alice, &f.project_id),
        ListGrantsQuery {
            principal_user: Some(f.alice.clone()),
            principal_role: None,
        },
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants.len(), 1);
    assert_eq!(page.grants[0].privilege, "operator");

    // The project-scoped listing still excludes them, so this really is the only route.
    let page = DenyServer::list_grants(
        f.ctx.clone(),
        as_principal(&f.alice, &f.project_id),
        ListGrantsQuery {
            principal_user: Some(f.alice.clone()),
            principal_role: None,
        },
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(
        page.grants
            .iter()
            .map(|g| g.privilege.as_str())
            .collect::<Vec<_>>(),
        vec!["get_metadata"]
    );

    let err = DenyServer::list_server_grants(
        f.ctx.clone(),
        as_principal(&f.alice, &f.project_id),
        ListGrantsQuery {
            principal_user: Some(f.bob.clone()),
            principal_role: None,
        },
        no_pagination(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 403);
    assert_eq!(err.error.r#type, "ServerActionForbidden");
}

/// An authorizer that has not implemented grants publishes no vocabulary, so every
/// write is refused before it reaches the store. Revocations stay possible: a
/// privilege that has left the vocabulary must not become unrevokable.
#[sqlx::test]
async fn an_authorizer_without_a_vocabulary_refuses_writes_but_allows_revokes(pool: PgPool) {
    let f = setup_denying(pool).await;
    let metadata = as_principal(&f.alice, &f.project_id);

    let err = DenyServer::apply_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        metadata.clone(),
        writes(vec![entry("get_metadata", &f.alice)]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 400);
    assert_eq!(err.error.r#type, "InvalidGrantPrivilege");

    // Nothing was written: the two seeded rows are still the only ones.
    let stored = PostgresBackend::list_grants(
        &GrantFilter::on(GrantResource::Warehouse(f.warehouse_id), None),
        no_pagination(),
        f.ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    assert_eq!(stored.grants.len(), 2);

    // A revoke of the same privilege is not validated against the vocabulary, so it
    // reaches the authority gate — which denies, because this authorizer confers none.
    let err = DenyServer::apply_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        metadata,
        deletes(vec![entry("get_metadata", &f.alice)]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 403);
    assert_eq!(err.error.r#type, "GrantActionForbidden");
    assert_eq!(
        err.error.message,
        "Granting or revoking `get_metadata` on this warehouse is forbidden"
    );
}

/// Deactivating a warehouse hides its children's grants but not its own, and the
/// project roll-up keeps reporting both. That split is inherited, not chosen here:
/// warehouse-level resolution admits inactive warehouses (as tag targets do), while
/// the shared namespace/tabular resolution helpers require an active one, and no
/// roll-up in the repo filters on warehouse status. Pinned so that changing it is a
/// deliberate, repo-wide decision rather than a silent drift in this surface.
#[sqlx::test]
async fn deactivating_a_warehouse_hides_child_grants_but_not_its_own(pool: PgPool) {
    let f = setup(pool).await;
    let table_id = create_table_returning_id(&f.ctx, f.warehouse_id, "grant_deact", "t1").await;
    Server::apply_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        writes(vec![entry("get_metadata", &f.alice)]),
    )
    .await
    .unwrap();
    Server::apply_table_grants(
        f.warehouse_id,
        table_id,
        f.ctx.clone(),
        f.metadata.clone(),
        writes(vec![entry("get_metadata", &f.alice)]),
    )
    .await
    .unwrap();

    Server::deactivate_warehouse(f.warehouse_id, f.ctx.clone(), f.metadata.clone())
        .await
        .unwrap();

    // The warehouse's own grants stay readable and revocable — an administrator must
    // not lose the ability to cut off access to a warehouse they just deactivated.
    let page = Server::list_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        ListGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants.len(), 1);
    Server::apply_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        deletes(vec![entry("get_metadata", &f.alice)]),
    )
    .await
    .unwrap();
    // The revoke landed. Read back from the warehouse's own listing, which stays
    // reachable while deactivated — only its children's grants become invisible.
    let page = Server::list_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        ListGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants.len(), 0);

    // The table's grants are not reachable: the shared tabular resolution requires an
    // active warehouse, so the table reads as absent.
    let err = Server::list_table_grants(
        f.warehouse_id,
        table_id,
        f.ctx.clone(),
        f.metadata.clone(),
        ListGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 404);
    assert_eq!(err.error.r#type, "NoSuchWarehouseException");

    // The roll-up still reports the table grant, which is how an administrator can
    // see that it survives the deactivation.
    let page = Server::list_grants(
        f.ctx.clone(),
        f.metadata.clone(),
        for_user(&f.alice),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants.len(), 1);
    assert_eq!(
        page.grants[0].resource,
        GrantResourceResponse::Table {
            warehouse_id: f.warehouse_id,
            table_id
        }
    );
}

// ---------------------------------------------------------------------------
// The router actually mounts this surface
// ---------------------------------------------------------------------------

/// Every test above calls a handler directly, which skips the router: a path that
/// collides with another route, a `Path` tuple whose ids are transposed, or a query
/// extractor that cannot deserialize would pass all of them and then fail at server
/// boot or on the first request. This drives real HTTP through the built router, with
/// the metadata extension the auth middleware normally attaches.
///
/// Each request is a revoke of a grant nobody holds. Revokes are deliberately not
/// validated against the authorizer's vocabulary, so one body works at every level and
/// a 200 means the route resolved, the ids parsed, and the handler ran.
#[sqlx::test]
async fn every_grant_route_is_reachable_through_the_router(pool: PgPool) {
    use lakekeeper::{
        api::endpoints::ManagementV1Endpoint,
        axum::{
            Router,
            body::Body,
            http::{Request, StatusCode},
        },
    };
    use strum::IntoEnumIterator as _;
    use tower::ServiceExt as _;

    let f = setup(pool).await;

    // One real resource per level: a route that resolves must reach a 200, so a
    // not-found can only mean the path is not mounted.
    let table_id = create_table_returning_id(&f.ctx, f.warehouse_id, "grant_routes", "t1").await;
    let namespace_id = PostgresBackend::get_namespace(
        f.warehouse_id,
        NamespaceIdent::new("grant_routes".to_string()),
        f.ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap()
    .namespace_id();
    let view_id: lakekeeper::service::ViewId = create_view(
        f.ctx.clone(),
        &f.warehouse_id.to_string(),
        "grant_routes",
        "v1",
        None,
    )
    .await
    .unwrap()
    .metadata
    .uuid()
    .into();
    create_generic_table(
        f.ctx.clone(),
        f.warehouse_id.to_string(),
        "grant_routes",
        "gt1",
    )
    .await
    .unwrap();
    let generic_table_id = CatalogServer::list_generic_tables(
        NamespaceParameters {
            prefix: Some(f.warehouse_id.to_string().into()),
            namespace: NamespaceIdent::new("grant_routes".to_string()),
        },
        ListGenericTablesQuery::default(),
        f.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap()
    .identifiers
    .iter()
    .find(|i| i.name == "gt1")
    .and_then(|i| i.id)
    .unwrap();
    let tag_definition_id = Server::create_tag_definition(
        CreateTagDefinitionRequest::builder()
            .name("routes".to_string())
            .scope(vec![TagScope::Table])
            .value_kind(TagValueKind::Marker)
            .allowed_values(None)
            .build(),
        f.ctx.clone(),
        f.metadata.clone(),
    )
    .await
    .unwrap()
    .id;

    let router: Router = Router::new()
        .nest(
            "/management/v1",
            Server::new_v1_router(&f.ctx.v1_state.authz),
        )
        .with_state(f.ctx.clone());

    let revoke = serde_json::to_string(&deletes(vec![entry("get_metadata", &f.alice)])).unwrap();
    let mut visited = 0;
    for endpoint in ManagementV1Endpoint::iter().filter(|e| e.path().contains("/grants")) {
        let path = endpoint
            .path()
            .replace("{warehouse_id}", &f.warehouse_id.to_string())
            .replace("{namespace_id}", &namespace_id.to_string())
            .replace("{table_id}", &table_id.to_string())
            .replace("{view_id}", &view_id.to_string())
            .replace("{generic_table_id}", &generic_table_id.to_string())
            .replace("{tag_definition_id}", &tag_definition_id.to_string());
        // The project-scoped listing requires a principal, so the request that proves
        // its route resolves has to name one. Every other path answers without a query.
        let path = if matches!(endpoint, ManagementV1Endpoint::ListGrants) {
            format!("{path}?principalUser={}", f.alice)
        } else {
            path
        };
        let body = if endpoint.method() == http::Method::POST {
            Body::from(revoke.clone())
        } else {
            Body::empty()
        };
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(endpoint.method())
                    .uri(&path)
                    .header("content-type", "application/json")
                    .extension(f.metadata.clone())
                    .body(body)
                    .unwrap(),
            )
            .await
            .unwrap();
        // An apply reports no delta, so it answers `204`; the listings and the
        // vocabulary endpoints return a body.
        let expected = if endpoint.method() == http::Method::POST {
            StatusCode::NO_CONTENT
        } else {
            StatusCode::OK
        };
        assert_eq!(
            response.status(),
            expected,
            "{} {path} did not reach its handler",
            endpoint.method()
        );
        visited += 1;
    }
    // Two per resource level across eight levels, plus one `grantable-privileges` each,
    // plus the project-wide listing and the deployment vocabulary.
    assert_eq!(visited, 26);
}

/// The per-resource vocabulary answers "what may I grant *here*", which the
/// deployment-wide one cannot: grant authority is a right of its own, invisible to
/// action introspection. Every entry of the level's vocabulary is returned, marked —
/// a picker must be able to show what it cannot offer.
#[sqlx::test]
async fn grantable_privileges_mark_the_whole_vocabulary(pool: PgPool) {
    let f = setup(pool).await;
    let table_id = create_table_returning_id(&f.ctx, f.warehouse_id, "grant_avail", "t1").await;

    let warehouse = Server::get_warehouse_grantable_privileges(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        no_principal(),
    )
    .await
    .unwrap();
    // Under AllowAll every privilege is grantable, and the list is the level's whole
    // vocabulary minus the grant-read gate, which is an action rather than a privilege.
    let names: Vec<&str> = warehouse
        .privileges
        .iter()
        .map(|p| p.privilege.name.as_str())
        .collect();
    assert!(names.contains(&"get_metadata"), "got {names:?}");
    assert!(!names.contains(&"read_grants"), "got {names:?}");
    assert!(warehouse.privileges.iter().all(|p| p.allowed));
    for grantable in &warehouse.privileges {
        assert_eq!(grantable.privilege.resource_type, ResourceType::Warehouse);
    }
    assert_eq!(
        names.len(),
        Server::get_grantable_privileges(f.ctx.clone(), f.metadata.clone())
            .await
            .unwrap()
            .privileges["warehouse"]
            .len(),
        "the per-resource list is the same vocabulary the deployment publishes"
    );

    // The table level publishes the table vocabulary, not the warehouse's.
    let table = Server::get_table_grantable_privileges(
        f.warehouse_id,
        table_id,
        f.ctx.clone(),
        f.metadata.clone(),
        no_principal(),
    )
    .await
    .unwrap();
    for grantable in &table.privileges {
        assert_eq!(grantable.privilege.resource_type, ResourceType::Table);
    }
    assert_ne!(
        table
            .privileges
            .iter()
            .map(|p| p.privilege.name.clone())
            .collect::<Vec<_>>(),
        warehouse
            .privileges
            .iter()
            .map(|p| p.privilege.name.clone())
            .collect::<Vec<_>>()
    );

    // Every entry is a name the same server accepts on a write — the point of
    // publishing it.
    for grantable in &warehouse.privileges {
        Server::apply_warehouse_grants(
            f.warehouse_id,
            f.ctx.clone(),
            f.metadata.clone(),
            writes(vec![entry(&grantable.privilege.name, &f.alice)]),
        )
        .await
        .unwrap();
    }
}

/// An authorizer that confers no grant authority still publishes the vocabulary it has;
/// every entry simply reports `allowed: false`.
#[sqlx::test]
async fn grantable_privileges_report_not_allowed_rather_than_omitting(pool: PgPool) {
    let f = setup_denying(pool).await;
    let response = DenyServer::get_warehouse_grantable_privileges(
        f.warehouse_id,
        f.ctx.clone(),
        as_principal(&f.alice, &f.project_id),
        no_principal(),
    )
    .await
    .unwrap();
    // HidingAuthorizer opts out of the grant surface entirely, so it publishes no
    // vocabulary to mark — the fail-closed shape of an authorizer with no grants.
    assert_eq!(response.privileges, Vec::new());
}

/// A resource in another project must read as absent, not as a list of unavailable
/// privileges that would confirm it exists.
#[sqlx::test]
async fn grantable_privileges_hide_a_resource_from_another_project(pool: PgPool) {
    let f = setup(pool).await;
    let other_project = RequestMetadataTestBuilder::builder()
        .project_id(Some(std::sync::Arc::new(ProjectId::new_random())))
        .build();
    let err = Server::get_warehouse_grantable_privileges(
        f.warehouse_id,
        f.ctx.clone(),
        other_project,
        no_principal(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 404);
}

/// Naming both principal parameters is ambiguous here for the same reason it is on the
/// listing, and reports the same error type.
#[sqlx::test]
async fn grantable_privileges_reject_two_principals(pool: PgPool) {
    let f = setup(pool).await;
    let err = Server::get_warehouse_grantable_privileges(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        GetGrantAccessQuery {
            principal_user: Some(f.alice.clone()),
            principal_role: Some(RoleId::new_random()),
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 400);
    assert_eq!(err.error.r#type, "AmbiguousGrantPrincipal");
}

fn no_principal() -> GetGrantAccessQuery {
    GetGrantAccessQuery::default()
}

/// The seam between writing a grant through the API and reading it back at decision
/// time. `list_grants_on_resources` is what an authorizer that resolves inheritance
/// itself calls, and it is the only grant operation with no other caller in tree — so
/// nothing else proves that a committed grant is visible to the next decision, at every
/// level of a hierarchy built the way a real one is.
///
/// Also pins the two properties the fetch's contract turns on: nothing is implied —
/// server grants come back because the chain names the server root — and a role's
/// grants come back only when the caller includes that role in the effective set.
#[sqlx::test]
async fn a_committed_grant_is_visible_to_the_evaluation_fetch(pool: PgPool) {
    let f = setup(pool).await;
    let role_id = create_role_in_project(&f.ctx, &f.metadata).await;
    let table_id = create_table_returning_id(&f.ctx, f.warehouse_id, "seam_ns", "seam_tbl").await;
    let namespace_id = PostgresBackend::get_namespace(
        f.warehouse_id,
        NamespaceIdent::new("seam_ns".to_string()),
        f.ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap()
    .namespace_id();

    // One grant per level, written through the API exactly as a client would.
    Server::apply_server_grants(
        f.ctx.clone(),
        f.metadata.clone(),
        writes(vec![entry("create_project", &f.alice)]),
    )
    .await
    .unwrap();
    Server::apply_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        writes(vec![entry("get_metadata", &f.alice)]),
    )
    .await
    .unwrap();
    Server::apply_namespace_grants(
        f.warehouse_id,
        namespace_id,
        f.ctx.clone(),
        f.metadata.clone(),
        writes(vec![entry("get_metadata", &f.alice)]),
    )
    .await
    .unwrap();
    Server::apply_table_grants(
        f.warehouse_id,
        table_id,
        f.ctx.clone(),
        f.metadata.clone(),
        writes(vec![entry("get_metadata", &f.alice)]),
    )
    .await
    .unwrap();
    // Held by the role, not by Alice.
    Server::apply_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        writes(vec![GrantEntry {
            privilege: "list_namespaces".to_string(),
            principal: UserOrRole::Role(role_id.into_api_assignee()),
        }]),
    )
    .await
    .unwrap();

    let sorted = |mut rows: Vec<(UserOrRoleId, GrantResource, String)>| {
        rows.sort_by_key(|row| format!("{row:?}"));
        rows
    };
    // The chain a decision about the table would resolve, root to leaf.
    let chain = vec![
        GrantResource::Server,
        GrantResource::Project(f.project_id.clone()),
        GrantResource::Warehouse(f.warehouse_id),
        GrantResource::Namespace {
            warehouse_id: f.warehouse_id,
            namespace_id,
        },
        GrantResource::Table {
            warehouse_id: f.warehouse_id,
            table_id,
        },
    ];
    let fetch = async |principals: Vec<UserOrRoleId>| {
        let rows = PostgresBackend::list_grants_on_resources_impl(
            &principals,
            &chain,
            f.ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap();
        sorted(
            rows.into_iter()
                .map(|spec| (spec.principal, spec.resource, spec.privilege))
                .collect(),
        )
    };

    // Alice alone: her four levels, and not the role's warehouse grant.
    let alice = UserOrRoleId::User(f.alice.clone());
    assert_eq!(
        fetch(vec![alice.clone()]).await,
        sorted(vec![
            (
                alice.clone(),
                GrantResource::Server,
                "create_project".to_string()
            ),
            (
                alice.clone(),
                GrantResource::Warehouse(f.warehouse_id),
                "get_metadata".to_string()
            ),
            (
                alice.clone(),
                GrantResource::Namespace {
                    warehouse_id: f.warehouse_id,
                    namespace_id
                },
                "get_metadata".to_string()
            ),
            (
                alice.clone(),
                GrantResource::Table {
                    warehouse_id: f.warehouse_id,
                    table_id
                },
                "get_metadata".to_string()
            ),
        ]),
        "every level committed through the API must be visible to the next decision"
    );

    // The role's grant appears only once the caller names the role in the effective set.
    let role = UserOrRoleId::Role(role_id);
    let with_role = fetch(vec![alice.clone(), role.clone()]).await;
    assert_eq!(
        with_role,
        sorted(vec![
            (
                alice.clone(),
                GrantResource::Server,
                "create_project".to_string()
            ),
            (
                alice.clone(),
                GrantResource::Warehouse(f.warehouse_id),
                "get_metadata".to_string()
            ),
            (
                alice.clone(),
                GrantResource::Namespace {
                    warehouse_id: f.warehouse_id,
                    namespace_id
                },
                "get_metadata".to_string()
            ),
            (
                alice.clone(),
                GrantResource::Table {
                    warehouse_id: f.warehouse_id,
                    table_id
                },
                "get_metadata".to_string()
            ),
            (
                role,
                GrantResource::Warehouse(f.warehouse_id),
                "list_namespaces".to_string()
            ),
        ]),
        "the effective set is the union of the user's grants and their roles'"
    );

    // Revoking through the API is visible to the fetch just as immediately.
    Server::apply_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        deletes(vec![entry("get_metadata", &f.alice)]),
    )
    .await
    .unwrap();
    let after_revoke = fetch(vec![alice.clone()]).await;
    assert_eq!(
        after_revoke.len(),
        3,
        "the revoked warehouse grant must be gone: {after_revoke:?}"
    );
    assert!(
        !after_revoke
            .iter()
            .any(|(_, resource, _)| *resource == GrantResource::Warehouse(f.warehouse_id)),
        "no warehouse-level grant may survive the revoke: {after_revoke:?}"
    );
}

/// Asking which privileges *another* principal may grant discloses that principal's
/// access, so it requires authority to read the resource's grants. This was documented as
/// the requirement and enforced only by the OpenFGA implementation; the endpoint enforces
/// it now, so the catalog arm honours it too.
#[sqlx::test]
async fn grantable_privileges_for_another_principal_need_the_read_gate(pool: PgPool) {
    let f = setup_denying(pool).await;
    f.authorizer.block_action("warehouse:ReadGrants");

    let err = DenyServer::get_warehouse_grantable_privileges(
        f.warehouse_id,
        f.ctx.clone(),
        as_principal(&f.alice, &f.project_id),
        GetGrantAccessQuery {
            principal_user: Some(f.bob.clone()),
            principal_role: None,
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 403);
    assert_eq!(err.error.r#type, "WarehouseActionForbidden");

    // The server level takes a different arm of the gate, with no resolved entity.
    f.authorizer.block_action("server:ReadGrants");
    let err = DenyServer::get_server_grantable_privileges(
        f.ctx.clone(),
        as_principal(&f.alice, &f.project_id),
        GetGrantAccessQuery {
            principal_user: Some(f.bob.clone()),
            principal_role: None,
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 403);
    assert_eq!(err.error.r#type, "ServerActionForbidden");
}

/// Asking about yourself discloses nothing you do not already have, so the gate must not
/// apply — including when you name yourself explicitly rather than leaving it implicit.
#[sqlx::test]
async fn grantable_privileges_for_yourself_need_no_read_gate(pool: PgPool) {
    let f = setup_denying(pool).await;
    f.authorizer.block_action("warehouse:ReadGrants");

    // Implicitly: no principal named at all.
    let implicit = DenyServer::get_warehouse_grantable_privileges(
        f.warehouse_id,
        f.ctx.clone(),
        as_principal(&f.alice, &f.project_id),
        GetGrantAccessQuery::default(),
    )
    .await
    .expect("asking about yourself must not need grant-read authority");
    // `HidingAuthorizer` publishes no grant vocabulary, so the list is empty either way;
    // what is under test is that the request was answered rather than refused.
    assert_eq!(implicit.privileges.len(), 0);

    // Explicitly: naming yourself asks the same question as naming nobody.
    let explicit = DenyServer::get_warehouse_grantable_privileges(
        f.warehouse_id,
        f.ctx.clone(),
        as_principal(&f.alice, &f.project_id),
        GetGrantAccessQuery {
            principal_user: Some(f.alice.clone()),
            principal_role: None,
        },
    )
    .await
    .expect("naming yourself is still a self-read");
    assert_eq!(explicit.privileges.len(), 0);
}
