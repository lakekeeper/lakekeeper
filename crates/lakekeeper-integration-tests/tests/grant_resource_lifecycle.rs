//! Deleting a role or a resource must take the grants on it with it.
//!
//! Every level's grant rows hang off a foreign key with `on delete cascade`, so no
//! grants code runs at delete time — which makes the cleanup easy to break
//! invisibly: a re-declared foreign key without the cascade leaves rows behind that
//! nothing lists and nothing revokes. One test per foreign key, each planting a
//! control grant that must survive the delete.
//!
//! The user foreign key is covered separately in `grant_user_lifecycle`: users are
//! soft-deleted, so their cleanup is explicit rather than a cascade.

use iceberg::NamespaceIdent;
use lakekeeper::{
    ProjectId,
    api::{
        ApiContext, RequestMetadata, RequestMetadataTestBuilder,
        data::v1::generic_tables::{GenericTableService as _, ListGenericTablesQuery},
        iceberg::v1::{
            NamespaceParameters, PageToken, PaginationQuery, namespace::NamespaceDropFlags,
        },
        management::v1::{
            ApiServer, DeleteWarehouseQuery,
            grant::{GrantResourceResponse, ListGrantsQuery, Service as _},
            project::{CreateProjectRequest, Service as _},
            role::{CreateRoleRequest, Service as _},
            tag::{CreateTagDefinitionRequest, Service as _},
            user::{UserLastUpdatedWith, UserType},
            warehouse::{Service as _, TabularDeleteProfile, UndropTabularsRequest},
        },
    },
    server::CatalogServer,
    service::{
        CatalogGrantOps as _, CatalogNamespaceOps as _, CatalogStore, NamespaceId, RoleId, State,
        TableId, TabularId, TagScope, TagValueKind, Transaction, UserId, UserUpsertMode,
        authz::{AllowAllAuthorizer, GrantResource, GrantSpec, UserOrRoleId},
    },
};
use lakekeeper_integration_tests::{
    SetupTestCatalog, create_generic_table, create_ns, create_table, create_view, drop_namespace,
    drop_table, memory_io_profile, random_request_metadata,
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
    setup_with_delete_profile(pool, TabularDeleteProfile::Hard {}).await
}

async fn setup_with_delete_profile(pool: PgPool, delete_profile: TabularDeleteProfile) -> Fixture {
    let (ctx, warehouse) = SetupTestCatalog::builder()
        .pool(pool)
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .delete_profile(delete_profile)
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

async fn provision_user(ctx: &Ctx, user_id: &UserId) {
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

/// Planted through the store rather than the management API: the delete path under
/// test is the catalog's, and the store takes the privilege verbatim.
async fn plant(ctx: &Ctx, grants: Vec<GrantSpec>) {
    PostgresBackend::apply_grants(&grants, &[], ctx.v1_state.catalog.clone())
        .await
        .unwrap();
}

fn held_by_user(user: &UserId, resource: GrantResource, privilege: &str) -> GrantSpec {
    GrantSpec {
        principal: UserOrRoleId::User(user.clone()),
        resource,
        privilege: privilege.to_string(),
    }
}

fn held_by_role(role: RoleId, resource: GrantResource, privilege: &str) -> GrantSpec {
    GrantSpec {
        principal: UserOrRoleId::Role(role),
        resource,
        privilege: privilege.to_string(),
    }
}

/// Every grant row left in the catalog, as `(resource_type, privilege)`, sorted. Read
/// straight from the table: a listing is scoped to one resource or one project, and
/// what these tests need to see is what a delete left behind anywhere.
async fn remaining_grants(ctx: &Ctx) -> Vec<(String, String)> {
    sqlx::query_as::<_, (String, String)>(
        "SELECT resource_type::text, privilege FROM grant_assignment \
         ORDER BY resource_type::text, privilege",
    )
    .fetch_all(&ctx.v1_state.catalog.write_pool())
    .await
    .unwrap()
}

fn rows(rows: &[(&str, &str)]) -> Vec<(String, String)> {
    rows.iter()
        .map(|(resource, privilege)| ((*resource).to_string(), (*privilege).to_string()))
        .collect()
}

async fn namespace_id(f: &Fixture, name: &str) -> NamespaceId {
    PostgresBackend::get_namespace(
        f.warehouse_id,
        NamespaceIdent::new(name.to_string()),
        f.ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap()
    .namespace_id()
}

async fn create_table_returning_id(f: &Fixture, ns: &str, name: &str) -> TableId {
    create_table(f.ctx.clone(), f.warehouse_id.to_string(), ns, name, false)
        .await
        .unwrap()
        .metadata
        .uuid()
        .into()
}

/// Generic tables carry no id on their load response; the listing is where it surfaces.
async fn generic_table_id(
    f: &Fixture,
    ns: &str,
    name: &str,
) -> lakekeeper::service::GenericTableId {
    CatalogServer::list_generic_tables(
        NamespaceParameters {
            prefix: Some(f.warehouse_id.to_string().into()),
            namespace: NamespaceIdent::new(ns.to_string()),
        },
        ListGenericTablesQuery::default(),
        f.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap()
    .identifiers
    .iter()
    .find(|i| i.name == name)
    .and_then(|i| i.id)
    .unwrap()
}

fn no_pagination() -> PaginationQuery {
    PaginationQuery::new(PageToken::Empty, None)
}

#[sqlx::test]
async fn deleting_a_role_takes_its_grants_with_it(pool: PgPool) {
    let f = setup(pool).await;
    let role_id = Server::create_role(
        CreateRoleRequest {
            name: "doomed".to_string(),
            description: None,
            project_id: None,
            provider_id: None,
            source_id: None,
        },
        f.ctx.clone(),
        f.metadata.clone(),
    )
    .await
    .unwrap()
    .id;

    plant(
        &f.ctx,
        vec![
            held_by_role(
                role_id,
                GrantResource::Warehouse(f.warehouse_id),
                "get_metadata",
            ),
            // Control: a grant on the same resource, held by someone else.
            held_by_user(
                &f.alice,
                GrantResource::Warehouse(f.warehouse_id),
                "list_namespaces",
            ),
        ],
    )
    .await;

    Server::delete_role(f.ctx.clone(), f.metadata.clone(), role_id)
        .await
        .unwrap();

    assert_eq!(
        remaining_grants(&f.ctx).await,
        rows(&[("warehouse", "list_namespaces")])
    );
}

#[sqlx::test]
async fn deleting_a_project_takes_its_grants_with_it(pool: PgPool) {
    let f = setup(pool).await;
    // A second, warehouse-free project: a project holding warehouses cannot be deleted.
    let doomed = Server::create_project(
        CreateProjectRequest {
            project_name: "doomed".to_string(),
            project_id: None,
        },
        f.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap()
    .project_id;

    plant(
        &f.ctx,
        vec![
            held_by_user(
                &f.alice,
                GrantResource::Project((*doomed).clone()),
                "get_metadata",
            ),
            // Control: the same principal's grant on the project that stays.
            held_by_user(
                &f.alice,
                GrantResource::Project(f.project_id.clone()),
                "create_warehouse",
            ),
        ],
    )
    .await;

    Server::delete_project(
        Some((*doomed).clone()),
        f.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    assert_eq!(
        remaining_grants(&f.ctx).await,
        rows(&[("project", "create_warehouse")])
    );
}

#[sqlx::test]
async fn deleting_a_warehouse_takes_its_grants_with_it(pool: PgPool) {
    let f = setup(pool).await;
    plant(
        &f.ctx,
        vec![
            held_by_user(
                &f.alice,
                GrantResource::Warehouse(f.warehouse_id),
                "get_metadata",
            ),
            // Control: the project above it keeps its own grants.
            held_by_user(
                &f.alice,
                GrantResource::Project(f.project_id.clone()),
                "create_warehouse",
            ),
        ],
    )
    .await;

    Server::delete_warehouse(
        f.warehouse_id,
        DeleteWarehouseQuery::builder().build(),
        f.ctx.clone(),
        f.metadata.clone(),
    )
    .await
    .unwrap();

    assert_eq!(
        remaining_grants(&f.ctx).await,
        rows(&[("project", "create_warehouse")])
    );
}

#[sqlx::test]
async fn dropping_a_namespace_takes_its_grants_with_it(pool: PgPool) {
    let f = setup(pool).await;
    create_ns(
        f.ctx.clone(),
        f.warehouse_id.to_string(),
        "doomed".to_string(),
    )
    .await;
    let doomed = namespace_id(&f, "doomed").await;
    create_ns(
        f.ctx.clone(),
        f.warehouse_id.to_string(),
        "keeper".to_string(),
    )
    .await;
    let keeper = namespace_id(&f, "keeper").await;

    plant(
        &f.ctx,
        vec![
            held_by_user(
                &f.alice,
                GrantResource::Namespace {
                    warehouse_id: f.warehouse_id,
                    namespace_id: doomed,
                },
                "get_metadata",
            ),
            // Control: a sibling namespace's grant, which shares the composite key's
            // warehouse column.
            held_by_user(
                &f.alice,
                GrantResource::Namespace {
                    warehouse_id: f.warehouse_id,
                    namespace_id: keeper,
                },
                "list_tables",
            ),
        ],
    )
    .await;

    drop_namespace(
        f.ctx.clone(),
        NamespaceDropFlags::default(),
        NamespaceParameters {
            prefix: Some(f.warehouse_id.to_string().into()),
            namespace: NamespaceIdent::new("doomed".to_string()),
        },
    )
    .await
    .unwrap();

    assert_eq!(
        remaining_grants(&f.ctx).await,
        rows(&[("namespace", "list_tables")])
    );
}

/// A recursive drop is the only path that removes several levels at once, and the
/// only one that reaches all three tabular kinds through one delete.
#[sqlx::test]
async fn dropping_a_namespace_recursively_takes_every_grant_below_it(pool: PgPool) {
    let f = setup(pool).await;
    let prefix = f.warehouse_id.to_string();
    create_ns(f.ctx.clone(), prefix.clone(), "parent".to_string()).await;
    let parent = namespace_id(&f, "parent").await;

    let table_id = create_table_returning_id(&f, "parent", "t1").await;
    let view_id: lakekeeper::service::ViewId =
        create_view(f.ctx.clone(), &prefix, "parent", "v1", None)
            .await
            .unwrap()
            .metadata
            .uuid()
            .into();
    create_generic_table(f.ctx.clone(), prefix.clone(), "parent", "gt1")
        .await
        .unwrap();
    let generic_table_id = generic_table_id(&f, "parent", "gt1").await;

    plant(
        &f.ctx,
        vec![
            held_by_user(
                &f.alice,
                GrantResource::Namespace {
                    warehouse_id: f.warehouse_id,
                    namespace_id: parent,
                },
                "get_metadata",
            ),
            held_by_user(
                &f.alice,
                GrantResource::Table {
                    warehouse_id: f.warehouse_id,
                    table_id,
                },
                "read_data",
            ),
            held_by_user(
                &f.alice,
                GrantResource::View {
                    warehouse_id: f.warehouse_id,
                    view_id,
                },
                "read_data",
            ),
            held_by_user(
                &f.alice,
                GrantResource::GenericTable {
                    warehouse_id: f.warehouse_id,
                    generic_table_id,
                },
                "read_data",
            ),
            // Control: the warehouse above the dropped subtree.
            held_by_user(
                &f.alice,
                GrantResource::Warehouse(f.warehouse_id),
                "list_namespaces",
            ),
        ],
    )
    .await;

    drop_namespace(
        f.ctx.clone(),
        NamespaceDropFlags::builder().recursive().build(),
        NamespaceParameters {
            prefix: Some(prefix.into()),
            namespace: NamespaceIdent::new("parent".to_string()),
        },
    )
    .await
    .unwrap();

    assert_eq!(
        remaining_grants(&f.ctx).await,
        rows(&[("warehouse", "list_namespaces")])
    );
}

#[sqlx::test]
async fn dropping_a_table_takes_its_grants_with_it(pool: PgPool) {
    let f = setup(pool).await;
    create_ns(f.ctx.clone(), f.warehouse_id.to_string(), "ns".to_string()).await;
    let table_id = create_table_returning_id(&f, "ns", "t1").await;
    let ns = namespace_id(&f, "ns").await;

    plant(
        &f.ctx,
        vec![
            held_by_user(
                &f.alice,
                GrantResource::Table {
                    warehouse_id: f.warehouse_id,
                    table_id,
                },
                "read_data",
            ),
            // Control: the namespace holding it keeps its own grant.
            held_by_user(
                &f.alice,
                GrantResource::Namespace {
                    warehouse_id: f.warehouse_id,
                    namespace_id: ns,
                },
                "get_metadata",
            ),
        ],
    )
    .await;

    drop_table(
        f.ctx.clone(),
        &f.warehouse_id.to_string(),
        "ns",
        "t1",
        Some(false),
        false,
    )
    .await
    .unwrap();

    assert_eq!(
        remaining_grants(&f.ctx).await,
        rows(&[("namespace", "get_metadata")])
    );
}

#[sqlx::test]
async fn deleting_a_tag_definition_takes_its_grants_with_it(pool: PgPool) {
    let f = setup(pool).await;
    let doomed = Server::create_tag_definition(
        CreateTagDefinitionRequest::builder()
            .name("doomed".to_string())
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

    plant(
        &f.ctx,
        vec![
            held_by_user(&f.alice, GrantResource::Tag(doomed), "apply"),
            // Control: the project the definition lived in.
            held_by_user(
                &f.alice,
                GrantResource::Project(f.project_id.clone()),
                "create_tag_definition",
            ),
        ],
    )
    .await;

    Server::delete_tag_definition(f.ctx.clone(), f.metadata.clone(), doomed)
        .await
        .unwrap();

    assert_eq!(
        remaining_grants(&f.ctx).await,
        rows(&[("project", "create_tag_definition")])
    );
}

/// A soft-dropped tabular keeps its row, so the cascade does not fire and the grants
/// stay. That is what makes an undrop restore the access that was there before it —
/// they go for real when expiration hard-deletes the row.
#[sqlx::test]
async fn a_soft_dropped_table_keeps_its_grants_for_undrop(pool: PgPool) {
    let f = setup_with_delete_profile(
        pool,
        TabularDeleteProfile::Soft {
            expiration_seconds: chrono::Duration::seconds(3600),
        },
    )
    .await;
    create_ns(f.ctx.clone(), f.warehouse_id.to_string(), "ns".to_string()).await;
    let table_id = create_table_returning_id(&f, "ns", "t1").await;

    plant(
        &f.ctx,
        vec![held_by_user(
            &f.alice,
            GrantResource::Table {
                warehouse_id: f.warehouse_id,
                table_id,
            },
            "read_data",
        )],
    )
    .await;

    drop_table(
        f.ctx.clone(),
        &f.warehouse_id.to_string(),
        "ns",
        "t1",
        Some(false),
        false,
    )
    .await
    .unwrap();
    assert_eq!(
        remaining_grants(&f.ctx).await,
        rows(&[("tabular", "read_data")])
    );

    Server::undrop_tabulars(
        f.warehouse_id,
        f.metadata.clone(),
        UndropTabularsRequest {
            targets: vec![TabularId::Table(table_id)],
        },
        f.ctx.clone(),
    )
    .await
    .unwrap();

    // Back on the live table, and readable through its own listing again.
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
    assert_eq!(page.grants[0].privilege, "read_data");
    assert_eq!(
        page.grants[0].resource,
        GrantResourceResponse::Table {
            warehouse_id: f.warehouse_id,
            table_id
        }
    );
}
