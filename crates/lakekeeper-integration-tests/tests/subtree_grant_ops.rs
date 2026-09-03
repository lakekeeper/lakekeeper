//! Behavioral tests for the subtree grant listing and the bounded subtree revoke, on the
//! catalog arm (Postgres). The per-resource twin is `grant_ops.rs`.
//!
//! Most run under `AllowAllAuthorizer`, which permits every action and stores grants in
//! the catalog; the per-member visibility and grant-authority sections switch to a
//! denying authorizer, which is the only way to tell the root's gate apart from the
//! per-member ones. Exact assertions throughout.

use std::sync::{Arc, Mutex};

use iceberg::{
    NamespaceIdent,
    spec::{NestedField, PrimitiveType, Schema, Type, UnboundPartitionSpec},
};
use iceberg_ext::catalog::rest::{CreateTableRequest, RenameTableRequest};
use lakekeeper::{
    WarehouseId,
    api::{
        ApiContext, RequestMetadata, RequestMetadataTestBuilder,
        iceberg::{
            types::Prefix,
            v1::{
                CreateNamespaceRequest, DropParams, NamespaceParameters, PageToken,
                PaginationQuery, TableParameters,
                namespace::NamespaceService as _,
                tables::{DataAccess, TablesService as _},
            },
        },
        management::v1::{
            ApiServer,
            check::UserOrRole,
            grant::{
                GrantResourceResponse, ListSubtreeGrantsQuery, RevokeSubtreeGrantsRequest,
                Service as _,
            },
            warehouse::{Service as _, TabularDeleteProfile},
        },
    },
    server::CatalogServer,
    service::{
        CatalogCreateRoleRequest, CatalogGrantOps as _, CatalogNamespaceOps as _,
        CatalogRoleOps as _, CatalogStore, NamespaceId, RoleId, RoleProviderId, RoleSourceId,
        State, TableId, Transaction as _, UserId,
        authn::Actor,
        authz::{
            AllowAllAuthorizer, GrantOp, GrantResource, GrantSpec, UserOrRoleId,
            tests::HidingAuthorizer,
        },
        events::{EventListener, GrantsChangedEvent},
    },
};
use lakekeeper_integration_tests::{SetupTestCatalog, memory_io_profile, random_request_metadata};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;

type Ctx = ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>;
type Server = ApiServer<PostgresBackend, AllowAllAuthorizer, SecretsState>;
type DenyCtx = ApiContext<State<HidingAuthorizer, PostgresBackend, SecretsState>>;
type DenyServer = ApiServer<PostgresBackend, HidingAuthorizer, SecretsState>;

/// A namespace tree with one table, and grants seeded at every level:
///
/// ```text
/// parent            <- alice: get_metadata
///   parent.child    <- alice: get_metadata, bob: get_metadata
///     table         <- alice: get_metadata
/// ```
///
/// Plus one grant on the warehouse itself, which no namespace-rooted operation may ever
/// touch and which a warehouse-rooted one takes only on request.
struct Tree {
    parent: NamespaceId,
    child: NamespaceId,
    table: TableId,
}

struct Fixture {
    ctx: Ctx,
    metadata: RequestMetadata,
    warehouse_id: WarehouseId,
    alice: UserId,
    bob: UserId,
}

async fn setup(pool: PgPool) -> Fixture {
    setup_with_delete_profile(pool, TabularDeleteProfile::Hard {}).await
}

/// The recycle-bin tests need a warehouse that soft-deletes; everything else is faster
/// without one.
async fn setup_soft_deleting(pool: PgPool) -> Fixture {
    setup_with_delete_profile(
        pool,
        TabularDeleteProfile::Soft {
            expiration_seconds: chrono::Duration::seconds(3600),
        },
    )
    .await
}

async fn setup_with_delete_profile(pool: PgPool, delete_profile: TabularDeleteProfile) -> Fixture {
    let (ctx, warehouse) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .delete_profile(delete_profile)
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;
    let alice = UserId::try_from("oidc~alice").unwrap();
    let bob = UserId::try_from("oidc~bob").unwrap();
    provision_user(&ctx, &alice).await;
    provision_user(&ctx, &bob).await;
    let metadata = RequestMetadataTestBuilder::builder()
        .actor(Actor::Principal(alice.clone()))
        .project_id(Some(warehouse.project_id.clone()))
        .build();
    Fixture {
        ctx,
        metadata,
        warehouse_id: warehouse.warehouse_id,
        alice,
        bob,
    }
}

async fn provision_user<A: lakekeeper::service::authz::Authorizer>(
    ctx: &ApiContext<State<A, PostgresBackend, SecretsState>>,
    user_id: &UserId,
) {
    use lakekeeper::{
        api::management::v1::user::{UserLastUpdatedWith, UserType},
        service::{Transaction as _, UserUpsertMode},
    };
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

async fn create_namespace<A: lakekeeper::service::authz::Authorizer>(
    ctx: &ApiContext<State<A, PostgresBackend, SecretsState>>,
    warehouse_id: WarehouseId,
    parts: &[&str],
) -> NamespaceId {
    let ident = NamespaceIdent::from_strs(parts).unwrap();
    CatalogServer::create_namespace(
        Some(Prefix::from(warehouse_id.to_string())),
        CreateNamespaceRequest {
            namespace: ident.clone(),
            properties: None,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    PostgresBackend::get_namespace(warehouse_id, ident, ctx.v1_state.catalog.clone())
        .await
        .unwrap()
        .unwrap()
        .namespace_id()
}

async fn create_table<A: lakekeeper::service::authz::Authorizer>(
    ctx: &ApiContext<State<A, PostgresBackend, SecretsState>>,
    warehouse_id: WarehouseId,
    namespace: &[&str],
    name: &str,
) -> TableId {
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .unwrap();
    let created = CatalogServer::create_table(
        NamespaceParameters {
            namespace: NamespaceIdent::from_strs(namespace).unwrap(),
            prefix: Some(Prefix::from(warehouse_id.to_string())),
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

/// A table that was created but never committed: it holds no metadata yet, and grants
/// written against its id are still real access to it.
async fn create_staged_table<A: lakekeeper::service::authz::Authorizer>(
    ctx: &ApiContext<State<A, PostgresBackend, SecretsState>>,
    warehouse_id: WarehouseId,
    namespace: &[&str],
    name: &str,
) -> TableId {
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .unwrap();
    let created = CatalogServer::create_table(
        NamespaceParameters {
            namespace: NamespaceIdent::from_strs(namespace).unwrap(),
            prefix: Some(Prefix::from(warehouse_id.to_string())),
        },
        CreateTableRequest {
            name: name.to_string(),
            location: None,
            schema,
            partition_spec: Some(UnboundPartitionSpec::builder().build()),
            write_order: None,
            stage_create: Some(true),
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

/// Seeds grants through the store rather than the apply API: the denying authorizer
/// publishes an empty vocabulary, so only a direct write puts real rows behind a gate.
async fn seed<A: lakekeeper::service::authz::Authorizer>(
    ctx: &ApiContext<State<A, PostgresBackend, SecretsState>>,
    grants: Vec<(&UserId, &str, GrantResource)>,
) {
    let writes: Vec<GrantSpec> = grants
        .into_iter()
        .map(|(user, privilege, resource)| GrantSpec {
            principal: UserOrRoleId::User(user.clone()),
            resource,
            privilege: privilege.to_string(),
        })
        .collect();
    PostgresBackend::apply_grants(&writes, &[], ctx.v1_state.catalog.clone())
        .await
        .unwrap();
}

/// Seed grants for principals of either kind.
async fn seed_principals<A: lakekeeper::service::authz::Authorizer>(
    ctx: &ApiContext<State<A, PostgresBackend, SecretsState>>,
    grants: Vec<(UserOrRoleId, &str, GrantResource)>,
) {
    let writes: Vec<GrantSpec> = grants
        .into_iter()
        .map(|(principal, privilege, resource)| GrantSpec {
            principal,
            resource,
            privilege: privilege.to_string(),
        })
        .collect();
    PostgresBackend::apply_grants(&writes, &[], ctx.v1_state.catalog.clone())
        .await
        .unwrap();
}

async fn build_tree(f: &Fixture) -> Tree {
    let parent = create_namespace(&f.ctx, f.warehouse_id, &["parent"]).await;
    let child = create_namespace(&f.ctx, f.warehouse_id, &["parent", "child"]).await;
    let table = create_table(&f.ctx, f.warehouse_id, &["parent", "child"], "t1").await;
    let warehouse_id = f.warehouse_id;
    seed(
        &f.ctx,
        vec![
            (
                &f.alice,
                "get_metadata",
                GrantResource::Namespace {
                    warehouse_id,
                    namespace_id: parent,
                },
            ),
            (
                &f.alice,
                "get_metadata",
                GrantResource::Namespace {
                    warehouse_id,
                    namespace_id: child,
                },
            ),
            (
                &f.bob,
                "get_metadata",
                GrantResource::Namespace {
                    warehouse_id,
                    namespace_id: child,
                },
            ),
            (
                &f.alice,
                "get_metadata",
                GrantResource::Table {
                    warehouse_id,
                    table_id: table,
                },
            ),
            (
                &f.alice,
                "get_metadata",
                GrantResource::Warehouse(warehouse_id),
            ),
        ],
    )
    .await;
    Tree {
        parent,
        child,
        table,
    }
}

fn no_pagination() -> PaginationQuery {
    PaginationQuery::new(PageToken::Empty, None)
}

type ListedGrant = (UserOrRole, String, GrantResourceResponse);

/// A page reduced to what a test asserts on, in a fixed order: a subtree listing merges
/// two arms whose grants share a `created_at`, so the order within one page is not
/// something to pin.
fn listed(grants: Vec<lakekeeper::api::management::v1::grant::GrantResponse>) -> Vec<ListedGrant> {
    sorted(
        grants
            .into_iter()
            .map(|grant| (grant.principal, grant.privilege, grant.resource))
            .collect(),
    )
}

fn sorted(mut grants: Vec<ListedGrant>) -> Vec<ListedGrant> {
    grants.sort_by_key(|(principal, privilege, resource)| {
        format!("{principal:?}|{privilege}|{resource:?}")
    });
    grants
}

fn revoke_all() -> RevokeSubtreeGrantsRequest {
    RevokeSubtreeGrantsRequest {
        principal: None,
        privilege: vec![],
        resource_type: vec![],
        created_before: None,
        limit: None,
        allow_partial: false,
        include_root_level: true,
        dry_run: false,
    }
}

/// Collects the grants each event announced as removed, so a test can assert that the
/// audit trail carries one record per revoked grant rather than a summary.
#[derive(Debug, Default)]
struct RevokedRecorder {
    removed: Arc<Mutex<Vec<GrantSpec>>>,
}

impl std::fmt::Display for RevokedRecorder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RevokedRecorder")
    }
}

#[async_trait::async_trait]
impl EventListener for RevokedRecorder {
    async fn grants_changed(&self, event: GrantsChangedEvent) -> anyhow::Result<()> {
        self.removed.lock().unwrap().extend(event.removed);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Listing
// ---------------------------------------------------------------------------

/// The whole point of the endpoint: one request instead of one per resource. The root's
/// own grants are in, the warehouse's are not, and a tabular inside a descendant
/// namespace comes back with the kind it actually is.
#[sqlx::test]
async fn a_namespace_listing_covers_the_root_its_descendants_and_their_tabulars(pool: PgPool) {
    let f = setup(pool).await;
    let tree = build_tree(&f).await;
    let warehouse_id = f.warehouse_id;

    let page = Server::list_namespace_subtree_grants(
        warehouse_id,
        tree.parent,
        f.ctx.clone(),
        f.metadata.clone(),
        ListSubtreeGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();

    assert_eq!(
        listed(page.grants),
        sorted(vec![
            (
                UserOrRole::User(f.alice.clone()),
                "get_metadata".to_string(),
                GrantResourceResponse::Namespace {
                    warehouse_id,
                    namespace_id: tree.parent
                }
            ),
            (
                UserOrRole::User(f.alice.clone()),
                "get_metadata".to_string(),
                GrantResourceResponse::Namespace {
                    warehouse_id,
                    namespace_id: tree.child
                }
            ),
            (
                UserOrRole::User(f.bob.clone()),
                "get_metadata".to_string(),
                GrantResourceResponse::Namespace {
                    warehouse_id,
                    namespace_id: tree.child
                }
            ),
            (
                UserOrRole::User(f.alice.clone()),
                "get_metadata".to_string(),
                GrantResourceResponse::Table {
                    warehouse_id,
                    table_id: tree.table
                }
            ),
        ])
    );
}

/// A sibling subtree is not under the root, however similar its name. `parent2` sorts
/// immediately after `parent` in the path btree, which is exactly where the range start
/// bound puts it — so only the prefix recheck keeps it out.
#[sqlx::test]
async fn a_sibling_namespace_is_not_in_the_subtree(pool: PgPool) {
    let f = setup(pool).await;
    let tree = build_tree(&f).await;
    let sibling = create_namespace(&f.ctx, f.warehouse_id, &["parent2"]).await;
    seed(
        &f.ctx,
        vec![(
            &f.alice,
            "get_metadata",
            GrantResource::Namespace {
                warehouse_id: f.warehouse_id,
                namespace_id: sibling,
            },
        )],
    )
    .await;

    let page = Server::list_namespace_subtree_grants(
        f.warehouse_id,
        tree.parent,
        f.ctx.clone(),
        f.metadata.clone(),
        ListSubtreeGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants.len(), 4);
    assert!(!page.grants.iter().any(|grant| grant.resource
        == GrantResourceResponse::Namespace {
            warehouse_id: f.warehouse_id,
            namespace_id: sibling
        }));
}

/// The warehouse-rooted listing covers the warehouse's own grants too — the subtree
/// contains its root — and `includeRootLevel=false` narrows it to strictly-below.
#[sqlx::test]
async fn a_warehouse_listing_includes_the_warehouses_own_grants(pool: PgPool) {
    let f = setup(pool).await;
    build_tree(&f).await;

    let page = Server::list_warehouse_subtree_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        ListSubtreeGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants.len(), 5);
    assert!(page.grants.iter().any(|grant| grant.resource
        == GrantResourceResponse::Warehouse {
            warehouse_id: f.warehouse_id
        }));

    let page = Server::list_warehouse_subtree_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        ListSubtreeGrantsQuery {
            include_root_level: false,
            ..ListSubtreeGrantsQuery::default()
        },
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants.len(), 4);
    assert!(!page.grants.iter().any(|grant| grant.resource
        == GrantResourceResponse::Warehouse {
            warehouse_id: f.warehouse_id
        }));
}

/// `resourceType` names kinds exactly: asking for tables does not imply views, and asking
/// for namespaces excludes the tabulars inside them.
#[sqlx::test]
async fn the_resource_type_filter_names_kinds_exactly(pool: PgPool) {
    let f = setup(pool).await;
    let tree = build_tree(&f).await;

    let page = Server::list_namespace_subtree_grants(
        f.warehouse_id,
        tree.parent,
        f.ctx.clone(),
        f.metadata.clone(),
        ListSubtreeGrantsQuery {
            resource_type: vec![lakekeeper::service::authz::ResourceType::Table],
            ..ListSubtreeGrantsQuery::default()
        },
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(
        listed(page.grants),
        vec![(
            UserOrRole::User(f.alice.clone()),
            "get_metadata".to_string(),
            GrantResourceResponse::Table {
                warehouse_id: f.warehouse_id,
                table_id: tree.table
            }
        )]
    );

    let page = Server::list_namespace_subtree_grants(
        f.warehouse_id,
        tree.parent,
        f.ctx.clone(),
        f.metadata.clone(),
        ListSubtreeGrantsQuery {
            resource_type: vec![lakekeeper::service::authz::ResourceType::View],
            ..ListSubtreeGrantsQuery::default()
        },
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants, vec![]);
}

/// A kind nothing under the root can be is refused, for the same reason an unrecognized
/// privilege is: matching nothing and answering `200` reads as "already clean".
#[sqlx::test]
async fn a_resource_type_outside_the_subtree_is_refused(pool: PgPool) {
    let f = setup(pool).await;
    let tree = build_tree(&f).await;

    let err = Server::list_namespace_subtree_grants(
        f.warehouse_id,
        tree.parent,
        f.ctx.clone(),
        f.metadata.clone(),
        ListSubtreeGrantsQuery {
            resource_type: vec![lakekeeper::service::authz::ResourceType::Tag],
            ..ListSubtreeGrantsQuery::default()
        },
        no_pagination(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 400);
    assert_eq!(err.error.r#type, "GrantSubtreeScopeMismatch");

    // The warehouse is not under a namespace either, even though it is a kind a
    // warehouse-rooted revoke can name.
    let err = Server::revoke_namespace_subtree_grants(
        f.warehouse_id,
        tree.parent,
        f.ctx.clone(),
        f.metadata.clone(),
        RevokeSubtreeGrantsRequest {
            resource_type: vec![lakekeeper::service::authz::ResourceType::Warehouse],
            ..revoke_all()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 400);
    assert_eq!(err.error.r#type, "GrantSubtreeScopeMismatch");

    // And it is accepted where it does mean something.
    let response = Server::revoke_warehouse_subtree_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        RevokeSubtreeGrantsRequest {
            resource_type: vec![lakekeeper::service::authz::ResourceType::Warehouse],
            ..revoke_all()
        },
    )
    .await
    .unwrap();
    assert_eq!(response.revoked, 1);
}

/// A privilege the authorizer does not publish is a typo: answering `200` with nothing
/// would read as "that access is gone".
#[sqlx::test]
async fn a_filter_privilege_outside_the_vocabulary_is_refused(pool: PgPool) {
    let f = setup(pool).await;
    let tree = build_tree(&f).await;

    let err = Server::list_namespace_subtree_grants(
        f.warehouse_id,
        tree.parent,
        f.ctx.clone(),
        f.metadata.clone(),
        ListSubtreeGrantsQuery {
            privilege: vec!["slect".to_string()],
            ..ListSubtreeGrantsQuery::default()
        },
        no_pagination(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 400);
    assert_eq!(err.error.r#type, "UnknownGrantPrivilege");

    // A privilege the vocabulary does publish is accepted, and matches nothing here
    // because no grant in the tree carries it.
    let page = Server::list_namespace_subtree_grants(
        f.warehouse_id,
        tree.parent,
        f.ctx.clone(),
        f.metadata.clone(),
        ListSubtreeGrantsQuery {
            privilege: vec!["select".to_string()],
            ..ListSubtreeGrantsQuery::default()
        },
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants, vec![]);
}

/// A grant on a table in the recycle bin is listed by default, because the matching
/// revoke removes it — and hidden on request, for an access review that only cares about
/// live objects.
#[sqlx::test]
async fn soft_deleted_tables_are_listed_by_default(pool: PgPool) {
    let f = setup_soft_deleting(pool).await;
    let tree = build_tree(&f).await;
    soft_delete_table(&f, tree.table).await;

    let page = Server::list_namespace_subtree_grants(
        f.warehouse_id,
        tree.parent,
        f.ctx.clone(),
        f.metadata.clone(),
        ListSubtreeGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants.len(), 4);

    let page = Server::list_namespace_subtree_grants(
        f.warehouse_id,
        tree.parent,
        f.ctx.clone(),
        f.metadata.clone(),
        ListSubtreeGrantsQuery {
            include_soft_deleted: false,
            ..ListSubtreeGrantsQuery::default()
        },
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants.len(), 3);
}

/// Deactivating a warehouse is defined to hide its children's grants, so the listing that
/// walks every child must not be the way around that.
#[sqlx::test]
async fn a_warehouse_listing_refuses_an_inactive_warehouse(pool: PgPool) {
    let f = setup(pool).await;
    build_tree(&f).await;
    Server::deactivate_warehouse(f.warehouse_id, f.ctx.clone(), f.metadata.clone())
        .await
        .unwrap();

    let err = Server::list_warehouse_subtree_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        ListSubtreeGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 404);
}

/// Pages are keyset-walked across both arms at once: a namespace grant and a tabular
/// grant are ordered by the same key, so a page boundary that falls between them still
/// resumes correctly.
#[sqlx::test]
async fn the_listing_pages_across_both_arms(pool: PgPool) {
    let f = setup(pool).await;
    let tree = build_tree(&f).await;

    let mut seen = Vec::new();
    let mut token = PageToken::Empty;
    for _ in 0..10 {
        let page = Server::list_namespace_subtree_grants(
            f.warehouse_id,
            tree.parent,
            f.ctx.clone(),
            f.metadata.clone(),
            ListSubtreeGrantsQuery::default(),
            PaginationQuery::new(token.clone(), Some(1)),
        )
        .await
        .unwrap();
        seen.extend(page.grants);
        match page.next_page_token {
            Some(next) => token = PageToken::Present(next),
            None => break,
        }
    }
    assert_eq!(seen.len(), 4);
}

// ---------------------------------------------------------------------------
// Revoking
// ---------------------------------------------------------------------------

/// The straightforward case: everything under the root goes, the warehouse's own grant
/// stays, and every removed grant is announced individually.
#[sqlx::test]
async fn a_namespace_revoke_clears_the_subtree_and_leaves_the_warehouse(pool: PgPool) {
    let f = setup(pool).await;
    let tree = build_tree(&f).await;
    let recorder = Arc::new(RevokedRecorder::default());
    let removed = recorder.removed.clone();
    let ctx = with_listener(&f.ctx, recorder).await;

    let response = Server::revoke_namespace_subtree_grants(
        f.warehouse_id,
        tree.parent,
        ctx.clone(),
        f.metadata.clone(),
        RevokeSubtreeGrantsRequest { ..revoke_all() },
    )
    .await
    .unwrap();
    assert_eq!(response.revoked, 4);
    assert!(!response.has_more);

    let page = Server::list_namespace_subtree_grants(
        f.warehouse_id,
        tree.parent,
        f.ctx.clone(),
        f.metadata.clone(),
        ListSubtreeGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants, vec![]);

    // The warehouse's own grant is outside a namespace subtree entirely.
    let remaining = PostgresBackend::list_grants(
        &lakekeeper::service::authz::GrantFilter::on(
            GrantResource::Warehouse(f.warehouse_id),
            None,
        ),
        no_pagination(),
        f.ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    assert_eq!(remaining.grants.len(), 1);

    // One audit record per grant, not one per call.
    assert_eq!(removed.lock().unwrap().len(), 4);
}

/// A request matching more than one call may take is refused outright, and refused
/// *before* anything is removed — a partially applied "success" would be a security
/// misreport.
#[sqlx::test]
async fn a_batch_larger_than_the_limit_revokes_nothing(pool: PgPool) {
    let f = setup(pool).await;
    let tree = build_tree(&f).await;
    let recorder = Arc::new(RevokedRecorder::default());
    let removed = recorder.removed.clone();
    let ctx = with_listener(&f.ctx, recorder).await;

    let err = Server::revoke_namespace_subtree_grants(
        f.warehouse_id,
        tree.parent,
        ctx.clone(),
        f.metadata.clone(),
        RevokeSubtreeGrantsRequest {
            limit: Some(1),
            ..revoke_all()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 400);
    assert_eq!(err.error.r#type, "GrantRevokeBatchTooLarge");

    let page = Server::list_namespace_subtree_grants(
        f.warehouse_id,
        tree.parent,
        f.ctx.clone(),
        f.metadata.clone(),
        ListSubtreeGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants.len(), 4);
    assert_eq!(removed.lock().unwrap().len(), 0);
}

/// The loop: repeat the same request with the ceiling echoed back until `has-more` is
/// false. Every pass strictly shrinks the set at or below the ceiling, and a grant
/// created after it is deliberately left behind.
#[sqlx::test]
async fn a_bounded_loop_drains_the_subtree_and_leaves_later_grants(pool: PgPool) {
    let f = setup(pool).await;
    let tree = build_tree(&f).await;
    let recorder = Arc::new(RevokedRecorder::default());
    let removed_events = recorder.removed.clone();
    let ctx = with_listener(&f.ctx, recorder).await;

    let first = Server::revoke_namespace_subtree_grants(
        f.warehouse_id,
        tree.parent,
        ctx.clone(),
        f.metadata.clone(),
        RevokeSubtreeGrantsRequest {
            limit: Some(2),
            allow_partial: true,
            ..revoke_all()
        },
    )
    .await
    .unwrap();
    assert_eq!(first.revoked, 2);
    assert!(first.has_more);

    // Granted between the two calls, so its timestamp is above the ceiling.
    seed(
        &f.ctx,
        vec![(
            &f.bob,
            "get_metadata",
            GrantResource::Namespace {
                warehouse_id: f.warehouse_id,
                namespace_id: tree.parent,
            },
        )],
    )
    .await;

    let second = Server::revoke_namespace_subtree_grants(
        f.warehouse_id,
        tree.parent,
        ctx.clone(),
        f.metadata.clone(),
        RevokeSubtreeGrantsRequest {
            limit: Some(2),
            allow_partial: true,
            created_before: Some(first.created_before),
            ..revoke_all()
        },
    )
    .await
    .unwrap();
    assert_eq!(second.revoked, 2);
    assert!(!second.has_more);
    assert_eq!(second.created_before, first.created_before);
    assert_eq!(removed_events.lock().unwrap().len(), 4);

    // The grant made after the operation began is exactly what is left.
    let page = Server::list_namespace_subtree_grants(
        f.warehouse_id,
        tree.parent,
        f.ctx.clone(),
        f.metadata.clone(),
        ListSubtreeGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(
        listed(page.grants),
        vec![(
            UserOrRole::User(f.bob.clone()),
            "get_metadata".to_string(),
            GrantResourceResponse::Namespace {
                warehouse_id: f.warehouse_id,
                namespace_id: tree.parent
            }
        )]
    );
}

/// The caller's own grants are revoked like anyone else's — there is no self-exemption.
/// The way to keep the administration plane, including the grant the caller's own
/// authority flows through, is to leave the root level out.
#[sqlx::test]
async fn the_actors_own_root_grant_goes_with_the_rest(pool: PgPool) {
    let f = setup(pool).await;
    let tree = build_tree(&f).await;

    let response = Server::revoke_namespace_subtree_grants(
        f.warehouse_id,
        tree.parent,
        f.ctx.clone(),
        f.metadata.clone(),
        revoke_all(),
    )
    .await
    .unwrap();
    // Alice acts, and her grant on the root goes with bob's and the two below it.
    assert_eq!(response.revoked, 4);

    let page = Server::list_namespace_subtree_grants(
        f.warehouse_id,
        tree.parent,
        f.ctx.clone(),
        f.metadata.clone(),
        ListSubtreeGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants, vec![]);
}

/// Keeping the root level keeps every grant on the root, the caller's included, and
/// clears everything beneath it — the deterministic replacement for a self-exemption.
#[sqlx::test]
async fn leaving_the_root_level_out_keeps_the_actors_own_grant(pool: PgPool) {
    let f = setup(pool).await;
    let tree = build_tree(&f).await;

    let response = Server::revoke_namespace_subtree_grants(
        f.warehouse_id,
        tree.parent,
        f.ctx.clone(),
        f.metadata.clone(),
        RevokeSubtreeGrantsRequest {
            include_root_level: false,
            ..revoke_all()
        },
    )
    .await
    .unwrap();
    assert_eq!(response.revoked, 3);

    let page = Server::list_namespace_subtree_grants(
        f.warehouse_id,
        tree.parent,
        f.ctx.clone(),
        f.metadata.clone(),
        ListSubtreeGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(
        listed(page.grants),
        vec![(
            UserOrRole::User(f.alice.clone()),
            "get_metadata".to_string(),
            GrantResourceResponse::Namespace {
                warehouse_id: f.warehouse_id,
                namespace_id: tree.parent
            }
        )]
    );
}

/// A namespace-rooted read costs in proportion to the namespaces it spans, so an
/// oversized root is refused before that read starts — with the size it would have read,
/// rather than after running into a timeout. The warehouse-rooted form has no such bound.
#[sqlx::test]
async fn an_oversized_namespace_subtree_is_refused(pool: PgPool) {
    let f = setup(pool.clone()).await;
    let tree = build_tree(&f).await;
    // One past the bound, counting the root and the child the fixture already made.
    sqlx::query(
        "insert into namespace (namespace_id, warehouse_id, namespace_name, namespace_properties)
         select gen_random_uuid(), $1, ARRAY['parent', 'wide_' || i], '{}'::jsonb
         from generate_series(1, $2) i",
    )
    .bind(*f.warehouse_id)
    .bind(i64::from(
        u32::try_from(lakekeeper::api::management::v1::grant::MAX_SUBTREE_NAMESPACES).unwrap(),
    ))
    .execute(&pool)
    .await
    .unwrap();

    let err = Server::list_namespace_subtree_grants(
        f.warehouse_id,
        tree.parent,
        f.ctx.clone(),
        f.metadata.clone(),
        ListSubtreeGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 400);
    assert_eq!(err.error.r#type, "GrantSubtreeTooLarge");

    let err = Server::revoke_namespace_subtree_grants(
        f.warehouse_id,
        tree.parent,
        f.ctx.clone(),
        f.metadata.clone(),
        revoke_all(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 400);
    assert_eq!(err.error.r#type, "GrantSubtreeTooLarge");

    // Nothing was removed, and the warehouse-rooted form still answers.
    let page = Server::list_warehouse_subtree_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        ListSubtreeGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants.len(), 5);
}

/// A table created but never committed still has an id, and a grant on that id is real
/// access to it. Both operations therefore cover it, on the same footing as one in the
/// recycle bin.
#[sqlx::test]
async fn a_staged_table_is_in_scope(pool: PgPool) {
    let f = setup(pool).await;
    let tree = build_tree(&f).await;
    let staged = create_staged_table(&f.ctx, f.warehouse_id, &["parent", "child"], "staged").await;
    seed(
        &f.ctx,
        vec![(
            &f.bob,
            "get_metadata",
            GrantResource::Table {
                warehouse_id: f.warehouse_id,
                table_id: staged,
            },
        )],
    )
    .await;

    let page = Server::list_namespace_subtree_grants(
        f.warehouse_id,
        tree.parent,
        f.ctx.clone(),
        f.metadata.clone(),
        ListSubtreeGrantsQuery {
            resource_type: vec![lakekeeper::service::authz::ResourceType::Table],
            ..ListSubtreeGrantsQuery::default()
        },
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants.len(), 2);
    assert!(page.grants.iter().any(|grant| grant.resource
        == GrantResourceResponse::Table {
            warehouse_id: f.warehouse_id,
            table_id: staged
        }));

    let response = Server::revoke_namespace_subtree_grants(
        f.warehouse_id,
        tree.parent,
        f.ctx.clone(),
        f.metadata.clone(),
        RevokeSubtreeGrantsRequest {
            resource_type: vec![lakekeeper::service::authz::ResourceType::Table],
            ..revoke_all()
        },
    )
    .await
    .unwrap();
    assert_eq!(response.revoked, 2);
}

/// A revoke reaches into the recycle bin whatever a listing would show: an undrop
/// restores a table together with its grants, so leaving them behind would restore access
/// the operator believed removed.
#[sqlx::test]
async fn a_revoke_removes_grants_on_soft_deleted_tables(pool: PgPool) {
    let f = setup_soft_deleting(pool).await;
    let tree = build_tree(&f).await;
    soft_delete_table(&f, tree.table).await;

    let response = Server::revoke_namespace_subtree_grants(
        f.warehouse_id,
        tree.parent,
        f.ctx.clone(),
        f.metadata.clone(),
        RevokeSubtreeGrantsRequest {
            resource_type: vec![lakekeeper::service::authz::ResourceType::Table],
            ..revoke_all()
        },
    )
    .await
    .unwrap();
    assert_eq!(response.revoked, 1);
}

/// A subtree contains its root: the default revoke takes the warehouse's own grants,
/// because a grant on the warehouse confers access to everything beneath it. Opting out
/// keeps the administration plane in place.
#[sqlx::test]
async fn a_warehouse_revoke_takes_the_warehouse_level_unless_opted_out(pool: PgPool) {
    let f = setup(pool).await;
    build_tree(&f).await;

    let response = Server::revoke_warehouse_subtree_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        RevokeSubtreeGrantsRequest {
            include_root_level: false,
            ..revoke_all()
        },
    )
    .await
    .unwrap();
    assert_eq!(response.revoked, 4);
    let remaining = PostgresBackend::list_grants(
        &lakekeeper::service::authz::GrantFilter::on(
            GrantResource::Warehouse(f.warehouse_id),
            None,
        ),
        no_pagination(),
        f.ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    assert_eq!(remaining.grants.len(), 1);

    let response = Server::revoke_warehouse_subtree_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        RevokeSubtreeGrantsRequest { ..revoke_all() },
    )
    .await
    .unwrap();
    assert_eq!(response.revoked, 1);
}

/// The namespace root gets the same opt-out: `include-root-level: false` keeps the
/// grants on the addressed namespace itself and still clears everything beneath it.
#[sqlx::test]
async fn a_namespace_revoke_keeps_the_roots_own_grants_on_request(pool: PgPool) {
    let f = setup(pool).await;
    let tree = build_tree(&f).await;

    let response = Server::revoke_namespace_subtree_grants(
        f.warehouse_id,
        tree.parent,
        f.ctx.clone(),
        f.metadata.clone(),
        RevokeSubtreeGrantsRequest {
            include_root_level: false,
            ..revoke_all()
        },
    )
    .await
    .unwrap();
    // alice's grant on `parent` survives; both grants on `child` and the table's go.
    assert_eq!(response.revoked, 3);

    let remaining = PostgresBackend::list_grants(
        &lakekeeper::service::authz::GrantFilter::on(
            GrantResource::Namespace {
                warehouse_id: f.warehouse_id,
                namespace_id: tree.parent,
            },
            None,
        ),
        no_pagination(),
        f.ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    assert_eq!(remaining.grants.len(), 1);
}

/// The page token pins the walk's ceiling: every page reads under the instant page one
/// bound, a grant created mid-walk never joins it, and a `createdBefore` that disagrees
/// with the token is refused.
#[sqlx::test]
async fn the_page_token_pins_the_walk_to_one_snapshot(pool: PgPool) {
    let f = setup(pool).await;
    let tree = build_tree(&f).await;

    let first = Server::list_warehouse_subtree_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        ListSubtreeGrantsQuery::default(),
        PaginationQuery::new(PageToken::Empty, Some(2)),
    )
    .await
    .unwrap();
    assert_eq!(first.grants.len(), 2);
    let mut token = first.next_page_token.clone().unwrap();

    // Created after page one bound the ceiling, so no page of this walk shows it.
    seed(
        &f.ctx,
        vec![(
            &f.bob,
            "get_metadata",
            GrantResource::Namespace {
                warehouse_id: f.warehouse_id,
                namespace_id: tree.parent,
            },
        )],
    )
    .await;

    let mut seen = first.grants.len();
    for _ in 0..10 {
        let page = Server::list_warehouse_subtree_grants(
            f.warehouse_id,
            f.ctx.clone(),
            f.metadata.clone(),
            ListSubtreeGrantsQuery::default(),
            PaginationQuery::new(PageToken::Present(token.clone()), Some(2)),
        )
        .await
        .unwrap();
        assert_eq!(page.as_of, first.as_of);
        seen += page.grants.len();
        match page.next_page_token {
            Some(next) => token = next,
            None => break,
        }
    }
    assert_eq!(seen, 5);

    // Echoing the walk's own ceiling alongside the token is fine.
    Server::list_warehouse_subtree_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        ListSubtreeGrantsQuery {
            created_before: Some(first.as_of),
            ..ListSubtreeGrantsQuery::default()
        },
        PaginationQuery::new(PageToken::Present(token.clone()), Some(2)),
    )
    .await
    .unwrap();

    // A ceiling that disagrees with the token's is refused.
    let err = Server::list_warehouse_subtree_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        ListSubtreeGrantsQuery {
            created_before: Some(first.as_of + chrono::TimeDelta::seconds(1)),
            ..ListSubtreeGrantsQuery::default()
        },
        PaginationQuery::new(PageToken::Present(token), Some(2)),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 400);
    assert_eq!(err.error.r#type, "InvalidPaginationToken");
}

/// A caller-supplied ceiling finer than a token can carry is answered at the token's
/// precision, and echoing it back alongside the token is not read as changing the
/// filter — a walk that pins `as-of` must not fail on its own second page.
#[sqlx::test]
async fn a_sub_microsecond_ceiling_survives_pagination(pool: PgPool) {
    let f = setup(pool).await;
    build_tree(&f).await;

    // 500ns past a microsecond boundary: representable in `chrono`, not in a token.
    let ragged = chrono::DateTime::from_timestamp_nanos(
        (chrono::Utc::now() + chrono::TimeDelta::seconds(60))
            .timestamp_nanos_opt()
            .unwrap()
            / 1_000
            * 1_000
            + 500,
    );
    assert_eq!(ragged.timestamp_subsec_nanos() % 1_000, 500);

    let query = || ListSubtreeGrantsQuery {
        created_before: Some(ragged),
        ..ListSubtreeGrantsQuery::default()
    };
    let first = Server::list_warehouse_subtree_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        query(),
        PaginationQuery::new(PageToken::Empty, Some(2)),
    )
    .await
    .unwrap();
    assert_eq!(first.grants.len(), 2);
    // Reported at the precision a token round-trips, so the caller can echo it.
    assert_eq!(first.as_of.timestamp_subsec_nanos() % 1_000, 0);
    assert_eq!(first.as_of.timestamp_micros(), ragged.timestamp_micros());

    // The caller's own ragged value, alongside the token that pinned it.
    let second = Server::list_warehouse_subtree_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        query(),
        PaginationQuery::new(
            PageToken::Present(first.next_page_token.clone().unwrap()),
            Some(2),
        ),
    )
    .await
    .unwrap();
    assert_eq!(second.as_of, first.as_of);
    assert_eq!(second.grants.len(), 2);
}

/// A dry run answers with the batch and removes nothing. It is not refused for size —
/// `has-more: true` is how the caller learns the live call needs `allow-partial` — and
/// the live call under the same ceiling removes exactly what was previewed.
#[sqlx::test]
async fn a_dry_run_previews_the_batch_and_removes_nothing(pool: PgPool) {
    let f = setup(pool).await;
    build_tree(&f).await;

    let bounded = Server::revoke_warehouse_subtree_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        RevokeSubtreeGrantsRequest {
            dry_run: true,
            limit: Some(2),
            ..revoke_all()
        },
    )
    .await
    .unwrap();
    assert_eq!(bounded.revoked, 0);
    assert!(bounded.has_more);
    assert_eq!(bounded.preview.as_ref().unwrap().len(), 2);

    let full = Server::revoke_warehouse_subtree_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        RevokeSubtreeGrantsRequest {
            dry_run: true,
            ..revoke_all()
        },
    )
    .await
    .unwrap();
    assert_eq!(full.revoked, 0);
    assert!(!full.has_more);
    let preview = full.preview.unwrap();

    // The preview is the listing's own rendering of the batch, and nothing was removed.
    let page = Server::list_warehouse_subtree_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        ListSubtreeGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(sorted(listed(preview)), sorted(listed(page.grants)));

    let live = Server::revoke_warehouse_subtree_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        RevokeSubtreeGrantsRequest {
            created_before: Some(full.created_before),
            ..revoke_all()
        },
    )
    .await
    .unwrap();
    assert_eq!(live.revoked, 5);
    assert_eq!(live.preview, None);
}

/// Membership is resolved per call, so a table renamed into the subtree between two calls
/// is swept by the second — and the response says so rather than pretending the first
/// call covered it.
#[sqlx::test]
async fn a_tabular_moved_into_the_subtree_is_swept_by_the_next_call(pool: PgPool) {
    let f = setup(pool).await;
    let tree = build_tree(&f).await;
    create_namespace(&f.ctx, f.warehouse_id, &["outside"]).await;
    let outsider = create_table(&f.ctx, f.warehouse_id, &["outside"], "t2").await;
    seed(
        &f.ctx,
        vec![(
            &f.bob,
            "get_metadata",
            GrantResource::Table {
                warehouse_id: f.warehouse_id,
                table_id: outsider,
            },
        )],
    )
    .await;

    let first = Server::revoke_namespace_subtree_grants(
        f.warehouse_id,
        tree.parent,
        f.ctx.clone(),
        f.metadata.clone(),
        RevokeSubtreeGrantsRequest { ..revoke_all() },
    )
    .await
    .unwrap();
    assert_eq!(first.revoked, 4);

    CatalogServer::rename_table(
        Some(Prefix::from(f.warehouse_id.to_string())),
        RenameTableRequest {
            source: iceberg::TableIdent::new(
                NamespaceIdent::new("outside".to_string()),
                "t2".to_string(),
            ),
            destination: iceberg::TableIdent::new(
                NamespaceIdent::from_strs(["parent", "child"]).unwrap(),
                "t2".to_string(),
            ),
        },
        f.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let second = Server::revoke_namespace_subtree_grants(
        f.warehouse_id,
        tree.parent,
        f.ctx.clone(),
        f.metadata.clone(),
        RevokeSubtreeGrantsRequest {
            created_before: Some(first.created_before),
            ..revoke_all()
        },
    )
    .await
    .unwrap();
    assert_eq!(second.revoked, 1);
}

/// Two overlapping revokes running at once must both finish. Every batch locks its rows
/// in `grant_id` order, a canonical one, so they cannot form a wait-for cycle.
#[sqlx::test]
async fn overlapping_revokes_do_not_deadlock(pool: PgPool) {
    let f = setup(pool).await;
    let tree = build_tree(&f).await;

    let left = Server::revoke_namespace_subtree_grants(
        f.warehouse_id,
        tree.parent,
        f.ctx.clone(),
        f.metadata.clone(),
        RevokeSubtreeGrantsRequest { ..revoke_all() },
    );
    let right = Server::revoke_warehouse_subtree_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        RevokeSubtreeGrantsRequest { ..revoke_all() },
    );
    let (left, right) = tokio::join!(left, right);
    // Both succeed; between them they remove each grant exactly once, because a
    // candidate another call already removed simply does not come back.
    assert_eq!(left.unwrap().revoked + right.unwrap().revoked, 5);
    let page = Server::list_warehouse_subtree_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        ListSubtreeGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants, vec![]);
}

// ---------------------------------------------------------------------------
// Principal filters
// ---------------------------------------------------------------------------

/// The principal filter is bound separately in every arm of both statements, each with a
/// guard against matching the other principal kind. A wrong guard in one arm answers
/// `200` having matched less than it should, which reads as "that access is gone" — so
/// each arm is asserted: warehouse level, namespace, and tabular.
#[sqlx::test]
async fn a_user_principal_filter_matches_every_arm(pool: PgPool) {
    let f = setup(pool).await;
    let tree = build_tree(&f).await;
    // build_tree gives alice all four resources and bob the child namespace; add bob
    // everywhere else so a filter that ignores its guard would pick him up.
    seed(
        &f.ctx,
        vec![
            (
                &f.bob,
                "get_metadata",
                GrantResource::Warehouse(f.warehouse_id),
            ),
            (
                &f.bob,
                "get_metadata",
                GrantResource::Table {
                    warehouse_id: f.warehouse_id,
                    table_id: tree.table,
                },
            ),
        ],
    )
    .await;

    let page = Server::list_warehouse_subtree_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        ListSubtreeGrantsQuery {
            principal_user: Some(f.alice.clone()),
            ..ListSubtreeGrantsQuery::default()
        },
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(
        listed(page.grants),
        sorted(vec![
            (
                UserOrRole::User(f.alice.clone()),
                "get_metadata".to_string(),
                GrantResourceResponse::Warehouse {
                    warehouse_id: f.warehouse_id
                }
            ),
            (
                UserOrRole::User(f.alice.clone()),
                "get_metadata".to_string(),
                GrantResourceResponse::Namespace {
                    warehouse_id: f.warehouse_id,
                    namespace_id: tree.parent
                }
            ),
            (
                UserOrRole::User(f.alice.clone()),
                "get_metadata".to_string(),
                GrantResourceResponse::Namespace {
                    warehouse_id: f.warehouse_id,
                    namespace_id: tree.child
                }
            ),
            (
                UserOrRole::User(f.alice.clone()),
                "get_metadata".to_string(),
                GrantResourceResponse::Table {
                    warehouse_id: f.warehouse_id,
                    table_id: tree.table
                }
            ),
        ])
    );

    // A filtered revoke takes that principal's grants and leaves the other's standing.
    let response = Server::revoke_warehouse_subtree_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        RevokeSubtreeGrantsRequest {
            principal: Some(UserOrRole::User(f.alice.clone())),
            ..revoke_all()
        },
    )
    .await
    .unwrap();
    assert_eq!(response.revoked, 4);

    let page = Server::list_warehouse_subtree_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        ListSubtreeGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(
        listed(page.grants),
        sorted(vec![
            (
                UserOrRole::User(f.bob.clone()),
                "get_metadata".to_string(),
                GrantResourceResponse::Warehouse {
                    warehouse_id: f.warehouse_id
                }
            ),
            (
                UserOrRole::User(f.bob.clone()),
                "get_metadata".to_string(),
                GrantResourceResponse::Namespace {
                    warehouse_id: f.warehouse_id,
                    namespace_id: tree.child
                }
            ),
            (
                UserOrRole::User(f.bob.clone()),
                "get_metadata".to_string(),
                GrantResourceResponse::Table {
                    warehouse_id: f.warehouse_id,
                    table_id: tree.table
                }
            ),
        ])
    );
}

/// The role filter is the other half of every guard: a role-held grant must match by
/// role and a user-held one must not, in each arm.
#[sqlx::test]
async fn a_role_principal_filter_matches_only_that_role(pool: PgPool) {
    let f = setup(pool).await;
    let tree = build_tree(&f).await;
    let project_id = f.metadata.require_project_id(None).unwrap();
    let role = make_role(&f.ctx, &project_id, "analysts", "analysts-src").await;
    seed_principals(
        &f.ctx,
        vec![
            (
                UserOrRoleId::Role(role),
                "get_metadata",
                GrantResource::Warehouse(f.warehouse_id),
            ),
            (
                UserOrRoleId::Role(role),
                "get_metadata",
                GrantResource::Namespace {
                    warehouse_id: f.warehouse_id,
                    namespace_id: tree.child,
                },
            ),
            (
                UserOrRoleId::Role(role),
                "get_metadata",
                GrantResource::Table {
                    warehouse_id: f.warehouse_id,
                    table_id: tree.table,
                },
            ),
        ],
    )
    .await;

    let page = Server::list_warehouse_subtree_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        ListSubtreeGrantsQuery {
            principal_role: Some(role),
            ..ListSubtreeGrantsQuery::default()
        },
        no_pagination(),
    )
    .await
    .unwrap();
    let resources: Vec<GrantResourceResponse> = page
        .grants
        .iter()
        .map(|grant| grant.resource.clone())
        .collect();
    assert_eq!(page.grants.len(), 3);
    assert!(page.grants.iter().all(|grant| grant.principal
        == UserOrRole::Role(
            lakekeeper::api::management::v1::check::RoleAssignee::from_role(role)
        )));
    assert!(resources.contains(&GrantResourceResponse::Warehouse {
        warehouse_id: f.warehouse_id
    }));
    assert!(resources.contains(&GrantResourceResponse::Namespace {
        warehouse_id: f.warehouse_id,
        namespace_id: tree.child
    }));
    assert!(resources.contains(&GrantResourceResponse::Table {
        warehouse_id: f.warehouse_id,
        table_id: tree.table
    }));

    // Revoking the role's grants leaves every user-held grant in place.
    let response = Server::revoke_warehouse_subtree_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        RevokeSubtreeGrantsRequest {
            principal: Some(UserOrRole::Role(
                lakekeeper::api::management::v1::check::RoleAssignee::from_role(role),
            )),
            ..revoke_all()
        },
    )
    .await
    .unwrap();
    assert_eq!(response.revoked, 3);

    let page = Server::list_warehouse_subtree_grants(
        f.warehouse_id,
        f.ctx.clone(),
        f.metadata.clone(),
        ListSubtreeGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants.len(), 5);
    assert!(
        page.grants
            .iter()
            .all(|grant| matches!(grant.principal, UserOrRole::User(_)))
    );
}

// ---------------------------------------------------------------------------
// Subtree membership below depth one
// ---------------------------------------------------------------------------

/// Membership is a path-prefix slice, and a root deeper than one element is the case
/// where an off-by-one in the slice bound would show: `analytics.finance_archive` sorts
/// after `analytics.finance` and shares its first element, so only the slice equality
/// keeps it out of `analytics.finance`'s subtree.
#[sqlx::test]
async fn a_deeper_root_excludes_a_sibling_sharing_its_prefix(pool: PgPool) {
    let f = setup(pool).await;
    let analytics = create_namespace(&f.ctx, f.warehouse_id, &["analytics"]).await;
    let finance = create_namespace(&f.ctx, f.warehouse_id, &["analytics", "finance"]).await;
    let archive = create_namespace(&f.ctx, f.warehouse_id, &["analytics", "finance_archive"]).await;
    let quarter = create_namespace(&f.ctx, f.warehouse_id, &["analytics", "finance", "q1"]).await;
    let table = create_table(
        &f.ctx,
        f.warehouse_id,
        &["analytics", "finance", "q1"],
        "t1",
    )
    .await;
    let warehouse_id = f.warehouse_id;
    seed(
        &f.ctx,
        vec![
            (
                &f.alice,
                "get_metadata",
                GrantResource::Namespace {
                    warehouse_id,
                    namespace_id: analytics,
                },
            ),
            (
                &f.alice,
                "get_metadata",
                GrantResource::Namespace {
                    warehouse_id,
                    namespace_id: finance,
                },
            ),
            (
                &f.alice,
                "get_metadata",
                GrantResource::Namespace {
                    warehouse_id,
                    namespace_id: archive,
                },
            ),
            (
                &f.alice,
                "get_metadata",
                GrantResource::Namespace {
                    warehouse_id,
                    namespace_id: quarter,
                },
            ),
            (
                &f.alice,
                "get_metadata",
                GrantResource::Table {
                    warehouse_id,
                    table_id: table,
                },
            ),
        ],
    )
    .await;

    let page = Server::list_namespace_subtree_grants(
        warehouse_id,
        finance,
        f.ctx.clone(),
        f.metadata.clone(),
        ListSubtreeGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(
        listed(page.grants),
        sorted(vec![
            (
                UserOrRole::User(f.alice.clone()),
                "get_metadata".to_string(),
                GrantResourceResponse::Namespace {
                    warehouse_id,
                    namespace_id: finance
                }
            ),
            (
                UserOrRole::User(f.alice.clone()),
                "get_metadata".to_string(),
                GrantResourceResponse::Namespace {
                    warehouse_id,
                    namespace_id: quarter
                }
            ),
            (
                UserOrRole::User(f.alice.clone()),
                "get_metadata".to_string(),
                GrantResourceResponse::Table {
                    warehouse_id,
                    table_id: table
                }
            ),
        ])
    );

    // The revoke's own membership recheck runs under the lock: same three go.
    let response = Server::revoke_namespace_subtree_grants(
        warehouse_id,
        finance,
        f.ctx.clone(),
        f.metadata.clone(),
        RevokeSubtreeGrantsRequest { ..revoke_all() },
    )
    .await
    .unwrap();
    assert_eq!(response.revoked, 3);

    // The ancestor and the prefix-sharing sibling are untouched.
    for namespace_id in [analytics, archive] {
        let remaining = PostgresBackend::list_grants(
            &lakekeeper::service::authz::GrantFilter::on(
                GrantResource::Namespace {
                    warehouse_id,
                    namespace_id,
                },
                None,
            ),
            no_pagination(),
            f.ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap();
        assert_eq!(remaining.grants.len(), 1);
    }
}

// ---------------------------------------------------------------------------
// Authorization
// ---------------------------------------------------------------------------

/// `ReadSubtreeGrants` on the root covers every member: a resource the caller could not
/// grant-read on its own is still part of the answer, and pages come back full. Losing
/// the root itself loses entry.
#[sqlx::test]
async fn subtree_read_at_the_root_covers_members_the_caller_cannot_see(pool: PgPool) {
    let authorizer = HidingAuthorizer::new();
    let (ctx, warehouse) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(authorizer.clone())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;
    let warehouse_id = warehouse.warehouse_id;
    let alice = UserId::try_from("oidc~alice").unwrap();
    provision_user(&ctx, &alice).await;
    let metadata = RequestMetadataTestBuilder::builder()
        .actor(Actor::Principal(alice.clone()))
        .project_id(Some(warehouse.project_id.clone()))
        .build();

    let parent = create_namespace(&ctx, warehouse_id, &["parent"]).await;
    let child = create_namespace(&ctx, warehouse_id, &["parent", "child"]).await;
    seed(
        &ctx,
        vec![
            (
                &alice,
                "get_metadata",
                GrantResource::Namespace {
                    warehouse_id,
                    namespace_id: parent,
                },
            ),
            (
                &alice,
                "get_metadata",
                GrantResource::Namespace {
                    warehouse_id,
                    namespace_id: child,
                },
            ),
        ],
    )
    .await;

    // Control: both members are readable.
    let page = DenyServer::list_namespace_subtree_grants(
        warehouse_id,
        parent,
        ctx.clone(),
        metadata.clone(),
        ListSubtreeGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants.len(), 2);

    authorizer.hide(&format!("namespace:{child}"));
    let page = DenyServer::list_namespace_subtree_grants(
        warehouse_id,
        parent,
        ctx.clone(),
        metadata.clone(),
        ListSubtreeGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants.len(), 2);

    // Hiding the root itself is a denial, not an empty page: the caller loses entry.
    authorizer.hide(&format!("namespace:{parent}"));
    let err = DenyServer::list_namespace_subtree_grants(
        warehouse_id,
        parent,
        ctx.clone(),
        metadata,
        ListSubtreeGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 404);
}

/// A subtree under a denying authorizer, with a grant on the root and one on its child.
async fn denying_tree(
    pool: PgPool,
    grant_ops: &'static [GrantOp],
) -> (
    DenyCtx,
    RequestMetadata,
    WarehouseId,
    NamespaceId,
    NamespaceId,
) {
    let authorizer = HidingAuthorizer::new().with_grant_authority(grant_ops);
    let (ctx, warehouse) = SetupTestCatalog::builder()
        .pool(pool)
        .storage_profile(memory_io_profile())
        .authorizer(authorizer)
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;
    let warehouse_id = warehouse.warehouse_id;
    let alice = UserId::try_from("oidc~alice").unwrap();
    provision_user(&ctx, &alice).await;
    let metadata = RequestMetadataTestBuilder::builder()
        .actor(Actor::Principal(alice.clone()))
        .project_id(Some(warehouse.project_id.clone()))
        .build();
    let parent = create_namespace(&ctx, warehouse_id, &["parent"]).await;
    let child = create_namespace(&ctx, warehouse_id, &["parent", "child"]).await;
    seed(
        &ctx,
        vec![
            (
                &alice,
                "get_metadata",
                GrantResource::Namespace {
                    warehouse_id,
                    namespace_id: parent,
                },
            ),
            (
                &alice,
                "get_metadata",
                GrantResource::Namespace {
                    warehouse_id,
                    namespace_id: child,
                },
            ),
        ],
    )
    .await;
    (ctx, metadata, warehouse_id, parent, child)
}

/// Revoke authority is one action, asked once at the root: without
/// `RevokeSubtreeGrants` there, the whole batch is refused and nothing is removed — a
/// "successful" call that left grants in place would be a security misreport. The
/// refusal names no privilege: with a content-independent gate there is nothing
/// content-derived to leak.
#[sqlx::test]
async fn no_revoke_authority_refuses_the_whole_batch(pool: PgPool) {
    let (ctx, metadata, warehouse_id, parent, child) =
        denying_tree(pool, &[GrantOp::Grant, GrantOp::Revoke]).await;
    ctx.v1_state.authz.block_action(&format!(
        "namespace:{:?}",
        lakekeeper::service::authz::CatalogNamespaceAction::RevokeSubtreeGrants
    ));

    let err = DenyServer::revoke_namespace_subtree_grants(
        warehouse_id,
        parent,
        ctx.clone(),
        metadata,
        RevokeSubtreeGrantsRequest { ..revoke_all() },
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 403);
    assert_eq!(err.error.r#type, "GrantActionForbidden");

    // The descendant's grant is untouched, so the refusal really removed nothing.
    let page = PostgresBackend::list_grants(
        &lakekeeper::service::authz::GrantFilter::on(
            GrantResource::Namespace {
                warehouse_id,
                namespace_id: child,
            },
            None,
        ),
        no_pagination(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants.len(), 1);
}

/// The warehouse-rooted gate is a separate branch from the namespace-rooted one, with
/// its own refusal shape: both mask as not-found, each naming the resource it was asked
/// about.
#[sqlx::test]
async fn a_warehouse_revoke_without_subtree_read_is_refused(pool: PgPool) {
    let (ctx, metadata, warehouse_id, _parent, child) =
        denying_tree(pool, &[GrantOp::Grant, GrantOp::Revoke]).await;
    ctx.v1_state.authz.block_action(&format!(
        "warehouse:{:?}",
        lakekeeper::service::authz::CatalogWarehouseAction::ReadSubtreeGrants
    ));

    let err = DenyServer::revoke_warehouse_subtree_grants(
        warehouse_id,
        ctx.clone(),
        metadata,
        RevokeSubtreeGrantsRequest { ..revoke_all() },
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 404);
    assert_eq!(err.error.r#type, "NoSuchWarehouseException");

    let page = PostgresBackend::list_grants(
        &lakekeeper::service::authz::GrantFilter::on(
            GrantResource::Namespace {
                warehouse_id,
                namespace_id: child,
            },
            None,
        ),
        no_pagination(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants.len(), 1);
}

/// Reading a warehouse's subtree grants does not carry revoking them: the batch is
/// refused, by an error that names no privilege.
#[sqlx::test]
async fn a_warehouse_revoke_without_revoke_authority_is_refused(pool: PgPool) {
    let (ctx, metadata, warehouse_id, _parent, child) =
        denying_tree(pool, &[GrantOp::Grant, GrantOp::Revoke]).await;
    ctx.v1_state.authz.block_action(&format!(
        "warehouse:{:?}",
        lakekeeper::service::authz::CatalogWarehouseAction::RevokeSubtreeGrants
    ));

    let err = DenyServer::revoke_warehouse_subtree_grants(
        warehouse_id,
        ctx.clone(),
        metadata,
        RevokeSubtreeGrantsRequest { ..revoke_all() },
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 403);
    assert_eq!(err.error.r#type, "GrantActionForbidden");

    let page = PostgresBackend::list_grants(
        &lakekeeper::service::authz::GrantFilter::on(
            GrantResource::Namespace {
                warehouse_id,
                namespace_id: child,
            },
            None,
        ),
        no_pagination(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants.len(), 1);
}

/// The warehouse-rooted listing has its own gate, and it refuses differently from the
/// revoke's: a listing asks through the visibility-aware path, so a caller who may see
/// the warehouse is told the permission is missing. The revoke asks the action bare —
/// an authority holder needs no `describe` — and therefore masks every refusal as
/// not-found. A caller who may not see the warehouse gets not-found from either.
#[sqlx::test]
async fn a_warehouse_listing_without_subtree_read_is_refused(pool: PgPool) {
    let (ctx, metadata, warehouse_id, _parent, _child) = denying_tree(pool, &[]).await;
    ctx.v1_state.authz.block_action(&format!(
        "warehouse:{:?}",
        lakekeeper::service::authz::CatalogWarehouseAction::ReadSubtreeGrants
    ));

    let err = DenyServer::list_warehouse_subtree_grants(
        warehouse_id,
        ctx.clone(),
        metadata.clone(),
        ListSubtreeGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 403);
    assert_eq!(err.error.r#type, "WarehouseActionForbidden");

    // Invisible instead of merely unreadable: not-found, disclosing nothing.
    ctx.v1_state
        .authz
        .hide(&format!("warehouse:{warehouse_id}"));
    let err = DenyServer::list_warehouse_subtree_grants(
        warehouse_id,
        ctx.clone(),
        metadata,
        ListSubtreeGrantsQuery::default(),
        no_pagination(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 404);
    assert_eq!(err.error.r#type, "NoSuchWarehouseException");
}

/// A revoke asks `ReadSubtreeGrants` on the root before it reads anything. Without that
/// gate the refusal itself answers whether the subtree holds a matching grant, and the
/// scan runs for anyone able to name the root.
///
/// Only `ReadSubtreeGrants` is blocked, so the refusal can only come from the read
/// gate — not from the revoke action, which stays allowed.
#[sqlx::test]
async fn a_revoke_without_grant_read_on_the_root_is_refused(pool: PgPool) {
    let (ctx, metadata, warehouse_id, parent, child) =
        denying_tree(pool, &[GrantOp::Grant, GrantOp::Revoke]).await;
    ctx.v1_state.authz.block_action(&format!(
        "namespace:{:?}",
        lakekeeper::service::authz::CatalogNamespaceAction::ReadSubtreeGrants
    ));

    let err = DenyServer::revoke_namespace_subtree_grants(
        warehouse_id,
        parent,
        ctx.clone(),
        metadata,
        RevokeSubtreeGrantsRequest { ..revoke_all() },
    )
    .await
    .unwrap_err();
    // The refusal an unreachable root gives, so it discloses neither the subtree's
    // contents nor whether anything matched.
    assert_eq!(err.error.code, 404);
    assert_eq!(err.error.r#type, "NoSuchNamespaceException");

    let page = PostgresBackend::list_grants(
        &lakekeeper::service::authz::GrantFilter::on(
            GrantResource::Namespace {
                warehouse_id,
                namespace_id: child,
            },
            None,
        ),
        no_pagination(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants.len(), 1);
}

/// `RevokeSubtreeGrants` at the root is the whole authorization: the descendant's grant
/// goes on the same one answer, with no per-grant or per-pair question asked.
#[sqlx::test]
async fn revoke_authority_at_the_root_covers_the_descendants(pool: PgPool) {
    let (ctx, metadata, warehouse_id, parent, _child) =
        denying_tree(pool, &[GrantOp::Revoke]).await;

    let response = DenyServer::revoke_namespace_subtree_grants(
        warehouse_id,
        parent,
        ctx,
        metadata,
        RevokeSubtreeGrantsRequest { ..revoke_all() },
    )
    .await
    .unwrap();
    assert_eq!(response.revoked, 2);
}

// ---------------------------------------------------------------------------
// Helpers that need the fixture
// ---------------------------------------------------------------------------

async fn soft_delete_table(f: &Fixture, table: TableId) {
    CatalogServer::drop_table(
        TableParameters {
            prefix: Some(Prefix::from(f.warehouse_id.to_string())),
            table: iceberg::TableIdent::new(
                NamespaceIdent::from_strs(["parent", "child"]).unwrap(),
                "t1".to_string(),
            ),
        },
        DropParams {
            purge_requested: false,
            force: false,
        },
        f.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    // The row must still be there, soft-deleted, or the test is asserting nothing.
    let still_there = PostgresBackend::list_grants(
        &lakekeeper::service::authz::GrantFilter::on(
            GrantResource::Table {
                warehouse_id: f.warehouse_id,
                table_id: table,
            },
            None,
        ),
        no_pagination(),
        f.ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    assert_eq!(still_there.grants.len(), 1);
}

async fn with_listener(ctx: &Ctx, listener: Arc<RevokedRecorder>) -> Ctx {
    ctx.v1_state.events.append(listener).await;
    ctx.clone()
}

/// The two phases of a revoke are separated by an authorizer round trip, and a tabular
/// can be renamed into another namespace in between — its grant row does not move with it.
/// Phase 2 therefore re-derives membership under the lock instead of deleting on id alone,
/// so a grant that has left the subtree is not removed by an authority that never covered
/// it. Driven phase-by-phase rather than raced, so the window is deterministic.
#[sqlx::test]
async fn a_tabular_renamed_out_between_the_phases_keeps_its_grant(pool: PgPool) {
    let f = setup(pool).await;
    let tree = build_tree(&f).await;
    create_namespace(&f.ctx, f.warehouse_id, &["elsewhere"]).await;

    let root = lakekeeper::service::authz::GrantSubtreeRoot::Namespace {
        warehouse_id: f.warehouse_id,
        namespace_id: tree.parent,
    };
    let filter = lakekeeper::service::authz::GrantSubtreeFilter {
        include_soft_deleted: true,
        ..Default::default()
    };

    // Phase 1: the table's grant is a candidate while the table is still under the root.
    let candidates = PostgresBackend::select_subtree_grant_candidates_impl(
        root,
        &filter,
        1000,
        f.ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    let table_resource = GrantResource::Table {
        warehouse_id: f.warehouse_id,
        table_id: tree.table,
    };
    assert!(
        candidates
            .candidates
            .iter()
            .any(|c| c.grant.resource == table_resource),
        "the table's grant should be a candidate before the rename"
    );
    let candidate_count = candidates.candidates.len();

    // The window: the table leaves the subtree, carrying no grant row with it.
    CatalogServer::rename_table(
        Some(Prefix::from(f.warehouse_id.to_string())),
        RenameTableRequest {
            source: iceberg::TableIdent::new(
                NamespaceIdent::from_strs(["parent", "child"]).unwrap(),
                "t1".to_string(),
            ),
            destination: iceberg::TableIdent::new(
                NamespaceIdent::new("elsewhere".to_string()),
                "t1".to_string(),
            ),
        },
        f.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    // Phase 2: everything still under the root goes; the moved-out table's grant does not.
    let removed = PostgresBackend::revoke_grant_candidates(
        root,
        &candidates.candidates,
        f.ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    assert_eq!(removed.len(), candidate_count - 1);
    assert!(
        !removed.iter().any(|spec| spec.resource == table_resource),
        "a grant on a resource that left the subtree must not be revoked"
    );

    let page = PostgresBackend::list_grants(
        &lakekeeper::service::authz::GrantFilter::on(table_resource, None),
        no_pagination(),
        f.ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    assert_eq!(page.grants.len(), 1);
}

/// A role in `project_id`, created through the store as the membership tests do.
async fn make_role<A: lakekeeper::service::authz::Authorizer>(
    ctx: &ApiContext<State<A, PostgresBackend, SecretsState>>,
    project_id: &lakekeeper::ProjectId,
    name: &str,
    source_id: &str,
) -> RoleId {
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
