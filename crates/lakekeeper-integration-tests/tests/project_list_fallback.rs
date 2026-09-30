//! The project listing of an authorizer that cannot enumerate projects asks it about
//! each project with `IncludeInList`.

use lakekeeper::{
    api::{
        ApiContext,
        management::v1::{
            ApiServer,
            project::{CreateProjectRequest, Service as _},
        },
    },
    service::{State, authz::tests::HidingAuthorizer},
};
use lakekeeper_storage_postgres::{
    PostgresBackend, SecretsState,
    test_utils::{SetupTestCatalog, memory_io_profile, random_request_metadata},
};
use sqlx::PgPool;

type Ctx = ApiContext<State<HidingAuthorizer, PostgresBackend, SecretsState>>;

/// `HidingAuthorizer` is `Clone` with shared state, so the test keeps a handle that
/// hides and blocks for the copy the catalog holds.
async fn setup(pool: PgPool, authorizer: HidingAuthorizer) -> (Ctx, [String; 2]) {
    let (ctx, warehouse) = SetupTestCatalog::builder()
        .pool(pool)
        .storage_profile(memory_io_profile())
        .authorizer(authorizer)
        .build()
        .setup()
        .await;
    let second = ApiServer::create_project(
        CreateProjectRequest {
            project_name: "second-project".to_string(),
            project_id: None,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    (
        ctx,
        [
            warehouse.project_id.to_string(),
            second.project_id.to_string(),
        ],
    )
}

async fn listed(ctx: Ctx) -> Vec<String> {
    let mut ids: Vec<String> = ApiServer::list_projects(ctx, random_request_metadata())
        .await
        .unwrap()
        .projects
        .into_iter()
        .map(|p| p.project_id.to_string())
        .collect();
    ids.sort();
    ids
}

#[sqlx::test]
async fn lists_the_projects_the_authorizer_includes(pool: PgPool) {
    let authorizer = HidingAuthorizer::new().with_unsupported_project_listing();
    let (ctx, [first, second]) = setup(pool, authorizer.clone()).await;

    let mut both = vec![first.clone(), second.clone()];
    both.sort();
    assert_eq!(listed(ctx.clone()).await, both);

    authorizer.hide(&format!("project:{second}"));
    assert_eq!(listed(ctx).await, vec![first]);
}

#[sqlx::test]
async fn asks_include_in_list_and_not_get_metadata(pool: PgPool) {
    let authorizer = HidingAuthorizer::new().with_unsupported_project_listing();
    let (ctx, [first, second]) = setup(pool, authorizer.clone()).await;

    authorizer.block_action("project:GetMetadata");
    let mut both = vec![first, second];
    both.sort();
    assert_eq!(listed(ctx.clone()).await, both);

    authorizer.block_action("project:IncludeInList");
    assert_eq!(listed(ctx).await, Vec::<String>::new());
}
