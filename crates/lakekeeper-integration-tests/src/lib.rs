//! Integration-test helpers and harnesses for Lakekeeper.
//!
//! Today these are pinned to `lakekeeper-storage-postgres` as the
//! backend; the helpers are structured so that a future SQLite or
//! FoundationDB backend can be slotted in with minimal churn.
//!
//! Individual test files live under `tests/` (cargo's per-file
//! integration-test convention) and import from this crate root.
//!
//! Postgres-pinned helpers (`setup`, `memory_io_profile`,
//! `SetupTestCatalog`, `TestWarehouseResponse`, `spawn_build_in_queues`,
//! `random_request_metadata`) are re-exported from
//! [`lakekeeper_storage_postgres::test_utils`] so that crate's own inline
//! tests can use them without a dev-dep cycle.

mod internal_helper;
mod pagination_macro; // exports `impl_pagination_tests!` via `#[macro_export]`
pub use internal_helper::*;
// `pastey` is needed at the macro call sites because `impl_pagination_tests!`
// expands to `paste! { ... }`. Re-export it so downstream test files don't
// need to add a direct dep.
pub use pastey;

/// Lightweight setup specifically for the views test suite: applies
/// migrations, initializes one warehouse + one namespace via the storage
/// backend's `initialize_*` helpers (skipping the bootstrap / API flow used
/// by [`setup`]).
///
/// Returns `(ctx, namespace_ident, warehouse_id, project_id)` — the original
/// `crate::server::views::test::setup` signature.
pub async fn views_test_setup(
    pool: sqlx::PgPool,
    namespace_name: Option<Vec<String>>,
) -> (
    lakekeeper::api::ApiContext<
        lakekeeper::service::State<
            lakekeeper::service::authz::AllowAllAuthorizer,
            lakekeeper_storage_postgres::PostgresBackend,
            lakekeeper_storage_postgres::SecretsState,
        >,
    >,
    iceberg::NamespaceIdent,
    lakekeeper::WarehouseId,
    lakekeeper::service::ArcProjectId,
) {
    use lakekeeper::service::{
        authz::AllowAllAuthorizer,
        storage::{MemoryProfile, StorageProfile},
    };
    use lakekeeper_storage_postgres::{
        migrations::migrate_core_only, namespace::tests::initialize_namespace,
        warehouse::test::initialize_warehouse,
    };

    migrate_core_only(&pool).await.unwrap();
    let api_context = get_api_context(&pool, AllowAllAuthorizer::default()).await;
    let state = api_context.v1_state.catalog.clone();
    let (project_id, warehouse_id) = initialize_warehouse(
        state.clone(),
        Some(StorageProfile::Memory(MemoryProfile::default())),
        None,
        None,
        true,
    )
    .await;

    let namespace = initialize_namespace(
        state,
        warehouse_id,
        &iceberg::NamespaceIdent::from_vec(
            namespace_name.unwrap_or_else(|| vec![uuid::Uuid::now_v7().to_string()]),
        )
        .unwrap(),
        None,
    )
    .await
    .namespace_ident()
    .clone();
    (api_context, namespace, warehouse_id, project_id)
}

/// Mirrors the original `crate::server::views::load::test::load_view` test
/// helper — dispatches through `CatalogServer` so request validation and
/// authz fire as in production.
pub async fn load_view_helper(
    api_context: lakekeeper::api::ApiContext<
        lakekeeper::service::State<
            lakekeeper::service::authz::AllowAllAuthorizer,
            lakekeeper_storage_postgres::PostgresBackend,
            lakekeeper_storage_postgres::SecretsState,
        >,
    >,
    parameters: lakekeeper::api::iceberg::v1::ViewParameters,
) -> lakekeeper::api::Result<iceberg_ext::catalog::rest::LoadViewResult> {
    use lakekeeper::{
        api::iceberg::v1::views::{LoadViewRequest, ViewService},
        server::CatalogServer,
        service::{State, authz::AllowAllAuthorizer},
    };
    use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};

    <CatalogServer<PostgresBackend, AllowAllAuthorizer, SecretsState> as ViewService<
        State<AllowAllAuthorizer, PostgresBackend, SecretsState>,
    >>::load_view(
        parameters,
        LoadViewRequest::default(),
        api_context,
        lakekeeper::api::RequestMetadata::new_unauthenticated(),
    )
    .await
}

/// Wraps [`lakekeeper::server::views::create::create_view`] with default
/// `DataAccess` and `RequestMetadata` so test files don't have to import
/// every type at every call site. Mirrors the original
/// `crate::server::views::create::test::create_view` test helper.
pub async fn create_view_helper(
    api_context: lakekeeper::api::ApiContext<
        lakekeeper::service::State<
            lakekeeper::service::authz::AllowAllAuthorizer,
            lakekeeper_storage_postgres::PostgresBackend,
            lakekeeper_storage_postgres::SecretsState,
        >,
    >,
    namespace: iceberg::NamespaceIdent,
    rq: iceberg_ext::catalog::rest::CreateViewRequest,
    prefix: Option<String>,
) -> lakekeeper::api::Result<iceberg_ext::catalog::rest::LoadViewResult> {
    use lakekeeper::api::iceberg::{
        types::Prefix,
        v1::{DataAccess, NamespaceParameters},
    };

    Box::pin(lakekeeper::server::views::create::create_view(
        NamespaceParameters {
            namespace,
            prefix: Some(Prefix(prefix.unwrap_or_else(|| {
                "b8683712-3484-11ef-a305-1bc8771ed40c".to_string()
            }))),
        },
        rq,
        api_context,
        DataAccess {
            vended_credentials: true,
            remote_signing: false,
        },
        lakekeeper::api::RequestMetadata::new_unauthenticated(),
    ))
    .await
}

/// 6-argument wrapper around [`setup`] that defaults `number_of_warehouses=1`
/// and `project_id=None`. Preserves the original `crate::server::test::setup`
/// signature for tests extracted from lakekeeper that pre-date the
/// num-warehouses / project-id arguments.
#[allow(clippy::too_many_arguments)]
pub async fn setup_simple<T: lakekeeper::service::authz::Authorizer>(
    pool: sqlx::PgPool,
    storage_profile: lakekeeper::service::storage::StorageProfile,
    storage_credential: Option<lakekeeper::service::storage::StorageCredential>,
    authorizer: T,
    delete_profile: lakekeeper::api::management::v1::warehouse::TabularDeleteProfile,
    user_id: Option<lakekeeper::service::UserId>,
) -> (
    lakekeeper::api::ApiContext<
        lakekeeper::service::State<
            T,
            lakekeeper_storage_postgres::PostgresBackend,
            lakekeeper_storage_postgres::SecretsState,
        >,
    >,
    TestWarehouseResponse,
) {
    setup(
        pool,
        storage_profile,
        storage_credential,
        authorizer,
        delete_profile,
        user_id,
        1,
        None,
    )
    .await
}
pub use lakekeeper_storage_postgres::test_utils::{
    SetupTestCatalog, TestWarehouseResponse, get_api_context, get_api_context_with_registry,
    memory_io_profile, random_request_metadata, s3_compatible_profile, setup, setup_with_registry,
    spawn_build_in_queues, tabular_test_multi_warehouse_setup,
};

/// Test-only public reach into [`lakekeeper::service::post_migration_hooks`]'s
/// `pub(crate)` backfill helper. Downstream test crates
/// drive specific spec lists through this wrapper to
/// avoid installing the process-wide registry (`OnceLock`), which would
/// pollute every other test in the same binary.
///
/// Production callers must go through
/// [`lakekeeper::service::run_post_migration_hooks`].
pub async fn upsert_system_roles_in_all_projects<C: lakekeeper::service::CatalogStore>(
    state: C::State,
    roles: &[lakekeeper::service::SystemRoleSpec],
) -> anyhow::Result<()> {
    lakekeeper::service::upsert_system_roles_in_all_projects::<C>(state, roles).await
}
