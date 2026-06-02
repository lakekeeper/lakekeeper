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
