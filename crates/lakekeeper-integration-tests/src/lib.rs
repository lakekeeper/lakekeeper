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
pub use internal_helper::*;
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
