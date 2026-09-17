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

use std::{future::Future, time::Duration};

use sqlx::PgPool;

mod authz_helper;
mod dataset;
mod internal_helper;
mod pagination_macro; // exports `impl_pagination_tests!` via `#[macro_export]`
pub use authz_helper::*;
pub use dataset::*;
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
/// Assert a `loadTable`-shaped response advertises client-side scan planning.
///
/// `loadTable`, `createTable` and `registerTable` all return the same body, so all
/// three must carry it; asserting on the response rather than on the server-side
/// helper is what catches a path that was never wired up.
pub fn assert_advertises_client_planning(
    config: Option<&std::collections::HashMap<String, String>>,
    endpoint: &str,
) {
    let config = config.unwrap_or_else(|| panic!("{endpoint} must carry a config"));
    assert_eq!(
        config.get("scan-planning-mode").map(String::as_str),
        Some("client"),
        "{endpoint} must advertise client-side planning: {config:?}"
    );
}

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

/// Records the authorization outcomes a handler dispatches, so a test can assert
/// *what* was audited and *how often*.
///
/// Append it to `ctx.v1_state.events` after setup so only the calls under test
/// are captured.
#[derive(Debug, Default)]
pub struct CapturingAuthzListener {
    /// Both event vectors live under one lock so a count pair is a snapshot of a
    /// single instant: a dispatch cannot land between reading one and the other.
    events: std::sync::Mutex<CapturedAuthzEvents>,
}

#[derive(Debug, Default)]
struct CapturedAuthzEvents {
    succeeded: Vec<lakekeeper::service::events::AuthorizationSucceededEvent>,
    failed: Vec<lakekeeper::service::events::AuthorizationFailedEvent>,
}

impl std::fmt::Display for CapturingAuthzListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CapturingAuthzListener")
    }
}

#[lakekeeper::async_trait::async_trait]
impl lakekeeper::service::events::EventListener for CapturingAuthzListener {
    async fn authorization_succeeded(
        &self,
        event: lakekeeper::service::events::AuthorizationSucceededEvent,
    ) -> anyhow::Result<()> {
        self.events.lock().unwrap().succeeded.push(event);
        Ok(())
    }

    async fn authorization_failed(
        &self,
        event: lakekeeper::service::events::AuthorizationFailedEvent,
    ) -> anyhow::Result<()> {
        self.events.lock().unwrap().failed.push(event);
        Ok(())
    }
}

impl CapturingAuthzListener {
    /// The succeeded and failed counts as of one instant, read under a single
    /// lock acquisition.
    #[must_use]
    pub fn counts(&self) -> (usize, usize) {
        let events = self.events.lock().unwrap();
        (events.succeeded.len(), events.failed.len())
    }

    /// Both counts, after letting the dispatch settle.
    ///
    /// Events are dispatched from a spawned task, so wait until each count has
    /// reached what the caller expects, then drain the run queue so a *surplus*
    /// emit is caught rather than raced past. Both counts come from one lock
    /// acquisition, so the returned pair is a snapshot of a single instant.
    ///
    /// Assert on the tuple (`assert_eq!(l.settled_counts(2, 0).await, (2, 0))`)
    /// so both dimensions are pinned to exact values.
    #[must_use]
    pub async fn settled_counts(
        &self,
        expected_succeeded: usize,
        expected_failed: usize,
    ) -> (usize, usize) {
        for _ in 0..100 {
            let (succeeded, failed) = self.counts();
            if succeeded >= expected_succeeded && failed >= expected_failed {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        self.counts()
    }

    /// The failure reasons recorded so far, in order — so a test can pin *why* a
    /// request was denied, not merely that it was.
    ///
    /// Does not settle on its own; call it after [`Self::settled_counts`].
    #[must_use]
    pub fn failure_reasons(&self) -> Vec<lakekeeper::service::events::AuthorizationFailureReason> {
        self.events
            .lock()
            .unwrap()
            .failed
            .iter()
            .map(|e| e.failure_reason.clone())
            .collect()
    }

    /// The actions each succeeded and each failed record names, in order — so a test can
    /// pin which action a request was recorded under.
    ///
    /// Does not settle on its own; call it after [`Self::settled_counts`].
    #[must_use]
    pub fn recorded_actions(&self) -> (Vec<Vec<RecordedAction>>, Vec<Vec<RecordedAction>>) {
        fn recorded(
            actions: &[lakekeeper::service::authz::ActionDescriptor],
        ) -> Vec<RecordedAction> {
            actions
                .iter()
                .map(|a| RecordedAction {
                    action_name: a.action_name.to_string(),
                    context: a.context.clone(),
                })
                .collect()
        }
        let events = self.events.lock().unwrap();
        (
            events
                .succeeded
                .iter()
                .map(|e| recorded(&e.actions))
                .collect(),
            events.failed.iter().map(|e| recorded(&e.actions)).collect(),
        )
    }
}

/// One action of a captured authorization record: its name and its context keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedAction {
    pub action_name: String,
    pub context: Vec<lakekeeper::service::events::context::ActionContextKey>,
}

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

/// Holds the commit of every transaction that wrote to one table until
/// [`Self::release`], so a test can act while a writer waits inside its `COMMIT`.
///
/// A deferred constraint trigger runs at commit time and waits for an advisory
/// lock that the gate holds. The trigger lives in the test's own database.
pub struct CommitGate {
    connection: sqlx::pool::PoolConnection<sqlx::Postgres>,
    pool: sqlx::PgPool,
}

/// The advisory lock key a [`CommitGate`] holds.
const COMMIT_GATE_KEY: i64 = 727_101;

impl CommitGate {
    /// Gate commits of transactions that ran `operation` (`INSERT`, `UPDATE` or
    /// `DELETE`) on `table` (an identifier, quoted where Postgres needs it).
    ///
    /// # Panics
    /// If the trigger cannot be created or the lock cannot be taken.
    pub async fn install(pool: &sqlx::PgPool, table: &str, operation: &str) -> Self {
        sqlx::query(
            "CREATE OR REPLACE FUNCTION test_commit_gate() RETURNS trigger LANGUAGE plpgsql AS \
             $$ BEGIN PERFORM pg_advisory_xact_lock_shared(727101); RETURN NULL; END $$",
        )
        .execute(pool)
        .await
        .unwrap();
        let trigger_name: String = table.chars().filter(char::is_ascii_alphanumeric).collect();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "CREATE CONSTRAINT TRIGGER test_commit_gate_{trigger_name} AFTER {operation} ON \
             {table} DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION \
             test_commit_gate()"
        )))
        .execute(pool)
        .await
        .unwrap();
        let mut connection = pool.acquire().await.unwrap();
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(COMMIT_GATE_KEY)
            .execute(&mut *connection)
            .await
            .unwrap();
        Self {
            connection,
            pool: pool.clone(),
        }
    }

    /// Returns once a transaction waits at the gate.
    ///
    /// # Panics
    /// If none does within 10 seconds.
    pub async fn wait_for_a_held_commit(&self) {
        let waiting = async {
            loop {
                let waiting: i64 = sqlx::query_scalar(
                    "SELECT count(*) FROM pg_stat_activity \
                     WHERE datname = current_database() AND wait_event = 'advisory'",
                )
                .fetch_one(&self.pool)
                .await
                .unwrap();
                if waiting > 0 {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        };
        tokio::time::timeout(std::time::Duration::from_secs(10), waiting)
            .await
            .expect("a commit waits at the gate");
    }

    /// Let the held commits finish.
    ///
    /// # Panics
    /// If the lock cannot be released.
    pub async fn release(mut self) {
        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(COMMIT_GATE_KEY)
            .execute(&mut *self.connection)
            .await
            .unwrap();
    }
}

/// Polls `condition` until it holds.
///
/// # Panics
/// If it still fails after 10 seconds; `what` names it in the message.
pub async fn eventually<F, Fut>(what: &str, condition: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    eventually_within(what, Duration::from_secs(10), condition).await;
}

/// [`eventually`], for a condition that may take longer than 10 seconds.
///
/// # Panics
/// If it still fails after `timeout`; `what` names it in the message.
pub async fn eventually_within<F, Fut>(what: &str, timeout: Duration, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let polled = async {
        while !condition().await {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::time::timeout(timeout, polled)
        .await
        .unwrap_or_else(|_| panic!("{what}"));
}

/// Returns once a statement containing `query_fragment` waits on a lock: a test
/// holding that lock then knows the other side has reached it. An empty fragment
/// matches any statement.
///
/// # Panics
/// If none does within 10 seconds.
pub async fn wait_for_lock_wait(pool: &PgPool, query_fragment: &str) {
    eventually("the other side never waited on the lock", || async {
        let waiting: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_stat_activity
             WHERE datname = current_database() AND wait_event_type = 'Lock'
               AND strpos(query, $1) > 0",
        )
        .bind(query_fragment)
        .fetch_one(pool)
        .await
        .unwrap();
        waiting > 0
    })
    .await;
}
