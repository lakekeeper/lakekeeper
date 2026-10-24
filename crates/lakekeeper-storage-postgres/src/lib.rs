//! PostgreSQL catalog backend for Lakekeeper.
//!
//! Provides [`PostgresBackend`] (an implementation of
//! [`lakekeeper::service::CatalogStore`]), the [`SecretsState`] secrets
//! backend, [`PostgresAdvisoryLock`] for cross-replica maintenance
//! coordination, and the migrations runner used at startup.

mod advisory_lock;
mod bootstrap;
mod cache_change_log;
mod catalog;
pub mod config;
pub(crate) mod dbutils;
pub mod endpoint_statistics;
pub(crate) mod grant;
pub(crate) mod idempotency;
pub mod migrations;
pub mod namespace;
mod pagination;
pub(crate) mod pool_metrics;
pub mod role;
pub(crate) mod role_assignment;
pub(crate) mod secrets;
pub mod tabular;
pub(crate) mod tag;
pub mod tasks;
pub(crate) mod user;
pub mod warehouse;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;
use std::{str::FromStr, sync::Arc};

pub use advisory_lock::PostgresAdvisoryLock;
use anyhow::anyhow;
use async_trait::async_trait;
pub use endpoint_statistics::sink::PostgresStatisticsSink;
use lakekeeper::{
    api::{ErrorModel, Result},
    service::{
        COMMIT_STATE_UNKNOWN_ERROR_TYPE, StateOrTransaction, StateOrTransactionEnum, Transaction,
        health::{Health, HealthExt, HealthStatus},
    },
};
pub use secrets::SecretsState;
use sqlx::{
    ConnectOptions, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
pub use tabular::DeletionKind;
#[cfg(test)]
pub(crate) use test_utils as tests;
use tokio::sync::RwLock;

use self::{
    config::{CONFIG, DynAppConfig, PgSchema, PgSslMode},
    dbutils::{DBErrorHandler, commit_state_unknown},
};

/// # Errors
/// Returns an error if the pool creation fails.
pub async fn get_reader_pool(pool_opts: PgPoolOptions) -> anyhow::Result<PgPool> {
    let pool = pool_opts
        .connect_with(build_connect_ops(ConnectionType::Read)?)
        .await
        .map_err(|e| anyhow::anyhow!(e).context("Error creating read pool."))?;
    Ok(pool)
}

/// # Errors
/// Returns an error if the pool cannot be created.
pub async fn get_writer_pool(pool_opts: PgPoolOptions) -> anyhow::Result<PgPool> {
    let pool = pool_opts
        .connect_with(build_connect_ops(ConnectionType::Write)?)
        .await
        .map_err(|e| anyhow::anyhow!(e).context("Error creating write pool."))?;
    Ok(pool)
}

#[derive(Debug, Clone)]
pub struct PostgresBackend {}

#[derive(Debug)]
pub struct PostgresTransaction {
    transaction: sqlx::Transaction<'static, sqlx::Postgres>,
}

type PostgresTransactionType<'a> = &'a mut sqlx::Transaction<'static, sqlx::Postgres>;

#[async_trait::async_trait]
impl Transaction<CatalogState> for PostgresTransaction {
    type Transaction<'a> = PostgresTransactionType<'a>;

    async fn begin_write(db_state: CatalogState) -> Result<Self> {
        let transaction = db_state.write_pool().begin().await.map_err(|e| {
            if crate::pool_metrics::is_pool_timeout(&e) {
                crate::pool_metrics::record_acquire_timeout("write");
            }
            e.into_error_model("Error starting transaction".to_string())
        })?;

        Ok(Self { transaction })
    }

    async fn begin_read(db_state: CatalogState) -> Result<Self> {
        let transaction = db_state
            .read_pool()
            .begin_with("BEGIN READ ONLY")
            .await
            .map_err(|e| {
                if crate::pool_metrics::is_pool_timeout(&e) {
                    crate::pool_metrics::record_acquire_timeout("read");
                }
                e.into_error_model("Error starting transaction".to_string())
            })?;

        Ok(Self { transaction })
    }

    async fn commit(self) -> Result<()> {
        self.transaction.commit().await.map_err(|e| {
            if commit_state_unknown(&e) {
                ErrorModel::internal(
                    "Outcome of the transaction commit is unknown",
                    COMMIT_STATE_UNKNOWN_ERROR_TYPE,
                    Some(Box::new(e)),
                )
            } else {
                e.into_error_model("Error committing transaction".to_string())
            }
        })?;
        Ok(())
    }

    async fn rollback(self) -> Result<()> {
        self.transaction
            .rollback()
            .await
            .map_err(|e| e.into_error_model("Error rolling back transaction".to_string()))?;
        Ok(())
    }

    fn transaction(&mut self) -> Self::Transaction<'_> {
        &mut self.transaction
    }
}

#[derive(Clone, Debug)]
pub struct ReadWrite {
    pub(crate) read_pool: PgPool,
    pub(crate) write_pool: PgPool,
    pub(crate) health: Arc<RwLock<Vec<Health>>>,
}

#[async_trait]
impl HealthExt for ReadWrite {
    async fn health(&self) -> Vec<Health> {
        self.health.read().await.clone()
    }

    async fn update_health(&self) {
        let read = self.read_health().await;
        let write = self.write_health().await;
        let mut lock = self.health.write().await;
        lock.clear();
        lock.extend([
            Health::now("read_pool", read),
            Health::now("write_pool", write),
        ]);
    }
}

impl ReadWrite {
    #[must_use]
    pub fn from_pools(read_pool: PgPool, write_pool: PgPool) -> Self {
        Self {
            read_pool,
            write_pool,
            health: Arc::new(RwLock::new(vec![
                Health::now("read_pool", HealthStatus::Unknown),
                Health::now("write_pool", HealthStatus::Unknown),
            ])),
        }
    }

    async fn health(pool: PgPool) -> HealthStatus {
        match sqlx::query("SELECT 1").fetch_one(&pool).await {
            Ok(_) => HealthStatus::Healthy,
            Err(e) => {
                tracing::warn!(?e, ?pool, "Pool is unhealthy");
                HealthStatus::Unhealthy
            }
        }
    }

    async fn write_health(&self) -> HealthStatus {
        Self::health(self.write_pool.clone()).await
    }

    async fn read_health(&self) -> HealthStatus {
        Self::health(self.read_pool.clone()).await
    }
}

#[derive(Clone, Debug)]
pub struct CatalogState {
    pub read_write: ReadWrite,
}

#[async_trait]
impl HealthExt for CatalogState {
    async fn health(&self) -> Vec<Health> {
        self.read_write.health().await
    }

    async fn update_health(&self) {
        self.read_write.update_health().await;
    }
}

impl CatalogState {
    #[must_use]
    pub fn from_pools(read_pool: PgPool, write_pool: PgPool) -> Self {
        Self {
            read_write: ReadWrite::from_pools(read_pool, write_pool),
        }
    }

    #[must_use]
    pub fn read_pool(&self) -> PgPool {
        self.read_write.read_pool.clone()
    }

    #[must_use]
    pub fn write_pool(&self) -> PgPool {
        self.read_write.write_pool.clone()
    }

    /// Spawn a detached background task that samples both pools' connection
    /// stats into Prometheus gauges every [`pool_metrics::SAMPLE_INTERVAL`].
    ///
    /// Detached on purpose: sampling is stateless and needs no graceful
    /// shutdown — the task is dropped when the runtime stops. Any ticks before
    /// the global metrics recorder is installed are harmless no-ops.
    pub fn spawn_pool_metrics(&self) {
        let read_pool = self.read_pool();
        let write_pool = self.write_pool();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(pool_metrics::SAMPLE_INTERVAL);
            loop {
                ticker.tick().await;
                pool_metrics::record(&read_pool, &write_pool);
            }
        });
    }
}

impl<'txn> StateOrTransaction<CatalogState, PostgresTransactionType<'txn>> for CatalogState {
    fn as_enum_mut<'b>(
        &'b mut self,
    ) -> StateOrTransactionEnum<'b, CatalogState, PostgresTransactionType<'txn>> {
        StateOrTransactionEnum::State(self.clone())
    }
}

impl<'txn> StateOrTransaction<CatalogState, PostgresTransactionType<'txn>>
    for PostgresTransactionType<'txn>
{
    fn as_enum_mut<'b>(
        &'b mut self,
    ) -> StateOrTransactionEnum<'b, CatalogState, PostgresTransactionType<'txn>> {
        StateOrTransactionEnum::Transaction(self)
    }
}

/// Install a per-connection `search_path` so Lakekeeper's tables live in
/// `schema`. `None` installs no hook.
///
/// `set_config` takes the name as a bind parameter and is equivalent to a
/// session-level `SET search_path`, which cannot be used here because `SET`
/// takes no parameters.
///
/// A pooler in transaction or statement pooling mode does not keep a
/// session-level `SET`. Use `ALTER ROLE <role> SET search_path` there instead:
/// [`migrations::migrate`] checks `current_schema()` and fails rather than
/// migrating into the wrong schema.
#[must_use]
pub fn with_search_path(pool_opts: PgPoolOptions, schema: Option<&PgSchema>) -> PgPoolOptions {
    let Some(schema) = schema else {
        return pool_opts;
    };
    // Rendered once here; the hook only clones the finished string.
    let search_path = schema.search_path_value();
    pool_opts.after_connect(move |conn, _meta| {
        let search_path = search_path.clone();
        Box::pin(async move {
            sqlx::query("SELECT set_config('search_path', $1, false)")
                .bind(search_path)
                .execute(&mut *conn)
                .await?;
            Ok(())
        })
    })
}

impl DynAppConfig {
    pub fn to_pool_opts(&self) -> PgPoolOptions {
        with_search_path(
            sqlx::pool::PoolOptions::default()
                .test_before_acquire(self.pg_test_before_acquire)
                .acquire_timeout(core::time::Duration::from_secs(self.pg_acquire_timeout))
                .max_lifetime(
                    self.pg_connection_max_lifetime
                        .map(core::time::Duration::from_secs),
                ),
            self.pg_schema.as_ref(),
        )
    }
}

#[derive(Debug, Clone, Copy)]
enum ConnectionType {
    Read,
    Write,
}

fn build_connect_ops(typ: ConnectionType) -> anyhow::Result<PgConnectOptions> {
    let url = match typ {
        ConnectionType::Read => CONFIG
            .pg_database_url_read
            .as_deref()
            .or(CONFIG.pg_database_url_write.as_deref()),
        ConnectionType::Write => CONFIG.pg_database_url_write.as_deref(),
    };

    let host = match typ {
        ConnectionType::Read => CONFIG.pg_host_r.as_deref().or(CONFIG.pg_host_w.as_deref()),
        ConnectionType::Write => CONFIG.pg_host_w.as_deref(),
    };
    let opts = if let Some(cfg) = url {
        PgConnectOptions::from_str(cfg)?
    } else {
        PgConnectOptions::new()
            .host(host.ok_or(anyhow!(
                "A connection string or postgres host must be provided."
            ))?)
            .port(CONFIG.pg_port.ok_or(anyhow!(
                "A connection string or postgres port must be provided."
            ))?)
            .username(CONFIG.pg_user.as_deref().ok_or(anyhow!(
                "A connection string or postgres user must be provided."
            ))?)
            .password(CONFIG.pg_password.as_deref().ok_or(anyhow!(
                "A connection string or postgres password must be provided."
            ))?)
            .database(CONFIG.pg_database.as_deref().ok_or(anyhow!(
                "A connection string or postgres database must be provided."
            ))?)
            .ssl_mode(CONFIG.pg_ssl_mode.unwrap_or(PgSslMode::Prefer).into())
    };
    let opts = if let Some(cert) = CONFIG.pg_ssl_root_cert.as_deref() {
        opts.ssl_root_cert(cert)
    } else {
        opts
    };
    let opts = if CONFIG.pg_enable_statement_logging {
        opts
    } else {
        opts.disable_statement_logging()
    };

    let conn_type = match typ {
        ConnectionType::Read => "read",
        ConnectionType::Write => "write",
    };
    tracing::info!(
        host = opts.get_host(),
        port = opts.get_port(),
        database = opts.get_database(),
        username = opts.get_username(),
        schema = CONFIG.pg_schema.as_ref().map(PgSchema::as_str),
        "Building PostgreSQL {conn_type} connection"
    );

    Ok(opts)
}

#[cfg(test)]
mod search_path_tests {
    use sqlx::{AssertSqlSafe, PgPool, postgres::PgPoolOptions};
    use uuid::Uuid;

    use super::{config::PgSchema, with_search_path};

    /// Build a pool against the same database as `admin_pool`, optionally with
    /// the `search_path` hook installed.
    async fn pool_with(admin_pool: &PgPool, schema: Option<&PgSchema>) -> PgPool {
        let opts = (*admin_pool.connect_options()).clone();
        with_search_path(PgPoolOptions::new().max_connections(2), schema)
            .connect_with(opts)
            .await
            .unwrap()
    }

    #[sqlx::test(migrations = false)]
    async fn test_search_path_hook_sets_current_schema(admin_pool: PgPool) {
        // Mixed case on purpose: the hook quotes the name, so Postgres has to
        // keep it verbatim. Were the quoting dropped, the name would be read as
        // lower case and every assertion below would fail.
        let name = format!("LkHook_{}", Uuid::new_v4().simple());
        sqlx::query(AssertSqlSafe(format!(r#"CREATE SCHEMA "{name}""#)))
            .execute(&admin_pool)
            .await
            .unwrap();
        let schema = name.parse::<PgSchema>().unwrap();

        let pool = pool_with(&admin_pool, Some(&schema)).await;

        // Hold two connections at once: the hook must run per connection, not
        // once per pool.
        let mut first = pool.acquire().await.unwrap();
        let mut second = pool.acquire().await.unwrap();
        for conn in [&mut first, &mut second] {
            let current: String = sqlx::query_scalar("SELECT current_schema()::text")
                .fetch_one(&mut **conn)
                .await
                .unwrap();
            assert_eq!(current, name);
        }

        // `public` stays on the path so unqualified extension objects resolve.
        let path: Vec<String> = sqlx::query_scalar("SELECT current_schemas(false)")
            .fetch_one(&mut *first)
            .await
            .unwrap();
        assert_eq!(path, vec![name.clone(), "public".to_string()]);

        drop(first);
        drop(second);
        pool.close().await;
    }

    #[sqlx::test(migrations = false)]
    async fn test_no_hook_when_schema_unset(admin_pool: PgPool) {
        let pool = pool_with(&admin_pool, None).await;

        let current: String = sqlx::query_scalar("SELECT current_schema()::text")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(current, "public");

        pool.close().await;
    }
}

#[cfg(test)]
mod transaction_tests {
    use sqlx::AssertSqlSafe;

    use super::*;

    #[sqlx::test]
    async fn read_transactions_refuse_writes(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool);
        let mut transaction = PostgresTransaction::begin_read(state).await.unwrap();

        let error = sqlx::query("CREATE TEMP TABLE read_only_probe (id int)")
            .execute(&mut **transaction.transaction())
            .await
            .unwrap_err();

        // 25006: read_only_sql_transaction
        assert_eq!(
            error.as_database_error().and_then(|e| e.code()).as_deref(),
            Some("25006")
        );
    }

    async fn probe_rows(pool: &PgPool) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM commit_probe")
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// Begin a transaction that inserts into `commit_probe`; returns it and its backend pid.
    async fn insert_probe(pool: &PgPool) -> (PostgresTransaction, i32) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let mut transaction = PostgresTransaction::begin_write(state).await.unwrap();
        let pid =
            sqlx::query_scalar("INSERT INTO commit_probe VALUES (1) RETURNING pg_backend_pid()")
                .fetch_one(&mut **transaction.transaction())
                .await
                .unwrap();
        (transaction, pid)
    }

    #[sqlx::test(migrations = false)]
    async fn commit_failing_in_deferred_trigger_is_classified_by_sqlstate(pool: PgPool) {
        sqlx::raw_sql(
            "CREATE TABLE commit_probe (id int);
            CREATE TABLE probe_errcode (code text);
            CREATE FUNCTION reject_probe() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                    RAISE EXCEPTION 'rejected at commit' USING ERRCODE = (SELECT code FROM probe_errcode);
                END $$;
            CREATE CONSTRAINT TRIGGER reject_probe AFTER INSERT ON commit_probe
                DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_probe();",
        )
        .execute(&pool)
        .await
        .unwrap();

        for (code, expected_type) in [
            ("P0001", "DatabaseError"),
            ("08006", COMMIT_STATE_UNKNOWN_ERROR_TYPE),
            ("40003", COMMIT_STATE_UNKNOWN_ERROR_TYPE),
            ("57014", COMMIT_STATE_UNKNOWN_ERROR_TYPE),
            ("XX000", COMMIT_STATE_UNKNOWN_ERROR_TYPE),
        ] {
            sqlx::raw_sql(AssertSqlSafe(format!(
                "TRUNCATE probe_errcode; INSERT INTO probe_errcode VALUES ('{code}')"
            )))
            .execute(&pool)
            .await
            .unwrap();
            let (transaction, _) = insert_probe(&pool).await;

            let error = transaction.commit().await.unwrap_err();

            assert_eq!(error.error.r#type, expected_type, "SQLSTATE {code}");
            assert_eq!(probe_rows(&pool).await, 0, "SQLSTATE {code}");
        }
    }

    #[sqlx::test(migrations = false)]
    async fn commit_on_terminated_session_is_unknown(pool: PgPool) {
        sqlx::raw_sql("CREATE TABLE commit_probe (id int)")
            .execute(&pool)
            .await
            .unwrap();
        let (transaction, pid) = insert_probe(&pool).await;
        let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1, 5000)")
            .bind(pid)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(terminated);

        let error = transaction.commit().await.unwrap_err();

        // FATAL 57P01 also arrives for a commit that was applied, if the signal comes during it.
        assert_eq!(error.error.r#type, COMMIT_STATE_UNKNOWN_ERROR_TYPE);
        assert_eq!(probe_rows(&pool).await, 0);
    }

    #[sqlx::test(migrations = false)]
    async fn commit_after_idle_in_transaction_timeout_is_rolled_back(pool: PgPool) {
        sqlx::raw_sql("CREATE TABLE commit_probe (id int)")
            .execute(&pool)
            .await
            .unwrap();
        let (mut transaction, _) = insert_probe(&pool).await;
        sqlx::raw_sql("SET idle_in_transaction_session_timeout = 200")
            .execute(&mut **transaction.transaction())
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(600)).await;

        let error = transaction.commit().await.unwrap_err();

        // 25P03 is mapped to a conflict like other aborted transactions.
        assert_eq!(error.error.r#type, "TransactionFailed");
        assert_eq!(probe_rows(&pool).await, 0);
    }

    #[test]
    fn commit_state_is_unknown_without_a_server_reply() {
        let dropped = sqlx::Error::Io(std::io::ErrorKind::UnexpectedEof.into());
        assert!(commit_state_unknown(&dropped));
        assert!(commit_state_unknown(&sqlx::Error::Protocol(
            "bad message".into()
        )));
    }
}
