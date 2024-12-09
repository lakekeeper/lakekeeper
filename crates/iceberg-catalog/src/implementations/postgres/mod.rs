mod bootstrap;
mod catalog;
pub(crate) mod dbutils;
pub mod migrations;
pub(crate) mod namespace;
mod pagination;
pub(crate) mod role;
pub(crate) mod secrets;
pub mod tabular;
pub mod task_queues;
pub(crate) mod user;
pub(crate) mod warehouse;

use self::dbutils::DBErrorHandler;
use crate::api::Result;
use crate::config::{DynAppConfig, PgSslMode};
use crate::service::health::{Health, HealthExt, HealthStatus};
use crate::CONFIG;
use anyhow::anyhow;
use async_trait::async_trait;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{ConnectOptions, Executor, PgPool};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;

pub use secrets::SecretsState;
pub use tabular::DeletionKind;

/// # Errors
/// Returns an error if the pool creation fails.
pub async fn get_reader_pool(pool_opts: PgPoolOptions) -> anyhow::Result<sqlx::PgPool> {
    let pool = pool_opts
        .connect_with(build_connect_ops(ConnectionType::Read)?)
        .await
        .map_err(|e| anyhow::anyhow!(e).context("Error creating read pool."))?;
    Ok(pool)
}

/// # Errors
/// Returns an error if the pool cannot be created.
pub async fn get_writer_pool(pool_opts: PgPoolOptions) -> anyhow::Result<sqlx::PgPool> {
    let pool = pool_opts
        .connect_with(build_connect_ops(ConnectionType::Write)?)
        .await
        .map_err(|e| anyhow::anyhow!(e).context("Error creating write pool."))?;
    Ok(pool)
}

#[derive(Debug, Clone)]
pub struct PostgresCatalog {}

#[derive(Debug)]

pub struct PostgresTransaction {
    transaction: sqlx::Transaction<'static, sqlx::Postgres>,
}

#[async_trait::async_trait]
impl crate::service::Transaction<CatalogState> for PostgresTransaction {
    type Transaction<'a> = &'a mut sqlx::Transaction<'static, sqlx::Postgres>;

    async fn begin_write(db_state: CatalogState) -> Result<Self> {
        let transaction = db_state
            .write_pool()
            .begin()
            .await
            .map_err(|e| e.into_error_model("Error starting transaction".to_string()))?;

        Ok(Self { transaction })
    }

    async fn begin_read(db_state: CatalogState) -> Result<Self> {
        let mut transaction = db_state
            .read_pool()
            .begin()
            .await
            .map_err(|e| e.into_error_model("Error starting transaction".to_string()))?;

        transaction
            .execute("SET TRANSACTION READ ONLY")
            .await
            .map_err(|e| {
                e.into_error_model("Error setting transaction to read-only".to_string())
            })?;
        Ok(Self { transaction })
    }

    async fn commit(self) -> Result<()> {
        self.transaction
            .commit()
            .await
            .map_err(|e| e.into_error_model("Error committing transaction".to_string()))?;
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
    pub read_pool: sqlx::PgPool,
    pub write_pool: sqlx::PgPool,
    pub health: Arc<RwLock<Vec<Health>>>,
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
            #[cfg(feature = "sqlx-postgres")]
            read_pool,
            #[cfg(feature = "sqlx-postgres")]
            write_pool,
            health: Arc::new(RwLock::new(vec![
                Health::now("read_pool", HealthStatus::Unknown),
                Health::now("write_pool", HealthStatus::Unknown),
            ])),
        }
    }

    #[cfg(feature = "sqlx-postgres")]
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
}

impl DynAppConfig {
    pub fn to_pool_opts(&self) -> PgPoolOptions {
        sqlx::pool::PoolOptions::default()
            .test_before_acquire(self.pg_test_before_acquire)
            .max_lifetime(
                self.pg_connection_max_lifetime
                    .map(core::time::Duration::from_secs),
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
    Ok(opts)
}
