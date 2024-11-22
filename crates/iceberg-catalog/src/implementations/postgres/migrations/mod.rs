use crate::implementations::postgres::migrations::split_table_metadata::SplitTableMetadataHook;
use crate::implementations::postgres::{CatalogState, PostgresTransaction};
use crate::service::Transaction;
use anyhow::anyhow;
use futures::future::BoxFuture;
use sqlx::migrate::{AppliedMigration, Migrate, MigrateError, Migrator};
use sqlx::{Error, Postgres};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

mod split_table_metadata;

/// # Errors
/// Returns an error if the migration fails.
pub async fn migrate(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    let migrator = sqlx::migrate!();
    let mut hooks = get_data_migrations();
    let catalog_state = CatalogState::from_pools(pool.clone(), pool.clone());
    tracing::info!("Hooks: {:?}", hooks.keys().collect::<Vec<_>>());
    let mut trx = PostgresTransaction::begin_write(catalog_state.clone())
        .await
        .map_err(|e| e.error)?;
    let locking = true;
    let tr = trx.transaction();
    // lock the database for exclusive access by the migrator
    if locking {
        tr.lock().await?;
    }

    let applied_migrations = run_checks(&migrator, tr).await?;

    for migration in migrator.iter() {
        tracing::info!(%migration.version, %migration.description, "Current migration");
        let mut migration = migration.clone();
        // we are in a tx, so we don't need to start a new one
        migration.no_tx = true;
        if migration.migration_type.is_down_migration() {
            continue;
        }

        if let Some(applied_migration) = applied_migrations.get(&migration.version) {
            if migration.checksum != applied_migration.checksum {
                return Err(MigrateError::VersionMismatch(migration.version))?;
            }
            tracing::info!(%migration.version, "Migration already applied");
        } else {
            tr.apply(&migration).await?;
            tracing::info!(%migration.version, "Applying migration");
            if let Some(hook) = hooks.remove(&migration.version) {
                tracing::info!(%migration.version, "Running split_table_metadata migration");
                hook.apply(tr).await.map_err(|e| e.error)?;
                tracing::info!(%migration.version, "split_table_metadata migration complete");
            } else {
                tracing::info!(%migration.version, "No hook for migration");
            }
        }
    }

    // unlock the migrator to allow other migrators to run
    // but do nothing as we already migrated
    if locking {
        tr.unlock().await?;
    }
    trx.commit().await.map_err(|e| anyhow::anyhow!(e.error))?;
    Ok(())
}

async fn run_checks(
    migrator: &Migrator,
    tr: &mut sqlx::Transaction<'_, Postgres>,
) -> Result<HashMap<i64, AppliedMigration>, MigrateError> {
    // creates [_migrations] table only if needed
    // eventually this will likely migrate previous versions of the table
    tr.ensure_migrations_table().await?;

    let version = tr.dirty_version().await?;
    if let Some(version) = version {
        return Err(MigrateError::Dirty(version))?;
    }

    let applied_migrations = tr.list_applied_migrations().await?;
    validate_applied_migrations(&applied_migrations, migrator)?;

    let applied_migrations: HashMap<_, _> = applied_migrations
        .into_iter()
        .map(|m| (m.version, m))
        .collect();
    Ok(applied_migrations)
}

/// # Errors
/// Returns an error if db connection fails or if migrations are missing.
pub async fn check_migration_status(pool: &sqlx::PgPool) -> anyhow::Result<MigrationState> {
    let mut conn = pool.acquire().await?;
    let m = sqlx::migrate!();
    let applied_migrations = match conn.list_applied_migrations().await {
        Ok(migrations) => migrations,
        Err(e) => {
            if let MigrateError::Execute(Error::Database(db)) = &e {
                if db.code().as_deref() == Some("42P01") {
                    tracing::debug!(?db, "No migrations have been applied.");
                    return Ok(MigrationState::NoMigrationsTable);
                };
            };
            // we discard the error here since sqlx prefixes db errors with "while executing
            // migrations" which is not what we are doing here.
            tracing::debug!(?e, "Error listing applied migrations, even though the error may say different things, we are not applying migrations here.");
            return Err(anyhow!("Error listing applied migrations"));
        }
    };

    let to_be_applied = m
        .migrations
        .iter()
        .map(|mig| (mig.version, &*mig.checksum))
        .collect::<HashSet<_>>();
    let applied = applied_migrations
        .iter()
        .map(|mig| (mig.version, &*mig.checksum))
        .collect::<HashSet<_>>();
    let missing = to_be_applied.difference(&applied).collect::<HashSet<_>>();

    if missing.is_empty() {
        tracing::debug!("Migrations are up to date.");
        Ok(MigrationState::Complete)
    } else {
        tracing::debug!(?missing, "Migrations are missing.");
        Ok(MigrationState::Missing)
    }
}

#[derive(Debug, Copy, Clone)]
pub enum MigrationState {
    Complete,
    Missing,
    NoMigrationsTable,
}

pub trait MigrationHook: Send + Sync + 'static {
    fn apply<'c>(
        &self,
        trx: &'c mut sqlx::Transaction<'_, Postgres>,
    ) -> BoxFuture<'c, crate::api::Result<()>>;

    fn version() -> i64
    where
        Self: Sized;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Migration {
    version: i64,
    description: Cow<'static, str>,
}

fn get_data_migrations() -> HashMap<i64, Box<dyn MigrationHook>> {
    HashMap::from([(
        SplitTableMetadataHook::version(),
        Box::new(SplitTableMetadataHook) as Box<_>,
    )])
}

fn validate_applied_migrations(
    applied_migrations: &[AppliedMigration],
    migrator: &Migrator,
) -> Result<(), MigrateError> {
    if migrator.ignore_missing {
        return Ok(());
    }

    let migrations: HashSet<_> = migrator.iter().map(|m| m.version).collect();

    for applied_migration in applied_migrations {
        if !migrations.contains(&applied_migration.version) {
            return Err(MigrateError::VersionMissing(applied_migration.version));
        }
    }

    Ok(())
}
