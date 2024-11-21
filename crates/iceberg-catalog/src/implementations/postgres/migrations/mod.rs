use crate::implementations::postgres::migrations::split_table_metadata::split_table_metadata;
use crate::implementations::postgres::{tabular, CatalogState, PostgresTransaction};
use crate::service::Transaction;
use anyhow::anyhow;
use futures::future::BoxFuture;
use futures::FutureExt;
use iceberg_ext::catalog::rest::IcebergErrorResponse;
use sqlx::error::DatabaseError;
use sqlx::migrate::{AppliedMigration, Migrate, MigrateError, MigrationType, Migrator};
use sqlx::{Error, Postgres};
use std::collections::{HashMap, HashSet};
use std::future::Future;

mod split_table_metadata;

#[derive(Debug, Eq, PartialEq, Hash)]
struct Migration {
    version: i64,
    description: String,
    is_up_migration: bool,
}

fn sqlx_migration_to_local_migration(sqlx_migration: &sqlx::migrate::Migration) -> Migration {
    Migration {
        version: sqlx_migration.version,
        description: sqlx_migration.description.to_string(),
        is_up_migration: sqlx_migration.migration_type.is_up_migration(),
    }
}

fn sqlx_migration_matches_migration(
    sqlx_migration: &sqlx::migrate::Migration,
    migration: &Migration,
) -> bool {
    migration.version == sqlx_migration.version
        && migration.description == sqlx_migration.description
        && migration.is_up_migration == sqlx_migration.migration_type.is_up_migration()
}

async fn other<'c>(tr: &mut sqlx::Transaction<'c, Postgres>) -> crate::api::Result<()> {
    Ok(())
}

pub fn get_data_migrations() -> HashMap<
    Migration,
    Box<
        dyn for<'c> Fn(
                &'c mut sqlx::Transaction<'_, Postgres>,
            ) -> BoxFuture<'c, crate::api::Result<()>>
            + Send
            + Sync
            + 'static,
    >,
> {
    [
        (
            Migration {
                version: 20_241_106_201_139,
                description: "split_table_metadata".to_string(),
                is_up_migration: true,
            },
            Box::new(|trx| split_table_metadata(trx).boxed()) as Box<_>,
        ),
        (
            Migration {
                version: 0,
                description: "".into(),
                is_up_migration: true,
            },
            Box::new(|trx| other(trx).boxed()) as Box<_>,
        ),
    ]
    .into_iter()
    .collect::<HashMap<_, _>>()
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

/// # Errors
/// Returns an error if the migration fails.
pub async fn migrate(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    let migrator = sqlx::migrate!();

    let catalog_state = CatalogState::from_pools(pool.clone(), pool.clone());
    let mut hooks = get_data_migrations();
    let mut trx = PostgresTransaction::begin_write(catalog_state.clone())
        .await
        .map_err(|e| e.error)?;
    let locking = true;
    {
        let tr = trx.transaction();
        // lock the database for exclusive access by the migrator
        if locking {
            tr.lock().await?;
        }

        // creates [_migrations] table only if needed
        // eventually this will likely migrate previous versions of the table
        tr.ensure_migrations_table().await?;

        let version = tr.dirty_version().await?;
        if let Some(version) = version {
            return Err(MigrateError::Dirty(version))?;
        }

        let applied_migrations = tr.list_applied_migrations().await?;
        validate_applied_migrations(&applied_migrations, &migrator)?;

        let applied_migrations: HashMap<_, _> = applied_migrations
            .into_iter()
            .map(|m| (m.version, m))
            .collect();

        for migration in migrator.iter() {
            let mut migration = migration.clone();
            // we are in a tx, so we don't need to start a new one
            migration.no_tx = true;
            if migration.migration_type.is_down_migration() {
                continue;
            }

            match applied_migrations.get(&migration.version) {
                Some(applied_migration) => {
                    if migration.checksum != applied_migration.checksum {
                        return Err(MigrateError::VersionMismatch(migration.version))?;
                    }
                }
                None => {
                    tr.apply(&migration).await?;
                    let local = sqlx_migration_to_local_migration(&migration);
                    if let Some(hook) = hooks.remove(&local) {
                        hook(tr).await.map_err(|e| e.error)?;
                    }
                }
            }
        }

        // unlock the migrator to allow other migrators to run
        // but do nothing as we already migrated
        if locking {
            tr.unlock().await?;
        }
    }
    trx.commit().await.map_err(|e| anyhow::anyhow!(e.error))?;
    Ok(())
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
