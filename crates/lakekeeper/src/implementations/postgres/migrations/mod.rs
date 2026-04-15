use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
};

use anyhow::anyhow;
use futures::future::BoxFuture;
use sqlx::{
    Error, Postgres,
    migrate::{AppliedMigration, Migrate, MigrateError, Migrator},
};

use crate::{
    implementations::postgres::{
        CatalogState, PostgresTransaction, bootstrap::get_or_set_server_id,
        migrations::split_table_metadata::SplitTableMetadataHook,
    },
    service::{ServerId, Transaction},
};

mod patch_migration_hash;
mod split_table_metadata;

/// # Errors
/// Returns an error if the migration fails.
pub async fn migrate(pool: &sqlx::PgPool) -> anyhow::Result<ServerId> {
    let migrator = sqlx::migrate!();
    let mut data_migration_hooks = get_data_migrations();
    let mut sha_patches = get_changed_migration_ids();
    let catalog_state = CatalogState::from_pools(pool.clone(), pool.clone());
    tracing::info!(
        "Data migration hooks: {:?}",
        data_migration_hooks.keys().collect::<Vec<_>>()
    );

    tracing::info!("SHA patches: {:?}", sha_patches.iter().collect::<Vec<_>>());
    let mut trx = PostgresTransaction::begin_write(catalog_state.clone())
        .await
        .map_err(|e| e.error)?;
    let locking = true;
    let transaction = trx.transaction();
    // Application advisory lock in the database to prevent concurrent migrations
    if locking {
        transaction.lock().await?;
    }

    let applied_migrations = run_checks(&migrator, transaction).await?;

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
                if sha_patches.remove(&migration.version) {
                    patch_migration_hash::patch(
                        transaction,
                        applied_migration.checksum.clone(),
                        migration.checksum.clone(),
                        migration.version,
                    )
                    .await?;
                    continue;
                }
                return Err(MigrateError::VersionMismatch(migration.version))?;
            }
            tracing::info!(%migration.version, "Migration already applied");
        } else {
            transaction.apply(&migration).await?;
            tracing::info!(%migration.version, "Applying migration");
            if let Some(hook) = data_migration_hooks.remove(&migration.version) {
                tracing::info!(%migration.version, "Running data migration {}", hook.name());
                hook.apply(transaction).await?;
                tracing::info!(%migration.version, "Data migration {} complete", hook.name());
            } else {
                tracing::debug!(%migration.version, "No hook for migration");
            }
        }
    }

    let server_id = get_or_set_server_id(&mut **transaction).await?;

    // unlock the migrator to allow other migrators to run
    // but do nothing as we already migrated
    if locking {
        transaction.unlock().await?;
    }
    trx.commit().await.map_err(|e| anyhow::anyhow!(e.error))?;
    Ok(server_id)
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
    let mut conn: sqlx::pool::PoolConnection<Postgres> = pool.acquire().await?;
    let m = sqlx::migrate!();
    let changed_migrations = get_changed_migration_ids();
    tracing::info!(
        "SHA patches: {:?}",
        changed_migrations.iter().collect::<Vec<_>>()
    );

    let applied_migrations = match conn.list_applied_migrations().await {
        Ok(migrations) => migrations,
        Err(e) => {
            if let MigrateError::Execute(Error::Database(db)) = &e
                && db.code().as_deref() == Some("42P01")
            {
                tracing::debug!(?db, "No migrations have been applied.");
                return Ok(MigrationState::NoMigrationsTable);
            }
            // we discard the error here since sqlx prefixes db errors with "while executing
            // migrations" which is not what we are doing here.
            tracing::debug!(
                ?e,
                "Error listing applied migrations, even though the error may say different things, we are not applying migrations here."
            );
            return Err(anyhow!("Error listing applied migrations"));
        }
    };

    let to_be_applied = m
        .migrations
        .iter()
        .map(|mig| (mig.version, &*mig.checksum))
        .filter(|(v, _)| !changed_migrations.contains(v))
        .collect::<HashSet<_>>();
    let applied = applied_migrations
        .iter()
        .map(|mig| (mig.version, &*mig.checksum))
        .filter(|(v, _)| !changed_migrations.contains(v))
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
    ) -> BoxFuture<'c, anyhow::Result<()>>;

    fn name(&self) -> &'static str;

    fn version() -> i64
    where
        Self: Sized;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Migration {
    version: i64,
    description: Cow<'static, str>,
}

fn get_changed_migration_ids() -> HashSet<i64> {
    HashSet::from([
        20_250_328_131_139,
        20_250_505_101_407,
        20_250_523_101_407,
        20_250_923_164_029,
        20_251_109_122_721,
        20_251_228_101_923,
    ])
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

#[cfg(test)]
mod tests {
    use sqlx::{
        PgPool,
        postgres::{PgConnectOptions, PgPoolOptions},
    };
    use uuid::Uuid;

    use super::migrate;

    /// Regression test for #1519 / #1707: migrations must succeed when run by a
    /// low-privilege role into a non-`public` schema, with the schema selected
    /// via the role's default `search_path`.
    #[sqlx::test(migrations = false)]
    async fn test_migrate_into_custom_schema_as_low_privilege_user(admin_pool: PgPool) {
        // Unique names so parallel tests don't collide on cluster-global roles.
        let suffix = Uuid::new_v4().simple().to_string();
        let schema = format!("lk_app_{suffix}");
        let role = format!("lk_app_user_{suffix}");
        let password = "lk_app_password";

        // Pre-install required extensions in `public` as admin. The application
        // role intentionally has no CREATE-on-database privilege, so the
        // `CREATE EXTENSION IF NOT EXISTS` calls in the migrations must hit the
        // no-op path.
        for ext in [
            "uuid-ossp",
            "pgcrypto",
            "pg_trgm",
            "btree_gin",
            "btree_gist",
        ] {
            sqlx::query(&format!(r#"CREATE EXTENSION IF NOT EXISTS "{ext}""#))
                .execute(&admin_pool)
                .await
                .unwrap();
        }

        sqlx::query(&format!(
            r#"CREATE ROLE "{role}" LOGIN PASSWORD '{password}'"#
        ))
        .execute(&admin_pool)
        .await
        .unwrap();
        sqlx::query(&format!(
            r#"CREATE SCHEMA "{schema}" AUTHORIZATION "{role}""#
        ))
        .execute(&admin_pool)
        .await
        .unwrap();

        let db = admin_pool
            .connect_options()
            .get_database()
            .unwrap()
            .to_string();
        sqlx::query(&format!(r#"GRANT CONNECT ON DATABASE "{db}" TO "{role}""#))
            .execute(&admin_pool)
            .await
            .unwrap();
        sqlx::query(&format!(r#"GRANT USAGE ON SCHEMA public TO "{role}""#))
            .execute(&admin_pool)
            .await
            .unwrap();
        sqlx::query(&format!(r#"REVOKE CREATE ON SCHEMA public FROM "{role}""#))
            .execute(&admin_pool)
            .await
            .unwrap();

        // The mechanism we document for #1707: server-side default search_path on
        // the role itself, so every new connection lands in the custom schema.
        // `public` is included so functions/operators from extensions installed
        // there (e.g. uuid_generate_v1mc from uuid-ossp) resolve.
        sqlx::query(&format!(
            r#"ALTER ROLE "{role}" SET search_path = "{schema}", public"#
        ))
        .execute(&admin_pool)
        .await
        .unwrap();

        let admin_opts = admin_pool.connect_options();
        let app_opts = PgConnectOptions::new()
            .host(admin_opts.get_host())
            .port(admin_opts.get_port())
            .database(&db)
            .username(&role)
            .password(password);
        let app_pool = PgPoolOptions::new()
            .max_connections(2)
            .connect_with(app_opts)
            .await
            .unwrap();

        migrate(&app_pool)
            .await
            .expect("migrations should succeed for low-privilege user in custom schema");

        let in_schema: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables \
             WHERE table_schema = $1 AND table_name = 'warehouse')",
        )
        .bind(&schema)
        .fetch_one(&admin_pool)
        .await
        .unwrap();
        assert!(in_schema, "`warehouse` should be created in {schema}");

        let in_public: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables \
             WHERE table_schema = 'public' AND table_name = 'warehouse')",
        )
        .fetch_one(&admin_pool)
        .await
        .unwrap();
        assert!(!in_public, "`warehouse` must not leak into public");

        // Drain the app pool before dropping the role it authenticated as.
        app_pool.close().await;
        let _ = sqlx::query(&format!(r#"DROP SCHEMA "{schema}" CASCADE"#))
            .execute(&admin_pool)
            .await;
        let _ = sqlx::query(&format!(r#"DROP OWNED BY "{role}""#))
            .execute(&admin_pool)
            .await;
        let _ = sqlx::query(&format!(r#"DROP ROLE "{role}""#))
            .execute(&admin_pool)
            .await;
    }
}
