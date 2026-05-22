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

const CORE_MIGRATIONS_TABLE: &str = "_sqlx_migrations";

/// A registered extension migration source.
///
/// Extensions implement features on top of the lakekeeper catalog and often
/// need their own Postgres tables. They contribute migrations alongside
/// upstream's core migrations via this struct. All registered extensions are
/// applied inside upstream's outer transaction — upgrades are atomic across
/// core and every extension; partial state is impossible.
///
/// Per the extension table convention (see CONTRIBUTING.md), extensions:
/// - Name tables `ext_<feature>_*`.
/// - FK only into upstream tables, with `ON DELETE CASCADE` or `ON DELETE SET NULL`.
/// - Create no triggers, functions, or indexes on upstream-owned objects.
#[allow(missing_debug_implementations)]
pub struct ExtensionMigrations {
    /// Short identifier for this extension (e.g. `"lakekeeper_plus"`). Used
    /// to derive the per-source migration tracker table name
    /// `ext_<name>_sqlx_migrations`.
    pub name: &'static str,
    /// Migrations to apply, typically produced by `sqlx::migrate!("./migrations")`
    /// in the extension crate.
    pub migrator: Migrator,
    /// Data migration hooks keyed by migration version, mirroring upstream's
    /// own hook registry. Each hook runs immediately after the matching
    /// migration is applied, inside the same transaction.
    pub data_hooks: HashMap<i64, Box<dyn MigrationHook>>,
}

impl ExtensionMigrations {
    fn tracker_table(&self) -> String {
        format!("ext_{}_sqlx_migrations", self.name)
    }
}

/// Apply core migrations only.
///
/// Back-compat entry-point for callers that don't register extensions.
/// Equivalent to `migrate(pool, vec![])`.
///
/// # Errors
/// Returns an error if the migration fails.
pub async fn migrate_core_only(pool: &sqlx::PgPool) -> anyhow::Result<ServerId> {
    migrate(pool, Vec::new()).await
}

/// Apply core migrations followed by every registered extension's migrations,
/// all in one outer transaction. Either all migrations succeed and the
/// transaction commits, or it rolls back — partial state is impossible.
///
/// Extensions are applied in registration order, after all core migrations.
/// Each extension tracks its applied migrations in its own
/// `ext_<name>_sqlx_migrations` table. Extensions must depend only on core
/// upstream state — never on each other.
///
/// # Errors
/// Returns an error if any migration fails.
pub async fn migrate(
    pool: &sqlx::PgPool,
    mut extensions: Vec<ExtensionMigrations>,
) -> anyhow::Result<ServerId> {
    let core_migrator = sqlx::migrate!();
    let core_hooks = get_data_migrations();
    let core_sha_patches = get_changed_migration_ids();
    let catalog_state = CatalogState::from_pools(pool.clone(), pool.clone());
    tracing::info!(
        "Core data migration hooks: {:?}",
        core_hooks.keys().collect::<Vec<_>>()
    );
    tracing::info!(
        "Core SHA patches: {:?}",
        core_sha_patches.iter().collect::<Vec<_>>()
    );

    let mut trx = PostgresTransaction::begin_write(catalog_state.clone())
        .await
        .map_err(|e| e.error)?;
    let transaction = trx.transaction();
    // Application advisory lock to prevent concurrent migrations.
    transaction.lock().await?;

    // 1. Core migrations.
    apply_source(
        transaction,
        &core_migrator,
        CORE_MIGRATIONS_TABLE,
        core_hooks,
        core_sha_patches,
    )
    .await?;

    // 2. Extension migrations, in registration order.
    for ext in &mut extensions {
        let table = ext.tracker_table();
        let hooks = std::mem::take(&mut ext.data_hooks);
        tracing::info!(
            extension = ext.name,
            "Applying extension migrations into {}",
            table,
        );
        apply_source(transaction, &ext.migrator, &table, hooks, HashSet::new()).await?;
    }

    let server_id = get_or_set_server_id(&mut **transaction).await?;

    // Unlock the migrator to allow other migrators to run — but do nothing
    // as we already migrated.
    transaction.unlock().await?;
    trx.commit().await.map_err(|e| anyhow::anyhow!(e.error))?;
    Ok(server_id)
}

async fn apply_source(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    migrator: &Migrator,
    table_name: &str,
    mut data_hooks: HashMap<i64, Box<dyn MigrationHook>>,
    mut sha_patches: HashSet<i64>,
) -> anyhow::Result<()> {
    let applied_migrations = run_checks(migrator, transaction, table_name).await?;

    for migration in migrator.iter() {
        tracing::info!(%migration.version, %migration.description, "Current migration");
        let mut migration = migration.clone();
        // we are in an outer tx, so don't start a new one per migration
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
            transaction.apply(table_name, &migration).await?;
            tracing::info!(%migration.version, "Applying migration");
            if let Some(hook) = data_hooks.remove(&migration.version) {
                tracing::info!(%migration.version, "Running data migration {}", hook.name());
                hook.apply(transaction).await?;
                tracing::info!(%migration.version, "Data migration {} complete", hook.name());
            } else {
                tracing::debug!(%migration.version, "No hook for migration");
            }
        }
    }
    Ok(())
}

async fn run_checks(
    migrator: &Migrator,
    tr: &mut sqlx::Transaction<'_, Postgres>,
    table_name: &str,
) -> Result<HashMap<i64, AppliedMigration>, MigrateError> {
    // creates [_migrations] table only if needed
    tr.ensure_migrations_table(table_name).await?;

    let version = tr.dirty_version(table_name).await?;
    if let Some(version) = version {
        return Err(MigrateError::Dirty(version))?;
    }

    let applied_migrations = tr.list_applied_migrations(table_name).await?;
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

    let applied_migrations = match conn.list_applied_migrations(CORE_MIGRATIONS_TABLE).await {
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
    use std::collections::HashMap;

    use sqlx::{
        AssertSqlSafe, PgPool,
        postgres::{PgConnectOptions, PgPoolOptions},
    };
    use uuid::Uuid;

    use super::{ExtensionMigrations, migrate, migrate_core_only};

    async fn table_exists(pool: &PgPool, name: &str) -> bool {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables \
             WHERE table_schema = current_schema() AND table_name = $1)",
        )
        .bind(name)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// Happy path: core migrations + one extension migration source apply in
    /// the same transaction; both end up with their tables present.
    #[sqlx::test(migrations = false)]
    async fn test_extension_migrations_apply(pool: PgPool) {
        let ext = ExtensionMigrations {
            name: "demo",
            migrator: sqlx::migrate!("./tests/extension_migrations_fixture"),
            data_hooks: HashMap::new(),
        };
        migrate(&pool, vec![ext])
            .await
            .expect("migrate(pool, vec![demo]) must succeed");

        // Core upstream tables are present.
        assert!(table_exists(&pool, "warehouse").await);
        // Extension table is present.
        assert!(table_exists(&pool, "ext_demo_state").await);
        // Both tracker tables are present and disjoint.
        assert!(table_exists(&pool, "_sqlx_migrations").await);
        assert!(table_exists(&pool, "ext_demo_sqlx_migrations").await);
    }

    /// Atomicity: when an extension migration fails, the outer transaction
    /// must roll back — core migrations included. Nothing should be visible
    /// in the database afterward.
    #[sqlx::test(migrations = false)]
    async fn test_extension_migrations_failure_rolls_back_core(pool: PgPool) {
        let ext = ExtensionMigrations {
            name: "demo",
            migrator: sqlx::migrate!("./tests/extension_migrations_fixture_invalid"),
            data_hooks: HashMap::new(),
        };
        let result = migrate(&pool, vec![ext]).await;
        assert!(
            result.is_err(),
            "migrate(pool, [invalid ext]) must fail, got: {result:?}"
        );

        // Outer transaction rolled back: every relation it would have created
        // must be absent — both core and extension.
        assert!(
            !table_exists(&pool, "warehouse").await,
            "core `warehouse` must not exist after failed transactional migration"
        );
        assert!(
            !table_exists(&pool, "ext_demo_atomic").await,
            "extension table must not exist after rollback"
        );
        assert!(
            !table_exists(&pool, "_sqlx_migrations").await,
            "core tracker table must not exist after rollback"
        );
        assert!(
            !table_exists(&pool, "ext_demo_sqlx_migrations").await,
            "extension tracker table must not exist after rollback"
        );
    }

    /// Core never creates `ext_*` objects: upstream's prefix reservation must
    /// remain a one-way contract — extensions may use the prefix, core may not.
    #[sqlx::test(migrations = false)]
    async fn test_core_does_not_create_ext_objects(pool: PgPool) {
        migrate_core_only(&pool)
            .await
            .expect("core migrations must succeed");

        let ext_table_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM information_schema.tables \
             WHERE table_schema = current_schema() AND table_name LIKE 'ext\\_%' ESCAPE '\\'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            ext_table_count, 0,
            "core migrations must not create any `ext_*` tables"
        );

        let ext_trigger_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_trigger t \
             JOIN pg_class c ON c.oid = t.tgrelid \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = current_schema() \
               AND NOT t.tgisinternal \
               AND t.tgname LIKE 'ext\\_%' ESCAPE '\\'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            ext_trigger_count, 0,
            "core migrations must not create any `ext_*` triggers"
        );

        let ext_routine_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_proc p \
             JOIN pg_namespace n ON n.oid = p.pronamespace \
             WHERE n.nspname = current_schema() \
               AND p.proname LIKE 'ext\\_%' ESCAPE '\\'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            ext_routine_count, 0,
            "core migrations must not create any `ext_*` functions/procedures"
        );

        let ext_type_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_type t \
             JOIN pg_namespace n ON n.oid = t.typnamespace \
             WHERE n.nspname = current_schema() \
               AND t.typname LIKE 'ext\\_%' ESCAPE '\\'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            ext_type_count, 0,
            "core migrations must not create any `ext_*` types"
        );
    }

    /// FK contract: every FK from an extension table to an upstream-owned
    /// table must be `ON DELETE CASCADE` or `ON DELETE SET NULL`. The
    /// fixture's `ext_demo_state.server_id` exercises the rule.
    #[sqlx::test(migrations = false)]
    async fn test_extension_fk_cascade_rule(pool: PgPool) {
        let ext = ExtensionMigrations {
            name: "demo",
            migrator: sqlx::migrate!("./tests/extension_migrations_fixture"),
            data_hooks: HashMap::new(),
        };
        migrate(&pool, vec![ext]).await.unwrap();

        let violations: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT tc.table_name::text, kcu.column_name::text, rc.delete_rule::text \
             FROM information_schema.referential_constraints rc \
             JOIN information_schema.table_constraints tc \
               ON tc.constraint_name = rc.constraint_name \
              AND tc.constraint_schema = rc.constraint_schema \
             JOIN information_schema.key_column_usage kcu \
               ON kcu.constraint_name = rc.constraint_name \
              AND kcu.constraint_schema = rc.constraint_schema \
             JOIN information_schema.constraint_column_usage ccu \
               ON ccu.constraint_name = rc.constraint_name \
              AND ccu.constraint_schema = rc.constraint_schema \
             WHERE tc.table_schema = current_schema() \
               AND tc.table_name LIKE 'ext\\_%' ESCAPE '\\' \
               AND ccu.table_name NOT LIKE 'ext\\_%' ESCAPE '\\' \
               AND rc.delete_rule NOT IN ('CASCADE', 'SET NULL')",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert!(
            violations.is_empty(),
            "ext_* tables must FK upstream with CASCADE or SET NULL — violations: {violations:?}"
        );

        // Reverse direction is forbidden: upstream tables must not FK into ext_*.
        let reverse: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM information_schema.referential_constraints rc \
             JOIN information_schema.table_constraints tc \
               ON tc.constraint_name = rc.constraint_name \
              AND tc.constraint_schema = rc.constraint_schema \
             JOIN information_schema.constraint_column_usage ccu \
               ON ccu.constraint_name = rc.constraint_name \
              AND ccu.constraint_schema = rc.constraint_schema \
             WHERE tc.table_schema = current_schema() \
               AND tc.table_name NOT LIKE 'ext\\_%' ESCAPE '\\' \
               AND ccu.table_name LIKE 'ext\\_%' ESCAPE '\\'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            reverse, 0,
            "upstream tables must never FK into ext_* tables (one-way contract)"
        );
    }

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
            sqlx::query(AssertSqlSafe(format!(
                r#"CREATE EXTENSION IF NOT EXISTS "{ext}""#
            )))
            .execute(&admin_pool)
            .await
            .unwrap();
        }

        sqlx::query(AssertSqlSafe(format!(
            r#"CREATE ROLE "{role}" LOGIN PASSWORD '{password}'"#
        )))
        .execute(&admin_pool)
        .await
        .unwrap();
        sqlx::query(AssertSqlSafe(format!(
            r#"CREATE SCHEMA "{schema}" AUTHORIZATION "{role}""#
        )))
        .execute(&admin_pool)
        .await
        .unwrap();

        let db = admin_pool
            .connect_options()
            .get_database()
            .unwrap()
            .to_string();
        sqlx::query(AssertSqlSafe(format!(
            r#"GRANT CONNECT ON DATABASE "{db}" TO "{role}""#
        )))
        .execute(&admin_pool)
        .await
        .unwrap();
        sqlx::query(AssertSqlSafe(format!(
            r#"GRANT USAGE ON SCHEMA public TO "{role}""#
        )))
        .execute(&admin_pool)
        .await
        .unwrap();
        sqlx::query(AssertSqlSafe(format!(
            r#"REVOKE CREATE ON SCHEMA public FROM "{role}""#
        )))
        .execute(&admin_pool)
        .await
        .unwrap();

        // The mechanism we document for #1707: server-side default search_path on
        // the role itself, so every new connection lands in the custom schema.
        // `public` is included so functions/operators from extensions installed
        // there (e.g. uuid_generate_v1mc from uuid-ossp) resolve.
        sqlx::query(AssertSqlSafe(format!(
            r#"ALTER ROLE "{role}" SET search_path = "{schema}", public"#
        )))
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

        migrate_core_only(&app_pool)
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
        let _ = sqlx::query(AssertSqlSafe(format!(r#"DROP SCHEMA "{schema}" CASCADE"#)))
            .execute(&admin_pool)
            .await;
        let _ = sqlx::query(AssertSqlSafe(format!(r#"DROP OWNED BY "{role}""#)))
            .execute(&admin_pool)
            .await;
        let _ = sqlx::query(AssertSqlSafe(format!(r#"DROP ROLE "{role}""#)))
            .execute(&admin_pool)
            .await;
    }
}
