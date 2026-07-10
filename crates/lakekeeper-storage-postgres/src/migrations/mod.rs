use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
};

use anyhow::{Context, anyhow};
use futures::future::BoxFuture;
use lakekeeper::service::{ServerId, Transaction};
/// Re-exported for convenience so the `ExtensionMigrations::migrator` field
/// type is reachable without naming `sqlx` in the caller's import list.
///
/// **Only the `Migrator` type is re-exported.** The `sqlx::migrate!` macro
/// itself cannot be re-exported usefully — its expansion references `::sqlx::*`
/// paths, so the extension crate must still depend on `sqlx` directly to
/// invoke `sqlx::migrate!("./migrations")`. `SqlxMigrator` only saves an
/// import line, not a Cargo dependency.
pub use sqlx::migrate::Migrator as SqlxMigrator;
use sqlx::{
    Error, Postgres,
    migrate::{AppliedMigration, Migrate, MigrateError, Migration as SqlxMigration, Migrator},
};
use typed_builder::TypedBuilder;

use crate::{
    CatalogState, PostgresTransaction, bootstrap::get_or_set_server_id,
    migrations::split_table_metadata::SplitTableMetadataHook,
};

pub(crate) mod normalize_schema;
mod patch_migration_hash;
mod split_table_metadata;

const CORE_MIGRATIONS_TABLE: &str = "_sqlx_migrations";

/// Lock scope for concurrent-migration prevention.
///
/// Bit-identical to sqlx's private `generate_lock_id` in
/// `sqlx-postgres::migrate` (CRC32-IEEE of the database name multiplied
/// by an arbitrary constant). Keeping the formula in sync means a
/// rolling upgrade between a lakekeeper version using sqlx's session-
/// scoped `pg_advisory_lock` and one using our transaction-scoped
/// `pg_advisory_xact_lock` still serializes on the same i64 key, so
/// concurrent migrations across the rollover are mutually exclusive.
fn migration_lock_id(database_name: &str) -> i64 {
    const CRC_IEEE: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISO_HDLC);
    // 0x3d32ad9e: same magic constant sqlx uses, "chosen by fair dice roll".
    0x3d32_ad9e * i64::from(CRC_IEEE.checksum(database_name.as_bytes()))
}

/// A registered extension migration source.
///
/// Extensions implement features on top of the lakekeeper catalog and often
/// need their own Postgres tables. They contribute migrations alongside
/// upstream's core migrations via this struct. All registered extensions are
/// applied inside upstream's outer transaction — upgrades are atomic across
/// core and every extension; partial state is impossible.
///
/// Per the extension table convention (see the "Extension tables" section of
/// the developer guide), extensions:
/// - Name tables `ext_<feature>_*`.
/// - FK only into upstream tables, with `ON DELETE CASCADE` or `ON DELETE SET NULL`.
/// - Create no triggers, functions, or indexes on upstream-owned objects.
///
/// Construct via the typed builder:
///
/// ```ignore
/// ExtensionMigrations::builder()
///     .name("audit")
///     .migrator(sqlx::migrate!("./migrations"))
///     .build()
/// ```
#[allow(missing_debug_implementations)]
#[derive(TypedBuilder)]
pub struct ExtensionMigrations {
    /// Short identifier for this extension (e.g. `"audit"`, `"demo"`). Used
    /// verbatim to derive the per-source migration tracker table name
    /// `ext_<name>_sqlx_migrations`.
    ///
    /// Length **1–40 characters**. First character `[a-z_]`, remaining
    /// characters `[a-z0-9_]`. Enforced at runtime by `validate_name()`,
    /// called from `migrate()` before any database work — non-conforming
    /// names fail fast with a clear error. The length cap keeps the
    /// derived tracker table name well within `PostgreSQL`'s `NAMEDATALEN`
    /// (63 bytes).
    #[builder(setter(into))]
    name: Cow<'static, str>,
    /// Migrations to apply, typically produced by `sqlx::migrate!("./migrations")`
    /// in the extension crate.
    migrator: Migrator,
    /// Data migration hooks keyed by migration version, mirroring upstream's
    /// own hook registry. Each hook runs immediately after the matching
    /// migration is applied, inside the same transaction.
    #[builder(default)]
    data_hooks: HashMap<i64, Box<dyn MigrationHook>>,
    /// Migration versions whose content was intentionally changed after they
    /// were first shipped (e.g. a previously-shipped `.sql` file's body had
    /// to be edited without a version bump). For each version listed here,
    /// the migrator will rewrite the checksum in this extension's tracker
    /// table to match the current file's checksum, instead of failing with
    /// `VersionMismatch`. Mirrors upstream's own
    /// `get_changed_migration_ids()` for the core source — use sparingly.
    #[builder(default)]
    sha_patches: HashSet<i64>,
}

/// Maximum length of `ExtensionMigrations::name`. Combined with the fixed
/// `ext_` prefix and `_sqlx_migrations` suffix the resulting tracker table
/// name stays well under `PostgreSQL`'s `NAMEDATALEN` (63 bytes).
const MAX_EXTENSION_NAME_LEN: usize = 40;

impl ExtensionMigrations {
    fn tracker_table(&self) -> String {
        format!("ext_{}_sqlx_migrations", self.name)
    }

    /// Validate `name`: 1–40 characters, first `[a-z_]`, remaining
    /// `[a-z0-9_]`. Returns `Ok` on conformance; otherwise returns an
    /// error naming the offending input. Called by `migrate()` before any
    /// DB work.
    fn validate_name(&self) -> anyhow::Result<()> {
        if self.name.is_empty() {
            return Err(anyhow!("extension name must not be empty"));
        }
        if self.name.len() > MAX_EXTENSION_NAME_LEN {
            return Err(anyhow!(
                "extension name {:?} is {} chars, must be ≤ {MAX_EXTENSION_NAME_LEN}",
                self.name,
                self.name.len(),
            ));
        }
        let mut chars = self.name.chars();
        let first = chars.next().expect("non-empty checked above");
        if !(first.is_ascii_lowercase() || first == '_') {
            return Err(anyhow!(
                "extension name {:?} must start with an ASCII lowercase letter or underscore",
                self.name,
            ));
        }
        for c in chars {
            if !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_') {
                return Err(anyhow!(
                    "extension name {:?} must match [a-z_][a-z0-9_]*, found {c:?}",
                    self.name,
                ));
            }
        }
        Ok(())
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

/// Apply every registered migration — core and all extensions — in a single
/// outer transaction. Either every migration succeeds and the transaction
/// commits, or it rolls back — partial state is impossible.
///
/// Ordering: all sources are pooled, then applied in ascending `version`
/// order (the timestamp prefix on the `.sql` filename). When two migrations
/// share a version, registration order breaks the tie — core first, then
/// extensions in the order passed to `migrate()`. Extensions are **not**
/// guaranteed to run strictly after all core migrations: an extension
/// migration dated earlier than a core migration interleaves into the
/// appropriate position in the merged timeline. This is what lets core
/// add a column → extension FKs it → extension migrates the FK target →
/// core drops the column to work as a single atomic sequence across both
/// repos, as long as the dates line up.
///
/// Each extension tracks its applied migrations in its own
/// `ext_<name>_sqlx_migrations` table; core uses `_sqlx_migrations`.
/// Extensions must depend only on core upstream state — never on each
/// other.
///
/// # Errors
/// Returns an error if any migration fails.
pub async fn migrate(
    pool: &sqlx::PgPool,
    mut extensions: Vec<ExtensionMigrations>,
) -> anyhow::Result<ServerId> {
    // Fail fast on misconfigured extension names — before any DB work, before
    // the advisory lock is acquired. Catches typos in caller crate source and
    // duplicate registrations (which would otherwise silently collide on the
    // same tracker table and corrupt history).
    {
        let mut seen: HashSet<&str> = HashSet::with_capacity(extensions.len());
        for ext in &extensions {
            ext.validate_name()?;
            if !seen.insert(ext.name.as_ref()) {
                return Err(anyhow!(
                    "extension name `{}` registered more than once: each `ExtensionMigrations` \
                     entry must have a unique name (they share a tracker table otherwise)",
                    ext.name,
                ));
            }
        }
    }

    let core_migrator = sqlx::migrate!();
    let catalog_state = CatalogState::from_pools(pool.clone(), pool.clone());
    tracing::info!(
        "Core data migration hooks: {:?}",
        get_data_migrations().keys().collect::<Vec<_>>()
    );
    tracing::info!(
        "Core SHA patches: {:?}",
        get_changed_migration_ids().iter().collect::<Vec<_>>()
    );

    let mut trx = PostgresTransaction::begin_write(catalog_state.clone())
        .await
        .map_err(|e| e.error)?;
    let transaction = trx.transaction();

    // The normalized-schema backfill can run minutes on large catalogs. Set a 60-min statement/idle
    // backstop on the migration txn: generous enough never to clip real work, finite enough to bound
    // a hung migration. SET LOCAL is txn-scoped; both are USERSET GUCs, overriding any managed default.
    sqlx::query("SET LOCAL statement_timeout = '60min'")
        .execute(&mut **transaction)
        .await?;
    sqlx::query("SET LOCAL idle_in_transaction_session_timeout = '60min'")
        .execute(&mut **transaction)
        .await?;

    // Transaction-scoped advisory lock to prevent concurrent migrations.
    // Postgres auto-releases on COMMIT or ROLLBACK — including when the
    // transaction is aborted by a failing SQL statement, which is the
    // dominant migration-failure mode. The session-scoped equivalent
    // (sqlx's `transaction.lock()` -> `pg_advisory_lock`) would leak
    // the lock on that path because `pg_advisory_unlock` is rejected
    // with `25P02 in_failed_sql_transaction` on a poisoned transaction.
    let db_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&mut **transaction)
        .await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(migration_lock_id(&db_name))
        .execute(&mut **transaction)
        .await?;

    // 1. Pre-flight per source: ensure tracker table, dirty-check, list applied.
    //    Done before the apply loop so we can look up already-applied state per
    //    source while iterating the merged timeline.
    //    Index 0 is core; 1..N are extensions in registration order.
    let mut sources: Vec<SourceState> = Vec::with_capacity(1 + extensions.len());
    sources.push(SourceState {
        table_name: CORE_MIGRATIONS_TABLE.to_string(),
        applied: run_checks(&core_migrator, transaction, CORE_MIGRATIONS_TABLE)
            .await
            .with_context(|| format!("pre-flight for source `{CORE_MIGRATIONS_TABLE}`"))?,
        hooks: get_data_migrations(),
        sha_patches: get_changed_migration_ids(),
    });
    for ext in &mut extensions {
        let table = ext.tracker_table();
        let applied = run_checks(&ext.migrator, transaction, &table)
            .await
            .with_context(|| format!("pre-flight for source `{table}`"))?;
        tracing::info!(
            extension = %ext.name,
            "Pre-flight checks passed; will apply via {}",
            table,
        );
        sources.push(SourceState {
            table_name: table,
            applied,
            hooks: std::mem::take(&mut ext.data_hooks),
            sha_patches: std::mem::take(&mut ext.sha_patches),
        });
    }

    // 2. Build merged timeline: every migration from every source, sorted by
    //    version id. This is what lets an extension migration land "between"
    //    two core migrations on a fresh install — e.g. core adds a column,
    //    the extension FKs it, the extension migrates the FK away, core drops
    //    the column. Stable secondary sort by source index keeps ordering
    //    deterministic when versions collide (rare; same prefix on two files).
    let mut timeline: Vec<(i64, usize, SqlxMigration)> = Vec::new();
    for m in core_migrator.iter() {
        if !m.migration_type.is_down_migration() {
            timeline.push((m.version, 0, (*m).clone()));
        }
    }
    for (idx, ext) in extensions.iter().enumerate() {
        for m in ext.migrator.iter() {
            if !m.migration_type.is_down_migration() {
                timeline.push((m.version, idx + 1, (*m).clone()));
            }
        }
    }
    timeline.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    // 3. Apply in merged order, dispatching each migration to its source's
    //    tracker table. Each source's checksum / sha-patch / hook state lives
    //    on its `SourceState`.
    for (version, src_idx, mut migration) in timeline {
        migration.no_tx = true;
        let source = &mut sources[src_idx];
        apply_migration(transaction, source, migration)
            .await
            .with_context(|| {
                format!(
                    "applying migration version {version} from source `{}`",
                    source.table_name,
                )
            })?;
    }

    let server_id = get_or_set_server_id(&mut **transaction).await?;

    // No manual unlock needed: `trx.commit()` (or any ROLLBACK if we
    // returned `Err` above) releases the transaction-scoped advisory
    // lock automatically.
    trx.commit().await.map_err(|e| anyhow::anyhow!(e.error))?;
    Ok(server_id)
}

/// Per-source apply state, built once during pre-flight and consulted as the
/// merged timeline executes.
struct SourceState {
    /// Tracker table — `_sqlx_migrations` for core, `ext_<name>_sqlx_migrations`
    /// for extensions.
    table_name: String,
    /// Migrations already in the tracker as of pre-flight (immutable for the
    /// rest of the apply loop).
    applied: HashMap<i64, AppliedMigration>,
    /// Data hooks, drained as their matching migrations apply.
    hooks: HashMap<i64, Box<dyn MigrationHook>>,
    /// SHA-patch overrides (only core uses these today).
    sha_patches: HashSet<i64>,
}

/// Apply (or skip, or sha-patch) one migration against its source's tracker.
async fn apply_migration(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    source: &mut SourceState,
    migration: SqlxMigration,
) -> anyhow::Result<()> {
    tracing::info!(%migration.version, %migration.description, "Current migration");

    // Clone the applied checksum so we don't hold a borrow into `source.applied`
    // while later taking a mutable borrow of `source.sha_patches` / `source.hooks`.
    let applied_checksum: Option<Cow<'static, [u8]>> = source
        .applied
        .get(&migration.version)
        .map(|m| m.checksum.clone());

    if let Some(existing) = applied_checksum {
        if migration.checksum != existing {
            if source.sha_patches.remove(&migration.version) {
                patch_migration_hash::patch(
                    transaction,
                    &source.table_name,
                    existing,
                    migration.checksum.clone(),
                    migration.version,
                )
                .await?;
                return Ok(());
            }
            return Err(MigrateError::VersionMismatch(migration.version))?;
        }
        tracing::info!(%migration.version, "Migration already applied");
    } else {
        transaction.apply(&source.table_name, &migration).await?;
        tracing::info!(%migration.version, "Applying migration");
        if let Some(hook) = source.hooks.remove(&migration.version) {
            tracing::info!(%migration.version, "Running data migration {}", hook.name());
            hook.apply(transaction).await?;
            tracing::info!(%migration.version, "Data migration {} complete", hook.name());
        } else {
            tracing::debug!(%migration.version, "No hook for migration");
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

    // Downgrade guard: if the DB carries an applied version this binary's
    // migrator doesn't contain, the DB was migrated by a newer Lakekeeper.
    // Compared on version alone — a known-version-but-changed-checksum is
    // drift (handled by `migrate()` via VersionMismatch / sha_patch), not a
    // newer DB. Reported before the "missing" check because an older binary's
    // migrations are a subset of applied, so it would otherwise look Complete.
    let binary_versions: HashSet<i64> = m.migrations.iter().map(|mig| mig.version).collect();
    let ahead: Vec<i64> = applied_migrations
        .iter()
        .map(|mig| mig.version)
        .filter(|v| !binary_versions.contains(v))
        .collect();
    if !ahead.is_empty() {
        tracing::warn!(
            ?ahead,
            "Database has applied migrations unknown to this binary — it was migrated by a newer \
             Lakekeeper. This binary is too old to run against it."
        );
        return Ok(MigrationState::Ahead);
    }

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
    /// The database has applied migrations whose versions this binary's
    /// migrator does not know — it was migrated by a *newer* Lakekeeper.
    /// Running this (older) binary against it is unsafe; callers must refuse
    /// to start rather than retry (waiting never resolves a newer DB).
    Ahead,
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
    HashMap::from([
        (
            SplitTableMetadataHook::version(),
            Box::new(SplitTableMetadataHook) as Box<_>,
        ),
        (
            normalize_schema::NormalizeSchemaHook::version(),
            Box::new(normalize_schema::NormalizeSchemaHook) as Box<_>,
        ),
    ])
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
    use std::collections::HashSet;

    use sqlx::{
        AssertSqlSafe, PgPool,
        postgres::{PgConnectOptions, PgPoolOptions},
    };
    use uuid::Uuid;

    use super::{
        ExtensionMigrations, MigrationState, check_migration_status, migrate, migrate_core_only,
    };

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

    /// Downgrade guard: a database migrated by a *newer* Lakekeeper — its
    /// `_sqlx_migrations` carries versions this binary's migrator does not know
    /// — must be reported as `Ahead`, not `Complete`. The one-directional
    /// "are all my migrations applied?" check missed this: an older binary's
    /// migrations are a subset of what's applied, so nothing looks missing.
    #[sqlx::test(migrations = false)]
    async fn test_check_migration_status_detects_newer_db(pool: PgPool) {
        migrate_core_only(&pool)
            .await
            .expect("core-only migrate must succeed");

        // Freshly migrated by this same binary → up to date.
        assert!(
            matches!(
                check_migration_status(&pool).await.unwrap(),
                MigrationState::Complete
            ),
            "freshly migrated DB should be Complete"
        );

        // Simulate a newer binary having migrated this DB: a tracker row whose
        // version is beyond anything this binary's migrator contains.
        sqlx::query(
            "INSERT INTO _sqlx_migrations \
               (version, description, installed_on, success, checksum, execution_time) \
             VALUES ($1, $2, now(), true, $3, 0)",
        )
        .bind(99_999_999_999_999_i64)
        .bind("future migration from a newer binary")
        .bind(vec![0_u8; 32])
        .execute(&pool)
        .await
        .unwrap();

        assert!(
            matches!(
                check_migration_status(&pool).await.unwrap(),
                MigrationState::Ahead
            ),
            "DB with a migration unknown to this binary must report Ahead"
        );
    }

    /// An operator runs upstream OSS by itself for a while (their
    /// `_sqlx_migrations` is populated, they have user data in core tables),
    /// then later switches to a binary that registers an extension. Calling
    /// `migrate(pool, vec![ext])` against the already-populated database must
    /// apply only the new extension migrations, preserve every byte of
    /// existing core state, and leave the FK from `ext_*` to the existing
    /// core row intact (insertable).
    #[sqlx::test(migrations = false)]
    async fn test_enable_extension_later(pool: PgPool) {
        // Phase 1: core-only. The OSS binary boots, migrate_core_only applies
        // every core migration, populates `_sqlx_migrations`.
        migrate_core_only(&pool)
            .await
            .expect("core-only migrate must succeed");
        assert!(table_exists(&pool, "_sqlx_migrations").await);
        assert!(!table_exists(&pool, "ext_demo_state").await);
        assert!(!table_exists(&pool, "ext_demo_sqlx_migrations").await);

        // Operator creates a project + warehouse via the normal binary path
        // (we simulate that here with direct inserts).
        let project_id = "test-project";
        sqlx::query("INSERT INTO project (project_id, project_name) VALUES ($1, $2)")
            .bind(project_id)
            .bind("Test Project")
            .execute(&pool)
            .await
            .unwrap();

        let warehouse_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO warehouse \
               (warehouse_id, project_id, warehouse_name, status, tabular_delete_mode) \
             VALUES ($1, $2, $3, 'active'::warehouse_status, 'hard'::tabular_delete_mode)",
        )
        .bind(warehouse_id)
        .bind(project_id)
        .bind("test_warehouse")
        .execute(&pool)
        .await
        .unwrap();

        let core_applied_before: i64 = sqlx::query_scalar("SELECT count(*) FROM _sqlx_migrations")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(
            core_applied_before > 0,
            "core-only migrate should have populated _sqlx_migrations"
        );

        // Phase 2: operator switches to a binary that registers an extension.
        // migrate() is called against the same DB, with extensions this time.
        let ext = ExtensionMigrations::builder()
            .name("demo")
            .migrator(sqlx::migrate!("./tests/extension_migrations_fixture"))
            .build();
        migrate(&pool, vec![ext])
            .await
            .expect("enabling an extension on a populated core DB must succeed");

        // Core data preserved: project + warehouse row still present, exact
        // identifiers and counts.
        let preserved_warehouse: Uuid = sqlx::query_scalar(
            "SELECT warehouse_id FROM warehouse WHERE warehouse_name = 'test_warehouse'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(preserved_warehouse, warehouse_id);
        let warehouse_count: i64 = sqlx::query_scalar("SELECT count(*) FROM warehouse")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(warehouse_count, 1);

        // Core's tracker untouched in row count (no new core migrations could
        // possibly apply on a DB that was already current).
        let core_applied_after: i64 = sqlx::query_scalar("SELECT count(*) FROM _sqlx_migrations")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(core_applied_after, core_applied_before);

        // Extension state present: table created and tracked in its own tracker.
        assert!(table_exists(&pool, "ext_demo_state").await);
        assert!(table_exists(&pool, "ext_demo_sqlx_migrations").await);
        let ext_applied: i64 = sqlx::query_scalar("SELECT count(*) FROM ext_demo_sqlx_migrations")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(ext_applied, 1);

        // Extension FK works against the existing core row — the actual point
        // of the convention.
        sqlx::query("INSERT INTO ext_demo_state (id, warehouse_id, payload) VALUES ($1, $2, $3)")
            .bind(Uuid::new_v4())
            .bind(warehouse_id)
            .bind(serde_json::json!({"hello": "world"}))
            .execute(&pool)
            .await
            .unwrap();
    }

    /// An extension shipped a migration, it landed in operators' databases,
    /// and the file's content was later edited in-place (e.g. a comment-only
    /// change that nonetheless flips the SHA). On the next boot, sqlx's
    /// `apply_migration` detects the checksum mismatch and ordinarily refuses
    /// to proceed. The extension can opt into `sha_patches` for that version,
    /// which rewrites the row in the extension's own tracker — never in core's.
    #[sqlx::test(migrations = false)]
    async fn test_extension_sha_patch_rewrites_checksum(pool: PgPool) {
        const PATCHED_VERSION: i64 = 20_260_101_000_000;

        // Phase 1: apply the original fixture.
        let v1 = ExtensionMigrations::builder()
            .name("demo")
            .migrator(sqlx::migrate!("./tests/extension_migrations_fixture"))
            .build();
        migrate(&pool, vec![v1]).await.unwrap();

        let original_checksum: Vec<u8> =
            sqlx::query_scalar("SELECT checksum FROM ext_demo_sqlx_migrations WHERE version = $1")
                .bind(PATCHED_VERSION)
                .fetch_one(&pool)
                .await
                .unwrap();

        // Phase 2: simulate the in-place edit by re-running with a fixture
        // whose file body differs but whose version is identical. Without
        // sha_patches the migrator must refuse to proceed.
        let v2_no_patch = ExtensionMigrations::builder()
            .name("demo")
            .migrator(sqlx::migrate!(
                "./tests/extension_migrations_fixture_patched"
            ))
            .build();
        let err = migrate(&pool, vec![v2_no_patch]).await.unwrap_err();
        // Top-level message carries our context (source + version); the
        // underlying sqlx `VersionMismatch` is in the chain. Use the
        // alternate Display (`{:#}`) to include both.
        let chain = format!("{err:#}");
        assert!(
            chain.contains(&PATCHED_VERSION.to_string())
                && chain.contains("previously applied but has been modified"),
            "expected checksum-mismatch error chain for version {PATCHED_VERSION}, got: {chain}",
        );
        // Source identification — operator must know which tracker is dirty.
        assert!(
            chain.contains("ext_demo_sqlx_migrations"),
            "error chain must name the offending source tracker, got: {chain}",
        );

        // Tracker row unchanged because the outer tx rolled back.
        let checksum_after_failed_attempt: Vec<u8> =
            sqlx::query_scalar("SELECT checksum FROM ext_demo_sqlx_migrations WHERE version = $1")
                .bind(PATCHED_VERSION)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(checksum_after_failed_attempt, original_checksum);

        // Phase 3: with the version listed in sha_patches, the migrator
        // rewrites the tracker row to match the new content's checksum.
        let v2_with_patch = ExtensionMigrations::builder()
            .name("demo")
            .migrator(sqlx::migrate!(
                "./tests/extension_migrations_fixture_patched"
            ))
            .sha_patches(HashSet::from([PATCHED_VERSION]))
            .build();
        migrate(&pool, vec![v2_with_patch]).await.unwrap();

        let patched_checksum: Vec<u8> =
            sqlx::query_scalar("SELECT checksum FROM ext_demo_sqlx_migrations WHERE version = $1")
                .bind(PATCHED_VERSION)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_ne!(
            patched_checksum, original_checksum,
            "extension sha patch should have rewritten the tracker row",
        );
    }

    /// `validate_name` rejects identifiers that would break PG DDL and accepts
    /// the well-formed subset. No DB connection needed — pure validation.
    #[test]
    fn test_extension_name_validation() {
        let mk = |name: &'static str| {
            ExtensionMigrations::builder()
                .name(name)
                .migrator(sqlx::migrate!("./tests/extension_migrations_fixture"))
                .build()
        };

        // Accept: simple lowercase, with digits, leading underscore.
        for ok in ["demo", "policy", "audit2", "_internal", "a"] {
            mk(ok)
                .validate_name()
                .unwrap_or_else(|e| panic!("`{ok}` must validate: {e}"));
        }

        // Reject: empty, leading digit, uppercase, hyphen, space, dot, too long.
        let too_long: &'static str = Box::leak("x".repeat(41).into_boxed_str());
        for bad in [
            "", "1ext", "ExtName", "my-ext", "my ext", "my.ext", too_long,
        ] {
            assert!(mk(bad).validate_name().is_err(), "`{bad}` must be rejected");
        }
    }

    /// Atomicity: when an extension migration fails, the outer transaction
    /// must roll back — core migrations included. Nothing should be visible
    /// in the database afterward.
    #[sqlx::test(migrations = false)]
    async fn test_extension_migrations_failure_rolls_back_core(pool: PgPool) {
        let ext = ExtensionMigrations::builder()
            .name("demo")
            .migrator(sqlx::migrate!(
                "./tests/extension_migrations_fixture_invalid"
            ))
            .build();
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

    /// Regression test for the migration-lock leak fixed by switching from
    /// session-scoped `pg_advisory_lock` to transaction-scoped
    /// `pg_advisory_xact_lock`. The fixture migration triggers a SQL error,
    /// which poisons the outer transaction and would prevent any session-
    /// level unlock from running. With the xact-scoped lock, `ROLLBACK`
    /// releases the lock automatically — so no advisory lock should remain
    /// held and a follow-up `migrate()` call must not block.
    #[sqlx::test(migrations = false)]
    async fn test_migration_lock_released_on_sql_failure(pool: PgPool) {
        let ext = ExtensionMigrations::builder()
            .name("demo")
            .migrator(sqlx::migrate!(
                "./tests/extension_migrations_fixture_invalid"
            ))
            .build();
        let result = migrate(&pool, vec![ext]).await;
        assert!(
            result.is_err(),
            "test setup: failing-fixture migration must error, got: {result:?}"
        );

        let advisory_locks_held: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_locks \
             WHERE locktype = 'advisory' \
               AND database = (SELECT oid FROM pg_database WHERE datname = current_database())",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            advisory_locks_held, 0,
            "advisory lock must be released after a failed migration; \
             a non-zero count means the lock leaked"
        );

        // End-to-end: a follow-up migration must not block on the prior
        // failure's lock. If the lock had leaked, this would hang for the
        // length of the test runner's timeout.
        migrate_core_only(&pool)
            .await
            .expect("follow-up migration must succeed after a failed prior attempt");
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

    /// Regression guard for the `pg_dump` → `pg_restore` failure mode hit by
    /// a customer migrating their Postgres backend: a BEFORE trigger whose
    /// `WHEN` clause references `OLD`/`NEW` as a whole row cannot be re-created
    /// once the underlying table has a generated column (PG enforces this at
    /// `CREATE TRIGGER` time, which is when `pg_restore` runs the dumped DDL).
    ///
    /// The migrator itself never re-creates such a trigger after a generated
    /// column is added, so the bug is invisible to ordinary migration tests
    /// and to runtime UPDATEs — but `pg_dump` faithfully emits the broken form,
    /// and `pg_restore` then refuses it. We assert on the post-migration catalog
    /// state instead.
    #[sqlx::test(migrations = false)]
    async fn test_no_wholerow_before_trigger_on_generated_column_tables(pool: PgPool) {
        migrate_core_only(&pool)
            .await
            .expect("core migrations must succeed");

        let broken: Vec<(String, String)> = sqlx::query_as(
            "SELECT t.tgname::text, c.relname::text \
             FROM   pg_trigger  t \
             JOIN   pg_class    c ON c.oid = t.tgrelid \
             JOIN   pg_namespace n ON n.oid = c.relnamespace \
             WHERE  n.nspname = current_schema() \
               AND  NOT t.tgisinternal \
               AND  (t.tgtype & 2) = 2 \
               AND  pg_get_triggerdef(t.oid) ~* '(old|new)\\.\\*' \
               AND  EXISTS ( \
                      SELECT 1 FROM pg_attribute a \
                      WHERE  a.attrelid = c.oid \
                        AND  a.attgenerated <> '' \
                    )",
        )
        .fetch_all(&pool)
        .await
        .unwrap();

        assert!(
            broken.is_empty(),
            "BEFORE trigger(s) with whole-row OLD/NEW reference on tables that have \
             generated columns — pg_dump → pg_restore will fail to restore them: {broken:?}"
        );
    }

    /// End-to-end guard for the customer-facing `pg_dump` → `pg_restore`
    /// failure: simulate what `pg_restore` does (re-execute every trigger's
    /// DDL against the post-migration schema), then drive the same catalog
    /// API paths the running server uses to confirm the restored DB is
    /// actually functional for namespaces, tables, and views.
    ///
    /// `pg_get_triggerdef(oid)` returns the exact text `pg_dump` emits, so
    /// re-executing it is equivalent — at the trigger DDL stage — to
    /// restoring a freshly-built dump. Any trigger Postgres now refuses
    /// (e.g. whole-row OLD/NEW on a table that has acquired a generated
    /// column) fails here, just as on a real restore.
    ///
    /// CRUD goes through the catalog ops so the test tracks schema changes
    /// automatically — adding a NOT NULL column to one of these tables won't
    /// silently fall out of the seed path.
    #[sqlx::test(migrations = false)]
    async fn test_pg_restore_round_trip_then_crud(pool: PgPool) {
        use std::collections::HashMap;

        use iceberg_ext::NamespaceIdent;
        use lakekeeper::service::{CatalogNamespaceOps as _, TabularId, Transaction as _};
        use lakekeeper_io::Location;

        use crate::{
            CatalogState, PostgresBackend, PostgresTransaction,
            namespace::tests::initialize_namespace,
            tabular::{
                drop_tabular, set_tabular_protected,
                table::tests::initialize_table,
                view::{create_view, tests::view_request},
            },
            warehouse::test::initialize_warehouse,
        };

        migrate_core_only(&pool)
            .await
            .expect("core migrations must succeed");

        // Snapshot every user trigger's canonical DDL, drop, and re-create
        // — exactly the post-data step pg_restore runs.
        let triggers: Vec<(String, String, String, String)> = sqlx::query_as(
            "SELECT t.tgname::text, n.nspname::text, c.relname::text, pg_get_triggerdef(t.oid) \
             FROM   pg_trigger  t \
             JOIN   pg_class    c ON c.oid = t.tgrelid \
             JOIN   pg_namespace n ON n.oid = c.relnamespace \
             WHERE  n.nspname = current_schema() \
               AND  NOT t.tgisinternal",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert!(
            !triggers.is_empty(),
            "expected core migrations to create at least one trigger"
        );
        for (tgname, schema, relname, def) in &triggers {
            let drop = format!(r#"DROP TRIGGER "{tgname}" ON "{schema}"."{relname}""#);
            sqlx::query(AssertSqlSafe(drop))
                .execute(&pool)
                .await
                .unwrap_or_else(|e| panic!("DROP TRIGGER {tgname} on {relname} failed: {e}"));
            sqlx::query(AssertSqlSafe(def.clone()))
                .execute(&pool)
                .await
                .unwrap_or_else(|e| {
                    panic!(
                        "re-creating trigger `{tgname}` on `{relname}` after migrations failed — \
                     pg_restore would hit the same error: {e}\nDDL: {def}",
                    )
                });
        }

        // Drive the catalog API the same way the server does.
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let ns_ident = NamespaceIdent::from_vec(vec!["test_ns".to_string()]).unwrap();
        let initial_props = HashMap::from([("k".to_string(), "v0".to_string())]);
        let ns =
            initialize_namespace(state.clone(), warehouse_id, &ns_ident, Some(initial_props)).await;
        let namespace_id = ns.namespace_id();
        assert_eq!(*ns.version(), 0, "freshly-inserted namespace starts at v0");

        // --- Namespace U: each UPDATE must fire the recreated trigger ---
        let mut tx = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let after_protect = PostgresBackend::set_namespace_protected(
            warehouse_id,
            namespace_id,
            true,
            tx.transaction(),
        )
        .await
        .expect("set_namespace_protected must succeed post-restore");
        assert_eq!(*after_protect.version(), 1);

        let new_props = HashMap::from([("k".to_string(), "v1".to_string())]);
        let after_props = PostgresBackend::update_namespace_properties(
            warehouse_id,
            namespace_id,
            new_props,
            tx.transaction(),
        )
        .await
        .expect("update_namespace_properties must succeed post-restore");
        assert_eq!(*after_props.version(), 2);

        // Unprotect so the namespace doesn't block table/view inserts (it
        // doesn't today, but keeps the test resilient to future tightening).
        PostgresBackend::set_namespace_protected(
            warehouse_id,
            namespace_id,
            false,
            tx.transaction(),
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();

        // --- Table C / U / D ---
        let table = initialize_table(
            warehouse_id,
            state.clone(),
            false,
            Some(ns_ident.clone()),
            None,
            Some("my_table".to_string()),
        )
        .await;
        let table_tab_id = TabularId::Table(table.table_id);

        let mut tx = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        set_tabular_protected(warehouse_id, table_tab_id, true, tx.transaction())
            .await
            .expect("set_tabular_protected on table must succeed post-restore");
        drop_tabular(warehouse_id, table_tab_id, true, None, tx.transaction())
            .await
            .expect("drop_tabular on table must succeed post-restore");
        tx.commit().await.unwrap();

        // --- View C / U / D ---
        let view_uuid = Uuid::now_v7();
        let view_location: Location = "s3://test_bucket/my_view/".parse().unwrap();
        let view_metadata = view_request(Some(view_uuid), &view_location);
        let view_metadata_location: Location = format!(
            "s3://test_bucket/my_view/metadata/{}.gz.json",
            Uuid::now_v7()
        )
        .parse()
        .unwrap();

        let mut tx = pool.begin().await.unwrap();
        create_view(
            warehouse_id,
            namespace_id,
            &view_metadata_location,
            &mut tx,
            "my_view",
            &view_metadata,
        )
        .await
        .expect("create_view must succeed post-restore");
        tx.commit().await.unwrap();

        let view_tab_id = TabularId::View(view_uuid.into());
        let mut tx = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        set_tabular_protected(warehouse_id, view_tab_id, true, tx.transaction())
            .await
            .expect("set_tabular_protected on view must succeed post-restore");
        drop_tabular(warehouse_id, view_tab_id, true, None, tx.transaction())
            .await
            .expect("drop_tabular on view must succeed post-restore");
        tx.commit().await.unwrap();
    }
}
