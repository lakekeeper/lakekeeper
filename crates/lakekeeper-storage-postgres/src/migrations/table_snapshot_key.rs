//! Completes migration `20261105120000_table_snapshot_table_fkey` after the migration run
//! commits. The run holds every lock until it commits, so the scans run here, where they
//! never block writes.

use anyhow::Context;
use sqlx::{AssertSqlSafe, PgPool, Postgres, SqlSafeStr};

/// How long adding and validating the key wait for locks that other sessions hold. Short in unit
/// tests, which keep both tables busy for longer.
const LOCK_WAIT: &str = if cfg!(test) { "2s" } else { "60s" };

/// Adds the key unless the migration already did. Waiting for `SHARE UPDATE EXCLUSIVE` blocks no
/// writer and lets Postgres cancel a blocking autovacuum. Writers lock both tables in either order,
/// so each attempt at the key's lock waits 100 ms, below `deadlock_timeout`; `NOWAIT` would starve.
fn add_key() -> AssertSqlSafe<String> {
    AssertSqlSafe(format!(
        r#"
SET LOCAL lock_timeout = '{LOCK_WAIT}';
LOCK TABLE table_snapshot, "table" IN SHARE UPDATE EXCLUSIVE MODE;
SELECT set_config('lock_timeout', '100ms', true);
DO $$
DECLARE
    give_up CONSTANT timestamptz := clock_timestamp() + interval '{LOCK_WAIT}';
BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint
               WHERE conrelid = 'table_snapshot'::regclass AND conname = 'table_snapshot_table_id_fkey') THEN
        RETURN;
    END IF;
    LOOP
        BEGIN
            LOCK TABLE table_snapshot, "table" IN SHARE ROW EXCLUSIVE MODE;
            EXIT;
        EXCEPTION WHEN lock_not_available OR deadlock_detected THEN
            IF clock_timestamp() > give_up THEN
                RAISE;
            END IF;
            PERFORM pg_sleep(0.01 + random() * 0.04);
        END;
    END LOOP;
    ALTER TABLE table_snapshot ADD CONSTRAINT table_snapshot_table_id_fkey
        FOREIGN KEY (warehouse_id, table_id) REFERENCES "table" (warehouse_id, table_id) ON DELETE CASCADE NOT VALID;
END
$$"#
    ))
}

/// Snapshots without a schema id that outlived the drop of their table, with their statistics.
const DELETE_ORPHANS: &str = r#"
DELETE FROM table_snapshot s
WHERE NOT EXISTS (SELECT 1 FROM "table" t WHERE t.warehouse_id = s.warehouse_id AND t.table_id = s.table_id)"#;

/// Takes a lock that does not conflict with writes to either table. The bounded wait keeps a
/// vacuum that Postgres does not cancel from holding up `migrate`.
fn validate_key() -> AssertSqlSafe<String> {
    AssertSqlSafe(format!(
        "SET LOCAL lock_timeout = '{LOCK_WAIT}';
ALTER TABLE table_snapshot VALIDATE CONSTRAINT table_snapshot_table_id_fkey"
    ))
}

/// Adds the key if the migration found the tables busy, deletes orphaned snapshots and validates
/// the key, each in its own transaction. Once the key exists, no drop leaves a snapshot behind,
/// so the delete removes all of them. Does nothing once the key is validated.
pub(super) async fn complete(pool: &PgPool, lock_id: i64) -> anyhow::Result<()> {
    let validated: Option<bool> = sqlx::query_scalar(
        "SELECT convalidated FROM pg_constraint \
         WHERE conrelid = 'table_snapshot'::regclass AND conname = 'table_snapshot_table_id_fkey'",
    )
    .fetch_optional(pool)
    .await?;
    if validated == Some(true) {
        return Ok(());
    }
    run(pool, lock_id, add_key()).await.context(
        "Could not add the foreign key from `table_snapshot` to `\"table\"`, because other \
         sessions kept both tables busy",
    )?;
    run(pool, lock_id, DELETE_ORPHANS)
        .await
        .context("Could not delete the snapshots of dropped tables")?;
    run(pool, lock_id, validate_key())
        .await
        .context("Could not validate the foreign key from `table_snapshot` to `\"table\"`")?;
    tracing::info!("Validated the foreign key from `table_snapshot` to `\"table\"`");
    Ok(())
}

/// Runs `statement` in its own transaction, serialized with other migration runs.
async fn run(pool: &PgPool, lock_id: i64, statement: impl SqlSafeStr) -> anyhow::Result<()> {
    let mut transaction: sqlx::Transaction<'_, Postgres> = pool.begin().await?;
    sqlx::query("SET LOCAL statement_timeout = '60min'")
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(lock_id)
        .execute(&mut *transaction)
        .await?;
    sqlx::raw_sql(statement).execute(&mut *transaction).await?;
    transaction.commit().await?;
    Ok(())
}
