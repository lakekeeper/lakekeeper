//! Completes migration `20261105120000_table_snapshot_table_fkey` after the migration run
//! commits. The run holds every lock until it commits, so the scans run here, where they
//! never block writes.

use anyhow::Context;
use sqlx::{PgPool, Postgres};

/// Adds the key unless the migration already did. Writers lock the two tables in either order, so
/// each attempt waits at most 100 ms, less than `deadlock_timeout`; `NOWAIT` starves under steady
/// writes. Retries for up to 60 s.
const ADD_KEY: &str = r#"
SET LOCAL lock_timeout = '100ms';
DO $$
DECLARE
    give_up CONSTANT timestamptz := clock_timestamp() + interval '60 seconds';
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
$$"#;

/// Snapshots without a schema id that outlived the drop of their table, with their statistics.
const DELETE_ORPHANS: &str = r#"
DELETE FROM table_snapshot s
WHERE NOT EXISTS (SELECT 1 FROM "table" t WHERE t.warehouse_id = s.warehouse_id AND t.table_id = s.table_id)"#;

/// Takes a lock that does not conflict with writes to either table.
const VALIDATE_KEY: &str =
    "ALTER TABLE table_snapshot VALIDATE CONSTRAINT table_snapshot_table_id_fkey";

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
    run(pool, lock_id, ADD_KEY).await.context(
        "Could not add the foreign key from `table_snapshot` to `\"table\"` within 60 s, because \
         writes kept both tables busy. The next `migrate` tries again.",
    )?;
    run(pool, lock_id, DELETE_ORPHANS)
        .await
        .context("Could not delete the snapshots of dropped tables")?;
    run(pool, lock_id, VALIDATE_KEY)
        .await
        .context("Could not validate the foreign key from `table_snapshot` to `\"table\"`")?;
    tracing::info!("Validated the foreign key from `table_snapshot` to `\"table\"`");
    Ok(())
}

/// Runs `statement` in its own transaction, serialized with other migration runs.
async fn run(pool: &PgPool, lock_id: i64, statement: &'static str) -> anyhow::Result<()> {
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
