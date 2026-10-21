use lakekeeper::service::cache_invalidation::{PruneOutcome, PruneSettings};
use sqlx::{PgConnection, PgPool};

use super::begin_with_timeout;

/// Seed of the pruning lock key, `hashtextextended(<oid of cache_change_log>::text, SEED)`:
/// one lock per log table, so Lakekeeper schemas in one database prune independently.
/// (`cachelog` in ASCII.)
const PRUNE_LOCK_SEED: i64 = 0x6361_6368_656C_6F67;

/// The most rows one delete transaction removes.
const PRUNE_BATCH_ROWS: i64 = 10_000;

/// One pruning run on `pool`'s database: delete the rows older than the retention. Every
/// transaction of the run takes the pruning lock; the run skips or stops when another
/// instance holds it.
pub(crate) async fn prune(pool: &PgPool, settings: PruneSettings) -> sqlx::Result<PruneOutcome> {
    prune_in_batches(pool, settings, PRUNE_BATCH_ROWS).await
}

async fn prune_in_batches(
    pool: &PgPool,
    settings: PruneSettings,
    batch_rows: i64,
) -> sqlx::Result<PruneOutcome> {
    let mut conn = pool.acquire().await?;
    let mut outcome = PruneOutcome::Skipped;
    while let Some(rows) = delete_batch(&mut conn, settings, batch_rows).await? {
        let deleted_rows = match outcome {
            PruneOutcome::Pruned { deleted_rows } => deleted_rows + rows,
            PruneOutcome::Skipped => rows,
        };
        outcome = PruneOutcome::Pruned { deleted_rows };
        if rows < u64::try_from(batch_rows).unwrap_or(u64::MAX) {
            break;
        }
    }
    Ok(outcome)
}

/// Delete up to `limit` rows in one transaction. `None` when another instance holds the
/// pruning lock.
async fn delete_batch(
    conn: &mut PgConnection,
    settings: PruneSettings,
    limit: i64,
) -> sqlx::Result<Option<u64>> {
    let mut tx = begin_with_timeout(conn, settings.statement_timeout).await?;
    let locked = sqlx::query_scalar!(
        r#"SELECT pg_try_advisory_xact_lock(
               hashtextextended(to_regclass('cache_change_log')::oid::text, $1)
           ) AS "locked!""#,
        PRUNE_LOCK_SEED,
    )
    .fetch_one(&mut *tx)
    .await?;
    if !locked {
        return Ok(None);
    }
    // Deleted by ctid: log rows are never updated.
    let rows = sqlx::query_scalar!(
        r#"
        WITH deleted AS (
            DELETE FROM cache_change_log
            WHERE ctid = ANY(ARRAY(
                SELECT ctid FROM cache_change_log
                WHERE changed_at < now() - make_interval(secs => $1)
                LIMIT $2))
            RETURNING 1
        )
        SELECT count(*) AS "rows!" FROM deleted
        "#,
        settings.retention.as_secs_f64(),
        limit,
    )
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Some(u64::try_from(rows).unwrap_or(0)))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::cache_change_log::test_support::record;

    const SETTINGS: PruneSettings = PruneSettings {
        retention: Duration::from_secs(3600),
        statement_timeout: Duration::from_secs(10),
    };

    /// Make the rows of `keys` look `minutes` old.
    async fn age(pool: &PgPool, keys: &[&str], minutes: i32) {
        sqlx::query(
            "UPDATE cache_change_log SET changed_at = now() - make_interval(mins => $2)
             WHERE keys[1] = ANY($1)",
        )
        .bind(keys)
        .bind(minutes)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn remaining_keys(pool: &PgPool) -> Vec<String> {
        sqlx::query_scalar("SELECT keys[1] FROM cache_change_log ORDER BY xid, id")
            .fetch_all(pool)
            .await
            .unwrap()
    }

    /// Hold the pruning lock of the log table `table` in an open transaction.
    async fn hold_lock(pool: &PgPool, table: &str) -> sqlx::Transaction<'static, sqlx::Postgres> {
        let mut tx = pool.begin().await.unwrap();
        sqlx::query(
            "SELECT pg_advisory_xact_lock(hashtextextended(to_regclass($1)::oid::text, $2))",
        )
        .bind(table)
        .bind(PRUNE_LOCK_SEED)
        .execute(&mut *tx)
        .await
        .unwrap();
        tx
    }

    #[sqlx::test]
    async fn deletes_the_rows_older_than_the_retention(pool: PgPool) {
        for key in ["old", "young", "new"] {
            record(&pool, "secret", key).await;
        }
        age(&pool, &["old"], 61).await;
        age(&pool, &["young"], 59).await;

        assert_eq!(
            prune(&pool, SETTINGS).await.unwrap(),
            PruneOutcome::Pruned { deleted_rows: 1 }
        );
        assert_eq!(remaining_keys(&pool).await, ["young", "new"]);
    }

    #[sqlx::test]
    async fn deletes_in_batches_until_no_old_row_is_left(pool: PgPool) {
        let keys = ["k1", "k2", "k3", "k4", "k5"];
        for key in keys {
            record(&pool, "secret", key).await;
        }
        age(&pool, &keys, 120).await;

        assert_eq!(
            prune_in_batches(&pool, SETTINGS, 2).await.unwrap(),
            PruneOutcome::Pruned { deleted_rows: 5 }
        );
        assert_eq!(remaining_keys(&pool).await, Vec::<String>::new());
    }

    #[sqlx::test]
    async fn a_run_skips_while_another_instance_prunes_the_same_log(pool: PgPool) {
        record(&pool, "secret", "old").await;
        age(&pool, &["old"], 120).await;
        let lock = hold_lock(&pool, "cache_change_log").await;

        assert_eq!(prune(&pool, SETTINGS).await.unwrap(), PruneOutcome::Skipped);
        assert_eq!(remaining_keys(&pool).await, ["old"]);
        lock.rollback().await.unwrap();
    }

    #[sqlx::test]
    async fn the_lock_of_a_log_in_another_schema_does_not_block_a_run(pool: PgPool) {
        record(&pool, "secret", "old").await;
        age(&pool, &["old"], 120).await;
        sqlx::raw_sql(
            "CREATE SCHEMA other_lakekeeper;
             CREATE TABLE other_lakekeeper.cache_change_log (LIKE cache_change_log)",
        )
        .execute(&pool)
        .await
        .unwrap();
        let lock = hold_lock(&pool, "other_lakekeeper.cache_change_log").await;

        assert_eq!(
            prune(&pool, SETTINGS).await.unwrap(),
            PruneOutcome::Pruned { deleted_rows: 1 }
        );
        lock.rollback().await.unwrap();
    }
}
