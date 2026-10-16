use lakekeeper::service::cache_invalidation::{PruneOutcome, PruneSettings};
use sqlx::{PgConnection, PgPool};

use super::{begin_with_timeout, system_identifier};

/// Seed of the pruning lock key, `hashtextextended(<oid of cache_change_log>::text, SEED)`:
/// one lock per log table, so Lakekeeper schemas in one database prune independently.
/// (`cachelog` in ASCII.)
const PRUNE_LOCK_SEED: i64 = 0x6361_6368_656C_6F67;

/// One pruning run on `pool`'s database.
///
/// Each run inserts a checkpoint of the current snapshot. Rows with an xid below the
/// `xmin` of the newest checkpoint older than the retention are deleted: their
/// transactions had finished when it was taken. Then rows of another cluster are deleted.
/// Every transaction of the run takes the pruning lock; the run skips or stops when
/// another instance holds it.
pub(crate) async fn prune(pool: &PgPool, settings: PruneSettings) -> sqlx::Result<PruneOutcome> {
    let mut conn = pool.acquire().await?;
    let Some(plan) = plan(&mut conn, settings).await? else {
        return Ok(PruneOutcome::Skipped);
    };
    let mut deleted_rows = 0;
    if let Some(below) = plan.below {
        let mut after = (plan.start_xid, 0);
        loop {
            let request = BatchRequest::builder()
                .system_identifier(plan.system_identifier)
                .after(after)
                .below(below)
                .limit(settings.batch_size.get())
                .build();
            let Some(batch) = delete_batch(&mut conn, settings, request).await? else {
                return Ok(PruneOutcome::Pruned { deleted_rows });
            };
            deleted_rows += batch.rows;
            match batch.last {
                Some(last) if batch.rows == u64::from(settings.batch_size.get()) => after = last,
                _ => break,
            }
        }
    }
    loop {
        let Some(rows) = delete_foreign_batch(&mut conn, settings).await? else {
            break;
        };
        deleted_rows += rows;
        if rows < u64::from(settings.batch_size.get()) {
            break;
        }
    }
    Ok(PruneOutcome::Pruned { deleted_rows })
}

/// Take the pruning lock for the current transaction, if no other session holds it.
async fn try_lock(conn: &mut PgConnection) -> sqlx::Result<bool> {
    sqlx::query_scalar!(
        r#"SELECT pg_try_advisory_xact_lock(
               hashtextextended(to_regclass('cache_change_log')::oid::text, $1)
           ) AS "locked!""#,
        PRUNE_LOCK_SEED,
    )
    .fetch_one(conn)
    .await
}

/// What a run deletes.
struct Plan {
    system_identifier: i64,
    /// Rows with an xid below it are deleted; `None` while no checkpoint is old enough.
    below: Option<u64>,
    /// Earlier runs deleted every row with a smaller xid.
    start_xid: u64,
}

/// Insert a checkpoint, delete the checkpoints a run no longer needs, and plan the
/// deletes. `None` when another instance holds the pruning lock.
async fn plan(conn: &mut PgConnection, settings: PruneSettings) -> sqlx::Result<Option<Plan>> {
    let mut tx = begin_with_timeout(conn, settings.statement_timeout).await?;
    if !try_lock(&mut tx).await? {
        return Ok(None);
    }
    let system_identifier = system_identifier(&mut tx).await?;
    sqlx::query!(
        "INSERT INTO cache_change_log_checkpoint (system_identifier, snapshot)
         VALUES ($1, pg_current_snapshot())",
        system_identifier,
    )
    .execute(&mut *tx)
    .await?;
    let old = sqlx::query!(
        r#"
        SELECT max(pg_snapshot_xmin(snapshot))::text AS "below",
               max(taken_at) AS "taken_at"
        FROM cache_change_log_checkpoint
        WHERE system_identifier = $1
          AND taken_at <= clock_timestamp() - make_interval(secs => $2)
        "#,
        system_identifier,
        settings.retention.as_secs_f64(),
    )
    .fetch_one(&mut *tx)
    .await?;
    // The newest old checkpoint stays: it sets the horizon until a newer one is old enough.
    sqlx::query!(
        "DELETE FROM cache_change_log_checkpoint
         WHERE system_identifier <> $1 OR taken_at < $2",
        system_identifier,
        old.taken_at,
    )
    .execute(&mut *tx)
    .await?;
    // A horizon above the current xmax came from another cluster with the same identifier,
    // such as a restored dump where the identifier is `0`.
    let pruned_below = sqlx::query_scalar!(
        r#"SELECT pruned_below::text AS "pruned_below!" FROM cache_change_log_horizon
           WHERE system_identifier = $1
             AND pruned_below <= pg_snapshot_xmax(pg_current_snapshot())"#,
        system_identifier,
    )
    .fetch_optional(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Some(Plan {
        system_identifier,
        below: old.below.as_deref().map(parse_xid).transpose()?,
        // A batch can stop inside the rows of one xid; the horizon is then one above it.
        start_xid: pruned_below
            .as_deref()
            .map(parse_xid)
            .transpose()?
            .map_or(0, |pruned_below| pruned_below.saturating_sub(1)),
    }))
}

fn parse_xid(xid: &str) -> sqlx::Result<u64> {
    xid.parse().map_err(|e| sqlx::Error::Decode(Box::new(e)))
}

/// One delete transaction: the rows after `after` with an xid below `below`.
#[derive(Debug, typed_builder::TypedBuilder)]
struct BatchRequest {
    system_identifier: i64,
    /// The batch starts after this `(xid, id)`.
    after: (u64, i64),
    below: u64,
    limit: u32,
}

/// What a delete transaction removed.
#[derive(Debug, PartialEq, Eq)]
struct Batch {
    rows: u64,
    /// The last `(xid, id)` deleted.
    last: Option<(u64, i64)>,
}

/// Run one delete transaction and move the horizon past the deleted rows in it. `None`
/// when another instance holds the pruning lock.
async fn delete_batch(
    conn: &mut PgConnection,
    settings: PruneSettings,
    request: BatchRequest,
) -> sqlx::Result<Option<Batch>> {
    let BatchRequest {
        system_identifier,
        after,
        below,
        limit,
    } = request;
    let mut tx = begin_with_timeout(conn, settings.statement_timeout).await?;
    if !try_lock(&mut tx).await? {
        return Ok(None);
    }
    // Deleted by ctid: log rows are never updated, and a range bound taken from a
    // subquery would make the planner scan the whole table.
    let deleted = sqlx::query!(
        r#"
        WITH deleted AS (
            DELETE FROM cache_change_log
            WHERE ctid = ANY(ARRAY(
                SELECT ctid FROM cache_change_log
                WHERE (xid, id) > ($1::text::xid8, $2::bigint) AND xid < $3::text::xid8
                ORDER BY xid, id
                LIMIT $4))
            RETURNING xid, id
        )
        SELECT (SELECT count(*) FROM deleted) AS "rows!",
               last.xid::text AS "last_xid?",
               last.id AS "last_id?"
        FROM (SELECT 1) one
        LEFT JOIN LATERAL (
            SELECT xid, id FROM deleted ORDER BY xid DESC, id DESC LIMIT 1
        ) last ON true
        "#,
        after.0.to_string(),
        after.1,
        below.to_string(),
        i64::from(limit),
    )
    .fetch_one(&mut *tx)
    .await?;
    let rows = u64::try_from(deleted.rows).unwrap_or(0);
    let last = match (deleted.last_xid, deleted.last_id) {
        (Some(xid), Some(id)) => Some((parse_xid(&xid)?, id)),
        _ => None,
    };
    // A full batch may have stopped inside the rows of its last xid.
    let pruned_below = match last {
        Some((xid, _)) if rows == u64::from(limit) => xid + 1,
        _ => below,
    };
    sqlx::query!(
        r#"
        INSERT INTO cache_change_log_horizon AS h (system_identifier, pruned_below)
        VALUES ($1, $2::text::xid8)
        ON CONFLICT (singleton) DO UPDATE SET
            pruned_below = CASE WHEN h.system_identifier = excluded.system_identifier
                                 AND h.pruned_below <= pg_snapshot_xmax(pg_current_snapshot())
                                THEN greatest(h.pruned_below, excluded.pruned_below)
                                ELSE excluded.pruned_below END,
            system_identifier = excluded.system_identifier
        "#,
        system_identifier,
        pruned_below.to_string(),
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Some(Batch { rows, last }))
}

/// Delete up to a batch of rows of another cluster. No committed row of this cluster has
/// an xid at or above the current xmax; such rows came from a dump of another cluster.
/// `None` when another instance holds the pruning lock.
async fn delete_foreign_batch(
    conn: &mut PgConnection,
    settings: PruneSettings,
) -> sqlx::Result<Option<u64>> {
    let mut tx = begin_with_timeout(conn, settings.statement_timeout).await?;
    if !try_lock(&mut tx).await? {
        return Ok(None);
    }
    let rows = sqlx::query_scalar!(
        r#"
        WITH deleted AS (
            DELETE FROM cache_change_log
            WHERE ctid = ANY(ARRAY(
                SELECT ctid FROM cache_change_log
                WHERE xid >= pg_snapshot_xmax(pg_current_snapshot())
                LIMIT $1))
            RETURNING 1
        )
        SELECT count(*) AS "rows!" FROM deleted
        "#,
        i64::from(settings.batch_size.get()),
    )
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Some(u64::try_from(rows).unwrap_or(0)))
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU32, time::Duration};

    use lakekeeper::service::cache_invalidation::{
        CacheChangeLog, ChangeLogSettings, PollOutcome, ResetReason,
    };

    use super::*;
    use crate::cache_change_log::{
        PostgresCacheChangeLog,
        test_support::{record, record_in},
    };

    fn settings(batch_size: u32) -> PruneSettings {
        PruneSettings {
            retention: Duration::from_secs(3600),
            batch_size: NonZeroU32::new(batch_size).unwrap(),
            statement_timeout: Duration::from_secs(10),
        }
    }

    async fn own_system_identifier(pool: &PgPool) -> i64 {
        system_identifier(&mut pool.acquire().await.unwrap())
            .await
            .unwrap()
    }

    /// Insert a checkpoint taken `minutes` ago whose snapshot has the given `xmin`, and
    /// return its id. Tests share one cluster, so a real snapshot's `xmin` depends on the
    /// transactions of other tests.
    async fn checkpoint(pool: &PgPool, system_identifier: i64, minutes: i32, xmin: u64) -> i64 {
        sqlx::query_scalar(
            "INSERT INTO cache_change_log_checkpoint (taken_at, system_identifier, snapshot)
             VALUES (now() - make_interval(mins => $2), $1, $3::text::pg_snapshot)
             RETURNING id",
        )
        .bind(system_identifier)
        .bind(minutes)
        .bind(format!("{xmin}:{xmin}:"))
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn remaining_keys(pool: &PgPool) -> Vec<String> {
        sqlx::query_scalar("SELECT keys[1] FROM cache_change_log ORDER BY xid, id")
            .fetch_all(pool)
            .await
            .unwrap()
    }

    async fn horizon(pool: &PgPool) -> Option<(i64, u64)> {
        let row: Option<(i64, String)> = sqlx::query_as(
            "SELECT system_identifier, pruned_below::text FROM cache_change_log_horizon",
        )
        .fetch_optional(pool)
        .await
        .unwrap();
        row.map(|(system_identifier, pruned_below)| {
            (system_identifier, pruned_below.parse().unwrap())
        })
    }

    async fn set_horizon(pool: &PgPool, system_identifier: i64, pruned_below: u64) {
        sqlx::query(
            "INSERT INTO cache_change_log_horizon (system_identifier, pruned_below)
             VALUES ($1, $2::text::xid8)",
        )
        .bind(system_identifier)
        .bind(pruned_below.to_string())
        .execute(pool)
        .await
        .unwrap();
    }

    async fn checkpoint_ids(pool: &PgPool) -> Vec<i64> {
        sqlx::query_scalar("SELECT id FROM cache_change_log_checkpoint ORDER BY id")
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
    async fn deletes_rows_below_the_xmin_of_an_old_checkpoint(pool: PgPool) {
        let sid = own_system_identifier(&pool).await;
        record(&pool, "secret", "a").await;
        record(&pool, "secret", "b").await;
        let c = record(&pool, "secret", "c").await;
        let old = checkpoint(&pool, sid, 120, c).await;

        assert_eq!(
            prune(&pool, settings(1000)).await.unwrap(),
            PruneOutcome::Pruned { deleted_rows: 2 }
        );
        assert_eq!(remaining_keys(&pool).await, ["c"]);
        assert_eq!(horizon(&pool).await, Some((sid, c)));
        // The run's own checkpoint follows the old one.
        let ids = checkpoint_ids(&pool).await;
        assert_eq!((ids.len(), ids[0]), (2, old));
    }

    #[sqlx::test]
    async fn a_checkpoint_younger_than_the_retention_prunes_nothing(pool: PgPool) {
        let sid = own_system_identifier(&pool).await;
        let a = record(&pool, "secret", "a").await;
        checkpoint(&pool, sid, 30, a + 1).await;

        assert_eq!(
            prune(&pool, settings(1000)).await.unwrap(),
            PruneOutcome::Pruned { deleted_rows: 0 }
        );
        assert_eq!(remaining_keys(&pool).await, ["a"]);
        assert_eq!(horizon(&pool).await, None);
        assert_eq!(checkpoint_ids(&pool).await.len(), 2);
    }

    #[sqlx::test]
    async fn a_run_checkpoints_running_transactions_as_running(pool: PgPool) {
        let mut long = pool.begin().await.unwrap();
        let long_xid = record_in(&mut long, "secret", "long").await;

        prune(&pool, settings(1000)).await.unwrap();
        long.commit().await.unwrap();
        // Once old, the run's checkpoint keeps the rows of the transaction.
        sqlx::query("UPDATE cache_change_log_checkpoint SET taken_at = now() - interval '2 hours'")
            .execute(&pool)
            .await
            .unwrap();
        let visible: bool = sqlx::query_scalar(
            "SELECT pg_visible_in_snapshot($1::text::xid8, snapshot) FROM cache_change_log_checkpoint",
        )
        .bind(long_xid.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();

        assert!(!visible);
        assert_eq!(
            prune(&pool, settings(1000)).await.unwrap(),
            PruneOutcome::Pruned { deleted_rows: 0 }
        );
        assert_eq!(remaining_keys(&pool).await, ["long"]);
    }

    #[sqlx::test]
    async fn rows_are_deleted_in_batches_across_transactions(pool: PgPool) {
        let sid = own_system_identifier(&pool).await;
        let mut tx = pool.begin().await.unwrap();
        for key in ["k1", "k2", "k3"] {
            record_in(&mut tx, "secret", key).await;
        }
        tx.commit().await.unwrap();
        record(&pool, "secret", "k4").await;
        let k5 = record(&pool, "secret", "k5").await;
        checkpoint(&pool, sid, 120, k5 + 1).await;

        assert_eq!(
            prune(&pool, settings(2)).await.unwrap(),
            PruneOutcome::Pruned { deleted_rows: 5 }
        );
        assert_eq!(remaining_keys(&pool).await, Vec::<String>::new());
        assert_eq!(horizon(&pool).await, Some((sid, k5 + 1)));
    }

    #[sqlx::test]
    async fn rows_filling_the_last_batch_exactly_are_all_deleted(pool: PgPool) {
        let sid = own_system_identifier(&pool).await;
        let mut last = 0;
        for key in ["k1", "k2", "k3", "k4"] {
            last = record(&pool, "secret", key).await;
        }
        checkpoint(&pool, sid, 120, last + 1).await;

        assert_eq!(
            prune(&pool, settings(2)).await.unwrap(),
            PruneOutcome::Pruned { deleted_rows: 4 }
        );
        assert_eq!(remaining_keys(&pool).await, Vec::<String>::new());
        assert_eq!(horizon(&pool).await, Some((sid, last + 1)));
    }

    #[sqlx::test]
    async fn a_batch_that_stops_inside_a_transaction_sets_the_horizon_above_it(pool: PgPool) {
        let sid = own_system_identifier(&pool).await;
        let mut tx = pool.begin().await.unwrap();
        let mut xid = 0;
        for key in ["k1", "k2", "k3"] {
            xid = record_in(&mut tx, "secret", key).await;
        }
        tx.commit().await.unwrap();
        let after = record(&pool, "secret", "after").await;
        checkpoint(&pool, sid, 120, after + 1).await;

        let request = BatchRequest::builder()
            .system_identifier(sid)
            .after((0, 0))
            .below(after + 1)
            .limit(2)
            .build();
        let batch = delete_batch(&mut pool.acquire().await.unwrap(), settings(2), request)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(batch.rows, 2);
        assert_eq!(remaining_keys(&pool).await, ["k3", "after"]);
        assert_eq!(horizon(&pool).await, Some((sid, xid + 1)));

        // The next run starts at the xid below the horizon.
        assert_eq!(
            prune(&pool, settings(1000)).await.unwrap(),
            PruneOutcome::Pruned { deleted_rows: 2 }
        );
        assert_eq!(remaining_keys(&pool).await, Vec::<String>::new());
        assert_eq!(horizon(&pool).await, Some((sid, after + 1)));
    }

    #[sqlx::test]
    async fn a_run_skips_while_another_instance_prunes_the_same_schema(pool: PgPool) {
        let sid = own_system_identifier(&pool).await;
        let a = record(&pool, "secret", "a").await;
        let old = checkpoint(&pool, sid, 120, a + 1).await;
        let lock = hold_lock(&pool, "cache_change_log").await;

        assert_eq!(
            prune(&pool, settings(1000)).await.unwrap(),
            PruneOutcome::Skipped
        );
        assert_eq!(remaining_keys(&pool).await, ["a"]);
        assert_eq!(checkpoint_ids(&pool).await, [old]);
        lock.rollback().await.unwrap();
    }

    #[sqlx::test]
    async fn a_batch_deletes_nothing_while_another_instance_holds_the_lock(pool: PgPool) {
        let sid = own_system_identifier(&pool).await;
        let a = record(&pool, "secret", "a").await;
        let lock = hold_lock(&pool, "cache_change_log").await;

        let request = BatchRequest::builder()
            .system_identifier(sid)
            .after((0, 0))
            .below(a + 1)
            .limit(1000)
            .build();
        assert_eq!(
            delete_batch(&mut pool.acquire().await.unwrap(), settings(1000), request)
                .await
                .unwrap(),
            None
        );
        assert_eq!(remaining_keys(&pool).await, ["a"]);
        lock.rollback().await.unwrap();
    }

    #[sqlx::test]
    async fn the_lock_of_a_log_in_another_schema_does_not_block_a_run(pool: PgPool) {
        let sid = own_system_identifier(&pool).await;
        let a = record(&pool, "secret", "a").await;
        checkpoint(&pool, sid, 120, a + 1).await;
        sqlx::raw_sql(
            "CREATE SCHEMA other_lakekeeper;
             CREATE TABLE other_lakekeeper.cache_change_log (LIKE cache_change_log)",
        )
        .execute(&pool)
        .await
        .unwrap();
        let lock = hold_lock(&pool, "other_lakekeeper.cache_change_log").await;

        assert_eq!(
            prune(&pool, settings(1000)).await.unwrap(),
            PruneOutcome::Pruned { deleted_rows: 1 }
        );
        lock.rollback().await.unwrap();
    }

    #[sqlx::test]
    async fn the_horizon_never_moves_back(pool: PgPool) {
        let sid = own_system_identifier(&pool).await;
        let a = record(&pool, "secret", "a").await;
        let b = record(&pool, "secret", "b").await;
        checkpoint(&pool, sid, 120, a + 1).await;
        set_horizon(&pool, sid, b + 1).await;

        // A run starts at the horizon: the rows below it count as deleted.
        assert_eq!(
            prune(&pool, settings(1000)).await.unwrap(),
            PruneOutcome::Pruned { deleted_rows: 0 }
        );
        assert_eq!(horizon(&pool).await, Some((sid, b + 1)));
    }

    #[sqlx::test]
    async fn a_horizon_above_the_current_xmax_is_replaced(pool: PgPool) {
        let sid = own_system_identifier(&pool).await;
        let a = record(&pool, "secret", "a").await;
        checkpoint(&pool, sid, 120, a + 1).await;
        set_horizon(&pool, sid, 1_000_000_000_000).await;

        assert_eq!(
            prune(&pool, settings(1000)).await.unwrap(),
            PruneOutcome::Pruned { deleted_rows: 1 }
        );
        assert_eq!(horizon(&pool).await, Some((sid, a + 1)));
    }

    #[sqlx::test]
    async fn a_failing_delete_of_foreign_rows_keeps_the_checkpoint_and_the_pruning(pool: PgPool) {
        let sid = own_system_identifier(&pool).await;
        let a = record(&pool, "secret", "a").await;
        checkpoint(&pool, sid, 120, a + 1).await;
        sqlx::query(
            "INSERT INTO cache_change_log (xid, domain, keys)
             VALUES ('1000000000000', 'secret', ARRAY['future'])",
        )
        .execute(&pool)
        .await
        .unwrap();
        let mut blocker = pool.begin().await.unwrap();
        sqlx::query("SELECT 1 FROM cache_change_log WHERE xid = '1000000000000' FOR UPDATE")
            .execute(&mut *blocker)
            .await
            .unwrap();

        let short = PruneSettings {
            statement_timeout: Duration::from_millis(200),
            ..settings(1000)
        };
        assert!(prune(&pool, short).await.is_err());
        blocker.rollback().await.unwrap();
        assert_eq!(remaining_keys(&pool).await, ["future"]);
        assert_eq!(checkpoint_ids(&pool).await.len(), 2);
        assert_eq!(horizon(&pool).await, Some((sid, a + 1)));
    }

    #[sqlx::test]
    async fn state_of_another_cluster_is_replaced_and_its_future_rows_deleted(pool: PgPool) {
        let sid = own_system_identifier(&pool).await;
        let foreign = sid ^ 1;
        set_horizon(&pool, foreign, 1_000_000_000_000).await;
        checkpoint(&pool, foreign, 180, 1_000_000_000).await;
        sqlx::query(
            "INSERT INTO cache_change_log (xid, domain, keys)
             SELECT (1000000000000 + n)::text::xid8, 'secret', ARRAY['future']
             FROM generate_series(1, 3) n",
        )
        .execute(&pool)
        .await
        .unwrap();
        record(&pool, "secret", "a").await;
        let c = record(&pool, "secret", "c").await;
        let own = checkpoint(&pool, sid, 120, c).await;

        assert_eq!(
            prune(&pool, settings(2)).await.unwrap(),
            PruneOutcome::Pruned { deleted_rows: 4 }
        );
        assert_eq!(remaining_keys(&pool).await, ["c"]);
        assert_eq!(horizon(&pool).await, Some((sid, c)));
        let ids = checkpoint_ids(&pool).await;
        assert_eq!((ids.len(), ids[0]), (2, own));
    }

    #[sqlx::test]
    async fn checkpoints_before_the_newest_old_one_are_deleted(pool: PgPool) {
        let sid = own_system_identifier(&pool).await;
        checkpoint(&pool, sid, 180, 1).await;
        let aged = checkpoint(&pool, sid, 120, 1).await;
        let young = checkpoint(&pool, sid, 30, 1).await;

        prune(&pool, settings(1000)).await.unwrap();

        let ids = checkpoint_ids(&pool).await;
        assert_eq!((ids.len(), ids[0], ids[1]), (3, aged, young));
    }

    #[sqlx::test]
    async fn a_reader_whose_cursor_is_below_the_new_horizon_resets(pool: PgPool) {
        let sid = own_system_identifier(&pool).await;
        let mut reader = PostgresCacheChangeLog::for_test_pool(
            &pool,
            ChangeLogSettings {
                statement_timeout: Duration::from_secs(10),
            },
        );
        reader.restart().await.unwrap();
        let a = record(&pool, "secret", "a").await;
        checkpoint(&pool, sid, 120, a + 1).await;

        prune(&pool, settings(1000)).await.unwrap();

        assert_eq!(
            reader.poll(1000).await.unwrap(),
            PollOutcome::Reset(ResetReason::HorizonPassed)
        );
    }
}
