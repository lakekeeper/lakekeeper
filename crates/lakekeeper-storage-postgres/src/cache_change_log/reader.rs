use std::time::Duration;

use chrono::{DateTime, Utc};
use lakekeeper::service::cache_invalidation::{
    CacheChangeLog, ChangeLogError, LoggedChange, PollOutcome, ResetReason,
};
use sqlx::{PgConnection, PgPool};

use super::begin_with_timeout;

/// The most rows one poll returns. More changes since the previous poll make it reset.
const POLL_MAX_ROWS: i64 = 1000;
/// The longest the poll statement may run on the server. The poll gives up after one and
/// a half times as long.
const POLL_STATEMENT_TIMEOUT: Duration = Duration::from_secs(10);

/// Where the previous poll stopped.
#[derive(Debug, Clone)]
struct Cursor {
    /// The previous poll's snapshot, as Postgres prints a `pg_snapshot`.
    snapshot: String,
    /// The start time of the server that took it.
    postmaster_start: DateTime<Utc>,
}

/// The Postgres change log: a snapshot diff over `cache_change_log` on the primary.
///
/// Each poll takes a snapshot and returns the rows of transactions that committed between
/// the previous poll's snapshot and it: those running at the previous snapshot and those
/// started after it.
#[derive(Debug)]
pub struct PostgresCacheChangeLog {
    pool: PgPool,
    statement_timeout: Duration,
    max_rows: i64,
    cursor: Option<Cursor>,
}

impl PostgresCacheChangeLog {
    /// The log of `state`'s database, read through the write pool.
    pub(crate) fn for_state(state: &crate::CatalogState) -> Self {
        Self::new(state.write_pool())
    }

    fn new(pool: PgPool) -> Self {
        Self {
            pool,
            statement_timeout: POLL_STATEMENT_TIMEOUT,
            max_rows: POLL_MAX_ROWS,
            cursor: None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("A change log poll took longer than {0:?}")]
struct PollTimedOut(Duration);

fn query_error(e: impl std::error::Error + Send + Sync + 'static) -> ChangeLogError {
    ChangeLogError::Query(Box::new(e))
}

/// What one poll statement read.
struct Poll {
    in_recovery: bool,
    postmaster_start: DateTime<Utc>,
    /// The statement's snapshot.
    snapshot: String,
    /// Whether the snapshot's `xmax` is below the previous snapshot's.
    went_back: bool,
    rows: Vec<LoggedChange>,
}

/// Read the rows of transactions that committed between `prev` and the statement's
/// snapshot, at most `limit`. Without `prev`, no rows.
async fn read_poll(
    conn: &mut PgConnection,
    statement_timeout: Duration,
    prev: Option<&str>,
    limit: i64,
) -> sqlx::Result<Poll> {
    let mut tx = begin_with_timeout(conn, statement_timeout).await?;
    // Two branches: transactions running at the previous snapshot, and transactions
    // started after it, below the new snapshot's xmax. A row with a higher xid came from
    // another server, by a dump and restore. `pg_current_snapshot()` is the statement's own
    // snapshot, so the rows it reads are those of transactions committed in it.
    let rows = sqlx::query!(
        r#"
        WITH s AS (SELECT pg_current_snapshot() AS next),
        changed AS (
            (SELECT l.xid, l.id, l.domain, l.keys
             FROM cache_change_log l
             WHERE l.xid = ANY(ARRAY(SELECT pg_snapshot_xip($1::text::pg_snapshot)))
             ORDER BY l.xid, l.id
             LIMIT $2)
            UNION ALL
            (SELECT l.xid, l.id, l.domain, l.keys
             FROM cache_change_log l
             WHERE l.xid >= pg_snapshot_xmax($1::text::pg_snapshot)
               AND l.xid < pg_snapshot_xmax(pg_current_snapshot())
             ORDER BY l.xid, l.id
             LIMIT $2)
            ORDER BY xid, id
            LIMIT $2
        )
        SELECT pg_is_in_recovery() AS "in_recovery!",
               pg_postmaster_start_time() AS "postmaster_start!",
               s.next::text AS "snapshot!",
               coalesce(pg_snapshot_xmax(s.next) < pg_snapshot_xmax($1::text::pg_snapshot), false)
                   AS "went_back!",
               c.domain AS "domain?",
               c.keys AS "keys?"
        FROM s LEFT JOIN changed c ON true
        ORDER BY c.xid, c.id
        "#,
        prev,
        limit,
    )
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    let first = rows.first().ok_or(sqlx::Error::RowNotFound)?;
    let poll = Poll {
        in_recovery: first.in_recovery,
        postmaster_start: first.postmaster_start,
        snapshot: first.snapshot.clone(),
        went_back: first.went_back,
        rows: Vec::new(),
    };
    let rows = rows
        .into_iter()
        .filter_map(|r| {
            Some(LoggedChange {
                domain: r.domain?,
                keys: r.keys?,
            })
        })
        .collect();
    Ok(Poll { rows, ..poll })
}

#[async_trait::async_trait]
impl CacheChangeLog for PostgresCacheChangeLog {
    async fn poll(&mut self) -> Result<PollOutcome, ChangeLogError> {
        let timeout = self.statement_timeout;
        let mut conn = self.pool.acquire().await.map_err(query_error)?;
        let prev = self.cursor.as_ref().map(|cursor| cursor.snapshot.as_str());
        // One row more than a poll returns shows that there are more.
        let read = read_poll(&mut conn, timeout, prev, self.max_rows + 1);
        // The client-side limit also covers a connection that stopped answering.
        let poll = match tokio::time::timeout(timeout.saturating_add(timeout / 2), read).await {
            Ok(poll) => poll.map_err(query_error)?,
            Err(_) => {
                conn.close_on_drop();
                return Err(query_error(PollTimedOut(timeout)));
            }
        };
        if poll.in_recovery {
            return Err(ChangeLogError::Standby);
        }
        let outcome = match &self.cursor {
            None => PollOutcome::Changes(Vec::new()),
            Some(cursor) if cursor.postmaster_start != poll.postmaster_start => {
                PollOutcome::Reset(ResetReason::ServerChanged)
            }
            Some(_) if poll.went_back => PollOutcome::Reset(ResetReason::TransactionIdsWentBack),
            Some(_) if i64::try_from(poll.rows.len()).unwrap_or(i64::MAX) > self.max_rows => {
                PollOutcome::Reset(ResetReason::Overflow)
            }
            Some(_) => PollOutcome::Changes(poll.rows),
        };
        self.cursor = Some(Cursor {
            snapshot: poll.snapshot,
            postmaster_start: poll.postmaster_start,
        });
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use sqlx::Connection;

    use super::*;
    use crate::cache_change_log::test_support::{record, record_in};

    fn log(pool: &PgPool) -> PostgresCacheChangeLog {
        PostgresCacheChangeLog::new(pool.clone())
    }

    /// A log whose first poll already ran.
    async fn started(pool: &PgPool) -> PostgresCacheChangeLog {
        let mut log = log(pool);
        assert_eq!(log.poll().await.unwrap(), changes(&[]));
        log
    }

    fn changes(changes: &[(&str, &str)]) -> PollOutcome {
        PollOutcome::Changes(
            changes
                .iter()
                .map(|(domain, key)| LoggedChange {
                    domain: (*domain).to_string(),
                    keys: vec![(*key).to_string()],
                })
                .collect(),
        )
    }

    #[sqlx::test]
    async fn the_first_poll_starts_the_log_and_returns_nothing(pool: PgPool) {
        record(&pool, "secret", "before").await;
        let mut log = started(&pool).await;

        record(&pool, "secret", "a").await;
        record(&pool, "role", "b").await;

        assert_eq!(
            log.poll().await.unwrap(),
            changes(&[("secret", "a"), ("role", "b")])
        );
        assert_eq!(log.poll().await.unwrap(), changes(&[]));
    }

    #[sqlx::test]
    async fn a_transaction_running_at_the_start_is_returned_when_it_commits(pool: PgPool) {
        let mut running = pool.begin().await.unwrap();
        record_in(&mut running, "secret", "k").await;
        let mut log = started(&pool).await;
        assert_eq!(log.poll().await.unwrap(), changes(&[]));

        running.commit().await.unwrap();
        assert_eq!(log.poll().await.unwrap(), changes(&[("secret", "k")]));
    }

    #[sqlx::test]
    async fn rows_of_more_subtransactions_than_a_snapshot_lists_are_returned(pool: PgPool) {
        // Postgres lists at most 64 subtransactions per backend; beyond that, snapshots
        // are marked overflowed. Rows carry the top-level xid either way.
        let mut log = started(&pool).await;
        let mut tx = pool.begin().await.unwrap();
        let mut kept = Vec::new();
        for i in 0..120 {
            let key = format!("k{i:03}");
            let mut savepoint = tx.begin().await.unwrap();
            record_in(&mut savepoint, "secret", &key).await;
            if i % 3 == 0 {
                savepoint.rollback().await.unwrap();
            } else {
                savepoint.commit().await.unwrap();
                kept.push(key);
            }
        }
        record(&pool, "role", "other").await;

        assert_eq!(log.poll().await.unwrap(), changes(&[("role", "other")]));
        tx.commit().await.unwrap();
        let expected: Vec<_> = kept.iter().map(|key| ("secret", key.as_str())).collect();
        assert_eq!(log.poll().await.unwrap(), changes(&expected));
        assert_eq!(log.poll().await.unwrap(), changes(&[]));
    }

    #[sqlx::test]
    async fn a_transaction_open_across_polls_is_returned_once_after_it_commits(pool: PgPool) {
        let mut log = started(&pool).await;
        let mut long = pool.begin().await.unwrap();
        record_in(&mut long, "secret", "l1").await;
        record(&pool, "role", "a").await;
        assert_eq!(log.poll().await.unwrap(), changes(&[("role", "a")]));

        record_in(&mut long, "secret", "l2").await;
        record(&pool, "role", "b").await;
        assert_eq!(log.poll().await.unwrap(), changes(&[("role", "b")]));

        record_in(&mut long, "secret", "l3").await;
        long.commit().await.unwrap();
        record(&pool, "role", "c").await;
        assert_eq!(
            log.poll().await.unwrap(),
            changes(&[
                ("secret", "l1"),
                ("secret", "l2"),
                ("secret", "l3"),
                ("role", "c")
            ])
        );
        assert_eq!(log.poll().await.unwrap(), changes(&[]));
    }

    #[sqlx::test]
    async fn rows_with_an_xid_this_server_has_not_reached_are_never_returned(pool: PgPool) {
        // Such rows come from another server, by a dump and restore.
        sqlx::query(
            "INSERT INTO cache_change_log (xid, domain, keys)
             VALUES ('1000000000000', 'secret', ARRAY['foreign'])",
        )
        .execute(&pool)
        .await
        .unwrap();
        let mut log = started(&pool).await;
        record(&pool, "secret", "own").await;

        assert_eq!(log.poll().await.unwrap(), changes(&[("secret", "own")]));
        assert_eq!(log.poll().await.unwrap(), changes(&[]));
    }

    #[sqlx::test]
    async fn more_rows_than_a_poll_returns_reset_and_the_next_poll_returns_the_later_changes(
        pool: PgPool,
    ) {
        let mut log = started(&pool).await;
        log.max_rows = 3;
        for key in ["k1", "k2", "k3"] {
            record(&pool, "secret", key).await;
        }
        assert_eq!(
            log.poll().await.unwrap(),
            changes(&[("secret", "k1"), ("secret", "k2"), ("secret", "k3")])
        );

        for key in ["k4", "k5", "k6", "k7"] {
            record(&pool, "secret", key).await;
        }

        assert_eq!(
            log.poll().await.unwrap(),
            PollOutcome::Reset(ResetReason::Overflow)
        );
        record(&pool, "secret", "later").await;
        assert_eq!(log.poll().await.unwrap(), changes(&[("secret", "later")]));
    }

    #[sqlx::test]
    async fn a_poll_past_the_statement_timeout_fails_and_the_next_returns_its_changes(
        pool: PgPool,
    ) {
        let mut log = started(&pool).await;
        log.statement_timeout = Duration::from_millis(200);
        record(&pool, "secret", "a").await;
        let mut lock = pool.begin().await.unwrap();
        sqlx::query("LOCK TABLE cache_change_log IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *lock)
            .await
            .unwrap();

        assert!(matches!(log.poll().await, Err(ChangeLogError::Query(_))));
        lock.rollback().await.unwrap();
        assert_eq!(log.poll().await.unwrap(), changes(&[("secret", "a")]));
    }

    #[sqlx::test]
    async fn a_restarted_server_resets_and_the_next_poll_returns_the_later_changes(pool: PgPool) {
        let mut log = started(&pool).await;
        record(&pool, "secret", "a").await;
        log.cursor.as_mut().unwrap().postmaster_start = DateTime::UNIX_EPOCH;

        assert_eq!(
            log.poll().await.unwrap(),
            PollOutcome::Reset(ResetReason::ServerChanged)
        );
        record(&pool, "secret", "b").await;
        assert_eq!(log.poll().await.unwrap(), changes(&[("secret", "b")]));
    }

    #[sqlx::test]
    async fn transaction_ids_going_back_reset(pool: PgPool) {
        let mut log = started(&pool).await;
        log.cursor.as_mut().unwrap().snapshot = "1000000000000:1000000000000:".to_string();

        assert_eq!(
            log.poll().await.unwrap(),
            PollOutcome::Reset(ResetReason::TransactionIdsWentBack)
        );
        record(&pool, "secret", "b").await;
        assert_eq!(log.poll().await.unwrap(), changes(&[("secret", "b")]));
    }

    /// What one writer did: the keys of committed rows, and of rows that were rolled back.
    #[derive(Default)]
    struct Written {
        committed: HashSet<String>,
        rolled_back: HashSet<String>,
    }

    /// Run random transactions: some rows in rolled-back savepoints, some transactions
    /// rolled back, pauses while the transaction is open.
    async fn write_randomly(pool: PgPool, seed: u64, writer: u64, transactions: usize) -> Written {
        let mut rng = fastrand::Rng::with_seed(seed * 1000 + writer);
        let mut written = Written::default();
        for t in 0..transactions {
            let mut tx = pool.begin().await.unwrap();
            let mut kept = Vec::new();
            for r in 0..rng.usize(1..=3) {
                let key = format!("s{seed}-w{writer}-t{t}-r{r}");
                if rng.u8(0..5) == 0 {
                    let mut savepoint = tx.begin().await.unwrap();
                    record_in(&mut savepoint, "secret", &key).await;
                    savepoint.rollback().await.unwrap();
                    written.rolled_back.insert(key);
                } else {
                    record_in(&mut tx, "secret", &key).await;
                    kept.push(key);
                }
                tokio::time::sleep(Duration::from_millis(rng.u64(0..4))).await;
            }
            if kept.is_empty() || rng.u8(0..4) == 0 {
                tx.rollback().await.unwrap();
                written.rolled_back.extend(kept);
            } else {
                tx.commit().await.unwrap();
                written.committed.extend(kept);
            }
        }
        written
    }

    /// Poll while writers run. Every committed row is returned exactly once, and no row that
    /// was rolled back.
    #[sqlx::test]
    async fn concurrent_writers_lose_no_committed_row(pool: PgPool) {
        const SEEDS: u64 = 12;
        const WRITERS: u64 = 4;
        const TRANSACTIONS: usize = 15;
        for seed in 0..SEEDS {
            let mut rng = fastrand::Rng::with_seed(seed);
            let mut log = started(&pool).await;
            let writers: Vec<_> = (0..WRITERS)
                .map(|w| tokio::spawn(write_randomly(pool.clone(), seed, w, TRANSACTIONS)))
                .collect();

            let mut seen: HashMap<String, usize> = HashMap::new();
            let mut poll = async |log: &mut PostgresCacheChangeLog| match log.poll().await {
                Ok(PollOutcome::Changes(changes)) => {
                    for key in changes.into_iter().flat_map(|c| c.keys) {
                        *seen.entry(key).or_default() += 1;
                    }
                }
                Ok(PollOutcome::Reset(reason)) => panic!("seed {seed}: reset {reason:?}"),
                Err(error) => panic!("seed {seed}: {error}"),
            };
            while !writers.iter().all(tokio::task::JoinHandle::is_finished) {
                poll(&mut log).await;
                tokio::time::sleep(Duration::from_millis(rng.u64(0..3))).await;
            }
            let mut expected = Written::default();
            for writer in writers {
                let written = writer.await.unwrap();
                expected.committed.extend(written.committed);
                expected.rolled_back.extend(written.rolled_back);
            }
            // Its snapshot is taken after every writer finished.
            poll(&mut log).await;

            assert!(
                seen.values().all(|count| *count == 1),
                "seed {seed}: a row was returned twice"
            );
            assert!(
                expected
                    .rolled_back
                    .iter()
                    .all(|key| !seen.contains_key(key)),
                "seed {seed}: a rolled-back row was returned"
            );
            assert_eq!(
                seen.into_keys().collect::<HashSet<_>>(),
                expected.committed,
                "seed {seed}"
            );
        }
    }
}
