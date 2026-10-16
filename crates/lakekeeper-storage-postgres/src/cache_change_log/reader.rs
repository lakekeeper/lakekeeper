use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use lakekeeper::service::cache_invalidation::{
    BARRIER_DOMAIN, CacheChangeLog, ChangeLogError, ChangeLogSettings, ChangeMarker, LoggedChange,
    PollOutcome, ResetReason,
};
use sqlx::{PgConnection, PgPool};

use super::{begin_with_timeout, snapshot::PgSnapshot, system_identifier};

/// Rows per page of a poll. A row holds at most 1000 keys, so a page holds at most
/// 100 000.
const PAGE_ROWS: i64 = 100;

/// Markers per confirmation statement.
const CONFIRM_CHUNK: usize = 10_000;

/// Opens a new connection to the primary.
pub(crate) type Connector =
    Box<dyn Fn() -> BoxFuture<'static, sqlx::Result<PgConnection>> + Send + Sync>;

/// The server a cursor belongs to. Transaction ids are comparable only within one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ServerIdentity {
    /// `0` where `pg_control_system()` may not be called.
    system_identifier: i64,
    postmaster_start: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct Cursor {
    prev: PgSnapshot,
    /// The poll's snapshot, taken by its first page and kept until its last page; a poll
    /// that stops at `max_keys` keeps it for the next call.
    next: Option<PgSnapshot>,
    after_xid: u64,
    after_id: i64,
}

impl Cursor {
    fn at(snapshot: PgSnapshot) -> Self {
        let after_xid = snapshot.first_unseen();
        Self {
            prev: snapshot,
            next: None,
            after_xid,
            // Identity ids start at 1, so every row of `after_xid` is after `(after_xid, 0)`.
            after_id: 0,
        }
    }
}

/// The Postgres change log: a snapshot diff over `cache_change_log` on the primary.
///
/// Each poll returns the rows of transactions that committed between the previous poll's
/// snapshot and a new one: those running at the previous snapshot and those started after
/// it. Rows are read in `(xid, id)` order, in pages.
pub struct PostgresCacheChangeLog {
    connect: Connector,
    conn: Option<PgConnection>,
    read_pool: PgPool,
    reads_from_primary: bool,
    settings: ChangeLogSettings,
    page_rows: i64,
    confirm_chunk: usize,
    /// The server of the cursor, set by [`CacheChangeLog::restart`].
    identity: Option<ServerIdentity>,
    /// Set when a reconnect reached a different server than the cursor's.
    server_changed: bool,
    cursor: Option<Cursor>,
    /// Fails the guarded call after the next `n` ones, after connecting, to test error
    /// handling.
    #[cfg(test)]
    fail_statement: Option<usize>,
}

impl std::fmt::Debug for PostgresCacheChangeLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresCacheChangeLog")
            .field("connected", &self.conn.is_some())
            .field("reads_from_primary", &self.reads_from_primary)
            .field("identity", &self.identity)
            .field("cursor", &self.cursor)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, thiserror::Error)]
#[error("A change log statement took longer than {0:?}")]
struct StatementTimedOut(std::time::Duration);

#[derive(Debug, thiserror::Error)]
#[error("The change log was polled before it was restarted")]
struct NotRestarted;

fn query_error(e: impl std::error::Error + Send + Sync + 'static) -> ChangeLogError {
    ChangeLogError::Query(Box::new(e))
}

fn parse_xid(xid: &str) -> Result<u64, ChangeLogError> {
    xid.parse().map_err(query_error)
}

/// Whether the rows the cursor still needs can be read: `None` if they can, otherwise
/// why not. Rows with an xid below `pruned_below` may have been deleted. `now_xmax` is the
/// `xmax` of the current statement; a poll that continues keeps its older `next`, and
/// pruning may have passed that since.
fn continuity(
    prev: &PgSnapshot,
    next: &PgSnapshot,
    pruned_below: Option<u64>,
    now_xmax: u64,
) -> Option<ResetReason> {
    if next.xmax < prev.xmax {
        return Some(ResetReason::TransactionIdsWentBack);
    }
    match pruned_below {
        Some(pruned_below) if prev.xmin < pruned_below && pruned_below <= now_xmax => {
            Some(ResetReason::HorizonPassed)
        }
        _ => None,
    }
}

impl PostgresCacheChangeLog {
    /// The log of `state`'s database. Its own connection to the primary comes from the
    /// process configuration, like the pools; confirmations use `state`'s read pool.
    pub(crate) fn for_state(state: &crate::CatalogState, settings: ChangeLogSettings) -> Self {
        let connect: Connector = Box::new(|| Box::pin(crate::connect_owned_to_primary()));
        Self::new(
            connect,
            state.read_pool(),
            crate::reads_from_primary(),
            settings,
        )
    }

    fn new(
        connect: Connector,
        read_pool: PgPool,
        reads_from_primary: bool,
        settings: ChangeLogSettings,
    ) -> Self {
        Self {
            connect,
            conn: None,
            read_pool,
            reads_from_primary,
            settings,
            page_rows: PAGE_ROWS,
            confirm_chunk: CONFIRM_CHUNK,
            identity: None,
            server_changed: false,
            cursor: None,
            #[cfg(test)]
            fail_statement: None,
        }
    }

    /// A log on `pool`'s database, connecting with `pool`'s options; reads come from the
    /// primary.
    #[cfg(test)]
    pub(super) fn for_test_pool(pool: &PgPool, settings: ChangeLogSettings) -> Self {
        let options: sqlx::postgres::PgConnectOptions = (*pool.connect_options()).clone();
        let connect: Connector = Box::new(move || {
            let options = options.clone();
            Box::pin(
                async move { <PgConnection as sqlx::Connection>::connect_with(&options).await },
            )
        });
        Self::new(connect, pool.clone(), false, settings)
    }

    /// Run `f` on the connection, connecting first if needed, all within the client
    /// timeout. On any error the connection is dropped, so the next call reconnects.
    async fn guarded<T>(
        &mut self,
        f: impl for<'c> FnOnce(&'c mut PgConnection) -> BoxFuture<'c, Result<T, ChangeLogError>>,
    ) -> Result<T, ChangeLogError> {
        let timeout = self.settings.statement_timeout;
        let result = tokio::time::timeout(client_timeout(timeout), async {
            self.connection().await?;
            #[cfg(test)]
            if let Some(n) = self.fail_statement.as_mut() {
                if *n == 0 {
                    self.fail_statement = None;
                    return Err(query_error(StatementTimedOut(timeout)));
                }
                *n -= 1;
            }
            f(self.conn.as_mut().expect("connected above")).await
        })
        .await
        .unwrap_or_else(|_| Err(query_error(StatementTimedOut(timeout))));
        if result.is_err() {
            self.conn = None;
        }
        result
    }

    /// Drop the cursor: polls fail until [`CacheChangeLog::restart`] sets a new one.
    fn reset(&mut self, reason: ResetReason) -> PollOutcome {
        self.cursor = None;
        PollOutcome::Reset(reason)
    }

    /// The open connection, or a new one. A new connection to another server than the
    /// cursor's sets `server_changed`.
    async fn connection(&mut self) -> Result<&mut PgConnection, ChangeLogError> {
        if self.conn.is_none() {
            let mut conn = (self.connect)().await.map_err(query_error)?;
            let (identity, _) = read_identity(&mut conn, self.settings).await?;
            if self.identity.is_some_and(|known| known != identity) {
                self.server_changed = true;
            }
            self.conn = Some(conn);
        }
        Ok(self.conn.as_mut().expect("connected above"))
    }
}

/// The client-side limit for one guarded call: the server's statement timeout plus room
/// for connecting and the round trips around it.
fn client_timeout(statement_timeout: std::time::Duration) -> std::time::Duration {
    statement_timeout + statement_timeout / 2
}

/// The server's identity and current snapshot. Fails with [`ChangeLogError::Standby`] on a
/// standby.
async fn read_identity(
    conn: &mut PgConnection,
    settings: ChangeLogSettings,
) -> Result<(ServerIdentity, PgSnapshot), ChangeLogError> {
    read_identity_on(conn, settings).await.map_err(|e| match e {
        IdentityError::Standby => ChangeLogError::Standby,
        IdentityError::Sql(e) => query_error(e),
        IdentityError::Snapshot(e) => query_error(e),
    })
}

#[derive(Debug, thiserror::Error)]
enum IdentityError {
    #[error("Connected to a standby")]
    Standby,
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
    #[error(transparent)]
    Snapshot(#[from] super::snapshot::InvalidSnapshot),
}

async fn read_identity_on(
    conn: &mut PgConnection,
    settings: ChangeLogSettings,
) -> Result<(ServerIdentity, PgSnapshot), IdentityError> {
    let mut tx = begin_with_timeout(conn, settings.statement_timeout).await?;
    let row = sqlx::query!(
        r#"
        SELECT pg_is_in_recovery() AS "in_recovery!",
               pg_postmaster_start_time() AS "postmaster_start!",
               pg_current_snapshot()::text AS "snapshot!"
        "#
    )
    .fetch_one(&mut *tx)
    .await?;
    if row.in_recovery {
        return Err(IdentityError::Standby);
    }
    let system_identifier = system_identifier(&mut tx).await?;
    tx.commit().await?;
    let snapshot = row.snapshot.parse()?;
    Ok((
        ServerIdentity {
            system_identifier,
            postmaster_start: row.postmaster_start,
        },
        snapshot,
    ))
}

/// One page of a poll.
struct Page {
    in_recovery: bool,
    postmaster_start: DateTime<Utc>,
    next: String,
    /// `xmax` of the page statement's own snapshot.
    now_xmax: String,
    pruned_below: Option<String>,
    rows: Vec<PageRow>,
}

struct PageRow {
    id: i64,
    xid: String,
    domain: String,
    keys: Vec<String>,
    version: Option<i64>,
}

/// What one page of a poll reads.
#[derive(Debug, typed_builder::TypedBuilder)]
struct PageRequest {
    /// The previous poll's snapshot.
    prev: String,
    /// This poll's snapshot; `None` on its first page, which takes it.
    next: Option<String>,
    /// The page starts after this `(xid, id)`.
    after_xid: String,
    after_id: i64,
    limit: i64,
    /// Selects the horizon row of this cluster.
    system_identifier: i64,
}

async fn read_page(
    conn: &mut PgConnection,
    settings: ChangeLogSettings,
    request: PageRequest,
) -> sqlx::Result<Page> {
    let PageRequest {
        prev,
        next,
        after_xid,
        after_id,
        limit,
        system_identifier,
    } = request;
    let mut tx = begin_with_timeout(conn, settings.statement_timeout).await?;
    // Two branches, each on its own range of the (xid, id) key: transactions running at
    // the previous snapshot, and transactions started after it. Both keep only rows
    // visible in the poll's snapshot, so a transaction is returned once it committed.
    let rows = sqlx::query!(
        r#"
        WITH s AS (
            SELECT coalesce($1::text::pg_snapshot, pg_current_snapshot()) AS next
        ),
        page AS (
            (SELECT l.id, l.xid, l.domain, l.keys, l.version
             FROM cache_change_log l CROSS JOIN s
             WHERE l.xid = ANY(ARRAY(SELECT pg_snapshot_xip($2::text::pg_snapshot)))
               AND pg_visible_in_snapshot(l.xid, s.next)
               AND (l.xid, l.id) > ($3::text::xid8, $4::bigint)
             ORDER BY l.xid, l.id
             LIMIT $5)
            UNION ALL
            (SELECT l.id, l.xid, l.domain, l.keys, l.version
             FROM cache_change_log l CROSS JOIN s
             WHERE l.xid >= pg_snapshot_xmax($2::text::pg_snapshot)
               AND pg_visible_in_snapshot(l.xid, s.next)
               AND (l.xid, l.id) > ($3::text::xid8, $4::bigint)
             ORDER BY l.xid, l.id
             LIMIT $5)
            ORDER BY xid, id
            LIMIT $5
        )
        SELECT pg_is_in_recovery() AS "in_recovery!",
               pg_postmaster_start_time() AS "postmaster_start!",
               s.next::text AS "next!",
               pg_snapshot_xmax(pg_current_snapshot())::text AS "now_xmax!",
               (SELECT h.pruned_below::text FROM cache_change_log_horizon h
                WHERE h.system_identifier = $6) AS "pruned_below",
               p.id AS "id?",
               p.xid::text AS "xid?",
               p.domain AS "domain?",
               p.keys AS "keys?",
               p.version AS "version?"
        FROM s LEFT JOIN page p ON true
        ORDER BY p.xid, p.id
        "#,
        next,
        prev,
        after_xid,
        after_id,
        limit,
        system_identifier,
    )
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    let first = rows
        .first()
        .ok_or_else(|| sqlx::Error::Protocol("a poll page returned no header".to_string()))?;
    let header = Page {
        in_recovery: first.in_recovery,
        postmaster_start: first.postmaster_start,
        next: first.next.clone(),
        now_xmax: first.now_xmax.clone(),
        pruned_below: first.pruned_below.clone(),
        rows: Vec::new(),
    };
    let rows = rows
        .into_iter()
        .filter_map(|r| {
            Some(PageRow {
                id: r.id?,
                xid: r.xid?,
                domain: r.domain?,
                keys: r.keys?,
                version: r.version,
            })
        })
        .collect();
    Ok(Page { rows, ..header })
}

/// The xids among `xids` that have no row in the log as `conn` sees it.
async fn unconfirmed_xids(
    conn: &mut PgConnection,
    settings: ChangeLogSettings,
    xids: &[String],
) -> sqlx::Result<Vec<String>> {
    let mut tx = begin_with_timeout(conn, settings.statement_timeout).await?;
    let missing = sqlx::query_scalar!(
        r#"
        SELECT x::text AS "xid!"
        FROM unnest($1::text[]::xid8[]) x
        WHERE NOT EXISTS (SELECT 1 FROM cache_change_log l WHERE l.xid = x)
        "#,
        xids,
    )
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(missing)
}

#[async_trait::async_trait]
impl CacheChangeLog for PostgresCacheChangeLog {
    async fn restart(&mut self) -> Result<(), ChangeLogError> {
        let settings = self.settings;
        let (identity, snapshot) = self
            .guarded(|conn| Box::pin(read_identity(conn, settings)))
            .await?;
        self.identity = Some(identity);
        self.server_changed = false;
        self.cursor = Some(Cursor::at(snapshot));
        Ok(())
    }

    async fn poll(&mut self, max_keys: usize) -> Result<PollOutcome, ChangeLogError> {
        let (Some(identity), Some(cursor)) = (self.identity, &self.cursor) else {
            return Err(query_error(NotRestarted));
        };
        // Work on a copy: an error on a later page must leave `self.cursor` before the rows
        // this call already read, so the next call returns them again.
        let mut cursor = cursor.clone();
        let mut changes = Vec::new();
        let mut keys = 0usize;
        loop {
            let request = PageRequest::builder()
                .prev(cursor.prev.to_string())
                .next(cursor.next.as_ref().map(ToString::to_string))
                .after_xid(cursor.after_xid.to_string())
                .after_id(cursor.after_id)
                .limit(self.page_rows)
                .system_identifier(identity.system_identifier)
                .build();
            let settings = self.settings;
            let page = self
                .guarded(|conn| {
                    Box::pin(async move {
                        read_page(conn, settings, request)
                            .await
                            .map_err(query_error)
                    })
                })
                .await;
            let page = match page {
                Ok(page) => page,
                // A reconnect reached another server; its errors don't matter any more.
                Err(_) if self.server_changed => return Ok(self.reset(ResetReason::ServerChanged)),
                Err(e) => return Err(e),
            };
            if page.in_recovery {
                self.conn = None;
                return Err(ChangeLogError::Standby);
            }
            if self.server_changed || page.postmaster_start != identity.postmaster_start {
                return Ok(self.reset(ResetReason::ServerChanged));
            }
            let next: PgSnapshot = page.next.parse().map_err(query_error)?;
            let pruned_below = page.pruned_below.as_deref().map(parse_xid).transpose()?;
            let now_xmax = parse_xid(&page.now_xmax)?;
            if let Some(reason) = continuity(&cursor.prev, &next, pruned_below, now_xmax) {
                return Ok(self.reset(reason));
            }
            let page_len = page.rows.len();
            for row in page.rows {
                let xid = parse_xid(&row.xid)?;
                cursor.after_xid = xid;
                cursor.after_id = row.id;
                keys += row.keys.len().max(1);
                changes.push(LoggedChange {
                    marker: ChangeMarker::from_raw(xid),
                    domain: row.domain,
                    keys: row.keys,
                    version: row.version,
                });
            }
            let complete = i64::try_from(page_len).unwrap_or(i64::MAX) < self.page_rows;
            if complete {
                cursor = Cursor::at(next);
            } else {
                cursor.next = Some(next);
            }
            if complete || keys >= max_keys {
                self.cursor = Some(cursor);
                return Ok(PollOutcome::Changes { changes, complete });
            }
        }
    }

    async fn insert_barrier(&mut self) -> Result<ChangeMarker, ChangeLogError> {
        let settings = self.settings;
        let xid = self
            .guarded(|conn| {
                Box::pin(async move {
                    let mut tx = begin_with_timeout(conn, settings.statement_timeout)
                        .await
                        .map_err(query_error)?;
                    let xid = sqlx::query_scalar!(
                        r#"INSERT INTO cache_change_log (domain, keys) VALUES ($1, '{}')
                           RETURNING xid::text AS "xid!""#,
                        BARRIER_DOMAIN,
                    )
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(query_error)?;
                    tx.commit().await.map_err(query_error)?;
                    Ok(xid)
                })
            })
            .await?;
        Ok(ChangeMarker::from_raw(parse_xid(&xid)?))
    }

    async fn unconfirmed(
        &self,
        markers: &[ChangeMarker],
    ) -> Result<Vec<ChangeMarker>, ChangeLogError> {
        let mut unconfirmed = Vec::new();
        let timeout = self.settings.statement_timeout;
        for chunk in markers.chunks(self.confirm_chunk) {
            let xids: Vec<String> = chunk.iter().map(|m| m.raw().to_string()).collect();
            let mut conn = self.read_pool.acquire().await.map_err(query_error)?;
            let missing = tokio::time::timeout(
                client_timeout(timeout),
                unconfirmed_xids(&mut conn, self.settings, &xids),
            )
            .await;
            let missing = match missing {
                Ok(missing) => missing.map_err(query_error)?,
                Err(_) => {
                    // The connection is mid-statement: never hand it back to the pool.
                    conn.close_on_drop();
                    return Err(query_error(StatementTimedOut(timeout)));
                }
            };
            for xid in missing {
                unconfirmed.push(ChangeMarker::from_raw(parse_xid(&xid)?));
            }
        }
        Ok(unconfirmed)
    }

    fn reads_from_primary(&self) -> bool {
        self.reads_from_primary
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        time::Duration,
    };

    use sqlx::{Connection, Executor};

    use super::*;
    use crate::cache_change_log::test_support::{record, record_in};

    const SETTINGS: ChangeLogSettings = ChangeLogSettings {
        statement_timeout: Duration::from_secs(10),
    };

    fn log(pool: &PgPool, page_rows: i64) -> PostgresCacheChangeLog {
        let mut log = PostgresCacheChangeLog::for_test_pool(pool, SETTINGS);
        log.page_rows = page_rows;
        log
    }

    fn change(xid: u64, domain: &str, key: &str) -> LoggedChange {
        LoggedChange {
            marker: ChangeMarker::from_raw(xid),
            domain: domain.to_string(),
            keys: vec![key.to_string()],
            version: None,
        }
    }

    fn complete(changes: Vec<LoggedChange>) -> PollOutcome {
        PollOutcome::Changes {
            changes,
            complete: true,
        }
    }

    #[sqlx::test]
    async fn returns_only_changes_committed_after_the_restart(pool: PgPool) {
        record(&pool, "secret", "before").await;
        let mut log = log(&pool, 100);
        log.restart().await.unwrap();
        assert_eq!(log.poll(1000).await.unwrap(), complete(vec![]));

        let a = record(&pool, "secret", "a").await;
        let b = record(&pool, "role", "b").await;

        assert_eq!(
            log.poll(1000).await.unwrap(),
            complete(vec![change(a, "secret", "a"), change(b, "role", "b")])
        );
        assert_eq!(log.poll(1000).await.unwrap(), complete(vec![]));
    }

    #[sqlx::test]
    async fn a_transaction_that_commits_late_is_returned_by_the_next_poll(pool: PgPool) {
        let mut log = log(&pool, 100);
        log.restart().await.unwrap();
        let mut late = pool.begin().await.unwrap();
        let late_xid = record_in(&mut late, "secret", "late").await;
        let early = record(&pool, "secret", "early").await;

        assert_eq!(
            log.poll(1000).await.unwrap(),
            complete(vec![change(early, "secret", "early")])
        );
        late.commit().await.unwrap();
        assert_eq!(
            log.poll(1000).await.unwrap(),
            complete(vec![change(late_xid, "secret", "late")])
        );
    }

    #[sqlx::test]
    async fn a_transaction_running_at_the_restart_is_returned_when_it_commits(pool: PgPool) {
        let mut running = pool.begin().await.unwrap();
        let xid = record_in(&mut running, "secret", "k").await;
        let mut log = log(&pool, 100);
        log.restart().await.unwrap();
        assert_eq!(log.poll(1000).await.unwrap(), complete(vec![]));

        running.commit().await.unwrap();
        assert_eq!(
            log.poll(1000).await.unwrap(),
            complete(vec![change(xid, "secret", "k")])
        );
    }

    #[sqlx::test]
    async fn rolled_back_rows_never_appear(pool: PgPool) {
        let mut log = log(&pool, 100);
        log.restart().await.unwrap();
        let mut tx = pool.begin().await.unwrap();
        record_in(&mut tx, "secret", "gone").await;
        tx.rollback().await.unwrap();
        let mut tx = pool.begin().await.unwrap();
        let mut savepoint = tx.begin().await.unwrap();
        record_in(&mut savepoint, "secret", "savepoint").await;
        savepoint.rollback().await.unwrap();
        let kept = record_in(&mut tx, "secret", "kept").await;
        tx.commit().await.unwrap();

        assert_eq!(
            log.poll(1000).await.unwrap(),
            complete(vec![change(kept, "secret", "kept")])
        );
    }

    #[sqlx::test]
    async fn rows_of_one_transaction_are_paged_without_loss(pool: PgPool) {
        let mut log = log(&pool, 2);
        log.restart().await.unwrap();
        let mut tx = pool.begin().await.unwrap();
        let mut xid = 0;
        for key in ["k1", "k2", "k3", "k4", "k5"] {
            xid = record_in(&mut tx, "secret", key).await;
        }
        tx.commit().await.unwrap();

        assert_eq!(
            log.poll(1000).await.unwrap(),
            complete(
                ["k1", "k2", "k3", "k4", "k5"]
                    .iter()
                    .map(|key| change(xid, "secret", key))
                    .collect()
            )
        );
    }

    #[sqlx::test]
    async fn a_poll_stops_after_max_keys_and_the_next_continues(pool: PgPool) {
        let mut log = log(&pool, 2);
        log.restart().await.unwrap();
        let mut expected = Vec::new();
        for key in ["k1", "k2", "k3", "k4", "k5"] {
            expected.push(change(record(&pool, "secret", key).await, "secret", key));
        }
        // Committed after the poll's snapshot: returned by a later poll.
        let first = log.poll(2).await.unwrap();
        let later = record(&pool, "secret", "later").await;

        assert_eq!(
            first,
            PollOutcome::Changes {
                changes: expected[..2].to_vec(),
                complete: false
            }
        );
        assert_eq!(
            log.poll(2).await.unwrap(),
            PollOutcome::Changes {
                changes: expected[2..4].to_vec(),
                complete: false
            }
        );
        assert_eq!(log.poll(2).await.unwrap(), complete(expected[4..].to_vec()));
        assert_eq!(
            log.poll(2).await.unwrap(),
            complete(vec![change(later, "secret", "later")])
        );
    }

    #[sqlx::test]
    async fn a_failed_page_keeps_the_rows_of_the_earlier_pages(pool: PgPool) {
        let mut log = log(&pool, 1);
        log.restart().await.unwrap();
        let mut expected = Vec::new();
        for key in ["k1", "k2", "k3"] {
            expected.push(change(record(&pool, "secret", key).await, "secret", key));
        }
        log.fail_statement = Some(2);

        assert!(matches!(
            log.poll(1000).await,
            Err(ChangeLogError::Query(_))
        ));
        assert_eq!(log.poll(1000).await.unwrap(), complete(expected));
    }

    #[sqlx::test]
    async fn running_and_new_transactions_are_paged_together(pool: PgPool) {
        let mut running = pool.begin().await.unwrap();
        let mut running_xid = 0;
        for key in ["r1", "r2", "r3"] {
            running_xid = record_in(&mut running, "secret", key).await;
        }
        let mut log = log(&pool, 2);
        log.restart().await.unwrap();
        running.commit().await.unwrap();
        let mut new_tx = pool.begin().await.unwrap();
        let mut new_xid = 0;
        for key in ["n1", "n2", "n3"] {
            new_xid = record_in(&mut new_tx, "secret", key).await;
        }
        new_tx.commit().await.unwrap();

        let mut expected: Vec<_> = ["r1", "r2", "r3"]
            .iter()
            .map(|key| change(running_xid, "secret", key))
            .collect();
        expected.extend(
            ["n1", "n2", "n3"]
                .iter()
                .map(|key| change(new_xid, "secret", key)),
        );
        assert_eq!(log.poll(1000).await.unwrap(), complete(expected));
    }

    #[sqlx::test]
    async fn rows_of_more_subtransactions_than_a_snapshot_lists_are_returned(pool: PgPool) {
        // Postgres lists at most 64 subtransactions per backend; beyond that, snapshots
        // are marked overflowed. Rows carry the top-level xid either way.
        let mut log = log(&pool, 7);
        log.restart().await.unwrap();
        let mut tx = pool.begin().await.unwrap();
        let mut xid = 0;
        let mut kept = Vec::new();
        for i in 0..120 {
            let key = format!("k{i:03}");
            let mut savepoint = tx.begin().await.unwrap();
            xid = record_in(&mut savepoint, "secret", &key).await;
            if i % 3 == 0 {
                savepoint.rollback().await.unwrap();
            } else {
                savepoint.commit().await.unwrap();
                kept.push(key);
            }
        }
        let other = record(&pool, "role", "other").await;

        assert_eq!(
            log.poll(1000).await.unwrap(),
            complete(vec![change(other, "role", "other")])
        );
        tx.commit().await.unwrap();
        assert_eq!(
            log.poll(1000).await.unwrap(),
            complete(kept.iter().map(|key| change(xid, "secret", key)).collect())
        );
        assert_eq!(log.poll(1000).await.unwrap(), complete(vec![]));
    }

    #[sqlx::test]
    async fn a_transaction_open_across_polls_is_returned_once_after_it_commits(pool: PgPool) {
        let mut log = log(&pool, 1);
        log.restart().await.unwrap();
        let mut long = pool.begin().await.unwrap();
        let long_xid = record_in(&mut long, "secret", "l1").await;
        let a = record(&pool, "role", "a").await;
        assert_eq!(
            log.poll(1000).await.unwrap(),
            complete(vec![change(a, "role", "a")])
        );

        // Writes on both sides of later polls.
        record_in(&mut long, "secret", "l2").await;
        let b = record(&pool, "role", "b").await;
        let c = record(&pool, "role", "c").await;
        assert_eq!(
            log.poll(1).await.unwrap(),
            PollOutcome::Changes {
                changes: vec![change(b, "role", "b")],
                complete: false
            }
        );
        // Commits while that poll is part-way through its pages: the poll ends at the
        // snapshot it started with, so the next poll returns the transaction.
        record_in(&mut long, "secret", "l3").await;
        long.commit().await.unwrap();
        let d = record(&pool, "role", "d").await;
        assert_eq!(
            log.poll(1000).await.unwrap(),
            complete(vec![change(c, "role", "c")])
        );
        assert_eq!(
            log.poll(1000).await.unwrap(),
            complete(vec![
                change(long_xid, "secret", "l1"),
                change(long_xid, "secret", "l2"),
                change(long_xid, "secret", "l3"),
                change(d, "role", "d"),
            ])
        );
        assert_eq!(log.poll(1000).await.unwrap(), complete(vec![]));
    }

    #[sqlx::test]
    async fn a_page_that_ends_exactly_full_completes_with_the_next_page(pool: PgPool) {
        let mut log = log(&pool, 2);
        log.restart().await.unwrap();
        let mut expected = Vec::new();
        for key in ["k1", "k2", "k3", "k4"] {
            expected.push(change(record(&pool, "secret", key).await, "secret", key));
        }

        assert_eq!(log.poll(1000).await.unwrap(), complete(expected));
        assert_eq!(log.poll(1000).await.unwrap(), complete(vec![]));
    }

    #[sqlx::test]
    async fn a_reconnect_to_the_same_server_keeps_the_cursor(pool: PgPool) {
        let mut log = log(&pool, 100);
        log.restart().await.unwrap();
        log.conn = None;
        let xid = record(&pool, "secret", "k").await;

        assert_eq!(
            log.poll(1000).await.unwrap(),
            complete(vec![change(xid, "secret", "k")])
        );
    }

    #[sqlx::test]
    async fn a_reconnect_to_another_server_resets(pool: PgPool) {
        let mut log = log(&pool, 100);
        log.restart().await.unwrap();
        let known = log.identity.unwrap();
        // The system id is compared only on a reconnect; the start time on every page.
        log.identity = Some(ServerIdentity {
            system_identifier: known.system_identifier + 1,
            ..known
        });
        log.conn = None;

        assert_eq!(
            log.poll(1000).await.unwrap(),
            PollOutcome::Reset(ResetReason::ServerChanged)
        );
        log.restart().await.unwrap();
        assert_eq!(log.poll(1000).await.unwrap(), complete(vec![]));
    }

    #[sqlx::test]
    async fn a_server_restart_seen_on_a_page_resets(pool: PgPool) {
        let mut log = log(&pool, 100);
        log.restart().await.unwrap();
        let known = log.identity.unwrap();
        // The connection stays open: only the page statement's start time differs.
        log.identity = Some(ServerIdentity {
            postmaster_start: known.postmaster_start - chrono::Duration::seconds(1),
            ..known
        });

        assert_eq!(
            log.poll(1000).await.unwrap(),
            PollOutcome::Reset(ResetReason::ServerChanged)
        );
    }

    #[sqlx::test]
    async fn after_a_reset_polls_fail_until_restarted(pool: PgPool) {
        let mut log = log(&pool, 100);
        log.restart().await.unwrap();
        let known = log.identity.unwrap();
        log.identity = Some(ServerIdentity {
            system_identifier: known.system_identifier + 1,
            ..known
        });
        log.conn = None;
        assert_eq!(
            log.poll(1000).await.unwrap(),
            PollOutcome::Reset(ResetReason::ServerChanged)
        );

        assert!(matches!(
            log.poll(1000).await,
            Err(ChangeLogError::Query(_))
        ));
        log.restart().await.unwrap();
        assert_eq!(log.poll(1000).await.unwrap(), complete(vec![]));
    }

    #[sqlx::test]
    async fn a_failing_page_on_another_server_still_resets(pool: PgPool) {
        let mut log = log(&pool, 100);
        log.restart().await.unwrap();
        let known = log.identity.unwrap();
        log.identity = Some(ServerIdentity {
            system_identifier: known.system_identifier + 1,
            ..known
        });
        log.conn = None;
        // The reconnect succeeds, then the page statement fails.
        log.fail_statement = Some(0);

        assert_eq!(
            log.poll(1000).await.unwrap(),
            PollOutcome::Reset(ResetReason::ServerChanged)
        );
    }

    #[sqlx::test]
    async fn a_horizon_past_the_cursor_resets_and_a_foreign_one_does_not(pool: PgPool) {
        let mut log = log(&pool, 100);
        log.restart().await.unwrap();
        let system_identifier = log.identity.unwrap().system_identifier;
        let xid = record(&pool, "secret", "k").await;
        let set_horizon = |system_identifier: i64| {
            let pool = pool.clone();
            async move {
                pool.execute("DELETE FROM cache_change_log_horizon")
                    .await
                    .unwrap();
                sqlx::query(
                    "INSERT INTO cache_change_log_horizon (system_identifier, pruned_below)
                     VALUES ($1, $2::text::xid8)",
                )
                .bind(system_identifier)
                .bind((xid + 1).to_string())
                .execute(&pool)
                .await
                .unwrap();
            }
        };

        set_horizon(system_identifier + 1).await;
        assert_eq!(
            log.poll(1000).await.unwrap(),
            complete(vec![change(xid, "secret", "k")])
        );

        let next = record(&pool, "secret", "n").await;
        let set_horizon = |pruned_below: u64| {
            let pool = pool.clone();
            async move {
                sqlx::query("UPDATE cache_change_log_horizon SET system_identifier = $1, pruned_below = $2::text::xid8")
                    .bind(system_identifier)
                    .bind(pruned_below.to_string())
                    .execute(&pool)
                    .await
                    .unwrap();
            }
        };
        set_horizon(next + 1).await;
        assert_eq!(
            log.poll(1000).await.unwrap(),
            PollOutcome::Reset(ResetReason::HorizonPassed)
        );
    }

    #[sqlx::test]
    async fn a_barrier_is_returned_like_a_change(pool: PgPool) {
        let mut log = log(&pool, 100);
        log.restart().await.unwrap();
        let barrier = log.insert_barrier().await.unwrap();

        assert_eq!(
            log.poll(1000).await.unwrap(),
            complete(vec![LoggedChange {
                marker: barrier,
                domain: BARRIER_DOMAIN.to_string(),
                keys: vec![],
                version: None,
            }])
        );
    }

    #[sqlx::test]
    async fn unconfirmed_lists_the_markers_the_read_source_does_not_show(pool: PgPool) {
        let mut log = log(&pool, 100);
        log.confirm_chunk = 2;
        let committed = record(&pool, "secret", "a").await;
        let mut open = pool.begin().await.unwrap();
        let running = record_in(&mut open, "secret", "b").await;
        let unknown = committed + 1_000_000;
        let markers = [committed, running, unknown].map(ChangeMarker::from_raw);

        assert_eq!(
            log.unconfirmed(&markers).await.unwrap(),
            vec![
                ChangeMarker::from_raw(running),
                ChangeMarker::from_raw(unknown)
            ]
        );
        open.commit().await.unwrap();
        assert_eq!(
            log.unconfirmed(&markers).await.unwrap(),
            vec![ChangeMarker::from_raw(unknown)]
        );
    }

    /// What one writer did: the rows of committed transactions, by key, and the keys of
    /// rows that were rolled back.
    #[derive(Default)]
    struct Written {
        committed: HashMap<String, u64>,
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
            let mut xid = 0;
            for r in 0..rng.usize(1..=3) {
                let key = format!("s{seed}-w{writer}-t{t}-r{r}");
                if rng.u8(0..5) == 0 {
                    let mut savepoint = tx.begin().await.unwrap();
                    record_in(&mut savepoint, "secret", &key).await;
                    savepoint.rollback().await.unwrap();
                    written.rolled_back.insert(key);
                } else {
                    xid = record_in(&mut tx, "secret", &key).await;
                    kept.push(key);
                }
                tokio::time::sleep(Duration::from_millis(rng.u64(0..4))).await;
            }
            if kept.is_empty() || rng.u8(0..4) == 0 {
                tx.rollback().await.unwrap();
                written.rolled_back.extend(kept);
            } else {
                tx.commit().await.unwrap();
                written
                    .committed
                    .extend(kept.into_iter().map(|key| (key, xid)));
            }
        }
        written
    }

    /// Poll while writers run, with small pages, small key limits, failed statements and
    /// dropped connections. Every committed row is returned with its xid, and no row that
    /// was rolled back.
    #[sqlx::test]
    async fn concurrent_writers_lose_no_committed_row(pool: PgPool) {
        const SEEDS: u64 = 12;
        const WRITERS: u64 = 4;
        const TRANSACTIONS: usize = 15;
        for seed in 0..SEEDS {
            let mut rng = fastrand::Rng::with_seed(seed);
            let mut log = log(&pool, 2);
            log.restart().await.unwrap();
            let writers: Vec<_> = (0..WRITERS)
                .map(|w| tokio::spawn(write_randomly(pool.clone(), seed, w, TRANSACTIONS)))
                .collect();

            let mut seen: HashMap<String, u64> = HashMap::new();
            let mut record_changes = |changes: Vec<LoggedChange>| {
                for change in changes {
                    for key in change.keys {
                        seen.insert(key, change.marker.raw());
                    }
                }
            };
            while !writers.iter().all(tokio::task::JoinHandle::is_finished) {
                if rng.u8(0..6) == 0 {
                    log.fail_statement = Some(rng.usize(0..3));
                }
                if rng.u8(0..8) == 0 {
                    log.conn = None;
                }
                log.page_rows = rng.i64(1..=4);
                match log.poll(rng.usize(1..=6)).await {
                    Ok(PollOutcome::Changes { changes, .. }) => record_changes(changes),
                    Ok(PollOutcome::Reset(reason)) => panic!("seed {seed}: reset {reason:?}"),
                    Err(_) => {}
                }
                tokio::time::sleep(Duration::from_millis(rng.u64(0..3))).await;
            }
            let mut expected = Written::default();
            for writer in writers {
                let written = writer.await.unwrap();
                expected.committed.extend(written.committed);
                expected.rolled_back.extend(written.rolled_back);
            }
            log.fail_statement = None;
            // The first complete poll may have taken its snapshot while writers still ran;
            // the second starts after all of them finished.
            for _ in 0..2 {
                loop {
                    match log.poll(3).await.unwrap() {
                        PollOutcome::Changes { changes, complete } => {
                            record_changes(changes);
                            if complete {
                                break;
                            }
                        }
                        PollOutcome::Reset(reason) => panic!("seed {seed}: reset {reason:?}"),
                    }
                }
            }

            assert!(
                expected
                    .rolled_back
                    .iter()
                    .all(|key| !seen.contains_key(key)),
                "seed {seed}: a rolled-back row was returned"
            );
            assert_eq!(seen, expected.committed, "seed {seed}");
        }
    }

    #[test]
    fn transaction_ids_going_back_reset() {
        let prev: PgSnapshot = "100:105:".parse().unwrap();
        let next: PgSnapshot = "90:95:".parse().unwrap();
        assert_eq!(
            continuity(&prev, &next, None, 95),
            Some(ResetReason::TransactionIdsWentBack)
        );
    }

    #[test]
    fn a_horizon_resets_only_between_the_cursor_and_the_new_snapshot() {
        let prev: PgSnapshot = "100:105:100".parse().unwrap();
        let next: PgSnapshot = "104:110:".parse().unwrap();
        for (pruned_below, expected) in [
            (None, None),
            (Some(99), None),
            (Some(100), None),
            (Some(101), Some(ResetReason::HorizonPassed)),
            (Some(110), Some(ResetReason::HorizonPassed)),
            (Some(111), None),
        ] {
            assert_eq!(
                continuity(&prev, &next, pruned_below, 110),
                expected,
                "{pruned_below:?}"
            );
        }
    }

    #[test]
    fn a_horizon_beyond_a_continued_polls_snapshot_resets() {
        // A poll that continues keeps its snapshot; pruning may since have passed it.
        let prev: PgSnapshot = "100:105:100".parse().unwrap();
        let next: PgSnapshot = "104:110:".parse().unwrap();
        assert_eq!(
            continuity(&prev, &next, Some(150), 200),
            Some(ResetReason::HorizonPassed)
        );
        assert_eq!(continuity(&prev, &next, Some(201), 200), None);
    }
}
