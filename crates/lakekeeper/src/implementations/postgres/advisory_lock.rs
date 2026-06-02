//! Postgres session-level advisory lock as a generic mutex primitive.
//!
//! Used by maintenance flows (currently: OpenFGA reconcile-with-deletion)
//! to prevent two long-running mutation passes from racing.
//!
//! The lock is session-scoped (`pg_try_advisory_lock`) and stays held
//! for the lifetime of the postgres backend session. To release on
//! `Drop`, the held [`sqlx::pool::PoolConnection`] is marked
//! `close_on_drop` at acquire time — when the guard goes out of scope,
//! sqlx spawns a task to terminate the connection, the server detects
//! the closed socket, and `PostgreSQL` releases every advisory lock the
//! session held. (Returning the connection to the pool would leave the
//! lock held and let a re-entrant acquire on the same recycled
//! connection silently succeed.)
//!
//! Release is async-deferred — bounded by the time it takes the close
//! task to run and the server to notice — but for a maintenance flow
//! that already runs at human cadence, the window is irrelevant.
//!
//! ## Pool footprint
//!
//! The guard holds one pool connection for its entire lifetime. The
//! protected operation typically needs at least one additional
//! connection (catalog reads, transactions). Ensure the pool's
//! `max_connections` is at least 2 — small or single-connection pools
//! will self-deadlock when the operation tries to acquire its second
//! connection while the lock conn is held.
//!
//! The lock key namespace is the caller's responsibility — pick a stable
//! arbitrary `i64` and document its scope at the call site.

use crate::{
    implementations::postgres::CatalogState,
    service::maintenance::{MaintenanceLockGuard, sealed::Sealed as MaintenanceLockGuardSealed},
};

/// Holds an exclusive Postgres session-level advisory lock for the lifetime
/// of the value. Release happens on `Drop`.
///
/// Hold across the operation you want to serialize, and let the guard drop
/// when you're done. Do not move the guard into a `'static` task unless you
/// intend the lock to outlive the awaiting flow.
#[must_use = "lock is released when guard is dropped"]
#[derive(Debug)]
pub struct PostgresAdvisoryLock {
    _conn: sqlx::pool::PoolConnection<sqlx::Postgres>,
}

impl MaintenanceLockGuardSealed for PostgresAdvisoryLock {}
impl MaintenanceLockGuard for PostgresAdvisoryLock {}

impl PostgresAdvisoryLock {
    /// Try to acquire the advisory lock identified by `key`.
    ///
    /// Returns `Ok(Some(guard))` when the lock was acquired, `Ok(None)`
    /// when another session already holds it, and `Err` on database
    /// failure.
    ///
    /// # Errors
    /// Database connection acquisition or query failure.
    pub async fn try_acquire(state: &CatalogState, key: i64) -> anyhow::Result<Option<Self>> {
        let mut conn = state
            .write_pool()
            .acquire()
            .await
            .map_err(|e| anyhow::anyhow!("advisory lock: failed to acquire pool conn: {e}"))?;
        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(key)
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| anyhow::anyhow!("advisory lock: pg_try_advisory_lock failed: {e}"))?;
        if acquired {
            // Terminate the postgres session when the guard drops, so the
            // session-level advisory lock is released. Otherwise the
            // connection would be returned to the pool with the lock
            // still held (and re-entrant on subsequent acquires).
            conn.close_on_drop();
            Ok(Some(Self { _conn: conn }))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use sqlx::PgPool;

    use super::*;

    const TEST_KEY: i64 = 0x5f8e_2d63_a4b1_dead;

    #[sqlx::test]
    async fn try_acquire_succeeds_when_free(pool: PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool);
        let lock = PostgresAdvisoryLock::try_acquire(&state, TEST_KEY)
            .await
            .expect("query ok");
        assert!(lock.is_some(), "lock must be acquired when key is free");
    }

    #[sqlx::test]
    async fn try_acquire_returns_none_when_held(pool: PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool);

        // Hold the lock manually on a separate session.
        let mut holder = state.write_pool().acquire().await.unwrap();
        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(TEST_KEY)
            .fetch_one(&mut *holder)
            .await
            .unwrap();
        assert!(acquired, "test setup: holder must acquire");

        let attempt = PostgresAdvisoryLock::try_acquire(&state, TEST_KEY)
            .await
            .expect("query ok");
        assert!(
            attempt.is_none(),
            "second acquire must fail while another session holds the lock"
        );

        // Releasing the holder's session frees the lock.
        drop(holder);
    }

    #[sqlx::test]
    async fn lock_released_on_drop(pool: PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool);

        {
            let lock = PostgresAdvisoryLock::try_acquire(&state, TEST_KEY)
                .await
                .expect("query ok");
            assert!(lock.is_some());
        }

        // Release is async-deferred: the dropped PoolConnection spawns a
        // close task, and `PostgreSQL` releases the lock once it sees the
        // socket close. Poll up to a few seconds before failing.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let attempt = PostgresAdvisoryLock::try_acquire(&state, TEST_KEY)
                .await
                .expect("query ok");
            if attempt.is_some() {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "lock not released within 5s after guard drop"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }
}
