//! Reading the change log: the contract between a storage backend and the poller.

use std::{num::NonZeroU32, time::Duration};

/// The domain of barrier rows. A barrier names no cache entry; see
/// [`CacheChangeLog::insert_barrier`].
pub const BARRIER_DOMAIN: &str = "barrier";

/// A backend's id for the transaction that wrote a change (in Postgres, the `xid8`).
///
/// Markers are compared only for equality: their numeric order is not commit order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChangeMarker(u64);

impl ChangeMarker {
    #[must_use]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

/// One change-log row: some keys of one domain that a committed transaction changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoggedChange {
    pub marker: ChangeMarker,
    /// The domain name as stored. Domains of extensions are not [`super::CacheDomain`]s.
    pub domain: String,
    /// Encoded keys; empty for the whole domain.
    pub keys: Vec<String>,
    /// A version the writer recorded, for domains whose readers compare versions.
    pub version: Option<i64>,
}

/// Why the log cannot tell what changed since the previous poll.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum_macros::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
#[non_exhaustive]
pub enum ResetReason {
    /// The poll reached a different database server, or the server restarted.
    ServerChanged,
    /// Transaction ids went backwards: a restore or a failover to an older state.
    TransactionIdsWentBack,
    /// Rows the poll still needed were pruned.
    HorizonPassed,
}

/// The result of one [`CacheChangeLog::poll`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PollOutcome {
    /// Changes committed since the previous poll, in log order. `complete` is false when
    /// more changes of the same poll remain; the next call returns them.
    Changes {
        changes: Vec<LoggedChange>,
        complete: bool,
    },
    /// The log lost continuity. The caller must treat every cached entry as stale and call
    /// [`CacheChangeLog::restart`].
    Reset(ResetReason),
}

/// An error reading or writing the change log. The call can be retried; the cursor is
/// unchanged.
#[derive(Debug, thiserror::Error)]
pub enum ChangeLogError {
    /// The change log was read from a standby; it must be read from the primary.
    #[error("The change log was read from a standby, not from the primary")]
    Standby,
    /// A statement or the connection failed.
    #[error("A change log statement failed")]
    Query(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
}

/// Settings of a [`CacheChangeLog`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChangeLogSettings {
    /// The longest one statement of the log may run.
    pub statement_timeout: Duration,
}

/// Settings of one pruning run of the change log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PruneSettings {
    /// Rows are kept at least this long after their transaction finished, and longer while
    /// an older transaction of the database server is still open.
    pub retention: Duration,
    /// The most rows one delete transaction removes.
    pub batch_size: NonZeroU32,
    /// The longest one statement of the run may run.
    pub statement_timeout: Duration,
}

/// The result of one pruning run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PruneOutcome {
    /// Another instance is pruning.
    Skipped,
    /// The run deleted `deleted_rows` rows. It stops early when another instance takes over.
    Pruned { deleted_rows: u64 },
}

/// The change log as the poller sees it.
///
/// A log reads the primary through a connection of its own and reconnects after an error.
/// It keeps a cursor: each [`Self::poll`] returns the changes committed since the previous
/// one. A reconnect to the same server keeps the cursor; a different server or a restarted
/// one makes the next poll return [`PollOutcome::Reset`].
#[async_trait::async_trait]
pub trait CacheChangeLog: Send {
    /// Move the cursor to now. Changes committed before are not returned. Must be called
    /// before the first [`Self::poll`] and after every [`PollOutcome::Reset`].
    async fn restart(&mut self) -> Result<(), ChangeLogError>;

    /// The next changes, at most about `max_keys` keys (always at least one page).
    async fn poll(&mut self, max_keys: usize) -> Result<PollOutcome, ChangeLogError>;

    /// Write a change that names no cache entry and return its marker. Seeing it on the
    /// read source shows that everything committed before it has arrived there.
    async fn insert_barrier(&mut self) -> Result<ChangeMarker, ChangeLogError>;

    /// The markers among `markers` that the read source does not show yet.
    async fn unconfirmed(
        &self,
        markers: &[ChangeMarker],
    ) -> Result<Vec<ChangeMarker>, ChangeLogError>;

    /// Whether cached loads read the primary, so they can never return data older than
    /// the log.
    fn reads_from_primary(&self) -> bool;
}

/// A [`CacheChangeLog`] of any backend, owned by the poller task.
pub type BoxedCacheChangeLog = Box<dyn CacheChangeLog>;
