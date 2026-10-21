//! Reading the change log: the contract between a storage backend and the poller.

/// One change-log row: some keys of one domain that a committed transaction changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoggedChange {
    /// The domain name as stored. Domains of extensions are not [`super::CacheDomain`]s.
    pub domain: String,
    /// Encoded keys; empty for the whole domain.
    pub keys: Vec<String>,
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
    /// More changes than one poll returns were committed since the previous poll.
    Overflow,
    /// The first read after startup failed or did not finish within 10 s, and the instance
    /// served requests before it knew its position in the log. Set by the poller; a backend
    /// never returns it.
    StartedLate,
}

/// The result of one [`CacheChangeLog::poll`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PollOutcome {
    /// The changes committed since the previous poll, in log order.
    Changes(Vec<LoggedChange>),
    /// The log cannot list the changes since the previous poll. The caller must treat
    /// every cached entry as stale. The next poll returns the changes committed after this
    /// one.
    Reset(ResetReason),
}

/// An error reading the change log. The poll can be retried; it then returns the changes
/// the failed poll would have returned.
#[derive(Debug, thiserror::Error)]
pub enum ChangeLogError {
    /// The change log was read from a standby; it must be read from the primary.
    #[error("The change log was read from a standby, not from the primary")]
    Standby,
    /// A statement or the connection failed.
    #[error("A change log statement failed")]
    Query(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
}

/// The change log as the poller sees it: each [`Self::poll`] returns the changes committed
/// since the previous one.
#[async_trait::async_trait]
pub trait CacheChangeLog: Send {
    /// The changes committed since the previous poll. The first poll starts the log at the
    /// current state of the database and returns no changes.
    async fn poll(&mut self) -> Result<PollOutcome, ChangeLogError>;
}

/// A [`CacheChangeLog`] of any backend, owned by the poller task.
pub type BoxedCacheChangeLog = Box<dyn CacheChangeLog>;
