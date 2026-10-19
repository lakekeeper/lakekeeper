//! Eviction of cache entries that another Lakekeeper instance changed.
//!
//! A write that changes cached data records the changed keys in a change log, in the same
//! transaction. Every instance reads the log and evicts those keys from its own caches.

mod change_set;
mod evict;
mod log;
mod metrics;
mod poller;

pub use change_set::{CacheDomain, CacheKey, CacheKeyDecodeError, ChangeSet};
pub use log::{
    BoxedCacheChangeLog, CacheChangeLog, ChangeLogError, ChangeLogSettings, LoggedChange,
    PollOutcome, PruneOutcome, PruneSettings, ResetReason,
};
pub use poller::background_tasks;

/// Whether writes record their changed keys (`cache.invalidation.recording_enabled`).
#[must_use]
pub fn recording_enabled() -> bool {
    crate::CONFIG.cache.invalidation.recording_enabled
}
