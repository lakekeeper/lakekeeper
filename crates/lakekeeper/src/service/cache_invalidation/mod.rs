//! Eviction of cache entries that another Lakekeeper instance changed.
//!
//! A write that changes cached data records the changed keys in a change log, in the same
//! transaction. Every instance reads the log and evicts those keys from its own caches.

mod change_set;

pub use change_set::{CacheDomain, CacheKey, CacheKeyDecodeError, ChangeSet};

/// Whether writes record their changed keys (`cache.invalidation.recording_enabled`).
#[must_use]
pub fn recording_enabled() -> bool {
    crate::CONFIG.cache.invalidation.recording_enabled
}
