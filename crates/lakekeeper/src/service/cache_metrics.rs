//! Shared Prometheus metric names and initialisation for all caches.
//!
//! Every cache emits the same three metric names differentiated by the
//! `cache_type` label (values: `"role"`, `"warehouse"`, `"namespace"`,
//! `"secrets"`, `"stc"`, `"user_assignments"`, `"role_members"`).

use std::sync::LazyLock;

use axum_prometheus::metrics;

pub(crate) const METRIC_CACHE_SIZE: &str = "lakekeeper_cache_size";
pub(crate) const METRIC_CACHE_HITS_TOTAL: &str = "lakekeeper_cache_hits_total";
pub(crate) const METRIC_CACHE_MISSES_TOTAL: &str = "lakekeeper_cache_misses_total";

/// Registers metric descriptions exactly once for the shared cache metrics.
pub(crate) static METRICS_INITIALIZED: LazyLock<()> = LazyLock::new(|| {
    metrics::describe_gauge!(METRIC_CACHE_SIZE, "Current number of entries in the cache");
    metrics::describe_counter!(METRIC_CACHE_HITS_TOTAL, "Total number of cache hits");
    metrics::describe_counter!(METRIC_CACHE_MISSES_TOTAL, "Total number of cache misses");
});
