//! Shared Prometheus metric names and initialisation for all caches.
//!
//! Every cache emits the same three metric names differentiated by the
//! `cache_type` label (values: `"role"`, `"warehouse"`, `"namespace"`,
//! `"secrets"`, `"stc"`, `"user_assignments"`, `"role_members"`,
//! `"warehouse_name_to_id"`, `"role_ident_to_id"`, `"namespace_ident_to_id"`).

use std::sync::LazyLock;

use axum_prometheus::metrics;

pub(crate) const METRIC_CACHE_SIZE: &str = "lakekeeper_cache_size";
pub(crate) const METRIC_CACHE_HITS_TOTAL: &str = "lakekeeper_cache_hits_total";
pub(crate) const METRIC_CACHE_MISSES_TOTAL: &str = "lakekeeper_cache_misses_total";

/// Histogram of how many users' cached role assignments are invalidated by a
/// single role→role membership edge change, labelled by `operation`
/// (`"add"`/`"remove"`). `USER_ASSIGNMENTS_CACHE` stores a fully-expanded
/// transitive closure, so one edge change fans out to every affected user; this
/// measures that fan-out distribution.
pub(crate) const METRIC_ROLE_MEMBERSHIP_EDGE_FANOUT_USERS: &str =
    "lakekeeper_role_membership_edge_fanout_users";

/// Registers metric descriptions exactly once for the shared cache metrics.
pub(crate) static METRICS_INITIALIZED: LazyLock<()> = LazyLock::new(|| {
    metrics::describe_gauge!(METRIC_CACHE_SIZE, "Current number of entries in the cache");
    metrics::describe_counter!(METRIC_CACHE_HITS_TOTAL, "Total number of cache hits");
    metrics::describe_counter!(METRIC_CACHE_MISSES_TOTAL, "Total number of cache misses");
    metrics::describe_histogram!(
        METRIC_ROLE_MEMBERSHIP_EDGE_FANOUT_USERS,
        "Number of users whose cached role assignments were invalidated by a single role-membership edge change"
    );
});
