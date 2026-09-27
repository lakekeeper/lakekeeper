//! Shared Prometheus metric names and initialisation for all caches.
//!
//! Every cache emits the same three metric names differentiated by the
//! `cache_type` label (values: `"role"`, `"warehouse"`, `"namespace"`,
//! `"secrets"`, `"stc"`, `"user_assignments"`, `"role_members"`,
//! `"role_ancestors"`, `"warehouse_name_to_id"`, `"role_ident_to_id"`,
//! `"namespace_ident_to_id"`, `"shared_role_idents"`, `"shared_project_ids"`).
//!
//! Caches owned by code outside this crate — an
//! [`AdmissionGate`](crate::service::admission::AdmissionGate), an
//! [`Authorizer`](crate::service::authz::Authorizer), or any other pluggable
//! implementation registered by a host binary — report into the same three
//! series through [`record_cache_hit`], [`record_cache_miss`] and
//! [`set_cache_size`]. Reusing them keeps one dashboard and one alerting rule
//! valid for every cache, whichever crate owns it; a new cache only needs a new
//! `cache_type` value.

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

/// Record one cache hit for `cache_type`.
pub fn record_cache_hit(cache_type: &'static str) {
    let () = &*METRICS_INITIALIZED;
    metrics::counter!(METRIC_CACHE_HITS_TOTAL, "cache_type" => cache_type).increment(1);
}

/// Record one cache miss for `cache_type`.
pub fn record_cache_miss(cache_type: &'static str) {
    let () = &*METRICS_INITIALIZED;
    metrics::counter!(METRIC_CACHE_MISSES_TOTAL, "cache_type" => cache_type).increment(1);
}

/// Publish the current number of entries held by `cache_type`.
///
/// For moka caches pass `entry_count()`, which is approximate until pending
/// maintenance tasks are drained.
#[allow(clippy::cast_precision_loss)]
pub fn set_cache_size(cache_type: &'static str, entries: u64) {
    let () = &*METRICS_INITIALIZED;
    metrics::gauge!(METRIC_CACHE_SIZE, "cache_type" => cache_type).set(entries as f64);
}
