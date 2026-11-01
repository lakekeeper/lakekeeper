//! Metrics of the change-log poller and of pruning.

use std::{
    sync::LazyLock,
    time::{SystemTime, UNIX_EPOCH},
};

use axum_prometheus::metrics;

use super::{CacheDomain, LoggedChange, ResetReason};

const METRIC_LAST_POLL: &str = "lakekeeper_cache_invalidation_last_poll_timestamp_seconds";
const METRIC_POLL_ERRORS: &str = "lakekeeper_cache_invalidation_poll_errors_total";
const METRIC_CHANGES: &str = "lakekeeper_cache_invalidation_changes_total";
const METRIC_RESETS: &str = "lakekeeper_cache_invalidation_resets_total";
const METRIC_PRUNED_ROWS: &str = "lakekeeper_cache_change_log_pruned_rows_total";
const METRIC_PRUNE_ERRORS: &str = "lakekeeper_cache_change_log_prune_errors_total";

static METRICS_INITIALIZED: LazyLock<()> = LazyLock::new(|| {
    metrics::describe_gauge!(
        METRIC_LAST_POLL,
        "Unix time of the last successful read of the cache change log"
    );
    metrics::describe_counter!(METRIC_POLL_ERRORS, "Failed reads of the cache change log");
    metrics::describe_counter!(
        METRIC_CHANGES,
        "Change-log rows read, by cache domain (`other` for domains this server does not cache)"
    );
    metrics::describe_counter!(
        METRIC_RESETS,
        "Reads of the cache change log that flushed every cache, by reason"
    );
    metrics::describe_counter!(METRIC_PRUNED_ROWS, "Change-log rows deleted by pruning");
    metrics::describe_counter!(METRIC_PRUNE_ERRORS, "Failed pruning runs of the change log");
});

fn set_last_poll() {
    if let Ok(since_epoch) = SystemTime::now().duration_since(UNIX_EPOCH) {
        metrics::gauge!(METRIC_LAST_POLL).set(since_epoch.as_secs_f64());
    }
}

pub(super) fn record_poll(changes: &[LoggedChange]) {
    let () = &*METRICS_INITIALIZED;
    set_last_poll();
    for change in changes {
        let domain = CacheDomain::from_name(&change.domain).map_or("other", CacheDomain::as_str);
        metrics::counter!(METRIC_CHANGES, "domain" => domain).increment(1);
    }
}

pub(super) fn record_reset(reason: ResetReason) {
    let () = &*METRICS_INITIALIZED;
    set_last_poll();
    let reason: &'static str = reason.into();
    metrics::counter!(METRIC_RESETS, "reason" => reason).increment(1);
}

pub(super) fn record_poll_error() {
    let () = &*METRICS_INITIALIZED;
    metrics::counter!(METRIC_POLL_ERRORS).increment(1);
}

pub(super) fn record_pruned(rows: u64) {
    let () = &*METRICS_INITIALIZED;
    metrics::counter!(METRIC_PRUNED_ROWS).increment(rows);
}

pub(super) fn record_prune_error() {
    let () = &*METRICS_INITIALIZED;
    metrics::counter!(METRIC_PRUNE_ERRORS).increment(1);
}
