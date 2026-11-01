//! Task queue counters that a catalog backend reports into.

use std::sync::LazyLock;

use axum_prometheus::metrics;

use super::TaskQueueName;

/// Attempts failed because their heartbeat expired, by `queue_name`.
const METRIC_TIMED_OUT_ATTEMPTS: &str = "lakekeeper_task_queue_timed_out_attempts_total";

static METRICS_INITIALIZED: LazyLock<()> = LazyLock::new(|| {
    metrics::describe_counter!(
        METRIC_TIMED_OUT_ATTEMPTS,
        "Task attempts failed because their worker stopped sending heartbeats, by queue"
    );
});

/// Count one attempt of `queue_name` failed because its heartbeat expired. A backend calls this
/// once per such attempt, after the failure is committed.
pub fn record_timed_out_attempt(queue_name: &TaskQueueName) {
    let () = &*METRICS_INITIALIZED;
    metrics::counter!(METRIC_TIMED_OUT_ATTEMPTS, "queue_name" => queue_name.to_string())
        .increment(1);
}

/// Register the counter of `queue_name` at 0, so that a timed-out attempt shows as an increase
/// once Prometheus has scraped the 0. A process calls this for each queue it runs workers for.
pub(super) fn init_timed_out_attempts(queue_name: &TaskQueueName) {
    let () = &*METRICS_INITIALIZED;
    metrics::counter!(METRIC_TIMED_OUT_ATTEMPTS, "queue_name" => queue_name.to_string())
        .increment(0);
}
