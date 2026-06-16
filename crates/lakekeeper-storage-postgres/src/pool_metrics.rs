//! Prometheus metrics for the Postgres connection pools.
//!
//! Exposes Lakekeeper's *client-side* pool saturation, which is distinct from
//! the Postgres *server's* connection slots (covered by `postgres_exporter`).
//! The read and write pools are reported separately via the `pool` label.

use std::{sync::LazyLock, time::Duration};

use metrics::{counter, describe_counter, describe_gauge, gauge};
use sqlx::PgPool;

pub(crate) const METRIC_POOL_CONNECTIONS: &str = "lakekeeper_catalog_pg_pool_connections";
pub(crate) const METRIC_POOL_MAX_CONNECTIONS: &str = "lakekeeper_catalog_pg_pool_max_connections";
pub(crate) const METRIC_POOL_ACQUIRE_TIMEOUTS: &str =
    "lakekeeper_catalog_pg_pool_acquire_timeouts_total";

/// Sample interval for the pool gauges. Saturation *events* are caught
/// per-occurrence by the timeout counter; the gauges only need to surface
/// *sustained* pressure.
pub(crate) const SAMPLE_INTERVAL: Duration = Duration::from_secs(15);

static METRICS_INITIALIZED: LazyLock<()> = LazyLock::new(|| {
    describe_gauge!(
        METRIC_POOL_CONNECTIONS,
        "Live Postgres pool connections, by pool (read/write) and state (in_use/idle)"
    );
    describe_gauge!(
        METRIC_POOL_MAX_CONNECTIONS,
        "Configured maximum connections per Postgres pool"
    );
    describe_counter!(
        METRIC_POOL_ACQUIRE_TIMEOUTS,
        "Total Postgres connection acquisitions that timed out (client-side pool saturation)"
    );
});

#[derive(Debug, PartialEq, Eq)]
struct PoolGaugeValues {
    in_use: u32,
    idle: u32,
    max: u32,
}

/// Pure derivation of gauge values from raw sqlx pool stats.
/// `size` = total connections in the pool (idle + in_use); `num_idle` = idle.
fn pool_gauge_values(size: u32, num_idle: usize, max: u32) -> PoolGaugeValues {
    let idle = u32::try_from(num_idle).unwrap_or(u32::MAX);
    PoolGaugeValues {
        in_use: size.saturating_sub(idle),
        idle,
        max,
    }
}

/// True if the error is a connection-pool acquire timeout.
pub(crate) fn is_pool_timeout(e: &sqlx::Error) -> bool {
    matches!(e, sqlx::Error::PoolTimedOut)
}

fn record_one(pool: &PgPool, pool_label: &'static str) {
    let v = pool_gauge_values(
        pool.size(),
        pool.num_idle(),
        pool.options().get_max_connections(),
    );
    gauge!(METRIC_POOL_CONNECTIONS, "pool" => pool_label, "state" => "in_use")
        .set(f64::from(v.in_use));
    gauge!(METRIC_POOL_CONNECTIONS, "pool" => pool_label, "state" => "idle").set(f64::from(v.idle));
    gauge!(METRIC_POOL_MAX_CONNECTIONS, "pool" => pool_label).set(f64::from(v.max));
}

/// Sample both pools once and set their gauges.
pub(crate) fn record(read_pool: &PgPool, write_pool: &PgPool) {
    let () = *METRICS_INITIALIZED;
    record_one(read_pool, "read");
    record_one(write_pool, "write");
}

/// Increment the acquire-timeout counter for `pool_label` (`"read"` | `"write"`).
pub(crate) fn record_acquire_timeout(pool_label: &'static str) {
    let () = *METRICS_INITIALIZED;
    counter!(METRIC_POOL_ACQUIRE_TIMEOUTS, "pool" => pool_label).increment(1);
}

#[cfg(test)]
mod tests {
    use super::{PoolGaugeValues, is_pool_timeout, pool_gauge_values};

    #[test]
    fn computes_in_use_from_size_and_idle() {
        assert_eq!(
            pool_gauge_values(10, 3, 10),
            PoolGaugeValues {
                in_use: 7,
                idle: 3,
                max: 10
            }
        );
    }

    #[test]
    fn empty_pool_is_all_zero_with_configured_max() {
        assert_eq!(
            pool_gauge_values(0, 0, 5),
            PoolGaugeValues {
                in_use: 0,
                idle: 0,
                max: 5
            }
        );
    }

    #[test]
    fn idle_exceeding_size_clamps_in_use_to_zero() {
        assert_eq!(
            pool_gauge_values(2, 5, 10),
            PoolGaugeValues {
                in_use: 0,
                idle: 5,
                max: 10
            }
        );
    }

    #[test]
    fn pool_timeout_is_detected_others_are_not() {
        assert!(is_pool_timeout(&sqlx::Error::PoolTimedOut));
        assert!(!is_pool_timeout(&sqlx::Error::RowNotFound));
    }
}
