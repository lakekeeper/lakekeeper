//! The background tasks of cache invalidation: reading the change log and pruning it.

use std::time::Duration;

use futures::future::BoxFuture;
use tracing::Instrument;

use super::{
    BoxedCacheChangeLog, CacheChangeReceivers, ChangeLogSettings, PollOutcome, PruneOutcome,
    PruneSettings, evict::Evictions, metrics,
};
use crate::{CONFIG, CancellationToken, service::CatalogStore};

/// The longest one read of the change log may run on the database.
const POLL_STATEMENT_TIMEOUT: Duration = Duration::from_secs(10);
/// The longest the first read of the log may delay startup.
const STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
/// The longest pause between reads after failed reads, unless the interval is longer.
const MAX_BACKOFF: Duration = Duration::from_secs(10);
/// How often the change log is pruned.
const PRUNE_INTERVAL: Duration = Duration::from_mins(5);
/// The longest one statement of a pruning run may run.
const PRUNE_STATEMENT_TIMEOUT: Duration = Duration::from_secs(30);

/// The background tasks of cache invalidation, by name, which run until
/// `cancellation_token` is cancelled: reading the change log, if
/// `cache.invalidation.enabled`, and pruning it. The changes of an extension domain go to its
/// receiver in `receivers`. The first read of the log runs before this returns, for at most
/// 10 s. Pruning does not run in read-only maintenance mode.
pub async fn background_tasks<C: CatalogStore>(
    catalog_state: C::State,
    receivers: CacheChangeReceivers,
    cancellation_token: &CancellationToken,
) -> Vec<(&'static str, BoxFuture<'static, ()>)> {
    let mut tasks: Vec<(&'static str, BoxFuture<'static, ()>)> = Vec::new();
    let invalidation = &CONFIG.cache.invalidation;
    if invalidation.enabled {
        let log = C::cache_change_log(
            catalog_state.clone(),
            ChangeLogSettings {
                statement_timeout: POLL_STATEMENT_TIMEOUT,
            },
        );
        let mut poller = Poller::new(
            log,
            ProcessCaches { receivers },
            invalidation.poll_interval,
            invalidation.reevict_grace,
        );
        if tokio::time::timeout(STARTUP_TIMEOUT, poller.start())
            .await
            .is_err()
        {
            tracing::warn!(
                "The first read of the cache change log timed out; retrying in the background"
            );
        }
        tasks.push((
            "Cache Invalidation Poller",
            Box::pin(poller.run(cancellation_token.clone())),
        ));
    } else {
        tracing::info!("Cache invalidation is disabled");
    }
    if !CONFIG.maintenance_mode.is_read_only() {
        tasks.push((
            "Cache Change Log Pruner",
            Box::pin(prune_periodically::<C>(
                catalog_state,
                cancellation_token.clone(),
            )),
        ));
    }
    tasks
}

/// The caches a [`Poller`] evicts from.
trait Caches: Clone + Send + Sync + 'static {
    fn evict(&self, evictions: &Evictions) -> impl Future<Output = ()> + Send;
}

/// The caches of this process, and the receivers of extension domains.
#[derive(Debug, Clone)]
struct ProcessCaches {
    receivers: CacheChangeReceivers,
}

impl Caches for ProcessCaches {
    async fn evict(&self, evictions: &Evictions) {
        // At the same time: an eviction of a built-in cache waits for the in-flight load of
        // its key.
        tokio::join!(evictions.apply(), evictions.apply_to(&self.receivers));
    }
}

/// Reads the change log and evicts the entries other instances changed: once when it sees
/// a change, and once more after the grace period, for a read replica that had not yet
/// replayed the change.
struct Poller<C> {
    log: BoxedCacheChangeLog,
    caches: C,
    interval: Duration,
    grace: Duration,
    failures: u32,
    rng: fastrand::Rng,
}

impl<C: Caches> Poller<C> {
    fn new(log: BoxedCacheChangeLog, caches: C, interval: Duration, grace: Duration) -> Self {
        Self {
            log,
            caches,
            interval,
            grace,
            failures: 0,
            rng: fastrand::Rng::new(),
        }
    }

    /// Start the log: the first read returns no changes. A failed start is retried by
    /// [`Self::run`].
    async fn start(&mut self) {
        self.poll_once().await;
    }

    /// Read the log until `cancellation_token` is cancelled.
    async fn run(mut self, cancellation_token: CancellationToken) {
        loop {
            let delay = self.delay();
            tokio::select! {
                () = cancellation_token.cancelled() => return,
                () = tokio::time::sleep(delay) => {}
            }
            tokio::select! {
                () = cancellation_token.cancelled() => return,
                () = self.poll_once() => {}
            }
        }
    }

    async fn poll_once(&mut self) {
        let evictions = match self.log.poll().await {
            Ok(PollOutcome::Changes(changes)) => {
                metrics::record_poll(&changes);
                Evictions::of(&changes)
            }
            Ok(PollOutcome::Reset(reason)) => {
                metrics::record_reset(reason);
                tracing::info!(
                    reason = <&'static str>::from(reason),
                    "The cache change log cannot list the changes since its previous read; evicting every cached entry"
                );
                Evictions::all()
            }
            Err(error) => {
                metrics::record_poll_error();
                self.failures = self.failures.saturating_add(1);
                tracing::warn!(
                    ?error,
                    "Reading the cache change log failed; retrying in {:?}",
                    self.delay_after_failures()
                );
                return;
            }
        };
        self.failures = 0;
        if !evictions.is_empty() {
            self.evict_twice(evictions);
        }
    }

    /// Evict now and once more after the grace period, in a task of its own: an eviction
    /// waits for the in-flight load of its key, and the reads of the log never wait.
    fn evict_twice(&self, evictions: Evictions) {
        let caches = self.caches.clone();
        let grace = self.grace;
        tokio::spawn(
            async move {
                caches.evict(&evictions).await;
                tokio::time::sleep(grace).await;
                caches.evict(&evictions).await;
            }
            .in_current_span(),
        );
    }

    /// The pause before the next read: the interval with 10 % jitter, or after failed
    /// reads, the interval doubled per failure up to 10 s or the interval if longer.
    fn delay(&mut self) -> Duration {
        if self.failures == 0 {
            jittered(self.interval, &mut self.rng)
        } else {
            self.delay_after_failures()
        }
    }

    fn delay_after_failures(&self) -> Duration {
        self.interval
            .saturating_mul(2_u32.saturating_pow(self.failures))
            .min(MAX_BACKOFF.max(self.interval))
    }
}

/// `interval` changed by up to 10 % either way, in whole milliseconds.
fn jittered(interval: Duration, rng: &mut fastrand::Rng) -> Duration {
    let millis = interval.as_millis() * u128::from(rng.u64(900..=1100)) / 1000;
    Duration::from_millis(u64::try_from(millis).unwrap_or(u64::MAX))
}

/// How long change-log rows are kept: twice the longest time-to-live of the caches the log
/// invalidates, at least two hours and at most 30 days. A poller that misses a pruned row
/// then only misses entries that have expired.
fn retention(time_to_live: impl IntoIterator<Item = Duration>) -> Duration {
    time_to_live
        .into_iter()
        .max()
        .unwrap_or_default()
        .saturating_mul(2)
        .clamp(Duration::from_hours(2), Duration::from_hours(30 * 24))
}

fn configured_retention() -> Duration {
    let cache = &CONFIG.cache;
    retention(
        [
            cache.warehouse.time_to_live_secs,
            cache.namespace.time_to_live_secs,
            cache.role.time_to_live_secs,
            cache.user_assignments.time_to_live_secs,
            cache.role_ancestors.time_to_live_secs,
            cache.secrets.time_to_live_secs,
        ]
        .map(Duration::from_secs),
    )
}

/// Prune the change log every few minutes until `cancellation_token` is cancelled. Every
/// instance runs this; one at a time prunes.
async fn prune_periodically<C: CatalogStore>(
    catalog_state: C::State,
    cancellation_token: CancellationToken,
) {
    let settings = PruneSettings {
        retention: configured_retention(),
        statement_timeout: PRUNE_STATEMENT_TIMEOUT,
    };
    let mut rng = fastrand::Rng::new();
    loop {
        tokio::select! {
            () = cancellation_token.cancelled() => return,
            () = tokio::time::sleep(jittered(PRUNE_INTERVAL, &mut rng)) => {}
        }
        let run = tokio::time::timeout(
            PRUNE_INTERVAL,
            C::prune_cache_change_log(catalog_state.clone(), settings),
        );
        let outcome = tokio::select! {
            () = cancellation_token.cancelled() => return,
            outcome = run => outcome,
        };
        match outcome {
            Err(_) => {
                metrics::record_prune_error();
                tracing::warn!("Pruning the cache change log timed out");
            }
            Ok(Ok(PruneOutcome::Pruned { deleted_rows })) => {
                metrics::record_pruned(deleted_rows);
                tracing::debug!(deleted_rows, "Pruned the cache change log");
            }
            Ok(Ok(PruneOutcome::Skipped)) => {
                tracing::debug!("Another instance is pruning the cache change log");
            }
            Ok(Err(error)) => {
                metrics::record_prune_error();
                tracing::warn!(?error, "Pruning the cache change log failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };

    use tokio::time::Instant;

    use super::*;
    use crate::service::cache_invalidation::{
        CacheChangeLog, ChangeLogError, LoggedChange, ResetReason,
    };

    const INTERVAL: Duration = Duration::from_secs(1);
    const GRACE: Duration = Duration::from_secs(10);
    const SEED: u64 = 7;

    /// A log that returns `outcomes` in order, then no changes.
    struct ScriptedLog(VecDeque<Result<PollOutcome, ChangeLogError>>);

    #[async_trait::async_trait]
    impl CacheChangeLog for ScriptedLog {
        async fn poll(&mut self) -> Result<PollOutcome, ChangeLogError> {
            self.0
                .pop_front()
                .unwrap_or(Ok(PollOutcome::Changes(Vec::new())))
        }
    }

    /// Caches that record each eviction with the time since the test started.
    #[derive(Clone)]
    struct Recorded {
        start: Instant,
        evictions: Arc<Mutex<Vec<(Duration, Evictions)>>>,
    }

    impl Caches for Recorded {
        async fn evict(&self, evictions: &Evictions) {
            self.evictions
                .lock()
                .unwrap()
                .push((self.start.elapsed(), evictions.clone()));
        }
    }

    fn secret_change() -> LoggedChange {
        LoggedChange {
            domain: "secret".to_string(),
            keys: vec![uuid::Uuid::now_v7().to_string()],
        }
    }

    /// The first pause of a poller seeded with [`SEED`].
    fn first_delay() -> Duration {
        jittered(INTERVAL, &mut fastrand::Rng::with_seed(SEED))
    }

    /// Start a poller on `outcomes` (after the start's empty read), run it for 60 s, and
    /// return what it evicted.
    async fn run(outcomes: Vec<Result<PollOutcome, ChangeLogError>>) -> Vec<(Duration, Evictions)> {
        let mut outcomes = VecDeque::from(outcomes);
        outcomes.push_front(Ok(PollOutcome::Changes(Vec::new())));
        let caches = Recorded {
            start: Instant::now(),
            evictions: Arc::default(),
        };
        let mut poller = Poller::new(
            Box::new(ScriptedLog(outcomes)),
            caches.clone(),
            INTERVAL,
            GRACE,
        );
        poller.rng = fastrand::Rng::with_seed(SEED);
        poller.start().await;
        let cancellation_token = CancellationToken::new();
        let task = tokio::spawn(poller.run(cancellation_token.clone()));
        tokio::time::sleep(Duration::from_secs(60)).await;
        cancellation_token.cancel();
        task.await.unwrap();
        caches.evictions.lock().unwrap().clone()
    }

    #[tokio::test(start_paused = true)]
    async fn a_change_is_evicted_at_once_and_again_after_the_grace_period() {
        let change = secret_change();
        let expected = Evictions::of(std::slice::from_ref(&change));

        let evictions = run(vec![Ok(PollOutcome::Changes(vec![change]))]).await;

        let first = first_delay();
        assert_eq!(
            evictions,
            vec![(first, expected.clone()), (first + GRACE, expected)]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_reset_evicts_everything_at_once_and_again_after_the_grace_period() {
        let evictions = run(vec![Ok(PollOutcome::Reset(ResetReason::Overflow))]).await;

        let first = first_delay();
        assert_eq!(
            evictions,
            vec![(first, Evictions::all()), (first + GRACE, Evictions::all())]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn failed_reads_evict_nothing_and_back_off_until_a_read_succeeds() {
        let (first, second) = (secret_change(), secret_change());
        let evict_first = Evictions::of(std::slice::from_ref(&first));
        let evict_second = Evictions::of(std::slice::from_ref(&second));

        let evictions = run(vec![
            Err(ChangeLogError::Standby),
            Err(ChangeLogError::Standby),
            Ok(PollOutcome::Changes(vec![first])),
            Ok(PollOutcome::Changes(vec![second])),
        ])
        .await;

        let mut rng = fastrand::Rng::with_seed(SEED);
        // 2 s after the first failure, 4 s after the second.
        let recovered = jittered(INTERVAL, &mut rng) + Duration::from_secs(6);
        let next = recovered + jittered(INTERVAL, &mut rng);
        assert_eq!(
            evictions,
            vec![
                (recovered, evict_first.clone()),
                (next, evict_second.clone()),
                (recovered + GRACE, evict_first),
                (next + GRACE, evict_second),
            ]
        );
    }

    #[test]
    fn the_backoff_doubles_per_failure_up_to_ten_seconds_or_the_interval() {
        let mut poller = Poller::new(
            Box::new(ScriptedLog(VecDeque::new())),
            ProcessCaches {
                receivers: CacheChangeReceivers::default(),
            },
            INTERVAL,
            GRACE,
        );
        let delays: Vec<_> = (1..=5)
            .map(|failures| {
                poller.failures = failures;
                poller.delay().as_secs()
            })
            .collect();

        assert_eq!(delays, [2, 4, 8, 10, 10]);

        // Never shorter than the interval.
        poller.interval = Duration::from_secs(30);
        poller.failures = 1;
        assert_eq!(poller.delay(), Duration::from_secs(30));
    }

    #[test]
    fn rows_are_kept_twice_the_longest_time_to_live_within_two_hours_and_30_days() {
        let hours = |h| Duration::from_hours(h);
        assert_eq!(retention([Duration::from_secs(600)]), hours(2));
        assert_eq!(retention([hours(1), hours(3)]), hours(6));
        assert_eq!(retention([hours(24 * 365)]), hours(30 * 24));
        assert_eq!(retention([]), hours(2));
    }
}
