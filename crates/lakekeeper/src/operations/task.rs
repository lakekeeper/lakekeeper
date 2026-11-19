use std::time::{Duration, Instant};

use tryhard::{RetryPolicy, backoff_strategies::BackoffStrategy};

/// Exponential backoff with optional jitter. The delay doubles each attempt
/// and is capped at `max_sleep_time`. When `jitter` is enabled, each delay is
/// multiplied by a random factor in `[0.5, 1.5)` to desynchronize concurrent
/// retries (prevents thundering-herd re-hitting the same rate limit).
#[derive(Debug, Clone, Copy)]
pub struct ExtendedExponentialBackoff {
    min_sleep_time: Duration,
    max_sleep_time: Duration,
    max_time_since_start: Duration,
    jitter: bool,
    start: Instant,
}

impl ExtendedExponentialBackoff {
    /// Create a new `ExponentialBackoff` without jitter.
    #[must_use]
    pub fn new(
        min_sleep_time: Duration,
        max_sleep_time: Duration,
        max_time_since_start: Duration,
    ) -> Self {
        Self {
            min_sleep_time,
            max_sleep_time,
            max_time_since_start,
            jitter: false,
            start: Instant::now(),
        }
    }

    /// Enable random jitter on each delay. Recommended for concurrent retries.
    #[must_use]
    pub fn with_jitter(mut self) -> Self {
        self.jitter = true;
        self
    }
}

impl<'a, E> BackoffStrategy<'a, E> for ExtendedExponentialBackoff
where
    E: 'a,
{
    type Output = RetryPolicy;

    // This function is called after the first failed attempt with `attempt` set to 1.
    fn delay(&mut self, attempt: u32, _error: &'a E) -> RetryPolicy {
        let elapsed = self.start.elapsed();
        if elapsed >= self.max_time_since_start {
            tracing::debug!(
                "ExponentialBackoff: Stopping retries after {} ms. Exceeds max time {} ms",
                elapsed.as_millis(),
                self.max_time_since_start.as_millis()
            );
            return RetryPolicy::Break;
        }

        let exp = attempt.saturating_sub(1).min(31);
        let factor = 1u32.checked_shl(exp).unwrap_or(u32::MAX);
        let base_sleep = self
            .min_sleep_time
            .saturating_mul(factor)
            .min(self.max_sleep_time);

        let sleep_time = if self.jitter {
            // Random factor in [0.5, 1.5): keeps mean = base_sleep, spread
            // out enough to desynchronize concurrent retriers.
            let base_nanos = u64::try_from(base_sleep.as_nanos()).unwrap_or(u64::MAX);
            let jitter_factor = 0.5 + fastrand::f64();
            #[allow(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                clippy::cast_precision_loss
            )]
            let scaled = (base_nanos as f64 * jitter_factor) as u64;
            Duration::from_nanos(scaled).min(self.max_sleep_time)
        } else {
            base_sleep
        };

        if elapsed + sleep_time > self.max_time_since_start {
            tracing::debug!(
                "ExponentialBackoff: Stopping retries after {} ms. Sleep time {} ms exceeds max time {} ms",
                elapsed.as_millis(),
                sleep_time.as_millis(),
                self.max_time_since_start.as_millis()
            );
            return RetryPolicy::Break;
        }

        RetryPolicy::Delay(sleep_time)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exponential_backoff_delays() {
        // attempt=0 should not panic and should yield min sleep
        let mut backoff0 = ExtendedExponentialBackoff::new(
            Duration::from_millis(100),
            Duration::from_secs(2),
            Duration::from_secs(10),
        );
        assert_eq!(
            backoff0.delay(0, &"error"),
            RetryPolicy::Delay(Duration::from_millis(100))
        );

        let mut backoff = ExtendedExponentialBackoff::new(
            Duration::from_millis(100), // min sleep time
            Duration::from_secs(2),     // max sleep time
            Duration::from_secs(10),    // max time since start
        );

        // Test the first few attempts to verify exponential growth
        assert_eq!(
            backoff.delay(1, &"error"),
            RetryPolicy::Delay(Duration::from_millis(100))
        );
        assert_eq!(
            backoff.delay(2, &"error"),
            RetryPolicy::Delay(Duration::from_millis(200))
        );
        assert_eq!(
            backoff.delay(3, &"error"),
            RetryPolicy::Delay(Duration::from_millis(400))
        );
        assert_eq!(
            backoff.delay(4, &"error"),
            RetryPolicy::Delay(Duration::from_millis(800))
        );
        assert_eq!(
            backoff.delay(5, &"error"),
            RetryPolicy::Delay(Duration::from_millis(1600))
        );

        // Test max sleep time capping
        assert_eq!(
            backoff.delay(6, &"error"),
            RetryPolicy::Delay(Duration::from_secs(2))
        );
    }

    #[test]
    fn test_exponential_backoff_max_time() {
        // Create a backoff with a very short max time
        let mut backoff = ExtendedExponentialBackoff::new(
            Duration::from_millis(100),
            Duration::from_secs(2),
            Duration::from_millis(150), // Very short max time
        );

        // First attempt should be delayed
        assert_eq!(
            backoff.delay(1, &"error"),
            RetryPolicy::Delay(Duration::from_millis(100))
        );

        // Next attempt should break because we've exceeded the max time
        assert_eq!(backoff.delay(2, &"error"), RetryPolicy::Break);
    }

    #[tokio::test]
    async fn test_exponential_backoff_integration() {
        let backoff = ExtendedExponentialBackoff::new(
            Duration::from_millis(10), // Small values for faster test
            Duration::from_millis(100),
            Duration::from_millis(500),
        );

        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter_clone = counter.clone();

        // This should eventually break due to max time
        let result = tryhard::retry_fn(move || {
            let counter = counter_clone.clone();
            async move {
                let _current = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Result::<(), &str>::Err("Simulated error")
            }
        })
        .retries(20)
        .custom_backoff(backoff)
        .await;

        // The operation should fail
        assert!(result.is_err());

        // We should have made at least a few attempts before timing out
        let attempt_count = counter.load(std::sync::atomic::Ordering::SeqCst);
        assert!(
            attempt_count > 1,
            "Should have attempted multiple retries, got {attempt_count}",
        );
    }
}
