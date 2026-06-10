//! Shared per-entry TTL jitter for the in-process caches.
//!
//! Every catalog cache uses a fixed `time_to_live`. Many replicas that warm the
//! same hot key around the same wall-clock instant then expire it on the same
//! TTL boundary and re-derive it simultaneously — a fleet-wide stampede that
//! per-replica single-flight cannot see. [`JitteredTtl`] desynchronizes those
//! boundaries by shortening each entry's lifetime by a small random fraction.
//!
//! The jitter is **downward-only**: an entry lives `base * f` for a random
//! `f ∈ (1 - JITTER, 1]`, so a cache's configured `time_to_live` stays an
//! unchanged upper bound on any single entry's life — the staleness window can
//! only shrink, never grow.
//!
//! **Cross-cache caveat.** The startup invariant `user_assignments.ttl ≤
//! role.ttl` (see `config.rs`) requires a `USER_ASSIGNMENTS_CACHE` entry to never
//! outlive the role-cache entry it references. With equal default TTLs, jittering
//! *both* downward could let a co-warmed UA entry outlive its role entry by up to
//! the jitter fraction. So `ROLE_CACHE` is deliberately **excluded** from jitter
//! (held at its exact base TTL); a UA entry then lives `≤ ua_base ≤ role_base =`
//! the role entry's life and never outlives it. Every other cache is jittered.
//!
//! Jittered caches keep their `.time_to_live(base)` builder call as that upper
//! bound and add `.expire_after(JitteredTtl::new(base, ..))`; moka evicts at the
//! *earliest* of the two, which is always the jittered value (it never exceeds
//! `base`).

use std::time::{Duration, Instant};

use moka::Expiry;

/// Default downward jitter fraction (10%): entries live 90–100% of their base TTL.
pub(crate) const DEFAULT_TTL_JITTER: f64 = 0.10;

/// Per-entry [`Expiry`] that returns a base TTL shortened by a small random
/// fraction, desynchronizing cross-replica expiry. Reused by every catalog cache
/// (it ignores key and value).
#[derive(Debug, Clone, Copy)]
pub(crate) struct JitteredTtl {
    base: Duration,
    /// Fraction in `[0.0, 1.0)`: sampled lifetime is `base * (1 - rand[0, jitter))`.
    jitter: f64,
}

impl JitteredTtl {
    pub(crate) fn new(base: Duration, jitter: f64) -> Self {
        debug_assert!(
            (0.0..1.0).contains(&jitter),
            "jitter must be in [0.0, 1.0) to stay downward-only"
        );
        Self { base, jitter }
    }

    /// [`JitteredTtl`] over `base` using the [`DEFAULT_TTL_JITTER`] fraction —
    /// the form every catalog cache uses.
    pub(crate) fn with_default_jitter(base: Duration) -> Self {
        Self::new(base, DEFAULT_TTL_JITTER)
    }

    /// Sample a jittered lifetime in `(base * (1 - jitter), base]`. Random per
    /// call (NOT derived from the key): deterministic jitter would give the same
    /// key the same lifetime on every replica and re-synchronize the expiry,
    /// defeating the purpose.
    fn sample(&self) -> Duration {
        // `fastrand::f64()` ∈ [0, 1) ⇒ factor ∈ (1 - jitter, 1] ⇒ result ≤ base.
        let factor = 1.0 - fastrand::f64() * self.jitter;
        self.base.mul_f64(factor)
    }
}

impl<K, V> Expiry<K, V> for JitteredTtl {
    fn expire_after_create(&self, _key: &K, _value: &V, _created_at: Instant) -> Option<Duration> {
        Some(self.sample())
    }

    /// MUST be implemented alongside `expire_after_create`. `time_to_live` resets
    /// the clock on every write, and the cache writers *replace* entries
    /// (version-gated re-inserts). The default `expire_after_update` returns
    /// `duration_until_expiry` ("no change"), so without this a hot,
    /// frequently-rewritten key would pin its first jittered TTL and never
    /// re-jitter. Sample fresh on every update too.
    fn expire_after_update(
        &self,
        _key: &K,
        _value: &V,
        _updated_at: Instant,
        _duration_until_expiry: Option<Duration>,
    ) -> Option<Duration> {
        Some(self.sample())
    }

    // `expire_after_read`: left at the default (no change) — these TTLs are
    // write-based, not access-based.
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every sample stays within `(base·(1−J), base]`, and across many samples
    /// the value actually varies (jitter is applied, not a constant).
    #[test]
    fn sample_is_in_downward_only_bounds_and_varies() {
        let base = Duration::from_secs(100);
        let jitter = JitteredTtl::new(base, DEFAULT_TTL_JITTER);
        let lower = base.mul_f64(1.0 - DEFAULT_TTL_JITTER); // 90s

        let mut min = base;
        let mut max = Duration::ZERO;
        for _ in 0..1000 {
            let d = jitter.sample();
            assert!(d >= lower, "sample {d:?} below lower bound {lower:?}");
            assert!(
                d <= base,
                "sample {d:?} above base {base:?} (must be downward-only)"
            );
            min = min.min(d);
            max = max.max(d);
        }
        assert!(
            min < max,
            "jitter must produce a spread of values, got constant {min:?}"
        );
    }

    /// Both write-path expiry hooks return a value (never `None`, which would
    /// clear expiry) within the downward-only bounds.
    #[test]
    fn expire_after_create_and_update_return_in_range() {
        let base = Duration::from_mins(2);
        let jitter = JitteredTtl::new(base, DEFAULT_TTL_JITTER);
        let lower = base.mul_f64(1.0 - DEFAULT_TTL_JITTER);
        let now = Instant::now();

        for _ in 0..1000 {
            let on_create =
                Expiry::<(), ()>::expire_after_create(&jitter, &(), &(), now).expect("create");
            let on_update = Expiry::<(), ()>::expire_after_update(&jitter, &(), &(), now, None)
                .expect("update");
            for d in [on_create, on_update] {
                assert!(
                    d >= lower && d <= base,
                    "expiry {d:?} out of [{lower:?}, {base:?}]"
                );
            }
        }
    }
}
