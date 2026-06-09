use std::{
    sync::LazyLock,
    time::{Duration, Instant},
};

use axum_prometheus::metrics;
use moka::{
    Expiry,
    future::Cache,
    ops::compute::{CompResult, Op},
};

use crate::{
    CONFIG,
    service::{
        cache_metrics::{
            METRIC_CACHE_HITS_TOTAL as METRIC_STC_CACHE_HITS,
            METRIC_CACHE_MISSES_TOTAL as METRIC_STC_CACHE_MISSES,
            METRIC_CACHE_SIZE as METRIC_STC_CACHE_SIZE, METRICS_INITIALIZED,
        },
        storage::{
            ShortTermCredentialsRequest, StorageCredentialBorrowed, StorageProfileBorrowed,
            gcs::CachedSTSResponse,
        },
    },
};

/// Cache key for STC tokens. This uniquely identifies a set of temporary credentials.
/// We hash the full context to ensure complete isolation and avoid missing any relevant fields.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(super) struct STCCacheKey {
    /// Request Hash
    pub(super) request: ShortTermCredentialsRequest,
    /// Hash of the storage profile
    storage_profile_hash: u64,
    /// Hash of the credentials used to create the STC token
    credential_hash: u64,
}

impl STCCacheKey {
    pub(super) fn new(
        request: ShortTermCredentialsRequest,
        storage_profile: StorageProfileBorrowed<'_>,
        credential: Option<StorageCredentialBorrowed<'_>>,
    ) -> Self {
        use std::hash::{Hash, Hasher};

        let mut hasher = xxhash_rust::xxh3::Xxh3::new();
        storage_profile.hash(&mut hasher);
        let storage_profile_hash = hasher.finish();

        let mut hasher = xxhash_rust::xxh3::Xxh3::new();
        credential.hash(&mut hasher);
        let credential_hash = hasher.finish();

        Self {
            request,
            storage_profile_hash,
            credential_hash,
        }
    }
}

/// A cached value `V` (a provider's short-term credential) plus the instant until
/// which it should be served. Caching the concrete `V` (rather than a shared
/// credential enum) lets each provider's read-through return its own credential
/// type by construction — no runtime variant check, no unreachable arms.
#[derive(Debug, Clone)]
pub(super) struct CachedStc<V> {
    pub(super) value: V,
    pub(super) valid_until: Option<Instant>,
}

impl<V> CachedStc<V> {
    pub(super) fn new(value: V, valid_until: Option<Instant>) -> Self {
        Self { value, valid_until }
    }
}

/// Per-entry expiry: cache until half the credential's remaining lifetime, capped
/// at 1 hour. Generic over the credential type `V` so one impl serves every cache.
#[derive(Debug)]
struct StcExpiry;

impl<V> Expiry<STCCacheKey, CachedStc<V>> for StcExpiry {
    /// Durations must be positive, so an unknown or already-elapsed validity
    /// yields a zero duration (immediate expiry).
    fn expire_after_create(
        &self,
        _key: &STCCacheKey,
        value: &CachedStc<V>,
        created_at: Instant,
    ) -> Option<Duration> {
        let Some(valid_until) = value.valid_until else {
            return Some(Duration::from_secs(0));
        };
        let Some(valid_for_duration) = valid_until.checked_duration_since(created_at) else {
            return Some(Duration::from_secs(0));
        };
        // Cache until half the validity duration, capped at 1 hour.
        Some((valid_for_duration / 2).min(Duration::from_hours(1)))
    }
}

fn build_stc_cache<V: Clone + Send + Sync + 'static>() -> Cache<STCCacheKey, CachedStc<V>> {
    Cache::builder()
        .max_capacity(CONFIG.cache.stc.capacity)
        .initial_capacity(100)
        // Per-entry expiry based on the credential's validity (see `StcExpiry`).
        .expire_after(StcExpiry)
        .build()
}

// Per-provider STC caches. Each stores a concrete credential type, so the
// read-through hands back the right credential without a runtime variant check.
// `max_capacity` is a ceiling, not a reservation — idle caches hold ~no entries —
// so three caches cost no more than one did in practice.
pub(super) static S3_STC_CACHE: LazyLock<
    Cache<STCCacheKey, CachedStc<aws_sdk_sts::types::Credentials>>,
> = LazyLock::new(build_stc_cache::<aws_sdk_sts::types::Credentials>);
pub(super) static ADLS_STC_CACHE: LazyLock<
    Cache<STCCacheKey, CachedStc<(String, time::OffsetDateTime)>>,
> = LazyLock::new(build_stc_cache::<(String, time::OffsetDateTime)>);
pub(super) static GCS_STC_CACHE: LazyLock<Cache<STCCacheKey, CachedStc<CachedSTSResponse>>> =
    LazyLock::new(build_stc_cache::<CachedSTSResponse>);

/// Update the cache size metric with the combined entry count of all STC caches.
#[inline]
#[allow(clippy::cast_precision_loss)]
fn update_cache_size_metric() {
    let () = &*METRICS_INITIALIZED; // Ensure metrics are described
    let total =
        S3_STC_CACHE.entry_count() + ADLS_STC_CACHE.entry_count() + GCS_STC_CACHE.entry_count();
    metrics::gauge!(METRIC_STC_CACHE_SIZE, "cache_type" => "stc").set(total as f64);
}

/// Single-flight read-through for a short-term-credentials cache.
///
/// A miss here is the **most expensive** in the system — a rate-limited STS/SAS
/// network round-trip — so concurrent identical requests are **coalesced** onto
/// one fetch per [`STCCacheKey`]: moka serializes the per-key compute, and later
/// callers observe the just-fetched entry instead of each calling the cloud
/// provider. STC has no not-found case, so the loader returns value-or-error;
/// errors are **never cached**, so a transient STS failure does not poison the
/// entry. The `enabled` flag and hit/miss metrics are preserved; when caching is
/// disabled the loader runs directly. The loader error is returned by value (no
/// `Arc`-sharing) and may borrow (`and_try_compute_with` imposes no `'static`
/// bound), so providers need not clone their profile.
///
/// Generic over the cached value type `V` (each provider's concrete credential),
/// so each provider caches and returns its own type — no shared enum, no runtime
/// variant check.
pub(super) async fn get_or_load_stc<V, Fut, E>(
    cache: &Cache<STCCacheKey, CachedStc<V>>,
    key: STCCacheKey,
    load: Fut,
) -> Result<V, E>
where
    V: Clone + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<CachedStc<V>, E>> + Send,
    E: Send + Sync + 'static,
{
    if !CONFIG.cache.stc.enabled {
        return Ok(load.await?.value);
    }

    // Fast path records a hit/miss. Under contention a coalesced waiter records a
    // miss here but then hits `Op::Nop` below without fetching, so the miss counter
    // is *cache misses*, not *STS fetches* (the two diverge under a herd).
    let () = &*METRICS_INITIALIZED;
    if let Some(cached) = cache.get(&key).await {
        metrics::counter!(METRIC_STC_CACHE_HITS, "cache_type" => "stc").increment(1);
        update_cache_size_metric();
        return Ok(cached.value);
    }
    metrics::counter!(METRIC_STC_CACHE_MISSES, "cache_type" => "stc").increment(1);

    let outcome = cache
        .entry(key)
        .and_try_compute_with(|maybe_entry| async move {
            if maybe_entry.is_some() {
                // Fetched by another caller while we waited on the key lock.
                return Ok::<_, E>(Op::Nop);
            }
            Ok(Op::Put(load.await?))
        })
        .await?;
    update_cache_size_metric();

    Ok(match outcome {
        CompResult::Inserted(entry)
        | CompResult::ReplacedWith(entry)
        | CompResult::Unchanged(entry) => entry.into_value().value,
        // Unreachable: the closure returns `Op::Nop` only when an entry already
        // exists (→ `Unchanged`) or `Op::Put` (→ `Inserted`/`ReplacedWith`). STC has
        // no not-found case, so the result always carries a value.
        CompResult::StillNone(_) | CompResult::Removed(_) => {
            unreachable!("STC compute yields a value on every reachable path")
        }
    })
}

#[cfg(test)]
mod tests {
    use std::{
        str::FromStr,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    };

    use lakekeeper_io::Location;

    use super::*;
    use crate::{
        WarehouseId,
        service::{
            TableId, TabularId,
            storage::{ShortTermCredentialsRequest, StoragePermissions},
        },
    };

    fn test_key(tag: &str) -> STCCacheKey {
        let request = ShortTermCredentialsRequest {
            table_location: Location::from_str(&format!("s3://bucket/{tag}")).unwrap(),
            storage_permissions: StoragePermissions::Read,
            warehouse_id: WarehouseId::new_random(),
            tabular_id: TabularId::Table(TableId::new_random()),
        };
        STCCacheKey {
            request,
            storage_profile_hash: 0,
            credential_hash: 0,
        }
    }

    /// `get_or_load_stc` must coalesce concurrent identical misses into ONE fetch —
    /// a miss is a rate-limited STS/SAS round-trip, the most expensive in the system.
    #[tokio::test]
    async fn get_or_load_stc_coalesces_concurrent_misses() {
        let key = test_key("coalesce");
        let loads = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..32 {
            let loads = Arc::clone(&loads);
            let key = key.clone();
            handles.push(tokio::spawn(async move {
                get_or_load_stc(&ADLS_STC_CACHE, key, async {
                    loads.fetch_add(1, Ordering::SeqCst);
                    // Widen the load window so all callers queue on the key lock
                    // before the first fetch completes.
                    for _ in 0..100 {
                        tokio::task::yield_now().await;
                    }
                    let valid_until = Instant::now().checked_add(Duration::from_hours(1));
                    Ok::<_, std::convert::Infallible>(CachedStc::new(
                        ("sas-token".to_string(), time::OffsetDateTime::now_utc()),
                        valid_until,
                    ))
                })
                .await
            }));
        }

        for h in handles {
            h.await.unwrap().unwrap();
        }

        assert_eq!(
            loads.load(Ordering::SeqCst),
            1,
            "concurrent STC misses must coalesce to a single fetch"
        );
    }
}
