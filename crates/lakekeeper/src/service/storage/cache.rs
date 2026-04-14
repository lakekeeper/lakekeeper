use std::{
    sync::LazyLock,
    time::{Duration, Instant},
};

use axum_prometheus::metrics;

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

/// Global cache for STC tokens, indexed by cache key.
/// Note: We implement per-entry TTL by storing expiration in the value.
static STC_CACHE: LazyLock<moka::future::Cache<STCCacheKey, STCCacheValue>> = LazyLock::new(|| {
    moka::future::Cache::builder()
        .max_capacity(CONFIG.cache.stc.capacity)
        .initial_capacity(100)
        // Per-entry expiration based on cache_expires_at in the value
        .expire_after(STCCacheExpiration {})
        .build()
});

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

#[derive(Debug, Clone, derive_more::From)]
pub(super) enum ShortTermCredential {
    S3(aws_sdk_sts::types::Credentials),
    Adls {
        sas_token: String,
        expiration: time::OffsetDateTime,
    },
    Gcs(CachedSTSResponse),
}

/// Wrapper for cached STC credentials with their expiration time.
/// We cache credentials until half their lifetime to ensure freshness.
#[derive(Debug, Clone)]
pub(super) struct STCCacheValue {
    pub(super) credentials: ShortTermCredential,
    pub(super) valid_until: Option<Instant>,
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

impl STCCacheValue {
    pub(super) fn new(
        credentials: impl Into<ShortTermCredential>,
        valid_until: Option<Instant>,
    ) -> Self {
        Self {
            credentials: credentials.into(),
            valid_until,
        }
    }
}

#[derive(Debug)]
struct STCCacheExpiration;

impl moka::Expiry<STCCacheKey, STCCacheValue> for STCCacheExpiration {
    /// Returns the duration of the expiration of the value that was just created.
    /// Durations must be positive, so we handle the case where the expiration is in the past.
    fn expire_after_create(
        &self,
        _key: &STCCacheKey,
        value: &STCCacheValue,
        created_at: std::time::Instant,
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

/// Update the cache size metric with the current number of entries
#[inline]
#[allow(clippy::cast_precision_loss)]
fn update_cache_size_metric() {
    let () = &*METRICS_INITIALIZED; // Ensure metrics are described
    metrics::gauge!(METRIC_STC_CACHE_SIZE, "cache_type" => "stc")
        .set(STC_CACHE.entry_count() as f64);
}

pub(super) async fn get_stc_from_cache(key: &STCCacheKey) -> Option<STCCacheValue> {
    let () = &*METRICS_INITIALIZED; // Ensure metrics are described
    let result = STC_CACHE.get(key).await;

    if result.is_some() {
        metrics::counter!(METRIC_STC_CACHE_HITS, "cache_type" => "stc").increment(1);
    } else {
        metrics::counter!(METRIC_STC_CACHE_MISSES, "cache_type" => "stc").increment(1);
    }

    update_cache_size_metric();

    result
}

pub(super) async fn insert_stc_into_cache(key: STCCacheKey, value: STCCacheValue) {
    STC_CACHE.insert(key, value).await;
    update_cache_size_metric();
}
