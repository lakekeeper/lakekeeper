use std::{
    sync::LazyLock,
    time::{Duration, Instant},
};

use crate::service::storage::{
    gcs::CachedSTSResponse, ShortTermCredentialsRequest, StorageCredentialBorrowed,
    StorageProfileBorrowed,
};

/// Global cache for STC tokens, indexed by cache key.
/// Note: We implement per-entry TTL by storing expiration in the value.
pub(super) static STC_CACHE: LazyLock<moka::future::Cache<STCCacheKey, STCCacheValue>> =
    LazyLock::new(|| {
        moka::future::Cache::builder()
            .max_capacity(10_000)
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
    pub(super) storage_profile_hash: u64,
    /// Hash of the credentials used to create the STC token
    pub(super) credential_hash: u64,
}

#[derive(Debug, Clone, derive_more::From)]
pub(super) enum ShortTermCredential {
    S3(aws_sdk_sts::types::Credentials),
    Adls(String), // SAS Token
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
        let storage_profile_hash = fxhash::hash64(&storage_profile);
        let credential_hash = fxhash::hash64(&credential);

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
        Some((valid_for_duration / 2).min(Duration::from_secs(3600)))
    }
}
