use std::{
    sync::LazyLock,
    time::{Duration, Instant},
};

use crate::{service::storage::s3::S3UrlStyleDetectionMode, WarehouseId};

pub(super) static WAREHOUSE_S3_URL_STYLE_CACHE: LazyLock<
    moka::future::Cache<WarehouseId, S3UrlStyleDetectionMode>,
> = LazyLock::new(|| {
    moka::future::Cache::builder()
        .max_capacity(10000)
        .expire_after(Expiry)
        .build()
});

struct Expiry;

impl<K, V> moka::Expiry<K, V> for Expiry {
    fn expire_after_create(&self, _key: &K, _value: &V, _created_at: Instant) -> Option<Duration> {
        Some(Duration::from_secs(300))
    }
}
