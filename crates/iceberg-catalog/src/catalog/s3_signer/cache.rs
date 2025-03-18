use std::{
    cell::LazyCell,
    time::{Duration, Instant},
};

use crate::{service::storage::s3::S3UrlStyleDetectionMode, WarehouseIdent};

#[allow(clippy::declare_interior_mutable_const)]
pub(super) const WAREHOUSE_S3_URL_STYLE_CACHE: LazyCell<
    moka::future::Cache<WarehouseIdent, S3UrlStyleDetectionMode>,
> = LazyCell::new(|| {
    moka::future::Cache::builder()
        .max_capacity(10000)
        .expire_after(Expiry)
        .build()
});

pub struct Expiry;

impl<K, V> moka::Expiry<K, V> for Expiry {
    fn expire_after_create(&self, _key: &K, _value: &V, _created_at: Instant) -> Option<Duration> {
        Some(Duration::from_secs(300))
    }
}
