use std::{
    sync::{Arc, LazyLock},
    time::Duration,
};

use async_trait::async_trait;
use axum_prometheus::metrics;
use http::StatusCode;
use iceberg_ext::catalog::rest::ErrorModel;
use moka::future::Cache;
use serde::{Deserialize, Serialize};

use crate::{
    api::Result,
    service::{health::HealthExt, storage::StorageCredential},
    CONFIG,
};

const METRIC_SECRETS_CACHE_SIZE: &str = "lakekeeper_secrets_cache_size";
const METRIC_SECRETS_CACHE_HITS: &str = "lakekeeper_secrets_cache_hits_total";
const METRIC_SECRETS_CACHE_MISSES: &str = "lakekeeper_secrets_cache_misses_total";

/// Initialize metric descriptions for STC cache metrics
static METRICS_INITIALIZED: LazyLock<()> = LazyLock::new(|| {
    metrics::describe_gauge!(
        METRIC_SECRETS_CACHE_SIZE,
        "Current number of entries in the secrets cache"
    );
    metrics::describe_counter!(
        METRIC_SECRETS_CACHE_HITS,
        "Total number of secrets cache hits"
    );
    metrics::describe_counter!(
        METRIC_SECRETS_CACHE_MISSES,
        "Total number of secrets cache misses"
    );
});

pub(crate) static SECRETS_CACHE: LazyLock<Cache<SecretId, CachedSecret>> = LazyLock::new(|| {
    Cache::builder()
        .max_capacity(CONFIG.cache.secrets.capacity)
        .initial_capacity(50)
        .time_to_live(Duration::from_secs(CONFIG.cache.secrets.time_to_live_secs))
        .build()
});

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CachedSecret {
    StorageCredential(Secret<Arc<StorageCredential>>),
}

/// Interface for Handling Secrets.
#[async_trait]
pub trait SecretStore
where
    Self: Send + Sync + 'static + HealthExt + Clone + std::fmt::Debug,
{
    /// Get the secret for a given warehouse.
    async fn require_storage_secret_by_id(
        &self,
        secret_id: SecretId,
    ) -> Result<Secret<Arc<StorageCredential>>> {
        // Check cache first
        if let Some(cached) = secrets_cache_get(secret_id).await {
            let CachedSecret::StorageCredential(secret) = cached;
            return Ok(secret);
        }

        let Some(secret) = self
            .get_secret_by_id_impl::<StorageCredential>(secret_id)
            .await?
        else {
            return Err(ErrorModel::builder()
                .code(StatusCode::NOT_FOUND.into())
                .message("Secret not found".to_string())
                .r#type("SecretNotFound".to_string())
                .stack(vec![format!("secret_id: {secret_id}")])
                .build()
                .into());
        };

        // Convert to Arc and insert into cache
        let arc_secret = secret.map(Arc::new);
        let cached = CachedSecret::StorageCredential(arc_secret.clone());
        secrets_cache_insert(secret_id, cached).await;

        Ok(arc_secret)
    }

    /// Create a new secret
    async fn create_storage_secret(&self, secret: StorageCredential) -> Result<SecretId> {
        let secret_id = self.create_secret_impl(secret.clone()).await?;

        // Fetch the created secret to get full metadata (created_at, updated_at)
        // and insert into cache
        if let Some(created_secret) = self
            .get_secret_by_id_impl::<StorageCredential>(secret_id)
            .await?
        {
            let arc_secret = created_secret.map(Arc::new);
            let cached = CachedSecret::StorageCredential(arc_secret);
            secrets_cache_insert(secret_id, cached).await;
        }

        Ok(secret_id)
    }

    /// Delete a secret
    async fn delete_secret(&self, secret_id: &SecretId) -> Result<()> {
        self.delete_secret_impl(secret_id).await?;
        secrets_cache_invalidate(*secret_id).await;
        Ok(())
    }

    /// Get the secret for a given warehouse.
    async fn get_secret_by_id_impl<S: SecretInStorage>(
        &self,
        secret_id: SecretId,
    ) -> Result<Option<Secret<S>>>;

    /// Create a new secret
    async fn create_secret_impl<S: SecretInStorage>(&self, secret: S) -> Result<SecretId>;

    /// Delete a secret
    async fn delete_secret_impl(&self, secret_id: &SecretId) -> Result<()>;
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "sqlx", derive(sqlx::Type))]
#[cfg_attr(feature = "sqlx", sqlx(transparent))]
#[serde(transparent)]
// Is UUID here too strict?
pub struct SecretId(uuid::Uuid);

impl SecretId {
    #[must_use]
    #[inline]
    pub fn into_uuid(&self) -> uuid::Uuid {
        self.0
    }

    #[must_use]
    #[inline]
    pub fn as_uuid(&self) -> &uuid::Uuid {
        &self.0
    }
}

impl From<uuid::Uuid> for SecretId {
    fn from(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl From<SecretId> for uuid::Uuid {
    fn from(ident: SecretId) -> Self {
        ident.0
    }
}

impl std::fmt::Display for SecretId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Secret<T> {
    pub secret_id: SecretId,
    pub secret: T,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl<T> Secret<T> {
    #[must_use]
    pub fn map<U, F: FnOnce(T) -> U>(self, f: F) -> Secret<U> {
        Secret {
            secret_id: self.secret_id,
            secret: f(self.secret),
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}

// Prohibits us to store unwanted types in the storage.
pub trait SecretInStorage:
    Send + Sync + Serialize + for<'de> Deserialize<'de> + std::fmt::Debug
{
}

/// Update the cache size metric with the current number of entries
#[inline]
#[allow(clippy::cast_precision_loss)]
fn update_cache_size_metric() {
    let () = &*METRICS_INITIALIZED; // Ensure metrics are described
    metrics::gauge!(METRIC_SECRETS_CACHE_SIZE, "cache_type" => "secrets")
        .set(SECRETS_CACHE.entry_count() as f64);
}

async fn secrets_cache_invalidate(secret_id: SecretId) {
    if CONFIG.cache.secrets.enabled {
        tracing::debug!("Invalidating secret id {secret_id} from cache");
        SECRETS_CACHE.invalidate(&secret_id).await;
        update_cache_size_metric();
    }
}

async fn secrets_cache_insert(secret_id: SecretId, secret: CachedSecret) {
    if CONFIG.cache.secrets.enabled {
        tracing::debug!("Inserting secret id {secret_id} into cache");
        SECRETS_CACHE.insert(secret_id, secret).await;
        update_cache_size_metric();
    }
}

async fn secrets_cache_get(secret_id: SecretId) -> Option<CachedSecret> {
    if !CONFIG.cache.secrets.enabled {
        return None;
    }

    update_cache_size_metric();
    let cached = SECRETS_CACHE.get(&secret_id).await;

    if cached.is_some() {
        tracing::trace!("Secret id {secret_id} found in cache");
        metrics::counter!(METRIC_SECRETS_CACHE_HITS, "cache_type" => "secrets").increment(1);
    } else {
        tracing::debug!("Secret id {secret_id} not found in cache");
        metrics::counter!(METRIC_SECRETS_CACHE_MISSES, "cache_type" => "secrets").increment(1);
    }

    cached
}
