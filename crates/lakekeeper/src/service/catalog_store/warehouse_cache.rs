use std::{
    sync::{Arc, LazyLock},
    time::Duration,
};

use axum_prometheus::metrics;
use moka::{future::Cache, notification::RemovalCause};

#[cfg(feature = "router")]
use crate::{
    api::{
        management::v1::warehouse::{
            RenameWarehouseRequest, UpdateWarehouseCredentialRequest,
            UpdateWarehouseDeleteProfileRequest, UpdateWarehouseStorageRequest,
        },
        RequestMetadata,
    },
    service::endpoint_hooks::EndpointHook,
    SecretId,
};
use crate::{service::ResolvedWarehouse, ProjectId, WarehouseId, CONFIG};

const METRIC_WAREHOUSE_CACHE_SIZE: &str = "lakekeeper_warehouse_cache_size";
const METRIC_WAREHOUSE_CACHE_HITS: &str = "lakekeeper_warehouse_cache_hits_total";
const METRIC_WAREHOUSE_CACHE_MISSES: &str = "lakekeeper_warehouse_cache_misses_total";

/// Initialize metric descriptions for STC cache metrics
static METRICS_INITIALIZED: LazyLock<()> = LazyLock::new(|| {
    metrics::describe_gauge!(
        METRIC_WAREHOUSE_CACHE_SIZE,
        "Current number of entries in the warehouse cache"
    );
    metrics::describe_counter!(
        METRIC_WAREHOUSE_CACHE_HITS,
        "Total number of warehouse cache hits"
    );
    metrics::describe_counter!(
        METRIC_WAREHOUSE_CACHE_MISSES,
        "Total number of warehouse cache misses"
    );
});

// Main cache: stores warehouses by ID only
static WAREHOUSE_CACHE: LazyLock<Cache<WarehouseId, CachedWarehouse>> = LazyLock::new(|| {
    Cache::builder()
        .max_capacity(CONFIG.cache.warehouse.capacity)
        .initial_capacity(50)
        .time_to_live(Duration::from_secs(30))
        .async_eviction_listener(|key, value: CachedWarehouse, cause| {
            Box::pin(async move {
                // Evictions:
                // - Replaced: only invalidate old-name mapping if the current entry
                //   either does not exist or has a different (project_id, name).
                // - Other causes: primary entry is gone; invalidate mapping.
                let should_invalidate = match cause {
                    RemovalCause::Replaced => {
                        if let Some(curr) = WAREHOUSE_CACHE.get(&*key).await {
                            curr.warehouse.project_id != value.warehouse.project_id
                                || curr.warehouse.name != value.warehouse.name
                        } else {
                            true
                        }
                    }
                    _ => true,
                };
                if should_invalidate {
                    NAME_TO_ID_CACHE
                        .invalidate(&(
                            value.warehouse.project_id.clone(),
                            value.warehouse.name.clone(),
                        ))
                        .await;
                }
            })
        })
        .build()
});

// Secondary index: (project_id, name) → warehouse_id
static NAME_TO_ID_CACHE: LazyLock<Cache<(ProjectId, String), WarehouseId>> = LazyLock::new(|| {
    Cache::builder()
        .max_capacity(CONFIG.cache.warehouse.capacity)
        .initial_capacity(50)
        .build()
});

#[derive(Debug, Clone)]
pub(super) struct CachedWarehouse {
    pub(super) warehouse: Arc<ResolvedWarehouse>,
}

#[allow(dead_code)] // Not required for all features
async fn warehouse_cache_invalidate(warehouse_id: WarehouseId) {
    if CONFIG.cache.warehouse.enabled {
        tracing::debug!("Invalidating warehouse id {warehouse_id} from cache");
        WAREHOUSE_CACHE.invalidate(&warehouse_id).await;
        update_cache_size_metric();
    }
}

pub(super) async fn warehouse_cache_insert(warehouse: Arc<ResolvedWarehouse>) {
    if CONFIG.cache.warehouse.enabled {
        let warehouse_id = warehouse.warehouse_id;
        let project_id = warehouse.project_id.clone();
        let name = warehouse.name.clone();
        let current_entry = WAREHOUSE_CACHE.get(&warehouse_id).await;
        if let Some(existing) = &current_entry {
            let curr = existing.warehouse.updated_at;
            let new_ = warehouse.updated_at;
            if matches!((curr, new_), (Some(curr), Some(new_)) if curr >= new_) {
                tracing::debug!(
                    "Skipping insert of warehouse id {warehouse_id} into cache; existing entry is newer or same"
                );
                // Existing entry is newer or same; skip insert
                return;
            }
        }
        tracing::debug!("Inserting warehouse id {warehouse_id} into cache");
        tokio::join!(
            WAREHOUSE_CACHE.insert(warehouse_id, CachedWarehouse { warehouse }),
            NAME_TO_ID_CACHE.insert((project_id, name), warehouse_id),
        );
        update_cache_size_metric();
    }
}

/// Update the cache size metric with the current number of entries
#[inline]
#[allow(clippy::cast_precision_loss)]
fn update_cache_size_metric() {
    let () = &*METRICS_INITIALIZED; // Ensure metrics are described
    metrics::gauge!(METRIC_WAREHOUSE_CACHE_SIZE, "cache_type" => "warehouse")
        .set(WAREHOUSE_CACHE.entry_count() as f64);
}

pub(super) async fn warehouse_cache_get_by_id(
    warehouse_id: WarehouseId,
) -> Option<Arc<ResolvedWarehouse>> {
    update_cache_size_metric();
    if let Some(value) = WAREHOUSE_CACHE.get(&warehouse_id).await {
        tracing::debug!("Warehouse id {warehouse_id} found in cache");
        metrics::counter!(METRIC_WAREHOUSE_CACHE_HITS, "cache_type" => "warehouse").increment(1);
        Some(value.warehouse.clone())
    } else {
        metrics::counter!(METRIC_WAREHOUSE_CACHE_MISSES, "cache_type" => "warehouse").increment(1);
        None
    }
}

pub(super) async fn warehouse_cache_get_by_name(
    name: &str,
    project_id: &ProjectId,
) -> Option<Arc<ResolvedWarehouse>> {
    update_cache_size_metric();
    let Some(warehouse_id) = NAME_TO_ID_CACHE
        .get(&(project_id.clone(), name.to_string()))
        .await
    else {
        metrics::counter!(METRIC_WAREHOUSE_CACHE_MISSES, "cache_type" => "warehouse").increment(1);
        return None;
    };
    tracing::debug!("Warehouse name {name} found in name-to-id cache");

    if let Some(value) = WAREHOUSE_CACHE.get(&(warehouse_id)).await {
        tracing::debug!("Warehouse id {warehouse_id} found in cache");
        metrics::counter!(METRIC_WAREHOUSE_CACHE_HITS, "cache_type" => "warehouse").increment(1);
        Some(value.warehouse.clone())
    } else {
        metrics::counter!(METRIC_WAREHOUSE_CACHE_MISSES, "cache_type" => "warehouse").increment(1);
        None
    }
}

#[cfg(feature = "router")]
#[derive(Debug, Clone)]
pub(crate) struct WarehouseCacheEndpointHook;

#[cfg(feature = "router")]
impl std::fmt::Display for WarehouseCacheEndpointHook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WarehouseCacheEndpointHook")
    }
}

#[cfg(feature = "router")]
#[async_trait::async_trait]
impl EndpointHook for WarehouseCacheEndpointHook {
    async fn create_warehouse(
        &self,
        warehouse: Arc<ResolvedWarehouse>,
        _request_metadata: Arc<RequestMetadata>,
    ) -> anyhow::Result<()> {
        warehouse_cache_insert(warehouse).await;
        Ok(())
    }

    async fn delete_warehouse(
        &self,
        warehouse_id: WarehouseId,
        _request_metadata: Arc<RequestMetadata>,
    ) -> anyhow::Result<()> {
        // When we invalidate by warehouse_id, the eviction listener will handle
        // removing the entry from NAME_TO_ID_CACHE
        warehouse_cache_invalidate(warehouse_id).await;
        Ok(())
    }

    async fn set_warehouse_protection(
        &self,
        _requested_protected: bool,
        updated_warehouse: Arc<ResolvedWarehouse>,
        _request_metadata: Arc<RequestMetadata>,
    ) -> anyhow::Result<()> {
        warehouse_cache_insert(updated_warehouse).await;
        Ok(())
    }

    async fn rename_warehouse(
        &self,
        _request: Arc<RenameWarehouseRequest>,
        updated_warehouse: Arc<ResolvedWarehouse>,
        _request_metadata: Arc<RequestMetadata>,
    ) -> anyhow::Result<()> {
        warehouse_cache_insert(updated_warehouse).await;
        Ok(())
    }

    async fn update_warehouse_delete_profile(
        &self,
        _request: Arc<UpdateWarehouseDeleteProfileRequest>,
        updated_warehouse: Arc<ResolvedWarehouse>,
        _request_metadata: Arc<RequestMetadata>,
    ) -> anyhow::Result<()> {
        warehouse_cache_insert(updated_warehouse).await;
        Ok(())
    }

    async fn update_warehouse_storage(
        &self,
        _request: Arc<UpdateWarehouseStorageRequest>,
        updated_warehouse: Arc<ResolvedWarehouse>,
        _request_metadata: Arc<RequestMetadata>,
    ) -> anyhow::Result<()> {
        warehouse_cache_insert(updated_warehouse).await;
        Ok(())
    }

    async fn update_warehouse_storage_credential(
        &self,
        _request: Arc<UpdateWarehouseCredentialRequest>,
        _old_secret_id: Option<SecretId>,
        updated_warehouse: Arc<ResolvedWarehouse>,
        _request_metadata: Arc<RequestMetadata>,
    ) -> anyhow::Result<()> {
        warehouse_cache_insert(updated_warehouse).await;
        Ok(())
    }
}
