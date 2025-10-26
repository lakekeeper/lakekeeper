use std::{
    sync::{Arc, LazyLock},
    time::Duration,
};

use axum_prometheus::metrics;
use moka::future::Cache;

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
        .async_eviction_listener(|_key, value: CachedWarehouse, _cause| {
            Box::pin(async move {
                // When evicted, also remove from name index
                NAME_TO_ID_CACHE
                    .invalidate(&(
                        value.warehouse.project_id.clone(),
                        value.warehouse.name.clone(),
                    ))
                    .await;
            })
        })
        .build()
});

// Secondary index: name → warehouse_id
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

// pub(super) async fn warehouse_cache_invalidate(warehouse_id: WarehouseId) {
//     WAREHOUSE_CACHE.invalidate(&warehouse_id).await;
// }

pub(super) async fn warehouse_cache_insert(warehouse: Arc<ResolvedWarehouse>) {
    let warehouse_id = warehouse.warehouse_id;
    let project_id = warehouse.project_id.clone();
    let name = warehouse.name.clone();
    tokio::join!(
        WAREHOUSE_CACHE.insert(warehouse_id, CachedWarehouse { warehouse }),
        NAME_TO_ID_CACHE.insert((project_id, name), warehouse_id),
    );
    update_cache_size_metric();
}

/// Update the cache size metric with the current number of entries
#[inline]
#[allow(clippy::cast_precision_loss)]
fn update_cache_size_metric() {
    let () = &*METRICS_INITIALIZED; // Ensure metrics are described
    metrics::gauge!(METRIC_WAREHOUSE_CACHE_SIZE, "cache_type" => "warehouse")
        .set(WAREHOUSE_CACHE.weighted_size() as f64);
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
