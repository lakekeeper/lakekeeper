use std::{
    sync::{Arc, LazyLock},
    time::Duration,
};

use axum_prometheus::metrics;
use moka::{future::Cache, notification::RemovalCause};
use unicase::UniCase;

use crate::{CONFIG, ProjectId, WarehouseId, service::ResolvedWarehouse};
#[cfg(feature = "router")]
use crate::{
    SecretId,
    api::{
        RequestMetadata,
        management::v1::warehouse::{
            RenameWarehouseRequest, UpdateWarehouseCredentialRequest,
            UpdateWarehouseDeleteProfileRequest, UpdateWarehouseStorageRequest,
        },
    },
    service::endpoint_hooks::EndpointHook,
};

const METRIC_WAREHOUSE_CACHE_SIZE: &str = "lakekeeper_warehouse_cache_size";
const METRIC_WAREHOUSE_CACHE_HITS: &str = "lakekeeper_warehouse_cache_hits_total";
const METRIC_WAREHOUSE_CACHE_MISSES: &str = "lakekeeper_warehouse_cache_misses_total";

/// Initialize metric descriptions for Warehouse cache metrics
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
pub(crate) static WAREHOUSE_CACHE: LazyLock<Cache<WarehouseId, CachedWarehouse>> =
    LazyLock::new(|| {
        Cache::builder()
            .max_capacity(CONFIG.cache.warehouse.capacity)
            .initial_capacity(50)
            .time_to_live(Duration::from_secs(
                CONFIG.cache.warehouse.time_to_live_secs,
            ))
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
                                UniCase::new(value.warehouse.name.clone()),
                            ))
                            .await;
                    }
                })
            })
            .build()
    });

// Secondary index: (project_id, name) → warehouse_id
// Uses UniCase for case-insensitive warehouse name lookups
static NAME_TO_ID_CACHE: LazyLock<Cache<(ProjectId, UniCase<String>), WarehouseId>> =
    LazyLock::new(|| {
        Cache::builder()
            .max_capacity(CONFIG.cache.warehouse.capacity)
            .initial_capacity(50)
            .build()
    });

#[derive(Debug, Clone)]
pub(crate) struct CachedWarehouse {
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
            let current_version = existing.warehouse.version;
            let new_version = warehouse.version;
            match new_version.cmp(&current_version) {
                std::cmp::Ordering::Less => {
                    tracing::debug!(
                        "Skipping insert of warehouse id {warehouse_id} into cache; existing version {current_version} is newer than new version {new_version}"
                    );
                    // Existing entry is newer; skip insert
                    return;
                }
                std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => {
                    // New entry is newer; proceed with insert.
                    // Also insert equal versions to avoid expiration
                }
            }
        }
        tracing::debug!("Inserting warehouse id {warehouse_id} into cache");
        tokio::join!(
            WAREHOUSE_CACHE.insert(warehouse_id, CachedWarehouse { warehouse }),
            NAME_TO_ID_CACHE.insert((project_id, UniCase::new(name)), warehouse_id),
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
        .get(&(project_id.clone(), UniCase::new(name.to_string())))
        .await
    else {
        metrics::counter!(METRIC_WAREHOUSE_CACHE_MISSES, "cache_type" => "warehouse").increment(1);
        return None;
    };
    tracing::debug!("Warehouse name {name} resolved in name-to-id cache to id {warehouse_id}");

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
        println!(
            "Updating delete profile in cache hook for warehouse id {}",
            updated_warehouse.warehouse_id
        );
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

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::{
        api::management::v1::warehouse::TabularDeleteProfile,
        service::{catalog_store::warehouse::WarehouseStatus, storage::MemoryProfile},
    };

    /// Helper function to create a test warehouse
    fn test_warehouse(
        warehouse_id: WarehouseId,
        name: String,
        project_id: ProjectId,
        updated_at: Option<chrono::DateTime<chrono::Utc>>,
        version: i64,
    ) -> Arc<ResolvedWarehouse> {
        Arc::new(ResolvedWarehouse {
            warehouse_id,
            name,
            project_id,
            storage_profile: MemoryProfile::default().into(),
            storage_secret_id: None,
            status: WarehouseStatus::Active,
            tabular_delete_profile: TabularDeleteProfile::Hard {},
            protected: false,
            updated_at,
            version: version.into(),
        })
    }

    #[tokio::test]
    async fn test_warehouse_cache_insert_and_get_by_id() {
        let warehouse_id = WarehouseId::new_random();
        let project_id = ProjectId::new_random();
        let name = "test-warehouse".to_string();
        let warehouse = test_warehouse(
            warehouse_id,
            name.clone(),
            project_id.clone(),
            Some(Utc::now()),
            0,
        );

        // Insert warehouse into cache
        warehouse_cache_insert(warehouse.clone()).await;

        // Retrieve by ID
        let cached = warehouse_cache_get_by_id(warehouse_id).await;
        assert!(cached.is_some());
        let cached = cached.unwrap();
        assert_eq!(cached.warehouse_id, warehouse_id);
        assert_eq!(cached.name, name);
        assert_eq!(cached.project_id, project_id);
    }

    #[tokio::test]
    async fn test_warehouse_cache_get_by_name() {
        let warehouse_id = WarehouseId::new_random();
        let project_id = ProjectId::new_random();
        let name = "test-warehouse-by-name".to_string();
        let warehouse = test_warehouse(
            warehouse_id,
            name.clone(),
            project_id.clone(),
            Some(Utc::now()),
            0,
        );

        // Insert warehouse into cache
        warehouse_cache_insert(warehouse.clone()).await;

        // Retrieve by name
        let cached = warehouse_cache_get_by_name(&name, &project_id).await;
        assert!(cached.is_some());
        let cached = cached.unwrap();
        assert_eq!(cached.warehouse_id, warehouse_id);
        assert_eq!(cached.name, name);
        assert_eq!(cached.project_id, project_id);
    }

    #[tokio::test]
    async fn test_warehouse_cache_get_by_name_different_project() {
        let warehouse_id = WarehouseId::new_random();
        let project_id = ProjectId::new_random();
        let different_project_id = ProjectId::new_random();
        let name = "test-warehouse-project".to_string();
        let warehouse = test_warehouse(
            warehouse_id,
            name.clone(),
            project_id.clone(),
            Some(Utc::now()),
            0,
        );

        // Insert warehouse into cache
        warehouse_cache_insert(warehouse.clone()).await;

        // Try to retrieve with same name but different project_id
        let cached = warehouse_cache_get_by_name(&name, &different_project_id).await;
        assert!(cached.is_none());
    }

    #[tokio::test]
    async fn test_warehouse_cache_invalidate() {
        let warehouse_id = WarehouseId::new_random();
        let project_id = ProjectId::new_random();
        let name = "test-warehouse-invalidate".to_string();
        let warehouse = test_warehouse(
            warehouse_id,
            name.clone(),
            project_id.clone(),
            Some(Utc::now()),
            0,
        );

        // Insert warehouse into cache
        warehouse_cache_insert(warehouse.clone()).await;

        // Verify it's cached
        let cached = warehouse_cache_get_by_id(warehouse_id).await;
        assert!(cached.is_some());

        // Invalidate
        warehouse_cache_invalidate(warehouse_id).await;

        // Verify it's no longer cached
        let cached = warehouse_cache_get_by_id(warehouse_id).await;
        assert!(cached.is_none());

        // Verify name-to-id cache is also invalidated
        let cached_by_name = warehouse_cache_get_by_name(&name, &project_id).await;
        assert!(cached_by_name.is_none());
    }

    #[tokio::test]
    async fn test_warehouse_cache_miss() {
        let warehouse_id = WarehouseId::new_random();
        let project_id = ProjectId::new_random();
        let name = "nonexistent-warehouse".to_string();

        // Try to get a warehouse that was never cached
        let cached = warehouse_cache_get_by_id(warehouse_id).await;
        assert!(cached.is_none());

        let cached_by_name = warehouse_cache_get_by_name(&name, &project_id).await;
        assert!(cached_by_name.is_none());
    }

    #[tokio::test]
    async fn test_warehouse_cache_insert_newer_timestamp() {
        let warehouse_id = WarehouseId::new_random();
        let project_id = ProjectId::new_random();
        let name = "test-warehouse-timestamp".to_string();

        let old_time = Utc::now();
        let new_time = old_time + chrono::Duration::seconds(10);

        // Insert older version
        let old_warehouse = test_warehouse(
            warehouse_id,
            name.clone(),
            project_id.clone(),
            Some(old_time),
            0,
        );
        warehouse_cache_insert(old_warehouse.clone()).await;

        // Verify older version is cached
        let cached = warehouse_cache_get_by_id(warehouse_id).await;
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().updated_at, Some(old_time));

        // Insert newer version
        let new_warehouse = test_warehouse(
            warehouse_id,
            name.clone(),
            project_id.clone(),
            Some(new_time),
            1,
        );
        warehouse_cache_insert(new_warehouse.clone()).await;

        // Verify newer version replaced the old one
        let cached = warehouse_cache_get_by_id(warehouse_id).await;
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().updated_at, Some(new_time));
    }

    #[tokio::test]
    async fn test_warehouse_cache_insert_older_timestamp_ignored() {
        let warehouse_id = WarehouseId::new_random();
        let project_id = ProjectId::new_random();
        let name = "test-warehouse-old-timestamp".to_string();

        let new_time = Utc::now();
        let old_time = new_time - chrono::Duration::seconds(10);

        // Insert newer version first
        let new_warehouse = test_warehouse(
            warehouse_id,
            name.clone(),
            project_id.clone(),
            Some(new_time),
            1,
        );
        warehouse_cache_insert(new_warehouse.clone()).await;

        // Verify newer version is cached
        let cached = warehouse_cache_get_by_id(warehouse_id).await;
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().updated_at, Some(new_time));

        // Try to insert older version
        let old_warehouse = test_warehouse(
            warehouse_id,
            name.clone(),
            project_id.clone(),
            Some(old_time),
            0,
        );
        warehouse_cache_insert(old_warehouse.clone()).await;

        // Verify newer version is still cached (old one was ignored)
        let cached = warehouse_cache_get_by_id(warehouse_id).await;
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().updated_at, Some(new_time));
    }

    #[tokio::test]
    async fn test_warehouse_cache_insert_same_timestamp_ignored() {
        let warehouse_id = WarehouseId::new_random();
        let project_id = ProjectId::new_random();
        let name = "test-warehouse-same-timestamp".to_string();

        let timestamp = Utc::now();

        // Insert first version
        let warehouse1 = test_warehouse(
            warehouse_id,
            name.clone(),
            project_id.clone(),
            Some(timestamp),
            0,
        );
        warehouse_cache_insert(warehouse1.clone()).await;

        // Try to insert another warehouse with same timestamp
        let warehouse2 = test_warehouse(
            warehouse_id,
            name.clone(),
            project_id.clone(),
            Some(timestamp),
            0,
        );
        warehouse_cache_insert(warehouse2.clone()).await;

        // Should still be in cache
        let cached = warehouse_cache_get_by_id(warehouse_id).await;
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().updated_at, Some(timestamp));
    }

    #[tokio::test]
    async fn test_warehouse_cache_rename_updates_name_to_id_cache() {
        let warehouse_id = WarehouseId::new_random();
        let project_id = ProjectId::new_random();
        let old_name = "old-warehouse-name".to_string();
        let new_name = "new-warehouse-name".to_string();

        let timestamp1 = Utc::now();
        let timestamp2 = timestamp1 + chrono::Duration::seconds(1);

        // Insert warehouse with old name
        let old_warehouse = test_warehouse(
            warehouse_id,
            old_name.clone(),
            project_id.clone(),
            Some(timestamp1),
            0,
        );
        warehouse_cache_insert(old_warehouse.clone()).await;

        // Verify old name works
        let cached = warehouse_cache_get_by_name(&old_name, &project_id).await;
        assert!(cached.is_some());

        // Rename warehouse (insert with new name and newer timestamp)
        let renamed_warehouse = test_warehouse(
            warehouse_id,
            new_name.clone(),
            project_id.clone(),
            Some(timestamp2),
            1,
        );
        warehouse_cache_insert(renamed_warehouse.clone()).await;

        // Give the eviction listener time to run
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Verify new name works
        let cached = warehouse_cache_get_by_name(&new_name, &project_id).await;
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().name, new_name);

        // Old name should no longer work (eviction listener should have invalidated it)
        let cached = warehouse_cache_get_by_name(&old_name, &project_id).await;
        assert!(cached.is_none());
    }

    #[tokio::test]
    async fn test_warehouse_cache_insert_none_timestamp() {
        let warehouse_id = WarehouseId::new_random();
        let project_id = ProjectId::new_random();
        let name = "test-warehouse-none-timestamp".to_string();

        // Insert warehouse without timestamp
        let warehouse = test_warehouse(warehouse_id, name.clone(), project_id.clone(), None, 0);
        warehouse_cache_insert(warehouse.clone()).await;

        // Verify it's cached
        let cached = warehouse_cache_get_by_id(warehouse_id).await;
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().updated_at, None);

        // Insert another with timestamp
        let new_time = Utc::now();
        let warehouse_with_time = test_warehouse(
            warehouse_id,
            name.clone(),
            project_id.clone(),
            Some(new_time),
            1,
        );
        warehouse_cache_insert(warehouse_with_time.clone()).await;

        // Should be replaced
        let cached = warehouse_cache_get_by_id(warehouse_id).await;
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().updated_at, Some(new_time));
    }

    #[tokio::test]
    async fn test_warehouse_cache_multiple_warehouses() {
        let project_id = ProjectId::new_random();

        // Create multiple warehouses
        let warehouse1_id = WarehouseId::new_random();
        let warehouse1 = test_warehouse(
            warehouse1_id,
            "warehouse1".to_string(),
            project_id.clone(),
            Some(Utc::now()),
            0,
        );

        let warehouse2_id = WarehouseId::new_random();
        let warehouse2 = test_warehouse(
            warehouse2_id,
            "warehouse2".to_string(),
            project_id.clone(),
            Some(Utc::now()),
            0,
        );

        let warehouse3_id = WarehouseId::new_random();
        let warehouse3 = test_warehouse(
            warehouse3_id,
            "warehouse3".to_string(),
            project_id.clone(),
            Some(Utc::now()),
            0,
        );

        // Insert all warehouses
        warehouse_cache_insert(warehouse1.clone()).await;
        warehouse_cache_insert(warehouse2.clone()).await;
        warehouse_cache_insert(warehouse3.clone()).await;

        // Verify all are cached by ID
        assert!(warehouse_cache_get_by_id(warehouse1_id).await.is_some());
        assert!(warehouse_cache_get_by_id(warehouse2_id).await.is_some());
        assert!(warehouse_cache_get_by_id(warehouse3_id).await.is_some());

        // Verify all are cached by name
        assert!(
            warehouse_cache_get_by_name("warehouse1", &project_id)
                .await
                .is_some()
        );
        assert!(
            warehouse_cache_get_by_name("warehouse2", &project_id)
                .await
                .is_some()
        );
        assert!(
            warehouse_cache_get_by_name("warehouse3", &project_id)
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn test_warehouse_cache_same_name_different_projects() {
        let project_id1 = ProjectId::new_random();
        let project_id2 = ProjectId::new_random();
        let name = "same-warehouse-name".to_string();

        let warehouse1_id = WarehouseId::new_random();
        let warehouse1 = test_warehouse(
            warehouse1_id,
            name.clone(),
            project_id1.clone(),
            Some(Utc::now()),
            0,
        );

        let warehouse2_id = WarehouseId::new_random();
        let warehouse2 = test_warehouse(
            warehouse2_id,
            name.clone(),
            project_id2.clone(),
            Some(Utc::now()),
            0,
        );

        // Insert both warehouses
        warehouse_cache_insert(warehouse1.clone()).await;
        warehouse_cache_insert(warehouse2.clone()).await;

        // Verify both are cached by name with their respective project IDs
        let cached1 = warehouse_cache_get_by_name(&name, &project_id1).await;
        assert!(cached1.is_some());
        assert_eq!(cached1.unwrap().warehouse_id, warehouse1_id);

        let cached2 = warehouse_cache_get_by_name(&name, &project_id2).await;
        assert!(cached2.is_some());
        assert_eq!(cached2.unwrap().warehouse_id, warehouse2_id);
    }

    #[tokio::test]
    async fn test_warehouse_cache_case_insensitive_lookup() {
        let warehouse_id = WarehouseId::new_random();
        let project_id = ProjectId::new_random();
        let name = "Test-Warehouse".to_string();
        let warehouse = test_warehouse(
            warehouse_id,
            name.clone(),
            project_id.clone(),
            Some(Utc::now()),
            0,
        );

        // Insert warehouse with mixed-case name
        warehouse_cache_insert(warehouse.clone()).await;

        // Verify we can retrieve it with different case variations
        let cached_lower = warehouse_cache_get_by_name("test-warehouse", &project_id).await;
        assert!(cached_lower.is_some());
        assert_eq!(cached_lower.unwrap().warehouse_id, warehouse_id);

        let cached_upper = warehouse_cache_get_by_name("TEST-WAREHOUSE", &project_id).await;
        assert!(cached_upper.is_some());
        assert_eq!(cached_upper.unwrap().warehouse_id, warehouse_id);

        let cached_mixed = warehouse_cache_get_by_name("TeSt-WaReHoUsE", &project_id).await;
        assert!(cached_mixed.is_some());
        assert_eq!(cached_mixed.unwrap().warehouse_id, warehouse_id);

        let cached_exact = warehouse_cache_get_by_name(&name, &project_id).await;
        assert!(cached_exact.is_some());
        assert_eq!(cached_exact.unwrap().warehouse_id, warehouse_id);
    }
}
