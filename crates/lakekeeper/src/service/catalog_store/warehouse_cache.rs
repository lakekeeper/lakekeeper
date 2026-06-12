use std::{
    sync::{Arc, LazyLock},
    time::Duration,
};

use axum_prometheus::metrics;
use moka::{
    future::Cache,
    notification::RemovalCause,
    ops::compute::{CompResult, Op},
};
use unicase::UniCase;

use super::secondary_index_get_or_load;
#[cfg(feature = "router")]
use crate::service::events::{self, EventListener};
use crate::{
    CONFIG, WarehouseId,
    service::{
        ArcProjectId, ResolvedWarehouse,
        cache_metrics::{
            METRIC_CACHE_HITS_TOTAL as METRIC_WAREHOUSE_CACHE_HITS,
            METRIC_CACHE_MISSES_TOTAL as METRIC_WAREHOUSE_CACHE_MISSES,
            METRIC_CACHE_SIZE as METRIC_WAREHOUSE_CACHE_SIZE, METRICS_INITIALIZED,
        },
        cache_ttl::JitteredTtl,
    },
};

// Main cache: stores warehouses by ID only
pub static WAREHOUSE_CACHE: LazyLock<Cache<WarehouseId, CachedWarehouse>> = LazyLock::new(|| {
    Cache::builder()
        .max_capacity(CONFIG.cache.warehouse.capacity)
        .initial_capacity(50)
        .time_to_live(Duration::from_secs(
            CONFIG.cache.warehouse.time_to_live_secs,
        ))
        .expire_after(JitteredTtl::with_default_jitter(Duration::from_secs(
            CONFIG.cache.warehouse.time_to_live_secs,
        )))
        .async_eviction_listener(|key, value: CachedWarehouse, cause| {
            Box::pin(async move {
                // On Replaced: invalidate the old secondary index mapping immediately,
                // then spawn a task to re-insert the new mapping (avoids re-entrant
                // WAREHOUSE_CACHE.get() calls which can deadlock).
                // On all other causes (expired, explicit): always invalidate.
                match cause {
                    RemovalCause::Replaced => {
                        let key = *key;
                        // Immediately invalidate the old (project_id, name) → warehouse_id mapping
                        NAME_TO_ID_CACHE
                            .invalidate(&(
                                value.warehouse.project_id.clone(),
                                UniCase::new(value.warehouse.name.clone()),
                            ))
                            .await;

                        // Spawn task to add the new mapping (avoids re-entrant WAREHOUSE_CACHE.get)
                        tokio::spawn(async move {
                            if let Some(curr) = WAREHOUSE_CACHE.get(&key).await {
                                NAME_TO_ID_CACHE
                                    .insert(
                                        (
                                            curr.warehouse.project_id.clone(),
                                            UniCase::new(curr.warehouse.name.clone()),
                                        ),
                                        key,
                                    )
                                    .await;
                            }
                        });
                    }
                    _ => {
                        NAME_TO_ID_CACHE
                            .invalidate(&(
                                value.warehouse.project_id.clone(),
                                UniCase::new(value.warehouse.name.clone()),
                            ))
                            .await;
                    }
                }
            })
        })
        .build()
});

// Secondary index: (project_id, name) → warehouse_id
// Uses UniCase for case-insensitive warehouse name lookups
static NAME_TO_ID_CACHE: LazyLock<Cache<(ArcProjectId, UniCase<String>), WarehouseId>> =
    LazyLock::new(|| {
        Cache::builder()
            .max_capacity(CONFIG.cache.warehouse.capacity)
            .initial_capacity(50)
            .time_to_live(Duration::from_secs(
                CONFIG.cache.warehouse.time_to_live_secs,
            ))
            .expire_after(JitteredTtl::with_default_jitter(Duration::from_secs(
                CONFIG.cache.warehouse.time_to_live_secs,
            )))
            .build()
    });

#[derive(Debug, Clone)]
pub struct CachedWarehouse {
    pub warehouse: Arc<ResolvedWarehouse>,
}

#[allow(dead_code)] // Not required for all features
async fn warehouse_cache_invalidate(warehouse_id: WarehouseId) {
    if CONFIG.cache.warehouse.enabled {
        tracing::debug!("Invalidating warehouse id {warehouse_id} from cache");
        // Remove via the loader's per-key compute lock (`Op::Remove`), not a bare
        // `invalidate()`: the version-gate fences stale *updates* but not *deletes*
        // (a delete leaves no entry to compare), so a bare invalidate racing an
        // in-flight by-id load lets the loader re-`Put` the deleted warehouse until
        // TTL. `Op::Remove` shares the loader's lock and still fires the eviction
        // listener (Explicit), preserving the NAME_TO_ID_CACHE cascade. Closes the
        // by-id path only; the by-name prime path keeps the residual — see
        // `secondary_index_get_or_load`.
        WAREHOUSE_CACHE
            .entry(warehouse_id)
            .and_compute_with(|_| async { Op::Remove })
            .await;
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

/// Single-flight read-through for the warehouse cache.
///
/// Coalesces concurrent misses for the same `warehouse_id`: moka serializes the
/// per-key compute, so the loader runs once and later callers observe the
/// just-inserted entry. Returns `Option` — a non-existent warehouse is **not**
/// negative-cached.
///
/// The version-gate is preserved without reworking the writer: before inserting
/// the loaded value we re-read the current entry and skip if a concurrent
/// `warehouse_cache_insert` already cached a newer/equal version. (moka's compute
/// lock does not serialize against the plain `insert()` that writer uses, so the
/// re-check is required — it carries the same sub-`await` residual the existing
/// get-then-insert gate already has, i.e. no regression.) The `(project, name) →
/// id` index is populated alongside, mirroring `warehouse_cache_insert`. The
/// `enabled` flag and hit/miss metrics are preserved; the loader error is
/// returned by value (no `Arc`-sharing).
pub(super) async fn warehouse_cache_get_or_load<Fut, E>(
    warehouse_id: WarehouseId,
    load: Fut,
) -> Result<Option<Arc<ResolvedWarehouse>>, E>
where
    Fut: std::future::Future<Output = Result<Option<Arc<ResolvedWarehouse>>, E>> + Send,
    E: Send + Sync + 'static,
{
    if !CONFIG.cache.warehouse.enabled {
        return load.await;
    }

    // Fast path records a hit/miss. Note: under contention each coalesced waiter
    // records a miss here but then hits `Op::Nop` below without loading, so the
    // miss counter is *cache misses*, not *DB loads* (the two diverge under a herd).
    if let Some(warehouse) = warehouse_cache_get_by_id(warehouse_id).await {
        return Ok(Some(warehouse));
    }

    let outcome = WAREHOUSE_CACHE
        .entry(warehouse_id)
        .and_try_compute_with(|maybe_entry| async move {
            if maybe_entry.is_some() {
                // Populated by another caller while we waited on the key lock.
                return Ok::<_, E>(Op::Nop);
            }
            let Some(warehouse) = load.await? else {
                // Missing warehouse — never negative-cached. Coalescing therefore
                // applies only to a found warehouse; concurrent lookups of a
                // missing one each re-run the loader (rare, no worse than before).
                return Ok(Op::Nop);
            };
            // Preserve the version-gate against a writer that cached a newer
            // version via plain `insert()` during our load. Skips on `>=` (newer
            // *or equal*), mirroring `warehouse_cache_insert`'s reluctance to churn
            // an equal entry. (The role helper skips only on strictly-newer `<`;
            // both are safe — re-putting an equal value is harmless either way.)
            if let Some(current) = WAREHOUSE_CACHE.get(&warehouse_id).await
                && current.warehouse.version >= warehouse.version
            {
                return Ok(Op::Nop);
            }
            NAME_TO_ID_CACHE
                .insert(
                    (
                        warehouse.project_id.clone(),
                        UniCase::new(warehouse.name.clone()),
                    ),
                    warehouse_id,
                )
                .await;
            Ok(Op::Put(CachedWarehouse { warehouse }))
        })
        .await?;
    update_cache_size_metric();

    Ok(match outcome {
        CompResult::Inserted(entry)
        | CompResult::ReplacedWith(entry)
        | CompResult::Unchanged(entry) => Some(entry.into_value().warehouse),
        // `StillNone` means either the loader returned `None` (genuine not-found,
        // never negative-cached) or the version-gate fired because a concurrent
        // writer cached a newer version during our load. moka derives the
        // `Op::Nop` result from the closure's entry snapshot, so it cannot surface
        // that concurrent `insert()` (a different lock domain) — a final raw read
        // does, returning the newer value if present and `None` otherwise.
        // `Removed` is unreachable (the closure only returns `Nop`/`Put`).
        CompResult::StillNone(_) | CompResult::Removed(_) => WAREHOUSE_CACHE
            .get(&warehouse_id)
            .await
            .map(|c| c.warehouse),
    })
}

/// Update the cache size metric with the current number of entries
#[inline]
#[allow(clippy::cast_precision_loss)]
fn update_cache_size_metric() {
    let () = &*METRICS_INITIALIZED; // Ensure metrics are described
    metrics::gauge!(METRIC_WAREHOUSE_CACHE_SIZE, "cache_type" => "warehouse")
        .set(WAREHOUSE_CACHE.entry_count() as f64);
    metrics::gauge!(METRIC_WAREHOUSE_CACHE_SIZE, "cache_type" => "warehouse_name_to_id")
        .set(NAME_TO_ID_CACHE.entry_count() as f64);
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
    project_id: &ArcProjectId,
) -> Option<Arc<ResolvedWarehouse>> {
    update_cache_size_metric();
    let name_key = (project_id.clone(), UniCase::new(name.to_string()));
    let Some(warehouse_id) = NAME_TO_ID_CACHE.get(&name_key).await else {
        metrics::counter!(METRIC_WAREHOUSE_CACHE_MISSES, "cache_type" => "warehouse_name_to_id")
            .increment(1);
        return None;
    };
    metrics::counter!(METRIC_WAREHOUSE_CACHE_HITS, "cache_type" => "warehouse_name_to_id")
        .increment(1);
    tracing::debug!("Warehouse name {name} resolved in name-to-id cache to id {warehouse_id}");

    if let Some(value) = WAREHOUSE_CACHE.get(&(warehouse_id)).await {
        tracing::debug!("Warehouse id {warehouse_id} found in cache");
        metrics::counter!(METRIC_WAREHOUSE_CACHE_HITS, "cache_type" => "warehouse").increment(1);
        Some(value.warehouse.clone())
    } else {
        tracing::debug!(
            "Warehouse id {warehouse_id} not found in cache, invalidating stale name mapping for {name}"
        );
        NAME_TO_ID_CACHE.invalidate(&name_key).await;
        metrics::counter!(METRIC_WAREHOUSE_CACHE_MISSES, "cache_type" => "warehouse").increment(1);
        None
    }
}

/// Single-flight read-through for the `(project, name) → id` resolution.
///
/// Coalesces concurrent **by-name** misses (clients usually address warehouses by
/// name, so this is the hot cold-start path): the by-name DB query runs once per
/// `(project, name)`, the loaded warehouse primes the by-id cache + name index, and
/// every coalesced caller resolves the full warehouse by id. Returns the resolved
/// `WarehouseId`, or `None` if it does not exist (**not** negative-cached). Thin
/// wrapper over [`secondary_index_get_or_load`](super::secondary_index_get_or_load).
pub(super) async fn warehouse_name_to_id_get_or_load<Fut, E>(
    project_id: ArcProjectId,
    name: &str,
    load: Fut,
) -> Result<Option<WarehouseId>, E>
where
    Fut: std::future::Future<Output = Result<Option<Arc<ResolvedWarehouse>>, E>> + Send,
    E: Send + Sync + 'static,
{
    secondary_index_get_or_load(
        CONFIG.cache.warehouse.enabled,
        &NAME_TO_ID_CACHE,
        (project_id, UniCase::new(name.to_string())),
        load,
        |warehouse: &Arc<ResolvedWarehouse>| warehouse.warehouse_id,
        warehouse_cache_insert,
    )
    .await
}

#[cfg(feature = "router")]
#[derive(Debug, Clone)]
pub struct WarehouseCacheEventListener;

#[cfg(feature = "router")]
impl std::fmt::Display for WarehouseCacheEventListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WarehouseCacheEventListener")
    }
}

#[cfg(feature = "router")]
#[async_trait::async_trait]
impl EventListener for WarehouseCacheEventListener {
    async fn warehouse_created(&self, event: events::CreateWarehouseEvent) -> anyhow::Result<()> {
        let events::CreateWarehouseEvent {
            warehouse,
            request_metadata: _request_metadata,
        } = event;
        warehouse_cache_insert(warehouse).await;
        Ok(())
    }

    async fn warehouse_deleted(&self, event: events::DeleteWarehouseEvent) -> anyhow::Result<()> {
        let events::DeleteWarehouseEvent {
            warehouse,
            request_metadata: _request_metadata,
        } = event;
        // When we invalidate by warehouse_id, the eviction listener will handle
        // removing the entry from NAME_TO_ID_CACHE
        warehouse_cache_invalidate(warehouse.warehouse_id).await;
        Ok(())
    }

    async fn warehouse_protection_set(
        &self,
        event: events::SetWarehouseProtectionEvent,
    ) -> anyhow::Result<()> {
        let events::SetWarehouseProtectionEvent {
            requested_protected: _requested_protected,
            updated_warehouse,
            request_metadata: _request_metadata,
        } = event;
        warehouse_cache_insert(updated_warehouse).await;
        Ok(())
    }

    async fn warehouse_managed_by_set(
        &self,
        event: events::SetWarehouseManagedByEvent,
    ) -> anyhow::Result<()> {
        let events::SetWarehouseManagedByEvent {
            requested_managed_by: _requested_managed_by,
            updated_warehouse,
            request_metadata: _request_metadata,
        } = event;
        warehouse_cache_insert(updated_warehouse).await;
        Ok(())
    }

    async fn warehouse_renamed(&self, event: events::RenameWarehouseEvent) -> anyhow::Result<()> {
        let events::RenameWarehouseEvent {
            request: _request,
            updated_warehouse,
            request_metadata: _request_metadata,
        } = event;
        warehouse_cache_insert(updated_warehouse).await;
        Ok(())
    }

    async fn warehouse_delete_profile_updated(
        &self,
        event: events::UpdateWarehouseDeleteProfileEvent,
    ) -> anyhow::Result<()> {
        let events::UpdateWarehouseDeleteProfileEvent {
            request: _request,
            updated_warehouse,
            request_metadata: _request_metadata,
        } = event;
        warehouse_cache_insert(updated_warehouse).await;
        Ok(())
    }

    async fn warehouse_format_version_policy_updated(
        &self,
        event: events::UpdateWarehouseFormatVersionPolicyEvent,
    ) -> anyhow::Result<()> {
        let events::UpdateWarehouseFormatVersionPolicyEvent {
            request: _request,
            updated_warehouse,
            request_metadata: _request_metadata,
        } = event;
        warehouse_cache_insert(updated_warehouse).await;
        Ok(())
    }

    async fn warehouse_storage_updated(
        &self,
        event: events::UpdateWarehouseStorageEvent,
    ) -> anyhow::Result<()> {
        let events::UpdateWarehouseStorageEvent {
            request: _request,
            updated_warehouse,
            request_metadata: _request_metadata,
        } = event;
        warehouse_cache_insert(updated_warehouse).await;
        Ok(())
    }

    async fn warehouse_storage_credential_updated(
        &self,
        event: events::UpdateWarehouseStorageCredentialEvent,
    ) -> anyhow::Result<()> {
        let events::UpdateWarehouseStorageCredentialEvent {
            request: _request,
            old_secret_id: _old_secret_id,
            updated_warehouse,
            request_metadata: _request_metadata,
        } = event;
        warehouse_cache_insert(updated_warehouse).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::{
        ProjectId,
        api::management::v1::warehouse::TabularDeleteProfile,
        service::{catalog_store::warehouse::WarehouseStatus, storage::MemoryProfile},
    };

    /// Helper function to create a test warehouse
    fn test_warehouse(
        warehouse_id: WarehouseId,
        name: String,
        project_id: ArcProjectId,
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
            managed_by: crate::service::ManagedBy::SelfManaged,
            allowed_format_versions: crate::service::AllowedFormatVersions::default(),
            default_format_version: None,
            updated_at,
            version: version.into(),
        })
    }

    #[tokio::test]
    async fn test_warehouse_cache_insert_and_get_by_id() {
        let warehouse_id = WarehouseId::new_random();
        let project_id = Arc::new(ProjectId::new_random());
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
        let project_id = Arc::new(ProjectId::new_random());
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
        let project_id = Arc::new(ProjectId::new_random());
        let different_project_id = Arc::new(ProjectId::new_random());
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
        let project_id = Arc::new(ProjectId::new_random());
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
    async fn test_name_to_id_cache_has_ttl_matching_primary() {
        let primary_ttl = WAREHOUSE_CACHE.policy().time_to_live();
        let secondary_ttl = NAME_TO_ID_CACHE.policy().time_to_live();
        assert_eq!(
            primary_ttl, secondary_ttl,
            "NAME_TO_ID_CACHE TTL must match WAREHOUSE_CACHE TTL"
        );
        assert!(
            secondary_ttl.is_some(),
            "NAME_TO_ID_CACHE must have a TTL configured"
        );
    }

    #[tokio::test]
    async fn test_warehouse_cache_miss() {
        let warehouse_id = WarehouseId::new_random();
        let project_id = Arc::new(ProjectId::new_random());
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
        let project_id = Arc::new(ProjectId::new_random());
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
        let project_id = Arc::new(ProjectId::new_random());
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
        let project_id = Arc::new(ProjectId::new_random());
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
        let project_id = Arc::new(ProjectId::new_random());
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
        let project_id = Arc::new(ProjectId::new_random());
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
        let project_id = Arc::new(ProjectId::new_random());

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
        let project_id1 = Arc::new(ProjectId::new_random());
        let project_id2 = Arc::new(ProjectId::new_random());
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

    /// `warehouse_cache_get_or_load` must coalesce concurrent misses for the same
    /// id into ONE loader run, with every caller observing the cached entry.
    #[tokio::test]
    async fn warehouse_get_or_load_coalesces_concurrent_misses() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let warehouse_id = WarehouseId::new_random();
        let project_id = Arc::new(ProjectId::new_random());
        warehouse_cache_invalidate(warehouse_id).await;

        let loads = Arc::new(AtomicUsize::new(0));
        let warehouse = test_warehouse(
            warehouse_id,
            "wh-coalesce".to_string(),
            project_id,
            Some(Utc::now()),
            0,
        );

        let mut handles = Vec::new();
        for _ in 0..32 {
            let loads = Arc::clone(&loads);
            let warehouse = warehouse.clone();
            handles.push(tokio::spawn(async move {
                warehouse_cache_get_or_load(warehouse_id, async move {
                    loads.fetch_add(1, Ordering::SeqCst);
                    // Widen the load window so all callers queue on the key lock
                    // before the first load completes.
                    for _ in 0..100 {
                        tokio::task::yield_now().await;
                    }
                    Ok::<_, std::convert::Infallible>(Some(warehouse))
                })
                .await
            }));
        }

        let mut results = Vec::new();
        for h in handles {
            results.push(h.await.unwrap().unwrap().expect("warehouse exists"));
        }

        assert_eq!(
            loads.load(Ordering::SeqCst),
            1,
            "concurrent misses must coalesce to a single loader run"
        );
        for r in &results[1..] {
            assert_eq!(r.warehouse_id, warehouse_id);
        }

        warehouse_cache_invalidate(warehouse_id).await;
    }

    /// The in-closure version-gate must not let a slow loader overwrite a newer
    /// value cached concurrently. We model the race by having the loader itself
    /// insert a newer version (as a concurrent `warehouse_cache_insert` would)
    /// before returning a stale older one — the helper must keep the newer entry
    /// and return it, never the stale load.
    #[tokio::test]
    async fn warehouse_get_or_load_version_gate_keeps_newer_concurrent_insert() {
        let warehouse_id = WarehouseId::new_random();
        let project_id = Arc::new(ProjectId::new_random());
        warehouse_cache_invalidate(warehouse_id).await;

        let name = "wh-version-gate".to_string();
        let newer = test_warehouse(
            warehouse_id,
            name.clone(),
            project_id.clone(),
            Some(Utc::now()),
            5,
        );
        let older = test_warehouse(warehouse_id, name, project_id, Some(Utc::now()), 3);

        let returned = warehouse_cache_get_or_load(warehouse_id, {
            let newer = newer.clone();
            let older = older.clone();
            async move {
                // A concurrent writer caches a newer version while we "load".
                warehouse_cache_insert(newer).await;
                Ok::<_, std::convert::Infallible>(Some(older))
            }
        })
        .await
        .unwrap()
        .expect("warehouse exists");

        assert_eq!(
            *returned.version, 5,
            "helper must return the newer concurrently-cached value, not the stale load"
        );
        assert_eq!(
            *warehouse_cache_get_by_id(warehouse_id)
                .await
                .unwrap()
                .version,
            5,
            "stale older load must be version-gated out of the cache"
        );

        warehouse_cache_invalidate(warehouse_id).await;
    }

    #[tokio::test]
    async fn test_warehouse_cache_case_insensitive_lookup() {
        let warehouse_id = WarehouseId::new_random();
        let project_id = Arc::new(ProjectId::new_random());
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

    /// `warehouse_name_to_id_get_or_load` must coalesce concurrent by-name misses
    /// into ONE loader run, with every caller resolving the same id.
    #[tokio::test]
    async fn warehouse_name_to_id_get_or_load_coalesces_concurrent_misses() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let warehouse_id = WarehouseId::new_random();
        let project_id = Arc::new(ProjectId::new_random());
        let name = "wh-name-coalesce".to_string();
        warehouse_cache_invalidate(warehouse_id).await;

        let loads = Arc::new(AtomicUsize::new(0));
        let warehouse = test_warehouse(
            warehouse_id,
            name.clone(),
            project_id.clone(),
            Some(Utc::now()),
            0,
        );

        let mut handles = Vec::new();
        for _ in 0..32 {
            let loads = Arc::clone(&loads);
            let warehouse = warehouse.clone();
            let project_id = project_id.clone();
            let name = name.clone();
            handles.push(tokio::spawn(async move {
                warehouse_name_to_id_get_or_load(project_id, &name, async move {
                    loads.fetch_add(1, Ordering::SeqCst);
                    for _ in 0..100 {
                        tokio::task::yield_now().await;
                    }
                    Ok::<_, std::convert::Infallible>(Some(warehouse))
                })
                .await
            }));
        }

        let mut results = Vec::new();
        for h in handles {
            results.push(h.await.unwrap().unwrap().expect("warehouse exists"));
        }

        assert_eq!(
            loads.load(Ordering::SeqCst),
            1,
            "concurrent by-name misses must coalesce to a single loader run"
        );
        for id in &results {
            assert_eq!(*id, warehouse_id);
        }

        warehouse_cache_invalidate(warehouse_id).await;
    }

    /// A `None` result (warehouse not found) must NOT be negative-cached or
    /// coalesced: concurrent missing-lookups each re-run the loader, and the entry
    /// stays absent so a later real insert is visible immediately.
    #[tokio::test]
    async fn warehouse_get_or_load_does_not_negative_cache() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let warehouse_id = WarehouseId::new_random();
        warehouse_cache_invalidate(warehouse_id).await;

        let loads = Arc::new(AtomicUsize::new(0));
        let calls = 4;

        let mut handles = Vec::new();
        for _ in 0..calls {
            let loads = Arc::clone(&loads);
            handles.push(tokio::spawn(async move {
                warehouse_cache_get_or_load(warehouse_id, async move {
                    loads.fetch_add(1, Ordering::SeqCst);
                    // Widen the window so callers contend; a missing entry must
                    // still not coalesce (nothing is cached to coalesce onto).
                    for _ in 0..100 {
                        tokio::task::yield_now().await;
                    }
                    Ok::<_, std::convert::Infallible>(None)
                })
                .await
            }));
        }

        for h in handles {
            assert!(
                h.await.unwrap().unwrap().is_none(),
                "a missing warehouse resolves to None"
            );
        }

        assert_eq!(
            loads.load(Ordering::SeqCst),
            calls,
            "a missing warehouse is not negative-cached, so each concurrent caller \
             re-runs the loader"
        );
        assert!(
            warehouse_cache_get_by_id(warehouse_id).await.is_none(),
            "None must not be cached"
        );

        warehouse_cache_invalidate(warehouse_id).await;
    }
}
