use std::{sync::LazyLock, time::Duration};

use axum_prometheus::metrics;
use iceberg::NamespaceIdent;
use moka::{future::Cache, notification::RemovalCause};
use unicase::UniCase;

#[cfg(feature = "router")]
use crate::service::events::{self, EventListener};
use crate::{
    CONFIG, WarehouseId,
    service::{NamespaceId, NamespaceWithParent, catalog_store::namespace::NamespaceHierarchy},
};

const METRIC_NAMESPACE_CACHE_SIZE: &str = "lakekeeper_namespace_cache_size";
const METRIC_NAMESPACE_CACHE_HITS: &str = "lakekeeper_namespace_cache_hits_total";
const METRIC_NAMESPACE_CACHE_MISSES: &str = "lakekeeper_namespace_cache_misses_total";

/// Initialize metric descriptions for namespace cache metrics
static METRICS_INITIALIZED: LazyLock<()> = LazyLock::new(|| {
    metrics::describe_gauge!(
        METRIC_NAMESPACE_CACHE_SIZE,
        "Current number of entries in the namespace cache"
    );
    metrics::describe_counter!(
        METRIC_NAMESPACE_CACHE_HITS,
        "Total number of namespace cache hits"
    );
    metrics::describe_counter!(
        METRIC_NAMESPACE_CACHE_MISSES,
        "Total number of namespace cache misses"
    );
});

// Main cache: stores individual namespaces by ID
pub(crate) static NAMESPACE_CACHE: LazyLock<Cache<NamespaceId, NamespaceWithParent>> =
    LazyLock::new(|| {
        Cache::builder()
            .max_capacity(CONFIG.cache.namespace.capacity)
            .initial_capacity(50)
            .time_to_live(Duration::from_secs(
                CONFIG.cache.namespace.time_to_live_secs,
            ))
            .async_eviction_listener(|key, value: NamespaceWithParent, cause| {
                Box::pin(async move {
                    // Evictions:
                    // - Replaced: only invalidate old-name mapping if the current entry
                    //   either does not exist or has a different (warehouse_id, namespace_ident).
                    // - Other causes: primary entry is gone; invalidate mapping.
                    let should_invalidate = match cause {
                        RemovalCause::Replaced => {
                            if let Some(curr) = NAMESPACE_CACHE.get(&*key).await {
                                curr.namespace.warehouse_id != value.namespace.warehouse_id
                                    || curr.namespace.namespace_ident
                                        != value.namespace.namespace_ident
                            } else {
                                true
                            }
                        }
                        _ => true,
                    };
                    if should_invalidate {
                        IDENT_TO_ID_CACHE
                            .invalidate(&(
                                value.namespace.warehouse_id,
                                namespace_ident_to_cache_key(&value.namespace.namespace_ident),
                            ))
                            .await;
                    }
                })
            })
            .build()
    });

// WarehouseId, Case Insensitive NamespaceIdent
type NamespaceCacheKey = (WarehouseId, Vec<UniCase<String>>);

// Secondary index: (warehouse_id, namespace_ident) → namespace_id
// Uses Vec<UniCase<String>> for case-insensitive namespace identifier lookups
// Each component of the namespace path is stored as UniCase to handle dots in names correctly
pub(crate) static IDENT_TO_ID_CACHE: LazyLock<Cache<NamespaceCacheKey, NamespaceId>> =
    LazyLock::new(|| {
        Cache::builder()
            .max_capacity(CONFIG.cache.namespace.capacity)
            .initial_capacity(50)
            .build()
    });

#[allow(dead_code)] // Not required for all features
async fn namespace_cache_invalidate(namespace_id: NamespaceId) {
    if CONFIG.cache.namespace.enabled {
        tracing::debug!("Invalidating namespace id {namespace_id} from cache");
        NAMESPACE_CACHE.invalidate(&namespace_id).await;
        update_cache_size_metric();
    }
}

#[allow(dead_code)] // Only required for listeners which are behind a feature flag
pub(super) async fn namespace_cache_insert(namespace: NamespaceWithParent) {
    if CONFIG.cache.namespace.enabled {
        let namespace_id = namespace.namespace.namespace_id;
        let warehouse_id = namespace.namespace.warehouse_id;

        let current_entry = NAMESPACE_CACHE.get(&namespace_id).await;
        if let Some(existing) = &current_entry {
            let current_version = existing.namespace.version;
            let new_version = namespace.namespace.version;
            match new_version.cmp(&current_version) {
                std::cmp::Ordering::Less => {
                    tracing::debug!(
                        "Skipping insert of namespace id {namespace_id} into cache; existing version {current_version} is newer than new version {new_version}"
                    );
                    return;
                }
                std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => {
                    // New entry is newer; proceed with insert.
                    // Also insert equal versions to avoid expiration
                }
            }
        }

        tracing::debug!("Inserting namespace id {namespace_id} into cache");
        let cache_key = namespace_ident_to_cache_key(&namespace.namespace.namespace_ident);
        tokio::join!(
            NAMESPACE_CACHE.insert(namespace_id, namespace.clone()),
            IDENT_TO_ID_CACHE.insert((warehouse_id, cache_key), namespace_id),
        );

        update_cache_size_metric();
    }
}

pub(super) async fn namespace_cache_insert_multiple(
    namespaces: impl IntoIterator<Item = NamespaceWithParent>,
) {
    let futures = namespaces
        .into_iter()
        .map(namespace_cache_insert)
        .collect::<Vec<_>>();

    futures::future::join_all(futures).await;
}

/// Update the cache size metric with the current number of entries
#[inline]
#[allow(clippy::cast_precision_loss)]
fn update_cache_size_metric() {
    let () = &*METRICS_INITIALIZED; // Ensure metrics are described
    metrics::gauge!(METRIC_NAMESPACE_CACHE_SIZE, "cache_type" => "namespace")
        .set(NAMESPACE_CACHE.entry_count() as f64);
}

/// Get a namespace by ID, reconstructing the hierarchy from cached parents.
pub(super) async fn namespace_cache_get_by_id(
    namespace_id: NamespaceId,
) -> Option<NamespaceHierarchy> {
    update_cache_size_metric();
    let cached = NAMESPACE_CACHE.get(&namespace_id).await?;

    // Reconstruct hierarchy by collecting parents
    if let Some(hierarchy) = build_hierarchy_from_cache(&cached).await {
        tracing::debug!("Namespace id {namespace_id} found in cache with valid parent versions");
        metrics::counter!(METRIC_NAMESPACE_CACHE_HITS, "cache_type" => "namespace").increment(1);
        Some(hierarchy)
    } else {
        tracing::debug!(
            "Failed to build complete hierarchy for namespace id {namespace_id} from cache"
        );
        metrics::counter!(METRIC_NAMESPACE_CACHE_MISSES, "cache_type" => "namespace").increment(1);
        None
    }
}

/// Get a namespace by identifier, reconstructing the hierarchy from cached parents.
pub(super) async fn namespace_cache_get_by_ident(
    namespace_ident: &NamespaceIdent,
    warehouse_id: WarehouseId,
) -> Option<NamespaceHierarchy> {
    update_cache_size_metric();
    let cache_key = namespace_ident_to_cache_key(namespace_ident);
    let namespace_id = IDENT_TO_ID_CACHE.get(&(warehouse_id, cache_key)).await?;

    tracing::debug!("Namespace ident {namespace_ident} found in ident-to-id cache");
    namespace_cache_get_by_id(namespace_id).await
}

/// Build a `NamespaceHierarchy` by collecting parents from the cache.
/// Uses `parent_id` for efficient lookups and validates parent idents and versions match expectations.
async fn build_hierarchy_from_cache(namespace: &NamespaceWithParent) -> Option<NamespaceHierarchy> {
    let mut parents = Vec::new();
    let mut current_namespace = namespace.clone();

    // Walk up the hierarchy using parent_id
    while let Some((parent_id, expected_parent_version)) = current_namespace.parent {
        let parent_cached = NAMESPACE_CACHE.get(&parent_id).await?;

        // Verify parent version matches expected
        if parent_cached.namespace.version < expected_parent_version {
            tracing::debug!(
                "Parent version mismatch for namespace {}: expected version {expected_parent_version}, got {}. Skipping cache use.",
                current_namespace.namespace_ident(),
                parent_cached.namespace.version
            );
            namespace_cache_invalidate(namespace.namespace_id()).await;
            return None;
        }

        // Verify parent ident matches expected (shortened namespace)
        if !is_parent_ident(
            current_namespace.namespace_ident(),
            parent_cached.namespace_ident(),
        ) {
            tracing::debug!(
                "Detected parent ident mismatch for namespace {}: Parent namespace has name `{}`, which is not the parent. Invalidating Cache.",
                current_namespace.namespace_ident(),
                parent_cached.namespace_ident()
            );
            namespace_cache_invalidate(namespace.namespace_id()).await;
            return None;
        }

        parents.push(parent_cached.clone());
        current_namespace = parent_cached;
    }

    Some(NamespaceHierarchy {
        namespace: namespace.clone(),
        parents,
    })
}

/// Convert a `NamespaceIdent` to a Vec<`UniCase`<String>> for case-insensitive comparison.
/// This uses the inner Vec<String> to avoid issues with dots in namespace names.
pub(crate) fn namespace_ident_to_cache_key(ident: &NamespaceIdent) -> Vec<UniCase<String>> {
    ident
        .clone()
        .inner()
        .into_iter()
        .map(UniCase::new)
        .collect()
}

fn is_parent_ident(child_ident: &NamespaceIdent, found_parent_ident: &NamespaceIdent) -> bool {
    let child_ident_unicase = child_ident
        .as_ref()
        .iter()
        .map(UniCase::new)
        .collect::<Vec<_>>();
    let found_parent_ident_unicase = found_parent_ident
        .as_ref()
        .iter()
        .map(UniCase::new)
        .collect::<Vec<_>>();

    // Get the expected parent by removing the last element from child
    let expected_parent_ident_unicase =
        &child_ident_unicase[..child_ident_unicase.len().saturating_sub(1)];

    // Compare the expected parent with the found parent
    expected_parent_ident_unicase == found_parent_ident_unicase.as_slice()
}

#[cfg(feature = "router")]
#[derive(Debug, Clone)]
pub(crate) struct NamespaceCacheEventListener;

#[cfg(feature = "router")]
impl std::fmt::Display for NamespaceCacheEventListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NamespaceCacheEventListener")
    }
}

#[cfg(feature = "router")]
#[async_trait::async_trait]
impl EventListener for NamespaceCacheEventListener {
    async fn namespace_created(&self, event: events::CreateNamespaceEvent) -> anyhow::Result<()> {
        let events::CreateNamespaceEvent {
            warehouse_id: _warehouse_id,
            namespace,
            request_metadata: _request_metadata,
        } = event;
        namespace_cache_insert(namespace).await;
        Ok(())
    }

    async fn namespace_dropped(&self, event: events::DropNamespaceEvent) -> anyhow::Result<()> {
        let events::DropNamespaceEvent {
            warehouse_id: _warehouse_id,
            namespace_id,
            request_metadata: _request_metadata,
        } = event;
        // This is sufficient also for recursive drops, as the cache only supports loading the full
        // hierarchy, which breaks if any of the entries in the path are missing.
        namespace_cache_invalidate(namespace_id).await;
        Ok(())
    }

    async fn namespace_properties_updated(
        &self,
        event: events::UpdateNamespacePropertiesEvent,
    ) -> anyhow::Result<()> {
        let events::UpdateNamespacePropertiesEvent {
            warehouse_id: _warehouse_id,
            namespace,
            updated_properties: _updated_properties,
            request_metadata: _request_metadata,
        } = event;
        namespace_cache_insert(namespace).await;
        Ok(())
    }

    async fn namespace_protection_set(
        &self,
        event: events::SetNamespaceProtectionEvent,
    ) -> anyhow::Result<()> {
        let events::SetNamespaceProtectionEvent {
            requested_protected: _requested_protected,
            updated_namespace,
            request_metadata: _request_metadata,
        } = event;
        namespace_cache_insert(updated_namespace).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::Utc;
    use iceberg::NamespaceIdent;

    use super::*;
    use crate::{WarehouseId, service::catalog_store::namespace::Namespace};

    /// Helper function to create a test namespace
    fn test_namespace(
        namespace_id: NamespaceId,
        namespace_ident: NamespaceIdent,
        warehouse_id: WarehouseId,
        updated_at: Option<chrono::DateTime<chrono::Utc>>,
        version: i64,
    ) -> Arc<Namespace> {
        Arc::new(Namespace {
            namespace_id,
            namespace_ident,
            warehouse_id,
            protected: false,
            properties: None,
            created_at: Utc::now(),
            updated_at,
            version: version.into(),
        })
    }

    /// Helper function to create a test namespace with parent
    fn test_namespace_with_parent(
        namespace: Arc<Namespace>,
        parent: Option<(NamespaceId, i64)>,
    ) -> NamespaceWithParent {
        NamespaceWithParent {
            namespace,
            parent: parent.map(|(id, version)| (id, version.into())),
        }
    }

    #[tokio::test]
    async fn test_namespace_cache_insert_and_get_by_id() {
        let namespace_id = NamespaceId::new_random();
        let warehouse_id = WarehouseId::new_random();
        let namespace_ident = NamespaceIdent::from_vec(vec!["test_ns".to_string()]).unwrap();

        let namespace = test_namespace(
            namespace_id,
            namespace_ident.clone(),
            warehouse_id,
            Some(Utc::now()),
            0,
        );
        let namespace_with_parent = test_namespace_with_parent(namespace, None);

        // Insert namespace into cache
        namespace_cache_insert_multiple(vec![namespace_with_parent]).await;

        // Retrieve by ID
        let cached = namespace_cache_get_by_id(namespace_id).await;
        assert!(cached.is_some());
        let cached = cached.unwrap();
        assert_eq!(cached.namespace_id(), namespace_id);
        assert_eq!(cached.namespace_ident(), &namespace_ident);
        assert_eq!(cached.warehouse_id(), warehouse_id);
    }

    #[tokio::test]
    async fn test_namespace_cache_get_by_ident() {
        let namespace_id = NamespaceId::new_random();
        let warehouse_id = WarehouseId::new_random();
        let namespace_ident = NamespaceIdent::from_vec(vec!["test_ident".to_string()]).unwrap();

        let namespace = test_namespace(
            namespace_id,
            namespace_ident.clone(),
            warehouse_id,
            Some(Utc::now()),
            0,
        );
        let namespace_with_parent = test_namespace_with_parent(namespace, None);

        // Insert namespace into cache
        namespace_cache_insert_multiple(vec![namespace_with_parent]).await;

        // Retrieve by ident
        let cached = namespace_cache_get_by_ident(&namespace_ident, warehouse_id).await;
        assert!(cached.is_some());
        let cached = cached.unwrap();
        assert_eq!(cached.namespace_id(), namespace_id);
        assert_eq!(cached.namespace_ident(), &namespace_ident);
    }

    #[tokio::test]
    async fn test_namespace_cache_case_insensitive_lookup() {
        let namespace_id = NamespaceId::new_random();
        let warehouse_id = WarehouseId::new_random();
        let namespace_ident = NamespaceIdent::from_vec(vec!["Test_Namespace".to_string()]).unwrap();

        let namespace = test_namespace(
            namespace_id,
            namespace_ident.clone(),
            warehouse_id,
            Some(Utc::now()),
            0,
        );
        let namespace_with_parent = test_namespace_with_parent(namespace, None);

        // Insert namespace with mixed-case ident
        namespace_cache_insert_multiple(vec![namespace_with_parent]).await;

        // Verify we can retrieve it with different case variations
        let cached_lower = namespace_cache_get_by_ident(
            &NamespaceIdent::from_vec(vec!["test_namespace".to_string()]).unwrap(),
            warehouse_id,
        )
        .await;
        assert!(cached_lower.is_some());
        assert_eq!(cached_lower.unwrap().namespace_id(), namespace_id);

        let cached_upper = namespace_cache_get_by_ident(
            &NamespaceIdent::from_vec(vec!["TEST_NAMESPACE".to_string()]).unwrap(),
            warehouse_id,
        )
        .await;
        assert!(cached_upper.is_some());
        assert_eq!(cached_upper.unwrap().namespace_id(), namespace_id);

        let cached_mixed = namespace_cache_get_by_ident(
            &NamespaceIdent::from_vec(vec!["TeSt_NaMeSpAcE".to_string()]).unwrap(),
            warehouse_id,
        )
        .await;
        assert!(cached_mixed.is_some());
        assert_eq!(cached_mixed.unwrap().namespace_id(), namespace_id);
    }

    #[tokio::test]
    async fn test_namespace_cache_with_hierarchy() {
        let warehouse_id = WarehouseId::new_random();

        // Create parent namespace "x"
        let parent_id = NamespaceId::new_random();
        let parent_ident = NamespaceIdent::from_vec(vec!["x".to_string()]).unwrap();
        let parent_namespace = test_namespace(
            parent_id,
            parent_ident.clone(),
            warehouse_id,
            Some(Utc::now()),
            0,
        );

        // Create child namespace "x.y"
        let child_id = NamespaceId::new_random();
        let child_ident = NamespaceIdent::from_vec(vec!["x".to_string(), "y".to_string()]).unwrap();
        let child_namespace = test_namespace(
            child_id,
            child_ident.clone(),
            warehouse_id,
            Some(Utc::now()),
            0,
        );

        // Create namespace with parent relationships
        let parent_with_parent = test_namespace_with_parent(parent_namespace.clone(), None);
        let child_with_parent =
            test_namespace_with_parent(child_namespace.clone(), Some((parent_id, 0)));

        // Insert both into cache
        namespace_cache_insert_multiple(vec![parent_with_parent, child_with_parent]).await;

        // Verify child is cached and hierarchy is reconstructed correctly
        let cached_child = namespace_cache_get_by_id(child_id).await;
        assert!(cached_child.is_some());
        let cached_child = cached_child.unwrap();
        assert_eq!(cached_child.namespace_id(), child_id);
        assert_eq!(cached_child.parents.len(), 1);
        assert_eq!(cached_child.parent().unwrap().namespace_id(), parent_id);

        // Verify parent is also cached independently
        let cached_parent = namespace_cache_get_by_id(parent_id).await;
        assert!(cached_parent.is_some());
        let cached_parent = cached_parent.unwrap();
        assert_eq!(cached_parent.namespace_id(), parent_id);
        assert_eq!(cached_parent.parents.len(), 0); // Parent is root

        // Update parent namespace (increment version)
        let updated_parent = test_namespace(
            parent_id,
            parent_ident.clone(),
            warehouse_id,
            Some(Utc::now()),
            1, // new version
        );
        let updated_parent_with_parent = test_namespace_with_parent(updated_parent, None);
        namespace_cache_insert_multiple(vec![updated_parent_with_parent]).await;

        // The child should still be valid because it has a minimum version requirement
        // Child expects parent version >= 0, and the cached parent is version 1, so it's valid
        let cached_child = namespace_cache_get_by_id(child_id).await;
        assert!(
            cached_child.is_some(),
            "Child should still be valid when parent version increases"
        );
        let cached_child = cached_child.unwrap();
        assert_eq!(cached_child.namespace_id(), child_id);
        // The parent in the hierarchy should be the updated version
        assert_eq!(cached_child.parents.len(), 1);
        assert_eq!(cached_child.parent().unwrap().namespace.version, 1.into());
    }

    #[tokio::test]
    async fn test_namespace_cache_insert_newer_version() {
        let namespace_id = NamespaceId::new_random();
        let warehouse_id = WarehouseId::new_random();
        let namespace_ident = NamespaceIdent::from_vec(vec!["versioned_ns".to_string()]).unwrap();

        let old_time = Utc::now();
        let new_time = old_time + chrono::Duration::seconds(10);

        // Insert older version
        let old_namespace = test_namespace(
            namespace_id,
            namespace_ident.clone(),
            warehouse_id,
            Some(old_time),
            0,
        );
        let old_namespace_with_parent = test_namespace_with_parent(old_namespace, None);
        namespace_cache_insert_multiple(vec![old_namespace_with_parent]).await;

        // Verify older version is cached
        let cached = namespace_cache_get_by_id(namespace_id).await;
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().namespace.updated_at(), Some(old_time));

        // Insert newer version
        let new_namespace = test_namespace(
            namespace_id,
            namespace_ident.clone(),
            warehouse_id,
            Some(new_time),
            1,
        );
        let new_namespace_with_parent = test_namespace_with_parent(new_namespace, None);
        namespace_cache_insert_multiple(vec![new_namespace_with_parent]).await;

        // Verify newer version replaced the old one
        let cached = namespace_cache_get_by_id(namespace_id).await;
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().namespace.updated_at(), Some(new_time));
    }

    #[tokio::test]
    async fn test_namespace_cache_insert_older_version_ignored() {
        let namespace_id = NamespaceId::new_random();
        let warehouse_id = WarehouseId::new_random();
        let namespace_ident = NamespaceIdent::from_vec(vec!["old_version_ns".to_string()]).unwrap();

        let new_time = Utc::now();
        let old_time = new_time - chrono::Duration::seconds(10);

        // Insert newer version first
        let new_namespace = test_namespace(
            namespace_id,
            namespace_ident.clone(),
            warehouse_id,
            Some(new_time),
            1,
        );
        let new_namespace_with_parent = test_namespace_with_parent(new_namespace, None);
        namespace_cache_insert_multiple(vec![new_namespace_with_parent]).await;

        // Verify newer version is cached
        let cached = namespace_cache_get_by_id(namespace_id).await;
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().namespace.updated_at(), Some(new_time));

        // Try to insert older version
        let old_namespace = test_namespace(
            namespace_id,
            namespace_ident.clone(),
            warehouse_id,
            Some(old_time),
            0,
        );
        let old_namespace_with_parent = test_namespace_with_parent(old_namespace, None);
        namespace_cache_insert_multiple(vec![old_namespace_with_parent]).await;

        // Verify newer version is still cached (old one was ignored)
        let cached = namespace_cache_get_by_id(namespace_id).await;
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().namespace.updated_at(), Some(new_time));
    }

    #[tokio::test]
    async fn test_namespace_cache_invalidate() {
        let namespace_id = NamespaceId::new_random();
        let warehouse_id = WarehouseId::new_random();
        let namespace_ident = NamespaceIdent::from_vec(vec!["invalidate_ns".to_string()]).unwrap();

        let namespace = test_namespace(
            namespace_id,
            namespace_ident.clone(),
            warehouse_id,
            Some(Utc::now()),
            0,
        );
        let namespace_with_parent = test_namespace_with_parent(namespace, None);

        // Insert namespace into cache
        namespace_cache_insert_multiple(vec![namespace_with_parent]).await;

        // Verify it's cached
        let cached = namespace_cache_get_by_id(namespace_id).await;
        assert!(cached.is_some());

        // Invalidate
        namespace_cache_invalidate(namespace_id).await;

        // Verify it's no longer cached
        let cached = namespace_cache_get_by_id(namespace_id).await;
        assert!(cached.is_none());

        // Verify ident-to-id cache is also invalidated
        let cached_by_ident = namespace_cache_get_by_ident(&namespace_ident, warehouse_id).await;
        assert!(cached_by_ident.is_none());
    }

    #[tokio::test]
    async fn test_namespace_cache_miss() {
        let namespace_id = NamespaceId::new_random();
        let warehouse_id = WarehouseId::new_random();

        // Try to get a namespace that was never cached
        let cached = namespace_cache_get_by_id(namespace_id).await;
        assert!(cached.is_none());

        let cached_by_ident = namespace_cache_get_by_ident(
            &NamespaceIdent::from_vec(vec!["nonexistent".to_string()]).unwrap(),
            warehouse_id,
        )
        .await;
        assert!(cached_by_ident.is_none());
    }
}
