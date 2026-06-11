use std::{
    sync::{Arc, LazyLock},
    time::Duration,
};

use axum_prometheus::metrics;
use iceberg::NamespaceIdent;
use moka::{
    future::Cache,
    notification::RemovalCause,
    ops::compute::{CompResult, Op},
};

use super::secondary_index_get_or_load;
#[cfg(feature = "router")]
use crate::service::events::{self, EventListener};
use crate::{
    CONFIG, WarehouseId,
    service::{
        NamespaceId, NamespaceWithParent,
        cache_metrics::{
            METRIC_CACHE_HITS_TOTAL as METRIC_NAMESPACE_CACHE_HITS,
            METRIC_CACHE_MISSES_TOTAL as METRIC_NAMESPACE_CACHE_MISSES,
            METRIC_CACHE_SIZE as METRIC_NAMESPACE_CACHE_SIZE, METRICS_INITIALIZED,
        },
        cache_ttl::JitteredTtl,
        catalog_store::namespace::NamespaceHierarchy,
    },
};

// Main cache: stores individual namespaces by ID
pub static NAMESPACE_CACHE: LazyLock<Cache<NamespaceId, NamespaceWithParent>> =
    LazyLock::new(|| {
        Cache::builder()
            .max_capacity(CONFIG.cache.namespace.capacity)
            .initial_capacity(50)
            .time_to_live(Duration::from_secs(
                CONFIG.cache.namespace.time_to_live_secs,
            ))
            .expire_after(JitteredTtl::with_default_jitter(Duration::from_secs(
                CONFIG.cache.namespace.time_to_live_secs,
            )))
            .async_eviction_listener(|key, value: NamespaceWithParent, cause| {
                Box::pin(async move {
                    // On Replaced: invalidate the old secondary index mapping immediately,
                    // then spawn a task to re-insert the new mapping (avoids re-entrant
                    // NAMESPACE_CACHE.get() calls which can deadlock).
                    // On all other causes (expired, explicit): always invalidate.
                    match cause {
                        RemovalCause::Replaced => {
                            let key = *key;
                            // Immediately invalidate the old (warehouse_id, namespace_ident) → namespace_id mapping
                            IDENT_TO_ID_CACHE
                                .invalidate(&(
                                    value.namespace.warehouse_id,
                                    namespace_ident_to_cache_key(&value.namespace.namespace_ident),
                                ))
                                .await;

                            // Spawn task to add the new mapping (avoids re-entrant NAMESPACE_CACHE.get)
                            tokio::spawn(async move {
                                if let Some(curr) = NAMESPACE_CACHE.get(&key).await {
                                    IDENT_TO_ID_CACHE
                                        .insert(
                                            (
                                                curr.namespace.warehouse_id,
                                                namespace_ident_to_cache_key(
                                                    &curr.namespace.namespace_ident,
                                                ),
                                            ),
                                            key,
                                        )
                                        .await;
                                }
                            });
                        }
                        _ => {
                            IDENT_TO_ID_CACHE
                                .invalidate(&(
                                    value.namespace.warehouse_id,
                                    namespace_ident_to_cache_key(&value.namespace.namespace_ident),
                                ))
                                .await;
                        }
                    }
                })
            })
            .build()
    });

// WarehouseId, NamespaceIdent components (plain strings, case-sensitive key).
// Same case → cache hit. Different case → cache miss → DB lookup → new cache entry.
// No Rust-side case folding: the DB (ICU collation) is the sole authority for case-insensitive matching.
type NamespaceCacheKey = (WarehouseId, Vec<String>);

// Secondary index: (warehouse_id, namespace_ident) → namespace_id
pub static IDENT_TO_ID_CACHE: LazyLock<Cache<NamespaceCacheKey, NamespaceId>> =
    LazyLock::new(|| {
        Cache::builder()
            .max_capacity(CONFIG.cache.namespace.capacity)
            .initial_capacity(50)
            .time_to_live(Duration::from_secs(
                CONFIG.cache.namespace.time_to_live_secs,
            ))
            .expire_after(JitteredTtl::with_default_jitter(Duration::from_secs(
                CONFIG.cache.namespace.time_to_live_secs,
            )))
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
        // The NAMESPACE_CACHE (id → data) always stores canonical case only. This
        // guarantees id-based lookups return deterministic case, independent of
        // which earlier caller (with whatever case variant) populated the cache.
        let canonical_entry = NamespaceWithParent {
            namespace: namespace.namespace.clone(),
            parent: namespace.parent,
            requested_ident: None,
        };
        // The IDENT_TO_ID_CACHE (ident → id) is keyed by the caller's case, so the
        // same caller's next lookup hits. Different case → cache miss → DB lookup
        // (case-insensitive via ICU collation) → new cache entry for that case.
        // The canonical-ident entry is also inserted so that looking up a namespace
        // by its canonical case (e.g. after creation) hits the cache.
        let user_key = namespace_ident_to_cache_key(namespace.namespace_ident());
        let canonical_key = namespace_ident_to_cache_key(&namespace.namespace.namespace_ident);
        if user_key == canonical_key {
            tokio::join!(
                NAMESPACE_CACHE.insert(namespace_id, canonical_entry),
                IDENT_TO_ID_CACHE.insert((warehouse_id, user_key), namespace_id),
            );
        } else {
            tokio::join!(
                NAMESPACE_CACHE.insert(namespace_id, canonical_entry),
                IDENT_TO_ID_CACHE.insert((warehouse_id, user_key), namespace_id),
                IDENT_TO_ID_CACHE.insert((warehouse_id, canonical_key), namespace_id),
            );
        }

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
    metrics::gauge!(METRIC_NAMESPACE_CACHE_SIZE, "cache_type" => "namespace_ident_to_id")
        .set(IDENT_TO_ID_CACHE.entry_count() as f64);
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
    let ident_key = (warehouse_id, namespace_ident_to_cache_key(namespace_ident));
    let Some(namespace_id) = IDENT_TO_ID_CACHE.get(&ident_key).await else {
        metrics::counter!(METRIC_NAMESPACE_CACHE_MISSES, "cache_type" => "namespace_ident_to_id")
            .increment(1);
        return None;
    };
    metrics::counter!(METRIC_NAMESPACE_CACHE_HITS, "cache_type" => "namespace_ident_to_id")
        .increment(1);
    tracing::debug!("Namespace ident {namespace_ident} found in ident-to-id cache");
    let result = namespace_cache_get_by_id(namespace_id).await;
    if result.is_none() {
        tracing::debug!(
            "Namespace id {namespace_id} not found in cache, invalidating stale ident mapping for {namespace_ident}"
        );
        IDENT_TO_ID_CACHE.invalidate(&ident_key).await;
    }
    result
}

/// The leaf of a fetched chain: the entry with the longest ident. Mirrors
/// `build_namespace_hierarchy_from_vec`'s target selection.
fn leaf_namespace_id(chain: &[NamespaceWithParent]) -> Option<NamespaceId> {
    chain
        .iter()
        .max_by_key(|n| n.namespace_ident().len())
        .map(NamespaceWithParent::namespace_id)
}

/// Single-flight read-through for the `(warehouse, namespace_ident) → id`
/// resolution.
///
/// Coalesces concurrent **by-ident** misses: the chain-fetch loader runs once per
/// `(warehouse, ident)` (clients usually address namespaces by name, so this is
/// the hot path), the whole hierarchy is cached via
/// `namespace_cache_insert_multiple`, and every coalesced caller resolves the leaf
/// id from the index. Returns the leaf `NamespaceId`, or `None` if the namespace
/// does not exist (**not** negative-cached). Callers rebuild the hierarchy from the
/// now-cached chain (the normal cache-hit path). The loader error is by value.
///
/// **Coalescing is only correct off a transaction** — see the call site in
/// `get_namespace`, which routes here only when given a pooled `State`.
pub(super) async fn namespace_ident_get_or_load<Fut, E>(
    warehouse_id: WarehouseId,
    namespace_ident: &NamespaceIdent,
    load: Fut,
) -> Result<Option<NamespaceId>, E>
where
    Fut: std::future::Future<Output = Result<Vec<NamespaceWithParent>, E>> + Send,
    E: Send + Sync + 'static,
{
    let key = (warehouse_id, namespace_ident_to_cache_key(namespace_ident));
    secondary_index_get_or_load(
        CONFIG.cache.namespace.enabled,
        &IDENT_TO_ID_CACHE,
        key,
        // Adapt the raw chain fetch to `Option<(leaf_id, chain)>`: an empty chain
        // (no leaf) means "not found".
        async move {
            let chain = load.await?;
            Ok(leaf_namespace_id(&chain).map(|leaf| (leaf, chain)))
        },
        |(leaf, _chain): &(NamespaceId, Vec<NamespaceWithParent>)| *leaf,
        // Cache the whole hierarchy (leaf + parents + ident mappings) so every
        // coalesced caller rebuilds it without another DB round-trip.
        |(_leaf, chain): (NamespaceId, Vec<NamespaceWithParent>)| {
            namespace_cache_insert_multiple(chain)
        },
    )
    .await
}

/// Single-flight read-through for the by-id namespace path.
///
/// Coalesces concurrent misses for the same `namespace_id`: the chain-fetch loader
/// runs once, the whole hierarchy is cached, and callers rebuild from cache.
/// Returns `true` if the namespace exists (and is now cached), `false` otherwise
/// (**not** negative-cached). Like the by-ident variant, coalescing is only correct
/// off a pooled `State` (see `get_namespace`).
pub(super) async fn namespace_id_get_or_load<Fut, E>(
    namespace_id: NamespaceId,
    load: Fut,
) -> Result<bool, E>
where
    Fut: std::future::Future<Output = Result<Vec<NamespaceWithParent>, E>> + Send,
    E: Send + Sync + 'static,
{
    if !CONFIG.cache.namespace.enabled {
        let chain = load.await?;
        let found = chain.iter().any(|n| n.namespace_id() == namespace_id);
        if found {
            namespace_cache_insert_multiple(chain).await;
        }
        return Ok(found);
    }

    if NAMESPACE_CACHE.get(&namespace_id).await.is_some() {
        return Ok(true);
    }

    // The compute always returns `Op::Nop` (the authoritative write is the
    // version-gated `insert_multiple`, not a raw `Op::Put`), so existence is
    // normally surfaced by re-reading the cache below. That re-read alone can
    // race a (microsecond, capacity-driven) eviction of the just-primed entry and
    // spuriously report not-found, so we also record whether the load itself found
    // the namespace — making `found` independent of the entry still being resident.
    let found_in_load = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let found_signal = Arc::clone(&found_in_load);
    let outcome = NAMESPACE_CACHE
        .entry(namespace_id)
        .and_try_compute_with(|maybe_entry| async move {
            if maybe_entry.is_some() {
                return Ok::<_, E>(Op::Nop);
            }
            let chain = load.await?;
            if !chain.iter().any(|n| n.namespace_id() == namespace_id) {
                // Namespace not found — never negative-cached.
                return Ok(Op::Nop);
            }
            found_signal.store(true, std::sync::atomic::Ordering::Relaxed);
            // `namespace_cache_insert_multiple` is the authoritative write: it is
            // version-gated (keeps a concurrently-cached newer version) and stores
            // *canonical* entries (the invariant `build_hierarchy_from_cache` relies
            // on). We deliberately return `Op::Nop` rather than `Op::Put(leaf)` — a
            // raw, possibly-stale, non-canonical leaf — which would clobber a newer
            // concurrent insert and break the canonical invariant.
            namespace_cache_insert_multiple(chain).await;
            Ok(Op::Nop)
        })
        .await?;

    Ok(match outcome {
        // `maybe_entry` was already populated (a concurrent caller won the race).
        CompResult::Inserted(_) | CompResult::ReplacedWith(_) | CompResult::Unchanged(_) => true,
        // We always return `Op::Nop`, so a found namespace lands here: our gated
        // `insert_multiple` wrote it via a different lock domain that moka's
        // snapshot-based `Op::Nop` result cannot surface. The load's own result is
        // authoritative for existence; the cache re-read additionally covers the
        // case where a concurrent caller populated the entry. Without the
        // `found_in_load` term, an eviction between our insert and this read would
        // spuriously report not-found for a namespace that exists.
        CompResult::StillNone(_) | CompResult::Removed(_) => {
            found_in_load.load(std::sync::atomic::Ordering::Relaxed)
                || NAMESPACE_CACHE.get(&namespace_id).await.is_some()
        }
    })
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
pub(crate) fn namespace_ident_to_cache_key(ident: &NamespaceIdent) -> Vec<String> {
    ident.as_ref().clone()
}

fn is_parent_ident(child_ident: &NamespaceIdent, found_parent_ident: &NamespaceIdent) -> bool {
    let child = child_ident.as_ref();
    let parent = found_parent_ident.as_ref();

    // Both idents come from NAMESPACE_CACHE which only stores canonical case
    // (requested_ident is stripped at insertion), so child_canonical[:-1] and
    // parent_canonical are byte-identical by construction for any valid
    // hierarchy. A mismatch here indicates stale cache state (e.g. rename).
    let expected_parent = &child[..child.len().saturating_sub(1)];
    expected_parent == parent
}

#[cfg(feature = "router")]
#[derive(Debug, Clone)]
pub struct NamespaceCacheEventListener;

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
            namespace,
            request_metadata: _request_metadata,
        } = event;
        // This is sufficient also for recursive drops, as the cache only supports loading the full
        // hierarchy, which breaks if any of the entries in the path are missing.
        namespace_cache_invalidate(namespace.namespace_id()).await;
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
            requested_ident: None,
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
    async fn test_namespace_cache_case_sensitive_key() {
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

        // Same case → cache hit
        let cached_exact = namespace_cache_get_by_ident(&namespace_ident, warehouse_id).await;
        assert!(cached_exact.is_some());
        assert_eq!(cached_exact.unwrap().namespace_id(), namespace_id);

        // Different case → cache miss (DB handles case-insensitive matching)
        let cached_lower = namespace_cache_get_by_ident(
            &NamespaceIdent::from_vec(vec!["test_namespace".to_string()]).unwrap(),
            warehouse_id,
        )
        .await;
        assert!(cached_lower.is_none());

        let cached_upper = namespace_cache_get_by_ident(
            &NamespaceIdent::from_vec(vec!["TEST_NAMESPACE".to_string()]).unwrap(),
            warehouse_id,
        )
        .await;
        assert!(cached_upper.is_none());
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
    async fn test_ident_to_id_cache_has_ttl_matching_primary() {
        let primary_ttl = NAMESPACE_CACHE.policy().time_to_live();
        let secondary_ttl = IDENT_TO_ID_CACHE.policy().time_to_live();
        assert_eq!(
            primary_ttl, secondary_ttl,
            "IDENT_TO_ID_CACHE TTL must match NAMESPACE_CACHE TTL"
        );
        assert!(
            secondary_ttl.is_some(),
            "IDENT_TO_ID_CACHE must have a TTL configured"
        );
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

    /// `namespace_ident_get_or_load` must coalesce concurrent by-ident misses into
    /// ONE chain-fetch, with every caller resolving the same leaf id.
    #[tokio::test]
    async fn namespace_ident_get_or_load_coalesces_concurrent_misses() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let warehouse_id = WarehouseId::new_random();
        let namespace_id = NamespaceId::new_random();
        let namespace_ident =
            NamespaceIdent::from_vec(vec!["ns-ident-coalesce".to_string()]).unwrap();
        let chain = vec![test_namespace_with_parent(
            test_namespace(
                namespace_id,
                namespace_ident.clone(),
                warehouse_id,
                Some(Utc::now()),
                0,
            ),
            None,
        )];

        let loads = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..32 {
            let loads = Arc::clone(&loads);
            let chain = chain.clone();
            let namespace_ident = namespace_ident.clone();
            handles.push(tokio::spawn(async move {
                namespace_ident_get_or_load(warehouse_id, &namespace_ident, async move {
                    loads.fetch_add(1, Ordering::SeqCst);
                    for _ in 0..100 {
                        tokio::task::yield_now().await;
                    }
                    Ok::<_, std::convert::Infallible>(chain)
                })
                .await
            }));
        }

        let mut results = Vec::new();
        for h in handles {
            results.push(h.await.unwrap().unwrap().expect("namespace exists"));
        }

        assert_eq!(
            loads.load(Ordering::SeqCst),
            1,
            "concurrent by-ident misses must coalesce to a single chain-fetch"
        );
        for id in &results {
            assert_eq!(*id, namespace_id);
        }
    }

    /// `namespace_id_get_or_load` must coalesce concurrent by-id misses into ONE
    /// chain-fetch.
    #[tokio::test]
    async fn namespace_id_get_or_load_coalesces_concurrent_misses() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let warehouse_id = WarehouseId::new_random();
        let namespace_id = NamespaceId::new_random();
        let namespace_ident = NamespaceIdent::from_vec(vec!["ns-id-coalesce".to_string()]).unwrap();
        let chain = vec![test_namespace_with_parent(
            test_namespace(
                namespace_id,
                namespace_ident,
                warehouse_id,
                Some(Utc::now()),
                0,
            ),
            None,
        )];

        let loads = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..32 {
            let loads = Arc::clone(&loads);
            let chain = chain.clone();
            handles.push(tokio::spawn(async move {
                namespace_id_get_or_load(namespace_id, async move {
                    loads.fetch_add(1, Ordering::SeqCst);
                    for _ in 0..100 {
                        tokio::task::yield_now().await;
                    }
                    Ok::<_, std::convert::Infallible>(chain)
                })
                .await
            }));
        }

        let mut found_all = true;
        for h in handles {
            found_all &= h.await.unwrap().unwrap();
        }

        assert_eq!(
            loads.load(Ordering::SeqCst),
            1,
            "concurrent by-id misses must coalesce to a single chain-fetch"
        );
        assert!(
            found_all,
            "every caller must observe the namespace as found"
        );
    }

    /// The by-id loader must not clobber a concurrently-cached newer version with
    /// its (possibly stale) just-loaded leaf — mirrors the warehouse/role version
    /// gate. `namespace_cache_insert_multiple` is the authoritative gated write;
    /// the compute returns `Op::Nop`, so a concurrent newer insert survives.
    // The combined namespace cache machinery (insert_multiple + eviction listeners)
    // makes this test's future exceed the lint threshold; it's test-only.
    #[allow(clippy::large_futures)]
    #[tokio::test]
    async fn namespace_id_get_or_load_version_gate_keeps_newer_concurrent_insert() {
        let warehouse_id = WarehouseId::new_random();
        let namespace_id = NamespaceId::new_random();
        let ident = NamespaceIdent::from_vec(vec!["ns-id-version-gate".to_string()]).unwrap();

        let newer = test_namespace_with_parent(
            test_namespace(
                namespace_id,
                ident.clone(),
                warehouse_id,
                Some(Utc::now()),
                5,
            ),
            None,
        );
        let older_chain = vec![test_namespace_with_parent(
            test_namespace(namespace_id, ident, warehouse_id, Some(Utc::now()), 3),
            None,
        )];

        let found = namespace_id_get_or_load(namespace_id, {
            let newer = newer.clone();
            async move {
                // A concurrent writer caches a newer version (e.g. the event
                // listener after a metadata update) while we "load" a stale one.
                namespace_cache_insert(newer).await;
                Ok::<_, std::convert::Infallible>(older_chain)
            }
        })
        .await
        .unwrap();

        assert!(found, "namespace exists");
        let cached = namespace_cache_get_by_id(namespace_id)
            .await
            .expect("namespace is cached");
        assert_eq!(
            *cached.namespace.version(),
            5,
            "the stale v3 load must not clobber the concurrently-cached v5"
        );
    }
}
