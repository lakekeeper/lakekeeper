use std::{sync::LazyLock, time::Duration};

use axum_prometheus::metrics;
use moka::{
    future::Cache,
    notification::RemovalCause,
    ops::compute::{CompResult, Op},
};

use super::secondary_index_get_or_load;
#[cfg(feature = "router")]
use crate::service::events::{self, EventListener};
use crate::{
    CONFIG,
    service::{
        ArcProjectId, ArcRole, ArcRoleIdent, RoleId,
        cache_metrics::{
            METRIC_CACHE_HITS_TOTAL as METRIC_ROLE_CACHE_HITS,
            METRIC_CACHE_MISSES_TOTAL as METRIC_ROLE_CACHE_MISSES,
            METRIC_CACHE_SIZE as METRIC_ROLE_CACHE_SIZE, METRICS_INITIALIZED,
        },
        cache_ttl::JitteredTtl,
    },
};

// Primary cache: RoleId → ArcRole
pub static ROLE_CACHE: LazyLock<Cache<RoleId, ArcRole>> = LazyLock::new(|| {
    Cache::builder()
        .max_capacity(CONFIG.cache.role.capacity)
        .initial_capacity(100)
        // Deliberately NOT jittered (unlike the other caches). `USER_ASSIGNMENTS_CACHE`
        // entries reference roles resolved through this cache, and the startup
        // invariant `user_assignments.ttl <= role.ttl` (see `config.rs`) requires a
        // UA entry to never outlive its role entry. Holding this cache at its exact
        // base TTL keeps that invariant airtight: a UA entry lives `<= ua_base <=
        // role_base` = this entry's life. Downward jitter here could let a co-warmed
        // UA entry outlive the role entry by up to the jitter fraction. See
        // `crate::service::cache_ttl`.
        .time_to_live(Duration::from_secs(CONFIG.cache.role.time_to_live_secs))
        .async_eviction_listener(|key, value: ArcRole, cause| {
            Box::pin(async move {
                // On Replaced: invalidate the old secondary index mapping immediately,
                // then spawn a task to re-insert the new mapping (avoids re-entrant
                // ROLE_CACHE.get() calls which can deadlock).
                // On all other causes (expired, explicit): always invalidate.
                match cause {
                    RemovalCause::Replaced => {
                        let key = *key;
                        // Immediately invalidate the old (project_id, ident) → role_id mapping
                        IDENT_TO_ID_CACHE
                            .invalidate(&(value.project_id_arc(), value.ident_arc()))
                            .await;

                        // Spawn task to add the new mapping (avoids re-entrant ROLE_CACHE.get)
                        tokio::spawn(async move {
                            if let Some(curr) = ROLE_CACHE.get(&key).await {
                                IDENT_TO_ID_CACHE
                                    .insert((curr.project_id_arc(), curr.ident_arc()), key)
                                    .await;
                            }
                        });
                    }
                    _ => {
                        IDENT_TO_ID_CACHE
                            .invalidate(&(value.project_id_arc(), value.ident_arc()))
                            .await;
                    }
                }
            })
        })
        .build()
});

// Secondary index: (ProjectId, RoleIdent) → RoleId
// Enables O(1) cache lookups when the caller has a project_id + ident instead of a RoleId.
static IDENT_TO_ID_CACHE: LazyLock<Cache<(ArcProjectId, ArcRoleIdent), RoleId>> =
    LazyLock::new(|| {
        Cache::builder()
            .max_capacity(CONFIG.cache.role.capacity)
            .initial_capacity(100)
            .time_to_live(Duration::from_secs(CONFIG.cache.role.time_to_live_secs))
            .expire_after(JitteredTtl::with_default_jitter(Duration::from_secs(
                CONFIG.cache.role.time_to_live_secs,
            )))
            .build()
    });

#[allow(dead_code)] // Not required for all features
pub async fn role_cache_invalidate(role_id: RoleId) {
    if CONFIG.cache.role.enabled {
        tracing::debug!("Invalidating role id {role_id} from cache");
        ROLE_CACHE.invalidate(&role_id).await;
        // The eviction listener on ROLE_CACHE cascades the invalidation to IDENT_TO_ID_CACHE.
        update_cache_size_metric();
    }
}

pub(super) async fn role_cache_insert(role: ArcRole) {
    if CONFIG.cache.role.enabled {
        let role_id = role.id();
        let project_id = role.project_id_arc();
        let ident = role.ident_arc();

        // Version-gated: skip insert if an entry with a strictly newer version already exists.
        if let Some(existing) = ROLE_CACHE.get(&role_id).await
            && role.version < existing.version
        {
            tracing::debug!(
                "Skipping insert of role id {role_id} into cache; \
                     existing version {} is newer than new version {}",
                existing.version,
                role.version
            );
            return;
        }

        tracing::debug!("Inserting role id {role_id} into cache");
        tokio::join!(
            ROLE_CACHE.insert(role_id, role),
            IDENT_TO_ID_CACHE.insert((project_id, ident), role_id),
        );
        update_cache_size_metric();
    }
}

/// Single-flight read-through for the role cache (by id).
///
/// Coalesces concurrent misses for the same `role_id`: moka serializes the
/// per-key compute, so the loader runs once and later callers observe the
/// inserted entry. The loader returns `Option` — `None` (role not found) is
/// **not** negative-cached; callers map it to their own not-found error. The
/// version-gate is preserved without reworking the writer: before inserting we
/// re-read the current entry and skip if a concurrent `role_cache_insert` cached
/// a strictly newer version (same sub-`await` residual the existing
/// get-then-insert gate has). The `(project, ident) → id` index is populated
/// alongside, like `role_cache_insert`. The `enabled` flag and hit/miss metrics
/// are preserved; the loader error is returned by value.
pub(super) async fn role_cache_get_or_load<Fut, E>(
    role_id: RoleId,
    load: Fut,
) -> Result<Option<ArcRole>, E>
where
    Fut: std::future::Future<Output = Result<Option<ArcRole>, E>> + Send,
    E: Send + Sync + 'static,
{
    if !CONFIG.cache.role.enabled {
        return load.await;
    }

    // Fast path records a hit/miss. Note: under contention each coalesced waiter
    // records a miss here but then hits `Op::Nop` below without loading, so the
    // miss counter is *cache misses*, not *DB loads* (the two diverge under a herd).
    if let Some(role) = role_cache_get_by_id(role_id).await {
        return Ok(Some(role));
    }

    let outcome = ROLE_CACHE
        .entry(role_id)
        .and_try_compute_with(|maybe_entry| async move {
            if maybe_entry.is_some() {
                // Populated by another caller while we waited on the key lock.
                return Ok::<_, E>(Op::Nop);
            }
            let Some(role) = load.await? else {
                // Role not found — never negative-cached. Coalescing therefore
                // applies only to a found role; concurrent lookups of a missing
                // one each re-run the loader (rare, and no worse than before).
                return Ok(Op::Nop);
            };
            // Preserve the version-gate against a writer that cached a newer
            // version via plain `insert()` during our load.
            if let Some(existing) = ROLE_CACHE.get(&role_id).await
                && role.version < existing.version
            {
                return Ok(Op::Nop);
            }
            IDENT_TO_ID_CACHE
                .insert((role.project_id_arc(), role.ident_arc()), role_id)
                .await;
            Ok(Op::Put(role))
        })
        .await?;
    update_cache_size_metric();

    Ok(match outcome {
        CompResult::Inserted(entry)
        | CompResult::ReplacedWith(entry)
        | CompResult::Unchanged(entry) => Some(entry.into_value()),
        // `StillNone` means either the loader returned `None` (genuine not-found,
        // never negative-cached) or the version-gate fired because a concurrent
        // writer cached a newer version during our load. moka derives the
        // `Op::Nop` result from the closure's entry snapshot, so it cannot surface
        // that concurrent `insert()` (a different lock domain) — a final raw read
        // does, returning the newer value if present and `None` otherwise.
        // `Removed` is unreachable (the closure only returns `Nop`/`Put`).
        CompResult::StillNone(_) | CompResult::Removed(_) => ROLE_CACHE.get(&role_id).await,
    })
}

/// Update the cache size metric with the current number of entries
#[inline]
#[allow(clippy::cast_precision_loss)]
fn update_cache_size_metric() {
    let () = &*METRICS_INITIALIZED; // Ensure metrics are described
    metrics::gauge!(METRIC_ROLE_CACHE_SIZE, "cache_type" => "role")
        .set(ROLE_CACHE.entry_count() as f64);
    metrics::gauge!(METRIC_ROLE_CACHE_SIZE, "cache_type" => "role_ident_to_id")
        .set(IDENT_TO_ID_CACHE.entry_count() as f64);
}

pub(super) async fn role_cache_get_by_id(role_id: RoleId) -> Option<ArcRole> {
    update_cache_size_metric();
    if let Some(role) = ROLE_CACHE.get(&role_id).await {
        tracing::debug!("Role id {role_id} found in cache");
        metrics::counter!(METRIC_ROLE_CACHE_HITS, "cache_type" => "role").increment(1);
        Some(role)
    } else {
        metrics::counter!(METRIC_ROLE_CACHE_MISSES, "cache_type" => "role").increment(1);
        None
    }
}

/// Resolve `(project_id, role_ident)` → `RoleId` using the secondary index only.
///
/// Returns `None` if the ident is not in the role ident cache.  Does not
/// touch the primary `ROLE_CACHE` — use [`role_cache_get_by_ident`] if the
/// full role is needed.
pub(crate) async fn role_ident_to_id(
    project_id: ArcProjectId,
    ident: ArcRoleIdent,
) -> Option<RoleId> {
    IDENT_TO_ID_CACHE.get(&(project_id, ident)).await
}

/// Insert a `(project_id, role_ident)` → `RoleId` mapping into the secondary
/// index without touching the primary [`ROLE_CACHE`].
///
/// Used by sync-event listeners that know the full ident but not the complete
/// [`ArcRole`] data required by [`role_cache_insert`].
pub(crate) async fn role_ident_insert(
    project_id: ArcProjectId,
    ident: ArcRoleIdent,
    role_id: RoleId,
) {
    if CONFIG.cache.role.enabled {
        IDENT_TO_ID_CACHE.insert((project_id, ident), role_id).await;
        update_cache_size_metric();
    }
}

pub(super) async fn role_cache_get_by_ident(
    project_id: ArcProjectId,
    ident: ArcRoleIdent,
) -> Option<ArcRole> {
    update_cache_size_metric();
    let ident_key = (project_id, ident.clone());
    let Some(role_id) = IDENT_TO_ID_CACHE.get(&ident_key).await else {
        metrics::counter!(METRIC_ROLE_CACHE_MISSES, "cache_type" => "role_ident_to_id")
            .increment(1);
        return None;
    };
    metrics::counter!(METRIC_ROLE_CACHE_HITS, "cache_type" => "role_ident_to_id").increment(1);
    tracing::debug!("Role ident {ident} resolved in ident-to-id cache to id {role_id}");

    if let Some(role) = ROLE_CACHE.get(&role_id).await {
        tracing::debug!("Role id {role_id} found in cache");
        metrics::counter!(METRIC_ROLE_CACHE_HITS, "cache_type" => "role").increment(1);
        Some(role)
    } else {
        tracing::debug!(
            "Role id {role_id} not found in cache, invalidating stale ident mapping for {ident}"
        );
        IDENT_TO_ID_CACHE.remove(&ident_key).await;
        update_cache_size_metric();
        metrics::counter!(METRIC_ROLE_CACHE_MISSES, "cache_type" => "role").increment(1);
        None
    }
}

/// Single-flight read-through for the `(project, ident) → id` resolution.
///
/// Coalesces concurrent **by-ident** misses (clients usually address roles by
/// ident, so this is the hot cold-start path): the by-ident DB query runs once per
/// `(project, ident)`, the loaded role primes the by-id cache + ident index, and
/// every coalesced caller resolves the full role by id. Returns the resolved
/// `RoleId`, or `None` if no role matches (**not** negative-cached). Thin wrapper
/// over [`secondary_index_get_or_load`](super::secondary_index_get_or_load).
pub(super) async fn role_ident_to_id_get_or_load<Fut, E>(
    project_id: ArcProjectId,
    ident: ArcRoleIdent,
    load: Fut,
) -> Result<Option<RoleId>, E>
where
    Fut: std::future::Future<Output = Result<Option<ArcRole>, E>> + Send,
    E: Send + Sync + 'static,
{
    secondary_index_get_or_load(
        CONFIG.cache.role.enabled,
        &IDENT_TO_ID_CACHE,
        (project_id, ident),
        load,
        |role: &ArcRole| role.id(),
        role_cache_insert,
    )
    .await
}

#[cfg(feature = "router")]
#[derive(Debug, Clone)]
pub struct RoleCacheEventListener;

#[cfg(feature = "router")]
impl std::fmt::Display for RoleCacheEventListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RoleCacheEventListener")
    }
}

#[cfg(feature = "router")]
#[async_trait::async_trait]
impl EventListener for RoleCacheEventListener {
    async fn role_created(&self, event: events::CreateRoleEvent) -> anyhow::Result<()> {
        let events::CreateRoleEvent {
            role,
            request_metadata: _,
        } = event;
        role_cache_insert(role).await;
        Ok(())
    }

    async fn role_updated(&self, event: events::UpdateRoleEvent) -> anyhow::Result<()> {
        let events::UpdateRoleEvent {
            role,
            request_metadata: _,
        } = event;
        role_cache_insert(role).await;
        Ok(())
    }

    async fn role_deleted(&self, event: events::DeleteRoleEvent) -> anyhow::Result<()> {
        let events::DeleteRoleEvent {
            role,
            request_metadata: _,
        } = event;
        role_cache_invalidate(role.id()).await;
        Ok(())
    }

    /// Warm the `IDENT_TO_ID_CACHE` secondary index after a role's members
    /// have been synced.
    ///
    /// The sync is authoritative proof that the role exists — its
    /// `(project_id, role_ident)` → `role_id` mapping is therefore valid and
    /// we can insert it without a DB round-trip.
    async fn role_members_synced(
        &self,
        event: events::RoleMembersSyncedEvent,
    ) -> anyhow::Result<()> {
        role_ident_insert(
            event.result.project_id.clone(),
            event.result.role_ident.clone(),
            event.result.role_id,
        )
        .await;
        Ok(())
    }

    /// Warm the `IDENT_TO_ID_CACHE` secondary index for every role present in
    /// the authoritative assignment list after a user's assignments are synced.
    ///
    /// Each [`AssignedRole`] in `result.roles` carries a valid
    /// `(project_id, role_ident, role_id)` triple we can insert directly.
    async fn user_role_assignments_synced(
        &self,
        event: events::UserRoleAssignmentsSyncedEvent,
    ) -> anyhow::Result<()> {
        for assigned in &event.result.roles {
            role_ident_insert(
                assigned.project_id.clone(),
                assigned.role_ident.clone(),
                assigned.role_id,
            )
            .await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{
        ProjectId,
        service::{
            ArcProjectId, RoleIdent,
            catalog_store::role::RoleVersion,
            identifier::role::{RoleProviderId, RoleSourceId},
        },
    };

    fn test_role(
        role_id: RoleId,
        project_id: ArcProjectId,
        provider: &str,
        source_id: &str,
        version: i64,
    ) -> ArcRole {
        use crate::service::catalog_store::role::Role;
        let ident = Arc::new(RoleIdent::new(
            RoleProviderId::try_new(provider).unwrap(),
            RoleSourceId::try_new(source_id).unwrap(),
        ));
        Arc::new(Role {
            id: role_id,
            ident,
            name: format!("role-{role_id}"),
            description: None,
            project_id,
            created_at: chrono::Utc::now(),
            updated_at: None,
            version: RoleVersion::new(version),
        })
    }

    #[tokio::test]
    async fn test_ident_to_id_cache_has_ttl_matching_primary() {
        let primary_ttl = ROLE_CACHE.policy().time_to_live();
        let secondary_ttl = IDENT_TO_ID_CACHE.policy().time_to_live();
        assert_eq!(
            primary_ttl, secondary_ttl,
            "IDENT_TO_ID_CACHE TTL must match ROLE_CACHE TTL"
        );
        assert!(
            secondary_ttl.is_some(),
            "IDENT_TO_ID_CACHE must have a TTL configured"
        );
    }

    #[tokio::test]
    async fn test_role_cache_insert_and_get_by_id() {
        let role_id = RoleId::new_random();
        let project_id = Arc::new(ProjectId::new_random());
        let role = test_role(role_id, project_id, "lakekeeper", "test-source", 0);

        role_cache_insert(role.clone()).await;

        let cached = role_cache_get_by_id(role_id).await;
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().id(), role_id);
    }

    #[tokio::test]
    async fn test_role_cache_get_by_ident() {
        let role_id = RoleId::new_random();
        let project_id = Arc::new(ProjectId::new_random());
        let role = test_role(role_id, project_id, "lakekeeper", "test-ident-source", 0);

        role_cache_insert(role.clone()).await;

        let cached = role_cache_get_by_ident(role.project_id_arc(), role.ident_arc()).await;
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().id(), role_id);
    }

    #[tokio::test]
    async fn test_role_cache_get_by_ident_wrong_project() {
        let role_id = RoleId::new_random();
        let project_id = Arc::new(ProjectId::new_random());
        let other_project_id = Arc::new(ProjectId::new_random());
        let role = test_role(role_id, project_id, "lakekeeper", "wrong-project-source", 0);

        role_cache_insert(role.clone()).await;

        let cached = role_cache_get_by_ident(other_project_id, role.ident_arc()).await;
        assert!(cached.is_none());
    }

    #[tokio::test]
    async fn test_role_cache_invalidate() {
        let role_id = RoleId::new_random();
        let project_id = Arc::new(ProjectId::new_random());
        let role = test_role(role_id, project_id, "lakekeeper", "invalidate-source", 0);
        let project_id = role.project_id_arc();
        let ident = role.ident_arc();

        role_cache_insert(role.clone()).await;
        assert!(role_cache_get_by_id(role_id).await.is_some());

        role_cache_invalidate(role_id).await;

        // Give eviction listener time to run
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        assert!(role_cache_get_by_id(role_id).await.is_none());
        assert!(role_cache_get_by_ident(project_id, ident).await.is_none());
    }

    #[tokio::test]
    async fn test_role_cache_insert_older_version_ignored() {
        let role_id = RoleId::new_random();
        let project_id = Arc::new(ProjectId::new_random());

        let newer = test_role(role_id, project_id.clone(), "lakekeeper", "ver-source", 5);
        role_cache_insert(newer.clone()).await;

        let older = test_role(role_id, project_id, "lakekeeper", "ver-source", 3);
        role_cache_insert(older).await;

        // Newer version should still be in cache
        let cached = role_cache_get_by_id(role_id).await.unwrap();
        assert_eq!(*cached.version, 5);
    }

    #[tokio::test]
    async fn test_role_cache_ident_updated_on_ident_change() {
        let role_id = RoleId::new_random();
        let project_id = Arc::new(ProjectId::new_random());

        let v1 = test_role(role_id, project_id.clone(), "lakekeeper", "old-source", 0);
        let old_ident = v1.ident_arc();
        role_cache_insert(v1).await;

        // Update with a new ident (simulates set_role_source_system)
        let v2 = test_role(role_id, project_id.clone(), "lakekeeper", "new-source", 1);
        let new_ident = v2.ident_arc();
        role_cache_insert(v2).await;

        // Give eviction listener time to run
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // New ident should hit
        let cached = role_cache_get_by_ident(project_id.clone(), new_ident).await;
        assert!(cached.is_some());

        // Old ident should miss
        let cached_old = role_cache_get_by_ident(project_id, old_ident).await;
        assert!(cached_old.is_none());
    }

    /// `role_cache_get_or_load` must coalesce concurrent misses for the same id
    /// into ONE loader run, with every caller observing the cached entry.
    #[tokio::test]
    async fn role_get_or_load_coalesces_concurrent_misses() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let role_id = RoleId::new_random();
        let project_id = Arc::new(ProjectId::new_random());
        role_cache_invalidate(role_id).await;

        let loads = Arc::new(AtomicUsize::new(0));
        let role = test_role(role_id, project_id, "lakekeeper", "coalesce-source", 0);

        let mut handles = Vec::new();
        for _ in 0..32 {
            let loads = Arc::clone(&loads);
            let role = role.clone();
            handles.push(tokio::spawn(async move {
                role_cache_get_or_load(role_id, async move {
                    loads.fetch_add(1, Ordering::SeqCst);
                    // Widen the load window so all callers queue on the key lock
                    // before the first load completes.
                    for _ in 0..100 {
                        tokio::task::yield_now().await;
                    }
                    Ok::<_, std::convert::Infallible>(Some(role))
                })
                .await
            }));
        }

        let mut results = Vec::new();
        for h in handles {
            results.push(h.await.unwrap().unwrap().expect("role exists"));
        }

        assert_eq!(
            loads.load(Ordering::SeqCst),
            1,
            "concurrent misses must coalesce to a single loader run"
        );
        for r in &results[1..] {
            assert_eq!(r.id(), role_id);
        }

        role_cache_invalidate(role_id).await;
    }

    /// The in-closure version-gate must not let a slow loader overwrite a newer
    /// value cached concurrently. We model the race by having the loader itself
    /// insert a newer version (as a concurrent `role_cache_insert` would) before
    /// returning a stale older one — the helper must keep the newer entry and
    /// return it, never the stale load.
    #[tokio::test]
    async fn role_get_or_load_version_gate_keeps_newer_concurrent_insert() {
        let role_id = RoleId::new_random();
        let project_id = Arc::new(ProjectId::new_random());
        role_cache_invalidate(role_id).await;

        let newer = test_role(role_id, project_id.clone(), "lakekeeper", "vg-source", 5);
        let older = test_role(role_id, project_id, "lakekeeper", "vg-source", 3);

        let returned = role_cache_get_or_load(role_id, {
            let newer = newer.clone();
            let older = older.clone();
            async move {
                // A concurrent writer caches a newer version while we "load".
                role_cache_insert(newer).await;
                Ok::<_, std::convert::Infallible>(Some(older))
            }
        })
        .await
        .unwrap()
        .expect("role exists");

        assert_eq!(
            *returned.version, 5,
            "helper must return the newer concurrently-cached value, not the stale load"
        );
        assert_eq!(
            *role_cache_get_by_id(role_id).await.unwrap().version,
            5,
            "stale older load must be version-gated out of the cache"
        );

        role_cache_invalidate(role_id).await;
    }

    /// `role_ident_to_id_get_or_load` must coalesce concurrent by-ident misses
    /// into ONE loader run, with every caller resolving the same id. Runs on a
    /// multi-threaded runtime so coalescing is exercised under true parallelism,
    /// not just cooperative `yield_now` interleaving.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn role_ident_to_id_get_or_load_coalesces_concurrent_misses() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let role_id = RoleId::new_random();
        let project_id = Arc::new(ProjectId::new_random());
        role_cache_invalidate(role_id).await;

        let loads = Arc::new(AtomicUsize::new(0));
        let role = test_role(
            role_id,
            project_id.clone(),
            "lakekeeper",
            "ident-coalesce",
            0,
        );
        let ident = role.ident_arc();

        let mut handles = Vec::new();
        for _ in 0..32 {
            let loads = Arc::clone(&loads);
            let role = role.clone();
            let project_id = project_id.clone();
            let ident = ident.clone();
            handles.push(tokio::spawn(async move {
                role_ident_to_id_get_or_load(project_id, ident, async move {
                    loads.fetch_add(1, Ordering::SeqCst);
                    for _ in 0..100 {
                        tokio::task::yield_now().await;
                    }
                    Ok::<_, std::convert::Infallible>(Some(role))
                })
                .await
            }));
        }

        let mut results = Vec::new();
        for h in handles {
            results.push(h.await.unwrap().unwrap().expect("role exists"));
        }

        assert_eq!(
            loads.load(Ordering::SeqCst),
            1,
            "concurrent by-ident misses must coalesce to a single loader run"
        );
        for id in &results {
            assert_eq!(*id, role_id);
        }

        role_cache_invalidate(role_id).await;
    }
}
