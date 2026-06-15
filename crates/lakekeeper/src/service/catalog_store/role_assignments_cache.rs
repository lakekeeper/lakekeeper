use std::{sync::Arc, time::Duration};

use axum_prometheus::metrics;
use moka::{
    future::Cache,
    ops::compute::{CompResult, Op},
};

use crate::{
    CONFIG,
    service::{
        ArcProjectId, ArcRoleIdent, CatalogBackendError, RoleId,
        authn::UserId,
        cache_metrics,
        cache_ttl::JitteredTtl,
        catalog_store::role_assignment::{ListRoleMembersResult, ListUserRoleAssignmentsResult},
    },
};

// ============================================================================
// User assignments cache  (UserId → Arc<ListUserRoleAssignmentsResult>)
// ============================================================================

const CACHE_TYPE_UA: &str = "user_assignments";

/// Hot path: one entry per active user.
///
/// Value is `Arc`-wrapped so every caller receives a pointer clone — O(1) —
/// rather than a deep copy of the `Vec<AssignedRole>`.
pub(crate) static USER_ASSIGNMENTS_CACHE: std::sync::LazyLock<
    Cache<UserId, Arc<ListUserRoleAssignmentsResult>>,
> = std::sync::LazyLock::new(|| {
    Cache::builder()
        .max_capacity(CONFIG.cache.user_assignments.capacity)
        .initial_capacity(1_000)
        .time_to_live(Duration::from_secs(
            CONFIG.cache.user_assignments.time_to_live_secs,
        ))
        .expire_after(JitteredTtl::with_default_jitter(Duration::from_secs(
            CONFIG.cache.user_assignments.time_to_live_secs,
        )))
        .build()
});

pub(crate) async fn user_assignments_cache_insert(
    user_id: &UserId,
    result: Arc<ListUserRoleAssignmentsResult>,
) {
    if CONFIG.cache.user_assignments.enabled {
        tracing::debug!("Inserting user assignments for {user_id} into cache");
        USER_ASSIGNMENTS_CACHE.insert(user_id.clone(), result).await;
        update_ua_size_metric();
    }
}

pub(crate) async fn user_assignments_cache_get(
    user_id: &UserId,
) -> Option<Arc<ListUserRoleAssignmentsResult>> {
    if !CONFIG.cache.user_assignments.enabled {
        return None;
    }
    update_ua_size_metric();
    if let Some(result) = USER_ASSIGNMENTS_CACHE.get(user_id).await {
        tracing::debug!("User assignments for {user_id} found in cache");
        metrics::counter!(cache_metrics::METRIC_CACHE_HITS_TOTAL, "cache_type" => CACHE_TYPE_UA)
            .increment(1);
        Some(result)
    } else {
        metrics::counter!(cache_metrics::METRIC_CACHE_MISSES_TOTAL, "cache_type" => CACHE_TYPE_UA)
            .increment(1);
        None
    }
}

#[allow(dead_code)] // Not required for all features
pub(crate) async fn user_assignments_cache_invalidate(user_id: &UserId) {
    if CONFIG.cache.user_assignments.enabled {
        tracing::debug!("Invalidating user assignments for {user_id} from cache");
        // Remove via the loader's per-key compute lock (`Op::Remove`), not a bare
        // `invalidate()`: a bare invalidate is a different moka lock domain, so one
        // landing mid-load is a no-op and the loader's later insert resurrects the
        // revoked entry until TTL. `Op::Remove` orders this post-commit removal after
        // any in-flight load's insert. See `user_assignments_cache_get_or_load`.
        USER_ASSIGNMENTS_CACHE
            .entry(user_id.clone())
            .and_compute_with(|_| async { Op::Remove })
            .await;
        update_ua_size_metric();
    }
}

/// Invalidate the user-assignments cache entry for every user in `user_ids`.
///
/// Convenience over calling [`user_assignments_cache_invalidate`] in a loop —
/// used when a single mutation (e.g. a `role_membership` edge change) makes the
/// effective-role list of a whole set of users stale at once.
pub(crate) async fn user_assignments_cache_invalidate_many(user_ids: &[UserId]) {
    if !CONFIG.cache.user_assignments.enabled {
        return;
    }
    for user_id in user_ids {
        tracing::debug!("Invalidating user assignments for {user_id} from cache");
        // Compute-based removal — serializes with the loader. See
        // `user_assignments_cache_invalidate`.
        USER_ASSIGNMENTS_CACHE
            .entry(user_id.clone())
            .and_compute_with(|_| async { Op::Remove })
            .await;
    }
    update_ua_size_metric();
}

#[inline]
#[allow(clippy::cast_precision_loss)]
fn update_ua_size_metric() {
    let () = &*cache_metrics::METRICS_INITIALIZED;
    metrics::gauge!(cache_metrics::METRIC_CACHE_SIZE, "cache_type" => CACHE_TYPE_UA)
        .set(USER_ASSIGNMENTS_CACHE.entry_count() as f64);
}

/// Single-flight read-through for the user-assignments cache.
///
/// Concurrent misses for the same `user_id` coalesce onto one loader run; hit/miss
/// metrics and the `enabled` flag are preserved; errors are never cached (returned
/// by value, never poisoning the entry).
///
/// Uses `and_try_compute_with`, not `try_get_with`, deliberately: moka holds the
/// per-key compute lock across the `load` await, and `user_assignments_cache_invalidate`
/// removes through that *same* lock (`Op::Remove`). So a revocation racing an
/// in-flight load can't be lost — it is serialized after this loader's insert
/// (cleaning it up) or before it starts. `try_get_with` removed in a different lock
/// domain, so the loader's insert resurrected the revoked grant until TTL.
pub(super) async fn user_assignments_cache_get_or_load<Fut>(
    user_id: &UserId,
    load: Fut,
) -> Result<Arc<ListUserRoleAssignmentsResult>, CatalogBackendError>
where
    Fut: std::future::Future<
            Output = Result<Arc<ListUserRoleAssignmentsResult>, CatalogBackendError>,
        > + Send,
{
    if !CONFIG.cache.user_assignments.enabled {
        return load.await;
    }

    // Fast path: a hit returns the stored `Arc`. Reuses `user_assignments_cache_get`
    // so hit/miss metrics and the size gauge stay in one place.
    if let Some(cached) = user_assignments_cache_get(user_id).await {
        return Ok(cached);
    }

    // Miss (already counted by the get above): coalesce concurrent loaders for this
    // key and hold the per-key compute lock across the load, so a racing invalidate
    // is serialized against us rather than lost (see the doc comment).
    let outcome = USER_ASSIGNMENTS_CACHE
        .entry(user_id.clone())
        .and_try_compute_with(|maybe_entry| async move {
            if maybe_entry.is_some() {
                // Populated by another caller while we waited on the key lock.
                return Ok::<_, CatalogBackendError>(Op::Nop);
            }
            Ok(Op::Put(load.await?))
        })
        .await?;
    update_ua_size_metric();

    Ok(match outcome {
        CompResult::Inserted(entry)
        | CompResult::ReplacedWith(entry)
        | CompResult::Unchanged(entry) => entry.into_value(),
        // Unreachable: the closure only emits `Put` (→ Inserted) or `Nop` with an
        // existing entry (→ Unchanged). Re-read defensively rather than panic.
        CompResult::StillNone(_) | CompResult::Removed(_) => {
            user_assignments_cache_get(user_id).await.ok_or_else(|| {
                CatalogBackendError::new_unexpected(std::io::Error::other(
                    "user-assignments cache compute returned no entry",
                ))
            })?
        }
    })
}

// ============================================================================
// Shared identity pools — dedup Arc<RoleIdent>/Arc<ProjectId> across cached entries
// ============================================================================
//
// The effective-roles loader allocates a fresh `Arc` for each (user, role) row,
// so without sharing a role held by N users keeps N copies of its identity alive.
// Sharing collapses those to one `Arc`. Content-addressed: the key is the `Arc`
// itself, whose `Hash`/`Eq` delegate to the inner value, so a rename produces a
// new ident → new key → self-heals, and stale (renamed/deleted) idents age out by
// idle-eviction — no invalidation hook required. The key clone and the stored
// value share one allocation. The shared `Arc` is strong: the canonical instance
// stays alive via cached entries even if the pool evicts the key, so eviction
// only transiently reduces sharing, never correctness.
//
// Sizing is INTERNAL, not an operator knob, and deliberately NOT tied to the
// role-by-id cache (`cache.role`): the pools serve `USER_ASSIGNMENTS_CACHE`, so
// their idle-TTL tracks *that* cache's TTL, and their capacity is a fixed bound on
// distinct identities in play. Above the capacity, dedup degrades (cold idents
// are LRU-evicted and re-allocated on the next load) but is never incorrect. We
// keep `moka` (sharded, lock-free reads) rather than a hand-rolled weak-value map
// precisely so this stays uncontended under the lazy per-user role-provider sync
// load. `share_identities` is gated on `user_assignments.enabled`, so when that
// cache is off the pools are never populated.
//
// Future option (deferred): if true self-sizing (no fixed cap) is ever wanted,
// swap these for weak-value pools whose canonical `Arc`s are kept alive by the
// cached entries. That design needs a periodic dead-`Weak` sweep under a lock —
// if that sweep (or the lock) ever becomes a bottleneck, shard it (an array of
// locked maps keyed by hash, or a `DashMap`). Not needed now: `moka` is already
// sharded/concurrent, and the fixed cap only degrades dedup, never correctness.

/// Upper bound on distinct shared identities. Generous — far above any realistic
/// distinct-role count (the role-by-id cache defaults to 10k). Exceeding it only
/// degrades dedup, never correctness, so it is a fixed internal constant rather
/// than an operator-facing knob.
const MAX_SHARED_IDENTITIES: u64 = 100_000;

const CACHE_TYPE_SHARED_ROLE_IDENTS: &str = "shared_role_idents";
const CACHE_TYPE_SHARED_PROJECT_IDS: &str = "shared_project_ids";

static SHARED_ROLE_IDENTS: std::sync::LazyLock<Cache<ArcRoleIdent, ArcRoleIdent>> =
    std::sync::LazyLock::new(|| {
        Cache::builder()
            .max_capacity(MAX_SHARED_IDENTITIES)
            .time_to_idle(Duration::from_secs(
                CONFIG.cache.user_assignments.time_to_live_secs,
            ))
            .build()
    });

static SHARED_PROJECT_IDS: std::sync::LazyLock<Cache<ArcProjectId, ArcProjectId>> =
    std::sync::LazyLock::new(|| {
        Cache::builder()
            .max_capacity(MAX_SHARED_IDENTITIES)
            .time_to_idle(Duration::from_secs(
                CONFIG.cache.user_assignments.time_to_live_secs,
            ))
            .build()
    });

async fn share_role_ident(ident: ArcRoleIdent) -> ArcRoleIdent {
    SHARED_ROLE_IDENTS
        .get_with(Arc::clone(&ident), async move { ident })
        .await
}

async fn share_project_id(project_id: ArcProjectId) -> ArcProjectId {
    SHARED_PROJECT_IDS
        .get_with(Arc::clone(&project_id), async move { project_id })
        .await
}

/// Replace the per-row `Arc<RoleIdent>` / `Arc<ProjectId>` in a freshly-loaded
/// user-assignments result with shared `Arc`s before it is cached, so a
/// role/project referenced by many users is stored once in memory. No-op when the
/// user-assignments cache is disabled (nothing is cached → nothing to dedup).
pub(super) async fn share_identities(result: &mut ListUserRoleAssignmentsResult) {
    if !CONFIG.cache.user_assignments.enabled {
        return;
    }
    for role in &mut result.roles {
        role.role_ident = share_role_ident(Arc::clone(&role.role_ident)).await;
        role.project_id = share_project_id(Arc::clone(&role.project_id)).await;
    }
    for sync in &mut result.provider_sync_times {
        sync.project_id = share_project_id(Arc::clone(&sync.project_id)).await;
    }
    update_shared_identity_metrics();
}

/// Gauge the shared-identity pools' entry counts so dedup can be confirmed in
/// prod: compare these against `cache_size{cache_type="user_assignments"}` — a
/// small pool size relative to UA entries means a role/project held by many users
/// is stored once. Reuses the shared `lakekeeper_cache_size` gauge with dedicated
/// `cache_type` labels. `entry_count()` is approximate until moka drains pending
/// tasks (same caveat the UA/RM gauges already accept).
#[inline]
#[allow(clippy::cast_precision_loss)]
fn update_shared_identity_metrics() {
    let () = &*cache_metrics::METRICS_INITIALIZED;
    metrics::gauge!(cache_metrics::METRIC_CACHE_SIZE, "cache_type" => CACHE_TYPE_SHARED_ROLE_IDENTS)
        .set(SHARED_ROLE_IDENTS.entry_count() as f64);
    metrics::gauge!(cache_metrics::METRIC_CACHE_SIZE, "cache_type" => CACHE_TYPE_SHARED_PROJECT_IDS)
        .set(SHARED_PROJECT_IDS.entry_count() as f64);
}

// ============================================================================
// Role members cache  (RoleId → Arc<ListRoleMembersResult>)
// ============================================================================

const CACHE_TYPE_RM: &str = "role_members";

/// Cold path: one entry per queried role. `RoleId` is `Copy` (UUID).
///
/// Value is `Arc`-wrapped because each entry may hold an arbitrarily large
/// `Vec<AssignedUser>`.
pub(crate) static ROLE_MEMBERS_CACHE: std::sync::LazyLock<
    Cache<RoleId, Arc<ListRoleMembersResult>>,
> = std::sync::LazyLock::new(|| {
    Cache::builder()
        .max_capacity(CONFIG.cache.role_members.capacity)
        .initial_capacity(100)
        .time_to_live(Duration::from_secs(
            CONFIG.cache.role_members.time_to_live_secs,
        ))
        .expire_after(JitteredTtl::with_default_jitter(Duration::from_secs(
            CONFIG.cache.role_members.time_to_live_secs,
        )))
        .build()
});

pub(crate) async fn role_members_cache_insert(role_id: RoleId, result: Arc<ListRoleMembersResult>) {
    if CONFIG.cache.role_members.enabled {
        tracing::debug!("Inserting role members for {role_id} into cache");
        ROLE_MEMBERS_CACHE.insert(role_id, result).await;
        update_rm_size_metric();
    }
}

pub(crate) async fn role_members_cache_get(role_id: RoleId) -> Option<Arc<ListRoleMembersResult>> {
    if !CONFIG.cache.role_members.enabled {
        return None;
    }
    update_rm_size_metric();
    if let Some(result) = ROLE_MEMBERS_CACHE.get(&role_id).await {
        tracing::debug!("Role members for {role_id} found in cache");
        metrics::counter!(cache_metrics::METRIC_CACHE_HITS_TOTAL, "cache_type" => CACHE_TYPE_RM)
            .increment(1);
        Some(result)
    } else {
        metrics::counter!(cache_metrics::METRIC_CACHE_MISSES_TOTAL, "cache_type" => CACHE_TYPE_RM)
            .increment(1);
        None
    }
}

#[allow(dead_code)] // Not required for all features
pub(crate) async fn role_members_cache_invalidate(role_id: RoleId) {
    if CONFIG.cache.role_members.enabled {
        tracing::debug!("Invalidating role members for {role_id} from cache");
        // Compute-based removal — serializes with the loader's per-key lock so a
        // racing in-flight load can't resurrect the entry. See
        // `user_assignments_cache_invalidate`.
        ROLE_MEMBERS_CACHE
            .entry(role_id)
            .and_compute_with(|_| async { Op::Remove })
            .await;
        update_rm_size_metric();
    }
}

#[inline]
#[allow(clippy::cast_precision_loss)]
fn update_rm_size_metric() {
    let () = &*cache_metrics::METRICS_INITIALIZED;
    metrics::gauge!(cache_metrics::METRIC_CACHE_SIZE, "cache_type" => CACHE_TYPE_RM)
        .set(ROLE_MEMBERS_CACHE.entry_count() as f64);
}

/// Single-flight read-through for the role-members cache.
///
/// On a miss, concurrent requests for the same `role_id` are **coalesced**: moka
/// serializes the per-key compute, so the loader runs once and later callers
/// observe the just-inserted entry instead of re-loading. Unlike the
/// user-assignments read-through this one returns `Option` — a non-existent role
/// yields `None` and is **not** negative-cached (the entry stays absent). The
/// `enabled` flag and hit/miss metrics are preserved; when caching is disabled
/// the loader runs directly. `and_try_compute_with` returns the loader error by
/// value (no `Arc`-sharing), so no wrapping or cloning is needed.
pub(super) async fn role_members_cache_get_or_load<Fut>(
    role_id: RoleId,
    load: Fut,
) -> Result<Option<Arc<ListRoleMembersResult>>, CatalogBackendError>
where
    Fut: std::future::Future<
            Output = Result<Option<Arc<ListRoleMembersResult>>, CatalogBackendError>,
        > + Send,
{
    if !CONFIG.cache.role_members.enabled {
        return load.await;
    }

    // Fast path: a hit returns the stored `Arc` and keeps the hit/miss metrics in
    // `role_members_cache_get`.
    if let Some(cached) = role_members_cache_get(role_id).await {
        return Ok(Some(cached));
    }

    // Miss: coalesce concurrent loaders for this key. moka holds a per-key lock
    // across the compute, so only the first caller runs `load`; the rest see the
    // entry it inserted and skip straight to it.
    let outcome = ROLE_MEMBERS_CACHE
        .entry(role_id)
        .and_try_compute_with(|maybe_entry| async move {
            if maybe_entry.is_some() {
                // Populated by another caller while we waited on the key lock.
                return Ok(Op::Nop);
            }
            match load.await {
                Ok(Some(value)) => Ok(Op::Put(value)),
                // Absent role — never negative-cached. Coalescing therefore
                // applies only to a found role; concurrent lookups of a missing
                // one each re-run the loader (rare, and no worse than before).
                Ok(None) => Ok(Op::Nop),
                Err(e) => Err(e),
            }
        })
        .await?;
    update_rm_size_metric();

    Ok(match outcome {
        CompResult::Inserted(entry)
        | CompResult::ReplacedWith(entry)
        | CompResult::Unchanged(entry) => Some(entry.into_value()),
        // `StillNone` = absent (loader returned `None`). `Removed` is unreachable
        // here — the closure only returns `Nop`/`Put`, never `Remove`.
        CompResult::StillNone(_) | CompResult::Removed(_) => None,
    })
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{
        ProjectId,
        service::{
            ArcProjectId, RoleId, RoleIdent, RoleProviderId,
            authn::UserId,
            catalog_store::role_assignment::{
                AssignedRole, AssignedUser, ListRoleMembersResult, ListUserRoleAssignmentsResult,
                UserProviderSyncInfo,
            },
            identifier::role::{ArcRoleIdent, RoleSourceId},
        },
    };

    fn test_user_id(s: &str) -> UserId {
        serde_json::from_str(&format!(r#""oidc~{s}""#)).unwrap()
    }

    fn test_role_ident(provider: &str, source: &str) -> ArcRoleIdent {
        Arc::new(RoleIdent::new(
            RoleProviderId::try_new(provider).unwrap(),
            RoleSourceId::try_new(source).unwrap(),
        ))
    }

    fn empty_user_result() -> Arc<ListUserRoleAssignmentsResult> {
        Arc::new(ListUserRoleAssignmentsResult {
            roles: vec![],
            provider_sync_times: vec![],
        })
    }

    fn user_result_with_role(
        role_id: RoleId,
        project_id: ArcProjectId,
        role_ident: ArcRoleIdent,
    ) -> Arc<ListUserRoleAssignmentsResult> {
        Arc::new(ListUserRoleAssignmentsResult {
            roles: vec![AssignedRole {
                role_id,
                role_ident,
                project_id,
            }],
            provider_sync_times: vec![],
        })
    }

    fn empty_role_result(role_id: RoleId) -> Arc<ListRoleMembersResult> {
        Arc::new(ListRoleMembersResult {
            role_id,
            project_id: Arc::new(ProjectId::new_random()),
            role_ident: test_role_ident("lakekeeper", "empty"),
            members: vec![],
            last_synced_at: None,
        })
    }

    fn role_result_with_members(
        role_id: RoleId,
        user_ids: Vec<UserId>,
    ) -> Arc<ListRoleMembersResult> {
        Arc::new(ListRoleMembersResult {
            role_id,
            project_id: Arc::new(ProjectId::new_random()),
            role_ident: test_role_ident("lakekeeper", "with-members"),
            members: user_ids
                .into_iter()
                .map(|user_id| AssignedUser {
                    user_id: Arc::new(user_id),
                })
                .collect(),
            last_synced_at: Some(chrono::Utc::now()),
        })
    }

    // ── User assignments ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_user_assignments_insert_and_get() {
        let user_id = test_user_id("insert-get");
        user_assignments_cache_insert(&user_id, empty_user_result()).await;

        let cached = user_assignments_cache_get(&user_id).await;
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().roles.len(), 0);
    }

    #[tokio::test]
    async fn test_user_assignments_miss() {
        let user_id = test_user_id("never-inserted-ua");
        assert!(user_assignments_cache_get(&user_id).await.is_none());
    }

    #[tokio::test]
    async fn test_user_assignments_invalidate() {
        let user_id = test_user_id("invalidate-ua");
        user_assignments_cache_insert(&user_id, empty_user_result()).await;
        assert!(user_assignments_cache_get(&user_id).await.is_some());

        user_assignments_cache_invalidate(&user_id).await;
        assert!(user_assignments_cache_get(&user_id).await.is_none());
    }

    /// Regression: a revocation racing an in-flight loader must win — no resurrecting
    /// the revoked entry. Deterministic: the loader holds the key's compute lock
    /// across its `await`, so the invalidate's `Op::Remove` is ordered after the
    /// loader's insert and removes it. (Before the fix the loader used `try_get_with`
    /// and the invalidate a bare, different-lock-domain `invalidate()` — a no-op
    /// mid-load, and the insert resurrected the grant until TTL.)
    #[tokio::test]
    async fn invalidate_wins_over_in_flight_user_assignments_loader() {
        use tokio::sync::oneshot;

        let user_id = test_user_id("race-invalidate-vs-loader");
        // Clean slate — the cache is a process-global static shared across tests.
        user_assignments_cache_invalidate(&user_id).await;

        let (started_tx, started_rx) = oneshot::channel::<()>();
        let (release_tx, release_rx) = oneshot::channel::<()>();

        let stale = user_result_with_role(
            RoleId::new_random(),
            Arc::new(ProjectId::new_random()),
            test_role_ident("lakekeeper", "stale-grant"),
        );
        let stale_for_loader = Arc::clone(&stale);

        // The loader runs inside the compute closure (holding the key lock). It
        // signals once mid-flight, then blocks until the test releases it before
        // returning the now-stale snapshot.
        let uid = user_id.clone();
        let loader = tokio::spawn(async move {
            let load = async move {
                started_tx.send(()).unwrap();
                release_rx.await.unwrap();
                Ok(stale_for_loader)
            };
            user_assignments_cache_get_or_load(&uid, load).await
        });

        // Once the loader holds the key lock, fire the revocation. Its `Op::Remove`
        // queues behind the loader on the same key lock.
        started_rx.await.unwrap();
        let inv_uid = user_id.clone();
        let invalidate = tokio::spawn(async move {
            user_assignments_cache_invalidate(&inv_uid).await;
        });

        release_tx.send(()).unwrap();
        let returned = loader.await.unwrap().expect("loader succeeds");
        invalidate.await.unwrap();

        // The loader still returns its snapshot to *its* caller (a read that raced a
        // write — acceptable) ...
        assert_eq!(returned.roles.len(), 1);
        // ... but the cache must NOT retain the revoked grant: the invalidate won.
        assert!(
            user_assignments_cache_get(&user_id).await.is_none(),
            "revoked grant was resurrected by the racing loader"
        );
    }

    /// Same race, for `ROLE_MEMBERS`: its loader was already compute-based, so this
    /// guards that the invalidate change (`Op::Remove`) serializes with it.
    #[tokio::test]
    async fn invalidate_wins_over_in_flight_role_members_loader() {
        use tokio::sync::oneshot;

        let role_id = RoleId::new_random();
        role_members_cache_invalidate(role_id).await;

        let (started_tx, started_rx) = oneshot::channel::<()>();
        let (release_tx, release_rx) = oneshot::channel::<()>();

        let stale = role_result_with_members(role_id, vec![test_user_id("stale-member")]);
        let stale_for_loader = Arc::clone(&stale);

        let loader = tokio::spawn(async move {
            let load = async move {
                started_tx.send(()).unwrap();
                release_rx.await.unwrap();
                Ok(Some(stale_for_loader))
            };
            role_members_cache_get_or_load(role_id, load).await
        });

        started_rx.await.unwrap();
        let invalidate = tokio::spawn(async move {
            role_members_cache_invalidate(role_id).await;
        });

        release_tx.send(()).unwrap();
        let returned = loader.await.unwrap().expect("loader succeeds");
        invalidate.await.unwrap();

        assert_eq!(returned.unwrap().members.len(), 1);
        assert!(
            role_members_cache_get(role_id).await.is_none(),
            "removed role-members entry was resurrected by the racing loader"
        );
    }

    #[tokio::test]
    async fn test_user_assignments_get_returns_same_arc() {
        let user_id = test_user_id("arc-check-ua");
        let role_id = RoleId::new_random();
        let project_id = Arc::new(ProjectId::new_random());
        let role_ident = test_role_ident("lakekeeper", "arc-source");
        let result = user_result_with_role(role_id, project_id, role_ident);

        user_assignments_cache_insert(&user_id, Arc::clone(&result)).await;
        let cached = user_assignments_cache_get(&user_id).await.unwrap();

        // Only the Arc counter was bumped — no heap allocation.
        assert!(Arc::ptr_eq(&result, &cached));
    }

    /// A result with `provider_sync_times` populated but no roles must survive
    /// a cache round-trip intact — this is the "synced but no assignments" shape.
    #[tokio::test]
    async fn test_user_assignments_sync_without_roles() {
        let user_id = test_user_id("sync-no-roles");
        let provider_id = RoleProviderId::try_new("oidc").unwrap();
        let project_id = Arc::new(ProjectId::new_random());
        let synced_at = chrono::Utc::now();

        let result = Arc::new(ListUserRoleAssignmentsResult {
            roles: vec![],
            provider_sync_times: vec![UserProviderSyncInfo {
                project_id: Arc::clone(&project_id),
                provider_id: provider_id.clone(),
                synced_at,
            }],
        });

        user_assignments_cache_insert(&user_id, Arc::clone(&result)).await;
        let cached = user_assignments_cache_get(&user_id).await.unwrap();

        assert_eq!(cached.roles.len(), 0, "no roles");
        assert_eq!(
            cached.provider_sync_times.len(),
            1,
            "sync record must survive cache round-trip"
        );
        assert_eq!(cached.provider_sync_times[0].provider_id, provider_id);
        assert_eq!(cached.provider_sync_times[0].synced_at, synced_at);
    }

    #[tokio::test]
    async fn test_user_assignments_overwrite() {
        let user_id = test_user_id("overwrite-ua");
        let role_id = RoleId::new_random();
        let project_id = Arc::new(ProjectId::new_random());
        let role_ident = test_role_ident("lakekeeper", "overwrite-src");

        user_assignments_cache_insert(&user_id, empty_user_result()).await;
        let rich = user_result_with_role(role_id, project_id, role_ident);
        user_assignments_cache_insert(&user_id, Arc::clone(&rich)).await;

        let cached = user_assignments_cache_get(&user_id).await.unwrap();
        assert_eq!(cached.roles.len(), 1);
    }

    /// Two independently-loaded results referencing the same role/project value
    /// (distinct `Arc` allocations) must, after sharing, collapse to ONE canonical
    /// `Arc` — the dedup that stops a role held by many users from storing its
    /// identity once per user.
    #[tokio::test]
    async fn share_identities_dedups_shared_identity_across_results() {
        let role_id = RoleId::new_random();
        let project = ProjectId::new_random();
        let mk = || ListUserRoleAssignmentsResult {
            roles: vec![AssignedRole {
                role_id,
                role_ident: test_role_ident("lakekeeper", "share-dedup-src"),
                project_id: Arc::new(project.clone()),
            }],
            provider_sync_times: vec![],
        };
        let mut a = mk();
        let mut b = mk();
        // Independently allocated before sharing.
        assert!(!Arc::ptr_eq(&a.roles[0].role_ident, &b.roles[0].role_ident));
        assert!(!Arc::ptr_eq(&a.roles[0].project_id, &b.roles[0].project_id));

        share_identities(&mut a).await;
        share_identities(&mut b).await;

        assert!(
            Arc::ptr_eq(&a.roles[0].role_ident, &b.roles[0].role_ident),
            "same role-ident value must dedup to one shared Arc"
        );
        assert!(
            Arc::ptr_eq(&a.roles[0].project_id, &b.roles[0].project_id),
            "same project-id value must dedup to one shared Arc"
        );
    }

    /// Distinct ident VALUES must not merge — a renamed role (new ident) gets a
    /// fresh canonical, so the content-addressed pool self-heals across renames.
    #[tokio::test]
    async fn share_identities_keeps_distinct_idents_separate() {
        let role_id = RoleId::new_random();
        let project = Arc::new(ProjectId::new_random());
        let mut before = ListUserRoleAssignmentsResult {
            roles: vec![AssignedRole {
                role_id,
                role_ident: test_role_ident("lakekeeper", "share-rename-before"),
                project_id: Arc::clone(&project),
            }],
            provider_sync_times: vec![],
        };
        let mut after = ListUserRoleAssignmentsResult {
            roles: vec![AssignedRole {
                role_id,
                role_ident: test_role_ident("lakekeeper", "share-rename-after"),
                project_id: Arc::clone(&project),
            }],
            provider_sync_times: vec![],
        };
        share_identities(&mut before).await;
        share_identities(&mut after).await;
        assert!(
            !Arc::ptr_eq(&before.roles[0].role_ident, &after.roles[0].role_ident),
            "different ident values must not be deduped together"
        );
    }

    /// `provider_sync_times` project ids are shared too, collapsing to the canonical
    /// `Arc` of a role carrying the same project value.
    #[tokio::test]
    async fn share_identities_dedups_provider_sync_project_id() {
        let project = ProjectId::new_random();
        let mut result = ListUserRoleAssignmentsResult {
            roles: vec![AssignedRole {
                role_id: RoleId::new_random(),
                role_ident: test_role_ident("lakekeeper", "share-sync-proj"),
                project_id: Arc::new(project.clone()),
            }],
            provider_sync_times: vec![UserProviderSyncInfo {
                project_id: Arc::new(project.clone()),
                provider_id: RoleProviderId::try_new("oidc").unwrap(),
                synced_at: chrono::Utc::now(),
            }],
        };
        assert!(!Arc::ptr_eq(
            &result.roles[0].project_id,
            &result.provider_sync_times[0].project_id
        ));
        share_identities(&mut result).await;
        assert!(
            Arc::ptr_eq(
                &result.roles[0].project_id,
                &result.provider_sync_times[0].project_id
            ),
            "provider_sync_times project_id must share the same canonical Arc"
        );
    }

    /// Single-flight: N concurrent misses for the same key run the loader
    /// **exactly once**, and every caller receives the same `Arc`. Guards against
    /// regressing to the per-caller get-load-insert path (a per-replica
    /// thundering herd on hot keys).
    #[tokio::test]
    async fn user_assignments_get_or_load_coalesces_concurrent_misses() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let user_id = test_user_id("single-flight-coalesce");
        // Guarantee a cold key (other tests share the process-wide cache).
        user_assignments_cache_invalidate(&user_id).await;

        let loads = Arc::new(AtomicUsize::new(0));
        let value = user_result_with_role(
            RoleId::new_random(),
            Arc::new(ProjectId::new_random()),
            test_role_ident("lakekeeper", "single-flight"),
        );

        let mut handles = Vec::new();
        for _ in 0..32 {
            let loads = Arc::clone(&loads);
            let uid = user_id.clone();
            let value = Arc::clone(&value);
            handles.push(tokio::spawn(async move {
                user_assignments_cache_get_or_load(&uid, async move {
                    loads.fetch_add(1, Ordering::SeqCst);
                    // Widen the miss window so every caller races in before the
                    // first load completes — without coalescing this forces N
                    // loader runs (the behaviour this guards against).
                    for _ in 0..100 {
                        tokio::task::yield_now().await;
                    }
                    Ok::<_, CatalogBackendError>(value)
                })
                .await
            }));
        }

        let mut results = Vec::new();
        for h in handles {
            results.push(h.await.unwrap().expect("loader succeeds"));
        }

        assert_eq!(
            loads.load(Ordering::SeqCst),
            1,
            "concurrent misses must coalesce to a single loader run"
        );
        for r in &results[1..] {
            assert!(
                Arc::ptr_eq(&results[0], r),
                "every caller must receive the same coalesced Arc"
            );
        }

        // Clean up the shared cache so we don't leak state into other tests.
        user_assignments_cache_invalidate(&user_id).await;
    }

    /// A failed load must not poison the entry: every caller observes the error and
    /// nothing is cached, so a later success still populates. Errors are NOT
    /// coalesced — `and_try_compute_with` inserts nothing on `Err`, so each
    /// serialized caller re-runs the load (consistent with every compute-based cache;
    /// the old `try_get_with` shared one failing load — traded away to serialize the
    /// loader against invalidation).
    #[tokio::test]
    async fn user_assignments_get_or_load_does_not_cache_errors() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        const CALLERS: usize = 16;
        let user_id = test_user_id("single-flight-error");
        user_assignments_cache_invalidate(&user_id).await;

        let loads = Arc::new(AtomicUsize::new(0));

        // Concurrent callers whose loader fails. They serialize on the key's compute
        // lock; since `Err` caches nothing, each one re-runs the failing load.
        let mut handles = Vec::new();
        for _ in 0..CALLERS {
            let loads = Arc::clone(&loads);
            let uid = user_id.clone();
            handles.push(tokio::spawn(async move {
                user_assignments_cache_get_or_load(&uid, async move {
                    loads.fetch_add(1, Ordering::SeqCst);
                    for _ in 0..100 {
                        tokio::task::yield_now().await;
                    }
                    Err::<Arc<ListUserRoleAssignmentsResult>, _>(
                        CatalogBackendError::new_unexpected(std::io::Error::other("boom")),
                    )
                })
                .await
            }));
        }

        for h in handles {
            assert!(
                h.await.unwrap().is_err(),
                "every caller observes the failure"
            );
        }
        assert_eq!(
            loads.load(Ordering::SeqCst),
            CALLERS,
            "errors are not negative-cached, so each serialized caller re-runs the failing load"
        );
        assert!(
            user_assignments_cache_get(&user_id).await.is_none(),
            "a failed load must not poison the entry"
        );

        // A subsequent successful load populates the cache as usual.
        let value = user_result_with_role(
            RoleId::new_random(),
            Arc::new(ProjectId::new_random()),
            test_role_ident("lakekeeper", "after-error"),
        );
        let loaded = user_assignments_cache_get_or_load(&user_id, {
            let value = Arc::clone(&value);
            async move { Ok::<_, CatalogBackendError>(value) }
        })
        .await
        .expect("loader succeeds after a prior failure");
        assert!(Arc::ptr_eq(&loaded, &value));
        assert!(user_assignments_cache_get(&user_id).await.is_some());

        user_assignments_cache_invalidate(&user_id).await;
    }

    /// `role_members_cache_get_or_load` must coalesce concurrent misses for the
    /// same role into ONE loader run, with every caller receiving the same `Arc`.
    /// Mirrors the user-assignments single-flight guard, but this read-through
    /// returns `Option` — a present role coalesces; a non-existent one must not
    /// be negative-cached (covered separately below).
    #[tokio::test]
    async fn role_members_get_or_load_coalesces_concurrent_misses() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let role_id = RoleId::new_random();
        role_members_cache_invalidate(role_id).await;

        let loads = Arc::new(AtomicUsize::new(0));
        let value = role_result_with_members(role_id, vec![test_user_id("rm-coalesce")]);

        let mut handles = Vec::new();
        for _ in 0..32 {
            let loads = Arc::clone(&loads);
            let value = Arc::clone(&value);
            handles.push(tokio::spawn(async move {
                role_members_cache_get_or_load(role_id, async move {
                    loads.fetch_add(1, Ordering::SeqCst);
                    for _ in 0..100 {
                        tokio::task::yield_now().await;
                    }
                    Ok::<_, CatalogBackendError>(Some(value))
                })
                .await
            }));
        }

        let mut results = Vec::new();
        for h in handles {
            results.push(
                h.await
                    .unwrap()
                    .expect("loader succeeds")
                    .expect("role exists"),
            );
        }

        assert_eq!(
            loads.load(Ordering::SeqCst),
            1,
            "concurrent misses must coalesce to a single loader run"
        );
        for r in &results[1..] {
            assert!(
                Arc::ptr_eq(&results[0], r),
                "every caller must receive the same coalesced Arc"
            );
        }

        role_members_cache_invalidate(role_id).await;
    }

    /// A non-existent role (loader returns `None`) must NOT be negative-cached:
    /// after a `None` load the entry stays absent, so a later real insert is
    /// visible immediately rather than shadowed until TTL.
    #[tokio::test]
    async fn role_members_get_or_load_does_not_negative_cache() {
        let role_id = RoleId::new_random();
        role_members_cache_invalidate(role_id).await;

        let missing = role_members_cache_get_or_load(role_id, async { Ok(None) })
            .await
            .expect("loader succeeds");
        assert!(missing.is_none(), "non-existent role resolves to None");
        assert!(
            role_members_cache_get(role_id).await.is_none(),
            "None must not be cached"
        );

        // A subsequent successful load populates the cache as usual.
        let value = role_result_with_members(role_id, vec![test_user_id("rm-late")]);
        let loaded = role_members_cache_get_or_load(role_id, {
            let value = Arc::clone(&value);
            async move { Ok::<_, CatalogBackendError>(Some(value)) }
        })
        .await
        .expect("loader succeeds")
        .expect("role now exists");
        assert!(Arc::ptr_eq(&loaded, &value));
        assert!(role_members_cache_get(role_id).await.is_some());

        role_members_cache_invalidate(role_id).await;
    }

    // ── Role members ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_role_members_insert_and_get() {
        let role_id = RoleId::new_random();
        role_members_cache_insert(role_id, empty_role_result(role_id)).await;

        let cached = role_members_cache_get(role_id).await;
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().members.len(), 0);
    }

    #[tokio::test]
    async fn test_role_members_miss() {
        let role_id = RoleId::new_random();
        assert!(role_members_cache_get(role_id).await.is_none());
    }

    #[tokio::test]
    async fn test_role_members_invalidate() {
        let role_id = RoleId::new_random();
        role_members_cache_insert(role_id, empty_role_result(role_id)).await;
        assert!(role_members_cache_get(role_id).await.is_some());

        role_members_cache_invalidate(role_id).await;
        assert!(role_members_cache_get(role_id).await.is_none());
    }

    #[tokio::test]
    async fn test_role_members_get_returns_same_arc() {
        let role_id = RoleId::new_random();
        let result = role_result_with_members(
            role_id,
            vec![test_user_id("member-1"), test_user_id("member-2")],
        );

        role_members_cache_insert(role_id, Arc::clone(&result)).await;
        let cached = role_members_cache_get(role_id).await.unwrap();

        assert!(Arc::ptr_eq(&result, &cached));
    }

    /// A result with `last_synced_at: Some(...)` but no members must survive
    /// a cache round-trip intact — this is the "synced but no members" shape.
    #[tokio::test]
    async fn test_role_members_sync_without_members() {
        let role_id = RoleId::new_random();
        let synced_at = chrono::Utc::now();

        let result = Arc::new(ListRoleMembersResult {
            role_id,
            project_id: Arc::new(ProjectId::new_random()),
            role_ident: test_role_ident("ldap", "empty-group"),
            members: vec![],
            last_synced_at: Some(synced_at),
        });

        role_members_cache_insert(role_id, Arc::clone(&result)).await;
        let cached = role_members_cache_get(role_id).await.unwrap();

        assert_eq!(cached.members.len(), 0, "no members");
        assert_eq!(
            cached.last_synced_at,
            Some(synced_at),
            "last_synced_at must survive cache round-trip even with no members"
        );
    }

    #[tokio::test]
    async fn test_role_members_different_roles_are_independent() {
        let role_a = RoleId::new_random();
        let role_b = RoleId::new_random();

        role_members_cache_insert(
            role_a,
            role_result_with_members(role_a, vec![test_user_id("user-a")]),
        )
        .await;
        role_members_cache_insert(
            role_b,
            role_result_with_members(role_b, vec![test_user_id("user-b"), test_user_id("user-c")]),
        )
        .await;

        assert_eq!(
            role_members_cache_get(role_a).await.unwrap().members.len(),
            1
        );
        assert_eq!(
            role_members_cache_get(role_b).await.unwrap().members.len(),
            2
        );

        role_members_cache_invalidate(role_a).await;
        assert!(role_members_cache_get(role_a).await.is_none());
        assert!(role_members_cache_get(role_b).await.is_some());
    }

    // ── Independence between the two caches ───────────────────────────────────

    #[tokio::test]
    async fn test_caches_are_independent() {
        let user_id = test_user_id("independent-cross");
        let role_id = RoleId::new_random();

        user_assignments_cache_insert(&user_id, empty_user_result()).await;
        role_members_cache_insert(role_id, empty_role_result(role_id)).await;

        user_assignments_cache_invalidate(&user_id).await;

        assert!(role_members_cache_get(role_id).await.is_some());
        assert!(user_assignments_cache_get(&user_id).await.is_none());
    }
}
