use std::{sync::Arc, time::Duration};

use axum_prometheus::metrics;
use moka::future::Cache;

#[cfg(feature = "router")]
use crate::service::events::{self, EventListener};
use crate::{
    CONFIG,
    service::{
        RoleId,
        authn::UserId,
        catalog_store::role_assignment::{ListRoleMembersResult, ListUserRoleAssignmentsResult},
    },
};

// ============================================================================
// User assignments cache  (UserId → Arc<ListUserRoleAssignmentsResult>)
// ============================================================================

const METRIC_UA_SIZE: &str = "lakekeeper_user_assignments_cache_size";
const METRIC_UA_HITS: &str = "lakekeeper_user_assignments_cache_hits_total";
const METRIC_UA_MISSES: &str = "lakekeeper_user_assignments_cache_misses_total";

static UA_METRICS_INITIALIZED: std::sync::LazyLock<()> = std::sync::LazyLock::new(|| {
    metrics::describe_gauge!(
        METRIC_UA_SIZE,
        "Current number of entries in the user-assignments cache"
    );
    metrics::describe_counter!(
        METRIC_UA_HITS,
        "Total number of user-assignments cache hits"
    );
    metrics::describe_counter!(
        METRIC_UA_MISSES,
        "Total number of user-assignments cache misses"
    );
});

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
    update_ua_size_metric();
    if let Some(result) = USER_ASSIGNMENTS_CACHE.get(user_id).await {
        tracing::debug!("User assignments for {user_id} found in cache");
        metrics::counter!(METRIC_UA_HITS).increment(1);
        Some(result)
    } else {
        metrics::counter!(METRIC_UA_MISSES).increment(1);
        None
    }
}

#[allow(dead_code)] // Not required for all features
pub(crate) async fn user_assignments_cache_invalidate(user_id: &UserId) {
    if CONFIG.cache.user_assignments.enabled {
        tracing::debug!("Invalidating user assignments for {user_id} from cache");
        USER_ASSIGNMENTS_CACHE.invalidate(user_id).await;
        update_ua_size_metric();
    }
}

#[inline]
#[allow(clippy::cast_precision_loss)]
fn update_ua_size_metric() {
    let () = &*UA_METRICS_INITIALIZED;
    metrics::gauge!(METRIC_UA_SIZE).set(USER_ASSIGNMENTS_CACHE.entry_count() as f64);
}

// ============================================================================
// Role members cache  (RoleId → Arc<ListRoleMembersResult>)
// ============================================================================

const METRIC_RM_SIZE: &str = "lakekeeper_role_members_cache_size";
const METRIC_RM_HITS: &str = "lakekeeper_role_members_cache_hits_total";
const METRIC_RM_MISSES: &str = "lakekeeper_role_members_cache_misses_total";

static RM_METRICS_INITIALIZED: std::sync::LazyLock<()> = std::sync::LazyLock::new(|| {
    metrics::describe_gauge!(
        METRIC_RM_SIZE,
        "Current number of entries in the role-members cache"
    );
    metrics::describe_counter!(METRIC_RM_HITS, "Total number of role-members cache hits");
    metrics::describe_counter!(
        METRIC_RM_MISSES,
        "Total number of role-members cache misses"
    );
});

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
    update_rm_size_metric();
    if let Some(result) = ROLE_MEMBERS_CACHE.get(&role_id).await {
        tracing::debug!("Role members for {role_id} found in cache");
        metrics::counter!(METRIC_RM_HITS).increment(1);
        Some(result)
    } else {
        metrics::counter!(METRIC_RM_MISSES).increment(1);
        None
    }
}

#[allow(dead_code)] // Not required for all features
pub(crate) async fn role_members_cache_invalidate(role_id: RoleId) {
    if CONFIG.cache.role_members.enabled {
        tracing::debug!("Invalidating role members for {role_id} from cache");
        ROLE_MEMBERS_CACHE.invalidate(&role_id).await;
        update_rm_size_metric();
    }
}

#[inline]
#[allow(clippy::cast_precision_loss)]
fn update_rm_size_metric() {
    let () = &*RM_METRICS_INITIALIZED;
    metrics::gauge!(METRIC_RM_SIZE).set(ROLE_MEMBERS_CACHE.entry_count() as f64);
}

// ============================================================================
// Event listener
// ============================================================================

/// Hooks into entity-lifecycle events that write paths cannot predict.
///
/// The sync write paths (`sync_role_members_by_ident`,
/// `sync_user_role_assignments_by_provider`) call the invalidation functions
/// directly — no event roundtrip needed for same-process invalidation.
#[cfg(feature = "router")]
#[derive(Debug, Clone)]
pub(crate) struct RoleAssignmentsCacheEventListener;

#[cfg(feature = "router")]
impl std::fmt::Display for RoleAssignmentsCacheEventListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RoleAssignmentsCacheEventListener")
    }
}

#[cfg(feature = "router")]
#[async_trait::async_trait]
impl EventListener for RoleAssignmentsCacheEventListener {
    /// Invalidate the role-members cache entry when the role is deleted.
    ///
    /// `USER_ASSIGNMENTS_CACHE` entries that reference this `role_id` expire
    /// naturally within `user_assignments.time_to_live_secs` (which must be
    /// ≤ `role.time_to_live_secs`, bounding the stale window).
    async fn role_deleted(&self, event: events::DeleteRoleEvent) -> anyhow::Result<()> {
        role_members_cache_invalidate(event.role.id()).await;
        Ok(())
    }

    /// Invalidate the role-members cache entry when role metadata changes.
    ///
    /// A metadata update can change `role_ident`, which is embedded in
    /// `AssignedRole` values inside `USER_ASSIGNMENTS_CACHE`. Those user
    /// entries can only be expired via TTL (scanning all users is not feasible
    /// without a reverse index).
    async fn role_updated(&self, event: events::UpdateRoleEvent) -> anyhow::Result<()> {
        role_members_cache_invalidate(event.role.id()).await;
        Ok(())
    }

    /// Populate the role-members cache and invalidate stale user-assignment
    /// entries after a role's member list has been synced.
    ///
    /// The event carries the complete, authoritative post-sync member list so
    /// this listener can **insert** into `ROLE_MEMBERS_CACHE` rather than just
    /// invalidating it — benefiting both the syncing instance and any other
    /// instance that receives this event via an event bus.
    ///
    /// Every user whose assignment changed now has a stale
    /// `USER_ASSIGNMENTS_CACHE` entry, so we invalidate those eagerly.
    async fn role_members_synced(
        &self,
        event: events::RoleMembersSyncedEvent,
    ) -> anyhow::Result<()> {
        role_members_cache_insert(event.result.role_id, event.result).await;
        for user_id in event.added.iter().chain(event.removed.iter()) {
            user_assignments_cache_invalidate(user_id).await;
        }
        Ok(())
    }

    /// Populate the user-assignments cache and invalidate stale role-member
    /// entries after a user's role assignments have been synced.
    ///
    /// The event carries the complete, authoritative post-sync assignment list
    /// (all providers merged) so this listener can **insert** into
    /// `USER_ASSIGNMENTS_CACHE` rather than just invalidating it — benefiting
    /// both the syncing instance and any other instance that receives this
    /// event via an event bus.
    ///
    /// Every role whose membership changed now has a stale `ROLE_MEMBERS_CACHE`
    /// entry, so we invalidate those eagerly.
    async fn user_role_assignments_synced(
        &self,
        event: events::UserRoleAssignmentsSyncedEvent,
    ) -> anyhow::Result<()> {
        user_assignments_cache_insert(&event.user_id, event.result).await;
        for role_id in event.added.iter().chain(event.removed.iter()) {
            role_members_cache_invalidate(*role_id).await;
        }
        Ok(())
    }
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
