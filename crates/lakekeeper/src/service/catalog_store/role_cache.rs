use std::{sync::LazyLock, time::Duration};

use axum_prometheus::metrics;
use moka::{future::Cache, notification::RemovalCause};

#[cfg(feature = "router")]
use crate::service::events::{self, EventListener};
use crate::{
    CONFIG,
    service::{ArcProjectId, ArcRole, ArcRoleIdent, RoleId},
};

const METRIC_ROLE_CACHE_SIZE: &str = "lakekeeper_role_cache_size";
const METRIC_ROLE_CACHE_HITS: &str = "lakekeeper_role_cache_hits_total";
const METRIC_ROLE_CACHE_MISSES: &str = "lakekeeper_role_cache_misses_total";

/// Initialize metric descriptions for Role cache metrics
static METRICS_INITIALIZED: LazyLock<()> = LazyLock::new(|| {
    metrics::describe_gauge!(
        METRIC_ROLE_CACHE_SIZE,
        "Current number of entries in the role cache"
    );
    metrics::describe_counter!(METRIC_ROLE_CACHE_HITS, "Total number of role cache hits");
    metrics::describe_counter!(
        METRIC_ROLE_CACHE_MISSES,
        "Total number of role cache misses"
    );
});

// Primary cache: RoleId → ArcRole
pub(crate) static ROLE_CACHE: LazyLock<Cache<RoleId, ArcRole>> = LazyLock::new(|| {
    Cache::builder()
        .max_capacity(CONFIG.cache.role.capacity)
        .initial_capacity(100)
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
            .build()
    });

#[allow(dead_code)] // Not required for all features
pub(crate) async fn role_cache_invalidate(role_id: RoleId) {
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

/// Update the cache size metric with the current number of entries
#[inline]
#[allow(clippy::cast_precision_loss)]
fn update_cache_size_metric() {
    let () = &*METRICS_INITIALIZED; // Ensure metrics are described
    metrics::gauge!(METRIC_ROLE_CACHE_SIZE, "cache_type" => "role")
        .set(ROLE_CACHE.entry_count() as f64);
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

pub(super) async fn role_cache_get_by_ident(
    project_id: ArcProjectId,
    ident: ArcRoleIdent,
) -> Option<ArcRole> {
    update_cache_size_metric();
    let ident_key = (project_id, ident.clone());
    let Some(role_id) = IDENT_TO_ID_CACHE.get(&ident_key).await else {
        metrics::counter!(METRIC_ROLE_CACHE_MISSES, "cache_type" => "role").increment(1);
        return None;
    };
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
        metrics::counter!(METRIC_ROLE_CACHE_MISSES, "cache_type" => "role").increment(1);
        None
    }
}

#[cfg(feature = "router")]
#[derive(Debug, Clone)]
pub(crate) struct RoleCacheEventListener;

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
}
