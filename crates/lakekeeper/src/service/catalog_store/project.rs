use super::{CatalogStore, WriteTransaction};
use crate::{
    ProjectId,
    service::{ArcProjectId, Result, cache_invalidation::CacheKey},
};

#[derive(Debug, Clone)]
pub struct GetProjectResponse {
    /// ID of the project.
    pub project_id: ArcProjectId,
    /// Name of the project.
    pub name: String,
}

#[async_trait::async_trait]
pub trait CatalogProjectOps
where
    Self: CatalogStore,
{
    /// Delete a project. Records the assignments of the users whose role-provider sync
    /// records the delete removed.
    async fn delete_project(
        project_id: &ProjectId,
        transaction: &mut WriteTransaction<Self>,
    ) -> Result<()> {
        let synced_users = Self::delete_project_impl(project_id, transaction.transaction()).await?;
        for user_id in synced_users {
            transaction.record(CacheKey::UserAssignments(user_id));
        }
        Ok(())
    }
}

impl<T> CatalogProjectOps for T where T: CatalogStore {}
