use crate::service::ArcProjectId;

#[derive(Debug, Clone)]
pub struct GetProjectResponse {
    /// ID of the project.
    pub project_id: ArcProjectId,
    /// Name of the project.
    pub name: String,
}
