use crate::ProjectId;

#[derive(Debug, Clone)]
pub struct GetProjectResponse {
    /// ID of the project.
    pub project_id: ProjectId,
    /// Name of the project.
    pub name: String,
}
