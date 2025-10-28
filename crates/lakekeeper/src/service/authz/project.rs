use std::collections::HashSet;

use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};

use crate::{
    api::RequestMetadata,
    service::{
        authz::{AuthorizationBackendUnavailable, Authorizer, CatalogProjectAction, MustUse},
        Actor,
    },
    ProjectId,
};
pub trait ProjectAction
where
    Self: std::fmt::Display + Send + Sync + Copy + From<CatalogProjectAction> + PartialEq,
{
}

impl ProjectAction for CatalogProjectAction {}

#[derive(Debug, Clone, PartialEq)]
pub enum ListProjectsResponse {
    /// List of projects that the user is allowed to see.
    Projects(HashSet<ProjectId>),
    /// The user is allowed to see all projects.
    All,
}

// --------------------------- Errors ---------------------------

#[derive(Debug, PartialEq, Eq)]
pub struct AuthZProjectActionForbidden {
    project_id: ProjectId,
    action: String,
    actor: Actor,
}
impl AuthZProjectActionForbidden {
    #[must_use]
    pub fn new(project_id: ProjectId, action: impl ProjectAction, actor: Actor) -> Self {
        Self {
            project_id,
            action: action.to_string(),
            actor,
        }
    }
}
impl From<AuthZProjectActionForbidden> for ErrorModel {
    fn from(err: AuthZProjectActionForbidden) -> Self {
        let AuthZProjectActionForbidden {
            project_id,
            action,
            actor,
        } = err;
        ErrorModel::forbidden(
            format!("Project action `{action}` forbidden for {actor} on project `{project_id}`",),
            "ProjectActionForbidden",
            None,
        )
    }
}
impl From<AuthZProjectActionForbidden> for IcebergErrorResponse {
    fn from(err: AuthZProjectActionForbidden) -> Self {
        ErrorModel::from(err).into()
    }
}

// --------------------------- Return Error types ---------------------------
#[derive(Debug, derive_more::From)]
pub enum RequireProjectActionError {
    AuthZProjectActionForbidden(AuthZProjectActionForbidden),
    AuthorizationBackendUnavailable(AuthorizationBackendUnavailable),
}
impl From<RequireProjectActionError> for ErrorModel {
    fn from(err: RequireProjectActionError) -> Self {
        match err {
            RequireProjectActionError::AuthZProjectActionForbidden(e) => e.into(),
            RequireProjectActionError::AuthorizationBackendUnavailable(e) => e.into(),
        }
    }
}
impl From<RequireProjectActionError> for IcebergErrorResponse {
    fn from(err: RequireProjectActionError) -> Self {
        ErrorModel::from(err).into()
    }
}

#[async_trait::async_trait]
pub trait AuthZProjectOps: Authorizer {
    async fn list_projects(
        &self,
        metadata: &RequestMetadata,
    ) -> Result<ListProjectsResponse, AuthorizationBackendUnavailable> {
        if metadata.has_admin_privileges() {
            Ok(ListProjectsResponse::All)
        } else {
            self.list_projects_impl(metadata).await
        }
    }

    async fn is_allowed_project_action(
        &self,
        metadata: &RequestMetadata,
        project_id: &ProjectId,
        action: impl Into<Self::ProjectAction> + Send,
    ) -> Result<MustUse<bool>, AuthorizationBackendUnavailable> {
        if metadata.has_admin_privileges() {
            Ok(true)
        } else {
            self.is_allowed_project_action_impl(metadata, project_id, action.into())
                .await
        }
        .map(MustUse::from)
    }

    async fn require_project_action(
        &self,
        metadata: &RequestMetadata,
        project_id: &ProjectId,
        action: CatalogProjectAction,
    ) -> Result<(), RequireProjectActionError> {
        if self
            .is_allowed_project_action(metadata, project_id, action)
            .await?
            .into_inner()
        {
            Ok(())
        } else {
            Err(AuthZProjectActionForbidden::new(
                project_id.clone(),
                action,
                metadata.actor().clone(),
            )
            .into())
        }
    }
}

impl<T> AuthZProjectOps for T where T: Authorizer {}
