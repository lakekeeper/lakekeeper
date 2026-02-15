use std::collections::HashSet;

use iceberg_ext::catalog::rest::ErrorModel;

use crate::{
    ProjectId,
    api::RequestMetadata,
    service::{
        authz::{
            AuthorizationBackendUnavailable, AuthorizationCountMismatch, Authorizer,
            BackendUnavailableOrCountMismatch, CannotInspectPermissions, CatalogProjectAction,
            MustUse, UserOrRole,
        },
        events::{
            AuthorizationFailureReason, AuthorizationFailureSource,
            delegate_authorization_failure_source,
        },
    },
};
pub trait ProjectAction
where
    Self: std::fmt::Display + Send + Sync + Copy + From<CatalogProjectAction> + PartialEq,
{
}

impl ProjectAction for CatalogProjectAction {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListProjectsResponse {
    /// List of projects that the user is allowed to see.
    Projects(HashSet<ProjectId>),
    /// The user is allowed to see all projects.
    All,
    /// Unsupported by the authorization backend.
    Unsupported,
}

// --------------------------- Errors ---------------------------
#[derive(Debug, PartialEq, Eq)]
pub struct AuthZProjectActionForbidden {
    project_id: ProjectId,
    action: String,
}
impl AuthZProjectActionForbidden {
    #[must_use]
    pub fn new(project_id: ProjectId, action: impl ProjectAction) -> Self {
        Self {
            project_id,
            action: action.to_string(),
        }
    }
}
impl AuthorizationFailureSource for AuthZProjectActionForbidden {
    fn into_error_model(self) -> ErrorModel {
        let AuthZProjectActionForbidden { project_id, action } = self;
        ErrorModel::forbidden(
            format!("Project action `{action}` forbidden on project `{project_id}`",),
            "ProjectActionForbidden",
            None,
        )
    }
    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        AuthorizationFailureReason::ActionForbidden
    }
}

// --------------------------- Return Error types ---------------------------
#[derive(Debug, derive_more::From)]
pub enum RequireProjectActionError {
    AuthZProjectActionForbidden(AuthZProjectActionForbidden),
    AuthorizationBackendUnavailable(AuthorizationBackendUnavailable),
    CannotInspectPermissions(CannotInspectPermissions),
    AuthorizationCountMismatch(AuthorizationCountMismatch),
}
impl From<BackendUnavailableOrCountMismatch> for RequireProjectActionError {
    fn from(err: BackendUnavailableOrCountMismatch) -> Self {
        match err {
            BackendUnavailableOrCountMismatch::AuthorizationBackendUnavailable(e) => e.into(),
            BackendUnavailableOrCountMismatch::CannotInspectPermissions(e) => e.into(),
            BackendUnavailableOrCountMismatch::AuthorizationCountMismatch(e) => e.into(),
        }
    }
}
delegate_authorization_failure_source!(RequireProjectActionError => {
    AuthZProjectActionForbidden,
    AuthorizationBackendUnavailable,
    CannotInspectPermissions,
    AuthorizationCountMismatch,
});

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

    async fn are_allowed_project_actions_vec<A: Into<Self::ProjectAction> + Send + Copy + Sync>(
        &self,
        metadata: &RequestMetadata,
        mut for_user: Option<&UserOrRole>,
        projects_with_actions: &[(&ProjectId, A)],
    ) -> Result<MustUse<Vec<bool>>, BackendUnavailableOrCountMismatch> {
        if metadata.actor().to_user_or_role().as_ref() == for_user {
            for_user = None;
        }

        Ok(MustUse::from(
            if metadata.has_admin_privileges() && for_user.is_none() {
                vec![true; projects_with_actions.len()]
            } else {
                let converted: Vec<(&ProjectId, Self::ProjectAction)> = projects_with_actions
                    .iter()
                    .map(|(id, action)| (*id, (*action).into()))
                    .collect();
                let decisions = self
                    .are_allowed_project_actions_impl(metadata, for_user, &converted)
                    .await?;

                if decisions.len() != projects_with_actions.len() {
                    return Err(AuthorizationCountMismatch::new(
                        projects_with_actions.len(),
                        decisions.len(),
                        "project",
                    )
                    .into());
                }

                decisions
            },
        ))
    }

    async fn are_allowed_project_actions_arr<
        const N: usize,
        A: Into<Self::ProjectAction> + Send + Copy + Sync,
    >(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        projects_with_actions: &[(&ProjectId, A); N],
    ) -> Result<MustUse<[bool; N]>, BackendUnavailableOrCountMismatch> {
        let result = self
            .are_allowed_project_actions_vec(metadata, for_user, projects_with_actions)
            .await?
            .into_inner();
        let n_returned = result.len();
        let arr: [bool; N] = result
            .try_into()
            .map_err(|_| AuthorizationCountMismatch::new(N, n_returned, "project"))?;
        Ok(MustUse::from(arr))
    }

    async fn is_allowed_project_action(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        project_id: &ProjectId,
        action: impl Into<Self::ProjectAction> + Send + Sync + Copy,
    ) -> Result<MustUse<bool>, BackendUnavailableOrCountMismatch> {
        let [decision] = self
            .are_allowed_project_actions_arr(metadata, for_user, &[(project_id, action)])
            .await?
            .into_inner();
        Ok(decision.into())
    }

    async fn require_project_action(
        &self,
        metadata: &RequestMetadata,
        project_id: &ProjectId,
        action: CatalogProjectAction,
    ) -> Result<(), RequireProjectActionError> {
        if self
            .is_allowed_project_action(metadata, None, project_id, action)
            .await?
            .into_inner()
        {
            Ok(())
        } else {
            Err(AuthZProjectActionForbidden::new(project_id.clone(), action).into())
        }
    }
}

impl<T> AuthZProjectOps for T where T: Authorizer {}
