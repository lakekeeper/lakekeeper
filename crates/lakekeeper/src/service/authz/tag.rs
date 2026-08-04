use iceberg_ext::catalog::rest::ErrorModel;

use crate::{
    api::RequestMetadata,
    service::{
        CatalogBackendError, TagDefinition, TagDefinitionId, TagDefinitionIdNotFound,
        authz::{
            AuthorizationBackendUnavailable, AuthorizationCountMismatch, AuthorizationDecision,
            Authorizer, AuthzBadRequest, BackendUnavailableOrCountMismatch,
            CannotInspectPermissions, CatalogAction, CatalogTagAction, IsAllowedActionError,
            MustUse, UserOrRole,
        },
        events::{
            AuthorizationFailureReason, AuthorizationFailureSource,
            delegate_authorization_failure_source,
        },
    },
};

pub trait TagAction
where
    Self: CatalogAction + Send + Sync + Clone + From<CatalogTagAction> + PartialEq,
{
}

impl TagAction for CatalogTagAction {}

// --------------------------- Errors ---------------------------

#[derive(Debug, PartialEq, Eq)]
pub struct AuthZCannotSeeTag {
    tag_definition_id: TagDefinitionId,
    /// Whether the resource was confirmed not to exist (for audit logging).
    /// The HTTP response is deliberately ambiguous, but the audit log is concrete.
    internal_resource_not_found: bool,
    internal_server_stack: Vec<String>,
}
impl AuthZCannotSeeTag {
    #[must_use]
    pub fn new(
        tag_definition_id: TagDefinitionId,
        resource_not_found: bool,
        error_stack: Vec<String>,
    ) -> Self {
        Self {
            tag_definition_id,
            internal_resource_not_found: resource_not_found,
            internal_server_stack: error_stack,
        }
    }
    #[must_use]
    pub fn new_not_found(tag_definition_id: TagDefinitionId) -> Self {
        Self::new(tag_definition_id, true, vec![])
    }
    #[must_use]
    pub fn new_forbidden(tag_definition_id: TagDefinitionId) -> Self {
        Self::new(tag_definition_id, false, vec![])
    }
}
impl AuthorizationFailureSource for AuthZCannotSeeTag {
    fn into_error_model(self) -> ErrorModel {
        let AuthZCannotSeeTag {
            tag_definition_id,
            internal_resource_not_found: _,
            internal_server_stack,
        } = self;
        TagDefinitionIdNotFound::new(tag_definition_id)
            .append_detail("Tag not found or access denied")
            .append_details(internal_server_stack)
            .into()
    }
    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        if self.internal_resource_not_found {
            AuthorizationFailureReason::ResourceNotFound
        } else {
            AuthorizationFailureReason::CannotSeeResource
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct AuthZTagActionForbidden {
    tag_definition_id: TagDefinitionId,
    action: String,
}
impl AuthZTagActionForbidden {
    #[must_use]
    pub fn new(tag_definition_id: TagDefinitionId, action: &impl TagAction) -> Self {
        Self {
            tag_definition_id,
            action: action.action_descriptor().log_string(),
        }
    }
}
impl AuthorizationFailureSource for AuthZTagActionForbidden {
    fn into_error_model(self) -> ErrorModel {
        let AuthZTagActionForbidden {
            tag_definition_id,
            action,
        } = self;
        ErrorModel::forbidden(
            format!("Tag action `{action}` forbidden on tag definition `{tag_definition_id}`"),
            "TagActionForbidden",
            None,
        )
    }
    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        AuthorizationFailureReason::ActionForbidden
    }
}

// --------------------------- Return Error type ---------------------------
#[derive(Debug, derive_more::From)]
pub enum RequireTagActionError {
    AuthZTagActionForbidden(AuthZTagActionForbidden),
    AuthorizationBackendUnavailable(AuthorizationBackendUnavailable),
    CannotInspectPermissions(CannotInspectPermissions),
    AuthorizationCountMismatch(AuthorizationCountMismatch),
    AuthorizerValidationFailed(AuthzBadRequest),
    // Hide the existence of the tag definition.
    AuthZCannotSeeTag(AuthZCannotSeeTag),
    // Propagated directly.
    CatalogBackendError(CatalogBackendError),
}
impl From<BackendUnavailableOrCountMismatch> for RequireTagActionError {
    fn from(err: BackendUnavailableOrCountMismatch) -> Self {
        match err {
            BackendUnavailableOrCountMismatch::AuthorizationBackendUnavailable(e) => e.into(),
            BackendUnavailableOrCountMismatch::AuthorizationCountMismatch(e) => e.into(),
        }
    }
}
impl From<IsAllowedActionError> for RequireTagActionError {
    fn from(err: IsAllowedActionError) -> Self {
        match err {
            IsAllowedActionError::AuthorizationBackendUnavailable(e) => e.into(),
            IsAllowedActionError::CannotInspectPermissions(e) => e.into(),
            IsAllowedActionError::BadRequest(e) => e.into(),
            IsAllowedActionError::CountMismatch(e) => e.into(),
        }
    }
}
delegate_authorization_failure_source!(RequireTagActionError => {
    AuthZTagActionForbidden,
    AuthorizationBackendUnavailable,
    CannotInspectPermissions,
    AuthorizationCountMismatch,
    AuthZCannotSeeTag,
    CatalogBackendError,
    AuthorizerValidationFailed
});

#[async_trait::async_trait]
pub trait AuthZTagOps: Authorizer {
    /// Fold the catalog get-result into the loaded definition, hiding non-existence
    /// (and cross-project absence) behind a not-found.
    fn require_tag_presence(
        &self,
        tag_definition_id: TagDefinitionId,
        tag: Result<Option<TagDefinition>, CatalogBackendError>,
    ) -> Result<TagDefinition, RequireTagActionError> {
        let tag = tag?;
        tag.ok_or_else(|| AuthZCannotSeeTag::new_not_found(tag_definition_id).into())
    }

    async fn is_allowed_tag_action(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        tag: &TagDefinition,
        action: impl Into<Self::TagAction> + Send + Clone + Sync,
    ) -> Result<MustUse<bool>, IsAllowedActionError> {
        let [decision] = self
            .are_allowed_tag_actions_arr(metadata, for_user, &[(tag, action)])
            .await?
            .into_inner();
        Ok(decision.into())
    }

    async fn are_allowed_tag_actions_vec<A: Into<Self::TagAction> + Send + Clone + Sync>(
        &self,
        metadata: &RequestMetadata,
        mut for_user: Option<&UserOrRole>,
        tags_with_actions: &[(&TagDefinition, A)],
    ) -> Result<MustUse<Vec<AuthorizationDecision>>, IsAllowedActionError> {
        if metadata.actor().to_user_or_role().as_ref() == for_user {
            for_user = None;
        }
        if metadata.bypasses_control_plane_authz(for_user) {
            Ok(vec![
                AuthorizationDecision::allow();
                tags_with_actions.len()
            ])
        } else {
            let converted = tags_with_actions
                .iter()
                .map(|(tag, action)| (*tag, action.clone().into()))
                .collect::<Vec<_>>();
            let decisions = self
                .are_allowed_tag_actions_impl(metadata, for_user, &converted)
                .await?;

            if decisions.len() != tags_with_actions.len() {
                return Err(AuthorizationCountMismatch::new(
                    tags_with_actions.len(),
                    decisions.len(),
                    "tag",
                )
                .into());
            }

            Ok(decisions)
        }
        .map(MustUse::from)
    }

    async fn are_allowed_tag_actions_arr<
        const N: usize,
        A: Into<Self::TagAction> + Send + Clone + Sync,
    >(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        tags_with_actions: &[(&TagDefinition, A); N],
    ) -> Result<MustUse<[bool; N]>, IsAllowedActionError> {
        let result = self
            .are_allowed_tag_actions_vec(metadata, for_user, tags_with_actions)
            .await?
            .into_allowed();
        let n_returned = result.len();
        let arr: [bool; N] = result
            .try_into()
            .map_err(|_| AuthorizationCountMismatch::new(N, n_returned, "tag"))?;
        Ok(MustUse::from(arr))
    }

    /// Fold presence, then authorize `action` on the loaded definition.
    async fn require_tag_action(
        &self,
        metadata: &RequestMetadata,
        tag_definition_id: TagDefinitionId,
        tag: Result<Option<TagDefinition>, CatalogBackendError>,
        action: impl Into<Self::TagAction> + Send,
    ) -> Result<TagDefinition, RequireTagActionError> {
        let tag = self.require_tag_presence(tag_definition_id, tag)?;

        let action = action.into();
        if self
            .is_allowed_tag_action(metadata, None, &tag, action.clone())
            .await?
            .into_inner()
        {
            Ok(tag)
        } else {
            Err(AuthZTagActionForbidden::new(tag.tag_definition_id, &action).into())
        }
    }
}

impl<T> AuthZTagOps for T where T: Authorizer {}
