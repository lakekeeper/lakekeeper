use std::str::FromStr;

use axum::{
    middleware::Next,
    response::{IntoResponse, Response},
};
use http::HeaderMap;
use limes::Authentication;
use uuid::Uuid;

use crate::{service::authn::Actor, ProjectIdent, DEFAULT_PROJECT_ID};

pub const PROJECT_ID_HEADER: &str = "x-project-ident";

/// A struct to hold metadata about a request.
#[derive(Debug, Clone)]
pub struct RequestMetadata {
    request_id: Uuid,
    project_id: Option<ProjectIdent>,
    authentication: Option<Authentication>,
    actor: Actor,
}

impl RequestMetadata {
    /// Set authentication information for the request.
    pub fn set_authentication(
        &mut self,
        actor: Actor,
        authentication: Authentication,
    ) -> &mut Self {
        self.actor = actor;
        self.authentication = Some(authentication);
        self
    }

    /// ID of the user performing the request.
    /// This returns the underlying user-id, even if a role is assumed.
    /// Please use `actor()` to get the full actor for `AuthZ` decisions.
    #[must_use]
    pub fn user_id(&self) -> Option<&crate::service::UserId> {
        match &self.actor {
            Actor::Principal(user_id) => Some(user_id),
            Actor::Role { principal, .. } => Some(principal),
            Actor::Anonymous => None,
        }
    }

    #[must_use]
    pub fn new_unauthenticated() -> Self {
        Self {
            request_id: Uuid::now_v7(),
            project_id: None,
            authentication: None,
            actor: Actor::Anonymous,
        }
    }

    #[must_use]
    pub fn preferred_project_id(&self) -> Option<ProjectIdent> {
        self.project_id.or(*DEFAULT_PROJECT_ID)
    }

    #[cfg(test)]
    #[must_use]
    pub fn random_human(user_id: crate::service::UserId) -> Self {
        Self {
            request_id: Uuid::now_v7(),
            authentication: Some(
                Authentication::builder()
                    .token_header(None)
                    .claims(serde_json::json!({}))
                    .subject(user_id.clone().into())
                    .name(Some("Test User".to_string()))
                    .email(None)
                    .principal_type(None)
                    .build(),
            ),
            actor: Actor::Principal(user_id),
            project_id: None,
        }
    }

    #[must_use]
    pub fn actor(&self) -> &Actor {
        &self.actor
    }

    #[must_use]
    pub fn authentication(&self) -> Option<&Authentication> {
        self.authentication.as_ref()
    }

    #[must_use]
    pub fn request_id(&self) -> Uuid {
        self.request_id
    }

    #[must_use]
    pub fn is_authenticated(&self) -> bool {
        self.actor.is_authenticated()
    }

    /// Determine the Project ID, return an error if none is provided.
    ///
    /// Resolution order:
    /// 1. User-provided project ID
    /// 2. Project ID from headers
    /// 3. Default project ID
    ///
    /// # Errors
    /// Fails if none of the above methods provide a project ID.
    pub fn require_project_id(
        &self,
        user_project: Option<ProjectIdent>, // Explicitly requested via an API parameter
    ) -> crate::api::Result<ProjectIdent> {
        user_project.or(self.preferred_project_id()).ok_or_else(|| {
            crate::api::ErrorModel::bad_request(
                format!("No project provided. Please provide the `{PROJECT_ID_HEADER}` header"),
                "NoProjectIdProvided",
                None,
            )
            .into()
        })
    }
}

#[cfg(feature = "router")]
/// Initializes request metadata with a random request ID as an axum Extension.
/// Does not authenticate the request.
///
/// Run this middleware before running [`auth_middleware_fn`](crate::service::authn::auth_middleware_fn).
pub(crate) async fn create_request_metadata_with_trace_and_project_fn(
    headers: HeaderMap,
    mut request: axum::extract::Request,
    next: Next,
) -> Response {
    let request_id: Uuid = headers
        .get("x-request-id")
        .and_then(|hv| {
            hv.to_str()
                .map(Uuid::from_str)
                .ok()
                .transpose()
                .ok()
                .flatten()
        })
        .unwrap_or(Uuid::now_v7());
    let project_id = headers
        .get(PROJECT_ID_HEADER)
        .and_then(|hv| hv.to_str().ok())
        .map(ProjectIdent::from_str)
        .transpose();
    let project_id = match project_id {
        Ok(ident) => ident,
        Err(err) => return err.into_response(),
    };
    request.extensions_mut().insert(RequestMetadata {
        request_id,
        authentication: None,
        actor: Actor::Anonymous,
        project_id,
    });
    next.run(request).await
}
