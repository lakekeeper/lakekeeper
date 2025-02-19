use std::{fmt::Debug, str::FromStr};

use axum::{
    extract::{Request, State},
    middleware::Next,
    response::{IntoResponse, Response},
};
use axum_extra::{
    headers::{authorization::Bearer, Authorization},
    TypedHeader,
};
use http::{HeaderMap, StatusCode};
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
use limes::{format_subject, parse_subject, Authenticator, Subject};
use serde::{Deserialize, Serialize};

use super::{authz::Authorizer, RoleId};
use crate::{
    api::{self},
    request_metadata::RequestMetadata,
};

pub const IDP_SEPARATOR: char = '~';
pub const ASSUME_ROLE_HEADER: &str = "x-assume-role";

#[derive(Debug, Clone, PartialEq, Eq, strum_macros::Display)]
pub enum Actor {
    Anonymous,
    #[strum(to_string = "Principal({0})")]
    Principal(UserId),
    #[strum(to_string = "AssumedRole({assumed_role}) by Principal({principal})")]
    Role {
        principal: UserId,
        assumed_role: RoleId,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct AuthMiddlewareState<T: Authenticator, A: Authorizer> {
    pub authenticator: T,
    pub authorizer: A,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserId(Subject);

/// Use a limes [`Authenticator`] to Authenticate a request.
///
/// Run this middleware after running [`create_request_metadata_with_trace_and_project_fn`](crate::request_metadata::create_request_metadata_with_trace_and_project_fn),
/// to support external trace ids.
pub(crate) async fn auth_middleware_fn<T: Authenticator, A: Authorizer>(
    State(state): State<AuthMiddlewareState<T, A>>,
    authorization: Option<TypedHeader<Authorization<Bearer>>>,
    headers: HeaderMap,
    mut request: Request,
    next: Next,
) -> Response {
    let authenticator = &state.authenticator;
    let authorizer = &state.authorizer;
    let Some(authorization) = authorization else {
        tracing::debug!("Missing authorization header");
        return (StatusCode::UNAUTHORIZED, "Missing authorization header").into_response();
    };

    let authentication = match authenticator.authenticate(authorization.token()).await {
        Ok(principal) => principal,
        Err(e) => {
            tracing::debug!("Failed to authenticate: {}", e);
            return (StatusCode::UNAUTHORIZED, "Failed to authenticate").into_response();
        }
    };
    let user_id = match UserId::try_new(authentication.subject().clone()) {
        Ok(user_id) => user_id,
        Err(e) => {
            tracing::error!(
                "Unexpected subject id in token - failed to create UserID from Subject: {}",
                e
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Unexpected subject id format",
            )
                .into_response();
        }
    };
    let role_id = match extract_role_id(&headers) {
        Ok(role_id) => role_id,
        Err(e) => return e.into_response(),
    };
    let actor = match role_id {
        Some(role_id) => Actor::Role {
            principal: user_id,
            assumed_role: role_id,
        },
        None => Actor::Principal(user_id),
    };

    // Ensure assume role, if present, is allowed
    if let Err(err) = authorizer.check_actor(&actor).await {
        return err.into_response();
    }

    if let Some(request_metadata) = request.extensions_mut().get_mut::<RequestMetadata>() {
        request_metadata.set_authentication(actor.clone(), authentication);
    } else {
        let mut metadata = RequestMetadata::new_unauthenticated();
        metadata.set_authentication(actor.clone(), authentication);
        request.extensions_mut().insert(metadata);
    }

    next.run(request).await
}

fn extract_role_id(headers: &HeaderMap) -> Result<Option<RoleId>, IcebergErrorResponse> {
    if let Some(role_id) = headers.get(ASSUME_ROLE_HEADER) {
        let role_id = role_id.to_str().map_err(|e| {
            ErrorModel::bad_request(
                "Failed to parse Role-ID",
                "InvalidRoleIdError",
                Some(Box::new(e)),
            )
        })?;
        Ok(Some(RoleId::from_str(role_id)?))
    } else {
        Ok(None)
    }
}

impl std::fmt::Display for UserId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", format_subject(&self.0, Some(IDP_SEPARATOR)))
    }
}

impl UserId {
    fn try_new(subject: Subject) -> Result<Self, ErrorModel> {
        Self::validate_subject(subject.subject_in_idp())?;
        Ok(Self(subject))
    }

    #[cfg(test)]
    #[must_use]
    pub fn new_unchecked(idp_id: &str, sub: &str) -> Self {
        Self(Subject::new(Some(idp_id.to_string()), sub.to_string()))
    }

    fn validate_subject(subject: &str) -> Result<(), ErrorModel> {
        Self::validate_len(subject)?;
        Self::no_illegal_chars(subject)?;
        Ok(())
    }

    fn validate_len(subject: &str) -> Result<(), ErrorModel> {
        if subject.len() >= 128 {
            return Err(ErrorModel::bad_request(
                "user id must be shorter than 128 chars",
                "UserIdTooLongError",
                None,
            ));
        }
        Ok(())
    }

    fn no_illegal_chars(subject: &str) -> Result<(), ErrorModel> {
        if subject
            .chars()
            .any(|c| !(c.is_alphanumeric() || c == '-' || c == '_'))
        {
            return Err(ErrorModel::bad_request(
                "sub or oid claim contain illegal characters. Only alphanumeric + - are legal.",
                "InvalidUserIdError",
                None,
            ));
        }
        Ok(())
    }
}

impl Actor {
    #[must_use]
    pub fn is_authenticated(&self) -> bool {
        match self {
            Actor::Anonymous => false,
            Actor::Principal(_) | Actor::Role { .. } => true,
        }
    }
}

impl TryFrom<String> for UserId {
    type Error = ErrorModel;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        let subject = parse_subject(&s, Some(IDP_SEPARATOR)).map_err(|_e| {
            ErrorModel::bad_request(
                format!("Invalid user id format: {s}"),
                "InvalidUserId",
                None,
            )
        })?;
        UserId::try_new(subject)
    }
}

impl TryFrom<Subject> for UserId {
    type Error = ErrorModel;

    fn try_from(subject: Subject) -> Result<Self, Self::Error> {
        UserId::try_new(subject)
    }
}

impl From<UserId> for Subject {
    fn from(user_id: UserId) -> Self {
        user_id.0
    }
}

impl<'de> Deserialize<'de> for UserId {
    fn deserialize<D>(deserializer: D) -> api::Result<UserId, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        UserId::try_from(s).map_err(|e| serde::de::Error::custom(e.message))
    }
}

impl Serialize for UserId {
    fn serialize<S>(&self, serializer: S) -> api::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.to_string().as_str())
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    #[test]
    fn test_user_id() {
        let user_id = UserId::try_from("oidc~123".to_string()).unwrap();
        assert_eq!(
            user_id,
            UserId(Subject::new(Some("oidc".to_string()), "123".to_string()))
        );
        assert_eq!(user_id.to_string(), "oidc~123");

        let user_id = UserId::try_from("kubernetes~1234".to_string()).unwrap();
        assert_eq!(
            user_id,
            UserId(Subject::new(
                Some("kubernetes".to_string()),
                "1234".to_string()
            ))
        );
        assert_eq!(user_id.to_string(), "kubernetes~1234");

        // ------ Serde ------
        let user_id: UserId = serde_json::from_str(r#""oidc~123""#).unwrap();
        assert_eq!(
            user_id,
            UserId(Subject::new(Some("oidc".to_string()), "123".to_string()))
        );

        let user_id: UserId = serde_json::from_str(r#""kubernetes~123""#).unwrap();
        assert_eq!(
            user_id,
            UserId(Subject::new(
                Some("kubernetes".to_string()),
                "123".to_string()
            ))
        );
    }

    #[test]
    fn test_extract_role_id_case_insensitivity() {
        let headers = HeaderMap::new();
        let role_id = extract_role_id(&headers).unwrap();
        assert_eq!(role_id, None);

        let mut headers = HeaderMap::new();
        let this_role_id = Uuid::now_v7();
        headers.insert(
            "X-Assume-Role",
            this_role_id.clone().to_string().parse().unwrap(),
        );
        let role_id = extract_role_id(&headers).unwrap().unwrap();
        assert_eq!(role_id, RoleId::new(this_role_id));

        let mut headers = HeaderMap::new();
        headers.insert(
            ASSUME_ROLE_HEADER,
            this_role_id.to_string().parse().unwrap(),
        );
        let role_id = extract_role_id(&headers).unwrap().unwrap();
        assert_eq!(role_id, RoleId::new(this_role_id));
    }
}
