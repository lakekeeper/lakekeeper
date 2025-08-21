use std::fmt::Debug;
#[cfg(feature = "router")]
use std::str::FromStr;

#[cfg(feature = "router")]
use axum::{
    extract::{Request, State},
    middleware::Next,
    response::{IntoResponse, Response},
};
#[cfg(feature = "router")]
use axum_extra::{
    headers::{authorization::Bearer, Authorization},
    TypedHeader,
};
#[cfg(feature = "router")]
use http::{HeaderMap, StatusCode};
use iceberg_ext::catalog::rest::ErrorModel;
use limes::{format_subject, parse_subject, AuthenticatorEnum, Subject};
use serde::{Deserialize, Serialize};

use super::RoleId;
#[cfg(feature = "router")]
use crate::request_metadata::RequestMetadata;
use crate::{api, CONFIG};

pub const IDP_SEPARATOR: char = '~';
pub const ASSUME_ROLE_HEADER: &str = "x-assume-role";

#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, strum_macros::Display,
)]
#[serde(rename_all = "kebab-case", tag = "type")]
pub enum Actor {
    Anonymous,
    #[strum(to_string = "Principal({0})")]
    #[serde(with = "principal_serde")]
    Principal(UserId),
    #[strum(to_string = "AssumedRole({assumed_role}) by Principal({principal})")]
    #[serde(rename_all = "kebab-case")]
    Role {
        principal: UserId,
        assumed_role: RoleId,
    },
}

#[cfg(feature = "router")]
#[derive(Debug, Clone)]
pub(crate) struct AuthMiddlewareState<T: limes::Authenticator, A: super::Authorizer> {
    pub authenticator: T,
    pub authorizer: A,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserId(Subject);

const OIDC_IDP_ID: &str = "oidc";
const K8S_IDP_ID: &str = "kubernetes";

#[derive(Debug, Clone)]
pub enum BuiltInAuthenticators {
    Single(AuthenticatorEnum),
    Chain(limes::AuthenticatorChain<AuthenticatorEnum>),
}

/// Get the default authenticator configuration from the environment.
///
/// # Errors
/// If the authenticator cannot be created, or if the configuration is invalid.
#[allow(clippy::too_many_lines)]
pub async fn get_default_authenticator_from_config() -> anyhow::Result<Option<BuiltInAuthenticators>>
{
    let authn_k8s_audience = if CONFIG.enable_kubernetes_authentication {
        Some(
            limes::kubernetes::KubernetesAuthenticator::try_new_with_default_client(
                Some(K8S_IDP_ID),
                CONFIG
                    .kubernetes_authentication_audience
                    .clone()
                    .unwrap_or_default(),
            )
            .await
            .inspect_err(|e| tracing::error!("Failed to create K8s authorizer: {e}"))
            .inspect(|v| tracing::info!("K8s authorizer created {:?}", v))?,
        )
    } else {
        tracing::info!("Running without Kubernetes authentication.");
        None
    };

    let authn_k8s_legacy = if CONFIG.enable_kubernetes_authentication
        && CONFIG.kubernetes_authentication_accept_legacy_serviceaccount
    {
        let mut authenticator =
            limes::kubernetes::KubernetesAuthenticator::try_new_with_default_client(
                Some(K8S_IDP_ID),
                vec![],
            )
            .await
            .inspect_err(|e| tracing::error!("Failed to create K8s authorizer: {e}"))?;
        authenticator.set_issuers(vec![
            "kubernetes/serviceaccount".to_string(),
            "https://kubernetes.default.svc.cluster.local".to_string(),
        ]);
        tracing::info!(
            "K8s authorizer for legacy service account tokens created {:?}",
            authenticator
        );

        Some(authenticator)
    } else {
        tracing::info!("Running without Kubernetes authentication for legacy service accounts.");
        None
    };

    let authn_oidc = if let Some(uri) = CONFIG.openid_provider_uri.clone() {
        let mut authenticator = limes::jwks::JWKSWebAuthenticator::new(
            uri.as_ref(),
            Some(std::time::Duration::from_secs(3600)),
        )
        .await?
        .set_idp_id(OIDC_IDP_ID);
        if let Some(aud) = &CONFIG.openid_audience {
            tracing::debug!("Setting accepted audiences: {aud:?}");
            authenticator = authenticator.set_accepted_audiences(aud.clone());
        }
        if let Some(iss) = &CONFIG.openid_additional_issuers {
            tracing::debug!("Setting openid_additional_issuers: {iss:?}");
            authenticator = authenticator.add_additional_issuers(iss.clone());
        }
        if let Some(scope) = &CONFIG.openid_scope {
            tracing::debug!("Setting openid_scope: {}", scope);
            authenticator = authenticator.set_scope(scope.clone());
        }
        if let Some(subject_claim) = &CONFIG.openid_subject_claim {
            tracing::debug!("Setting openid_subject_claim: {}", subject_claim);
            authenticator = authenticator.with_subject_claim(subject_claim.clone());
        } else {
            // "oid" should be used for entra-id, as the `sub` is different between applications.
            // We prefer oid here by default as no other IdP sets this field (that we know of) and
            // we can provide an out-of-the-box experience for users.
            // Nevertheless, we document this behavior in the docs and recommend as part of the
            // `production` checklist to set the claim explicitly.
            tracing::debug!("Defaulting openid_subject_claim to: oid, sub");
            authenticator =
                authenticator.with_subject_claims(vec!["oid".to_string(), "sub".to_string()]);
        }
        tracing::info!("Running with OIDC authentication.");
        Some(authenticator)
    } else {
        tracing::info!("Running without OIDC authentication.");
        None
    };

    let authn_k8s = authn_k8s_audience.map(AuthenticatorEnum::from);
    let authn_k8s_legacy = authn_k8s_legacy.map(AuthenticatorEnum::from);
    let authn_oidc = authn_oidc.map(AuthenticatorEnum::from);
    match (authn_k8s, authn_oidc, authn_k8s_legacy) {
        (Some(k8s), Some(oidc), Some(authn_k8s_legacy)) => {
            Ok(Some(limes::AuthenticatorChain::<AuthenticatorEnum>::builder()
                .add_authenticator(oidc)
                .add_authenticator(k8s)
                .add_authenticator(authn_k8s_legacy)
                .build().into()))
        }
        (None, Some(auth1), Some(auth2))
        | (Some(auth1), None, Some(auth2))
        // OIDC has priority over k8s if specified
        | (Some(auth2), Some(auth1), None) => {
            Ok(Some(limes::AuthenticatorChain::<AuthenticatorEnum>::builder()
                .add_authenticator(auth1)
                .add_authenticator(auth2)
                .build().into()))
        }
        (Some(auth), None, None) | (None, Some(auth), None) | (None, None, Some(auth)) => {
            Ok(Some(auth.into()))
        }
        (None, None, None) => {
            tracing::warn!("Authentication is disabled. This is not suitable for production!");
            Ok(None)
        }
    }
}

#[cfg(feature = "router")]
/// Use a limes [`Authenticator`] to Authenticate a request.
///
/// This middleware needs to run after [`create_request_metadata_with_trace_and_project_fn`](crate::request_metadata::create_request_metadata_with_trace_and_project_fn).
pub(crate) async fn auth_middleware_fn<T: limes::Authenticator, A: super::authz::Authorizer>(
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
    }

    next.run(request).await
}

#[cfg(feature = "router")]
fn extract_role_id(
    headers: &HeaderMap,
) -> Result<Option<RoleId>, iceberg_ext::catalog::rest::IcebergErrorResponse> {
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
        if subject.idp_id().is_none() {
            return Err(ErrorModel::bad_request(
                "User ID must contain an IdP ID.",
                "InvalidUserIdError",
                None,
            ));
        }
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
        // Check for empty subject
        if subject.is_empty() {
            return Err(ErrorModel::bad_request(
                "user id cannot be empty",
                "EmptyUserIdError",
                None,
            ));
        }
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
        // Check for control characters
        if subject.chars().any(char::is_control) {
            return Err(ErrorModel::bad_request(
                "User ID cannot contain control characters.",
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
        UserId::try_from(s.as_str())
    }
}

impl<'a> TryFrom<&'a str> for UserId {
    type Error = ErrorModel;

    fn try_from(s: &'a str) -> Result<Self, Self::Error> {
        let subject = parse_subject(s, Some(IDP_SEPARATOR)).map_err(|_e| {
            ErrorModel::bad_request(
                format!("Invalid user id: `{s}`. Expected format: `<idp_id>~<user-id>`"),
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

impl From<AuthenticatorEnum> for BuiltInAuthenticators {
    fn from(authenticator: AuthenticatorEnum) -> Self {
        Self::Single(authenticator)
    }
}

impl From<limes::AuthenticatorChain<AuthenticatorEnum>> for BuiltInAuthenticators {
    fn from(authenticator: limes::AuthenticatorChain<AuthenticatorEnum>) -> Self {
        Self::Chain(authenticator)
    }
}

mod principal_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::UserId;
    #[derive(serde::Serialize, serde::Deserialize)]
    struct PrincipalWrapper {
        principal: UserId,
    }

    pub(super) fn serialize<S>(user_id: &UserId, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let wrapper = PrincipalWrapper {
            principal: user_id.clone(),
        };
        wrapper.serialize(serializer)
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<UserId, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wrapper = PrincipalWrapper::deserialize(deserializer)?;
        Ok(wrapper.principal)
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
    /// Test special cases:
    /// * empty idp (must not work)
    /// * empty sub (must not work)
    /// * sub with control characters (must not work)
    fn test_invalid_user_ids() {
        // empty idp
        let user_id = UserId::try_from("~123");
        assert!(user_id.is_err());

        // empty sub
        let user_id = UserId::try_from("oidc~");
        assert!(user_id.is_err());

        // sub with control characters
        let user_id = UserId::try_from("oidc~123\n");
        assert!(user_id.is_err());
    }

    #[test]
    /// Test UTF-8
    /// * user-id contains UTF-8 character (non-ASCII)
    /// * user-id starts with separator
    /// * user-id ends with separator
    /// * user-id contains separator
    fn test_user_ids_utf8() {
        // user-id contains UTF-8 character (non-ASCII)
        let user_id = UserId::try_from("oidc~1234é").unwrap();
        assert_eq!(
            user_id,
            UserId(Subject::new(Some("oidc".to_string()), "1234é".to_string()))
        );

        // user-id starts with separator
        let user_id = UserId::try_from("oidc~~1234").unwrap();
        assert_eq!(
            user_id,
            UserId(Subject::new(Some("oidc".to_string()), "~1234".to_string()))
        );

        // user-id ends with separator
        let user_id = UserId::try_from("oidc~1234~").unwrap();
        assert_eq!(
            user_id,
            UserId(Subject::new(Some("oidc".to_string()), "1234~".to_string()))
        );

        // user-id contains separator
        let user_id = UserId::try_from("oidc~1234~5678").unwrap();
        assert_eq!(
            user_id,
            UserId(Subject::new(
                Some("oidc".to_string()),
                "1234~5678".to_string()
            ))
        );

        // e-mail address as user-id
        let user_id = UserId::try_from("oidc~foo.bar@lakekeeper.io").unwrap();
        assert_eq!(
            user_id,
            UserId(Subject::new(
                Some("oidc".to_string()),
                "foo.bar@lakekeeper.io".to_string()
            ))
        );

        // e-mail with separator
        let user_id = UserId::try_from("oidc~foo~bar@lakekeeper.io").unwrap();
        assert_eq!(
            user_id,
            UserId(Subject::new(
                Some("oidc".to_string()),
                "foo~bar@lakekeeper.io".to_string()
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

    #[test]
    fn test_actor_serde_principal() {
        let actor = Actor::Principal(UserId::try_from("oidc~123").unwrap());
        let expected_json = serde_json::json!({
            "type": "principal",
            "principal": "oidc~123"
        });

        let actor_json = serde_json::to_value(&actor).unwrap();
        assert_eq!(actor_json, expected_json);
        let actor_from_json: Actor = serde_json::from_value(actor_json).unwrap();
        assert_eq!(actor_from_json, actor);
    }

    #[test]
    fn test_actor_serde_role() {
        let actor_json = serde_json::json!({
            "type": "role",
            "principal": "oidc~123",
            "assumed-role": "00000000-0000-0000-0000-000000000000"
        });
        let actor: Actor = serde_json::from_value(actor_json.clone()).unwrap();
        assert_eq!(
            actor,
            Actor::Role {
                principal: UserId::try_from("oidc~123").unwrap(),
                assumed_role: RoleId::new(Uuid::nil())
            }
        );
        let actor_json_2 = serde_json::to_value(&actor).unwrap();
        assert_eq!(actor_json, actor_json_2);
    }

    #[test]
    fn test_actor_serde_anonymous() {
        let actor_json = serde_json::json!({"type": "anonymous"});
        let actor: Actor = serde_json::from_value(actor_json.clone()).unwrap();
        assert_eq!(actor, Actor::Anonymous);
        let actor_json_2 = serde_json::to_value(&actor).unwrap();
        assert_eq!(actor_json, actor_json_2);
    }
}
