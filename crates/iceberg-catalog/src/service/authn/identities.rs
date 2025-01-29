use crate::api;
use crate::api::management::v1::user::UserType;
use crate::service::Actor;
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
use serde::{Deserialize, Serialize};

/// Unique identifier of a user in the system.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, utoipa::ToSchema)]
pub enum UserId {
    /// OIDC principal
    OIDC(String),
    /// K8s principal
    Kubernetes(String),
}

impl TryFrom<String> for UserId {
    type Error = IcebergErrorResponse;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.split_once('~') {
            Some(("oidc", user_id)) => Ok(UserId::oidc(user_id)?),
            Some(("kubernetes", user_id)) => Ok(UserId::kubernetes(user_id)?),
            _ => Err(ErrorModel::bad_request(
                format!("Invalid user id format: {s}"),
                "InvalidUserId",
                None,
            )
            .into()),
        }
    }
}

impl std::fmt::Display for UserId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UserId::OIDC(user_id) => write!(f, "oidc~{user_id}"),
            UserId::Kubernetes(user_id) => write!(f, "kubernetes~{user_id}"),
        }
    }
}

impl UserId {
    fn validate_subject(subject: &str) -> api::Result<()> {
        Self::validate_len(subject)?;
        Self::no_illegal_chars(subject)?;
        Ok(())
    }

    pub(crate) fn oidc(subject: &str) -> api::Result<Self> {
        Self::validate_subject(subject)?;
        Ok(Self::OIDC(subject.to_string()))
    }

    pub(crate) fn kubernetes(subject: &str) -> api::Result<Self> {
        Self::validate_subject(subject)?;
        Ok(Self::Kubernetes(subject.to_string()))
    }

    fn validate_len(subject: &str) -> api::Result<()> {
        if subject.len() >= 128 {
            return Err(ErrorModel::bad_request(
                "user id must be shorter than 128 chars",
                "UserIdTooLongError",
                None,
            )
            .into());
        }
        Ok(())
    }

    fn no_illegal_chars(subject: &str) -> api::Result<()> {
        if subject
            .chars()
            .any(|c| !(c.is_alphanumeric() || c == '-' || c == '_'))
        {
            return Err(ErrorModel::bad_request(
                "sub or oid claim contain illegal characters. Only alphanumeric + - are legal.",
                "InvalidUserIdError",
                None,
            )
            .into());
        }
        Ok(())
    }
}

impl From<UserId> for String {
    fn from(user_id: UserId) -> Self {
        user_id.to_string()
    }
}

impl<'de> Deserialize<'de> for UserId {
    fn deserialize<D>(deserializer: D) -> api::Result<UserId, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        UserId::try_from(s).map_err(|e| serde::de::Error::custom(e.error))
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

#[derive(Debug, Clone)]
pub struct Principal {
    pub(super) actor: Actor,
    pub(super) user_id: UserId,
    pub(super) name: Option<String>,
    pub(super) display_name: Option<String>,
    pub(super) application_id: Option<String>,
    pub(super) issuer: String,
    pub(super) email: Option<String>,
    pub(super) idtyp: Option<String>,
}

impl Principal {
    #[must_use]
    #[cfg(test)]
    pub fn random_human(user_id: UserId) -> Self {
        Self {
            actor: Actor::Principal(user_id.clone()),
            name: Some(format!("name-{user_id}")),
            display_name: Some(format!("display-name-{user_id}")),
            user_id,
            application_id: None,
            issuer: "test-issuer".to_string(),
            email: None,
            idtyp: None,
        }
    }

    /// Best effort to determine the name of this principal from the claims.
    ///
    /// # Errors
    /// - name, display name and email are all missing
    pub fn get_name_and_type(&self) -> api::Result<(&str, UserType)> {
        let human_name = self.display_name().or(self.email());
        let human_result = human_name
            .ok_or(
                ErrorModel::bad_request(
                    "Cannot register principal as no name could be determined",
                    "InvalidAccessTokenClaims",
                    None,
                )
                .into(),
            )
            .map(|name| (name, UserType::Human));
        let app_name = self.display_name().or(self.app_id());
        let app_result = app_name
            .ok_or(
                ErrorModel::bad_request(
                    "Cannot register principal as no name could be determined",
                    "InvalidAccessTokenClaims",
                    None,
                )
                .into(),
            )
            .map(|name| (name, UserType::Application));

        // Best indicator: Type has been explicitly set
        if let Some(idtyp) = self.idtyp.as_deref() {
            if idtyp.to_lowercase() == "app" {
                return app_result;
            } else if idtyp.to_lowercase() == "user" {
                return human_result;
            }
        }

        if self.app_id().is_some() {
            return app_result;
        }

        human_result
    }

    #[must_use]
    pub fn actor(&self) -> &Actor {
        &self.actor
    }

    #[must_use]
    pub fn user_id(&self) -> &UserId {
        &self.user_id
    }

    #[must_use]
    pub fn app_id(&self) -> Option<&str> {
        self.application_id.as_deref()
    }

    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    #[must_use]
    pub fn display_name(&self) -> Option<&str> {
        self.display_name.as_deref()
    }

    #[must_use]
    pub fn issuer(&self) -> &str {
        self.issuer.as_str()
    }

    #[must_use]
    pub fn email(&self) -> Option<&str> {
        self.email.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_id() {
        let user_id = UserId::try_from("oidc~123".to_string()).unwrap();
        assert_eq!(user_id, UserId::OIDC("123".to_string()));
        assert_eq!(user_id.to_string(), "oidc~123");

        let user_id = UserId::try_from("kubernetes~1234".to_string()).unwrap();
        assert_eq!(user_id, UserId::Kubernetes("1234".to_string()));
        assert_eq!(user_id.to_string(), "kubernetes~1234");

        let user_id: UserId = serde_json::from_str(r#""oidc~123""#).unwrap();
        assert_eq!(user_id, UserId::OIDC("123".to_string()));

        let user_id: UserId = serde_json::from_str(r#""kubernetes~123""#).unwrap();
        assert_eq!(user_id, UserId::Kubernetes("123".to_string()));

        serde_json::from_str::<UserId>(r#""nonexistant~123""#).unwrap_err();
    }
}
