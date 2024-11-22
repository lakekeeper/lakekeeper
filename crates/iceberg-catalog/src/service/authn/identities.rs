use crate::api;
use crate::api::management::v1::user::UserType;
use crate::service::authn::Claims;
use crate::service::Actor;
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
use serde::{Deserialize, Serialize};

/// Unique identifier of a user in the system.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, utoipa::ToSchema)]
pub struct UserId {
    pub idp: String,
    pub user_id: String,
}

impl TryFrom<String> for UserId {
    type Error = IcebergErrorResponse;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        if let Some((idp, user_id)) = s.split_once('/') {
            UserId::idp_prefixed(user_id, idp)
        } else {
            UserId::without_prefix(&s)
        }
    }
}

impl std::fmt::Display for UserId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(format!("{}/{}", self.idp, self.user_id).as_str())
    }
}

impl UserId {
    pub(crate) fn idp_prefixed(user_id: &str, idp_prefix: &str) -> api::Result<Self> {
        Self::validate_len(user_id)?;
        Self::no_illegal_chars(user_id)?;

        // Lowercase all subjects
        Ok(Self {
            idp: idp_prefix.to_string(),
            user_id: user_id.to_lowercase(),
        })
    }

    pub(crate) fn without_prefix(subject: &str) -> api::Result<Self> {
        Self::validate_len(subject)?;

        Self::no_illegal_chars(subject)?;

        // Lowercase all subjects
        let subject = subject.to_lowercase();

        Ok(Self {
            idp: "default".to_string(),
            user_id: subject,
        })
    }

    pub(super) fn try_from_claims(claims: &Claims) -> api::Result<Self> {
        // For azure, the oid claim is permanent to the user account
        // accross all Entra ID applications. sub is only unique for one client.
        // To enable collaboration between projects, we use oid as the user id if
        // provided.
        let sub = if let Some(oid) = &claims.oid {
            oid.as_str()
        } else {
            claims.sub.as_str()
        };

        Self::without_prefix(sub)
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
            .any(|c| !(c.is_alphanumeric() || c == '-' || c == '_' || c == '/'))
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

    #[must_use]
    pub fn inner(&self) -> String {
        self.to_string()
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
