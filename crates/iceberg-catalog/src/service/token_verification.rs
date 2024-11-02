use anyhow::Context;
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum_extra::{
    headers::{authorization::Bearer, Authorization},
    TypedHeader,
};
use http::StatusCode;
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
use jsonwebtoken::{Algorithm, DecodingKey, Header, Validation};
use jwks_client_rs::source::WebSource;
use jwks_client_rs::{JsonWebKey, JwksClient};

use super::{ProjectIdent, RoleId, WarehouseIdent};
use crate::api::management::v1::user::UserType;
use crate::api::Result;
use crate::request_metadata::RequestMetadata;
use axum::Extension;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;
use url::Url;

/// Unique identifier of a user in the system.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, PartialOrd, utoipa::ToSchema)]
#[serde(transparent)]
pub struct UserId(String);

impl<'de> Deserialize<'de> for UserId {
    fn deserialize<D>(deserializer: D) -> Result<UserId, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        UserId::new(&s)
            .map_err(|e| serde::de::Error::custom(format!("Invalid UserId: {}", e.error.message)))
    }
}

#[derive(Debug, Clone)]
pub enum AuthDetails {
    Principal(Principal),
    Unauthenticated,
}

#[derive(Debug, Clone)]
pub struct Principal {
    actor: Actor,
    user_id: UserId,
    name: Option<String>,
    display_name: Option<String>,
    // e.g. client_id in azure, should be there for technical users
    application_id: Option<String>,
    issuer: String,
    email: Option<String>,
    idtyp: Option<String>,
}

impl Principal {
    /// Best effort to determine the name of this principal from the claims.
    ///
    /// # Errors
    /// - name, display name and email are all missing
    pub fn get_name_and_type(&self) -> Result<(&str, UserType)> {
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
}

impl AuthDetails {
    fn try_from_jwt_claims(claims: Claims) -> Result<Self> {
        let user_id = UserId::try_from_claims(&claims)?;

        let first_name = claims.given_name.or(claims.first_name);
        let last_name = claims.family_name.or(claims.last_name);
        let name =
            claims
                .name
                .as_deref()
                .map(String::from)
                .or_else(|| match (first_name, last_name) {
                    (Some(first), Some(last)) => Some(format!("{first} {last}")),
                    (Some(first), None) => Some(first.to_string()),
                    (None, Some(last)) => Some(last.to_string()),
                    (None, None) => None,
                });

        let preferred_username = claims
            .name
            .clone()
            // Keycloak
            .or(claims.preferred_username)
            // Azure
            .or(claims.app_displayname)
            // Humans
            .or(name.clone());

        let email = claims.email.or(claims.upn);
        let application_id = claims
            .appid
            .or(claims.app_id)
            .or(claims.application_id)
            .or(claims.client_id);

        let principal = Principal {
            actor: Actor::Principal(user_id.clone()),
            user_id,
            name: name.clone(),
            display_name: preferred_username,
            issuer: claims.iss,
            email,
            application_id,
            idtyp: claims.idtyp,
        };

        Ok(Self::Principal(principal))
    }

    #[must_use]
    pub fn actor(&self) -> &Actor {
        match self {
            Self::Principal(principal) => &principal.actor,
            Self::Unauthenticated => &Actor::Anonymous,
        }
    }

    #[must_use]
    pub fn project_id(&self) -> Option<ProjectIdent> {
        None
    }

    #[must_use]
    pub fn warehouse_id(&self) -> Option<WarehouseIdent> {
        None
    }
}

impl Principal {
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

impl std::fmt::Display for UserId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.as_str())
    }
}

impl UserId {
    pub(crate) fn new(subject: &str) -> Result<Self> {
        if subject.len() >= 128 {
            return Err(ErrorModel::bad_request(
                "user id must be shorter than 128 chars",
                "UserIdTooLongError",
                None,
            )
            .into());
        }

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

        // Lowercase all subjects
        let subject = subject.to_lowercase();

        Ok(Self(subject.to_string()))
    }

    fn try_from_claims(claims: &Claims) -> Result<Self> {
        // For azure, the oid claim is permanent to the user account
        // accross all Entra ID applications. sub is only unique for one client.
        // To enable collaboration between projects, we use oid as the user id if
        // provided.
        let sub = if let Some(oid) = &claims.oid {
            oid.as_str()
        } else {
            claims.sub.as_str()
        };

        Self::new(sub)
    }

    #[must_use]
    pub fn inner(&self) -> &str {
        self.0.as_str()
    }
}

impl From<UserId> for String {
    fn from(user_id: UserId) -> Self {
        user_id.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Actor {
    Anonymous,
    Principal(UserId),
    Role {
        principal: UserId,
        assumed_role: RoleId,
    },
}

#[derive(Debug, Clone, Deserialize)]
// If multiple aliases are present, serde errors with "duplicate field".
// To avoid this, we use aliases scarcely and prefer to add a separate field for each alias.
struct Claims {
    sub: String,
    iss: String,
    // aud: Aud,
    // exp: usize,
    // iat: usize,
    oid: Option<String>,
    _azp: Option<String>,
    appid: Option<String>,
    app_id: Option<String>,
    application_id: Option<String>,
    client_id: Option<String>,
    idtyp: Option<String>,
    name: Option<String>,
    #[serde(alias = "given_name", alias = "given-name", alias = "givenName")]
    given_name: Option<String>,
    #[serde(alias = "firstName", alias = "first-name")]
    first_name: Option<String>,
    #[serde(alias = "family_name", alias = "family-name", alias = "familyName")]
    family_name: Option<String>,
    #[serde(alias = "lastName", alias = "last-name")]
    last_name: Option<String>,
    #[serde(
        // azure calls this app_displayname
        alias = "app_displayname",
        alias = "app-displayname",
        alias = "app_display_name",
        alias = "appDisplayName",
    )]
    app_displayname: Option<String>,
    #[serde(
        // keycloak calls this preferred_username
        alias = "preferred_username",
        alias = "preferred-username",
        alias = "preferredUsername"
    )]
    preferred_username: Option<String>,
    #[serde(
        alias = "email-address",
        alias = "emailAddress",
        alias = "email_address"
    )]
    email: Option<String>,
    upn: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum Aud {
    String(String),
    Vec(Vec<String>),
}

pub(crate) async fn auth_middleware_fn(
    State(verifier): State<Verifier>,
    authorization: Option<TypedHeader<Authorization<Bearer>>>,
    Extension(mut metadata): Extension<RequestMetadata>,
    mut request: Request,
    next: Next,
) -> Response {
    if let Some(authorization) = authorization {
        match verifier.decode::<Claims>(authorization.token()).await {
            Ok(val) => {
                match AuthDetails::try_from_jwt_claims(val) {
                    Ok(details) => {
                        metadata.auth_details = details;
                    }
                    Err(err) => return err.into_response(),
                }
                request.extensions_mut().insert(metadata);
            }
            Err(err) => {
                tracing::debug!("Failed to verify token: {:?}", err);
                return IcebergErrorResponse::from(err).into_response();
            }
        };
    } else {
        return IcebergErrorResponse::from(
            ErrorModel::builder()
                .message("Missing authorization header")
                .code(StatusCode::UNAUTHORIZED.into())
                .r#type("UnauthorizedError")
                .build(),
        )
        .into_response();
    }

    next.run(request).await
}

#[derive(Clone)]
pub struct Verifier {
    client: JwksClient<WebSource>,
    issuer: String,
}

impl Verifier {
    const WELL_KNOWN_CONFIG: &'static str = ".well-known/openid-configuration";
    /// Create a new verifier with the given openid configuration url and audience.
    ///
    /// # Errors
    ///
    /// This function can fail if the openid configuration cannot be fetched or parsed.
    /// This function can also fail if the `WebSource` cannot be built from the jwks uri in the
    /// fetched openid configuration
    pub async fn new(mut url: Url) -> anyhow::Result<Self> {
        if !url.path().ends_with('/') {
            url.set_path(&format!("{}/", url.path()));
        }
        let config = Arc::new(
            reqwest::get(url.join(Self::WELL_KNOWN_CONFIG)?)
                .await
                .context("Failed to fetch openid configuration")?
                .json::<WellKnownConfig>()
                .await
                .context("Failed to parse openid configuration")?,
        );
        let source = WebSource::builder().build(config.jwks_uri.clone())?;
        let client = JwksClient::builder().build(source);
        Ok(Self {
            client,
            issuer: config.issuer.clone(),
        })
    }

    // this function is mostly lifted out of jwks_client_rs which is incompatible with azure jwks.
    async fn decode<O: DeserializeOwned>(&self, token: &str) -> Result<O, ErrorModel> {
        let header: Header = jsonwebtoken::decode_header(token).map_err(|e| {
            ErrorModel::builder()
                .message("Failed to decode auth token header.")
                .code(StatusCode::UNAUTHORIZED.into())
                .r#type("UnauthorizedError")
                .source(Some(Box::new(e)))
                .build()
        })?;

        if let Some(kid) = header.kid.as_ref() {
            let key: JsonWebKey = self
                .client
                .get_opt(kid)
                .await
                .map_err(|e| Self::internal_error(e, "Failed to fetch key from jwks endpoint."))?
                .ok_or_else(|| {
                    ErrorModel::builder()
                        .message("Unknown kid")
                        .r#type("UnauthorizedError")
                        .code(StatusCode::UNAUTHORIZED.into())
                        .build()
                })?;

            let validation = self.setup_validation(&header, &key)?;
            let decoding_key = Self::setup_decoding_key(key)?;

            return Ok(jsonwebtoken::decode(token, &decoding_key, &validation)
                .map_err(|e| {
                    tracing::debug!("Failed to decode token: {}", e);
                    ErrorModel::builder()
                        .message("Failed to decode token.")
                        .code(StatusCode::UNAUTHORIZED.into())
                        .r#type("UnauthorizedError")
                        .source(Some(Box::new(e)))
                        .build()
                })?
                .claims);
        }

        Err(ErrorModel::builder()
            .message("Token header does not contain a key id.")
            .code(StatusCode::UNAUTHORIZED.into())
            .r#type("UnauthorizedError")
            .build())
    }

    fn setup_decoding_key(key: JsonWebKey) -> Result<DecodingKey, ErrorModel> {
        let decoding_key = match key {
            JsonWebKey::Rsa(jwk) => DecodingKey::from_rsa_components(jwk.modulus(), jwk.exponent())
                .map_err(|e| {
                    Self::internal_error(
                        e,
                        "Failed to create rsa decoding key from key components.",
                    )
                })?,
            JsonWebKey::Ec(jwk) => {
                DecodingKey::from_ec_components(jwk.x(), jwk.y()).map_err(|e| {
                    Self::internal_error(e, "Failed to create ec decoding key from key components.")
                })?
            }
        };
        Ok(decoding_key)
    }

    fn setup_validation(
        &self,
        header: &Header,
        key: &JsonWebKey,
    ) -> Result<Validation, ErrorModel> {
        let mut validation = if let Some(alg) = key.alg() {
            Validation::new(Algorithm::from_str(alg).map_err(|e| {
                Self::internal_error(
                    e,
                    "Failed to parse algorithm from key obtained from the jwks endpoint.",
                )
            })?)
        } else {
            // We need this fallback since e.g. azure's keys at
            // https://login.microsoftonline.com/common/discovery/keys don't have the alg field
            Validation::new(header.alg)
        };

        // TODO: aud validation in multi-tenant setups
        validation.validate_aud = false;

        validation.set_issuer(&[&self.issuer]);

        Ok(validation)
    }

    fn internal_error(
        e: impl std::error::Error + Sync + Send + 'static,
        message: &str,
    ) -> ErrorModel {
        ErrorModel::builder()
            .message(message)
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .r#type("InternalServerError")
            .source(Some(Box::new(e)))
            .build()
    }
}

impl Debug for Verifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Verifier").finish()
    }
}

#[derive(Deserialize, Clone, Debug)]
pub struct WellKnownConfig {
    #[serde(flatten)]
    pub other: serde_json::Value,
    pub jwks_uri: Url,
    pub issuer: String,
    pub userinfo_endpoint: Option<Url>,
    pub token_endpoint: Url,
}

#[cfg(test)]
mod test {
    use crate::service::token_verification::Claims;

    #[test]
    fn test_aud_with_array() {
        let _: Claims = serde_json::from_value(serde_json::json!({
            "sub": "1234567890",
            "iat": 22,
            "aud": ["aud1", "aud2"],
            "iss": "https://example.com",
            "exp": 9022
        }))
        .unwrap();
    }

    #[test]
    fn test_aud_with_string() {
        let _: Claims = serde_json::from_value(serde_json::json!({
            "sub": "1234567890",
            "iat": 22,
            "aud": "aud1",
            "iss": "https://example.com",
            "exp": 9022
        }))
        .unwrap();
    }

    #[test]
    fn test_machine_discovery_entra() {
        let claims: Claims = serde_json::from_value(serde_json::json!({
                    "aio": "k2BgYGiZGnb+zdtzaReDdlQfWjHBAgA=",
                    "app_displayname": "ht-testing-lakekeeper-oauth",
                    "appid": "d53edae2-1b58-4c56-a243-xxxxxxxxxxxx",
                    "appidacr": "1",
                    "aud": "00000003-0000-0000-c000-000000000000",
                    "exp": 1_730_052_519,
                    "iat": 1_730_048_619,
                    "idp": "https://sts.windows.net/00000003-1234-0000-c000-000000000000/",
                    "idtyp": "app",
                    "iss": "https://sts.windows.net/00000003-1234-0000-c000-000000000000/",
                    "nbf": 1_730_048_619,
                    "oid": "f621fc83-4ec9-4bf8-bc8d-xxxxxxxxxxxx",
                    "rh": "0.AU8A4hqJeoi7wkGOJROkB9ygQAMAAAAAAAAAwAAAAAAAAABPAAA.",
                    "sub": "f621fc83-4ec9-4bf8-bc8d-xxxxxxxxxxxx",
                    "tenant_region_scope": "EU",
                    "tid": "00000003-1234-0000-c000-000000000000",
                    "uti": "mBOqwjvzLUqboKm591ccAA",
                    "ver": "1.0",
                    "wids": ["0997a1d0-0d1d-4acb-b408-xxxxxxxxxxxx"],
                    "xms_idrel": "7 24",
                    "xms_tcdt": 1_638_946_153,
                    "xms_tdbr": "EU"
        }))
        .unwrap();

        let auth_details = super::AuthDetails::try_from_jwt_claims(claims).unwrap();
        let principal = match auth_details {
            super::AuthDetails::Principal(principal) => principal,
            super::AuthDetails::Unauthenticated => panic!("Expected principal"),
        };
        let (name, user_type) = principal.get_name_and_type().unwrap();
        assert_eq!(name, "ht-testing-lakekeeper-oauth");
        assert_eq!(user_type, super::UserType::Application);
    }

    #[test]
    fn test_human_discovery_entra() {
        let claims: Claims = serde_json::from_value(serde_json::json!({
            "acct": 0,
            "acr": "1",
            "aio": "AVQAq/8YAAAAkP/tBF3Bd+1dl8nN3d7MY7maGp32OtfupUwxJPaOxxkEiLSzd1yMjh/2AZIg0NB97mxp1+Q0ZfCQVCjKyUINhp2XYMuKtQSy3FC3WiM4aFM=",
            "amr": ["pwd", "mfa"],
            "app_displayname": "ht-testing-lakekeeper-oauth",
            "appid": "d53edae2-1b58-4c56-a243-xxxxxxxxxxxx",
            "appidacr": "0",
            "aud": "00000003-0000-0000-c000-000000000000",
            "exp": 1_730_054_207,
            "family_name": "Frost",
            "given_name": "Jack",
            "iat": 1_730_049_088,
            "idtyp": "user",
            "ipaddr": "12.206.221.219",
            "iss": "https://sts.windows.net/00000003-1234-0000-c000-000000000000/",
            "name": "Jack Frost",
            "nbf": 1_730_049_088,
            "oid": "eb54b4f5-0d20-46eb-b703-b1c910262e89",
            "platf": "14",
            "puid": "100320025A52FAC4",
            "rh": "0.AU8A4hqJeoi7wkGOJROkB9ygQAMAAAAAAAAAwAAAAAAAAABPAJo.",
            "scp": "openid profile User.Read email",
            "signin_state": ["kmsi"],
            "sub": "SFUpMUKjypW6q3w3Vc9u8N3LNAGlZmIrmGdvQVN53AI",
            "tenant_region_scope": "EU",
            "tid": "00000003-1234-0000-c000-000000000000",
            "unique_name": "jack@example.com",
            "upn": "jack@example.com",
            "uti": "FXRr3wnAA0e8YADs1adQAA",
            "ver": "1.0",
            "wids": ["62e90394-69f5-4237-9190-xxxxxxxxxxxx",
                    "b79fbf4d-3ef9-4689-8143-xxxxxxxxxxxx"],
            "xms_idrel": "1 8",
            "xms_st": {"sub": "VZ5XLBqhasu6qISBjalO9e45lQjr_EyLLtKzCFcWw-8"},
            "xms_tcdt": 1_638_946_153,
            "xms_tdbr": "EU"
        })).unwrap();

        let auth_details = super::AuthDetails::try_from_jwt_claims(claims).unwrap();
        let principal = match auth_details {
            super::AuthDetails::Principal(principal) => principal,
            super::AuthDetails::Unauthenticated => panic!("Expected principal"),
        };
        let (name, user_type) = principal.get_name_and_type().unwrap();
        assert_eq!(name, "Jack Frost");
        assert_eq!(user_type, super::UserType::Human);
        assert_eq!(principal.email(), Some("jack@example.com"));
    }

    #[test]
    fn test_human_discovery_keycloak() {
        let claims: Claims = serde_json::from_value(serde_json::json!({
          "exp": 1_729_990_458,
          "iat": 1_729_990_158,
          "jti": "97cdc5d9-8717-4826-a425-30c6682342b4",
          "iss": "http://localhost:30080/realms/iceberg",
          "aud": "account",
          "sub": "f1616ed0-18d8-48ea-9fb3-832f42db0b1b",
          "typ": "Bearer",
          "azp": "iceberg-catalog",
          "sid": "6f2ca33d-2513-43fe-ab53-4a945c78a66d",
          "acr": "1",
          "allowed-origins": [
            "*"
          ],
          "realm_access": {
            "roles": [
              "offline_access",
              "uma_authorization",
              "default-roles-iceberg"
            ]
          },
          "resource_access": {
            "account": {
              "roles": [
                "manage-account",
                "manage-account-links",
                "view-profile"
              ]
            }
          },
          "scope": "openid email profile",
          "email_verified": true,
          "name": "Peter Cold",
          "preferred_username": "peter",
          "given_name": "Peter",
          "family_name": "Cold",
          "email": "peter@example.com"
        }))
        .unwrap();

        let auth_details = super::AuthDetails::try_from_jwt_claims(claims).unwrap();
        let principal = match auth_details {
            super::AuthDetails::Principal(principal) => principal,
            super::AuthDetails::Unauthenticated => panic!("Expected principal"),
        };
        let (name, user_type) = principal.get_name_and_type().unwrap();
        assert_eq!(name, "Peter Cold");
        assert_eq!(user_type, super::UserType::Human);
        assert_eq!(principal.email(), Some("peter@example.com"));
    }

    #[test]
    fn test_machine_discovery_keycloak() {
        let claims: Claims = serde_json::from_value(serde_json::json!({
          "exp": 1_730_050_563,
          "iat": 1_730_050_563,
          "jti": "b1e96701-b718-4714-88a2-d25d985c38ed",
          "iss": "http://keycloak:8080/realms/iceberg",
          "aud": [
            "iceberg-catalog",
            "account"
          ],
          "sub": "b6cc7aa0-1af0-460e-9174-e05c881fb6d4",
          "typ": "Bearer",
          "azp": "iceberg-machine-client",
          "acr": "1",
          "allowed-origins": [
            "/*"
          ],
          "realm_access": {
            "roles": [
              "offline_access",
              "uma_authorization",
              "default-roles-iceberg"
            ]
          },
          "resource_access": {
            "iceberg-machine-client": {
              "roles": [
                "uma_protection"
              ]
            },
            "account": {
              "roles": [
                "manage-account",
                "manage-account-links",
                "view-profile"
              ]
            }
          },
          "scope": "email profile",
          "clientHost": "10.89.0.2",
          "email_verified": false,
          "preferred_username": "service-account-iceberg-machine-client",
          "clientAddress": "10.89.0.2",
          "client_id": "iceberg-machine-client"
        }))
        .unwrap();

        let auth_details = super::AuthDetails::try_from_jwt_claims(claims).unwrap();
        let principal = match auth_details {
            super::AuthDetails::Principal(principal) => principal,
            super::AuthDetails::Unauthenticated => panic!("Expected principal"),
        };
        let (name, user_type) = principal.get_name_and_type().unwrap();
        assert_eq!(name, "service-account-iceberg-machine-client");
        assert_eq!(user_type, super::UserType::Application);
    }
}
