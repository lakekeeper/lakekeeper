use super::{ProjectIdent, RoleId, WarehouseIdent};
use crate::api::Result;
use iceberg_ext::catalog::rest::ErrorModel;
use k8s_openapi::api::authentication::v1::TokenReviewStatus;
use serde::Deserialize;
use std::fmt::Debug;

mod identities;
mod verification;

pub use identities::{Principal, UserId};
pub(crate) use verification::{auth_middleware_fn, VerifierChain};
pub use verification::{IdpVerifier, K8sVerifier};

#[derive(Debug, Clone)]
pub enum AuthDetails {
    Principal(Principal),
    Unauthenticated,
}

impl AuthDetails {
    fn try_from_kubernetes_review(token_review: Option<TokenReviewStatus>) -> Result<Self> {
        if let Some(state) = token_review {
            if let Some(claims) = state.user {
                let Some(uid) = claims.uid else {
                    return Err(ErrorModel::unauthorized(
                        "No username in token review",
                        "UnauthorizedError",
                        None,
                    )
                    .into());
                };
                let prefixed_id = UserId::kubernetes(uid.as_str())?;
                return Ok(AuthDetails::Principal(Principal {
                    actor: Actor::Principal(prefixed_id.clone()),
                    user_id: prefixed_id,
                    name: None,
                    display_name: claims.username,
                    issuer: "kubernetes".to_string(),
                    email: None,
                    application_id: Some(uid),
                    idtyp: Some("app".to_string()),
                }));
            }
        };
        Err(ErrorModel::unauthorized("No user in token review", "UnauthorizedError", None).into())
    }

    fn try_from_jwt_claims(claims: Claims) -> Result<Self> {
        // For azure, the oid claim is permanent to the user account
        // accross all Entra ID applications. sub is only unique for one client.
        // To enable collaboration between projects, we use oid as the user id if
        // provided.
        let sub = if let Some(oid) = &claims.oid {
            oid.as_str()
        } else {
            claims.sub.as_str()
        };
        let user_id = UserId::oidc(sub)?;

        let first_name = claims.given_name.or(claims.first_name);
        let last_name = claims.family_name.clone().or(claims.last_name);
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
            idtyp: claims
                .idtyp
                .or(claims.family_name.map(|_| "user".to_string())),
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

#[cfg(test)]
mod test {
    use crate::api::management::v1::user::UserType;
    use crate::service::authn::Claims;

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
        assert_eq!(user_type, UserType::Application);
    }

    #[test]
    fn test_human_discovery_entra_2() {
        let claims: Claims = serde_json::from_value(serde_json::json!({
          "aud": "api://xyz",
          "iss": "https://sts.windows.net/my-tenant-id/",
          "iat": 1_733_673_952,
          "nbf": 1_733_673_952,
          "exp": 1_733_679_587,
          "acr": "1",
          "aio": "...",
          "amr": [
            "pwd",
            "mfa"
          ],
          "appid": "xyz",
          "appidacr": "0",
          "family_name": "Peter",
          "given_name": "Cold",
          "ipaddr": "192.168.5.1",
          "name": "Peter Cold",
          "oid": "user-oid",
          "pwd_exp": "49828",
          "pwd_url": "https://portal.microsoftonline.com/ChangePassword.aspx",
          "scp": "lakekeeper",
          "sub": "user-sub",
          "tid": "my-tenant-id",
          "unique_name": "peter@example.com",
          "upn": "peter@example.com",
          "uti": "...",
          "ver": "1.0"
        }))
        .unwrap();

        let auth_details = super::AuthDetails::try_from_jwt_claims(claims).unwrap();
        let principal = match auth_details {
            super::AuthDetails::Principal(principal) => principal,
            super::AuthDetails::Unauthenticated => panic!("Expected principal"),
        };
        let (name, user_type) = principal.get_name_and_type().unwrap();
        assert_eq!(name, "Peter Cold");
        assert_eq!(user_type, UserType::Human);
        assert_eq!(principal.email(), Some("peter@example.com"));
    }

    #[test]
    fn test_human_discovery_entra() {
        let claims: Claims = serde_json::from_value(serde_json::json!({
            "acct": 0,
            "acr": "1",
            "aio": "...",
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
        }))
        .unwrap();

        let auth_details = super::AuthDetails::try_from_jwt_claims(claims).unwrap();
        let principal = match auth_details {
            super::AuthDetails::Principal(principal) => principal,
            super::AuthDetails::Unauthenticated => panic!("Expected principal"),
        };
        let (name, user_type) = principal.get_name_and_type().unwrap();
        assert_eq!(name, "Jack Frost");
        assert_eq!(user_type, UserType::Human);
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
        assert_eq!(user_type, UserType::Human);
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
        assert_eq!(user_type, UserType::Application);
    }
}
