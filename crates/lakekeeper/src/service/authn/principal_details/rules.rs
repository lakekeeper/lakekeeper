//! How Lakekeeper reads a principal from a verified token.
//!
//! For claims, of a JWT or of a custom authenticator:
//! - name: `name`, or `given_name` / `first_name` and `family_name` / `last_name`, or
//!   `app_displayname`, or `preferred_username`; if none is present, the provider's
//!   `display_name_template` rendered from the claims.
//! - email: `email`, or `upn` if it contains an `@`, or `preferred_username` if it contains an
//!   `@`.
//! - user type: from `idtyp`; else `Human` if `name`, `given_name`, `first_name`,
//!   `family_name` or `last_name` is present, `Application` if `app_displayname` or
//!   `client_id` is present.
//! - roles: the strings in the claim that the provider's `roles_claim` names.
//!
//! For a Kubernetes identity, the name is `user.username` and the user type is
//! `Application`; there is no email and there are no roles.

use limes::{Authentication, Identity, KubernetesUser, VerifiedClaims};

use super::{super::DisplayNameTemplate, PrincipalDetails};
use crate::api::management::v1::user::UserType;

impl PrincipalDetails {
    /// Reads the details from `authentication`. `display_name_template` names a principal
    /// whose claims carry no name.
    pub(crate) fn of(
        authentication: &Authentication,
        display_name_template: Option<&DisplayNameTemplate>,
    ) -> Self {
        match authentication.identity() {
            Identity::Claims(claims) => Self::of_claims(claims, display_name_template),
            Identity::Kubernetes(user) => Self::of_kubernetes_user(user),
            _ => {
                tracing::warn!("Lakekeeper reads no principal details from this kind of identity.");
                Self::default()
            }
        }
    }

    fn of_claims(
        claims: &VerifiedClaims,
        display_name_template: Option<&DisplayNameTemplate>,
    ) -> Self {
        let human_name = human_name(claims);
        let app_name = claims
            .claim("app_displayname")
            .and_then(serde_json::Value::as_str)
            .map(ToString::to_string);

        let user_type = idtyp_user_type(claims)
            .or(human_name.as_ref().map(|_| UserType::Human))
            .or(app_name.as_ref().map(|_| UserType::Application))
            // In Keycloak the client_id is the requesting application
            .or(claims.claim("client_id").map(|_| UserType::Application));

        let name = human_name
            .or(app_name)
            .or_else(|| claims.preferred_username().map(ToString::to_string))
            // Last resort for tokens that carry no name claim at all (e.g. machine /
            // service-account tokens): render the configured display-name template.
            .or_else(|| display_name_template.and_then(|t| t.render(claims)));

        Self {
            name,
            email: email(claims),
            user_type,
        }
    }

    /// Lakekeeper reads no email from a Kubernetes identity.
    fn of_kubernetes_user(user: &KubernetesUser) -> Self {
        Self {
            name: user.username().map(ToString::to_string),
            email: None,
            user_type: Some(UserType::Application),
        }
    }
}

/// The roles in the claim at `roles_claim`: the strings of an array, or a single string.
/// Elements that are not strings are skipped. `None` if the claim is absent or holds no
/// string, and for identities other than claims.
#[cfg(feature = "router")]
pub(crate) fn token_roles<'a>(
    authentication: &'a Authentication,
    roles_claim: &str,
) -> Option<Vec<&'a str>> {
    let Identity::Claims(claims) = authentication.identity() else {
        return None;
    };
    let roles: Vec<&str> = match claims.claim(roles_claim) {
        Some(serde_json::Value::Array(values)) => values
            .iter()
            .filter_map(serde_json::Value::as_str)
            .collect(),
        Some(value) => value.as_str().into_iter().collect(),
        None => Vec::new(),
    };
    if roles.is_empty() {
        tracing::debug!(
            "Roles claim `{roles_claim}` is configured, but the token carries no roles in it. \
             The claim may be missing or hold values that are not strings."
        );
        return None;
    }
    Some(roles)
}

fn idtyp_user_type(claims: &VerifiedClaims) -> Option<UserType> {
    match claims.claim("idtyp").and_then(serde_json::Value::as_str) {
        Some("user") => Some(UserType::Human),
        Some("app" | "device") => Some(UserType::Application),
        _ => None,
    }
}

fn email(claims: &VerifiedClaims) -> Option<String> {
    claims
        .email()
        .or(claims
            .claim("upn")
            .and_then(serde_json::Value::as_str)
            .filter(|s| s.contains('@')))
        .or(claims.preferred_username().filter(|s| s.contains('@')))
        .map(ToString::to_string)
}

fn human_name(claims: &VerifiedClaims) -> Option<String> {
    let first_name = claims
        .claim("given_name")
        .or(claims.claim("first_name"))
        .and_then(serde_json::Value::as_str);
    let last_name = claims
        .claim("family_name")
        .or(claims.claim("last_name"))
        .and_then(serde_json::Value::as_str);

    claims
        .name()
        .map(ToString::to_string)
        .or_else(|| match (first_name, last_name) {
            (Some(first), Some(last)) => Some(format!("{first} {last}")),
            (Some(name), None) | (None, Some(name)) => Some(name.to_string()),
            (None, None) => None,
        })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use limes::Subject;

    use super::*;

    fn jwt(claims: &serde_json::Value) -> Authentication {
        Authentication::builder()
            .subject(Subject::new(Some("idp".to_string()), "sub".to_string()))
            .identity(VerifiedClaims::try_from(claims.clone()).unwrap())
            .build()
    }

    fn details(name: Option<&str>, email: Option<&str>, user_type: UserType) -> PrincipalDetails {
        PrincipalDetails {
            name: name.map(ToString::to_string),
            email: email.map(ToString::to_string),
            user_type: Some(user_type),
        }
    }

    #[cfg(feature = "router")]
    fn get_roles(claims: &serde_json::Value, roles_claim: &str) -> Option<Vec<String>> {
        token_roles(&jwt(claims), roles_claim)
            .map(|roles| roles.into_iter().map(ToString::to_string).collect())
    }

    #[test]
    fn test_payload_entra_application() {
        let claims = serde_json::json!({
                    "aio": "example-aio",
                    "app_displayname": "example-lakekeeper-app",
                    "appid": "aaaaaaaa-0000-0000-0000-000000000001",
                    "appidacr": "1",
                    "aud": "00000003-0000-0000-c000-000000000000",
                    "exp": 1_730_052_519,
                    "iat": 1_730_048_619,
                    "idp": "https://sts.windows.net/00000003-1234-0000-c000-000000000000/",
                    "idtyp": "app",
                    "iss": "https://sts.windows.net/00000003-1234-0000-c000-000000000000/",
                    "nbf": 1_730_048_619,
                    "oid": "aaaaaaaa-0000-0000-0000-000000000002",
                    "rh": "example-rh",
                    "sub": "aaaaaaaa-0000-0000-0000-000000000002",
                    "tenant_region_scope": "EU",
                    "tid": "00000003-1234-0000-c000-000000000000",
                    "uti": "example-uti",
                    "ver": "1.0",
                    "wids": ["0997a1d0-0d1d-4acb-b408-xxxxxxxxxxxx"],
                    "xms_idrel": "7 24",
                    "xms_tcdt": 1_638_946_153,
                    "xms_tdbr": "EU"
        });

        assert_eq!(
            PrincipalDetails::of(&jwt(&claims), None),
            details(Some("example-lakekeeper-app"), None, UserType::Application)
        );
    }

    #[test]
    fn test_payload_entra_human_1() {
        let claims = serde_json::json!({
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
        });

        assert_eq!(
            PrincipalDetails::of(&jwt(&claims), None),
            details(
                Some("Peter Cold"),
                Some("peter@example.com"),
                UserType::Human
            )
        );
    }

    #[test]
    fn test_payload_entra_human_2() {
        let claims = serde_json::json!({
            "acct": 0,
            "acr": "1",
            "aio": "...",
            "amr": ["pwd", "mfa"],
            "app_displayname": "example-lakekeeper-app",
            "appid": "aaaaaaaa-0000-0000-0000-000000000001",
            "appidacr": "0",
            "aud": "00000003-0000-0000-c000-000000000000",
            "exp": 1_730_054_207,
            "family_name": "Frost",
            "given_name": "Jack",
            "iat": 1_730_049_088,
            "idtyp": "user",
            "ipaddr": "192.0.2.10",
            "iss": "https://sts.windows.net/00000003-1234-0000-c000-000000000000/",
            "name": "Jack Frost",
            "nbf": 1_730_049_088,
            "oid": "aaaaaaaa-0000-0000-0000-000000000003",
            "platf": "14",
            "puid": "0000000000000001",
            "rh": "example-rh",
            "scp": "openid profile User.Read email",
            "signin_state": ["kmsi"],
            "sub": "example-subject-of-jack",
            "tenant_region_scope": "EU",
            "tid": "00000003-1234-0000-c000-000000000000",
            "unique_name": "jack@example.com",
            "upn": "jack@example.com",
            "uti": "example-uti",
            "ver": "1.0",
            "wids": ["62e90394-69f5-4237-9190-xxxxxxxxxxxx",
                    "b79fbf4d-3ef9-4689-8143-xxxxxxxxxxxx"],
            "xms_idrel": "1 8",
            "xms_st": {"sub": "example-xms-st-subject"},
            "xms_tcdt": 1_638_946_153,
            "xms_tdbr": "EU"
        });

        assert_eq!(
            PrincipalDetails::of(&jwt(&claims), None),
            details(
                Some("Jack Frost"),
                Some("jack@example.com"),
                UserType::Human
            )
        );
    }

    #[test]
    fn test_payload_keycloak_human() {
        let claims = serde_json::json!({
          "exp": 1_729_990_458,
          "iat": 1_729_990_158,
          "jti": "bbbbbbbb-0000-0000-0000-000000000001",
          "iss": "http://localhost:30080/realms/iceberg",
          "aud": "account",
          "sub": "bbbbbbbb-0000-0000-0000-000000000002",
          "typ": "Bearer",
          "azp": "iceberg-catalog",
          "sid": "bbbbbbbb-0000-0000-0000-000000000003",
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
        });
        let authentication = jwt(&claims);

        assert_eq!(
            PrincipalDetails::of(&authentication, None),
            details(
                Some("Peter Cold"),
                Some("peter@example.com"),
                UserType::Human
            )
        );
        #[cfg(feature = "router")]
        assert_eq!(
            token_roles(&authentication, "realm_access.roles"),
            Some(vec![
                "offline_access",
                "uma_authorization",
                "default-roles-iceberg",
            ])
        );
    }

    #[test]
    fn test_payload_keycloak_machine() {
        let claims = serde_json::json!({
          "exp": 1_730_050_563,
          "iat": 1_730_050_563,
          "jti": "bbbbbbbb-0000-0000-0000-000000000004",
          "iss": "http://keycloak:8080/realms/iceberg",
          "aud": [
            "iceberg-catalog",
            "account"
          ],
          "sub": "bbbbbbbb-0000-0000-0000-000000000005",
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
          "clientHost": "192.0.2.20",
          "email_verified": false,
          "preferred_username": "service-account-iceberg-machine-client",
          "clientAddress": "192.0.2.20",
          "client_id": "iceberg-machine-client"
        });
        let authentication = jwt(&claims);

        assert_eq!(
            PrincipalDetails::of(&authentication, None),
            details(
                Some("service-account-iceberg-machine-client"),
                None,
                UserType::Application
            )
        );
        #[cfg(feature = "router")]
        assert_eq!(
            token_roles(&authentication, "resource_access.account.roles"),
            Some(vec![
                "manage-account",
                "manage-account-links",
                "view-profile"
            ])
        );
    }

    #[test]
    fn test_display_name_template_applied_when_no_name_claim() {
        // A bare machine token (no name/given_name/app_displayname/preferred_username)
        // — as issued by e.g. service-account IdPs — falls back to the template.
        let claims = serde_json::json!({
            "sub": "00000000-0000-0000-0000-000000000000",
            "email": "service-account@example.com",
            "iss": "https://issuer.example.com",
        });
        let template = DisplayNameTemplate::parse("Service Account {email}").unwrap();
        let principal = PrincipalDetails::of(&jwt(&claims), Some(&template));
        assert_eq!(
            principal.name(),
            Some("Service Account service-account@example.com")
        );
        assert_eq!(principal.user_type(), None);
    }

    #[test]
    fn test_display_name_template_ignored_when_name_claim_present() {
        // A real human-name claim always wins; the template is a fallback only.
        let claims = serde_json::json!({
            "sub": "x",
            "name": "Jane Doe",
            "email": "jane@example.com",
        });
        let template = DisplayNameTemplate::parse("Service Account {email}").unwrap();
        assert_eq!(
            PrincipalDetails::of(&jwt(&claims), Some(&template)).name(),
            Some("Jane Doe")
        );
    }

    #[test]
    fn test_kubernetes_user_is_an_application_named_by_its_username() {
        let user = KubernetesUser::builder()
            .username("system:serviceaccount:my-namespace:my-serviceaccount")
            .uid("cccccccc-0000-0000-0000-000000000001")
            .groups(vec!["system:serviceaccounts".to_string()])
            .extra(BTreeMap::from([(
                "email".to_string(),
                vec!["sa@example.com".to_string()],
            )]))
            .build();
        let authentication = Authentication::builder()
            .subject(Subject::new(
                Some("kubernetes".to_string()),
                "cccccccc-0000-0000-0000-000000000001".to_string(),
            ))
            .identity(user)
            .build();

        assert_eq!(
            PrincipalDetails::of(&authentication, None),
            details(
                Some("system:serviceaccount:my-namespace:my-serviceaccount"),
                None,
                UserType::Application
            )
        );
        #[cfg(feature = "router")]
        assert_eq!(token_roles(&authentication, "groups"), None);
    }

    #[cfg(feature = "router")]
    #[test]
    fn test_get_roles_simple_claim() {
        let claims = serde_json::json!({
            "roles": ["admin", "user", "editor"]
        });

        let roles = get_roles(&claims, "roles");
        assert_eq!(
            roles,
            Some(vec![
                "admin".to_string(),
                "user".to_string(),
                "editor".to_string()
            ])
        );
    }

    #[cfg(feature = "router")]
    #[test]
    fn test_get_roles_nested_claim() {
        let claims = serde_json::json!({
            "resource_access": {
                "account": {
                    "roles": ["manage-account", "view-profile"]
                }
            }
        });

        let roles = get_roles(&claims, "resource_access.account.roles");
        assert_eq!(
            roles,
            Some(vec![
                "manage-account".to_string(),
                "view-profile".to_string()
            ])
        );
    }

    #[cfg(feature = "router")]
    #[test]
    fn test_get_roles_no_match() {
        let claims = serde_json::json!({
            "other_field": "value"
        });

        let roles = get_roles(&claims, "roles");
        assert_eq!(roles, None);
    }

    #[cfg(feature = "router")]
    #[test]
    fn test_get_roles_single_string() {
        let claims = serde_json::json!({
            "role": "admin"
        });

        let roles = get_roles(&claims, "role");
        assert_eq!(roles, Some(vec!["admin".to_string()]));
    }

    #[cfg(feature = "router")]
    #[test]
    fn test_get_roles_array_with_non_strings() {
        // Array contains only non-string values (numbers, objects, nulls)
        let claims = serde_json::json!({
            "roles": [42, {"name": "admin"}, null, true]
        });

        // Should return None, not Some(vec![]), because no valid strings found
        let roles = get_roles(&claims, "roles");
        assert_eq!(roles, None);
    }

    #[cfg(feature = "router")]
    #[test]
    fn test_get_roles_empty_array() {
        // Explicitly empty array
        let claims = serde_json::json!({
            "roles": []
        });

        // Should return None, not Some(vec![]), treating empty like non-existent
        let roles = get_roles(&claims, "roles");
        assert_eq!(roles, None);
    }

    #[cfg(feature = "router")]
    #[test]
    fn test_get_roles_mixed_array() {
        // Array contains both strings and non-strings
        let claims = serde_json::json!({
            "roles": ["admin", 42, "user", null, "editor", {"name": "invalid"}]
        });

        // Should extract only the string values, filtering out non-strings
        let roles = get_roles(&claims, "roles");
        assert_eq!(
            roles,
            Some(vec![
                "admin".to_string(),
                "user".to_string(),
                "editor".to_string()
            ])
        );
    }
}
