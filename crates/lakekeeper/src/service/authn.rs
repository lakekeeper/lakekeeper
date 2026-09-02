#[cfg(feature = "router")]
use std::sync::Arc;
use std::{collections::HashMap, fmt::Debug};

#[cfg(feature = "router")]
use axum::{
    extract::{Request, State},
    middleware::Next,
    response::{IntoResponse, Response},
};
#[cfg(feature = "router")]
use axum_extra::{
    TypedHeader,
    headers::{Authorization, authorization::Bearer},
};
#[cfg(feature = "router")]
use http::HeaderMap;
use iceberg_ext::catalog::rest::ErrorModel;
use limes::{AuthenticatorEnum, Subject, format_subject, parse_subject};
use serde::{Deserialize, Serialize};

use crate::{CONFIG, api, service::ArcRole};
#[cfg(feature = "router")]
use crate::{
    XXHashSet,
    request_metadata::{RequestMetadata, TokenRoles},
    service::{
        RoleIdent,
        admission::{AdmissionContext, AdmissionGates, AdmissionRejection},
        authz::InstanceAdminMembership,
        events::EventDispatcher,
    },
};

pub const IDP_SEPARATOR: char = '~';
pub const ASSUME_ROLE_BY_ID_HEADER: &str = "x-assume-role";

#[derive(Debug, Clone, PartialEq, Eq, strum_macros::Display)]
pub enum Actor {
    Anonymous,
    #[strum(to_string = "Principal({0})")]
    Principal(UserId),
    #[strum(to_string = "AssumedRole({assumed_role}) by Principal({principal})")]
    Role {
        principal: UserId,
        assumed_role: ArcRole,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, derive_more::From, strum_macros::Display)]
pub(crate) enum InternalActor {
    LakekeeperInternal,
    External(Actor),
}

#[cfg(feature = "router")]
#[derive(Debug, Clone)]
pub(crate) struct AuthMiddlewareState<
    C: super::CatalogStore,
    T: limes::Authenticator,
    A: super::Authorizer,
> {
    pub authenticator: T,
    pub authorizer: A,
    pub events: EventDispatcher,
    pub catalog_state: C::State,
    /// Source of instance-admin membership, resolved once per request into the
    /// binary `RequestMetadata::is_instance_admin` flag. Defaults to
    /// [`ConfiguredInstanceAdmins`](super::authz::ConfiguredInstanceAdmins).
    pub instance_admin_membership: Arc<dyn InstanceAdminMembership>,
    /// Post-authentication admission gates, evaluated once per authenticated
    /// request after actor/instance-admin resolution and before the request
    /// reaches any handler. Empty by default (admits everything).
    pub admission_gates: AdmissionGates,
}

#[derive(Hash, Debug, Clone, PartialEq, Eq)]
pub struct UserId(Subject);

pub type UserIdRef = std::sync::Arc<UserId>;

pub(crate) const OIDC_IDP_ID: &str = "oidc";
pub(crate) const K8S_IDP_ID: &str = "kubernetes";

/// Default subject-claim preference order applied when a provider does not set
/// `subject_claims` explicitly. Kept in sync with the `subject_claims` doc on
/// [`OidcProviderConfig`].
///
/// `oid` is preferred so Entra-ID gets the stable per-tenant identifier
/// out-of-the-box; everything else falls through to `sub`.
const DEFAULT_SUBJECT_CLAIMS: &[&str] = &["oid", "sub"];
/// The `separator` value that means "any whitespace" rather than a literal to match.
pub(crate) const WHITESPACE_SEPARATOR: &str = "whitespace";

/// Configuration for a single OIDC provider in multi-provider mode.
///
/// Lives next to the rest of the OIDC machinery (`build_oidc_authenticator`,
/// the chain assembly, the IdP-ID constants) so the type and its consumers
/// share one module. `DynAppConfig` only holds a `HashMap<String, _>` of these.
///
/// Each provider fetches its own JWKS keys independently, allowing
/// authentication from multiple identity sources (e.g., Okta for users + EKS
/// OIDC for Kubernetes service accounts).
///
/// # Example Environment Variables
/// ```bash
/// LAKEKEEPER__OPENID_PROVIDERS__OKTA__URI=https://company.okta.com
/// LAKEKEEPER__OPENID_PROVIDERS__OKTA__AUDIENCE=https://company.okta.com
/// LAKEKEEPER__OPENID_PROVIDERS__OKTA__SUBJECT_CLAIMS=sub
/// ```
#[derive(Clone, Deserialize, Serialize, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct OidcProviderConfig {
    /// The OIDC provider URI (must expose .well-known/openid-configuration)
    pub uri: url::Url,
    /// Expected audience(s) for tokens from this provider.
    /// Specify multiple audiences as a comma-separated list.
    #[serde(
        default,
        deserialize_with = "crate::config::deserialize_comma_separated",
        serialize_with = "crate::config::serialize_comma_separated"
    )]
    pub audience: Option<Vec<String>>,
    /// Additional issuers to trust for this provider.
    #[serde(
        default,
        deserialize_with = "crate::config::deserialize_comma_separated",
        serialize_with = "crate::config::serialize_comma_separated"
    )]
    pub additional_issuers: Option<Vec<String>>,
    /// A scope that must be present in tokens from this provider.
    #[serde(default)]
    pub scope: Option<String>,
    /// Claim paths to use as the subject (user ID), in order of preference.
    /// Supports nested claims using dot notation, as for `roles_claim`.
    /// Defaults to `oid`, then `sub` if not specified.
    #[serde(
        default,
        deserialize_with = "crate::config::deserialize_comma_separated",
        serialize_with = "crate::config::serialize_comma_separated"
    )]
    pub subject_claims: Option<Vec<String>>,
    /// Claim to use in provided JWT tokens to extract roles.
    /// The field should contain a single string claim path.
    /// Supports nested claims using dot notation, e.g., `resource_access.account.roles`
    #[serde(default)]
    pub roles_claim: Option<String>,
    /// Template for a user's display name when the token carries no name claim
    /// (e.g. a machine / service-account token). Placeholders of the form
    /// `{claim.path}` are substituted from the token's claims using dot notation;
    /// `{email}` and `{sub}` are the common cases. Example: `Service Account {email}`.
    /// Write a literal brace by doubling it (`{{`/`}}`). If any referenced claim is
    /// absent or not a string, the template is skipped and the user keeps the
    /// default placeholder name. A real name claim always takes precedence.
    /// Validated at startup: a structurally malformed template (unbalanced braces
    /// or an empty `{}`) aborts boot. Unset by default.
    #[serde(default)]
    pub display_name_template: Option<String>,
    /// If true, fail startup when this provider's OIDC/JWKS configuration cannot be loaded.
    #[serde(default = "default_true")]
    pub require_connected_on_startup: bool,
    /// Rules a verified token must satisfy, keyed by rule name (`[a-z0-9-]+`). All rules must
    /// hold; a missing claim fails. Rejected tokens get a generic 401.
    ///
    /// ```bash
    /// LAKEKEEPER__OPENID_PROVIDERS__OKTA__REQUIRED_CLAIMS__ORG__CLAIM=organizations
    /// LAKEKEEPER__OPENID_PROVIDERS__OKTA__REQUIRED_CLAIMS__ORG__ANY_OF=[tenant-a, tenant-b]
    /// ```
    #[serde(default)]
    pub required_claims: HashMap<String, ClaimRuleConfig>,
}

/// One required-claim rule as written in configuration; see
/// [`OidcProviderConfig::required_claims`]. Exactly one of `any_of`, `all_of`, `none_of`,
/// `exists` must be set; converted to a validated [`limes::ClaimRule`] at startup.
#[derive(Clone, Deserialize, Serialize, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClaimRuleConfig {
    /// Dotted claim path, e.g. `organizations` or `realm_access.roles`.
    pub claim: String,
    /// Split a string claim on this literal before matching. Env values are trimmed, so a
    /// space must be quoted: `SEPARATOR='" "'`.
    #[serde(default)]
    pub separator: Option<String>,
    /// At least one claim value is in the list.
    #[serde(default, deserialize_with = "crate::config::deserialize_string_list")]
    pub any_of: Option<Vec<String>>,
    /// Every listed value is a claim value.
    #[serde(default, deserialize_with = "crate::config::deserialize_string_list")]
    pub all_of: Option<Vec<String>>,
    /// No claim value is in the list; a missing claim fails.
    #[serde(default, deserialize_with = "crate::config::deserialize_string_list")]
    pub none_of: Option<Vec<String>>,
    /// `true`: the claim is populated — it holds a value that is not blank, or, for an
    /// object, a member; `false`: absent or `null`. These are not opposites: a claim that is
    /// present but empty (`[]`, `""`, `"   "`, `{}`) satisfies neither.
    #[serde(default)]
    pub exists: Option<bool>,
}

impl OidcProviderConfig {
    /// Whether this provider rejects tokens a bare signature check would admit.
    ///
    /// The one definition, shared by startup validation and the routing check, so a third
    /// kind of guard cannot be added to one and forgotten in the other.
    pub(crate) fn is_guarded(&self) -> bool {
        !self.required_claims.is_empty() || self.scope.is_some()
    }
}

impl ClaimRuleConfig {
    /// Build the validated rule.
    ///
    /// # Errors
    /// If not exactly one operator is set, or the rule is structurally invalid.
    pub fn to_rule(&self) -> anyhow::Result<limes::ClaimRule> {
        type Build = fn(&str, &[String]) -> Result<limes::ClaimRule, limes::ClaimRuleError>;
        let list: [(&Option<Vec<String>>, Build); 3] = [
            (&self.any_of, |c, v| limes::ClaimRule::any_of(c, v)),
            (&self.all_of, |c, v| limes::ClaimRule::all_of(c, v)),
            (&self.none_of, |c, v| limes::ClaimRule::none_of(c, v)),
        ];
        let mut builders: Vec<_> = list
            .into_iter()
            .filter_map(|(values, build)| values.as_ref().map(|v| (v, build)))
            .map(|(values, build)| build(&self.claim, values))
            .collect();
        if let Some(exists) = self.exists {
            builders.push(limes::ClaimRule::exists(&self.claim, exists));
        }
        let [rule] = <[_; 1]>::try_from(builders).map_err(|found| {
            anyhow::anyhow!(
                "exactly one of `any_of`, `all_of`, `none_of`, `exists` must be set, found {}",
                found.len()
            )
        })?;
        let mut rule = rule?;
        if let Some(separator) = &self.separator {
            // A literal separator is byte-exact, so a deny written for space-delimited scopes
            // is blind to a tab. `whitespace` reaches the separator that splits on any of them.
            let separator = if separator.eq_ignore_ascii_case(WHITESPACE_SEPARATOR) {
                limes::Separator::Whitespace
            } else {
                limes::Separator::Literal(separator.clone())
            };
            rule = rule.with_separator(separator).map_err(|e| match e {
                limes::ClaimRuleError::EmptySeparator => anyhow::anyhow!(
                    "{e}; environment values are trimmed, quote a space as SEPARATOR='\" \"'"
                ),
                e => anyhow::anyhow!("{e}"),
            })?;
        }
        Ok(rule)
    }
}

/// Validated rules for one provider, in deterministic (name) order.
///
/// Rules are named `<idp_id>/<rule>` so a rejection identifies the provider as well as the
/// rule; rule names are only unique within a provider.
fn required_claim_rules(
    idp_id: &str,
    rules: &HashMap<String, ClaimRuleConfig>,
) -> anyhow::Result<Vec<(String, limes::ClaimRule)>> {
    let mut names: Vec<&String> = rules.keys().collect();
    names.sort();
    names
        .into_iter()
        .map(|name| {
            let rule = rules[name].to_rule().map_err(|e| {
                anyhow::anyhow!(
                    "invalid required claim rule `{name}` for OIDC provider `{idp_id}`: {e}"
                )
            })?;
            Ok((format!("{idp_id}/{name}"), rule))
        })
        .collect()
}

const fn default_true() -> bool {
    true
}

#[derive(Debug, Clone)]
pub enum BuiltInAuthenticators {
    Single(AuthenticatorEnum),
    Chain(limes::AuthenticatorChain<AuthenticatorEnum>),
}

/// Get the default authenticator configuration from the environment.
///
/// Supports both single-provider mode (via `OPENID_PROVIDER_URI`) and
/// multi-provider mode (via `OPENID_PROVIDERS` map). Multi-provider mode
/// is additive and extends the single-provider configuration.
///
/// # Errors
/// If the authenticator cannot be created, or if the configuration is invalid.
#[allow(clippy::too_many_lines)]
pub async fn get_default_authenticator_from_config() -> anyhow::Result<Option<BuiltInAuthenticators>>
{
    // K8s has no `require_connected_on_startup` analog: there's only ever one
    // cluster, so a failure here is always fatal. Unlike OIDC (where N
    // independent providers can each be marked optional), an unavailable K8s
    // API at boot means we can't authenticate service-account tokens at all,
    // which would silently degrade authn — so we fail closed via `?` below.
    let authn_k8s_audience = if CONFIG.enable_kubernetes_authentication {
        let mut authenticator =
            limes::kubernetes::KubernetesAuthenticator::try_new_with_default_client(
                Some(K8S_IDP_ID),
                CONFIG
                    .kubernetes_authentication_audience
                    .clone()
                    .unwrap_or_default(),
            )
            .await
            .inspect_err(|e| tracing::error!("Failed to create K8s authorizer: {e}"))?;
        authenticator
            .set_subject_source(CONFIG.kubernetes_authentication_subject_source.to_limes());
        tracing::info!("K8s authorizer created {authenticator:?}");
        Some(authenticator)
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
        authenticator
            .set_subject_source(CONFIG.kubernetes_authentication_subject_source.to_limes());
        tracing::info!(
            "K8s authorizer for legacy service account tokens created {:?}",
            authenticator
        );

        Some(authenticator)
    } else {
        tracing::info!("Running without Kubernetes authentication for legacy service accounts.");
        None
    };

    assemble_authenticator_chain(
        &CONFIG,
        authn_k8s_audience.map(AuthenticatorEnum::from),
        authn_k8s_legacy.map(AuthenticatorEnum::from),
    )
    .await
}

/// Build the OIDC list, apply the fail-closed guard, then assemble the
/// final chain with any pre-built K8s authenticators. Shared by the
/// production entry point and tests so the fail-closed error message
/// has exactly one source of truth.
async fn assemble_authenticator_chain(
    config: &crate::config::DynAppConfig,
    authn_k8s_audience: Option<AuthenticatorEnum>,
    authn_k8s_legacy: Option<AuthenticatorEnum>,
) -> anyhow::Result<Option<BuiltInAuthenticators>> {
    let oidc_provider_configs = oidc_provider_configs_from_config(config);
    let configured_provider_count = oidc_provider_configs.len();
    let authn_oidc_list = if oidc_provider_configs.is_empty() {
        tracing::info!("Running without OIDC authentication.");
        vec![]
    } else {
        tracing::info!("Configuring {configured_provider_count} OIDC provider(s)");
        build_oidc_authenticators(oidc_provider_configs).await?
    };

    // `require_connected_on_startup=false` gates only THIS provider's boot-time
    // failure; it must not allow the whole auth system to silently disable
    // itself. If every configured provider was skipped, refuse to boot.
    if configured_provider_count > 0 && authn_oidc_list.is_empty() {
        return Err(anyhow::anyhow!(
            "All {configured_provider_count} configured OIDC provider(s) failed to initialize. \
             Refusing to start with authentication disabled. Fix the providers' OIDC discovery \
             endpoints, or remove `REQUIRE_CONNECTED_ON_STARTUP=false` from at least one to \
             surface the underlying error."
        ));
    }

    // Collect all authenticators into a chain: OIDC first (priority), then any additional
    let mut all_authenticators: Vec<AuthenticatorEnum> = authn_oidc_list;
    if let Some(authn) = authn_k8s_audience {
        all_authenticators.push(authn);
    }
    if let Some(authn) = authn_k8s_legacy {
        all_authenticators.push(authn);
    }

    match all_authenticators.len() {
        0 => {
            tracing::warn!("Authentication is disabled. This is not suitable for production!");
            Ok(None)
        }
        1 => Ok(Some(all_authenticators.remove(0).into())),
        _ => {
            let mut chain_builder = limes::AuthenticatorChain::<AuthenticatorEnum>::builder();
            for auth in all_authenticators {
                chain_builder = chain_builder.add_authenticator(auth);
            }
            Ok(Some(chain_builder.build().into()))
        }
    }
}

fn oidc_provider_configs_from_config(
    config: &crate::config::DynAppConfig,
) -> Vec<(String, OidcProviderConfig)> {
    let mut providers = Vec::new();

    if let Some(uri) = config.openid_provider_uri.clone() {
        providers.push((
            OIDC_IDP_ID.to_string(),
            OidcProviderConfig {
                uri,
                audience: config.openid_audience.clone(),
                additional_issuers: config.openid_additional_issuers.clone(),
                scope: config.openid_scope.clone(),
                subject_claims: config.openid_subject_claim.clone(),
                roles_claim: config.openid_roles_claim.clone(),
                display_name_template: config.openid_display_name_template.clone(),
                require_connected_on_startup: true,
                required_claims: config.openid_required_claims.clone(),
            },
        ));
    }

    if !config.openid_providers.is_empty() {
        let mut extras = config
            .openid_providers
            .iter()
            .map(|(idp_id, provider)| (idp_id.clone(), provider.clone()))
            .collect::<Vec<_>>();
        extras.sort_by(|(left, _), (right, _)| left.cmp(right));
        providers.extend(extras);
    }

    providers
}

/// Build authenticators for configured OIDC providers.
///
/// `providers` must be supplied in the order they should appear in the
/// authenticator chain — sort upstream (see `oidc_provider_configs_from_config`).
/// `Vec` is used over `HashMap` precisely to carry this ordering.
async fn build_oidc_authenticators(
    providers: Vec<(String, OidcProviderConfig)>,
) -> anyhow::Result<Vec<AuthenticatorEnum>> {
    // Validate every provider's display-name template up front, before any network
    // work. A malformed template is a static config bug, so a typo in any provider's
    // template aborts startup cleanly rather than after earlier providers' JWKS have
    // already been fetched. Malformed templates never take the "skip this provider"
    // path below (which is only for connectivity failures) — that would silently
    // disable an IdP over a typo.
    let mut validated = Vec::with_capacity(providers.len());
    for (idp_id, provider) in providers {
        let display_name_template = provider
            .display_name_template
            .as_deref()
            .filter(|t| !t.is_empty())
            .map(limes::jwks::DisplayNameTemplate::parse)
            .transpose()
            .map_err(|e| {
                anyhow::anyhow!("invalid `display_name_template` for OIDC provider `{idp_id}`: {e}")
            })?;
        let required_claims = required_claim_rules(&idp_id, &provider.required_claims)?;
        validated.push((idp_id, provider, display_name_template, required_claims));
    }

    let mut authenticators = Vec::new();
    let mut routing = Vec::new();
    for (idp_id, provider, display_name_template, required_claims) in validated {
        tracing::info!(
            "Creating OIDC authenticator for {} ({})",
            idp_id,
            provider.uri
        );

        match build_oidc_authenticator(&idp_id, &provider, display_name_template, required_claims)
            .await
        {
            Ok(authenticator) => {
                routing.push(AuthenticatorRouting {
                    idp_id: idp_id.clone(),
                    // The first issuer is the one published by the provider's discovery
                    // document, which is not derivable from configuration.
                    issuers: authenticator.issuers().to_vec(),
                    audiences: authenticator.audiences().to_vec(),
                    guarded: provider.is_guarded(),
                });
                authenticators.push(AuthenticatorEnum::from(authenticator));
                tracing::info!("Successfully added OIDC authenticator: {}", idp_id);
            }
            Err(e) => {
                if provider.require_connected_on_startup {
                    return Err(anyhow::anyhow!(
                        "Failed to create required OIDC authenticator for {idp_id} ({uri}): {e}",
                        uri = provider.uri
                    ));
                }
                tracing::error!(
                    "Failed to create OIDC authenticator for {} ({}): {}. Skipping this provider.",
                    idp_id,
                    provider.uri,
                    e
                );
            }
        }
    }

    validate_authenticator_routing(&routing)?;

    Ok(authenticators)
}

/// How a built authenticator is selected for a token.
#[derive(Debug)]
struct AuthenticatorRouting {
    idp_id: String,
    issuers: Vec<String>,
    audiences: Vec<String>,
    /// The provider enforces a scope or required-claim rules, so it matters that its own
    /// tokens actually reach it.
    guarded: bool,
}

/// Reject provider combinations whose tokens cannot be routed as configured.
///
/// A token goes to the first authenticator whose issuer set contains its `iss` and whose
/// audience set intersects its `aud` (an empty audience set matches anything); there is no
/// fallthrough. Two providers publishing the same issuer therefore compete, and a token
/// naming both audiences reaches only the first — which is why disjoint audiences are not
/// enough to protect a provider that enforces a scope or required claims.
///
/// This runs on the issuers each provider actually published, so it also catches one identity
/// provider reached through two URLs (an in-cluster and a public hostname, say).
fn validate_authenticator_routing(providers: &[AuthenticatorRouting]) -> anyhow::Result<()> {
    for (i, a) in providers.iter().enumerate() {
        for b in &providers[i + 1..] {
            if !a.issuers.iter().any(|issuer| b.issuers.contains(issuer)) {
                continue;
            }
            let (first, second) = (&a.idp_id, &b.idp_id);
            // Disjoint audience lists do not separate the providers: one token may name both
            // audiences, and it reaches `{first}` alone. Only the second provider loses its
            // rules — when `{first}` is the guarded one it still enforces them on everything
            // it takes, which is strict rather than permissive.
            anyhow::ensure!(
                !b.guarded,
                "OIDC providers `{first}` and `{second}` publish the same issuer, and \
                 `{second}` enforces a scope or required claims. A token naming both \
                 audiences reaches only `{first}`, so the rules of `{second}` would not be \
                 enforced. Configure the same rules on `{first}`, or serve them from separate \
                 issuers."
            );
            if a.audiences.is_empty()
                || b.audiences.is_empty()
                || a.audiences.iter().any(|aud| b.audiences.contains(aud))
            {
                tracing::warn!(
                    "OIDC providers `{first}` and `{second}` publish the same issuer and accept \
                     overlapping audiences. Tokens matching both reach only `{first}`, so the \
                     settings of `{second}` may never apply."
                );
            } else if a.guarded {
                // Disjoint audiences do not make this safe: `{first}` enforces its rules only
                // on tokens it takes, and the issuer's other tokens reach `{second}`, which
                // enforces nothing.
                tracing::warn!(
                    "OIDC provider `{first}` publishes the same issuer as `{second}` and \
                     enforces a scope or required claims, but `{second}` does not. Tokens of \
                     that issuer carrying only `{second}`'s audience are admitted without \
                     `{first}`'s rules."
                );
            }
        }
    }
    Ok(())
}

async fn build_oidc_authenticator(
    idp_id: &str,
    provider: &OidcProviderConfig,
    display_name_template: Option<limes::jwks::DisplayNameTemplate>,
    required_claims: Vec<(String, limes::ClaimRule)>,
) -> anyhow::Result<limes::jwks::JWKSWebAuthenticator> {
    let mut authenticator = limes::jwks::JWKSWebAuthenticator::new(
        provider.uri.as_ref(),
        Some(std::time::Duration::from_hours(1)),
    )
    .await?
    .set_idp_id(idp_id);

    if let Some(audiences) = &provider.audience {
        tracing::debug!("Setting accepted audiences for {idp_id}: {audiences:?}");
        authenticator = authenticator.set_accepted_audiences(audiences.clone());
    }

    if let Some(issuers) = &provider.additional_issuers {
        tracing::debug!("Setting additional issuers for {idp_id}: {issuers:?}");
        authenticator = authenticator.add_additional_issuers(issuers.clone());
    }

    if let Some(scope) = &provider.scope {
        tracing::debug!("Setting scope for {idp_id}: {scope}");
        // Unreachable when config validation ran (`validate_required_claims`); kept as a
        // guard for programmatic configs.
        authenticator = authenticator
            .set_scope(scope.clone())
            .map_err(|e| anyhow::anyhow!("invalid `scope` for OIDC provider `{idp_id}`: {e}"))?;
    }

    if let Some(claims) = &provider.subject_claims {
        tracing::debug!("Setting subject claims for {idp_id}: {claims:?}");
        authenticator = authenticator.with_subject_claims(claims.clone());
    } else {
        tracing::debug!(
            "Defaulting subject claims for {idp_id} to: {DEFAULT_SUBJECT_CLAIMS:?}. \
             We prefer `oid` for Entra-ID (where `sub` differs per application); other IdPs \
             fall through to `sub`. Set `subject_claims` explicitly in production."
        );
        authenticator = authenticator.with_subject_claims(
            DEFAULT_SUBJECT_CLAIMS
                .iter()
                .map(|s| (*s).to_string())
                .collect(),
        );
    }

    if let Some(roles_claim) = &provider.roles_claim {
        tracing::debug!("Setting roles claim for {idp_id}: {roles_claim}");
        authenticator = authenticator.with_role_claim(roles_claim.clone());
    }

    if let Some(template) = display_name_template {
        tracing::debug!("Setting display name template for {idp_id}");
        authenticator = authenticator.with_display_name_template(template);
    }

    // At info, and unconditionally: an operator needs boot-time confirmation that the rules
    // they configured are active, and a count of zero is how a misspelled `REQUIRED_CLAIMS`
    // container shows up — the spelling that reaches no field is silently dropped, so the
    // line that would be missing is exactly the one worth reading. Names only, never values.
    let names: Vec<&str> = required_claims.iter().map(|(n, _)| n.as_str()).collect();
    tracing::info!(
        "Requiring {} claim rule(s) for OIDC provider {idp_id}: {names:?}",
        names.len()
    );
    if !required_claims.is_empty() {
        authenticator = authenticator.with_required_claims(required_claims);
    }

    Ok(authenticator)
}

/// The 401 for a token the authenticator did not accept. The cause is attached for
/// server-side logging only; the body never names a rule, claim or expected value.
#[cfg(feature = "router")]
fn authentication_failed_response(cause: limes::error::Error) -> Response {
    ErrorModel::unauthorized(
        "Authentication failed",
        "AuthenticationFailed",
        Some(Box::new(cause)),
    )
    .into_response()
}

#[cfg(feature = "router")]
#[allow(clippy::too_many_lines)]
/// Use a limes [`Authenticator`] to Authenticate a request.
///
/// This middleware needs to run after [`create_request_metadata_with_trace_and_project_fn`](crate::request_metadata::create_request_metadata_with_trace_and_project_fn).
pub(crate) async fn auth_middleware_fn<
    C: super::CatalogStore,
    T: limes::Authenticator,
    A: super::authz::Authorizer,
>(
    State(state): State<AuthMiddlewareState<C, T, A>>,
    authorization: Option<TypedHeader<Authorization<Bearer>>>,
    headers: HeaderMap,
    mut request: Request,
    next: Next,
) -> Response {
    use crate::service::authz::AuthZServerOps;

    let authenticator = &state.authenticator;
    let authorizer = &state.authorizer;
    let catalog_state = state.catalog_state;
    let Some(authorization) = authorization else {
        return ErrorModel::unauthorized(
            "Missing Authorization Header",
            "MissingAuthorizationHeader",
            None,
        )
        .into_response();
    };

    let token = authorization.token();
    let introspection = limes::introspect::introspect(token);
    let authentication = match authenticator.authenticate(token, &introspection).await {
        Ok(principal) => principal,
        Err(e) => return authentication_failed_response(e),
    };
    let user_id = match UserId::try_new(authentication.subject().clone()) {
        Ok(user_id) => user_id,
        Err(e) => {
            return e.into_response();
        }
    };
    let role_id = match extract_role_id(&headers) {
        Ok(role_id) => role_id,
        Err(e) => return e.into_response(),
    };
    let actor = match resolve_actor::<C>(user_id, role_id, catalog_state).await {
        Ok(actor) => actor,
        Err(e) => return e,
    };

    if let Some(request_metadata) = request.extensions_mut().get_mut::<RequestMetadata>() {
        match extract_and_set_token_roles(&authentication, request_metadata) {
            Ok(Some(token_roles)) => {
                request_metadata.set_token_roles(token_roles);
            }
            Ok(None) => {}
            Err(e) => return e.into_response(),
        }

        request_metadata.set_authentication(actor.clone(), authentication.clone());

        // Instance-admin membership is only ever consulted for an authenticated
        // principal. Assumed-roles (`Actor::Role`) and anonymous callers never
        // inherit instance-admin — role assumption is an explicit opt-in to a
        // narrower scope — so we extract the `UserId` here and never reach the
        // membership source for non-principal actors.
        if let Actor::Principal(user_id) = &actor
            && state
                .instance_admin_membership
                .is_instance_admin(user_id)
                .await
        {
            request_metadata.set_instance_admin(true);
        }

        // Identify trusted engines based on token identity (IdP, audiences, subject).
        // Each engine defines `identities` specifying who may act as that engine.
        // Multiple engines may match — this is intentional (e.g. an admin token
        // whose audience appears in several engine configs).
        let token_idp_id = authentication.subject().idp_id();
        let token_audiences: std::collections::HashSet<&str> = authentication
            .audiences()
            .iter()
            .map(String::as_str)
            .collect();
        let token_subject = Some(authentication.subject().subject_in_idp());

        if let Some(token_idp) = token_idp_id {
            let matching_engines: Vec<_> = CONFIG
                .trusted_engines
                .iter()
                .filter(|(_key, engine)| {
                    engine
                        .identities()
                        .get(token_idp)
                        .is_some_and(|id| id.matches(&token_audiences, token_subject))
                })
                .map(|(_, engine)| engine.clone())
                .collect();

            if !matching_engines.is_empty() {
                tracing::debug!(
                    count = matching_engines.len(),
                    "Identified trusted engine(s) from token identity"
                );
                request_metadata.set_engines(crate::config::MatchedEngines::new(matching_engines));
            }
        }

        let check_result = if let Some(role_id) = role_id {
            use crate::service::{
                authz::{ActionDescriptor, CatalogAction},
                events::APIEventContext,
            };

            #[derive(Debug)]
            struct AssumeRoleAction;
            impl CatalogAction for AssumeRoleAction {
                fn action_descriptor(&self) -> ActionDescriptor {
                    ActionDescriptor::builder()
                        .action_name("assume_role")
                        .build()
                }
            }

            let event_ctx = APIEventContext::for_role(
                std::sync::Arc::new(request_metadata.clone()),
                state.events.clone(),
                role_id,
                AssumeRoleAction,
            );

            event_ctx
                .emit_authz(authorizer.check_actor(&actor, request_metadata).await)
                .map(|_| ())
        } else {
            authorizer
                .check_actor(&actor, request_metadata)
                .await
                .map_err(crate::service::events::context::authz_to_error_no_audit)
        };

        // Ensure assume role, if present, is allowed
        if let Err(err) = check_result {
            return err.into_response();
        }

        // Post-authentication admission gates: a coarse, pluggable rejection of
        // an already-authenticated principal that must not be admitted to this
        // instance at all (e.g. an external control-plane permission service).
        // Runs after instance-admin and assumed-role resolution so a gate can
        // honor instance-admin status and see the resolved actor.
        // No-op unless the host binary registered at least one gate.
        if !state.admission_gates.is_empty() {
            // The raw bearer is handed to gates via a transient context, never
            // stored on `RequestMetadata`, so a gate can relay it to an external
            // service without it leaking into metadata/audit.
            let bearer_token = authorization.token();
            match state
                .admission_gates
                .admit(AdmissionContext::new(request_metadata, Some(bearer_token)))
                .await
            {
                // On admit, fold any roles the gate(s) resolved into the request
                // metadata for downstream authorization and audit.
                Ok(admission) => {
                    if let Some(roles) = admission.resolved_roles {
                        request_metadata.set_admission_roles(roles);
                    }
                }
                // The rejection variant carries its own HTTP semantics: an
                // authoritative deny is a plain 403, while a fail-closed
                // `Unavailable` is a 503 with the gate's chosen `Retry-After`.
                Err(rejection) => {
                    return match rejection {
                        AdmissionRejection::Forbidden { error, .. } => error.into_response(),
                        AdmissionRejection::Unavailable {
                            error, retry_after, ..
                        } => {
                            // `Retry-After` is whole seconds; round any
                            // sub-second remainder up so a sub-second Duration
                            // still asks for at least 1s of backoff rather than
                            // truncating to 0 ("retry immediately").
                            let secs =
                                retry_after.as_secs() + u64::from(retry_after.subsec_nanos() > 0);
                            let mut response = error.into_response();
                            response.headers_mut().insert(
                                axum::http::header::RETRY_AFTER,
                                axum::http::HeaderValue::from(secs),
                            );
                            response
                        }
                    };
                }
            }
        }
    }

    next.run(request).await
}

#[cfg(feature = "router")]
fn extract_role_id(
    headers: &HeaderMap,
) -> Result<Option<super::RoleId>, iceberg_ext::catalog::rest::IcebergErrorResponse> {
    if let Some(role_id) = headers.get(ASSUME_ROLE_BY_ID_HEADER) {
        let role_id = role_id.to_str().map_err(|e| {
            ErrorModel::bad_request(
                "Failed to parse Role-ID",
                "InvalidRoleIdError",
                Some(Box::new(e)),
            )
        })?;
        Ok(Some(super::RoleId::from_str_or_bad_request(role_id)?))
    } else {
        Ok(None)
    }
}

#[cfg(feature = "router")]
async fn resolve_actor<C: super::CatalogStore>(
    user_id: UserId,
    role_id: Option<super::RoleId>,
    catalog_state: C::State,
) -> Result<Actor, Response> {
    use crate::service::CatalogRoleOps;

    match role_id {
        Some(role_id) => {
            match C::get_role_by_id_across_projects_cache_aware(
                role_id,
                crate::service::CachePolicy::Use,
                catalog_state,
            )
            .await
            {
                Ok(role) => Ok(Actor::Role {
                    principal: user_id,
                    assumed_role: role,
                }),
                Err(e) => Err(ErrorModel::bad_request(
                    format!("Failed to resolve role with id {role_id} presented in header {ASSUME_ROLE_BY_ID_HEADER}"),
                    "InvalidAssumeRoleId",
                    Some(Box::new(e)),
                )
                .into_response()),
            }
        }
        None => Ok(Actor::Principal(user_id)),
    }
}

#[cfg(feature = "router")]
fn extract_and_set_token_roles(
    authentication: &limes::Authentication,
    request_metadata: &RequestMetadata,
) -> Result<Option<TokenRoles>, ErrorModel> {
    use crate::service::{RoleProviderId, RoleSourceId};

    let Some(roles) = authentication.roles() else {
        return Ok(None);
    };

    let Some(project_id) = request_metadata.preferred_project_id() else {
        return Err(ErrorModel::bad_request(
            "Default project must be set or X-Project-ID header must be provided if roles are extracted from tokens",
            "MissingProjectId",
            None,
        ));
    };

    let role_idents = roles
        .iter()
        .map(|source_id| {
            let source_id = RoleSourceId::try_new(source_id).map_err(|e| {
                ErrorModel::bad_request(
                    format!("Invalid Role in token: {e}"),
                    "RoleSourceIdError",
                    None,
                )
                .append_detail("Could not build Request Metadata")
            })?;
            let provider_id = authentication.subject().idp_id().ok_or_else(|| {
                ErrorModel::internal(
                    "Encountered Authenticator without provider / idp_id",
                    "AuthenticatorMissingProviderId",
                    None,
                )
            })?;
            let provider_id = RoleProviderId::new_unchecked(provider_id.clone());

            Ok(Arc::new(RoleIdent::new(provider_id, source_id)))
        })
        .collect::<Result<XXHashSet<_>, ErrorModel>>()?;

    Ok(Some(TokenRoles::new(project_id, role_idents)))
}

impl std::fmt::Display for UserId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", format_subject(&self.0, Some(IDP_SEPARATOR)))
    }
}

impl UserId {
    /// Create a new `UserId` from a `Subject`.
    ///
    /// # Errors
    /// Returns an error if the subject is invalid, e.g. empty or too long.
    pub fn try_new(subject: Subject) -> Result<Self, ErrorModel> {
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

    #[must_use]
    pub fn idp_id(&self) -> Option<&str> {
        self.0.idp_id().map(std::string::String::as_str)
    }

    #[must_use]
    pub fn subject_in_idp(&self) -> &str {
        self.0.subject_in_idp()
    }

    #[cfg(feature = "test-utils")]
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

impl InternalActor {
    #[must_use]
    #[inline]
    pub(crate) fn is_authenticated(&self) -> bool {
        match self {
            InternalActor::LakekeeperInternal => true,
            InternalActor::External(actor) => actor.is_authenticated(),
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

#[cfg(test)]
// `figment::Jail::expect_with` closures return `Result<(), figment::Error>`.
#[allow(clippy::result_large_err)]
mod tests {
    use std::time::Duration;

    use axum::{Json, Router, routing::get};
    use limes::Authenticator;
    use serde_json::json;
    use tokio::{net::TcpListener, task::JoinHandle};
    use url::Url;
    use uuid::Uuid;

    use super::*;
    use crate::{config::DynAppConfig, service::RoleId};

    async fn spawn_oidc_test_server() -> (Url, Url, JoinHandle<()>) {
        spawn_oidc_test_server_with_keys(json!({ "keys": [] })).await
    }

    async fn spawn_oidc_test_server_with_keys(
        jwks: serde_json::Value,
    ) -> (Url, Url, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind oidc test server");
        let addr = listener.local_addr().expect("oidc test server addr");
        let base = Url::parse(&format!("http://{addr}")).expect("oidc test server base url");
        let good_base = base.join("good/").expect("good base url");
        let bad_base = base.join("bad/").expect("bad base url");

        let good_config = json!({
            "issuer": good_base.as_str(),
            "jwks_uri": format!("{good_base}jwks"),
        });
        let bad_config = json!({
            "issuer": bad_base.as_str(),
        });

        let app = Router::new()
            .route(
                "/good/.well-known/openid-configuration",
                get({
                    let good_config = good_config.clone();
                    move || async move { Json(good_config) }
                }),
            )
            .route(
                "/bad/.well-known/openid-configuration",
                get({
                    let bad_config = bad_config.clone();
                    move || async move { Json(bad_config) }
                }),
            )
            .route(
                "/good/jwks",
                get({
                    let jwks = jwks.clone();
                    move || async move { Json(jwks) }
                }),
            );

        let handle = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("oidc test server failed");
        });

        (good_base, bad_base, handle)
    }

    fn any_of_rule(claim: &str, values: &[&str]) -> ClaimRuleConfig {
        ClaimRuleConfig {
            claim: claim.to_string(),
            separator: None,
            any_of: Some(values.iter().map(ToString::to_string).collect()),
            all_of: None,
            none_of: None,
            exists: None,
        }
    }

    fn provider(
        uri: &Url,
        required_claims: HashMap<String, ClaimRuleConfig>,
    ) -> OidcProviderConfig {
        OidcProviderConfig {
            uri: uri.clone(),
            audience: Some(vec![TEST_AUD.to_string()]),
            additional_issuers: None,
            scope: None,
            subject_claims: None,
            roles_claim: None,
            display_name_template: None,
            require_connected_on_startup: true,
            required_claims,
        }
    }

    const TEST_AUD: &str = "lakekeeper";

    struct Signer {
        key: jsonwebtoken::EncodingKey,
        jwks: serde_json::Value,
    }

    impl Signer {
        fn ed25519() -> Self {
            use base64::Engine as _;
            let key_pair = aws_lc_rs::signature::Ed25519KeyPair::generate().unwrap();
            let pkcs8 = key_pair.to_pkcs8().unwrap();
            let x = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(aws_lc_rs::signature::KeyPair::public_key(&key_pair).as_ref());
            Self {
                key: jsonwebtoken::EncodingKey::from_ed_der(pkcs8.as_ref()),
                jwks: json!({ "keys": [
                    { "kty": "OKP", "crv": "Ed25519", "alg": "EdDSA", "kid": "k1", "x": x }
                ]}),
            }
        }

        fn sign(&self, issuer: &Url, mut claims: serde_json::Value) -> String {
            let exp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 3600;
            claims["exp"] = json!(exp);
            claims["iss"] = json!(issuer.as_str());
            claims["aud"] = json!(TEST_AUD);
            let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::EdDSA);
            header.kid = Some("k1".to_string());
            jsonwebtoken::encode(&header, &claims, &self.key).unwrap()
        }
    }

    async fn authenticate(
        chain: &BuiltInAuthenticators,
        token: &str,
    ) -> Result<limes::Authentication, limes::error::Error> {
        let introspection = limes::introspect::introspect(token);
        match chain {
            BuiltInAuthenticators::Single(auth) => auth.authenticate(token, &introspection).await,
            BuiltInAuthenticators::Chain(chain) => chain.authenticate(token, &introspection).await,
        }
    }

    fn routing(
        idp_id: &str,
        issuers: &[&str],
        audiences: &[&str],
        guarded: bool,
    ) -> AuthenticatorRouting {
        AuthenticatorRouting {
            idp_id: idp_id.to_string(),
            issuers: issuers.iter().map(ToString::to_string).collect(),
            audiences: audiences.iter().map(ToString::to_string).collect(),
            guarded,
        }
    }

    /// One identity provider reached through two URLs publishes one issuer, which no
    /// configuration-time check can see. Disjoint audiences do not rescue it: a token naming
    /// both reaches only the first provider.
    #[test]
    fn shared_published_issuer_is_rejected_when_the_shadowed_provider_is_guarded() {
        let err = validate_authenticator_routing(&[
            routing(
                "oidc",
                &["https://sso.example.com/realms/x"],
                &["internal"],
                false,
            ),
            routing(
                "corp",
                &["https://sso.example.com/realms/x"],
                &["lakekeeper"],
                true,
            ),
        ])
        .unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("publish the same issuer"), "{msg}");
        assert!(msg.contains("`oidc`") && msg.contains("`corp`"), "{msg}");
    }

    #[test]
    fn shared_published_issuer_is_allowed_when_no_provider_is_guarded() {
        validate_authenticator_routing(&[
            routing("oidc", &["https://idp.example.com"], &["a"], false),
            routing("corp", &["https://idp.example.com"], &["b"], false),
        ])
        .unwrap();
    }

    /// The provider that wins routing enforces its own rules on everything it takes, so
    /// guarding it and not the one it shadows leaves nothing under-enforced.
    #[test]
    fn shared_published_issuer_is_allowed_when_only_the_first_provider_is_guarded() {
        validate_authenticator_routing(&[
            routing("oidc", &["https://idp.example.com"], &["a"], true),
            routing("corp", &["https://idp.example.com"], &["b"], false),
        ])
        .unwrap();
    }

    /// Only a shared issuer creates competition; different issuers never do, whatever the
    /// audiences are.
    #[test]
    fn distinct_issuers_never_compete() {
        validate_authenticator_routing(&[
            routing("oidc", &["https://a.example.com"], &["lakekeeper"], true),
            routing("corp", &["https://b.example.com"], &["lakekeeper"], true),
        ])
        .unwrap();
    }

    /// An issuer shared through `additional_issuers` competes just as much as a primary one.
    #[test]
    fn additional_issuers_are_compared_too() {
        let err = validate_authenticator_routing(&[
            routing(
                "oidc",
                &["https://a.example.com", "https://sts.example.com"],
                &["a"],
                false,
            ),
            routing(
                "corp",
                &["https://b.example.com", "https://sts.example.com"],
                &["b"],
                true,
            ),
        ])
        .unwrap_err();
        assert!(format!("{err:#}").contains("publish the same issuer"));
    }

    /// The routing check is unit-tested; what feeds it is not. A hardcoded `guarded: false`
    /// would make the check dead in production while every routing test stayed green.
    #[tokio::test]
    async fn routing_inputs_are_wired_from_the_built_authenticators() {
        let signer = Signer::ed25519();
        let (issuer, _bad, server) = spawn_oidc_test_server_with_keys(signer.jwks.clone()).await;
        let mut config = DynAppConfig::default();
        // Alphabetical order puts the unguarded provider first, so the guarded one is shadowed.
        config
            .openid_providers
            .insert("aaa".to_string(), provider(&issuer, HashMap::new()));
        config.openid_providers.insert(
            "bbb".to_string(),
            provider(
                &issuer,
                HashMap::from([(
                    "org".to_string(),
                    any_of_rule("organizations", &["tenant-a"]),
                )]),
            ),
        );
        let err = assemble_authenticator_chain(&config, None, None)
            .await
            .unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("publish the same issuer"), "{msg}");
        assert!(msg.contains("`bbb`"), "{msg}");
        server.abort();
    }

    /// Every rule of a provider must hold, and the alphabetically first failure is reported.
    #[tokio::test]
    async fn all_rules_of_a_provider_are_enforced_in_name_order() {
        let signer = Signer::ed25519();
        let (issuer, _bad, server) = spawn_oidc_test_server_with_keys(signer.jwks.clone()).await;
        let mut config = DynAppConfig::default();
        config.openid_providers.insert(
            "x".to_string(),
            provider(
                &issuer,
                HashMap::from([
                    (
                        "aaa".to_string(),
                        any_of_rule("organizations", &["tenant-a"]),
                    ),
                    ("zzz".to_string(), any_of_rule("department", &["eng"])),
                ]),
            ),
        );
        let chain = assemble_authenticator_chain(&config, None, None)
            .await
            .unwrap()
            .unwrap();

        let both = json!({ "sub": "u", "organizations": ["tenant-a"], "department": "eng" });
        assert!(
            authenticate(&chain, &signer.sign(&issuer, both))
                .await
                .is_ok()
        );

        // Satisfying only the first rule is not enough: the second is reported.
        let only_first = json!({ "sub": "u", "organizations": ["tenant-a"] });
        assert_eq!(
            authenticate(&chain, &signer.sign(&issuer, only_first))
                .await
                .unwrap_err()
                .rejection(),
            Some(&limes::RejectionReason::ClaimRuleFailed {
                rule: "x/zzz".to_string(),
            })
        );
        // Failing both names the alphabetically first one only.
        let neither = json!({ "sub": "u" });
        assert_eq!(
            authenticate(&chain, &signer.sign(&issuer, neither))
                .await
                .unwrap_err()
                .rejection(),
            Some(&limes::RejectionReason::ClaimRuleFailed {
                rule: "x/aaa".to_string(),
            })
        );
        server.abort();
    }

    /// Environment variables through to a signed token, the seam config tests and
    /// token tests each stop short of.
    #[tokio::test]
    async fn env_configured_required_claims_are_enforced() {
        let signer = Signer::ed25519();
        let (issuer, _bad, server) = spawn_oidc_test_server_with_keys(signer.jwks.clone()).await;
        let mut captured = None;
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__OPENID_PROVIDERS__X__URI", &issuer);
            jail.set_env(
                "LAKEKEEPER_TEST__OPENID_PROVIDERS__X__REQUIRED_CLAIMS__ORG__CLAIM",
                "organizations",
            );
            jail.set_env(
                "LAKEKEEPER_TEST__OPENID_PROVIDERS__X__REQUIRED_CLAIMS__ORG__ANY_OF",
                "[tenant-a]",
            );
            captured = Some(crate::config::get_config());
            Ok(())
        });
        let config = captured.unwrap();
        assert_eq!(config.openid_providers["x"].required_claims.len(), 1);

        let chain = assemble_authenticator_chain(&config, None, None)
            .await
            .unwrap()
            .unwrap();
        let ok = signer.sign(
            &issuer,
            json!({ "sub": "u", "organizations": ["tenant-a"] }),
        );
        assert_eq!(
            authenticate(&chain, &ok)
                .await
                .unwrap()
                .subject()
                .subject_in_idp(),
            "u"
        );
        let wrong = signer.sign(
            &issuer,
            json!({ "sub": "u", "organizations": ["tenant-b"] }),
        );
        assert_eq!(
            authenticate(&chain, &wrong).await.unwrap_err().rejection(),
            Some(&limes::RejectionReason::ClaimRuleFailed {
                rule: "x/org".to_string(),
            })
        );
        server.abort();
    }

    /// The flat container takes any spelling the environment offers, so a misspelt one reaches
    /// no field and the rules simply vanish — every token from that provider is then admitted.
    /// Nothing at parse time can catch it, which is why the rule count is logged at boot even
    /// when it is zero: the missing confirmation is the only signal an operator gets.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn a_misspelt_required_claims_container_leaves_no_rules() {
        let signer = Signer::ed25519();
        let (issuer, _bad, server) = spawn_oidc_test_server_with_keys(signer.jwks.clone()).await;
        let load = |key: &str| {
            let mut captured = None;
            let (issuer, key) = (issuer.clone(), key.to_string());
            figment::Jail::expect_with(|jail| {
                jail.set_env("LAKEKEEPER_TEST__OPENID_PROVIDER_URI", &issuer);
                jail.set_env(
                    format!("LAKEKEEPER_TEST__{key}__ORG__CLAIM"),
                    "organizations",
                );
                jail.set_env(format!("LAKEKEEPER_TEST__{key}__ORG__ANY_OF"), "[tenant-a]");
                captured = Some(crate::config::get_config());
                Ok(())
            });
            captured.unwrap()
        };
        assert_eq!(
            load("OPENID_REQUIRED_CLAIMS").openid_required_claims.len(),
            1
        );
        // Singular, and transposed: both are silently dropped.
        for misspelt in ["OPENID_REQUIRED_CLAIM", "OPENID_REQUIRED_CLAMIS"] {
            let config = load(misspelt);
            assert!(
                config.openid_required_claims.is_empty(),
                "{misspelt} must reach no field"
            );
            let chain = assemble_authenticator_chain(&config, None, None)
                .await
                .unwrap()
                .unwrap();
            let unwanted = signer.sign(
                &issuer,
                json!({ "sub": "u", "organizations": ["tenant-b"] }),
            );
            assert!(
                authenticate(&chain, &unwanted).await.is_ok(),
                "{misspelt}: no rule is enforced"
            );
            // The zero count is the operator's only signal that the rules never arrived, so
            // it must be logged rather than skipped as uninteresting.
            assert!(
                logs_contain("Requiring 0 claim rule(s) for OIDC provider oidc: []"),
                "{misspelt}: the zero rule count must be logged"
            );
        }
        server.abort();
    }

    #[tokio::test]
    async fn required_claims_admit_and_reject_signed_tokens() {
        let signer = Signer::ed25519();
        let (issuer, _bad, server) = spawn_oidc_test_server_with_keys(signer.jwks.clone()).await;
        let mut config = DynAppConfig::default();
        config.openid_providers.insert(
            "x".to_string(),
            provider(
                &issuer,
                HashMap::from([(
                    "org".to_string(),
                    any_of_rule("organizations", &["tenant-a"]),
                )]),
            ),
        );
        let chain = assemble_authenticator_chain(&config, None, None)
            .await
            .unwrap()
            .unwrap();

        let ok = signer.sign(
            &issuer,
            json!({ "sub": "u", "organizations": ["tenant-a"] }),
        );
        let auth = authenticate(&chain, &ok).await.unwrap();
        assert_eq!(auth.subject().subject_in_idp(), "u");

        let wrong = signer.sign(
            &issuer,
            json!({ "sub": "u", "organizations": ["tenant-b"] }),
        );
        let err = authenticate(&chain, &wrong).await.unwrap_err();
        assert_eq!(
            err.rejection(),
            Some(&limes::RejectionReason::ClaimRuleFailed {
                // Rules are reported as `<idp_id>/<rule>`.
                rule: "x/org".to_string(),
            })
        );
        let rendered = format!("{err} {err:?}");
        assert!(!rendered.contains("tenant-"), "{rendered}");

        let missing = signer.sign(&issuer, json!({ "sub": "u" }));
        assert_eq!(
            authenticate(&chain, &missing)
                .await
                .unwrap_err()
                .rejection(),
            Some(&limes::RejectionReason::ClaimRuleFailed {
                rule: "x/org".to_string(),
            })
        );

        server.abort();
    }

    /// `SCOPE=x` and an explicit whitespace-separated `all_of` rule on `scope` reject the
    /// same token the same way.
    #[tokio::test]
    async fn scope_sugar_is_equivalent_to_explicit_scope_rule() {
        let signer = Signer::ed25519();
        let (issuer, _bad, server) = spawn_oidc_test_server_with_keys(signer.jwks.clone()).await;

        let mut sugar = DynAppConfig::default();
        let mut p = provider(&issuer, HashMap::new());
        p.scope = Some("admin".to_string());
        sugar.openid_providers.insert("x".to_string(), p);

        let mut explicit = DynAppConfig::default();
        let rule = ClaimRuleConfig {
            claim: "scope".to_string(),
            separator: Some(" ".to_string()),
            any_of: None,
            all_of: Some(vec!["admin".to_string()]),
            none_of: None,
            exists: None,
        };
        explicit.openid_providers.insert(
            "x".to_string(),
            provider(&issuer, HashMap::from([("scopes".to_string(), rule)])),
        );

        let token_ok = signer.sign(&issuer, json!({ "sub": "u", "scope": "openid admin" }));
        let token_bad = signer.sign(&issuer, json!({ "sub": "u", "scope": "openid" }));
        // Where the two intentionally diverge: the sugar splits on any whitespace and falls
        // back to `scp`; an explicit rule splits only on its literal separator and reads
        // only the named claim.
        let token_tab = signer.sign(&issuer, json!({ "sub": "u", "scope": "openid\tadmin" }));
        let token_scp = signer.sign(&issuer, json!({ "sub": "u", "scp": ["admin"] }));
        for (config, rejection) in [
            (sugar, limes::RejectionReason::ScopeMissing),
            (
                explicit,
                limes::RejectionReason::ClaimRuleFailed {
                    rule: "x/scopes".to_string(),
                },
            ),
        ] {
            let chain = assemble_authenticator_chain(&config, None, None)
                .await
                .unwrap()
                .unwrap();
            authenticate(&chain, &token_ok).await.unwrap();
            let err = authenticate(&chain, &token_bad).await.unwrap_err();
            assert_eq!(err.rejection(), Some(&rejection));

            let tab = authenticate(&chain, &token_tab).await;
            let scp = authenticate(&chain, &token_scp).await;
            if rejection == limes::RejectionReason::ScopeMissing {
                tab.unwrap();
                scp.unwrap();
            } else {
                assert_eq!(tab.unwrap_err().rejection(), Some(&rejection));
                assert_eq!(scp.unwrap_err().rejection(), Some(&rejection));
            }
        }
        server.abort();
    }

    #[tokio::test]
    async fn rejected_token_response_is_a_generic_401() {
        use http_body_util::BodyExt as _;
        let cause = limes::error::Error::rejected(limes::RejectionReason::ClaimRuleFailed {
            rule: "org".to_string(),
        });
        let response = authentication_failed_response(cause);
        assert_eq!(response.status(), http::StatusCode::UNAUTHORIZED);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"]["type"], "AuthenticationFailed");
        assert_eq!(body["error"]["message"], "Authentication failed");
        assert_eq!(body["error"]["code"], 401);
        let stack = body["error"]["stack"].as_array().unwrap();
        assert_eq!(stack.len(), 1);
        assert!(stack[0].as_str().unwrap().starts_with("Error ID: "));
        let text = body.to_string();
        assert!(!text.contains("org"), "{text}");
        assert!(!text.contains("rule"), "{text}");
    }

    #[tokio::test]
    async fn invalid_required_claim_fails_startup_before_network() {
        let (_good, bad_base, server) = spawn_oidc_test_server().await;
        let mut rule = any_of_rule("org", &[]);
        rule.any_of = Some(vec![]);
        let providers = vec![(
            "acme".to_string(),
            provider(&bad_base, HashMap::from([("org".to_string(), rule)])),
        )];
        let err = build_oidc_authenticators(providers).await.unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("required claim rule `org`"), "{msg}");
        assert!(msg.contains("acme"), "{msg}");
        server.abort();
    }

    #[test]
    fn oidc_provider_configs_from_config_uses_legacy_provider_id_and_roles_claim() {
        let mut config = DynAppConfig::default();
        config.openid_provider_uri = Some(url::Url::parse("https://issuer.example.com").unwrap());
        config.openid_audience = Some(vec!["lakekeeper".to_string()]);
        config.openid_additional_issuers = Some(vec!["https://sts.example.com".to_string()]);
        config.openid_scope = Some("openid".to_string());
        config.openid_subject_claim = Some(vec!["sub".to_string()]);
        config.openid_roles_claim = Some("roles".to_string());
        config.openid_display_name_template = Some("Service Account {email}".to_string());
        config.openid_required_claims.insert(
            "org".to_string(),
            any_of_rule("organizations", &["tenant-a"]),
        );

        let providers = oidc_provider_configs_from_config(&config);

        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].0, OIDC_IDP_ID);
        assert_eq!(
            providers[0].1.required_claims,
            config.openid_required_claims
        );
        assert_eq!(
            providers[0].1.audience,
            Some(vec!["lakekeeper".to_string()])
        );
        assert_eq!(
            providers[0].1.additional_issuers,
            Some(vec!["https://sts.example.com".to_string()])
        );
        assert_eq!(providers[0].1.scope, Some("openid".to_string()));
        assert_eq!(providers[0].1.subject_claims, Some(vec!["sub".to_string()]));
        assert_eq!(providers[0].1.roles_claim, Some("roles".to_string()));
        assert_eq!(
            providers[0].1.display_name_template,
            Some("Service Account {email}".to_string())
        );
        assert!(providers[0].1.require_connected_on_startup);
    }

    #[test]
    fn oidc_provider_configs_from_config_adds_multi_provider_config() {
        let mut config = DynAppConfig::default();
        config.openid_provider_uri = Some(url::Url::parse("https://legacy.example.com").unwrap());
        config.openid_providers.insert(
            "okta".to_string(),
            OidcProviderConfig {
                uri: url::Url::parse("https://company.okta.com").unwrap(),
                audience: None,
                additional_issuers: None,
                scope: None,
                subject_claims: None,
                roles_claim: Some("groups".to_string()),
                display_name_template: None,
                require_connected_on_startup: false,
                required_claims: HashMap::new(),
            },
        );

        let providers = oidc_provider_configs_from_config(&config);

        assert_eq!(providers.len(), 2);
        assert_eq!(providers[0].0, OIDC_IDP_ID);
        // Primary's `require_connected_on_startup` is hardcoded `true` in
        // `oidc_provider_configs_from_config` and must stay that way even when
        // optional extras are also configured. Pinning the invariant.
        assert!(providers[0].1.require_connected_on_startup);
        assert_eq!(providers[1].0, "okta");
        assert_eq!(providers[1].1.roles_claim, Some("groups".to_string()));
        assert!(!providers[1].1.require_connected_on_startup);
    }

    /// Multiple extras are returned in deterministic alphabetical order of
    /// `idp_id`. This is operator-visible (chain order ⇒ which provider gets
    /// tried first for an ambiguous token) and `HashMap`'s iteration order is
    /// non-deterministic, so the explicit sort must hold.
    #[test]
    fn oidc_provider_configs_from_config_sorts_extras_alphabetically() {
        let mut config = DynAppConfig::default();
        // Insert in a non-alphabetical order so a naive "iteration order" sort
        // would still produce the wrong result.
        for name in ["zapier", "entra", "okta"] {
            config.openid_providers.insert(
                name.to_string(),
                OidcProviderConfig {
                    uri: url::Url::parse(&format!("https://{name}.example.com")).unwrap(),
                    audience: None,
                    additional_issuers: None,
                    scope: None,
                    subject_claims: None,
                    roles_claim: None,
                    display_name_template: None,
                    require_connected_on_startup: true,
                    required_claims: HashMap::new(),
                },
            );
        }

        let providers = oidc_provider_configs_from_config(&config);
        let ids: Vec<&str> = providers.iter().map(|(id, _)| id.as_str()).collect();

        // No primary URI → just the extras, alphabetically.
        assert_eq!(ids, vec!["entra", "okta", "zapier"]);
    }

    fn provider_with_template(template: Option<&str>) -> (String, OidcProviderConfig) {
        (
            "acme".to_string(),
            OidcProviderConfig {
                uri: url::Url::parse("https://issuer.example.com").unwrap(),
                audience: None,
                additional_issuers: None,
                scope: None,
                subject_claims: None,
                roles_claim: None,
                display_name_template: template.map(str::to_string),
                require_connected_on_startup: true,
                required_claims: HashMap::new(),
            },
        )
    }

    #[tokio::test]
    async fn build_oidc_authenticators_rejects_malformed_display_name_template() {
        // A malformed template is a static config error: it must fail before any
        // network work and regardless of `require_connected_on_startup`. Parsing
        // happens in a pre-pass before the build loop, so this errors without touching the
        // network — keeping the test hermetic.
        let providers = vec![provider_with_template(Some("Service Account {email"))];
        let err = build_oidc_authenticators(providers).await.unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("display_name_template"), "message: {msg}");
        assert!(msg.contains("acme"), "message names the provider: {msg}");
    }

    #[tokio::test]
    async fn get_default_authenticator_from_config_chain_order_primary_additional_k8s() {
        let (good_base, _bad_base, server) = spawn_oidc_test_server().await;
        let mut config = DynAppConfig::default();
        config.openid_provider_uri = Some(good_base.clone());
        config.openid_providers.insert(
            "okta".to_string(),
            OidcProviderConfig {
                uri: good_base.clone(),
                audience: None,
                additional_issuers: None,
                scope: None,
                subject_claims: None,
                roles_claim: None,
                display_name_template: None,
                require_connected_on_startup: true,
                required_claims: HashMap::new(),
            },
        );

        let k8s_stub = limes::jwks::JWKSWebAuthenticator::new(
            good_base.as_str(),
            Some(Duration::from_hours(1)),
        )
        .await
        .expect("k8s stub authenticator")
        .set_idp_id(K8S_IDP_ID);

        let authenticator =
            assemble_authenticator_chain(&config, Some(AuthenticatorEnum::from(k8s_stub)), None)
                .await
                .expect("build authenticators")
                .expect("authn enabled");

        let idp_ids = match authenticator {
            BuiltInAuthenticators::Single(auth) => auth
                .idp_ids()
                .into_iter()
                .map(|id| id.map(str::to_string))
                .collect::<Vec<_>>(),
            BuiltInAuthenticators::Chain(chain) => chain
                .idp_ids()
                .into_iter()
                .map(|id| id.map(str::to_string))
                .collect::<Vec<_>>(),
        };
        assert_eq!(
            idp_ids,
            vec![
                Some(OIDC_IDP_ID.to_string()),
                Some("okta".to_string()),
                Some(K8S_IDP_ID.to_string()),
            ]
        );

        server.abort();
    }

    #[tokio::test]
    async fn get_default_authenticator_from_config_skips_optional_provider() {
        let (good_base, bad_base, server) = spawn_oidc_test_server().await;
        let mut config = DynAppConfig::default();
        config.openid_provider_uri = Some(good_base);
        config.openid_providers.insert(
            "bad".to_string(),
            OidcProviderConfig {
                uri: bad_base,
                audience: None,
                additional_issuers: None,
                scope: None,
                subject_claims: None,
                roles_claim: None,
                display_name_template: None,
                require_connected_on_startup: false,
                required_claims: HashMap::new(),
            },
        );

        let authenticator = assemble_authenticator_chain(&config, None, None)
            .await
            .expect("build authenticators")
            .expect("authn enabled");

        let idp_ids = match authenticator {
            BuiltInAuthenticators::Single(auth) => auth
                .idp_ids()
                .into_iter()
                .map(|id| id.map(str::to_string))
                .collect::<Vec<_>>(),
            BuiltInAuthenticators::Chain(chain) => chain
                .idp_ids()
                .into_iter()
                .map(|id| id.map(str::to_string))
                .collect::<Vec<_>>(),
        };
        assert_eq!(idp_ids, vec![Some(OIDC_IDP_ID.to_string())]);

        server.abort();
    }

    #[tokio::test]
    async fn get_default_authenticator_from_config_fails_required_provider() {
        let (good_base, bad_base, server) = spawn_oidc_test_server().await;
        let mut config = DynAppConfig::default();
        config.openid_provider_uri = Some(good_base);
        config.openid_providers.insert(
            "bad".to_string(),
            OidcProviderConfig {
                uri: bad_base,
                audience: None,
                additional_issuers: None,
                scope: None,
                subject_claims: None,
                roles_claim: None,
                display_name_template: None,
                require_connected_on_startup: true,
                required_claims: HashMap::new(),
            },
        );

        let result = assemble_authenticator_chain(&config, None, None).await;
        assert!(result.is_err());

        server.abort();
    }

    /// `require_connected_on_startup=false` must not let the whole auth system
    /// silently disable itself: when every configured provider is optional and
    /// all of them fail, refuse to boot.
    #[tokio::test]
    async fn get_default_authenticator_refuses_when_all_optional_providers_fail() {
        let (_good_base, bad_base, server) = spawn_oidc_test_server().await;
        let mut config = DynAppConfig::default();
        // No primary URI, no K8s — only an optional provider that will fail discovery.
        config.openid_providers.insert(
            "bad".to_string(),
            OidcProviderConfig {
                uri: bad_base,
                audience: None,
                additional_issuers: None,
                scope: None,
                subject_claims: None,
                roles_claim: None,
                display_name_template: None,
                require_connected_on_startup: false,
                required_claims: HashMap::new(),
            },
        );

        let result = assemble_authenticator_chain(&config, None, None).await;
        let err = result.expect_err(
            "must refuse to boot when every configured provider failed, even if all optional",
        );
        let chain = format!("{err:#}");
        assert!(
            chain.contains("Refusing to start with authentication disabled"),
            "error must explain the refusal, got: {chain}",
        );

        server.abort();
    }

    /// EKS-only shape: no primary `OPENID_PROVIDER_URI`, one extra OIDC provider
    /// for Kubernetes workloads, and `enable_kubernetes_authentication` for
    /// in-cluster service accounts. The chain must contain exactly the extra
    /// provider followed by the K8s authenticator — no `OIDC_IDP_ID` link.
    #[tokio::test]
    async fn get_default_authenticator_from_config_k8s_and_provider_no_primary() {
        let (good_base, _bad_base, server) = spawn_oidc_test_server().await;
        let mut config = DynAppConfig::default();
        // Intentionally no `openid_provider_uri` — only an extra provider + K8s.
        config.openid_providers.insert(
            "ekscluster".to_string(),
            OidcProviderConfig {
                uri: good_base.clone(),
                audience: None,
                additional_issuers: None,
                scope: None,
                subject_claims: None,
                roles_claim: None,
                display_name_template: None,
                require_connected_on_startup: true,
                required_claims: HashMap::new(),
            },
        );

        let k8s_stub = limes::jwks::JWKSWebAuthenticator::new(
            good_base.as_str(),
            Some(Duration::from_hours(1)),
        )
        .await
        .expect("k8s stub authenticator")
        .set_idp_id(K8S_IDP_ID);

        let authenticator =
            assemble_authenticator_chain(&config, Some(AuthenticatorEnum::from(k8s_stub)), None)
                .await
                .expect("build authenticators")
                .expect("authn enabled");

        let idp_ids = match authenticator {
            BuiltInAuthenticators::Single(auth) => auth
                .idp_ids()
                .into_iter()
                .map(|id| id.map(str::to_string))
                .collect::<Vec<_>>(),
            BuiltInAuthenticators::Chain(chain) => chain
                .idp_ids()
                .into_iter()
                .map(|id| id.map(str::to_string))
                .collect::<Vec<_>>(),
        };
        assert_eq!(
            idp_ids,
            vec![Some("ekscluster".to_string()), Some(K8S_IDP_ID.to_string()),],
            "chain must be extra-provider then K8s, with no primary `oidc` link",
        );

        server.abort();
    }

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
        headers.insert("X-Assume-Role", this_role_id.to_string().parse().unwrap());
        let role_id = extract_role_id(&headers).unwrap().unwrap();
        assert_eq!(role_id, RoleId::new(this_role_id));

        let mut headers = HeaderMap::new();
        headers.insert(
            ASSUME_ROLE_BY_ID_HEADER,
            this_role_id.to_string().parse().unwrap(),
        );
        let role_id = extract_role_id(&headers).unwrap().unwrap();
        assert_eq!(role_id, RoleId::new(this_role_id));
    }
}
