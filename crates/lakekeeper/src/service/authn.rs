use std::fmt::Debug;
#[cfg(feature = "router")]
use std::sync::Arc;

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
    service::{RoleIdent, authz::InstanceAdminMembership, events::EventDispatcher},
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
    /// Claims to use as the subject (user ID), in order of preference.
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
    /// If true, fail startup when this provider's OIDC/JWKS configuration cannot be loaded.
    #[serde(default = "default_true")]
    pub require_connected_on_startup: bool,
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
                require_connected_on_startup: true,
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
    let mut authenticators = Vec::new();

    for (idp_id, provider) in providers {
        tracing::info!(
            "Creating OIDC authenticator for {} ({})",
            idp_id,
            provider.uri
        );

        match build_oidc_authenticator(&idp_id, &provider).await {
            Ok(authenticator) => {
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

    Ok(authenticators)
}

async fn build_oidc_authenticator(
    idp_id: &str,
    provider: &OidcProviderConfig,
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
        authenticator = authenticator.set_scope(scope.clone());
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

    Ok(authenticator)
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

    let authentication = match authenticator.authenticate(authorization.token()).await {
        Ok(principal) => principal,
        Err(e) => {
            return ErrorModel::unauthorized(
                "Authentication failed",
                "AuthenticationFailed",
                Some(Box::new(e)),
            )
            .into_response();
        }
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
        let jwks = json!({ "keys": [] });

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

    #[test]
    fn oidc_provider_configs_from_config_uses_legacy_provider_id_and_roles_claim() {
        let mut config = DynAppConfig::default();
        config.openid_provider_uri = Some(url::Url::parse("https://issuer.example.com").unwrap());
        config.openid_audience = Some(vec!["lakekeeper".to_string()]);
        config.openid_additional_issuers = Some(vec!["https://sts.example.com".to_string()]);
        config.openid_scope = Some("openid".to_string());
        config.openid_subject_claim = Some(vec!["sub".to_string()]);
        config.openid_roles_claim = Some("roles".to_string());

        let providers = oidc_provider_configs_from_config(&config);

        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].0, OIDC_IDP_ID);
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
                require_connected_on_startup: false,
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
                    require_connected_on_startup: true,
                },
            );
        }

        let providers = oidc_provider_configs_from_config(&config);
        let ids: Vec<&str> = providers.iter().map(|(id, _)| id.as_str()).collect();

        // No primary URI → just the extras, alphabetically.
        assert_eq!(ids, vec!["entra", "okta", "zapier"]);
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
                require_connected_on_startup: true,
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
                require_connected_on_startup: false,
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
                require_connected_on_startup: true,
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
                require_connected_on_startup: false,
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
                require_connected_on_startup: true,
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
