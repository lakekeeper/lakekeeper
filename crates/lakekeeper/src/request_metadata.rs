#[cfg(feature = "router")]
use std::str::FromStr;
use std::sync::Arc;

#[cfg(feature = "router")]
use axum::{
    extract::MatchedPath,
    middleware::Next,
    response::{IntoResponse, Response},
};
use http::{HeaderMap, HeaderName, Method, StatusCode};
use iceberg::TableIdent;
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
use limes::Authentication;
use uuid::Uuid;

use crate::{
    CONFIG, DEFAULT_PROJECT_ID, ProjectId, WarehouseId, XXHashSet,
    api::iceberg::v1::namespace::NamespaceIdentUrl,
    config::MatchedEngines,
    service::{
        ArcProjectId, RoleIdent, TabularId,
        authn::{Actor, InternalActor},
        authz::{InstanceAdminAuthorizer, UserOrRole},
        events::{AuthorizationFailureReason, AuthorizationFailureSource},
        idempotency::IdempotencyKey,
    },
};

#[cfg(feature = "router")]
const PROJECT_ID_HEADER_DEPRECATED: &str = "x-project-ident";
pub const X_PROJECT_ID_HEADER: &str = "x-project-id";
pub const X_REQUEST_ID_HEADER: &str = "x-request-id";

pub const X_FORWARDED_HOST_HEADER: &str = "x-forwarded-host";
pub const X_FORWARDED_PROTO_HEADER: &str = "x-forwarded-proto";
pub const X_FORWARDED_PORT_HEADER: &str = "x-forwarded-port";
pub const X_FORWARDED_PREFIX_HEADER: &str = "x-forwarded-prefix";

pub const X_PROJECT_ID_HEADER_NAME: HeaderName = HeaderName::from_static(X_PROJECT_ID_HEADER);
pub const X_REQUEST_ID_HEADER_NAME: HeaderName = HeaderName::from_static(X_REQUEST_ID_HEADER);

const ANONYMOUS_ACTOR: &Actor = &Actor::Anonymous;

/// The `User-Agent` request header, recorded as the caller sent it.
///
/// Deliberately not parsed into a client taxonomy. The value is surfaced in the
/// audit log, where a normalised form would be a reconstruction rather than
/// evidence, and where classifying it would mean maintaining a parser against
/// strings the caller chooses. Consumers classify; Lakekeeper records.
#[derive(Debug, Clone)]
pub struct UserAgent(String);

impl UserAgent {
    /// Longest header value retained. The header is caller-controlled and is
    /// recorded on every audit event, so it is bounded at capture.
    #[cfg(any(feature = "router", test))]
    const MAX_LEN: usize = 256;

    #[cfg(any(feature = "router", test))]
    pub(crate) fn parse(user_agent: &str) -> Self {
        let mut end = user_agent.len().min(Self::MAX_LEN);
        // Header values are visible ASCII in practice, but never split a code
        // point — a partial character would not survive JSON encoding.
        while !user_agent.is_char_boundary(end) {
            end -= 1;
        }
        Self(user_agent[..end].to_string())
    }

    /// The header value, truncated at capture to a bounded length.
    ///
    /// Caller-supplied and unverified — see the audit-log documentation.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The version from a `PyIceberg/<version>` user agent, if this is one.
    #[must_use]
    pub fn pyiceberg_version(&self) -> Option<&str> {
        self.0.strip_prefix("PyIceberg/")
    }
}

/// Source of an authorization decision, surfaced in audit events as
/// `privilege_source`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum_macros::VariantArray)]
pub enum PrivilegeSource {
    /// In-process caller via [`RequestMetadata::new_lakekeeper_internal`].
    /// Full bypass including data-plane actions.
    Internal,
    /// Principal listed in `LAKEKEEPER__INSTANCE_ADMINS`. Control-plane bypass
    /// only; data-plane actions still route through the configured authorizer.
    InstanceAdmin,
    /// Decision came from the configured authorizer (OpenFGA, Cedar, `AllowAll`, ...).
    Authorizer,
}

impl PrivilegeSource {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Internal => "internal",
            Self::InstanceAdmin => "instance_admin",
            Self::Authorizer => "authorizer",
        }
    }
}

/// A struct to hold metadata about a request.
#[derive(Debug, Clone)]
pub struct RequestMetadata {
    request_id: Uuid,
    project_id: Option<ArcProjectId>,
    authentication: Option<Authentication>,
    token_roles: Option<TokenRoles>,
    /// Roles resolved by a post-authentication admission gate (see
    /// [`AdmissionGate`](crate::service::admission::AdmissionGate)) — e.g. from
    /// an external entitlement service. Kept separate from `token_roles` so the
    /// provenance (token claim vs externally resolved) stays explicit.
    admission_roles: Option<TokenRoles>,
    base_url: String,
    actor: InternalActor,
    matched_path: Option<Arc<str>>,
    request_method: Method,
    user_agent: Option<UserAgent>,
    engines: MatchedEngines,
    idempotency_key: Option<IdempotencyKey>,
    is_instance_admin: bool,
}

#[derive(Debug, Clone)]
pub struct TokenRoles {
    project_id: ArcProjectId,
    roles: XXHashSet<Arc<RoleIdent>>,
}

impl TokenRoles {
    #[must_use]
    pub fn new(project_id: ArcProjectId, roles: XXHashSet<Arc<RoleIdent>>) -> Self {
        Self { project_id, roles }
    }
}

impl TokenRoles {
    #[must_use]
    pub fn project_id(&self) -> &ArcProjectId {
        &self.project_id
    }

    #[must_use]
    pub fn roles(&self) -> &XXHashSet<Arc<RoleIdent>> {
        &self.roles
    }

    /// Union `other`'s roles into this set, consuming it (no cloning). Keeps
    /// `self`'s project id; callers resolve roles for the request's single
    /// project, so the ids normally match, and if they differ the first wins.
    pub(crate) fn merge(&mut self, other: TokenRoles) {
        self.roles.extend(other.roles);
    }
}

#[derive(Debug, Clone, thiserror::Error)]
#[error(
    "This endpoint requires a project ID to be specified, but none was provided. Please set the x-project-id header."
)]
pub struct ProjectIdMissing;
impl AuthorizationFailureSource for ProjectIdMissing {
    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        AuthorizationFailureReason::InvalidRequestData
    }

    fn into_error_model(self) -> ErrorModel {
        self.into()
    }
}
impl From<ProjectIdMissing> for iceberg_ext::catalog::rest::ErrorModel {
    fn from(e: ProjectIdMissing) -> Self {
        ErrorModel::builder()
            .message(e.to_string())
            .r#type("ProjectIdMissing")
            .code(StatusCode::BAD_REQUEST.as_u16())
            .build()
    }
}

impl From<ProjectIdMissing> for iceberg_ext::catalog::rest::IcebergErrorResponse {
    fn from(e: ProjectIdMissing) -> Self {
        IcebergErrorResponse::from(ErrorModel::from(e))
    }
}

impl RequestMetadata {
    /// Set authentication information for the request.
    pub fn set_authentication(
        &mut self,
        actor: Actor,
        authentication: Authentication,
    ) -> &mut Self {
        self.actor = actor.into();
        self.authentication = Some(authentication);
        self
    }

    /// Mark the request as originating from an instance admin (principal listed in
    /// `LAKEKEEPER__INSTANCE_ADMINS`). Set by the authn middleware after the actor
    /// has been resolved; never flipped after request entry.
    #[cfg_attr(not(feature = "router"), allow(dead_code))]
    pub(crate) fn set_instance_admin(&mut self, is_instance_admin: bool) -> &mut Self {
        self.is_instance_admin = is_instance_admin;
        self
    }

    /// Whether the authenticated principal is an instance admin. Instance admins are
    /// configured via `LAKEKEEPER__INSTANCE_ADMINS` and bypass authorization for all
    /// control-plane actions (but not for `CatalogTableAction::ReadData` /
    /// `WriteData`). Only ever `true` for `Actor::Principal`; role-assumed requests
    /// do not inherit this.
    #[must_use]
    pub fn is_instance_admin(&self) -> bool {
        self.is_instance_admin
    }

    /// Set the matched trusted engines for this request.
    pub fn set_engines(&mut self, engines: MatchedEngines) -> &mut Self {
        self.engines = engines;
        self
    }

    /// Trusted engines matched for this request.
    #[must_use]
    pub fn engines(&self) -> &MatchedEngines {
        &self.engines
    }

    /// Idempotency key from the `Idempotency-Key` request header, if present.
    #[must_use]
    pub fn idempotency_key(&self) -> Option<&IdempotencyKey> {
        self.idempotency_key.as_ref()
    }

    pub fn set_token_roles(&mut self, token_roles: TokenRoles) -> &mut Self {
        self.token_roles = Some(token_roles);
        self
    }

    /// Set the roles resolved by a post-authentication admission gate. Written
    /// by the auth middleware after the gates run; kept separate from
    /// [`set_token_roles`](Self::set_token_roles) to preserve provenance.
    #[cfg_attr(not(feature = "router"), allow(dead_code))]
    pub(crate) fn set_admission_roles(&mut self, admission_roles: TokenRoles) -> &mut Self {
        self.admission_roles = Some(admission_roles);
        self
    }

    /// Roles resolved by a post-authentication admission gate, if any.
    #[must_use]
    pub fn admission_roles(&self) -> Option<&TokenRoles> {
        self.admission_roles.as_ref()
    }

    #[must_use]
    pub fn user_agent(&self) -> Option<&UserAgent> {
        self.user_agent.as_ref()
    }

    /// ID of the user performing the request.
    /// This returns the underlying user-id, even if a role is assumed.
    /// Please use `actor()` to get the full actor for `AuthZ` decisions.
    #[must_use]
    pub fn user_id(&self) -> Option<&crate::service::UserId> {
        match &self.actor {
            InternalActor::External(Actor::Principal(user_id)) => Some(user_id),
            InternalActor::External(Actor::Role { principal, .. }) => Some(principal),
            InternalActor::External(Actor::Anonymous) | InternalActor::LakekeeperInternal => None,
        }
    }

    #[must_use]
    pub(crate) fn matched_path(&self) -> Option<&str> {
        self.matched_path.as_deref()
    }

    pub(crate) fn request_method(&self) -> &Method {
        &self.request_method
    }

    #[must_use]
    pub fn token_roles(&self) -> Option<&TokenRoles> {
        self.token_roles.as_ref()
    }

    #[must_use]
    pub fn new_lakekeeper_internal(request_id: Uuid) -> Self {
        Self {
            request_id,
            project_id: None,
            authentication: None,
            base_url: "http://localhost:8181".to_string(),
            actor: InternalActor::LakekeeperInternal,
            matched_path: None,
            request_method: Method::default(),
            user_agent: None,
            engines: MatchedEngines::default(),
            token_roles: None,
            admission_roles: None,
            idempotency_key: None,
            is_instance_admin: false,
        }
    }

    // If this grants admin-level privileges:
    #[must_use]
    #[inline]
    pub fn is_lakekeeper_internal(&self) -> bool {
        matches!(self.actor, InternalActor::LakekeeperInternal)
    }

    /// Source of the authz decision for this request, for audit logging.
    #[must_use]
    pub fn privilege_source(&self) -> PrivilegeSource {
        if self.is_lakekeeper_internal() {
            PrivilegeSource::Internal
        } else if self.is_instance_admin {
            PrivilegeSource::InstanceAdmin
        } else {
            PrivilegeSource::Authorizer
        }
    }

    /// Whether this request should bypass control-plane authorization checks
    /// against the given target. Returns `true` when:
    ///
    /// * the caller holds bypass privileges — in-process
    ///   (`LakekeeperInternal`) or a configured instance admin, **and**
    /// * `for_user` is `None`, i.e. the request is not being made *on behalf
    ///   of* a different principal.
    ///
    /// Callers that query "what can user X do?" pass `Some(&X)` so that the
    /// bypass does not incorrectly auto-approve delegated checks. Callers
    /// should first normalize `for_user` to `None` when it resolves to the
    /// acting principal itself (see existing pattern at the `_vec` call
    /// sites).
    ///
    /// Data-plane actions (`CatalogTableAction::ReadData` / `WriteData`) are
    /// NOT covered by this for instance admins — those checks must route
    /// through the configured authorizer even when this returns `true`. Use
    /// `is_lakekeeper_internal()` for the in-process variant (which
    /// additionally bypasses data-plane) and `is_instance_admin()` to detect
    /// the instance-admin case specifically.
    #[must_use]
    #[inline]
    pub fn bypasses_control_plane_authz(&self, for_user: Option<&UserOrRole>) -> bool {
        for_user.is_none() && InstanceAdminAuthorizer::has_bypass(self)
    }

    #[cfg(any(test, feature = "test-utils"))]
    #[must_use]
    pub fn new_unauthenticated() -> Self {
        Self {
            request_id: Uuid::now_v7(),
            project_id: None,
            authentication: None,
            base_url: "http://localhost:8181".to_string(),
            actor: Actor::Anonymous.into(),
            matched_path: None,
            request_method: Method::default(),
            user_agent: None,
            engines: MatchedEngines::default(),
            token_roles: None,
            admission_roles: None,
            idempotency_key: None,
            is_instance_admin: false,
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn with_project_id(&mut self, project_id: ProjectId) -> &mut Self {
        self.project_id = Some(Arc::new(project_id));
        self
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn with_idempotency_key(
        &mut self,
        key: crate::service::idempotency::IdempotencyKey,
    ) -> &mut Self {
        self.idempotency_key = Some(key);
        self
    }

    /// The project the request itself names, before any default is applied.
    ///
    /// [`Self::preferred_project_id`] answers "which project should this request read", folding
    /// in the configured default. This answers the narrower question "did the caller say which
    /// project" — which is what an authorizer needs to tell a caller who named the wrong
    /// project apart from one who named none and got the default.
    ///
    /// The two cannot be told apart from `preferred_project_id` alone: a request naming the
    /// default project and a request naming nothing return the same value.
    #[must_use]
    pub fn requested_project_id(&self) -> Option<&ArcProjectId> {
        self.project_id.as_ref()
    }

    #[must_use]
    pub fn preferred_project_id(&self) -> Option<ArcProjectId> {
        self.project_id.clone().or(DEFAULT_PROJECT_ID.clone())
    }

    /// Build an [`Authentication`] for a user with the given optional `name`
    /// claim and otherwise-empty claims. Test-only — used by the named test
    /// helpers below and reachable from tests that need a custom shape.
    #[cfg(any(test, feature = "test-utils"))]
    #[must_use]
    pub fn test_authentication(
        user_id: crate::service::UserId,
        name: Option<String>,
    ) -> Authentication {
        Authentication::builder()
            .token_header(None)
            .claims(serde_json::json!({}))
            .subject(user_id.into())
            .name(name)
            .email(None)
            .principal_type(None)
            .build()
    }

    #[cfg(any(test, feature = "test-utils"))]
    #[must_use]
    pub fn test_user(user_id: crate::service::UserId) -> Self {
        RequestMetadataTestBuilder::builder()
            .actor(Actor::Principal(user_id.clone()))
            .authentication(Self::test_authentication(
                user_id,
                Some("Test User".to_string()),
            ))
            .build()
    }

    /// Like [`Self::test_user`] but the token carries no `name` claim — exercises
    /// the nameless-token path (e.g. the role-provider stub backfill gate, which
    /// must NOT downgrade a row from a token that provides no name).
    #[cfg(any(test, feature = "test-utils"))]
    #[must_use]
    pub fn test_user_without_name(user_id: crate::service::UserId) -> Self {
        RequestMetadataTestBuilder::builder()
            .actor(Actor::Principal(user_id.clone()))
            .authentication(Self::test_authentication(user_id, None))
            .build()
    }

    #[cfg(any(test, feature = "test-utils"))]
    #[must_use]
    pub fn test_instance_admin(user_id: crate::service::UserId) -> Self {
        RequestMetadataTestBuilder::builder()
            .actor(Actor::Principal(user_id.clone()))
            .authentication(Self::test_authentication(
                user_id,
                Some("Test User".to_string()),
            ))
            .is_instance_admin(true)
            .build()
    }

    #[cfg(any(test, feature = "test-utils"))]
    #[must_use]
    pub fn test_user_assumed_role(
        user_id: crate::service::UserId,
        role_id: crate::service::RoleId,
    ) -> Self {
        use crate::service::Role;
        RequestMetadataTestBuilder::builder()
            .actor(Actor::Role {
                principal: user_id.clone(),
                assumed_role: Arc::new(Role::new_random_with_id(role_id)),
            })
            .authentication(Self::test_authentication(
                user_id,
                Some("Test User".to_string()),
            ))
            .build()
    }

    #[must_use]
    pub fn actor(&self) -> &Actor {
        match &self.actor {
            InternalActor::External(actor) => actor,
            InternalActor::LakekeeperInternal => ANONYMOUS_ACTOR,
        }
    }

    #[must_use]
    pub(crate) fn internal_actor(&self) -> &InternalActor {
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
        user_project: Option<ProjectId>,
    ) -> Result<ArcProjectId, ProjectIdMissing> {
        user_project
            .map(Arc::new)
            .or(self.preferred_project_id())
            .ok_or(ProjectIdMissing)
    }

    /// Get the host that the request was made to.
    ///
    /// Contains the value of `CONFIG.base_uri` if configered, else the
    /// (`x-forward-proto`|https)://`x-forwarded-host`:`x-forwarded-port` headers if present,
    /// otherwise the `host` header.
    #[must_use]
    pub fn base_url(&self) -> &str {
        self.base_url.as_str().trim_end_matches('/')
    }

    #[must_use]
    pub fn s3_signer_uri(&self, _warehouse_id: WarehouseId) -> String {
        format!("{}/", self.base_uri_catalog())
    }

    #[must_use]
    pub fn s3_signer_endpoint_for_table(
        &self,
        warehouse_id: WarehouseId,
        table_id: TabularId,
    ) -> String {
        format!("v1/signer/{warehouse_id}/tabular-id/{table_id}/v1/aws/s3/sign")
    }

    #[must_use]
    pub fn refresh_client_credentials_endpoint_for_table(
        &self,
        warehouse_id: WarehouseId,
        table_ident: &TableIdent,
    ) -> String {
        format!(
            "{}/v1/{warehouse_id}/namespaces/{}/tables/{}/credentials",
            self.base_uri_catalog(),
            NamespaceIdentUrl::from(table_ident.namespace().clone()).to_url_string(),
            percent_encoding::utf8_percent_encode(
                &table_ident.name,
                percent_encoding::NON_ALPHANUMERIC
            ),
        )
    }

    #[must_use]
    pub fn base_uri_catalog(&self) -> String {
        format!("{}/catalog", self.base_url())
    }

    #[must_use]
    pub fn base_uri_management(&self) -> String {
        format!("{}/management", self.base_url())
    }
}

/// Test-only builder for [`RequestMetadata`]. Anonymous actor, no project, GET
/// method, not an instance admin by default — override only what your test
/// needs. Prefer the named helpers on [`RequestMetadata`] for common shapes
/// ([`RequestMetadata::test_user`], [`RequestMetadata::test_instance_admin`],
/// [`RequestMetadata::test_user_assumed_role`]); reach for this builder when
/// no named helper fits.
#[cfg(any(test, feature = "test-utils"))]
#[derive(Debug, typed_builder::TypedBuilder)]
#[builder(build_method(into = RequestMetadata))]
pub struct RequestMetadataTestBuilder {
    #[builder(default = Actor::Anonymous)]
    pub actor: Actor,
    #[builder(default, setter(strip_option))]
    pub authentication: Option<Authentication>,
    #[builder(default = "http://localhost:8181".to_string(), setter(into))]
    pub base_url: String,
    #[builder(default, setter(into))]
    pub project_id: Option<ArcProjectId>,
    #[builder(default, setter(into))]
    pub matched_path: Option<Arc<str>>,
    #[builder(default = Method::default())]
    pub request_method: Method,
    #[builder(default = false)]
    pub is_instance_admin: bool,
    #[builder(default, setter(strip_option))]
    pub token_roles: Option<TokenRoles>,
    /// Roles a post-authentication admission gate resolved for the caller. In
    /// production only the auth middleware sets these (via the `pub(crate)`
    /// [`RequestMetadata::set_admission_roles`]); this builder field lets tests
    /// construct a request that carries them.
    #[builder(default, setter(strip_option))]
    pub admission_roles: Option<TokenRoles>,
    /// The `User-Agent` header the caller sent, as captured by the request
    /// middleware. Lets tests exercise the audit log's `user_agent` field.
    #[builder(default, setter(strip_option))]
    pub user_agent: Option<UserAgent>,
}

#[cfg(any(test, feature = "test-utils"))]
impl From<RequestMetadataTestBuilder> for RequestMetadata {
    fn from(b: RequestMetadataTestBuilder) -> Self {
        Self {
            request_id: Uuid::now_v7(),
            authentication: b.authentication,
            base_url: b.base_url,
            actor: b.actor.into(),
            project_id: b.project_id,
            matched_path: b.matched_path,
            request_method: b.request_method,
            user_agent: b.user_agent,
            engines: MatchedEngines::default(),
            token_roles: b.token_roles,
            admission_roles: b.admission_roles,
            idempotency_key: None,
            is_instance_admin: b.is_instance_admin,
        }
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
        .get(X_REQUEST_ID_HEADER)
        .and_then(|hv| hv.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(Uuid::from_str)
        .transpose()
        .ok()
        .flatten()
        .unwrap_or(Uuid::now_v7());

    let Some(base_uri) = determine_base_uri(&headers) else {
        return iceberg_ext::catalog::rest::IcebergErrorResponse::from(ErrorModel::bad_request(
            "base_uri is not set and neither x-forwarded-host nor host header are set. Either send the appropriate headers or configure the base_uri according to the documentation.".to_string(),
            "NoHostHeader",
            None,
        ))
        .into_response();
    };

    let project_id = headers
        .get(X_PROJECT_ID_HEADER)
        .or(headers.get(PROJECT_ID_HEADER_DEPRECATED))
        .and_then(|hv| hv.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ProjectId::from_str)
        .transpose()
        .map_err(|e| e.append_detail(format!("Invalid {X_PROJECT_ID_HEADER} header value.")));
    let project_id = match project_id {
        Ok(ident) => ident,
        Err(err) => {
            return err.into_response();
        }
    };

    let matched_path = request
        .extensions()
        .get::<MatchedPath>()
        .cloned()
        .map(|mp| Arc::from(mp.as_str()));
    let request_method = request.method().clone();

    let user_agent = headers
        .get(http::header::USER_AGENT)
        .and_then(|hv| hv.to_str().ok())
        .map(UserAgent::parse);

    let idempotency_key = if CONFIG.idempotency.enabled {
        match IdempotencyKey::from_headers(&headers) {
            Ok(key) => key,
            Err(err) => return err.into_response(),
        }
    } else {
        None
    };

    request.extensions_mut().insert(RequestMetadata {
        request_id,
        authentication: None,
        token_roles: None,
        admission_roles: None,
        base_url: base_uri,
        actor: Actor::Anonymous.into(),
        project_id: project_id.map(Arc::new),
        matched_path,
        request_method,
        user_agent,
        engines: MatchedEngines::default(),
        idempotency_key,
        is_instance_admin: false,
    });
    next.run(request).await
}

#[must_use]
/// Determines the forwarded prefix from the request headers, if configured to use x-forwarded headers.
/// Returns `None` if the prefix is not set or if the configuration does not use x-forwarded headers.
/// Skips leading and trailing slashes from the prefix.
pub fn determine_forwarded_prefix(headers: &HeaderMap) -> Option<&str> {
    if CONFIG.use_x_forwarded_headers {
        headers
            .get(X_FORWARDED_PREFIX_HEADER)
            .and_then(|hv| hv.to_str().ok())
            .map(|s| s.trim_matches('/'))
            .filter(|s| !s.is_empty())
    } else {
        None
    }
}

pub fn determine_base_uri(headers: &HeaderMap) -> Option<String> {
    if let Some(uri) = CONFIG.base_uri.as_ref() {
        return Some(uri.to_string());
    }

    let host_header = headers
        .get(http::header::HOST)
        .and_then(|hv| hv.to_str().ok());

    if CONFIG.use_x_forwarded_headers {
        let any_x_forwarded_header_present = headers
            .get(X_FORWARDED_HOST_HEADER)
            .or(headers.get(X_FORWARDED_PROTO_HEADER))
            .or(headers.get(X_FORWARDED_PORT_HEADER))
            .is_some();

        let host = headers
            .get(X_FORWARDED_HOST_HEADER)
            .and_then(|hv| hv.to_str().ok())
            .or(host_header)?;

        let x_forwarded_proto = headers
            .get(X_FORWARDED_PROTO_HEADER)
            .and_then(|hv| hv.to_str().ok());
        let x_forwarded_port = headers
            .get(X_FORWARDED_PORT_HEADER)
            .and_then(|hv| hv.to_str().ok());
        let x_forwarded_prefix = headers
            .get(X_FORWARDED_PREFIX_HEADER)
            .and_then(|hv| hv.to_str().ok())
            .map(|s| s.trim_matches('/'));

        let mut base_uri = String::new();
        let proto = x_forwarded_proto.unwrap_or({
            if any_x_forwarded_header_present {
                // In the unlikely case that x-forwarded headers are present, but the proto header
                // is missing, we assume https.
                "https"
            } else {
                "http"
            }
        });
        base_uri.push_str(proto);
        base_uri.push_str("://");
        base_uri.push_str(host);

        // Skip port if it's in the forwarded host header or it's the default port for the protocol
        if let Some(port) = x_forwarded_port
            && !(host.contains(':')
                || proto == "https" && port == "443"
                || proto == "http" && port == "80")
        {
            base_uri.push(':');
            base_uri.push_str(port);
        }

        // Append the x-forwarded prefix if present
        if let Some(prefix) = x_forwarded_prefix {
            base_uri.push('/');
            base_uri.push_str(prefix);
        }

        Some(base_uri)
    } else {
        // If no BASE_URI is set and no x-forwarded headers are present, the encryption is unencrypted,
        // as lakekeeper does not terminate TLS. Any external entity that terminates TLS should set the x-forwarded headers.
        host_header.map(|host| format!("http://{host}"))
    }
}

#[cfg(test)]
#[allow(clippy::result_large_err)]
mod test {
    use http::{HeaderMap, header::HeaderValue};

    use super::*;

    /// The audit log records the user agent as the caller sent it, so the
    /// header must survive capture byte-for-byte. A parsed representation that
    /// only kept the version would make the recorded value a reconstruction
    /// rather than evidence.
    #[test]
    fn a_user_agent_is_captured_verbatim() {
        for raw in [
            "PyIceberg/0.9.1",
            "Trino/476",
            "Apache-Spark/3.5.1 (Scala/2.12)",
            "",
        ] {
            assert_eq!(UserAgent::parse(raw).as_str(), raw);
        }
    }

    /// The two project accessors answer different questions, and the difference is the
    /// whole reason the narrower one exists: a request that names no project still has a
    /// preferred one when a default is configured, and a consumer that cannot tell those
    /// apart cannot report "you named the wrong project" without also rejecting callers who
    /// named nothing at all.
    #[test]
    fn a_requested_project_is_distinct_from_a_preferred_one() {
        let named = ProjectId::from(uuid::Uuid::from_u128(1));
        let mut with_header = RequestMetadata::new_unauthenticated();
        with_header.with_project_id(named.clone());
        assert_eq!(
            with_header.requested_project_id().map(AsRef::as_ref),
            Some(&named)
        );
        assert_eq!(with_header.preferred_project_id().as_deref(), Some(&named));

        let without_header = RequestMetadata::new_unauthenticated();
        assert_eq!(
            without_header.requested_project_id(),
            None,
            "a request that names no project must say so, whatever the default is"
        );
        assert_eq!(
            without_header.preferred_project_id(),
            DEFAULT_PROJECT_ID.clone(),
            "while the preferred project still falls back to the configured default"
        );
    }

    /// The header is caller-controlled and lands on every audit record, so its
    /// length is bounded at capture rather than at emit.
    #[test]
    fn an_overlong_user_agent_is_truncated() {
        let raw = "x".repeat(UserAgent::MAX_LEN * 2);
        let captured = UserAgent::parse(&raw);
        assert_eq!(captured.as_str().len(), UserAgent::MAX_LEN);
        assert!(raw.starts_with(captured.as_str()));
    }

    /// Truncation must not split a multi-byte character — a partial code point
    /// would render as invalid JSON in the audit log.
    #[test]
    fn truncation_respects_char_boundaries() {
        // 'ä' is two bytes, so a 256-byte cut lands mid-character.
        let raw = "ä".repeat(UserAgent::MAX_LEN);
        let captured = UserAgent::parse(&raw);
        assert!(captured.as_str().len() <= UserAgent::MAX_LEN);
        assert!(raw.starts_with(captured.as_str()));
    }

    /// The ADLS SAS-property workaround needs the `PyIceberg` version; it is
    /// derived from the raw header rather than stored separately.
    #[test]
    fn a_pyiceberg_version_is_derived_from_the_raw_header() {
        assert_eq!(
            UserAgent::parse("PyIceberg/0.9.1").pyiceberg_version(),
            Some("0.9.1")
        );
        assert_eq!(UserAgent::parse("Trino/476").pyiceberg_version(), None);
        // Case-sensitive prefix: a different product is not PyIceberg.
        assert_eq!(
            UserAgent::parse("pyiceberg/0.9.1").pyiceberg_version(),
            None
        );
    }

    #[test]
    fn test_bypass_matrix() {
        use crate::service::{UserId, authz::UserOrRole};

        let alice = UserId::try_from("oidc~alice").unwrap();
        let bob = UserOrRole::User(UserId::try_from("oidc~bob").unwrap());

        // Anonymous: no bypass.
        let md = RequestMetadata::new_unauthenticated();
        assert!(!md.is_lakekeeper_internal());
        assert!(!md.is_instance_admin());
        assert!(!md.bypasses_control_plane_authz(None));
        assert!(!md.bypasses_control_plane_authz(Some(&bob)));

        // Lakekeeper-internal: full bypass when acting as self …
        let md = RequestMetadata::new_lakekeeper_internal(Uuid::now_v7());
        assert!(md.is_lakekeeper_internal());
        assert!(!md.is_instance_admin());
        assert!(md.bypasses_control_plane_authz(None));
        // … but NOT when querying on behalf of another principal.
        assert!(!md.bypasses_control_plane_authz(Some(&bob)));

        // Normal authenticated user: no bypass regardless of for_user.
        let md = RequestMetadata::test_user(alice.clone());
        assert!(!md.is_lakekeeper_internal());
        assert!(!md.is_instance_admin());
        assert!(!md.bypasses_control_plane_authz(None));
        assert!(!md.bypasses_control_plane_authz(Some(&bob)));

        // Instance admin: control-plane bypass when acting as self, no bypass
        // when querying on behalf of another principal. Data-plane still
        // routes through the configured authorizer — the caller is
        // responsible for excluding data-plane actions.
        let md = RequestMetadata::test_instance_admin(alice);
        assert!(!md.is_lakekeeper_internal());
        assert!(md.is_instance_admin());
        assert!(md.bypasses_control_plane_authz(None));
        assert!(!md.bypasses_control_plane_authz(Some(&bob)));
    }

    #[test]
    fn test_determine_host_without_host_header_with_config_provided_base_uri() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__BASE_URI", "https://localhost:8181/a/b/");
            let host = determine_base_uri(&HeaderMap::new());
            assert_eq!(host, Some("https://localhost:8181/a/b/".to_string()));
            Ok(())
        });
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__BASE_URI", "https://localhost:8181/a/b");
            let host = determine_base_uri(&HeaderMap::new());
            assert_eq!(host, Some("https://localhost:8181/a/b/".to_string()));
            Ok(())
        });
    }

    #[test]
    fn test_443_port_is_skipped_for_https() {
        let mut headers = HeaderMap::new();
        headers.insert(
            X_FORWARDED_HOST_HEADER,
            HeaderValue::from_static("example.com"),
        );
        headers.insert(X_FORWARDED_PROTO_HEADER, HeaderValue::from_static("https"));
        headers.insert(X_FORWARDED_PORT_HEADER, HeaderValue::from_static("443"));

        let result = determine_base_uri(&headers);
        assert_eq!(result, Some("https://example.com".to_string()));
    }

    #[test]
    fn test_80_port_is_skipped_for_http() {
        let mut headers = HeaderMap::new();
        headers.insert(
            X_FORWARDED_HOST_HEADER,
            HeaderValue::from_static("example.com"),
        );
        headers.insert(X_FORWARDED_PROTO_HEADER, HeaderValue::from_static("http"));
        headers.insert(X_FORWARDED_PORT_HEADER, HeaderValue::from_static("80"));

        let result = determine_base_uri(&headers);
        assert_eq!(result, Some("http://example.com".to_string()));
    }

    #[test]
    fn test_determine_host_with_host_header_with_config_provided_base_uri() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__BASE_URI", "https://localhost:8181/a/b/");
            let mut headers = HeaderMap::new();
            headers.insert(http::header::HOST, HeaderValue::from_static("example.com"));
            let host = determine_base_uri(&headers);
            assert_eq!(host, Some("https://localhost:8181/a/b/".to_string()));
            Ok(())
        });
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__BASE_URI", "https://localhost:8181/a/b");
            let mut headers = HeaderMap::new();
            headers.insert(http::header::HOST, HeaderValue::from_static("example.com"));
            let host = determine_base_uri(&headers);
            assert_eq!(host, Some("https://localhost:8181/a/b/".to_string()));
            Ok(())
        });
    }

    #[test]
    fn test_determine_host_with_x_forwarded_for_with_config_provided_base_uri() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__BASE_URI", "https://localhost:8181/a/b/");
            let mut headers = HeaderMap::new();
            headers.insert(
                X_FORWARDED_HOST_HEADER,
                HeaderValue::from_static("example.com"),
            );
            let host = determine_base_uri(&headers);
            assert_eq!(host, Some("https://localhost:8181/a/b/".to_string()));
            Ok(())
        });
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__BASE_URI", "https://localhost:8181/a/b");
            let mut headers = HeaderMap::new();
            headers.insert(
                X_FORWARDED_HOST_HEADER,
                HeaderValue::from_static("example.com"),
            );
            let host = determine_base_uri(&headers);
            assert_eq!(host, Some("https://localhost:8181/a/b/".to_string()));
            Ok(())
        });
    }

    #[test]
    fn test_determine_host_with_x_forwarded_https() {
        let mut headers = HeaderMap::new();
        headers.insert(
            X_FORWARDED_HOST_HEADER,
            HeaderValue::from_static("example.com"),
        );
        headers.insert(X_FORWARDED_PROTO_HEADER, HeaderValue::from_static("https"));
        headers.insert(X_FORWARDED_PORT_HEADER, HeaderValue::from_static("8080"));

        let result = determine_base_uri(&headers);
        assert_eq!(result, Some("https://example.com:8080".to_string()));
    }

    #[test]
    fn test_determine_host_with_x_forwarded_http() {
        let mut headers = HeaderMap::new();
        headers.insert(
            X_FORWARDED_HOST_HEADER,
            HeaderValue::from_static("example.com"),
        );
        headers.insert(X_FORWARDED_PROTO_HEADER, HeaderValue::from_static("http"));
        headers.insert(X_FORWARDED_PORT_HEADER, HeaderValue::from_static("8080"));

        let result = determine_base_uri(&headers);
        assert_eq!(result, Some("http://example.com:8080".to_string()));
    }

    #[test]
    fn test_determine_host_with_x_forwarded_no_port() {
        let mut headers = HeaderMap::new();
        headers.insert(
            X_FORWARDED_HOST_HEADER,
            HeaderValue::from_static("example.com"),
        );
        headers.insert(X_FORWARDED_PROTO_HEADER, HeaderValue::from_static("https"));

        let result = determine_base_uri(&headers);
        assert_eq!(result, Some("https://example.com".to_string()));
    }

    #[test]
    fn test_determine_host_with_x_forwarded_host_and_port_with_port_in_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            X_FORWARDED_HOST_HEADER,
            HeaderValue::from_static("example.com:8443"),
        );
        headers.insert(X_FORWARDED_PROTO_HEADER, HeaderValue::from_static("https"));
        headers.insert(X_FORWARDED_PORT_HEADER, HeaderValue::from_static("8443"));

        let result = determine_base_uri(&headers);
        assert_eq!(result, Some("https://example.com:8443".to_string()));
    }

    #[test]
    fn test_determine_host_with_x_forwarded_no_proto() {
        let mut headers = HeaderMap::new();
        headers.insert(
            X_FORWARDED_HOST_HEADER,
            HeaderValue::from_static("example.com"),
        );
        headers.insert("x-forwarded-port", HeaderValue::from_static("8080"));

        let result = determine_base_uri(&headers);
        assert_eq!(result, Some("https://example.com:8080".to_string()));
    }

    #[test]
    fn test_determine_host_with_only_x_forwarded_for() {
        let mut headers = HeaderMap::new();
        headers.insert(
            X_FORWARDED_HOST_HEADER,
            HeaderValue::from_static("example.com"),
        );

        let result = determine_base_uri(&headers);
        assert_eq!(result, Some("https://example.com".to_string()));
    }

    #[test]
    fn test_determine_host_with_host_header() {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::HOST, HeaderValue::from_static("example.com"));

        let result = determine_base_uri(&headers);
        assert_eq!(result, Some("http://example.com".to_string()));
    }

    #[test]
    fn test_determine_host_empty_headers() {
        let headers = HeaderMap::new();
        let result = determine_base_uri(&headers);
        assert_eq!(result, None);
    }

    #[test]
    fn test_determine_host_invalid_header_values() {
        let mut headers = HeaderMap::new();
        // Insert an invalid UTF-8 sequence as header value
        headers.insert(
            X_FORWARDED_HOST_HEADER,
            HeaderValue::from_bytes(&[0xFF]).unwrap(),
        );

        let result = determine_base_uri(&headers);
        assert_eq!(result, None);
    }

    #[test]
    fn test_determine_host_prefers_x_forwarded() {
        let mut headers = HeaderMap::new();
        headers.insert(
            X_FORWARDED_HOST_HEADER,
            HeaderValue::from_static("forwarded.example.com"),
        );
        headers.insert(X_FORWARDED_PROTO_HEADER, HeaderValue::from_static("https"));
        headers.insert(
            http::header::HOST,
            HeaderValue::from_static("host.example.com"),
        );

        let result = determine_base_uri(&headers);
        assert_eq!(result, Some("https://forwarded.example.com".to_string()));
    }

    #[test]
    fn test_determine_host_with_port_in_host_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::HOST,
            HeaderValue::from_static("example.com:8080"),
        );

        let result = determine_base_uri(&headers);
        assert_eq!(result, Some("http://example.com:8080".to_string()));
    }

    #[test]
    fn test_determine_host_with_prefix_clean() {
        let mut headers = HeaderMap::new();
        headers.insert(
            X_FORWARDED_HOST_HEADER,
            HeaderValue::from_static("example.com"),
        );
        headers.insert(X_FORWARDED_PROTO_HEADER, HeaderValue::from_static("https"));
        headers.insert(
            X_FORWARDED_PREFIX_HEADER,
            HeaderValue::from_static("/lakekeeper"),
        );

        let result = determine_base_uri(&headers);
        assert_eq!(result, Some("https://example.com/lakekeeper".to_string()));
    }

    #[test]
    fn test_determine_host_with_prefix_no_prefix_slash() {
        let mut headers = HeaderMap::new();
        headers.insert(
            X_FORWARDED_HOST_HEADER,
            HeaderValue::from_static("example.com"),
        );
        headers.insert(X_FORWARDED_PROTO_HEADER, HeaderValue::from_static("https"));
        headers.insert(
            X_FORWARDED_PREFIX_HEADER,
            HeaderValue::from_static("lakekeeper"),
        );

        let result = determine_base_uri(&headers);
        assert_eq!(result, Some("https://example.com/lakekeeper".to_string()));
    }

    #[test]
    fn test_determine_host_with_prefix_trailing_slash() {
        let mut headers = HeaderMap::new();
        headers.insert(
            X_FORWARDED_HOST_HEADER,
            HeaderValue::from_static("example.com"),
        );
        headers.insert(X_FORWARDED_PROTO_HEADER, HeaderValue::from_static("https"));
        headers.insert(
            X_FORWARDED_PREFIX_HEADER,
            HeaderValue::from_static("api/lakekeeper/"),
        );

        let result = determine_base_uri(&headers);
        assert_eq!(
            result,
            Some("https://example.com/api/lakekeeper".to_string())
        );
    }

    #[test]
    fn test_determine_forwarded_prefix() {
        // Case 1: No prefix header
        let headers = HeaderMap::new();
        assert_eq!(determine_forwarded_prefix(&headers), None);

        // Case 2: Normal prefix
        let mut headers = HeaderMap::new();
        headers.insert(
            X_FORWARDED_PREFIX_HEADER,
            HeaderValue::from_static("lakekeeper"),
        );
        assert_eq!(determine_forwarded_prefix(&headers), Some("lakekeeper"));

        // Case 3: Prefix with leading slash
        let mut headers = HeaderMap::new();
        headers.insert(
            X_FORWARDED_PREFIX_HEADER,
            HeaderValue::from_static("/lakekeeper"),
        );
        assert_eq!(determine_forwarded_prefix(&headers), Some("lakekeeper"));

        // Case 4: Prefix with trailing slash
        let mut headers = HeaderMap::new();
        headers.insert(
            X_FORWARDED_PREFIX_HEADER,
            HeaderValue::from_static("lakekeeper/"),
        );
        assert_eq!(determine_forwarded_prefix(&headers), Some("lakekeeper"));

        // Case 5: Prefix with both leading and trailing slashes
        let mut headers = HeaderMap::new();
        headers.insert(
            X_FORWARDED_PREFIX_HEADER,
            HeaderValue::from_static("/lakekeeper/"),
        );
        assert_eq!(determine_forwarded_prefix(&headers), Some("lakekeeper"));

        // Case 6: Multi-segment prefix
        let mut headers = HeaderMap::new();
        headers.insert(
            X_FORWARDED_PREFIX_HEADER,
            HeaderValue::from_static("/api/lakekeeper"),
        );
        assert_eq!(determine_forwarded_prefix(&headers), Some("api/lakekeeper"));

        // Case 7: Empty prefix should return None
        let mut headers = HeaderMap::new();
        headers.insert(X_FORWARDED_PREFIX_HEADER, HeaderValue::from_static(""));
        assert_eq!(determine_forwarded_prefix(&headers), None);
    }
}
