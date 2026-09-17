//! Post-authentication admission gates.
//!
//! An [`AdmissionGate`] is a coarse, pluggable check run once per request
//! immediately after authentication and actor resolution — instance-admin
//! membership and assumed-role are already resolved — and before the request
//! reaches any handler. It can reject a *validated* principal that must not be
//! admitted to this instance at all, for example by consulting an external
//! control-plane permission service.
//!
//! This is deliberately a distinct layer from:
//! - **authentication** (is the token valid — answered by the
//!   [`Authenticator`](limes::Authenticator)), and
//! - **authorization** (may this actor perform action X on resource Y —
//!   answered per-endpoint by the [`Authorizer`](crate::service::authz::Authorizer)).
//!
//! Keeping it separate means a gate can return the right HTTP semantics (a
//! denial is not an authentication failure, and "permission service
//! unreachable" is not a `401`), runs *after* instance-admin status is
//! resolved, and sees the full [`RequestMetadata`].
//!
//! Gates are composed as a list ([`AdmissionGates`]) and evaluated in
//! registration order; the first rejection wins and short-circuits the rest.
//! The default — no gates configured — admits every request, so existing
//! deployments are unaffected.

use std::{
    borrow::Cow,
    sync::{Arc, LazyLock},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use axum_prometheus::metrics;
#[cfg(feature = "router")]
use iceberg_ext::catalog::rest::ErrorModel;
use uuid::Uuid;

use crate::{
    request_metadata::{RequestMetadata, TokenRoles},
    service::events::backends::audit::{AuditOperation, AuditOutcome},
};

/// Histogram of each gate's evaluation time, labelled by `gate` and `outcome`.
/// Its `_count` series is also the authoritative rejection rate: admission
/// denials are indistinguishable from authorization denials in
/// `axum_http_requests_total{status="403"}`, but separable here.
const METRIC_ADMISSION_GATE_DURATION_SECONDS: &str = "lakekeeper_admission_gate_duration_seconds";

/// Registers metric descriptions exactly once; forced before every emission in
/// [`record_gate_duration`].
static METRICS_INITIALIZED: LazyLock<()> = LazyLock::new(|| {
    metrics::describe_histogram!(
        METRIC_ADMISSION_GATE_DURATION_SECONDS,
        "Duration of a single admission-gate evaluation in seconds. Labelled by \
         `gate` and `outcome` (admitted/skipped/forbidden/unavailable), where \
         `skipped` means the gate does not govern the request and decided \
         nothing. Time spent inside the gate's own cache is included, so this \
         is the latency the request actually paid, not the upstream call time."
    );
});

/// Why an [`AdmissionGate`] rejected a request.
///
/// The [`kind`](AdmissionRejection::kind) — not an inferred status code —
/// determines the HTTP response, so a gate states its intent explicitly rather
/// than leaving the middleware to interpret one. Everything else a rejection
/// carries lives here rather than on the kind, so a new kind neither repeats it
/// nor silently omits it.
///
/// Construct with [`AdmissionRejection::forbidden`] or
/// [`AdmissionRejection::unavailable`]. The response body the caller receives is
/// rendered from this at the boundary; a gate never builds one.
#[derive(Debug)]
#[non_exhaustive]
pub struct AdmissionRejection {
    kind: RejectionKind,
    error_type: &'static str,
    message: Cow<'static, str>,
    denied_by: Option<Cow<'static, str>>,
    /// What went wrong underneath, for the fail-closed warning. Never reaches
    /// the caller, so it may name hosts, statuses and client errors.
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    error_id: Uuid,
}

/// What kind of rejection this is, and with it the response the caller gets.
///
/// Non-exhaustive: further kinds may be added without a breaking change, so
/// external matches must include a wildcard arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RejectionKind {
    /// The principal is authenticated but not entitled to this instance. This
    /// is an authoritative decision and is **terminal**: returned as
    /// `403 Forbidden` with no `Retry-After`.
    Forbidden,
    /// The gate could not reach an upstream it depends on and is **failing
    /// closed**. Returned as `503 Service Unavailable` with a `Retry-After`
    /// header set to `retry_after`, so clients back off and retry instead of
    /// treating the rejection as terminal. The gate owns the duration (it
    /// reflects that gate's upstream recovery characteristics, not a global
    /// default).
    Unavailable { retry_after: Duration },
}

impl RejectionKind {
    /// The HTTP status this kind is returned as.
    #[must_use]
    pub fn status(self) -> u16 {
        match self {
            Self::Forbidden => http::StatusCode::FORBIDDEN.as_u16(),
            Self::Unavailable { .. } => http::StatusCode::SERVICE_UNAVAILABLE.as_u16(),
        }
    }

    /// Label for the metric and the audit record, shared by both so the two
    /// cannot drift into different vocabularies for the same outcome.
    /// `unavailable` is the fail-closed outcome, kept distinct from `forbidden`
    /// so an outage of an upstream a gate depends on shows up as an outage
    /// rather than as a wave of denials.
    ///
    /// Returns the enum rather than the string so that the value reaches the
    /// wire-value manifest: a rename then fails `check-audit-format` instead of
    /// silently breaking every consumer matching on it.
    fn label(self) -> AuditOutcome {
        match self {
            Self::Forbidden => AuditOutcome::Forbidden,
            Self::Unavailable { .. } => AuditOutcome::Unavailable,
        }
    }
}

impl AdmissionRejection {
    /// Authoritative `403 Forbidden` denial (terminal).
    ///
    /// `error_type` is the `type` of the error body the caller receives, in the
    /// server's `<Subject><Condition>` convention (e.g.
    /// `"ExternalEnforceForbidden"`). It is a wire contract clients branch on:
    /// keep a gate's set small and fixed, and never rename one. A denial has no
    /// cause beyond the rule that produced it — name that with
    /// [`denied_by`](Self::denied_by).
    #[must_use]
    pub fn forbidden(message: impl Into<Cow<'static, str>>, error_type: &'static str) -> Self {
        Self {
            kind: RejectionKind::Forbidden,
            error_type,
            message: message.into(),
            denied_by: None,
            source: None,
            error_id: Uuid::now_v7(),
        }
    }

    /// Fail-closed `503 Service Unavailable` with a gate-chosen `Retry-After`.
    ///
    /// `error_type` is the caller-visible type, as for
    /// [`forbidden`](Self::forbidden). `source` is what actually failed: it is
    /// logged with the fail-closed warning and never reaches the caller, so
    /// `message` can stay generic while the warning still says why.
    #[must_use]
    pub fn unavailable(
        message: impl Into<Cow<'static, str>>,
        error_type: &'static str,
        retry_after: Duration,
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ) -> Self {
        Self {
            kind: RejectionKind::Unavailable { retry_after },
            error_type,
            message: message.into(),
            denied_by: None,
            source,
            error_id: Uuid::now_v7(),
        }
    }

    /// Name the gate's own rule that produced this rejection, so the log record
    /// identifies the decision rather than leaving it inside prose. A gate with
    /// a single rule leaves it unset.
    #[must_use]
    pub fn denied_by(mut self, rule: impl Into<Cow<'static, str>>) -> Self {
        self.denied_by = Some(rule.into());
        self
    }

    /// Pin the `error_id`, which is otherwise a fresh uuid per rejection.
    ///
    /// For tests that compare a whole emitted audit record against a committed
    /// one: the id reaches the record, so a random one per run would make that
    /// comparison impossible. Never used in production, where the whole point
    /// of the id is being unique to one rejection.
    #[cfg(any(test, feature = "test-utils"))]
    #[must_use]
    pub fn with_error_id(mut self, error_id: Uuid) -> Self {
        self.error_id = error_id;
        self
    }

    /// The gate's own rule that decided this, when it named one.
    #[must_use]
    pub fn deciding_rule(&self) -> Option<&str> {
        self.denied_by.as_deref()
    }

    /// What kind of rejection this is, and with it the response the caller gets.
    #[must_use]
    pub fn kind(&self) -> RejectionKind {
        self.kind
    }

    /// The `type` of the error body the caller receives.
    #[must_use]
    pub fn error_type(&self) -> &'static str {
        self.error_type
    }

    /// The caller-facing wording.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// The id the caller is handed, and the one the audit record carries, so a
    /// user's report resolves to the decision.
    #[must_use]
    pub fn error_id(&self) -> Uuid {
        self.error_id
    }

    /// Render the response body for this rejection.
    ///
    /// `skip_log` is set because [`AdmissionGates::admit`] has already recorded
    /// the decision, naming the principal the generic error-response line cannot.
    #[cfg(feature = "router")]
    pub(crate) fn into_error(self) -> ErrorModel {
        ErrorModel::builder()
            .message(self.message.into_owned())
            .r#type(self.error_type)
            .code(self.kind.status())
            .error_id(self.error_id)
            .skip_log(true)
            .build()
    }
}

/// The `source` chain flattened for one log field, so a fail-closed warning
/// names what actually failed and not only its outermost wrapper.
fn cause_chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut rendered = error.to_string();
    let mut next = error.source();
    while let Some(cause) = next {
        rendered.push_str(": ");
        rendered.push_str(&cause.to_string());
        next = cause.source();
    }
    rendered
}

/// Enrichment a gate contributes when it admits a request: an opt-in payload
/// merged into the request's [`RequestMetadata`] for downstream authorization
/// and audit. A gate that only allows/denies contributes [`Admission::admit`].
///
/// Non-exhaustive so further enrichment can be added without a breaking change;
/// construct it with [`Admission::admit`] / [`Admission::with_roles`].
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct Admission {
    /// Roles the gate resolved for the principal in the same call (for example
    /// from an external entitlement service). Merged into
    /// [`RequestMetadata::admission_roles`] by the auth middleware, kept
    /// separate from token-claim roles so the provenance stays explicit.
    /// `None` when the gate resolves no roles.
    pub resolved_roles: Option<TokenRoles>,
}

impl Admission {
    /// Admit the request without contributing any enrichment.
    #[must_use]
    pub fn admit() -> Self {
        Self::default()
    }

    /// Admit the request and contribute the roles the gate resolved.
    #[must_use]
    pub fn with_roles(roles: TokenRoles) -> Self {
        Self {
            resolved_roles: Some(roles),
        }
    }
}

/// What a single [`AdmissionGate`] concluded about a request it did not reject.
///
/// A gate that governs only some principals — scoped to one identity provider,
/// tenant or path — returns [`GateDecision::NotApplicable`] for the rest. The
/// request proceeds either way; the distinction is that a gate which silently
/// stopped covering its principals would otherwise be indistinguishable from
/// one approving them all.
///
/// Non-exhaustive: further conclusions may be added without a breaking change.
#[derive(Debug)]
#[non_exhaustive]
pub enum GateDecision {
    /// The gate governs this request and admits it, contributing the
    /// [`Admission`]'s enrichment.
    Admitted(Admission),
    /// The gate does not govern this request and adjudicated nothing. Reported
    /// as `outcome="skipped"`, never as an admission.
    NotApplicable,
}

impl GateDecision {
    /// Admit the request without contributing any enrichment.
    #[must_use]
    pub fn admit() -> Self {
        Self::Admitted(Admission::admit())
    }

    /// Admit the request and contribute the roles the gate resolved.
    #[must_use]
    pub fn with_roles(roles: TokenRoles) -> Self {
        Self::Admitted(Admission::with_roles(roles))
    }

    /// The gate does not govern this request, so it decided nothing.
    #[must_use]
    pub fn not_applicable() -> Self {
        Self::NotApplicable
    }
}

impl From<Admission> for GateDecision {
    fn from(admission: Admission) -> Self {
        Self::Admitted(admission)
    }
}

/// Per-request inputs handed to an [`AdmissionGate`].
///
/// Carries borrowed request state for the duration of the [`AdmissionGate::admit`]
/// call only. Nothing here is persisted onto [`RequestMetadata`] or logged — in
/// particular the raw bearer token is exposed to gates that must relay it to an
/// external service, without it leaking into the request's metadata or audit
/// trail (which are cloned, debugged, and serialized).
///
/// Non-exhaustive so further per-request inputs can be added without a breaking
/// change. The auth middleware is the only constructor; gates read the fields
/// they need.
#[derive(Clone, Copy, veil::Redact)]
#[non_exhaustive]
pub struct AdmissionContext<'a> {
    /// Resolved metadata for the request (actor, project, instance-admin, …).
    pub metadata: &'a RequestMetadata,
    /// The caller's raw bearer token — the value after `Bearer `. Present for
    /// every authenticated request (anonymous requests are rejected before any
    /// gate runs). A gate that relays it to an external service MUST use TLS.
    #[redact]
    pub bearer_token: Option<&'a str>,
}

impl<'a> AdmissionContext<'a> {
    /// Construct a context for a request. Called by the auth middleware.
    #[must_use]
    pub fn new(metadata: &'a RequestMetadata, bearer_token: Option<&'a str>) -> Self {
        Self {
            metadata,
            bearer_token,
        }
    }
}

/// A single post-authentication admission check.
///
/// Implementations are expected to be cheap and to cache aggressively: `admit`
/// runs on the hot path of every authenticated request.
#[async_trait]
pub trait AdmissionGate: std::fmt::Debug + Send + Sync {
    /// Short, stable name used in logs and metrics.
    fn name(&self) -> &'static str;

    /// Decide whether the (already authenticated) request may proceed.
    ///
    /// [`AdmissionContext`] carries the resolved [`RequestMetadata`] and the
    /// caller's raw `bearer_token` (for gates that relay it to an external
    /// service). Return [`GateDecision::admit`] for a plain allow, or
    /// [`GateDecision::with_roles`] to also contribute roles resolved in the
    /// same call. A gate that governs only some principals must return
    /// [`GateDecision::not_applicable`] for the rest.
    ///
    /// Return `Err(..)` to reject the request before it reaches any handler.
    /// The implementation owns the fail-open vs fail-closed policy by choosing
    /// the constructor: [`AdmissionRejection::forbidden`] for an authoritative
    /// deny, or [`AdmissionRejection::unavailable`] to fail closed when an
    /// upstream the gate depends on is unreachable.
    async fn admit(&self, ctx: AdmissionContext<'_>) -> Result<GateDecision, AdmissionRejection>;
}

/// An ordered collection of [`AdmissionGate`]s.
///
/// Evaluated in registration order; the first rejection wins and short-circuits
/// the rest, so register cheap or most-likely-to-deny gates first. An empty
/// collection (the default) admits every request, so the gate is a no-op unless
/// a host binary registers at least one gate.
#[derive(Debug, Clone, Default)]
pub struct AdmissionGates {
    gates: Vec<Arc<dyn AdmissionGate>>,
}

impl AdmissionGates {
    #[must_use]
    pub fn new(gates: Vec<Arc<dyn AdmissionGate>>) -> Self {
        Self { gates }
    }

    /// `true` when no gates are configured. The auth middleware uses this to
    /// skip the admission step entirely on the hot path.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.gates.is_empty()
    }

    /// Run every gate in order, returning the first rejection. On success the
    /// returned [`Admission`] carries the union of every gate's resolved roles.
    /// Whether a given gate governed the request is its own result, reported per
    /// gate as `outcome="skipped"`, and is not a question the merged value
    /// answers.
    ///
    /// # Errors
    /// Returns the [`AdmissionRejection`] from the first gate that rejects the
    /// request.
    pub async fn admit(&self, ctx: AdmissionContext<'_>) -> Result<Admission, AdmissionRejection> {
        let mut resolved_roles: Option<TokenRoles> = None;
        for gate in &self.gates {
            let start = Instant::now();
            let result = gate.admit(ctx).await;
            record_gate_duration(gate.name(), &result, start.elapsed());
            match result {
                Ok(GateDecision::Admitted(admission)) => {
                    if let Some(roles) = admission.resolved_roles {
                        // Common case is a single role-resolving gate: just move
                        // the set in. Extra gates union in place (no cloning).
                        match resolved_roles.as_mut() {
                            Some(acc) => acc.merge(roles),
                            None => resolved_roles = Some(roles),
                        }
                    }
                }
                Ok(GateDecision::NotApplicable) => {}
                Err(rejection) => {
                    // The one record of the rejection. It names the principal,
                    // so it belongs in the audit stream: a denial nothing can
                    // attribute answers "someone was refused" and never "who".
                    // Rendering the response then suppresses the generic
                    // error-response line, which would otherwise repeat this
                    // without the actor — and, for a fail-closed `503`, repeat
                    // it at ERROR as an internal error this server did not have.
                    crate::audit_operation!(
                        operation = AuditOperation::AdmissionDecided.as_str(),
                        actor = ctx.metadata.audit_actor(),
                        outcome = rejection.kind.label().as_str(),
                        context = AdmissionRejectedContext {
                            gate: gate.name(),
                            denied_by: rejection.deciding_rule(),
                            status: rejection.kind.status(),
                            error_type: rejection.error_type,
                            message: rejection.message.as_ref(),
                            error_id: rejection.error_id.to_string(),
                            request_id: ctx.metadata.request_id().to_string(),
                        },
                        "Request rejected by admission gate"
                    );
                    // A gate failing closed is an outage of something this
                    // server depends on, not a decision about the caller, and
                    // it needs a level that survives `RUST_LOG=warn` — which
                    // the audit record above does not have. Carries no
                    // principal, so it stays on the general stream: the same
                    // pairing the role providers use, warning here and
                    // auditing there. `cause` is the only place the gate's
                    // `source` is rendered; it never reaches the caller.
                    if matches!(rejection.kind, RejectionKind::Unavailable { .. }) {
                        let cause = rejection.source.as_deref().map(|e| cause_chain(e));
                        tracing::warn!(
                            gate = gate.name(),
                            error_type = rejection.error_type,
                            error_id = %rejection.error_id,
                            request_id = %ctx.metadata.request_id(),
                            cause = cause.as_deref(),
                            "Admission gate failed closed; rejecting the request"
                        );
                    }
                    return Err(rejection);
                }
            }
        }
        Ok(Admission { resolved_roles })
    }
}

/// Label for how a gate resolved.
fn outcome_label(result: &Result<GateDecision, AdmissionRejection>) -> &'static str {
    match result {
        Ok(GateDecision::Admitted(_)) => "admitted",
        Ok(GateDecision::NotApplicable) => "skipped",
        Err(rejection) => rejection.kind.label().as_str(),
    }
}

/// Context for the admission-rejection audit record.
///
/// The operation-specific fields live here rather than at the top level,
/// because that is the shape every `event_source="audit"` operational record
/// promises. `error_id` correlates with what the caller was handed, and
/// `request_id` is repeated out of the span so the record stands alone.
struct AdmissionRejectedContext<'a> {
    gate: &'a str,
    denied_by: Option<&'a str>,
    status: u16,
    error_type: &'a str,
    /// The gate's own wording. Suppressing the error-response line takes this
    /// with it, and it is what separates two rejections that share a type —
    /// a gate failing closed on a missing precondition from the same gate
    /// failing closed on an unreachable upstream.
    message: &'a str,
    error_id: String,
    request_id: String,
}

// Hand-written rather than derived so that `denied_by` is *omitted* when the gate
// named no rule, instead of reaching the wire as `null`. The audit format omits a
// field rather than emitting it empty, and a consumer testing for the key's presence
// would read a `null` as "a rule decided this" — see `GrantContextValue`, which
// drops its optional ids the same way.
impl valuable::Valuable for AdmissionRejectedContext<'_> {
    fn as_value(&self) -> valuable::Value<'_> {
        valuable::Value::Mappable(self)
    }

    fn visit(&self, visit: &mut dyn valuable::Visit) {
        use valuable::Value;
        visit.visit_entry(Value::String("gate"), Value::String(self.gate));
        if let Some(rule) = self.denied_by {
            visit.visit_entry(Value::String("denied_by"), Value::String(rule));
        }
        visit.visit_entry(Value::String("status"), Value::U16(self.status));
        visit.visit_entry(Value::String("error_type"), Value::String(self.error_type));
        visit.visit_entry(Value::String("message"), Value::String(self.message));
        visit.visit_entry(Value::String("error_id"), Value::String(&self.error_id));
        visit.visit_entry(Value::String("request_id"), Value::String(&self.request_id));
    }
}

impl valuable::Mappable for AdmissionRejectedContext<'_> {
    fn size_hint(&self) -> (usize, Option<usize>) {
        // Exact, matching `visit` above. `serde_json` ignores the hint, but a
        // length-prefixed serializer would emit a corrupt frame on a wrong one.
        let len = 6 + usize::from(self.denied_by.is_some());
        (len, Some(len))
    }
}

fn record_gate_duration(
    gate: &'static str,
    result: &Result<GateDecision, AdmissionRejection>,
    elapsed: Duration,
) {
    let () = &*METRICS_INITIALIZED;
    metrics::histogram!(
        METRIC_ADMISSION_GATE_DURATION_SECONDS,
        "gate" => gate,
        "outcome" => outcome_label(result),
    )
    .record(elapsed.as_secs_f64());
}

#[cfg(test)]
mod tests {
    use http::StatusCode;

    use super::*;
    use crate::service::{ProjectId, RoleIdent};

    /// Build a project-scoped role set from role source-id names.
    fn token_roles(names: &[&str]) -> TokenRoles {
        let roles = names
            .iter()
            .map(|n| Arc::new(RoleIdent::new_unchecked("test", *n)))
            .collect();
        TokenRoles::new(Arc::new(ProjectId::new_random()), roles)
    }

    #[derive(Debug)]
    struct RolesGate(&'static [&'static str]);
    #[async_trait]
    impl AdmissionGate for RolesGate {
        fn name(&self) -> &'static str {
            "roles"
        }
        async fn admit(&self, _: AdmissionContext<'_>) -> Result<GateDecision, AdmissionRejection> {
            Ok(GateDecision::with_roles(token_roles(self.0)))
        }
    }

    #[derive(Debug)]
    struct AllowGate;
    #[async_trait]
    impl AdmissionGate for AllowGate {
        fn name(&self) -> &'static str {
            "allow"
        }
        async fn admit(&self, _: AdmissionContext<'_>) -> Result<GateDecision, AdmissionRejection> {
            Ok(GateDecision::admit())
        }
    }

    #[derive(Debug)]
    struct DenyGate;
    #[async_trait]
    impl AdmissionGate for DenyGate {
        fn name(&self) -> &'static str {
            "deny"
        }
        async fn admit(&self, _: AdmissionContext<'_>) -> Result<GateDecision, AdmissionRejection> {
            Err(AdmissionRejection::forbidden("nope", "TestDenied"))
        }
    }

    #[derive(Debug)]
    struct UnavailableGate;
    #[async_trait]
    impl AdmissionGate for UnavailableGate {
        fn name(&self) -> &'static str {
            "unavailable"
        }
        async fn admit(&self, _: AdmissionContext<'_>) -> Result<GateDecision, AdmissionRejection> {
            Err(AdmissionRejection::unavailable(
                "upstream down",
                "TestUnavailable",
                Duration::from_secs(7),
                None,
            ))
        }
    }

    /// A gate that must never be consulted; used to assert short-circuiting.
    #[derive(Debug)]
    struct PanicGate;
    #[async_trait]
    impl AdmissionGate for PanicGate {
        fn name(&self) -> &'static str {
            "panic"
        }
        async fn admit(&self, _: AdmissionContext<'_>) -> Result<GateDecision, AdmissionRejection> {
            panic!("gate after a rejection must not be evaluated");
        }
    }

    /// Admits only when the caller's bearer token is threaded through to the
    /// gate and matches the expected value; otherwise denies.
    #[derive(Debug)]
    struct ExpectTokenGate(&'static str);
    #[async_trait]
    impl AdmissionGate for ExpectTokenGate {
        fn name(&self) -> &'static str {
            "expect-token"
        }
        async fn admit(
            &self,
            ctx: AdmissionContext<'_>,
        ) -> Result<GateDecision, AdmissionRejection> {
            if ctx.bearer_token == Some(self.0) {
                Ok(GateDecision::admit())
            } else {
                Err(AdmissionRejection::forbidden(
                    "missing or wrong token",
                    "TestNoToken",
                ))
            }
        }
    }

    fn gates(gates: Vec<Arc<dyn AdmissionGate>>) -> AdmissionGates {
        AdmissionGates::new(gates)
    }

    #[tokio::test]
    async fn bearer_token_is_threaded_to_gates() {
        let md = RequestMetadata::new_unauthenticated();
        assert!(
            gates(vec![Arc::new(ExpectTokenGate("tok-123"))])
                .admit(AdmissionContext::new(&md, Some("tok-123")))
                .await
                .is_ok()
        );
        // A gate that needs the token rejects when it is absent.
        assert!(
            gates(vec![Arc::new(ExpectTokenGate("tok-123"))])
                .admit(AdmissionContext::new(&md, None))
                .await
                .is_err()
        );
    }

    /// A gate scoped to principals it does not cover: it adjudicates nothing.
    #[derive(Debug)]
    struct NotApplicableGate;
    #[async_trait]
    impl AdmissionGate for NotApplicableGate {
        fn name(&self) -> &'static str {
            "not-applicable"
        }
        async fn admit(&self, _: AdmissionContext<'_>) -> Result<GateDecision, AdmissionRejection> {
            Ok(GateDecision::not_applicable())
        }
    }

    #[derive(Debug)]
    struct NamedRuleDenyGate;
    #[async_trait]
    impl AdmissionGate for NamedRuleDenyGate {
        fn name(&self) -> &'static str {
            "named-rule"
        }
        async fn admit(&self, _: AdmissionContext<'_>) -> Result<GateDecision, AdmissionRejection> {
            Err(AdmissionRejection::forbidden("nope", "TestDenied").denied_by("instance_access"))
        }
    }

    /// A gate that does not govern the request must not report the requests it
    /// never examined as ones it approved: the metric label is what tells an
    /// operator a gate has stopped covering its principals.
    #[tokio::test]
    async fn a_gate_that_does_not_apply_is_not_an_allow() {
        let md = RequestMetadata::new_unauthenticated();
        let ctx = AdmissionContext::new(&md, None);
        // The histogram labels each gate's own result, which is what the driver
        // passes to `record_gate_duration` — not its merged return value.
        for (gate, expected) in [
            (Arc::new(AllowGate) as Arc<dyn AdmissionGate>, "admitted"),
            (Arc::new(NotApplicableGate), "skipped"),
            (Arc::new(RolesGate(&["a"])), "admitted"),
            (Arc::new(DenyGate), "forbidden"),
            (Arc::new(UnavailableGate), "unavailable"),
        ] {
            assert_eq!(
                outcome_label(&gate.admit(ctx).await),
                expected,
                "{}",
                gate.name()
            );
        }

        // Skipping still admits, and contributes nothing.
        let skipped = gates(vec![Arc::new(NotApplicableGate)])
            .admit(ctx)
            .await
            .expect("a gate that does not apply admits");
        assert!(skipped.resolved_roles.is_none());
    }

    /// The driver emits the one record of a rejection, so the generic
    /// error-response line must not repeat it — for a fail-closed `503` that
    /// duplicate is logged at ERROR as an internal error this server did not
    /// have.
    /// Renders through the JSON formatter the binaries configure — the only one
    /// that expands a `valuable` field into real structure. Asserting against
    /// any other formatter checks a rendering no consumer ever sees.
    fn capture_json(emit: impl FnOnce()) -> Vec<serde_json::Value> {
        #[derive(Clone, Default)]
        struct Buf(Arc<std::sync::Mutex<Vec<u8>>>);
        impl std::io::Write for Buf {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                self.0.lock().expect("poisoned").extend_from_slice(b);
                Ok(b.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Buf {
            type Writer = Self;
            fn make_writer(&'a self) -> Self::Writer {
                self.clone()
            }
        }

        let buf = Buf::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .flatten_event(true)
            .with_writer(buf.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, emit);

        let bytes = buf.0.lock().expect("poisoned").clone();
        String::from_utf8(bytes)
            .expect("log output must be utf-8")
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| serde_json::from_str(l).expect("log line must be valid json"))
            .collect()
    }

    /// The record a rejection leaves behind is the only one there is, so its
    /// shape is a contract: `docs/docs/logging.md` tells consumers to read the
    /// operation-specific fields under `context`.
    #[tokio::test]
    async fn the_rejection_record_is_a_complete_audit_event() {
        let md = RequestMetadata::new_unauthenticated();
        for (gate, outcome, status, error_type, rule) in [
            (
                Arc::new(NamedRuleDenyGate) as Arc<dyn AdmissionGate>,
                "forbidden",
                403,
                "TestDenied",
                Some("instance_access"),
            ),
            (
                Arc::new(UnavailableGate),
                "unavailable",
                503,
                "TestUnavailable",
                None,
            ),
        ] {
            let name = gate.name();
            let lines = capture_json(|| {
                futures::executor::block_on(
                    gates(vec![gate]).admit(AdmissionContext::new(&md, None)),
                )
                .expect_err("gate rejects");
            });

            let record = lines
                .iter()
                .find(|l| l["operation"] == "admission_decided")
                .unwrap_or_else(|| panic!("no admission_decided record in {lines:#?}"));
            assert_eq!(record["event_source"], "audit");
            assert_eq!(record["outcome"], outcome);
            assert_eq!(record["actor"]["actor_type"], "anonymous");
            assert_eq!(record["context"]["gate"], name);
            assert_eq!(record["context"]["status"], status);
            assert_eq!(record["context"]["error_type"], error_type);
            assert_eq!(
                record["context"]["denied_by"],
                rule.map_or(serde_json::Value::Null, Into::into),
            );
            // Present and non-empty: they are how a reported error id and a
            // request trace resolve back to this decision.
            assert!(
                record["context"]["error_id"]
                    .as_str()
                    .is_some_and(|s| !s.is_empty())
            );
            assert!(
                record["context"]["request_id"]
                    .as_str()
                    .is_some_and(|s| !s.is_empty())
            );
            assert!(
                record["context"]["message"]
                    .as_str()
                    .is_some_and(|s| !s.is_empty())
            );
        }
    }

    /// Admission runs after assume-role resolution, so the actor on the record
    /// is the one the request is acting as. A gate raising its own record for
    /// the same request reaches the identical shape through
    /// `RequestMetadata::audit_actor`; rendering only the principal would leave
    /// the two disagreeing about who was refused.
    #[tokio::test]
    async fn the_record_names_the_role_the_caller_assumed() {
        let user_id = crate::service::UserId::new_unchecked("oidc", "u-1");
        let role_id = crate::service::RoleId::new_random();
        let md = RequestMetadata::test_user_assumed_role(user_id.clone(), role_id);

        let lines = capture_json(|| {
            futures::executor::block_on(
                gates(vec![Arc::new(DenyGate)]).admit(AdmissionContext::new(&md, None)),
            )
            .expect_err("gate rejects");
        });
        let record = lines
            .iter()
            .find(|l| l["operation"] == "admission_decided")
            .unwrap_or_else(|| panic!("no admission_decided record in {lines:#?}"));
        assert_eq!(record["actor"]["actor_type"], "assumed-role");
        assert_eq!(record["actor"]["principal"], user_id.to_string());
        assert_eq!(
            record["actor"]["assumed_role"]["role_id"],
            role_id.to_string()
        );
    }

    /// A fail-closed gate is an outage of a dependency, and must stay visible to
    /// an operator filtering at `RUST_LOG=warn` — which the audit record, being
    /// `INFO`, is not.
    #[tokio::test]
    async fn failing_closed_also_warns_on_the_general_stream() {
        let md = RequestMetadata::new_unauthenticated();

        let lines = capture_json(|| {
            futures::executor::block_on(
                gates(vec![Arc::new(UnavailableGate)]).admit(AdmissionContext::new(&md, None)),
            )
            .expect_err("gate rejects");
        });
        let warn = lines
            .iter()
            .find(|l| l["level"] == "WARN")
            .unwrap_or_else(|| panic!("no WARN for a fail-closed gate in {lines:#?}"));
        assert_eq!(warn["gate"], "unavailable");
        assert_eq!(warn["error_type"], "TestUnavailable");
        // The general stream never names the principal.
        assert!(warn.get("actor").is_none(), "{warn:#?}");

        // An authoritative denial is a decision about the caller, not an
        // outage, and must not warn.
        let lines = capture_json(|| {
            futures::executor::block_on(
                gates(vec![Arc::new(DenyGate)]).admit(AdmissionContext::new(&md, None)),
            )
            .expect_err("gate rejects");
        });
        assert!(
            !lines.iter().any(|l| l["level"] == "WARN"),
            "a 403 must not warn: {lines:#?}"
        );
    }

    /// A gate fails closed with a generic message and the real cause attached.
    /// The cause is what an operator needs and what the caller must not see, so
    /// it belongs on the warning and nowhere else.
    #[derive(Debug)]
    struct Timeout(std::io::Error);
    impl std::fmt::Display for Timeout {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("connecting to https://enforce.internal")
        }
    }
    impl std::error::Error for Timeout {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.0)
        }
    }

    #[derive(Debug)]
    struct CausedGate;
    #[async_trait]
    impl AdmissionGate for CausedGate {
        fn name(&self) -> &'static str {
            "caused"
        }
        async fn admit(&self, _: AdmissionContext<'_>) -> Result<GateDecision, AdmissionRejection> {
            Err(AdmissionRejection::unavailable(
                "admission could not be verified upstream",
                "TestUnavailable",
                Duration::from_secs(5),
                Some(Box::new(Timeout(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "timed out",
                )))),
            ))
        }
    }

    #[tokio::test]
    async fn the_cause_of_a_fail_closed_reaches_the_warning_only() {
        let md = RequestMetadata::new_unauthenticated();
        let lines = capture_json(|| {
            futures::executor::block_on(
                gates(vec![Arc::new(CausedGate)]).admit(AdmissionContext::new(&md, None)),
            )
            .expect_err("gate rejects");
        });

        let warn = lines
            .iter()
            .find(|l| l["level"] == "WARN")
            .unwrap_or_else(|| panic!("no WARN in {lines:#?}"));
        // Flattened, so the innermost failure is in the record and not only the
        // wrapper that happens to be outermost.
        assert_eq!(
            warn["cause"],
            "connecting to https://enforce.internal: timed out"
        );

        // The audit record keeps the gate's own generic wording, and the caller
        // is handed that same message — neither learns the endpoint.
        let record = lines
            .iter()
            .find(|l| l["operation"] == "admission_decided")
            .unwrap_or_else(|| panic!("no admission_decided record in {lines:#?}"));
        assert_eq!(
            record["context"]["message"],
            "admission could not be verified upstream"
        );
        assert!(record["context"].get("cause").is_none(), "{record:#?}");

        #[cfg(feature = "router")]
        {
            let rendered = futures::executor::block_on(
                gates(vec![Arc::new(CausedGate)]).admit(AdmissionContext::new(&md, None)),
            )
            .expect_err("gate rejects")
            .into_error();
            assert_eq!(rendered.message, "admission could not be verified upstream");
            assert!(rendered.source.is_none(), "{rendered:?}");
        }
    }

    #[cfg(feature = "router")]
    #[tokio::test]
    async fn a_rejection_suppresses_the_duplicate_error_log() {
        let md = RequestMetadata::new_unauthenticated();
        for gate in [
            Arc::new(DenyGate) as Arc<dyn AdmissionGate>,
            Arc::new(UnavailableGate) as Arc<dyn AdmissionGate>,
        ] {
            let rejection = gates(vec![gate])
                .admit(AdmissionContext::new(&md, None))
                .await
                .expect_err("gate rejects");
            let error_type = rejection.error_type();
            assert!(
                rejection.into_error().skip_log,
                "{error_type} must not be logged twice"
            );
        }
    }

    /// The deciding rule reaches the record as a field, rather than surviving
    /// only inside the caller-facing message.
    #[tokio::test]
    async fn a_rejection_carries_the_rule_that_decided_it() {
        let md = RequestMetadata::new_unauthenticated();
        let named = gates(vec![Arc::new(NamedRuleDenyGate)])
            .admit(AdmissionContext::new(&md, None))
            .await
            .expect_err("gate rejects");
        assert_eq!(named.deciding_rule(), Some("instance_access"));

        // A gate with one rule names none, and that is not an error.
        let unnamed = gates(vec![Arc::new(DenyGate)])
            .admit(AdmissionContext::new(&md, None))
            .await
            .expect_err("gate rejects");
        assert_eq!(unnamed.deciding_rule(), None);
    }

    #[tokio::test]
    async fn empty_admits() {
        let md = RequestMetadata::new_unauthenticated();
        assert!(AdmissionGates::default().is_empty());
        assert!(
            AdmissionGates::default()
                .admit(AdmissionContext::new(&md, None))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn single_allow_admits() {
        let md = RequestMetadata::new_unauthenticated();
        assert!(
            gates(vec![Arc::new(AllowGate)])
                .admit(AdmissionContext::new(&md, None))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn forbidden_is_403() {
        let md = RequestMetadata::new_unauthenticated();
        let rejection = gates(vec![Arc::new(DenyGate)])
            .admit(AdmissionContext::new(&md, None))
            .await
            .expect_err("DenyGate rejects");
        assert_eq!(rejection.kind(), RejectionKind::Forbidden);
        assert_eq!(rejection.kind().status(), StatusCode::FORBIDDEN.as_u16());
        assert_eq!(rejection.error_type(), "TestDenied");
    }

    #[tokio::test]
    async fn unavailable_is_503_with_gate_chosen_retry_after() {
        let md = RequestMetadata::new_unauthenticated();
        let rejection = gates(vec![Arc::new(UnavailableGate)])
            .admit(AdmissionContext::new(&md, None))
            .await
            .expect_err("UnavailableGate rejects");
        assert_eq!(
            rejection.kind(),
            RejectionKind::Unavailable {
                retry_after: Duration::from_secs(7)
            }
        );
        assert_eq!(
            rejection.kind().status(),
            StatusCode::SERVICE_UNAVAILABLE.as_u16()
        );
    }

    #[tokio::test]
    async fn first_rejection_wins_and_short_circuits() {
        let md = RequestMetadata::new_unauthenticated();
        // allow -> deny -> panic: deny must win and PanicGate must never run.
        let rejection = gates(vec![
            Arc::new(AllowGate),
            Arc::new(DenyGate),
            Arc::new(PanicGate),
        ])
        .admit(AdmissionContext::new(&md, None))
        .await
        .expect_err("DenyGate rejects before PanicGate is reached");
        assert_eq!(rejection.error_type(), "TestDenied");
    }

    #[tokio::test]
    async fn resolved_roles_surface_on_admit() {
        let md = RequestMetadata::new_unauthenticated();
        let admission = gates(vec![Arc::new(RolesGate(&["a", "b"]))])
            .admit(AdmissionContext::new(&md, None))
            .await
            .expect("RolesGate admits");
        let roles = admission.resolved_roles.expect("roles were resolved");
        assert_eq!(roles.roles().len(), 2);
    }

    #[test]
    fn outcome_labels_distinguish_deny_from_fail_closed() {
        assert_eq!(outcome_label(&Ok(GateDecision::admit())), "admitted");
        assert_eq!(
            outcome_label(&Ok(GateDecision::not_applicable())),
            "skipped"
        );
        assert_eq!(
            outcome_label(&Err(AdmissionRejection::forbidden("no", "T"))),
            "forbidden"
        );
        assert_eq!(
            outcome_label(&Err(AdmissionRejection::unavailable(
                "down",
                "T",
                Duration::from_secs(1),
                None
            ))),
            "unavailable"
        );
    }

    #[tokio::test]
    async fn resolved_roles_are_unioned_across_gates() {
        let md = RequestMetadata::new_unauthenticated();
        // Overlapping ("b") plus distinct roles: union is {a, b, c}.
        let admission = gates(vec![
            Arc::new(AllowGate),
            Arc::new(RolesGate(&["a", "b"])),
            Arc::new(RolesGate(&["b", "c"])),
        ])
        .admit(AdmissionContext::new(&md, None))
        .await
        .expect("all gates admit");
        let roles = admission.resolved_roles.expect("roles were resolved");
        assert_eq!(roles.roles().len(), 3);
    }
}
