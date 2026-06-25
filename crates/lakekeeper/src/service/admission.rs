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
//! unreachable" is not a `401`), runs *after* the instance-admin break-glass is
//! resolved, and sees the full [`RequestMetadata`].
//!
//! Gates are composed as a list ([`AdmissionGates`]) and evaluated in
//! registration order; the first rejection wins and short-circuits the rest.
//! The default — no gates configured — admits every request, so existing
//! deployments are unaffected.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use iceberg_ext::catalog::rest::ErrorModel;

use crate::request_metadata::{RequestMetadata, TokenRoles};

/// Why an [`AdmissionGate`] rejected a request.
///
/// The variant — not an inferred status code — determines the HTTP response, so
/// a gate states its intent explicitly rather than encoding it in an
/// [`ErrorModel`] the middleware has to interpret.
///
/// Non-exhaustive: further rejection kinds may be added without a breaking
/// change, so external matches must include a wildcard arm.
#[derive(Debug)]
#[non_exhaustive]
pub enum AdmissionRejection {
    /// The principal is authenticated but not entitled to this instance. This
    /// is an authoritative decision and is **terminal**: returned as
    /// `403 Forbidden` with no `Retry-After`.
    Forbidden(ErrorModel),
    /// The gate could not reach an upstream it depends on and is **failing
    /// closed**. Returned as `503 Service Unavailable` with a `Retry-After`
    /// header set to `retry_after`, so clients back off and retry instead of
    /// treating the rejection as terminal. The gate owns the duration (it
    /// reflects that gate's upstream recovery characteristics, not a global
    /// default).
    Unavailable {
        error: ErrorModel,
        retry_after: Duration,
    },
}

impl AdmissionRejection {
    /// Authoritative `403 Forbidden` denial (terminal).
    #[must_use]
    pub fn forbidden(
        message: impl Into<String>,
        r#type: impl Into<String>,
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ) -> Self {
        Self::Forbidden(ErrorModel::forbidden(message, r#type, source))
    }

    /// Fail-closed `503 Service Unavailable` with a gate-chosen `Retry-After`.
    #[must_use]
    pub fn unavailable(
        message: impl Into<String>,
        r#type: impl Into<String>,
        retry_after: Duration,
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ) -> Self {
        Self::Unavailable {
            error: ErrorModel::service_unavailable(message, r#type, source),
            retry_after,
        }
    }

    /// The underlying error payload, for logging and serialization.
    #[must_use]
    pub fn error(&self) -> &ErrorModel {
        match self {
            Self::Forbidden(error) | Self::Unavailable { error, .. } => error,
        }
    }
}

/// What a gate returns when it admits a request: an opt-in enrichment payload
/// merged into the request's [`RequestMetadata`] for downstream authorization
/// and audit. A gate that only allows/denies returns [`Admission::admit`].
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
    /// Return `Ok(`[`Admission`]`)` to admit — use [`Admission::admit`] for a
    /// plain allow, or [`Admission::with_roles`] to also contribute roles
    /// resolved in the same call. Return `Err(..)` to reject the request before
    /// it reaches any handler. The implementation owns the fail-open vs
    /// fail-closed policy by choosing the [`AdmissionRejection`] variant:
    /// [`AdmissionRejection::forbidden`] for an authoritative deny, or
    /// [`AdmissionRejection::unavailable`] to fail closed when an upstream the
    /// gate depends on is unreachable.
    async fn admit(&self, metadata: &RequestMetadata) -> Result<Admission, AdmissionRejection>;
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
    ///
    /// # Errors
    /// Returns the [`AdmissionRejection`] from the first gate that rejects the
    /// request.
    pub async fn admit(&self, metadata: &RequestMetadata) -> Result<Admission, AdmissionRejection> {
        let mut resolved_roles: Option<TokenRoles> = None;
        for gate in &self.gates {
            match gate.admit(metadata).await {
                Ok(admission) => {
                    if let Some(roles) = admission.resolved_roles {
                        // Common case is a single role-resolving gate: just move
                        // the set in. Extra gates union in place (no cloning).
                        match resolved_roles.as_mut() {
                            Some(acc) => acc.merge(roles),
                            None => resolved_roles = Some(roles),
                        }
                    }
                }
                Err(rejection) => {
                    let error = rejection.error();
                    tracing::info!(
                        gate = gate.name(),
                        status = error.code,
                        error_type = %error.r#type,
                        request_id = %metadata.request_id(),
                        "Request rejected by admission gate"
                    );
                    return Err(rejection);
                }
            }
        }
        Ok(Admission { resolved_roles })
    }
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
        async fn admit(&self, _: &RequestMetadata) -> Result<Admission, AdmissionRejection> {
            Ok(Admission::with_roles(token_roles(self.0)))
        }
    }

    #[derive(Debug)]
    struct AllowGate;
    #[async_trait]
    impl AdmissionGate for AllowGate {
        fn name(&self) -> &'static str {
            "allow"
        }
        async fn admit(&self, _: &RequestMetadata) -> Result<Admission, AdmissionRejection> {
            Ok(Admission::admit())
        }
    }

    #[derive(Debug)]
    struct DenyGate;
    #[async_trait]
    impl AdmissionGate for DenyGate {
        fn name(&self) -> &'static str {
            "deny"
        }
        async fn admit(&self, _: &RequestMetadata) -> Result<Admission, AdmissionRejection> {
            Err(AdmissionRejection::forbidden("nope", "TestDenied", None))
        }
    }

    #[derive(Debug)]
    struct UnavailableGate;
    #[async_trait]
    impl AdmissionGate for UnavailableGate {
        fn name(&self) -> &'static str {
            "unavailable"
        }
        async fn admit(&self, _: &RequestMetadata) -> Result<Admission, AdmissionRejection> {
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
        async fn admit(&self, _: &RequestMetadata) -> Result<Admission, AdmissionRejection> {
            panic!("gate after a rejection must not be evaluated");
        }
    }

    fn gates(gates: Vec<Arc<dyn AdmissionGate>>) -> AdmissionGates {
        AdmissionGates::new(gates)
    }

    #[tokio::test]
    async fn empty_admits() {
        let md = RequestMetadata::new_unauthenticated();
        assert!(AdmissionGates::default().is_empty());
        assert!(AdmissionGates::default().admit(&md).await.is_ok());
    }

    #[tokio::test]
    async fn single_allow_admits() {
        let md = RequestMetadata::new_unauthenticated();
        assert!(gates(vec![Arc::new(AllowGate)]).admit(&md).await.is_ok());
    }

    #[tokio::test]
    async fn forbidden_is_403() {
        let md = RequestMetadata::new_unauthenticated();
        let rejection = gates(vec![Arc::new(DenyGate)])
            .admit(&md)
            .await
            .expect_err("DenyGate rejects");
        assert!(matches!(rejection, AdmissionRejection::Forbidden(_)));
        assert_eq!(rejection.error().code, StatusCode::FORBIDDEN.as_u16());
        assert_eq!(rejection.error().r#type, "TestDenied");
    }

    #[tokio::test]
    async fn unavailable_is_503_with_gate_chosen_retry_after() {
        let md = RequestMetadata::new_unauthenticated();
        let rejection = gates(vec![Arc::new(UnavailableGate)])
            .admit(&md)
            .await
            .expect_err("UnavailableGate rejects");
        match rejection {
            AdmissionRejection::Unavailable { error, retry_after } => {
                assert_eq!(error.code, StatusCode::SERVICE_UNAVAILABLE.as_u16());
                assert_eq!(retry_after, Duration::from_secs(7));
            }
            AdmissionRejection::Forbidden(_) => panic!("expected Unavailable"),
        }
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
        .admit(&md)
        .await
        .expect_err("DenyGate rejects before PanicGate is reached");
        assert_eq!(rejection.error().r#type, "TestDenied");
    }

    #[tokio::test]
    async fn resolved_roles_surface_on_admit() {
        let md = RequestMetadata::new_unauthenticated();
        let admission = gates(vec![Arc::new(RolesGate(&["a", "b"]))])
            .admit(&md)
            .await
            .expect("RolesGate admits");
        let roles = admission.resolved_roles.expect("roles were resolved");
        assert_eq!(roles.roles().len(), 2);
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
        .admit(&md)
        .await
        .expect("all gates admit");
        let roles = admission.resolved_roles.expect("roles were resolved");
        assert_eq!(roles.roles().len(), 3);
    }
}
