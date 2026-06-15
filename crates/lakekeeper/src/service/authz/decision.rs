//! Per-decision authorization results and their contributing diagnostics.
//!
//! These types are authorizer-agnostic and contain no authorizer-specific
//! types, so they can live in the audit-event payload while each authorizer
//! maps its own diagnostics down to them.

/// One authorization verdict together with the diagnostics that explain it.
///
/// Returned per checked `(resource, action)` tuple by the batch authorizer
/// methods. `allowed` is the decision; `determined_by` lists the policies or
/// rules that determined it. `determined_by` is empty when the authorizer
/// produces no per-decision diagnostics (`AllowAll`, OpenFGA) or for a
/// default-deny where no policy matched.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthorizationDecision {
    pub allowed: bool,
    pub determined_by: Vec<DeterminingFactor>,
}

impl AuthorizationDecision {
    /// An allow carrying no diagnostics.
    #[must_use]
    pub fn allow() -> Self {
        Self {
            allowed: true,
            determined_by: Vec::new(),
        }
    }

    /// A deny carrying no diagnostics.
    #[must_use]
    pub fn deny() -> Self {
        Self {
            allowed: false,
            determined_by: Vec::new(),
        }
    }

    /// A decision carrying the factors that determined it.
    #[must_use]
    pub fn new(allowed: bool, determined_by: Vec<DeterminingFactor>) -> Self {
        Self {
            allowed,
            determined_by,
        }
    }
}

impl PartialEq<bool> for AuthorizationDecision {
    /// A decision compares equal to a `bool` by its verdict (`allowed`),
    /// ignoring diagnostics. Convenient for asserting allow/deny outcomes,
    /// including `Vec<AuthorizationDecision> == Vec<bool>` via the standard
    /// library's cross-type `Vec` equality.
    fn eq(&self, other: &bool) -> bool {
        self.allowed == *other
    }
}

impl From<bool> for AuthorizationDecision {
    /// A verdict with no diagnostics — for authorizers that produce only a
    /// boolean (`AllowAll`, OpenFGA) or call sites that have no trace.
    fn from(allowed: bool) -> Self {
        Self {
            allowed,
            determined_by: Vec::new(),
        }
    }
}

/// A single factor that contributed to an authorization decision.
///
/// Enum-tagged so new producers (restriction-profile matched rules, native
/// OSS-authorizer diagnostics) add a variant without breaking existing audit
/// consumers.
#[derive(Clone, Debug, PartialEq, Eq, valuable::Valuable)]
pub enum DeterminingFactor {
    /// A policy that determined the decision, surfaced by a policy-based
    /// authorizer.
    Policy {
        /// Stable, authorizer-assigned identifier of the policy (e.g. the Cedar
        /// `PolicyId`). Always present.
        policy_id: String,
        /// Optional human-facing name the author gave the policy (e.g. a `@name`
        /// or `@id` annotation). Neither required nor guaranteed unique; `None`
        /// when the author provided none.
        name: Option<String>,
        /// Whether the policy permits or forbids.
        effect: PolicyEffect,
        /// Opaque origin of the policy (e.g. a policy-source identifier). `None`
        /// when the producer cannot attribute a source.
        source: Option<String>,
    },
}

/// Whether a determining policy permits or forbids.
#[derive(Clone, Copy, Debug, PartialEq, Eq, valuable::Valuable)]
pub enum PolicyEffect {
    Permit,
    Forbid,
}
