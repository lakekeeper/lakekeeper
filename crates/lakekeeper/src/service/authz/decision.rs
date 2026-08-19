//! Per-decision authorization results and their contributing diagnostics.
//!
//! These types are authorizer-agnostic and contain no authorizer-specific
//! types, so they can live in the audit-event payload while each authorizer
//! maps its own diagnostics down to them.

/// One authorization verdict together with the diagnostics that explain it.
///
/// Returned per checked `(resource, action)` tuple by the batch authorizer
/// methods. `allowed` is the decision; `determined_by` lists the factors that
/// determined it — matched policies, or a system-authority override.
/// `determined_by` is empty when the authorizer produces no per-decision
/// diagnostics (`AllowAll`, OpenFGA) or for a default-deny where no policy
/// matched.
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
///
/// # Optional fields: path 3 of 3 — key present, value `null`
///
/// `name`, `source` and `reason` are emitted **unconditionally**: `valuable-derive`
/// supports only `rename`, `transparent` and unconditional `skip`
/// (`valuable-derive-0.1.1/src/attr.rs:10-38`) — there is no `skip_serializing_if`
/// equivalent — so the derive visits every field and `None` becomes JSON `null` via
/// `impl Valuable for Option<T>`.
///
/// The audit record's other two optional-field paths, both in
/// [`crate::service::events::backends::audit`]:
///
/// - **Top-level `tracing` field** — `user_agent_value`: also `null`, but because the
///   field is always recorded rather than because a derive forced it. Same outcome by
///   coincidence, not by design.
/// - **Hand-written `visit`** — `Authorization::visit`: the key is **omitted** when
///   `None`. This is the path that disagrees.
///
/// So a consumer cannot infer from one field how another behaves. Unifying the three
/// is an open issue for the next major version bump — see the audit-log section of
/// `docs/docs/developer-guide.md`.
#[derive(Clone, Debug, PartialEq, Eq, valuable::Valuable, strum_macros::VariantNames)]
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
    /// An allow contributed by a built-in/system authority tier that takes
    /// precedence over normal authored policy — e.g. a recovery mechanism that
    /// lets a privileged system role act despite a policy that would otherwise
    /// forbid it. Recorded so audit consumers can see that the verdict rested on
    /// built-in authority rather than on a configured policy.
    ///
    /// Authorizer-agnostic like the rest of this module: names no specific
    /// authorizer or mechanism.
    SystemAuthority {
        /// Opaque, authorizer-assigned identifier of the built-in authority tier
        /// that granted the action. `None` when none can be attributed.
        source: Option<String>,
        /// Optional human-facing reason the tier applied (e.g. an administrator
        /// lockout-recovery grant). `None` when the producer gives none.
        reason: Option<String>,
    },
}

/// Whether a determining policy permits or forbids.
#[derive(Clone, Copy, Debug, PartialEq, Eq, valuable::Valuable, strum_macros::VariantArray)]
pub enum PolicyEffect {
    Permit,
    Forbid,
}
