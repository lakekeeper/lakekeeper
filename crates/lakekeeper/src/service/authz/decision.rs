//! Per-decision authorization results and their contributing diagnostics.
//!
//! These types are authorizer-agnostic and contain no authorizer-specific
//! types, so they can live in the audit-event payload while each authorizer
//! maps its own diagnostics down to them.

use serde::{Deserialize, Serialize};

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
/// Discriminated by `type`: `policy` names a policy the authorizer matched,
/// `system-authority` records that a built-in authority tier decided the
/// request. Further kinds may be added, so treat an unrecognised `type` as an
/// opaque factor rather than an error.
// Enum-tagged so new producers (restriction-profile matched rules, native
// OSS-authorizer diagnostics) add a variant without breaking consumers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, valuable::Valuable)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum DeterminingFactor {
    /// A policy that determined the decision, surfaced by a policy-based
    /// authorizer.
    #[cfg_attr(feature = "open-api", schema(title = "DeterminingFactorPolicy"))]
    #[serde(rename_all = "kebab-case")]
    Policy {
        /// Stable, authorizer-assigned identifier of the policy (e.g. the Cedar
        /// `PolicyId`). Always present.
        policy_id: String,
        /// Human-facing name the author gave the policy (e.g. a `@name` or
        /// `@id` annotation). Neither required nor guaranteed unique; absent
        /// when the author provided none.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        /// Whether the policy permits or forbids.
        effect: PolicyEffect,
        /// Opaque origin of the policy (e.g. a policy-source identifier).
        /// Absent when the authorizer cannot attribute a source.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        source: Option<String>,
    },
    /// An allow contributed by a built-in/system authority tier that takes
    /// precedence over normal authored policy — e.g. a recovery mechanism that
    /// lets a privileged system role act despite a policy that would otherwise
    /// forbid it. Its presence means the verdict rested on built-in authority
    /// rather than on a configured policy.
    #[cfg_attr(
        feature = "open-api",
        schema(title = "DeterminingFactorSystemAuthority")
    )]
    #[serde(rename_all = "kebab-case")]
    SystemAuthority {
        /// Opaque, authorizer-assigned identifier of the built-in authority
        /// tier that granted the action. Absent when none can be attributed.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        source: Option<String>,
        /// Human-facing reason the tier applied (e.g. an administrator
        /// lockout-recovery grant). Absent when the authorizer gives none.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
}

/// Whether a determining policy permits or forbids.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, valuable::Valuable)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub enum PolicyEffect {
    /// The policy grants the action.
    Permit,
    /// The policy denies the action.
    Forbid,
}
