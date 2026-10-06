use std::fmt::Display;

use crate::audit::audit_part;

pub mod assemble;
pub mod emitter;
pub mod part;
pub mod parts;
pub mod render;
#[cfg(any(test, feature = "test-utils"))]
pub mod schema;
pub mod shapes;
#[cfg(any(test, feature = "test-utils"))]
pub mod validate;

pub use emitter::{AuditEmitter, EmitterStamp, is_emitter_name};
pub use part::{
    AUDIT_TARGET, AnyWireStr, AuditPart, Kind, OperationValues, OutcomeValues, RecordContextKey,
    Registration, Vocabulary, Wire, WireKey, WireName, enabled, warn_on_retired_audit_filter,
};
pub use parts::{
    ActionRecord, ActorRecord, AssumedRoleRecord, DecisionRecord, EntityRecord, ErrorRecord,
    GrantContextRecord, HandlerContext, RoleSubjectRecord, SubjectRecord, UserSubjectRecord,
};
pub use render::AuditJson;
pub use shapes::{AuthorizationRecord, OperationRecord, RecordOrigin, ReplayRecord};

use crate::service::events::{
    AuthorizationFailedEvent, AuthorizationSucceededEvent, EventListener, GrantsChangedEvent,
    IdempotentReplayEvent,
};

/// Wire-format version of every `event_source = "audit"` record, emitted
/// unconditionally as the `audit_format` field.
///
/// **Not edited by hand.** The value is derived from committed state — the version the
/// last release shipped (`audit-format/released.json`), raised once by the highest level
/// among the changes recorded since (`audit-format/unreleased/*.md`) — and written by
/// `just update-audit-fixtures`. A release therefore raises it at most once however many
/// changes it carries, and a major change absorbs every minor change in the same cycle.
///
/// **MAJOR** covers a `major` change: an existing field renamed, retyped, or structurally
/// moved — including a scalar becoming an object, an object becoming an array, or a field
/// changing case or separator — or a wire value renamed.
///
/// **MINOR** covers a `minor` change: a field added and nothing existing changed.
/// Consumers must ignore unknown fields.
///
/// The value describes a RELEASED build. On an unreleased build it names the version the
/// next release will carry, which that build may not yet emit in full; `docs/docs/logging.md`
/// states this to consumers.
///
/// Consumers must split on `'.'` and compare each half as an **integer**. Do not
/// compare the string lexically: `"1.10"` sorts *before* `"1.9"`.
///
/// One counter covers both audit families, authorization and operational. Separate
/// counters would be worse for the operational family: its `context` is supplied by
/// whoever builds an [`OperationRecord`], including crates outside this repository, so no
/// version stamped here could describe those shapes accurately.
///
/// See the audit-log section of `docs/docs/developer-guide.md` for what to do when
/// the format changes, and `docs/docs/logging.md` for the consumer-facing contract.
pub const AUDIT_FORMAT: &str = "1.0";

/// The `event_source` every audit record carries: what marks a log line as one.
pub const EVENT_SOURCE: &str = "audit";

/// Whether `s` is exactly `MAJOR.MINOR`. Hand-rolled over bytes because `==` on `&str` is
/// not const-evaluable (rust-lang/rust#143874).
#[must_use]
pub const fn is_major_minor(s: &str) -> bool {
    let b = s.as_bytes();
    let mut dots = 0usize;
    let mut digits_in_part = 0usize;
    let mut i = 0usize;
    while i < b.len() {
        match b[i] {
            // A dot with no digits before it (".0") or a second dot ("1.0.0") is
            // not `MAJOR.MINOR`.
            b'.' => {
                if digits_in_part == 0 || dots == 1 {
                    return false;
                }
                dots += 1;
                digits_in_part = 0;
            }
            b'0'..=b'9' => digits_in_part += 1,
            _ => return false,
        }
        i += 1;
    }
    // Exactly one dot, and the minor part is non-empty ("1." is rejected).
    dots == 1 && digits_in_part > 0
}

// Pins the shape of the version string, not its value — correctness is the fixtures' job.
const _: () = assert!(
    is_major_minor(AUDIT_FORMAT),
    "AUDIT_FORMAT must be `MAJOR.MINOR`, e.g. \"1.0\""
);

/// The `actor_type` value on every audit record.
#[audit_part(field = "actor_type")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, strum_macros::VariantNames)]
#[strum(serialize_all = "snake_case")]
pub enum ActorType {
    Anonymous,
    Principal,
    AssumedRole,
    LakekeeperInternal,
}

/// The `record_type` value: which shape a record has.
///
/// A consumer routes on this field alone. Nothing has to be inferred from which fields are
/// absent, and a shape can be added without changing how the existing ones are recognised.
#[audit_part(field = "record_type")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, strum_macros::VariantNames)]
#[strum(serialize_all = "snake_case")]
pub enum RecordType {
    /// Was this caller permitted to do these actions on these entities?
    Authorization,
    /// A retry answered from an idempotency record, so no authorization ran.
    Replay,
    /// Something the system did that touches identity or access.
    Operation,
}

/// The `decision` value on an authorization record.
#[audit_part(field = "decision", closed)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, strum_macros::VariantNames)]
#[strum(serialize_all = "snake_case")]
pub enum Decision {
    Allowed,
    Denied,
}

/// The `operation` value on the operational records this crate emits.
///
/// This does not close the `operation` space. [`OperationRecord`] takes a value of any
/// operation vocabulary of its emitter, so a crate outside this repository names its own
/// operations and is responsible for its own vocabulary — see the audit log section of `docs/docs/developer-guide.md`. What
/// the enum does is bring Lakekeeper's own operations under the same rename check as
/// everything else it emits.
#[audit_part(field = "operation")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, strum_macros::VariantNames)]
#[strum(serialize_all = "snake_case")]
pub enum AuditOperation {
    AdmissionDecided,
    GrantCreated,
    GrantRevoked,
}

/// The `outcome` value on the operational records this crate emits. Open to other crates in
/// the same way [`AuditOperation`] is.
#[audit_part(field = "outcome")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, strum_macros::VariantNames)]
#[strum(serialize_all = "snake_case")]
pub enum AuditOutcome {
    /// The operation completed.
    Success,
    /// An admission gate denied the caller authoritatively.
    Forbidden,
    /// An admission gate could not reach an upstream it needs and failed closed.
    /// Separate from `forbidden` so an outage of that upstream reads as an outage
    /// rather than as a wave of denials.
    Unavailable,
}

// `TableUpdateKind` reaches the wire as the `update_kinds` field of a commit action's
// context, but it lives in `iceberg-ext`, which cannot carry the attribute: the expansion
// names `::lakekeeper`, and this crate already depends on that one. Registered here instead,
// from the same `VariantNames` the attribute would have read, so a renamed variant still
// fails the format check rather than reaching consumers unannounced.
//
// The values are the Iceberg REST specification's own table-update action names, so they are
// external: they keep that spelling rather than this log's.
#[cfg(debug_assertions)]
const UPDATE_KIND_TEXTS: &[&str] =
    <iceberg_ext::catalog::TableUpdateKind as strum::VariantNames>::VARIANTS;
#[cfg(debug_assertions)]
const UPDATE_KIND_COUNT: usize = UPDATE_KIND_TEXTS.len();
#[cfg(debug_assertions)]
static UPDATE_KIND_NAMES: [crate::audit::WireName; UPDATE_KIND_COUNT] =
    crate::audit::WireName::from_texts(UPDATE_KIND_TEXTS);

#[cfg(debug_assertions)]
crate::__private::inventory::submit! {
    Registration {
        kind: Kind::Values {
            field: "update_kinds",
            names: &UPDATE_KIND_NAMES,
            closed: false,
        },
        type_name: || core::any::type_name::<iceberg_ext::catalog::TableUpdateKind>(),
        emitter: EmitterStamp::of::<crate::Lakekeeper>(),
        emitter_type: || core::any::type_name::<crate::Lakekeeper>(),
        defining_crate: env!("CARGO_PKG_NAME"),
        external_values: true,
        schema_name: None,
        schema: None,
    }
}

// `TableUpdateKind` is a vocabulary by hand for the same reason it is registered by hand: its
// crate cannot carry the attribute. Its names are `VariantNames`' list, in declaration order,
// which is also the order of its discriminants.
impl Vocabulary for iceberg_ext::catalog::TableUpdateKind {
    type Emitter = crate::Lakekeeper;
    const SCHEMA_NAME: &'static str = "TableUpdateKind";
    fn wire(&self) -> Wire<Self> {
        Wire::new(<Self as strum::VariantNames>::VARIANTS[*self as usize])
    }
}

/// The audit backend: renders events into audit records and writes them as log lines.
///
/// One method per event kind it records. Each asks [`crate::audit::enabled`] before it does
/// any work, then assembles the record's shape from the event ([`assemble`]) and calls the
/// shape's `emit()`; nothing else in this crate writes an audit record.
///
/// The gate is asked here as well as inside `emit()` because assembly is the expensive half:
/// it enriches the event and serializes every nested object. Asking first means a catalog
/// with the audit trail switched off pays nothing per request, rather than building records
/// that are dropped on the way out.
#[derive(Debug)]
pub struct AuditEventListener;

impl Display for AuditEventListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AuditEventListener")
    }
}

#[async_trait::async_trait]
impl EventListener for AuditEventListener {
    async fn authorization_failed(&self, event: AuthorizationFailedEvent) -> anyhow::Result<()> {
        if !enabled() {
            return Ok(());
        }
        assemble::authorization_failed(&event).emit("Authorization failed event");
        Ok(())
    }

    /// The grants that actually landed.
    ///
    /// The authorization event records the *attempt*, and deduplicates principals and
    /// privileges into separate lists — so it cannot say which principal received which
    /// privilege. This records the confirmed triples, which is what attribution and
    /// reconstruction of current access need. A revoked grant is hard-deleted, so its
    /// record here is the only remaining evidence the access ever existed.
    async fn grants_changed(&self, event: GrantsChangedEvent) -> anyhow::Result<()> {
        if !enabled() {
            return Ok(());
        }
        // One record per triple, not one per request: the batch is a dispatch
        // optimisation, while the audit trail is answered per grant.
        for spec in &event.removed {
            OperationRecord::new(
                AuditOperation::GrantRevoked.as_wire(),
                &*event.request_metadata,
                AuditOutcome::Success.as_wire(),
            )
            .context(GrantContextRecord::new(
                &spec.principal,
                &spec.privilege,
                &spec.resource,
            ))
            .message("Grant revoked")
            .emit();
        }
        for spec in &event.created {
            OperationRecord::new(
                AuditOperation::GrantCreated.as_wire(),
                &*event.request_metadata,
                AuditOutcome::Success.as_wire(),
            )
            .context(GrantContextRecord::new(
                &spec.principal,
                &spec.privilege,
                &spec.resource,
            ))
            .message("Grant created")
            .emit();
        }
        Ok(())
    }

    async fn authorization_succeeded(
        &self,
        event: AuthorizationSucceededEvent,
    ) -> anyhow::Result<()> {
        if !enabled() {
            return Ok(());
        }
        assemble::authorization_succeeded(&event).emit("Authorization succeeded event");
        Ok(())
    }

    /// A retry answered from an idempotency record.
    ///
    /// Carries `actions` and `entities` in the same shape as the two authorization records
    /// above, so one query over the audit stream sees the original request and every replay
    /// of it. It carries no `decision`, because no authorization ran: the mutation had
    /// already happened and there was nothing left to permit. `record_type` is `replay`,
    /// which is what a consumer routes on.
    async fn idempotent_replay_served(&self, event: IdempotentReplayEvent) -> anyhow::Result<()> {
        if !enabled() {
            return Ok(());
        }
        assemble::replay(&event).emit("Idempotent replay served");
        Ok(())
    }
}

#[cfg(test)]
mod tests;
