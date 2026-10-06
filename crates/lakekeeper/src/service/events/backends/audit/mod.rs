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
#[audit_part(field = "decision")]
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

/// Rules that hold for every audit record, whatever produced it.
///
/// Available under `test` and the `test-utils` feature so that the unit tests and the
/// integration tests share one implementation. Two copies of these rules would be two
/// things to keep in step, which is the failure this module exists to catch.
///
/// These complement the committed fixtures rather than duplicating them. A fixture pins
/// the exact bytes of one scenario, and is generated by the test that asserts against it —
/// so a wrongly built event yields a fixture that agrees with it and passes for ever. The
/// rules here are statements about the format, so they reject a record that should not
/// exist regardless of which test produced it, including one nobody wrote a fixture for.
#[cfg(any(test, feature = "test-utils"))]
pub mod contract {
    use std::collections::BTreeSet;

    use strum::VariantArray as _;

    use crate::service::events::{
        AuthorizationFailureReason,
        context::{ActionContextKey, EntityField, EntityType},
    };

    /// Keys the log subscriber adds, which `AUDIT_FORMAT` deliberately does not cover.
    pub const ENVELOPE_KEYS: &[&str] = &[
        "timestamp",
        "level",
        "message",
        "target",
        "span",
        "spans",
        "filename",
        "line_number",
    ];

    /// Whether a reason means the request was evaluated and refused, as opposed to never
    /// having reached a verdict.
    ///
    /// Exhaustive rather than a list of literals: a bare `&["action_forbidden", ...]` stops
    /// matching the moment a variant is renamed, which retires the rule below in silence.
    #[deny(clippy::wildcard_enum_match_arm)]
    const fn is_definitive(reason: &AuthorizationFailureReason) -> bool {
        match reason {
            AuthorizationFailureReason::ActionForbidden
            | AuthorizationFailureReason::ResourceNotFound
            | AuthorizationFailureReason::CannotSeeResource => true,
            AuthorizationFailureReason::InternalAuthorizationError
            | AuthorizationFailureReason::InternalCatalogError
            | AuthorizationFailureReason::InvalidRequestData => false,
        }
    }

    fn definitive_denials() -> Vec<&'static str> {
        AuthorizationFailureReason::VARIANTS
            .iter()
            .filter(|reason| is_definitive(reason))
            .map(|reason| reason.as_wire().text())
            .collect()
    }

    /// Strip the subscriber-owned envelope, leaving only the fields `AUDIT_FORMAT`
    /// makes promises about, in the order they were emitted.
    ///
    /// `retain` rather than `remove`: with `serde_json`'s `preserve_order` feature (which
    /// this workspace enables) a `Map` is index-backed and `remove` is a *swap*-remove,
    /// which would shuffle the surviving fields. Order is worth keeping — a fixture that
    /// reads in wire order is a fixture a reviewer can check against a real log line.
    #[must_use]
    pub fn contract_fields(mut record: serde_json::Value) -> serde_json::Value {
        if let Some(object) = record.as_object_mut() {
            object.retain(|key, _| !ENVELOPE_KEYS.contains(&key.as_str()));
        }
        record
    }

    /// The values of `singular`/`plural` on one object, with an array flattened to its items.
    fn objects_at<'a>(
        value: &'a serde_json::Value,
        singular: &str,
        plural: &str,
    ) -> Vec<&'a serde_json::Value> {
        let mut out = Vec::new();
        for field in [singular, plural] {
            match value.get(field) {
                Some(serde_json::Value::Array(items)) => out.extend(items),
                Some(value) => out.push(value),
                None => {}
            }
        }
        out
    }

    /// Every place a record carries an entity, or an action: at the top level, and once per
    /// `authorizations` entry.
    ///
    /// Enumerated rather than found by walking the record. A walk also descends into
    /// `properties`, whose keys are client input — so a caller who names a table property
    /// `entity_type` would trip the rules below, and a record that is entirely valid would
    /// be reported as breaking the contract.
    fn described<'a>(
        record: &'a serde_json::Value,
        singular: &str,
        plural: &str,
    ) -> Vec<&'a serde_json::Value> {
        let mut out = objects_at(record, singular, plural);
        if let Some(entries) = record
            .get("authorizations")
            .and_then(serde_json::Value::as_array)
        {
            for entry in entries {
                out.extend(objects_at(entry, singular, plural));
            }
        }
        out
    }

    fn keys_at(record: &serde_json::Value, singular: &str, plural: &str) -> BTreeSet<String> {
        described(record, singular, plural)
            .into_iter()
            .flat_map(object_keys)
            .collect()
    }

    fn object_keys(value: &serde_json::Value) -> Vec<String> {
        value
            .as_object()
            .map(|o| o.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Check one record, returning every rule it breaks.
    ///
    /// Returns violations rather than panicking so a caller can report all of them at once
    /// across a whole corpus, and so the rules themselves stay testable.
    #[must_use]
    pub fn violations(record: &serde_json::Value) -> Vec<String> {
        let mut out = Vec::new();

        if record
            .get("event_source")
            .and_then(serde_json::Value::as_str)
            != Some("audit")
        {
            out.push("`event_source` is not \"audit\"".to_string());
        }
        if record
            .get("audit_format")
            .and_then(serde_json::Value::as_str)
            .is_none()
        {
            out.push(
                "no `audit_format`: every audit record must declare its wire format version"
                    .to_string(),
            );
        }

        let known_entity: BTreeSet<String> = EntityField::VARIANTS
            .iter()
            .map(|f| f.as_str().to_string())
            .chain(["entity_type".to_string()])
            .collect();
        let unknown_entity: Vec<String> = keys_at(record, "entity", "entities")
            .difference(&known_entity)
            .cloned()
            .collect();
        if !unknown_entity.is_empty() {
            out.push(format!(
                "entity keys not in `EntityField`: {unknown_entity:?}. Every key an entity can \
                 carry must be a variant of that enum, so the key space stays enumerable and \
                 documentable"
            ));
        }

        let known_action: BTreeSet<String> = ActionContextKey::WIRE_NAMES
            .iter()
            .map(|k| (*k).to_string())
            .chain(["action_name".to_string()])
            .collect();
        let unknown_action: Vec<String> = keys_at(record, "action", "actions")
            .difference(&known_action)
            .cloned()
            .collect();
        if !unknown_action.is_empty() {
            out.push(format!(
                "action context keys not in `ActionContextKey`: {unknown_action:?}. Add a \
                 variant rather than a bare literal, so the key is enumerable and the \
                 documentation test sees it"
            ));
        }

        // Only where an entity actually is. See `described`: a walk would also read
        // client-supplied property keys.
        let known_type: BTreeSet<&str> = EntityType::VARIANTS
            .iter()
            .map(EntityType::as_str)
            .collect();
        for entity in described(record, "entity", "entities") {
            if let Some(serde_json::Value::String(kind)) = entity.get("entity_type")
                && !known_type.contains(kind.as_str())
            {
                out.push(format!("`entity_type` is `{kind}`, not in `EntityType`"));
            }
        }

        let time = record.get("time").and_then(serde_json::Value::as_str);
        if !time.is_some_and(|time| {
            time.ends_with('Z') && chrono::DateTime::parse_from_rfc3339(time).is_ok()
        }) {
            out.push(format!(
                "`time` is {time:?}: every record carries when it happened, as RFC 3339 in UTC"
            ));
        }

        out.extend(failure_reason_violations(record));

        out
    }

    /// The rules that relate `failure_reason` to the rest of the record.
    ///
    /// Split out of [`violations`] only for length; they belong to the same contract.
    fn failure_reason_violations(record: &serde_json::Value) -> Vec<String> {
        let mut out = Vec::new();
        let Some(reason) = record.get("failure_reason") else {
            return out;
        };

        // Independent of how `failure_reason` is encoded, so it is checked before the
        // shape rule below returns.
        if record.get("decision").and_then(serde_json::Value::as_str) != Some("denied") {
            out.push("`failure_reason` is present but `decision` is not `denied`".to_string());
        }

        // `failure_reason` is the vocabulary's own value, so the definitive-denial rule below
        // reads the variant from the string itself. Re-encoding it — wrapping it in an object,
        // say — would make `as_str` return `None` and silently retire that rule. The
        // re-encoding itself is loud, the fixture diff shows it; losing the rule with it would
        // not be. So trip here, and make whoever re-encodes it teach the rule again rather
        // than drop it.
        let Some(named) = reason.as_str() else {
            out.push(format!(
                "`failure_reason` is `{reason}`, not a string. The definitive-denial rule \
                 reads the variant from that string, so a re-encoding disables it: teach that \
                 rule the new encoding, then update this one"
            ));
            return out;
        };

        // A definitive denial means the request was evaluated and refused, so no per-decision
        // entry may claim it was allowed. This is the rule a fixture cannot state: it relates
        // two fields, and a fixture only ever records one combination of them.
        let definitive_denials = definitive_denials();
        let definitive = definitive_denials.contains(&named);
        if definitive
            && record
                .get("authorizations")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|entries| {
                    entries.iter().any(|entry| {
                        entry.get("allowed").and_then(serde_json::Value::as_bool) == Some(true)
                    })
                })
        {
            out.push(
                "a definitive denial carries an `authorizations` entry with `allowed: true`. The \
                 emitter cannot produce that, so either the record is wrong or this rule is"
                    .to_string(),
            );
        }

        out
    }

    /// Assert `record` satisfies the contract, naming every rule it breaks.
    ///
    /// `whence` identifies the record in the failure message — a fixture name, or an index
    /// into a captured corpus.
    ///
    /// # Panics
    ///
    /// If `record` breaks any rule. That is the point: this is for use in tests.
    pub fn assert_satisfies(record: &serde_json::Value, whence: &str) {
        let violations = violations(record);
        assert!(
            violations.is_empty(),
            "{whence} breaks the audit format contract:\n  - {}\n\nrecord:\n{}",
            violations.join("\n  - "),
            serde_json::to_string_pretty(record).unwrap_or_else(|_| "<unserialisable>".to_string()),
        );
    }
}

#[cfg(test)]
mod tests;
