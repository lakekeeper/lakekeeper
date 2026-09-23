//! The three shapes an audit record can have, and the one `tracing::info!` each ends in.
//!
//! A shape is what a consumer receives. Its `emit()` is the only code that writes an audit
//! record: it stamps `event_source` and `audit_format`, passes scalar fields as `tracing`
//! fields, and hands every object field to `tracing` as a JSON tree through the bridge.
//!
//! Every record names its shape in `record_type` and its producer in `emitter`, carries its
//! action and entity lists under one name whatever their length, and omits a field it has no
//! value for rather than writing `null`.

use tracing::field::valuable;

use super::{
    AUDIT_FORMAT, Decision, RecordType,
    parts::{
        ActionRecord, ActorRecord, DecisionRecord, EmitterRecord, EntityRecord, ErrorRecord,
        HandlerContext,
    },
    render::AuditJson,
};
use crate::{
    audit::{AnyWireStr, AuditEmitter, AuditPart, WireStr, audit_part},
    request_metadata::PrivilegeSource,
};

/// Every top-level field name a record of any shape can carry. The context-key rule reads
/// this: no key of a `context` map may spell one of these, whatever its separator.
pub const TOP_LEVEL_FIELDS: &[&str] = &[
    "event_source",
    "audit_format",
    "record_type",
    "emitter",
    "action",
    "actions",
    "entity",
    "entities",
    "actor",
    "privilege_source",
    "user_agent",
    "break_glass",
    "failure_reason",
    "error",
    "context",
    "authorizations",
    "idempotency_key",
    "decision",
    "operation",
    "outcome",
];

/// The `tracing` target of every audit record. The module path the previous implementation
/// emitted from, kept so operator filters keep matching; the move to a stable, documented
/// target is a versioned change of its own.
const TARGET: &str = "lakekeeper::service::events::backends::audit";

/// The one `tracing::info!` every audit record goes through: it stamps the target,
/// `event_source` and `audit_format`, so no record can miss the version. Every other emission
/// macro in this module expands to it, and a CI check counts that this literal exists once.
macro_rules! emit_stamped {
    ({ $($fields:tt)* }, $msg:expr) => {
        tracing::info!(
            target: TARGET,
            event_source = "audit",
            audit_format = AUDIT_FORMAT,
            $($fields)*
            "{}", $msg
        )
    };
}

/// An authorization record: was this caller permitted to do these actions on these entities?
#[audit_part(shape = "authorization")]
#[derive(Debug, Clone, PartialEq)]
pub struct AuthorizationRecord {
    /// Names this record's shape. Always `authorization`.
    #[schemars(with = "String")]
    pub(crate) record_type: RecordType,
    /// Which product produced this record, and the version of what it governs.
    pub(crate) emitter: EmitterRecord,
    /// The actions evaluated, always a list however many there are.
    pub(crate) actions: Vec<ActionRecord>,
    /// The entities they were evaluated against, always a list.
    pub(crate) entities: Vec<EntityRecord>,
    /// Who made the request, as authentication established it.
    pub(crate) actor: ActorRecord,
    /// Which authority answered: the authorizer, or a bypass.
    #[schemars(with = "String")]
    pub(crate) privilege_source: PrivilegeSource,
    /// The `User-Agent` header, verbatim and unverified. Absent when none was sent.
    pub(crate) user_agent: Option<String>,
    /// The stated break-glass reason. Absent unless the caller claimed one.
    pub(crate) break_glass: Option<String>,
    /// Handler-supplied detail about the request. Absent when the handler added none.
    pub(crate) context: Option<HandlerContext>,
    /// One entry per permission evaluated.
    pub(crate) authorizations: Vec<DecisionRecord>,
    /// The request's `Idempotency-Key`. Absent when the caller sent none.
    pub(crate) idempotency_key: Option<String>,
    /// Whether the request was permitted.
    #[schemars(with = "String")]
    pub(crate) decision: Decision,
    /// Why a denied record was denied, as the vocabulary spells it.
    pub(crate) failure_reason: Option<WireStr<crate::Lakekeeper>>,
    /// The error the caller received. Present only on a denial that produced one.
    pub(crate) error: Option<ErrorRecord>,
}

impl AuthorizationRecord {
    pub(crate) fn emit(self) {
        let emitter = AuditJson::of(&self.emitter);
        let actions = AuditJson::of(&self.actions);
        let entities = AuditJson::of(&self.entities);
        let actor = AuditJson::of(&self.actor);
        let authorizations = AuditJson::of(&self.authorizations);
        let context = self.context.as_ref().map(AuditJson::of);
        let error = self.error.as_ref().map(AuditJson::of);
        let message = match self.decision {
            Decision::Allowed => "Authorization succeeded event",
            Decision::Denied => "Authorization failed event",
        };
        emit_stamped!(
            {
                record_type = self.record_type.as_str(),
                emitter = valuable(&emitter),
                actions = valuable(&actions),
                entities = valuable(&entities),
                actor = valuable(&actor),
                privilege_source = self.privilege_source.as_str(),
                user_agent = self.user_agent.as_deref(),
                break_glass = self.break_glass.as_deref(),
                context = context.as_ref().map(valuable),
                authorizations = valuable(&authorizations),
                idempotency_key = self.idempotency_key.as_deref(),
                decision = self.decision.as_str(),
                failure_reason = self.failure_reason.map(WireStr::text),
                error = error.as_ref().map(valuable),
            },
            message
        );
    }
}

/// A replay record: a retry answered from an idempotency record, so no authorization ran.
#[audit_part(shape = "replay")]
#[derive(Debug, Clone, PartialEq)]
pub struct ReplayRecord {
    /// Names this record's shape. Always `replay`.
    #[schemars(with = "String")]
    pub(crate) record_type: RecordType,
    /// Which product produced this record, and the version of what it governs.
    pub(crate) emitter: EmitterRecord,
    /// The actions the replayed request named, always a list.
    pub(crate) actions: Vec<ActionRecord>,
    /// The entities it named, always a list. As the caller wrote them: a replay resolves
    /// nothing.
    pub(crate) entities: Vec<EntityRecord>,
    /// Who made the request, as authentication established it.
    pub(crate) actor: ActorRecord,
    /// Which authority would have answered, had one been asked.
    #[schemars(with = "String")]
    pub(crate) privilege_source: PrivilegeSource,
    /// The `User-Agent` header, verbatim and unverified. Absent when none was sent.
    pub(crate) user_agent: Option<String>,
    /// The key whose stored response was served. Always present: it is what makes this a
    /// replay.
    pub(crate) idempotency_key: String,
}

impl ReplayRecord {
    pub(crate) fn emit(self) {
        let emitter = AuditJson::of(&self.emitter);
        let actions = AuditJson::of(&self.actions);
        let entities = AuditJson::of(&self.entities);
        let actor = AuditJson::of(&self.actor);
        emit_stamped!(
            {
                record_type = self.record_type.as_str(),
                emitter = valuable(&emitter),
                actions = valuable(&actions),
                entities = valuable(&entities),
                actor = valuable(&actor),
                privilege_source = self.privilege_source.as_str(),
                user_agent = self.user_agent.as_deref(),
                idempotency_key = self.idempotency_key.as_str(),
            },
            "Idempotent replay served"
        );
    }
}

/// An operation record: something the system did that touches identity or access, with no
/// permission decision of its own. The one shape any crate can emit.
///
/// `E` is the emitter. `operation`, `outcome` and `context` must all belong to it, or the
/// record does not compile; `E` is what stamps the emitter on the record once that field
/// exists.
#[derive(Debug)]
pub struct OperationRecord<E: AuditEmitter, C: AuditPart<Emitter = E>> {
    operation: WireStr<E>,
    actor: ActorRecord,
    outcome: WireStr<E>,
    context: Option<C>,
    message: &'static str,
}

impl<E: AuditEmitter> OperationRecord<E, E::NoContext> {
    /// A record without context. Add one with [`OperationRecord::context`].
    #[must_use]
    pub fn new(operation: WireStr<E>, actor: ActorRecord, outcome: WireStr<E>) -> Self {
        Self {
            operation,
            actor,
            outcome,
            context: None,
            message: "Audit operation",
        }
    }
}

impl<E: AuditEmitter, C: AuditPart<Emitter = E>> OperationRecord<E, C> {
    /// Attach the operation's `context` object, an audit type of the same emitter.
    #[must_use]
    pub fn context<C2: AuditPart<Emitter = E>>(self, context: C2) -> OperationRecord<E, C2> {
        OperationRecord {
            operation: self.operation,
            actor: self.actor,
            outcome: self.outcome,
            context: Some(context),
            message: self.message,
        }
    }

    /// The human-readable `message` of the log line. Outside `audit_format`.
    #[must_use]
    pub fn message(mut self, message: &'static str) -> Self {
        self.message = message;
        self
    }

    /// Write the record.
    pub fn emit(self) {
        OperationWire {
            record_type: RecordType::Operation,
            emitter: EmitterRecord::of::<E>(),
            operation: self.operation.into(),
            actor: self.actor,
            outcome: self.outcome.into(),
            context: self.context.as_ref().map(AuditJson::of),
        }
        .emit(self.message);
    }
}

/// An operation record as it reaches the wire.
///
/// The builder above is generic, so a record cannot mix two emitters' vocabularies. What it
/// writes is not generic, and this is it: one struct, so the schema describes the record
/// rather than a description of it kept alongside.
#[audit_part(shape = "operation")]
#[schemars(rename = "OperationRecord")]
#[derive(Debug)]
struct OperationWire {
    /// Names this record's shape. Always `operation`.
    #[schemars(with = "String")]
    record_type: RecordType,
    /// Which product produced this record, and the version of what it governs.
    emitter: EmitterRecord,
    /// What was done, from the emitter's own vocabulary.
    #[schemars(with = "String")]
    operation: AnyWireStr,
    /// Who made the request, as authentication established it.
    actor: ActorRecord,
    /// How it ended, from the emitter's own vocabulary.
    #[schemars(with = "String")]
    outcome: AnyWireStr,
    /// The operation's own detail. One shape per operation kind, each declared by its
    /// emitter; absent for an operation that carries none.
    #[schemars(with = "Option<serde_json::Value>")]
    context: Option<AuditJson>,
}

impl OperationWire {
    fn emit(self, message: &'static str) {
        let emitter = AuditJson::of(&self.emitter);
        let actor = AuditJson::of(&self.actor);
        emit_stamped!(
            {
                record_type = self.record_type.as_str(),
                emitter = valuable(&emitter),
                operation = self.operation.text(),
                actor = valuable(&actor),
                outcome = self.outcome.text(),
                context = self.context.as_ref().map(valuable),
            },
            message
        );
    }
}
