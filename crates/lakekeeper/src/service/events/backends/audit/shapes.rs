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
    audit::{AuditEmitter, AuditPart, WireStr},
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
#[derive(Debug, Clone, PartialEq)]
pub struct AuthorizationRecord {
    pub(crate) actions: Vec<ActionRecord>,
    pub(crate) entities: Vec<EntityRecord>,
    pub(crate) actor: ActorRecord,
    pub(crate) privilege_source: PrivilegeSource,
    pub(crate) user_agent: Option<String>,
    pub(crate) break_glass: Option<String>,
    pub(crate) context: Option<HandlerContext>,
    pub(crate) authorizations: Vec<DecisionRecord>,
    pub(crate) idempotency_key: Option<String>,
    pub(crate) decision: Decision,
    /// Why a denied record was denied, as the vocabulary spells it.
    pub(crate) failure_reason: Option<WireStr<crate::Lakekeeper>>,
    pub(crate) error: Option<ErrorRecord>,
}

impl AuthorizationRecord {
    pub(crate) fn emit(self) {
        let emitter = AuditJson::of(&EmitterRecord::of::<crate::Lakekeeper>());
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
                record_type = RecordType::Authorization.as_str(),
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
#[derive(Debug, Clone, PartialEq)]
pub struct ReplayRecord {
    pub(crate) actions: Vec<ActionRecord>,
    pub(crate) entities: Vec<EntityRecord>,
    pub(crate) actor: ActorRecord,
    pub(crate) privilege_source: PrivilegeSource,
    pub(crate) user_agent: Option<String>,
    pub(crate) idempotency_key: String,
}

impl ReplayRecord {
    pub(crate) fn emit(self) {
        let emitter = AuditJson::of(&EmitterRecord::of::<crate::Lakekeeper>());
        let actions = AuditJson::of(&self.actions);
        let entities = AuditJson::of(&self.entities);
        let actor = AuditJson::of(&self.actor);
        emit_stamped!(
            {
                record_type = RecordType::Replay.as_str(),
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
        let emitter = AuditJson::of(&EmitterRecord::of::<E>());
        let actor = AuditJson::of(&self.actor);
        let context = self.context.as_ref().map(AuditJson::of);
        emit_stamped!(
            {
                record_type = RecordType::Operation.as_str(),
                emitter = valuable(&emitter),
                operation = self.operation.text(),
                actor = valuable(&actor),
                outcome = self.outcome.text(),
                context = context.as_ref().map(valuable),
            },
            self.message
        );
    }
}
