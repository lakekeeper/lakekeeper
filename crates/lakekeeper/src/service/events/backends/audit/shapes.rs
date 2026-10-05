//! The three shapes an audit record can have.
//!
//! A shape is what a consumer receives. `#[audit_part(shape = "...")]` writes its `emit()`,
//! the only code that writes an audit record: it stamps `event_source`, `audit_format` and
//! `record_type`, and puts every field on the line under its own name.
//!
//! Every record names its shape in `record_type` and its producers in `emitters`, carries its
//! action and entity lists under one name whatever their length, and omits a field it has no
//! value for rather than writing `null`.

use super::{
    Decision,
    parts::{
        ActionRecord, ActorRecord, DecisionRecord, EmitterRecord, EntityRecord, ErrorRecord,
        HandlerContext,
    },
    render::AuditJson,
};
use crate::{
    audit::{
        AnyWireStr, AuditEmitter, AuditPart, OperationValues, OutcomeValues, Vocabulary, Wire,
        audit_part,
    },
    request_metadata::PrivilegeSource,
    service::events::AuthorizationFailureReason,
};

/// An authorization record: was this caller permitted to do these actions on these entities?
#[audit_part(shape = "authorization")]
#[derive(Debug, Clone, PartialEq)]
pub struct AuthorizationRecord {
    /// Every product that contributed to this record, with the version of what each
    /// governs: the one that assembled it and any whose vocabulary it carries. Sorted by
    /// name, always a list however many there are.
    pub(crate) emitters: Vec<EmitterRecord>,
    /// The actions evaluated, always a list however many there are.
    pub(crate) actions: Vec<ActionRecord>,
    /// The entities they were evaluated against, always a list.
    pub(crate) entities: Vec<EntityRecord>,
    /// Who made the request, as authentication established it.
    pub(crate) actor: ActorRecord,
    /// Which authority answered: the authorizer, or a bypass.
    pub(crate) privilege_source: Wire<PrivilegeSource>,
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
    pub(crate) decision: Wire<Decision>,
    /// Why a denied record was denied, as the vocabulary spells it.
    pub(crate) failure_reason: Option<Wire<AuthorizationFailureReason>>,
    /// The error the caller received. Present only on a denial that produced one.
    pub(crate) error: Option<ErrorRecord>,
}

/// A replay record: a retry answered from an idempotency record, so no authorization ran.
#[audit_part(shape = "replay")]
#[derive(Debug, Clone, PartialEq)]
pub struct ReplayRecord {
    /// Every product that contributed to this record, with the version of what each
    /// governs: the one that assembled it and any whose vocabulary it carries. Sorted by
    /// name, always a list however many there are.
    pub(crate) emitters: Vec<EmitterRecord>,
    /// The actions the replayed request named, always a list.
    pub(crate) actions: Vec<ActionRecord>,
    /// The entities it named, always a list. As the caller wrote them: a replay resolves
    /// nothing.
    pub(crate) entities: Vec<EntityRecord>,
    /// Who made the request, as authentication established it.
    pub(crate) actor: ActorRecord,
    /// Which authority would have answered, had one been asked.
    pub(crate) privilege_source: Wire<PrivilegeSource>,
    /// The `User-Agent` header, verbatim and unverified. Absent when none was sent.
    pub(crate) user_agent: Option<String>,
    /// The key whose stored response was served. Always present: it is what makes this a
    /// replay.
    pub(crate) idempotency_key: String,
}

/// An operation record: something the system did that touches identity or access, with no
/// permission decision of its own. The one shape any crate can emit.
///
/// `E` is the emitter. `operation`, `outcome` and `context` must all belong to it, or the
/// record does not compile; `E` is what stamps the emitter on the record.
#[derive(Debug)]
pub struct OperationRecord<E: AuditEmitter> {
    operation: AnyWireStr,
    actor: ActorRecord,
    outcome: AnyWireStr,
    context: Option<AuditJson>,
    message: &'static str,
    _emitter: std::marker::PhantomData<E>,
}

impl<E: AuditEmitter> OperationRecord<E> {
    /// A record without context. Add one with [`OperationRecord::context`].
    #[must_use]
    ///
    /// `operation` and `outcome` come from the emitter's own vocabularies for those two
    /// fields, and each is accepted only in its own place.
    pub fn new<O, C>(operation: Wire<O>, actor: ActorRecord, outcome: Wire<C>) -> Self
    where
        O: OperationValues + Vocabulary<Emitter = E>,
        C: OutcomeValues + Vocabulary<Emitter = E>,
    {
        Self {
            operation: operation.into(),
            actor,
            outcome: outcome.into(),
            context: None,
            message: "Audit operation",
            _emitter: std::marker::PhantomData,
        }
    }

    /// Attach the operation's `context` object, an audit type of the same emitter.
    ///
    /// Taken by value like every other part of the record, so a caller can hand over one it
    /// built on the spot; it is serialized here and the record carries the tree, not the type.
    /// Serialized here, which keeps this type free of a second parameter, and only when the
    /// audit trail is on, so a switched-off trail costs nothing here either.
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn context<C: AuditPart<Emitter = E>>(mut self, context: C) -> Self {
        if crate::audit::enabled() {
            self.context = Some(AuditJson::of(&context));
        }
        self
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
            emitters: vec![EmitterRecord::of::<E>()],
            operation: self.operation,
            actor: self.actor,
            outcome: self.outcome,
            context: self.context,
        }
        .emit(self.message);
    }
}

/// An operation record: something the system did that touches identity or access, with no
/// permission decision of its own. Any emitter can produce one.
// Not generic, while the builder is: the schema is derived from this struct, so a type
// parameter here would leave the schema describing a description of the record.
#[audit_part(shape = "operation")]
#[schemars(rename = "OperationRecord")]
#[derive(Debug)]
struct OperationWire {
    /// Every product that contributed to this record, with the version of what each
    /// governs: the one that assembled it and any whose vocabulary it carries. Sorted by
    /// name, always a list however many there are.
    emitters: Vec<EmitterRecord>,
    /// What was done, from the emitter's own vocabulary.
    operation: AnyWireStr,
    /// Who made the request, as authentication established it.
    actor: ActorRecord,
    /// How it ended, from the emitter's own vocabulary.
    outcome: AnyWireStr,
    /// The operation's own detail. One shape per operation kind, each declared by its
    /// emitter; absent for an operation that carries none.
    #[schemars(with = "Option<serde_json::Value>")]
    context: Option<AuditJson>,
}
