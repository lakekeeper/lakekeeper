use std::sync::Arc;

use crate::{
    audit::{ActorRecord, AnyWireStr, AuditJson, Emitters, RecordTime},
    request_metadata::RequestId,
};

/// Event emitted for an operation record: something the system did that touches identity or
/// access, with no permission decision of its own, such as an admission rejection.
///
/// Built only from an [`OperationRecord`](crate::audit::OperationRecord), whose emitter and
/// vocabulary checks the record has already passed. Cloning it is cheap: the record sits
/// behind an [`ArcOperationRecordData`], so every listener shares one copy.
#[derive(Clone, Debug)]
pub struct OperationRecordedEvent {
    /// The record, shared by every listener that receives this event.
    pub record: ArcOperationRecordData,
}

/// The parts of an operation record, with the emitter and vocabulary types erased.
///
/// Non-exhaustive, so a record is built through
/// [`OperationRecord`](crate::audit::OperationRecord) and its checks, never by hand.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct OperationRecordData {
    /// Every product that contributed to the record: the emitter that built it.
    pub emitters: Emitters,
    /// The request the record belongs to. `None` for an operation no request triggered.
    pub request_id: Option<RequestId>,
    /// When the operation happened, in UTC.
    pub time: RecordTime,
    /// What was done, from the emitter's own vocabulary.
    pub operation: AnyWireStr,
    /// Who made the request, or on whose behalf the operation ran.
    pub actor: ActorRecord,
    /// How it ended, from the emitter's own vocabulary.
    pub outcome: AnyWireStr,
    /// The operation's own detail, serialized. `None` for an operation that carries none.
    pub context: Option<AuditJson>,
    /// The human-readable message of the log line.
    pub message: &'static str,
}

/// Reference to [`OperationRecordData`] that can be cheaply cloned and shared.
pub type ArcOperationRecordData = Arc<OperationRecordData>;
