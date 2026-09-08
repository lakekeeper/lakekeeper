use std::sync::Arc;

use crate::{
    api::RequestMetadata,
    service::{
        authz::ActionDescriptor, events::context::EventEntities, idempotency::IdempotencyKey,
    },
};

/// Event emitted when a request carrying an `Idempotency-Key` was answered from
/// a stored record instead of being executed.
///
/// Emitted by the drop and rename handlers, which answer the replay *above* the
/// authorization banner, and deliberately so: the mutation already happened, so
/// re-deciding it could only turn a completed operation into a reported
/// failure. Without this event the retry left no audit record at all — request
/// volume is still counted by the statistics middleware and the request span
/// still carries the method and URI, but nothing attributed the retry to a
/// principal.
///
/// Most other idempotent endpoints re-derive their response body by loading it,
/// so their replays emit the *load's* authorization record rather than one for
/// the original action; `updateTable` emits both, and `commitTransaction`
/// authorizes before it detects the replay and re-derives nothing.
///
/// Field shapes match the authorization events, so a retry reads with the same
/// queries as the request that did the work. It describes the *request*, not the
/// outcome — see the field docs.
#[derive(Clone, Debug)]
pub struct IdempotentReplayEvent {
    /// Request metadata of the *retry*, not of the request that did the work.
    pub request_metadata: Arc<RequestMetadata>,

    /// The entities this request named, as the caller spelled them. A replay
    /// resolves nothing, so at every current call site this is an identifier
    /// rather than an id — and it need not be the entity the original request
    /// acted on, because a record is matched on warehouse, key and endpoint
    /// alone.
    pub entities: Arc<EventEntities>,

    /// The action this request asked for. Its context flags (`force`, `purge`,
    /// `recursive`) are the retry's, for the same reason: the parameters are
    /// not part of the match, so a retry may report flags the original never
    /// carried.
    pub actions: Arc<Vec<ActionDescriptor>>,

    /// The key whose record served the request.
    pub idempotency_key: IdempotencyKey,
}
