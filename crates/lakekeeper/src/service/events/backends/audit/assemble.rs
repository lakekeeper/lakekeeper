//! Event payload in, shape out. The only place a wire field gets its value.

use std::collections::BTreeMap;

use super::{
    Decision,
    parts::{
        ActionRecord, ActorRecord, DecisionRecord, Emitters, EntityRecord, ErrorRecord,
        HandlerContext, SubjectRecord,
    },
    shapes::{AuthorizationRecord, ReplayRecord},
};
use crate::{
    audit::EmitterStamp,
    request_metadata::{RequestMetadata, UserAgent},
    service::{
        authz::ActionDescriptor,
        events::{
            Authorization, AuthorizationError, AuthorizationFailedEvent,
            AuthorizationFailureReason, AuthorizationSucceededEvent, IdempotentReplayEvent,
            context::{ContextEntry, EntityDescriptor, EventEntities},
        },
    },
};

pub(crate) fn authorization_succeeded(event: &AuthorizationSucceededEvent) -> AuthorizationRecord {
    authorization(
        &event.request_metadata,
        event.occurred_at,
        &event.actions,
        &event.entities,
        &event.extra_context,
        &event.authorizations,
        Decision::Allowed,
        None,
    )
}

pub(crate) fn authorization_failed(event: &AuthorizationFailedEvent) -> AuthorizationRecord {
    authorization(
        &event.request_metadata,
        event.occurred_at,
        &event.actions,
        &event.entities,
        &event.extra_context,
        &event.authorizations,
        Decision::Denied,
        Some((&event.failure_reason, &event.error)),
    )
}

#[allow(clippy::too_many_arguments)]
fn authorization(
    request_metadata: &RequestMetadata,
    occurred_at: chrono::DateTime<chrono::Utc>,
    actions: &[ActionDescriptor],
    entities: &EventEntities,
    extra_context: &BTreeMap<&'static str, ContextEntry>,
    authorizations: &[Authorization],
    decision: Decision,
    failure: Option<(&AuthorizationFailureReason, &AuthorizationError)>,
) -> AuthorizationRecord {
    AuthorizationRecord {
        emitters: Emitters::of(
            EmitterStamp::of::<crate::Lakekeeper>(),
            contributors(actions, authorizations, extra_context),
        ),
        actions: self::actions(actions),
        entities: self::entities(entities),
        request_id: request_metadata.request_id().clone(),
        time: occurred_at.into(),
        actor: ActorRecord::from_request(request_metadata),
        privilege_source: request_metadata.privilege_source().as_wire(),
        user_agent: user_agent(request_metadata),
        break_glass: request_metadata.break_glass_reason().map(str::to_owned),
        context: handler_context(extra_context),
        authorizations: decisions(authorizations),
        idempotency_key: idempotency_key(request_metadata),
        decision: decision.as_wire(),
        failure_reason: failure.map(|(reason, _)| reason.as_wire()),
        error: failure.map(|(_, error)| ErrorRecord::from(error)),
    }
}

pub(crate) fn replay(event: &IdempotentReplayEvent) -> ReplayRecord {
    ReplayRecord {
        emitters: Emitters::of(
            EmitterStamp::of::<crate::Lakekeeper>(),
            contributors(&event.actions, &[], &BTreeMap::new()),
        ),
        actions: actions(&event.actions),
        entities: entities(&event.entities),
        request_id: event.request_metadata.request_id().clone(),
        time: event.occurred_at.into(),
        actor: ActorRecord::from_request(&event.request_metadata),
        privilege_source: event.request_metadata.privilege_source().as_wire(),
        user_agent: user_agent(&event.request_metadata),
        idempotency_key: event.idempotency_key.as_uuid().to_string(),
    }
}

/// Every emitter other than Lakekeeper whose vocabulary this record carries a name from.
///
/// An action name comes from a vocabulary any authorizer crate may declare, and a `context`
/// key from a crate this one does not know. Both carry the emitter that declared them, so the
/// record can name its contributors without Lakekeeper knowing who they are.
fn contributors<'a>(
    actions: &'a [ActionDescriptor],
    authorizations: &'a [Authorization],
    extra_context: &'a BTreeMap<&'static str, ContextEntry>,
) -> impl Iterator<Item = EmitterStamp> + 'a {
    let from_actions = actions
        .iter()
        .chain(authorizations.iter().map(|a| &a.action))
        .map(|descriptor| descriptor.action_name.emitter());
    let from_context = extra_context.values().map(|entry| entry.emitter);
    from_actions.chain(from_context)
}

/// The `User-Agent` header, verbatim and unverified, or `None` when the caller sent none.
fn user_agent(request_metadata: &RequestMetadata) -> Option<String> {
    request_metadata
        .user_agent()
        .map(UserAgent::as_str)
        .map(str::to_owned)
}

/// The request's `Idempotency-Key`, or `None` when the caller sent none.
fn idempotency_key(request_metadata: &RequestMetadata) -> Option<String> {
    request_metadata
        .idempotency_key()
        .map(|key| key.as_uuid().to_string())
}

pub(crate) fn action(descriptor: &ActionDescriptor) -> ActionRecord {
    ActionRecord {
        action_name: descriptor.action_name,
        context: descriptor
            .context
            .iter()
            .map(|key| (key.as_str(), key.value()))
            .collect(),
    }
}

fn actions(descriptors: &[ActionDescriptor]) -> Vec<ActionRecord> {
    descriptors.iter().map(action).collect()
}

pub(crate) fn entity(descriptor: &EntityDescriptor) -> EntityRecord {
    EntityRecord {
        entity_type: descriptor.entity_type.as_wire(),
        fields: descriptor
            .fields
            .iter()
            .map(|field| (field.key.as_str(), field.value.clone()))
            .collect(),
    }
}

fn entities(entities: &EventEntities) -> Vec<EntityRecord> {
    entities.entities.iter().map(entity).collect()
}

pub(crate) fn decision(authorization: &Authorization) -> DecisionRecord {
    DecisionRecord {
        id: authorization.id.clone(),
        for_principal: authorization
            .for_principal
            .as_ref()
            .map(SubjectRecord::from_id),
        action: action(&authorization.action),
        entity: entity(&authorization.entity),
        allowed: authorization.allowed,
        determined_by: authorization.determined_by.clone(),
    }
}

fn decisions(authorizations: &[Authorization]) -> Vec<DecisionRecord> {
    authorizations.iter().map(decision).collect()
}

/// The handler-recorded `context`, or `None` when the handler recorded nothing, so the key is
/// absent rather than an empty object.
fn handler_context(extra_context: &BTreeMap<&'static str, ContextEntry>) -> Option<HandlerContext> {
    if extra_context.is_empty() {
        None
    } else {
        Some(HandlerContext(
            extra_context
                .iter()
                .map(|(key, entry)| (*key, entry.value.clone()))
                .collect(),
        ))
    }
}
