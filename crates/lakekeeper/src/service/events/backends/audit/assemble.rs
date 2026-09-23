//! Event payload in, shape out. The only place a wire field gets its value.

use std::collections::{BTreeMap, HashMap};

use super::{
    Decision,
    parts::{
        ActionRecord, ActorRecord, DecisionRecord, EntityRecord, ErrorRecord, HandlerContext,
        SubjectRecord,
    },
    shapes::{AuthorizationRecord, ReplayRecord},
};
use crate::{
    request_metadata::{RequestMetadata, UserAgent},
    service::{
        authz::ActionDescriptor,
        events::{
            Authorization, AuthorizationError, AuthorizationFailedEvent,
            AuthorizationFailureReason, AuthorizationSucceededEvent, IdempotentReplayEvent,
            context::{EntityDescriptor, EventEntities},
        },
    },
};

pub(crate) fn authorization_succeeded(event: &AuthorizationSucceededEvent) -> AuthorizationRecord {
    authorization(
        &event.request_metadata,
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
        &event.actions,
        &event.entities,
        &event.extra_context,
        &event.authorizations,
        Decision::Denied,
        Some((&event.failure_reason, &event.error)),
    )
}

fn authorization(
    request_metadata: &RequestMetadata,
    actions: &[ActionDescriptor],
    entities: &EventEntities,
    extra_context: &HashMap<String, String>,
    authorizations: &[Authorization],
    decision: Decision,
    failure: Option<(&AuthorizationFailureReason, &AuthorizationError)>,
) -> AuthorizationRecord {
    AuthorizationRecord {
        actions: self::actions(actions),
        entities: self::entities(entities),
        actor: ActorRecord::from_request(request_metadata),
        privilege_source: request_metadata.privilege_source(),
        user_agent: user_agent(request_metadata),
        break_glass: request_metadata.break_glass_reason().map(str::to_owned),
        context: handler_context(extra_context),
        authorizations: decisions(authorizations),
        idempotency_key: idempotency_key(request_metadata),
        decision,
        failure_reason: failure.map(|(reason, _)| reason.as_wire()),
        error: failure.map(|(_, error)| ErrorRecord::from(error)),
    }
}

pub(crate) fn replay(event: &IdempotentReplayEvent) -> ReplayRecord {
    ReplayRecord {
        actions: actions(&event.actions),
        entities: entities(&event.entities),
        actor: ActorRecord::from_request(&event.request_metadata),
        privilege_source: event.request_metadata.privilege_source(),
        user_agent: user_agent(&event.request_metadata),
        idempotency_key: event.idempotency_key.as_uuid().to_string(),
    }
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
            .map(|(key, value)| (key.as_str().to_string(), value.clone()))
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
            .map(|field| (field.key.as_str().to_string(), field.value.clone()))
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
fn handler_context(extra_context: &HashMap<String, String>) -> Option<HandlerContext> {
    if extra_context.is_empty() {
        None
    } else {
        Some(HandlerContext(
            extra_context
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<BTreeMap<_, _>>(),
        ))
    }
}
