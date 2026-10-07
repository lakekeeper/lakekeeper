//! Event payload in, shape out. The only place a wire field gets its value.

use std::collections::BTreeMap;

use super::{
    Decision,
    email::Emails,
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
        UserId,
        authz::{ActionDescriptor, UserOrRoleId},
        events::{
            Authorization, AuthorizationError, AuthorizationFailedEvent,
            AuthorizationFailureReason, AuthorizationSucceededEvent, IdempotentReplayEvent,
            context::{ActionContextKey, ContextEntry, EntityDescriptor, EventEntities},
        },
    },
};

pub(crate) fn authorization_succeeded(
    event: &AuthorizationSucceededEvent,
    emails: &Emails,
) -> AuthorizationRecord {
    authorization(
        &event.request_metadata,
        event.occurred_at,
        &event.actions,
        &event.entities,
        &event.extra_context,
        &event.authorizations,
        Decision::Allowed,
        None,
        emails,
    )
}

pub(crate) fn authorization_failed(
    event: &AuthorizationFailedEvent,
    emails: &Emails,
) -> AuthorizationRecord {
    authorization(
        &event.request_metadata,
        event.occurred_at,
        &event.actions,
        &event.entities,
        &event.extra_context,
        &event.authorizations,
        Decision::Denied,
        Some((&event.failure_reason, &event.error)),
        emails,
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
    emails: &Emails,
) -> AuthorizationRecord {
    AuthorizationRecord {
        emitters: Emitters::of(
            EmitterStamp::of::<crate::Lakekeeper>(),
            contributors(actions, authorizations, extra_context),
        ),
        actions: self::actions(actions, emails),
        entities: self::entities(entities),
        request_id: request_metadata.request_id().clone(),
        time: occurred_at.into(),
        actor: actor(request_metadata, emails),
        privilege_source: request_metadata.privilege_source().as_wire(),
        user_agent: user_agent(request_metadata),
        break_glass: request_metadata.break_glass_reason().map(str::to_owned),
        context: handler_context(extra_context),
        authorizations: decisions(authorizations, emails),
        idempotency_key: idempotency_key(request_metadata),
        decision: decision.as_wire(),
        failure_reason: failure.map(|(reason, _)| reason.as_wire()),
        error: failure.map(|(_, error)| ErrorRecord::from(error)),
    }
}

pub(crate) fn replay(event: &IdempotentReplayEvent, emails: &Emails) -> ReplayRecord {
    ReplayRecord {
        emitters: Emitters::of(
            EmitterStamp::of::<crate::Lakekeeper>(),
            contributors(&event.actions, &[], &BTreeMap::new()),
        ),
        actions: actions(&event.actions, emails),
        entities: entities(&event.entities),
        request_id: event.request_metadata.request_id().clone(),
        time: event.occurred_at.into(),
        actor: actor(&event.request_metadata, emails),
        privilege_source: event.request_metadata.privilege_source().as_wire(),
        user_agent: user_agent(&event.request_metadata),
        idempotency_key: event.idempotency_key.as_uuid().to_string(),
    }
}

/// The request's actor, with its email from the token or, failing that, from `emails`.
pub(crate) fn actor(request_metadata: &RequestMetadata, emails: &Emails) -> ActorRecord {
    let actor = ActorRecord::from_request(request_metadata);
    match request_metadata.user_id() {
        Some(user_id) if actor.lacks_email() => actor.with_email(emails.get(user_id)),
        _ => actor,
    }
}

/// A principal named as a target, with its email from `emails` when it is a user.
pub(crate) fn subject(id: &UserOrRoleId, emails: &Emails) -> SubjectRecord {
    subject_record_with_email(&SubjectRecord::from_id(id), emails)
}

/// `subject` with its email from `emails` when it is a user.
fn subject_record_with_email(subject: &SubjectRecord, emails: &Emails) -> SubjectRecord {
    let mut record = subject.clone();
    if let SubjectRecord::User(user) = &mut record
        && let Ok(user_id) = UserId::try_from(user.user.as_str())
    {
        user.email = emails.get(&user_id);
    }
    record
}

/// Every emitter other than Lakekeeper whose vocabulary this record carries a name from.
///
/// Action names and `context` keys may come from crates Lakekeeper does not know; each carries
/// the emitter that declared it.
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

pub(crate) fn action(descriptor: &ActionDescriptor, emails: &Emails) -> ActionRecord {
    ActionRecord {
        action_name: descriptor.action_name,
        context: descriptor
            .context
            .iter()
            .map(|key| (key.as_str(), context_value(key, emails)))
            .collect(),
    }
}

/// The value of an action's context key, with an email on each user it names.
fn context_value(key: &ActionContextKey, emails: &Emails) -> serde_json::Value {
    match key {
        ActionContextKey::Principals(subjects) => serde_json::to_value(
            subjects
                .iter()
                .map(|subject| subject_record_with_email(subject, emails))
                .collect::<Vec<_>>(),
        )
        .expect("audit types serialize to JSON"),
        ActionContextKey::Principal(subject) => {
            serde_json::to_value(subject_record_with_email(subject, emails))
                .expect("audit types serialize to JSON")
        }
        _ => key.value(),
    }
}

fn actions(descriptors: &[ActionDescriptor], emails: &Emails) -> Vec<ActionRecord> {
    descriptors
        .iter()
        .map(|descriptor| action(descriptor, emails))
        .collect()
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

pub(crate) fn decision(authorization: &Authorization, emails: &Emails) -> DecisionRecord {
    DecisionRecord {
        id: authorization.id.clone(),
        for_principal: authorization
            .for_principal
            .as_ref()
            .map(|id| subject(id, emails)),
        action: action(&authorization.action, emails),
        entity: entity(&authorization.entity),
        allowed: authorization.allowed,
        determined_by: authorization.determined_by.clone(),
    }
}

fn decisions(authorizations: &[Authorization], emails: &Emails) -> Vec<DecisionRecord> {
    authorizations
        .iter()
        .map(|authorization| decision(authorization, emails))
        .collect()
}

/// The users a record names besides its actor, for [`Emails::resolve`]: the subjects of its
/// decisions, and the users in its actions' `principals` and `principal`.
pub(crate) fn named_users(
    actions: &[ActionDescriptor],
    authorizations: &[Authorization],
) -> Vec<UserId> {
    let subjects =
        authorizations
            .iter()
            .filter_map(|authorization| match &authorization.for_principal {
                Some(UserOrRoleId::User(user_id)) => Some(user_id.clone()),
                _ => None,
            });
    let in_actions = actions
        .iter()
        .chain(
            authorizations
                .iter()
                .map(|authorization| &authorization.action),
        )
        .flat_map(|descriptor| &descriptor.context)
        .flat_map(|key| match key {
            ActionContextKey::Principals(principals) => principals.as_slice(),
            ActionContextKey::Principal(principal) => std::slice::from_ref(principal),
            _ => &[],
        })
        .filter_map(|subject| match subject {
            SubjectRecord::User(user) => UserId::try_from(user.user.as_str()).ok(),
            SubjectRecord::Role(_) => None,
        });
    subjects.chain(in_actions).collect()
}

/// The handler-recorded `context`, or `None` when the handler recorded nothing, so the key is
/// omitted, not written as an empty object.
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
