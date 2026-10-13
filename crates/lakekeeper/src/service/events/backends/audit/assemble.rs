//! Event payload in, shape out. The only place a wire field gets its value.

use std::{collections::BTreeMap, sync::Arc};

use super::{
    Decision,
    enrichment::Enrichment,
    parts::{
        ActionRecord, ActorRecord, DecisionRecord, Emitters, EntityFields, EntityRecord,
        ErrorRecord, HandlerContext, SubjectRecord, include_role_source_id,
    },
    shapes::{AuthorizationRecord, ReplayRecord},
};
use crate::{
    audit::EmitterStamp,
    request_metadata::{RequestMetadata, UserAgent},
    service::{
        RoleId, UserId,
        authz::{ActionDescriptor, UserOrRoleId},
        events::{
            Authorization, AuthorizationError, AuthorizationFailedEvent,
            AuthorizationFailureReason, AuthorizationSucceededEvent, IdempotentReplayEvent,
            context::{ActionContextKey, ContextEntry, EntityDescriptor, EventEntities},
        },
        idempotency::IdempotencyKey,
    },
};

pub(crate) fn authorization_succeeded<'a>(
    event: &'a AuthorizationSucceededEvent,
    enrichment: &Enrichment<'_>,
) -> AuthorizationRecord<'a> {
    authorization(
        &event.request_metadata,
        event.occurred_at,
        &event.actions,
        &event.entities,
        &event.extra_context,
        &event.authorizations,
        Decision::Allowed,
        None,
        enrichment,
    )
}

pub(crate) fn authorization_failed<'a>(
    event: &'a AuthorizationFailedEvent,
    enrichment: &Enrichment<'_>,
) -> AuthorizationRecord<'a> {
    authorization(
        &event.request_metadata,
        event.occurred_at,
        &event.actions,
        &event.entities,
        &event.extra_context,
        &event.authorizations,
        Decision::Denied,
        Some((&event.failure_reason, &event.error)),
        enrichment,
    )
}

#[allow(clippy::too_many_arguments)]
fn authorization<'a>(
    request_metadata: &'a RequestMetadata,
    occurred_at: chrono::DateTime<chrono::Utc>,
    actions: &[ActionDescriptor],
    entities: &'a EventEntities,
    extra_context: &'a BTreeMap<&'static str, ContextEntry>,
    authorizations: &'a [Authorization],
    decision: Decision,
    failure: Option<(&AuthorizationFailureReason, &'a AuthorizationError)>,
    enrichment: &Enrichment<'_>,
) -> AuthorizationRecord<'a> {
    AuthorizationRecord {
        emitters: Emitters::of(
            EmitterStamp::of::<crate::Lakekeeper>(),
            contributors(actions, authorizations, extra_context),
        ),
        actions: self::actions(actions, enrichment),
        entities: self::entities(entities),
        request_id: request_metadata.request_id().clone(),
        time: occurred_at.into(),
        actor: actor(request_metadata, enrichment),
        privilege_source: request_metadata.privilege_source().as_wire(),
        user_agent: user_agent(request_metadata),
        break_glass: request_metadata.break_glass_reason(),
        context: handler_context(extra_context),
        authorizations: decisions(authorizations, enrichment),
        idempotency_key: idempotency_key(request_metadata),
        decision: decision.as_wire(),
        failure_reason: failure.map(|(reason, _)| reason.as_wire()),
        error: failure.map(|(_, error)| ErrorRecord::from(error)),
    }
}

pub(crate) fn replay<'a>(
    event: &'a IdempotentReplayEvent,
    enrichment: &Enrichment<'_>,
) -> ReplayRecord<'a> {
    ReplayRecord {
        emitters: Emitters::of(
            EmitterStamp::of::<crate::Lakekeeper>(),
            contributors(&event.actions, &[], &BTreeMap::new()),
        ),
        actions: actions(&event.actions, enrichment),
        entities: entities(&event.entities),
        request_id: event.request_metadata.request_id().clone(),
        time: event.occurred_at.into(),
        actor: actor(&event.request_metadata, enrichment),
        privilege_source: event.request_metadata.privilege_source().as_wire(),
        user_agent: user_agent(&event.request_metadata),
        idempotency_key: event.idempotency_key.as_uuid(),
    }
}

/// The request's actor, with its email from the token or, failing that, from `enrichment`.
pub(crate) fn actor(
    request_metadata: &RequestMetadata,
    enrichment: &Enrichment<'_>,
) -> ActorRecord {
    let actor = ActorRecord::from_request(request_metadata);
    match request_metadata.user_id() {
        Some(user_id) if actor.lacks_email() => {
            actor.with_email(enrichment.email(user_id).cloned())
        }
        _ => actor,
    }
}

/// A principal named as a target, with what `enrichment` knows about it.
pub(crate) fn subject<'a>(id: &'a UserOrRoleId, enrichment: &Enrichment<'_>) -> SubjectRecord<'a> {
    enriched_subject(SubjectRecord::of(id), enrichment)
}

/// `subject` with what `enrichment` knows about it: a user's email, a role's provider and
/// source.
fn enriched_subject<'a>(
    mut record: SubjectRecord<'a>,
    enrichment: &Enrichment<'_>,
) -> SubjectRecord<'a> {
    match &mut record {
        SubjectRecord::User(user) => {
            user.email = enrichment.email(&user.user).cloned();
        }
        SubjectRecord::Role(role) => {
            if let Some(ident) = enrichment.role(&role.role) {
                role.provider_id = Some(Arc::clone(ident));
                role.source_id = include_role_source_id().then(|| Arc::clone(ident));
            }
        }
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
fn user_agent(request_metadata: &RequestMetadata) -> Option<&str> {
    request_metadata.user_agent().map(UserAgent::as_str)
}

/// The request's `Idempotency-Key`, or `None` when the caller sent none.
fn idempotency_key(request_metadata: &RequestMetadata) -> Option<uuid::Uuid> {
    request_metadata
        .idempotency_key()
        .map(IdempotencyKey::as_uuid)
}

pub(crate) fn action(descriptor: &ActionDescriptor, enrichment: &Enrichment<'_>) -> ActionRecord {
    ActionRecord {
        action_name: descriptor.action_name,
        context: descriptor
            .context
            .iter()
            .map(|key| (key.as_str(), context_value(key, enrichment)))
            .collect(),
    }
}

/// The value of an action's context key, with an email on each user it names.
fn context_value(key: &ActionContextKey, enrichment: &Enrichment<'_>) -> serde_json::Value {
    match key {
        ActionContextKey::Principals(subjects) => serde_json::to_value(
            subjects
                .iter()
                .map(|subject| enriched_subject(subject.reborrow(), enrichment))
                .collect::<Vec<_>>(),
        )
        .expect("audit types serialize to JSON"),
        ActionContextKey::Principal(subject) => {
            serde_json::to_value(enriched_subject(subject.reborrow(), enrichment))
                .expect("audit types serialize to JSON")
        }
        _ => key.value(),
    }
}

fn actions(descriptors: &[ActionDescriptor], enrichment: &Enrichment<'_>) -> Vec<ActionRecord> {
    descriptors
        .iter()
        .map(|descriptor| action(descriptor, enrichment))
        .collect()
}

pub(crate) fn entity(descriptor: &EntityDescriptor) -> EntityRecord<'_> {
    EntityRecord {
        entity_type: descriptor.entity_type.as_wire(),
        fields: EntityFields(&descriptor.fields),
    }
}

fn entities(entities: &EventEntities) -> Vec<EntityRecord<'_>> {
    entities.entities.iter().map(entity).collect()
}

pub(crate) fn decision<'a>(
    authorization: &'a Authorization,
    enrichment: &Enrichment<'_>,
) -> DecisionRecord<'a> {
    DecisionRecord {
        id: authorization.id.as_deref(),
        for_principal: authorization
            .for_principal
            .as_ref()
            .map(|id| subject(id, enrichment)),
        action: action(&authorization.action, enrichment),
        entity: entity(&authorization.entity),
        allowed: authorization.allowed,
        determined_by: &authorization.determined_by,
    }
}

fn decisions<'a>(
    authorizations: &'a [Authorization],
    enrichment: &Enrichment<'_>,
) -> Vec<DecisionRecord<'a>> {
    authorizations
        .iter()
        .map(|authorization| decision(authorization, enrichment))
        .collect()
}

/// The principals a record names besides its actor, for [`Enrichment::resolve`].
#[derive(Debug, Default)]
pub(crate) struct NamedPrincipals<'a> {
    pub(crate) users: Vec<&'a UserId>,
    pub(crate) roles: Vec<RoleId>,
}

impl<'a> NamedPrincipals<'a> {
    fn add(&mut self, principal: &'a UserOrRoleId) {
        match principal {
            UserOrRoleId::User(user_id) => self.users.push(user_id),
            UserOrRoleId::Role(role_id) => self.roles.push(*role_id),
        }
    }

    fn add_record(&mut self, subject: &'a SubjectRecord<'_>) {
        match subject {
            SubjectRecord::User(user) => self.users.push(&user.user),
            SubjectRecord::Role(role) => self.roles.push(role.role),
        }
    }

    /// The principals of grants: their recipients.
    pub(crate) fn of_grants(principals: impl IntoIterator<Item = &'a UserOrRoleId>) -> Self {
        let mut named = Self::default();
        for principal in principals {
            named.add(principal);
        }
        named
    }

    /// The principals of a record's decisions and actions: the subjects of its decisions,
    /// and the principals in its actions' `principals` and `principal`.
    pub(crate) fn of(actions: &'a [ActionDescriptor], authorizations: &'a [Authorization]) -> Self {
        let mut named = Self::default();
        for subject in authorizations
            .iter()
            .filter_map(|authorization| authorization.for_principal.as_ref())
        {
            named.add(subject);
        }
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
            });
        for subject in in_actions {
            named.add_record(subject);
        }
        named
    }
}

/// The handler-recorded `context`, or `None` when the handler recorded nothing, so the key is
/// omitted, not written as an empty object.
fn handler_context<'a>(
    extra_context: &'a BTreeMap<&'static str, ContextEntry>,
) -> Option<HandlerContext<'a>> {
    if extra_context.is_empty() {
        None
    } else {
        Some(HandlerContext(
            extra_context
                .iter()
                .map(|(key, entry)| (*key, &entry.value))
                .collect(),
        ))
    }
}
