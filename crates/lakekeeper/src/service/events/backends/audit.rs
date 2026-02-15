use std::fmt::Display;

use valuable::{Mappable, Valuable, Value, Visit};

use crate::service::{
    authn::{Actor, InternalActor},
    authz::{ActionDescriptor, ContextValue},
    events::{
        AuthorizationFailedEvent, AuthorizationSucceededEvent, EventListener,
        context::EntityDescriptor,
    },
};

/// Emits an audit `tracing::info!` event, using singular field names (`action`/`entity`)
/// when only one item is present, and plural (`actions`/`entities`) otherwise.
macro_rules! audit_log {
    ($actions:expr, $entities:expr, { $($common:tt)* }, $msg:literal) => {{
        let __actions = $actions;
        let __entities = $entities;
        match (__actions.len() == 1, __entities.entities.len() == 1) {
            (true, true) => tracing::info!(
                event_source = "audit",
                action = tracing::field::valuable(&__actions[0].as_value()),
                entity = tracing::field::valuable(&__entities.entities[0].as_value()),
                $($common)*
                $msg
            ),
            (true, false) => tracing::info!(
                event_source = "audit",
                action = tracing::field::valuable(&__actions[0].as_value()),
                entities = tracing::field::valuable(&__entities.as_value()),
                $($common)*
                $msg
            ),
            (false, true) => tracing::info!(
                event_source = "audit",
                actions = tracing::field::valuable(&__actions.as_value()),
                entity = tracing::field::valuable(&__entities.entities[0].as_value()),
                $($common)*
                $msg
            ),
            (false, false) => tracing::info!(
                event_source = "audit",
                actions = tracing::field::valuable(&__actions.as_value()),
                entities = tracing::field::valuable(&__entities.as_value()),
                $($common)*
                $msg
            ),
        }
    }};
}

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
        if event.extra_context.is_empty() {
            audit_log!(
                &*event.actions,
                &*event.entities,
                {
                    actor = tracing::field::valuable(&event.request_metadata.internal_actor().as_value()),
                    failure_reason = tracing::field::valuable(&event.failure_reason.as_value()),
                    error = tracing::field::valuable(&event.error.as_value()),
                    decision = "denied",
                },
                "Authorization failed event"
            );
        } else {
            audit_log!(
                &*event.actions,
                &*event.entities,
                {
                    actor = tracing::field::valuable(&event.request_metadata.internal_actor().as_value()),
                    failure_reason = tracing::field::valuable(&event.failure_reason.as_value()),
                    error = tracing::field::valuable(&event.error.as_value()),
                    context = tracing::field::valuable(&event.extra_context.as_value()),
                    decision = "denied",
                },
                "Authorization failed event"
            );
        }
        Ok(())
    }

    async fn authorization_succeeded(
        &self,
        event: AuthorizationSucceededEvent,
    ) -> anyhow::Result<()> {
        if event.extra_context.is_empty() {
            audit_log!(
                &*event.actions,
                &*event.entities,
                {
                    actor = tracing::field::valuable(&event.request_metadata.internal_actor().as_value()),
                    decision = "allowed",
                },
                "Authorization succeeded event"
            );
        } else {
            audit_log!(
                &*event.actions,
                &*event.entities,
                {
                    actor = tracing::field::valuable(&event.request_metadata.internal_actor().as_value()),
                    context = tracing::field::valuable(&event.extra_context.as_value()),
                    decision = "allowed",
                },
                "Authorization succeeded event"
            );
        }
        Ok(())
    }
}

impl Valuable for EntityDescriptor {
    fn as_value(&self) -> Value<'_> {
        Value::Mappable(self)
    }

    fn visit(&self, visit: &mut dyn Visit) {
        visit.visit_entry(
            Value::String("entity_type"),
            Value::String(self.entity_type),
        );
        for field in &self.fields {
            visit.visit_entry(Value::String(field.key), Value::String(&field.value));
        }
    }
}

impl Mappable for EntityDescriptor {
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.fields.len() + 1;
        (len, Some(len))
    }
}

impl Valuable for ActionDescriptor {
    fn as_value(&self) -> Value<'_> {
        Value::Mappable(self)
    }

    fn visit(&self, visit: &mut dyn Visit) {
        visit.visit_entry(
            Value::String("action_name"),
            Value::String(self.action_name),
        );
        for (key, value) in &self.context {
            visit.visit_entry(Value::String(key), value.as_value());
        }
    }
}

impl Mappable for ActionDescriptor {
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = 1 + self.context.len();
        (len, Some(len))
    }
}

impl Valuable for ContextValue {
    fn as_value(&self) -> Value<'_> {
        match self {
            Self::Map(map) => map.as_value(),
            Self::List(list) => list.as_value(),
        }
    }

    fn visit(&self, visit: &mut dyn Visit) {
        match self {
            Self::Map(map) => map.visit(visit),
            Self::List(list) => list.visit(visit),
        }
    }
}

impl Valuable for Actor {
    fn as_value(&self) -> Value<'_> {
        Value::Mappable(self)
    }

    fn visit(&self, visit: &mut dyn Visit) {
        match self {
            Actor::Anonymous => {
                visit.visit_entry(Value::String("actor_type"), Value::String("anonymous"));
            }
            Actor::Principal(user_id) => {
                let user_id = user_id.to_string();
                visit.visit_entry(Value::String("actor_type"), Value::String("principal"));
                visit.visit_entry(Value::String("principal"), Value::String(&user_id));
            }
            Actor::Role {
                principal,
                assumed_role,
            } => {
                let principal = principal.to_string();
                let assumed_role = assumed_role.to_string();
                visit.visit_entry(Value::String("actor_type"), Value::String("assumed-role"));
                visit.visit_entry(Value::String("principal"), Value::String(&principal));
                visit.visit_entry(Value::String("assumed_role"), Value::String(&assumed_role));
            }
        }
    }
}

impl Mappable for Actor {
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = match self {
            Actor::Anonymous => 1,
            Actor::Principal(_) => 2,
            Actor::Role { .. } => 3,
        };
        (len, Some(len))
    }
}

impl Valuable for InternalActor {
    fn as_value(&self) -> Value<'_> {
        Value::Mappable(self)
    }

    fn visit(&self, visit: &mut dyn Visit) {
        match self {
            InternalActor::LakekeeperInternal => {
                visit.visit_entry(
                    Value::String("actor_type"),
                    Value::String("lakekeeper-internal"),
                );
            }
            InternalActor::External(actor) => actor.visit(visit),
        }
    }
}

impl Mappable for InternalActor {
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            InternalActor::LakekeeperInternal => (1, Some(1)),
            InternalActor::External(actor) => actor.size_hint(),
        }
    }
}
