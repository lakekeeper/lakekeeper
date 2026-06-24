use std::fmt::Display;

use valuable::{Listable, Mappable, Valuable, Value, Visit};

use crate::service::{
    authn::{Actor, InternalActor},
    authz::{ActionDescriptor, ContextValue, DeterminingFactor, UserOrRoleId},
    events::{
        Authorization, AuthorizationFailedEvent, AuthorizationSucceededEvent, EventListener,
        context::EntityDescriptor,
    },
};

/// Newtype around `Vec<Authorization>` so we can implement `Valuable` /
/// `Listable` for it without an orphan-rule violation. Borrowed because the
/// audit emit path holds the Vec via `Arc`.
struct AuthorizationsList<'a>(&'a [Authorization]);

impl Valuable for AuthorizationsList<'_> {
    fn as_value(&self) -> Value<'_> {
        Value::Listable(self)
    }

    fn visit(&self, visit: &mut dyn Visit) {
        for entry in self.0 {
            visit.visit_value(entry.as_value());
        }
    }
}

impl Listable for AuthorizationsList<'_> {
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.0.len(), Some(self.0.len()))
    }
}

impl Valuable for Authorization {
    fn as_value(&self) -> Value<'_> {
        Value::Mappable(self)
    }

    fn visit(&self, visit: &mut dyn Visit) {
        if let Some(id) = &self.id {
            visit.visit_entry(Value::String("id"), Value::String(id));
        }
        if let Some(principal) = &self.for_principal {
            let wrapped = UserOrRoleIdValue(principal);
            visit.visit_entry(Value::String("for-principal"), wrapped.as_value());
        }
        visit.visit_entry(Value::String("action"), self.action.as_value());
        visit.visit_entry(Value::String("entity"), self.entity.as_value());
        if let Some(allowed) = self.allowed {
            visit.visit_entry(Value::String("allowed"), Value::Bool(allowed));
        }
        if !self.determined_by.is_empty() {
            let determined_by = DeterminingFactorsList(&self.determined_by);
            visit.visit_entry(Value::String("determined_by"), determined_by.as_value());
        }
    }
}

impl Mappable for Authorization {
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = 2
            + usize::from(self.id.is_some())
            + usize::from(self.for_principal.is_some())
            + usize::from(self.allowed.is_some())
            + usize::from(!self.determined_by.is_empty());
        (len, Some(len))
    }
}

/// Newtype around `[DeterminingFactor]` so we can implement `Valuable` /
/// `Listable` for it without an orphan-rule violation, mirroring
/// [`AuthorizationsList`].
struct DeterminingFactorsList<'a>(&'a [DeterminingFactor]);

impl Valuable for DeterminingFactorsList<'_> {
    fn as_value(&self) -> Value<'_> {
        Value::Listable(self)
    }

    fn visit(&self, visit: &mut dyn Visit) {
        for entry in self.0 {
            visit.visit_value(entry.as_value());
        }
    }
}

impl Listable for DeterminingFactorsList<'_> {
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.0.len(), Some(self.0.len()))
    }
}

/// Render `UserOrRoleId` as a single-key map (`{"user": "..."}` or
/// `{"role": "..."}`) for the `for-principal` field of an `Authorization`.
struct UserOrRoleIdValue<'a>(&'a UserOrRoleId);

impl Valuable for UserOrRoleIdValue<'_> {
    fn as_value(&self) -> Value<'_> {
        Value::Mappable(self)
    }

    fn visit(&self, visit: &mut dyn Visit) {
        match self.0 {
            UserOrRoleId::User(id) => {
                let s = id.to_string();
                visit.visit_entry(Value::String("user"), Value::String(&s));
            }
            UserOrRoleId::Role(id) => {
                let s = id.to_string();
                visit.visit_entry(Value::String("role"), Value::String(&s));
            }
        }
    }
}

impl Mappable for UserOrRoleIdValue<'_> {
    fn size_hint(&self) -> (usize, Option<usize>) {
        (1, Some(1))
    }
}

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
        let authorizations = AuthorizationsList(&event.authorizations);
        if event.extra_context.is_empty() {
            audit_log!(
                &*event.actions,
                &*event.entities,
                {
                    actor = tracing::field::valuable(&event.request_metadata.internal_actor().as_value()),
                    privilege_source = event.request_metadata.privilege_source().as_str(),
                    failure_reason = tracing::field::valuable(&event.failure_reason.as_value()),
                    error = tracing::field::valuable(&event.error.as_value()),
                    authorizations = tracing::field::valuable(&authorizations.as_value()),
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
                    privilege_source = event.request_metadata.privilege_source().as_str(),
                    failure_reason = tracing::field::valuable(&event.failure_reason.as_value()),
                    error = tracing::field::valuable(&event.error.as_value()),
                    context = tracing::field::valuable(&event.extra_context.as_value()),
                    authorizations = tracing::field::valuable(&authorizations.as_value()),
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
        let authorizations = AuthorizationsList(&event.authorizations);
        if event.extra_context.is_empty() {
            audit_log!(
                &*event.actions,
                &*event.entities,
                {
                    actor = tracing::field::valuable(&event.request_metadata.internal_actor().as_value()),
                    privilege_source = event.request_metadata.privilege_source().as_str(),
                    authorizations = tracing::field::valuable(&authorizations.as_value()),
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
                    privilege_source = event.request_metadata.privilege_source().as_str(),
                    context = tracing::field::valuable(&event.extra_context.as_value()),
                    authorizations = tracing::field::valuable(&authorizations.as_value()),
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
            Self::String(s) => Value::String(s),
        }
    }

    fn visit(&self, visit: &mut dyn Visit) {
        match self {
            Self::Map(map) => map.visit(visit),
            Self::List(list) => list.visit(visit),
            Self::String(s) => s.visit(visit),
        }
    }
}

#[allow(clippy::struct_field_names)]
struct AssumedRoleValue {
    role_id: String,
    provider_id: String,
    source_id: String,
}

impl Valuable for AssumedRoleValue {
    fn as_value(&self) -> Value<'_> {
        Value::Mappable(self)
    }

    fn visit(&self, visit: &mut dyn Visit) {
        visit.visit_entry(Value::String("role_id"), Value::String(&self.role_id));
        visit.visit_entry(
            Value::String("provider_id"),
            Value::String(&self.provider_id),
        );
        visit.visit_entry(Value::String("source_id"), Value::String(&self.source_id));
    }
}

impl Mappable for AssumedRoleValue {
    fn size_hint(&self) -> (usize, Option<usize>) {
        (3, Some(3))
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
                let role_value = AssumedRoleValue {
                    role_id: assumed_role.id.to_string(),
                    provider_id: assumed_role.provider_id().to_string(),
                    source_id: assumed_role.source_id().to_string(),
                };
                visit.visit_entry(Value::String("actor_type"), Value::String("assumed-role"));
                visit.visit_entry(Value::String("principal"), Value::String(&principal));
                visit.visit_entry(Value::String("assumed_role"), role_value.as_value());
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

// ============================================================================
// Operational audit helpers
// ============================================================================

/// Borrowed actor value for **operational** audit events.
///
/// Produces the same JSON shape as [`Actor::Principal`]:
/// ```json
/// {"actor_type": "principal", "principal": "oidc~user@example.com"}
/// ```
/// but without requiring an owned `Arc<UserId>`.
///
/// Use this with [`audit_operation!`] for non-authz events that contain user
/// identity (PII), such as role resolution, token introspection, etc.
#[derive(Debug)]
pub struct AuditPrincipal<'a>(pub &'a crate::service::authn::UserId);

impl Valuable for AuditPrincipal<'_> {
    fn as_value(&self) -> Value<'_> {
        Value::Mappable(self)
    }

    fn visit(&self, visit: &mut dyn Visit) {
        visit.visit_entry(Value::String("actor_type"), Value::String("principal"));
        let principal = self.0.to_string();
        visit.visit_entry(Value::String("principal"), Value::String(&principal));
    }
}

impl Mappable for AuditPrincipal<'_> {
    fn size_hint(&self) -> (usize, Option<usize>) {
        (2, Some(2))
    }
}

/// Emit an audit `tracing::info!` event for a **non-authz** operation that
/// touches user identity (PII).
///
/// Enforces the operational audit schema:
/// ```json
/// {
///   "event_source": "audit",
///   "operation":    "<operation name>",
///   "actor":        { "actor_type": "principal", "principal": "oidc~…" },
///   "outcome":      "<outcome>",
///   "context":      { … }   // optional
/// }
/// ```
///
/// This is the counterpart to the authz-focused `audit_log!` macro. Use it
/// whenever there is no `decision = "allowed"|"denied"` to emit — e.g. for
/// role resolution, user lookup, or token enrichment.
///
/// # Examples
/// ```rust,ignore
/// use lakekeeper::audit_operation;
/// use lakekeeper::service::events::backends::audit::AuditPrincipal;
///
/// // Without context
/// audit_operation!(
///     operation = "ldap_resolve_roles",
///     actor     = AuditPrincipal(user_id),
///     outcome   = "success",
///     "LDAP role resolution complete"
/// );
///
/// // With context (any type implementing `Valuable`)
/// #[derive(valuable::Valuable)]
/// struct Ctx<'a> { provider_id: &'a str, role_count: usize }
///
/// audit_operation!(
///     operation = "ldap_resolve_roles",
///     actor     = AuditPrincipal(user_id),
///     outcome   = "success",
///     context   = Ctx { provider_id: "ldap", role_count: 3 },
///     "LDAP role resolution complete"
/// );
/// ```
#[macro_export]
macro_rules! audit_operation {
    (
        operation = $op:expr,
        actor     = $actor:expr,
        outcome   = $outcome:expr,
        $msg:literal $(,)?
    ) => {
        $crate::tracing::info!(
            event_source = "audit",
            operation = $op,
            actor = $crate::tracing::field::valuable(&$actor),
            outcome = $outcome,
            $msg
        )
    };
    (
        operation = $op:expr,
        actor     = $actor:expr,
        outcome   = $outcome:expr,
        context   = $ctx:expr,
        $msg:literal $(,)?
    ) => {
        $crate::tracing::info!(
            event_source = "audit",
            operation = $op,
            actor = $crate::tracing::field::valuable(&$actor),
            outcome = $outcome,
            context = $crate::tracing::field::valuable(&$ctx),
            $msg
        )
    };
}

#[cfg(test)]
mod tests {
    use valuable::{Valuable, Value, Visit};

    use super::*;
    use crate::service::authz::{ActionDescriptor, DeterminingFactor, PolicyEffect};

    /// Records the top-level map keys an `Authorization` emits when visited.
    #[derive(Default)]
    struct KeyCollector {
        keys: Vec<String>,
    }

    impl Visit for KeyCollector {
        fn visit_value(&mut self, _value: Value<'_>) {}
        fn visit_entry(&mut self, key: Value<'_>, _value: Value<'_>) {
            if let Value::String(k) = key {
                self.keys.push(k.to_string());
            }
        }
    }

    fn sample(determined_by: Vec<DeterminingFactor>) -> Authorization {
        Authorization {
            id: None,
            for_principal: None,
            action: ActionDescriptor {
                action_name: "read",
                context: Vec::new(),
            },
            entity: EntityDescriptor::new("table"),
            allowed: Some(true),
            determined_by,
        }
    }

    #[test]
    fn determined_by_emitted_when_present() {
        let auth = sample(vec![DeterminingFactor::Policy {
            policy_id: "policy0".to_string(),
            name: Some("allow-read".to_string()),
            effect: PolicyEffect::Permit,
            source: None,
        }]);
        let mut collector = KeyCollector::default();
        auth.visit(&mut collector);
        assert_eq!(
            collector.keys,
            vec!["action", "entity", "allowed", "determined_by"],
        );
        assert_eq!(auth.size_hint().0, collector.keys.len());
    }

    #[test]
    fn determined_by_absent_when_empty() {
        let auth = sample(Vec::new());
        let mut collector = KeyCollector::default();
        auth.visit(&mut collector);
        assert_eq!(collector.keys, vec!["action", "entity", "allowed"]);
        assert_eq!(auth.size_hint().0, collector.keys.len());
    }
}
