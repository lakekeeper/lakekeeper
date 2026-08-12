use std::fmt::Display;

use valuable::{Listable, Mappable, Valuable, Value, Visit};

use crate::{
    audit_operation,
    service::{
        authn::{Actor, InternalActor},
        authz::{ActionDescriptor, ContextValue, DeterminingFactor, GrantResource, UserOrRoleId},
        events::{
            Authorization, AuthorizationFailedEvent, AuthorizationSucceededEvent, EventListener,
            GrantsChangedEvent, context::EntityDescriptor,
        },
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

/// A grant's full `(principal, privilege, resource)` triple, as audit context.
///
/// Grants are hard-deleted and carry no history, so a revocation's triple exists
/// nowhere else once the row is gone — the event has to be self-contained.
struct GrantContextValue<'a> {
    principal: &'a UserOrRoleId,
    privilege: &'a str,
    resource: &'a GrantResource,
}

impl Valuable for GrantContextValue<'_> {
    fn as_value(&self) -> Value<'_> {
        Value::Mappable(self)
    }

    fn visit(&self, visit: &mut dyn Visit) {
        visit.visit_entry(
            Value::String("principal"),
            UserOrRoleIdValue(self.principal).as_value(),
        );
        visit.visit_entry(Value::String("privilege"), Value::String(self.privilege));
        visit.visit_entry(
            Value::String("resource_type"),
            Value::String(self.resource.resource_type().as_str()),
        );
        // Identifies the exact resource. Server grants name no id — the resource type
        // is the whole identity — so the key is omitted rather than emitted empty.
        let resource_id = grant_resource_id(self.resource);
        if let Some(id) = resource_id.as_deref() {
            visit.visit_entry(Value::String("resource_id"), Value::String(id));
        }
        let warehouse_id = self.resource.warehouse_id().map(|id| id.to_string());
        if let Some(id) = warehouse_id.as_deref() {
            visit.visit_entry(Value::String("warehouse_id"), Value::String(id));
        }
    }
}

impl Mappable for GrantContextValue<'_> {
    fn size_hint(&self) -> (usize, Option<usize>) {
        (3, Some(5))
    }
}

/// The id identifying the exact resource, or `None` for a server grant.
fn grant_resource_id(resource: &GrantResource) -> Option<String> {
    match resource {
        GrantResource::Server => None,
        GrantResource::Project(project_id) => Some(project_id.to_string()),
        GrantResource::Warehouse(warehouse_id) => Some(warehouse_id.to_string()),
        GrantResource::Namespace { namespace_id, .. } => Some(namespace_id.to_string()),
        GrantResource::Table { table_id, .. } => Some(table_id.to_string()),
        GrantResource::View { view_id, .. } => Some(view_id.to_string()),
        GrantResource::GenericTable {
            generic_table_id, ..
        } => Some(generic_table_id.to_string()),
        GrantResource::Tag(tag_definition_id) => Some(tag_definition_id.to_string()),
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

    /// The grants that actually landed.
    ///
    /// The authorization event records the *attempt*, and deduplicates principals and
    /// privileges into separate lists — so it cannot say which principal received which
    /// privilege. This records the confirmed triples, which is what attribution and
    /// reconstruction of current access need. A revoked grant is hard-deleted, so its
    /// record here is the only remaining evidence the access ever existed.
    async fn grants_changed(&self, event: GrantsChangedEvent) -> anyhow::Result<()> {
        let actor = event.request_metadata.internal_actor();
        // One record per triple, not one per request: the batch is a dispatch
        // optimisation, while the audit trail is answered per grant.
        for spec in &event.removed {
            audit_operation!(
                operation = "grant_revoked",
                actor = actor,
                outcome = "success",
                context = GrantContextValue {
                    principal: &spec.principal,
                    privilege: &spec.privilege,
                    resource: &spec.resource,
                },
                "Grant revoked"
            );
        }
        for spec in &event.created {
            audit_operation!(
                operation = "grant_created",
                actor = actor,
                outcome = "success",
                context = GrantContextValue {
                    principal: &spec.principal,
                    privilege: &spec.privilege,
                    resource: &spec.resource,
                },
                "Grant created"
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

    /// Records key/value pairs, flattening a nested map into `key=value` pairs joined
    /// by `,` so a whole context can be asserted with one exact comparison.
    #[derive(Default)]
    struct EntryCollector {
        entries: Vec<(String, String)>,
    }

    impl Visit for EntryCollector {
        fn visit_value(&mut self, _value: Value<'_>) {}
        fn visit_entry(&mut self, key: Value<'_>, value: Value<'_>) {
            let Value::String(key) = key else { return };
            let rendered = match value {
                Value::String(s) => s.to_string(),
                Value::Mappable(m) => {
                    let mut inner = EntryCollector::default();
                    m.visit(&mut inner);
                    inner
                        .entries
                        .iter()
                        .map(|(k, v)| format!("{k}={v}"))
                        .collect::<Vec<_>>()
                        .join(",")
                }
                other => format!("{other:?}"),
            };
            self.entries.push((key.to_string(), rendered));
        }
    }

    fn grant_context(
        principal: &UserOrRoleId,
        privilege: &str,
        resource: &GrantResource,
    ) -> Vec<(String, String)> {
        let mut collector = EntryCollector::default();
        GrantContextValue {
            principal,
            privilege,
            resource,
        }
        .visit(&mut collector);
        collector.entries
    }

    /// A revoked grant is hard-deleted, so this context is the only surviving record of
    /// it — every part of the triple has to be present and correctly labelled.
    #[test]
    fn a_grant_context_carries_the_full_triple() {
        let warehouse_id = crate::service::WarehouseId::new_random();
        let table_id = crate::service::TableId::new_random();
        let principal = UserOrRoleId::User(
            crate::service::authn::UserId::try_from("oidc~alice").expect("valid test user id"),
        );

        let entries = grant_context(
            &principal,
            "select",
            &GrantResource::Table {
                warehouse_id,
                table_id,
            },
        );

        assert_eq!(
            entries,
            vec![
                ("principal".to_string(), "user=oidc~alice".to_string()),
                ("privilege".to_string(), "select".to_string()),
                ("resource_type".to_string(), "table".to_string()),
                ("resource_id".to_string(), table_id.to_string()),
                ("warehouse_id".to_string(), warehouse_id.to_string()),
            ]
        );
    }

    /// A server grant has no id and no warehouse: the resource type is its whole
    /// identity. Those keys are omitted rather than emitted empty, so a consumer can
    /// tell "server-wide" from "an id we failed to record".
    #[test]
    fn a_server_grant_context_omits_the_id_and_warehouse() {
        let principal = UserOrRoleId::Role(crate::service::RoleId::new_random());
        let entries = grant_context(&principal, "admin", &GrantResource::Server);

        let keys: Vec<&str> = entries.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, vec!["principal", "privilege", "resource_type"]);
        assert_eq!(entries[2].1, "server");
        // A role principal is labelled as one, so it cannot be read as a user id.
        assert!(
            entries[0].1.starts_with("role="),
            "expected a role-labelled principal, got {}",
            entries[0].1
        );
    }

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
