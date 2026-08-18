use std::fmt::Display;

use valuable::{Listable, Mappable, Valuable, Value, Visit};

use crate::{
    audit_operation,
    request_metadata::{RequestMetadata, UserAgent},
    service::{
        authn::{Actor, InternalActor},
        authz::{ActionDescriptor, ContextValue, DeterminingFactor, GrantResource, UserOrRoleId},
        events::{
            Authorization, AuthorizationFailedEvent, AuthorizationSucceededEvent, EventListener,
            GrantsChangedEvent, context::EntityDescriptor,
        },
    },
};

/// Wire-format version of every `event_source = "audit"` record, emitted
/// unconditionally as the `audit_format` field.
///
/// **MAJOR** is bumped when an existing field is renamed, retyped, or structurally
/// moved — including a scalar becoming an object, an object becoming an array, or a
/// key changing case or separator.
///
/// **MINOR** is bumped when a field is added and nothing existing changes.
/// Consumers must ignore unknown keys.
///
/// Consumers must split on `'.'` and compare each half as an **integer**. Do not
/// compare the string lexically: `"1.10"` sorts *before* `"1.9"`.
///
/// One counter covers both audit families, authorization and operational. Separate
/// counters would be worse for the operational family: its `context` is supplied by
/// whoever calls the exported [`audit_operation`] macro, including crates outside this
/// repository, so no version stamped here could describe those shapes accurately.
///
/// See the audit-log section of `docs/docs/developer-guide.md` for what to do when
/// the format changes, and `docs/docs/logging.md` for the consumer-facing contract.
pub const AUDIT_FORMAT: &str = "1.0";

/// Whether `s` is exactly `MAJOR.MINOR`, with at least one digit either side.
///
/// Hand-rolled over bytes because the obvious spelling is not const-evaluable:
/// `AUDIT_FORMAT == "1.0"` fails with E0658 plus "`PartialEq` is not yet stable as a
/// const trait" (rust-lang/rust#143874). `str::as_bytes`, `while` and integer
/// arithmetic are all permitted in a `const fn`.
const fn is_major_minor(s: &str) -> bool {
    let b = s.as_bytes();
    let mut dots = 0usize;
    let mut digits_in_part = 0usize;
    let mut i = 0usize;
    while i < b.len() {
        match b[i] {
            // A dot with no digits before it (".0") or a second dot ("1.0.0") is
            // not `MAJOR.MINOR`.
            b'.' => {
                if digits_in_part == 0 || dots == 1 {
                    return false;
                }
                dots += 1;
                digits_in_part = 0;
            }
            b'0'..=b'9' => digits_in_part += 1,
            _ => return false,
        }
        i += 1;
    }
    // Exactly one dot, and the minor part is non-empty ("1." is rejected).
    dots == 1 && digits_in_part > 0
}

// `const _: () = …` forces evaluation at COMPILE time; the `_` name means the item is
// never referenced, so it exists purely so that a failed assert fails the build.
// This pins the *shape* of the version string only — it cannot check that the value
// is correct, because a const panic message must be a literal `&'static str`
// (a computed one is `error[E0015]: cannot call non-const formatting macro in
// constants`). Correctness is the job of the golden fixtures.
const _: () = assert!(
    is_major_minor(AUDIT_FORMAT),
    "AUDIT_FORMAT must be `MAJOR.MINOR`, e.g. \"1.0\""
);

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

    /// # Optional fields: path 2 of 3 — key **omitted**
    ///
    /// `id`, `for-principal` and `allowed` are skipped entirely when `None`, and
    /// `determined_by` when empty, so those keys are **absent** from the JSON rather
    /// than `null`. [`Mappable::size_hint`] below counts the same four conditions by
    /// hand and must be kept in step with this body.
    ///
    /// The record's other two optional-field paths both emit `null` instead — this is
    /// the one that disagrees:
    ///
    /// - **Top-level `tracing` field** — [`user_agent_value`] in this file: always
    ///   recorded; `None` becomes `null` via `impl Valuable for Option<T>`.
    /// - **Derived `Valuable`** — [`DeterminingFactor`] in
    ///   [`crate::service::authz::decision`]: always recorded, because
    ///   `valuable-derive` has no conditional skip.
    ///
    /// Unifying the three is an `audit_format` 2.0 candidate — see the audit-log
    /// section of `docs/docs/developer-guide.md`.
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
                audit_format = AUDIT_FORMAT,
                action = tracing::field::valuable(&__actions[0].as_value()),
                entity = tracing::field::valuable(&__entities.entities[0].as_value()),
                $($common)*
                $msg
            ),
            (true, false) => tracing::info!(
                event_source = "audit",
                audit_format = AUDIT_FORMAT,
                action = tracing::field::valuable(&__actions[0].as_value()),
                entities = tracing::field::valuable(&__entities.as_value()),
                $($common)*
                $msg
            ),
            (false, true) => tracing::info!(
                event_source = "audit",
                audit_format = AUDIT_FORMAT,
                actions = tracing::field::valuable(&__actions.as_value()),
                entity = tracing::field::valuable(&__entities.entities[0].as_value()),
                $($common)*
                $msg
            ),
            (false, false) => tracing::info!(
                event_source = "audit",
                audit_format = AUDIT_FORMAT,
                actions = tracing::field::valuable(&__actions.as_value()),
                entities = tracing::field::valuable(&__entities.as_value()),
                $($common)*
                $msg
            ),
        }
    }};
}

/// The `User-Agent` header for the `user_agent` audit field, or `None` when the
/// caller sent none.
///
/// Recorded verbatim and **unverified**: any caller can set the header to any
/// value, including one naming another client. `actor` and `privilege_source`
/// are the authenticated facts on the same event.
///
/// # Optional fields: path 1 of 3 — key present, value `null`
///
/// This is a top-level `tracing` field, so it is always recorded. `None` reaches the
/// wire through `impl Valuable for Option<T>`, which maps it to `Value::Unit`
/// (`valuable-0.1.1/src/valuable.rs:253-259`) and thence to JSON `null`. A consumer
/// therefore always sees the `user_agent` key.
///
/// The audit record has two other optional-field paths, and only one of them agrees:
///
/// - **Hand-written `visit`** — [`Authorization::visit`] in this file: the key is
///   **omitted** entirely when `None`. Affects `id`, `for-principal` and `allowed`.
///   This is the path that disagrees.
/// - **Derived `Valuable`** — [`DeterminingFactor`] in
///   [`crate::service::authz::decision`]: also `null`, but for a different reason:
///   `valuable-derive` has no conditional skip, so it visits every field
///   unconditionally. Same outcome by coincidence, not by design — a change that
///   unifies one will not automatically unify the other.
///
/// Net effect for consumers: *absent* and *`null`* both mean "not recorded", and
/// which one you get depends on where in the record the field sits, so no field's
/// behaviour can be inferred from another's. Unifying the three is an `audit_format`
/// 2.0 candidate — see the audit-log section of `docs/docs/developer-guide.md`.
fn user_agent_value(request_metadata: &RequestMetadata) -> Option<&str> {
    request_metadata.user_agent().map(UserAgent::as_str)
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
        let user_agent = user_agent_value(&event.request_metadata);
        if event.extra_context.is_empty() {
            audit_log!(
                &*event.actions,
                &*event.entities,
                {
                    actor = tracing::field::valuable(&event.request_metadata.internal_actor().as_value()),
                    privilege_source = event.request_metadata.privilege_source().as_str(),
                    user_agent = tracing::field::valuable(&user_agent),
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
                    user_agent = tracing::field::valuable(&user_agent),
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
        let user_agent = user_agent_value(&event.request_metadata);
        if event.extra_context.is_empty() {
            audit_log!(
                &*event.actions,
                &*event.entities,
                {
                    actor = tracing::field::valuable(&event.request_metadata.internal_actor().as_value()),
                    privilege_source = event.request_metadata.privilege_source().as_str(),
                    user_agent = tracing::field::valuable(&user_agent),
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
                    user_agent = tracing::field::valuable(&user_agent),
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
            audit_format = $crate::service::events::backends::audit::AUDIT_FORMAT,
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
            audit_format = $crate::service::events::backends::audit::AUDIT_FORMAT,
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
    use std::sync::{Arc, Mutex};

    use assert_json_diff::{CompareMode, Config, assert_json_matches_no_panic};
    use valuable::{Valuable, Value, Visit};

    use super::*;
    use crate::{
        request_metadata::{RequestMetadata, RequestMetadataTestBuilder, UserAgent},
        service::{
            authz::{ActionDescriptor, DeterminingFactor, PolicyEffect},
            events::context::{
                EventEntities, FIELD_NAME_NAMESPACE, FIELD_NAME_NAMESPACE_ID, FIELD_NAME_TABLE,
                FIELD_NAME_TABLE_ID, FIELD_NAME_WAREHOUSE_ID,
            },
        },
    };

    /// Collects rendered log lines so a test can assert on the JSON a consumer
    /// actually receives, rather than on the `Valuable` shape alone.
    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for CapturedLogs {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("log buffer poisoned").extend(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl tracing_subscriber::fmt::MakeWriter<'_> for CapturedLogs {
        type Writer = Self;

        fn make_writer(&self) -> Self::Writer {
            self.clone()
        }
    }

    /// Render audit events through the same JSON formatter the binary configures
    /// (`crates/lakekeeper-bin/src/main.rs`), and return the parsed lines.
    ///
    /// Generic over the emitting call so the whole audit surface is reachable:
    /// [`EventListener::authorization_succeeded`],
    /// [`EventListener::authorization_failed`] and
    /// [`EventListener::grants_changed`].
    ///
    /// Returns a `Vec` because `grants_changed` emits one record *per grant
    /// triple*, not one per call. Use [`emit_and_capture_one`] where exactly one
    /// record is expected.
    fn emit_and_capture<F, Fut>(emit: F) -> Vec<serde_json::Value>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<()>>,
    {
        let logs = CapturedLogs::default();
        // Mirrors the binary's subscriber. Every setting is pinned deliberately,
        // including those that match today's defaults, so a `tracing-subscriber`
        // upgrade that changes a default breaks this line rather than silently
        // rewriting what every test sees.
        let subscriber = tracing_subscriber::fmt()
            .json()
            .flatten_event(true)
            // Production sets this; `Json::default()` leaves it `true`. Without it
            // the helper renders a `span` object the binary never emits — harmless
            // while no span is active, wrong the moment a test runs under one (and
            // production always does: the router installs a request span).
            .with_current_span(false)
            .with_span_list(true)
            // Production gates these on `CONFIG_BIN.debug.extended_logs`, i.e. off
            // by default. Pin them off so this file's own line numbers can never
            // leak into a captured record.
            .with_file(false)
            .with_line_number(false)
            .with_writer(logs.clone())
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            futures::executor::block_on(emit()).expect("emitting an audit event must not fail");
        });

        let bytes = logs.0.lock().expect("log buffer poisoned").clone();
        let text = String::from_utf8(bytes).expect("log output must be utf-8");
        text.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| serde_json::from_str(line).expect("log line must be valid json"))
            .collect()
    }

    /// [`emit_and_capture`] for the case where exactly one record is expected.
    #[track_caller]
    fn emit_and_capture_one<F, Fut>(emit: F) -> serde_json::Value
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<()>>,
    {
        let mut records = emit_and_capture(emit);
        assert_eq!(
            records.len(),
            1,
            "expected exactly one audit record, got {}",
            records.len()
        );
        records.pop().expect("length asserted above")
    }

    fn succeeded_event(request_metadata: RequestMetadata) -> AuthorizationSucceededEvent {
        let entities = Arc::new(EventEntities::one(EntityDescriptor::new("table")));
        let actions = Arc::new(vec![
            ActionDescriptor::builder().action_name("read_data").build(),
        ]);
        AuthorizationSucceededEvent {
            request_metadata: Arc::new(request_metadata),
            entities,
            actions,
            extra_context: Arc::new(std::collections::HashMap::new()),
            authorizations: Arc::new(vec![sample(Vec::new())]),
        }
    }

    // ── Wire-format fixtures ────────────────────────────────────────────────────
    //
    // Each fixture is a committed record of exactly what one audit event renders to
    // on the wire. Together they are the only thing in the tree that observes the
    // emitted JSON, and therefore the only thing that can detect an unintended
    // change to the audit format.
    //
    // Every value below is fixed. Random ids or a clock would make each run differ,
    // and at most one `extra_context` key is used per fixture: `extra_context` is a
    // `HashMap`, so two or more keys render in an unstable order and the fixtures
    // would fail at random.
    //
    // To regenerate after a deliberate change: `just update-audit-fixtures`.

    const FIXTURE_WAREHOUSE_ID: &str = "019684ff-0000-7000-8000-000000000001";
    const FIXTURE_TABLE_ID: &str = "019684ff-0000-7000-8000-000000000002";
    const FIXTURE_NAMESPACE_ID: &str = "019684ff-0000-7000-8000-000000000003";

    /// Keys the log subscriber adds, which `AUDIT_FORMAT` deliberately does not
    /// cover — see the stability section of `docs/docs/logging.md`. They are stripped
    /// before comparison so that a `tracing-subscriber` upgrade, or moving this
    /// module (which changes `target`), cannot be mistaken for a format change.
    const ENVELOPE_KEYS: &[&str] = &[
        "timestamp",
        "level",
        "message",
        "target",
        "span",
        "spans",
        "filename",
        "line_number",
    ];

    /// Strip the subscriber-owned envelope, leaving only the fields `AUDIT_FORMAT`
    /// makes promises about, in the order they were emitted.
    ///
    /// `retain` rather than `remove`: with `serde_json`'s `preserve_order` feature (which
    /// this workspace enables) a `Map` is index-backed and `remove` is a *swap*-remove,
    /// which would shuffle the surviving keys. Order is worth keeping — a fixture that
    /// reads in wire order is a fixture a reviewer can check against a real log line.
    fn contract_fields(mut record: serde_json::Value) -> serde_json::Value {
        record
            .as_object_mut()
            .expect("an audit record is a JSON object")
            .retain(|key, _| !ENVELOPE_KEYS.contains(&key.as_str()));
        record
    }

    fn fixture_path(name: &str) -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/service/events/backends/audit/fixtures/v1")
            .join(format!("{name}.json"))
    }

    /// Assert that `emitted` still matches the committed fixture, and classify any
    /// difference as a major or a minor change to [`AUDIT_FORMAT`].
    ///
    /// Read and written at runtime rather than embedded with `include_str!`, so the
    /// same code path can also regenerate the file. A brand-new fixture would
    /// otherwise fail to compile before it could be generated.
    #[track_caller]
    fn assert_matches_fixture(name: &str, emitted: &serde_json::Value) {
        let path = fixture_path(name);

        if std::env::var_os("LAKEKEEPER_UPDATE_AUDIT_FIXTURES").is_some() {
            std::fs::create_dir_all(path.parent().expect("fixture path has a parent"))
                .expect("creating the fixture directory");
            let mut json =
                serde_json::to_string_pretty(emitted).expect("an audit record serialises");
            json.push('\n');
            std::fs::write(&path, json)
                .unwrap_or_else(|e| panic!("writing {}: {e}", path.display()));
            return;
        }

        let committed = std::fs::read_to_string(&path).unwrap_or_else(|e| {
            panic!(
                "cannot read the committed audit fixture {}: {e}\n\n\
                 If this fixture is new, generate it with `just update-audit-fixtures`. \
                 If it was moved or deleted, restore it: it is the record of what \
                 audit_format {AUDIT_FORMAT} puts on the wire, and without it nothing \
                 detects a change to the audit log format.",
                path.display()
            )
        });
        let committed: serde_json::Value = serde_json::from_str(&committed)
            .unwrap_or_else(|e| panic!("fixture {} is not valid JSON: {e}", path.display()));

        // A fixture of `{}` satisfies the subset check below unconditionally, so an
        // emptied or truncated file would switch the breaking-change check off while
        // leaving a green test. Floor the key count.
        assert!(
            committed
                .as_object()
                .is_some_and(|object| object.len() >= 6),
            "fixture {} has fewer than 6 keys and looks truncated. Compared against a \
             near-empty fixture, the check below asserts almost nothing.",
            path.display()
        );

        // Is every key the fixture records still present, with the same type and
        // value? `CompareMode::Inclusive` walks the right-hand value and requires the
        // left to contain it, so with the fixture on the right this asserts
        // "fixture is a subset of emitted": extra keys in `emitted` pass.
        //
        // Do not re-derive that direction from assert-json-diff's own documentation,
        // which describes `Inclusive` the other way round; the behaviour above is
        // what its `diff.rs` implements and what this test relies on. Reversed, this
        // check would pass while a field was being deleted.
        if let Err(difference) =
            assert_json_matches_no_panic(emitted, &committed, Config::new(CompareMode::Inclusive))
        {
            panic!(
                "the audit log format changed in a way that BREAKS CONSUMERS: a field \
                 recorded in {name} is now missing, renamed, or has a different type \
                 or value.\n\n{difference}\n\n\
                 If this change is intended, bump the MAJOR half of AUDIT_FORMAT \
                 (currently {AUDIT_FORMAT}), start a new fixture directory for it, and \
                 keep the old one passing — consumers replaying older logs still need \
                 it. Then update docs/docs/logging.md and say so in the release notes. \
                 If it is not intended, this is the bug.\n\n\
                 Note: a changed *value* fails here too, and reads the same as a \
                 changed type. If the value is nondeterministic, the fixture needs to \
                 stop depending on it.\n\n\
                 See the audit log section of docs/docs/developer-guide.md."
            );
        }

        // Reaching here means nothing recorded in the fixture moved, so the only way
        // to differ is a key present in `emitted` and absent from the fixture: a
        // purely additive change, which existing consumers can ignore.
        if let Err(difference) =
            assert_json_matches_no_panic(emitted, &committed, Config::new(CompareMode::Strict))
        {
            panic!(
                "the audit log format gained a field. Nothing existing changed, so this \
                 is additive and existing consumers keep working.\n\n{difference}\n\n\
                 Bump the MINOR half of AUDIT_FORMAT (currently {AUDIT_FORMAT}), \
                 regenerate the fixtures with `just update-audit-fixtures`, document \
                 the new field in docs/docs/logging.md, and say so in the release \
                 notes.\n\n\
                 See the audit log section of docs/docs/developer-guide.md."
            );
        }
    }

    fn fixture_table_entity() -> EntityDescriptor {
        EntityDescriptor::new("table")
            .field(FIELD_NAME_WAREHOUSE_ID, &FIXTURE_WAREHOUSE_ID)
            .field(FIELD_NAME_TABLE_ID, &FIXTURE_TABLE_ID)
            .field(FIELD_NAME_TABLE, &"sales.orders")
    }

    fn fixture_namespace_entity() -> EntityDescriptor {
        EntityDescriptor::new("namespace")
            .field(FIELD_NAME_WAREHOUSE_ID, &FIXTURE_WAREHOUSE_ID)
            .field(FIELD_NAME_NAMESPACE_ID, &FIXTURE_NAMESPACE_ID)
            .field(FIELD_NAME_NAMESPACE, &"sales")
    }

    fn fixture_read_action() -> ActionDescriptor {
        ActionDescriptor::builder().action_name("read_data").build()
    }

    /// An action carrying context, so the fixtures pin that nesting too.
    fn fixture_action_with_context() -> ActionDescriptor {
        ActionDescriptor::builder()
            .action_name("update_table_properties")
            .context_string("name", "orders")
            .context_list("removed-properties", vec!["stale.key".to_string()])
            .build()
    }

    /// The simplest per-decision entry: no id, no `for-principal`, no
    /// `determined_by`. Pins which keys are omitted rather than emitted as null.
    fn fixture_plain_authorization() -> Authorization {
        Authorization {
            id: None,
            for_principal: None,
            action: fixture_read_action(),
            entity: fixture_table_entity(),
            allowed: Some(true),
            determined_by: Vec::new(),
        }
    }

    /// A fully-populated entry, so the fixtures pin the optional keys in their
    /// present form as well as their absent one, and both `DeterminingFactor`
    /// variants including its own `None` fields.
    fn fixture_detailed_authorization() -> Authorization {
        Authorization {
            id: Some("check-0".to_string()),
            for_principal: Some(UserOrRoleId::User(
                crate::service::authn::UserId::try_from("oidc~bob").expect("valid test user id"),
            )),
            action: fixture_read_action(),
            entity: fixture_namespace_entity(),
            allowed: Some(false),
            determined_by: vec![
                DeterminingFactor::Policy {
                    policy_id: "policy-42".to_string(),
                    name: Some("deny-stale-namespaces".to_string()),
                    effect: PolicyEffect::Forbid,
                    source: Some("cedar".to_string()),
                },
                DeterminingFactor::SystemAuthority {
                    source: None,
                    reason: None,
                },
            ],
        }
    }

    fn fixture_context(entries: &[(&str, &str)]) -> Arc<std::collections::HashMap<String, String>> {
        Arc::new(
            entries
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect(),
        )
    }

    /// An authenticated caller with a `User-Agent`, so the fixtures pin the populated
    /// form of both `actor` and `user_agent`.
    fn fixture_metadata() -> RequestMetadata {
        RequestMetadataTestBuilder::builder()
            .actor(Actor::Principal(
                crate::service::authn::UserId::try_from("oidc~alice").expect("valid test user id"),
            ))
            .user_agent(UserAgent::parse("Apache-Spark/3.5.1 (Scala/2.12)"))
            .build()
    }

    fn fixture_error() -> Arc<crate::service::events::AuthorizationError> {
        Arc::new(crate::service::events::AuthorizationError {
            r#type: "NotAuthorized".to_string(),
            code: 403,
            message: "Principal is not allowed to read this table".to_string(),
            stack: vec!["authorizer: no matching grant".to_string()],
            error_id: "019684ff-0000-7000-8000-0000000000ff".to_string(),
        })
    }

    /// Every fixture, so that both tests below cover the whole committed set rather
    /// than whichever files happen to exist.
    const FIXTURE_NAMES: &[&str] = &[
        "authz_succeeded_single",
        "authz_succeeded_plural",
        "authz_succeeded_action_entities",
        "authz_succeeded_actions_entity",
        "authz_failed_single",
        "authz_failed_context",
        "grant_created",
        "grant_revoked",
    ];

    fn read_fixture(name: &str) -> serde_json::Value {
        let path = fixture_path(name);
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
        serde_json::from_str(&text)
            .unwrap_or_else(|e| panic!("fixture {} is not valid JSON: {e}", path.display()))
    }

    /// Collect every JSON object key in `value`, at any depth.
    fn collect_keys(value: &serde_json::Value, out: &mut Vec<String>) {
        match value {
            serde_json::Value::Object(fields) => {
                for (key, nested) in fields {
                    out.push(key.clone());
                    collect_keys(nested, out);
                }
            }
            serde_json::Value::Array(items) => {
                for item in items {
                    collect_keys(item, out);
                }
            }
            _ => {}
        }
    }

    /// Every field the audit log puts on the wire must be documented, so the reference
    /// in `docs/docs/logging.md` cannot quietly fall behind the code.
    ///
    /// Driven off the committed fixtures, so it covers what is actually emitted rather
    /// than what some type declares. Add a field and this fails, naming it.
    ///
    /// Coverage is therefore bounded by the fixtures: a key emitted only by a code path
    /// no fixture exercises is invisible here. Widening the fixture set widens this
    /// check too, which is the main reason to add one.
    ///
    /// Keys are matched as `` `key` `` — a field table entry or inline mention, not a
    /// bare appearance inside a JSON example, since an example is not a description.
    #[test]
    fn every_emitted_audit_field_is_documented() {
        // Resolved and embedded at COMPILE time: if `logging.md` is deleted or moved,
        // this line fails the build with "couldn't read …: No such file or directory".
        // It can never silently read an empty string. The path is relative to this
        // file, so it climbs from `backends/audit/` to the repository root; the same
        // technique is used in `crate::api::endpoints` for the committed OpenAPI specs.
        const LOGGING_DOC: &str = include_str!("../../../../../../../docs/docs/logging.md");

        // The check above only covers the file being gone. This covers the other
        // failure: the file is still there but no longer holds the audit reference —
        // split into another page, replaced by a stub, or gutted — which would
        // otherwise surface as one baffling failure per field.
        assert!(
            LOGGING_DOC.contains("{#audit-logs}"),
            "docs/docs/logging.md no longer contains the `{{#audit-logs}}` anchor. The \
             audit log documentation has moved, been split, or been deleted. This test \
             asserts that every field the audit log emits is documented there, so point \
             it at the new location and update the `#audit-logs` links in the other docs."
        );

        let mut keys = Vec::new();
        for name in FIXTURE_NAMES {
            collect_keys(&read_fixture(name), &mut keys);
        }
        keys.sort();
        keys.dedup();

        let undocumented: Vec<&String> = keys
            .iter()
            .filter(|key| !LOGGING_DOC.contains(&format!("`{key}`")))
            .collect();

        assert!(
            undocumented.is_empty(),
            "these audit log fields are emitted but not documented in \
             docs/docs/logging.md: {undocumented:?}\n\n\
             Add each one to the relevant field table. A field nobody documented is a \
             field consumers have to reverse-engineer from example output, which is how \
             the reference fell out of step with the code before.\n\n\
             Adding a field is a minor change to the audit format: see the audit log \
             section of docs/docs/developer-guide.md."
        );
    }

    /// The fixture directory and [`FIXTURE_NAMES`] must agree. Without this, deleting a
    /// test leaves an orphan fixture that nothing asserts, and a fixture added by hand
    /// is never compared against anything.
    #[test]
    fn the_fixture_directory_matches_the_declared_set() {
        let directory = fixture_path("unused")
            .parent()
            .expect("fixture path has a parent")
            .to_path_buf();

        let mut on_disk: Vec<String> = std::fs::read_dir(&directory)
            .unwrap_or_else(|e| panic!("reading {}: {e}", directory.display()))
            .map(|entry| entry.expect("readable directory entry").file_name())
            .filter_map(|name| {
                name.to_str()
                    .and_then(|name| name.strip_suffix(".json"))
                    .map(str::to_owned)
            })
            .collect();
        on_disk.sort();

        let mut declared: Vec<String> = FIXTURE_NAMES.iter().map(|n| (*n).to_string()).collect();
        declared.sort();

        assert_eq!(
            on_disk, declared,
            "the fixtures on disk and the ones declared in FIXTURE_NAMES have drifted. A \
             fixture with no test asserting it detects nothing; a declared fixture with \
             no file makes the tests fail on a missing file instead of on a real change. \
             Regenerate with `just update-audit-fixtures`."
        );
    }

    /// One action, one entity: `audit_log!` emits the singular `action` / `entity`
    /// keys. No `extra_context`, and an anonymous caller with no `User-Agent`, so
    /// this fixture is the one that pins the absent and null forms.
    #[test]
    fn fixture_authz_succeeded_single_action_single_entity() {
        let record = emit_and_capture_one(|| {
            AuditEventListener.authorization_succeeded(AuthorizationSucceededEvent {
                request_metadata: Arc::new(RequestMetadataTestBuilder::builder().build()),
                entities: Arc::new(EventEntities::one(fixture_table_entity())),
                actions: Arc::new(vec![fixture_read_action()]),
                extra_context: fixture_context(&[]),
                authorizations: Arc::new(vec![fixture_plain_authorization()]),
            })
        });

        assert_matches_fixture("authz_succeeded_single", &contract_fields(record));
    }

    /// Several actions and several entities: `audit_log!` switches to the plural
    /// `actions` / `entities` keys. Also carries `extra_context`, an action with its
    /// own context, and a fully-populated per-decision entry.
    #[test]
    fn fixture_authz_succeeded_plural_actions_plural_entities() {
        let record = emit_and_capture_one(|| {
            AuditEventListener.authorization_succeeded(AuthorizationSucceededEvent {
                request_metadata: Arc::new(fixture_metadata()),
                entities: Arc::new(EventEntities::many([
                    fixture_table_entity(),
                    fixture_namespace_entity(),
                ])),
                actions: Arc::new(vec![fixture_read_action(), fixture_action_with_context()]),
                extra_context: fixture_context(&[("invoked-by", "maintenance-task")]),
                authorizations: Arc::new(vec![
                    fixture_plain_authorization(),
                    fixture_detailed_authorization(),
                ]),
            })
        });

        assert_matches_fixture("authz_succeeded_plural", &contract_fields(record));
    }

    /// One action, several entities: the singular `action` key with the plural
    /// `entities` key. This mixed arity is its own arm of `audit_log!`.
    #[test]
    fn fixture_authz_succeeded_single_action_plural_entities() {
        let record = emit_and_capture_one(|| {
            AuditEventListener.authorization_succeeded(AuthorizationSucceededEvent {
                request_metadata: Arc::new(fixture_metadata()),
                entities: Arc::new(EventEntities::many([
                    fixture_table_entity(),
                    fixture_namespace_entity(),
                ])),
                actions: Arc::new(vec![fixture_read_action()]),
                extra_context: fixture_context(&[]),
                authorizations: Arc::new(vec![fixture_plain_authorization()]),
            })
        });

        assert_matches_fixture("authz_succeeded_action_entities", &contract_fields(record));
    }

    /// Several actions, one entity: the remaining arm, plural `actions` with the
    /// singular `entity` key.
    #[test]
    fn fixture_authz_succeeded_plural_actions_single_entity() {
        let record = emit_and_capture_one(|| {
            AuditEventListener.authorization_succeeded(AuthorizationSucceededEvent {
                request_metadata: Arc::new(fixture_metadata()),
                entities: Arc::new(EventEntities::one(fixture_table_entity())),
                actions: Arc::new(vec![fixture_read_action(), fixture_action_with_context()]),
                extra_context: fixture_context(&[]),
                authorizations: Arc::new(vec![fixture_plain_authorization()]),
            })
        });

        assert_matches_fixture("authz_succeeded_actions_entity", &contract_fields(record));
    }

    /// A denied authorization. Carries `failure_reason` and `error`, which succeeded
    /// events do not, and records `decision: "denied"`.
    #[test]
    fn fixture_authz_failed_single_action_single_entity() {
        let record = emit_and_capture_one(|| {
            AuditEventListener.authorization_failed(AuthorizationFailedEvent {
                request_metadata: Arc::new(fixture_metadata()),
                entities: Arc::new(EventEntities::one(fixture_table_entity())),
                actions: Arc::new(vec![fixture_read_action()]),
                failure_reason: crate::service::events::AuthorizationFailureReason::ActionForbidden,
                error: fixture_error(),
                extra_context: fixture_context(&[]),
                authorizations: Arc::new(vec![fixture_detailed_authorization()]),
            })
        });

        assert_matches_fixture("authz_failed_single", &contract_fields(record));
    }

    /// A denied authorization that also carries `extra_context`, which is emitted by
    /// a different arm of the listener from the one above.
    #[test]
    fn fixture_authz_failed_with_context() {
        let record = emit_and_capture_one(|| {
            AuditEventListener.authorization_failed(AuthorizationFailedEvent {
                request_metadata: Arc::new(fixture_metadata()),
                entities: Arc::new(EventEntities::one(fixture_namespace_entity())),
                actions: Arc::new(vec![fixture_read_action()]),
                failure_reason:
                    crate::service::events::AuthorizationFailureReason::CannotSeeResource,
                error: fixture_error(),
                extra_context: fixture_context(&[("self-read", "false")]),
                authorizations: Arc::new(vec![fixture_plain_authorization()]),
            })
        });

        assert_matches_fixture("authz_failed_context", &contract_fields(record));
    }

    /// The operational family, emitted through `audit_operation!` rather than
    /// `audit_log!` — a different shape entirely, with `operation` / `outcome` /
    /// `context` and no `entity` or `decision`.
    ///
    /// One `grants_changed` event emits one record per grant triple, revocations
    /// first, so this covers both operations in the order a consumer sees them.
    #[test]
    fn fixture_grants_changed_emits_one_record_per_triple() {
        let principal = UserOrRoleId::User(
            crate::service::authn::UserId::try_from("oidc~alice").expect("valid test user id"),
        );
        let spec = |privilege: &str, resource: GrantResource| crate::service::authz::GrantSpec {
            principal: principal.clone(),
            resource,
            privilege: privilege.to_string(),
        };
        let uuid = |s: &str| s.parse::<uuid::Uuid>().expect("fixed test uuid");
        let table = || GrantResource::Table {
            warehouse_id: crate::service::WarehouseId::new(uuid(FIXTURE_WAREHOUSE_ID)),
            table_id: crate::service::TableId::new(uuid(FIXTURE_TABLE_ID)),
        };

        let records = emit_and_capture(|| {
            AuditEventListener.grants_changed(GrantsChangedEvent::new(
                vec![spec("modify", table())],
                vec![spec("select", table())],
                Arc::new(fixture_metadata()),
            ))
        });

        assert_eq!(
            records.len(),
            2,
            "one record per grant triple, revocation first: {records:?}"
        );
        let mut records = records.into_iter();
        let revoked = records.next().expect("the revoked record");
        let created = records.next().expect("the created record");

        assert_matches_fixture("grant_revoked", &contract_fields(revoked));
        assert_matches_fixture("grant_created", &contract_fields(created));
    }

    /// The envelope keys are deliberately outside the format contract, so no fixture
    /// records them — which means nothing would notice if the subscriber stopped
    /// emitting them entirely. Assert the ones a consumer genuinely relies on.
    #[test]
    fn audit_records_carry_the_envelope_keys_consumers_rely_on() {
        let record = emit_and_capture_one(|| {
            AuditEventListener.authorization_succeeded(succeeded_event(fixture_metadata()))
        });

        for key in ["timestamp", "level", "message", "target"] {
            assert!(
                record.get(key).is_some(),
                "the log subscriber stopped emitting `{key}`. It is outside the \
                 audit_format contract, so no fixture covers it, but consumers do rely \
                 on it: {record}"
            );
        }
    }

    /// The audit log has to say which client made the call, verbatim — a SIEM
    /// classifies the string, so Lakekeeper must not normalise it away.
    #[test]
    fn an_audit_event_records_the_user_agent_verbatim() {
        let metadata = RequestMetadataTestBuilder::builder()
            .user_agent(UserAgent::parse("Apache-Spark/3.5.1 (Scala/2.12)"))
            .build();

        let event = emit_and_capture_one(|| {
            AuditEventListener.authorization_succeeded(succeeded_event(metadata))
        });

        assert_eq!(
            event.get("user_agent").and_then(serde_json::Value::as_str),
            Some("Apache-Spark/3.5.1 (Scala/2.12)"),
        );
    }

    /// The capture helper must render what the binary renders. Nothing else pins
    /// that, and if it drifts every fixture captured through it silently describes
    /// a shape production never emits.
    ///
    /// `with_current_span(false)` is the setting that is easy to lose, and it is
    /// only observable while a span is active — which production always is, since
    /// the router installs a request span around every call.
    #[test]
    fn the_capture_helper_omits_envelope_keys_production_omits() {
        use tracing::Instrument as _;

        let metadata = RequestMetadataTestBuilder::builder().build();
        let record = emit_and_capture_one(|| {
            // Built inside the closure, so the span is registered with the capture
            // subscriber rather than whatever is globally installed, and
            // `Instrument` makes it current while the future is polled.
            let span = tracing::info_span!("request");
            AuditEventListener
                .authorization_succeeded(succeeded_event(metadata))
                .instrument(span)
        });

        assert!(
            record.get("span").is_none(),
            "captured record carries a `span` key. Production sets \
             `.with_current_span(false)` (crates/lakekeeper-bin/src/main.rs), so this \
             helper must too — otherwise captured fixtures describe a shape the binary \
             never emits. Got: {record}"
        );
    }

    /// Every audit record must declare its wire format version. This covers the
    /// authorization family, emitted through `audit_log!`.
    #[test]
    fn an_authorization_audit_record_declares_its_format_version() {
        let metadata = RequestMetadataTestBuilder::builder().build();

        let record = emit_and_capture_one(|| {
            AuditEventListener.authorization_succeeded(succeeded_event(metadata))
        });

        assert_eq!(
            record
                .get("audit_format")
                .and_then(serde_json::Value::as_str),
            Some(AUDIT_FORMAT),
            "every audit record must declare its wire format version: {record}"
        );
    }

    /// Every audit record must declare its wire format version. This covers the
    /// operational family, emitted through the `#[macro_export]`ed
    /// `audit_operation!`.
    ///
    /// It is also the only test that exercises the `$crate`-qualified path to
    /// [`AUDIT_FORMAT`] that the exported macro needs, and the only one that emits
    /// more than one record per call: `grants_changed` produces one record per grant
    /// triple, not one per event.
    #[test]
    fn operational_audit_records_declare_their_format_version() {
        let principal = UserOrRoleId::User(
            crate::service::authn::UserId::try_from("oidc~alice").expect("valid test user id"),
        );
        let spec = |privilege: &str| crate::service::authz::GrantSpec {
            principal: principal.clone(),
            resource: GrantResource::Server,
            privilege: privilege.to_string(),
        };
        let event = GrantsChangedEvent::new(
            vec![spec("revoked_privilege")],
            vec![spec("created_privilege")],
            Arc::new(RequestMetadataTestBuilder::builder().build()),
        );

        let records = emit_and_capture(|| AuditEventListener.grants_changed(event));

        assert_eq!(
            records.len(),
            2,
            "one record per grant triple — one revoked, one created: {records:?}"
        );
        for record in &records {
            assert_eq!(
                record
                    .get("audit_format")
                    .and_then(serde_json::Value::as_str),
                Some(AUDIT_FORMAT),
                "operational audit records must declare the format version too: {record}"
            );
        }
    }

    /// Makes "every audit record declares its format version" a gate rather than a
    /// convention: every audit emission site in this file must set `audit_format` on
    /// the following line.
    ///
    /// This is a lint that lives in `cargo test` because the repo has no lint
    /// harness. A runtime test can only cover emission arms it actually exercises,
    /// and the whole point is to cover the arms nobody thought to write a test for.
    #[test]
    fn every_audit_emission_site_carries_the_format_version() {
        // Reads this file's own source: `include_str!` resolves relative to the
        // containing file, so `"mod.rs"` is this file.
        let src = include_str!("mod.rs");

        // Assembled with `concat!`, which joins at compile time, so neither needle
        // ever appears literally in this file. Written out, the scan would match its
        // own source and fail against itself.
        let needle = concat!("event_source", " = ", "\"audit\"");
        let marker = concat!("audit_format", " =");

        let lines: Vec<&str> = src.lines().collect();
        let mut sites = 0;
        for (i, line) in lines.iter().enumerate() {
            // Skip comments: the doc comment on `AUDIT_FORMAT` quotes the needle,
            // and future prose may too.
            if !line.contains(needle) || line.trim_start().starts_with("//") {
                continue;
            }
            sites += 1;
            let next = lines.get(i + 1).copied().unwrap_or_default();
            assert!(
                next.contains(marker),
                "audit.rs:{} starts an audit emission but line {} does not set \
                 `audit_format`. Every audit record must declare its wire format \
                 version. Line {}: {}",
                i + 1,
                i + 2,
                i + 2,
                next.trim()
            );
        }

        assert_eq!(
            sites, 6,
            "expected 6 audit emission sites (4 `audit_log!` arity arms, 2 \
             `audit_operation!` arms), found {sites}. An arm was added or removed — \
             confirm it declares the format version, then update this count."
        );
    }

    /// A request that sent no `User-Agent` must be distinguishable from one
    /// that sent a client named "unknown", so the field is null rather than a
    /// sentinel.
    #[test]
    fn an_audit_event_without_a_user_agent_records_null() {
        let metadata = RequestMetadataTestBuilder::builder().build();

        let event = emit_and_capture_one(|| {
            AuditEventListener.authorization_succeeded(succeeded_event(metadata))
        });

        assert_eq!(
            event.get("user_agent"),
            Some(&serde_json::Value::Null),
            "the key must be present so consumers can tell 'not sent' from 'not recorded'"
        );
    }

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
