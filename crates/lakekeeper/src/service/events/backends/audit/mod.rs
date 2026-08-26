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

/// Whether `s` is exactly `MAJOR.MINOR`. Hand-rolled over bytes because `==` on `&str` is
/// not const-evaluable (rust-lang/rust#143874).
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

// Pins the shape of the version string, not its value — correctness is the fixtures' job.
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

    /// Optional fields are omitted when `None`, not emitted as `null`. Other parts of the
    /// record do the opposite; see "Optional fields" in the audit-log section of
    /// `docs/docs/developer-guide.md`.
    ///
    /// [`Mappable::size_hint`] below hand-counts the same four conditions and must be kept
    /// in step with this body.
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
        // Exact, matching `visit` above. A range is tolerated by `serde_json`, which
        // ignores the hint, but a length-prefixed serializer would emit a corrupt frame.
        let len = 3
            + usize::from(grant_resource_id(self.resource).is_some())
            + usize::from(self.resource.warehouse_id().is_some());
        (len, Some(len))
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

/// The one `tracing::info!` that emits an audit record.
///
/// Every audit event routes through here, so `event_source` and `audit_format` are
/// stamped in exactly one place. Spelling them at each call site instead is what made
/// the version field a convention that a test had to police by reading this file — a
/// new emission path could simply omit it.
///
/// `#[doc(hidden)] #[macro_export]` rather than a private `macro_rules!`: the exported
/// [`audit_operation`] expands in the caller's crate and so has to be able to name this
/// macro there. It is not part of the public API.
#[doc(hidden)]
#[macro_export]
macro_rules! __audit_emit {
    ({ $($fields:tt)* }, $msg:literal) => {
        $crate::tracing::info!(
            event_source = "audit",
            audit_format = $crate::service::events::backends::audit::AUDIT_FORMAT,
            $($fields)*
            $msg
        )
    };
}

/// Emits an audit record, using singular field names (`action`/`entity`) when only one
/// item is present and plural (`actions`/`entities`) otherwise.
macro_rules! audit_log {
    ($actions:expr, $entities:expr, { $($common:tt)* }, $msg:literal) => {{
        let __actions = $actions;
        let __entities = $entities;
        // A `tracing` field name has to be a literal ident at the invocation, and the
        // name is singular when one item was checked and plural otherwise, so the four
        // combinations cannot be collapsed into one call here. What they no longer do is
        // repeat `event_source` and `audit_format` — every arm funnels into
        // `__audit_emit!`, which is the only place an audit record is emitted.
        match (__actions.len() == 1, __entities.entities.len() == 1) {
            (true, true) => $crate::__audit_emit!({
                action = tracing::field::valuable(&__actions[0].as_value()),
                entity = tracing::field::valuable(&__entities.entities[0].as_value()),
                $($common)*
            }, $msg),
            (true, false) => $crate::__audit_emit!({
                action = tracing::field::valuable(&__actions[0].as_value()),
                entities = tracing::field::valuable(&__entities.as_value()),
                $($common)*
            }, $msg),
            (false, true) => $crate::__audit_emit!({
                actions = tracing::field::valuable(&__actions.as_value()),
                entity = tracing::field::valuable(&__entities.entities[0].as_value()),
                $($common)*
            }, $msg),
            (false, false) => $crate::__audit_emit!({
                actions = tracing::field::valuable(&__actions.as_value()),
                entities = tracing::field::valuable(&__entities.as_value()),
                $($common)*
            }, $msg),
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
/// A top-level `tracing` field, so it is always recorded: `None` becomes `null`, never an
/// absent key. See "Optional fields" in the audit-log section of
/// `docs/docs/developer-guide.md`.
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
            Value::String(self.entity_type.as_str()),
        );
        for field in &self.fields {
            visit.visit_entry(
                Value::String(field.key.as_str()),
                Value::String(&field.value),
            );
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
            visit.visit_entry(Value::String(key.as_str()), value.as_value());
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
        $(context = $ctx:expr,)?
        $msg:literal $(,)?
    ) => {
        $crate::__audit_emit!({
            operation = $op,
            actor = $crate::tracing::field::valuable(&$actor),
            outcome = $outcome,
            // `context` is optional; the `$(...)?` group emits the field only when the
            // caller passed one, which is what keeps the key absent rather than null.
            $(context = $crate::tracing::field::valuable(&$ctx),)?
        }, $msg)
    };
}

/// Rules that hold for every audit record, whatever produced it.
///
/// Available under `test` and the `test-utils` feature so that the unit tests and the
/// integration tests share one implementation. Two copies of these rules would be two
/// things to keep in step, which is the failure this module exists to catch.
///
/// These complement the committed fixtures rather than duplicating them. A fixture pins
/// the exact bytes of one scenario, and is generated by the test that asserts against it —
/// so a wrongly built event yields a fixture that agrees with it and passes for ever. The
/// rules here are statements about the format, so they reject a record that should not
/// exist regardless of which test produced it, including one nobody wrote a fixture for.
#[cfg(any(test, feature = "test-utils"))]
pub mod contract {
    use std::collections::BTreeSet;

    use strum::VariantArray as _;

    use crate::service::events::{
        AuthorizationFailureReason,
        context::{ActionContextKey, EntityField, EntityType},
    };

    /// Keys the log subscriber adds, which `AUDIT_FORMAT` deliberately does not cover.
    pub const ENVELOPE_KEYS: &[&str] = &[
        "timestamp",
        "level",
        "message",
        "target",
        "span",
        "spans",
        "filename",
        "line_number",
    ];

    /// The wire tag of a failure reason. `valuable` tags externally, using the variant name
    /// verbatim, so these must stay identical to it.
    #[deny(clippy::wildcard_enum_match_arm)]
    #[must_use]
    pub fn failure_reason_tag(reason: &AuthorizationFailureReason) -> &'static str {
        match reason {
            AuthorizationFailureReason::ActionForbidden => "ActionForbidden",
            AuthorizationFailureReason::ResourceNotFound => "ResourceNotFound",
            AuthorizationFailureReason::CannotSeeResource => "CannotSeeResource",
            AuthorizationFailureReason::InternalAuthorizationError => "InternalAuthorizationError",
            AuthorizationFailureReason::InternalCatalogError => "InternalCatalogError",
            AuthorizationFailureReason::InvalidRequestData => "InvalidRequestData",
        }
    }

    /// Whether a reason means the request was evaluated and refused, as opposed to never
    /// having reached a verdict.
    ///
    /// Exhaustive rather than a list of literals: a bare `&["ActionForbidden", ...]` stops
    /// matching the moment a variant is renamed, which retires the rule below in silence.
    #[deny(clippy::wildcard_enum_match_arm)]
    const fn is_definitive(reason: &AuthorizationFailureReason) -> bool {
        match reason {
            AuthorizationFailureReason::ActionForbidden
            | AuthorizationFailureReason::ResourceNotFound
            | AuthorizationFailureReason::CannotSeeResource => true,
            AuthorizationFailureReason::InternalAuthorizationError
            | AuthorizationFailureReason::InternalCatalogError
            | AuthorizationFailureReason::InvalidRequestData => false,
        }
    }

    fn definitive_denials() -> Vec<&'static str> {
        AuthorizationFailureReason::VARIANTS
            .iter()
            .filter(|reason| is_definitive(reason))
            .map(failure_reason_tag)
            .collect()
    }

    /// Strip the subscriber-owned envelope, leaving only the fields `AUDIT_FORMAT`
    /// makes promises about, in the order they were emitted.
    ///
    /// `retain` rather than `remove`: with `serde_json`'s `preserve_order` feature (which
    /// this workspace enables) a `Map` is index-backed and `remove` is a *swap*-remove,
    /// which would shuffle the surviving keys. Order is worth keeping — a fixture that
    /// reads in wire order is a fixture a reviewer can check against a real log line.
    #[must_use]
    pub fn contract_fields(mut record: serde_json::Value) -> serde_json::Value {
        if let Some(object) = record.as_object_mut() {
            object.retain(|key, _| !ENVELOPE_KEYS.contains(&key.as_str()));
        }
        record
    }

    /// The values of `singular`/`plural` on one object, with an array flattened to its items.
    fn objects_at<'a>(
        value: &'a serde_json::Value,
        singular: &str,
        plural: &str,
    ) -> Vec<&'a serde_json::Value> {
        let mut out = Vec::new();
        for field in [singular, plural] {
            match value.get(field) {
                Some(serde_json::Value::Array(items)) => out.extend(items),
                Some(value) => out.push(value),
                None => {}
            }
        }
        out
    }

    /// Every place a record carries an entity, or an action: at the top level, and once per
    /// `authorizations` entry.
    ///
    /// Enumerated rather than found by walking the record. A walk also descends into
    /// `properties`, whose keys are client input — so a caller who names a table property
    /// `entity_type` would trip the rules below, and a record that is entirely valid would
    /// be reported as breaking the contract.
    fn described<'a>(
        record: &'a serde_json::Value,
        singular: &str,
        plural: &str,
    ) -> Vec<&'a serde_json::Value> {
        let mut out = objects_at(record, singular, plural);
        if let Some(entries) = record
            .get("authorizations")
            .and_then(serde_json::Value::as_array)
        {
            for entry in entries {
                out.extend(objects_at(entry, singular, plural));
            }
        }
        out
    }

    fn keys_at(record: &serde_json::Value, singular: &str, plural: &str) -> BTreeSet<String> {
        described(record, singular, plural)
            .into_iter()
            .flat_map(object_keys)
            .collect()
    }

    fn object_keys(value: &serde_json::Value) -> Vec<String> {
        value
            .as_object()
            .map(|o| o.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Check one record, returning every rule it breaks.
    ///
    /// Returns violations rather than panicking so a caller can report all of them at once
    /// across a whole corpus, and so the rules themselves stay testable.
    #[must_use]
    pub fn violations(record: &serde_json::Value) -> Vec<String> {
        let mut out = Vec::new();

        if record
            .get("event_source")
            .and_then(serde_json::Value::as_str)
            != Some("audit")
        {
            out.push("`event_source` is not \"audit\"".to_string());
        }
        if record
            .get("audit_format")
            .and_then(serde_json::Value::as_str)
            .is_none()
        {
            out.push(
                "no `audit_format`: every audit record must declare its wire format version"
                    .to_string(),
            );
        }

        let known_entity: BTreeSet<String> = EntityField::VARIANTS
            .iter()
            .map(|f| f.as_str().to_string())
            .chain(["entity_type".to_string()])
            .collect();
        let unknown_entity: Vec<String> = keys_at(record, "entity", "entities")
            .difference(&known_entity)
            .cloned()
            .collect();
        if !unknown_entity.is_empty() {
            out.push(format!(
                "entity keys not in `EntityField`: {unknown_entity:?}. Every key an entity can \
                 carry must be a variant of that enum, so the key space stays enumerable and \
                 documentable"
            ));
        }

        let known_action: BTreeSet<String> = ActionContextKey::VARIANTS
            .iter()
            .map(|k| k.as_str().to_string())
            .chain(["action_name".to_string()])
            .collect();
        let unknown_action: Vec<String> = keys_at(record, "action", "actions")
            .difference(&known_action)
            .cloned()
            .collect();
        if !unknown_action.is_empty() {
            out.push(format!(
                "action context keys not in `ActionContextKey`: {unknown_action:?}. Add a \
                 variant rather than a bare literal, so the key is enumerable and the \
                 documentation test sees it"
            ));
        }

        // Only where an entity actually is. See `described`: a walk would also read
        // client-supplied property keys.
        let known_type: BTreeSet<&str> = EntityType::VARIANTS.iter().map(|t| t.as_str()).collect();
        for entity in described(record, "entity", "entities") {
            if let Some(serde_json::Value::String(kind)) = entity.get("entity_type")
                && !known_type.contains(kind.as_str())
            {
                out.push(format!("`entity_type` is `{kind}`, not in `EntityType`"));
            }
        }

        out.extend(failure_reason_violations(record));

        out
    }

    /// The rules that relate `failure_reason` to the rest of the record.
    ///
    /// Split out of [`violations`] only for length; they belong to the same contract.
    fn failure_reason_violations(record: &serde_json::Value) -> Vec<String> {
        let mut out = Vec::new();
        let Some(reason) = record.get("failure_reason") else {
            return out;
        };

        // Independent of how `failure_reason` is encoded, so it is checked before the
        // shape rule below returns.
        if record.get("decision").and_then(serde_json::Value::as_str) != Some("denied") {
            out.push("`failure_reason` is present but `decision` is not `denied`".to_string());
        }

        // `failure_reason` is externally tagged today, so the definitive-denial rule below
        // reads the variant from the object's key. Re-encoding it — as a plain string, say,
        // which the audit log section of `docs/docs/developer-guide.md` lists as an open
        // issue for the next major version — would make `as_object` return `None` and
        // silently retire that rule. The re-encoding itself is loud (the fixture diff shows
        // it); losing the rule with it would not be. So trip here, and make the bump
        // re-teach the rule rather than drop it.
        let Some(tagged) = reason.as_object() else {
            out.push(format!(
                "`failure_reason` is `{reason}`, not an object. The definitive-denial rule \
                 reads the variant from this object's key, so a re-encoding disables it: \
                 teach that rule the new encoding, then update this one"
            ));
            return out;
        };

        // A definitive denial means the request was evaluated and refused, so no per-decision
        // entry may claim it was allowed. This is the rule a fixture cannot state: it relates
        // two fields, and a fixture only ever records one combination of them.
        let definitive_denials = definitive_denials();
        let definitive = tagged
            .keys()
            .any(|k| definitive_denials.contains(&k.as_str()));
        if definitive
            && record
                .get("authorizations")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|entries| {
                    entries.iter().any(|entry| {
                        entry.get("allowed").and_then(serde_json::Value::as_bool) == Some(true)
                    })
                })
        {
            out.push(
                "a definitive denial carries an `authorizations` entry with `allowed: true`. The \
                 emitter cannot produce that, so either the record is wrong or this rule is"
                    .to_string(),
            );
        }

        out
    }

    /// Assert `record` satisfies the contract, naming every rule it breaks.
    ///
    /// `whence` identifies the record in the failure message — a fixture name, or an index
    /// into a captured corpus.
    ///
    /// # Panics
    ///
    /// If `record` breaks any rule. That is the point: this is for use in tests.
    pub fn assert_satisfies(record: &serde_json::Value, whence: &str) {
        let violations = violations(record);
        assert!(
            violations.is_empty(),
            "{whence} breaks the audit format contract:\n  - {}\n\nrecord:\n{}",
            violations.join("\n  - "),
            serde_json::to_string_pretty(record).unwrap_or_else(|_| "<unserialisable>".to_string()),
        );
    }
}

#[cfg(test)]
mod tests;
