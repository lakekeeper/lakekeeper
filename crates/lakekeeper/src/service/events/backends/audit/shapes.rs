//! The three shapes an audit record can have, and the one `tracing::info!` each ends in.
//!
//! A shape is what a consumer receives. Its `emit()` is the only code that writes an audit
//! record: it stamps `event_source` and `audit_format`, passes scalar fields as `tracing`
//! fields, and hands every object field to `tracing` as a JSON tree through the bridge.
//!
//! Field lists are the ones the previous emit macros wrote, so the output is identical to the
//! previous implementation's: `action`/`actions` and `entity`/`entities` switch on count,
//! `user_agent` and `idempotency_key` render `null` when absent, and a replay record carries
//! `operation` and `outcome` as its markers. The deliberate changes to that shape are a later,
//! separately versioned step.

use tracing::field::valuable;

use super::{
    AUDIT_FORMAT, AuditOperation, AuditOutcome, Decision,
    parts::{ActionRecord, ActorRecord, DecisionRecord, EntityRecord, ErrorRecord, HandlerContext},
    render::AuditJson,
};
use crate::{
    audit::{AuditEmitter, AuditPart, WireStr},
    request_metadata::PrivilegeSource,
};

/// The `tracing` target of every audit record. The module path the previous implementation
/// emitted from, kept so operator filters keep matching; the move to a stable, documented
/// target is a versioned change of its own.
const TARGET: &str = "lakekeeper::service::events::backends::audit";

/// Emit one `tracing::info!` with the singular or plural `action`/`entity` fields, by count.
///
/// `tracing` needs literal field names at the call site, so the four arities are four calls;
/// everything else is shared. Local to this module; it disappears with the arity switch.
macro_rules! emit_with_arity {
    ($actions:expr, $entities:expr, { $($fields:tt)* }, $msg:expr) => {{
        let actions: &[ActionRecord] = $actions;
        let entities: &[EntityRecord] = $entities;
        let actions_json = AuditJson::of(actions);
        let entities_json = AuditJson::of(entities);
        let action_json = actions.first().map(AuditJson::of);
        let entity_json = entities.first().map(AuditJson::of);
        match (actions.len() == 1, entities.len() == 1) {
            (true, true) => tracing::info!(
                target: TARGET,
                event_source = "audit",
                audit_format = AUDIT_FORMAT,
                action = valuable(action_json.as_ref().expect("one action")),
                entity = valuable(entity_json.as_ref().expect("one entity")),
                $($fields)*
                "{}", $msg
            ),
            (true, false) => tracing::info!(
                target: TARGET,
                event_source = "audit",
                audit_format = AUDIT_FORMAT,
                action = valuable(action_json.as_ref().expect("one action")),
                entities = valuable(&entities_json),
                $($fields)*
                "{}", $msg
            ),
            (false, true) => tracing::info!(
                target: TARGET,
                event_source = "audit",
                audit_format = AUDIT_FORMAT,
                actions = valuable(&actions_json),
                entity = valuable(entity_json.as_ref().expect("one entity")),
                $($fields)*
                "{}", $msg
            ),
            (false, false) => tracing::info!(
                target: TARGET,
                event_source = "audit",
                audit_format = AUDIT_FORMAT,
                actions = valuable(&actions_json),
                entities = valuable(&entities_json),
                $($fields)*
                "{}", $msg
            ),
        }
    }};
}

/// An authorization record: was this caller permitted to do these actions on these entities?
#[derive(Debug, Clone, PartialEq)]
pub struct AuthorizationRecord {
    pub(crate) actions: Vec<ActionRecord>,
    pub(crate) entities: Vec<EntityRecord>,
    pub(crate) actor: ActorRecord,
    pub(crate) privilege_source: PrivilegeSource,
    pub(crate) user_agent: Option<String>,
    pub(crate) break_glass: Option<String>,
    pub(crate) context: Option<HandlerContext>,
    pub(crate) authorizations: Vec<DecisionRecord>,
    pub(crate) idempotency_key: Option<String>,
    pub(crate) decision: Decision,
    /// The failure reason of a denied record, pre-rendered from its `valuable` form.
    pub(crate) failure_reason: Option<serde_json::Value>,
    pub(crate) error: Option<ErrorRecord>,
}

impl AuthorizationRecord {
    pub(crate) fn emit(self) {
        let actor = AuditJson::of(&self.actor);
        let authorizations = AuditJson::of(&self.authorizations);
        let context = self.context.as_ref().map(AuditJson::of);
        let failure_reason = self.failure_reason.clone().map(AuditJson::from);
        let error = self.error.as_ref().map(AuditJson::of);
        let user_agent = self.user_agent.as_deref();
        let idempotency_key = self.idempotency_key.as_deref();
        let message = match self.decision {
            Decision::Allowed => "Authorization succeeded event",
            Decision::Denied => "Authorization failed event",
        };
        emit_with_arity!(
            &self.actions,
            &self.entities,
            {
                actor = valuable(&actor),
                privilege_source = self.privilege_source.as_str(),
                user_agent = valuable(&user_agent),
                break_glass = self.break_glass.as_deref(),
                failure_reason = failure_reason.as_ref().map(valuable),
                error = error.as_ref().map(valuable),
                context = context.as_ref().map(valuable),
                authorizations = valuable(&authorizations),
                idempotency_key = valuable(&idempotency_key),
                decision = self.decision.as_str(),
            },
            message
        );
    }
}

/// A replay record: a retry answered from an idempotency record, so no authorization ran.
#[derive(Debug, Clone, PartialEq)]
pub struct ReplayRecord {
    pub(crate) actions: Vec<ActionRecord>,
    pub(crate) entities: Vec<EntityRecord>,
    pub(crate) actor: ActorRecord,
    pub(crate) privilege_source: PrivilegeSource,
    pub(crate) user_agent: Option<String>,
    pub(crate) idempotency_key: String,
}

impl ReplayRecord {
    pub(crate) fn emit(self) {
        let actor = AuditJson::of(&self.actor);
        let user_agent = self.user_agent.as_deref();
        emit_with_arity!(
            &self.actions,
            &self.entities,
            {
                actor = valuable(&actor),
                privilege_source = self.privilege_source.as_str(),
                user_agent = valuable(&user_agent),
                operation = AuditOperation::IdempotentReplay.as_str(),
                idempotency_key = self.idempotency_key.as_str(),
                outcome = AuditOutcome::Replayed.as_str(),
            },
            "Idempotent replay served"
        );
    }
}

/// An operation record: something the system did that touches identity or access, with no
/// permission decision of its own. The one shape any crate can emit.
///
/// `E` is the emitter. `operation`, `outcome` and `context` must all belong to it, or the
/// record does not compile; `E` is what stamps the emitter on the record once that field
/// exists.
#[derive(Debug)]
pub struct OperationRecord<E: AuditEmitter, C: AuditPart<Emitter = E>> {
    operation: WireStr<E>,
    actor: ActorRecord,
    outcome: WireStr<E>,
    context: Option<C>,
    message: &'static str,
}

impl<E: AuditEmitter> OperationRecord<E, E::NoContext> {
    /// A record without context. Add one with [`OperationRecord::context`].
    #[must_use]
    pub fn new(operation: WireStr<E>, actor: ActorRecord, outcome: WireStr<E>) -> Self {
        Self {
            operation,
            actor,
            outcome,
            context: None,
            message: "Audit operation",
        }
    }
}

impl<E: AuditEmitter, C: AuditPart<Emitter = E>> OperationRecord<E, C> {
    /// Attach the operation's `context` object, an audit type of the same emitter.
    #[must_use]
    pub fn context<C2: AuditPart<Emitter = E>>(self, context: C2) -> OperationRecord<E, C2> {
        OperationRecord {
            operation: self.operation,
            actor: self.actor,
            outcome: self.outcome,
            context: Some(context),
            message: self.message,
        }
    }

    /// The human-readable `message` of the log line. Outside `audit_format`.
    #[must_use]
    pub fn message(mut self, message: &'static str) -> Self {
        self.message = message;
        self
    }

    /// Write the record.
    pub fn emit(self) {
        let actor = AuditJson::of(&self.actor);
        let context = self.context.as_ref().map(AuditJson::of);
        tracing::info!(
            target: TARGET,
            event_source = "audit",
            audit_format = AUDIT_FORMAT,
            operation = self.operation.text(),
            actor = valuable(&actor),
            outcome = self.outcome.text(),
            context = context.as_ref().map(valuable),
            "{}",
            self.message
        );
    }
}
