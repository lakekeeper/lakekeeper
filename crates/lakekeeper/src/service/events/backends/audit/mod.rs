use std::fmt::Display;

use crate::audit::audit_part;

pub mod assemble;
pub mod email;
pub mod emitter;
pub mod part;
pub mod parts;
pub mod render;
#[cfg(any(test, feature = "test-utils"))]
pub mod schema;
pub mod shapes;
#[cfg(any(test, feature = "test-utils"))]
pub mod validate;

use std::sync::Arc;

use email::Emails;
pub use email::user_email;
pub use emitter::{AuditEmitter, EmitterStamp, is_emitter_name};
pub use part::{
    AUDIT_TARGET, AnyWireStr, AuditPart, Kind, OperationValues, OutcomeValues, RecordContextKey,
    Registration, Vocabulary, Wire, WireKey, WireName, enabled, warn_on_retired_audit_filter,
};
use parts::include_user_email;
pub use parts::{
    ActionRecord, ActorRecord, AssumedRoleRecord, DecisionRecord, EntityRecord, ErrorRecord,
    GrantContextRecord, HandlerContext, RoleSubjectRecord, SubjectRecord, UserSubjectRecord,
};
pub use render::{AuditJson, log_format};
pub use shapes::{AuthorizationRecord, OperationRecord, RecordOrigin, ReplayRecord};

use crate::{
    request_metadata::RequestMetadata,
    service::{
        UserId,
        authz::UserOrRoleId,
        events::{
            AuthorizationFailedEvent, AuthorizationSucceededEvent, EventCatalog, EventListener,
            GrantsChangedEvent, IdempotentReplayEvent,
        },
    },
};

/// The `MAJOR.MINOR` version of the audit record's shape, carried on every
/// `event_source = "audit"` record as `audit_format`.
///
/// Derived from `audit-format/` by the format checker; never edit it, write a fragment. See
/// the audit log section of `docs/docs/developer-guide.md`.
pub const AUDIT_FORMAT: &str = "1.0";

/// The `event_source` every audit record carries: what marks a log line as one.
pub const EVENT_SOURCE: &str = "audit";

/// Whether `s` is exactly `MAJOR.MINOR`. Hand-rolled over bytes because `==` on `&str` is
/// not const-evaluable (rust-lang/rust#143874).
#[must_use]
pub const fn is_major_minor(s: &str) -> bool {
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

/// The `actor_type` value on every audit record.
#[audit_part(field = "actor_type")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, strum_macros::VariantNames)]
#[strum(serialize_all = "snake_case")]
pub enum ActorType {
    Anonymous,
    Principal,
    AssumedRole,
    LakekeeperInternal,
}

/// The `record_type` value: which shape a record has. Consumers route on this field alone.
#[audit_part(field = "record_type")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, strum_macros::VariantNames)]
#[strum(serialize_all = "snake_case")]
pub enum RecordType {
    /// Was this caller permitted to do these actions on these entities?
    Authorization,
    /// A retry answered from an idempotency record, so no authorization ran.
    Replay,
    /// Something the system did that touches identity or access.
    Operation,
}

/// The `decision` value on an authorization record.
#[audit_part(field = "decision", closed)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, strum_macros::VariantNames)]
#[strum(serialize_all = "snake_case")]
pub enum Decision {
    Allowed,
    Denied,
}

/// The `operation` value on the operational records this crate emits.
///
/// Not the whole `operation` space: [`OperationRecord`] takes a value from any operation
/// vocabulary of its emitter, so another crate declares its own operations (see the audit log
/// section of `docs/docs/developer-guide.md`). This enum puts Lakekeeper's own operations
/// under the rename check.
#[audit_part(field = "operation")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, strum_macros::VariantNames)]
#[strum(serialize_all = "snake_case")]
pub enum AuditOperation {
    AdmissionDecided,
    GrantCreated,
    GrantRevoked,
}

/// The `outcome` value on the operational records this crate emits. Open to other crates in
/// the same way [`AuditOperation`] is.
#[audit_part(field = "outcome")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, strum_macros::VariantNames)]
#[strum(serialize_all = "snake_case")]
pub enum AuditOutcome {
    /// The operation completed.
    Success,
    /// An admission gate denied the caller authoritatively.
    Forbidden,
    /// An admission gate could not reach an upstream it needs and failed closed: an outage,
    /// not a denial.
    Unavailable,
}

// `TableUpdateKind` reaches the wire as the `update_kinds` field of a commit action's
// context. It lives in `iceberg-ext`, which cannot carry the attribute: the expansion names
// `::lakekeeper`, which depends on `iceberg-ext`. Registered here by hand from its
// `VariantNames`, so a renamed variant still fails the format check.
//
// The values are the Iceberg REST specification's table-update action names, so they are
// external and keep that spelling.
#[cfg(debug_assertions)]
const UPDATE_KIND_TEXTS: &[&str] =
    <iceberg_ext::catalog::TableUpdateKind as strum::VariantNames>::VARIANTS;
#[cfg(debug_assertions)]
const UPDATE_KIND_COUNT: usize = UPDATE_KIND_TEXTS.len();
#[cfg(debug_assertions)]
static UPDATE_KIND_NAMES: [crate::audit::WireName; UPDATE_KIND_COUNT] =
    crate::audit::WireName::from_texts(UPDATE_KIND_TEXTS);

#[cfg(debug_assertions)]
crate::__private::inventory::submit! {
    Registration {
        kind: Kind::Values {
            field: "update_kinds",
            names: &UPDATE_KIND_NAMES,
            closed: false,
        },
        type_name: || core::any::type_name::<iceberg_ext::catalog::TableUpdateKind>(),
        emitter: EmitterStamp::of::<crate::Lakekeeper>(),
        defining_crate: env!("CARGO_PKG_NAME"),
        external_values: true,
        def_name: || std::borrow::Cow::Borrowed("TableUpdateKind"),
        schema: None,
    }
}

// A vocabulary by hand, since its crate cannot carry the attribute. `VariantNames` lists the
// names in declaration order, which is also the order of the discriminants.
impl Vocabulary for iceberg_ext::catalog::TableUpdateKind {
    type Emitter = crate::Lakekeeper;
    const SCHEMA_NAME: &'static str = "TableUpdateKind";
    fn wire(&self) -> Wire<Self> {
        Wire::new(<Self as strum::VariantNames>::VARIANTS[*self as usize])
    }
}

/// The audit backend: renders events into audit records and writes them as log lines.
///
/// One method per event kind it records. Each asks [`crate::audit::enabled`] before it does
/// any work, then assembles the record's shape from the event ([`assemble`]) and calls the
/// shape's `emit()`; nothing else in this crate writes an audit record.
///
/// The gate is asked here as well as inside `emit()` because assembly is the expensive half:
/// it enriches the event and serializes every nested object. With the audit trail switched
/// off, a request pays nothing.
#[derive(Debug, Default, Clone)]
pub struct AuditEventListener {
    /// Read access to the catalog, set once at startup and never changed: records read the
    /// emails of user principals the token does not cover from it. `None` puts only the
    /// token's email on a record.
    catalog: Option<Arc<dyn EventCatalog>>,
}

impl AuditEventListener {
    /// A listener that puts only the token's email on a record.
    #[must_use]
    pub const fn new() -> Self {
        Self { catalog: None }
    }

    /// A listener that reads from `catalog`, such as the emails of user principals when the
    /// operator enabled them.
    #[must_use]
    pub fn with_catalog(catalog: Arc<dyn EventCatalog>) -> Self {
        Self {
            catalog: Some(catalog),
        }
    }

    async fn emails<'a>(
        &self,
        request_metadata: &'a RequestMetadata,
        principals: impl IntoIterator<Item = &'a UserId>,
    ) -> Emails {
        Emails::resolve(self.catalog.as_deref(), request_metadata, principals).await
    }
}

impl Display for AuditEventListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AuditEventListener")
    }
}

/// One grant record per triple, not one per request: the batch is a dispatch optimisation,
/// while the audit trail is answered per grant.
fn emit_grant_records(event: &GrantsChangedEvent, emails: &Emails) {
    let actor = || assemble::actor(&event.request_metadata, emails);
    for (specs, operation, message) in [
        (
            &event.removed,
            AuditOperation::GrantRevoked,
            "Grant revoked",
        ),
        (
            &event.created,
            AuditOperation::GrantCreated,
            "Grant created",
        ),
    ] {
        for spec in specs {
            OperationRecord::new(
                operation.as_wire(),
                RecordOrigin::of_request(&event.request_metadata, actor()),
                AuditOutcome::Success.as_wire(),
            )
            .context(GrantContextRecord::new(
                assemble::subject(&spec.principal, emails),
                &spec.privilege,
                &spec.resource,
            ))
            .message(message)
            .emit();
        }
    }
}

#[async_trait::async_trait]
impl EventListener for AuditEventListener {
    async fn authorization_failed(&self, event: AuthorizationFailedEvent) -> anyhow::Result<()> {
        if !enabled() {
            return Ok(());
        }
        let emails = self
            .emails(
                &event.request_metadata,
                assemble::decision_subjects(&event.authorizations),
            )
            .await;
        assemble::authorization_failed(&event, &emails).emit("Authorization failed event");
        Ok(())
    }

    /// The grants that actually landed.
    ///
    /// The authorization event records the *attempt*, with principals and privileges in
    /// separate deduplicated lists, so it cannot say which principal received which
    /// privilege. This records the confirmed triples. A revoked grant is hard-deleted, so its
    /// record here is the only remaining evidence the access existed.
    ///
    /// The grant endpoints wait for this listener, so with emails enabled the lookup and the
    /// records move to a task of their own. The records of a deleted user's grants carry no
    /// email for that user: the delete cleared it before they are written.
    async fn grants_changed(&self, event: GrantsChangedEvent) -> anyhow::Result<()> {
        if !enabled() {
            return Ok(());
        }
        let Some(catalog) = self.catalog.clone().filter(|_| include_user_email()) else {
            emit_grant_records(&event, &Emails::default());
            return Ok(());
        };
        tokio::spawn(async move {
            let principals = event
                .removed
                .iter()
                .chain(&event.created)
                .filter_map(|spec| match &spec.principal {
                    UserOrRoleId::User(user_id) => Some(user_id),
                    UserOrRoleId::Role(_) => None,
                });
            let emails =
                Emails::resolve(Some(catalog.as_ref()), &event.request_metadata, principals).await;
            emit_grant_records(&event, &emails);
        });
        Ok(())
    }

    async fn authorization_succeeded(
        &self,
        event: AuthorizationSucceededEvent,
    ) -> anyhow::Result<()> {
        if !enabled() {
            return Ok(());
        }
        let emails = self
            .emails(
                &event.request_metadata,
                assemble::decision_subjects(&event.authorizations),
            )
            .await;
        assemble::authorization_succeeded(&event, &emails).emit("Authorization succeeded event");
        Ok(())
    }

    /// A retry answered from an idempotency record.
    ///
    /// Carries `actions` and `entities` in the same shape as an authorization record, so one
    /// query finds the original request and every replay of it. No `decision`: no
    /// authorization ran, the mutation had already happened.
    async fn idempotent_replay_served(&self, event: IdempotentReplayEvent) -> anyhow::Result<()> {
        if !enabled() {
            return Ok(());
        }
        let emails = self.emails(&event.request_metadata, []).await;
        assemble::replay(&event, &emails).emit("Idempotent replay served");
        Ok(())
    }
}

#[cfg(test)]
mod tests;
