//! The nested objects of an audit record.
//!
//! Each is an audit type: `#[audit_part]` gives it serde, a schema and a registry entry.
//! Assembly builds them from the event payload and its enrichment.

use std::{borrow::Cow, collections::BTreeMap, sync::Arc};

use serde::ser::SerializeMap as _;

use super::ActorType;
use crate::{
    audit::{AnyWireStr, Wire, audit_part},
    request_metadata::RequestMetadata,
    service::{
        ArcRoleIdent, RoleId, UserId,
        authn::{Actor, InternalActor},
        authz::{GrantResource, ResourceType, UserOrRoleId},
        events::{
            AuthorizationError,
            context::{EntityDescriptorField, EntityType},
        },
    },
};

/// The `actor` object: who made the request, as authentication established it.
#[audit_part]
#[derive(Debug, Clone, PartialEq)]
pub struct ActorRecord {
    /// One of `anonymous`, `principal`, `assumed_role`, `lakekeeper_internal`.
    pub(crate) actor_type: Wire<ActorType>,
    /// The authenticated principal. Present for `principal` and `assumed_role`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) principal: Option<String>,
    /// The role acted as. Present for `assumed_role`; `principal` is still the human.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) assumed_role: Option<AssumedRoleRecord>,
    /// The principal's email, best-effort: only when the operator enabled it, and absent
    /// whenever it is not known. Metadata, not identity: correlate on `principal`.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<String>")]
    pub(crate) email: Option<Arc<str>>,
}

impl ActorRecord {
    /// The request's resolved actor. The one way to put a request's caller on a record, so
    /// every record raised while serving it agrees on who the caller is.
    ///
    /// With emails enabled, carries the email from the caller's token when it has one.
    #[must_use]
    pub fn from_request(request_metadata: &RequestMetadata) -> Self {
        Self::from_internal_actor(request_metadata.internal_actor())
            .with_email(claims_email(request_metadata))
    }

    /// This actor with `email` for its principal. Only a `principal` or `assumed_role` actor
    /// carries one, and only with emails enabled; `None` keeps the email it has. A `&str` is
    /// copied only when it is kept; an `Arc<str>` is shared.
    #[must_use]
    pub fn with_email(mut self, email: Option<impl Into<Arc<str>>>) -> Self {
        if let Some(email) = email
            && self.principal.is_some()
            && include_user_email()
        {
            self.email = Some(email.into());
        }
        self
    }

    /// Whether this actor is a principal that has no email yet.
    pub(crate) fn lacks_email(&self) -> bool {
        self.principal.is_some() && self.email.is_none()
    }

    /// A bare principal, for records raised without a request: role resolution, syncs.
    #[must_use]
    pub fn principal(id: &UserId) -> Self {
        Self {
            actor_type: ActorType::Principal.as_wire(),
            principal: Some(id.to_string()),
            assumed_role: None,
            email: None,
        }
    }

    pub(crate) fn from_internal_actor(actor: &InternalActor) -> Self {
        match actor {
            InternalActor::LakekeeperInternal => Self {
                actor_type: ActorType::LakekeeperInternal.as_wire(),
                principal: None,
                assumed_role: None,
                email: None,
            },
            InternalActor::External(actor) => Self::from_actor(actor),
        }
    }

    pub(crate) fn from_actor(actor: &Actor) -> Self {
        match actor {
            Actor::Anonymous => Self {
                actor_type: ActorType::Anonymous.as_wire(),
                principal: None,
                assumed_role: None,
                email: None,
            },
            Actor::Principal(user_id) => Self::principal(user_id),
            Actor::Role {
                principal,
                assumed_role,
            } => Self {
                actor_type: ActorType::AssumedRole.as_wire(),
                principal: Some(principal.to_string()),
                assumed_role: Some(AssumedRoleRecord {
                    role_id: assumed_role.id,
                    provider_id: Arc::clone(&assumed_role.ident),
                    source_id: include_role_source_id().then(|| Arc::clone(&assumed_role.ident)),
                }),
                email: None,
            },
        }
    }
}

/// Whether audit records name a role's source id next to its id and provider.
#[must_use]
pub fn include_role_source_id() -> bool {
    #[cfg(any(test, feature = "test-utils"))]
    if OMIT_ROLE_SOURCE_ID_IN_TESTS.get().is_some() {
        return false;
    }
    crate::CONFIG.audit.tracing.include_role_source_id
}

#[cfg(any(test, feature = "test-utils"))]
static OMIT_ROLE_SOURCE_ID_IN_TESTS: std::sync::OnceLock<()> = std::sync::OnceLock::new();

/// Make every audit record in this process name roles without their source id, whatever
/// the configuration says. For a test binary of its own: it cannot be undone, and it
/// reaches every test in the process.
#[cfg(any(test, feature = "test-utils"))]
pub fn omit_role_source_id_in_tests() {
    let _ = OMIT_ROLE_SOURCE_ID_IN_TESTS.set(());
}

/// Whether the operator enabled emails on audit records.
pub(crate) fn include_user_email() -> bool {
    #[cfg(any(test, feature = "test-utils"))]
    if INCLUDE_USER_EMAIL_IN_TESTS.get().is_some() {
        return true;
    }
    crate::CONFIG.audit.tracing.include_user_email
}

#[cfg(any(test, feature = "test-utils"))]
static INCLUDE_USER_EMAIL_IN_TESTS: std::sync::OnceLock<()> = std::sync::OnceLock::new();

/// Make every audit record in this process carry emails, whatever the configuration says.
/// For a test binary of its own: it cannot be undone, and it reaches every test in the
/// process.
#[cfg(any(test, feature = "test-utils"))]
pub fn include_user_email_in_tests() {
    let _ = INCLUDE_USER_EMAIL_IN_TESTS.set(());
}

/// The email in the caller's token, with emails enabled. It belongs to the token's
/// principal, `request_metadata.user_id()`.
pub(crate) fn claims_email(request_metadata: &RequestMetadata) -> Option<&str> {
    if !include_user_email() {
        return None;
    }
    request_metadata
        .authentication()?
        .email()
        .filter(|email| !email.is_empty())
}

/// The `emitters` object: every product that contributed to a record, keyed by its name, with
/// the version of what it contributes as the value.
///
/// A consumer routes the core shape on `audit_format`, and everything an emitter owns — its
/// `context` keys, its vocabulary, any shape it defines — on the entry naming it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Emitters(BTreeMap<&'static str, &'static str>);

impl Emitters {
    /// The emitter that assembled a record, and each one whose vocabulary supplied a name in
    /// it.
    pub(crate) fn of(
        assembler: crate::audit::EmitterStamp,
        contributed: impl IntoIterator<Item = crate::audit::EmitterStamp>,
    ) -> Self {
        Self(
            std::iter::once(assembler)
                .chain(contributed)
                .map(|stamp| (stamp.name, stamp.format))
                .collect(),
        )
    }
}

impl serde::Serialize for Emitters {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl schemars::JsonSchema for Emitters {
    fn inline_schema() -> bool {
        true
    }
    fn schema_name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("Emitters")
    }
    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "object",
            "minProperties": 1,
            "propertyNames": { "pattern": "^[a-z][a-z0-9]*(_[a-z0-9]+)*$" },
            "additionalProperties": { "type": "string", "pattern": "^[0-9]+\\.[0-9]+$" }
        })
    }
}

/// When a record's event happened, in UTC.
///
/// Written as RFC 3339 with microseconds and a `Z`, so every record spells a time the same
/// way and a lexical sort is a time sort.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordTime(chrono::DateTime<chrono::Utc>);

impl RecordTime {
    /// Now.
    #[must_use]
    pub fn now() -> Self {
        Self(chrono::Utc::now())
    }
}

impl From<chrono::DateTime<chrono::Utc>> for RecordTime {
    fn from(time: chrono::DateTime<chrono::Utc>) -> Self {
        Self(time)
    }
}

impl serde::Serialize for RecordTime {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0.to_rfc3339_opts(chrono::SecondsFormat::Micros, true))
    }
}

impl schemars::JsonSchema for RecordTime {
    fn inline_schema() -> bool {
        true
    }
    fn schema_name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("RecordTime")
    }
    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({ "type": "string", "format": "date-time" })
    }
}

/// The role an `assumed_role` actor acts as.
#[audit_part]
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_field_names)]
pub struct AssumedRoleRecord {
    /// The role's id in this catalog.
    #[schemars(with = "String")]
    pub(crate) role_id: RoleId,
    /// The provider that supplied the role.
    #[serde(serialize_with = "role_provider_id")]
    #[schemars(with = "String")]
    pub(crate) provider_id: ArcRoleIdent,
    /// The role's id at the provider. Absent when the operator turned role source ids off.
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "role_source_id"
    )]
    #[schemars(with = "Option<String>")]
    pub(crate) source_id: Option<ArcRoleIdent>,
}

// A role's provider and source id fields hold the role's shared ident, and write their part
// of it: a record copies no string of a role.

/// Write the provider id of the role `ident`.
fn role_provider_id<'a, S: serde::Serializer>(
    ident: impl Into<Option<&'a ArcRoleIdent>>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match ident.into() {
        Some(ident) => serializer.serialize_str(ident.provider_id().as_str()),
        None => serializer.serialize_none(),
    }
}

/// Write the source id of the role `ident`.
fn role_source_id<'a, S: serde::Serializer>(
    ident: impl Into<Option<&'a ArcRoleIdent>>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match ident.into() {
        Some(ident) => serializer.serialize_str(ident.source_id().as_str()),
        None => serializer.serialize_none(),
    }
}

/// A principal named as a target: `for_principal` on a decision entry, `principal` on a grant
/// record. `{"user": …}` or `{"role": …}`.
#[audit_part]
#[serde(untagged)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubjectRecord<'a> {
    /// A user.
    User(UserSubjectRecord<'a>),
    /// A role.
    Role(RoleSubjectRecord),
}

impl SubjectRecord<'static> {
    /// The subject `id`, owning its id: for an action's context, which outlives the request.
    pub(crate) fn from_id(id: &UserOrRoleId) -> Self {
        SubjectRecord::of(id).into_owned()
    }
}

impl<'a> SubjectRecord<'a> {
    /// The subject `id`, borrowing its id.
    pub(crate) fn of(id: &'a UserOrRoleId) -> Self {
        match id {
            UserOrRoleId::User(user) => Self::User(UserSubjectRecord {
                user: Cow::Borrowed(user),
                email: None,
            }),
            UserOrRoleId::Role(role) => Self::Role(RoleSubjectRecord {
                role: *role,
                provider_id: None,
                source_id: None,
            }),
        }
    }

    /// This subject, borrowing its user id from `self`.
    pub(crate) fn reborrow(&self) -> SubjectRecord<'_> {
        match self {
            Self::User(user) => SubjectRecord::User(UserSubjectRecord {
                user: Cow::Borrowed(&*user.user),
                email: user.email.clone(),
            }),
            Self::Role(role) => SubjectRecord::Role(role.clone()),
        }
    }

    fn into_owned(self) -> SubjectRecord<'static> {
        match self {
            Self::User(user) => SubjectRecord::User(UserSubjectRecord {
                user: Cow::Owned(user.user.into_owned()),
                email: user.email,
            }),
            Self::Role(role) => SubjectRecord::Role(role),
        }
    }
}

/// A user named as a target.
#[audit_part]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserSubjectRecord<'a> {
    /// The user's principal id.
    #[schemars(with = "String")]
    pub(crate) user: Cow<'a, UserId>,
    /// The user's email, best-effort: only when the operator enabled it, and absent
    /// whenever it is not known. Metadata, not identity: correlate on `user`.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<String>")]
    pub(crate) email: Option<Arc<str>>,
}

/// A role named as a target.
#[audit_part]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleSubjectRecord {
    /// The role's id.
    #[schemars(with = "String")]
    pub(crate) role: RoleId,
    /// The provider that supplied the role. Absent when it is not known: the role no longer
    /// exists, or it could not be looked up.
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "role_provider_id"
    )]
    #[schemars(with = "Option<String>")]
    pub(crate) provider_id: Option<ArcRoleIdent>,
    /// The role's id at the provider, for looking it up there. Absent when `provider_id` is,
    /// or when the operator turned role source ids off.
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "role_source_id"
    )]
    #[schemars(with = "Option<String>")]
    pub(crate) source_id: Option<ArcRoleIdent>,
}

/// One entry of `authorizations[]`: which action on which entity was evaluated, for whom,
/// with what result.
#[audit_part]
#[derive(Debug, Clone, PartialEq)]
pub struct DecisionRecord<'a> {
    /// The client's id for this check in a batch, or its index. Absent for single checks.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) id: Option<&'a str>,
    /// The principal whose permission was evaluated, when it is not the request's actor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) for_principal: Option<SubjectRecord<'a>>,
    /// The action evaluated.
    pub(crate) action: ActionRecord,
    /// The entity the action was evaluated against.
    pub(crate) entity: EntityRecord<'a>,
    /// The authorizer's answer. Absent when an upstream error stopped the evaluation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) allowed: Option<bool>,
    /// The policies or rules that determined the decision. Empty when the authorizer
    /// reports none. The same shape the management API returns for a check.
    pub(crate) determined_by: &'a [crate::service::authz::DeterminingFactor],
}

/// An `action` object: the wire name and the action's context fields.
#[audit_part]
#[derive(Debug, Clone, PartialEq)]
pub struct ActionRecord {
    /// What was attempted. One of the action names this schema lists; a product that plugs
    /// into Lakekeeper may contribute its own.
    pub(crate) action_name: AnyWireStr,
    /// The action's context fields, in the order the action recorded them.
    #[serde(flatten)]
    pub(crate) context: OrderedFields<serde_json::Value>,
}

/// An `entity` object: the kind of resource and its identifying fields.
#[audit_part]
#[derive(Debug, Clone, PartialEq)]
pub struct EntityRecord<'a> {
    /// The kind of resource: `table`, `namespace`, `warehouse`, …
    pub(crate) entity_type: Wire<EntityType>,
    /// The entity's identifying fields, in the order the entity recorded them.
    #[serde(flatten)]
    pub(crate) fields: EntityFields<'a>,
}

/// An entity's identifying fields, read from its descriptor: a map from field name to value,
/// in the order the descriptor holds them.
#[derive(Debug, Clone, Copy)]
pub struct EntityFields<'a>(pub(crate) &'a [EntityDescriptorField]);

impl PartialEq for EntityFields<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.0.len() == other.0.len()
            && self
                .0
                .iter()
                .zip(other.0)
                .all(|(a, b)| a.key.as_str() == b.key.as_str() && a.value == b.value)
    }
}

impl serde::Serialize for EntityFields<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for field in self.0 {
            map.serialize_entry(field.key.as_str(), &field.value)?;
        }
        map.end()
    }
}

impl schemars::JsonSchema for EntityFields<'_> {
    fn inline_schema() -> bool {
        <OrderedFields<String> as schemars::JsonSchema>::inline_schema()
    }
    fn schema_name() -> std::borrow::Cow<'static, str> {
        <OrderedFields<String> as schemars::JsonSchema>::schema_name()
    }
    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        <OrderedFields<String> as schemars::JsonSchema>::json_schema(generator)
    }
}

/// The `error` object of a denied authorization record.
#[audit_part]
#[derive(Debug, Clone, PartialEq)]
pub struct ErrorRecord<'a> {
    /// The error type the caller received.
    #[serde(rename = "type")]
    pub(crate) r#type: &'a str,
    /// The HTTP status the caller received.
    pub(crate) code: u16,
    /// The error message the caller received.
    pub(crate) message: &'a str,
    /// The error's stack of causes, innermost first. Empty when the error has none.
    pub(crate) stack: &'a [String],
    /// The id the caller can quote to correlate with this record.
    pub(crate) error_id: &'a str,
}

impl<'a> From<&'a AuthorizationError> for ErrorRecord<'a> {
    fn from(error: &'a AuthorizationError) -> Self {
        Self {
            r#type: &error.r#type,
            code: error.code,
            message: &error.message,
            stack: &error.stack,
            error_id: &error.error_id,
        }
    }
}

/// The `context` of a grant record: the full `(principal, privilege, resource)` triple. Grants
/// are hard-deleted, so after a revocation this record is the only trace of the triple.
#[audit_part(context)]
#[derive(Debug, Clone, PartialEq)]
pub struct GrantContextRecord<'a> {
    /// Who holds the grant.
    pub(crate) principal: SubjectRecord<'a>,
    /// The privilege name, verbatim from the authorizer's vocabulary.
    pub(crate) privilege: &'a str,
    /// The kind of resource the grant is on.
    pub(crate) resource_type: Wire<ResourceType>,
    /// The exact resource. Absent for server grants, whose type is their whole identity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) resource_id: Option<String>,
    /// The containing warehouse, for warehouse-scoped resources.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) warehouse_id: Option<String>,
}

impl<'a> GrantContextRecord<'a> {
    pub(crate) fn new(
        principal: SubjectRecord<'a>,
        privilege: &'a str,
        resource: &GrantResource,
    ) -> Self {
        Self {
            principal,
            privilege,
            resource_type: resource.resource_type().as_wire(),
            resource_id: grant_resource_id(resource),
            warehouse_id: resource.warehouse_id().map(|id| id.to_string()),
        }
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
        GrantResource::Dataset { dataset_id, .. } => Some(dataset_id.to_string()),
        GrantResource::Tag(tag_definition_id) => Some(tag_definition_id.to_string()),
    }
}

/// The `context` object of an authorization record: what the handler recorded about the
/// request beyond its action and its entity.
///
/// Only the keys relevant to that request appear, each described on its own. A product that
/// plugs into Lakekeeper may contribute keys of its own.
// Typed as a free map: the keys come from every emitter's own vocabulary, so this type cannot
// name them. The schema publishes each emitter's keys as properties of its own definition of
// this object.
#[audit_part]
#[derive(Debug, Clone, PartialEq)]
pub struct HandlerContext<'a>(pub(crate) BTreeMap<&'static str, &'a serde_json::Value>);

/// Closed-key fields in the order they were recorded: what a flattened action or entity object
/// carries. A map by contract (every key is unique and comes from a closed enum), a `Vec` in
/// memory so the wire keeps the order the emitter chose.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderedFields<V>(pub(crate) Vec<(&'static str, V)>);

impl<V> FromIterator<(&'static str, V)> for OrderedFields<V> {
    fn from_iter<I: IntoIterator<Item = (&'static str, V)>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl<V: serde::Serialize> serde::Serialize for OrderedFields<V> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (key, value) in &self.0 {
            map.serialize_entry(key, value)?;
        }
        map.end()
    }
}

impl<V: schemars::JsonSchema> schemars::JsonSchema for OrderedFields<V> {
    fn inline_schema() -> bool {
        true
    }
    fn schema_name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("OrderedFields")
    }
    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        let value = generator.subschema_for::<V>();
        schemars::json_schema!({ "type": "object", "additionalProperties": value })
    }
}
