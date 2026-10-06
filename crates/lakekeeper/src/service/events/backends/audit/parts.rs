//! The nested objects of an audit record.
//!
//! Each is an audit type: `#[audit_part]` gives it serde, a schema and a registry entry. A
//! part is built by assembly from the event payload and, where the design provides for it,
//! from enrichment; it is the only place a wire field gets its value.

use std::collections::BTreeMap;

use serde::ser::SerializeMap as _;

use super::ActorType;
use crate::{
    audit::{AnyWireStr, Wire, audit_part},
    request_metadata::RequestMetadata,
    service::{
        UserId,
        authn::{Actor, InternalActor},
        authz::{GrantResource, ResourceType, UserOrRoleId},
        events::{AuthorizationError, context::EntityType},
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
}

impl ActorRecord {
    /// The request's resolved actor. The one way to put a request's caller on a record, so
    /// every record raised while serving it agrees on who the caller is.
    #[must_use]
    pub fn from_request(request_metadata: &RequestMetadata) -> Self {
        Self::from_internal_actor(request_metadata.internal_actor())
    }

    /// A bare principal, for records raised without a request: role resolution, syncs.
    #[must_use]
    pub fn principal(id: &UserId) -> Self {
        Self {
            actor_type: ActorType::Principal.as_wire(),
            principal: Some(id.to_string()),
            assumed_role: None,
        }
    }

    pub(crate) fn from_internal_actor(actor: &InternalActor) -> Self {
        match actor {
            InternalActor::LakekeeperInternal => Self {
                actor_type: ActorType::LakekeeperInternal.as_wire(),
                principal: None,
                assumed_role: None,
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
            },
            Actor::Principal(user_id) => Self::principal(user_id),
            Actor::Role {
                principal,
                assumed_role,
            } => Self {
                actor_type: ActorType::AssumedRole.as_wire(),
                principal: Some(principal.to_string()),
                assumed_role: Some(AssumedRoleRecord {
                    role_id: assumed_role.id.to_string(),
                    provider_id: assumed_role.provider_id().to_string(),
                    source_id: assumed_role.source_id().to_string(),
                }),
            },
        }
    }
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
    pub(crate) role_id: String,
    /// The provider that supplied the role.
    pub(crate) provider_id: String,
    /// The role's id at the provider.
    pub(crate) source_id: String,
}

/// A principal named as a target: `for_principal` on a decision entry, `principal` on a grant
/// record. `{"user": …}` or `{"role": …}`.
#[audit_part]
#[serde(untagged)]
#[derive(Debug, Clone, PartialEq)]
pub enum SubjectRecord {
    /// A user.
    User(UserSubjectRecord),
    /// A role.
    Role(RoleSubjectRecord),
}

impl SubjectRecord {
    pub(crate) fn from_id(id: &UserOrRoleId) -> Self {
        match id {
            UserOrRoleId::User(user) => Self::User(UserSubjectRecord {
                user: user.to_string(),
            }),
            UserOrRoleId::Role(role) => Self::Role(RoleSubjectRecord {
                role: role.to_string(),
            }),
        }
    }
}

/// A user named as a target.
#[audit_part]
#[derive(Debug, Clone, PartialEq)]
pub struct UserSubjectRecord {
    /// The user's principal id.
    pub(crate) user: String,
}

/// A role named as a target.
#[audit_part]
#[derive(Debug, Clone, PartialEq)]
pub struct RoleSubjectRecord {
    /// The role's id.
    pub(crate) role: String,
}

/// One entry of `authorizations[]`: which action on which entity was evaluated, for whom,
/// with what result.
#[audit_part]
#[derive(Debug, Clone, PartialEq)]
pub struct DecisionRecord {
    /// The client's id for this check in a batch, or its index. Absent for single checks.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) id: Option<String>,
    /// The principal whose permission was evaluated, when it is not the request's actor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) for_principal: Option<SubjectRecord>,
    /// The action evaluated.
    pub(crate) action: ActionRecord,
    /// The entity the action was evaluated against.
    pub(crate) entity: EntityRecord,
    /// The authorizer's answer. Absent when an upstream error stopped the evaluation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) allowed: Option<bool>,
    /// The policies or rules that determined the decision. Empty when the authorizer
    /// reports none, which is itself the answer to "what decided this".
    /// The same shape the management API returns for a check, so one parser reads both.
    pub(crate) determined_by: Vec<crate::service::authz::DeterminingFactor>,
}

/// An `action` object: the wire name and the action's context fields.
#[audit_part]
#[derive(Debug, Clone, PartialEq)]
pub struct ActionRecord {
    /// What was attempted. One of the action names this schema lists; a product that plugs
    /// into Lakekeeper may contribute its own.
    pub(crate) action_name: AnyWireStr,
    /// The action's context, keyed by `ActionContextKey` wire names, in the order the action
    /// recorded them.
    #[serde(flatten)]
    pub(crate) context: OrderedFields<serde_json::Value>,
}

/// An `entity` object: the kind of resource and its identifying fields.
#[audit_part]
#[derive(Debug, Clone, PartialEq)]
pub struct EntityRecord {
    /// The kind of resource: `table`, `namespace`, `warehouse`, …
    pub(crate) entity_type: Wire<EntityType>,
    /// The entity's identifying fields, keyed by `EntityField` wire names, in the order the
    /// entity recorded them.
    #[serde(flatten)]
    pub(crate) fields: OrderedFields<String>,
}

/// The `error` object of a denied authorization record.
#[audit_part]
#[derive(Debug, Clone, PartialEq)]
pub struct ErrorRecord {
    /// The error type the caller received.
    #[serde(rename = "type")]
    pub(crate) r#type: String,
    /// The HTTP status the caller received.
    pub(crate) code: u16,
    /// The error message the caller received.
    pub(crate) message: String,
    /// The error's stack of causes, innermost first. Empty when the error has none.
    pub(crate) stack: Vec<String>,
    /// The id the caller can quote to correlate with this record.
    pub(crate) error_id: String,
}

impl From<&AuthorizationError> for ErrorRecord {
    fn from(error: &AuthorizationError) -> Self {
        Self {
            r#type: error.r#type.clone(),
            code: error.code,
            message: error.message.clone(),
            stack: error.stack.clone(),
            error_id: error.error_id.clone(),
        }
    }
}

/// The `context` of a grant record: the full `(principal, privilege, resource)` triple. Grants
/// are hard-deleted and keep no history, so a revocation's triple exists nowhere else once the
/// row is gone.
#[audit_part(context)]
#[derive(Debug, Clone, PartialEq)]
pub struct GrantContextRecord {
    /// Who holds the grant.
    pub(crate) principal: SubjectRecord,
    /// The privilege name, verbatim from the authorizer's vocabulary.
    pub(crate) privilege: String,
    /// The kind of resource the grant is on.
    pub(crate) resource_type: Wire<ResourceType>,
    /// The exact resource. Absent for server grants, whose type is their whole identity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) resource_id: Option<String>,
    /// The containing warehouse, for warehouse-scoped resources.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) warehouse_id: Option<String>,
}

impl GrantContextRecord {
    pub(crate) fn new(principal: &UserOrRoleId, privilege: &str, resource: &GrantResource) -> Self {
        Self {
            principal: SubjectRecord::from_id(principal),
            privilege: privilege.to_string(),
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
        GrantResource::Tag(tag_definition_id) => Some(tag_definition_id.to_string()),
    }
}

/// The `context` object of an authorization record: what the handler recorded about the
/// request beyond its action and its entity.
///
/// Only the keys relevant to that request appear. What a given key carries is stated for that
/// key, not here, because a product that plugs into Lakekeeper contributes keys of its own.
// Typed as a free map: the keys come from every emitter's own vocabulary, so this type cannot
// name them. The schema publishes each emitter's keys as properties of its own definition of
// this object.
#[audit_part]
#[derive(Debug, Clone, PartialEq)]
pub struct HandlerContext(pub(crate) BTreeMap<&'static str, serde_json::Value>);

/// Closed-key fields in the order they were recorded: what a flattened action or entity object
/// carries. A map by contract (every key is unique and comes from a closed enum), a `Vec` in
/// memory so the wire keeps the order the emitter chose, which a sorted map would not.
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
