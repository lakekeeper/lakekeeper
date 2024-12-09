use std::collections::HashMap;

use crate::service::{authz::implementations::FgaType, RoleId};
use openfga_rs::{Condition, TypeDefinition};
use std::sync::LazyLock;

const V1_MODEL: &str = include_str!("../../../../../../../authz/openfga/v1/schema.json");
const V2_MODEL: &str = include_str!("../../../../../../../authz/openfga/v2/schema.json");

static MODEL: LazyLock<CollaborationModels> = LazyLock::new(|| CollaborationModels {
    v1: serde_json::from_str(V1_MODEL).expect("Failed to parse OpenFGA model V1 as JSON"),
    v2: serde_json::from_str(V2_MODEL).expect("Failed to parse OpenFGA model V2 as JSON"),
});

const ACTIVE_MODEL: ModelVersion = ModelVersion::V2;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub(crate) struct RoleAssignee(RoleId);

impl RoleAssignee {
    #[must_use]
    pub(crate) fn from_role(role: RoleId) -> Self {
        RoleAssignee(role)
    }

    #[must_use]
    pub(crate) fn role(&self) -> &RoleId {
        &self.0
    }
}

impl RoleId {
    #[must_use]
    pub(crate) fn into_assignees(self) -> RoleAssignee {
        RoleAssignee::from_role(self)
    }
}

pub(crate) trait OpenFgaType {
    fn user_of(&self) -> &[FgaType];

    fn usersets(&self) -> &'static [&'static str];
}

impl OpenFgaType for FgaType {
    fn user_of(&self) -> &[FgaType] {
        match self {
            FgaType::Server => &[FgaType::Project],
            FgaType::User | FgaType::Role => &[
                FgaType::Role,
                FgaType::Server,
                FgaType::Project,
                FgaType::Warehouse,
                FgaType::Namespace,
                FgaType::Table,
                FgaType::View,
            ],
            FgaType::Project => &[FgaType::Server, FgaType::Warehouse],
            FgaType::Warehouse => &[FgaType::Project, FgaType::Namespace],
            FgaType::Namespace => &[
                FgaType::Warehouse,
                FgaType::Namespace,
                FgaType::Table,
                FgaType::View,
            ],
            FgaType::View | FgaType::Table => &[FgaType::Namespace],
            FgaType::ModelVersion => &[],
            FgaType::AuthModelId => &[FgaType::ModelVersion],
        }
    }

    /// Usersets of this type that are used in relations to other types
    fn usersets(&self) -> &'static [&'static str] {
        match self {
            FgaType::Role => &["assignee"],
            _ => &[],
        }
    }
}

#[derive(Debug)]
pub(crate) struct CollaborationModels {
    v1: AuthorizationModel,
    v2: AuthorizationModel,
}

impl CollaborationModels {
    #[must_use]
    pub(crate) fn get_model(&self, version: ModelVersion) -> &AuthorizationModel {
        match version {
            ModelVersion::V1 => &self.v1,
            ModelVersion::V2 => &self.v2,
        }
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    strum_macros::EnumString,
    strum::FromRepr,
    strum_macros::Display,
    strum_macros::EnumIter,
)]
#[strum(serialize_all = "lowercase")]
#[repr(u64)]
pub(crate) enum ModelVersion {
    V1 = 1,
    V2 = 2,
}

impl ModelVersion {
    #[must_use]
    pub(crate) fn active() -> Self {
        ACTIVE_MODEL
    }

    #[must_use]
    pub(crate) fn get_model(self) -> AuthorizationModel {
        MODEL.get_model(self).clone()
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn get_model_ref(self) -> &'static AuthorizationModel {
        MODEL.get_model(self)
    }

    #[must_use]
    pub(crate) fn as_monotonic_int(self) -> u64 {
        self as u64
    }
    #[must_use]
    pub(crate) fn from_monotonic_int(value: u64) -> Option<Self> {
        Self::from_repr(value)
    }
}

impl PartialOrd for ModelVersion {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.as_monotonic_int().cmp(&other.as_monotonic_int()))
    }
}

#[derive(Debug, serde::Deserialize, Clone, PartialEq)]
#[serde(from = "ser_de::AuthorizationModel")]
pub(crate) struct AuthorizationModel {
    pub(crate) type_definitions: Vec<TypeDefinition>,
    pub(crate) schema_version: String,
    pub(crate) conditions: Option<HashMap<String, Condition>>,
}

impl AuthorizationModel {
    pub(crate) fn into_write_request(
        self,
        store_id: String,
    ) -> openfga_rs::WriteAuthorizationModelRequest {
        openfga_rs::WriteAuthorizationModelRequest {
            store_id,
            type_definitions: self.type_definitions.into_iter().collect(),
            schema_version: self.schema_version,
            conditions: self.conditions.unwrap_or_default(),
        }
    }
}

impl From<openfga_rs::AuthorizationModel> for AuthorizationModel {
    fn from(value: openfga_rs::AuthorizationModel) -> Self {
        let openfga_rs::AuthorizationModel {
            id: _,
            schema_version,
            type_definitions,
            conditions,
        } = value;
        AuthorizationModel {
            type_definitions: type_definitions
                .into_iter()
                .map(std::convert::Into::into)
                .collect(),
            schema_version,
            conditions: Some(conditions).filter(|v: &HashMap<String, Condition>| !v.is_empty()),
        }
    }
}

mod ser_de {
    use super::HashMap;
    use openfga_rs::relation_reference::RelationOrWildcard;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq)]
    #[serde(deny_unknown_fields)]
    pub(super) struct AuthorizationModel {
        type_definitions: Vec<TypeDefinition>,
        schema_version: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        conditions: Option<HashMap<String, Condition>>,
    }

    impl From<AuthorizationModel> for super::AuthorizationModel {
        fn from(value: AuthorizationModel) -> Self {
            super::AuthorizationModel {
                type_definitions: value
                    .type_definitions
                    .into_iter()
                    .map(std::convert::Into::into)
                    .collect(),
                schema_version: value.schema_version,
                conditions: value
                    .conditions
                    .map(|conditions| conditions.into_iter().map(|(k, v)| (k, v.into())).collect()),
            }
        }
    }

    impl TryFrom<super::AuthorizationModel> for AuthorizationModel {
        type Error = String;

        fn try_from(value: super::AuthorizationModel) -> Result<Self, String> {
            Ok(AuthorizationModel {
                type_definitions: value
                    .type_definitions
                    .into_iter()
                    .map(std::convert::Into::into)
                    .collect(),
                schema_version: value.schema_version,
                conditions: match value.conditions {
                    Some(conditions) => Some(
                        conditions
                            .into_iter()
                            .map(|(k, v)| (k, v.try_into()))
                            .map(|(k, v)| v.map(|v| (k, v)))
                            .collect::<Result<_, _>>()?,
                    ),
                    None => None,
                },
            })
        }
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct TypeDefinition {
        #[serde(rename = "type")]
        r#type: String,
        #[serde(rename = "relations", skip_serializing_if = "Option::is_none")]
        relations: Option<HashMap<String, Userset>>,
        #[serde(rename = "metadata", skip_serializing_if = "Option::is_none")]
        metadata: Option<Box<Metadata>>,
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Metadata {
        #[serde(rename = "relations", skip_serializing_if = "Option::is_none")]
        relations: Option<HashMap<String, RelationMetadata>>,
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct RelationMetadata {
        #[serde(
            rename = "directly_related_user_types",
            skip_serializing_if = "Option::is_none"
        )]
        directly_related_user_types: Option<Vec<RelationReference>>,
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct RelationReference {
        #[serde(rename = "type")]
        r#type: String,
        #[serde(rename = "relation", skip_serializing_if = "Option::is_none")]
        relation: Option<String>,
        #[serde(rename = "wildcard", skip_serializing_if = "Option::is_none")]
        wildcard: Option<serde_json::Value>,
        /// The name of a condition that is enforced over the allowed relation.
        #[serde(rename = "condition", skip_serializing_if = "Option::is_none")]
        condition: Option<String>,
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Userset {
        /// A `DirectUserset` is a sentinel message for referencing the direct members specified by an object/relation mapping.
        #[serde(rename = "this", skip_serializing_if = "Option::is_none")]
        this: Option<serde_json::Value>,
        #[serde(rename = "computedUserset", skip_serializing_if = "Option::is_none")]
        #[allow(clippy::struct_field_names)]
        computed_userset: Option<Box<ObjectRelation>>,
        #[serde(rename = "tupleToUserset", skip_serializing_if = "Option::is_none")]
        #[allow(clippy::struct_field_names)]
        tuple_to_userset: Option<Box<V1PeriodTupleToUserset>>,
        #[serde(rename = "union", skip_serializing_if = "Option::is_none")]
        union: Option<Box<Usersets>>,
        #[serde(rename = "intersection", skip_serializing_if = "Option::is_none")]
        intersection: Option<Box<Usersets>>,
        #[serde(rename = "difference", skip_serializing_if = "Option::is_none")]
        difference: Option<Box<V1PeriodDifference>>,
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub(crate) struct Condition {
        #[serde(rename = "name")]
        pub(crate) name: String,
        /// A Google CEL expression, expressed as a string.
        #[serde(rename = "expression")]
        pub(crate) expression: String,
        /// A map of parameter names to the parameter's defined type reference.
        #[serde(rename = "parameters", skip_serializing_if = "Option::is_none")]
        pub(crate) parameters: Option<HashMap<String, ConditionParamTypeRef>>,
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub(crate) struct ConditionParamTypeRef {
        #[serde(rename = "type_name")]
        pub(crate) type_name: TypeName,
        #[serde(rename = "generic_types", skip_serializing_if = "Option::is_none")]
        pub(crate) generic_types: Option<Vec<ConditionParamTypeRef>>,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
    pub(crate) enum TypeName {
        #[serde(rename = "TYPE_NAME_UNSPECIFIED")]
        Unspecified,
        #[serde(rename = "TYPE_NAME_ANY")]
        Any,
        #[serde(rename = "TYPE_NAME_BOOL")]
        Bool,
        #[serde(rename = "TYPE_NAME_STRING")]
        String,
        #[serde(rename = "TYPE_NAME_INT")]
        Int,
        #[serde(rename = "TYPE_NAME_UINT")]
        Uint,
        #[serde(rename = "TYPE_NAME_DOUBLE")]
        Double,
        #[serde(rename = "TYPE_NAME_DURATION")]
        Duration,
        #[serde(rename = "TYPE_NAME_TIMESTAMP")]
        Timestamp,
        #[serde(rename = "TYPE_NAME_MAP")]
        Map,
        #[serde(rename = "TYPE_NAME_LIST")]
        List,
        #[serde(rename = "TYPE_NAME_IPADDRESS")]
        Ipaddress,
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct ObjectRelation {
        #[serde(rename = "object", skip_serializing_if = "Option::is_none")]
        object: Option<String>,
        #[serde(rename = "relation", skip_serializing_if = "Option::is_none")]
        relation: Option<String>,
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct V1PeriodTupleToUserset {
        #[serde(rename = "tupleset")]
        tupleset: Box<ObjectRelation>,
        #[serde(rename = "computedUserset")]
        computed_userset: Box<ObjectRelation>,
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct V1PeriodDifference {
        #[serde(rename = "base")]
        base: Box<Userset>,
        #[serde(rename = "subtract")]
        subtract: Box<Userset>,
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Usersets {
        #[serde(rename = "child")]
        child: Vec<Userset>,
    }

    impl From<Condition> for openfga_rs::Condition {
        fn from(value: Condition) -> Self {
            openfga_rs::Condition {
                name: value.name,
                expression: value.expression,
                parameters: value
                    .parameters
                    .map(|parameters| parameters.into_iter().map(|(k, v)| (k, v.into())).collect())
                    .unwrap_or_default(),
                metadata: None,
            }
        }
    }

    impl From<ConditionParamTypeRef> for openfga_rs::ConditionParamTypeRef {
        fn from(value: ConditionParamTypeRef) -> Self {
            let type_name: openfga_rs::condition_param_type_ref::TypeName = value.type_name.into();
            openfga_rs::ConditionParamTypeRef {
                type_name: type_name.into(),
                generic_types: value
                    .generic_types
                    .map(|generic_types| {
                        generic_types
                            .into_iter()
                            .map(std::convert::Into::into)
                            .collect()
                    })
                    .unwrap_or_default(),
            }
        }
    }

    impl From<TypeName> for openfga_rs::condition_param_type_ref::TypeName {
        fn from(value: TypeName) -> Self {
            match value {
                TypeName::Unspecified => {
                    openfga_rs::condition_param_type_ref::TypeName::Unspecified
                }
                TypeName::Any => openfga_rs::condition_param_type_ref::TypeName::Any,
                TypeName::Bool => openfga_rs::condition_param_type_ref::TypeName::Bool,
                TypeName::String => openfga_rs::condition_param_type_ref::TypeName::String,
                TypeName::Int => openfga_rs::condition_param_type_ref::TypeName::Int,
                TypeName::Uint => openfga_rs::condition_param_type_ref::TypeName::Uint,
                TypeName::Double => openfga_rs::condition_param_type_ref::TypeName::Double,
                TypeName::Duration => openfga_rs::condition_param_type_ref::TypeName::Duration,
                TypeName::Timestamp => openfga_rs::condition_param_type_ref::TypeName::Timestamp,
                TypeName::Map => openfga_rs::condition_param_type_ref::TypeName::Map,
                TypeName::List => openfga_rs::condition_param_type_ref::TypeName::List,
                TypeName::Ipaddress => openfga_rs::condition_param_type_ref::TypeName::Ipaddress,
            }
        }
    }

    impl From<TypeDefinition> for openfga_rs::TypeDefinition {
        fn from(value: TypeDefinition) -> Self {
            openfga_rs::TypeDefinition {
                r#type: value.r#type,
                relations: value
                    .relations
                    .map(|relations| relations.into_iter().map(|(k, v)| (k, v.into())).collect())
                    .unwrap_or_default(),
                metadata: value.metadata.map(|metadata| (*metadata).into()),
            }
        }
    }

    impl From<Metadata> for openfga_rs::Metadata {
        fn from(value: Metadata) -> Self {
            openfga_rs::Metadata {
                relations: value
                    .relations
                    .map(|relations| relations.into_iter().map(|(k, v)| (k, v.into())).collect())
                    .unwrap_or_default(),
                source_info: None,
                // https://github.com/openfga/api/blob/339f6b8ff0f0d25f77d374ede323daebe98cbe7e/openfga/v1/authzmodel.proto#L86
                module: String::new(),
            }
        }
    }

    impl From<RelationMetadata> for openfga_rs::RelationMetadata {
        fn from(value: RelationMetadata) -> Self {
            openfga_rs::RelationMetadata {
                directly_related_user_types: value
                    .directly_related_user_types
                    .map(|v| v.into_iter().map(std::convert::Into::into).collect())
                    .unwrap_or_default(),
                module: String::new(),
                source_info: None,
            }
        }
    }

    impl From<RelationReference> for openfga_rs::RelationReference {
        fn from(value: RelationReference) -> Self {
            openfga_rs::RelationReference {
                r#type: value.r#type,
                relation_or_wildcard: value.relation.map(RelationOrWildcard::Relation).or(value
                    .wildcard
                    .map(|_| RelationOrWildcard::Wildcard(openfga_rs::Wildcard {}))),
                condition: value.condition.unwrap_or_default(),
            }
        }
    }

    impl From<Userset> for openfga_rs::Userset {
        fn from(value: Userset) -> Self {
            let userset = if let Some(_this) = value.this {
                Some(openfga_rs::userset::Userset::This(
                    openfga_rs::DirectUserset {},
                ))
            } else if let Some(computed_userset) = value.computed_userset {
                Some(openfga_rs::userset::Userset::ComputedUserset(
                    (*computed_userset).into(),
                ))
            } else if let Some(tuple_to_userset) = value.tuple_to_userset {
                Some(openfga_rs::userset::Userset::TupleToUserset(
                    (*tuple_to_userset).into(),
                ))
            } else if let Some(union) = value.union {
                Some(openfga_rs::userset::Userset::Union((*union).into()))
            } else if let Some(intersection) = value.intersection {
                Some(openfga_rs::userset::Userset::Intersection(
                    (*intersection).into(),
                ))
            } else {
                value.difference.map(|difference| {
                    openfga_rs::userset::Userset::Difference(Box::new((*difference).into()))
                })
            };

            openfga_rs::Userset { userset }
        }
    }

    impl From<ObjectRelation> for openfga_rs::ObjectRelation {
        fn from(value: ObjectRelation) -> Self {
            openfga_rs::ObjectRelation {
                object: value.object.unwrap_or_default(),
                relation: value.relation.unwrap_or_default(),
            }
        }
    }

    impl From<V1PeriodTupleToUserset> for openfga_rs::TupleToUserset {
        fn from(value: V1PeriodTupleToUserset) -> Self {
            openfga_rs::TupleToUserset {
                tupleset: Some((*value.tupleset).into()),
                computed_userset: Some((*value.computed_userset).into()),
            }
        }
    }

    impl From<V1PeriodDifference> for openfga_rs::Difference {
        fn from(value: V1PeriodDifference) -> Self {
            openfga_rs::Difference {
                base: Some(Box::new((*value.base).into())),
                subtract: Some(Box::new((*value.subtract).into())),
            }
        }
    }

    impl From<Usersets> for openfga_rs::Usersets {
        fn from(value: Usersets) -> Self {
            openfga_rs::Usersets {
                child: value
                    .child
                    .into_iter()
                    .map(std::convert::Into::into)
                    .collect(),
            }
        }
    }

    impl From<openfga_rs::TypeDefinition> for TypeDefinition {
        fn from(value: openfga_rs::TypeDefinition) -> Self {
            TypeDefinition {
                r#type: value.r#type,
                relations: Some(
                    value
                        .relations
                        .into_iter()
                        .map(|(k, v)| (k, v.into()))
                        .collect(),
                )
                .filter(|v: &HashMap<String, Userset>| !v.is_empty()),
                metadata: value.metadata.map(|metadata| Box::new(metadata.into())),
            }
        }
    }

    impl From<openfga_rs::Metadata> for Metadata {
        fn from(value: openfga_rs::Metadata) -> Self {
            Metadata {
                relations: Some(
                    value
                        .relations
                        .into_iter()
                        .map(|(k, v)| (k, v.into()))
                        .collect(),
                )
                .filter(|v: &HashMap<String, RelationMetadata>| !v.is_empty()),
            }
        }
    }

    impl From<openfga_rs::RelationMetadata> for RelationMetadata {
        fn from(value: openfga_rs::RelationMetadata) -> Self {
            RelationMetadata {
                directly_related_user_types: Some(
                    value
                        .directly_related_user_types
                        .into_iter()
                        .map(std::convert::Into::into)
                        .collect(),
                )
                .filter(|v: &Vec<RelationReference>| !v.is_empty()),
            }
        }
    }

    impl From<openfga_rs::RelationReference> for RelationReference {
        fn from(value: openfga_rs::RelationReference) -> Self {
            RelationReference {
                r#type: value.r#type,
                relation: value.relation_or_wildcard.as_ref().and_then(|v| match v {
                    RelationOrWildcard::Relation(v) => Some(v.clone()),
                    RelationOrWildcard::Wildcard(_) => None,
                }),
                wildcard: value.relation_or_wildcard.as_ref().and_then(|v| match v {
                    RelationOrWildcard::Wildcard(_) => Some(serde_json::json!({})),
                    RelationOrWildcard::Relation(_) => None,
                }),
                condition: Some(value.condition).filter(|s| !s.is_empty()),
            }
        }
    }

    impl TryFrom<openfga_rs::Condition> for Condition {
        type Error = String;

        fn try_from(value: openfga_rs::Condition) -> Result<Self, String> {
            Ok(Condition {
                name: value.name,
                expression: value.expression,
                parameters: Some(
                    value
                        .parameters
                        .into_iter()
                        .map(|(k, v)| (k, v.try_into()))
                        .map(|(k, v)| v.map(|v| (k, v)))
                        .collect::<Result<_, _>>()?,
                )
                .filter(|v: &HashMap<String, ConditionParamTypeRef>| !v.is_empty()),
            })
        }
    }

    impl TryFrom<openfga_rs::ConditionParamTypeRef> for ConditionParamTypeRef {
        type Error = String;

        fn try_from(value: openfga_rs::ConditionParamTypeRef) -> Result<Self, String> {
            let type_name =
                openfga_rs::condition_param_type_ref::TypeName::try_from(value.type_name)
                    .map_err(|e| format!("Failed to convert TypeName: {e}"))?;

            Ok(ConditionParamTypeRef {
                type_name: type_name.into(),
                generic_types: Some(
                    value
                        .generic_types
                        .into_iter()
                        .map(std::convert::TryInto::try_into)
                        .collect::<Result<_, _>>()?,
                )
                .filter(|v: &Vec<ConditionParamTypeRef>| !v.is_empty()),
            })
        }
    }

    impl From<openfga_rs::condition_param_type_ref::TypeName> for TypeName {
        fn from(value: openfga_rs::condition_param_type_ref::TypeName) -> Self {
            match value {
                openfga_rs::condition_param_type_ref::TypeName::Unspecified => {
                    TypeName::Unspecified
                }
                openfga_rs::condition_param_type_ref::TypeName::Any => TypeName::Any,
                openfga_rs::condition_param_type_ref::TypeName::Bool => TypeName::Bool,
                openfga_rs::condition_param_type_ref::TypeName::String => TypeName::String,
                openfga_rs::condition_param_type_ref::TypeName::Int => TypeName::Int,
                openfga_rs::condition_param_type_ref::TypeName::Uint => TypeName::Uint,
                openfga_rs::condition_param_type_ref::TypeName::Double => TypeName::Double,
                openfga_rs::condition_param_type_ref::TypeName::Duration => TypeName::Duration,
                openfga_rs::condition_param_type_ref::TypeName::Timestamp => TypeName::Timestamp,
                openfga_rs::condition_param_type_ref::TypeName::Map => TypeName::Map,
                openfga_rs::condition_param_type_ref::TypeName::List => TypeName::List,
                openfga_rs::condition_param_type_ref::TypeName::Ipaddress => TypeName::Ipaddress,
            }
        }
    }

    impl From<openfga_rs::Userset> for Userset {
        fn from(value: openfga_rs::Userset) -> Self {
            match value.userset {
                Some(openfga_rs::userset::Userset::This(_)) => Userset {
                    this: Some(serde_json::json!({})),
                    computed_userset: None,
                    tuple_to_userset: None,
                    union: None,
                    intersection: None,
                    difference: None,
                },
                Some(openfga_rs::userset::Userset::ComputedUserset(v)) => Userset {
                    this: None,
                    computed_userset: Some(Box::new(v.into())),
                    tuple_to_userset: None,
                    union: None,
                    intersection: None,
                    difference: None,
                },
                Some(openfga_rs::userset::Userset::TupleToUserset(v)) => Userset {
                    this: None,
                    computed_userset: None,
                    tuple_to_userset: Some(Box::new(v.into())),
                    union: None,
                    intersection: None,
                    difference: None,
                },
                Some(openfga_rs::userset::Userset::Union(v)) => Userset {
                    this: None,
                    computed_userset: None,
                    tuple_to_userset: None,
                    union: Some(Box::new(v.into())),
                    intersection: None,
                    difference: None,
                },
                Some(openfga_rs::userset::Userset::Intersection(v)) => Userset {
                    this: None,
                    computed_userset: None,
                    tuple_to_userset: None,
                    union: None,
                    intersection: Some(Box::new(v.into())),
                    difference: None,
                },
                Some(openfga_rs::userset::Userset::Difference(v)) => Userset {
                    this: None,
                    computed_userset: None,
                    tuple_to_userset: None,
                    union: None,
                    intersection: None,
                    difference: Some(Box::new((*v).into())),
                },
                None => Userset {
                    this: None,
                    computed_userset: None,
                    tuple_to_userset: None,
                    union: None,
                    intersection: None,
                    difference: None,
                },
            }
        }
    }

    impl From<openfga_rs::ObjectRelation> for ObjectRelation {
        fn from(value: openfga_rs::ObjectRelation) -> Self {
            ObjectRelation {
                object: Some(value.object).filter(|s| !s.is_empty()),
                relation: Some(value.relation).filter(|s| !s.is_empty()),
            }
        }
    }

    impl From<openfga_rs::TupleToUserset> for V1PeriodTupleToUserset {
        fn from(value: openfga_rs::TupleToUserset) -> Self {
            V1PeriodTupleToUserset {
                tupleset: Box::new(value.tupleset.map_or(
                    ObjectRelation {
                        object: None,
                        relation: None,
                    },
                    Into::into,
                )),
                computed_userset: Box::new(value.computed_userset.map_or(
                    ObjectRelation {
                        object: None,
                        relation: None,
                    },
                    Into::into,
                )),
            }
        }
    }

    impl From<openfga_rs::Difference> for V1PeriodDifference {
        fn from(value: openfga_rs::Difference) -> Self {
            V1PeriodDifference {
                base: Box::new(value.base.map_or(
                    Userset {
                        this: None,
                        computed_userset: None,
                        tuple_to_userset: None,
                        union: None,
                        intersection: None,
                        difference: None,
                    },
                    |v| (*v).into(),
                )),
                subtract: Box::new(value.subtract.map_or(
                    Userset {
                        this: None,
                        computed_userset: None,
                        tuple_to_userset: None,
                        union: None,
                        intersection: None,
                        difference: None,
                    },
                    |v| (*v).into(),
                )),
            }
        }
    }

    impl From<openfga_rs::Usersets> for Usersets {
        fn from(value: openfga_rs::Usersets) -> Self {
            Usersets {
                child: value.child.into_iter().map(Into::into).collect(),
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use strum::IntoEnumIterator;

    #[test]
    fn test_load_model() {
        let _model = MODEL.get_model(ModelVersion::V1);
    }

    #[test]
    fn test_load_ser_de_roundtrip() {
        for v in ModelVersion::iter() {
            match v {
                ModelVersion::V1 => {
                    let ser_model: ser_de::AuthorizationModel =
                        serde_json::from_str(V1_MODEL).unwrap();
                    assert_eq!(
                        serde_json::to_value(ser_model).unwrap(),
                        serde_json::from_str::<serde_json::Value>(V1_MODEL).unwrap()
                    );
                }
                ModelVersion::V2 => {
                    let ser_model: ser_de::AuthorizationModel =
                        serde_json::from_str(V2_MODEL).unwrap();
                    assert_eq!(
                        serde_json::to_value(ser_model).unwrap(),
                        serde_json::from_str::<serde_json::Value>(V2_MODEL).unwrap()
                    );
                }
            }
        }
    }

    #[test]
    fn test_proto_roundtrip() {
        for v in ModelVersion::iter() {
            let proto = MODEL.get_model(v).clone();
            let write: openfga_rs::WriteAuthorizationModelRequest =
                proto.into_write_request("store_id".to_string());
            let proto2 = AuthorizationModel {
                schema_version: write.schema_version,
                type_definitions: write.type_definitions,
                conditions: Some(write.conditions).filter(|v| !v.is_empty()),
            };
            let model: ser_de::AuthorizationModel = proto2.clone().try_into().unwrap();
            let value = serde_json::to_value(model).unwrap();
            match v {
                ModelVersion::V1 => {
                    assert_eq!(
                        value,
                        serde_json::from_str::<serde_json::Value>(V1_MODEL).unwrap()
                    );
                }
                ModelVersion::V2 => {
                    assert_eq!(
                        value,
                        serde_json::from_str::<serde_json::Value>(V2_MODEL).unwrap()
                    );
                }
            }
        }
    }

    #[test]
    fn test_model_version_to_and_from_int_matches() {
        for version in ModelVersion::iter() {
            let int = version.as_monotonic_int();
            let from_int = ModelVersion::from_monotonic_int(int).unwrap();
            assert_eq!(version, from_int);
        }
    }
}
