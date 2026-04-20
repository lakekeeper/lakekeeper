//! Serde structs for the OPA decision input document.
//!
//! One `OpaRequest { input: OpaInput }` is `POST`ed per `(resource, action)` tuple.
//! The decision methods in `authorizer.rs` build these in batches and fan out
//! parallel HTTP calls bounded by `max_concurrency`.

use std::collections::BTreeMap;

use serde::Serialize;

/// Envelope matching OPA's data-API contract: `POST /v1/data/{path}` with
/// `{"input": …}` returns `{"result": …}`.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct OpaRequest {
    pub input: OpaInput,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct OpaInput {
    pub actor: OpaActor,
    pub operation: OpaOperation,
    pub resource: OpaResource,
    pub request: OpaRequestContext,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ActorKind {
    Anonymous,
    Principal,
    Role,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct OpaActor {
    pub kind: ActorKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assumed_role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub token_roles: Vec<String>,
    /// Raw JWT claims from `RequestMetadata.authentication().claims()`. Rego
    /// rules may match on any field here (e.g. `groups`, custom attributes).
    #[serde(skip_serializing_if = "serde_json::Value::is_null")]
    pub claims: serde_json::Value,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OperationKind {
    Server,
    Project,
    Warehouse,
    Namespace,
    Table,
    View,
    User,
    Role,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct OpaOperation {
    pub kind: OperationKind,
    /// Serialized catalog action. Uses the canonical `Catalog*Action` serde
    /// representation, i.e. `{"action": "commit", …context}`.
    pub action: serde_json::Value,
}

#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct OpaResource {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warehouse_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warehouse_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace_id: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub namespace_path: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub view_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_protected: Option<bool>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub properties: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct OpaRequestContext {
    pub request_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub engines: Vec<String>,
}

/// Envelope of OPA's response. We ignore everything beyond `result`.
#[derive(Debug, Clone, serde::Deserialize)]
pub(crate) struct OpaResponse {
    #[serde(default)]
    pub result: Option<serde_json::Value>,
}
