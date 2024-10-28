use iceberg::NamespaceIdent;

#[cfg(feature = "axum")]
use super::impl_into_response;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CreateNamespaceRequest {
    /// Reference to one or more levels of a namespace
    pub namespace: NamespaceIdent,
    /// Configured string to string map of properties for the namespace
    pub properties: Option<std::collections::HashMap<String, String>>,
}

impl TryFrom<CreateNamespaceRequest> for iceberg::Namespace {
    type Error = iceberg::Error;
    fn try_from(value: CreateNamespaceRequest) -> std::result::Result<Self, Self::Error> {
        Ok(iceberg::Namespace::with_properties(
            value.namespace,
            value.properties.unwrap_or_default(),
        ))
    }
}

impl From<&iceberg::Namespace> for CreateNamespaceRequest {
    fn from(value: &iceberg::Namespace) -> Self {
        Self {
            namespace: value.name().clone(),
            properties: Some(value.properties().clone()),
        }
    }
}

impl CreateNamespaceRequest {
    #[must_use]
    pub fn new(namespace: NamespaceIdent) -> CreateNamespaceRequest {
        CreateNamespaceRequest {
            namespace,
            properties: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CreateNamespaceResponse {
    /// Reference to one or more levels of a namespace
    #[serde(rename = "namespace")]
    pub namespace: NamespaceIdent,
    /// Properties stored on the namespace, if supported by the server.
    #[serde(rename = "properties", skip_serializing_if = "Option::is_none")]
    pub properties: Option<std::collections::HashMap<String, String>>,
}

impl CreateNamespaceResponse {
    #[must_use]
    pub fn new(namespace: NamespaceIdent) -> CreateNamespaceResponse {
        CreateNamespaceResponse {
            namespace,
            properties: None,
        }
    }
}

#[cfg(feature = "axum")]
impl_into_response!(CreateNamespaceResponse);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct UpdateNamespacePropertiesRequest {
    pub removals: Option<Vec<String>>,
    pub updates: Option<std::collections::HashMap<String, String>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct UpdateNamespacePropertiesResponse {
    /// List of property keys that were added or updated
    pub updated: Vec<String>,
    /// List of properties that were removed
    pub removed: Vec<String>,
    /// List of properties requested for removal that were not found in the
    /// namespace's properties. Represents a partial success response.
    /// Server's do not need to implement this.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub missing: Option<Vec<String>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct GetNamespaceResponse {
    /// Reference to one or more levels of a namespace
    pub namespace: NamespaceIdent,
    /// Lakekeeper ID of this namespace.
    /// The Lakekeeper ID persists even if the Namespace is renamed / moved.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace_uuid: Option<uuid::Uuid>,
    /// Properties stored on the namespace, if supported by the server.
    /// If the server does not support namespace properties, it should return
    /// null for this field. If namespace properties are supported, but none are
    /// set, it should return an empty object.
    pub properties: Option<std::collections::HashMap<String, String>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ListNamespacesResponse {
    /// An opaque token that allows clients to make use of pagination for list APIs
    /// (e.g. `ListTables`). Clients may initiate the first paginated request by
    /// sending an empty query parameter `pageToken` to the server.
    /// Servers that support pagination should identify the `pageToken` parameter and
    /// return a `next-page-token` in the response if there are more results available.  
    /// After the initial request, the value of `next-page-token` from each response
    /// must be used as the `pageToken` parameter value for the next request.
    /// The server must return `null` value for the `next-page-token` in the last response.
    /// Servers that support pagination must return all results in a single response with
    /// the value of `next-page-token` set to `null` if the query parameter `pageToken`
    /// is not set in the request. Servers that do not support pagination should ignore
    /// the `pageToken` parameter and return all results in a single response.
    /// The `next-page-token` must be omitted from the response. Clients must interpret
    /// either `null` or missing response value of `next-page-token` as the end of the listing results.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
    pub namespaces: Vec<NamespaceIdent>,
    /// Lakekeeper IDs of the namespaces.
    /// The Lakekeeper ID persists even if the Namespace is renamed / moved.
    /// The order of the IDs in this list corresponds to the order of the namespaces in the `namespaces` field.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace_uuids: Option<Vec<uuid::Uuid>>,
}

#[cfg(feature = "axum")]
impl_into_response!(UpdateNamespacePropertiesResponse);
#[cfg(feature = "axum")]
impl_into_response!(ListNamespacesResponse);
#[cfg(feature = "axum")]
impl_into_response!(GetNamespaceResponse);

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_create_namespace_request_serialization() {
        let j = serde_json::json!({
            "namespace": ["ns1", "ns2"],
            "properties": {"owner": "Hank Bendickson"}
        });

        let r: CreateNamespaceRequest = serde_json::from_value(j.clone()).unwrap();
        assert_eq!(serde_json::to_value(r).unwrap(), j);
    }

    #[test]
    fn test_update_namespace_properties_request_serialization() {
        let j = serde_json::json!({
            "removals": ["department", "access_group"],
            "updates": {"owner": "Hank Bendickson"}
        });

        let r: UpdateNamespacePropertiesRequest = serde_json::from_value(j.clone()).unwrap();
        assert_eq!(serde_json::to_value(r).unwrap(), j);
    }
}
