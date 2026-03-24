use std::collections::HashMap;

use async_trait::async_trait;
use axum::{
    Extension, Json, Router,
    extract::{Path, Query, State},
    response::IntoResponse,
    routing::{get, post},
};
use http::{HeaderMap, StatusCode};
use iceberg_ext::catalog::rest::StorageCredential;
use serde::{Deserialize, Serialize};

use crate::{
    api::{
        ApiContext, Result,
        iceberg::{
            types::Prefix,
            v1::{
                namespace::{NamespaceIdentUrl, NamespaceParameters},
                tables::parse_data_access,
            },
        },
    },
    request_metadata::RequestMetadata,
    service::{GenericTableFormat, GenericTableId},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CreateGenericTableRequest {
    pub name: String,
    pub format: GenericTableFormat,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_location: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub statistics: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct GenericTableData {
    pub name: String,
    pub format: GenericTableFormat,
    pub base_location: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub statistics: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct LoadGenericTableResponse {
    pub table: GenericTableData,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<HashMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_credentials: Option<Vec<StorageCredential>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct GenericTableIdentifier {
    pub namespace: Vec<String>,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<GenericTableFormat>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<GenericTableId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ListGenericTablesResponse {
    pub identifiers: Vec<GenericTableIdentifier>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListGenericTablesQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_size: Option<i64>,
}

impl axum::response::IntoResponse for LoadGenericTableResponse {
    fn into_response(self) -> axum::response::Response {
        axum::Json(self).into_response()
    }
}

impl axum::response::IntoResponse for ListGenericTablesResponse {
    fn into_response(self) -> axum::response::Response {
        axum::Json(self).into_response()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct GenericTableParameters {
    pub prefix: Option<Prefix>,
    pub namespace: iceberg::NamespaceIdent,
    pub table_name: String,
}

#[async_trait]
pub trait GenericTableService<S: crate::api::ThreadSafe>
where
    Self: Send + Sync + 'static,
{
    async fn create_generic_table(
        parameters: NamespaceParameters,
        request: CreateGenericTableRequest,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadGenericTableResponse>;

    async fn load_generic_table(
        parameters: GenericTableParameters,
        state: ApiContext<S>,
        data_access: impl Into<crate::api::iceberg::v1::DataAccessMode> + Send,
        request_metadata: RequestMetadata,
    ) -> Result<LoadGenericTableResponse>;

    async fn list_generic_tables(
        parameters: NamespaceParameters,
        query: ListGenericTablesQuery,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<ListGenericTablesResponse>;

    async fn drop_generic_table(
        parameters: GenericTableParameters,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<()>;
}

pub fn router<I: GenericTableService<S>, S: crate::api::ThreadSafe>() -> Router<ApiContext<S>> {
    Router::new()
        // /{prefix}/namespaces/{namespace}/generic-tables
        .route(
            "/{prefix}/namespaces/{namespace}/generic-tables",
            post(
                |Path((prefix, namespace)): Path<(Prefix, NamespaceIdentUrl)>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>,
                 Json(request): Json<CreateGenericTableRequest>| {
                    I::create_generic_table(
                        NamespaceParameters {
                            prefix: Some(prefix),
                            namespace: namespace.into(),
                        },
                        request,
                        api_context,
                        metadata,
                    )
                },
            )
            .get(
                |Path((prefix, namespace)): Path<(Prefix, NamespaceIdentUrl)>,
                 Query(query): Query<ListGenericTablesQuery>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>| {
                    I::list_generic_tables(
                        NamespaceParameters {
                            prefix: Some(prefix),
                            namespace: namespace.into(),
                        },
                        query,
                        api_context,
                        metadata,
                    )
                },
            ),
        )
        // /{prefix}/namespaces/{namespace}/generic-tables/{table}
        .route(
            "/{prefix}/namespaces/{namespace}/generic-tables/{table}",
            get(
                |Path((prefix, namespace, table)): Path<(Prefix, NamespaceIdentUrl, String)>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap,
                 Extension(metadata): Extension<RequestMetadata>| {
                    I::load_generic_table(
                        GenericTableParameters {
                            prefix: Some(prefix),
                            namespace: namespace.into(),
                            table_name: table,
                        },
                        api_context,
                        parse_data_access(&headers),
                        metadata,
                    )
                },
            )
            .delete(
                |Path((prefix, namespace, table)): Path<(Prefix, NamespaceIdentUrl, String)>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>| async move {
                    I::drop_generic_table(
                        GenericTableParameters {
                            prefix: Some(prefix),
                            namespace: namespace.into(),
                            table_name: table,
                        },
                        api_context,
                        metadata,
                    )
                    .await
                    .map(|()| StatusCode::NO_CONTENT.into_response())
                },
            ),
        )
}
