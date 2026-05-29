use std::collections::HashMap;

use async_trait::async_trait;
use axum::{
    Extension, Json, Router,
    extract::{Path, Query, RawQuery, State},
    routing::{get, post},
};
use http::{HeaderMap, StatusCode};
#[cfg(feature = "open-api")]
use iceberg_ext::catalog::rest::IcebergErrorResponse;
use iceberg_ext::catalog::rest::StorageCredential;
use serde::{Deserialize, Serialize};

#[cfg(feature = "open-api")]
use crate::api::endpoints::GenericTableV1Endpoint;
use crate::{
    api::{
        ApiContext, Result,
        iceberg::{
            types::{DropParams, Prefix, ReferencedByQuery, ReferencingView},
            v1::{
                DataAccess, DataAccessMode,
                namespace::{NamespaceIdentUrl, NamespaceParameters},
                tables::{parse_data_access, parse_referenced_by_param},
            },
        },
    },
    request_metadata::RequestMetadata,
    service::{GenericTableFormat, GenericTableId},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct CreateGenericTableRequest {
    pub name: String,
    #[cfg_attr(feature = "open-api", schema(value_type = String))]
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
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct GenericTableData {
    pub name: String,
    #[cfg_attr(feature = "open-api", schema(value_type = String))]
    pub format: GenericTableFormat,
    pub base_location: String,
    /// Whether the generic table is protected from being deleted.
    pub protected: bool,
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
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct LoadGenericTableResponse {
    pub table: GenericTableData,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<HashMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_credentials: Option<Vec<StorageCredential>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct GenericTableIdentifier {
    pub namespace: Vec<String>,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "open-api", schema(value_type = Option<String>))]
    pub format: Option<GenericTableFormat>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "open-api", schema(value_type = Option<uuid::Uuid>))]
    pub id: Option<GenericTableId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ListGenericTablesResponse {
    pub identifiers: Vec<GenericTableIdentifier>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "open-api", derive(utoipa::IntoParams))]
#[serde(rename_all = "camelCase")]
pub struct ListGenericTablesQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_size: Option<i64>,
}

/// Query params for `load_generic_table_credentials`; mirrors iceberg's
/// `LoadTableCredentialsQuery`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, Default)]
#[serde(rename_all = "kebab-case")]
pub struct LoadGenericTableCredentialsQuery {
    pub referenced_by: Option<ReferencedByQuery>,
}

impl<'de> serde::Deserialize<'de> for LoadGenericTableCredentialsQuery {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};

        struct V;

        impl Visitor<'_> for V {
            type Value = LoadGenericTableCredentialsQuery;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a string containing query parameters")
            }

            fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(LoadGenericTableCredentialsQuery {
                    referenced_by: parse_referenced_by_param(s),
                })
            }
        }

        deserializer.deserialize_str(V)
    }
}

#[derive(Debug, Clone, PartialEq, Default, typed_builder::TypedBuilder)]
pub struct LoadGenericTableCredentialsRequest {
    #[builder(default)]
    pub referenced_by: Option<Vec<ReferencingView>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct LoadGenericTableCredentialsResponse {
    pub storage_credentials: Vec<StorageCredential>,
}

impl axum::response::IntoResponse for LoadGenericTableCredentialsResponse {
    fn into_response(self) -> axum::response::Response {
        axum::Json(self).into_response()
    }
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct RenameGenericTableTarget {
    pub namespace: Vec<String>,
    pub name: String,
}

impl TryFrom<RenameGenericTableTarget> for iceberg::TableIdent {
    type Error = iceberg::Error;

    fn try_from(t: RenameGenericTableTarget) -> std::result::Result<Self, Self::Error> {
        let namespace = iceberg::NamespaceIdent::from_vec(t.namespace)?;
        Ok(iceberg::TableIdent::new(namespace, t.name))
    }
}

impl From<iceberg::TableIdent> for RenameGenericTableTarget {
    fn from(t: iceberg::TableIdent) -> Self {
        RenameGenericTableTarget {
            namespace: t.namespace.inner(),
            name: t.name,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct RenameGenericTableRequest {
    pub source: RenameGenericTableTarget,
    pub destination: RenameGenericTableTarget,
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

    /// Load only credentials for a generic table; `referenced_by` carries the
    /// DEFINER chain context.
    async fn load_generic_table_credentials(
        parameters: GenericTableParameters,
        request: LoadGenericTableCredentialsRequest,
        data_access: DataAccess,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadGenericTableCredentialsResponse>;

    async fn list_generic_tables(
        parameters: NamespaceParameters,
        query: ListGenericTablesQuery,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<ListGenericTablesResponse>;

    async fn drop_generic_table(
        parameters: GenericTableParameters,
        drop_params: DropParams,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<()>;

    async fn rename_generic_table(
        prefix: Option<Prefix>,
        request: RenameGenericTableRequest,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<()>;
}

/// Create a generic table
#[cfg_attr(feature = "open-api", utoipa::path(
    post,
    tag = "generic-table",
    path = GenericTableV1Endpoint::CreateGenericTable.path(),
    params(("prefix" = String,), ("namespace" = String,)),
    request_body = CreateGenericTableRequest,
    responses(
        (status = 200, body = LoadGenericTableResponse),
        (status = "4XX", body = IcebergErrorResponse),
    ),
))]
async fn create_generic_table<I: GenericTableService<S>, S: crate::api::ThreadSafe>(
    Path((prefix, namespace)): Path<(Prefix, NamespaceIdentUrl)>,
    State(api_context): State<ApiContext<S>>,
    Extension(metadata): Extension<RequestMetadata>,
    Json(request): Json<CreateGenericTableRequest>,
) -> Result<LoadGenericTableResponse> {
    I::create_generic_table(
        NamespaceParameters {
            prefix: Some(prefix),
            namespace: namespace.into(),
        },
        request,
        api_context,
        metadata,
    )
    .await
}

/// List generic tables in a namespace
#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "generic-table",
    path = GenericTableV1Endpoint::ListGenericTables.path(),
    params(("prefix" = String,), ("namespace" = String,), ListGenericTablesQuery),
    responses(
        (status = 200, body = ListGenericTablesResponse),
        (status = "4XX", body = IcebergErrorResponse),
    ),
))]
async fn list_generic_tables<I: GenericTableService<S>, S: crate::api::ThreadSafe>(
    Path((prefix, namespace)): Path<(Prefix, NamespaceIdentUrl)>,
    Query(query): Query<ListGenericTablesQuery>,
    State(api_context): State<ApiContext<S>>,
    Extension(metadata): Extension<RequestMetadata>,
) -> Result<ListGenericTablesResponse> {
    I::list_generic_tables(
        NamespaceParameters {
            prefix: Some(prefix),
            namespace: namespace.into(),
        },
        query,
        api_context,
        metadata,
    )
    .await
}

/// Load a generic table
#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "generic-table",
    path = GenericTableV1Endpoint::LoadGenericTable.path(),
    params(("prefix" = String,), ("namespace" = String,), ("table" = String,)),
    responses(
        (status = 200, body = LoadGenericTableResponse),
        (status = "4XX", body = IcebergErrorResponse),
    ),
))]
async fn load_generic_table<I: GenericTableService<S>, S: crate::api::ThreadSafe>(
    Path((prefix, namespace, table)): Path<(Prefix, NamespaceIdentUrl, String)>,
    State(api_context): State<ApiContext<S>>,
    headers: HeaderMap,
    Extension(metadata): Extension<RequestMetadata>,
) -> Result<LoadGenericTableResponse> {
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
    .await
}

/// Drop a generic table
#[cfg_attr(feature = "open-api", utoipa::path(
    delete,
    tag = "generic-table",
    path = GenericTableV1Endpoint::DropGenericTable.path(),
    params(("prefix" = String,), ("namespace" = String,), ("table" = String,)),
    responses(
        (status = 204, description = "Generic table dropped successfully"),
        (status = "4XX", body = IcebergErrorResponse),
    ),
))]
async fn drop_generic_table<I: GenericTableService<S>, S: crate::api::ThreadSafe>(
    Path((prefix, namespace, table)): Path<(Prefix, NamespaceIdentUrl, String)>,
    Query(drop_params): Query<DropParams>,
    State(api_context): State<ApiContext<S>>,
    Extension(metadata): Extension<RequestMetadata>,
) -> Result<StatusCode> {
    I::drop_generic_table(
        GenericTableParameters {
            prefix: Some(prefix),
            namespace: namespace.into(),
            table_name: table,
        },
        drop_params,
        api_context,
        metadata,
    )
    .await
    .map(|()| StatusCode::NO_CONTENT)
}

/// Rename a generic table
#[cfg_attr(feature = "open-api", utoipa::path(
    post,
    tag = "generic-table",
    path = GenericTableV1Endpoint::RenameGenericTable.path(),
    params(("prefix" = String,)),
    request_body = RenameGenericTableRequest,
    responses(
        (status = 204, description = "Generic table renamed successfully"),
        (status = "4XX", body = IcebergErrorResponse),
    ),
))]
async fn rename_generic_table<I: GenericTableService<S>, S: crate::api::ThreadSafe>(
    Path(prefix): Path<Prefix>,
    State(api_context): State<ApiContext<S>>,
    Extension(metadata): Extension<RequestMetadata>,
    Json(request): Json<RenameGenericTableRequest>,
) -> Result<StatusCode> {
    I::rename_generic_table(Some(prefix), request, api_context, metadata)
        .await
        .map(|()| StatusCode::NO_CONTENT)
}

/// Load credentials for a generic table
#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "generic-table",
    path = GenericTableV1Endpoint::LoadGenericTableCredentials.path(),
    params(
        ("prefix" = String,),
        ("namespace" = String,),
        ("table" = String,),
        ("referenced-by" = Option<String>, Query),
    ),
    responses(
        (status = 200, body = LoadGenericTableCredentialsResponse),
        (status = "4XX", body = IcebergErrorResponse),
    ),
))]
async fn load_generic_table_credentials<I: GenericTableService<S>, S: crate::api::ThreadSafe>(
    Path((prefix, namespace, table)): Path<(Prefix, NamespaceIdentUrl, String)>,
    State(api_context): State<ApiContext<S>>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
    Extension(metadata): Extension<RequestMetadata>,
) -> Result<LoadGenericTableCredentialsResponse> {
    // Deserialization cannot fail in practice: StrDeserializer always reaches
    // visit_str, and visit_str delegates to parse_referenced_by_param which
    // returns Option (invalid input → None, never Err). Mirrors the iceberg
    // load-table-credentials route — the warn is defensive in case the
    // deserialize impl ever grows a real error path.
    let load_credentials_query = raw_query
        .as_deref()
        .and_then(|q| {
            use serde::de::{IntoDeserializer, value::StrDeserializer};
            let deserializer: StrDeserializer<'_, serde::de::value::Error> = q.into_deserializer();
            LoadGenericTableCredentialsQuery::deserialize(deserializer)
                .map_err(|e| {
                    tracing::warn!("Failed to parse load generic table credentials query: {e}");
                    e
                })
                .ok()
        })
        .unwrap_or_default();

    let data_access = match parse_data_access(&headers) {
        DataAccessMode::ClientManaged => DataAccess::not_specified(),
        DataAccessMode::ServerDelegated(da) => da,
    };

    I::load_generic_table_credentials(
        GenericTableParameters {
            prefix: Some(prefix),
            namespace: namespace.into(),
            table_name: table,
        },
        LoadGenericTableCredentialsRequest {
            referenced_by: load_credentials_query
                .referenced_by
                .map(ReferencedByQuery::into_inner),
        },
        data_access,
        api_context,
        metadata,
    )
    .await
}

pub fn router<I: GenericTableService<S>, S: crate::api::ThreadSafe>() -> Router<ApiContext<S>> {
    Router::new()
        .route(
            "/{prefix}/namespaces/{namespace}/generic-tables",
            post(create_generic_table::<I, S>).get(list_generic_tables::<I, S>),
        )
        .route(
            "/{prefix}/namespaces/{namespace}/generic-tables/{table}",
            get(load_generic_table::<I, S>).delete(drop_generic_table::<I, S>),
        )
        .route(
            "/{prefix}/generic-tables/rename",
            post(rename_generic_table::<I, S>),
        )
        .route(
            "/{prefix}/namespaces/{namespace}/generic-tables/{table}/credentials",
            get(load_generic_table_credentials::<I, S>),
        )
}

#[cfg(feature = "open-api")]
mod openapi;
#[cfg(feature = "open-api")]
pub use openapi::api_doc;
