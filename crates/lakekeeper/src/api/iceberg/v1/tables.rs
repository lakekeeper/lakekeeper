use async_trait::async_trait;
use axum::{
    Extension, Json, Router,
    extract::{Path, Query, State},
    http::header,
    response::IntoResponse,
    routing::{get, post},
};
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use iceberg::TableIdent;
use iceberg_ext::catalog::rest::{ETag, LoadCredentialsResponse};

use super::{PageToken, PaginationQuery};
use crate::{
    api::{
        ApiContext, CommitTableRequest, CommitTableResponse, CommitTransactionRequest,
        CreateTableRequest, ListTablesResponse, LoadTableResult, RegisterTableRequest,
        RenameTableRequest, Result,
        iceberg::{
            types::{DropParams, Prefix, ReferencedByQuery},
            v1::namespace::{NamespaceIdentUrl, NamespaceParameters},
        },
    },
    request_metadata::RequestMetadata,
};

/// Normalize table name by replacing `+` with space.
/// This is needed because `+` in URLs is decoded to space by some clients.
pub(super) fn normalize_tabular_name(table: &str) -> String {
    table.replace('+', " ")
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListTablesQuery {
    #[serde(skip_serializing_if = "PageToken::skip_serialize")]
    pub page_token: PageToken,
    /// For servers that support pagination, this signals an upper bound of the number of results that a client will receive. For servers that do not support pagination, clients may receive results larger than the indicated `pageSize`.
    #[serde(rename = "pageSize")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_size: Option<i64>,
    /// Flag to indicate if the response should include UUIDs for tables.
    /// Default is false.
    #[serde(default)]
    pub return_uuids: bool,
    #[serde(default)]
    pub return_protection_status: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SnapshotsQuery {
    /// Load all snapshots
    #[default]
    All,
    /// load all snapshots referenced by branches or tags
    Refs,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub struct LoadTableQuery {
    pub snapshots: Option<SnapshotsQuery>,
    pub referenced_by: Option<ReferencedByQuery>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct LoadTableFilters {
    pub snapshots: SnapshotsQuery,
}

impl From<ListTablesQuery> for PaginationQuery {
    fn from(query: ListTablesQuery) -> Self {
        PaginationQuery {
            page_token: query.page_token,
            page_size: query.page_size,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum LoadTableResultOrNotModified {
    LoadTableResult(LoadTableResult),
    NotModifiedResponse(ETag),
}

impl IntoResponse for LoadTableResultOrNotModified {
    fn into_response(self) -> axum::response::Response {
        match self {
            LoadTableResultOrNotModified::NotModifiedResponse(etag) => {
                let mut header = HeaderMap::new();

                let etag = etag.as_str();

                match etag.parse::<HeaderValue>() {
                    Ok(header_value) => {
                        header.insert(header::ETAG, header_value);
                    }
                    Err(e) => {
                        tracing::error!(
                            "Failed to create valid ETAG header from String {etag}, error: {e}"
                        );
                    }
                }
                (StatusCode::NOT_MODIFIED, header).into_response()
            }
            LoadTableResultOrNotModified::LoadTableResult(load_table_result) => {
                load_table_result.into_response()
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub struct LoadTableCredentialsQuery {
    pub referenced_by: Option<ReferencedByQuery>,
}

#[async_trait]
pub trait TablesService<S: crate::api::ThreadSafe>
where
    Self: Send + Sync + 'static,
{
    /// List all table identifiers underneath a given namespace
    async fn list_tables(
        parameters: NamespaceParameters,
        query: ListTablesQuery,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<ListTablesResponse>;

    /// Create a table in the given namespace
    async fn create_table(
        parameters: NamespaceParameters,
        request: CreateTableRequest,
        data_access: impl Into<DataAccessMode> + Send,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadTableResult>;

    /// Register a table in the given namespace using given metadata file location
    async fn register_table(
        parameters: NamespaceParameters,
        request: RegisterTableRequest,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadTableResult>;

    /// Load a table from the catalog
    async fn load_table(
        parameters: TableParameters,
        data_access: impl Into<DataAccessMode> + Send,
        filters: LoadTableFilters,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
        etags: Vec<ETag>,
    ) -> Result<LoadTableResultOrNotModified>;

    /// Load a table from the catalog
    async fn load_table_credentials(
        parameters: TableParameters,
        data_access: DataAccess,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadCredentialsResponse>;

    /// Commit updates to a table
    async fn commit_table(
        parameters: TableParameters,
        request: CommitTableRequest,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<CommitTableResponse>;

    /// Drop a table from the catalog
    async fn drop_table(
        parameters: TableParameters,
        drop_params: DropParams,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<()>;

    /// Check if a table exists
    async fn table_exists(
        parameters: TableParameters,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<()>;

    /// Rename a table
    async fn rename_table(
        prefix: Option<Prefix>,
        request: RenameTableRequest,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<()>;

    /// Commit updates to multiple tables in an atomic operation
    async fn commit_transaction(
        prefix: Option<Prefix>,
        request: CommitTransactionRequest,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<()>;
}

#[allow(clippy::too_many_lines)]
pub fn router<I: TablesService<S>, S: crate::api::ThreadSafe>() -> Router<ApiContext<S>> {
    Router::new()
        // /{prefix}/namespaces/{namespace}/tables
        .route(
            "/{prefix}/namespaces/{namespace}/tables",
            // Create a table in the given namespace
            get(
                |Path((prefix, namespace)): Path<(Prefix, NamespaceIdentUrl)>,
                 Query(query): Query<ListTablesQuery>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>| {
                    I::list_tables(
                        NamespaceParameters {
                            prefix: Some(prefix),
                            namespace: namespace.into(),
                        },
                        query,
                        api_context,
                        metadata,
                    )
                },
            )
            // Create a table in the given namespace
            .post(
                |Path((prefix, namespace)): Path<(Prefix, NamespaceIdentUrl)>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap,
                 Extension(metadata): Extension<RequestMetadata>,
                 Json(request): Json<CreateTableRequest>| {
                    I::create_table(
                        NamespaceParameters {
                            prefix: Some(prefix),
                            namespace: namespace.into(),
                        },
                        request,
                        parse_data_access(&headers),
                        api_context,
                        metadata,
                    )
                },
            ),
        )
        // /{prefix}/namespaces/{namespace}/register
        .route(
            "/{prefix}/namespaces/{namespace}/register",
            // Register a table in the given namespace using given metadata file location
            post(
                |Path((prefix, namespace)): Path<(Prefix, NamespaceIdentUrl)>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>,
                 Json(request): Json<RegisterTableRequest>| {
                    I::register_table(
                        NamespaceParameters {
                            prefix: Some(prefix),
                            namespace: namespace.into(),
                        },
                        request,
                        api_context,
                        metadata,
                    )
                },
            ),
        )
        // /{prefix}/namespaces/{namespace}/tables/{table}
        .route(
            "/{prefix}/namespaces/{namespace}/tables/{table}",
            // Load a table from the catalog
            get(
                |Path((prefix, namespace, table)): Path<(Prefix, NamespaceIdentUrl, String)>,
                 Query(load_table_query): Query<LoadTableQuery>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap,
                 Extension(metadata): Extension<RequestMetadata>| {
                    let filters = LoadTableFilters {
                        snapshots: load_table_query.snapshots.unwrap_or_default(),
                    };
                    I::load_table(
                        TableParameters {
                            prefix: Some(prefix),
                            table: TableIdent {
                                namespace: namespace.into(),
                                name: normalize_tabular_name(&table),
                            },
                        },
                        parse_data_access(&headers),
                        filters,
                        api_context,
                        metadata,
                        parse_if_none_match(&headers),
                    )
                },
            )
            // Commit updates to a table
            .post(
                |Path((prefix, namespace, table)): Path<(Prefix, NamespaceIdentUrl, String)>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>,
                 Json(request): Json<CommitTableRequest>| {
                    I::commit_table(
                        TableParameters {
                            prefix: Some(prefix),
                            table: TableIdent {
                                namespace: namespace.into(),
                                name: normalize_tabular_name(&table),
                            },
                        },
                        request,
                        api_context,
                        metadata,
                    )
                },
            )
            // Drop a table from the catalog
            .delete(
                |Path((prefix, namespace, table)): Path<(Prefix, NamespaceIdentUrl, String)>,
                 Query(drop_params): Query<DropParams>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>| async move {
                    I::drop_table(
                        TableParameters {
                            prefix: Some(prefix),
                            table: TableIdent {
                                namespace: namespace.into(),
                                name: normalize_tabular_name(&table),
                            },
                        },
                        drop_params,
                        api_context,
                        metadata,
                    )
                    .await
                    .map(|()| StatusCode::NO_CONTENT.into_response())
                },
            )
            // Check if a table exists
            .head(
                |Path((prefix, namespace, table)): Path<(Prefix, NamespaceIdentUrl, String)>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>| async move {
                    I::table_exists(
                        TableParameters {
                            prefix: Some(prefix),
                            table: TableIdent {
                                namespace: namespace.into(),
                                name: normalize_tabular_name(&table),
                            },
                        },
                        api_context,
                        metadata,
                    )
                    .await
                    .map(|()| StatusCode::NO_CONTENT.into_response())
                },
            ),
        )
        // {prefix}/namespaces/{namespace}/tables/{table}/credentials
        .route(
            "/{prefix}/namespaces/{namespace}/tables/{table}/credentials",
            // Load a table from the catalog
            get(
                |Path((prefix, namespace, table)): Path<(Prefix, NamespaceIdentUrl, String)>,
                 State(api_context): State<ApiContext<S>>,
                 Query(_load_table_credentials_query): Query<LoadTableCredentialsQuery>,
                 headers: HeaderMap,
                 Extension(metadata): Extension<RequestMetadata>| {
                    I::load_table_credentials(
                        TableParameters {
                            prefix: Some(prefix),
                            table: TableIdent {
                                namespace: namespace.into(),
                                name: normalize_tabular_name(&table),
                            },
                        },
                        match parse_data_access(&headers) {
                            DataAccessMode::ClientManaged => DataAccess::not_specified(),
                            DataAccessMode::ServerDelegated(da) => da,
                        },
                        api_context,
                        metadata,
                    )
                },
            ),
        )
        // /{prefix}/tables/rename
        .route(
            "/{prefix}/tables/rename",
            // Rename a table in the given namespace
            post(
                |Path(prefix): Path<Prefix>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>,
                 Json(request): Json<RenameTableRequest>| {
                    async {
                        I::rename_table(Some(prefix), request, api_context, metadata)
                            .await
                            .map(|()| StatusCode::NO_CONTENT)
                    }
                },
            ),
        )
        // /{prefix}/transactions/commit
        .route(
            "/{prefix}/transactions/commit",
            // Commit updates to multiple tables in an atomic operation
            post(
                |Path(prefix): Path<Prefix>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>,
                 Json(request): Json<CommitTransactionRequest>| {
                    I::commit_transaction(Some(prefix), request, api_context, metadata)
                },
            ),
        )
}

// Deliberately not ser / de so that it can't be used in the router directly
#[derive(Debug, Clone, PartialEq)]
pub struct TableParameters {
    /// The prefix of the namespace
    pub prefix: Option<Prefix>,
    /// The table to load metadata for
    pub table: TableIdent,
}

pub const DATA_ACCESS_HEADER: &str = "x-iceberg-access-delegation";
pub const IF_NONE_MATCH_HEADER: &str = "if-none-match";
pub const ETAG_HEADER: &str = "etag";

pub const DATA_ACCESS_HEADER_NAME: HeaderName = HeaderName::from_static(DATA_ACCESS_HEADER);
pub const ETAG_HEADER_NAME: HeaderName = HeaderName::from_static(ETAG_HEADER);
pub const IF_NONE_MATCH_HEADER_NAME: HeaderName = HeaderName::from_static(IF_NONE_MATCH_HEADER);

#[derive(Debug, Hash, Clone, PartialEq, Eq, Copy)]
// Modeled as a string to enable multiple values to be specified.
pub struct DataAccess {
    pub vended_credentials: bool,
    pub remote_signing: bool,
}

#[derive(Debug, Hash, Clone, Copy, PartialEq, Eq, derive_more::From)]
pub enum DataAccessMode {
    // For internal use only - indicates that the client has credentials
    // and thus doesn't need any form of data access delegation.
    ClientManaged,
    ServerDelegated(DataAccess),
}

impl DataAccessMode {
    #[must_use]
    pub(crate) fn provide_credentials(self) -> bool {
        match self {
            DataAccessMode::ClientManaged => false,
            DataAccessMode::ServerDelegated(_) => true,
        }
    }
}

impl DataAccess {
    #[must_use]
    pub(crate) fn not_specified() -> Self {
        Self {
            vended_credentials: false,
            remote_signing: false,
        }
    }

    #[must_use]
    pub fn requested(&self) -> bool {
        self.vended_credentials || self.remote_signing
    }
}

fn parse_etags(etags: &str) -> Vec<ETag> {
    let etags = etags.trim().trim_matches('"');
    etags
        .split(',')
        .map(|s| {
            s.trim()
                .trim_matches('"')
                .trim_start_matches("W/")
                .trim_matches('"')
        })
        .filter(|s| !s.is_empty())
        .map(ETag::from)
        .collect()
}

pub fn parse_if_none_match(headers: &HeaderMap) -> Vec<ETag> {
    headers
        .get_all(header::IF_NONE_MATCH)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(parse_etags)
        .collect()
}

pub(crate) fn parse_data_access(headers: &HeaderMap) -> DataAccessMode {
    let header = headers
        .get_all(DATA_ACCESS_HEADER)
        .iter()
        .map(|v| v.to_str().unwrap())
        .collect::<Vec<_>>();
    let vended_credentials = header.contains(&"vended-credentials");
    let remote_signing = header.contains(&"remote-signing");
    let client_managed = header.contains(&"client-managed");
    if !vended_credentials && !remote_signing && client_managed {
        return DataAccessMode::ClientManaged;
    }
    DataAccess {
        vended_credentials,
        remote_signing,
    }
    .into()
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, error::Error, str::FromStr, sync::Arc};

    use axum::response::Response;
    use http_body_util::BodyExt;
    use iceberg::spec::{
        FormatVersion, Schema, SortOrder, TableMetadata, TableMetadataBuilder, UnboundPartitionSpec,
    };

    use super::*;

    #[test]
    fn test_parse_data_access() {
        let headers = http::header::HeaderMap::new();
        let data_access = super::parse_data_access(&headers);
        assert_eq!(
            data_access,
            DataAccessMode::ServerDelegated(DataAccess::not_specified())
        );
    }

    #[test]
    fn test_parse_data_access_capitalization() {
        let mut headers = http::header::HeaderMap::new();
        headers.insert(
            http::header::HeaderName::from_str(super::DATA_ACCESS_HEADER).unwrap(),
            http::header::HeaderValue::from_static("vended-credentials"),
        );
        let data_access = super::parse_data_access(&headers);
        assert_eq!(
            data_access,
            DataAccessMode::ServerDelegated(DataAccess {
                vended_credentials: true,
                remote_signing: false
            })
        );

        let mut headers = http::header::HeaderMap::new();
        headers.insert(
            "x-iceberg-access-delegation",
            http::header::HeaderValue::from_static("vended-credentials"),
        );
        let data_access = super::parse_data_access(&headers);
        assert_eq!(
            data_access,
            DataAccessMode::ServerDelegated(DataAccess {
                vended_credentials: true,
                remote_signing: false
            })
        );
    }

    #[test]
    fn test_parse_data_access_client_managed() {
        let mut headers = http::header::HeaderMap::new();
        headers.insert(
            http::header::HeaderName::from_str(super::DATA_ACCESS_HEADER).unwrap(),
            http::header::HeaderValue::from_static("client-managed"),
        );
        let data_access = super::parse_data_access(&headers);
        assert_eq!(data_access, DataAccessMode::ClientManaged);
    }

    #[test]
    fn test_load_table_query_defaults() {
        let query = super::LoadTableQuery::default();
        assert_eq!(query.snapshots, None);
        assert_eq!(query.referenced_by, None);
    }

    #[test]
    fn test_load_table_query_deserialization_with_referenced_by() {
        let query = serde_json::json!({
            "referenced-by": "prod\u{1f}analytics\u{1f}quarterly_view,prod\u{1f}analytics\u{1f}monthly_view"
        });
        let deserialized: LoadTableQuery = serde_json::from_value(query).unwrap();
        assert_eq!(
            deserialized,
            LoadTableQuery {
                snapshots: None,
                referenced_by: Some(ReferencedByQuery::from(vec![
                    TableIdent::from_strs(vec!["prod", "analytics", "quarterly_view"]).unwrap(),
                    TableIdent::from_strs(vec!["prod", "analytics", "monthly_view"]).unwrap(),
                ]))
            }
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_load_table_query_snapshots_deserialization() {
        use async_trait::async_trait;
        use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
        use tower::ServiceExt;

        use crate::{
            api::{ApiContext, LoadTableResult},
            request_metadata::RequestMetadata,
        };

        #[derive(Debug, Clone)]
        struct TestService;

        #[derive(Debug, Clone)]
        struct ThisState;

        impl crate::api::ThreadSafe for ThisState {}

        #[async_trait]
        impl super::TablesService<ThisState> for TestService {
            async fn list_tables(
                _parameters: super::super::namespace::NamespaceParameters,
                _query: super::ListTablesQuery,
                _state: ApiContext<ThisState>,
                _request_metadata: RequestMetadata,
            ) -> crate::api::Result<crate::api::ListTablesResponse> {
                panic!("Should not be called");
            }

            async fn create_table(
                _parameters: super::super::namespace::NamespaceParameters,
                _request: crate::api::CreateTableRequest,
                _data_access: impl Into<super::DataAccessMode> + Send,
                _state: ApiContext<ThisState>,
                _request_metadata: RequestMetadata,
            ) -> crate::api::Result<LoadTableResult> {
                panic!("Should not be called");
            }

            async fn register_table(
                _parameters: super::super::namespace::NamespaceParameters,
                _request: crate::api::RegisterTableRequest,
                _state: ApiContext<ThisState>,
                _request_metadata: RequestMetadata,
            ) -> crate::api::Result<LoadTableResult> {
                panic!("Should not be called");
            }

            async fn load_table(
                _parameters: super::TableParameters,
                _data_access: impl Into<super::DataAccessMode> + Send,
                filters: super::LoadTableFilters,
                _state: ApiContext<ThisState>,
                _request_metadata: RequestMetadata,
                _etags: Vec<ETag>,
            ) -> crate::api::Result<LoadTableResultOrNotModified> {
                // Return the snapshots filter in the error message for testing
                let snapshots_str = match filters.snapshots {
                    super::SnapshotsQuery::All => "all",
                    super::SnapshotsQuery::Refs => "refs",
                };

                Err(ErrorModel::builder()
                    .message(format!("snapshots={snapshots_str}"))
                    .r#type("UnsupportedOperationException".to_string())
                    .code(406)
                    .build()
                    .into())
            }

            async fn load_table_credentials(
                _parameters: super::TableParameters,
                _data_access: super::DataAccess,
                _state: ApiContext<ThisState>,
                _request_metadata: RequestMetadata,
            ) -> crate::api::Result<iceberg_ext::catalog::rest::LoadCredentialsResponse>
            {
                panic!("Should not be called");
            }

            async fn commit_table(
                _parameters: super::TableParameters,
                _request: crate::api::CommitTableRequest,
                _state: ApiContext<ThisState>,
                _request_metadata: RequestMetadata,
            ) -> crate::api::Result<crate::api::CommitTableResponse> {
                panic!("Should not be called");
            }

            async fn drop_table(
                _parameters: super::TableParameters,
                _drop_params: crate::api::iceberg::types::DropParams,
                _state: ApiContext<ThisState>,
                _request_metadata: RequestMetadata,
            ) -> crate::api::Result<()> {
                panic!("Should not be called");
            }

            async fn table_exists(
                _parameters: super::TableParameters,
                _state: ApiContext<ThisState>,
                _request_metadata: RequestMetadata,
            ) -> crate::api::Result<()> {
                panic!("Should not be called");
            }

            async fn rename_table(
                _prefix: Option<crate::api::iceberg::types::Prefix>,
                _request: crate::api::RenameTableRequest,
                _state: ApiContext<ThisState>,
                _request_metadata: RequestMetadata,
            ) -> crate::api::Result<()> {
                panic!("Should not be called");
            }

            async fn commit_transaction(
                _prefix: Option<crate::api::iceberg::types::Prefix>,
                _request: crate::api::CommitTransactionRequest,
                _state: ApiContext<ThisState>,
                _request_metadata: RequestMetadata,
            ) -> crate::api::Result<()> {
                panic!("Should not be called");
            }
        }

        let api_context = ApiContext {
            v1_state: ThisState,
        };

        let app = super::router::<TestService, ThisState>();
        let router = axum::Router::new().merge(app).with_state(api_context);

        // Test 1: Default snapshots (should be "all")
        let mut req = http::Request::builder()
            .uri("/test/namespaces/test-namespace/tables/test-table")
            .body(axum::body::Body::empty())
            .unwrap();
        req.extensions_mut()
            .insert(RequestMetadata::new_unauthenticated());

        let r = router.clone().oneshot(req).await.unwrap();
        assert_eq!(r.status().as_u16(), 406);
        let bytes = http_body_util::BodyExt::collect(r)
            .await
            .unwrap()
            .to_bytes();
        let response_str = String::from_utf8(bytes.to_vec()).unwrap();
        let error = serde_json::from_str::<IcebergErrorResponse>(&response_str).unwrap();
        assert_eq!(error.error.message, "snapshots=all");

        // Test 2: Explicit snapshots=all
        let mut req = http::Request::builder()
            .uri("/test/namespaces/test-namespace/tables/test-table?snapshots=all")
            .body(axum::body::Body::empty())
            .unwrap();
        req.extensions_mut()
            .insert(RequestMetadata::new_unauthenticated());

        let r = router.clone().oneshot(req).await.unwrap();
        assert_eq!(r.status().as_u16(), 406);
        let bytes = http_body_util::BodyExt::collect(r)
            .await
            .unwrap()
            .to_bytes();
        let response_str = String::from_utf8(bytes.to_vec()).unwrap();
        let error = serde_json::from_str::<IcebergErrorResponse>(&response_str).unwrap();
        assert_eq!(error.error.message, "snapshots=all");

        // Test 3: snapshots=refs
        let mut req = http::Request::builder()
            .uri("/test/namespaces/test-namespace/tables/test-table?snapshots=refs")
            .body(axum::body::Body::empty())
            .unwrap();
        req.extensions_mut()
            .insert(RequestMetadata::new_unauthenticated());

        let r = router.oneshot(req).await.unwrap();
        assert_eq!(r.status().as_u16(), 406);
        let bytes = http_body_util::BodyExt::collect(r)
            .await
            .unwrap()
            .to_bytes();
        let response_str = String::from_utf8(bytes.to_vec()).unwrap();
        let error = serde_json::from_str::<IcebergErrorResponse>(&response_str).unwrap();
        assert_eq!(error.error.message, "snapshots=refs");
    }

    #[test]
    fn test_load_table_result_or_not_modified_from_not_modified_response() {
        let etag = "\"abcdef1234567890\"".to_string();
        let not_modified_response =
            LoadTableResultOrNotModified::NotModifiedResponse(etag.clone().into()).into_response();
        assert_eq!(not_modified_response.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(
            not_modified_response.headers().get(header::ETAG).unwrap(),
            &etag
        );
    }

    #[tokio::test]
    async fn test_load_table_result_or_not_modified_from_load_table_result() {
        let table_metadata = create_table_metadata_mock();
        let load_table_result = LoadTableResult {
            metadata_location: Some("s3://bucket/table/metadata.json".to_string()),
            metadata: table_metadata,
            config: None,
            storage_credentials: None,
        };
        let load_table_result_response_expected = load_table_result.clone().into_response();

        let load_table_result_response_result =
            LoadTableResultOrNotModified::LoadTableResult(load_table_result).into_response();

        assert_eq!(load_table_result_response_result.status(), StatusCode::OK);
        match (
            extract_body_from_response(load_table_result_response_expected).await,
            extract_body_from_response(load_table_result_response_result).await,
        ) {
            (Ok(body_result), Ok(body_expected)) => {
                assert_eq!(body_result, body_expected);
            }
            (Err(e), _) | (_, Err(e)) => {
                panic!("Failed to extract body: {e}");
            }
        }
    }

    #[test]
    fn test_load_table_credentials_query_defaults() {
        let query = super::LoadTableCredentialsQuery::default();
        assert_eq!(query.referenced_by, None);
    }

    #[test]
    fn test_load_table_credentials_query_deserialization_with_referenced_by() {
        let query = serde_json::json!({
            "referenced-by": "prod\u{1f}analytics\u{1f}quarterly_view,prod\u{1f}analytics\u{1f}monthly_view"
        });
        let deserialized: LoadTableCredentialsQuery = serde_json::from_value(query).unwrap();
        assert_eq!(
            deserialized,
            LoadTableCredentialsQuery {
                referenced_by: Some(ReferencedByQuery::from(vec![
                    TableIdent::from_strs(vec!["prod", "analytics", "quarterly_view"]).unwrap(),
                    TableIdent::from_strs(vec!["prod", "analytics", "monthly_view"]).unwrap(),
                ]))
            }
        );
    }

    async fn extract_body_from_response(response: Response) -> Result<String, Box<dyn Error>> {
        let bytes = response.into_body().collect().await?.to_bytes();
        Ok(String::from_utf8(bytes.to_vec())?)
    }

    // Duplicated from iceberg-ext/src/catalog/rest/table.rs because package should be independent
    fn create_table_metadata_mock() -> Arc<TableMetadata> {
        let schema = Schema::builder().with_schema_id(0).build().unwrap();

        let unbound_spec = UnboundPartitionSpec::default();

        let sort_order = SortOrder::builder()
            .with_order_id(0)
            .build(&schema)
            .unwrap();

        let props = HashMap::new();

        let mut builder = TableMetadataBuilder::new(
            schema.clone(),
            unbound_spec.clone(),
            sort_order.clone(),
            "memory://dummy".to_string(),
            FormatVersion::V2,
            props,
        )
        .unwrap();
        builder = builder.add_schema(schema.clone()).unwrap();
        builder = builder.set_current_schema(0).unwrap();
        builder = builder.add_partition_spec(unbound_spec).unwrap();
        builder = builder
            .set_default_partition_spec(TableMetadataBuilder::LAST_ADDED)
            .unwrap();
        builder = builder.add_sort_order(sort_order).unwrap();
        builder = builder
            .set_default_sort_order(i64::from(TableMetadataBuilder::LAST_ADDED))
            .unwrap();

        let build_result: TableMetadata = builder.build().unwrap().into();
        build_result.into()
    }

    #[test]
    fn test_parse_if_none_match_returns_empty_list_when_no_header_exists() {
        let headers = HeaderMap::new();
        let etags = parse_if_none_match(&headers);
        assert!(etags.is_empty());
    }

    #[test]
    fn test_parse_if_none_match_returns_single_value() {
        let etag = "\"abcdefghi123456789\"";

        let mut headers = HeaderMap::new();
        headers.insert(header::IF_NONE_MATCH, etag.parse().unwrap());

        let etags = parse_if_none_match(&headers);

        assert_eq!(etags, vec!["abcdefghi123456789".into()]);
    }

    #[test]
    fn test_parse_if_none_match_returns_single_value_without_additional_space() {
        let etag = "\"abcdefghi123456789\"".to_string();

        let mut headers = HeaderMap::new();
        headers.insert(header::IF_NONE_MATCH, format!(" {etag}").parse().unwrap());

        let etags = parse_if_none_match(&headers);

        assert_eq!(etags, vec!["abcdefghi123456789".into()]);
    }

    #[test]
    fn test_parse_if_none_match_returns_single_value_with_weak_etag() {
        let etag = "W/\"abcdefghi123456789\"".to_string();

        let mut headers = HeaderMap::new();
        headers.insert(header::IF_NONE_MATCH, etag.parse().unwrap());

        let etags = parse_if_none_match(&headers);

        assert_eq!(etags, vec!["abcdefghi123456789".into()]);
    }

    #[test]
    fn test_parse_if_none_match_returns_asterisk_with_asterisk() {
        let etag = "*".to_string();

        let mut headers = HeaderMap::new();
        headers.insert(header::IF_NONE_MATCH, etag.parse().unwrap());

        let etags = parse_if_none_match(&headers);

        assert_eq!(etags, vec!["*".into()]);
    }

    #[test]
    #[allow(clippy::similar_names)]
    fn test_parse_if_none_match_returns_multiple_values() {
        let etag1 = "W/\"abcdefghi123456789\"".to_string();
        let etag2 = "\"123456789abcdefghi\"".to_string();

        let mut headers = HeaderMap::new();
        headers.append(
            header::IF_NONE_MATCH,
            format!("{etag1},{etag2}").parse().unwrap(),
        );

        let etags = parse_if_none_match(&headers);

        assert_eq!(
            etags,
            vec!["abcdefghi123456789".into(), "123456789abcdefghi".into()]
        );
    }

    #[test]
    #[allow(clippy::similar_names)]
    fn test_parse_if_none_match_returns_multiple_values_with_spaces_inbetween() {
        let etag1 = "W/\"abcdefghi123456789\"".to_string();
        let etag2 = "\"123456789abcdefghi\"".to_string();

        let mut headers = HeaderMap::new();
        headers.append(
            header::IF_NONE_MATCH,
            format!("{etag1}, {etag2}").parse().unwrap(),
        );

        let etags = parse_if_none_match(&headers);

        assert_eq!(
            etags,
            vec!["abcdefghi123456789".into(), "123456789abcdefghi".into()]
        );
    }

    #[test]
    #[allow(clippy::similar_names)]
    #[allow(clippy::needless_raw_string_hashes)]
    fn test_parse_if_none_match_returns_multiple_values_with_mixed_styles() {
        let etag1 = r#"etag-without-quote"#.to_string();
        let etag2 = r#""etag-with-normal-quote""#.to_string();
        let etag3 = r#"""etag-with-quotes-twice"""#.to_string();
        let etag4 = r#"W/weak-etag-without-quote"#.to_string();
        let etag5 = r#"W/"weak-etag-with-normal-quote""#.to_string();
        let etag6 = r#"W/""weak-etag-with-quotes-twice"""#.to_string();
        let etag7 = r#""W/weak-etag-without-inner-quote-and-outer-quote""#.to_string();
        let etag8 = r#"""W/weak-etag-without-inner-quote-and-outer-quote-twice"""#.to_string();
        let etag9 = r#""W/"weak-etag-with-normal-inner-quote-and-outer-quote"""#.to_string();
        let etag10 =
            r#"""W/"weak-etag-with-normal-inner-quote-and-outer-quote-twice""""#.to_string();
        let etag11 = r#""W/""weak-etag-with-inner-quote-twice-and-outer-quote""""#.to_string();
        let etag12 =
            r#"""W/""weak-etag-with-inner-quote-twice-and-outer-quote-twice"""""#.to_string();

        let mut headers = HeaderMap::new();
        headers.append(
            header::IF_NONE_MATCH,
            format!("{etag1}, {etag2}, {etag3}, {etag4}, {etag5}, {etag6}, {etag7}, {etag8}, {etag9}, {etag10}, {etag11}, {etag12}").parse().unwrap(),
        );

        let etags = parse_if_none_match(&headers);

        assert_eq!(
            etags,
            vec![
                "etag-without-quote".into(),
                "etag-with-normal-quote".into(),
                "etag-with-quotes-twice".into(),
                "weak-etag-without-quote".into(),
                "weak-etag-with-normal-quote".into(),
                "weak-etag-with-quotes-twice".into(),
                "weak-etag-without-inner-quote-and-outer-quote".into(),
                "weak-etag-without-inner-quote-and-outer-quote-twice".into(),
                "weak-etag-with-normal-inner-quote-and-outer-quote".into(),
                "weak-etag-with-normal-inner-quote-and-outer-quote-twice".into(),
                "weak-etag-with-inner-quote-twice-and-outer-quote".into(),
                "weak-etag-with-inner-quote-twice-and-outer-quote-twice".into(),
            ]
        );
    }

    #[test]
    fn test_parse_if_none_match_returns_with_empty_header() {
        let etag = String::new();

        let mut headers = HeaderMap::new();
        headers.append(header::IF_NONE_MATCH, etag.parse().unwrap());

        let etags = parse_if_none_match(&headers);

        assert!(etags.is_empty());
    }

    #[test]
    fn test_parse_if_none_match_only_contains_spaces() {
        let etag = " ".to_string();

        let mut headers = HeaderMap::new();
        headers.append(header::IF_NONE_MATCH, etag.parse().unwrap());

        let etags = parse_if_none_match(&headers);

        assert!(etags.is_empty());
    }

    #[test]
    fn test_parse_if_none_match_is_quoted_twice() {
        let etag = "\"\"abcdefghi123456789\"\"".to_string();

        let mut headers = HeaderMap::new();
        headers.append(header::IF_NONE_MATCH, etag.parse().unwrap());

        let etags = parse_if_none_match(&headers);

        assert_eq!(etags, vec!["abcdefghi123456789".into()]);
    }

    #[test]
    #[allow(clippy::similar_names)]
    fn test_parse_if_none_match_recieves_multiple_single_headers_with_etags() {
        let etag = "\"abcdefghi123456789\"".to_string();
        let etag2 = "\"123456789abcdefghi\"".to_string();

        let mut headers = HeaderMap::new();
        headers.append(header::IF_NONE_MATCH, etag.parse().unwrap());
        headers.append(header::IF_NONE_MATCH, etag2.parse().unwrap());

        let etags = parse_if_none_match(&headers);

        assert_eq!(
            etags,
            vec!["abcdefghi123456789".into(), "123456789abcdefghi".into()]
        );
    }

    #[test]
    fn test_load_table_result_or_not_modified_into_response_should_return_response_without_header_when_parsing_failed()
     {
        let result = LoadTableResultOrNotModified::NotModifiedResponse("\n".into());
        let response = result.into_response();

        let headers = response.headers();

        assert!(headers.is_empty());
    }
}
