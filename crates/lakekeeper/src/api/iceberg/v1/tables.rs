use async_trait::async_trait;
use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
    routing::{get, post},
    Extension, Json, Router,
};
use http::{HeaderMap, StatusCode};
use iceberg::TableIdent;
use iceberg_ext::catalog::rest::LoadCredentialsResponse;

use super::{PageToken, PaginationQuery};
use crate::{
    api::{
        iceberg::{
            types::{DropParams, Prefix},
            v1::namespace::{NamespaceIdentUrl, NamespaceParameters},
        },
        ApiContext, CommitTableRequest, CommitTableResponse, CommitTransactionRequest,
        CreateTableRequest, ListTablesResponse, LoadTableResult, RegisterTableRequest,
        RenameTableRequest, Result,
    },
    request_metadata::RequestMetadata,
};

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
#[serde(rename_all = "camelCase")]
pub struct LoadTableQuery {
    pub snapshots: Option<SnapshotsQuery>,
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
    ) -> Result<LoadTableResult>;

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
                                name: table,
                            },
                        },
                        parse_data_access(&headers),
                        filters,
                        api_context,
                        metadata,
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
                                name: table,
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
                 Extension(metadata): Extension<RequestMetadata>| async {
                    I::drop_table(
                        TableParameters {
                            prefix: Some(prefix),
                            table: TableIdent {
                                namespace: namespace.into(),
                                name: table,
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
                 Extension(metadata): Extension<RequestMetadata>| async {
                    I::table_exists(
                        TableParameters {
                            prefix: Some(prefix),
                            table: TableIdent {
                                namespace: namespace.into(),
                                name: table,
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
                 headers: HeaderMap,
                 Extension(metadata): Extension<RequestMetadata>| {
                    I::load_table_credentials(
                        TableParameters {
                            prefix: Some(prefix),
                            table: TableIdent {
                                namespace: namespace.into(),
                                name: table,
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

pub const DATA_ACCESS_HEADER: &str = "X-Iceberg-Access-Delegation";
pub const IF_NONE_MATCH_HEADER: &str = "If-None-Match";
pub const ETAG_HEADER: &str = "ETag";

#[derive(Debug, Clone, PartialEq, Copy)]
// Modeled as a string to enable multiple values to be specified.
pub struct DataAccess {
    pub vended_credentials: bool,
    pub remote_signing: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, derive_more::From)]
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
    use std::str::FromStr;

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
            ) -> crate::api::Result<LoadTableResult> {
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
}
