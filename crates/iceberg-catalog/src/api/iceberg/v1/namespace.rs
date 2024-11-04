use std::ops::Deref;

use crate::api::iceberg::types::{PageToken, Prefix};
use crate::api::{ApiContext, Result};
use crate::request_metadata::RequestMetadata;
use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{async_trait, Extension, Json, Router};
use http::StatusCode;
use iceberg::NamespaceIdent;
use iceberg_ext::catalog::rest::{
    CreateNamespaceRequest, CreateNamespaceResponse, GetNamespaceResponse, ListNamespacesResponse,
    UpdateNamespacePropertiesRequest, UpdateNamespacePropertiesResponse,
};
use serde::{Deserialize, Deserializer, Serialize};

#[async_trait]
pub trait Service<S: crate::api::ThreadSafe>
where
    Self: Send + Sync + 'static,
{
    /// List all namespaces at a certain level, optionally starting from a given parent namespace.
    /// If table accounting.tax.paid.info exists, using 'SELECT NAMESPACE IN accounting'
    /// would translate into `GET /namespaces?parent=accounting` and must return a namespace,
    /// ["accounting", "tax"] only. Using 'SELECT NAMESPACE IN accounting.tax' would translate into `GET /namespaces?parent=accounting%1Ftax` and must return a namespace, ["accounting", "tax", "paid"]. If `parent` is not provided, all top-level namespaces should be listed.
    async fn list_namespaces(
        prefix: Option<Prefix>,
        query: ListNamespacesQuery,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<ListNamespacesResponse>;

    /// Create a namespace, with an optional set of properties.
    /// The server might also add properties, such as `last_modified_time` etc.
    async fn create_namespace(
        prefix: Option<Prefix>,
        request: CreateNamespaceRequest,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<CreateNamespaceResponse>;

    /// Return all stored metadata properties for a given namespace
    async fn load_namespace_metadata(
        parameters: NamespaceParameters,
        query: GetNamespacePropertiesQuery,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<GetNamespaceResponse>;

    /// Check if a namespace exists
    async fn namespace_exists(
        parameters: NamespaceParameters,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<()>;

    /// Drop a namespace from the catalog. Namespace must be empty.
    async fn drop_namespace(
        parameters: NamespaceParameters,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<()>;

    /// Set or remove properties on a namespace
    async fn update_namespace_properties(
        parameters: NamespaceParameters,
        request: UpdateNamespacePropertiesRequest,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<UpdateNamespacePropertiesResponse>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct NamespaceIdentUrl(Vec<String>);

impl From<NamespaceIdentUrl> for NamespaceIdent {
    fn from(param: NamespaceIdentUrl) -> Self {
        NamespaceIdent::from_vec(param.0).unwrap()
    }
}

impl From<NamespaceIdent> for NamespaceIdentUrl {
    fn from(param: NamespaceIdent) -> Self {
        NamespaceIdentUrl(param.deref().to_owned())
    }
}

impl<'de> Deserialize<'de> for NamespaceIdentUrl {
    fn deserialize<D>(deserializer: D) -> Result<NamespaceIdentUrl, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        // Split on multipart \u001f
        Ok(NamespaceIdentUrl(
            s.split('\u{1f}').map(ToString::to_string).collect(),
        ))
    }
}

impl Serialize for NamespaceIdentUrl {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        // Join on multipart \u001f
        serializer.serialize_str(&self.0.join("\u{1f}"))
    }
}

fn serialize_namespace_ident_as_url<S>(
    value: &Option<NamespaceIdent>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::ser::Serializer,
{
    match value {
        Some(ident) => NamespaceIdentUrl::from(ident.clone()).serialize(serializer),
        None => serializer.serialize_none(),
    }
}

fn deserialize_namespace_ident_from_url<'de, D>(
    deserializer: D,
) -> Result<Option<NamespaceIdent>, D::Error>
where
    D: Deserializer<'de>,
{
    // Deserialize option first
    let url = Option::<NamespaceIdentUrl>::deserialize(deserializer)?;
    Ok(url.map(NamespaceIdent::from))
}

#[allow(clippy::too_many_lines)]
pub fn router<I: Service<S>, S: crate::api::ThreadSafe>() -> Router<ApiContext<S>> {
    Router::new()
        // List Namespaces
        .route(
            "/:prefix/namespaces",
            // List Namespaces
            get(
                |Path(prefix): Path<Prefix>,
                 Query(query): Query<ListNamespacesQuery>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>| {
                    I::list_namespaces(Some(prefix), query, api_context, metadata)
                },
            )
            // Create Namespace
            .post(
                |Path(prefix): Path<Prefix>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>,
                 Json(request): Json<CreateNamespaceRequest>| {
                    I::create_namespace(Some(prefix), request, api_context, metadata)
                },
            ),
        )
        // Load the metadata properties for a namespace
        .route(
            "/:prefix/namespaces/:namespace",
            // Load the metadata properties for a namespace
            get(
                |Path((prefix, namespace)): Path<(Prefix, NamespaceIdentUrl)>,
                 Query(query): Query<GetNamespacePropertiesQuery>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>| {
                    I::load_namespace_metadata(
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
            // Check if a namespace exists
            .head(
                |Path((prefix, namespace)): Path<(Prefix, NamespaceIdentUrl)>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>| async {
                    I::namespace_exists(
                        NamespaceParameters {
                            prefix: Some(prefix),
                            namespace: namespace.into(),
                        },
                        api_context,
                        metadata,
                    )
                    .await
                    .map(|()| StatusCode::NO_CONTENT.into_response())
                },
            )
            // Drop a namespace from the catalog. Namespace must be empty.
            .delete(
                |Path((prefix, namespace)): Path<(Prefix, NamespaceIdentUrl)>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>| async {
                    I::drop_namespace(
                        NamespaceParameters {
                            prefix: Some(prefix),
                            namespace: namespace.into(),
                        },
                        api_context,
                        metadata,
                    )
                    .await
                    .map(|()| StatusCode::NO_CONTENT.into_response())
                },
            ),
        )
        // UpdateNamespacePropertiesResponse
        .route(
            "/:prefix/namespaces/:namespace/properties",
            // Set or remove properties on a namespace
            post(
                |Path((prefix, namespace)): Path<(Prefix, NamespaceIdentUrl)>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>,
                 Json(request): Json<UpdateNamespacePropertiesRequest>| {
                    I::update_namespace_properties(
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
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListNamespacesQuery {
    #[serde(skip_serializing_if = "PageToken::skip_serialize")]
    pub page_token: PageToken,
    /// For servers that support pagination, this signals an upper bound of the number of results that a client will receive. For servers that do not support pagination, clients may receive results larger than the indicated `pageSize`.
    #[serde(rename = "pageSize")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_size: Option<i64>,
    /// An optional namespace, underneath which to list namespaces. If not provided or empty, all top-level namespaces should be listed. If parent is a multipart namespace, the parts must be separated by the unit separator (`0x1F`) byte.
    #[serde(rename = "parent")]
    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        serialize_with = "serialize_namespace_ident_as_url",
        deserialize_with = "deserialize_namespace_ident_from_url"
    )]
    pub parent: Option<NamespaceIdent>,
    /// Flag to indicate if the response should include UUIDs for namespaces.
    /// Default is false.
    #[serde(default)]
    pub return_uuids: bool,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetNamespacePropertiesQuery {
    /// Flag to indicate if the response should include the UUID for the namespace.
    /// Default is false.
    #[serde(default)]
    pub return_uuid: bool,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, utoipa::IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct PaginationQuery {
    /// Next page token
    #[serde(skip_serializing_if = "PageToken::skip_serialize")]
    #[param(value_type=String)]
    pub page_token: PageToken,
    /// Signals an upper bound of the number of results that a client will receive.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub page_size: Option<i64>,
}

impl From<ListNamespacesQuery> for PaginationQuery {
    fn from(query: ListNamespacesQuery) -> Self {
        PaginationQuery {
            page_token: query.page_token,
            page_size: query.page_size,
        }
    }
}

impl PaginationQuery {
    #[cfg(test)]
    #[must_use]
    pub fn empty() -> Self {
        PaginationQuery {
            page_token: PageToken::Empty,
            page_size: None,
        }
    }
}

// Deliberately not ser / de so that it can't be used in the router directly

#[derive(Debug, Clone, PartialEq)]
pub struct NamespaceParameters {
    /// The prefix of the namespace
    pub prefix: Option<Prefix>,
    /// The namespace to load metadata for
    pub namespace: NamespaceIdent,
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use super::*;
    use crate::request_metadata::RequestMetadata;
    use axum::async_trait;
    use http_body_util::BodyExt;
    use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};

    #[tokio::test]
    async fn test_namespace_params() {
        use tower::ServiceExt;

        #[derive(Debug, Clone)]
        struct TestService;

        #[derive(Debug, Clone)]
        struct ThisState;

        impl ThreadSafe for ThisState {}

        // ToDo: Use Mock instead for impl. I couldn't get mockall to work though.
        #[async_trait]
        impl Service<ThisState> for TestService {
            async fn list_namespaces(
                prefix: Option<Prefix>,
                query: ListNamespacesQuery,
                _state: ApiContext<ThisState>,
                _request_metadata: RequestMetadata,
            ) -> Result<ListNamespacesResponse> {
                assert_eq!(prefix.unwrap().into_string(), "test".to_string());
                assert_eq!(query.page_size, Some(10));
                assert_eq!(query.page_token, PageToken::Empty);

                Err(ErrorModel::builder()
                    .message("The server does not support this operation".to_string())
                    .r#type("UnsupportedOperationException".to_string())
                    .code(406)
                    .build()
                    .into())
            }

            /// Create a namespace, with an optional set of properties.
            /// The server might also add properties, such as `last_modified_time` etc.
            async fn create_namespace(
                _prefix: Option<Prefix>,
                _request: CreateNamespaceRequest,
                _state: ApiContext<ThisState>,
                _request_metadata: RequestMetadata,
            ) -> Result<CreateNamespaceResponse> {
                panic!("Should not be called");
            }

            /// Return all stored metadata properties for a given namespace
            async fn load_namespace_metadata(
                _parameters: NamespaceParameters,
                _query: GetNamespacePropertiesQuery,
                _state: ApiContext<ThisState>,
                _request_metadata: RequestMetadata,
            ) -> Result<GetNamespaceResponse> {
                panic!("Should not be called");
            }

            /// Check if a namespace exists
            async fn namespace_exists(
                _parameters: NamespaceParameters,
                _state: ApiContext<ThisState>,
                _request_metadata: RequestMetadata,
            ) -> Result<()> {
                panic!("Should not be called");
            }

            /// Drop a namespace from the catalog. Namespace must be empty.
            async fn drop_namespace(
                _parameters: NamespaceParameters,
                _state: ApiContext<ThisState>,
                _request_metadata: RequestMetadata,
            ) -> Result<()> {
                panic!("Should not be called");
            }

            /// Set or remove properties on a namespace
            async fn update_namespace_properties(
                _parameters: NamespaceParameters,
                _request: UpdateNamespacePropertiesRequest,
                _state: ApiContext<ThisState>,
                _request_metadata: RequestMetadata,
            ) -> Result<UpdateNamespacePropertiesResponse> {
                panic!("Should not be called");
            }
        }

        let api_context = ApiContext {
            v1_state: ThisState,
        };

        let app = router::<TestService, ThisState>();
        let router = axum::Router::new().merge(app).with_state(api_context);

        let mut req = http::Request::builder()
            .uri("/test/namespaces?pageToken&pageSize=10")
            .body(axum::body::Body::empty())
            .unwrap();
        req.extensions_mut().insert(RequestMetadata::new_random());
        let r = router.oneshot(req).await.unwrap();

        // // parse json body
        // let bytes = r.collect().await.unwrap().to_bytes();
        // let r = String::from_utf8(bytes.to_vec()).unwrap();

        assert_eq!(r.status().as_u16(), 406);
    }

    #[tokio::test]
    async fn test_namespace_deserialization() {
        use super::*;
        use tower::ServiceExt;

        #[derive(Debug, Clone)]
        struct TestService;

        #[derive(Debug, Clone)]
        struct ThisState;

        impl crate::api::ThreadSafe for ThisState {}

        #[async_trait]
        impl Service<ThisState> for TestService {
            async fn list_namespaces(
                _prefix: Option<Prefix>,
                _query: ListNamespacesQuery,
                _state: ApiContext<ThisState>,
                _request_metadata: RequestMetadata,
            ) -> Result<ListNamespacesResponse> {
                panic!("Should not be called");
            }

            /// Create a namespace, with an optional set of properties.
            /// The server might also add properties, such as `last_modified_time` etc.
            async fn create_namespace(
                _prefix: Option<Prefix>,
                _request: CreateNamespaceRequest,
                _state: ApiContext<ThisState>,
                _request_metadata: RequestMetadata,
            ) -> Result<CreateNamespaceResponse> {
                panic!("Should not be called");
            }

            /// Return all stored metadata properties for a given namespace
            async fn load_namespace_metadata(
                parameters: NamespaceParameters,
                _query: GetNamespacePropertiesQuery,
                _state: ApiContext<ThisState>,
                _request_metadata: RequestMetadata,
            ) -> Result<GetNamespaceResponse> {
                assert_eq!(parameters.prefix.unwrap().into_string(), "test".to_string());

                Err(ErrorModel::builder()
                    .message(serde_json::to_string(&(parameters.namespace)).unwrap())
                    .r#type("UnsupportedOperationException".to_string())
                    .code(406)
                    .build()
                    .into())
            }

            /// Check if a namespace exists
            async fn namespace_exists(
                _parameters: NamespaceParameters,
                _state: ApiContext<ThisState>,
                _request_metadata: RequestMetadata,
            ) -> Result<()> {
                panic!("Should not be called");
            }

            /// Drop a namespace from the catalog. Namespace must be empty.
            async fn drop_namespace(
                _parameters: NamespaceParameters,
                _state: ApiContext<ThisState>,
                _request_metadata: RequestMetadata,
            ) -> Result<()> {
                panic!("Should not be called");
            }

            /// Set or remove properties on a namespace
            async fn update_namespace_properties(
                _parameters: NamespaceParameters,
                _request: UpdateNamespacePropertiesRequest,
                _state: ApiContext<ThisState>,
                _request_metadata: RequestMetadata,
            ) -> Result<UpdateNamespacePropertiesResponse> {
                panic!("Should not be called");
            }
        }

        let api_context = ApiContext {
            v1_state: ThisState,
        };

        let app = router::<TestService, ThisState>();
        let router = axum::Router::new().merge(app).with_state(api_context);

        // Test 1: Single identifier
        let mut req = http::Request::builder()
            .uri("/test/namespaces/this-namespace?pageToken&pageSize=10")
            .body(axum::body::Body::empty())
            .unwrap();
        req.extensions_mut().insert(RequestMetadata::new_random());

        let r = router.clone().oneshot(req).await.unwrap();

        assert_eq!(r.status().as_u16(), 406);
        let bytes = r.collect().await.unwrap().to_bytes();
        let r = String::from_utf8(bytes.to_vec()).unwrap();
        let error = serde_json::from_str::<IcebergErrorResponse>(&r).unwrap();
        assert_eq!(error.error.message, "[\"this-namespace\"]");

        // Test 2: Composed identifier
        let mut req = http::Request::builder()
            .uri("/test/namespaces/accounting%1Ftax?pageToken&pageSize=10")
            .body(axum::body::Body::empty())
            .unwrap();
        req.extensions_mut().insert(RequestMetadata::new_random());

        let r = router.oneshot(req).await.unwrap();
        assert_eq!(r.status().as_u16(), 406);
        let bytes = r.collect().await.unwrap().to_bytes();
        let r = String::from_utf8(bytes.to_vec()).unwrap();
        let error = serde_json::from_str::<IcebergErrorResponse>(&r).unwrap();
        assert_eq!(error.error.message, "[\"accounting\",\"tax\"]");
    }
}
