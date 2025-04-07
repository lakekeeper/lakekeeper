use async_trait::async_trait;
use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
    routing::{get, post},
    Extension, Json, Router,
};
use http::{HeaderMap, StatusCode};
use iceberg::TableIdent;

use super::ListTablesQuery;
use crate::{
    api::{
        iceberg::{
            types::{DropParams, Prefix},
            v1::{
                namespace::{NamespaceIdentUrl, NamespaceParameters},
                DataAccess,
            },
        },
        management::v1::ProtectionResponse,
        ApiContext, CommitViewRequest, CreateViewRequest, ListTablesResponse, LoadViewResult,
        RenameTableRequest, Result,
    },
    request_metadata::RequestMetadata,
    service::ViewIdentUuid,
    WarehouseIdent,
};

#[async_trait]
pub trait ViewService<S: crate::api::ThreadSafe>
where
    Self: Send + Sync + 'static,
{
    /// List all views underneath a given namespace
    async fn list_views(
        parameters: NamespaceParameters,
        query: ListTablesQuery,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<ListTablesResponse>;

    /// Create a view in the given namespace
    async fn create_view(
        parameters: NamespaceParameters,
        request: CreateViewRequest,
        state: ApiContext<S>,
        data_access: DataAccess,
        request_metadata: RequestMetadata,
    ) -> Result<LoadViewResult>;

    /// Load a view from the catalog
    async fn load_view(
        parameters: ViewParameters,
        state: ApiContext<S>,
        data_access: DataAccess,
        request_metadata: RequestMetadata,
    ) -> Result<LoadViewResult>;

    /// Commit updates to a view.
    async fn commit_view(
        parameters: ViewParameters,
        request: CommitViewRequest,
        state: ApiContext<S>,
        data_access: DataAccess,
        request_metadata: RequestMetadata,
    ) -> Result<LoadViewResult>;

    /// Remove a view from the catalog
    async fn drop_view(
        parameters: ViewParameters,
        drop_params: DropParams,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<()>;

    /// Check if a view exists
    async fn view_exists(
        parameters: ViewParameters,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<()>;

    /// Rename a view from its current name to a new name
    async fn rename_view(
        prefix: Option<Prefix>,
        request: RenameTableRequest,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<()>;

    async fn set_view_protection(
        view_id: ViewIdentUuid,
        warehouse_ident: WarehouseIdent,
        protected: bool,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<ProtectionResponse>;
}

#[allow(clippy::too_many_lines)]
pub fn router<I: ViewService<S>, S: crate::api::ThreadSafe>() -> Router<ApiContext<S>> {
    Router::new()
        // /{prefix}/namespaces/{namespace}/views
        .route(
            "/{prefix}/namespaces/{namespace}/views",
            get(
                |Path((prefix, namespace)): Path<(Prefix, NamespaceIdentUrl)>,
                 Query(query): Query<ListTablesQuery>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>| {
                    {
                        I::list_views(
                            NamespaceParameters {
                                prefix: Some(prefix),
                                namespace: namespace.into(),
                            },
                            query,
                            api_context,
                            metadata,
                        )
                    }
                },
            )
            // Create a view in the given namespace
            .post(
                |Path((prefix, namespace)): Path<(Prefix, NamespaceIdentUrl)>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap,
                 Extension(metadata): Extension<RequestMetadata>,
                 Json(request): Json<CreateViewRequest>| {
                    {
                        I::create_view(
                            NamespaceParameters {
                                prefix: Some(prefix),
                                namespace: namespace.into(),
                            },
                            request,
                            api_context,
                            crate::api::iceberg::v1::tables::parse_data_access(&headers),
                            metadata,
                        )
                    }
                },
            ),
        )
        // /{prefix}/namespaces/{namespace}/views/{view}
        .route(
            "/{prefix}/namespaces/{namespace}/views/{view}",
            get(
                |Path((prefix, namespace, view)): Path<(Prefix, NamespaceIdentUrl, String)>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap,
                 Extension(metadata): Extension<RequestMetadata>| {
                    {
                        I::load_view(
                            ViewParameters {
                                prefix: Some(prefix),
                                view: TableIdent {
                                    namespace: namespace.into(),
                                    name: view,
                                },
                            },
                            api_context,
                            crate::api::iceberg::v1::tables::parse_data_access(&headers),
                            metadata,
                        )
                    }
                },
            )
            .post(
                |Path((prefix, namespace, view)): Path<(Prefix, NamespaceIdentUrl, String)>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap,
                 Extension(metadata): Extension<RequestMetadata>,
                 Json(request): Json<CommitViewRequest>| {
                    {
                        I::commit_view(
                            ViewParameters {
                                prefix: Some(prefix),
                                view: TableIdent {
                                    namespace: namespace.into(),
                                    name: view,
                                },
                            },
                            request,
                            api_context,
                            crate::api::iceberg::v1::tables::parse_data_access(&headers),
                            metadata,
                        )
                    }
                },
            )
            .delete(
                |Path((prefix, namespace, view)): Path<(Prefix, NamespaceIdentUrl, String)>,
                 Query(drop_params): Query<DropParams>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>| async {
                    {
                        I::drop_view(
                            ViewParameters {
                                prefix: Some(prefix),
                                view: TableIdent {
                                    namespace: namespace.into(),
                                    name: view,
                                },
                            },
                            drop_params,
                            api_context,
                            metadata,
                        )
                        .await
                        .map(|()| StatusCode::NO_CONTENT.into_response())
                    }
                },
            )
            .head(
                |Path((prefix, namespace, view)): Path<(Prefix, NamespaceIdentUrl, String)>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>| async {
                    {
                        I::view_exists(
                            ViewParameters {
                                prefix: Some(prefix),
                                view: TableIdent {
                                    namespace: namespace.into(),
                                    name: view,
                                },
                            },
                            api_context,
                            metadata,
                        )
                        .await
                        .map(|()| StatusCode::NO_CONTENT.into_response())
                    }
                },
            ),
        )
        // /{prefix}/views/rename
        .route(
            "/{prefix}/views/rename",
            post(
                |Path(prefix): Path<Prefix>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>,
                 Json(request): Json<RenameTableRequest>| async {
                    {
                        I::rename_view(Some(prefix), request, api_context, metadata)
                            .await
                            .map(|()| StatusCode::NO_CONTENT.into_response())
                    }
                },
            ),
        )
}

// Deliberately not ser / de so that it can't be used in the router directly
#[derive(Debug, Clone, PartialEq)]
pub struct ViewParameters {
    /// The prefix of the namespace
    pub prefix: Option<Prefix>,
    /// The table to load metadata for
    pub view: TableIdent,
}
