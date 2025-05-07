pub(crate) mod server;
mod tools;

pub(crate) mod v1 {

    use crate::api::endpoints::MCPV1Endpoint;
    use crate::api::management::v1::bootstrap::{ServerInfo, Service};
    use crate::api::management::v1::ApiServer;
    use crate::api::mcp::server;
    use crate::api::IcebergErrorResponse;
    use crate::api::{ApiContext, RequestMetadata};
    use crate::service::authz::Authorizer;
    use crate::service::{Catalog, SecretStore, State};
    use axum::routing::{get, post};
    use axum::{extract::State as AxumState, Extension, Json, Router};
    use http::StatusCode;
    use std::marker::PhantomData;
    use tracing::info;
    use utoipa::OpenApi;

    #[derive(Clone, Debug)]
    pub struct MCPServer<C: Catalog, A: Authorizer + Clone, S: SecretStore> {
        auth_handler: PhantomData<A>,
        config_server: PhantomData<C>,
        secret_store: PhantomData<S>,
    }

    /// ServerInfo
    ///
    /// Returns basic information about the server configuration and status.
    #[utoipa::path(
            get,
            tag = "server",
            path = MCPV1Endpoint::ServerInfo.path(),
            responses(
                (status = 200, description = "MCP Server info", body = ServerInfo),
                (status = "4XX", body = IcebergErrorResponse),
                (status = 500, description = "Unauthorized", body = IcebergErrorResponse)
            )
    )]
    async fn get_server_info<C: Catalog, A: Authorizer, S: SecretStore>(
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> crate::api::Result<(StatusCode, Json<ServerInfo>)> {
        info!("MCP API: get_server_info");
        crate::api::management::v1::ApiServer::<C, A, S>::server_info(api_context, metadata)
            .await
            .map(|user| (StatusCode::OK, Json(user)))
    }

    #[must_use]
    pub fn api_doc<A: Authorizer>() -> utoipa::openapi::OpenApi {
        MCPApiDoc::openapi()
    }

    #[derive(Debug, OpenApi)]
    #[openapi(
        info(
            title = "Lakekeeper MCP API",
            description = "Lakekeeper MCP",
        ),
        tags(),
        security(
            ("bearerAuth" = [])
        ),
        paths(
            get_server_info,
        ),
    )]
    struct MCPApiDoc;

    impl<C: Catalog, A: Authorizer, S: SecretStore> MCPServer<C, A, S> {
        pub fn new_v1_router(authorizer: &A) -> Router<ApiContext<State<A, C, S>>> {
            let (server, router) =
                server::get_mcp_router_and_server().expect("Failed to create MCP Server");
            Router::new()
                // Server
                .route("/info", get(get_server_info))
            // .merge(authorizer.new_router())
            // .merge(router)
        }
    }
}
