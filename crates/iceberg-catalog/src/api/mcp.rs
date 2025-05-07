
pub(crate) mod v1 {

    use std::marker::PhantomData;
    use http::StatusCode;
    use crate::api::{ApiContext, RequestMetadata};
    use crate::api::endpoints::MCPV1Endpoint;
    use crate::api::management::v1::bootstrap::{ServerInfo, Service};
    use crate::service::authz::Authorizer;
    use crate::service::{Catalog, SecretStore, State};
    use crate::api::IcebergErrorResponse;
    use axum::{extract::State as AxumState, Extension, Json};
    use tracing::info;
    use utoipa::OpenApi;

    #[derive(Clone, Debug)]
    pub struct ApiServer<C: Catalog, A: Authorizer + Clone, S: SecretStore> {
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
            description = "Lakekeeper is a rust-native Apache Iceberg REST Catalog implementation. The MCP API provides endpoints to manage the server, projects, warehouses, users, and roles. If Authorization is enabled, permissions can also be managed. An interactive Swagger-UI for the specific Lakekeeper Version and configuration running is available at `/swagger-ui/#/` of Lakekeeper (by default [http://localhost:8181/swagger-ui/#/](http://localhost:8181/swagger-ui/#/)).",
        ),
        tags(
            (name = "server", description = "Manage Server"),
            (name = "project", description = "Manage Projects"),
            (name = "warehouse", description = "Manage Warehouses"),
            (name = "user", description = "Manage Users"),
            (name = "role", description = "Manage Roles")
        ),
        security(
            ("bearerAuth" = [])
        ),
        paths(
            get_server_info,
        ),
    )]
    struct MCPApiDoc;

}