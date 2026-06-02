#![allow(clippy::needless_for_each)]

use utoipa::{OpenApi, openapi::security::SecurityScheme};

#[derive(Debug, OpenApi)]
#[openapi(
    info(
        title = "Lakekeeper Generic Table API",
        description = "Lakekeeper data-plane API for non-Iceberg formats (Lance, Delta, ...).",
    ),
    servers(
        (
            url = "{scheme}://{host}{basePath}",
            description = "Lakekeeper Generic Table API",
            variables(
                ("scheme" = (default = "https", description = "The scheme of the URI, either http or https")),
                ("host" = (default = "localhost", description = "The host (and optional port) for the specified server")),
                ("basePath" = (default = "", description = "Optional path prefix (starting with '/') to be prepended to all routes"))
            )
        )
    ),
    tags(
        (name = "generic-table", description = "Manage generic (non-Iceberg) tables")
    ),
    security(("bearerAuth" = [])),
    paths(
        super::create_generic_table,
        super::list_generic_tables,
        super::load_generic_table,
        super::drop_generic_table,
        super::rename_generic_table,
        super::load_generic_table_credentials,
    ),
    modifiers(&SecurityAddon)
)]
struct GenericTableApiDoc;

struct SecurityAddon;

impl utoipa::Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi
            .components
            .get_or_insert_with(|| utoipa::openapi::ComponentsBuilder::new().build());
        components.add_security_scheme(
            "bearerAuth",
            SecurityScheme::Http(
                utoipa::openapi::security::HttpBuilder::new()
                    .scheme(utoipa::openapi::security::HttpAuthScheme::Bearer)
                    .bearer_format("JWT")
                    .build(),
            ),
        );
    }
}

#[must_use]
pub fn api_doc() -> utoipa::openapi::OpenApi {
    GenericTableApiDoc::openapi()
}
