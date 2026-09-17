#![allow(clippy::needless_for_each)]

use utoipa::{OpenApi, openapi::security::SecurityScheme};

#[derive(Debug, OpenApi)]
#[openapi(
    info(
        title = "Lakekeeper Dataset API",
        description = "Lakekeeper data-plane API for datasets: versioned collections of files.",
    ),
    servers(
        (
            url = "{scheme}://{host}{basePath}",
            description = "Lakekeeper Dataset API",
            variables(
                ("scheme" = (default = "https", description = "The scheme of the URI, either http or https")),
                ("host" = (default = "localhost", description = "The host (and optional port) for the specified server")),
                ("basePath" = (default = "", description = "Optional path prefix (starting with '/') to be prepended to all routes"))
            )
        )
    ),
    tags(
        (name = "dataset", description = "Manage datasets")
    ),
    security(("bearerAuth" = [])),
    paths(
        super::create_dataset,
        super::list_datasets,
        super::load_dataset,
        super::drop_dataset,
        super::commit_dataset,
        super::list_dataset_refs,
        super::create_dataset_ref,
        super::move_dataset_ref,
        super::delete_dataset_ref,
        super::list_dataset_files,
        super::set_dataset_ref_protection,
        super::rename_dataset,
        super::load_dataset_credentials,
        super::import_dataset,
    ),
    modifiers(&SecurityAddon)
)]
struct DatasetApiDoc;

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
    DatasetApiDoc::openapi()
}
