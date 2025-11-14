#![allow(clippy::needless_for_each)]

use utoipa::{
    openapi::{security::SecurityScheme, KnownFormat, RefOr},
    OpenApi, PartialSchema, ToSchema,
};

use crate::{
    api::{
        endpoints::ManagementV1Endpoint,
        management::v1::warehouse::{GetTaskQueueConfigResponse, SetTaskQueueConfigRequest},
    },
    service::{authz::Authorizer, tasks::QueueApiConfig},
};

#[derive(Debug, OpenApi)]
#[openapi(
    info(
        title = "Lakekeeper Management API",
        description = "Lakekeeper is a rust-native Apache Iceberg REST Catalog implementation. The Management API provides endpoints to manage the server, projects, warehouses, users, and roles. If Authorization is enabled, permissions can also be managed. An interactive Swagger-UI for the specific Lakekeeper Version and configuration running is available at `/swagger-ui/#/` of Lakekeeper (by default [http://localhost:8181/swagger-ui/#/](http://localhost:8181/swagger-ui/#/)).",
    ),
    tags(
        (name = "server", description = "Manage Server"),
        (name = "project", description = "Manage Projects"),
        (name = "warehouse", description = "Manage Warehouses"),
        (name = "tasks", description = "View & Manage Tasks"),
        (name = "user", description = "Manage Users"),
        (name = "role", description = "Manage Roles")
    ),
    security(
        ("bearerAuth" = [])
    ),
    paths(
        super::activate_warehouse,
        super::bootstrap,
        super::control_tasks,
        super::create_project,
        super::create_role,
        super::create_user,
        super::create_warehouse,
        super::deactivate_warehouse,
        super::delete_default_project,
        super::delete_default_project_deprecated,
        super::delete_project_by_id,
        super::delete_role,
        super::delete_user,
        super::delete_warehouse,
        super::get_default_project,
        super::get_default_project_deprecated,
        super::get_endpoint_statistics,
        super::get_project_by_id,
        super::get_role,
        super::get_server_info,
        super::get_task_details,
        super::get_user,
        super::get_warehouse,
        super::get_warehouse_statistics,
        super::list_deleted_tabulars,
        super::list_projects,
        super::list_roles,
        super::list_tasks,
        super::list_user,
        super::list_warehouses,
        super::rename_default_project,
        super::rename_default_project_deprecated,
        super::rename_project_by_id,
        super::rename_warehouse,
        super::search_role,
        super::search_user,
        super::search_tabular,
        super::set_namespace_protection,
        super::set_table_protection,
        super::set_task_queue_config,
        super::get_task_queue_config,
        super::set_view_protection,
        super::set_warehouse_protection,
        super::get_namespace_protection,
        super::get_table_protection,
        super::get_view_protection,
        super::undrop_tabulars,
        super::undrop_tabulars_deprecated,
        super::update_role,
        super::update_storage_credential,
        super::update_storage_profile,
        super::update_user,
        super::update_warehouse_delete_profile,
        super::whoami,
    ),
    modifiers(&SecurityAddon)
)]
pub(super) struct ManagementApiDoc;

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

/// Get the `OpenAPI` documentation for the management API.
///
/// # Errors
/// Never fails, but returns warnings if components cannot be patched.
#[allow(clippy::too_many_lines)]
pub fn api_doc<A: Authorizer>(queue_api_configs: &[&QueueApiConfig]) -> utoipa::openapi::OpenApi {
    let mut doc = ManagementApiDoc::openapi();
    doc.merge(A::api_doc());

    let Some(comps) = doc.components.as_mut() else {
        tracing::warn!(
            "No components found in the OpenAPI document, not patching queue configs in."
        );
        return doc;
    };
    let paths = &mut doc.paths.paths;
    let Some(config_path) = paths.remove(ManagementV1Endpoint::SetTaskQueueConfig.path()) else {
        tracing::warn!("No path found for SetTaskQueueConfig, not patching queue configs in.");
        return doc;
    };

    for QueueApiConfig {
        queue_name,
        utoipa_type_name,
        utoipa_schema,
    } in queue_api_configs
    {
        let mut set_queue_config_schema = SetTaskQueueConfigRequest::schema();
        let mut get_queue_config_schema = GetTaskQueueConfigResponse::schema();
        let set_queue_config_type_name = format!("Set{utoipa_type_name}");
        let get_queue_config_type_name = format!("Get{utoipa_type_name}");
        let queue_config_type_ref = RefOr::Ref(
            utoipa::openapi::schema::RefBuilder::new()
                .ref_location_from_schema_name(utoipa_type_name.to_string())
                .build(),
        );
        let set_queue_config_type_ref = RefOr::Ref(
            utoipa::openapi::schema::RefBuilder::new()
                .ref_location_from_schema_name(set_queue_config_type_name.clone())
                .build(),
        );
        let get_queue_config_type_ref = RefOr::Ref(
            utoipa::openapi::schema::RefBuilder::new()
                .ref_location_from_schema_name(get_queue_config_type_name.clone())
                .build(),
        );

        // replace the "queue-config" property with a ref to the actual queue config type
        match &mut set_queue_config_schema {
            RefOr::Ref(_) => {
                unreachable!("The schema for SetTaskQueueConfigRequest should not be a reference.");
            }
            RefOr::T(s) => match s {
                utoipa::openapi::schema::Schema::Object(obj) => {
                    let ins = obj
                        .properties
                        .insert("queue-config".to_string(), queue_config_type_ref.clone());
                    if ins.is_none() {
                        unreachable!("The schema for SetTaskQueueConfigRequest should have a 'queue-config' property.");
                    }
                }
                _ => {
                    unreachable!("The schema for SetTaskQueueConfigRequest should be an object.");
                }
            },
        }
        match &mut get_queue_config_schema {
            RefOr::Ref(_) => {
                unreachable!(
                    "The schema for GetTaskQueueConfigResponse should not be a reference."
                );
            }
            RefOr::T(s) => match s {
                utoipa::openapi::schema::Schema::Object(obj) => {
                    let ins = obj
                        .properties
                        .insert("queue-config".to_string(), queue_config_type_ref.clone());
                    if ins.is_none() {
                        unreachable!("The schema for GetTaskQueueConfigResponse should have a 'queue-config' property.");
                    }
                }
                _ => {
                    unreachable!("The schema for GetTaskQueueConfigResponse should be an object.");
                }
            },
        }

        let path = ManagementV1Endpoint::SetTaskQueueConfig
            .path()
            .replace("{queue_name}", queue_name);

        let mut p = config_path.clone();

        let Some(post) = p.post.as_mut() else {
            tracing::warn!(
                "No post method found for '{}', not patching queue configs into the ApiDoc.",
                ManagementV1Endpoint::SetTaskQueueConfig.path()
            );
            return doc;
        };
        post.parameters = post.parameters.take().map(|params| {
            params
                .into_iter()
                .filter(|param| param.name != "queue_name")
                .collect()
        });
        post.operation_id = Some(format!(
            "set_task_queue_config_{}",
            queue_name.replace('-', "_")
        ));
        let Some(body) = post.request_body.as_mut() else {
            tracing::warn!(
                "No request body found for the '{}', not patching queue configs into the ApiDoc.",
                ManagementV1Endpoint::SetTaskQueueConfig.path()
            );
            return doc;
        };
        body.content.insert(
            "application/json".to_string(),
            utoipa::openapi::ContentBuilder::new()
                .schema(Some(set_queue_config_type_ref))
                .build(),
        );
        let Some(get) = p.get.as_mut() else {
            tracing::warn!(
                "No get method found for '{}', not patching queue configs into the ApiDoc.",
                ManagementV1Endpoint::SetTaskQueueConfig.path()
            );
            return doc;
        };
        get.parameters = get.parameters.take().map(|params| {
            params
                .into_iter()
                .filter(|param| param.name != "queue_name")
                .collect()
        });
        get.operation_id = Some(format!(
            "get_task_queue_config_{}",
            queue_name.replace('-', "_")
        ));
        let response = utoipa::openapi::response::ResponseBuilder::new()
            .content(
                "application/json",
                utoipa::openapi::content::ContentBuilder::new()
                    .schema(Some(get_queue_config_type_ref))
                    .build(),
            )
            .header(
                "x-request-id",
                utoipa::openapi::HeaderBuilder::new()
                    .schema(
                        utoipa::openapi::schema::Object::builder()
                            .schema_type(utoipa::openapi::schema::SchemaType::new(
                                utoipa::openapi::schema::Type::String,
                            ))
                            .format(Some(utoipa::openapi::schema::SchemaFormat::KnownFormat(
                                KnownFormat::Uuid,
                            ))),
                    )
                    .description(Some("Request identifier, add this to your bug reports."))
                    .build(),
            );
        get.responses
            .responses
            .insert("200".to_string(), RefOr::T(response.build()));

        paths.insert(path, p);

        comps
            .schemas
            .insert(utoipa_type_name.to_string(), utoipa_schema.clone());
        comps
            .schemas
            .insert(set_queue_config_type_name, set_queue_config_schema);
        comps
            .schemas
            .insert(get_queue_config_type_name, get_queue_config_schema);

        // Remove original SetTaskQueueConfigRequest and GetTaskQueueConfigResponse schemas
        comps
            .schemas
            .remove(&SetTaskQueueConfigRequest::name().to_string());
        comps
            .schemas
            .remove(&GetTaskQueueConfigResponse::name().to_string());
    }

    doc
}
