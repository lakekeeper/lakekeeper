#![allow(clippy::needless_for_each)]

use std::{collections::HashMap, sync::LazyLock};

use utoipa::{
    OpenApi, PartialSchema, ToSchema,
    openapi::{ComponentsBuilder, KnownFormat, RefOr, Schema, security::SecurityScheme},
};

use crate::{
    api::{
        endpoints::ManagementV1Endpoint,
        management::v1::task_queue::{
            GetTaskQueueConfigResponse, ScheduleTaskRequest, SetTaskQueueConfigRequest,
        },
    },
    service::{
        authz::Authorizer,
        tasks::{BUILT_IN_DEPENDENT_SCHEMAS, QueueApiConfig, QueueScope, UserScheduling},
    },
};

#[derive(Debug, OpenApi)]
#[openapi(
    info(
        title = "Lakekeeper Management API",
        description = "Lakekeeper is a rust-native Apache Iceberg REST Catalog implementation. The Management API provides endpoints to manage the server, projects, warehouses, users, and roles. If Authorization is enabled, permissions can also be managed. An interactive Swagger-UI for the specific Lakekeeper Version and configuration running is available at `/swagger-ui/#/` of Lakekeeper (by default [http://localhost:8181/swagger-ui/#/](http://localhost:8181/swagger-ui/#/)).",
    ),
    servers(
        (
            url = "{scheme}://{host}{basePath}",
            description = "Lakekeeper Management API",
            variables(
                ("scheme" = (default = "https", description = "The scheme of the URI, either http or https")),
                ("host" = (default = "localhost", description = "The host (and optional port) for the specified server")),
                ("basePath" = (default = "", description = "Optional path prefix (starting with '/') to be prepended to all routes"))
            )
        )
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
        super::batch_check_actions,
        super::bootstrap,
        super::control_tasks,
        super::control_project_tasks,
        super::create_project,
        super::create_role,
        super::create_user,
        super::create_warehouse,
        super::deactivate_warehouse,
        super::delete_project_by_id_deprecated,
        super::delete_project,
        super::delete_role,
        super::delete_user,
        super::delete_warehouse,
        super::get_endpoint_statistics,
        super::get_namespace_actions,
        super::get_namespace_protection,
        super::get_project_actions,
        super::get_project_by_id_deprecated,
        super::get_project,
        super::get_project_task_details,
        super::get_project_task_queue_config,
        super::get_role_actions,
        super::get_role_metadata,
        super::get_role,
        super::get_server_actions,
        super::get_server_info,
        super::get_table_actions,
        super::get_table_protection,
        super::get_task_details,
        super::get_task_queue_config,
        super::get_user_actions,
        super::get_user,
        super::get_view_actions,
        super::get_generic_table_actions,
        super::get_generic_table_protection,
        super::get_view_protection,
        super::get_warehouse_actions,
        super::get_warehouse_statistics,
        super::get_warehouse,
        super::list_deleted_tabulars,
        super::list_projects,
        super::list_project_tasks,
        super::list_roles,
        super::list_tasks,
        super::list_user,
        super::list_warehouses,
        super::rename_project_by_id_deprecated,
        super::rename_project,
        super::rename_warehouse,
        super::search_role,
        super::search_tabular,
        super::search_user,
        super::set_namespace_protection,
        super::set_project_task_queue_config,
        super::set_generic_table_protection,
        super::set_table_protection,
        super::schedule_task,
        super::set_task_queue_config,
        super::set_view_protection,
        super::set_warehouse_protection,
        super::set_warehouse_managed_by,
        super::undrop_tabulars,
        super::update_role_source_system,
        super::update_role,
        super::update_storage_credential,
        super::update_storage_profile,
        super::update_user,
        super::update_warehouse_delete_profile,
        super::update_warehouse_format_version_policy,
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
#[must_use]
pub fn api_doc<A: Authorizer>(
    queue_api_configs: &[&QueueApiConfig],
    project_queue_api_configs: &[&QueueApiConfig],
) -> utoipa::openapi::OpenApi {
    let mut doc = ManagementApiDoc::openapi();
    doc.merge(A::api_doc());

    add_dependent_schemas(&mut doc, &BUILT_IN_DEPENDENT_SCHEMAS);

    fix_task_queue_config_paths(
        &mut doc,
        queue_api_configs,
        ManagementV1Endpoint::SetTaskQueueConfig.path(),
    );
    fix_task_queue_config_paths(
        &mut doc,
        project_queue_api_configs,
        ManagementV1Endpoint::SetProjectTaskQueueConfig.path(),
    );

    fix_task_queue_schedule_paths(
        &mut doc,
        queue_api_configs,
        ManagementV1Endpoint::ScheduleTask.path(),
    );

    doc
}

/// Materialise per-queue schedule paths and request schemas.
///
/// The utoipa-registered placeholder uses `{queue_name}` as a path
/// parameter and a single generic `ScheduleTaskRequest` body. For each
/// queue that opted in via `TaskConfig::user_schedulable()` we:
///
/// - Clone the placeholder, hard-code its `queue_name` into the URL, and
///   rewrite `operation_id` to `schedule_task_<queue>` so each queue is a
///   distinct `OpenAPI` operation.
/// - Clone `ScheduleTaskRequest::schema()` and strip the generic `payload`
///   property when the queue declares no payload (`utoipa_payload_schema =
///   None`), or rewrite it to reference the queue's payload schema when
///   provided. Either way the published request body is type-correct per
///   queue rather than the generic "any JSON" the placeholder shows.
/// - Insert the per-queue request schema as `Schedule{TypeName}TaskRequest`
///   in components.
///
/// Finally the generic `ScheduleTaskRequest` placeholder is removed from
/// `components/schemas`. Queues that did not opt in are invisible in the
/// generated spec.
#[allow(clippy::too_many_lines)]
fn fix_task_queue_schedule_paths(
    doc: &mut utoipa::openapi::OpenApi,
    queue_api_configs: &[&QueueApiConfig],
    schedule_path: &str,
) {
    let Some(comps) = doc.components.as_mut() else {
        tracing::warn!(
            "No components found in the OpenAPI document; \
             not patching per-queue schedule schemas in."
        );
        return;
    };
    let paths = &mut doc.paths.paths;
    let Some(placeholder) = paths.remove(schedule_path) else {
        tracing::warn!(
            "No path found for ScheduleTask placeholder '{schedule_path}'; \
             skipping per-queue schedule path materialisation."
        );
        return;
    };

    for QueueApiConfig {
        queue_name,
        utoipa_type_name,
        user_scheduling,
        scope: _,
        utoipa_schema: _,
    } in queue_api_configs
    {
        let UserScheduling::Enabled { payload_schema } = user_scheduling else {
            continue;
        };

        // Build the per-queue request schema by cloning the placeholder
        // and adjusting its `payload` property to match what the queue
        // actually accepts.
        let mut per_queue_request_schema = ScheduleTaskRequest::schema();
        match &mut per_queue_request_schema {
            RefOr::Ref(_) => {
                unreachable!("ScheduleTaskRequest::schema() returns an inline schema");
            }
            RefOr::T(Schema::Object(obj)) => match payload_schema.as_ref() {
                None => {
                    obj.properties.remove("payload");
                    obj.required.retain(|r| r != "payload");
                }
                Some(payload_ref) => {
                    obj.properties
                        .insert("payload".to_string(), payload_ref.clone());
                }
            },
            RefOr::T(_) => {
                unreachable!("ScheduleTaskRequest::schema() returns an Object schema");
            }
        }
        // The display stem for the schedule request schema. We strip a
        // trailing `QueueConfig` from the config type name so we get e.g.
        // `ScheduleRemoveOrphanFilesTaskRequest` instead of
        // `ScheduleRemoveOrphanFilesQueueConfigTaskRequest`.
        let display_stem = utoipa_type_name
            .strip_suffix("QueueConfig")
            .unwrap_or(utoipa_type_name);
        let per_queue_request_name = format!("Schedule{display_stem}TaskRequest");

        comps
            .schemas
            .insert(per_queue_request_name.clone(), per_queue_request_schema);

        let concrete_path = schedule_path.replace("{queue_name}", queue_name);
        let mut path_item = placeholder.clone();

        let Some(post) = path_item.post.as_mut() else {
            // Skip this queue rather than bailing out of the entire loop —
            // one malformed item shouldn't hide the rest from the spec.
            tracing::warn!(
                "No POST method on ScheduleTask placeholder '{schedule_path}'; \
                 not materialising schedule path for queue '{queue_name}'."
            );
            continue;
        };
        post.parameters = post.parameters.take().map(|params| {
            params
                .into_iter()
                .filter(|p| p.name != "queue_name")
                .collect()
        });
        post.operation_id = Some(format!("schedule_task_{}", queue_name.replace('-', "_")));
        if let Some(body) = post.request_body.as_mut() {
            body.content.insert(
                "application/json".to_string(),
                utoipa::openapi::ContentBuilder::new()
                    .schema(Some(RefOr::Ref(
                        utoipa::openapi::schema::RefBuilder::new()
                            .ref_location_from_schema_name(per_queue_request_name)
                            .build(),
                    )))
                    .build(),
            );
        }

        paths.insert(concrete_path, path_item);
    }

    // Remove the generic placeholder schema — every callable schedule path
    // now references a concrete `Schedule{TypeName}TaskRequest`.
    comps
        .schemas
        .remove(&ScheduleTaskRequest::name().to_string());
}

#[allow(clippy::too_many_lines)]
fn fix_task_queue_config_paths(
    doc: &mut utoipa::openapi::OpenApi,
    queue_api_configs: &[&QueueApiConfig],
    set_task_queue_config_path: &str,
) {
    let Some(comps) = doc.components.as_mut() else {
        tracing::warn!(
            "No components found in the OpenAPI document, not patching queue configs in."
        );
        return;
    };
    let paths = &mut doc.paths.paths;
    let Some(config_path) = paths.remove(set_task_queue_config_path) else {
        tracing::warn!(
            "No path found for SetTaskQueueConfigRequest, not patching queue configs in."
        );
        return;
    };

    for QueueApiConfig {
        queue_name,
        utoipa_type_name,
        utoipa_schema,
        scope,
        user_scheduling: _,
    } in queue_api_configs
    {
        let operation_object = match scope {
            QueueScope::Project => "project_task_queue_config",
            QueueScope::Warehouse => "task_queue_config",
        };

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
                        unreachable!(
                            "The schema for SetTaskQueueConfigRequest should have a 'queue-config' property."
                        );
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
                        unreachable!(
                            "The schema for GetTaskQueueConfigResponse should have a 'queue-config' property."
                        );
                    }
                }
                _ => {
                    unreachable!("The schema for GetTaskQueueConfigResponse should be an object.");
                }
            },
        }

        let path = set_task_queue_config_path.replace("{queue_name}", queue_name);

        let mut p = config_path.clone();

        let Some(post) = p.post.as_mut() else {
            tracing::warn!(
                "No post method found for '{}' for queue '{queue_name}'; \
                 skipping this queue and continuing with the rest.",
                set_task_queue_config_path
            );
            continue;
        };
        post.parameters = post.parameters.take().map(|params| {
            params
                .into_iter()
                .filter(|param| param.name != "queue_name")
                .collect()
        });
        post.operation_id = Some(format!(
            "set_{operation_object}_{}",
            queue_name.replace('-', "_")
        ));
        let Some(body) = post.request_body.as_mut() else {
            tracing::warn!(
                "No request body found for '{}' for queue '{queue_name}'; \
                 skipping this queue and continuing with the rest.",
                set_task_queue_config_path
            );
            continue;
        };
        body.content.insert(
            "application/json".to_string(),
            utoipa::openapi::ContentBuilder::new()
                .schema(Some(set_queue_config_type_ref))
                .build(),
        );
        let Some(get) = p.get.as_mut() else {
            tracing::warn!(
                "No get method found for '{}' for queue '{queue_name}'; \
                 skipping this queue and continuing with the rest.",
                set_task_queue_config_path
            );
            continue;
        };
        get.parameters = get.parameters.take().map(|params| {
            params
                .into_iter()
                .filter(|param| param.name != "queue_name")
                .collect()
        });
        get.operation_id = Some(format!(
            "get_{operation_object}_{}",
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
    }

    // Remove the generic placeholder schemas — every callable path now
    // references a concrete per-queue type. Doing this after the loop
    // ensures the placeholders are dropped even when `queue_api_configs`
    // is empty.
    comps
        .schemas
        .remove(&SetTaskQueueConfigRequest::name().to_string());
    comps
        .schemas
        .remove(&GetTaskQueueConfigResponse::name().to_string());
}

fn add_dependent_schemas(
    doc: &mut utoipa::openapi::OpenApi,
    dependent_schemas: &LazyLock<HashMap<String, RefOr<Schema>>>,
) {
    let dependent_schemas = dependent_schemas
        .iter()
        .map(|(name, schema)| (name.clone(), (*schema).clone()));
    let Some(comps) = doc.components.as_mut() else {
        let mut comps = ComponentsBuilder::new().build();
        comps.schemas.extend(dependent_schemas);
        doc.components = Some(comps);
        return;
    };
    comps.schemas.extend(dependent_schemas);
}
