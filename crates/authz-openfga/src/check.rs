use std::sync::Arc;

use http::StatusCode;
use lakekeeper::{
    ProjectId, WarehouseId,
    api::{
        ApiContext, RequestMetadata,
        management::v1::check::{NamespaceIdentOrUuid, TabularIdentOrUuid, UserOrRole},
    },
    axum::{Extension, Json, extract::State as AxumState},
    iceberg::TableIdent,
    service::{
        AuthZGenericTableInfo as _, AuthZTableInfo, AuthZViewInfo as _, CatalogNamespaceOps,
        CatalogStore, CatalogTabularOps, CatalogWarehouseOps, GenericTableId,
        GenericTableIdentOrId, NamespaceIdentOrId, Result, SecretStore, State, TableId,
        TableIdentOrId, TabularListFlags, ViewId, ViewIdentOrId,
        authz::{
            AuthZError, AuthZGenericTableOps, AuthZTableOps, AuthZViewOps, AuthzNamespaceOps as _,
            AuthzWarehouseOps, RequireGenericTableActionError, RequireTableActionError,
            RequireViewActionError,
        },
        events::{APIEventContext, EventDispatcher, context::authz_to_error_no_audit},
    },
    tokio,
};
use openfga_client::client::CheckRequestTupleKey;
use serde::{Deserialize, Serialize};

use super::{
    OpenFGAAuthorizer, OpenFGAError,
    relations::{
        APIGenericTableAction as GenericTableAction, APINamespaceAction as NamespaceAction,
        APIProjectAction as ProjectAction, APIProjectAction, APIServerAction as ServerAction,
        APIServerAction, APITableAction as TableAction, APIViewAction as ViewAction,
        APIWarehouseAction as WarehouseAction, APIWarehouseAction,
        GenericTableRelation as AllGenericTableRelations,
        NamespaceRelation as AllNamespaceRelations, ProjectRelation as AllProjectRelations,
        ReducedRelation, ServerRelation as AllServerAction, TableRelation as AllTableRelations,
        ViewRelation as AllViewRelations, WarehouseRelation as AllWarehouseRelation,
    },
};
use crate::entities::OpenFgaEntity;

/// Check if a specific action is allowed on the given object
#[cfg_attr(feature = "open-api", utoipa::path(
    post,
    tag = "permissions-openfga",
    path = "/management/v1/permissions/check",
    request_body = CheckRequest,
    responses(
            (status = 200, body = CheckResponse),
    )
))]
pub(super) async fn check<C: CatalogStore, S: SecretStore>(
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Json(request): Json<CheckRequest>,
) -> Result<(StatusCode, Json<CheckResponse>)> {
    let allowed = check_internal(api_context, Arc::new(metadata), request).await?;
    Ok((StatusCode::OK, Json(CheckResponse { allowed })))
}

async fn check_internal<C: CatalogStore, S: SecretStore>(
    api_context: ApiContext<State<OpenFGAAuthorizer, C, S>>,
    metadata: Arc<RequestMetadata>,
    request: CheckRequest,
) -> Result<bool> {
    let authorizer = api_context.v1_state.authz.clone();
    let event_dispatcher = api_context.v1_state.events.clone();

    let CheckRequest {
        // If for_principal is specified, the user needs to have the
        // CanReadAssignments permission
        identity: mut for_principal,
        operation: action_request,
    } = request;
    // Set for_principal to None if the user is checking their own access
    let user_or_role = metadata.actor().api_user_or_role();
    if let Some(user_or_role) = &user_or_role {
        for_principal = for_principal.filter(|p| p != user_or_role);
    }

    let metadata_clone = metadata.clone();
    let (action, object) = match &action_request {
        CheckOperation::Server { action } => {
            check_server(
                metadata_clone,
                &authorizer,
                &mut for_principal,
                action,
                event_dispatcher,
            )
            .await?
        }
        CheckOperation::Project { action, project_id } => {
            check_project(
                metadata_clone,
                &authorizer,
                for_principal.as_ref(),
                action,
                project_id.clone(),
                event_dispatcher,
            )
            .await?
        }
        CheckOperation::Warehouse {
            action,
            warehouse_id,
        } => {
            check_warehouse(
                metadata_clone,
                &authorizer,
                for_principal.as_ref(),
                action,
                *warehouse_id,
                event_dispatcher,
            )
            .await?
        }
        CheckOperation::Namespace { action, namespace } => (
            action.to_openfga().to_string(),
            check_namespace(
                api_context.clone(),
                metadata_clone,
                namespace,
                for_principal.as_ref(),
            )
            .await?,
        ),
        CheckOperation::Table { action, table } => (action.to_openfga().to_string(), {
            check_table(
                api_context.clone(),
                metadata_clone,
                table,
                for_principal.as_ref(),
            )
            .await?
        }),
        CheckOperation::View { action, view } => (action.to_openfga().to_string(), {
            check_view(api_context, metadata_clone, view, for_principal.as_ref()).await?
        }),
        CheckOperation::GenericTable {
            action,
            generic_table,
        } => (action.to_openfga().to_string(), {
            check_generic_table(
                api_context,
                metadata_clone,
                generic_table,
                for_principal.as_ref(),
            )
            .await?
        }),
    };

    let user = if let Some(for_principal) = &for_principal {
        for_principal.to_openfga()
    } else {
        metadata.actor().to_openfga()
    };

    let allowed = authorizer
        .check(CheckRequestTupleKey {
            user,
            relation: action,
            object,
        })
        .await
        .map_err(authz_to_error_no_audit)?;

    Ok(allowed)
}

async fn check_warehouse(
    metadata: Arc<RequestMetadata>,
    authorizer: &OpenFGAAuthorizer,
    for_principal: Option<&UserOrRole>,
    action: &APIWarehouseAction,
    warehouse_id: WarehouseId,
    event_dispatcher: EventDispatcher,
) -> Result<(String, String)> {
    let required_action = for_principal
        .as_ref()
        .map_or(AllWarehouseRelation::CanGetMetadata, |_| {
            AllWarehouseRelation::CanReadAssignments
        });
    let event_ctx =
        APIEventContext::for_warehouse(metadata, event_dispatcher, warehouse_id, required_action);

    let authz_result = authorizer
        .require_action(
            event_ctx.request_metadata(),
            *event_ctx.action(),
            &warehouse_id.to_openfga(),
        )
        .await;

    let (_event_ctx, ()) = event_ctx.emit_authz(authz_result)?;

    Ok((
        action.to_openfga().to_string(),
        warehouse_id.to_openfga().clone(),
    ))
}

async fn check_project(
    metadata: Arc<RequestMetadata>,
    authorizer: &OpenFGAAuthorizer,
    for_principal: Option<&UserOrRole>,
    action: &APIProjectAction,
    project_id: Option<ProjectId>,
    event_dispatcher: EventDispatcher,
) -> Result<(String, String)> {
    let project_id = project_id
        .or_else(|| metadata.preferred_project_id().map(|p| (*p).clone()))
        .ok_or(OpenFGAError::NoProjectId)
        .map_err(authz_to_error_no_audit)?;
    let project_id_openfga = project_id.to_openfga();

    let action_to_check = for_principal
        .as_ref()
        .map_or(AllProjectRelations::CanGetMetadata, |_| {
            AllProjectRelations::CanReadAssignments
        });

    let event_ctx = APIEventContext::for_project(
        metadata,
        event_dispatcher,
        project_id.clone(),
        action_to_check,
    );

    let authz_result = authorizer
        .require_action(
            event_ctx.request_metadata(),
            action_to_check,
            &project_id_openfga,
        )
        .await;
    let (_event_ctx, ()) = event_ctx.emit_authz(authz_result)?;
    Ok((action.to_openfga().to_string(), project_id_openfga))
}

async fn check_server(
    metadata: Arc<RequestMetadata>,
    authorizer: &OpenFGAAuthorizer,
    for_principal: &mut Option<UserOrRole>,
    action: &APIServerAction,
    event_dispatcher: EventDispatcher,
) -> Result<(String, String)> {
    let openfga_server = authorizer.openfga_server().clone();

    let event_ctx = APIEventContext::for_server(
        metadata,
        event_dispatcher,
        AllServerAction::CanReadAssignments,
        lakekeeper::service::authz::Authorizer::server_id(authorizer),
    );

    if for_principal.is_some() {
        let authz_result = authorizer
            .require_action(
                event_ctx.request_metadata(),
                *event_ctx.action(),
                &openfga_server,
            )
            .await;
        let (_event_ctx, ()) = event_ctx.emit_authz(authz_result)?;
    }

    Ok((action.to_openfga().to_string(), openfga_server))
}

async fn check_namespace<C: CatalogStore, S: SecretStore>(
    api_context: ApiContext<State<OpenFGAAuthorizer, C, S>>,
    metadata: Arc<RequestMetadata>,
    namespace: &NamespaceIdentOrUuid,
    for_principal: Option<&UserOrRole>,
) -> Result<String> {
    let action = for_principal.map_or(AllNamespaceRelations::CanGetMetadata, |_| {
        AllNamespaceRelations::CanReadAssignments
    });

    let (warehouse_id, user_provided_ns) = match namespace {
        NamespaceIdentOrUuid::Id {
            namespace_id,
            warehouse_id,
        } => (*warehouse_id, NamespaceIdentOrId::from(*namespace_id)),
        NamespaceIdentOrUuid::Name {
            namespace,
            warehouse_id,
        } => (*warehouse_id, NamespaceIdentOrId::from(namespace.clone())),
    };

    let event_ctx = APIEventContext::for_namespace(
        metadata,
        api_context.v1_state.events.clone(),
        warehouse_id,
        user_provided_ns.clone(),
        action,
    );

    let authz_result = authorize_check_namespace::<C, S>(
        &api_context,
        event_ctx.request_metadata(),
        warehouse_id,
        user_provided_ns,
        action,
    )
    .await;
    let (_, ns_openfga) = event_ctx.emit_authz(authz_result)?;

    Ok(ns_openfga)
}

async fn authorize_check_namespace<C: CatalogStore, S: SecretStore>(
    api_context: &ApiContext<State<OpenFGAAuthorizer, C, S>>,
    metadata: &RequestMetadata,
    warehouse_id: WarehouseId,
    user_provided_ns: NamespaceIdentOrId,
    action: AllNamespaceRelations,
) -> Result<String, AuthZError> {
    let authorizer = api_context.v1_state.authz.clone();
    let (warehouse, namespace) = tokio::join!(
        C::get_active_warehouse_by_id(warehouse_id, api_context.v1_state.catalog.clone(),),
        C::get_namespace(
            warehouse_id,
            user_provided_ns.clone(),
            api_context.v1_state.catalog.clone(),
        )
    );
    let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;
    let namespace = authorizer
        .require_namespace_action(metadata, &warehouse, user_provided_ns, namespace, action)
        .await?;

    Ok(namespace.namespace_id().to_openfga())
}

async fn check_table<C: CatalogStore, S: SecretStore>(
    api_context: ApiContext<State<OpenFGAAuthorizer, C, S>>,
    metadata: Arc<RequestMetadata>,
    table: &TabularIdentOrUuid,
    for_principal: Option<&UserOrRole>,
) -> Result<String> {
    let action = for_principal.map_or(AllTableRelations::CanGetMetadata, |_| {
        AllTableRelations::CanReadAssignments
    });

    let (warehouse_id, table) = match table {
        TabularIdentOrUuid::IdInWarehouse {
            warehouse_id,
            table_id,
        } => (*warehouse_id, TableIdentOrId::Id(TableId::from(*table_id))),
        TabularIdentOrUuid::Name {
            namespace,
            table,
            warehouse_id,
        } => (
            *warehouse_id,
            TableIdentOrId::Ident(TableIdent {
                namespace: namespace.clone(),
                name: table.clone(),
            }),
        ),
    };

    let event_ctx = APIEventContext::for_table(
        metadata,
        api_context.v1_state.events.clone(),
        warehouse_id,
        table.clone(),
        action,
    );

    let authz_result = authorize_check_table::<C, S>(
        &api_context,
        event_ctx.request_metadata(),
        warehouse_id,
        table,
        action,
    )
    .await;
    let (_, table_openfga) = event_ctx.emit_authz(authz_result)?;

    Ok(table_openfga)
}

async fn authorize_check_table<C: CatalogStore, S: SecretStore>(
    api_context: &ApiContext<State<OpenFGAAuthorizer, C, S>>,
    metadata: &RequestMetadata,
    warehouse_id: WarehouseId,
    table: TableIdentOrId,
    action: AllTableRelations,
) -> Result<String, AuthZError> {
    let authorizer = api_context.v1_state.authz.clone();
    let (warehouse, table_info) = tokio::join!(
        C::get_active_warehouse_by_id(warehouse_id, api_context.v1_state.catalog.clone()),
        C::get_table_info(
            warehouse_id,
            table.clone(),
            // Include deleted + staged so `/check` evaluates permissions
            // (e.g. `Undrop`) instead of returning NotFound on soft-deleted
            // tabulars. Matches the bulk /check endpoint.
            TabularListFlags::all(),
            api_context.v1_state.catalog.clone(),
        )
    );
    let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;
    let table_info = authorizer.require_table_presence(warehouse_id, table.clone(), table_info)?;
    let namespace = C::get_namespace(
        warehouse_id,
        table_info.namespace_id(),
        api_context.v1_state.catalog.clone(),
    )
    .await;
    let namespace = authorizer.require_namespace_presence(
        warehouse_id,
        table_info.namespace_id(),
        namespace,
    )?;
    let table_info = authorizer
        .require_table_action(
            metadata,
            &warehouse,
            &namespace,
            table,
            Ok::<_, RequireTableActionError>(Some(table_info)),
            action,
        )
        .await?;

    Ok((warehouse_id, table_info.table_id()).to_openfga())
}

async fn check_view<C: CatalogStore, S: SecretStore>(
    api_context: ApiContext<State<OpenFGAAuthorizer, C, S>>,
    metadata: Arc<RequestMetadata>,
    view: &TabularIdentOrUuid,
    for_principal: Option<&UserOrRole>,
) -> Result<String> {
    let action = for_principal.map_or(AllViewRelations::CanGetMetadata, |_| {
        AllViewRelations::CanReadAssignments
    });

    let (warehouse_id, view) = match view {
        TabularIdentOrUuid::IdInWarehouse {
            warehouse_id,
            table_id,
        } => (*warehouse_id, ViewIdentOrId::Id(ViewId::from(*table_id))),
        TabularIdentOrUuid::Name {
            namespace,
            table,
            warehouse_id,
        } => (
            *warehouse_id,
            ViewIdentOrId::Ident(TableIdent {
                namespace: namespace.clone(),
                name: table.clone(),
            }),
        ),
    };

    let event_ctx = APIEventContext::for_view(
        metadata,
        api_context.v1_state.events.clone(),
        warehouse_id,
        view.clone(),
        action,
    );

    let authz_result = authorize_check_view::<C, S>(
        &api_context,
        event_ctx.request_metadata(),
        warehouse_id,
        view,
        action,
    )
    .await;
    let (_, view_openfga) = event_ctx.emit_authz(authz_result)?;

    Ok(view_openfga)
}

async fn authorize_check_view<C: CatalogStore, S: SecretStore>(
    api_context: &ApiContext<State<OpenFGAAuthorizer, C, S>>,
    metadata: &RequestMetadata,
    warehouse_id: WarehouseId,
    view: ViewIdentOrId,
    action: AllViewRelations,
) -> Result<String, AuthZError> {
    let authorizer = api_context.v1_state.authz.clone();
    let (warehouse, table_info) = tokio::join!(
        C::get_active_warehouse_by_id(warehouse_id, api_context.v1_state.catalog.clone()),
        C::get_view_info(
            warehouse_id,
            view.clone(),
            // Include deleted + staged so `/check` evaluates permissions
            // (e.g. `Undrop`) instead of returning NotFound on soft-deleted
            // tabulars. Matches the bulk /check endpoint.
            TabularListFlags::all(),
            api_context.v1_state.catalog.clone(),
        )
    );
    let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;
    let view_info = authorizer.require_view_presence(warehouse_id, view.clone(), table_info)?;
    let namespace = C::get_namespace(
        warehouse_id,
        view_info.namespace_id(),
        api_context.v1_state.catalog.clone(),
    )
    .await;
    let namespace =
        authorizer.require_namespace_presence(warehouse_id, view_info.namespace_id(), namespace)?;

    let view_info = authorizer
        .require_view_action(
            metadata,
            &warehouse,
            &namespace,
            view,
            Ok::<_, RequireViewActionError>(Some(view_info)),
            action,
        )
        .await?;

    Ok((warehouse_id, view_info.view_id()).to_openfga())
}

async fn check_generic_table<C: CatalogStore, S: SecretStore>(
    api_context: ApiContext<State<OpenFGAAuthorizer, C, S>>,
    metadata: Arc<RequestMetadata>,
    generic_table: &TabularIdentOrUuid,
    for_principal: Option<&UserOrRole>,
) -> Result<String> {
    let action = for_principal.map_or(AllGenericTableRelations::CanGetMetadata, |_| {
        AllGenericTableRelations::CanReadAssignments
    });

    let (warehouse_id, generic_table) = match generic_table {
        TabularIdentOrUuid::IdInWarehouse {
            warehouse_id,
            table_id,
        } => (
            *warehouse_id,
            GenericTableIdentOrId::Id(GenericTableId::from(*table_id)),
        ),
        TabularIdentOrUuid::Name {
            namespace,
            table,
            warehouse_id,
        } => (
            *warehouse_id,
            GenericTableIdentOrId::Ident(TableIdent {
                namespace: namespace.clone(),
                name: table.clone(),
            }),
        ),
    };

    let event_ctx = APIEventContext::for_generic_table(
        metadata,
        api_context.v1_state.events.clone(),
        warehouse_id,
        generic_table.clone(),
        action,
    );

    let authz_result = authorize_check_generic_table::<C, S>(
        &api_context,
        event_ctx.request_metadata(),
        warehouse_id,
        generic_table,
        action,
    )
    .await;
    let (_, gt_openfga) = event_ctx.emit_authz(authz_result)?;

    Ok(gt_openfga)
}

async fn authorize_check_generic_table<C: CatalogStore, S: SecretStore>(
    api_context: &ApiContext<State<OpenFGAAuthorizer, C, S>>,
    metadata: &RequestMetadata,
    warehouse_id: WarehouseId,
    generic_table: GenericTableIdentOrId,
    action: AllGenericTableRelations,
) -> Result<String, AuthZError> {
    let authorizer = api_context.v1_state.authz.clone();
    let (warehouse, gt_info) = tokio::join!(
        C::get_active_warehouse_by_id(warehouse_id, api_context.v1_state.catalog.clone()),
        C::get_generic_table_info(
            warehouse_id,
            generic_table.clone(),
            // Include deleted + staged so `/check` evaluates permissions
            // (e.g. `Undrop`) instead of returning NotFound on soft-deleted
            // tabulars. Matches the bulk /check endpoint.
            TabularListFlags::all(),
            api_context.v1_state.catalog.clone(),
        )
    );
    let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;
    let gt_info =
        authorizer.require_generic_table_presence(warehouse_id, generic_table.clone(), gt_info)?;
    let namespace = C::get_namespace(
        warehouse_id,
        gt_info.namespace_id(),
        api_context.v1_state.catalog.clone(),
    )
    .await;
    let namespace =
        authorizer.require_namespace_presence(warehouse_id, gt_info.namespace_id(), namespace)?;

    let gt_info = authorizer
        .require_generic_table_action(
            metadata,
            &warehouse,
            &namespace,
            generic_table,
            Ok::<_, RequireGenericTableActionError>(Some(gt_info)),
            action,
        )
        .await?;

    Ok((warehouse_id, gt_info.generic_table_id()).to_openfga())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
/// Represents an action on an object
pub(super) enum CheckOperation {
    Server {
        action: ServerAction,
    },
    #[serde(rename_all = "kebab-case")]
    Project {
        action: ProjectAction,
        #[cfg_attr(feature = "open-api", schema(value_type = Option<uuid::Uuid>))]
        project_id: Option<ProjectId>,
    },
    #[serde(rename_all = "kebab-case")]
    Warehouse {
        action: WarehouseAction,
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        warehouse_id: WarehouseId,
    },
    Namespace {
        action: NamespaceAction,
        #[serde(flatten)]
        namespace: NamespaceIdentOrUuid,
    },
    Table {
        action: TableAction,
        #[serde(flatten)]
        table: TabularIdentOrUuid,
    },
    View {
        action: ViewAction,
        #[serde(flatten)]
        view: TabularIdentOrUuid,
    },
    GenericTable {
        action: GenericTableAction,
        #[serde(flatten)]
        generic_table: TabularIdentOrUuid,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
/// Check if a specific action is allowed on the given object
pub(super) struct CheckRequest {
    /// The user or role to check access for.
    identity: Option<UserOrRole>,
    /// The operation to check.
    operation: CheckOperation,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub(super) struct CheckResponse {
    /// Whether the action is allowed.
    allowed: bool,
}

#[cfg(test)]
mod tests {
    use lakekeeper::service::{NamespaceId, NamespaceIdent, UserId};

    use super::*;

    #[test]
    fn test_serde_check_action_namespace_id() {
        let action = CheckOperation::Namespace {
            action: NamespaceAction::CreateTable,
            namespace: NamespaceIdentOrUuid::Id {
                warehouse_id: WarehouseId::from_str_or_internal(
                    "490cbf7a-cbfe-11ef-84c5-178606d4cab3",
                )
                .unwrap(),
                namespace_id: NamespaceId::from_str_or_internal(
                    "00000000-0000-0000-0000-000000000000",
                )
                .unwrap(),
            },
        };
        let json = serde_json::to_value(&action).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "namespace": {
                    "action": "create_table",
                    "namespace-id": "00000000-0000-0000-0000-000000000000",
                    "warehouse-id": "490cbf7a-cbfe-11ef-84c5-178606d4cab3"
                }
            })
        );
    }

    #[test]
    fn test_serde_check_action_table_id_variant() {
        let action = CheckOperation::Table {
            action: TableAction::GetMetadata,
            table: TabularIdentOrUuid::IdInWarehouse {
                table_id: uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap(),
                warehouse_id: WarehouseId::from_str_or_internal(
                    "490cbf7a-cbfe-11ef-84c5-178606d4cab3",
                )
                .unwrap(),
            },
        };
        let json = serde_json::to_value(&action).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "table": {
                    "action": "get_metadata",
                    "table-id": "00000000-0000-0000-0000-000000000001",
                    "warehouse-id": "490cbf7a-cbfe-11ef-84c5-178606d4cab3"
                }
            })
        );
    }

    #[test]
    fn test_serde_check_action_generic_table_id_variant() {
        let action = CheckOperation::GenericTable {
            action: GenericTableAction::ReadData,
            generic_table: TabularIdentOrUuid::IdInWarehouse {
                table_id: uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000003").unwrap(),
                warehouse_id: WarehouseId::from_str_or_internal(
                    "490cbf7a-cbfe-11ef-84c5-178606d4cab3",
                )
                .unwrap(),
            },
        };
        let json = serde_json::to_value(&action).unwrap();
        // Output uses the canonical `table-id` field name (the alias
        // `generic_table_id` is only accepted on input — consistent with
        // how view checks emit `table-id`).
        assert_eq!(
            json,
            serde_json::json!({
                "generic-table": {
                    "action": "read_data",
                    "table-id": "00000000-0000-0000-0000-000000000003",
                    "warehouse-id": "490cbf7a-cbfe-11ef-84c5-178606d4cab3"
                }
            })
        );
        // And the alias is accepted on input.
        let from_alias: CheckOperation = serde_json::from_value(serde_json::json!({
            "generic-table": {
                "action": "read_data",
                "generic_table_id": "00000000-0000-0000-0000-000000000003",
                "warehouse-id": "490cbf7a-cbfe-11ef-84c5-178606d4cab3"
            }
        }))
        .expect("generic_table_id alias must deserialize");
        assert_eq!(from_alias, action);
    }

    #[test]
    fn test_serde_check_action_view_id_alias() {
        let action = CheckOperation::View {
            action: ViewAction::GetMetadata,
            view: TabularIdentOrUuid::IdInWarehouse {
                table_id: uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap(),
                warehouse_id: WarehouseId::from_str_or_internal(
                    "490cbf7a-cbfe-11ef-84c5-178606d4cab3",
                )
                .unwrap(),
            },
        };
        let json = serde_json::to_value(&action).unwrap();
        // Accepts "view_id" as alias on input; on output it should be "table-id".
        assert_eq!(
            json,
            serde_json::json!({
                "view": {
                    "action": "get_metadata",
                    "table-id": "00000000-0000-0000-0000-000000000002",
                    "warehouse-id": "490cbf7a-cbfe-11ef-84c5-178606d4cab3"
                }
            })
        );
    }

    #[test]
    fn test_serde_check_action_table_name() {
        let action = CheckOperation::Table {
            action: TableAction::GetMetadata,
            table: TabularIdentOrUuid::Name {
                namespace: NamespaceIdent::from_vec(vec!["trino_namespace".to_string()]).unwrap(),
                table: "trino_table".to_string(),
                warehouse_id: WarehouseId::from_str_or_internal(
                    "490cbf7a-cbfe-11ef-84c5-178606d4cab3",
                )
                .unwrap(),
            },
        };
        let json = serde_json::to_value(&action).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "table": {
                    "action": "get_metadata",
                    "namespace": ["trino_namespace"],
                    "table": "trino_table",
                    "warehouse-id": "490cbf7a-cbfe-11ef-84c5-178606d4cab3"
                }
            })
        );
    }

    #[test]
    fn test_serde_check_request_namespace() {
        let operation = CheckOperation::Namespace {
            action: NamespaceAction::GetMetadata,
            namespace: NamespaceIdentOrUuid::Name {
                namespace: NamespaceIdent::from_vec(vec!["trino_namespace".to_string()]).unwrap(),
                warehouse_id: WarehouseId::from_str_or_internal(
                    "490cbf7a-cbfe-11ef-84c5-178606d4cab3",
                )
                .unwrap(),
            },
        };
        let check = CheckRequest {
            identity: Some(UserOrRole::User(UserId::new_unchecked(
                "oidc",
                "cfb55bf6-fcbb-4a1e-bfec-30c6649b52f8",
            ))),
            operation,
        };
        let json = serde_json::to_value(&check).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                    "identity": {
                        "user": "oidc~cfb55bf6-fcbb-4a1e-bfec-30c6649b52f8"
                    },
                    "operation": {
                        "namespace": {
                            "action": "get_metadata",
                            "namespace": ["trino_namespace"],
                            "warehouse-id": "490cbf7a-cbfe-11ef-84c5-178606d4cab3"
                        }
                    }
                }
            )
        );
    }

    mod openfga_integration_tests {
        use lakekeeper::{
            api::{
                CreateNamespaceRequest,
                iceberg::v1::{Prefix, namespace::NamespaceService},
                management::v1::{
                    ApiServer,
                    role::{CreateRoleRequest, Service as RoleService},
                },
            },
            implementations::postgres::{PostgresBackend, SecretsState},
            server::{CatalogServer, NAMESPACE_ID_PROPERTY},
            service::{CreateNamespaceResponse, NamespaceId, NamespaceIdent, authn::UserId},
            sqlx,
            tests::{SetupTestCatalog, TestWarehouseResponse},
        };
        use openfga_client::client::TupleKey;
        use strum::IntoEnumIterator;
        use uuid::Uuid;

        use super::super::*;
        use crate::migration::tests::authorizer_for_empty_store;

        async fn setup(
            operator_id: UserId,
            pool: sqlx::PgPool,
        ) -> (
            ApiContext<State<OpenFGAAuthorizer, PostgresBackend, SecretsState>>,
            TestWarehouseResponse,
            CreateNamespaceResponse,
        ) {
            let authorizer = authorizer_for_empty_store().await.1;

            let (ctx, warehouse) = SetupTestCatalog::builder()
                .pool(pool.clone())
                .authorizer(authorizer.clone())
                .user_id(Some(operator_id.clone()))
                .build()
                .setup()
                .await;

            let namespace = CatalogServer::create_namespace(
                Some(Prefix::from(warehouse.warehouse_id.to_string())),
                CreateNamespaceRequest {
                    namespace: NamespaceIdent::from_vec(vec!["ns1".to_string()]).unwrap(),
                    properties: None,
                },
                ctx.clone(),
                RequestMetadata::test_user(operator_id.clone()),
            )
            .await
            .unwrap();

            (ctx, warehouse, namespace)
        }

        #[sqlx::test]
        async fn test_check_assume_role(pool: sqlx::PgPool) {
            let operator_id = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());
            let (ctx, _warehouse, _namespace) = setup(operator_id.clone(), pool).await;
            let user_id = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());
            let user_metadata = Arc::new(RequestMetadata::test_user(user_id.clone()));
            let operator_metadata = Arc::new(RequestMetadata::test_user(operator_id.clone()));

            let role_id = ApiServer::create_role(
                CreateRoleRequest::builder()
                    .name("test_role".to_string())
                    .build(),
                ctx.clone(),
                (*operator_metadata).clone(),
            )
            .await
            .unwrap()
            .id;
            let role = UserOrRole::Role(role_id.into_api_assignee());

            // User cannot check access for role without beeing a member
            let request = CheckRequest {
                identity: Some(role.clone()),
                operation: CheckOperation::Server {
                    action: ServerAction::ProvisionUsers,
                },
            };
            check_internal(ctx.clone(), user_metadata, request.clone())
                .await
                .unwrap_err();
            // Admin can check access for role
            let request = CheckRequest {
                identity: Some(role.clone()),
                operation: CheckOperation::Server {
                    action: ServerAction::ProvisionUsers,
                },
            };
            let allowed = check_internal(ctx.clone(), operator_metadata, request)
                .await
                .unwrap();
            assert!(!allowed);
        }

        #[sqlx::test]
        async fn test_check(pool: sqlx::PgPool) {
            let operator_id = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());
            let (ctx, warehouse, namespace) = setup(operator_id.clone(), pool).await;
            let namespace_id = NamespaceId::from_str_or_internal(
                namespace
                    .properties
                    .unwrap()
                    .get(NAMESPACE_ID_PROPERTY)
                    .unwrap(),
            )
            .unwrap();

            let nobody_id = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());
            let nobody_metadata = Arc::new(RequestMetadata::test_user(nobody_id.clone()));
            let user_1_id = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());
            let user_1_metadata = Arc::new(RequestMetadata::test_user(user_1_id.clone()));

            ctx.v1_state
                .authz
                .write(
                    Some(vec![TupleKey {
                        condition: None,
                        object: namespace_id.clone().to_openfga(),
                        relation: AllNamespaceRelations::Select.to_string(),
                        user: user_1_id.to_openfga(),
                    }]),
                    None,
                )
                .await
                .unwrap();

            let server_operations =
                ServerAction::iter().map(|a| CheckOperation::Server { action: a });
            let project_operations = ProjectAction::iter().map(|a| CheckOperation::Project {
                action: a,
                project_id: None,
            });
            let warehouse_operations = WarehouseAction::iter().map(|a| CheckOperation::Warehouse {
                action: a,
                warehouse_id: warehouse.warehouse_id,
            });
            let namespace_ids = &[
                NamespaceIdentOrUuid::Id {
                    namespace_id,
                    warehouse_id: warehouse.warehouse_id,
                },
                NamespaceIdentOrUuid::Name {
                    namespace: namespace.namespace,
                    warehouse_id: warehouse.warehouse_id,
                },
            ];
            let namespace_operations = NamespaceAction::iter().flat_map(|a| {
                namespace_ids
                    .iter()
                    .map(move |n| CheckOperation::Namespace {
                        action: a,
                        namespace: n.clone(),
                    })
            });

            for action in itertools::chain!(
                server_operations,
                project_operations,
                warehouse_operations,
                namespace_operations
            ) {
                let request = CheckRequest {
                    identity: None,
                    operation: action.clone(),
                };

                // Nobody & anonymous can check own access on server level
                if let CheckOperation::Server { .. } = &action {
                    let allowed =
                        check_internal(ctx.clone(), nobody_metadata.clone(), request.clone())
                            .await
                            .unwrap();
                    assert!(!allowed);
                    // Anonymous can check his own access
                    let allowed = check_internal(
                        ctx.clone(),
                        Arc::new(RequestMetadata::new_unauthenticated()),
                        request,
                    )
                    .await
                    .unwrap();
                    assert!(!allowed);
                } else {
                    check_internal(ctx.clone(), nobody_metadata.clone(), request.clone())
                        .await
                        .unwrap_err();
                }

                // User 1 can check own access
                let request = CheckRequest {
                    identity: None,
                    operation: action.clone(),
                };
                check_internal(ctx.clone(), user_1_metadata.clone(), request.clone())
                    .await
                    .unwrap();
                // User 1 can check own access with principal
                let request = CheckRequest {
                    identity: Some(UserOrRole::User(user_1_id.clone())),
                    operation: action.clone(),
                };
                check_internal(ctx.clone(), user_1_metadata.clone(), request.clone())
                    .await
                    .unwrap();
                // User 1 cannot check operator access
                let request = CheckRequest {
                    identity: Some(UserOrRole::User(operator_id.clone())),
                    operation: action.clone(),
                };
                check_internal(ctx.clone(), user_1_metadata.clone(), request.clone())
                    .await
                    .unwrap_err();
                // Anonymous cannot check operator access
                let request = CheckRequest {
                    identity: Some(UserOrRole::User(operator_id.clone())),
                    operation: action.clone(),
                };
                check_internal(
                    ctx.clone(),
                    Arc::new(RequestMetadata::new_unauthenticated()),
                    request.clone(),
                )
                .await
                .unwrap_err();
                // Operator can check own access
                let request = CheckRequest {
                    identity: Some(UserOrRole::User(operator_id.clone())),
                    operation: action.clone(),
                };
                let allowed = check_internal(
                    ctx.clone(),
                    Arc::new(RequestMetadata::test_user(operator_id.clone())),
                    request,
                )
                .await
                .unwrap();
                assert!(allowed);
                // Operator can check access of other user
                let request = CheckRequest {
                    identity: Some(UserOrRole::User(nobody_id.clone())),
                    operation: action.clone(),
                };
                check_internal(
                    ctx.clone(),
                    Arc::new(RequestMetadata::test_user(operator_id.clone())),
                    request,
                )
                .await
                .unwrap();
            }
        }

        /// Assigning a role through the management endpoint with OpenFGA as the
        /// authorizer writes the assignment as an OpenFGA tuple and leaves the
        /// catalog `role_assignment` table empty. The assignee can read their
        /// own assignments back.
        #[sqlx::test]
        async fn test_role_assignment_endpoint_uses_openfga(pool: sqlx::PgPool) {
            use lakekeeper::{
                api::{
                    iceberg::v1::PaginationQuery,
                    management::v1::{
                        role_assignment::{
                            CreateRoleAssignmentRequest, ListRoleAssignmentsQuery,
                            Service as RoleAssignmentService,
                        },
                        user::{UserLastUpdatedWith, UserType},
                    },
                },
                service::{CatalogStore, RoleAssignmentFilter, Transaction, authz::Authorizer},
            };

            let operator_id = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());
            let (ctx, warehouse, _namespace) = setup(operator_id.clone(), pool.clone()).await;
            let operator_metadata = RequestMetadata::test_user(operator_id.clone());

            // The OpenFGA authorizer is the source of truth for assignments.
            assert!(ctx.v1_state.authz.manages_role_assignments());

            let role_id = ApiServer::create_role(
                CreateRoleRequest::builder()
                    .name("assignable_role".to_string())
                    .build(),
                ctx.clone(),
                operator_metadata.clone(),
            )
            .await
            .unwrap()
            .id;

            let member_id = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());

            // The handler requires the assignee to exist; provision it first.
            // OpenFGA still owns the assignment tuple (no catalog row written).
            let mut tx = <PostgresBackend as CatalogStore>::Transaction::begin_write(
                ctx.v1_state.catalog.clone(),
            )
            .await
            .unwrap();
            PostgresBackend::create_or_update_user(
                &member_id,
                &format!("User {member_id}"),
                None,
                UserLastUpdatedWith::CreateEndpoint,
                UserType::Human,
                tx.transaction(),
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();

            ApiServer::create_role_assignment(
                ctx.clone(),
                operator_metadata.clone(),
                CreateRoleAssignmentRequest {
                    user_id: member_id.clone(),
                    role_id,
                },
            )
            .await
            .unwrap();

            // The catalog table stays empty — OpenFGA owns the assignment.
            let table_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM role_assignment")
                .fetch_one(&pool)
                .await
                .unwrap();
            assert_eq!(table_count, 0);

            // The tuple is present: listing by-role (via the authorizer) returns
            // the member.
            let by_role = ctx
                .v1_state
                .authz
                .list_role_assignments(
                    &operator_metadata,
                    warehouse.project_id.clone(),
                    RoleAssignmentFilter::ByRole(role_id),
                    PaginationQuery::new_with_page_size(100),
                )
                .await
                .unwrap();
            assert_eq!(by_role.assignments.len(), 1);
            assert_eq!(by_role.assignments[0].user_id, member_id);
            assert_eq!(by_role.assignments[0].role_id, role_id);

            // The assignee can read their own assignments through the endpoint.
            let member_metadata = RequestMetadata::test_user(member_id.clone());
            let by_user = ApiServer::list_role_assignments(
                ctx.clone(),
                member_metadata,
                ListRoleAssignmentsQuery {
                    user_id: Some(member_id.clone()),
                    role_id: None,
                    page_token: None,
                    page_size: None,
                },
            )
            .await
            .unwrap();
            assert_eq!(by_user.assignments.len(), 1);
            assert_eq!(by_user.assignments[0].user_id, member_id);
            assert_eq!(by_user.assignments[0].role_id, role_id);

            // Revoke through the endpoint: OpenFGA removes the tuple and the
            // catalog table must *still* be empty (it was never the source of
            // truth, neither create nor delete touches it).
            ApiServer::delete_role_assignment(
                ctx.clone(),
                operator_metadata.clone(),
                member_id.clone(),
                role_id,
            )
            .await
            .unwrap();

            let table_count_after_delete: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM role_assignment")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(table_count_after_delete, 0);

            // The tuple is gone: by-role now returns nothing.
            let by_role_after = ctx
                .v1_state
                .authz
                .list_role_assignments(
                    &operator_metadata,
                    warehouse.project_id.clone(),
                    RoleAssignmentFilter::ByRole(role_id),
                    PaginationQuery::new_with_page_size(100),
                )
                .await
                .unwrap();
            assert_eq!(by_role_after.assignments.len(), 0);
        }

        /// `list_role_assignments` with a `ByUser` filter must be scoped to the
        /// requested project: a user assigned to roles in *multiple* projects
        /// only sees the assignments whose role belongs to the queried project.
        ///
        /// Regression test for the bug where the OpenFGA `ByUser` path returned
        /// the user's assignee tuples across ALL projects (the `role#project`
        /// membership was not checked). Project A and project B each hold one
        /// role; user `U` is assigned to both. Listing scoped to A must return
        /// only `roleA`, and scoped to B only `roleB`.
        #[sqlx::test]
        async fn test_list_role_assignments_by_user_is_project_scoped(pool: sqlx::PgPool) {
            use std::collections::HashSet;

            use lakekeeper::{
                api::{
                    iceberg::v1::PaginationQuery,
                    management::v1::{
                        project::{CreateProjectRequest, Service as ProjectService},
                        user::{UserLastUpdatedWith, UserType},
                    },
                },
                service::{CatalogStore, RoleAssignmentFilter, Transaction, authz::Authorizer},
            };

            let operator_id = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());
            // `setup` bootstraps `operator_id` as a global operator and creates a
            // warehouse in project A; reuse project A as the first project.
            let (ctx, warehouse, _namespace) = setup(operator_id.clone(), pool.clone()).await;
            let operator_metadata = RequestMetadata::test_user(operator_id.clone());

            assert!(ctx.v1_state.authz.manages_role_assignments());

            let project_a = warehouse.project_id.clone();

            // Create a second, independent project B.
            let project_b = ApiServer::create_project(
                CreateProjectRequest {
                    project_name: "project_b".to_string(),
                    project_id: None,
                },
                ctx.clone(),
                operator_metadata.clone(),
            )
            .await
            .unwrap()
            .project_id;
            assert_ne!(project_a, project_b);

            // One role per project. Pin the project explicitly on the request so
            // each role's `role#project@project:<id>` tuple points where we want.
            let role_a = ApiServer::create_role(
                CreateRoleRequest::builder()
                    .name("role_a".to_string())
                    .project_id(Some((*project_a).clone()))
                    .build(),
                ctx.clone(),
                operator_metadata.clone(),
            )
            .await
            .unwrap()
            .id;
            let role_b = ApiServer::create_role(
                CreateRoleRequest::builder()
                    .name("role_b".to_string())
                    .project_id(Some((*project_b).clone()))
                    .build(),
                ctx.clone(),
                operator_metadata.clone(),
            )
            .await
            .unwrap()
            .id;

            // Provision the assignee `U` (the handler requires the user to exist).
            let user_u = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());
            let mut tx = <PostgresBackend as CatalogStore>::Transaction::begin_write(
                ctx.v1_state.catalog.clone(),
            )
            .await
            .unwrap();
            PostgresBackend::create_or_update_user(
                &user_u,
                &format!("User {user_u}"),
                None,
                UserLastUpdatedWith::CreateEndpoint,
                UserType::Human,
                tx.transaction(),
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();

            // Assign `U` to `role_a` (project A) and `role_b` (project B) via the
            // authorizer write the service uses. The assignee tuple is just
            // `user -> role`, so both project assignments now exist for `U`.
            ctx.v1_state
                .authz
                .add_role_assignments(
                    &operator_metadata,
                    project_a.clone(),
                    &[(user_u.clone(), role_a)],
                )
                .await
                .unwrap();
            ctx.v1_state
                .authz
                .add_role_assignments(
                    &operator_metadata,
                    project_b.clone(),
                    &[(user_u.clone(), role_b)],
                )
                .await
                .unwrap();

            // Scoped to project A: exactly `(U, role_a)`, never `role_b`.
            let by_user_in_a = ctx
                .v1_state
                .authz
                .list_role_assignments(
                    &operator_metadata,
                    project_a.clone(),
                    RoleAssignmentFilter::ByUser(user_u.clone()),
                    PaginationQuery::new_with_page_size(100),
                )
                .await
                .unwrap();
            assert!(by_user_in_a.next_page_token.is_none());
            assert_eq!(by_user_in_a.assignments.len(), 1);
            assert_eq!(by_user_in_a.assignments[0].user_id, user_u);
            assert_eq!(by_user_in_a.assignments[0].role_id, role_a);
            let roles_in_a: HashSet<_> =
                by_user_in_a.assignments.iter().map(|a| a.role_id).collect();
            assert_eq!(roles_in_a, HashSet::from([role_a]));
            assert!(!roles_in_a.contains(&role_b));

            // Symmetrically, scoped to project B: exactly `(U, role_b)`.
            let by_user_in_b = ctx
                .v1_state
                .authz
                .list_role_assignments(
                    &operator_metadata,
                    project_b.clone(),
                    RoleAssignmentFilter::ByUser(user_u.clone()),
                    PaginationQuery::new_with_page_size(100),
                )
                .await
                .unwrap();
            assert!(by_user_in_b.next_page_token.is_none());
            assert_eq!(by_user_in_b.assignments.len(), 1);
            assert_eq!(by_user_in_b.assignments[0].user_id, user_u);
            assert_eq!(by_user_in_b.assignments[0].role_id, role_b);
            let roles_in_b: HashSet<_> =
                by_user_in_b.assignments.iter().map(|a| a.role_id).collect();
            assert_eq!(roles_in_b, HashSet::from([role_b]));
            assert!(!roles_in_b.contains(&role_a));
        }

        /// On the OpenFGA path, the require-user-exists guard still applies:
        /// assigning a role to a user with no `users` row → 404
        /// `RoleAssignmentUserNotFound`, and no OpenFGA tuple is written.
        #[sqlx::test]
        async fn test_role_assignment_openfga_unknown_user_404(pool: sqlx::PgPool) {
            use lakekeeper::{
                api::{
                    iceberg::v1::PaginationQuery,
                    management::v1::role_assignment::{
                        CreateRoleAssignmentRequest, Service as RoleAssignmentService,
                    },
                },
                service::{RoleAssignmentFilter, authz::Authorizer},
            };

            let operator_id = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());
            let (ctx, warehouse, _namespace) = setup(operator_id.clone(), pool.clone()).await;
            let operator_metadata = RequestMetadata::test_user(operator_id.clone());

            assert!(ctx.v1_state.authz.manages_role_assignments());

            let role_id = ApiServer::create_role(
                CreateRoleRequest::builder()
                    .name("unknown_user_role".to_string())
                    .build(),
                ctx.clone(),
                operator_metadata.clone(),
            )
            .await
            .unwrap()
            .id;

            // A user that was never provisioned (no `users` row).
            let ghost_id = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());

            let err = ApiServer::create_role_assignment(
                ctx.clone(),
                operator_metadata.clone(),
                CreateRoleAssignmentRequest {
                    user_id: ghost_id.clone(),
                    role_id,
                },
            )
            .await
            .unwrap_err();
            assert_eq!(err.error.r#type, "RoleAssignmentUserNotFound");
            assert_eq!(err.error.code, http::StatusCode::NOT_FOUND.as_u16());

            // No tuple was written: by-role listing is empty.
            let by_role = ctx
                .v1_state
                .authz
                .list_role_assignments(
                    &operator_metadata,
                    warehouse.project_id.clone(),
                    RoleAssignmentFilter::ByRole(role_id),
                    PaginationQuery::new_with_page_size(100),
                )
                .await
                .unwrap();
            assert_eq!(by_role.assignments.len(), 0);
        }
    }
}
