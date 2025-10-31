use http::StatusCode;
use lakekeeper::{
    api::{ApiContext, RequestMetadata},
    axum::{extract::State as AxumState, Extension, Json},
    iceberg::{NamespaceIdent, TableIdent},
    service::{
        authz::{AuthZTableOps, AuthZViewOps, Authorizer, AuthzNamespaceOps as _},
        AuthZTableInfo, AuthZViewInfo as _, CatalogNamespaceOps, CatalogStore, CatalogTabularOps,
        NamespaceId, NamespaceIdentOrId, Result, SecretStore, State, TableId, TableIdentOrId,
        TabularListFlags, ViewId, ViewIdentOrId,
    },
    ProjectId, WarehouseId,
};
use openfga_client::client::CheckRequestTupleKey;
use serde::{Deserialize, Serialize};

use super::{
    relations::{
        APINamespaceAction as NamespaceAction, APIProjectAction as ProjectAction, APIProjectAction,
        APIServerAction as ServerAction, APIServerAction, APITableAction as TableAction,
        APIViewAction as ViewAction, APIWarehouseAction as WarehouseAction, APIWarehouseAction,
        NamespaceRelation as AllNamespaceRelations, ProjectRelation as AllProjectRelations,
        ReducedRelation, ServerRelation as AllServerAction, TableRelation as AllTableRelations,
        UserOrRole, ViewRelation as AllViewRelations, WarehouseRelation as AllWarehouseRelation,
    },
    OpenFGAAuthorizer, OpenFGAError,
};
use crate::{entities::OpenFgaEntity, relations::ActorExt};

/// Check if a specific action is allowed on the given object
#[cfg_attr(feature = "open-api", utoipa::path(
    post,
    tag = "permissions",
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
    let allowed = check_internal(api_context, &metadata, request).await?;
    Ok((StatusCode::OK, Json(CheckResponse { allowed })))
}

async fn check_internal<C: CatalogStore, S: SecretStore>(
    api_context: ApiContext<State<OpenFGAAuthorizer, C, S>>,
    metadata: &RequestMetadata,
    request: CheckRequest,
) -> Result<bool> {
    let authorizer = api_context.v1_state.authz.clone();
    let CheckRequest {
        // If for_principal is specified, the user needs to have the
        // CanReadAssignments relation
        identity: mut for_principal,
        operation: action_request,
    } = request;
    // Set for_principal to None if the user is checking their own access
    let user_or_role = metadata.actor().to_user_or_role();
    if let Some(user_or_role) = &user_or_role {
        for_principal = for_principal.filter(|p| p != user_or_role);
    }

    let (action, object) = match &action_request {
        CheckOperation::Server { action } => {
            check_server(metadata, &authorizer, &mut for_principal, action).await?
        }
        CheckOperation::Project { action, project_id } => {
            check_project(
                metadata,
                &authorizer,
                for_principal.as_ref(),
                action,
                project_id.as_ref(),
            )
            .await?
        }
        CheckOperation::Warehouse {
            action,
            warehouse_id,
        } => {
            check_warehouse(
                metadata,
                &authorizer,
                for_principal.as_ref(),
                action,
                *warehouse_id,
            )
            .await?
        }
        CheckOperation::Namespace { action, namespace } => (
            action.to_openfga().to_string(),
            check_namespace(
                api_context.clone(),
                metadata,
                namespace,
                for_principal.as_ref(),
            )
            .await?,
        ),
        CheckOperation::Table { action, table } => (action.to_openfga().to_string(), {
            check_table(api_context.clone(), metadata, table, for_principal.as_ref()).await?
        }),
        CheckOperation::View { action, view } => (action.to_openfga().to_string(), {
            check_view(api_context, metadata, view, for_principal.as_ref()).await?
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
        .await?;

    Ok(allowed)
}

async fn check_warehouse(
    metadata: &RequestMetadata,
    authorizer: &OpenFGAAuthorizer,
    for_principal: Option<&UserOrRole>,
    action: &APIWarehouseAction,
    warehouse_id: WarehouseId,
) -> Result<(String, String)> {
    authorizer
        .require_action(
            metadata,
            for_principal
                .as_ref()
                .map_or(AllWarehouseRelation::CanGetMetadata, |_| {
                    AllWarehouseRelation::CanReadAssignments
                }),
            &warehouse_id.to_openfga(),
        )
        .await?;
    Ok((
        action.to_openfga().to_string(),
        warehouse_id.to_openfga().clone(),
    ))
}

async fn check_project(
    metadata: &RequestMetadata,
    authorizer: &OpenFGAAuthorizer,
    for_principal: Option<&UserOrRole>,
    action: &APIProjectAction,
    project_id: Option<&ProjectId>,
) -> Result<(String, String)> {
    let project_id = project_id
        .or(metadata.preferred_project_id().as_ref())
        .ok_or(OpenFGAError::NoProjectId)?
        .to_openfga();
    authorizer
        .require_action(
            metadata,
            for_principal
                .as_ref()
                .map_or(AllProjectRelations::CanGetMetadata, |_| {
                    AllProjectRelations::CanReadAssignments
                }),
            &project_id,
        )
        .await?;
    Ok((action.to_openfga().to_string(), project_id))
}

async fn check_server(
    metadata: &RequestMetadata,
    authorizer: &OpenFGAAuthorizer,
    for_principal: &mut Option<UserOrRole>,
    action: &APIServerAction,
) -> Result<(String, String)> {
    let openfga_server = authorizer.openfga_server().clone();
    if for_principal.is_some() {
        authorizer
            .require_action(
                metadata,
                AllServerAction::CanReadAssignments,
                &openfga_server,
            )
            .await?;
    } else {
        authorizer.check_actor(metadata.actor()).await?;
    }
    Ok((action.to_openfga().to_string(), openfga_server))
}

async fn check_namespace<C: CatalogStore, S: SecretStore>(
    api_context: ApiContext<State<OpenFGAAuthorizer, C, S>>,
    metadata: &RequestMetadata,
    namespace: &NamespaceIdentOrUuid,
    for_principal: Option<&UserOrRole>,
) -> Result<String> {
    let authorizer = api_context.v1_state.authz;
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
    let namespace = C::get_namespace(
        warehouse_id,
        user_provided_ns.clone(),
        api_context.v1_state.catalog,
    )
    .await;
    let namespace = authorizer
        .require_namespace_action(metadata, warehouse_id, user_provided_ns, namespace, action)
        .await?;

    Ok(namespace.namespace_id().to_openfga())
}

async fn check_table<C: CatalogStore, S: SecretStore>(
    api_context: ApiContext<State<OpenFGAAuthorizer, C, S>>,
    metadata: &RequestMetadata,
    table: &TabularIdentOrUuid,
    for_principal: Option<&UserOrRole>,
) -> Result<String> {
    let authorizer = api_context.v1_state.authz;
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

    let table_info = C::get_table_info(
        warehouse_id,
        table.clone(),
        TabularListFlags::active(),
        api_context.v1_state.catalog,
    )
    .await;
    let table_info = authorizer
        .require_table_action(metadata, warehouse_id, table, table_info, action)
        .await?;

    Ok((warehouse_id, table_info.table_id()).to_openfga())
}

async fn check_view<C: CatalogStore, S: SecretStore>(
    api_context: ApiContext<State<OpenFGAAuthorizer, C, S>>,
    metadata: &RequestMetadata,
    view: &TabularIdentOrUuid,
    for_principal: Option<&UserOrRole>,
) -> Result<String> {
    let authorizer = api_context.v1_state.authz;
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

    let view_info = C::get_view_info(
        warehouse_id,
        view.clone(),
        TabularListFlags::active(),
        api_context.v1_state.catalog,
    )
    .await;
    let view_info = authorizer
        .require_view_action(metadata, warehouse_id, view, view_info, action)
        .await?;

    Ok((warehouse_id, view_info.view_id()).to_openfga())
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case", untagged)]
/// Identifier for a namespace, either a UUID or its name and warehouse ID
pub(super) enum NamespaceIdentOrUuid {
    #[serde(rename_all = "kebab-case")]
    Id {
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        namespace_id: NamespaceId,
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        warehouse_id: WarehouseId,
    },
    #[serde(rename_all = "kebab-case")]
    Name {
        #[cfg_attr(feature = "open-api", schema(value_type = Vec<String>))]
        namespace: NamespaceIdent,
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        warehouse_id: WarehouseId,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case", untagged)]
/// Identifier for a table or view, either a UUID or its name and namespace
pub(super) enum TabularIdentOrUuid {
    #[serde(rename_all = "kebab-case")]
    IdInWarehouse {
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        warehouse_id: WarehouseId,
        #[serde(alias = "view_id")]
        table_id: uuid::Uuid,
    },
    #[serde(rename_all = "kebab-case")]
    Name {
        #[cfg_attr(feature = "open-api", schema(value_type = Vec<String>))]
        namespace: NamespaceIdent,
        /// Name of the table or view
        #[serde(alias = "view")]
        table: String,
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        warehouse_id: WarehouseId,
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
    use lakekeeper::service::UserId;

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
                iceberg::v1::{namespace::NamespaceService, Prefix},
                management::v1::{
                    role::{CreateRoleRequest, Service as RoleService},
                    ApiServer,
                },
                CreateNamespaceRequest,
            },
            implementations::postgres::{PostgresBackend, SecretsState},
            server::{CatalogServer, NAMESPACE_ID_PROPERTY},
            service::{authn::UserId, CreateNamespaceResponse},
            sqlx,
            tests::{SetupTestCatalog, TestWarehouseResponse},
        };
        use openfga_client::client::TupleKey;
        use strum::IntoEnumIterator;
        use uuid::Uuid;

        use super::super::{super::relations::*, *};
        use crate::{migration::tests::authorizer_for_empty_store, models::RoleAssignee};

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
                RequestMetadata::random_human(operator_id.clone()),
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
            let user_metadata = RequestMetadata::random_human(user_id.clone());
            let operator_metadata = RequestMetadata::random_human(operator_id.clone());

            let role_id = ApiServer::create_role(
                CreateRoleRequest {
                    name: "test_role".to_string(),
                    description: None,
                    project_id: None,
                },
                ctx.clone(),
                operator_metadata.clone(),
            )
            .await
            .unwrap()
            .id;
            let role = UserOrRole::Role(RoleAssignee::from_role(role_id));

            // User cannot check access for role without beeing a member
            let request = CheckRequest {
                identity: Some(role.clone()),
                operation: CheckOperation::Server {
                    action: ServerAction::ProvisionUsers,
                },
            };
            check_internal(ctx.clone(), &user_metadata, request.clone())
                .await
                .unwrap_err();
            // Admin can check access for role
            let request = CheckRequest {
                identity: Some(role.clone()),
                operation: CheckOperation::Server {
                    action: ServerAction::ProvisionUsers,
                },
            };
            let allowed = check_internal(ctx.clone(), &operator_metadata, request)
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
            let nobody_metadata = RequestMetadata::random_human(nobody_id.clone());
            let user_1_id = UserId::new_unchecked("oidc", &Uuid::now_v7().to_string());
            let user_1_metadata = RequestMetadata::random_human(user_1_id.clone());

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
                    let allowed = check_internal(ctx.clone(), &nobody_metadata, request.clone())
                        .await
                        .unwrap();
                    assert!(!allowed);
                    // Anonymous can check his own access
                    let allowed = check_internal(
                        ctx.clone(),
                        &RequestMetadata::new_unauthenticated(),
                        request,
                    )
                    .await
                    .unwrap();
                    assert!(!allowed);
                } else {
                    check_internal(ctx.clone(), &nobody_metadata, request.clone())
                        .await
                        .unwrap_err();
                }

                // User 1 can check own access
                let request = CheckRequest {
                    identity: None,
                    operation: action.clone(),
                };
                check_internal(ctx.clone(), &user_1_metadata, request.clone())
                    .await
                    .unwrap();
                // User 1 can check own access with principal
                let request = CheckRequest {
                    identity: Some(UserOrRole::User(user_1_id.clone())),
                    operation: action.clone(),
                };
                check_internal(ctx.clone(), &user_1_metadata, request.clone())
                    .await
                    .unwrap();
                // User 1 cannot check operator access
                let request = CheckRequest {
                    identity: Some(UserOrRole::User(operator_id.clone())),
                    operation: action.clone(),
                };
                check_internal(ctx.clone(), &user_1_metadata, request.clone())
                    .await
                    .unwrap_err();
                // Anonymous cannot check operator access
                let request = CheckRequest {
                    identity: Some(UserOrRole::User(operator_id.clone())),
                    operation: action.clone(),
                };
                check_internal(
                    ctx.clone(),
                    &RequestMetadata::new_unauthenticated(),
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
                    &RequestMetadata::random_human(operator_id.clone()),
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
                    &RequestMetadata::random_human(operator_id.clone()),
                    request,
                )
                .await
                .unwrap();
            }
        }
    }
}
