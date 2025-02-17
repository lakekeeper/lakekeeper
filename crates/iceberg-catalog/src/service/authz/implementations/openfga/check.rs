use axum::{extract::State as AxumState, Extension, Json};
use http::StatusCode;
use iceberg::{NamespaceIdent, TableIdent};
use openfga_rs::CheckRequestTupleKey;
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
    OpenFGAAuthorizer, OpenFGAError, OPENFGA_SERVER,
};
use crate::{
    api::ApiContext,
    catalog::{
        namespace::authorized_namespace_ident_to_id, tables::authorized_table_ident_to_id,
        views::authorized_view_ident_to_id,
    },
    request_metadata::RequestMetadata,
    service::{
        authz::{implementations::openfga::entities::OpenFgaEntity, Authorizer},
        Catalog, ListFlags, NamespaceIdentUuid, Result, SecretStore, State, TableIdentUuid,
        Transaction, ViewIdentUuid,
    },
    ProjectId, WarehouseIdent,
};

/// Check if a specific action is allowed on the given object
#[utoipa::path(
    post,
    tag = "permissions",
    path = "/management/v1/permissions/check",
    request_body = CheckRequest,
    responses(
            (status = 200, body = CheckResponse),
    )
)]
pub(super) async fn check<C: Catalog, S: SecretStore>(
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Json(request): Json<CheckRequest>,
) -> Result<(StatusCode, Json<CheckResponse>)> {
    let allowed = check_internal(api_context, &metadata, request).await?;
    Ok((StatusCode::OK, Json(CheckResponse { allowed })))
}

async fn check_internal<C: Catalog, S: SecretStore>(
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
    warehouse_id: WarehouseIdent,
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
        warehouse_id.to_openfga().to_string(),
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
    if for_principal.is_some() {
        authorizer
            .require_action(
                metadata,
                AllServerAction::CanReadAssignments,
                &OPENFGA_SERVER,
            )
            .await?;
    } else {
        authorizer.check_actor(metadata.actor()).await?;
    }
    Ok((action.to_openfga().to_string(), OPENFGA_SERVER.to_string()))
}

async fn check_namespace<C: Catalog, S: SecretStore>(
    api_context: ApiContext<State<OpenFGAAuthorizer, C, S>>,
    metadata: &RequestMetadata,
    namespace: &NamespaceIdentOrUuid,
    for_principal: Option<&UserOrRole>,
) -> Result<String> {
    let authorizer = api_context.v1_state.authz;
    let action = for_principal.map_or(AllNamespaceRelations::CanGetMetadata, |_| {
        AllNamespaceRelations::CanReadAssignments
    });
    Ok(match namespace {
        NamespaceIdentOrUuid::Id {
            namespace_id: identifier,
        } => {
            authorizer
                .require_namespace_action(metadata, Ok(Some(*identifier)), action)
                .await?;
            *identifier
        }
        NamespaceIdentOrUuid::Name {
            namespace: name,
            warehouse_id,
        } => {
            let mut t = C::Transaction::begin_read(api_context.v1_state.catalog).await?;
            let namespace_id = authorized_namespace_ident_to_id::<C, _>(
                authorizer.clone(),
                metadata,
                warehouse_id,
                name,
                action,
                t.transaction(),
            )
            .await?;
            t.commit().await.ok();
            namespace_id
        }
    }
    .to_openfga())
}

async fn check_table<C: Catalog, S: SecretStore>(
    api_context: ApiContext<State<OpenFGAAuthorizer, C, S>>,
    metadata: &RequestMetadata,
    table: &TabularIdentOrUuid,
    for_principal: Option<&UserOrRole>,
) -> Result<String> {
    let authorizer = api_context.v1_state.authz;
    let action = for_principal.map_or(AllTableRelations::CanGetMetadata, |_| {
        AllTableRelations::CanReadAssignments
    });
    Ok(match table {
        TabularIdentOrUuid::Id { table_id } => {
            let table_id = TableIdentUuid::from(*table_id);
            authorizer
                .require_table_action(metadata, Ok(Some(table_id)), action)
                .await?;
            table_id
        }
        TabularIdentOrUuid::Name {
            namespace,
            table,
            warehouse_id,
        } => {
            let mut t = C::Transaction::begin_read(api_context.v1_state.catalog).await?;
            let table_id = authorized_table_ident_to_id::<C, _>(
                authorizer.clone(),
                metadata,
                *warehouse_id,
                &TableIdent {
                    namespace: namespace.clone(),
                    name: table.clone(),
                },
                ListFlags {
                    include_active: true,
                    include_staged: false,
                    include_deleted: false,
                },
                action,
                t.transaction(),
            )
            .await?;
            t.commit().await.ok();
            table_id
        }
    }
    .to_openfga())
}

async fn check_view<C: Catalog, S: SecretStore>(
    api_context: ApiContext<State<OpenFGAAuthorizer, C, S>>,
    metadata: &RequestMetadata,
    view: &TabularIdentOrUuid,
    for_principal: Option<&UserOrRole>,
) -> Result<String> {
    let authorizer = api_context.v1_state.authz;
    let action = for_principal.map_or(AllViewRelations::CanGetMetadata, |_| {
        AllViewRelations::CanReadAssignments
    });
    Ok(match view {
        TabularIdentOrUuid::Id { table_id } => {
            let view_id = ViewIdentUuid::from(*table_id);
            authorizer
                .require_view_action(metadata, Ok(Some(view_id)), action)
                .await?;
            view_id
        }
        TabularIdentOrUuid::Name {
            namespace,
            table,
            warehouse_id,
        } => {
            let mut t = C::Transaction::begin_read(api_context.v1_state.catalog).await?;
            let view_id = authorized_view_ident_to_id::<C, _>(
                authorizer.clone(),
                metadata,
                *warehouse_id,
                &TableIdent {
                    namespace: namespace.clone(),
                    name: table.clone(),
                },
                action,
                t.transaction(),
            )
            .await?;
            t.commit().await.ok();
            view_id
        }
    }
    .to_openfga())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
/// Represents an action on an object
pub(super) enum CheckOperation {
    Server {
        action: ServerAction,
    },
    #[serde(rename_all = "kebab-case")]
    Project {
        action: ProjectAction,
        #[schema(value_type = Option<uuid::Uuid>)]
        project_id: Option<ProjectId>,
    },
    #[serde(rename_all = "kebab-case")]
    Warehouse {
        action: WarehouseAction,
        #[schema(value_type = uuid::Uuid)]
        warehouse_id: WarehouseIdent,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case", untagged)]
/// Identifier for a namespace, either a UUID or its name and warehouse ID
pub(super) enum NamespaceIdentOrUuid {
    #[serde(rename_all = "kebab-case")]
    Id {
        #[schema(value_type = uuid::Uuid)]
        namespace_id: NamespaceIdentUuid,
    },
    #[serde(rename_all = "kebab-case")]
    Name {
        #[schema(value_type = Vec<String>)]
        namespace: NamespaceIdent,
        #[schema(value_type = uuid::Uuid)]
        warehouse_id: WarehouseIdent,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case", untagged)]
/// Identifier for a table or view, either a UUID or its name and namespace
pub(super) enum TabularIdentOrUuid {
    #[serde(rename_all = "kebab-case")]
    Id {
        #[serde(alias = "view_id")]
        table_id: uuid::Uuid,
    },
    #[serde(rename_all = "kebab-case")]
    Name {
        #[schema(value_type = Vec<String>)]
        namespace: NamespaceIdent,
        /// Name of the table or view
        #[serde(alias = "view")]
        table: String,
        #[schema(value_type = uuid::Uuid)]
        warehouse_id: WarehouseIdent,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
/// Check if a specific action is allowed on the given object
pub(super) struct CheckRequest {
    /// The user or role to check access for.
    identity: Option<UserOrRole>,
    /// The operation to check.
    operation: CheckOperation,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
pub(super) struct CheckResponse {
    /// Whether the action is allowed.
    allowed: bool,
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use needs_env_var::needs_env_var;

    use super::*;
    use crate::service::UserId;

    #[test]
    fn test_serde_check_action_table_id() {
        let action = CheckOperation::Namespace {
            action: NamespaceAction::CreateTable,
            namespace: NamespaceIdentOrUuid::Id {
                namespace_id: NamespaceIdentUuid::from_str("00000000-0000-0000-0000-000000000000")
                    .unwrap(),
            },
        };
        let json = serde_json::to_value(&action).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "namespace": {
                    "action": "create_table",
                    "namespace-id": "00000000-0000-0000-0000-000000000000"
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
                warehouse_id: WarehouseIdent::from_str("490cbf7a-cbfe-11ef-84c5-178606d4cab3")
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
                warehouse_id: WarehouseIdent::from_str("490cbf7a-cbfe-11ef-84c5-178606d4cab3")
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

    #[needs_env_var(TEST_OPENFGA = 1)]
    mod openfga {
        use std::str::FromStr;

        use iceberg_ext::catalog::rest::{CreateNamespaceRequest, CreateNamespaceResponse};
        use openfga_rs::TupleKey;
        use strum::IntoEnumIterator;
        use uuid::Uuid;

        use super::super::{super::relations::*, *};
        use crate::{
            api::{
                iceberg::v1::{namespace::Service, Prefix},
                management::v1::{
                    role::{CreateRoleRequest, Service as RoleService},
                    warehouse::{CreateWarehouseResponse, TabularDeleteProfile},
                    ApiServer,
                },
            },
            catalog::{CatalogServer, NAMESPACE_ID_PROPERTY},
            implementations::postgres::{PostgresCatalog, SecretsState},
            service::{
                authn::UserId,
                authz::implementations::openfga::{
                    migration::tests::authorizer_for_empty_store, RoleAssignee,
                },
            },
        };

        async fn setup(
            operator_id: UserId,
            pool: sqlx::PgPool,
        ) -> (
            ApiContext<State<OpenFGAAuthorizer, PostgresCatalog, SecretsState>>,
            CreateWarehouseResponse,
            CreateNamespaceResponse,
        ) {
            let prof = crate::catalog::test::test_io_profile();
            let authorizer = authorizer_for_empty_store().await.1;
            let (ctx, warehouse) = crate::catalog::test::setup(
                pool.clone(),
                prof,
                None,
                authorizer.clone(),
                TabularDeleteProfile::Hard {},
                Some(operator_id.clone()),
            )
            .await;

            let namespace = CatalogServer::create_namespace(
                Some(Prefix(warehouse.warehouse_id.to_string())),
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
            let namespace_id = NamespaceIdentUuid::from_str(
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
                NamespaceIdentOrUuid::Id { namespace_id },
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
