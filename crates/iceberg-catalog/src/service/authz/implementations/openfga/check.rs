use super::relations::{
    APINamespaceAction as NamespaceAction, APIProjectAction as ProjectAction,
    APIServerAction as ServerAction, APITableAction as TableAction, APIViewAction as ViewAction,
    APIWarehouseAction as WarehouseAction, NamespaceRelation as AllNamespaceRelations,
    ProjectRelation as AllProjectRelations, ReducedRelation, ServerRelation as AllServerAction,
    TableRelation as AllTableRelations, UserOrRole, ViewRelation as AllViewRelations,
    WarehouseRelation as AllWarehouseRelation,
};
use super::{OpenFGAAuthorizer, OpenFGAError, OPENFGA_SERVER};
use crate::catalog::namespace::authorized_namespace_ident_to_id;
use crate::catalog::tables::authorized_table_ident_to_id;
use crate::catalog::views::authorized_view_ident_to_id;
use crate::request_metadata::RequestMetadata;
use crate::service::authz::implementations::openfga::entities::OpenFgaEntity;
use crate::service::authz::Authorizer;
use crate::service::{
    Catalog, NamespaceIdentUuid, Result, SecretStore, State, TableIdentUuid, ViewIdentUuid,
};
use crate::service::{ListFlags, Transaction};
use crate::{api::ApiContext, ProjectIdent};
use crate::{WarehouseIdent, DEFAULT_PROJECT_ID};
use axum::extract::State as AxumState;
use axum::{Extension, Json};
use http::StatusCode;
use iceberg::{NamespaceIdent, TableIdent};
use openfga_rs::CheckRequestTupleKey;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case", tag = "type")]
/// Represents an action on an object
pub(super) enum CheckAction {
    Server {
        action: ServerAction,
    },
    #[serde(rename_all = "kebab-case")]
    Project {
        action: ProjectAction,
        #[schema(value_type = Option<uuid::Uuid>)]
        id: Option<ProjectIdent>,
    },
    #[serde(rename_all = "kebab-case")]
    Warehouse {
        action: WarehouseAction,
        #[schema(value_type = uuid::Uuid)]
        id: WarehouseIdent,
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
#[serde(rename_all = "kebab-case", tag = "identifier-type")]
/// Identifier for a namespace, either a UUID or its name and warehouse ID
pub(super) enum NamespaceIdentOrUuid {
    Id {
        #[schema(value_type = uuid::Uuid)]
        id: NamespaceIdentUuid,
    },
    #[serde(rename_all = "kebab-case")]
    Name {
        #[schema(value_type = Vec<String>)]
        name: NamespaceIdent,
        #[schema(value_type = uuid::Uuid)]
        warehouse_id: WarehouseIdent,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case", tag = "identifier-type")]
/// Identifier for a table or view, either a UUID or its name and namespace
pub(super) enum TabularIdentOrUuid {
    Id {
        id: uuid::Uuid,
    },
    #[serde(rename_all = "kebab-case")]
    Name {
        #[schema(value_type = Vec<String>)]
        namespace: NamespaceIdent,
        /// Name of the table or view
        name: String,
        #[schema(value_type = uuid::Uuid)]
        warehouse_id: WarehouseIdent,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
/// Check if a specific action is allowed on the given object
pub(super) struct CheckRequest {
    /// The user or role to check access for.
    for_principal: Option<UserOrRole>,
    /// The action to check.
    action: CheckAction,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
pub(super) struct CheckResponse {
    /// Whether the action is allowed.
    allowed: bool,
}

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
        mut for_principal,
        action: action_request,
    } = request;
    // Set for_principal to None if the user is checking their own access
    let user_or_role = metadata.actor().to_user_or_role();
    if let Some(user_or_role) = &user_or_role {
        for_principal = for_principal.filter(|p| p != user_or_role);
    }

    let (action, object) = match &action_request {
        CheckAction::Server { action } => {
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
            (action.to_openfga().to_string(), OPENFGA_SERVER.to_string())
        }
        CheckAction::Project {
            action,
            id: project_id,
        } => {
            let project_id = project_id
                .or(metadata.auth_details.project_id())
                .or(*DEFAULT_PROJECT_ID)
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
            (action.to_openfga().to_string(), project_id)
        }
        CheckAction::Warehouse {
            action,
            id: warehouse_id,
        } => {
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
            (
                action.to_openfga().to_string(),
                warehouse_id.to_openfga().to_string(),
            )
        }
        CheckAction::Namespace { action, namespace } => (
            action.to_openfga().to_string(),
            check_namespace(
                api_context.clone(),
                metadata,
                namespace,
                for_principal.as_ref(),
            )
            .await?,
        ),
        CheckAction::Table { action, table } => (action.to_openfga().to_string(), {
            check_table(api_context.clone(), metadata, table, for_principal.as_ref()).await?
        }),
        CheckAction::View { action, view } => (action.to_openfga().to_string(), {
            check_view(api_context, metadata, view, for_principal.as_ref()).await?
        }),
    };

    let user = if let Some(for_principal) = &for_principal {
        for_principal.to_openfga()
    } else {
        metadata.auth_details.actor().to_openfga()
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
        NamespaceIdentOrUuid::Id { id: identifier } => {
            authorizer
                .require_namespace_action(metadata, Ok(Some(*identifier)), action)
                .await?;
            *identifier
        }
        NamespaceIdentOrUuid::Name { name, warehouse_id } => {
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
        TabularIdentOrUuid::Id { id: identifier } => {
            let table_id = TableIdentUuid::from(*identifier);
            authorizer
                .require_table_action(metadata, Ok(Some(table_id)), action)
                .await?;
            table_id
        }
        TabularIdentOrUuid::Name {
            namespace,
            name,
            warehouse_id,
        } => {
            let mut t = C::Transaction::begin_read(api_context.v1_state.catalog).await?;
            let table_id = authorized_table_ident_to_id::<C, _>(
                authorizer.clone(),
                metadata,
                *warehouse_id,
                &TableIdent {
                    namespace: namespace.clone(),
                    name: name.clone(),
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
        TabularIdentOrUuid::Id { id: identifier } => {
            let view_id = ViewIdentUuid::from(*identifier);
            authorizer
                .require_view_action(metadata, Ok(Some(view_id)), action)
                .await?;
            view_id
        }
        TabularIdentOrUuid::Name {
            namespace,
            name,
            warehouse_id,
        } => {
            let mut t = C::Transaction::begin_read(api_context.v1_state.catalog).await?;
            let view_id = authorized_view_ident_to_id::<C, _>(
                authorizer.clone(),
                metadata,
                *warehouse_id,
                &TableIdent {
                    namespace: namespace.clone(),
                    name: name.clone(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use needs_env_var::needs_env_var;
    use std::str::FromStr;

    #[test]
    fn test_serde_check_action() {
        let action = CheckAction::Namespace {
            action: NamespaceAction::CreateTable,
            namespace: NamespaceIdentOrUuid::Id {
                id: NamespaceIdentUuid::from_str("00000000-0000-0000-0000-000000000000").unwrap(),
            },
        };
        let json = serde_json::to_value(&action).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "type": "namespace",
                "action": "create_table",
                "identifier-type": "id",
                "id": "00000000-0000-0000-0000-000000000000"
            })
        );
    }

    #[needs_env_var(TEST_OPENFGA = 1)]
    mod openfga {
        use super::super::super::relations::*;
        use super::super::*;
        use crate::api::iceberg::v1::namespace::Service;
        use crate::api::iceberg::v1::Prefix;
        use crate::api::management::v1::role::{CreateRoleRequest, Service as RoleService};
        use crate::api::management::v1::warehouse::{
            CreateWarehouseResponse, TabularDeleteProfile,
        };
        use crate::api::management::v1::ApiServer;
        use crate::catalog::{CatalogServer, NAMESPACE_ID_PROPERTY};
        use crate::implementations::postgres::{PostgresCatalog, SecretsState};
        use crate::service::authn::UserId;
        use crate::service::authz::implementations::openfga::migration::tests::authorizer_for_empty_store;
        use crate::service::authz::implementations::openfga::RoleAssignee;
        use iceberg_ext::catalog::rest::CreateNamespaceRequest;
        use iceberg_ext::catalog::rest::CreateNamespaceResponse;
        use openfga_rs::TupleKey;
        use std::str::FromStr;
        use strum::IntoEnumIterator;

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
            let operator_id = UserId::oidc(&uuid::Uuid::now_v7().to_string()).unwrap();
            let (ctx, _warehouse, _namespace) = setup(operator_id.clone(), pool).await;
            let user_id = UserId::oidc(&uuid::Uuid::now_v7().to_string()).unwrap();
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
                for_principal: Some(role.clone()),
                action: CheckAction::Server {
                    action: ServerAction::ProvisionUsers,
                },
            };
            check_internal(ctx.clone(), &user_metadata, request.clone())
                .await
                .unwrap_err();
            // Admin can check access for role
            let request = CheckRequest {
                for_principal: Some(role.clone()),
                action: CheckAction::Server {
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
            let operator_id = UserId::oidc(&uuid::Uuid::now_v7().to_string()).unwrap();
            let (ctx, warehouse, namespace) = setup(operator_id.clone(), pool).await;
            let namespace_id = NamespaceIdentUuid::from_str(
                namespace
                    .properties
                    .unwrap()
                    .get(NAMESPACE_ID_PROPERTY)
                    .unwrap(),
            )
            .unwrap();

            let nobody_id = UserId::oidc(&uuid::Uuid::now_v7().to_string()).unwrap();
            let nobody_metadata = RequestMetadata::random_human(nobody_id.clone());
            let user_1_id = UserId::oidc(&uuid::Uuid::now_v7().to_string()).unwrap();
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

            let server_actions = ServerAction::iter().map(|a| CheckAction::Server { action: a });
            let project_actions = ProjectAction::iter().map(|a| CheckAction::Project {
                action: a,
                id: None,
            });
            let warehouse_actions = WarehouseAction::iter().map(|a| CheckAction::Warehouse {
                action: a,
                id: warehouse.warehouse_id,
            });
            let namespace_ids = &[
                NamespaceIdentOrUuid::Id { id: namespace_id },
                NamespaceIdentOrUuid::Name {
                    name: namespace.namespace,
                    warehouse_id: warehouse.warehouse_id,
                },
            ];
            let namespace_actions = NamespaceAction::iter().flat_map(|a| {
                namespace_ids.iter().map(move |n| CheckAction::Namespace {
                    action: a,
                    namespace: n.clone(),
                })
            });

            for action in itertools::chain!(
                server_actions,
                project_actions,
                warehouse_actions,
                namespace_actions
            ) {
                let request = CheckRequest {
                    for_principal: None,
                    action: action.clone(),
                };

                // Nobody & anonymous can check own access on server level
                if let CheckAction::Server { .. } = &action {
                    let allowed = check_internal(ctx.clone(), &nobody_metadata, request.clone())
                        .await
                        .unwrap();
                    assert!(!allowed);
                    // Anonymous can check his own access
                    let allowed =
                        check_internal(ctx.clone(), &RequestMetadata::new_random(), request)
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
                    for_principal: None,
                    action: action.clone(),
                };
                check_internal(ctx.clone(), &user_1_metadata, request.clone())
                    .await
                    .unwrap();
                // User 1 can check own access with principal
                let request = CheckRequest {
                    for_principal: Some(UserOrRole::User(user_1_id.clone())),
                    action: action.clone(),
                };
                check_internal(ctx.clone(), &user_1_metadata, request.clone())
                    .await
                    .unwrap();
                // User 1 cannot check operator access
                let request = CheckRequest {
                    for_principal: Some(UserOrRole::User(operator_id.clone())),
                    action: action.clone(),
                };
                check_internal(ctx.clone(), &user_1_metadata, request.clone())
                    .await
                    .unwrap_err();
                // Anonymous cannot check operator access
                let request = CheckRequest {
                    for_principal: Some(UserOrRole::User(operator_id.clone())),
                    action: action.clone(),
                };
                check_internal(ctx.clone(), &RequestMetadata::new_random(), request.clone())
                    .await
                    .unwrap_err();
                // Operator can check own access
                let request = CheckRequest {
                    for_principal: Some(UserOrRole::User(operator_id.clone())),
                    action: action.clone(),
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
                    for_principal: Some(UserOrRole::User(nobody_id.clone())),
                    action: action.clone(),
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
