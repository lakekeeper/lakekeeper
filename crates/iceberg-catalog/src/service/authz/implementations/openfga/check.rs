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
        project_id: Option<ProjectIdent>,
    },
    #[serde(rename_all = "kebab-case")]
    Warehouse {
        action: WarehouseAction,
        #[schema(value_type = uuid::Uuid)]
        warehouse_id: WarehouseIdent,
    },
    Namespace {
        action: NamespaceAction,
        namespace: NamespaceIdentOrUuid,
    },
    Table {
        action: TableAction,
        table: TabularIdentOrUuid,
    },
    View {
        action: ViewAction,
        view: TabularIdentOrUuid,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case", tag = "identifier-type")]
/// Identifier for a namespace, either a UUID or its name and warehouse ID
pub(super) enum NamespaceIdentOrUuid {
    Uuid {
        #[schema(value_type = uuid::Uuid)]
        identifier: NamespaceIdentUuid,
    },
    Name {
        namespace: NamespaceIdent,
        #[schema(value_type = uuid::Uuid)]
        warehouse_id: WarehouseIdent,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case", tag = "identifier-type")]
/// Identifier for a table or view, either a UUID or its name and namespace
pub(super) enum TabularIdentOrUuid {
    Uuid {
        identifier: uuid::Uuid,
    },
    Name {
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
    get,
    tag = "permissions",
    path = "/management/v1/permissions/check",
    request_body = CheckRequest,
    responses(
            (status = 200, body = GetRoleAccessResponse),
    )
)]
pub(super) async fn check<C: Catalog, S: SecretStore>(
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Json(request): Json<CheckRequest>,
) -> Result<(StatusCode, Json<CheckResponse>)> {
    let allowed = _check(api_context, &metadata, request).await?;
    Ok((StatusCode::OK, Json(CheckResponse { allowed })))
}

async fn _check<C: Catalog, S: SecretStore>(
    api_context: ApiContext<State<OpenFGAAuthorizer, C, S>>,
    metadata: &RequestMetadata,
    request: CheckRequest,
) -> Result<bool> {
    let authorizer = api_context.v1_state.authz.clone();
    let CheckRequest {
        // If for_principal is specified, the user needs to have the
        // CanReadAssignments relation
        for_principal,
        action: action_request,
    } = request;

    let (action, object) = match &action_request {
        CheckAction::Server { action } => {
            if for_principal.is_some() {
                authorizer
                    .require_action(
                        &metadata,
                        AllServerAction::CanReadAssignments,
                        &OPENFGA_SERVER,
                    )
                    .await?;
            }
            (action.to_openfga().to_string(), OPENFGA_SERVER.to_string())
        }
        CheckAction::Project { action, project_id } => {
            let project_id = project_id
                .or(metadata.auth_details.project_id())
                .or(*DEFAULT_PROJECT_ID)
                .ok_or(OpenFGAError::NoProjectId)?
                .to_openfga();
            authorizer
                .require_action(
                    &metadata,
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
            warehouse_id,
        } => {
            authorizer
                .require_action(
                    &metadata,
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
                &metadata,
                namespace,
                for_principal.as_ref(),
            )
            .await?,
        ),
        CheckAction::Table { action, table } => (action.to_openfga().to_string(), {
            check_table(
                api_context.clone(),
                &metadata,
                table,
                for_principal.as_ref(),
            )
            .await?
        }),
        CheckAction::View { action, view } => (action.to_openfga().to_string(), {
            check_view(api_context, &metadata, view, for_principal.as_ref()).await?
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
        NamespaceIdentOrUuid::Uuid { identifier } => {
            authorizer
                .require_namespace_action(metadata, Ok(Some(*identifier)), action)
                .await?;
            *identifier
        }
        NamespaceIdentOrUuid::Name {
            namespace,
            warehouse_id,
        } => {
            let mut t = C::Transaction::begin_read(api_context.v1_state.catalog).await?;
            let namespace_id = authorized_namespace_ident_to_id::<C, _>(
                authorizer.clone(),
                metadata,
                warehouse_id,
                namespace,
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
        TabularIdentOrUuid::Uuid { identifier } => {
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
        TabularIdentOrUuid::Uuid { identifier } => {
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

    #[needs_env_var(TEST_OPENFGA = 1)]
    mod openfga {
        use super::super::*;
        use crate::service::authn::UserId;
        use crate::service::authz::implementations::openfga::migration::tests::authorizer_for_empty_store;
        use openfga_rs::TupleKey;

        #[tokio::test]
        async fn test_check_server() {
            // ToDo: COntinue
            todo!()
        }
    }
}
