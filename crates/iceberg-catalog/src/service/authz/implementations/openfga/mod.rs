use std::{collections::HashSet, fmt::Debug, sync::Arc};

use axum::Router;
use openfga_client::{
    client::{
        CheckRequestTupleKey, ReadRequestTupleKey, ReadResponse, Tuple, TupleKey,
        TupleKeyWithoutCondition,
    },
    tonic,
};

use crate::{
    request_metadata::RequestMetadata,
    service::{
        authn::Actor,
        authz::{
            Authorizer, CatalogNamespaceAction, CatalogProjectAction, CatalogServerAction,
            CatalogTableAction, CatalogViewAction, CatalogWarehouseAction, ErrorModel,
            ListProjectsResponse, Result,
        },
        NamespaceIdentUuid, TableIdentUuid,
    },
    ProjectId, WarehouseIdent, CONFIG,
};

pub(super) mod api;
mod check;
mod client;
mod entities;
mod error;
mod health;
mod migration;
mod models;
mod relations;

pub(crate) use client::{new_authorizer_from_config, new_client_from_config};
pub use client::{
    BearerOpenFGAAuthorizer, ClientCredentialsOpenFGAAuthorizer, UnauthenticatedOpenFGAAuthorizer,
};
use entities::{OpenFgaEntity, ParseOpenFgaEntity as _};
pub(crate) use error::{OpenFGAError, OpenFGAResult};
use iceberg_ext::catalog::rest::IcebergErrorResponse;
pub(crate) use migration::migrate;
pub(crate) use models::{OpenFgaType, RoleAssignee};
use openfga_client::client::BasicOpenFgaClient;
use relations::{
    NamespaceRelation, ProjectRelation, RoleRelation, ServerRelation, TableRelation, ViewRelation,
    WarehouseRelation,
};
use tokio::sync::RwLock;
use utoipa::OpenApi;

use crate::{
    api::ApiContext,
    service::{
        authn::UserId,
        authz::{
            implementations::{openfga::relations::OpenFgaRelation, FgaType},
            CatalogRoleAction, CatalogUserAction, NamespaceParent,
        },
        health::Health,
        Catalog, RoleId, SecretStore, State, ViewIdentUuid,
    },
};

const MAX_TUPLES_PER_WRITE: i32 = 100;

lazy_static::lazy_static! {
    static ref AUTH_CONFIG: crate::config::OpenFGAConfig = {
        CONFIG.openfga.clone().expect("OpenFGAConfig not found")
    };
    pub(crate) static ref OPENFGA_SERVER: String = {
        format!("server:{}", CONFIG.server_id)
    };
}

#[derive(Clone, Debug)]
pub struct OpenFGAAuthorizer {
    client: BasicOpenFgaClient,
    health: Arc<RwLock<Vec<Health>>>,
}

#[async_trait::async_trait]
impl Authorizer for OpenFGAAuthorizer {
    fn api_doc() -> utoipa::openapi::OpenApi {
        api::ApiDoc::openapi()
    }

    fn new_router<C: Catalog, S: SecretStore>(&self) -> Router<ApiContext<State<Self, C, S>>> {
        api::new_v1_router()
    }

    /// Check if the requested actor combination is allowed - especially if the user
    /// is allowed to assume the specified role.
    async fn check_actor(&self, actor: &Actor) -> Result<()> {
        match actor {
            Actor::Principal(_user_id) => Ok(()),
            Actor::Anonymous => Ok(()),
            Actor::Role {
                principal,
                assumed_role,
            } => {
                let assume_role_allowed = self
                    .check(CheckRequestTupleKey {
                        user: Actor::Principal(principal.clone()).to_openfga(),
                        relation: relations::RoleRelation::CanAssume.to_string(),
                        object: assumed_role.to_openfga(),
                    })
                    .await?;

                if assume_role_allowed {
                    Ok(())
                } else {
                    Err(ErrorModel::forbidden(
                        format!(
                            "Principal is not allowed to assume the role with id {assumed_role}"
                        ),
                        "RoleAssumptionNotAllowed",
                        None,
                    )
                    .into())
                }
            }
        }
    }

    async fn can_bootstrap(&self, metadata: &RequestMetadata) -> Result<()> {
        let actor = metadata.actor();
        // We don't check the actor as assumed roles are irrelevant for bootstrapping.
        // The principal is the only relevant actor.
        if &Actor::Anonymous == actor {
            return Err(ErrorModel::unauthorized(
                "Anonymous users cannot bootstrap the catalog",
                "AnonymousBootstrap",
                None,
            )
            .into());
        }
        Ok(())
    }

    async fn bootstrap(&self, metadata: &RequestMetadata, is_operator: bool) -> Result<()> {
        let actor = metadata.actor();
        // We don't check the actor as assumed roles are irrelevant for bootstrapping.
        // The principal is the only relevant actor.
        let user = match actor {
            Actor::Principal(principal) | Actor::Role { principal, .. } => principal,
            Actor::Anonymous => {
                return Err(ErrorModel::internal(
                    "can_bootstrap should be called before bootstrap",
                    "AnonymousBootstrap",
                    None,
                )
                .into())
            }
        };

        let relation = if is_operator {
            ServerRelation::Operator
        } else {
            ServerRelation::Admin
        };

        self.write(
            Some(vec![TupleKey {
                user: user.to_openfga(),
                relation: relation.to_string(),
                object: OPENFGA_SERVER.clone(),
                condition: None,
            }]),
            None,
        )
        .await?;

        Ok(())
    }

    async fn list_projects(&self, metadata: &RequestMetadata) -> Result<ListProjectsResponse> {
        let actor = metadata.actor();
        self.list_projects_internal(actor).await
    }

    async fn can_search_users(&self, metadata: &RequestMetadata) -> Result<bool> {
        // Currently all authenticated principals can search users
        Ok(metadata.actor().is_authenticated())
    }

    async fn is_allowed_role_action(
        &self,
        metadata: &RequestMetadata,
        role_id: RoleId,
        action: CatalogRoleAction,
    ) -> Result<bool> {
        self.check(CheckRequestTupleKey {
            user: metadata.actor().to_openfga(),
            relation: action.to_string(),
            object: role_id.to_openfga(),
        })
        .await
        .map_err(Into::into)
    }

    async fn is_allowed_user_action(
        &self,
        metadata: &RequestMetadata,
        user_id: &UserId,
        action: CatalogUserAction,
    ) -> Result<bool> {
        let actor = metadata.actor();

        let is_same_user = match actor {
            Actor::Role {
                principal,
                assumed_role: _,
            }
            | Actor::Principal(principal) => principal == user_id,
            Actor::Anonymous => false,
        };

        if is_same_user {
            return match action {
                CatalogUserAction::CanRead
                | CatalogUserAction::CanUpdate
                | CatalogUserAction::CanDelete => Ok(true),
            };
        }

        let server_id = OPENFGA_SERVER.clone();
        match action {
            // Currently, given a user-id, all information about a user can be retrieved.
            // For multi-tenant setups, we need to restrict this to a tenant.
            CatalogUserAction::CanRead => Ok(true),
            CatalogUserAction::CanUpdate => {
                self.check(CheckRequestTupleKey {
                    user: actor.to_openfga(),
                    relation: CatalogServerAction::CanUpdateUsers.to_string(),
                    object: server_id,
                })
                .await
            }
            CatalogUserAction::CanDelete => {
                self.check(CheckRequestTupleKey {
                    user: actor.to_openfga(),
                    relation: CatalogServerAction::CanDeleteUsers.to_string(),
                    object: server_id,
                })
                .await
            }
        }
        .map_err(Into::into)
    }

    async fn is_allowed_server_action(
        &self,
        metadata: &RequestMetadata,
        action: CatalogServerAction,
    ) -> Result<bool> {
        self.check(CheckRequestTupleKey {
            user: metadata.actor().to_openfga(),
            relation: action.to_string(),
            object: OPENFGA_SERVER.clone(),
        })
        .await
        .map_err(Into::into)
    }

    async fn is_allowed_project_action(
        &self,
        metadata: &RequestMetadata,
        project_id: &ProjectId,
        action: CatalogProjectAction,
    ) -> Result<bool> {
        self.check(CheckRequestTupleKey {
            user: metadata.actor().to_openfga(),
            relation: action.to_string(),
            object: project_id.to_openfga(),
        })
        .await
        .map_err(Into::into)
    }

    async fn is_allowed_warehouse_action(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseIdent,
        action: CatalogWarehouseAction,
    ) -> Result<bool> {
        self.check(CheckRequestTupleKey {
            user: metadata.actor().to_openfga(),
            relation: action.to_string(),
            object: warehouse_id.to_openfga(),
        })
        .await
        .map_err(Into::into)
    }

    async fn is_allowed_namespace_action<A>(
        &self,
        metadata: &RequestMetadata,
        namespace_id: NamespaceIdentUuid,
        action: A,
    ) -> Result<bool>
    where
        A: From<CatalogNamespaceAction> + std::fmt::Display + Send,
    {
        self.check(CheckRequestTupleKey {
            user: metadata.actor().to_openfga(),
            relation: action.to_string(),
            object: namespace_id.to_openfga(),
        })
        .await
        .map_err(Into::into)
    }

    async fn is_allowed_table_action<A>(
        &self,
        metadata: &RequestMetadata,
        table_id: TableIdentUuid,
        action: A,
    ) -> Result<bool>
    where
        A: From<CatalogTableAction> + std::fmt::Display + Send,
    {
        self.check(CheckRequestTupleKey {
            user: metadata.actor().to_openfga(),
            relation: action.to_string(),
            object: table_id.to_openfga(),
        })
        .await
        .map_err(Into::into)
    }

    async fn is_allowed_view_action<A>(
        &self,
        metadata: &RequestMetadata,
        view_id: ViewIdentUuid,
        action: A,
    ) -> Result<bool>
    where
        A: From<CatalogViewAction> + std::fmt::Display + Send,
    {
        self.check(CheckRequestTupleKey {
            user: metadata.actor().to_openfga(),
            relation: action.to_string(),
            object: view_id.to_openfga(),
        })
        .await
        .map_err(Into::into)
    }

    async fn delete_user(&self, _metadata: &RequestMetadata, user_id: UserId) -> Result<()> {
        self.delete_all_relations(&user_id).await
    }

    async fn create_role(
        &self,
        metadata: &RequestMetadata,
        role_id: RoleId,
        parent_project_id: ProjectId,
    ) -> Result<()> {
        let actor = metadata.actor();

        self.require_no_relations(&role_id).await?;
        let parent_id = parent_project_id.to_openfga();
        let this_id = role_id.to_openfga();
        self.write(
            Some(vec![
                TupleKey {
                    user: actor.to_openfga(),
                    relation: RoleRelation::Ownership.to_string(),
                    object: this_id.clone(),
                    condition: None,
                },
                TupleKey {
                    user: parent_id.clone(),
                    relation: RoleRelation::Project.to_string(),
                    object: this_id.clone(),
                    condition: None,
                },
            ]),
            None,
        )
        .await
        .map_err(Into::into)
    }

    async fn delete_role(&self, _metadata: &RequestMetadata, role_id: RoleId) -> Result<()> {
        self.delete_all_relations(&role_id).await
    }

    async fn create_project(
        &self,
        metadata: &RequestMetadata,
        project_id: &ProjectId,
    ) -> Result<()> {
        let actor = metadata.actor();

        self.require_no_relations(&project_id).await?;
        let server = OPENFGA_SERVER.clone();
        let this_id = project_id.to_openfga();
        self.write(
            Some(vec![
                TupleKey {
                    user: actor.to_openfga(),
                    relation: ProjectRelation::ProjectAdmin.to_string(),
                    object: this_id.clone(),
                    condition: None,
                },
                TupleKey {
                    user: server.clone(),
                    relation: ProjectRelation::Server.to_string(),
                    object: this_id.clone(),
                    condition: None,
                },
                TupleKey {
                    user: this_id,
                    relation: ServerRelation::Project.to_string(),
                    object: server,
                    condition: None,
                },
            ]),
            None,
        )
        .await
        .map_err(Into::into)
    }

    async fn delete_project(
        &self,
        _metadata: &RequestMetadata,
        project_id: ProjectId,
    ) -> Result<()> {
        self.delete_all_relations(&project_id).await
    }

    async fn create_warehouse(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseIdent,
        parent_project_id: &ProjectId,
    ) -> Result<()> {
        let actor = metadata.actor();

        self.require_no_relations(&warehouse_id).await?;
        let project_id = parent_project_id.to_openfga();
        let this_id = warehouse_id.to_openfga();
        self.write(
            Some(vec![
                TupleKey {
                    user: actor.to_openfga(),
                    relation: WarehouseRelation::Ownership.to_string(),
                    object: this_id.clone(),
                    condition: None,
                },
                TupleKey {
                    user: project_id.clone(),
                    relation: WarehouseRelation::Project.to_string(),
                    object: this_id.clone(),
                    condition: None,
                },
                TupleKey {
                    user: this_id.clone(),
                    relation: ProjectRelation::Warehouse.to_string(),
                    object: project_id.clone(),
                    condition: None,
                },
            ]),
            None,
        )
        .await
        .map_err(Into::into)
    }

    async fn delete_warehouse(
        &self,
        _metadata: &RequestMetadata,
        warehouse_id: WarehouseIdent,
    ) -> Result<()> {
        self.delete_all_relations(&warehouse_id).await
    }

    async fn create_namespace(
        &self,
        metadata: &RequestMetadata,
        namespace_id: NamespaceIdentUuid,
        parent: NamespaceParent,
    ) -> Result<()> {
        let actor = metadata.actor();

        self.require_no_relations(&namespace_id).await?;

        let (parent_id, parent_child_relation) = match parent {
            NamespaceParent::Warehouse(warehouse_id) => (
                warehouse_id.to_openfga(),
                WarehouseRelation::Namespace.to_string(),
            ),
            NamespaceParent::Namespace(parent_namespace_id) => (
                parent_namespace_id.to_openfga(),
                NamespaceRelation::Child.to_string(),
            ),
        };
        let this_id = namespace_id.to_openfga();

        self.write(
            Some(vec![
                TupleKey {
                    user: actor.to_openfga(),
                    relation: NamespaceRelation::Ownership.to_string(),
                    object: this_id.clone(),
                    condition: None,
                },
                TupleKey {
                    user: parent_id.clone(),
                    relation: NamespaceRelation::Parent.to_string(),
                    object: this_id.clone(),
                    condition: None,
                },
                TupleKey {
                    user: this_id.clone(),
                    relation: parent_child_relation,
                    object: parent_id.clone(),
                    condition: None,
                },
            ]),
            None,
        )
        .await
        .map_err(Into::into)
    }

    async fn delete_namespace(
        &self,
        _metadata: &RequestMetadata,
        namespace_id: NamespaceIdentUuid,
    ) -> Result<()> {
        self.delete_all_relations(&namespace_id).await
    }

    async fn create_table(
        &self,
        metadata: &RequestMetadata,
        table_id: TableIdentUuid,
        parent: NamespaceIdentUuid,
    ) -> Result<()> {
        let actor = metadata.actor();
        let parent_id = parent.to_openfga();
        let this_id = table_id.to_openfga();

        // Higher consistency as for stage create overwrites old relations are deleted
        // immediately before
        self.require_no_relations(&table_id).await?;

        self.write(
            Some(vec![
                TupleKey {
                    user: actor.to_openfga(),
                    relation: TableRelation::Ownership.to_string(),
                    object: this_id.clone(),
                    condition: None,
                },
                TupleKey {
                    user: parent_id.clone(),
                    relation: TableRelation::Parent.to_string(),
                    object: this_id.clone(),
                    condition: None,
                },
                TupleKey {
                    user: this_id.clone(),
                    relation: NamespaceRelation::Child.to_string(),
                    object: parent_id.clone(),
                    condition: None,
                },
            ]),
            None,
        )
        .await
        .map_err(Into::into)
    }

    async fn delete_table(&self, table_id: TableIdentUuid) -> Result<()> {
        self.delete_all_relations(&table_id).await
    }

    async fn create_view(
        &self,
        metadata: &RequestMetadata,
        view_id: ViewIdentUuid,
        parent: NamespaceIdentUuid,
    ) -> Result<()> {
        let actor = metadata.actor();
        let parent_id = parent.to_openfga();
        let this_id = view_id.to_openfga();

        self.require_no_relations(&view_id).await?;

        self.write(
            Some(vec![
                TupleKey {
                    user: actor.to_openfga(),
                    relation: ViewRelation::Ownership.to_string(),
                    object: this_id.clone(),
                    condition: None,
                },
                TupleKey {
                    user: parent_id.clone(),
                    relation: ViewRelation::Parent.to_string(),
                    object: this_id.clone(),
                    condition: None,
                },
                TupleKey {
                    user: this_id.clone(),
                    relation: NamespaceRelation::Child.to_string(),
                    object: parent_id.clone(),
                    condition: None,
                },
            ]),
            None,
        )
        .await
        .map_err(Into::into)
    }

    async fn delete_view(&self, view_id: ViewIdentUuid) -> Result<()> {
        self.delete_all_relations(&view_id).await
    }
}

impl OpenFGAAuthorizer {
    async fn list_projects_internal(&self, actor: &Actor) -> Result<ListProjectsResponse> {
        let list_all = self
            .check(CheckRequestTupleKey {
                user: actor.to_openfga(),
                relation: ServerRelation::CanListAllProjects.to_string(),
                object: OPENFGA_SERVER.clone(),
            })
            .await?;

        if list_all {
            return Ok(ListProjectsResponse::All);
        }

        let projects = self
            .list_objects(
                FgaType::Project.to_string(),
                CatalogProjectAction::CanIncludeInList.to_string(),
                actor.to_openfga(),
            )
            .await?
            .iter()
            .map(|p| ProjectId::parse_from_openfga(p))
            .collect::<std::result::Result<HashSet<ProjectId>, _>>()?;

        Ok(ListProjectsResponse::Projects(projects))
    }

    /// A convenience wrapper around write.
    /// All writes happen in a single transaction.
    /// At most 100 writes can be performed in a single transaction.
    async fn write(
        &self,
        writes: impl Into<Option<Vec<TupleKey>>>,
        deletes: impl Into<Option<Vec<TupleKeyWithoutCondition>>>,
    ) -> OpenFGAResult<()> {
        self.client.write(writes, deletes).await.inspect_err(|e| {
            tracing::error!("Failed to write to OpenFGA: {e}");
        })?;
        Ok(())
    }

    /// A convenience wrapper around read that handles error conversion
    async fn read(
        &self,
        page_size: i32,
        tuple_key: impl Into<ReadRequestTupleKey>,
        continuation_token: impl Into<Option<String>>,
    ) -> OpenFGAResult<ReadResponse> {
        self.client
            .read(page_size, tuple_key, continuation_token)
            .await
            .inspect_err(|e| {
                tracing::error!("Failed to read from OpenFGA: {e}");
            })
            .map(tonic::Response::into_inner)
            .map_err(Into::into)
    }

    /// Read all tuples for a given request
    async fn read_all(
        &self,
        tuple_key: impl Into<ReadRequestTupleKey>,
    ) -> OpenFGAResult<Vec<Tuple>> {
        self.client
            .read_all_pages(tuple_key, 100, 500)
            .await
            .map_err(Into::into)
    }

    /// A convenience wrapper around check
    async fn check(&self, tuple_key: impl Into<CheckRequestTupleKey>) -> OpenFGAResult<bool> {
        self.client
            .check(tuple_key, None, None, false)
            .await
            .inspect_err(|e| {
                tracing::error!("Failed to check with OpenFGA: {e}");
            })
            .map_err(Into::into)
    }

    async fn require_action(
        &self,
        metadata: &RequestMetadata,
        action: impl OpenFgaRelation,
        object: &str,
    ) -> Result<()> {
        let allowed = self
            .check(CheckRequestTupleKey {
                user: metadata.actor().to_openfga(),
                relation: action.to_string(),
                object: object.to_string(),
            })
            .await?;

        if !allowed {
            return Err(ErrorModel::forbidden(
                format!("Action {action} not allowed for object {object}"),
                "ActionForbidden",
                None,
            )
            .into());
        }
        Ok(())
    }

    /// Returns Ok(()) only if not tuples are associated in any relation with the given object.
    async fn require_no_relations(&self, object: &impl OpenFgaEntity) -> Result<()> {
        let openfga_tpye = object.openfga_type().clone();
        let fga_object = object.to_openfga();
        let objects = openfga_tpye.user_of();
        let fga_object_str = fga_object.as_str();

        // --------------------- 1. Object as "object" for any user ---------------------
        let relations_exist = self
            .client
            .exists_relation_to(&fga_object)
            .await
            .map_err(|e| {
                tracing::error!("Failed to check if relations to {fga_object} exists: {e}");
                OpenFGAError::from(e)
            })?;

        if relations_exist {
            return Err(ErrorModel::conflict(
                format!("Object to create {fga_object} already has relations"),
                "ObjectHasRelations",
                None,
            )
            .into());
        }

        // --------------------- 2. Object as "user" for related objects ---------------------
        let suffixes = suffixes_for_user(&openfga_tpye);

        let futures = objects
            .iter()
            .map(|i| (i, &suffixes))
            .map(|(o, s)| async move {
                for suffix in s {
                    let user = format!("{fga_object_str}{suffix}");
                    let tuples = self
                        .read(
                            1,
                            ReadRequestTupleKey {
                                user,
                                relation: String::new(),
                                object: format!("{o}:"),
                            },
                            None,
                        )
                        .await?;

                    if !tuples.tuples.is_empty() {
                        return Err(IcebergErrorResponse::from(
                            ErrorModel::conflict(
                                format!(
                                    "Object to create {fga_object_str} is used as user for type {o}",
                                ),
                                "ObjectUsedInRelation",
                                None,
                            )
                                .append_detail(format!("Found: {tuples:?}")),
                        ));
                    }
                }

                Ok(())
            })
            .collect::<Vec<_>>();

        futures::future::try_join_all(futures).await?;

        Ok(())
    }

    async fn delete_all_relations(&self, object: &impl OpenFgaEntity) -> Result<()> {
        let object_openfga = object.to_openfga();
        let (own_relations, user_relations) = futures::join!(
            self.delete_own_relations(object),
            self.delete_user_relations(object)
        );
        own_relations?;
        user_relations.inspect_err(|e| {
            tracing::error!("Failed to delete user relations for {object_openfga}: {e:?}");
        })
    }

    async fn delete_user_relations(&self, user: &impl OpenFgaEntity) -> Result<()> {
        let user_type = user.openfga_type().clone();
        let fga_user = user.to_openfga();
        let objects = user_type.user_of();
        let fga_user_str = fga_user.as_str();

        let suffixes = suffixes_for_user(&user_type);

        let futures = objects
            .iter()
            .map(|o| (o, &suffixes))
            .map(|(o, s)| async move {
                let mut continuation_token = None;
                for suffix in s {
                    let user = format!("{fga_user_str}{suffix}");
                    while continuation_token != Some(String::new()) {
                        let response = self
                            .read(
                                MAX_TUPLES_PER_WRITE,
                                ReadRequestTupleKey {
                                    user: user.clone(),
                                    relation: String::new(),
                                    object: format!("{o}:"),
                                },
                                continuation_token.clone(),
                            )
                            .await?;
                        continuation_token = Some(response.continuation_token);
                        let keys = response
                            .tuples
                            .into_iter()
                            .filter_map(|t| t.key)
                            .collect::<Vec<_>>();
                        self.write(
                            None,
                            Some(
                                keys.into_iter()
                                    .map(|t| TupleKeyWithoutCondition {
                                        user: t.user,
                                        relation: t.relation,
                                        object: t.object,
                                    })
                                    .collect(),
                            ),
                        )
                        .await?;
                    }
                }

                Result::<_, IcebergErrorResponse>::Ok(())
            })
            .collect::<Vec<_>>();

        futures::future::try_join_all(futures).await?;

        Ok(())
    }

    async fn delete_own_relations(&self, object: &impl OpenFgaEntity) -> Result<()> {
        let object_openfga = object.to_openfga();
        self.client
            .delete_relations_to_object(&object_openfga)
            .await
            .inspect_err(|e| tracing::error!("Failed to delete relations to {object_openfga}: {e}"))
            .map_err(OpenFGAError::from)
            .map_err(Into::into)
    }

    /// A convenience wrapper around `client.list_objects`
    async fn list_objects(
        &self,
        r#type: impl Into<String>,
        relation: impl Into<String>,
        user: impl Into<String>,
    ) -> Result<Vec<String>> {
        let user = user.into();
        self.client
            .list_objects(r#type, relation, user, None, None)
            .await
            .map_err(|e| OpenFGAError::from(e).into())
            .map(|response| response.into_inner().objects)
    }
}

fn suffixes_for_user(user: &FgaType) -> Vec<String> {
    user.usersets()
        .iter()
        .map(|s| format!("#{s}"))
        .chain(vec![String::new()])
        .collect::<Vec<_>>()
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) mod tests {
    use needs_env_var::needs_env_var;

    #[needs_env_var(TEST_OPENFGA = 1)]
    mod openfga {
        use http::StatusCode;
        use openfga_client::client::ConsistencyPreference;

        use super::super::*;
        use crate::service::{authz::implementations::openfga::client::new_authorizer, RoleId};

        const TEST_CONSISTENCY: ConsistencyPreference = ConsistencyPreference::HigherConsistency;

        async fn new_authorizer_in_empty_store() -> OpenFGAAuthorizer {
            let client = new_client_from_config()
                .await
                .expect("Failed to create OpenFGA client");

            let store_name = format!("test_store_{}", uuid::Uuid::now_v7());
            migrate(&client, Some(store_name.clone())).await.unwrap();

            new_authorizer(client, Some(store_name), TEST_CONSISTENCY)
                .await
                .unwrap()
        }

        #[tokio::test]
        async fn test_list_projects() {
            let authorizer = new_authorizer_in_empty_store().await;
            let user_id = UserId::new_unchecked("oidc", "this_user");
            let actor = Actor::Principal(user_id.clone());
            let project = ProjectId::from(uuid::Uuid::now_v7());

            let projects = authorizer
                .list_projects_internal(&actor)
                .await
                .expect("Failed to list projects");
            assert_eq!(projects, ListProjectsResponse::Projects(HashSet::new()));

            authorizer
                .write(
                    Some(vec![TupleKey {
                        user: user_id.to_openfga(),
                        relation: ProjectRelation::ProjectAdmin.to_string(),
                        object: project.to_openfga(),
                        condition: None,
                    }]),
                    None,
                )
                .await
                .unwrap();

            let projects = authorizer
                .list_projects_internal(&actor)
                .await
                .expect("Failed to list projects");
            assert_eq!(
                projects,
                ListProjectsResponse::Projects(HashSet::from_iter(vec![project]))
            );
        }

        #[tokio::test]
        async fn test_require_no_relations_own_relations() {
            let authorizer = new_authorizer_in_empty_store().await;

            let project_id = ProjectId::from(uuid::Uuid::now_v7());
            authorizer.require_no_relations(&project_id).await.unwrap();

            authorizer
                .write(
                    Some(vec![TupleKey {
                        user: "user:this_user".to_string(),
                        relation: ProjectRelation::ProjectAdmin.to_string(),
                        object: project_id.to_openfga(),
                        condition: None,
                    }]),
                    None,
                )
                .await
                .unwrap();

            let err = authorizer
                .require_no_relations(&project_id)
                .await
                .unwrap_err();
            assert_eq!(err.error.code, StatusCode::CONFLICT.as_u16());
            assert_eq!(err.error.r#type, "ObjectHasRelations");
        }

        #[tokio::test]
        async fn test_require_no_relations_used_in_other_relations() {
            let authorizer = new_authorizer_in_empty_store().await;
            let project_id = ProjectId::from(uuid::Uuid::now_v7());
            authorizer.require_no_relations(&project_id).await.unwrap();

            authorizer
                .write(
                    Some(vec![TupleKey {
                        user: project_id.to_openfga(),
                        relation: ServerRelation::Project.to_string(),
                        object: "server:this_server".to_string(),
                        condition: None,
                    }]),
                    None,
                )
                .await
                .unwrap();

            let err = authorizer
                .require_no_relations(&project_id)
                .await
                .unwrap_err();
            assert_eq!(err.error.code, StatusCode::CONFLICT.as_u16());
            assert_eq!(err.error.r#type, "ObjectUsedInRelation");
        }

        #[tokio::test]
        async fn test_delete_own_relations_direct() {
            let authorizer = new_authorizer_in_empty_store().await;
            let project_id = ProjectId::from(uuid::Uuid::now_v7());
            authorizer.require_no_relations(&project_id).await.unwrap();

            authorizer
                .write(
                    Some(vec![TupleKey {
                        user: "user:my_user".to_string(),
                        relation: ProjectRelation::ProjectAdmin.to_string(),
                        object: project_id.to_openfga(),
                        condition: None,
                    }]),
                    None,
                )
                .await
                .unwrap();

            authorizer
                .require_no_relations(&project_id)
                .await
                .unwrap_err();
            authorizer.delete_own_relations(&project_id).await.unwrap();
            authorizer.require_no_relations(&project_id).await.unwrap();
        }

        #[tokio::test]
        async fn test_delete_own_relations_usersets() {
            let authorizer = new_authorizer_in_empty_store().await;
            let project_id = ProjectId::from(uuid::Uuid::now_v7());
            authorizer.require_no_relations(&project_id).await.unwrap();

            authorizer
                .write(
                    Some(vec![TupleKey {
                        user: "role:my_role#assignee".to_string(),
                        relation: ProjectRelation::ProjectAdmin.to_string(),
                        object: project_id.to_openfga(),
                        condition: None,
                    }]),
                    None,
                )
                .await
                .unwrap();

            authorizer
                .require_no_relations(&project_id)
                .await
                .unwrap_err();
            authorizer.delete_own_relations(&project_id).await.unwrap();
            authorizer.require_no_relations(&project_id).await.unwrap();
        }

        #[tokio::test]
        async fn test_delete_own_relations_many() {
            let authorizer = new_authorizer_in_empty_store().await;
            let project_id = ProjectId::from(uuid::Uuid::now_v7());
            authorizer.require_no_relations(&project_id).await.unwrap();

            for i in 0..502 {
                authorizer
                    .write(
                        Some(vec![
                            TupleKey {
                                user: format!("user:user{i}"),
                                relation: ProjectRelation::ProjectAdmin.to_string(),
                                object: project_id.to_openfga(),
                                condition: None,
                            },
                            TupleKey {
                                user: format!("warehouse:warehouse_{i}"),
                                relation: ProjectRelation::Warehouse.to_string(),
                                object: project_id.to_openfga(),
                                condition: None,
                            },
                        ]),
                        None,
                    )
                    .await
                    .unwrap();
            }

            authorizer
                .require_no_relations(&project_id)
                .await
                .unwrap_err();
            authorizer.delete_own_relations(&project_id).await.unwrap();
            // openfga is eventually consistent, this should make tests less flaky
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            authorizer.require_no_relations(&project_id).await.unwrap();
        }

        #[tokio::test]
        async fn test_delete_own_relations_empty() {
            let authorizer = new_authorizer_in_empty_store().await;
            let project_id = ProjectId::from(uuid::Uuid::now_v7());
            authorizer.require_no_relations(&project_id).await.unwrap();

            authorizer.delete_own_relations(&project_id).await.unwrap();
            authorizer.require_no_relations(&project_id).await.unwrap();
        }

        #[tokio::test]
        async fn test_delete_user_relations() {
            let authorizer = new_authorizer_in_empty_store().await;
            let project_id = ProjectId::from(uuid::Uuid::now_v7());
            authorizer.require_no_relations(&project_id).await.unwrap();

            let project_id = ProjectId::from(uuid::Uuid::now_v7());

            authorizer
                .write(
                    Some(vec![TupleKey {
                        user: project_id.to_openfga(),
                        relation: WarehouseRelation::Project.to_string(),
                        object: "warehouse:my_warehouse".to_string(),
                        condition: None,
                    }]),
                    None,
                )
                .await
                .unwrap();

            authorizer
                .require_no_relations(&project_id)
                .await
                .unwrap_err();
            authorizer.delete_user_relations(&project_id).await.unwrap();
            authorizer.require_no_relations(&project_id).await.unwrap();
        }

        #[tokio::test]
        async fn test_delete_non_existing_relation_gives_404() {
            let authorizer = new_authorizer_in_empty_store().await;
            let project_id = ProjectId::from(uuid::Uuid::now_v7());
            let result = authorizer
                .write(
                    None,
                    Some(vec![TupleKeyWithoutCondition {
                        user: project_id.to_openfga(),
                        relation: WarehouseRelation::Project.to_string(),
                        object: "warehouse:my_warehouse".to_string(),
                    }]),
                )
                .await
                .unwrap_err();

            assert_eq!(
                ErrorModel::from(result).code,
                StatusCode::NOT_FOUND.as_u16()
            );
        }

        #[tokio::test]
        async fn test_duplicate_writes_give_409() {
            let authorizer = new_authorizer_in_empty_store().await;
            let project_id = ProjectId::from(uuid::Uuid::now_v7());
            authorizer
                .write(
                    Some(vec![TupleKey {
                        user: project_id.to_openfga(),
                        relation: WarehouseRelation::Project.to_string(),
                        object: "warehouse:my_warehouse".to_string(),
                        condition: None,
                    }]),
                    None,
                )
                .await
                .unwrap();

            let result = authorizer
                .write(
                    Some(vec![TupleKey {
                        user: project_id.to_openfga(),
                        relation: WarehouseRelation::Project.to_string(),
                        object: "warehouse:my_warehouse".to_string(),
                        condition: None,
                    }]),
                    None,
                )
                .await
                .unwrap_err();
            assert_eq!(ErrorModel::from(result).code, StatusCode::CONFLICT.as_u16());
        }

        #[tokio::test]
        async fn test_delete_user_relations_empty() {
            let authorizer = new_authorizer_in_empty_store().await;
            let project_id = ProjectId::from(uuid::Uuid::now_v7());
            authorizer.require_no_relations(&project_id).await.unwrap();
            authorizer.delete_user_relations(&project_id).await.unwrap();
            authorizer.require_no_relations(&project_id).await.unwrap();
        }

        #[tokio::test]
        async fn test_delete_user_relations_many() {
            let authorizer = new_authorizer_in_empty_store().await;
            let project_id = ProjectId::from(uuid::Uuid::now_v7());
            authorizer.require_no_relations(&project_id).await.unwrap();

            for i in 0..502 {
                authorizer
                    .write(
                        Some(vec![
                            TupleKey {
                                user: project_id.to_openfga(),
                                relation: WarehouseRelation::Project.to_string(),
                                object: format!("warehouse:warehouse_{i}"),
                                condition: None,
                            },
                            TupleKey {
                                user: project_id.to_openfga(),
                                relation: ServerRelation::Project.to_string(),
                                object: format!("server:server_{i}"),
                                condition: None,
                            },
                        ]),
                        None,
                    )
                    .await
                    .unwrap();
            }

            authorizer
                .require_no_relations(&project_id)
                .await
                .unwrap_err();
            authorizer.delete_user_relations(&project_id).await.unwrap();
            authorizer.require_no_relations(&project_id).await.unwrap();
        }

        #[tokio::test]
        async fn test_delete_user_relations_userset() {
            let authorizer = new_authorizer_in_empty_store().await;
            let user = RoleId::new(uuid::Uuid::nil());
            authorizer.require_no_relations(&user).await.unwrap();

            authorizer
                .write(
                    Some(vec![TupleKey {
                        user: format!("{}#assignee", user.to_openfga()),
                        relation: ProjectRelation::ProjectAdmin.to_string(),
                        object: "project:my_project".to_string(),
                        condition: None,
                    }]),
                    None,
                )
                .await
                .unwrap();

            authorizer.require_no_relations(&user).await.unwrap_err();
            authorizer.delete_user_relations(&user).await.unwrap();
            authorizer.require_no_relations(&user).await.unwrap();
        }
    }
}
