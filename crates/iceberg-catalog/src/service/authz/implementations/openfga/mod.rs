use crate::{
    request_metadata::RequestMetadata,
    service::{
        authz::{
            Authorizer, CatalogNamespaceAction, CatalogProjectAction, CatalogServerAction,
            CatalogTableAction, CatalogViewAction, CatalogWarehouseAction, ErrorModel,
            ListProjectsResponse, Result,
        },
        token_verification::Actor,
        NamespaceIdentUuid, TableIdentUuid,
    },
    ProjectIdent, WarehouseIdent, CONFIG,
};
use async_stream::__private::AsyncStream;
use async_stream::stream;
use async_trait::async_trait;
use axum::Router;
use futures::{pin_mut, StreamExt};
use openfga_rs::open_fga_service_client::OpenFgaServiceClient;
use openfga_rs::tonic::{Response, Status};
use openfga_rs::{
    tonic::{self},
    CheckRequest, CheckRequestTupleKey, CheckResponse, ConsistencyPreference, ListObjectsRequest,
    ListObjectsResponse, ReadRequest, ReadRequestTupleKey, ReadResponse, Tuple, TupleKey,
    TupleKeyWithoutCondition, WriteRequest, WriteRequestDeletes, WriteRequestWrites, WriteResponse,
};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::{collections::HashSet, fmt, str::FromStr};

pub(super) mod api;
mod client;
mod entities;
mod error;
mod health;
mod migration;
mod models;
mod relations;

mod service_ext;

use crate::api::ApiContext;
use crate::service::authz::implementations::openfga::client::ClientConnection;
use crate::service::authz::implementations::openfga::relations::OpenFgaRelation;
use crate::service::authz::implementations::FgaType;
use crate::service::authz::{CatalogRoleAction, CatalogUserAction, NamespaceParent};
use crate::service::health::Health;
use crate::service::{AuthDetails, Catalog, RoleId, SecretStore, State, UserId, ViewIdentUuid};
pub(crate) use client::new_client_from_config;
pub use client::{
    new_authorizer_from_config, BearerOpenFGAAuthorizer, ClientCredentialsOpenFGAAuthorizer,
    UnauthenticatedOpenFGAAuthorizer,
};
use entities::OpenFgaEntity;
pub use error::{OpenFGAError, OpenFGAResult};
use iceberg_ext::catalog::rest::IcebergErrorResponse;
pub(crate) use migration::migrate;
pub(crate) use models::{ModelVersion, OpenFgaType, RoleAssignee};
use relations::{
    NamespaceRelation, ProjectRelation, RoleRelation, ServerRelation, TableRelation, ViewRelation,
    WarehouseRelation,
};
pub(crate) use service_ext::ClientHelper;
use service_ext::MAX_TUPLES_PER_WRITE;
use tokio::sync::RwLock;
use utoipa::OpenApi;

lazy_static::lazy_static! {
    static ref AUTH_CONFIG: crate::config::OpenFGAConfig = {
        CONFIG.openfga.clone().expect("OpenFGAConfig not found")
    };
    pub(crate) static ref OPENFGA_SERVER: String = {
        format!("server:{}", CONFIG.server_id)
    };
}

#[derive(Clone)]
pub struct OpenFGAAuthorizer {
    pub(crate) client: Arc<dyn Client + Send + Sync + 'static>,
    pub(crate) store_id: String,
    pub(crate) authorization_model_id: String,
    pub(crate) health: Arc<RwLock<Vec<Health>>>,
}

impl Debug for OpenFGAAuthorizer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenFGAAuthorizer")
            .field("store_id", &self.store_id)
            .field("authorization_model_id", &self.authorization_model_id)
            .field("health", &self.health)
            .field("client", &"...")
            .finish()
    }
}

#[async_trait::async_trait]
impl Authorizer for OpenFGAAuthorizer {
    fn api_doc() -> utoipa::openapi::OpenApi {
        api::ApiDoc::openapi()
    }

    fn new_router<C: Catalog, S: SecretStore>(&self) -> Router<ApiContext<State<Self, C, S>>> {
        api::new_v1_router()
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

    async fn bootstrap(&self, metadata: &RequestMetadata) -> Result<()> {
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

        self.write(
            Some(vec![TupleKey {
                user: user.to_openfga(),
                relation: ServerRelation::GlobalAdmin.to_string(),
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

        let check_actor_fut = self.check_actor(actor);
        let list_all_fut = self.check(CheckRequestTupleKey {
            user: metadata.actor().to_openfga(),
            relation: relations::ServerRelation::CanListAllProjects.to_string(),
            object: OPENFGA_SERVER.clone(),
        });

        let (check_actor, list_all) = futures::join!(check_actor_fut, list_all_fut);
        check_actor?;
        let list_all = list_all?;

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
            .map(|p| {
                ProjectIdent::from_str(p).map_err(|_e| {
                    ErrorModel::internal(
                        "Failed to parse project id",
                        "ListProjectsIdParseError",
                        None,
                    )
                    .append_detail(format!("Project id: {p}"))
                    .into()
                })
            })
            .collect::<Result<HashSet<ProjectIdent>>>()?;

        Ok(ListProjectsResponse::Projects(projects))
    }

    async fn can_search_users(&self, metadata: &RequestMetadata) -> Result<bool> {
        let actor = metadata.actor();
        // Currently all authenticated principals can search users
        self.check_actor(actor).await?;

        match metadata.auth_details {
            AuthDetails::Unauthenticated => Ok(false),
            AuthDetails::Principal(_) => Ok(true),
        }
    }

    async fn is_allowed_role_action(
        &self,
        metadata: &RequestMetadata,
        role_id: RoleId,
        action: &CatalogRoleAction,
    ) -> Result<bool> {
        let actor = metadata.actor();
        let check_actor_fut = self.check_actor(actor);
        let check_fut = self.check(CheckRequestTupleKey {
            user: actor.to_openfga(),
            relation: action.to_string(),
            object: role_id.to_openfga(),
        });

        let (check_actor, check) = futures::join!(check_actor_fut, check_fut);
        check_actor?;
        check.map_err(Into::into)
    }

    async fn is_allowed_user_action(
        &self,
        metadata: &RequestMetadata,
        user_id: &UserId,
        action: &CatalogUserAction,
    ) -> Result<bool> {
        let actor = metadata.actor();
        self.check_actor(actor).await?;

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
        action: &CatalogServerAction,
    ) -> Result<bool> {
        let actor = metadata.actor();
        let check_actor_fut = self.check_actor(actor);
        let check_fut = self.check(CheckRequestTupleKey {
            user: actor.to_openfga(),
            relation: action.to_string(),
            object: OPENFGA_SERVER.clone(),
        });

        let (check_actor, check) = futures::join!(check_actor_fut, check_fut);
        check_actor?;
        check.map_err(Into::into)
    }

    async fn is_allowed_project_action(
        &self,
        metadata: &RequestMetadata,
        project_id: ProjectIdent,
        action: &CatalogProjectAction,
    ) -> Result<bool> {
        let actor = metadata.actor();
        let check_actor_fut = self.check_actor(actor);
        let check_fut = self.check(CheckRequestTupleKey {
            user: actor.to_openfga(),
            relation: action.to_string(),
            object: format!("project:{project_id}"),
        });

        let (check_actor, check) = futures::join!(check_actor_fut, check_fut);
        check_actor?;
        check.map_err(Into::into)
    }

    async fn is_allowed_warehouse_action(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseIdent,
        action: &CatalogWarehouseAction,
    ) -> Result<bool> {
        let actor = metadata.actor();
        let check_actor_fut = self.check_actor(actor);
        let check_fut = self.check(CheckRequestTupleKey {
            user: actor.to_openfga(),
            relation: action.to_string(),
            object: format!("warehouse:{warehouse_id}"),
        });

        let (check_actor, check) = futures::join!(check_actor_fut, check_fut);
        check_actor?;
        check.map_err(Into::into)
    }

    /// Return the namespace_id if the action is allowed, otherwise return None.
    async fn is_allowed_namespace_action(
        &self,
        metadata: &RequestMetadata,
        _warehouse_id: WarehouseIdent,
        namespace_id: NamespaceIdentUuid,
        action: &CatalogNamespaceAction,
    ) -> Result<bool> {
        let actor = metadata.actor();
        let check_actor_fut = self.check_actor(actor);
        let check_fut = self.check(CheckRequestTupleKey {
            user: actor.to_openfga(),
            relation: action.to_string(),
            object: format!("namespace:{namespace_id}"),
        });

        let (check_actor, check) = futures::join!(check_actor_fut, check_fut);
        check_actor?;
        check.map_err(Into::into)
    }

    /// Return the table_id if the action is allowed, otherwise return None.
    async fn is_allowed_table_action(
        &self,
        metadata: &RequestMetadata,
        _warehouse_id: WarehouseIdent,
        table_id: TableIdentUuid,
        action: &CatalogTableAction,
    ) -> Result<bool> {
        let actor = metadata.actor();
        let check_actor_fut = self.check_actor(actor);
        let check_fut = self.check(CheckRequestTupleKey {
            user: actor.to_openfga(),
            relation: action.to_string(),
            object: format!("table:{table_id}"),
        });

        let (check_actor, check) = futures::join!(check_actor_fut, check_fut);
        check_actor?;
        check.map_err(Into::into)
    }

    /// Return the view_id if the action is allowed, otherwise return None.
    async fn is_allowed_view_action(
        &self,
        metadata: &RequestMetadata,
        _warehouse_id: WarehouseIdent,
        view_id: ViewIdentUuid,
        action: &CatalogViewAction,
    ) -> Result<bool> {
        let actor = metadata.actor();
        let check_actor_fut = self.check_actor(actor);
        let check_fut = self.check(CheckRequestTupleKey {
            user: actor.to_openfga(),
            relation: action.to_string(),
            object: format!("view:{view_id}"),
        });

        let (check_actor, check) = futures::join!(check_actor_fut, check_fut);
        check_actor?;
        check.map_err(Into::into)
    }

    async fn delete_user(&self, _metadata: &RequestMetadata, user_id: UserId) -> Result<()> {
        self.delete_all_relations(&user_id).await
    }

    async fn create_role(
        &self,
        metadata: &RequestMetadata,
        role_id: RoleId,
        parent_project_id: ProjectIdent,
    ) -> Result<()> {
        let actor = metadata.actor();

        self.require_no_relations(&role_id, ConsistencyPreference::MinimizeLatency)
            .await?;
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
        project_id: ProjectIdent,
    ) -> Result<()> {
        let actor = metadata.actor();

        self.require_no_relations(&project_id, ConsistencyPreference::MinimizeLatency)
            .await?;
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
        project_id: ProjectIdent,
    ) -> Result<()> {
        self.delete_all_relations(&project_id).await
    }

    async fn create_warehouse(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseIdent,
        parent_project_id: ProjectIdent,
    ) -> Result<()> {
        let actor = metadata.actor();

        self.require_no_relations(&warehouse_id, ConsistencyPreference::MinimizeLatency)
            .await?;
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

        self.require_no_relations(&namespace_id, ConsistencyPreference::MinimizeLatency)
            .await?;

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
        self.require_no_relations(&table_id, ConsistencyPreference::HigherConsistency)
            .await?;

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

        self.require_no_relations(&view_id, ConsistencyPreference::MinimizeLatency)
            .await?;

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
    /// A convenience wrapper around write.
    /// All writes happen in a single transaction.
    /// At most 100 writes can be performed in a single transaction.
    async fn write(
        &self,
        writes: Option<Vec<TupleKey>>,
        deletes: Option<Vec<TupleKeyWithoutCondition>>,
    ) -> OpenFGAResult<()> {
        let writes = writes.and_then(|w| (!w.is_empty()).then_some(w));
        let deletes = deletes.and_then(|d| (!d.is_empty()).then_some(d));

        if writes.is_none() && deletes.is_none() {
            return Ok(());
        }

        let num_writes_and_deletes = i32::try_from(
            writes.as_ref().map_or(0, Vec::len) + deletes.as_ref().map_or(0, Vec::len),
        )
        .unwrap_or(i32::MAX);
        if num_writes_and_deletes > MAX_TUPLES_PER_WRITE {
            return Err(OpenFGAError::TooManyWrites {
                actual: num_writes_and_deletes,
                max: MAX_TUPLES_PER_WRITE,
            });
        }

        let write_request = WriteRequest {
            store_id: self.store_id.clone(),
            writes: writes.map(|writes| WriteRequestWrites { tuple_keys: writes }),
            deletes: deletes.map(|deletes| WriteRequestDeletes {
                tuple_keys: deletes,
            }),
            authorization_model_id: self.authorization_model_id.clone(),
        };
        self.client
            .write(write_request.clone())
            .await
            .map_err(|e| OpenFGAError::WriteFailed {
                write_request,
                source: e,
            })
            .map(|_| ())
    }

    /// A convenience wrapper around read that handles error conversion
    async fn read(
        &self,
        page_size: i32,
        tuple_key: ReadRequestTupleKey,
        continuation_token: Option<String>,
        consistency: ConsistencyPreference,
    ) -> OpenFGAResult<ReadResponse> {
        let read_request = ReadRequest {
            store_id: self.store_id.clone(),
            page_size: Some(page_size),
            continuation_token: continuation_token.unwrap_or_default(),
            tuple_key: Some(tuple_key),
            consistency: consistency.into(),
        };
        self.client
            .read(read_request.clone())
            .await
            .map_err(|e| OpenFGAError::ReadFailed {
                read_request: Box::new(read_request),
                source: e,
            })
            .map(tonic::Response::into_inner)
    }

    /// Read all tuples for a given request
    async fn read_all(&self, tuple_key: ReadRequestTupleKey) -> OpenFGAResult<Vec<Tuple>> {
        self.client.read_all_pages(&self.store_id, tuple_key).await
    }

    /// A convenience wrapper around check
    async fn check(&self, tuple_key: CheckRequestTupleKey) -> OpenFGAResult<bool> {
        let check_request = CheckRequest {
            tuple_key: Some(tuple_key),
            store_id: self.store_id.clone(),
            authorization_model_id: self.authorization_model_id.clone(),
            contextual_tuples: None,
            trace: false,
            context: None,
            consistency: ConsistencyPreference::MinimizeLatency.into(),
        };

        self.client
            .check(check_request.clone())
            .await
            .map_err(|source| OpenFGAError::CheckFailed {
                check_request: Box::new(check_request),
                source,
            })
            .map(|response| response.get_ref().allowed)
    }

    async fn require_action(
        &self,
        metadata: &RequestMetadata,
        action: impl OpenFgaRelation,
        object: &str,
    ) -> Result<()> {
        let actor = metadata.actor();
        let check_actor_fut = self.check_actor(actor);
        let check_fut = self.check(CheckRequestTupleKey {
            user: actor.to_openfga(),
            relation: action.to_string(),
            object: object.to_string(),
        });

        let (check_actor, check) = futures::join!(check_actor_fut, check_fut);
        check_actor?;
        let allowed = check?;
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
    async fn require_no_relations(
        &self,
        object: &impl OpenFgaEntity,
        consistency: ConsistencyPreference,
    ) -> Result<()> {
        let openfga_tpye = object.openfga_type().clone();
        let fga_object = object.to_openfga();
        let objects = openfga_tpye.user_of();
        let fga_object_str = fga_object.as_str();

        // --------------------- 1. Object as "object" for any user ---------------------
        let tuples = self
            .read(
                1,
                ReadRequestTupleKey {
                    user: String::new(),
                    relation: String::new(),
                    object: fga_object.clone(),
                },
                None,
                consistency,
            )
            .await?
            .tuples;

        if !tuples.is_empty() {
            return Err(ErrorModel::conflict(
                format!("Object to create {fga_object} already has relations"),
                "ObjectHasRelations",
                None,
            )
            .append_detail(format!("Found: {tuples:?}"))
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
                            consistency,
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
        let (own_relations, user_relations) = futures::join!(
            self.delete_own_relations(object),
            self.delete_user_relations(object)
        );
        own_relations?;
        user_relations
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
                                ConsistencyPreference::HigherConsistency,
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
        self.delete_own_relations_inner(object).await?;
        // OpenFGA does not guarantee transactional consistency, by running a second delete, we have a higher chance of deleting all relations.
        self.delete_own_relations_inner(object).await
    }

    async fn delete_own_relations_inner(&self, object: &impl OpenFgaEntity) -> Result<()> {
        let fga_object = object.to_openfga();

        let read_stream: AsyncStream<_, _> = stream! {
            let mut continuation_token = None;
            let mut seen= HashSet::new();
            while continuation_token != Some(String::new()) {
                let response = self.read(
                    MAX_TUPLES_PER_WRITE,
                    ReadRequestTupleKey {
                        user: String::new(),
                        relation: String::new(),
                        object: fga_object.clone(),
                    },
                    continuation_token.clone(),
                    ConsistencyPreference::HigherConsistency,
                ).await?;
                continuation_token = Some(response.continuation_token);
                let keys = response.tuples.into_iter().filter_map(|t| t.key).filter(|k| !seen.contains(&(k.user.clone(), k.relation.clone()))).collect::<Vec<_>>();
                seen.extend(keys.iter().map(|k| (k.user.clone(), k.relation.clone())));
                yield Result::<_, IcebergErrorResponse>::Ok(keys);
            }
        };
        pin_mut!(read_stream);
        let mut read_tuples: Option<Vec<TupleKey>> = None;

        let delete_tuples = |t: Option<Vec<TupleKey>>| async {
            match t {
                Some(tuples) => {
                    self.write(
                        None,
                        Some(
                            tuples
                                .into_iter()
                                .map(|t| TupleKeyWithoutCondition {
                                    user: t.user,
                                    relation: t.relation,
                                    object: t.object,
                                })
                                .collect(),
                        ),
                    )
                    .await
                }
                None => Ok(()),
            }
        };

        loop {
            let next_future = read_stream.next();
            let deletion_future = delete_tuples(read_tuples.clone());

            let (tuples, delete) = futures::join!(next_future, deletion_future);
            delete?;

            if let Some(tuples) = tuples.transpose()? {
                read_tuples = (!tuples.is_empty()).then_some(tuples);
            } else {
                break Ok(());
            }
        }
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
            .list_objects(ListObjectsRequest {
                r#type: r#type.into(),
                relation: relation.into(),
                user: user.clone(),
                store_id: self.store_id.clone(),
                authorization_model_id: self.authorization_model_id.clone(),
                contextual_tuples: None,
                context: None,
                consistency: ConsistencyPreference::MinimizeLatency.into(),
            })
            .await
            .map_err(|e| {
                let msg = e.message().to_string();
                let code = e.code().to_string();
                ErrorModel::internal(
                    "Failed to list authorization objects",
                    "AuthorizationListObjectsFailed",
                    Some(Box::new(e)),
                )
                .append_detail(msg)
                .append_detail(format!("Tonic code: {code}"))
                .into()
            })
            .map(|response| {
                let s: Vec<String> = response.into_inner().objects;
                // cut off the user: prefix
                s.iter().map(|s| s[user.len()..].to_string()).collect()
            })
    }

    /// Check if the requested actor combination is allowed - especially if the user
    /// is allowed to assume the specified role.
    async fn check_actor(&self, actor: &Actor) -> Result<()> {
        match actor {
            Actor::Principal(_) | Actor::Anonymous => Ok(()),
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
                            "Principal is not allowed to assume the specified role with id {assumed_role}"
                        ),
                        "RoleAssumptionNotAllowed",
                        None,
                    )
                    .into())
                }
            }
        }
    }
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait Client {
    async fn write(&self, request: WriteRequest) -> Result<Response<WriteResponse>, tonic::Status>;
    async fn list_objects(
        &self,
        request: ListObjectsRequest,
    ) -> Result<Response<ListObjectsResponse>, tonic::Status>;
    async fn read(
        &self,
        request: ReadRequest,
    ) -> std::result::Result<Response<ReadResponse>, tonic::Status>;
    async fn read_all_pages(
        &self,
        store_id: &str,
        tuple: ReadRequestTupleKey,
    ) -> OpenFGAResult<Vec<Tuple>>;

    async fn check(
        &self,
        request: CheckRequest,
    ) -> std::result::Result<Response<CheckResponse>, tonic::Status>;
}

fn suffixes_for_user(user: &FgaType) -> Vec<String> {
    user.usersets()
        .iter()
        .map(|s| format!("#{s}"))
        .chain(vec![String::new()])
        .collect::<Vec<_>>()
}

#[async_trait]
impl Client for OpenFgaServiceClient<ClientConnection> {
    async fn write(&self, request: WriteRequest) -> Result<Response<WriteResponse>, Status> {
        Self::write(&mut self.clone(), request).await
    }

    async fn list_objects(
        &self,
        request: ListObjectsRequest,
    ) -> Result<Response<ListObjectsResponse>, Status> {
        Self::list_objects(&mut self.clone(), request).await
    }

    async fn read(
        &self,
        request: ReadRequest,
    ) -> std::result::Result<Response<ReadResponse>, Status> {
        Self::read(&mut self.clone(), request).await
    }

    async fn read_all_pages(
        &self,
        store_id: &str,
        tuple: ReadRequestTupleKey,
    ) -> OpenFGAResult<Vec<Tuple>> {
        ClientHelper::read_all_pages(&mut self.clone(), store_id, tuple).await
    }

    async fn check(
        &self,
        request: CheckRequest,
    ) -> std::result::Result<Response<CheckResponse>, Status> {
        Self::check(&mut self.clone(), request).await
    }
}
#[cfg(test)]
#[allow(dead_code)]
pub(crate) mod tests {
    use crate::service::authz::implementations::openfga::{MockClient, OpenFGAAuthorizer};
    use needs_env_var::needs_env_var;
    use openfga_rs::{CheckResponse, ReadResponse, WriteResponse};
    use std::collections::HashSet;
    use std::sync::{Arc, RwLock};

    /// A mock for the `OpenFGA` client that allows to hide objects.
    /// This is useful to test the behavior of the authorizer when objects are hidden.
    ///
    /// Create via `ObjectHidingMock::new()`, use `ObjectHidingMock::to_authorizer` to create an authorizer.
    /// Hide objects via `ObjectHidingMock::hide`. Objects that have been hidden will return `allowed: false`
    /// for any check request.
    pub(crate) struct ObjectHidingMock {
        pub hidden: Arc<RwLock<HashSet<String>>>,
        pub mock: Arc<MockClient>,
    }

    impl ObjectHidingMock {
        pub(crate) fn new() -> Self {
            let hidden: Arc<RwLock<HashSet<String>>> = Arc::default();
            let hidden_clone = hidden.clone();
            let mut mock = MockClient::default();
            mock.expect_check().returning(move |r| {
                let hidden = hidden_clone.clone();
                let hidden = hidden.read().unwrap();

                if hidden.contains(&r.tuple_key.unwrap().object) {
                    return Ok(openfga_rs::tonic::Response::new(CheckResponse {
                        allowed: false,
                        resolution: String::new(),
                    }));
                }

                Ok(openfga_rs::tonic::Response::new(CheckResponse {
                    allowed: true,
                    resolution: String::new(),
                }))
            });
            mock.expect_read().returning(|_| {
                Ok(openfga_rs::tonic::Response::new(ReadResponse {
                    tuples: vec![],
                    continuation_token: String::new(),
                }))
            });
            mock.expect_write()
                .returning(|_| Ok(openfga_rs::tonic::Response::new(WriteResponse {})));

            Self {
                hidden,
                mock: Arc::new(mock),
            }
        }

        #[cfg(test)]
        pub(crate) fn hide(&self, object: &str) {
            self.hidden.write().unwrap().insert(object.to_string());
        }

        #[cfg(test)]
        pub(crate) fn to_authorizer(&self) -> OpenFGAAuthorizer {
            OpenFGAAuthorizer {
                client: self.mock.clone(),
                store_id: "test_store".to_string(),
                authorization_model_id: "test_model".to_string(),
                health: Arc::default(),
            }
        }
    }

    #[needs_env_var(TEST_OPENFGA = 1)]
    mod openfga {
        use super::super::*;
        use crate::service::authz::implementations::openfga::client::new_authorizer;
        use crate::service::RoleId;
        use http::StatusCode;

        const TEST_CONSISTENCY: ConsistencyPreference = ConsistencyPreference::HigherConsistency;

        async fn new_authorizer_in_empty_store() -> OpenFGAAuthorizer {
            let mut client = new_client_from_config()
                .await
                .expect("Failed to create OpenFGA client");

            let store_name = format!("test_store_{}", uuid::Uuid::now_v7());
            migrate(&mut client, Some(store_name.clone()))
                .await
                .unwrap();

            new_authorizer(client, Some(store_name)).await.unwrap()
        }

        #[tokio::test]
        async fn test_require_no_relations_own_relations() {
            let authorizer = new_authorizer_in_empty_store().await;

            let project_id = ProjectIdent::from(uuid::Uuid::now_v7());
            authorizer
                .require_no_relations(&project_id, TEST_CONSISTENCY)
                .await
                .unwrap();

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
                .require_no_relations(&project_id, TEST_CONSISTENCY)
                .await
                .unwrap_err();
            assert_eq!(err.error.code, StatusCode::CONFLICT.as_u16());
            assert_eq!(err.error.r#type, "ObjectHasRelations");
        }

        #[tokio::test]
        async fn test_require_no_relations_used_in_other_relations() {
            let authorizer = new_authorizer_in_empty_store().await;
            let project_id = ProjectIdent::from(uuid::Uuid::now_v7());
            authorizer
                .require_no_relations(&project_id, TEST_CONSISTENCY)
                .await
                .unwrap();

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
                .require_no_relations(&project_id, TEST_CONSISTENCY)
                .await
                .unwrap_err();
            assert_eq!(err.error.code, StatusCode::CONFLICT.as_u16());
            assert_eq!(err.error.r#type, "ObjectUsedInRelation");
        }

        #[tokio::test]
        async fn test_delete_own_relations_direct() {
            let authorizer = new_authorizer_in_empty_store().await;
            let project_id = ProjectIdent::from(uuid::Uuid::now_v7());
            authorizer
                .require_no_relations(&project_id, TEST_CONSISTENCY)
                .await
                .unwrap();

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
                .require_no_relations(&project_id, TEST_CONSISTENCY)
                .await
                .unwrap_err();
            authorizer.delete_own_relations(&project_id).await.unwrap();
            authorizer
                .require_no_relations(&project_id, TEST_CONSISTENCY)
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn test_delete_own_relations_usersets() {
            let authorizer = new_authorizer_in_empty_store().await;
            let project_id = ProjectIdent::from(uuid::Uuid::now_v7());
            authorizer
                .require_no_relations(&project_id, TEST_CONSISTENCY)
                .await
                .unwrap();

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
                .require_no_relations(&project_id, TEST_CONSISTENCY)
                .await
                .unwrap_err();
            authorizer.delete_own_relations(&project_id).await.unwrap();
            authorizer
                .require_no_relations(&project_id, TEST_CONSISTENCY)
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn test_delete_own_relations_many() {
            let authorizer = new_authorizer_in_empty_store().await;
            let project_id = ProjectIdent::from(uuid::Uuid::now_v7());
            authorizer
                .require_no_relations(&project_id, TEST_CONSISTENCY)
                .await
                .unwrap();

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
                .require_no_relations(&project_id, TEST_CONSISTENCY)
                .await
                .unwrap_err();
            authorizer.delete_own_relations(&project_id).await.unwrap();
            authorizer
                .require_no_relations(&project_id, TEST_CONSISTENCY)
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn test_delete_own_relations_empty() {
            let authorizer = new_authorizer_in_empty_store().await;
            let project_id = ProjectIdent::from(uuid::Uuid::now_v7());
            authorizer
                .require_no_relations(&project_id, TEST_CONSISTENCY)
                .await
                .unwrap();

            authorizer.delete_own_relations(&project_id).await.unwrap();
            authorizer
                .require_no_relations(&project_id, TEST_CONSISTENCY)
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn test_delete_user_relations() {
            let authorizer = new_authorizer_in_empty_store().await;
            let project_id = ProjectIdent::from(uuid::Uuid::now_v7());
            authorizer
                .require_no_relations(&project_id, TEST_CONSISTENCY)
                .await
                .unwrap();

            let project_id = ProjectIdent::from(uuid::Uuid::now_v7());

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
                .require_no_relations(&project_id, TEST_CONSISTENCY)
                .await
                .unwrap_err();
            authorizer.delete_user_relations(&project_id).await.unwrap();
            authorizer
                .require_no_relations(&project_id, TEST_CONSISTENCY)
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn test_delete_user_relations_empty() {
            let authorizer = new_authorizer_in_empty_store().await;
            let project_id = ProjectIdent::from(uuid::Uuid::now_v7());
            authorizer
                .require_no_relations(&project_id, TEST_CONSISTENCY)
                .await
                .unwrap();
            authorizer.delete_user_relations(&project_id).await.unwrap();
            authorizer
                .require_no_relations(&project_id, TEST_CONSISTENCY)
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn test_delete_user_relations_many() {
            let authorizer = new_authorizer_in_empty_store().await;
            let project_id = ProjectIdent::from(uuid::Uuid::now_v7());
            authorizer
                .require_no_relations(&project_id, TEST_CONSISTENCY)
                .await
                .unwrap();

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
                .require_no_relations(&project_id, TEST_CONSISTENCY)
                .await
                .unwrap_err();
            authorizer.delete_user_relations(&project_id).await.unwrap();
            authorizer
                .require_no_relations(&project_id, TEST_CONSISTENCY)
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn test_delete_user_relations_userset() {
            let authorizer = new_authorizer_in_empty_store().await;
            let user = RoleId::new(uuid::Uuid::nil());
            authorizer
                .require_no_relations(&user, TEST_CONSISTENCY)
                .await
                .unwrap();

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

            authorizer
                .require_no_relations(&user, TEST_CONSISTENCY)
                .await
                .unwrap_err();
            authorizer.delete_user_relations(&user).await.unwrap();
            authorizer
                .require_no_relations(&user, TEST_CONSISTENCY)
                .await
                .unwrap();
        }
    }
}
