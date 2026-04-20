//! [`Authorizer`] implementation that asks an Open Policy Agent (OPA) sidecar.
//!
//! Each `(resource, action)` tuple becomes a `POST {endpoint}/v1/data/{policy}`
//! with `{"input": …}`. OPA's reply is expected to be `{"result": true|false}`
//! and the boolean flows back as one slot in the `Vec<bool>` returned from the
//! `are_allowed_*_actions_impl` methods.
//!
//! All the `create_*` / `delete_*` hooks are no-op stubs: OPA has no tuple
//! store to keep in sync, unlike OpenFGA. `list_projects_impl` returns
//! [`ListProjectsResponse::Unsupported`], which makes Lakekeeper fall back to
//! per-project point checks via [`are_allowed_project_actions_impl`].

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use futures::future::try_join_all;
use lakekeeper::{
    ProjectId, WarehouseId,
    api::{ApiContext, IcebergErrorResponse, RequestMetadata},
    async_trait,
    axum::Router,
    service::{
        Actor, ArcProjectId, AuthZNamespaceInfo, AuthZTableInfo, AuthZViewInfo, CatalogStore,
        ErrorModel, NamespaceId, NamespaceWithParent, ResolvedWarehouse, Role, RoleId, SecretStore,
        ServerId, State, TableId, UserId, ViewId,
        authz::{
            ActionOnTable, ActionOnView, Authorizer, AuthzBackendErrorOrBadRequest,
            CatalogNamespaceAction, CatalogProjectAction, CatalogRoleAction, CatalogServerAction,
            CatalogTableAction, CatalogUserAction, CatalogViewAction, CatalogWarehouseAction,
            IsAllowedActionError, ListProjectsResponse, NamespaceParent, UserOrRole,
        },
        health::Health,
    },
    tokio::sync::RwLock,
};
use reqwest::Client;
use url::Url;

use crate::{
    OpaError,
    config::OpaConfig,
    error::OpaResult,
    input::{
        ActorKind, OpaActor, OpaInput, OpaOperation, OpaRequest, OpaRequestContext, OpaResource,
        OpaResponse, OperationKind,
    },
};

type AuthorizerResult<T> = std::result::Result<T, IcebergErrorResponse>;

#[derive(Clone, Debug)]
pub struct OpaAuthorizer {
    pub(crate) http: Client,
    pub(crate) decision_url: Url,
    pub(crate) config: Arc<OpaConfig>,
    pub(crate) server_id: ServerId,
    pub(crate) health: Arc<RwLock<Vec<Health>>>,
}

impl OpaAuthorizer {
    pub(crate) fn new_with_client(
        http: Client,
        decision_url: Url,
        config: OpaConfig,
        server_id: ServerId,
    ) -> Self {
        Self {
            http,
            decision_url,
            config: Arc::new(config),
            server_id,
            health: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Exposed for `HealthExt::update_health`. Sends a minimal decision request
    /// with an empty input document; any HTTP success counts as healthy.
    pub(crate) async fn probe_health(&self) -> OpaResult<()> {
        let body = serde_json::json!({ "input": {} });
        let resp = self
            .http
            .post(self.decision_url.clone())
            .json(&body)
            .send()
            .await?;
        resp.error_for_status()?;
        Ok(())
    }

    async fn decide_one(&self, input: OpaInput) -> OpaResult<bool> {
        let req = OpaRequest { input };
        let resp = self
            .http
            .post(self.decision_url.clone())
            .json(&req)
            .send()
            .await?
            .error_for_status()?;
        let body: OpaResponse = resp.json().await?;
        let policy_path = self.config.policy_path.clone();
        match body.result {
            Some(serde_json::Value::Bool(b)) => Ok(b),
            Some(other) => Err(OpaError::UnexpectedResult {
                policy_path,
                value: other,
            }),
            None => Err(OpaError::MissingResult { policy_path }),
        }
    }

    /// Fan out `inputs` as concurrent POSTs, preserving input order in the
    /// returned `Vec<bool>`. Concurrency is bounded by
    /// [`OpaConfig::max_concurrency`].
    async fn decide_batch(&self, inputs: Vec<OpaInput>) -> Result<Vec<bool>, OpaError> {
        if inputs.is_empty() {
            return Ok(Vec::new());
        }
        let max = self.config.max_concurrency.max(1);
        let mut out = Vec::with_capacity(inputs.len());
        let mut iter = inputs.into_iter();
        loop {
            let chunk: Vec<OpaInput> = iter.by_ref().take(max).collect();
            if chunk.is_empty() {
                break;
            }
            let results =
                try_join_all(chunk.into_iter().map(|input| self.decide_one(input))).await?;
            out.extend(results);
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Helpers that build the `input` document from Lakekeeper types.
// ---------------------------------------------------------------------------

fn actor_to_opa(actor: &Actor, metadata: &RequestMetadata) -> OpaActor {
    let (kind, user_id, assumed_role) = match actor {
        Actor::Anonymous => (ActorKind::Anonymous, None, None),
        Actor::Principal(user) => (ActorKind::Principal, Some(user.to_string()), None),
        Actor::Role {
            principal,
            assumed_role,
        } => (
            ActorKind::Role,
            Some(principal.to_string()),
            Some(assumed_role.id().to_string()),
        ),
    };

    let token_roles: Vec<String> = metadata.token_roles().map_or_else(Vec::new, |tr| {
        tr.roles()
            .iter()
            .map(std::string::ToString::to_string)
            .collect()
    });

    let project_id = metadata.token_roles().map(|tr| tr.project_id().to_string());

    // Raw OIDC claims: we hand them to OPA unmodified so Rego can match on
    // custom attributes (groups, email, …) that are never flattened into
    // `TokenRoles` or `Actor`. See #266 and the input-schema discussion.
    // TODO: once limes exposes a stable `claims() -> serde_json::Value`, plumb
    // it in here. For now we leave `claims` null and document this in the PR.
    let claims = serde_json::Value::Null;

    OpaActor {
        kind,
        user_id,
        assumed_role,
        project_id,
        token_roles,
        claims,
    }
}

fn for_user_to_opa(for_user: &UserOrRole) -> OpaActor {
    match for_user {
        UserOrRole::User(u) => OpaActor {
            kind: ActorKind::Principal,
            user_id: Some(u.to_string()),
            assumed_role: None,
            project_id: None,
            token_roles: Vec::new(),
            claims: serde_json::Value::Null,
        },
        UserOrRole::Role(assignee) => OpaActor {
            kind: ActorKind::Role,
            user_id: None,
            assumed_role: Some(assignee.role().id().to_string()),
            project_id: None,
            token_roles: Vec::new(),
            claims: serde_json::Value::Null,
        },
    }
}

fn build_request_context(metadata: &RequestMetadata) -> OpaRequestContext {
    OpaRequestContext {
        request_id: metadata.request_id().to_string(),
        method: None,
        path: None,
        engines: Vec::new(),
    }
}

fn serialize_action<A: serde::Serialize>(action: &A) -> serde_json::Value {
    serde_json::to_value(action).unwrap_or(serde_json::Value::Null)
}

fn warehouse_resource_defaults(warehouse: &ResolvedWarehouse) -> OpaResource {
    OpaResource {
        warehouse_id: Some(warehouse.warehouse_id.to_string()),
        warehouse_name: Some(warehouse.name.clone()),
        project_id: Some(warehouse.project_id.to_string()),
        ..OpaResource::default()
    }
}

// ---------------------------------------------------------------------------
// Authorizer trait impl
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl Authorizer for OpaAuthorizer {
    type ServerAction = CatalogServerAction;
    type ProjectAction = CatalogProjectAction;
    type WarehouseAction = CatalogWarehouseAction;
    type NamespaceAction = CatalogNamespaceAction;
    type TableAction = CatalogTableAction;
    type ViewAction = CatalogViewAction;
    type UserAction = CatalogUserAction;
    type RoleAction = CatalogRoleAction;

    fn implementation_name() -> &'static str {
        "opa"
    }

    fn server_id(&self) -> ServerId {
        self.server_id
    }

    #[cfg(feature = "open-api")]
    #[allow(clippy::needless_for_each)]
    fn api_doc() -> utoipa::openapi::OpenApi {
        // OPA exposes no role/assignment management endpoints of its own; the
        // OpenAPI surface is therefore empty.
        use utoipa::OpenApi as _;

        #[derive(utoipa::OpenApi)]
        #[openapi()]
        struct ApiDoc;
        ApiDoc::openapi()
    }

    fn new_router<C: CatalogStore, S: SecretStore>(&self) -> Router<ApiContext<State<Self, C, S>>> {
        // No OPA-specific endpoints — all policy management lives in OPA itself
        // (or in a policy bundle pipeline). Return an empty router.
        Router::new()
    }

    async fn check_assume_role_impl(
        &self,
        principal: &UserId,
        assumed_role: &Role,
        metadata: &RequestMetadata,
    ) -> Result<bool, AuthzBackendErrorOrBadRequest> {
        let mut actor = actor_to_opa(metadata.actor(), metadata);
        // Override actor.user_id so the policy sees the principal that is
        // asking to assume the role, independent of any role already assumed.
        actor.user_id = Some(principal.to_string());

        let input = OpaInput {
            actor,
            operation: OpaOperation {
                kind: OperationKind::Role,
                action: serde_json::json!({ "action": "assume_role" }),
            },
            resource: OpaResource {
                role_id: Some(assumed_role.id().to_string()),
                ..OpaResource::default()
            },
            request: build_request_context(metadata),
        };
        self.decide_one(input).await.map_err(Into::into)
    }

    async fn can_bootstrap(&self, metadata: &RequestMetadata) -> AuthorizerResult<()> {
        // Mirrors the OpenFGA behavior: only authenticated principals may
        // bootstrap. Additional checks are a policy concern and belong in
        // `are_allowed_server_actions_impl` / the bootstrap action.
        match metadata.actor() {
            Actor::Principal(_) | Actor::Role { .. } => Ok(()),
            Actor::Anonymous => Err(ErrorModel::unauthorized(
                "Anonymous users cannot bootstrap the catalog",
                "AnonymousBootstrap",
                None,
            )
            .into()),
        }
    }

    async fn bootstrap(
        &self,
        _metadata: &RequestMetadata,
        _is_operator: bool,
    ) -> AuthorizerResult<()> {
        // No-op: OPA has no tuple store to seed. Initial grants are a
        // policy-bundle concern (e.g. "admin group present in JWT → allow").
        tracing::trace!("OPA authorizer: bootstrap — no-op");
        Ok(())
    }

    async fn list_projects_impl(
        &self,
        _metadata: &RequestMetadata,
    ) -> Result<ListProjectsResponse, AuthzBackendErrorOrBadRequest> {
        // Sanctioned fallback: Lakekeeper will iterate and call
        // are_allowed_project_actions_impl per project.
        Ok(ListProjectsResponse::Unsupported)
    }

    async fn can_search_users_impl(
        &self,
        metadata: &RequestMetadata,
    ) -> Result<bool, AuthzBackendErrorOrBadRequest> {
        // Same default as OpenFGA: any authenticated principal can search users.
        Ok(metadata.actor().is_authenticated())
    }

    async fn are_allowed_user_actions_impl(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        users_with_actions: &[(&UserId, Self::UserAction)],
    ) -> Result<Vec<bool>, IsAllowedActionError> {
        let base_actor = actor_to_opa(metadata.actor(), metadata);
        let inputs: Vec<OpaInput> = users_with_actions
            .iter()
            .map(|(user_id, action)| OpaInput {
                actor: for_user.map_or_else(|| base_actor.clone(), for_user_to_opa),
                operation: OpaOperation {
                    kind: OperationKind::User,
                    action: serialize_action(action),
                },
                resource: OpaResource {
                    user_id: Some(user_id.to_string()),
                    ..OpaResource::default()
                },
                request: build_request_context(metadata),
            })
            .collect();
        self.decide_batch(inputs).await.map_err(Into::into)
    }

    async fn are_allowed_role_actions_impl(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        roles_with_actions: &[(&Role, Self::RoleAction)],
    ) -> Result<Vec<bool>, IsAllowedActionError> {
        let base_actor = actor_to_opa(metadata.actor(), metadata);
        let inputs: Vec<OpaInput> = roles_with_actions
            .iter()
            .map(|(role, action)| OpaInput {
                actor: for_user.map_or_else(|| base_actor.clone(), for_user_to_opa),
                operation: OpaOperation {
                    kind: OperationKind::Role,
                    action: serialize_action(action),
                },
                resource: OpaResource {
                    role_id: Some(role.id().to_string()),
                    ..OpaResource::default()
                },
                request: build_request_context(metadata),
            })
            .collect();
        self.decide_batch(inputs).await.map_err(Into::into)
    }

    async fn are_allowed_server_actions_impl(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        actions: &[Self::ServerAction],
    ) -> Result<Vec<bool>, IsAllowedActionError> {
        let base_actor = actor_to_opa(metadata.actor(), metadata);
        let inputs: Vec<OpaInput> = actions
            .iter()
            .map(|action| OpaInput {
                actor: for_user.map_or_else(|| base_actor.clone(), for_user_to_opa),
                operation: OpaOperation {
                    kind: OperationKind::Server,
                    action: serialize_action(action),
                },
                resource: OpaResource::default(),
                request: build_request_context(metadata),
            })
            .collect();
        self.decide_batch(inputs).await.map_err(Into::into)
    }

    async fn are_allowed_project_actions_impl(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        projects_with_actions: &[(&ArcProjectId, Self::ProjectAction)],
    ) -> Result<Vec<bool>, IsAllowedActionError> {
        let base_actor = actor_to_opa(metadata.actor(), metadata);
        let inputs: Vec<OpaInput> = projects_with_actions
            .iter()
            .map(|(project_id, action)| OpaInput {
                actor: for_user.map_or_else(|| base_actor.clone(), for_user_to_opa),
                operation: OpaOperation {
                    kind: OperationKind::Project,
                    action: serialize_action(action),
                },
                resource: OpaResource {
                    project_id: Some(project_id.to_string()),
                    ..OpaResource::default()
                },
                request: build_request_context(metadata),
            })
            .collect();
        self.decide_batch(inputs).await.map_err(Into::into)
    }

    async fn are_allowed_warehouse_actions_impl(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        warehouses_with_actions: &[(&ResolvedWarehouse, Self::WarehouseAction)],
    ) -> Result<Vec<bool>, IsAllowedActionError> {
        let base_actor = actor_to_opa(metadata.actor(), metadata);
        let inputs: Vec<OpaInput> = warehouses_with_actions
            .iter()
            .map(|(wh, action)| {
                let mut resource = warehouse_resource_defaults(wh);
                resource.is_protected = Some(wh.protected);
                OpaInput {
                    actor: for_user.map_or_else(|| base_actor.clone(), for_user_to_opa),
                    operation: OpaOperation {
                        kind: OperationKind::Warehouse,
                        action: serialize_action(action),
                    },
                    resource,
                    request: build_request_context(metadata),
                }
            })
            .collect();
        self.decide_batch(inputs).await.map_err(Into::into)
    }

    async fn are_allowed_namespace_actions_impl(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        warehouse: &ResolvedWarehouse,
        _parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
        actions: &[(&impl AuthZNamespaceInfo, Self::NamespaceAction)],
    ) -> Result<Vec<bool>, IsAllowedActionError> {
        let base_actor = actor_to_opa(metadata.actor(), metadata);
        let inputs: Vec<OpaInput> = actions
            .iter()
            .map(|(info, action)| {
                let ns = info.namespace();
                let namespace_path = ns
                    .namespace_ident
                    .clone()
                    .inner()
                    .into_iter()
                    .collect::<Vec<_>>();
                let properties = ns.properties.as_ref().map_or_else(BTreeMap::new, |p| {
                    p.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
                });
                let mut resource = warehouse_resource_defaults(warehouse);
                resource.namespace_id = Some(info.namespace_id().to_string());
                resource.namespace_path = namespace_path;
                resource.is_protected = Some(ns.protected);
                resource.properties = properties;
                OpaInput {
                    actor: for_user.map_or_else(|| base_actor.clone(), for_user_to_opa),
                    operation: OpaOperation {
                        kind: OperationKind::Namespace,
                        action: serialize_action(action),
                    },
                    resource,
                    request: build_request_context(metadata),
                }
            })
            .collect();
        self.decide_batch(inputs).await.map_err(Into::into)
    }

    async fn are_allowed_table_actions_impl<A: Into<Self::TableAction> + Send + Clone + Sync>(
        &self,
        metadata: &RequestMetadata,
        warehouse: &ResolvedWarehouse,
        _parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
        actions: &[(
            &NamespaceWithParent,
            ActionOnTable<'_, '_, impl AuthZTableInfo, A>,
        )],
    ) -> Result<Vec<bool>, IsAllowedActionError> {
        let base_actor = actor_to_opa(metadata.actor(), metadata);
        let inputs: Vec<OpaInput> = actions
            .iter()
            .map(|(parent_ns, action)| {
                let table_action: Self::TableAction = action.action.clone().into();
                let namespace_path = parent_ns
                    .namespace_ident()
                    .clone()
                    .inner()
                    .into_iter()
                    .collect::<Vec<_>>();
                let properties = action
                    .info
                    .properties()
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                let mut resource = warehouse_resource_defaults(warehouse);
                resource.namespace_id = Some(action.info.namespace_id().to_string());
                resource.namespace_path = namespace_path;
                resource.table_id = Some(action.info.table_id().to_string());
                resource.properties = properties;
                OpaInput {
                    actor: action
                        .user
                        .map_or_else(|| base_actor.clone(), for_user_to_opa),
                    operation: OpaOperation {
                        kind: OperationKind::Table,
                        action: serialize_action(&table_action),
                    },
                    resource,
                    request: build_request_context(metadata),
                }
            })
            .collect();
        self.decide_batch(inputs).await.map_err(Into::into)
    }

    async fn are_allowed_view_actions_impl<A: Into<Self::ViewAction> + Send + Clone + Sync>(
        &self,
        metadata: &RequestMetadata,
        warehouse: &ResolvedWarehouse,
        _parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
        actions: &[(
            &NamespaceWithParent,
            ActionOnView<'_, '_, impl AuthZViewInfo, A>,
        )],
    ) -> Result<Vec<bool>, IsAllowedActionError> {
        let base_actor = actor_to_opa(metadata.actor(), metadata);
        let inputs: Vec<OpaInput> = actions
            .iter()
            .map(|(parent_ns, action)| {
                let view_action: Self::ViewAction = action.action.clone().into();
                let namespace_path = parent_ns
                    .namespace_ident()
                    .clone()
                    .inner()
                    .into_iter()
                    .collect::<Vec<_>>();
                let properties = action
                    .info
                    .properties()
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                let mut resource = warehouse_resource_defaults(warehouse);
                resource.namespace_id = Some(action.info.namespace_id().to_string());
                resource.namespace_path = namespace_path;
                resource.view_id = Some(action.info.view_id().to_string());
                resource.properties = properties;
                OpaInput {
                    actor: action
                        .user
                        .map_or_else(|| base_actor.clone(), for_user_to_opa),
                    operation: OpaOperation {
                        kind: OperationKind::View,
                        action: serialize_action(&view_action),
                    },
                    resource,
                    request: build_request_context(metadata),
                }
            })
            .collect();
        self.decide_batch(inputs).await.map_err(Into::into)
    }

    // --- CRUD hooks: no-ops. OPA has no tuple store to sync. ---

    async fn delete_user(
        &self,
        _metadata: &RequestMetadata,
        user_id: UserId,
    ) -> AuthorizerResult<()> {
        tracing::trace!(?user_id, "OPA authorizer: delete_user — no-op");
        Ok(())
    }

    async fn create_role(
        &self,
        _metadata: &RequestMetadata,
        role_id: RoleId,
        _parent_project_id: ArcProjectId,
    ) -> AuthorizerResult<()> {
        tracing::trace!(?role_id, "OPA authorizer: create_role — no-op");
        Ok(())
    }

    async fn delete_role(
        &self,
        _metadata: &RequestMetadata,
        role_id: RoleId,
    ) -> AuthorizerResult<()> {
        tracing::trace!(?role_id, "OPA authorizer: delete_role — no-op");
        Ok(())
    }

    async fn create_project(
        &self,
        _metadata: &RequestMetadata,
        project_id: &ProjectId,
    ) -> AuthorizerResult<()> {
        tracing::trace!(%project_id, "OPA authorizer: create_project — no-op");
        Ok(())
    }

    async fn delete_project(
        &self,
        _metadata: &RequestMetadata,
        project_id: &ProjectId,
    ) -> AuthorizerResult<()> {
        tracing::trace!(%project_id, "OPA authorizer: delete_project — no-op");
        Ok(())
    }

    async fn create_warehouse(
        &self,
        _metadata: &RequestMetadata,
        warehouse_id: WarehouseId,
        _parent_project_id: &ProjectId,
    ) -> AuthorizerResult<()> {
        tracing::trace!(%warehouse_id, "OPA authorizer: create_warehouse — no-op");
        Ok(())
    }

    async fn delete_warehouse(
        &self,
        _metadata: &RequestMetadata,
        warehouse_id: WarehouseId,
    ) -> AuthorizerResult<()> {
        tracing::trace!(%warehouse_id, "OPA authorizer: delete_warehouse — no-op");
        Ok(())
    }

    async fn create_namespace(
        &self,
        _metadata: &RequestMetadata,
        namespace_id: NamespaceId,
        _parent: NamespaceParent,
    ) -> AuthorizerResult<()> {
        tracing::trace!(%namespace_id, "OPA authorizer: create_namespace — no-op");
        Ok(())
    }

    async fn delete_namespace(
        &self,
        _metadata: &RequestMetadata,
        namespace_id: NamespaceId,
    ) -> AuthorizerResult<()> {
        tracing::trace!(%namespace_id, "OPA authorizer: delete_namespace — no-op");
        Ok(())
    }

    async fn create_table(
        &self,
        _metadata: &RequestMetadata,
        warehouse_id: WarehouseId,
        table_id: TableId,
        _parent: NamespaceId,
    ) -> AuthorizerResult<()> {
        tracing::trace!(%warehouse_id, %table_id, "OPA authorizer: create_table — no-op");
        Ok(())
    }

    async fn delete_table(
        &self,
        warehouse_id: WarehouseId,
        table_id: TableId,
    ) -> AuthorizerResult<()> {
        tracing::trace!(%warehouse_id, %table_id, "OPA authorizer: delete_table — no-op");
        Ok(())
    }

    async fn create_view(
        &self,
        _metadata: &RequestMetadata,
        warehouse_id: WarehouseId,
        view_id: ViewId,
        _parent: NamespaceId,
    ) -> AuthorizerResult<()> {
        tracing::trace!(%warehouse_id, %view_id, "OPA authorizer: create_view — no-op");
        Ok(())
    }

    async fn delete_view(
        &self,
        warehouse_id: WarehouseId,
        view_id: ViewId,
    ) -> AuthorizerResult<()> {
        tracing::trace!(%warehouse_id, %view_id, "OPA authorizer: delete_view — no-op");
        Ok(())
    }
}
