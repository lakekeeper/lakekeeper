use std::{collections::HashMap, sync::Arc};

use iceberg::TableIdent;
use iceberg_ext::catalog::rest::ErrorModel;
use lakekeeper_io::s3::S3Location;

use crate::{
    ProjectId, WarehouseId,
    api::{RequestMetadata, management::v1::check::CatalogActionCheckItem},
    service::{
        NamespaceIdentOrId, NamespaceWithParent, ResolvedWarehouse, RoleId, TableIdentOrId,
        TableInfo, TabularId, ViewIdentOrId, ViewInfo,
        authn::UserIdRef,
        authz::{CatalogAction, CatalogTableAction, CatalogViewAction},
        events::{
            AuthorizationError, AuthorizationFailedEvent, AuthorizationFailureSource,
            EventDispatcher,
        },
        storage::StoragePermissions,
        tasks::TaskId,
    },
};

// ── Traits ──────────────────────────────────────────────────────────────────

// Marker trait to indicate resolution state
pub trait ResolutionState: Clone + Send + Sync {}

pub trait UserProvidedEntity: Clone + std::fmt::Debug {}

pub trait APIEventAction {
    fn event_action_str(&self) -> String;
}

impl<T> APIEventAction for T
where
    T: CatalogAction,
{
    fn event_action_str(&self) -> String {
        CatalogAction::as_log_str(self)
    }
}

// Marker trait to indicate authorization state
pub trait AuthzState: Clone + Send + Sync {}

// ── Resolution type states ──────────────────────────────────────────────────

// Type state: Entity has not been resolved yet
#[derive(Clone, Debug)]
pub struct Unresolved;
impl ResolutionState for Unresolved {}

// Type state: Entity has been resolved with data
#[derive(Clone, Debug)]
pub struct Resolved<T: Clone + Send + Sync> {
    pub data: T,
}
impl<T: Clone + Send + Sync> ResolutionState for Resolved<T> {}

// ── Authorization type states ───────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct AuthzUnchecked;
impl AuthzState for AuthzUnchecked {}

#[derive(Clone, Debug)]
pub struct AuthzChecked;
impl AuthzState for AuthzChecked {}

// ── Resolved data types ─────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct ResolvedNamespace {
    pub warehouse: Arc<ResolvedWarehouse>,
    pub namespace: NamespaceWithParent,
}

#[derive(Clone, Debug)]
pub struct ResolvedTable {
    pub warehouse: Arc<ResolvedWarehouse>,
    pub table: Arc<TableInfo>,
    pub storage_permissions: Option<StoragePermissions>,
}

#[derive(Clone, Debug)]
pub struct ResolvedView {
    pub warehouse: Arc<ResolvedWarehouse>,
    pub view: Arc<ViewInfo>,
}

// ── User-provided entity types ──────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct UserProvidedEntityServer {}
impl UserProvidedEntity for UserProvidedEntityServer {}

#[derive(Clone, Debug)]
pub struct UserProvidedNamespace {
    pub warehouse_id: WarehouseId,
    pub namespace: NamespaceIdentOrId,
}

impl UserProvidedNamespace {
    pub fn new(warehouse_id: WarehouseId, namespace: impl Into<NamespaceIdentOrId>) -> Self {
        Self {
            warehouse_id,
            namespace: namespace.into(),
        }
    }
}

impl UserProvidedEntity for UserProvidedNamespace {}

#[derive(Clone, Debug)]
pub struct UserProvidedTable {
    pub warehouse_id: WarehouseId,
    pub table: TableIdentOrId,
}

impl UserProvidedEntity for UserProvidedTable {}

#[derive(Clone, Debug)]
pub struct UserProvidedTableLocation {
    pub warehouse_id: WarehouseId,
    pub table_location: Arc<S3Location>,
}

impl UserProvidedEntity for UserProvidedTableLocation {}

#[derive(Clone, Debug)]
pub struct UserProvidedTabularsById {
    pub warehouse_id: WarehouseId,
    pub tabulars: Vec<TabularId>,
}

impl UserProvidedEntity for UserProvidedTabularsById {}

#[derive(Clone, Debug)]
pub struct UserProvidedTablesByIdent {
    pub warehouse_id: WarehouseId,
    pub tables: Vec<TableIdent>,
}

impl UserProvidedEntity for UserProvidedTablesByIdent {}

#[derive(Clone, Debug)]
pub struct UserProvidedView {
    pub warehouse_id: WarehouseId,
    pub view: ViewIdentOrId,
}

impl UserProvidedEntity for UserProvidedView {}

#[derive(Clone, Debug, derive_more::From)]
pub enum UserProvidedTableOrView {
    Table(UserProvidedTable),
    View(UserProvidedView),
}

impl UserProvidedEntity for UserProvidedTableOrView {}

#[derive(Clone, Debug)]
pub struct UserProvidedTask {
    pub warehouse_id: WarehouseId,
    pub task_id: TaskId,
}
impl UserProvidedEntity for UserProvidedTask {}

// Primitive entity impls
impl UserProvidedEntity for WarehouseId {}
impl UserProvidedEntity for ProjectId {}
impl UserProvidedEntity for RoleId {}
impl UserProvidedEntity for UserIdRef {}
impl UserProvidedEntity for Arc<Vec<CatalogActionCheckItem>> {}

// ── Action types ────────────────────────────────────────────────────────────
#[derive(Clone, Debug)]
pub struct ServerActionSearchUsers {}
impl APIEventAction for ServerActionSearchUsers {
    fn event_action_str(&self) -> String {
        "search_users".to_string()
    }
}

#[derive(Clone, Debug)]
pub struct ServerActionListProjects {}
impl APIEventAction for ServerActionListProjects {
    fn event_action_str(&self) -> String {
        "list_projects".to_string()
    }
}

#[derive(Clone, Debug)]
pub struct WarehouseActionSearchTabulars {}
impl APIEventAction for WarehouseActionSearchTabulars {
    fn event_action_str(&self) -> String {
        "search_tabulars".to_string()
    }
}

#[derive(Clone, Debug)]
pub struct IntrospectPermissions {}
impl APIEventAction for IntrospectPermissions {
    fn event_action_str(&self) -> String {
        "introspect_permissions".to_string()
    }
}

#[derive(Clone, Debug)]
pub struct GetTaskDetailsAction {}
impl APIEventAction for GetTaskDetailsAction {
    fn event_action_str(&self) -> String {
        "get_task_details".to_string()
    }
}

#[derive(Clone, Debug)]
pub struct TabularAction {
    pub table_action: CatalogTableAction,
    pub view_action: CatalogViewAction,
}

impl APIEventAction for TabularAction {
    fn event_action_str(&self) -> String {
        let tbl_str = self.table_action.as_log_str();
        let view_str = self.view_action.as_log_str();
        if tbl_str == view_str {
            tbl_str
        } else {
            format!("{}, {}", tbl_str, view_str)
        }
    }
}

impl APIEventAction for Vec<CatalogTableAction> {
    fn event_action_str(&self) -> String {
        let actions_str = self
            .iter()
            .map(|a| a.as_log_str())
            .collect::<Vec<_>>()
            .join(", ");
        format!("Batch[{}]", actions_str)
    }
}

// ── APIEventContext ─────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct APIEventContext<P, R, A, Z = AuthzUnchecked>
where
    P: UserProvidedEntity,
    R: ResolutionState,
    A: APIEventAction,
    Z: AuthzState,
{
    pub(super) request_metadata: Arc<RequestMetadata>,
    pub(super) dispatcher: EventDispatcher,
    pub(super) user_provided_entity: P,
    pub(super) action: A,
    pub(super) resolved_entity: R,
    pub(super) _authz: std::marker::PhantomData<Z>,
    pub(super) extra_context: HashMap<String, String>,
}

// ── Core impl (Unresolved) ──────────────────────────────────────────────────

impl<P: UserProvidedEntity, A: APIEventAction> APIEventContext<P, Unresolved, A, AuthzUnchecked> {
    /// Create a new context with request metadata, dispatcher, and user-provided entity (unresolved)
    #[must_use]
    pub fn new(
        request_metadata: Arc<RequestMetadata>,
        dispatcher: EventDispatcher,
        entity: P,
        action: A,
    ) -> APIEventContext<P, Unresolved, A, AuthzUnchecked> {
        APIEventContext {
            request_metadata,
            dispatcher,
            user_provided_entity: entity,
            resolved_entity: Unresolved,
            action,
            _authz: std::marker::PhantomData,
            extra_context: HashMap::new(),
        }
    }
}

impl<P: UserProvidedEntity, A: APIEventAction, Z: AuthzState> APIEventContext<P, Unresolved, A, Z> {
    #[must_use]
    pub fn resolve<T>(self, resolved_data: T) -> APIEventContext<P, Resolved<T>, A, Z>
    where
        T: Clone + Send + Sync,
    {
        APIEventContext {
            request_metadata: self.request_metadata,
            dispatcher: self.dispatcher,
            user_provided_entity: self.user_provided_entity,
            resolved_entity: Resolved {
                data: resolved_data,
            },
            action: self.action,
            _authz: std::marker::PhantomData,
            extra_context: self.extra_context,
        }
    }
}

// ── Entity-specific constructors ────────────────────────────────────────────

impl<A: APIEventAction> APIEventContext<UserProvidedEntityServer, Unresolved, A> {
    #[must_use]
    pub fn for_server(
        request_metadata: Arc<RequestMetadata>,
        dispatcher: EventDispatcher,
        action: A,
    ) -> Self {
        Self::new(
            request_metadata,
            dispatcher,
            UserProvidedEntityServer {},
            action,
        )
    }
}

impl<A: APIEventAction> APIEventContext<ProjectId, Unresolved, A> {
    #[must_use]
    pub fn for_project(
        request_metadata: Arc<RequestMetadata>,
        dispatcher: EventDispatcher,
        project_id: ProjectId,
        action: A,
    ) -> Self {
        Self::new(request_metadata, dispatcher, project_id, action)
    }
}

impl<A: APIEventAction> APIEventContext<UserIdRef, Unresolved, A> {
    #[must_use]
    pub fn for_user(
        request_metadata: Arc<RequestMetadata>,
        dispatcher: EventDispatcher,
        user_id: UserIdRef,
        action: A,
    ) -> Self {
        Self::new(request_metadata, dispatcher, user_id, action)
    }
}

impl<A: APIEventAction> APIEventContext<RoleId, Unresolved, A> {
    #[must_use]
    pub fn for_role(
        request_metadata: Arc<RequestMetadata>,
        dispatcher: EventDispatcher,
        role_id: RoleId,
        action: A,
    ) -> Self {
        Self::new(request_metadata, dispatcher, role_id, action)
    }
}

impl<A: APIEventAction> APIEventContext<WarehouseId, Unresolved, A> {
    #[must_use]
    pub fn for_warehouse(
        request_metadata: Arc<RequestMetadata>,
        dispatcher: EventDispatcher,
        warehouse_id: WarehouseId,
        action: A,
    ) -> Self {
        Self::new(request_metadata, dispatcher, warehouse_id, action)
    }
}

impl<A: APIEventAction> APIEventContext<UserProvidedNamespace, Unresolved, A> {
    #[must_use]
    pub fn for_namespace(
        request_metadata: Arc<RequestMetadata>,
        dispatcher: EventDispatcher,
        warehouse_id: WarehouseId,
        namespace: impl Into<NamespaceIdentOrId>,
        action: A,
    ) -> Self {
        Self::new(
            request_metadata,
            dispatcher,
            UserProvidedNamespace {
                warehouse_id,
                namespace: namespace.into(),
            },
            action,
        )
    }
}

impl<A: APIEventAction> APIEventContext<UserProvidedTable, Unresolved, A> {
    #[must_use]
    pub fn for_table(
        request_metadata: Arc<RequestMetadata>,
        dispatcher: EventDispatcher,
        warehouse_id: WarehouseId,
        table: impl Into<TableIdentOrId>,
        action: A,
    ) -> Self {
        Self::new(
            request_metadata,
            dispatcher,
            UserProvidedTable {
                warehouse_id,
                table: table.into(),
            },
            action,
        )
    }
}

impl APIEventContext<UserProvidedTableLocation, Unresolved, CatalogTableAction> {
    #[must_use]
    pub fn for_table_location(
        request_metadata: Arc<RequestMetadata>,
        dispatcher: EventDispatcher,
        warehouse_id: WarehouseId,
        table_location: Arc<S3Location>,
        action: CatalogTableAction,
    ) -> Self {
        Self::new(
            request_metadata,
            dispatcher,
            UserProvidedTableLocation {
                warehouse_id,
                table_location,
            },
            action,
        )
    }
}

impl<A: APIEventAction> APIEventContext<UserProvidedTablesByIdent, Unresolved, A> {
    #[must_use]
    pub fn for_tables_by_ident(
        request_metadata: Arc<RequestMetadata>,
        dispatcher: EventDispatcher,
        warehouse_id: WarehouseId,
        tables: Vec<TableIdent>,
        action: A,
    ) -> Self {
        Self::new(
            request_metadata,
            dispatcher,
            UserProvidedTablesByIdent {
                warehouse_id,
                tables,
            },
            action,
        )
    }
}

impl<A: APIEventAction> APIEventContext<UserProvidedTabularsById, Unresolved, A> {
    #[must_use]
    pub fn for_tabulars(
        request_metadata: Arc<RequestMetadata>,
        dispatcher: EventDispatcher,
        warehouse_id: WarehouseId,
        tabulars: Vec<TabularId>,
        action: A,
    ) -> Self {
        Self::new(
            request_metadata,
            dispatcher,
            UserProvidedTabularsById {
                warehouse_id,
                tabulars,
            },
            action,
        )
    }
}

impl<A: APIEventAction> APIEventContext<UserProvidedView, Unresolved, A> {
    #[must_use]
    pub fn for_view(
        request_metadata: Arc<RequestMetadata>,
        dispatcher: EventDispatcher,
        warehouse_id: WarehouseId,
        view: impl Into<ViewIdentOrId>,
        action: A,
    ) -> Self {
        Self::new(
            request_metadata,
            dispatcher,
            UserProvidedView {
                warehouse_id,
                view: view.into(),
            },
            action,
        )
    }
}

impl<A: APIEventAction> APIEventContext<UserProvidedTask, Unresolved, A> {
    #[must_use]
    pub fn for_task(
        request_metadata: Arc<RequestMetadata>,
        dispatcher: EventDispatcher,
        warehouse_id: WarehouseId,
        task_id: TaskId,
        action: A,
    ) -> Self {
        Self::new(
            request_metadata,
            dispatcher,
            UserProvidedTask {
                warehouse_id,
                task_id,
            },
            action,
        )
    }
}

// ── Accessors (any resolution state, any authz state) ───────────────────────

impl<R, A, P, Z> APIEventContext<P, R, A, Z>
where
    R: ResolutionState,
    A: APIEventAction,
    P: UserProvidedEntity,
    Z: AuthzState,
{
    #[must_use]
    pub fn action(&self) -> &A {
        &self.action
    }

    #[must_use]
    pub fn action_mut(&mut self) -> &mut A {
        &mut self.action
    }

    #[must_use]
    pub fn user_provided_entity(&self) -> &P {
        &self.user_provided_entity
    }

    #[must_use]
    pub fn request_metadata(&self) -> &RequestMetadata {
        &self.request_metadata
    }

    #[must_use]
    pub fn request_metadata_arc(&self) -> Arc<RequestMetadata> {
        self.request_metadata.clone()
    }

    pub fn push_extra_context(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.extra_context.insert(key.into(), value.into());
    }

    #[must_use]
    pub fn extra_context(&self) -> &HashMap<String, String> {
        &self.extra_context
    }
}

// ── Accessors (resolved) ────────────────────────────────────────────────────

impl<T, A, P, Z> APIEventContext<P, Resolved<T>, A, Z>
where
    T: Clone + Send + Sync,
    A: APIEventAction,
    P: UserProvidedEntity,
    Z: AuthzState,
{
    #[must_use]
    pub fn resolved(&self) -> &T {
        &self.resolved_entity.data
    }

    #[must_use]
    pub fn resolved_mut(&mut self) -> &mut T {
        &mut self.resolved_entity.data
    }

    #[must_use]
    pub fn dispatcher(&self) -> &EventDispatcher {
        &self.dispatcher
    }
}

// ── Event emission ──────────────────────────────────────────────────────────

impl<R: ResolutionState, A: APIEventAction, P: UserProvidedEntity>
    APIEventContext<P, R, A, AuthzUnchecked>
{
    /// Check authorization result and emit the corresponding event.
    ///
    /// Consumes `self` and returns an `AuthzChecked` context on success,
    /// ensuring this can only be called once per context.
    pub fn emit_authz<T, E>(
        self,
        result: Result<T, E>,
    ) -> Result<(APIEventContext<P, R, A, AuthzChecked>, T), ErrorModel>
    where
        E: AuthorizationFailureSource,
    {
        match result {
            Ok(_value) => {
                // TODO: emit authorization success event
                todo!("Emit authorization success event");
                // Ok((checked, value))
            }
            Err(e) => {
                let failure_reason = e.to_failure_reason();
                let error = e.into_error_model();
                let event = AuthorizationFailedEvent {
                    request_metadata: self.request_metadata.clone(),
                    entity: format!("{:?}", self.user_provided_entity),
                    action: self.action.event_action_str(),
                    failure_reason,
                    error: Arc::new(AuthorizationError::clone_from_error_model(&error)),
                };
                let dispatcher = self.dispatcher.clone();
                tokio::spawn(async move {
                    let () = dispatcher.authorization_failed(event).await;
                });
                Err(error)
            }
        }
    }
}

impl<R: ResolutionState, A: APIEventAction, P: UserProvidedEntity>
    APIEventContext<P, R, A, AuthzChecked>
{
    /// Convert an authorization failure to an [`ErrorModel`] without emitting any event.
    ///
    /// Use this for sub-filtering in list-style operations where logging
    /// every filtered-out entry would be too noisy.
    pub fn authz_to_error_no_audit(&self, error: impl AuthorizationFailureSource) -> ErrorModel {
        authz_to_error_no_audit(error)
    }
}

/// Convert an authorization failure to an [`ErrorModel`] without emitting any event.
///
/// Use this for sub-filtering in list-style operations where logging
/// every filtered-out entry would be too noisy.
pub fn authz_to_error_no_audit(error: impl AuthorizationFailureSource) -> ErrorModel {
    error.into_error_model()
}

impl<T: ResolutionState, A: APIEventAction, P: UserProvidedEntity, Z: AuthzState>
    APIEventContext<P, T, A, Z>
{
    #[deprecated(note = "Use `emit_authz` instead")]
    pub fn emit_authz_failure_async(&self, _error: impl AuthorizationFailureSource) -> ErrorModel {
        todo!("Don't use")
    }

    pub fn emit_early_authz_failure(&self, _error: impl AuthorizationFailureSource) -> ErrorModel {
        todo!("Implement")
    }
}
