use std::{collections::HashMap, sync::Arc};

use iceberg::TableIdent;
use iceberg_ext::catalog::rest::ErrorModel;
use lakekeeper_io::s3::S3Location;
use tracing::Instrument;

use crate::{
    CONFIG, ProjectId, WarehouseId,
    api::{
        RequestMetadata,
        management::v1::{
            check::{
                CatalogActionCheckItem, CatalogActionCheckOperation, NamespaceIdentOrUuid,
                TabularIdentOrUuid,
            },
            tasks::{ControlTasksRequest, ListTasksRequest},
        },
    },
    service::{
        NamespaceId, NamespaceIdentOrId, NamespaceWithParent, ResolvedWarehouse, RoleId, ServerId,
        TableIdentOrId, TableInfo, TabularId, UserId, ViewIdentOrId, ViewInfo,
        authn::UserIdRef,
        authz::{ActionDescriptor, CatalogAction, CatalogTableAction, CatalogViewAction},
        events::{
            AuthorizationError, AuthorizationFailedEvent, AuthorizationFailureSource,
            AuthorizationSucceededEvent, EventDispatcher,
        },
        storage::StoragePermissions,
        tasks::TaskId,
    },
};

pub const FIELD_NAME_SERVER_ID: &str = "server-id";
pub const FIELD_NAME_PROJECT_ID: &str = "project-id";
pub const FIELD_NAME_WAREHOUSE_ID: &str = "warehouse-id";
pub const FIELD_NAME_NAMESPACE: &str = "namespace";
pub const FIELD_NAME_NAMESPACE_ID: &str = "namespace-id";
pub const FIELD_NAME_TABLE: &str = "table";
pub const FIELD_NAME_TABLE_ID: &str = "table-id";
pub const FIELD_NAME_TABLE_LOCATION: &str = "table-location";
pub const FIELD_NAME_VIEW: &str = "view";
pub const FIELD_NAME_VIEW_ID: &str = "view-id";
pub const FIELD_NAME_TASK_ID: &str = "task-id";
pub const FIELD_NAME_ROLE_ID: &str = "role-id";
pub const FIELD_NAME_USER_ID: &str = "user-id";
pub const FIELD_FOR_USER: &str = "for-user";

pub const ENTITY_TYPE_SERVER: &str = "server";
pub const ENTITY_TYPE_PROJECT: &str = "project";
pub const ENTITY_TYPE_WAREHOUSE: &str = "warehouse";
pub const ENTITY_TYPE_NAMESPACE: &str = "namespace";
pub const ENTITY_TYPE_TABLE: &str = "table";
pub const ENTITY_TYPE_VIEW: &str = "view";
pub const ENTITY_TYPE_TASK: &str = "task";
pub const ENTITY_TYPE_ROLE: &str = "role";
pub const ENTITY_TYPE_USER: &str = "user";

// ── Traits ──────────────────────────────────────────────────────────────────

// Marker trait to indicate resolution state
pub trait ResolutionState: Clone + Send + Sync {}

/// A single key-value descriptor for an entity (e.g. "warehouse-id" = "abc-123")
#[derive(Clone, Debug)]
pub struct EntityDescriptorField {
    pub key: &'static str,
    pub value: String,
}

impl EntityDescriptorField {
    pub fn new(key: &'static str, value: &impl ToString) -> Self {
        Self {
            key,
            value: value.to_string(),
        }
    }
}

/// All fields describing one logical entity (e.g. one table: warehouse-id + namespace + name)
#[derive(Clone, Debug)]
pub struct EntityDescriptor {
    pub fields: Vec<EntityDescriptorField>,
    pub entity_type: &'static str,
}

impl EntityDescriptor {
    #[must_use]
    pub fn new(entity_type: &'static str) -> Self {
        Self {
            fields: Vec::new(),
            entity_type,
        }
    }

    #[must_use]
    pub fn field(mut self, key: &'static str, value: &impl ToString) -> Self {
        self.fields.push(EntityDescriptorField::new(key, value));
        self
    }
}

/// The full set of entities involved in an event
#[derive(Clone, Debug, valuable::Valuable)]
#[valuable(transparent)]
pub struct EventEntities {
    pub entities: Vec<EntityDescriptor>,
}

impl EventEntities {
    #[must_use]
    pub fn one(descriptor: EntityDescriptor) -> Self {
        Self {
            entities: vec![descriptor],
        }
    }

    pub fn many(descriptors: impl IntoIterator<Item = EntityDescriptor>) -> Self {
        Self {
            entities: descriptors.into_iter().collect(),
        }
    }
}

pub trait UserProvidedEntity: 'static + Send + Sync + std::fmt::Debug {
    /// A list of key-value pairs representing the user-provided entity in log messages.
    fn event_entities(&self) -> EventEntities;
}

pub trait APIEventActions: 'static + Send + Sync + std::fmt::Debug {
    /// A list of string representations of the actions being performed, for use in log messages.
    fn event_actions(&self) -> Vec<ActionDescriptor>;
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

impl UserProvidedEntity for UserProvidedNamespace {
    fn event_entities(&self) -> EventEntities {
        let desc = EntityDescriptor::new(ENTITY_TYPE_NAMESPACE)
            .field(FIELD_NAME_WAREHOUSE_ID, &self.warehouse_id);
        EventEntities::one(match &self.namespace {
            NamespaceIdentOrId::Name(ident) => desc.field(FIELD_NAME_NAMESPACE, ident),
            NamespaceIdentOrId::Id(id) => desc.field(FIELD_NAME_NAMESPACE_ID, id),
        })
    }
}

#[derive(Clone, Debug)]
pub struct UserProvidedTable {
    pub warehouse_id: WarehouseId,
    pub table: TableIdentOrId,
}

impl UserProvidedEntity for UserProvidedTable {
    fn event_entities(&self) -> EventEntities {
        let desc = EntityDescriptor::new(ENTITY_TYPE_TABLE)
            .field(FIELD_NAME_WAREHOUSE_ID, &self.warehouse_id);
        EventEntities::one(match &self.table {
            TableIdentOrId::Ident(ident) => desc
                .field(FIELD_NAME_NAMESPACE, &ident.namespace)
                .field(FIELD_NAME_TABLE, &ident.name),
            TableIdentOrId::Id(id) => desc.field(FIELD_NAME_TABLE_ID, id),
        })
    }
}

#[derive(Clone, Debug)]
pub struct UserProvidedTableLocation {
    pub warehouse_id: WarehouseId,
    pub table_location: Arc<S3Location>,
}

impl UserProvidedEntity for UserProvidedTableLocation {
    fn event_entities(&self) -> EventEntities {
        EventEntities::one(
            EntityDescriptor::new(ENTITY_TYPE_TABLE)
                .field(FIELD_NAME_WAREHOUSE_ID, &self.warehouse_id)
                .field(FIELD_NAME_TABLE_LOCATION, &self.table_location),
        )
    }
}

#[derive(Clone, Debug)]
pub struct UserProvidedTabularsIDs {
    pub warehouse_id: WarehouseId,
    pub tabulars: Vec<TabularId>,
}

impl UserProvidedEntity for UserProvidedTabularsIDs {
    fn event_entities(&self) -> EventEntities {
        EventEntities::many(self.tabulars.iter().map(|id| {
            match id {
                TabularId::Table(table_id) => EntityDescriptor::new(ENTITY_TYPE_TABLE)
                    .field(FIELD_NAME_WAREHOUSE_ID, &self.warehouse_id)
                    .field(FIELD_NAME_TABLE_ID, table_id),
                TabularId::View(view_id) => EntityDescriptor::new(ENTITY_TYPE_VIEW)
                    .field(FIELD_NAME_WAREHOUSE_ID, &self.warehouse_id)
                    .field(FIELD_NAME_VIEW_ID, view_id),
            }
        }))
    }
}

#[derive(Clone, Debug)]
pub struct UserProvidedTableIdents {
    pub warehouse_id: WarehouseId,
    pub tables: Vec<TableIdent>,
}

impl UserProvidedEntity for UserProvidedTableIdents {
    fn event_entities(&self) -> EventEntities {
        EventEntities::many(self.tables.iter().map(|t| {
            EntityDescriptor::new(ENTITY_TYPE_TABLE)
                .field(FIELD_NAME_WAREHOUSE_ID, &self.warehouse_id)
                .field(FIELD_NAME_NAMESPACE, &t.namespace)
                .field(FIELD_NAME_TABLE, &t.name)
        }))
    }
}

#[derive(Clone, Debug)]
pub struct UserProvidedView {
    pub warehouse_id: WarehouseId,
    pub view: ViewIdentOrId,
}

impl UserProvidedEntity for UserProvidedView {
    fn event_entities(&self) -> EventEntities {
        let desc = EntityDescriptor::new(ENTITY_TYPE_VIEW)
            .field(FIELD_NAME_WAREHOUSE_ID, &self.warehouse_id);
        EventEntities::one(match &self.view {
            ViewIdentOrId::Ident(ident) => desc
                .field(FIELD_NAME_NAMESPACE, &ident.namespace)
                .field(FIELD_NAME_VIEW, &ident.name),
            ViewIdentOrId::Id(id) => desc.field(FIELD_NAME_VIEW_ID, id),
        })
    }
}

#[derive(Clone, Debug)]
pub struct UserProvidedTask {
    pub warehouse_id: WarehouseId,
    pub task_id: TaskId,
}
impl UserProvidedEntity for UserProvidedTask {
    fn event_entities(&self) -> EventEntities {
        EventEntities::one(
            EntityDescriptor::new(ENTITY_TYPE_TASK)
                .field(FIELD_NAME_WAREHOUSE_ID, &self.warehouse_id)
                .field(FIELD_NAME_TASK_ID, &self.task_id),
        )
    }
}

impl UserProvidedEntity for (ServerId, Vec<CatalogActionCheckItem>) {
    fn event_entities(&self) -> EventEntities {
        EventEntities::many(self.1.iter().map(|item| {
            let mut desc = match &item.operation {
                CatalogActionCheckOperation::Server { .. } => {
                    EntityDescriptor::new(ENTITY_TYPE_SERVER).field(FIELD_NAME_SERVER_ID, &self.0)
                }
                CatalogActionCheckOperation::Project { project_id, .. } => match project_id {
                    Some(id) => {
                        EntityDescriptor::new(ENTITY_TYPE_PROJECT).field(FIELD_NAME_PROJECT_ID, id)
                    }
                    None => EntityDescriptor::new(ENTITY_TYPE_PROJECT)
                        .field(FIELD_NAME_PROJECT_ID, &"default"),
                },
                CatalogActionCheckOperation::Warehouse { warehouse_id, .. } => {
                    EntityDescriptor::new(ENTITY_TYPE_WAREHOUSE)
                        .field(FIELD_NAME_WAREHOUSE_ID, warehouse_id)
                }
                CatalogActionCheckOperation::Namespace { namespace, .. } => match namespace {
                    NamespaceIdentOrUuid::Id {
                        namespace_id,
                        warehouse_id,
                    } => EntityDescriptor::new(ENTITY_TYPE_NAMESPACE)
                        .field(FIELD_NAME_WAREHOUSE_ID, warehouse_id)
                        .field(FIELD_NAME_NAMESPACE_ID, namespace_id),
                    NamespaceIdentOrUuid::Name {
                        namespace,
                        warehouse_id,
                    } => EntityDescriptor::new(ENTITY_TYPE_NAMESPACE)
                        .field(FIELD_NAME_WAREHOUSE_ID, warehouse_id)
                        .field(FIELD_NAME_NAMESPACE, namespace),
                },
                CatalogActionCheckOperation::Table { table, .. } => match table {
                    TabularIdentOrUuid::IdInWarehouse {
                        warehouse_id,
                        table_id,
                    } => EntityDescriptor::new(ENTITY_TYPE_TABLE)
                        .field(FIELD_NAME_WAREHOUSE_ID, warehouse_id)
                        .field(FIELD_NAME_TABLE_ID, table_id),
                    TabularIdentOrUuid::Name {
                        namespace,
                        table,
                        warehouse_id,
                    } => EntityDescriptor::new(ENTITY_TYPE_TABLE)
                        .field(FIELD_NAME_WAREHOUSE_ID, warehouse_id)
                        .field(FIELD_NAME_NAMESPACE, namespace)
                        .field(FIELD_NAME_TABLE, table),
                },
                CatalogActionCheckOperation::View { view, .. } => match view {
                    TabularIdentOrUuid::IdInWarehouse {
                        warehouse_id,
                        table_id,
                    } => EntityDescriptor::new(ENTITY_TYPE_VIEW)
                        .field(FIELD_NAME_WAREHOUSE_ID, warehouse_id)
                        .field(FIELD_NAME_VIEW_ID, table_id),
                    TabularIdentOrUuid::Name {
                        namespace,
                        table,
                        warehouse_id,
                    } => EntityDescriptor::new(ENTITY_TYPE_VIEW)
                        .field(FIELD_NAME_WAREHOUSE_ID, warehouse_id)
                        .field(FIELD_NAME_NAMESPACE, namespace)
                        .field(FIELD_NAME_VIEW, table),
                },
            };
            if let Some(identity) = &item.identity {
                desc = desc.field(FIELD_FOR_USER, identity);
            }
            desc
        }))
    }
}

// Primitive entity impls
macro_rules! impl_user_provided_entity {
    ($ty:ty, $entity_type:expr, $field:expr) => {
        impl UserProvidedEntity for $ty {
            fn event_entities(&self) -> EventEntities {
                EventEntities::one(EntityDescriptor::new($entity_type).field($field, self))
            }
        }
    };
}

impl_user_provided_entity!(ServerId, ENTITY_TYPE_SERVER, FIELD_NAME_SERVER_ID);
impl_user_provided_entity!(ProjectId, ENTITY_TYPE_PROJECT, FIELD_NAME_PROJECT_ID);
impl_user_provided_entity!(WarehouseId, ENTITY_TYPE_WAREHOUSE, FIELD_NAME_WAREHOUSE_ID);
impl_user_provided_entity!(NamespaceId, ENTITY_TYPE_NAMESPACE, FIELD_NAME_NAMESPACE_ID);
impl_user_provided_entity!(RoleId, ENTITY_TYPE_ROLE, FIELD_NAME_ROLE_ID);
impl_user_provided_entity!(UserId, ENTITY_TYPE_USER, FIELD_NAME_USER_ID);

// ── Action types ────────────────────────────────────────────────────────────
#[derive(Clone, Debug)]
pub struct ServerActionSearchUsers {}
impl APIEventActions for ServerActionSearchUsers {
    fn event_actions(&self) -> Vec<ActionDescriptor> {
        vec![
            ActionDescriptor::builder()
                .action_name("search_users")
                .build(),
        ]
    }
}

#[derive(Clone, Debug)]
pub struct ServerActionListProjects {}
impl APIEventActions for ServerActionListProjects {
    fn event_actions(&self) -> Vec<ActionDescriptor> {
        vec![
            ActionDescriptor::builder()
                .action_name("list_projects")
                .build(),
        ]
    }
}

#[derive(Clone, Debug)]
pub struct WarehouseActionSearchTabulars {}
impl APIEventActions for WarehouseActionSearchTabulars {
    fn event_actions(&self) -> Vec<ActionDescriptor> {
        vec![
            ActionDescriptor::builder()
                .action_name("search_tabulars")
                .build(),
        ]
    }
}

#[derive(Clone, Debug)]
pub struct IntrospectPermissions {}
impl APIEventActions for IntrospectPermissions {
    fn event_actions(&self) -> Vec<ActionDescriptor> {
        vec![
            ActionDescriptor::builder()
                .action_name("introspect_permissions")
                .build(),
        ]
    }
}

#[derive(Clone, Debug)]
pub struct GetTaskDetailsAction {}
impl APIEventActions for GetTaskDetailsAction {
    fn event_actions(&self) -> Vec<ActionDescriptor> {
        vec![
            ActionDescriptor::builder()
                .action_name("get_task_details")
                .build(),
        ]
    }
}

impl APIEventActions for ListTasksRequest {
    fn event_actions(&self) -> Vec<ActionDescriptor> {
        vec![
            ActionDescriptor::builder()
                .action_name("list_tasks")
                .build(),
        ]
    }
}

impl APIEventActions for ControlTasksRequest {
    fn event_actions(&self) -> Vec<ActionDescriptor> {
        vec![
            ActionDescriptor::builder()
                .action_name("control_tasks")
                .build(),
        ]
    }
}

#[derive(Clone, Debug)]
pub struct TabularAction {
    pub table_action: CatalogTableAction,
    pub view_action: CatalogViewAction,
}

impl APIEventActions for TabularAction {
    fn event_actions(&self) -> Vec<ActionDescriptor> {
        let mut actions = self.table_action.event_actions();
        actions.extend(self.view_action.event_actions());
        actions
    }
}

impl APIEventActions for Vec<CatalogTableAction> {
    fn event_actions(&self) -> Vec<ActionDescriptor> {
        self.iter()
            .flat_map(APIEventActions::event_actions)
            .collect()
    }
}

impl<T: CatalogAction> APIEventActions for T {
    fn event_actions(&self) -> Vec<ActionDescriptor> {
        vec![self.action_descriptor()]
    }
}

// ── APIEventContext ─────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct APIEventContext<P, R, A, Z = AuthzUnchecked>
where
    P: UserProvidedEntity,
    R: ResolutionState,
    A: APIEventActions,
    Z: AuthzState,
{
    pub(super) request_metadata: Arc<RequestMetadata>,
    pub(super) dispatcher: EventDispatcher,
    pub(super) user_provided_entity: Arc<P>,
    pub(super) action: Arc<A>,
    pub(super) resolved_entity: R,
    pub(super) _authz: std::marker::PhantomData<Z>,
    pub(super) extra_context: HashMap<String, String>,
}

// ── Core impl (Unresolved) ──────────────────────────────────────────────────

impl<P: UserProvidedEntity, A: APIEventActions> APIEventContext<P, Unresolved, A, AuthzUnchecked> {
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
            user_provided_entity: Arc::new(entity),
            resolved_entity: Unresolved,
            action: Arc::new(action),
            _authz: std::marker::PhantomData,
            extra_context: HashMap::new(),
        }
    }

    #[must_use]
    pub fn new_arc(
        request_metadata: Arc<RequestMetadata>,
        dispatcher: EventDispatcher,
        entity: Arc<P>,
        action: Arc<A>,
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

impl<P: UserProvidedEntity, A: APIEventActions, Z: AuthzState>
    APIEventContext<P, Unresolved, A, Z>
{
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

impl<A: APIEventActions> APIEventContext<ServerId, Unresolved, A> {
    #[must_use]
    pub fn for_server(
        request_metadata: Arc<RequestMetadata>,
        dispatcher: EventDispatcher,
        action: A,
        server_id: ServerId,
    ) -> Self {
        Self::new(request_metadata, dispatcher, server_id, action)
    }
}

impl<A: APIEventActions> APIEventContext<ProjectId, Unresolved, A> {
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

impl<A: APIEventActions> APIEventContext<UserId, Unresolved, A> {
    #[must_use]
    pub fn for_user(
        request_metadata: Arc<RequestMetadata>,
        dispatcher: EventDispatcher,
        user_id: UserIdRef,
        action: A,
    ) -> Self {
        Self::new_arc(request_metadata, dispatcher, user_id, Arc::new(action))
    }
}

impl<A: APIEventActions> APIEventContext<RoleId, Unresolved, A> {
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

impl<A: APIEventActions> APIEventContext<WarehouseId, Unresolved, A> {
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

impl<A: APIEventActions> APIEventContext<UserProvidedNamespace, Unresolved, A> {
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

impl<A: APIEventActions> APIEventContext<NamespaceId, Unresolved, A> {
    #[must_use]
    pub fn for_namespace_only_id(
        request_metadata: Arc<RequestMetadata>,
        dispatcher: EventDispatcher,
        namespace: NamespaceId,
        action: A,
    ) -> Self {
        Self::new(request_metadata, dispatcher, namespace, action)
    }
}

impl<A: APIEventActions> APIEventContext<UserProvidedTable, Unresolved, A> {
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

impl<A: APIEventActions> APIEventContext<UserProvidedTableIdents, Unresolved, A> {
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
            UserProvidedTableIdents {
                warehouse_id,
                tables,
            },
            action,
        )
    }
}

impl<A: APIEventActions> APIEventContext<UserProvidedTabularsIDs, Unresolved, A> {
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
            UserProvidedTabularsIDs {
                warehouse_id,
                tabulars,
            },
            action,
        )
    }
}

impl<A: APIEventActions> APIEventContext<UserProvidedView, Unresolved, A> {
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

impl<A: APIEventActions> APIEventContext<UserProvidedTask, Unresolved, A> {
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
    A: APIEventActions,
    P: UserProvidedEntity,
    Z: AuthzState,
{
    #[must_use]
    pub fn action(&self) -> &A {
        &self.action
    }

    pub fn override_action(&mut self, action: A) {
        self.action = Arc::new(action);
    }

    pub fn override_action_arc(&mut self, action: Arc<A>) {
        self.action = action;
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
    A: APIEventActions,
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

pub type CheckedAPIEventContext<P, R, A, T> = (APIEventContext<P, R, A, AuthzChecked>, T);

impl<R: ResolutionState, A: APIEventActions, P: UserProvidedEntity>
    APIEventContext<P, R, A, AuthzUnchecked>
{
    /// Check authorization result and emit the corresponding event.
    ///
    /// Consumes `self` and returns an `AuthzChecked` context on success,
    /// ensuring this can only be called once per context.
    pub fn emit_authz<T, E>(
        self,
        result: Result<T, E>,
    ) -> Result<CheckedAPIEventContext<P, R, A, T>, ErrorModel>
    where
        E: AuthorizationFailureSource,
    {
        match result {
            Ok(value) => {
                let event = AuthorizationSucceededEvent {
                    request_metadata: self.request_metadata.clone(),
                    entities: Arc::new(self.user_provided_entity.event_entities()),
                    actions: Arc::new(self.action.event_actions()),
                    extra_context: Arc::new(self.extra_context.clone()),
                };
                let dispatcher = self.dispatcher.clone();
                let span = tracing::Span::current();
                tokio::spawn(
                    async move {
                        let () = dispatcher.authorization_succeeded(event).await;
                    }
                    .instrument(span),
                );
                Ok((self.into(), value))
            }
            Err(e) => Err(self.emit_authz_failure_event(e)),
        }
    }
}

impl<R: ResolutionState, A: APIEventActions, P: UserProvidedEntity>
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

impl<T: ResolutionState, A: APIEventActions, P: UserProvidedEntity, Z: AuthzState>
    APIEventContext<P, T, A, Z>
{
    fn emit_authz_failure_event(&self, error: impl AuthorizationFailureSource) -> ErrorModel {
        let failure_reason = error.to_failure_reason();
        let mut error = error.into_error_model();

        if CONFIG.audit.tracing.enabled {
            error.skip_log = true; // Already emitted in more detail by audit logger
        }

        let event = AuthorizationFailedEvent {
            request_metadata: self.request_metadata.clone(),
            entities: Arc::new(self.user_provided_entity.event_entities()),
            actions: Arc::new(self.action.event_actions()),
            failure_reason,
            error: Arc::new(AuthorizationError::clone_from_error_model(&error)),
            extra_context: Arc::new(self.extra_context.clone()),
        };
        let dispatcher = self.dispatcher.clone();
        let span = tracing::Span::current();
        tokio::spawn(
            async move {
                let () = dispatcher.authorization_failed(event).await;
            }
            .instrument(span),
        );
        error
    }
}

impl<T: ResolutionState, A: APIEventActions, P: UserProvidedEntity>
    APIEventContext<P, T, A, AuthzUnchecked>
{
    pub fn emit_early_authz_failure(&self, error: impl AuthorizationFailureSource) -> ErrorModel {
        self.emit_authz_failure_event(error)
    }
}

impl<T: ResolutionState, A: APIEventActions, P: UserProvidedEntity>
    APIEventContext<P, T, A, AuthzChecked>
{
    pub fn emit_late_authz_failure(&self, error: impl AuthorizationFailureSource) -> ErrorModel {
        self.emit_authz_failure_event(error)
    }
}

impl<P, R, A> From<APIEventContext<P, R, A, AuthzUnchecked>>
    for APIEventContext<P, R, A, AuthzChecked>
where
    P: UserProvidedEntity,
    R: ResolutionState,
    A: APIEventActions,
{
    fn from(context: APIEventContext<P, R, A, AuthzUnchecked>) -> Self {
        APIEventContext {
            request_metadata: context.request_metadata,
            dispatcher: context.dispatcher,
            user_provided_entity: context.user_provided_entity,
            action: context.action,
            resolved_entity: context.resolved_entity,
            _authz: std::marker::PhantomData,
            extra_context: context.extra_context,
        }
    }
}
