use std::{collections::HashMap, sync::Arc};

use iceberg::spec::{TableMetadataRef, ViewMetadata, ViewMetadataRef};
use iceberg_ext::catalog::rest::{
    CreateTableRequest, CreateViewRequest, ErrorModel, RegisterTableRequest,
    UpdateNamespacePropertiesResponse,
};
use lakekeeper_io::Location;

use crate::{
    WarehouseId,
    api::{RequestMetadata, iceberg::v1::DataAccessMode},
    service::{
        MovedNamespace, NamespaceId, NamespaceIdent, NamespaceWithParent, ResolvedWarehouse,
        authz::{CatalogNamespaceAction, CatalogWarehouseAction},
        events::{
            APIEventContext, AuthorizationFailureSource, EventDispatcher,
            context::{
                AuthzChecked, AuthzState, AuthzUnchecked, ResolutionState, Resolved,
                ResolvedNamespace, UserProvidedNamespace,
            },
        },
    },
};

// ===== Namespace Events =====

/// Event emitted when a namespace is created
#[derive(Clone, Debug)]
pub struct CreateNamespaceEvent {
    pub warehouse_id: WarehouseId,
    pub namespace: NamespaceWithParent,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when a namespace is dropped
#[derive(Clone, Debug)]
pub struct DropNamespaceEvent {
    pub namespace: NamespaceWithParent,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when a namespace is moved (re-parented and/or renamed)
///
/// `previous_ident` and `previous_parent` describe the namespace before the move.
/// Consumers that mirror the hierarchy — the namespace cache, the authorizer — need them
/// to retire the old path; a listener that only sees the new state cannot tell what to
/// evict. Never emitted for a no-op move.
#[derive(Clone, Debug)]
pub struct MoveNamespaceEvent {
    pub warehouse_id: WarehouseId,
    /// The namespace after the move.
    pub namespace: NamespaceWithParent,
    /// Canonical path the namespace had before the move.
    pub previous_ident: NamespaceIdent,
    /// Parent before the move. `None` if the namespace was top-level.
    pub previous_parent: Option<NamespaceId>,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when namespace protection status changes
#[derive(Clone, Debug)]
pub struct SetNamespaceProtectionEvent {
    pub requested_protected: bool,
    pub updated_namespace: NamespaceWithParent,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when namespace properties are updated
#[derive(Clone, Debug)]
pub struct UpdateNamespacePropertiesEvent {
    pub warehouse_id: WarehouseId,
    pub namespace: NamespaceWithParent,
    pub updated_properties: Arc<UpdateNamespacePropertiesResponse>,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when Namespace metadata is loaded
#[derive(Clone, Debug)]
pub struct NamespaceMetadataLoadedEvent {
    pub namespace: NamespaceWithParent,
    pub properties: Arc<HashMap<String, String>>,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when a table is created (within a namespace)
#[derive(Clone, Debug)]
pub struct CreateTableEvent {
    pub namespace: ResolvedNamespace,
    pub table_name: String,
    pub metadata: TableMetadataRef,
    pub metadata_location: Option<Arc<Location>>,
    pub data_access: DataAccessMode,
    pub request_metadata: Arc<RequestMetadata>,
    pub request: Arc<CreateTableRequest>,
}

/// Event emitted when a table is registered within a namespace (imported with existing metadata)
#[derive(Clone, Debug)]
pub struct RegisterTableEvent {
    pub namespace: ResolvedNamespace,
    pub request: Arc<RegisterTableRequest>,
    pub metadata: TableMetadataRef,
    pub metadata_location: Arc<Location>,
    /// The delegation the response was built for. Register vends credentials, so
    /// without this a subscriber cannot tell a registration that handed out
    /// read-write-delete credentials from one that handed out none.
    pub data_access: DataAccessMode,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when a view is created (within a namespace)
#[derive(Clone, Debug)]
pub struct CreateViewEvent {
    pub namespace: ResolvedNamespace,
    pub view_name: String,
    pub metadata: Arc<ViewMetadata>,
    pub metadata_location: Arc<Location>,
    pub request_metadata: Arc<RequestMetadata>,
    pub request: Arc<CreateViewRequest>,
}

#[derive(Clone, Debug, derive_more::From)]
pub enum NamespaceOrWarehouseAPIContext<
    RW: ResolutionState,
    RN: ResolutionState,
    Z: AuthzState = AuthzUnchecked,
> {
    Warehouse(APIEventContext<WarehouseId, RW, CatalogWarehouseAction, Z>),
    Namespace(APIEventContext<UserProvidedNamespace, RN, CatalogNamespaceAction, Z>),
}

impl<RW: ResolutionState, RN: ResolutionState, Z: AuthzState>
    NamespaceOrWarehouseAPIContext<RW, RN, Z>
{
    pub fn request_metadata(&self) -> &Arc<RequestMetadata> {
        match self {
            NamespaceOrWarehouseAPIContext::Warehouse(ctx) => &ctx.request_metadata,
            NamespaceOrWarehouseAPIContext::Namespace(ctx) => &ctx.request_metadata,
        }
    }

    /// Either arm carries the same dispatcher — namespace creation is authorized against
    /// the warehouse or the parent namespace, but emits into one event stream.
    pub fn dispatcher(&self) -> &EventDispatcher {
        match self {
            NamespaceOrWarehouseAPIContext::Warehouse(ctx) => ctx.dispatcher(),
            NamespaceOrWarehouseAPIContext::Namespace(ctx) => ctx.dispatcher(),
        }
    }
}

impl<RW: ResolutionState, RN: ResolutionState>
    NamespaceOrWarehouseAPIContext<RW, RN, AuthzUnchecked>
{
    pub fn emit_authz<T, E>(
        self,
        result: Result<T, E>,
    ) -> Result<(NamespaceOrWarehouseAPIContext<RW, RN, AuthzChecked>, T), ErrorModel>
    where
        E: AuthorizationFailureSource,
    {
        match self {
            NamespaceOrWarehouseAPIContext::Warehouse(ctx) => {
                let (ctx, val) = ctx.emit_authz(result)?;
                Ok((NamespaceOrWarehouseAPIContext::Warehouse(ctx), val))
            }
            NamespaceOrWarehouseAPIContext::Namespace(ctx) => {
                let (ctx, val) = ctx.emit_authz(result)?;
                Ok((NamespaceOrWarehouseAPIContext::Namespace(ctx), val))
            }
        }
    }
}

pub(crate) type ResolvedNamespaceOrWarehouseContext = NamespaceOrWarehouseAPIContext<
    Resolved<Arc<ResolvedWarehouse>>,
    Resolved<ResolvedNamespace>,
    AuthzChecked,
>;

impl ResolvedNamespaceOrWarehouseContext {
    pub(crate) fn emit_namespace_created_async(self, created_namespace: NamespaceWithParent) {
        match self {
            NamespaceOrWarehouseAPIContext::Warehouse(ctx) => {
                let event = CreateNamespaceEvent {
                    warehouse_id: ctx.resolved().warehouse_id,
                    namespace: created_namespace,
                    request_metadata: ctx.request_metadata,
                };
                let dispatcher = ctx.dispatcher;
                tokio::spawn(async move {
                    let () = dispatcher.namespace_created(event).await;
                });
            }
            NamespaceOrWarehouseAPIContext::Namespace(ctx) => {
                let event = CreateNamespaceEvent {
                    warehouse_id: ctx.resolved().warehouse.warehouse_id,
                    namespace: created_namespace,
                    request_metadata: ctx.request_metadata,
                };
                let dispatcher = ctx.dispatcher;
                tokio::spawn(async move {
                    let () = dispatcher.namespace_created(event).await;
                });
            }
        }
    }
}

impl
    APIEventContext<
        UserProvidedNamespace,
        Resolved<ResolvedNamespace>,
        CatalogNamespaceAction,
        AuthzChecked,
    >
{
    pub(crate) fn emit_namespace_dropped_async(self) {
        let event = DropNamespaceEvent {
            namespace: self.resolved_entity.data.namespace,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.namespace_dropped(event).await;
        });
    }

    pub(crate) fn emit_namespace_properties_updated_async(
        self,
        updated_namespace: NamespaceWithParent,
        updated_properties: Arc<UpdateNamespacePropertiesResponse>,
    ) {
        let event = UpdateNamespacePropertiesEvent {
            warehouse_id: self.resolved().warehouse.warehouse_id,
            namespace: updated_namespace,
            updated_properties,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.namespace_properties_updated(event).await;
        });
    }

    pub(crate) fn emit_namespace_metadata_loaded_async(
        self,
        properties: Arc<HashMap<String, String>>,
    ) {
        let event = NamespaceMetadataLoadedEvent {
            namespace: self.resolved().namespace.clone(),
            properties,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.namespace_metadata_loaded(event).await;
        });
    }

    /// Emit `namespace_moved`.
    ///
    /// Takes the whole [`MovedNamespace`] rather than the new state alone, because
    /// listeners that mirror the hierarchy have to retire the old path. Does nothing when
    /// the move changed nothing — there is no state transition to announce, and emitting
    /// would make the namespace cache evict and re-insert an unchanged entry.
    pub(crate) fn emit_namespace_moved_async(self, moved: MovedNamespace) {
        if moved.is_noop() {
            return;
        }
        let event = MoveNamespaceEvent {
            warehouse_id: self.resolved().warehouse.warehouse_id,
            namespace: moved.namespace,
            previous_ident: moved.previous_ident,
            previous_parent: moved.previous_parent,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.namespace_moved(event).await;
        });
    }

    pub(crate) fn emit_namespace_protection_set(
        self,
        requested_protected: bool,
        updated_namespace: NamespaceWithParent,
    ) {
        let event = SetNamespaceProtectionEvent {
            requested_protected,
            updated_namespace,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.namespace_protection_set(event).await;
        });
    }

    /// Emit `table_created` event
    pub(crate) fn emit_table_created_async(
        self,
        metadata: TableMetadataRef,
        metadata_location: Option<Arc<Location>>,
        data_access: DataAccessMode,
        table_name: String,
        request: Arc<CreateTableRequest>,
    ) {
        let event = CreateTableEvent {
            namespace: self.resolved_entity.data,
            table_name,
            metadata,
            metadata_location,
            data_access,
            request_metadata: self.request_metadata,
            request,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.table_created(event).await;
        });
    }

    /// Emit table registered event
    pub(crate) fn emit_table_registered_async(
        self,
        request: Arc<RegisterTableRequest>,
        metadata: TableMetadataRef,
        metadata_location: Arc<Location>,
        data_access: DataAccessMode,
    ) {
        let event = RegisterTableEvent {
            namespace: self.resolved_entity.data,
            request,
            metadata,
            metadata_location,
            data_access,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.table_registered(event).await;
        });
    }

    /// Emit view created event
    pub(crate) fn emit_view_created_async(
        self,
        metadata: ViewMetadataRef,
        metadata_location: Arc<Location>,
        view_name: String,
        request: Arc<CreateViewRequest>,
    ) {
        let event = CreateViewEvent {
            namespace: self.resolved_entity.data,
            view_name,
            metadata,
            metadata_location,
            request_metadata: self.request_metadata,
            request,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.view_created(event).await;
        });
    }
}
