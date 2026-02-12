use std::{collections::HashMap, sync::Arc};

use iceberg::{TableIdent, spec::TableMetadataRef};
use iceberg_ext::catalog::rest::{CommitTransactionRequest, RenameTableRequest};
use lakekeeper_io::Location;

use crate::{
    WarehouseId,
    api::{RequestMetadata, iceberg::types::DropParams},
    server::tables::CommitContext,
    service::{
        NamespaceWithParent, ResolvedWarehouse, TableInfo,
        authz::CatalogTableAction,
        events::{
            APIEventContext,
            context::{
                AuthzChecked, Resolved, ResolvedTable, UserProvidedTable, UserProvidedTablesByIdent,
            },
        },
        storage::StoragePermissions,
    },
};

// ===== Table Events =====

/// Event emitted when a transaction is committed containing multiple table changes
#[derive(Clone)]
pub struct CommitTransactionEvent {
    pub warehouse_id: WarehouseId,
    pub request: Arc<CommitTransactionRequest>,
    pub commits: Arc<Vec<CommitContext>>,
    pub request_metadata: Arc<RequestMetadata>,
}

impl std::fmt::Debug for CommitTransactionEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitTransactionEvent")
            .field("warehouse_id", &self.warehouse_id)
            .field("request", &self.request)
            .field("commits_len", &self.commits.len())
            .field("request_metadata", &self.request_metadata)
            .field("table_ident_to_id_fn", &"TableIdentToIdFn(...)")
            .finish()
    }
}

/// Event emitted when a table is dropped
#[derive(Clone, Debug)]
pub struct DropTableEvent {
    pub table: ResolvedTable,
    pub drop_params: DropParams,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when a table is renamed
#[derive(Clone, Debug)]
pub struct RenameTableEvent {
    pub source_table: ResolvedTable,
    pub destination_namespace: NamespaceWithParent,
    pub request: Arc<RenameTableRequest>,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Load Table Event - emitted when a user loads a table's metadata
#[derive(Clone, Debug)]
pub struct LoadTableEvent {
    pub warehouse: Arc<ResolvedWarehouse>,
    pub table: Arc<TableInfo>,
    pub storage_permissions: Option<StoragePermissions>,
    pub metadata: TableMetadataRef,
    pub metadata_location: Option<Arc<Location>>,
}

pub type APIEventCommitContext = APIEventContext<
    UserProvidedTablesByIdent,
    Resolved<HashMap<TableIdent, Arc<TableInfo>>>,
    Vec<CatalogTableAction>,
    AuthzChecked,
>;
pub type TableEventContext =
    APIEventContext<UserProvidedTable, Resolved<ResolvedTable>, CatalogTableAction>;

impl APIEventContext<UserProvidedTable, Resolved<ResolvedTable>, CatalogTableAction, AuthzChecked> {
    /// Emit table_created event using context fields
    pub(crate) fn emit_table_loaded_async(
        self,
        metadata: TableMetadataRef,
        metadata_location: Option<Arc<lakekeeper_io::Location>>,
    ) {
        let event = LoadTableEvent {
            warehouse: self.resolved_entity.data.warehouse,
            table: self.resolved_entity.data.table,
            storage_permissions: self.resolved_entity.data.storage_permissions,
            metadata,
            metadata_location,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.table_loaded(event).await;
        });
    }

    /// Emit table dropped event
    pub(crate) fn emit_table_dropped_async(self, drop_parameters: DropParams) {
        let event = DropTableEvent {
            table: self.resolved_entity.data,
            drop_params: drop_parameters,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.table_dropped(event).await;
        });
    }

    /// Emit table renamed event
    pub(crate) fn emit_table_renamed_async(
        self,
        destination_namespace: NamespaceWithParent,
        request: Arc<RenameTableRequest>,
    ) {
        let event = RenameTableEvent {
            source_table: self.resolved_entity.data,
            destination_namespace,
            request,
            request_metadata: self.request_metadata,
        };
        let dispatcher = self.dispatcher;
        tokio::spawn(async move {
            let () = dispatcher.table_renamed(event).await;
        });
    }
}
