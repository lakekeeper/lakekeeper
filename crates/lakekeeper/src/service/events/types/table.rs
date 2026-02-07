use std::sync::Arc;

use iceberg::spec::TableMetadataRef;
use iceberg_ext::catalog::rest::{
    CommitTransactionRequest, CreateTableRequest, RegisterTableRequest, RenameTableRequest,
};
use lakekeeper_io::Location;

use crate::{
    WarehouseId,
    api::{
        RequestMetadata,
        iceberg::{
            types::DropParams,
            v1::{DataAccessMode, NamespaceParameters, TableParameters},
        },
    },
    server::tables::CommitContext,
    service::TableId,
};

/// Function type used by hooks to resolve a `TableIdent` to its `TableId`.
/// Implementations should be cheap and non-blocking.
/// Note: Hooks receive this as a borrowed reference valid only for the duration of the call.
/// Do not store it for use outside the async method invocation.
pub type TableIdentToIdFn = dyn Fn(&iceberg::TableIdent) -> Option<TableId> + Send + Sync;

// ===== Table Events =====

/// Event emitted when a transaction is committed containing multiple table changes
#[derive(Clone)]
pub struct CommitTransactionEvent {
    pub warehouse_id: WarehouseId,
    pub request: Arc<CommitTransactionRequest>,
    pub commits: Arc<Vec<CommitContext>>,
    pub table_ident_to_id_fn: Arc<TableIdentToIdFn>,
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

/// Event emitted when a table is created
#[derive(Clone, Debug)]
pub struct CreateTableEvent {
    pub warehouse_id: WarehouseId,
    pub parameters: NamespaceParameters,
    pub request: Arc<CreateTableRequest>,
    pub metadata: TableMetadataRef,
    pub metadata_location: Option<Arc<Location>>,
    pub data_access: DataAccessMode,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when a table is registered (imported with existing metadata)
#[derive(Clone, Debug)]
pub struct RegisterTableEvent {
    pub warehouse_id: WarehouseId,
    pub parameters: NamespaceParameters,
    pub request: Arc<RegisterTableRequest>,
    pub metadata: TableMetadataRef,
    pub metadata_location: Arc<Location>,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when a table is dropped
#[derive(Clone, Debug)]
pub struct DropTableEvent {
    pub warehouse_id: WarehouseId,
    pub parameters: TableParameters,
    pub drop_params: DropParams,
    pub table_id: TableId,
    pub request_metadata: Arc<RequestMetadata>,
}

/// Event emitted when a table is renamed
#[derive(Clone, Debug)]
pub struct RenameTableEvent {
    pub warehouse_id: WarehouseId,
    pub table_id: TableId,
    pub request: Arc<RenameTableRequest>,
    pub request_metadata: Arc<RequestMetadata>,
}
