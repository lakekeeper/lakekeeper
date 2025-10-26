use std::sync::Arc;

use http::StatusCode;
use iceberg::{
    spec::{TableMetadata, TableMetadataRef},
    TableIdent, TableUpdate,
};
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
use lakekeeper_io::Location;

use crate::{
    api::iceberg::v1::tables::LoadTableFilters,
    server::tables::TableMetadataDiffs,
    service::{
        define_simple_error, define_simple_tabular_err, define_transparent_error,
        impl_error_stack_methods, impl_from_with_detail, CatalogBackendError, CatalogStore,
        ConversionError, CreateTabularError, InternalBackendErrors, InternalParseLocationError,
        InvalidNamespaceIdentifier, LocationAlreadyTaken, NamespaceId, SerializationError, TableId,
        TableInfo, TabularAlreadyExists, TabularNotFound, Transaction, UnexpectedTabularInResponse,
    },
    WarehouseId,
};

#[derive(Debug, PartialEq, Eq)]
pub struct LoadTableResponse {
    pub table_id: TableId,
    pub namespace_id: NamespaceId,
    pub table_metadata: TableMetadata,
    pub metadata_location: Option<Location>,
    pub warehouse_updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone)]
pub struct TableCommit {
    pub new_metadata: TableMetadataRef,
    pub new_metadata_location: Location,
    pub previous_metadata_location: Option<Location>,
    pub updates: Arc<Vec<TableUpdate>>,
    pub diffs: TableMetadataDiffs,
}

#[derive(Debug, Clone)]
pub struct TableCreation<'c> {
    pub warehouse_id: WarehouseId,
    pub namespace_id: NamespaceId,
    pub table_ident: &'c TableIdent,
    pub metadata_location: Option<&'c Location>,
    pub table_metadata: &'c TableMetadata,
}

define_simple_tabular_err!(
    RequiredTableComponentMissing,
    "A required table field could not be loaded"
);
impl From<RequiredTableComponentMissing> for ErrorModel {
    fn from(err: RequiredTableComponentMissing) -> Self {
        ErrorModel {
            message: err.to_string(),
            r#type: "RequiredTableComponentMissing".to_string(),
            code: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
            source: None,
            stack: err.stack,
        }
    }
}

define_simple_tabular_err!(
    InternalTableMetadataBuildFailed,
    "Failed to build table metadata from loaded components"
);
impl From<InternalTableMetadataBuildFailed> for ErrorModel {
    fn from(err: InternalTableMetadataBuildFailed) -> Self {
        ErrorModel {
            message: err.to_string(),
            r#type: "TableMetadataBuildFailed".to_string(),
            code: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
            source: None,
            stack: err.stack,
        }
    }
}

define_simple_tabular_err!(
    TableMetadataValidationFailedInternal,
    "Table Metadata validation failed after loading table from catalog"
);
impl From<TableMetadataValidationFailedInternal> for ErrorModel {
    fn from(err: TableMetadataValidationFailedInternal) -> Self {
        ErrorModel {
            message: err.to_string(),
            r#type: "TableMetadataValidationFailedInternal".to_string(),
            code: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
            source: None,
            stack: err.stack,
        }
    }
}

define_simple_error!(TooManyUpdatesInCommit, "Too many updates in single commit");
impl From<TooManyUpdatesInCommit> for ErrorModel {
    fn from(err: TooManyUpdatesInCommit) -> Self {
        ErrorModel {
            code: StatusCode::BAD_REQUEST.as_u16(),
            r#type: "TooManyUpdatesInCommit".to_string(),
            message: err.to_string(),
            stack: err.stack,
            source: None,
        }
    }
}

define_transparent_error! {
    pub enum LoadTableError,
    stack_message: "Error loading table from catalog",
    variants: [
        CatalogBackendError,
        InvalidNamespaceIdentifier,
        RequiredTableComponentMissing,
        InternalParseLocationError,
        TableMetadataValidationFailedInternal,
        ConversionError,
        InternalTableMetadataBuildFailed
    ]
}

define_transparent_error! {
    pub enum CreateTableError,
    stack_message: "Error creating table in catalog",
    variants: [
        CatalogBackendError,
        InternalParseLocationError,
        LocationAlreadyTaken,
        InvalidNamespaceIdentifier,
        TabularAlreadyExists,
        UnexpectedTabularInResponse,
        SerializationError,
        ConversionError
    ]
}
impl From<CreateTabularError> for CreateTableError {
    fn from(err: CreateTabularError) -> Self {
        match err {
            // Skip additional stack (not using .into())
            CreateTabularError::CatalogBackendError(e) => CreateTableError::CatalogBackendError(e),
            CreateTabularError::LocationAlreadyTaken(e) => {
                CreateTableError::LocationAlreadyTaken(e)
            }
            CreateTabularError::InternalParseLocationError(e) => {
                CreateTableError::InternalParseLocationError(e)
            }
            CreateTabularError::InvalidNamespaceIdentifier(e) => {
                CreateTableError::InvalidNamespaceIdentifier(e)
            }
            CreateTabularError::TabularAlreadyExists(e) => {
                CreateTableError::TabularAlreadyExists(e)
            }
        }
    }
}
impl From<InternalBackendErrors> for CreateTableError {
    fn from(err: InternalBackendErrors) -> Self {
        match err {
            InternalBackendErrors::SerializationError(e) => e.into(),
            InternalBackendErrors::CatalogBackendError(e) => e.into(),
            InternalBackendErrors::InternalConversionError(e) => e.into(),
        }
    }
}

define_transparent_error! {
    pub enum CommitTableTransactionError,
    stack_message: "Error committing table changes in catalog",
    variants: [
        CatalogBackendError,
        TabularNotFound,
        TooManyUpdatesInCommit,
        SerializationError,
        InternalParseLocationError,
        ConversionError,
        InvalidNamespaceIdentifier,
        UnexpectedTabularInResponse
    ]
}
impl From<InternalBackendErrors> for CommitTableTransactionError {
    fn from(err: InternalBackendErrors) -> Self {
        match err {
            InternalBackendErrors::SerializationError(e) => e.into(),
            InternalBackendErrors::CatalogBackendError(e) => e.into(),
            InternalBackendErrors::InternalConversionError(e) => e.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StagedTableId(pub TableId);

#[async_trait::async_trait]
pub trait CatalogTableOps
where
    Self: CatalogStore,
{
    async fn create_table<'a>(
        table_creation: TableCreation<'_>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<(TableInfo, Option<StagedTableId>), CreateTableError> {
        Self::create_table_impl(table_creation, transaction).await
    }

    /// Load tables by table id.
    /// Does not return staged tables.
    /// If a table does not exist, do not include it in the response.
    async fn load_tables<'a>(
        warehouse_id: WarehouseId,
        tables: impl IntoIterator<Item = TableId> + Send,
        include_deleted: bool,
        filters: &LoadTableFilters,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Vec<LoadTableResponse>, LoadTableError> {
        Self::load_tables_impl(warehouse_id, tables, include_deleted, filters, transaction).await
    }

    /// Commit changes to a table.
    /// The table might be staged or not.
    async fn commit_table_transaction<'a>(
        warehouse_id: WarehouseId,
        commits: impl IntoIterator<Item = TableCommit> + Send,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Vec<TableInfo>, CommitTableTransactionError> {
        Self::commit_table_transaction_impl(warehouse_id, commits, transaction).await
    }
}

impl<T> CatalogTableOps for T where T: CatalogStore {}
