use std::sync::Arc;

use iceberg::{
    spec::{TableMetadata, TableMetadataRef, ViewMetadata},
    NamespaceIdent, TableIdent, TableUpdate,
};
use lakekeeper_io::Location;

use crate::{
    server::tables::TableMetadataDiffs,
    service::{
        authz::AsTableId, storage::StorageProfile, tasks::TaskId, NamespaceId, Result, TableId,
        TabularIdentOwned, ViewId,
    },
    SecretIdent, WarehouseId,
};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TabularListFlags {
    pub include_active: bool,
    pub include_staged: bool,
    pub include_deleted: bool,
}

impl TabularListFlags {
    #[must_use]
    pub fn active() -> Self {
        Self {
            include_staged: false,
            include_deleted: false,
            include_active: true,
        }
    }

    #[must_use]
    pub fn all() -> Self {
        Self {
            include_staged: true,
            include_deleted: true,
            include_active: true,
        }
    }

    #[must_use]
    pub fn only_deleted() -> Self {
        Self {
            include_staged: false,
            include_deleted: true,
            include_active: false,
        }
    }
}

#[derive(Debug)]
pub struct CreateTableResponse {
    pub table_metadata: TableMetadata,
    pub staged_table_id: Option<TableId>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct LoadTableResponse {
    pub table_id: TableId,
    pub namespace_id: NamespaceId,
    pub table_metadata: TableMetadata,
    pub metadata_location: Option<Location>,
    pub storage_secret_ident: Option<SecretIdent>,
    pub storage_profile: StorageProfile,
}

#[derive(Debug, PartialEq, Eq)]
pub struct GetTableMetadataResponse {
    pub table: TableIdent,
    pub table_id: TableId,
    pub namespace_id: NamespaceId,
    pub warehouse_id: WarehouseId,
    pub location: String,
    pub metadata_location: Option<String>,
    pub storage_secret_ident: Option<SecretIdent>,
    pub storage_profile: StorageProfile,
}

impl AsTableId for GetTableMetadataResponse {
    fn as_table_id(&self) -> TableId {
        self.table_id
    }
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
pub struct ViewCommit<'a> {
    pub warehouse_id: WarehouseId,
    pub namespace_id: NamespaceId,
    pub view_id: ViewId,
    pub view_ident: &'a TableIdent,
    pub new_metadata_location: &'a Location,
    pub previous_metadata_location: &'a Location,
    pub metadata: ViewMetadata,
    pub new_location: &'a Location,
}

#[derive(Debug, Clone)]
pub struct TableCreation<'c> {
    pub warehouse_id: WarehouseId,
    pub namespace_id: NamespaceId,
    pub table_ident: &'c TableIdent,
    pub metadata_location: Option<&'c Location>,
    pub table_metadata: TableMetadata,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DeletionDetails {
    pub expiration_task_id: uuid::Uuid,
    pub expiration_date: chrono::DateTime<chrono::Utc>,
    pub deleted_at: chrono::DateTime<chrono::Utc>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, PartialEq)]
pub struct TableInfo {
    pub table_ident: TableIdent,
    pub deletion_details: Option<DeletionDetails>,
    pub protected: bool,
}

#[derive(Debug, PartialEq)]
pub struct TabularInfo {
    pub table_ident: TabularIdentOwned,
    pub deletion_details: Option<DeletionDetails>,
    pub protected: bool,
}

impl TabularInfo {
    /// Verifies that `self` is a table before converting the `TabularInfo` into a `TableInfo`.
    ///
    /// # Errors
    /// If the `TabularInfo` is a view, this will return an error.
    pub fn into_table_info(self) -> Result<TableInfo> {
        Ok(TableInfo {
            table_ident: self.table_ident.into_table()?,
            deletion_details: self.deletion_details,
            protected: self.protected,
        })
    }

    /// Verifies that `self` is a view before converting the `TabularInfo` into a `TableInfo`.
    ///
    /// # Errors
    /// If the `TabularInfo` is a table, this will return an error.
    pub fn into_view_info(self) -> Result<TableInfo> {
        Ok(TableInfo {
            table_ident: self.table_ident.into_view()?,
            deletion_details: self.deletion_details,
            protected: self.protected,
        })
    }
}

#[derive(Debug, Clone)]
pub struct UndropTabularResponse {
    pub table_id: TableId,
    pub expiration_task_id: Option<TaskId>,
    pub name: String,
    pub namespace: NamespaceIdent,
}

#[derive(Debug, Clone)]
pub struct ViewMetadataWithLocation {
    pub metadata_location: String,
    pub metadata: ViewMetadata,
}
