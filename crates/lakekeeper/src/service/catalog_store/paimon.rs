use std::collections::HashMap;

use http::StatusCode;
use iceberg::{NamespaceIdent, TableIdent};
use iceberg_ext::catalog::rest::ErrorModel;
use lakekeeper_io::Location;
use serde::{Deserialize, Serialize};

use super::{
    BasicTabularInfo, CatalogStore, Transaction, define_simple_error, define_transparent_error,
    impl_error_stack_methods, impl_from_with_detail,
};
use crate::{
    WarehouseId,
    service::{
        CatalogBackendError, ConcurrentUpdateError, InternalParseLocationError,
        InvalidNamespaceIdentifier, LocationAlreadyTaken, LogicalSchema, NamespaceId,
        NamespaceVersion, ProtectedTabularDeletionWithoutForce, TableId, TabularId,
        WarehouseVersion,
    },
};

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    strum_macros::Display,
    strum_macros::EnumIter,
)]
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
#[cfg_attr(feature = "sqlx-postgres", derive(sqlx::Type))]
#[cfg_attr(
    feature = "sqlx-postgres",
    sqlx(type_name = "paimon_commit_state", rename_all = "kebab-case")
)]
pub enum PaimonCommitState {
    Stable,
    PendingPublish,
    PublishFailed,
}

#[derive(Debug, Clone)]
pub struct PaimonTableInfo {
    pub tabular_id: TableId,
    pub warehouse_id: WarehouseId,
    pub warehouse_version: WarehouseVersion,
    pub namespace_id: NamespaceId,
    pub namespace_version: NamespaceVersion,
    pub namespace_ident: NamespaceIdent,
    pub name: String,
    pub tabular_ident: TableIdent,
    pub location: Location,
    pub metadata_location: Option<Location>,
    pub protected: bool,
    pub current_snapshot_id: Option<i64>,
    pub current_branch: String,
    pub schema: Option<LogicalSchema>,
    pub table_options: HashMap<String, String>,
    pub partition_keys: Vec<String>,
    pub primary_keys: Vec<String>,
    pub comment: Option<String>,
    pub commit_state: PaimonCommitState,
    pub pending_commit_token: Option<uuid::Uuid>,
    pub last_commit_error: Option<String>,
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl BasicTabularInfo for PaimonTableInfo {
    fn warehouse_id(&self) -> WarehouseId {
        self.warehouse_id
    }

    fn warehouse_version(&self) -> WarehouseVersion {
        self.warehouse_version
    }

    fn tabular_ident(&self) -> &TableIdent {
        &self.tabular_ident
    }

    fn tabular_id(&self) -> TabularId {
        TabularId::Table(self.tabular_id)
    }

    fn namespace_id(&self) -> NamespaceId {
        self.namespace_id
    }

    fn namespace_version(&self) -> NamespaceVersion {
        self.namespace_version
    }
}

#[derive(Debug, Clone)]
pub struct PaimonTableListEntry {
    pub tabular_id: TableId,
    pub warehouse_id: WarehouseId,
    pub namespace_id: NamespaceId,
    pub namespace_ident: NamespaceIdent,
    pub name: String,
    pub tabular_ident: TableIdent,
    pub commit_state: PaimonCommitState,
    pub protected: bool,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone)]
pub struct PaimonTableCreation {
    pub tabular_id: TableId,
    pub warehouse_id: WarehouseId,
    pub namespace_id: NamespaceId,
    pub name: String,
    pub location: Location,
    pub metadata_location: Option<Location>,
    pub current_snapshot_id: Option<i64>,
    pub current_branch: String,
    pub schema: Option<LogicalSchema>,
    pub table_options: HashMap<String, String>,
    pub partition_keys: Vec<String>,
    pub primary_keys: Vec<String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub struct PaimonCommitResult {
    pub tabular_id: TableId,
    pub commit_token: uuid::Uuid,
    pub state: PaimonCommitState,
}

#[derive(Debug, Clone)]
pub struct PaimonCommitStateUpdate {
    pub commit_state: PaimonCommitState,
    pub pending_commit_token: Option<uuid::Uuid>,
    pub current_snapshot_id: Option<i64>,
    pub metadata_location: Option<Location>,
    pub last_commit_error: Option<String>,
}

define_simple_error!(PaimonTableAlreadyExists, "Paimon table already exists");
impl From<PaimonTableAlreadyExists> for ErrorModel {
    fn from(err: PaimonTableAlreadyExists) -> Self {
        ErrorModel::builder()
            .message(err.to_string())
            .r#type("PaimonTableAlreadyExists")
            .code(StatusCode::CONFLICT.as_u16())
            .stack(err.stack)
            .build()
    }
}

define_simple_error!(PaimonTableNotFound, "Paimon table not found");
impl From<PaimonTableNotFound> for ErrorModel {
    fn from(err: PaimonTableNotFound) -> Self {
        ErrorModel::builder()
            .message(err.to_string())
            .r#type("PaimonTableNotFound")
            .code(StatusCode::NOT_FOUND.as_u16())
            .stack(err.stack)
            .build()
    }
}

define_transparent_error! {
    pub enum CreatePaimonTableError,
    stack_message: "Error creating Paimon table",
    variants: [
        PaimonTableAlreadyExists,
        CatalogBackendError,
        InternalParseLocationError,
        LocationAlreadyTaken,
        InvalidNamespaceIdentifier,
    ]
}

define_transparent_error! {
    pub enum LoadPaimonTableError,
    stack_message: "Error loading Paimon table",
    variants: [
        PaimonTableNotFound,
        CatalogBackendError,
    ]
}

define_transparent_error! {
    pub enum ListPaimonTablesError,
    stack_message: "Error listing Paimon tables",
    variants: [
        CatalogBackendError,
    ]
}

define_transparent_error! {
    pub enum DropPaimonTableError,
    stack_message: "Error dropping Paimon table",
    variants: [
        PaimonTableNotFound,
        CatalogBackendError,
        InvalidNamespaceIdentifier,
        InternalParseLocationError,
        ProtectedTabularDeletionWithoutForce,
        ConcurrentUpdateError,
    ]
}

define_transparent_error! {
    pub enum UpdatePaimonCommitStateError,
    stack_message: "Error updating Paimon commit state",
    variants: [
        PaimonTableNotFound,
        CatalogBackendError,
    ]
}

#[async_trait::async_trait]
pub trait CatalogPaimonOps
where
    Self: CatalogStore,
{
    async fn create_paimon_table<'a>(
        creation: PaimonTableCreation,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<PaimonTableInfo, CreatePaimonTableError> {
        Self::create_paimon_table_impl(creation, transaction).await
    }

    async fn load_paimon_table<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        table_name: &str,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<PaimonTableInfo, LoadPaimonTableError> {
        Self::load_paimon_table_impl(warehouse_id, namespace_id, table_name, transaction).await
    }

    async fn load_paimon_table_by_id<'a>(
        warehouse_id: WarehouseId,
        tabular_id: TableId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<PaimonTableInfo, LoadPaimonTableError> {
        Self::load_paimon_table_by_id_impl(warehouse_id, tabular_id, transaction).await
    }

    async fn list_paimon_tables<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        namespace_ident: &NamespaceIdent,
        page_size: Option<i64>,
        page_token: Option<&str>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<(Vec<PaimonTableListEntry>, Option<String>), ListPaimonTablesError>
    {
        Self::list_paimon_tables_impl(
            warehouse_id,
            namespace_id,
            namespace_ident,
            page_size,
            page_token,
            transaction,
        )
        .await
    }

    async fn update_paimon_commit_state<'a>(
        warehouse_id: WarehouseId,
        tabular_id: TableId,
        update: PaimonCommitStateUpdate,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<PaimonTableInfo, UpdatePaimonCommitStateError> {
        Self::update_paimon_commit_state_impl(warehouse_id, tabular_id, update, transaction).await
    }

    async fn drop_paimon_table<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        table_name: &str,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<TableId, DropPaimonTableError> {
        Self::drop_paimon_table_impl(warehouse_id, namespace_id, table_name, transaction).await
    }
}

impl<T> CatalogPaimonOps for T where T: CatalogStore {}
