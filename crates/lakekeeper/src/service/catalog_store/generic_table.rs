use std::{collections::HashMap, fmt};

use http::StatusCode;
use iceberg::{NamespaceIdent, TableIdent};
use iceberg_ext::catalog::rest::ErrorModel;
use lakekeeper_io::Location;
use serde::{Deserialize, Serialize};

use super::{
    AuthZGenericTableInfo, BasicTabularInfo, define_simple_error, define_transparent_error,
    impl_error_stack_methods, impl_from_with_detail,
};
use crate::{
    WarehouseId,
    service::{
        CatalogBackendError, GenericTableId, NamespaceId, NamespaceVersion, TabularId,
        WarehouseVersion,
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GenericTableFormat {
    Unknown(String),
}

impl GenericTableFormat {
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            GenericTableFormat::Unknown(s) => s,
        }
    }
}

impl fmt::Display for GenericTableFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<String> for GenericTableFormat {
    fn from(s: String) -> Self {
        GenericTableFormat::Unknown(s)
    }
}

impl From<&str> for GenericTableFormat {
    fn from(s: &str) -> Self {
        GenericTableFormat::Unknown(s.to_string())
    }
}

impl std::str::FromStr for GenericTableFormat {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(GenericTableFormat::Unknown(s.to_string()))
    }
}

impl Serialize for GenericTableFormat {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for GenericTableFormat {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Ok(GenericTableFormat::from(s))
    }
}

#[derive(Debug, Clone)]
pub struct GenericTableInfo {
    pub generic_table_id: GenericTableId,
    pub warehouse_id: WarehouseId,
    pub warehouse_version: WarehouseVersion,
    pub namespace_id: NamespaceId,
    pub namespace_version: NamespaceVersion,
    pub namespace_ident: NamespaceIdent,
    pub name: String,
    pub tabular_ident: TableIdent,
    pub location: Location,
    pub properties: HashMap<String, String>,
    pub protected: bool,
    pub format: GenericTableFormat,
    pub doc: Option<String>,
    pub schema: Option<serde_json::Value>,
    pub statistics: Option<serde_json::Value>,
}

impl BasicTabularInfo for GenericTableInfo {
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
        TabularId::GenericTable(self.generic_table_id)
    }

    fn namespace_id(&self) -> NamespaceId {
        self.namespace_id
    }

    fn namespace_version(&self) -> NamespaceVersion {
        self.namespace_version
    }
}

impl AuthZGenericTableInfo for GenericTableInfo {
    fn warehouse_id(&self) -> WarehouseId {
        self.warehouse_id
    }

    fn generic_table_ident(&self) -> &TableIdent {
        &self.tabular_ident
    }

    fn generic_table_id(&self) -> GenericTableId {
        self.generic_table_id
    }

    fn namespace_id(&self) -> NamespaceId {
        self.namespace_id
    }

    fn is_protected(&self) -> bool {
        self.protected
    }

    fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }
}

#[derive(Debug, Clone)]
pub struct GenericTableCreation {
    pub generic_table_id: GenericTableId,
    pub namespace_id: NamespaceId,
    pub warehouse_id: WarehouseId,
    pub name: String,
    pub format: GenericTableFormat,
    pub location: Location,
    pub doc: Option<String>,
    pub schema: Option<serde_json::Value>,
    pub statistics: Option<serde_json::Value>,
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct GenericTableListEntry {
    pub generic_table_id: GenericTableId,
    pub warehouse_id: WarehouseId,
    pub namespace_id: NamespaceId,
    pub name: String,
    pub tabular_ident: TableIdent,
    pub format: GenericTableFormat,
    pub namespace_ident: NamespaceIdent,
    pub protected: bool,
    pub properties: HashMap<String, String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl AuthZGenericTableInfo for GenericTableListEntry {
    fn warehouse_id(&self) -> WarehouseId {
        self.warehouse_id
    }

    fn generic_table_ident(&self) -> &TableIdent {
        &self.tabular_ident
    }

    fn generic_table_id(&self) -> GenericTableId {
        self.generic_table_id
    }

    fn namespace_id(&self) -> NamespaceId {
        self.namespace_id
    }

    fn is_protected(&self) -> bool {
        self.protected
    }

    fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }
}

define_simple_error!(GenericTableAlreadyExists, "Generic table already exists");
impl From<GenericTableAlreadyExists> for ErrorModel {
    fn from(err: GenericTableAlreadyExists) -> Self {
        ErrorModel::builder()
            .message(err.to_string())
            .r#type("GenericTableAlreadyExists")
            .code(StatusCode::CONFLICT.as_u16())
            .stack(err.stack)
            .build()
    }
}

define_simple_error!(GenericTableNotFound, "Generic table not found");
impl From<GenericTableNotFound> for ErrorModel {
    fn from(err: GenericTableNotFound) -> Self {
        ErrorModel::builder()
            .message(err.to_string())
            .r#type("GenericTableNotFound")
            .code(StatusCode::NOT_FOUND.as_u16())
            .stack(err.stack)
            .build()
    }
}

define_transparent_error! {
    pub enum CreateGenericTableError,
    stack_message: "Error creating generic table",
    variants: [
        GenericTableAlreadyExists,
        CatalogBackendError,
    ]
}

define_transparent_error! {
    pub enum LoadGenericTableError,
    stack_message: "Error loading generic table",
    variants: [
        GenericTableNotFound,
        CatalogBackendError,
    ]
}

define_transparent_error! {
    pub enum ListGenericTablesError,
    stack_message: "Error listing generic tables",
    variants: [
        CatalogBackendError,
    ]
}

define_transparent_error! {
    pub enum DropGenericTableError,
    stack_message: "Error dropping generic table",
    variants: [
        GenericTableNotFound,
        CatalogBackendError,
    ]
}

use super::{CatalogStore, Transaction};

#[async_trait::async_trait]
pub trait CatalogGenericTableOps
where
    Self: CatalogStore,
{
    async fn create_generic_table<'a>(
        creation: GenericTableCreation,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<GenericTableInfo, CreateGenericTableError> {
        Self::create_generic_table_impl(creation, transaction).await
    }

    async fn load_generic_table<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        table_name: &str,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<GenericTableInfo, LoadGenericTableError> {
        Self::load_generic_table_impl(warehouse_id, namespace_id, table_name, transaction).await
    }

    async fn list_generic_tables<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        namespace_ident: &iceberg::NamespaceIdent,
        page_size: Option<i64>,
        page_token: Option<&str>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<(Vec<GenericTableListEntry>, Option<String>), ListGenericTablesError>
    {
        Self::list_generic_tables_impl(
            warehouse_id,
            namespace_id,
            namespace_ident,
            page_size,
            page_token,
            transaction,
        )
        .await
    }

    async fn drop_generic_table<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        table_name: &str,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<GenericTableId, DropGenericTableError> {
        Self::drop_generic_table_impl(warehouse_id, namespace_id, table_name, transaction).await
    }
}

impl<T> CatalogGenericTableOps for T where T: CatalogStore {}
