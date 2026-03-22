use std::collections::HashMap;

use http::StatusCode;
use iceberg::{NamespaceIdent, TableIdent};
use iceberg_ext::catalog::rest::ErrorModel;

use super::{
    BasicTabularInfo, TableId, define_simple_error, define_transparent_error,
    impl_error_stack_methods, impl_from_with_detail,
};
use crate::{
    WarehouseId,
    service::{
        CatalogBackendError, GenericTableId, NamespaceId, NamespaceVersion, TabularId,
        WarehouseVersion,
    },
};

#[derive(Debug, Clone)]
pub struct GenericTableInfo {
    pub generic_table_id: GenericTableId,
    pub warehouse_id: WarehouseId,
    pub warehouse_version: WarehouseVersion,
    pub namespace_id: NamespaceId,
    pub namespace_version: NamespaceVersion,
    pub namespace_ident: NamespaceIdent,
    pub name: String,
    pub format: String,
    pub base_location: String,
    pub doc: Option<String>,
    pub schema: Option<serde_json::Value>,
    pub statistics: Option<serde_json::Value>,
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct GenericTableCreation {
    pub namespace_id: NamespaceId,
    pub warehouse_id: WarehouseId,
    pub name: String,
    pub format: String,
    pub base_location: String,
    pub doc: Option<String>,
    pub schema: Option<serde_json::Value>,
    pub statistics: Option<serde_json::Value>,
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct GenericTableListEntry {
    pub generic_table_id: GenericTableId,
    pub name: String,
    pub format: String,
    pub namespace_ident: NamespaceIdent,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

// Wraps GenericTableInfo as BasicTabularInfo so generate_table_config() can vend
// STS credentials. Uses a synthetic TabularId::Table — TODO: add TabularId::GenericTable.
#[derive(Debug)]
pub struct GenericTableTabularBridge {
    pub warehouse_id: WarehouseId,
    pub warehouse_version: WarehouseVersion,
    pub namespace_id: NamespaceId,
    pub namespace_version: NamespaceVersion,
    pub tabular_ident: TableIdent,
    pub generic_table_id: GenericTableId,
}

impl GenericTableTabularBridge {
    #[must_use]
    pub fn from_info(info: &GenericTableInfo) -> Self {
        Self {
            warehouse_id: info.warehouse_id,
            warehouse_version: info.warehouse_version,
            namespace_id: info.namespace_id,
            namespace_version: info.namespace_version,
            tabular_ident: TableIdent {
                namespace: info.namespace_ident.clone(),
                name: info.name.clone(),
            },
            generic_table_id: info.generic_table_id,
        }
    }
}

impl BasicTabularInfo for GenericTableTabularBridge {
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
        TabularId::Table(TableId::from(self.generic_table_id.into_uuid()))
    }

    fn namespace_id(&self) -> NamespaceId {
        self.namespace_id
    }

    fn namespace_version(&self) -> NamespaceVersion {
        self.namespace_version
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
