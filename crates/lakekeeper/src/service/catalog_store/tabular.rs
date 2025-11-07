use std::collections::HashMap;

use http::StatusCode;
use iceberg::TableIdent;
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
use lakekeeper_io::{Location, LocationParseError};

use crate::{
    api::{
        iceberg::v1::{PaginatedMapping, PaginationQuery},
        management::v1::TabularType,
    },
    service::{
        authz::ActionOnTableOrView, define_simple_error, define_transparent_error,
        impl_error_stack_methods, impl_from_with_detail, tasks::TaskId, CatalogBackendError,
        CatalogStore, InvalidNamespaceIdentifier, InvalidPaginationToken, NamespaceId,
        NamespaceVersion, Result, TableId, TabularId, TabularIdentBorrowed, TabularIdentOwned,
        Transaction, ViewId, WarehouseVersion,
    },
    WarehouseId,
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

    #[must_use]
    pub fn active_and_staged() -> Self {
        Self {
            include_staged: true,
            include_deleted: false,
            include_active: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ExpirationTaskInfo {
    pub task_id: TaskId,
    pub expiration_date: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TabularInfo<T: std::fmt::Debug + PartialEq + Copy> {
    pub warehouse_id: WarehouseId,
    pub namespace_id: NamespaceId,
    pub namespace_version: NamespaceVersion,
    pub warehouse_version: WarehouseVersion,
    pub tabular_ident: TableIdent, // Not used to determine type
    pub tabular_id: T,             // Contains type info
    pub location: Location,
    pub metadata_location: Option<Location>,
    pub protected: bool,
    pub properties: HashMap<String, String>,
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}
impl BasicTabularInfo for TableInfo {
    fn warehouse_id(&self) -> WarehouseId {
        self.warehouse_id
    }

    fn tabular_ident(&self) -> &TableIdent {
        &self.tabular_ident
    }

    fn tabular_id(&self) -> TabularId {
        self.tabular_id.into()
    }

    fn namespace_id(&self) -> NamespaceId {
        self.namespace_id
    }
}
impl BasicTabularInfo for ViewInfo {
    fn warehouse_id(&self) -> WarehouseId {
        self.warehouse_id
    }

    fn tabular_ident(&self) -> &TableIdent {
        &self.tabular_ident
    }

    fn tabular_id(&self) -> TabularId {
        self.tabular_id.into()
    }

    fn namespace_id(&self) -> NamespaceId {
        self.namespace_id
    }
}
#[derive(Debug, Clone, PartialEq, derive_more::From)]
pub enum ViewOrTableInfo {
    Table(TableInfo),
    View(ViewInfo),
}
pub type TableInfo = TabularInfo<TableId>;
pub type ViewInfo = TabularInfo<ViewId>;

#[derive(thiserror::Error, Debug, PartialEq)]
#[error("{source}")]
pub struct InternalParseLocationError {
    #[source]
    source: LocationParseError,
    stack: Vec<String>,
}
impl_error_stack_methods!(InternalParseLocationError);
impl From<LocationParseError> for InternalParseLocationError {
    fn from(source: LocationParseError) -> Self {
        Self {
            source,
            stack: Vec::new(),
        }
    }
}
impl From<InternalParseLocationError> for ErrorModel {
    fn from(err: InternalParseLocationError) -> Self {
        ErrorModel {
            code: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
            r#type: "InternalParseLocationError".to_string(),
            message: err.to_string(),
            stack: err.stack,
            source: None,
        }
    }
}
impl From<InternalParseLocationError> for IcebergErrorResponse {
    fn from(err: InternalParseLocationError) -> Self {
        IcebergErrorResponse {
            error: ErrorModel::from(err),
        }
    }
}

#[cfg(feature = "sqlx-postgres")]
pub(crate) fn build_tabular_ident_from_vec(
    name_parts: &[String],
) -> Result<TableIdent, InvalidTabularIdentifier> {
    TableIdent::from_strs(name_parts.to_owned()).map_err(|e| {
        let external_err = InvalidTabularIdentifier::new();
        tracing::error!("{external_err}: {e} - name parts: {:?}", name_parts);
        InvalidTabularIdentifier::new()
    })
}

define_simple_error!(
    InvalidTabularIdentifier,
    "Encountered invalid tabular identifier in catalog response"
);
impl From<InvalidTabularIdentifier> for ErrorModel {
    fn from(err: InvalidTabularIdentifier) -> Self {
        ErrorModel {
            code: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
            r#type: "InvalidTabularIdentifier".to_string(),
            message: err.to_string(),
            stack: err.stack,
            source: None,
        }
    }
}
impl From<InvalidTabularIdentifier> for IcebergErrorResponse {
    fn from(err: InvalidTabularIdentifier) -> Self {
        IcebergErrorResponse {
            error: ErrorModel::from(err),
        }
    }
}

impl ViewOrTableInfo {
    #[must_use]
    pub fn tabular_id(&self) -> TabularId {
        match self {
            Self::Table(info) => TabularId::Table(info.tabular_id),
            Self::View(info) => TabularId::View(info.tabular_id),
        }
    }

    #[must_use]
    pub fn tabular_ident(&self) -> &TableIdent {
        match self {
            Self::Table(info) => &info.tabular_ident,
            Self::View(info) => &info.tabular_ident,
        }
    }

    #[must_use]
    pub fn namespace_id(&self) -> NamespaceId {
        match self {
            Self::Table(info) => info.namespace_id,
            Self::View(info) => info.namespace_id,
        }
    }

    #[must_use]
    pub fn protected(&self) -> bool {
        match self {
            Self::Table(info) => info.protected,
            Self::View(info) => info.protected,
        }
    }

    #[must_use]
    pub fn updated_at(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        match self {
            Self::Table(info) => info.updated_at,
            Self::View(info) => info.updated_at,
        }
    }

    pub fn as_action_request<AV, AT>(
        &self,
        view_action: AV,
        table_action: AT,
    ) -> ActionOnTableOrView<'_, TableInfo, ViewInfo, AT, AV> {
        match self {
            Self::View(view) => ActionOnTableOrView::View((view, view_action)),
            Self::Table(table) => ActionOnTableOrView::Table((table, table_action)),
        }
    }

    #[must_use]
    pub fn metadata_location(&self) -> Option<&Location> {
        match self {
            Self::Table(info) => info.metadata_location.as_ref(),
            Self::View(info) => info.metadata_location.as_ref(),
        }
    }

    #[must_use]
    pub fn location(&self) -> &Location {
        match self {
            Self::Table(info) => &info.location,
            Self::View(info) => &info.location,
        }
    }
}

#[cfg(test)]
impl TableInfo {
    pub(crate) fn new_random() -> Self {
        use std::str::FromStr;

        let table_id = TableId::new_random();
        let tabular_ident = TableIdent::new(
            iceberg::NamespaceIdent::new("test".to_string()),
            format!("table_{table_id}"),
        );
        let location =
            Location::from_str(&format!("s3://bucket/path/to/table_{table_id}")).unwrap();
        TableInfo {
            warehouse_id: WarehouseId::new_random(),
            namespace_id: NamespaceId::new_random(),
            namespace_version: 0.into(),
            warehouse_version: 0.into(),
            tabular_ident,
            tabular_id: table_id,
            metadata_location: Some(
                Location::from_str(&format!("{location}/metadata/metadata.json")).unwrap(),
            ),
            location,
            protected: false,
            updated_at: Some(chrono::Utc::now()),
            properties: HashMap::new(),
        }
    }
}

#[cfg(test)]
impl ViewInfo {
    pub(crate) fn new_random() -> Self {
        use std::str::FromStr;

        let view_id = ViewId::new_random();
        let tabular_ident = TableIdent::new(
            iceberg::NamespaceIdent::new("test".to_string()),
            format!("table_{view_id}"),
        );
        let location = Location::from_str(&format!("s3://bucket/path/to/view_{view_id}")).unwrap();
        ViewInfo {
            warehouse_id: WarehouseId::new_random(),
            namespace_id: NamespaceId::new_random(),
            namespace_version: 0.into(),
            warehouse_version: 0.into(),
            tabular_ident,
            tabular_id: view_id,
            metadata_location: Some(
                Location::from_str(&format!("{location}/metadata/metadata.json")).unwrap(),
            ),
            location,
            protected: false,
            updated_at: Some(chrono::Utc::now()),
            properties: HashMap::new(),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableNamed {
    pub warehouse_id: WarehouseId,
    pub table_ident: TableIdent,
    pub table_id: TableId,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewNamed {
    pub warehouse_id: WarehouseId,
    pub view_ident: TableIdent,
    pub view_id: ViewId,
}
pub trait AuthZTableInfo: Send + Sync {
    fn warehouse_id(&self) -> WarehouseId;
    fn table_ident(&self) -> &TableIdent;
    fn table_id(&self) -> TableId;
    fn namespace_id(&self) -> NamespaceId;
}
pub trait AuthZViewInfo: Send + Sync {
    fn warehouse_id(&self) -> WarehouseId;
    fn view_ident(&self) -> &TableIdent;
    fn view_id(&self) -> ViewId;
    fn namespace_id(&self) -> NamespaceId;
}

impl AuthZTableInfo for TableInfo {
    fn warehouse_id(&self) -> WarehouseId {
        self.warehouse_id
    }
    fn table_ident(&self) -> &TableIdent {
        &self.tabular_ident
    }
    fn table_id(&self) -> TableId {
        self.tabular_id
    }
    fn namespace_id(&self) -> NamespaceId {
        self.namespace_id
    }
}

impl AuthZTableInfo for TableDeletionInfo {
    fn warehouse_id(&self) -> WarehouseId {
        self.tabular.warehouse_id
    }
    fn table_ident(&self) -> &TableIdent {
        &self.tabular.tabular_ident
    }
    fn table_id(&self) -> TableId {
        self.tabular.tabular_id
    }
    fn namespace_id(&self) -> NamespaceId {
        self.tabular.namespace_id
    }
}

impl AuthZViewInfo for ViewInfo {
    fn warehouse_id(&self) -> WarehouseId {
        self.warehouse_id
    }
    fn view_ident(&self) -> &TableIdent {
        &self.tabular_ident
    }
    fn view_id(&self) -> ViewId {
        self.tabular_id
    }
    fn namespace_id(&self) -> NamespaceId {
        self.namespace_id
    }
}

impl AuthZViewInfo for ViewDeletionInfo {
    fn warehouse_id(&self) -> WarehouseId {
        self.tabular.warehouse_id
    }
    fn view_ident(&self) -> &TableIdent {
        &self.tabular.tabular_ident
    }
    fn view_id(&self) -> ViewId {
        self.tabular.tabular_id
    }
    fn namespace_id(&self) -> NamespaceId {
        self.tabular.namespace_id
    }
}

pub trait BasicTabularInfo: Send + Sync {
    fn warehouse_id(&self) -> WarehouseId;
    fn tabular_ident(&self) -> &TableIdent;
    fn tabular_id(&self) -> TabularId;
    fn namespace_id(&self) -> NamespaceId;
}

impl BasicTabularInfo for ViewOrTableInfo {
    fn namespace_id(&self) -> NamespaceId {
        match self {
            Self::Table(info) => info.namespace_id,
            Self::View(info) => info.namespace_id,
        }
    }
    fn warehouse_id(&self) -> WarehouseId {
        match self {
            Self::Table(info) => info.warehouse_id,
            Self::View(info) => info.warehouse_id,
        }
    }
    fn tabular_ident(&self) -> &TableIdent {
        match self {
            Self::Table(info) => &info.tabular_ident,
            Self::View(info) => &info.tabular_ident,
        }
    }
    fn tabular_id(&self) -> TabularId {
        match self {
            Self::Table(info) => TabularId::Table(info.tabular_id),
            Self::View(info) => TabularId::View(info.tabular_id),
        }
    }
}
impl BasicTabularInfo for ViewOrTableDeletionInfo {
    fn warehouse_id(&self) -> WarehouseId {
        match self {
            Self::Table(info) => info.tabular.warehouse_id,
            Self::View(info) => info.tabular.warehouse_id,
        }
    }
    fn tabular_ident(&self) -> &TableIdent {
        match self {
            Self::Table(info) => &info.tabular.tabular_ident,
            Self::View(info) => &info.tabular.tabular_ident,
        }
    }
    fn tabular_id(&self) -> TabularId {
        match self {
            Self::Table(info) => TabularId::Table(info.tabular.tabular_id),
            Self::View(info) => TabularId::View(info.tabular.tabular_id),
        }
    }
    fn namespace_id(&self) -> NamespaceId {
        match self {
            Self::Table(info) => info.tabular.namespace_id,
            Self::View(info) => info.tabular.namespace_id,
        }
    }
}

impl ViewOrTableInfo {
    #[must_use]
    pub fn into_table_info(self) -> Option<TableInfo> {
        match self {
            Self::Table(info) => Some(info),
            Self::View(_) => None,
        }
    }

    #[must_use]
    pub fn into_view_info(self) -> Option<ViewInfo> {
        match self {
            Self::View(info) => Some(info),
            Self::Table(_) => None,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct TabularDeletionInfo<T: std::fmt::Debug + PartialEq + Copy> {
    pub tabular: TabularInfo<T>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub deleted_at: Option<chrono::DateTime<chrono::Utc>>,
    pub expiration_task: Option<ExpirationTaskInfo>,
}
#[derive(Debug, PartialEq, derive_more::From)]
pub enum ViewOrTableDeletionInfo {
    Table(TableDeletionInfo),
    View(ViewDeletionInfo),
}
pub type TableDeletionInfo = TabularDeletionInfo<TableId>;
pub type ViewDeletionInfo = TabularDeletionInfo<ViewId>;

impl ViewOrTableDeletionInfo {
    #[must_use]
    pub fn into_table_or_view_info(self) -> ViewOrTableInfo {
        match self {
            Self::Table(info) => ViewOrTableInfo::Table(info.tabular),
            Self::View(info) => ViewOrTableInfo::View(info.tabular),
        }
    }

    #[must_use]
    pub fn namespace_id(&self) -> NamespaceId {
        match self {
            Self::Table(info) => info.tabular.namespace_id,
            Self::View(info) => info.tabular.namespace_id,
        }
    }

    #[must_use]
    pub fn tabular_id(&self) -> TabularId {
        match self {
            Self::Table(info) => TabularId::Table(info.tabular.tabular_id),
            Self::View(info) => TabularId::View(info.tabular.tabular_id),
        }
    }

    #[must_use]
    pub fn deleted_at(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        match self {
            Self::Table(info) => info.deleted_at,
            Self::View(info) => info.deleted_at,
        }
    }

    #[must_use]
    pub fn created_at(&self) -> chrono::DateTime<chrono::Utc> {
        match self {
            Self::Table(info) => info.created_at,
            Self::View(info) => info.created_at,
        }
    }

    #[must_use]
    pub fn expiration_task(&self) -> Option<&ExpirationTaskInfo> {
        match self {
            Self::Table(info) => info.expiration_task.as_ref(),
            Self::View(info) => info.expiration_task.as_ref(),
        }
    }

    #[must_use]
    pub fn tabular_ident(&self) -> &TableIdent {
        match self {
            Self::Table(info) => &info.tabular.tabular_ident,
            Self::View(info) => &info.tabular.tabular_ident,
        }
    }

    #[must_use]
    pub fn into_table_info(self) -> Option<TableDeletionInfo> {
        match self {
            Self::Table(info) => Some(info),
            Self::View(_) => None,
        }
    }

    #[must_use]
    pub fn into_view_info(self) -> Option<ViewDeletionInfo> {
        match self {
            Self::View(info) => Some(info),
            Self::Table(_) => None,
        }
    }

    pub fn as_action_request<AV, AT>(
        &self,
        view_action: AV,
        table_action: AT,
    ) -> ActionOnTableOrView<'_, TableDeletionInfo, ViewDeletionInfo, AT, AV> {
        match self {
            Self::View(view) => ActionOnTableOrView::View((view, view_action)),
            Self::Table(table) => ActionOnTableOrView::Table((table, table_action)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CatalogSearchTabularInfo {
    pub tabular: ViewOrTableInfo,
    pub distance: Option<f32>,
}

impl BasicTabularInfo for CatalogSearchTabularInfo {
    fn warehouse_id(&self) -> WarehouseId {
        self.tabular.warehouse_id()
    }
    fn tabular_ident(&self) -> &TableIdent {
        self.tabular.tabular_ident()
    }
    fn tabular_id(&self) -> TabularId {
        self.tabular.tabular_id()
    }
    fn namespace_id(&self) -> NamespaceId {
        self.tabular.namespace_id()
    }
}

#[derive(Debug, Clone)]
pub struct CatalogSearchTabularResponse {
    pub search_results: Vec<CatalogSearchTabularInfo>,
}

// #[derive(Debug, Clone)]
// pub struct UndropTabularResponse {
//     pub table_id: TableId,
//     pub expiration_task_id: Option<TaskId>,
//     pub name: String,
//     pub namespace: NamespaceIdent,
// }

macro_rules! define_ident_or_id {
    ($enum_name:ident, $id_type:ty, $tabular_variant:ident) => {
        #[derive(Hash, Debug, Clone, PartialEq, Eq, derive_more::From)]
        pub enum $enum_name {
            Ident(TableIdent),
            Id($id_type),
        }

        impl From<$enum_name> for TabularIdentOrId {
            fn from(value: $enum_name) -> Self {
                match value {
                    $enum_name::Ident(ident) => {
                        TabularIdentOrId::Ident(TabularIdentOwned::$tabular_variant(ident))
                    }
                    $enum_name::Id(id) => TabularIdentOrId::Id(TabularId::$tabular_variant(id)),
                }
            }
        }

        impl std::fmt::Display for $enum_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    $enum_name::Ident(ident) => write!(f, "'{}'", ident),
                    $enum_name::Id(id) => write!(f, "ID '{}'", id),
                }
            }
        }
    };
}
define_ident_or_id!(TableIdentOrId, TableId, Table);
define_ident_or_id!(ViewIdentOrId, ViewId, View);

#[derive(Hash, Debug, Clone, PartialEq, Eq, derive_more::From)]
pub enum TabularIdentOrId {
    Ident(TabularIdentOwned),
    Id(TabularId),
}
impl TabularIdentOrId {
    #[must_use]
    pub fn is_table(&self) -> bool {
        matches!(
            self,
            TabularIdentOrId::Ident(TabularIdentOwned::Table(_))
                | TabularIdentOrId::Id(TabularId::Table(_))
        )
    }

    #[must_use]
    pub fn is_view(&self) -> bool {
        matches!(
            self,
            TabularIdentOrId::Ident(TabularIdentOwned::View(_))
                | TabularIdentOrId::Id(TabularId::View(_))
        )
    }

    #[must_use]
    pub fn type_str(&self) -> &'static str {
        if self.is_table() {
            "table"
        } else {
            "view"
        }
    }
}
impl std::fmt::Display for TabularIdentOrId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TabularIdentOrId::Ident(ident) => match ident {
                TabularIdentOwned::Table(t) => write!(f, "Table '{t}'"),
                TabularIdentOwned::View(v) => write!(f, "View '{v}'"),
            },
            TabularIdentOrId::Id(id) => match id {
                TabularId::Table(t) => write!(f, "Table ID '{t}'"),
                TabularId::View(v) => write!(f, "View ID '{v}'"),
            },
        }
    }
}
impl From<TableId> for TabularIdentOrId {
    fn from(value: TableId) -> Self {
        Self::Id(TabularId::Table(value))
    }
}
impl From<ViewId> for TabularIdentOrId {
    fn from(value: ViewId) -> Self {
        Self::Id(TabularId::View(value))
    }
}

macro_rules! define_simple_tabular_err {
    ($error_name:ident, $error_message:literal) => {
        #[derive(thiserror::Error, Debug)]
        #[error($error_message)]
        pub struct $error_name {
            warehouse_id: $crate::WarehouseId,
            tabular: $crate::service::TabularIdentOrId,
            stack: Vec<String>,
        }

        impl $error_name {
            #[must_use]
            pub fn new(
                warehouse_id: $crate::WarehouseId,
                tabular: impl Into<$crate::service::TabularIdentOrId>,
            ) -> Self {
                Self {
                    warehouse_id,
                    tabular: tabular.into(),
                    stack: Vec::new(),
                }
            }

            #[must_use]
            pub fn warehouse_id(&self) -> $crate::WarehouseId {
                self.warehouse_id
            }

            #[must_use]
            pub fn tabular(&self) -> &$crate::service::TabularIdentOrId {
                &self.tabular
            }

            #[must_use]
            pub fn is_table(&self) -> bool {
                self.tabular().is_table()
            }

            #[must_use]
            pub fn is_view(&self) -> bool {
                self.tabular().is_view()
            }
        }

        impl_error_stack_methods!($error_name);
    };
}
pub(crate) use define_simple_tabular_err;

define_simple_error!(
    TabularAlreadyExists,
    "Tabular with the same name already exists in the namespace"
);
impl From<TabularAlreadyExists> for ErrorModel {
    fn from(err: TabularAlreadyExists) -> Self {
        ErrorModel {
            code: StatusCode::CONFLICT.as_u16(),
            r#type: "AlreadyExistsException".to_string(),
            message: err.to_string(),
            stack: err.stack,
            source: None,
        }
    }
}

define_simple_tabular_err!(
    ProtectedTabularDeletionWithoutForce,
    "Cannot delete protected tabular {tabular} without force flag"
);
impl From<ProtectedTabularDeletionWithoutForce> for ErrorModel {
    fn from(err: ProtectedTabularDeletionWithoutForce) -> Self {
        ErrorModel {
            code: StatusCode::CONFLICT.as_u16(),
            r#type: "ProtectedTabularDeletionWithoutForce".to_string(),
            message: err.to_string(),
            stack: err.stack,
            source: None,
        }
    }
}

pub const CONCURRENT_UPDATE_ERROR_TYPE: &str = "ConcurrentUpdateError";
define_simple_tabular_err!(
    ConcurrentUpdateError,
    "Tabular {tabular} was concurrently updated"
);
impl From<ConcurrentUpdateError> for ErrorModel {
    fn from(err: ConcurrentUpdateError) -> Self {
        ErrorModel {
            code: StatusCode::CONFLICT.as_u16(),
            r#type: CONCURRENT_UPDATE_ERROR_TYPE.to_string(),
            message: err.to_string(),
            stack: err.stack,
            source: None,
        }
    }
}

define_simple_tabular_err!(TabularNotFound, "Error getting tabular from catalog");
impl From<TabularNotFound> for ErrorModel {
    fn from(err: TabularNotFound) -> Self {
        let t = if err.is_view() {
            "NoSuchViewException"
        } else {
            "NoSuchTableException"
        };

        ErrorModel {
            code: StatusCode::NOT_FOUND.as_u16(),
            r#type: t.to_string(),
            message: err.to_string(),
            stack: err.stack,
            source: None,
        }
    }
}
impl From<TabularNotFound> for IcebergErrorResponse {
    fn from(err: TabularNotFound) -> Self {
        IcebergErrorResponse {
            error: ErrorModel::from(err),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Error serializing {entity}: {source}")]
pub struct SerializationError {
    entity: String,
    stack: Vec<String>,
    #[source]
    source: Box<serde_json::Error>,
}
impl_error_stack_methods!(SerializationError);
impl SerializationError {
    #[must_use]
    pub fn new(entity: impl Into<String>, source: serde_json::Error) -> Self {
        Self {
            entity: entity.into(),
            stack: Vec::new(),
            source: Box::new(source),
        }
    }
}
impl From<SerializationError> for ErrorModel {
    fn from(err: SerializationError) -> Self {
        ErrorModel {
            code: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
            r#type: "SerializationError".to_string(),
            message: err.to_string(),
            stack: err.stack,
            source: Some(err.source),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{message}: {source}")]
pub struct ConversionError {
    message: String,
    stack: Vec<String>,
    #[source]
    source: Box<dyn std::error::Error + Send + Sync>,
    is_external: bool,
}
impl_error_stack_methods!(ConversionError);
impl ConversionError {
    #[must_use]
    pub fn new(
        message: impl Into<String>,
        source: impl Into<Box<dyn std::error::Error + Send + Sync>>,
    ) -> Self {
        Self {
            message: message.into(),
            stack: Vec::new(),
            source: source.into(),
            is_external: false,
        }
    }

    #[must_use]
    pub fn new_external(
        message: impl Into<String>,
        source: impl Into<Box<dyn std::error::Error + Send + Sync>>,
    ) -> Self {
        Self {
            message: message.into(),
            stack: Vec::new(),
            source: source.into(),
            is_external: true,
        }
    }
}
impl From<ConversionError> for ErrorModel {
    fn from(err: ConversionError) -> Self {
        let code = if err.is_external {
            StatusCode::BAD_REQUEST
        } else {
            StatusCode::INTERNAL_SERVER_ERROR
        };
        ErrorModel {
            code: code.as_u16(),
            r#type: "ConversionError".to_string(),
            message: err.to_string(),
            stack: err.stack,
            source: Some(err.source),
        }
    }
}

#[derive(Debug, derive_more::From)]
pub enum InternalBackendErrors {
    SerializationError(SerializationError),
    CatalogBackendError(CatalogBackendError),
    InternalConversionError(ConversionError),
}

// --------------------------- Set Tabular Protection---------------------------
define_transparent_error! {
    pub enum SetTabularProtectionError,
    stack_message: "Error setting tabular protection in catalog",
    variants: [
        CatalogBackendError,
        TabularNotFound,
        InvalidNamespaceIdentifier,
        InternalParseLocationError
    ]
}

// --------------------------- List Tabulars ---------------------------
define_simple_tabular_err!(
    ViewInTableList,
    "Catalog returned a view when filtering for tables"
);
impl From<ViewInTableList> for ErrorModel {
    fn from(err: ViewInTableList) -> Self {
        ErrorModel {
            message: err.to_string(),
            r#type: "ViewInTableList".to_string(),
            code: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
            source: None,
            stack: err.stack,
        }
    }
}
define_simple_tabular_err!(
    TableInViewList,
    "Catalog returned a table when filtering for views"
);
impl From<TableInViewList> for ErrorModel {
    fn from(err: TableInViewList) -> Self {
        ErrorModel {
            message: err.to_string(),
            r#type: "TableInViewList".to_string(),
            code: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
            source: None,
            stack: err.stack,
        }
    }
}

define_transparent_error! {
    pub enum ListTabularsError,
    stack_message: "Error listing tabulars in catalog",
    variants: [
        CatalogBackendError,
        InvalidPaginationToken,
        InvalidNamespaceIdentifier,
        InternalParseLocationError
    ]
}

define_transparent_error! {
    pub enum ListTablesError,
    stack_message: "Error listing tables in catalog",
    variants: [
        ListTabularsError,
        ViewInTableList
    ]
}

define_transparent_error! {
    pub enum ListViewsError,
    stack_message: "Error listing tables in catalog",
    variants: [
        ListTabularsError,
        TableInViewList
    ]
}

// --------------------------- Get Tabulars ---------------------------
define_transparent_error! {
    pub enum GetTabularInfoError,
    stack_message: "Error getting tabular in catalog",
    variants: [
        CatalogBackendError,
        SerializationError,
        InvalidNamespaceIdentifier,
        UnexpectedTabularInResponse,
        InternalParseLocationError
    ]
}

define_transparent_error! {
    pub enum GetTabularInfoByLocationError,
    stack_message: "Error getting tabular by location in catalog",
    variants: [
        CatalogBackendError,
        SerializationError,
        InvalidNamespaceIdentifier,
        UnexpectedTabularInResponse,
        InternalParseLocationError
    ]
}

define_simple_error!(
    UnexpectedTabularInResponse,
    "Catalog response includes unexpected tabulars that where not requested"
);

impl From<UnexpectedTabularInResponse> for ErrorModel {
    fn from(err: UnexpectedTabularInResponse) -> Self {
        ErrorModel {
            message: err.to_string(),
            r#type: "UnexpectedTabularInResponse".to_string(),
            code: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
            source: None,
            stack: err.stack,
        }
    }
}

// --------------------------- Search Tabulars ----------------
define_transparent_error! {
    pub enum SearchTabularError,
    stack_message: "Error searching tabular in catalog",
    variants: [
        CatalogBackendError,
        InvalidNamespaceIdentifier,
        InternalParseLocationError
    ]
}

// --------------------------- Rename Tabulars ----------------
define_transparent_error! {
    pub enum RenameTabularError,
    stack_message: "Error renaming tabular in catalog",
    variants: [
        CatalogBackendError,
        InvalidNamespaceIdentifier,
        InternalParseLocationError,
        TabularNotFound
    ]
}

// --------------------------- Undrop Tabulars ----------------
define_transparent_error! {
    pub enum ClearTabularDeletedAtError,
    stack_message: "Error removing soft-deletion marker from tabular in catalog",
    variants: [
        CatalogBackendError,
        InvalidNamespaceIdentifier,
        InternalParseLocationError,
        TabularAlreadyExists,
        TabularNotFound
    ]
}

// --------------------------- Soft-Delete Tabulars ----------------
define_transparent_error! {
    pub enum MarkTabularAsDeletedError,
    stack_message: "Error marking tabular as soft-deleted in catalog",
    variants: [
        CatalogBackendError,
        InvalidNamespaceIdentifier,
        InternalParseLocationError,
        TabularNotFound,
        ProtectedTabularDeletionWithoutForce
    ]
}

// --------------------------- Drop Tabulars ----------------
define_transparent_error! {
    pub enum DropTabularError,
    stack_message: "Error dropping tabular in catalog",
    variants: [
        CatalogBackendError,
        InvalidNamespaceIdentifier,
        InternalParseLocationError,
        TabularNotFound,
        ProtectedTabularDeletionWithoutForce,
        ConcurrentUpdateError
    ]
}

// --------------------------- Create Tabulars ----------------
#[derive(thiserror::Error, Debug)]
#[error("Location '{location}' is already taken by another table or view.")]
pub struct LocationAlreadyTaken {
    location: Location,
    stack: Vec<String>,
}
impl LocationAlreadyTaken {
    #[must_use]
    pub fn new(location: Location) -> Self {
        Self {
            location,
            stack: Vec::new(),
        }
    }
}
impl_error_stack_methods!(LocationAlreadyTaken);
impl From<LocationAlreadyTaken> for ErrorModel {
    fn from(err: LocationAlreadyTaken) -> Self {
        ErrorModel {
            code: StatusCode::CONFLICT.as_u16(),
            r#type: "LocationAlreadyTaken".to_string(),
            message: err.to_string(),
            stack: err.stack,
            source: None,
        }
    }
}

define_transparent_error! {
    pub enum CreateTabularError,
    stack_message: "Error creating tabular in catalog",
    variants: [
        CatalogBackendError,
        InternalParseLocationError,
        LocationAlreadyTaken,
        InvalidNamespaceIdentifier,
        TabularAlreadyExists
    ]
}

#[async_trait::async_trait]
pub trait CatalogTabularOps
where
    Self: CatalogStore,
{
    /// Drops staged and non-staged tables and views.
    ///
    /// Returns the table location
    async fn drop_tabular<'a>(
        warehouse_id: WarehouseId,
        tabular_id: impl Into<TabularId> + Send,
        force: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<Location, DropTabularError> {
        Self::drop_tabular_impl(warehouse_id, tabular_id.into(), force, transaction).await
    }

    async fn clear_tabular_deleted_at(
        tabular_ids: &[TabularId],
        warehouse_id: WarehouseId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<Vec<ViewOrTableDeletionInfo>, ClearTabularDeletedAtError> {
        Self::clear_tabular_deleted_at_impl(tabular_ids, warehouse_id, transaction).await
    }

    async fn mark_tabular_as_deleted(
        warehouse_id: WarehouseId,
        tabular_id: impl Into<TabularId> + Send,
        force: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<ViewOrTableInfo, MarkTabularAsDeletedError> {
        Self::mark_tabular_as_deleted_impl(warehouse_id, tabular_id.into(), force, transaction)
            .await
    }

    async fn search_tabular(
        warehouse_id: WarehouseId,
        search_term: &str,
        catalog_state: Self::State,
    ) -> std::result::Result<CatalogSearchTabularResponse, SearchTabularError> {
        Self::search_tabular_impl(warehouse_id, search_term, catalog_state).await
    }

    async fn rename_tabular(
        warehouse_id: WarehouseId,
        source_id: impl Into<TabularId> + Send,
        source_ident: &TableIdent,
        destination_ident: &TableIdent,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> std::result::Result<ViewOrTableInfo, RenameTabularError> {
        Self::rename_tabular_impl(
            warehouse_id,
            source_id.into(),
            source_ident,
            destination_ident,
            transaction,
        )
        .await
    }

    async fn get_tabular_infos_by_ident(
        warehouse_id: WarehouseId,
        tabulars: &[TabularIdentBorrowed<'_>],
        list_flags: TabularListFlags,
        catalog_state: Self::State,
    ) -> Result<Vec<ViewOrTableInfo>, GetTabularInfoError> {
        Self::get_tabular_infos_by_ident_impl(warehouse_id, tabulars, list_flags, catalog_state)
            .await
    }

    async fn get_table_infos_by_ident(
        warehouse_id: WarehouseId,
        tabulars: &[&TableIdent],
        list_flags: TabularListFlags,
        catalog_state: Self::State,
    ) -> Result<Vec<TableInfo>, GetTabularInfoError> {
        let tabulars = tabulars
            .iter()
            .map(|ident| TabularIdentBorrowed::Table(ident))
            .collect::<Vec<_>>();

        let tables =
            Self::get_tabular_infos_by_ident(warehouse_id, &tabulars, list_flags, catalog_state)
                .await?
                .into_iter()
                .map(|info| {
                    let tabular_id = info.tabular_id();
                    info.into_table_info().ok_or_else(|| {
                        UnexpectedTabularInResponse::new()
                            .append_detail(format!("Expected only tables, got {tabular_id}"))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;

        Ok(tables)
    }

    async fn get_tabular_infos_by_id<'a>(
        warehouse_id: WarehouseId,
        tabulars: &[TabularId],
        list_flags: TabularListFlags,
        catalog_state: Self::State,
    ) -> Result<Vec<ViewOrTableInfo>, GetTabularInfoError> {
        Self::get_tabular_infos_by_id_impl(warehouse_id, tabulars, list_flags, catalog_state).await
    }

    async fn get_tabular_infos_by_s3_location(
        warehouse_id: WarehouseId,
        location: &Location,
        list_flags: TabularListFlags,
        catalog_state: Self::State,
    ) -> Result<Option<ViewOrTableInfo>, GetTabularInfoByLocationError> {
        Self::get_tabular_infos_by_s3_location_impl(
            warehouse_id,
            location,
            list_flags,
            catalog_state,
        )
        .await
    }

    async fn get_table_info(
        warehouse_id: WarehouseId,
        tabular: impl Into<TableIdentOrId> + Send,
        filter: TabularListFlags,
        catalog_state: Self::State,
    ) -> Result<Option<TableInfo>, GetTabularInfoError> {
        let tabular = tabular.into();
        let info = match tabular {
            TableIdentOrId::Ident(ident) => {
                let tabular_ident = TabularIdentOwned::Table(ident.clone());
                let borrowed = tabular_ident.as_borrowed();
                Self::get_tabular_infos_by_ident(warehouse_id, &[borrowed], filter, catalog_state)
                    .await?
            }
            TableIdentOrId::Id(id) => {
                Self::get_tabular_infos_by_id(warehouse_id, &[id.into()], filter, catalog_state)
                    .await?
            }
        };

        if info.len() > 1 {
            return Err(UnexpectedTabularInResponse::new().into());
        }

        let Some(info) = info.into_iter().next() else {
            return Ok(None);
        };

        let obtained_id = info.tabular_id();

        let Some(table_info) = info.into_table_info() else {
            return Err(UnexpectedTabularInResponse::new()
                .append_detail(format!("Expected only tables, got {obtained_id}"))
                .into());
        };

        Ok(Some(table_info))
    }

    async fn get_view_info(
        warehouse_id: WarehouseId,
        tabular: impl Into<ViewIdentOrId> + Send,
        filter: TabularListFlags,
        catalog_state: Self::State,
    ) -> Result<Option<ViewInfo>, GetTabularInfoError> {
        let tabular = tabular.into();
        let info = match tabular {
            ViewIdentOrId::Ident(ident) => {
                let tabular_ident = TabularIdentOwned::View(ident.clone());
                let borrowed = tabular_ident.as_borrowed();
                Self::get_tabular_infos_by_ident(warehouse_id, &[borrowed], filter, catalog_state)
                    .await?
            }
            ViewIdentOrId::Id(id) => {
                Self::get_tabular_infos_by_id(warehouse_id, &[id.into()], filter, catalog_state)
                    .await?
            }
        };

        if info.len() > 1 {
            return Err(UnexpectedTabularInResponse::new().into());
        }

        let Some(info) = info.into_iter().next() else {
            return Ok(None);
        };

        let obtained_id = info.tabular_id();

        let Some(view_info) = info.into_view_info() else {
            return Err(UnexpectedTabularInResponse::new()
                .append_detail(format!("Expected only views, got {obtained_id}"))
                .into());
        };

        Ok(Some(view_info))
    }

    async fn set_tabular_protected(
        warehouse_id: WarehouseId,
        tabular_id: TabularId,
        protect: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<ViewOrTableInfo, SetTabularProtectionError> {
        Self::set_tabular_protected_impl(warehouse_id, tabular_id, protect, transaction).await
    }

    async fn list_tabulars(
        warehouse_id: WarehouseId,
        namespace_id: Option<NamespaceId>, // Filter by namespace
        list_flags: TabularListFlags,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
        typ: Option<TabularType>, // Optional type filter
        pagination_query: PaginationQuery,
    ) -> Result<PaginatedMapping<TabularId, ViewOrTableDeletionInfo>, ListTabularsError> {
        Self::list_tabulars_impl(
            warehouse_id,
            namespace_id,
            list_flags,
            transaction,
            typ,
            pagination_query,
        )
        .await
    }

    async fn list_views<'a>(
        warehouse_id: WarehouseId,
        namespace_id: Option<NamespaceId>,
        list_flags: TabularListFlags,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
        pagination_query: PaginationQuery,
    ) -> Result<PaginatedMapping<ViewId, ViewDeletionInfo>, ListViewsError> {
        let page = Self::list_tabulars(
            warehouse_id,
            namespace_id,
            list_flags,
            transaction,
            Some(TabularType::View),
            pagination_query,
        )
        .await?;
        let views = page.map::<ViewId, ViewDeletionInfo, ListViewsError>(
            |k| match k {
                TabularId::Table(_) => Err(TableInViewList::new(warehouse_id, k).into()),
                TabularId::View(t) => Ok(t),
            },
            |v| {
                let tabular_id = v.tabular_id();
                match v.into_view_info() {
                    Some(view) => Ok(view),
                    None => Err(TableInViewList::new(warehouse_id, tabular_id).into()),
                }
            },
        )?;
        Ok(views)
    }

    async fn list_tables<'a>(
        warehouse_id: WarehouseId,
        namespace_id: Option<NamespaceId>,
        list_flags: TabularListFlags,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
        pagination_query: PaginationQuery,
    ) -> Result<PaginatedMapping<TableId, TableDeletionInfo>, ListTablesError> {
        let page = Self::list_tabulars(
            warehouse_id,
            namespace_id,
            list_flags,
            transaction,
            Some(TabularType::Table),
            pagination_query,
        )
        .await?;
        let tables = page.map::<TableId, TableDeletionInfo, ListTablesError>(
            |k| match k {
                TabularId::Table(t) => Ok(t),
                TabularId::View(_) => Err(ViewInTableList::new(warehouse_id, k).into()),
            },
            |v| {
                let tabular_id = v.tabular_id();
                match v.into_table_info() {
                    Some(table) => Ok(table),
                    None => Err(ViewInTableList::new(warehouse_id, tabular_id).into()),
                }
            },
        )?;
        Ok(tables)
    }
}

impl<T> CatalogTabularOps for T where T: CatalogStore {}
