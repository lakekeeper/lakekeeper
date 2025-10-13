use http::StatusCode;
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};

use super::{CatalogStore, Transaction};
use crate::{
    api::management::v1::{
        warehouse::TabularDeleteProfile, DeleteWarehouseQuery, ProtectionResponse,
    },
    service::{
        catalog_store::{impl_error_stack_methods, impl_from_with_detail, CatalogBackendError},
        define_simple_error,
        storage::StorageProfile,
        DatabaseIntegrityError, Result as ServiceResult,
    },
    ProjectId, SecretIdent, WarehouseId,
};

/// Status of a warehouse
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    strum_macros::Display,
    strum_macros::EnumIter,
    serde::Serialize,
    serde::Deserialize,
    utoipa::ToSchema,
)]
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
#[cfg_attr(feature = "sqlx", derive(sqlx::Type))]
#[cfg_attr(
    feature = "sqlx",
    sqlx(type_name = "warehouse_status", rename_all = "kebab-case")
)]
pub enum WarehouseStatus {
    /// The warehouse is active and can be used
    Active,
    /// The warehouse is inactive and cannot be used.
    Inactive,
}

#[derive(Debug)]
pub struct GetStorageConfigResponse {
    pub storage_profile: StorageProfile,
    pub storage_secret_ident: Option<SecretIdent>,
}

#[derive(Debug, Clone)]
pub struct GetWarehouseResponse {
    /// ID of the warehouse.
    pub id: WarehouseId,
    /// Name of the warehouse.
    pub name: String,
    /// Project ID in which the warehouse is created.
    pub project_id: ProjectId,
    /// Storage profile used for the warehouse.
    pub storage_profile: StorageProfile,
    /// Storage secret ID used for the warehouse.
    pub storage_secret_id: Option<SecretIdent>,
    /// Whether the warehouse is active.
    pub status: WarehouseStatus,
    /// Tabular delete profile used for the warehouse.
    pub tabular_delete_profile: TabularDeleteProfile,
    /// Whether the warehouse is protected from being deleted.
    pub protected: bool,
}

// --------------------------- GENERAL ERROR ---------------------------
#[derive(thiserror::Error, Debug, PartialEq)]
#[error("A warehouse with id '{warehouse_id}' does not exist")]
pub struct WarehouseIdNotFound {
    pub warehouse_id: WarehouseId,
    pub stack: Vec<String>,
}
impl WarehouseIdNotFound {
    #[must_use]
    pub fn new(warehouse_id: WarehouseId) -> Self {
        Self {
            warehouse_id,
            stack: Vec::new(),
        }
    }
}
impl_error_stack_methods!(WarehouseIdNotFound);

impl From<WarehouseIdNotFound> for ErrorModel {
    fn from(err: WarehouseIdNotFound) -> Self {
        ErrorModel {
            r#type: "WarehouseNotFound".to_string(),
            code: StatusCode::NOT_FOUND.as_u16(),
            message: err.to_string(),
            stack: err.stack,
            source: None,
        }
    }
}
impl From<WarehouseIdNotFound> for IcebergErrorResponse {
    fn from(err: WarehouseIdNotFound) -> Self {
        ErrorModel::from(err).into()
    }
}

#[derive(thiserror::Error, Debug, PartialEq)]
#[error("A warehouse '{warehouse_name}' does not exist")]
pub struct WarehouseNameNotFound {
    pub warehouse_name: String,
    pub stack: Vec<String>,
}
impl WarehouseNameNotFound {
    #[must_use]
    pub fn new(warehouse_name: impl Into<String>) -> Self {
        Self {
            warehouse_name: warehouse_name.into(),
            stack: Vec::new(),
        }
    }
}
impl_error_stack_methods!(WarehouseNameNotFound);

impl From<WarehouseNameNotFound> for ErrorModel {
    fn from(err: WarehouseNameNotFound) -> Self {
        ErrorModel {
            r#type: "WarehouseNotFound".to_string(),
            code: StatusCode::NOT_FOUND.as_u16(),
            message: err.to_string(),
            stack: err.stack,
            source: None,
        }
    }
}
impl From<WarehouseNameNotFound> for IcebergErrorResponse {
    fn from(err: WarehouseNameNotFound) -> Self {
        ErrorModel::from(err).into()
    }
}

#[derive(thiserror::Error, Debug)]
#[error("Error serializing storage profile: {source}")]
pub struct StorageProfileSerializationError {
    source: serde_json::Error,
    stack: Vec<String>,
}
impl_error_stack_methods!(StorageProfileSerializationError);
impl From<serde_json::Error> for StorageProfileSerializationError {
    fn from(source: serde_json::Error) -> Self {
        Self {
            source,
            stack: Vec::new(),
        }
    }
}
impl PartialEq for StorageProfileSerializationError {
    fn eq(&self, other: &Self) -> bool {
        self.source.to_string() == other.source.to_string() && self.stack == other.stack
    }
}
impl From<StorageProfileSerializationError> for ErrorModel {
    fn from(err: StorageProfileSerializationError) -> Self {
        let message = err.to_string();
        let StorageProfileSerializationError { source, stack } = err;

        ErrorModel {
            r#type: "StorageProfileSerializationError".to_string(),
            code: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
            message,
            stack,
            source: Some(Box::new(source)),
        }
    }
}

// --------------------------- CREATE ERROR ---------------------------
#[derive(thiserror::Error, Debug)]
pub enum CatalogCreateWarehouseError {
    #[error(transparent)]
    WarehouseAlreadyExists(WarehouseAlreadyExists),
    #[error(transparent)]
    CatalogBackendError(CatalogBackendError),
    #[error(transparent)]
    StorageProfileSerializationError(StorageProfileSerializationError),
    #[error(transparent)]
    ProjectIdNotFoundError(ProjectIdNotFoundError),
}

const CREATE_ERROR_STACK: &str = "Error creating warehouse in catalog";
impl_from_with_detail!(CatalogBackendError => CatalogCreateWarehouseError::CatalogBackendError, CREATE_ERROR_STACK);
impl_from_with_detail!(StorageProfileSerializationError => CatalogCreateWarehouseError::StorageProfileSerializationError, CREATE_ERROR_STACK);
impl_from_with_detail!(ProjectIdNotFoundError => CatalogCreateWarehouseError::ProjectIdNotFoundError, CREATE_ERROR_STACK);
impl_from_with_detail!(WarehouseAlreadyExists => CatalogCreateWarehouseError::WarehouseAlreadyExists, CREATE_ERROR_STACK);

#[derive(thiserror::Error, Debug)]
#[error(
    "A warehouse with the name '{warehouse_name}' already exists in project with id '{project_id}'"
)]
pub struct WarehouseAlreadyExists {
    pub warehouse_name: String,
    pub project_id: ProjectId,
    pub stack: Vec<String>,
}
impl WarehouseAlreadyExists {
    #[must_use]
    pub fn new(warehouse_name: String, project_id: ProjectId) -> Self {
        Self {
            warehouse_name,
            project_id,
            stack: Vec::new(),
        }
    }
}
impl_error_stack_methods!(WarehouseAlreadyExists);

#[derive(thiserror::Error, Debug)]
#[error("Project with id '{project_id}' not found")]
pub struct ProjectIdNotFoundError {
    project_id: ProjectId,
    stack: Vec<String>,
}
impl_error_stack_methods!(ProjectIdNotFoundError);
impl ProjectIdNotFoundError {
    #[must_use]
    pub fn new(project_id: ProjectId) -> Self {
        Self {
            project_id,
            stack: Vec::new(),
        }
    }
}

impl From<CatalogCreateWarehouseError> for ErrorModel {
    fn from(err: CatalogCreateWarehouseError) -> Self {
        match err {
            CatalogCreateWarehouseError::WarehouseAlreadyExists(e) => ErrorModel {
                r#type: "WarehouseAlreadyExists".to_string(),
                code: StatusCode::CONFLICT.as_u16(),
                message: e.to_string(),
                stack: e.stack,
                source: None,
            },
            CatalogCreateWarehouseError::CatalogBackendError(e) => e.into(),
            CatalogCreateWarehouseError::StorageProfileSerializationError(e) => e.into(),
            CatalogCreateWarehouseError::ProjectIdNotFoundError(e) => ErrorModel {
                r#type: "ProjectNotFound".to_string(),
                code: StatusCode::NOT_FOUND.as_u16(),
                message: e.to_string(),
                stack: e.stack,
                source: None,
            },
        }
    }
}

impl From<CatalogCreateWarehouseError> for IcebergErrorResponse {
    fn from(err: CatalogCreateWarehouseError) -> Self {
        ErrorModel::from(err).into()
    }
}

// --------------------------- DELETE ERROR ---------------------------
#[derive(thiserror::Error, Debug, PartialEq)]
pub enum CatalogDeleteWarehouseError {
    #[error(transparent)]
    CatalogBackendError(CatalogBackendError),
    #[error(transparent)]
    WarehouseHasUnfinishedTasks(WarehouseHasUnfinishedTasks),
    #[error(transparent)]
    WarehouseIdNotFound(WarehouseIdNotFound),
    #[error(transparent)]
    WarehouseNotEmpty(WarehouseNotEmpty),
    #[error(transparent)]
    WarehouseProtected(WarehouseProtected),
}

const DELETE_ERROR_STACK: &str = "Error deleting warehouse in catalog";

impl_from_with_detail!(CatalogBackendError => CatalogDeleteWarehouseError::CatalogBackendError, DELETE_ERROR_STACK);
impl_from_with_detail!(WarehouseHasUnfinishedTasks => CatalogDeleteWarehouseError::WarehouseHasUnfinishedTasks, DELETE_ERROR_STACK);
impl_from_with_detail!(WarehouseIdNotFound => CatalogDeleteWarehouseError::WarehouseIdNotFound, DELETE_ERROR_STACK);
impl_from_with_detail!(WarehouseNotEmpty => CatalogDeleteWarehouseError::WarehouseNotEmpty, DELETE_ERROR_STACK);
impl_from_with_detail!(WarehouseProtected => CatalogDeleteWarehouseError::WarehouseProtected, DELETE_ERROR_STACK);

define_simple_error!(
    WarehouseHasUnfinishedTasks,
    "Warehouse has unfinished tasks. Cannot delete warehouse until all tasks are finished."
);

define_simple_error!(
    WarehouseNotEmpty,
    "Warehouse is not empty. Cannot delete a non-empty warehouse."
);
define_simple_error!(
    WarehouseProtected,
    "Warehouse is protected and force flag not set. Cannot delete protected warehouse."
);

impl From<CatalogDeleteWarehouseError> for ErrorModel {
    fn from(err: CatalogDeleteWarehouseError) -> Self {
        match err {
            CatalogDeleteWarehouseError::WarehouseHasUnfinishedTasks(e) => ErrorModel {
                r#type: "WarehouseHasUnfinishedTasks".to_string(),
                code: StatusCode::CONFLICT.as_u16(),
                message: e.to_string(),
                stack: e.stack,
                source: None,
            },
            CatalogDeleteWarehouseError::WarehouseIdNotFound(e) => e.into(),
            CatalogDeleteWarehouseError::WarehouseNotEmpty(e) => ErrorModel {
                r#type: "WarehouseNotEmpty".to_string(),
                code: StatusCode::CONFLICT.as_u16(),
                message: e.to_string(),
                stack: e.stack,
                source: None,
            },
            CatalogDeleteWarehouseError::WarehouseProtected(e) => ErrorModel {
                r#type: "WarehouseProtected".to_string(),
                code: StatusCode::CONFLICT.as_u16(),
                message: e.to_string(),
                stack: e.stack,
                source: None,
            },
            CatalogDeleteWarehouseError::CatalogBackendError(e) => e.into(),
        }
    }
}
impl From<CatalogDeleteWarehouseError> for IcebergErrorResponse {
    fn from(err: CatalogDeleteWarehouseError) -> Self {
        ErrorModel::from(err).into()
    }
}

// --------------------------- RENAME ERROR ---------------------------
#[derive(thiserror::Error, Debug)]
pub enum CatalogRenameWarehouseError {
    #[error(transparent)]
    CatalogBackendError(CatalogBackendError),
    #[error(transparent)]
    WarehouseIdNotFound(WarehouseIdNotFound),
}
const RENAME_ERROR_STACK: &str = "Error renaming warehouse in catalog";
impl_from_with_detail!(CatalogBackendError => CatalogRenameWarehouseError::CatalogBackendError, RENAME_ERROR_STACK);
impl_from_with_detail!(WarehouseIdNotFound => CatalogRenameWarehouseError::WarehouseIdNotFound, RENAME_ERROR_STACK);

impl From<CatalogRenameWarehouseError> for ErrorModel {
    fn from(err: CatalogRenameWarehouseError) -> Self {
        match err {
            CatalogRenameWarehouseError::WarehouseIdNotFound(e) => e.into(),
            CatalogRenameWarehouseError::CatalogBackendError(e) => e.into(),
        }
    }
}
impl From<CatalogRenameWarehouseError> for IcebergErrorResponse {
    fn from(err: CatalogRenameWarehouseError) -> Self {
        ErrorModel::from(err).into()
    }
}

// --------------------------- LIST ERROR ---------------------------

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum CatalogListWarehousesError {
    #[error(transparent)]
    CatalogBackendError(CatalogBackendError),
    #[error(transparent)]
    DatabaseIntegrityError(DatabaseIntegrityError),
}

const LIST_ERROR_STACK: &str = "Error listing warehouses in catalog";
impl_from_with_detail!(CatalogBackendError => CatalogListWarehousesError::CatalogBackendError, LIST_ERROR_STACK);
impl_from_with_detail!(DatabaseIntegrityError => CatalogListWarehousesError::DatabaseIntegrityError, LIST_ERROR_STACK);

impl From<CatalogListWarehousesError> for ErrorModel {
    fn from(err: CatalogListWarehousesError) -> Self {
        match err {
            CatalogListWarehousesError::DatabaseIntegrityError(e) => e.into(),
            CatalogListWarehousesError::CatalogBackendError(e) => e.into(),
        }
    }
}
impl From<CatalogListWarehousesError> for IcebergErrorResponse {
    fn from(err: CatalogListWarehousesError) -> Self {
        ErrorModel::from(err).into()
    }
}

// --------------------------- GET ERROR ---------------------------
#[derive(thiserror::Error, Debug, PartialEq)]
pub enum CatalogGetWarehouseByIdError {
    #[error(transparent)]
    CatalogBackendError(CatalogBackendError),
    #[error(transparent)]
    DatabaseIntegrityError(DatabaseIntegrityError),
    #[error(transparent)]
    WarehouseIdNotFound(WarehouseIdNotFound),
}
impl CatalogGetWarehouseByIdError {
    #[must_use]
    pub fn append_detail(mut self, detail: String) -> Self {
        match &mut self {
            CatalogGetWarehouseByIdError::CatalogBackendError(e) => {
                e.append_detail_mut(detail);
            }
            CatalogGetWarehouseByIdError::DatabaseIntegrityError(e) => {
                e.append_detail_mut(detail);
            }
            CatalogGetWarehouseByIdError::WarehouseIdNotFound(e) => {
                e.append_detail_mut(detail);
            }
        }
        self
    }
}
const GET_ERROR_STACK: &str = "Error getting warehouse by id in catalog";
impl_from_with_detail!(CatalogBackendError => CatalogGetWarehouseByIdError::CatalogBackendError, GET_ERROR_STACK);
impl_from_with_detail!(DatabaseIntegrityError => CatalogGetWarehouseByIdError::DatabaseIntegrityError, GET_ERROR_STACK);
impl_from_with_detail!(WarehouseIdNotFound => CatalogGetWarehouseByIdError::WarehouseIdNotFound, GET_ERROR_STACK);

impl From<CatalogGetWarehouseByIdError> for ErrorModel {
    fn from(err: CatalogGetWarehouseByIdError) -> Self {
        match err {
            CatalogGetWarehouseByIdError::DatabaseIntegrityError(e) => e.into(),
            CatalogGetWarehouseByIdError::CatalogBackendError(e) => e.into(),
            CatalogGetWarehouseByIdError::WarehouseIdNotFound(e) => e.into(),
        }
    }
}
impl From<CatalogGetWarehouseByIdError> for IcebergErrorResponse {
    fn from(err: CatalogGetWarehouseByIdError) -> Self {
        ErrorModel::from(err).into()
    }
}

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum CatalogGetWarehouseByNameError {
    #[error(transparent)]
    CatalogBackendError(CatalogBackendError),
    #[error(transparent)]
    DatabaseIntegrityError(DatabaseIntegrityError),
    #[error(transparent)]
    WarehouseNameNotFound(WarehouseNameNotFound),
}
impl CatalogGetWarehouseByNameError {
    #[must_use]
    pub fn append_detail(mut self, detail: String) -> Self {
        match &mut self {
            CatalogGetWarehouseByNameError::CatalogBackendError(e) => {
                e.append_detail_mut(detail);
            }
            CatalogGetWarehouseByNameError::DatabaseIntegrityError(e) => {
                e.append_detail_mut(detail);
            }
            CatalogGetWarehouseByNameError::WarehouseNameNotFound(e) => {
                e.append_detail_mut(detail);
            }
        }
        self
    }
}
const GET_BY_NAME_ERROR_STACK: &str = "Error getting warehouse by name in catalog";
impl_from_with_detail!(CatalogBackendError => CatalogGetWarehouseByNameError::CatalogBackendError, GET_BY_NAME_ERROR_STACK);
impl_from_with_detail!(DatabaseIntegrityError => CatalogGetWarehouseByNameError::DatabaseIntegrityError, GET_BY_NAME_ERROR_STACK);
impl_from_with_detail!(WarehouseNameNotFound => CatalogGetWarehouseByNameError::WarehouseNameNotFound, GET_BY_NAME_ERROR_STACK);

impl From<CatalogGetWarehouseByNameError> for ErrorModel {
    fn from(err: CatalogGetWarehouseByNameError) -> Self {
        match err {
            CatalogGetWarehouseByNameError::DatabaseIntegrityError(e) => e.into(),
            CatalogGetWarehouseByNameError::CatalogBackendError(e) => e.into(),
            CatalogGetWarehouseByNameError::WarehouseNameNotFound(e) => e.into(),
        }
    }
}
impl From<CatalogGetWarehouseByNameError> for IcebergErrorResponse {
    fn from(err: CatalogGetWarehouseByNameError) -> Self {
        ErrorModel::from(err).into()
    }
}

// --------------------------- Set Warehouse Delete Profile Error ---------------------------
#[derive(thiserror::Error, Debug, PartialEq)]
pub enum SetWarehouseDeletionProfileError {
    #[error(transparent)]
    CatalogBackendError(CatalogBackendError),
    #[error(transparent)]
    WarehouseIdNotFound(WarehouseIdNotFound),
}
const SET_WAREHOUSE_DELETION_PROFILE_ERROR_STACK: &str =
    "Error setting warehouse deletion profile in catalog";
impl_from_with_detail!(CatalogBackendError => SetWarehouseDeletionProfileError::CatalogBackendError,
    SET_WAREHOUSE_DELETION_PROFILE_ERROR_STACK);
impl_from_with_detail!(WarehouseIdNotFound => SetWarehouseDeletionProfileError::WarehouseIdNotFound,
    SET_WAREHOUSE_DELETION_PROFILE_ERROR_STACK);

impl From<SetWarehouseDeletionProfileError> for ErrorModel {
    fn from(err: SetWarehouseDeletionProfileError) -> Self {
        match err {
            SetWarehouseDeletionProfileError::WarehouseIdNotFound(e) => e.into(),
            SetWarehouseDeletionProfileError::CatalogBackendError(e) => e.into(),
        }
    }
}
impl From<SetWarehouseDeletionProfileError> for IcebergErrorResponse {
    fn from(err: SetWarehouseDeletionProfileError) -> Self {
        ErrorModel::from(err).into()
    }
}

// --------------------------- Set Warehouse Status Error ---------------------------
#[derive(thiserror::Error, Debug, PartialEq)]
pub enum SetWarehouseStatusError {
    #[error(transparent)]
    CatalogBackendError(CatalogBackendError),
    #[error(transparent)]
    WarehouseIdNotFound(WarehouseIdNotFound),
}
const SET_WAREHOUSE_STATUS_ERROR_STACK: &str = "Error setting warehouse status in catalog";
impl_from_with_detail!(CatalogBackendError => SetWarehouseStatusError::CatalogBackendError,
    SET_WAREHOUSE_STATUS_ERROR_STACK);
impl_from_with_detail!(WarehouseIdNotFound => SetWarehouseStatusError::WarehouseIdNotFound,
    SET_WAREHOUSE_STATUS_ERROR_STACK);

impl From<SetWarehouseStatusError> for ErrorModel {
    fn from(err: SetWarehouseStatusError) -> Self {
        match err {
            SetWarehouseStatusError::WarehouseIdNotFound(e) => e.into(),
            SetWarehouseStatusError::CatalogBackendError(e) => e.into(),
        }
    }
}
impl From<SetWarehouseStatusError> for IcebergErrorResponse {
    fn from(err: SetWarehouseStatusError) -> Self {
        ErrorModel::from(err).into()
    }
}

// --------------------------- Set Warehouse Status Error ---------------------------
#[derive(thiserror::Error, Debug, PartialEq)]
pub enum UpdateWarehouseStorageProfileError {
    #[error(transparent)]
    CatalogBackendError(CatalogBackendError),
    #[error(transparent)]
    WarehouseIdNotFound(WarehouseIdNotFound),
    #[error(transparent)]
    StorageProfileSerializationError(StorageProfileSerializationError),
}
const UPDATE_WAREHOUSE_STORAGE_PROFILE_ERROR_STACK: &str =
    "Error updating warehouse storage profile in catalog";
impl_from_with_detail!(CatalogBackendError => UpdateWarehouseStorageProfileError::CatalogBackendError,
    UPDATE_WAREHOUSE_STORAGE_PROFILE_ERROR_STACK);
impl_from_with_detail!(WarehouseIdNotFound => UpdateWarehouseStorageProfileError::WarehouseIdNotFound,
    UPDATE_WAREHOUSE_STORAGE_PROFILE_ERROR_STACK);
impl_from_with_detail!(StorageProfileSerializationError => UpdateWarehouseStorageProfileError::StorageProfileSerializationError,
    UPDATE_WAREHOUSE_STORAGE_PROFILE_ERROR_STACK);
impl From<UpdateWarehouseStorageProfileError> for ErrorModel {
    fn from(err: UpdateWarehouseStorageProfileError) -> Self {
        match err {
            UpdateWarehouseStorageProfileError::WarehouseIdNotFound(e) => e.into(),
            UpdateWarehouseStorageProfileError::CatalogBackendError(e) => e.into(),
            UpdateWarehouseStorageProfileError::StorageProfileSerializationError(e) => e.into(),
        }
    }
}
impl From<UpdateWarehouseStorageProfileError> for IcebergErrorResponse {
    fn from(err: UpdateWarehouseStorageProfileError) -> Self {
        ErrorModel::from(err).into()
    }
}

// --------------------------- Set Warehouse Protected Error ---------------------------
#[derive(thiserror::Error, Debug, PartialEq)]
pub enum SetWarehouseProtectedError {
    #[error(transparent)]
    CatalogBackendError(CatalogBackendError),
    #[error(transparent)]
    WarehouseIdNotFound(WarehouseIdNotFound),
}
const SET_WAREHOUSE_PROTECTED_ERROR_STACK: &str = "Error setting warehouse protected in catalog";
impl_from_with_detail!(CatalogBackendError => SetWarehouseProtectedError::CatalogBackendError,
    SET_WAREHOUSE_PROTECTED_ERROR_STACK);
impl_from_with_detail!(WarehouseIdNotFound => SetWarehouseProtectedError::WarehouseIdNotFound,
    SET_WAREHOUSE_PROTECTED_ERROR_STACK);
impl From<SetWarehouseProtectedError> for ErrorModel {
    fn from(err: SetWarehouseProtectedError) -> Self {
        match err {
            SetWarehouseProtectedError::WarehouseIdNotFound(e) => e.into(),
            SetWarehouseProtectedError::CatalogBackendError(e) => e.into(),
        }
    }
}
impl From<SetWarehouseProtectedError> for IcebergErrorResponse {
    fn from(err: SetWarehouseProtectedError) -> Self {
        ErrorModel::from(err).into()
    }
}

#[async_trait::async_trait]
pub trait CatalogWarehouseOps
where
    Self: CatalogStore,
{
    /// Create a warehouse.
    async fn create_warehouse<'a>(
        warehouse_name: String,
        project_id: &ProjectId,
        storage_profile: StorageProfile,
        tabular_delete_profile: TabularDeleteProfile,
        storage_secret_id: Option<SecretIdent>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> ServiceResult<WarehouseId> {
        Self::create_warehouse_impl(
            warehouse_name,
            project_id,
            storage_profile,
            tabular_delete_profile,
            storage_secret_id,
            transaction,
        )
        .await
        .map_err(Into::into)
    }

    /// Delete a warehouse.
    async fn delete_warehouse<'a>(
        warehouse_id: WarehouseId,
        query: DeleteWarehouseQuery,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> ServiceResult<()> {
        Self::delete_warehouse_impl(warehouse_id, query, transaction)
            .await
            .map_err(Into::into)
    }

    /// Rename a warehouse.
    async fn rename_warehouse<'a>(
        warehouse_id: WarehouseId,
        new_name: &str,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<(), CatalogRenameWarehouseError> {
        Self::rename_warehouse_impl(warehouse_id, new_name, transaction).await
    }

    /// Return a list of all warehouse in a project
    async fn list_warehouses(
        project_id: &ProjectId,
        // If None, returns active warehouses
        // If Some, returns warehouses with any of the statuses in the set
        include_inactive: Option<Vec<WarehouseStatus>>,
        state: Self::State,
    ) -> Result<Vec<GetWarehouseResponse>, CatalogListWarehousesError> {
        Self::list_warehouses_impl(project_id, include_inactive, state).await
    }

    /// Get the warehouse metadata - should only return active warehouses.
    ///
    /// Return Ok(None) if the warehouse does not exist.
    async fn get_warehouse_by_id<'a>(
        warehouse_id: WarehouseId,
        state: Self::State,
    ) -> Result<Option<GetWarehouseResponse>, CatalogGetWarehouseByIdError> {
        Self::get_warehouse_by_id_impl(warehouse_id, state).await
    }

    /// Wrapper around `get_warehouse` that returns a not-found error if the warehouse does not exist.
    async fn require_warehouse_by_id<'a>(
        warehouse_id: WarehouseId,
        state: Self::State,
    ) -> Result<GetWarehouseResponse, CatalogGetWarehouseByIdError> {
        Self::get_warehouse_by_id(warehouse_id, state)
            .await?
            .ok_or(WarehouseIdNotFound::new(warehouse_id).into())
    }

    async fn get_warehouse_by_name(
        warehouse_name: &str,
        project_id: &ProjectId,
        catalog_state: Self::State,
    ) -> Result<Option<GetWarehouseResponse>, CatalogGetWarehouseByNameError> {
        Self::get_warehouse_by_name_impl(warehouse_name, project_id, catalog_state).await
    }

    /// Wrapper around `get_warehouse_by_name` that returns
    /// not found error if the warehouse does not exist.
    async fn require_warehouse_by_name(
        warehouse_name: &str,
        project_id: &ProjectId,
        catalog_state: Self::State,
    ) -> Result<GetWarehouseResponse, CatalogGetWarehouseByNameError> {
        Self::get_warehouse_by_name(warehouse_name, project_id, catalog_state)
            .await?
            .ok_or(WarehouseNameNotFound::new(warehouse_name.to_string()).into())
    }

    /// Set warehouse deletion profile
    async fn set_warehouse_deletion_profile<'a>(
        warehouse_id: WarehouseId,
        deletion_profile: &TabularDeleteProfile,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<(), SetWarehouseDeletionProfileError> {
        Self::set_warehouse_deletion_profile_impl(warehouse_id, deletion_profile, transaction).await
    }

    async fn set_warehouse_status<'a>(
        warehouse_id: WarehouseId,
        status: WarehouseStatus,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<(), SetWarehouseStatusError> {
        Self::set_warehouse_status_impl(warehouse_id, status, transaction).await
    }

    async fn update_storage_profile<'a>(
        warehouse_id: WarehouseId,
        storage_profile: StorageProfile,
        storage_secret_id: Option<SecretIdent>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<(), UpdateWarehouseStorageProfileError> {
        Self::update_storage_profile_impl(
            warehouse_id,
            storage_profile,
            storage_secret_id,
            transaction,
        )
        .await
    }

    async fn set_warehouse_protected(
        warehouse_id: WarehouseId,
        protect: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> std::result::Result<ProtectionResponse, SetWarehouseProtectedError> {
        Self::set_warehouse_protected_impl(warehouse_id, protect, transaction).await
    }
}

impl<T> CatalogWarehouseOps for T where T: CatalogStore {}
