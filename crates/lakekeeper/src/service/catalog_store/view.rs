use http::StatusCode;
use iceberg::{
    TableIdent,
    spec::{ViewMetadata, ViewMetadataRef},
};
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
use lakekeeper_io::Location;

use crate::{
    WarehouseId,
    service::{
        CatalogBackendError, CatalogGetNamespaceError, CatalogStore, ConcurrentUpdateError,
        ConversionError, CreateTabularError, DropTabularError, InternalParseLocationError,
        InvalidNamespaceIdentifier, LocationAlreadyTaken, NamespaceId,
        ProtectedTabularDeletionWithoutForce, SerializationError, TabularAlreadyExists,
        TabularNotFound, Transaction, UnexpectedTabularInResponse, ViewId, ViewInfo,
        WarehouseVersion, define_simple_tabular_err, define_transparent_error,
        impl_error_stack_methods, impl_from_with_detail,
    },
};

#[derive(Debug, Clone)]
pub struct CatalogView {
    pub metadata_location: Location,
    pub metadata: ViewMetadataRef,
    // Typesafe location for the view
    pub location: Location,
    pub warehouse_version: WarehouseVersion,
}

#[derive(Debug, Clone)]
pub struct ViewCommit<'a> {
    pub view_ident: &'a TableIdent,
    pub namespace_id: NamespaceId,
    pub warehouse_id: WarehouseId,
    pub previous_view: &'a CatalogView,
    pub new_view: &'a CatalogView,
}

impl ViewCommit<'_> {
    #[must_use]
    pub fn previous_metadata_location(&self) -> &Location {
        &self.previous_view.metadata_location
    }
}

define_simple_tabular_err!(
    RequiredViewComponentMissing,
    "A required field for a view could not be loaded"
);

impl From<RequiredViewComponentMissing> for ErrorModel {
    fn from(err: RequiredViewComponentMissing) -> Self {
        ErrorModel {
            message: err.to_string(),
            r#type: "RequiredViewComponentMissing".to_string(),
            code: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
            source: None,
            stack: err.stack,
        }
    }
}

define_simple_tabular_err!(
    InvalidViewRepresentationsInternal,
    "Failed to build view representations"
);

impl From<InvalidViewRepresentationsInternal> for ErrorModel {
    fn from(err: InvalidViewRepresentationsInternal) -> Self {
        ErrorModel {
            message: err.to_string(),
            r#type: "InvalidViewRepresentations".to_string(),
            code: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
            source: None,
            stack: err.stack,
        }
    }
}

define_simple_tabular_err!(
    ViewMetadataValidationFailedInternal,
    "View Metadata validation failed after loading view from catalog"
);

impl From<ViewMetadataValidationFailedInternal> for ErrorModel {
    fn from(err: ViewMetadataValidationFailedInternal) -> Self {
        ErrorModel {
            message: err.to_string(),
            r#type: "ViewMetadataValidationFailed".to_string(),
            code: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
            source: None,
            stack: err.stack,
        }
    }
}

define_transparent_error! {
    pub enum CreateViewVersionError,
    stack_message: "Error creating view version",
    variants: [
        CatalogBackendError,
        SerializationError,
        ConversionError
    ]
}
impl From<CreateViewVersionError> for CreateViewError {
    fn from(err: CreateViewVersionError) -> Self {
        // Use direct constructors to avoid additional stack
        match err {
            CreateViewVersionError::CatalogBackendError(e) => {
                CreateViewError::CatalogBackendError(e)
            }
            CreateViewVersionError::SerializationError(e) => CreateViewError::SerializationError(e),
            CreateViewVersionError::ConversionError(e) => CreateViewError::ConversionError(e),
        }
    }
}

define_transparent_error! {
    pub enum LoadViewError,
    stack_message: "Error loading view from catalog",
    variants: [
        CatalogBackendError,
        InvalidNamespaceIdentifier,
        RequiredViewComponentMissing,
        InvalidViewRepresentationsInternal,
        InternalParseLocationError,
        ViewMetadataValidationFailedInternal,
        TabularNotFound,
        SerializationError
    ]
}
impl From<CatalogGetNamespaceError> for LoadViewError {
    fn from(err: CatalogGetNamespaceError) -> Self {
        match err {
            CatalogGetNamespaceError::InvalidNamespaceIdentifier(e) => {
                LoadViewError::InvalidNamespaceIdentifier(e)
            }
            CatalogGetNamespaceError::CatalogBackendError(e) => {
                LoadViewError::CatalogBackendError(e)
            }
            CatalogGetNamespaceError::SerializationError(e) => LoadViewError::SerializationError(e),
        }
    }
}

define_transparent_error! {
    pub enum CreateViewError,
    stack_message: "Error creating view in catalog",
    variants: [
        CatalogBackendError,
        InternalParseLocationError,
        LocationAlreadyTaken,
        SerializationError,
        ConversionError,
        UnexpectedTabularInResponse,
        InvalidNamespaceIdentifier,
        TabularAlreadyExists,
    ]
}
impl From<CreateTabularError> for CreateViewError {
    fn from(err: CreateTabularError) -> Self {
        match err {
            CreateTabularError::CatalogBackendError(e) => e.into(),
            CreateTabularError::InternalParseLocationError(e) => e.into(),
            CreateTabularError::LocationAlreadyTaken(e) => e.into(),
            CreateTabularError::InvalidNamespaceIdentifier(e) => e.into(),
            CreateTabularError::TabularAlreadyExists(e) => e.into(),
        }
    }
}

define_transparent_error! {
    pub enum CommitViewError,
    stack_message: "Error committing view in catalog",
    variants: [
        CatalogBackendError,
        InternalParseLocationError,
        TabularNotFound,
        InvalidNamespaceIdentifier,
        LocationAlreadyTaken,
        SerializationError,
        ConversionError,
        UnexpectedTabularInResponse,
        ConcurrentUpdateError,
        TabularAlreadyExists,
        ProtectedTabularDeletionWithoutForce
    ]
}
impl From<CreateViewError> for CommitViewError {
    fn from(err: CreateViewError) -> Self {
        match err {
            CreateViewError::CatalogBackendError(e) => e.into(),
            CreateViewError::InternalParseLocationError(e) => e.into(),
            CreateViewError::LocationAlreadyTaken(e) => e.into(),
            CreateViewError::SerializationError(e) => e.into(),
            CreateViewError::ConversionError(e) => e.into(),
            CreateViewError::UnexpectedTabularInResponse(e) => e.into(),
            CreateViewError::InvalidNamespaceIdentifier(e) => e.into(),
            CreateViewError::TabularAlreadyExists(e) => e.into(),
        }
    }
}
impl From<DropTabularError> for CommitViewError {
    fn from(err: DropTabularError) -> Self {
        match err {
            DropTabularError::CatalogBackendError(e) => e.into(),
            DropTabularError::TabularNotFound(e) => e.into(),
            DropTabularError::InternalParseLocationError(e) => e.into(),
            DropTabularError::InvalidNamespaceIdentifier(e) => e.into(),
            DropTabularError::ProtectedTabularDeletionWithoutForce(e) => e.into(),
            DropTabularError::ConcurrentUpdateError(e) => e.into(),
        }
    }
}

#[async_trait::async_trait]
pub trait CatalogViewOps
where
    Self: CatalogStore,
{
    async fn load_view<'a>(
        warehouse_id: WarehouseId,
        view_id: ViewId,
        include_deleted: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<CatalogView, LoadViewError> {
        Self::load_view_impl(warehouse_id, view_id, include_deleted, transaction).await
    }

    async fn create_view<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        view_ident: &TableIdent,
        request: &ViewMetadata,
        metadata_location: &Location,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<ViewInfo, CreateViewError> {
        Self::create_view_impl(
            warehouse_id,
            namespace_id,
            view_ident,
            request,
            metadata_location,
            transaction,
        )
        .await
    }

    async fn commit_view(
        commit: ViewCommit<'_>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<ViewInfo, CommitViewError> {
        Self::commit_view_impl(commit, transaction).await
    }
}

impl<T> CatalogViewOps for T where T: CatalogStore {}
