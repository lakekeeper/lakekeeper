use std::sync::Arc;

use http::StatusCode;
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};

use crate::{
    api::{
        iceberg::v1::PaginationQuery,
        management::v1::role::{ListRolesResponse, Role, SearchRoleResponse},
    },
    service::{
        define_transparent_error, impl_error_stack_methods, impl_from_with_detail,
        CatalogBackendError, CatalogStore, InvalidPaginationToken, ProjectIdNotFoundError, RoleId,
        Transaction,
    },
    ProjectId,
};

#[derive(thiserror::Error, Debug, PartialEq)]
#[error("A role with id '{role_id}' does not exist in project with id '{project_id}'")]
pub struct RoleIdNotFound {
    pub role_id: RoleId,
    pub project_id: ProjectId,
    pub stack: Vec<String>,
}
impl RoleIdNotFound {
    #[must_use]
    pub fn new(role_id: RoleId, project_id: ProjectId) -> Self {
        Self {
            role_id,
            project_id,
            stack: Vec::new(),
        }
    }
}
impl_error_stack_methods!(RoleIdNotFound);

impl From<RoleIdNotFound> for ErrorModel {
    fn from(err: RoleIdNotFound) -> Self {
        ErrorModel {
            r#type: "RoleNotFound".to_string(),
            code: StatusCode::NOT_FOUND.as_u16(),
            message: err.to_string(),
            stack: err.stack,
            source: None,
        }
    }
}

// --------------------------- CREATE ERROR ---------------------------
define_transparent_error! {
    pub enum CreateRoleError,
    stack_message: "Error creating role in catalog",
    variants: [
        RoleNameAlreadyExists,
        CatalogBackendError,
        ProjectIdNotFoundError,
        RoleExternalIdAlreadyExists,
    ]
}

#[derive(thiserror::Error, PartialEq, Debug)]
#[error("A role with name '{role_name}' already exists in project with id '{project_id}'")]
pub struct RoleNameAlreadyExists {
    pub role_name: String,
    pub project_id: ProjectId,
    pub stack: Vec<String>,
}
impl RoleNameAlreadyExists {
    #[must_use]
    pub fn new(role_name: impl Into<String>, project_id: ProjectId) -> Self {
        Self {
            role_name: role_name.into(),
            project_id,
            stack: Vec::new(),
        }
    }
}
impl_error_stack_methods!(RoleNameAlreadyExists);
impl From<RoleNameAlreadyExists> for ErrorModel {
    fn from(err: RoleNameAlreadyExists) -> Self {
        ErrorModel {
            r#type: "RoleNameAlreadyExists".to_string(),
            code: StatusCode::CONFLICT.as_u16(),
            message: err.to_string(),
            stack: err.stack,
            source: None,
        }
    }
}

#[derive(thiserror::Error, PartialEq, Debug)]
#[error("A role with external ID '{external_id}' already exists in project with id '{project_id}'")]
pub struct RoleExternalIdAlreadyExists {
    pub external_id: String,
    pub project_id: ProjectId,
    pub stack: Vec<String>,
}
impl RoleExternalIdAlreadyExists {
    #[must_use]
    pub fn new(external_id: impl Into<String>, project_id: ProjectId) -> Self {
        Self {
            external_id: external_id.into(),
            project_id,
            stack: Vec::new(),
        }
    }
}
impl_error_stack_methods!(RoleExternalIdAlreadyExists);
impl From<RoleExternalIdAlreadyExists> for ErrorModel {
    fn from(err: RoleExternalIdAlreadyExists) -> Self {
        ErrorModel {
            r#type: "RoleExternalIdAlreadyExists".to_string(),
            code: StatusCode::CONFLICT.as_u16(),
            message: err.to_string(),
            stack: err.stack,
            source: None,
        }
    }
}

// --------------------------- LIST ERROR ---------------------------
define_transparent_error! {
    pub enum ListRolesError,
    stack_message: "Error listing Roles catalog",
    variants: [
        CatalogBackendError,
        InvalidPaginationToken
    ]
}

// --------------------------- GET ROLE ERROR ---------------------------
define_transparent_error! {
    pub enum GetRoleError,
    stack_message: "Error getting Role from catalog",
    variants: [
        CatalogBackendError,
        InvalidPaginationToken,
        RoleIdNotFound,
    ]
}

impl From<ListRolesError> for GetRoleError {
    fn from(err: ListRolesError) -> Self {
        match err {
            ListRolesError::CatalogBackendError(e) => e.into(),
            ListRolesError::InvalidPaginationToken(e) => e.into(),
        }
    }
}

// --------------------------- DELETE ERROR ---------------------------
define_transparent_error! {
    pub enum DeleteRoleError,
    stack_message: "Error deleting role in catalog",
    variants: [
        CatalogBackendError,
        RoleIdNotFound
    ]
}

// --------------------------- UPDATE ERROR ----------------------
define_transparent_error! {
    pub enum UpdateRoleError,
    stack_message: "Error updating role in catalog",
    variants: [
        CatalogBackendError,
        RoleExternalIdAlreadyExists,
        RoleNameAlreadyExists,
        RoleIdNotFound,
    ]
}

// --------------------------- LIST ERROR ---------------------------
define_transparent_error! {
    pub enum SearchRolesError,
    stack_message: "Error searching Roles catalog",
    variants: [
        CatalogBackendError,
    ]
}

#[async_trait::async_trait]
pub trait CatalogRoleOps
where
    Self: CatalogStore,
{
    async fn create_role<'a>(
        role_id: RoleId,
        project_id: &ProjectId,
        role_name: &str,
        description: Option<&str>,
        external_id: Option<&str>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Arc<Role>, CreateRoleError> {
        let role = Self::create_role_impl(
            role_id,
            project_id,
            role_name,
            description,
            external_id,
            transaction,
        )
        .await?;
        let role_ref = Arc::new(role);
        Ok(role_ref)
    }

    async fn delete_role<'a>(
        project_id: &ProjectId,
        role_id: RoleId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<(), DeleteRoleError> {
        Self::delete_role_impl(project_id, role_id, transaction).await
    }

    /// If description is None, the description must be removed.
    async fn update_role<'a>(
        project_id: &ProjectId,
        role_id: RoleId,
        role_name: &str,
        description: Option<&str>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Arc<Role>, UpdateRoleError> {
        Self::update_role_impl(project_id, role_id, role_name, description, transaction)
            .await
            .map(Arc::new)
    }

    /// Update the external ID of the role.
    async fn set_role_external_id<'a>(
        project_id: &ProjectId,
        role_id: RoleId,
        external_id: Option<&str>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Arc<Role>, UpdateRoleError> {
        Self::set_role_external_id_impl(project_id, role_id, external_id, transaction)
            .await
            .map(Arc::new)
    }

    async fn list_roles<'a>(
        filter_project_id: Option<ProjectId>,
        filter_role_id: Option<Vec<RoleId>>,
        filter_name: Option<String>,
        pagination: PaginationQuery,
        catalog_state: Self::State,
    ) -> Result<ListRolesResponse, ListRolesError> {
        Self::list_roles_impl(
            filter_project_id,
            filter_role_id,
            filter_name,
            pagination,
            catalog_state,
        )
        .await
    }

    async fn get_role_by_id(
        project_id: &ProjectId,
        role_id: RoleId,
        catalog_state: Self::State,
    ) -> Result<Arc<Role>, GetRoleError> {
        let roles = Self::list_roles_impl(
            Some(project_id.clone()),
            Some(vec![role_id]),
            None,
            PaginationQuery::new_with_page_size(1),
            catalog_state,
        )
        .await?;

        if let Some(role) = roles.roles.into_iter().next() {
            Ok(role)
        } else {
            Err(RoleIdNotFound::new(role_id, project_id.clone()).into())
        }
    }

    async fn search_role(
        project_id: &ProjectId,
        search_term: &str,
        catalog_state: Self::State,
    ) -> Result<SearchRoleResponse, SearchRolesError> {
        Self::search_role_impl(project_id, search_term, catalog_state).await
    }
}

impl<T> CatalogRoleOps for T where T: CatalogStore {}
