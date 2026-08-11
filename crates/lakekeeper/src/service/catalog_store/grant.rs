//! Catalog-side grant storage: the `None` arm of
//! [`Authorizer::grants`](crate::service::authz::Authorizer::grants).
//!
//! [`CatalogGrantOps`] owns the transactions so handlers do not have to; the
//! `*_impl` methods on [`CatalogStore`] do the work inside a caller-supplied one.

use http::StatusCode;
use iceberg_ext::catalog::rest::ErrorModel;

use super::{CatalogStore, Transaction};
use crate::{
    api::iceberg::v1::PaginationQuery,
    service::{
        CatalogBackendError, DatabaseIntegrityError, InvalidPaginationToken, ProjectId,
        authz::{
            AppliedGrants, GrantFilter, GrantRow, GrantSpec, ListGrantsResultPage, UserOrRoleId,
        },
        define_transparent_error, impl_error_stack_methods, impl_from_with_detail,
    },
};

/// A grant named a principal or resource that does not exist.
#[derive(thiserror::Error, PartialEq, Eq, Debug)]
#[error("The principal or resource of a grant does not exist")]
pub struct GrantTargetNotFound {
    stack: Vec<String>,
}
impl_error_stack_methods!(GrantTargetNotFound);
impl GrantTargetNotFound {
    #[must_use]
    pub fn new() -> Self {
        Self { stack: Vec::new() }
    }
}
impl Default for GrantTargetNotFound {
    fn default() -> Self {
        Self::new()
    }
}
impl From<GrantTargetNotFound> for ErrorModel {
    fn from(err: GrantTargetNotFound) -> Self {
        ErrorModel::builder()
            .r#type("GrantTargetNotFound")
            .code(StatusCode::NOT_FOUND.as_u16())
            .message(err.to_string())
            .stack(err.stack)
            .build()
    }
}

/// Returned when a concurrent grant diff for the same resource is in progress and
/// this one could not take its turn within the timeout. The caller should retry.
///
/// Applying a diff removes and adds rows in one transaction. Two diffs that cross —
/// each revoking a grant the other adds — would otherwise wait on each other's
/// uncommitted rows and one would be killed as a deadlock victim, so diffs serialize
/// per resource via a transaction-scoped advisory lock. Serializing also keeps the
/// outcome equal to some order of the two requests: applied concurrently, both
/// revokes could fail to stick and leave a state neither caller asked for.
/// The message names no operation on purpose. This also reaches callers that never
/// applied a diff — deleting a user removes their grants without taking the lock, so it
/// can be chosen as the deadlock victim, and telling that caller their *grant apply*
/// conflicted would describe something they did not do.
#[derive(thiserror::Error, PartialEq, Eq, Debug)]
#[error("A concurrent change to the same grants is in progress — retry")]
pub struct GrantLockTimeout {
    stack: Vec<String>,
}
impl_error_stack_methods!(GrantLockTimeout);
impl GrantLockTimeout {
    #[must_use]
    pub fn new() -> Self {
        Self { stack: Vec::new() }
    }
}
impl Default for GrantLockTimeout {
    fn default() -> Self {
        Self::new()
    }
}
impl From<GrantLockTimeout> for ErrorModel {
    fn from(err: GrantLockTimeout) -> Self {
        ErrorModel::builder()
            .r#type("GrantLockTimeout")
            .code(StatusCode::CONFLICT.as_u16())
            .message(err.to_string())
            .stack(err.stack)
            .build()
    }
}

define_transparent_error! {
    /// Failure modes of applying a grant diff.
    pub enum ApplyGrantsStoreError,
    stack_message: "Error applying grants in catalog",
    variants: [
        CatalogBackendError,
        GrantTargetNotFound,
        GrantLockTimeout,
        DatabaseIntegrityError
    ]
}

define_transparent_error! {
    /// Failure modes of listing grants.
    pub enum ListGrantsStoreError,
    stack_message: "Error listing grants in catalog",
    variants: [
        CatalogBackendError,
        InvalidPaginationToken,
        DatabaseIntegrityError
    ]
}

/// Transaction-owning grant operations, available on every [`CatalogStore`].
#[async_trait::async_trait]
pub trait CatalogGrantOps
where
    Self: CatalogStore,
{
    /// Apply a grant diff in its own transaction. See
    /// [`CatalogStore::apply_grants_impl`] for the semantics.
    async fn apply_grants(
        writes: &[GrantSpec],
        deletes: &[GrantSpec],
        catalog_state: Self::State,
    ) -> crate::api::Result<AppliedGrants> {
        let mut t = Self::Transaction::begin_write(catalog_state).await?;
        let applied = Self::apply_grants_impl(writes, deletes, t.transaction()).await?;
        t.commit().await?;
        Ok(applied)
    }

    /// List direct grants matching `filter`.
    async fn list_grants(
        filter: &GrantFilter,
        pagination: PaginationQuery,
        catalog_state: Self::State,
    ) -> crate::api::Result<ListGrantsResultPage> {
        Ok(Self::list_grants_impl(filter, pagination, catalog_state).await?)
    }

    /// The evaluation-path fetch. See
    /// [`CatalogStore::list_grants_for_principals_impl`] for the semantics, including
    /// the requirement that `principals` already be the transitive set.
    async fn list_grants_for_principals(
        principals: &[UserOrRoleId],
        project_id: &ProjectId,
        catalog_state: Self::State,
    ) -> crate::api::Result<Vec<GrantRow>> {
        Ok(Self::list_grants_for_principals_impl(principals, project_id, catalog_state).await?)
    }
}

impl<T> CatalogGrantOps for T where T: CatalogStore {}
