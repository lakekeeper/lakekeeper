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
        CatalogBackendError, DatabaseIntegrityError, InvalidPaginationToken,
        authz::{
            AppliedGrants, GrantCandidate, GrantFilter, GrantSpec, GrantSubtreeFilter,
            GrantSubtreeRoot, ListGrantsResultPage, ListSubtreeGrantsResultPage,
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

/// A grant in `writes` named a user that does not exist.
///
/// Checked before the insert so the error can name the id — the foreign key behind it
/// reports only that *something* was missing. Names the first missing id, not all of
/// them, exactly as the role validation names its first missing role. Deletes stay
/// unvalidated: every existing grant's user row exists by foreign key, so a check
/// could never save a revoke — only turn the harmless no-op of revoking from a user
/// that never existed into an error.
#[derive(thiserror::Error, PartialEq, Eq, Debug)]
#[error("User `{user}` does not exist")]
pub struct GrantUserNotFound {
    user: String,
    stack: Vec<String>,
}
impl_error_stack_methods!(GrantUserNotFound);
impl GrantUserNotFound {
    #[must_use]
    pub fn new(user: impl Into<String>) -> Self {
        Self {
            user: user.into(),
            stack: Vec::new(),
        }
    }
}
impl From<GrantUserNotFound> for ErrorModel {
    fn from(err: GrantUserNotFound) -> Self {
        ErrorModel::builder()
            .r#type("GrantUserNotFound")
            .code(StatusCode::BAD_REQUEST.as_u16())
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

/// A namespace spans more namespaces than the subtree operations read.
///
/// Reading a subtree's grants costs in proportion to the namespaces it spans, so an
/// oversized one is refused before that read starts rather than left to run. The caller
/// addresses a namespace further down, or uses the warehouse-rooted operation, which is
/// served by an index and carries no such bound.
#[derive(thiserror::Error, PartialEq, Eq, Debug)]
#[error(
    "This namespace spans {namespaces} namespaces; subtree grant operations read up to \
     {limit}. Address a namespace further down, or use the warehouse-rooted operation."
)]
pub struct GrantSubtreeTooLarge {
    namespaces: u64,
    limit: u64,
    stack: Vec<String>,
}
impl_error_stack_methods!(GrantSubtreeTooLarge);
impl GrantSubtreeTooLarge {
    #[must_use]
    pub fn new(namespaces: u64, limit: u64) -> Self {
        Self {
            namespaces,
            limit,
            stack: Vec::new(),
        }
    }
}
impl From<GrantSubtreeTooLarge> for ErrorModel {
    fn from(err: GrantSubtreeTooLarge) -> Self {
        ErrorModel::builder()
            .r#type("GrantSubtreeTooLarge")
            .code(StatusCode::BAD_REQUEST.as_u16())
            .message(err.to_string())
            .stack(err.stack)
            .build()
    }
}

/// A subtree read did not finish inside the time the database allows it.
///
/// Distinct from [`GrantSubtreeTooLarge`], which is decided from the namespace count
/// before the read starts: this is what remains when a subtree spans few namespaces but
/// holds very many grants, or when the catalog database is under load. Retrying the same
/// request unchanged reaches the same bound.
#[derive(thiserror::Error, PartialEq, Eq, Debug)]
#[error(
    "Reading this subtree's grants did not finish in time. Narrow the request with a \
     filter or a root further down; if the subtree is modest, the catalog database may \
     be under load."
)]
pub struct GrantSubtreeReadTimeout {
    stack: Vec<String>,
}
impl_error_stack_methods!(GrantSubtreeReadTimeout);
impl GrantSubtreeReadTimeout {
    #[must_use]
    pub fn new() -> Self {
        Self { stack: Vec::new() }
    }
}
impl Default for GrantSubtreeReadTimeout {
    fn default() -> Self {
        Self::new()
    }
}
impl From<GrantSubtreeReadTimeout> for ErrorModel {
    fn from(err: GrantSubtreeReadTimeout) -> Self {
        ErrorModel::builder()
            .r#type("GrantSubtreeReadTimeout")
            .code(StatusCode::BAD_REQUEST.as_u16())
            .message(err.to_string())
            .stack(err.stack)
            .build()
    }
}

/// A subtree revoke matched more grants than one call may remove.
///
/// Nothing was removed. The caller either narrows the filter or re-issues with
/// partial revocation allowed and loops until the response reports no more matches.
/// Refusing by default is what keeps a caller from starting a multi-call operation
/// without knowing each call is its own transaction.
#[derive(thiserror::Error, PartialEq, Eq, Debug)]
#[error(
    "More than {limit} grants match; nothing was revoked. Narrow the filter, or allow \
     partial revocation and repeat the request until no matches remain."
)]
pub struct GrantRevokeBatchTooLarge {
    limit: usize,
    stack: Vec<String>,
}
impl_error_stack_methods!(GrantRevokeBatchTooLarge);
impl GrantRevokeBatchTooLarge {
    #[must_use]
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            stack: Vec::new(),
        }
    }
}
impl From<GrantRevokeBatchTooLarge> for ErrorModel {
    fn from(err: GrantRevokeBatchTooLarge) -> Self {
        ErrorModel::builder()
            .r#type("GrantRevokeBatchTooLarge")
            .code(StatusCode::BAD_REQUEST.as_u16())
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
        GrantUserNotFound,
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
        GrantSubtreeTooLarge,
        GrantSubtreeReadTimeout,
        DatabaseIntegrityError
    ]
}

define_transparent_error! {
    /// Failure modes of removing a batch of grants.
    ///
    /// No `GrantRevokeBatchTooLarge`: the bound is decided before anything is read, from
    /// the candidates the caller was handed, not by the delete.
    pub enum RevokeSubtreeGrantsStoreError,
    stack_message: "Error revoking a batch of grants",
    variants: [
        CatalogBackendError,
        GrantLockTimeout,
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

    /// One page of the direct grants held anywhere under `root`.
    async fn list_grants_in_subtree(
        root: GrantSubtreeRoot,
        filter: &GrantSubtreeFilter,
        pagination: PaginationQuery,
        catalog_state: Self::State,
    ) -> crate::api::Result<ListSubtreeGrantsResultPage> {
        Ok(Self::list_grants_in_subtree_impl(root, filter, pagination, catalog_state).await?)
    }

    /// Remove the grants named by `candidates` in their own transaction. See
    /// [`CatalogStore::revoke_grant_candidates_impl`].
    async fn revoke_grant_candidates(
        root: GrantSubtreeRoot,
        candidates: &[GrantCandidate],
        catalog_state: Self::State,
    ) -> crate::api::Result<Vec<GrantSpec>> {
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let mut t = Self::Transaction::begin_write(catalog_state).await?;
        let removed = Self::revoke_grant_candidates_impl(root, candidates, t.transaction()).await?;
        t.commit().await?;
        Ok(removed)
    }

    // Deliberately no wrapper for `select_subtree_grant_candidates_impl`: its caller
    // folds the read into the authorization result, which needs the store's own error
    // type rather than an already-rendered one.

    // Deliberately no wrapper for `list_grants_on_resources_impl`: the
    // evaluation-path fetch gets its ergonomic surface with its first caller,
    // shaped by what that caller needs.
}

impl<T> CatalogGrantOps for T where T: CatalogStore {}

// Grant writes that happen on behalf of another operation — the rows a resource is born
// with — surface through that operation's error. Each variant keeps its own status: a
// missing user is the caller's 400, a lock conflict their retriable 409.
crate::service::events::impl_authorization_failure_source!(
    ApplyGrantsStoreError => InternalCatalogError
);

// A subtree revoke reads its candidates inside the authorization result, so that a store
// failure there is recorded and reported like every other failure the gate covers rather
// than escaping it.
crate::service::events::impl_authorization_failure_source!(
    ListGrantsStoreError => InternalCatalogError
);
