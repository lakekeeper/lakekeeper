//! Catalog-side grant storage: the `None` arm of
//! [`Authorizer::grants`](crate::service::authz::Authorizer::grants).
//!
//! [`CatalogGrantOps`] owns the transactions so handlers do not have to; the
//! `*_impl` methods on [`CatalogStore`] do the work inside a caller-supplied one.

use std::sync::Arc;

use http::StatusCode;
use iceberg_ext::catalog::rest::ErrorModel;

use super::{CatalogStore, Transaction};
use crate::{
    api::{RequestMetadata, iceberg::v1::PaginationQuery},
    service::{
        CatalogBackendError, DatabaseIntegrityError, InvalidPaginationToken,
        authz::{
            AppliedGrants, Authorizer, GrantFilter, GrantResource, GrantSpec, ListGrantsResultPage,
            UserOrRoleId,
        },
        define_transparent_error,
        events::{EventDispatcher, GrantsChangedEvent},
        impl_error_stack_methods, impl_from_with_detail,
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
        DatabaseIntegrityError
    ]
}

/// The grant rows a freshly created resource starts with.
///
/// Empty — and cheap, without touching the store — when the authorizer keeps its own
/// grants, when it declares no bootstrap privileges for this kind of resource, or when
/// nobody is acting. An anonymous create has no owner to name: the server-bootstrap path
/// creates the default project that way when authentication is disabled.
///
/// The owner is the acting identity, so a request narrowed to a role makes the role the
/// owner rather than the user behind it.
pub(crate) fn bootstrap_grant_specs<A: Authorizer>(
    authorizer: &A,
    metadata: &RequestMetadata,
    resource: &GrantResource,
) -> Vec<GrantSpec> {
    if authorizer.grants().is_some() {
        return Vec::new();
    }
    let privileges = authorizer.bootstrap_grants(resource.resource_type());
    if privileges.is_empty() {
        return Vec::new();
    }
    let Some(owner) = metadata.actor().to_user_or_role() else {
        return Vec::new();
    };
    let owner = UserOrRoleId::from(&owner);
    privileges
        .iter()
        .map(|privilege| GrantSpec {
            principal: owner.clone(),
            resource: resource.clone(),
            privilege: (*privilege).to_string(),
        })
        .collect()
}

/// Write the bootstrap grants for a just-created resource, in that resource's own
/// transaction. Returns what was created, for the caller to emit after it commits.
///
/// Called after the authorizer's `create_*` hook so a hook failure still aborts first,
/// and inside the create transaction because the rows reference a resource no other
/// transaction can see yet.
pub(crate) async fn write_bootstrap_grants<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    metadata: &RequestMetadata,
    resource: &GrantResource,
    transaction: <C::Transaction as Transaction<C::State>>::Transaction<'_>,
) -> Result<Vec<GrantSpec>, ApplyGrantsStoreError> {
    let writes = bootstrap_grant_specs(authorizer, metadata, resource);
    if writes.is_empty() {
        return Ok(Vec::new());
    }
    C::bootstrap_grants_impl(&writes, transaction).await
}

/// Announce bootstrap grants after their transaction committed.
///
/// Grant rows born with a resource still have to reach the audit log: the backend derives
/// its per-grant records from grant events, so a consumer mirroring them would otherwise
/// never learn the creator holds anything. Carries no removals — creation revokes nothing.
pub(crate) async fn emit_bootstrap_grants(
    dispatcher: &EventDispatcher,
    request_metadata: Arc<RequestMetadata>,
    created: Vec<GrantSpec>,
) {
    if created.is_empty() {
        return;
    }
    dispatcher
        .grants_changed(GrantsChangedEvent::new(
            Vec::new(),
            created,
            request_metadata,
        ))
        .await;
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

    // Deliberately no wrapper for `list_grants_on_resources_impl`: the
    // evaluation-path fetch gets its ergonomic surface with its first caller,
    // shaped by what that caller needs.
}

impl<T> CatalogGrantOps for T where T: CatalogStore {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        request_metadata::RequestMetadataTestBuilder,
        service::{
            RoleId, WarehouseId,
            authn::{Actor, UserId},
            authz::{AllowAllAuthorizer, tests::HidingAuthorizer},
        },
    };

    fn as_user(user: &UserId) -> RequestMetadata {
        RequestMetadataTestBuilder::builder()
            .actor(Actor::Principal(user.clone()))
            .build()
    }

    #[test]
    fn a_full_vocabulary_alone_confers_no_ownership() {
        // AllowAll publishes every privilege at every level and stores grants in the
        // catalog, yet declares no bootstrap privileges: an authorizer has to opt in
        // before creation starts writing rows.
        let authorizer = AllowAllAuthorizer::default();
        let metadata = as_user(&UserId::new_unchecked("oidc", "alice"));
        assert_eq!(
            bootstrap_grant_specs(
                &authorizer,
                &metadata,
                &GrantResource::Warehouse(WarehouseId::new_random())
            ),
            Vec::new()
        );
    }

    #[test]
    fn the_creating_user_gets_one_row_per_declared_privilege() {
        let authorizer = HidingAuthorizer::new().with_bootstrap_grants(&["ownership", "modify"]);
        let alice = UserId::new_unchecked("oidc", "alice");
        let warehouse_id = WarehouseId::new_random();
        let resource = GrantResource::Warehouse(warehouse_id);

        assert_eq!(
            bootstrap_grant_specs(&authorizer, &as_user(&alice), &resource),
            vec![
                GrantSpec {
                    principal: UserOrRoleId::User(alice.clone()),
                    resource: resource.clone(),
                    privilege: "ownership".to_string(),
                },
                GrantSpec {
                    principal: UserOrRoleId::User(alice),
                    resource,
                    privilege: "modify".to_string(),
                },
            ]
        );
    }

    #[test]
    fn an_assumed_role_owns_what_it_creates() {
        // The acting identity, not the user behind it: a token narrowed to a role must
        // not make the whole user an owner.
        let authorizer = HidingAuthorizer::new().with_bootstrap_grants(&["ownership"]);
        let role_id = RoleId::new_random();
        let metadata = RequestMetadataTestBuilder::builder()
            .actor(Actor::Role {
                principal: UserId::new_unchecked("oidc", "alice"),
                assumed_role: crate::service::Role::new_random_with_id(role_id).into(),
            })
            .build();

        let specs = bootstrap_grant_specs(
            &authorizer,
            &metadata,
            &GrantResource::Project(crate::service::ProjectId::new_random()),
        );
        assert_eq!(
            specs
                .iter()
                .map(|spec| spec.principal.clone())
                .collect::<Vec<_>>(),
            vec![UserOrRoleId::Role(role_id)]
        );
    }

    #[test]
    fn an_anonymous_create_leaves_no_owner() {
        // The server-bootstrap path creates the default project this way when
        // authentication is disabled: there is nobody to own it.
        let authorizer = HidingAuthorizer::new().with_bootstrap_grants(&["ownership"]);
        let metadata = RequestMetadataTestBuilder::builder()
            .actor(Actor::Anonymous)
            .build();
        assert_eq!(
            bootstrap_grant_specs(
                &authorizer,
                &metadata,
                &GrantResource::Warehouse(WarehouseId::new_random())
            ),
            Vec::new()
        );
    }

    #[test]
    fn declaring_nothing_writes_nothing() {
        let authorizer = HidingAuthorizer::new();
        assert_eq!(
            bootstrap_grant_specs(
                &authorizer,
                &as_user(&UserId::new_unchecked("oidc", "alice")),
                &GrantResource::Warehouse(WarehouseId::new_random())
            ),
            Vec::new()
        );
    }
}
