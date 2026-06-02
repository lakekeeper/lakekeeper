use std::{collections::HashSet, sync::Arc};

use http::StatusCode;
use iceberg_ext::catalog::rest::ErrorModel;

use crate::{
    ProjectId,
    api::management::v1::user::{UserLastUpdatedWith, UserType},
    service::{
        ArcProjectId, CatalogBackendError, CatalogStore, DatabaseIntegrityError, RoleId, RoleIdent,
        RoleNameAlreadyExists, RoleProviderId, Transaction,
        authn::{UserId, UserIdRef},
        define_transparent_error,
        events::{EventDispatcher, RoleMembersSyncedEvent, UserRoleAssignmentsSyncedEvent},
        identifier::role::ArcRoleIdent,
        impl_error_stack_methods, impl_from_with_detail, role_assignments_cache, role_cache,
    },
};

// ============================================================================
// Request / response types
// ============================================================================

/// User data supplied by an external role provider (LDAP, SCIM, …) when
/// assigning a user to a role.  The implementation upserts the user into the
/// `users` table before writing the `user_role` row.
#[derive(Debug, Clone)]
pub struct CatalogUserRoleAssignmentUser<'a> {
    pub user_id: &'a UserIdRef,
    /// Display name. When `None` the user is stored with `"Nameless User with id {id}"`
    /// as fallback for new rows, and the existing name is preserved for updates.
    pub name: Option<&'a str>,
    pub email: Option<&'a str>,
    /// User type. When `None` defaults to `Human` for new users and preserves
    /// the existing type for updates.
    pub user_type: Option<UserType>,
    /// Mechanism that triggered the upsert; stored in `last_updated_with` on
    /// the `users` row. Allows distinguishing role-provider syncs from future
    /// callers such as a SCIM endpoint.
    pub updated_with: UserLastUpdatedWith,
}

/// Role data supplied by an external provider when syncing a user's assignments.
///
/// The implementation upserts the role (creating it with a fresh [`RoleId`] if
/// absent, or leaving it unchanged if it already exists) before writing the
/// `user_role` row.  `ident.provider_id()` must equal the `provider_id`
/// argument passed to [`CatalogRoleAssignmentOps::sync_user_role_assignments_by_provider`];
/// passing a mismatched provider returns [`RoleProviderMismatchError`].
#[derive(Debug, Clone)]
pub struct CatalogRoleForAssignment<'a> {
    pub ident: &'a Arc<RoleIdent>,
    /// Display name. When `None` the role is stored / kept with its `source_id`
    /// as the name for new rows, and the existing name is preserved for updates.
    pub name: Option<&'a str>,
    pub description: Option<&'a str>,
}

// ----------------------------------------------------------------------------
// List / result item types
// ----------------------------------------------------------------------------

/// A role assignment as seen from a **user's** perspective.
///
/// Carries all three identifiers so different consumers can pick what they need:
/// - `role_id` — opaque UUID used by internal authorizers (e.g. OpenFGA).
/// - `role_ident` + `project_id` — stable external identifiers used by
///   external authorizers (e.g. LDAP / SCIM-based providers) that need to
///   correlate Lakekeeper roles back to their source system.
#[derive(Debug, Clone)]
pub struct AssignedRole {
    pub role_id: RoleId,
    pub role_ident: ArcRoleIdent,
    pub project_id: ArcProjectId,
}

/// A member as seen from a **role's** perspective.
#[derive(Debug, Clone)]
pub struct AssignedUser {
    pub user_id: UserIdRef,
}

/// Sync metadata for one `(project_id, provider_id)` pair as recorded in the
/// external-provider sync log for a user.
///
/// The sync timestamp is at the `(user_id, project_id, provider_id)` level —
/// one entry covers **all** roles from that provider in that project.
#[derive(Debug, Clone)]
pub struct UserProviderSyncInfo {
    pub project_id: ArcProjectId,
    pub provider_id: RoleProviderId,
    /// When this provider last successfully synced the user's assignments for
    /// `project_id`.  Role providers compare this against their TTL to decide
    /// whether a re-fetch from the source system is required.
    pub synced_at: chrono::DateTime<chrono::Utc>,
}

// ----------------------------------------------------------------------------
// Aggregate results
// ----------------------------------------------------------------------------

/// Result of [`CatalogRoleAssignmentOps::list_role_assignments_for_user`].
#[derive(Debug, Clone)]
pub struct ListUserRoleAssignmentsResult {
    pub roles: Vec<AssignedRole>,
    /// One [`UserProviderSyncInfo`] entry per `(project_id, provider_id)` pair
    /// for which the user currently has at least one assignment row.  Pairs
    /// whose assignments have all been removed are not included even if a sync
    /// was previously recorded for them.  Empty when the user has no
    /// externally-managed assignments.
    pub provider_sync_times: Vec<UserProviderSyncInfo>,
}

/// Result of [`CatalogRoleAssignmentOps::list_role_assignments_for_role`] and
/// [`CatalogRoleAssignmentOps::list_role_assignments_for_role_by_ident`].
///
/// Carries the role's own identifiers so that every consumer — cache layers,
/// event listeners, external callers — can work with the result without a
/// second look-up.
#[derive(Debug, Clone)]
pub struct ListRoleMembersResult {
    /// The UUID of this role.
    pub role_id: RoleId,
    /// The project that owns this role.
    pub project_id: ArcProjectId,
    /// The provider-scoped identifier of this role.
    pub role_ident: ArcRoleIdent,
    pub members: Vec<AssignedUser>,
    /// When a provider last successfully synced this role's member list.
    /// `None` if the role's members have never been synced by an external
    /// provider (e.g. all assignments are manually managed).
    pub last_synced_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Outcome of a [`CatalogRoleAssignmentOps::sync_role_members_by_ident`] call.
#[derive(Debug, Clone)]
pub struct SyncRoleMembersResult {
    /// The UUID of the role whose members were synced.
    ///
    /// Returned so the caller (and the default trait impl) can invalidate the
    /// role-members cache without a separate role-lookup round-trip.
    pub role_id: RoleId,
    /// Members for whom a new `user_role` row was inserted.
    pub added: Vec<AssignedUser>,
    /// Members for whom the `user_role` row was removed because they were
    /// absent from `members`.
    pub removed: Vec<AssignedUser>,
    /// The timestamp written to the role member sync log by this sync run.
    pub synced_at: chrono::DateTime<chrono::Utc>,
}

/// Outcome of a [`CatalogRoleAssignmentOps::sync_user_role_assignments_by_provider`] call.
#[derive(Debug, Clone)]
pub struct SyncUserRoleAssignmentsResult {
    /// IDs of roles newly assigned to the user.
    pub added: Vec<RoleId>,
    /// IDs of roles removed from the user (were assigned via `provider_id`,
    /// are no longer present in `roles`).
    pub removed: Vec<RoleId>,
    /// The timestamp written to the user role sync log for
    /// `(user_id, project_id, provider_id)` by this sync run.
    pub synced_at: chrono::DateTime<chrono::Utc>,
    /// The complete, authoritative role assignment list for this user after the
    /// sync run, covering **all** providers (not just `provider_id`).
    ///
    /// Returned so the caller can build [`ListUserRoleAssignmentsResult`]
    /// without a separate DB round-trip.
    pub all_roles: Vec<AssignedRole>,
    /// One [`UserProviderSyncInfo`] per `(project_id, provider_id)` pair for
    /// which the user has at least one assignment row after this sync run.
    /// Pairs with no remaining assignments are omitted even if a prior sync was
    /// recorded for them.
    ///
    /// Matches the `provider_sync_times` field of [`ListUserRoleAssignmentsResult`].
    pub provider_sync_times: Vec<UserProviderSyncInfo>,
}

// ============================================================================
// Validated input wrappers — uniqueness encoded in the type
// ============================================================================

/// A member with the given `user_id` appears more than once in the input slice.
#[derive(Debug)]
pub struct DuplicateMemberError {
    pub user_id: String,
    pub stack: Vec<String>,
}
impl_error_stack_methods!(DuplicateMemberError);
impl DuplicateMemberError {
    fn new(user_id: impl Into<String>) -> Self {
        Self {
            user_id: user_id.into(),
            stack: Vec::new(),
        }
    }
}
impl std::error::Error for DuplicateMemberError {}
impl std::fmt::Display for DuplicateMemberError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Duplicate member user_id in sync request: '{}'",
            self.user_id
        )
    }
}
impl From<DuplicateMemberError> for ErrorModel {
    fn from(err: DuplicateMemberError) -> Self {
        ErrorModel::builder()
            .r#type("DuplicateMemberError")
            .code(StatusCode::BAD_REQUEST.as_u16())
            .message(format!(
                "Duplicate member user_id in sync request: '{}'",
                err.user_id
            ))
            .stack(err.stack)
            .build()
    }
}

/// A role with the given `source_id` appears more than once in the input slice.
#[derive(Debug)]
pub struct DuplicateRoleError {
    pub source_id: String,
    pub stack: Vec<String>,
}
impl_error_stack_methods!(DuplicateRoleError);
impl DuplicateRoleError {
    fn new(source_id: impl Into<String>) -> Self {
        Self {
            source_id: source_id.into(),
            stack: Vec::new(),
        }
    }
}
impl std::error::Error for DuplicateRoleError {}
impl std::fmt::Display for DuplicateRoleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Duplicate role source_id in sync request: '{}'",
            self.source_id
        )
    }
}
impl From<DuplicateRoleError> for ErrorModel {
    fn from(err: DuplicateRoleError) -> Self {
        ErrorModel::builder()
            .r#type("DuplicateRoleError")
            .code(StatusCode::BAD_REQUEST.as_u16())
            .message(format!(
                "Duplicate role source_id in sync request: '{}'",
                err.source_id
            ))
            .stack(err.stack)
            .build()
    }
}

/// A role in the input slice has a `provider_id` that differs from the
/// `provider_id` argument supplied to the sync call.
#[derive(Debug)]
pub struct RoleProviderMismatchError {
    pub expected: RoleProviderId,
    pub found: RoleProviderId,
    pub source_id: String,
    pub stack: Vec<String>,
}
impl_error_stack_methods!(RoleProviderMismatchError);
impl RoleProviderMismatchError {
    fn new(
        expected: &RoleProviderId,
        found: &RoleProviderId,
        source_id: impl Into<String>,
    ) -> Self {
        Self {
            expected: expected.clone(),
            found: found.clone(),
            source_id: source_id.into(),
            stack: Vec::new(),
        }
    }
}
impl std::error::Error for RoleProviderMismatchError {}
impl std::fmt::Display for RoleProviderMismatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Role '{}' has provider_id '{}' but the sync call targets provider '{}'",
            self.source_id, self.found, self.expected
        )
    }
}
impl From<RoleProviderMismatchError> for ErrorModel {
    fn from(err: RoleProviderMismatchError) -> Self {
        ErrorModel::builder()
            .r#type("RoleProviderMismatchError")
            .code(StatusCode::BAD_REQUEST.as_u16())
            .message(format!(
                "Role '{}' has provider_id '{}' but the sync call targets provider '{}'",
                err.source_id, err.found, err.expected
            ))
            .stack(err.stack)
            .build()
    }
}

/// A validated, ordered collection of role members where every `user_id` is unique.
///
/// Constructable via [`UniqueMembers::try_from_slice`] (validates, returns an error
/// on the first duplicate) or [`UniqueMembers::from_unchecked`] (crate-internal,
/// skips validation when uniqueness is guaranteed by the caller — i.e. after the
/// catalog-store layer has already validated).
pub struct UniqueMembers<'slice, 'data> {
    inner: &'slice [CatalogUserRoleAssignmentUser<'data>],
}
impl std::fmt::Debug for UniqueMembers<'_, '_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("UniqueMembers").field(&self.inner).finish()
    }
}
impl<'slice, 'data> UniqueMembers<'slice, 'data> {
    /// Validates that every `user_id` in `members` is unique.
    /// Returns [`DuplicateMemberError`] naming the first duplicate found.
    pub fn try_from_slice(
        members: &'slice [CatalogUserRoleAssignmentUser<'data>],
    ) -> Result<Self, DuplicateMemberError> {
        let mut seen = HashSet::with_capacity(members.len());
        for m in members {
            let key = m.user_id.to_string();
            if !seen.insert(key.clone()) {
                return Err(DuplicateMemberError::new(key));
            }
        }
        Ok(Self { inner: members })
    }

    /// Wraps `members` without validating uniqueness.
    ///
    /// # Caller contract
    /// Every `user_id` in `members` must be unique. Violating this contract
    /// causes a runtime database error, not memory unsafety.
    #[cfg(feature = "sqlx-postgres")]
    pub(crate) fn from_unchecked(members: &'slice [CatalogUserRoleAssignmentUser<'data>]) -> Self {
        Self { inner: members }
    }
}
impl<'data> std::ops::Deref for UniqueMembers<'_, 'data> {
    type Target = [CatalogUserRoleAssignmentUser<'data>];
    fn deref(&self) -> &Self::Target {
        self.inner
    }
}

/// A validated, ordered collection of roles where every `source_id` is unique.
///
/// Constructable via [`UniqueRoles::try_from_slice`] (validates, returns an error
/// on the first duplicate) or [`UniqueRoles::from_unchecked`] (crate-internal).
pub struct UniqueRoles<'slice, 'data> {
    inner: &'slice [CatalogRoleForAssignment<'data>],
}
impl std::fmt::Debug for UniqueRoles<'_, '_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("UniqueRoles").field(&self.inner).finish()
    }
}
impl<'slice, 'data> UniqueRoles<'slice, 'data> {
    /// Validates that every `source_id` in `roles` is unique.
    /// Returns [`DuplicateRoleError`] naming the first duplicate found.
    pub fn try_from_slice(
        roles: &'slice [CatalogRoleForAssignment<'data>],
    ) -> Result<Self, DuplicateRoleError> {
        let mut seen = HashSet::with_capacity(roles.len());
        for r in roles {
            let key = r.ident.source_id().to_string();
            if !seen.insert(key.clone()) {
                return Err(DuplicateRoleError::new(key));
            }
        }
        Ok(Self { inner: roles })
    }

    /// Wraps `roles` without validating uniqueness.
    ///
    /// # Caller contract
    /// Every `source_id` in `roles` must be unique. Violating this contract
    /// causes a runtime database error, not memory unsafety.
    #[cfg(feature = "sqlx-postgres")]
    pub(crate) fn from_unchecked(roles: &'slice [CatalogRoleForAssignment<'data>]) -> Self {
        Self { inner: roles }
    }
}
impl<'data> std::ops::Deref for UniqueRoles<'_, 'data> {
    type Target = [CatalogRoleForAssignment<'data>];
    fn deref(&self) -> &Self::Target {
        self.inner
    }
}

// ============================================================================
// Error types
// ============================================================================

define_transparent_error! {
    pub enum SyncRoleMembersError,
    stack_message: "Error syncing role members",
    variants: [
        CatalogBackendError,
        DatabaseIntegrityError,
        RoleNameAlreadyExists,
        DuplicateMemberError
    ]
}

define_transparent_error! {
    pub enum SyncUserRoleAssignmentsError,
    stack_message: "Error syncing user role assignments",
    variants: [
        CatalogBackendError,
        RoleNameAlreadyExists,
        DuplicateRoleError,
        RoleProviderMismatchError
    ]
}

// ============================================================================
// Trait
// ============================================================================

#[async_trait::async_trait]
pub trait CatalogRoleAssignmentOps
where
    Self: CatalogStore,
{
    // -----------------------------------------------------------------------
    // WRITE: bulk sync
    // -----------------------------------------------------------------------

    /// Atomically replace the complete member list of a role, creating the role
    /// if it does not yet exist.
    ///
    /// Designed for external role providers (LDAP, SCIM) that supply the
    /// authoritative member list and need Lakekeeper to converge to it:
    ///
    /// 1. Upsert the role for `project_id`:
    ///    create with a fresh [`RoleId`] if absent, leave unchanged if present.
    /// 2. Upsert every user in `members`.
    /// 3. Add assignment rows for newly assigned users.
    /// 4. Delete assignment rows for users absent from `members`.
    /// 5. Record the sync timestamp in the role member sync log.
    async fn sync_role_members_by_ident<'a>(
        project_id: &ProjectId,
        role: &CatalogRoleForAssignment<'_>,
        members: &[CatalogUserRoleAssignmentUser<'_>],
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<SyncRoleMembersResult, SyncRoleMembersError> {
        UniqueMembers::try_from_slice(members)?;
        Self::sync_role_members_by_ident_impl(project_id, role, members, transaction).await
    }

    /// Atomically converge a user's role assignments from one provider to an
    /// authoritative list, creating roles that do not yet exist.
    ///
    /// After querying "which groups does user U belong to in provider P?", call
    /// this method with the result to converge the store to that truth.
    ///
    /// Scoped to `(user_id, project_id, provider_id)` — assignments to roles
    /// owned by **other providers** for the same user are never touched.
    ///
    /// Steps performed in a single transaction:
    /// 1. Upsert the user.
    /// 2. Upsert each role in `roles` for `project_id`:
    ///    create with a fresh [`RoleId`] if absent, leave unchanged if present.
    /// 3. Add assignment rows for roles not yet assigned.
    /// 4. Remove assignment rows for roles (from `provider_id`) that are no
    ///    longer in `roles`.
    /// 5. Record the sync timestamp in the user role sync log for
    ///    `(user_id, project_id, provider_id)`.
    ///
    /// Returns [`RoleProviderMismatchError`] if any role's `ident.provider_id()`
    /// differs from `provider_id`.
    async fn sync_user_role_assignments_by_provider<'a>(
        user: CatalogUserRoleAssignmentUser<'_>,
        project_id: &ProjectId,
        provider_id: &RoleProviderId,
        roles: &[CatalogRoleForAssignment<'_>],
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<SyncUserRoleAssignmentsResult, SyncUserRoleAssignmentsError> {
        UniqueRoles::try_from_slice(roles)?;
        if let Some(r) = roles.iter().find(|r| r.ident.provider_id() != provider_id) {
            return Err(RoleProviderMismatchError::new(
                provider_id,
                r.ident.provider_id(),
                r.ident.source_id().to_string(),
            )
            .into());
        }
        Self::sync_user_role_assignments_by_provider_impl(
            &user,
            project_id,
            provider_id,
            roles,
            transaction,
        )
        .await
    }

    // -----------------------------------------------------------------------
    // WRITE: standalone sync (owns its own transaction)
    // -----------------------------------------------------------------------

    /// Sync a role's complete member list, commit, populate the cache with the
    /// authoritative result, and dispatch a [`RoleMembersSyncedEvent`].
    ///
    /// This is the preferred entry point for external role providers (LDAP,
    /// SCIM) that drive the sync and want to serve cached data immediately
    /// after.  Unlike [`sync_role_members_by_ident`], this method:
    ///
    /// 1. Opens and commits its own transaction.
    /// 2. Builds [`ListRoleMembersResult`] directly from the inputs — the
    ///    `members` slice is by definition the complete new member list, so no
    ///    extra DB round-trip is needed.
    /// 3. Inserts the result into `ROLE_MEMBERS_CACHE`.
    /// 4. Emits [`RoleMembersSyncedEvent`] via `dispatcher`
    async fn sync_role_members(
        project_id: &ArcProjectId,
        role: &CatalogRoleForAssignment<'_>,
        members: &[CatalogUserRoleAssignmentUser<'_>],
        catalog_state: Self::State,
        dispatcher: &EventDispatcher,
    ) -> crate::api::Result<Arc<ListRoleMembersResult>> {
        UniqueMembers::try_from_slice(members).map_err(SyncRoleMembersError::from)?;
        let mut t = Self::Transaction::begin_write(catalog_state).await?;
        let sync_result =
            Self::sync_role_members_by_ident_impl(project_id, role, members, t.transaction())
                .await?;
        t.commit().await?;

        // Build the authoritative member list directly from the sync inputs.
        // `members` is exactly the complete new state — no DB read required.
        let list_result = Arc::new(ListRoleMembersResult {
            role_id: sync_result.role_id,
            project_id: project_id.clone(),
            role_ident: role.ident.clone(),
            members: members
                .iter()
                .map(|m| AssignedUser {
                    user_id: m.user_id.clone(),
                })
                .collect(),
            last_synced_at: Some(sync_result.synced_at),
        });

        role_assignments_cache::role_members_cache_insert(
            sync_result.role_id,
            Arc::clone(&list_result),
        )
        .await;
        for user_id in sync_result
            .added
            .iter()
            .chain(sync_result.removed.iter())
            .map(|u| &u.user_id)
        {
            role_assignments_cache::user_assignments_cache_invalidate(user_id).await;
        }

        let event = RoleMembersSyncedEvent {
            added: sync_result.added.into_iter().map(|u| u.user_id).collect(),
            removed: sync_result.removed.into_iter().map(|u| u.user_id).collect(),
            synced_at: sync_result.synced_at,
            result: Arc::clone(&list_result),
        };
        let dispatcher = dispatcher.clone();
        tokio::spawn(async move {
            dispatcher.role_members_synced(event).await;
        });

        Ok(list_result)
    }

    /// Sync a user's role assignments for one provider scope, commit, populate
    /// the cache with fresh data, and dispatch a
    /// [`UserRoleAssignmentsSyncedEvent`].
    ///
    /// This is the preferred entry point for external role providers (LDAP,
    /// SCIM) that drive the sync and want to serve cached data immediately
    /// after.  Unlike [`sync_user_role_assignments_by_provider`], this method:
    ///
    /// 1. Opens and commits its own transaction.
    /// 2. Builds [`ListUserRoleAssignmentsResult`] directly from the data
    ///    returned by the impl — `all_roles` and `provider_sync_times` contain
    ///    the authoritative post-sync state across all providers, so no extra
    ///    DB round-trip is needed.
    /// 3. Inserts the result into `USER_ASSIGNMENTS_CACHE`.
    /// 4. Emits [`UserRoleAssignmentsSyncedEvent`] via `dispatcher` so that
    ///    listeners can invalidate per-role member caches on this and other
    ///    instances.
    async fn sync_user_role_assignments(
        user: CatalogUserRoleAssignmentUser<'_>,
        project_id: &ProjectId,
        provider_id: &RoleProviderId,
        roles: &[CatalogRoleForAssignment<'_>],
        catalog_state: Self::State,
        dispatcher: &EventDispatcher,
    ) -> crate::api::Result<Arc<ListUserRoleAssignmentsResult>> {
        UniqueRoles::try_from_slice(roles).map_err(SyncUserRoleAssignmentsError::from)?;
        if let Some(r) = roles.iter().find(|r| r.ident.provider_id() != provider_id) {
            return Err(
                SyncUserRoleAssignmentsError::from(RoleProviderMismatchError::new(
                    provider_id,
                    r.ident.provider_id(),
                    r.ident.source_id().to_string(),
                ))
                .into(),
            );
        }
        let mut t = Self::Transaction::begin_write(catalog_state).await?;
        let sync_result = Self::sync_user_role_assignments_by_provider_impl(
            &user,
            project_id,
            provider_id,
            roles,
            t.transaction(),
        )
        .await?;
        t.commit().await?;

        let list_result = Arc::new(ListUserRoleAssignmentsResult {
            roles: sync_result.all_roles,
            provider_sync_times: sync_result.provider_sync_times,
        });

        // Update the cache directly after the commit, before dispatching the
        // event (mirrors the warehouse / namespace cache-on-read pattern).
        role_assignments_cache::user_assignments_cache_insert(
            user.user_id,
            Arc::clone(&list_result),
        )
        .await;
        for role_id in sync_result
            .added
            .iter()
            .chain(sync_result.removed.iter())
            .copied()
        {
            role_assignments_cache::role_members_cache_invalidate(role_id).await;
        }

        let event = UserRoleAssignmentsSyncedEvent {
            user_id: user.user_id.clone(),
            added: sync_result.added.into(),
            removed: sync_result.removed.into(),
            synced_at: sync_result.synced_at,
            result: Arc::clone(&list_result),
        };
        let dispatcher = dispatcher.clone();
        tokio::spawn(async move {
            dispatcher.user_role_assignments_synced(event).await;
        });

        Ok(list_result)
    }

    // -----------------------------------------------------------------------
    // READ: three lookup paths
    // -----------------------------------------------------------------------

    /// Return all roles the given user is currently assigned to, together with
    /// per-provider sync metadata.
    ///
    /// Each [`AssignedRole`] carries `role_id` (for internal authorizers) as
    /// well as `role_ident` + `project_id` (for external authorizers).
    /// [`ListUserRoleAssignmentsResult::provider_sync_times`] holds one
    /// [`UserProviderSyncInfo`] per `(project_id, provider_id)` pair for which
    /// the user has at least one active assignment — the sync clock is at that
    /// granularity, not per individual role.
    ///
    /// Primary consumer: authorizers resolving a user's effective roles for a
    /// permission check.
    async fn list_role_assignments_for_user(
        user_id: &UserId,
        catalog_state: Self::State,
    ) -> Result<Arc<ListUserRoleAssignmentsResult>, CatalogBackendError> {
        if let Some(cached) = role_assignments_cache::user_assignments_cache_get(user_id).await {
            return Ok(cached);
        }
        let result =
            Arc::new(Self::list_role_assignments_for_user_impl(user_id, catalog_state).await?);
        role_assignments_cache::user_assignments_cache_insert(user_id, Arc::clone(&result)).await;
        Ok(result)
    }

    /// Return all members of the given role, together with the last sync
    /// timestamp for this role's member list.
    ///
    /// Identified by [`RoleId`] — use when the UUID is already known (e.g.
    /// inside an authorizer after having resolved the role).
    async fn list_role_assignments_for_role(
        role_id: RoleId,
        catalog_state: Self::State,
    ) -> Result<Option<Arc<ListRoleMembersResult>>, CatalogBackendError> {
        if let Some(cached) = role_assignments_cache::role_members_cache_get(role_id).await {
            return Ok(Some(cached));
        }
        let result = match Self::list_role_assignments_for_role_impl(role_id, catalog_state).await?
        {
            Some(r) => Arc::new(r),
            None => return Ok(None),
        };
        role_assignments_cache::role_members_cache_insert(role_id, Arc::clone(&result)).await;
        role_cache::role_ident_insert(
            result.project_id.clone(),
            result.role_ident.clone(),
            role_id,
        )
        .await;
        Ok(Some(result))
    }

    /// Return all members of a role identified by its project-scoped ident,
    /// together with the last sync timestamp for this role's member list.
    ///
    /// Resolves `(project_id, role_ident)` → [`RoleId`] internally.  Use when
    /// the caller has an external identifier (LDAP group DN, SCIM group ID)
    /// and wants to avoid a separate role-lookup round-trip.
    async fn list_role_assignments_for_role_by_ident(
        project_id: &ArcProjectId,
        role_ident: &Arc<RoleIdent>,
        catalog_state: Self::State,
    ) -> Result<Option<Arc<ListRoleMembersResult>>, CatalogBackendError> {
        // Try to resolve (project_id, role_ident) → RoleId via the secondary
        // ident cache (populated by role-create/update events and sync events).
        // A cache hit lets us serve ROLE_MEMBERS_CACHE without touching the DB.
        if let Some(role_id) =
            role_cache::role_ident_to_id(project_id.clone(), role_ident.clone()).await
            && let Some(cached) = role_assignments_cache::role_members_cache_get(role_id).await
        {
            // Guard against stale ident→id mappings: if the cached result's
            // ident no longer matches the requested ident (e.g. source_id
            // was changed since the entry was written), fall through to the
            // DB path so we don't return members for the wrong role.
            if cached.role_ident.as_ref() == role_ident.as_ref() {
                return Ok(Some(cached));
            }
        }

        // DB fetch — the result carries role_id, project_id and role_ident so
        // we can populate both caches without a second round-trip.
        let result = match Self::list_role_assignments_for_role_by_ident_impl(
            project_id,
            role_ident,
            catalog_state,
        )
        .await?
        {
            Some(r) => Arc::new(r),
            None => return Ok(None),
        };
        role_assignments_cache::role_members_cache_insert(result.role_id, Arc::clone(&result))
            .await;
        role_cache::role_ident_insert(
            result.project_id.clone(),
            result.role_ident.clone(),
            result.role_id,
        )
        .await;
        Ok(Some(result))
    }
}

impl<T> CatalogRoleAssignmentOps for T where T: CatalogStore {}
