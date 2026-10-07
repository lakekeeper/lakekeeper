use std::sync::Arc;

use crate::{api::management::v1::user::User, service::RoleId};

/// What [`crate::service::CatalogStore::create_or_update_user`] did to the row.
///
/// A write changed the user when it changed any of `name`, `email`, `user_type` or
/// whether the user is deleted; `last_updated_with` moving on its own is no change.
#[derive(Debug, Clone)]
pub enum CreateOrUpdateUserResponse {
    /// No row existed; this is the new one.
    Created(User),
    /// The row changed. `previous` is the row as it was before the write, `None` only
    /// when another transaction created the user while this write ran.
    Updated { user: User, previous: Option<User> },
    /// The write left the user as it was, or the upsert mode skipped it.
    Unchanged(User),
}

impl CreateOrUpdateUserResponse {
    /// The user as it is after the write.
    #[must_use]
    pub fn user(&self) -> &User {
        match self {
            Self::Created(user) | Self::Updated { user, .. } | Self::Unchanged(user) => user,
        }
    }

    /// The user as it is after the write.
    #[must_use]
    pub fn into_user(self) -> User {
        match self {
            Self::Created(user) | Self::Updated { user, .. } | Self::Unchanged(user) => user,
        }
    }

    /// The change this write made, or `None` when it made none.
    #[must_use]
    pub fn write(&self) -> Option<UserWrite> {
        match self {
            Self::Created(user) => Some(UserWrite::Created(Arc::new(user.clone()))),
            Self::Updated { user, previous } => Some(UserWrite::Updated {
                user: Arc::new(user.clone()),
                previous: previous.clone().map(Arc::new),
            }),
            Self::Unchanged(_) => None,
        }
    }
}

/// A user row a write created or changed, as the user lifecycle events carry it.
#[derive(Debug, Clone)]
pub enum UserWrite {
    /// The row did not exist before the write.
    Created(Arc<User>),
    /// The row changed. `previous` is the row as it was before the write, `None` only
    /// when another transaction created the user while this write ran.
    Updated {
        user: Arc<User>,
        previous: Option<Arc<User>>,
    },
}

/// A user [`crate::service::CatalogStore::delete_user`] deleted.
#[derive(Debug, Clone)]
pub struct DeletedUser {
    /// The row as it was before the delete cleared its name and email.
    pub user: User,
    /// The roles the user was assigned to; the delete removed those assignments.
    pub affected_roles: Vec<RoleId>,
}

/// Overwrite policy for [`crate::service::CatalogStore::create_or_update_user`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserUpsertMode {
    /// Insert, or unconditionally overwrite an existing row. Used by the explicit
    /// create- and update-user endpoints.
    Overwrite,
    /// Insert, or backfill ONLY an un-named role-provider stub (`name IS NULL`
    /// and `last_updated_with = role-provider`); any existing real name is left
    /// untouched — atomically, even against a concurrent role-provider sync.
    /// Used by the first-login (`GET /v1/config`) hook.
    BackfillUnnamedStub,
}
