use crate::api::management::v1::user::User;

#[derive(Debug, Clone)]
pub enum CreateOrUpdateUserResponse {
    Created(User),
    Updated(User),
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
