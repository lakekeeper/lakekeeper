use std::{collections::HashMap, sync::Arc};

use lakekeeper::{
    CONFIG,
    api::{
        iceberg::v1::PaginationQuery,
        management::v1::user::{
            ListUsersResponse, SearchUser, SearchUserResponse, User, UserLastUpdatedWith, UserType,
        },
    },
    service::{
        CreateOrUpdateUserResponse, DatabaseIntegrityError, DeletedUser, Result, RoleId, UserId,
        UserUpsertMode, UserWrite,
    },
};

use super::dbutils::DBErrorHandler;
use crate::pagination::{PaginateToken, V1PaginateToken};

#[derive(sqlx::Type, Debug, Clone, Copy)]
#[sqlx(rename_all = "kebab-case", type_name = "user_last_updated_with")]
pub(super) enum DbUserLastUpdatedWith {
    CreateEndpoint,
    ConfigCallCreation,
    UpdateEndpoint,
    RoleProvider,
}

#[derive(sqlx::Type, Debug, Clone, Copy, PartialEq, Eq)]
#[sqlx(rename_all = "kebab-case", type_name = "user_type")]
pub(super) enum DbUserType {
    Application,
    Human,
}

impl From<DbUserType> for UserType {
    fn from(db_user_type: DbUserType) -> Self {
        match db_user_type {
            DbUserType::Application => UserType::Application,
            DbUserType::Human => UserType::Human,
        }
    }
}

impl From<UserType> for DbUserType {
    fn from(user_type: UserType) -> Self {
        match user_type {
            UserType::Application => DbUserType::Application,
            UserType::Human => DbUserType::Human,
        }
    }
}

impl From<UserLastUpdatedWith> for DbUserLastUpdatedWith {
    fn from(u: UserLastUpdatedWith) -> Self {
        match u {
            UserLastUpdatedWith::CreateEndpoint => DbUserLastUpdatedWith::CreateEndpoint,
            UserLastUpdatedWith::ConfigCallCreation => DbUserLastUpdatedWith::ConfigCallCreation,
            UserLastUpdatedWith::UpdateEndpoint => DbUserLastUpdatedWith::UpdateEndpoint,
            UserLastUpdatedWith::RoleProvider => DbUserLastUpdatedWith::RoleProvider,
        }
    }
}

/// Display name for a user. A role-provider stub has no name yet (`name IS
/// NULL`); render the historical placeholder at read time so the API contract
/// (`User.name: String`) is unchanged while the not-yet-named state is stored
/// honestly as NULL. This is the single source of the placeholder string.
fn display_user_name(id: &str, name: Option<String>) -> String {
    name.unwrap_or_else(|| format!("Nameless User with id {id}"))
}

#[derive(sqlx::FromRow, Debug)]
pub(super) struct UserRow {
    pub(super) id: String,
    pub(super) name: Option<String>,
    pub(super) email: Option<String>,
    pub(super) last_updated_with: DbUserLastUpdatedWith,
    pub(super) user_type: DbUserType,
    pub(super) created_at: chrono::DateTime<chrono::Utc>,
    pub(super) updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// The columns a user lifecycle event reports a change of. `last_updated_with` moving
/// on its own is no change.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct UserIdentity<'a> {
    pub(super) name: Option<&'a str>,
    pub(super) email: Option<&'a str>,
    pub(super) user_type: DbUserType,
    pub(super) deleted: bool,
}

impl UserRow {
    fn identity(&self, deleted: bool) -> UserIdentity<'_> {
        UserIdentity {
            name: self.name.as_deref(),
            email: self.email.as_deref(),
            user_type: self.user_type,
            deleted,
        }
    }
}

/// What a write did to one user row: `None` when it changed none of the columns
/// [`UserIdentity`] compares. `before` is the row as locked before the write, with
/// whether it was deleted.
pub(super) fn user_write(
    created: bool,
    after: (UserRow, bool),
    before: Option<(UserRow, bool)>,
) -> std::result::Result<Option<UserWrite>, DatabaseIntegrityError> {
    let (after, after_deleted) = after;
    if created {
        return Ok(Some(UserWrite::Created(Arc::new(after.into_user()?))));
    }
    if let Some((previous, was_deleted)) = &before
        && previous.identity(*was_deleted) == after.identity(after_deleted)
    {
        return Ok(None);
    }
    Ok(Some(UserWrite::Updated {
        user: Arc::new(after.into_user()?),
        previous: before
            .map(|(previous, _)| previous.into_user().map(Arc::new))
            .transpose()?,
    }))
}

/// The rows of `ids` as they are now, with whether each is deleted. Read in the
/// transaction that wrote them, so the read sees the write.
pub(super) async fn user_rows(
    ids: &[String],
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> std::result::Result<HashMap<String, (UserRow, bool)>, sqlx::Error> {
    Ok(sqlx::query!(
        r#"
        SELECT
            id,
            name,
            email,
            last_updated_with AS "last_updated_with: DbUserLastUpdatedWith",
            user_type AS "user_type: DbUserType",
            created_at,
            updated_at,
            deleted_at IS NOT NULL AS "deleted!"
        FROM users
        WHERE id = ANY($1::TEXT[])
        "#,
        ids,
    )
    .fetch_all(&mut **transaction)
    .await?
    .into_iter()
    .map(|row| {
        (
            row.id.clone(),
            (
                UserRow {
                    id: row.id,
                    name: row.name,
                    email: row.email,
                    last_updated_with: row.last_updated_with,
                    user_type: row.user_type,
                    created_at: row.created_at,
                    updated_at: row.updated_at,
                },
                row.deleted,
            ),
        )
    })
    .collect())
}

impl UserRow {
    /// The API user, with a malformed id reported as a database integrity error.
    pub(super) fn into_user(self) -> std::result::Result<User, DatabaseIntegrityError> {
        User::try_from(self).map_err(|e| DatabaseIntegrityError::new(e.error.message))
    }
}

impl TryFrom<UserRow> for User {
    type Error = lakekeeper::service::IcebergErrorResponse;

    fn try_from(
        UserRow {
            id,
            name,
            email,
            last_updated_with,
            user_type,
            created_at,
            updated_at,
        }: UserRow,
    ) -> Result<Self> {
        let name = display_user_name(&id, name);
        Ok(User {
            id: id.try_into()?,
            name,
            email,
            user_type: user_type.into(),
            last_updated_with: match last_updated_with {
                DbUserLastUpdatedWith::CreateEndpoint => UserLastUpdatedWith::CreateEndpoint,
                DbUserLastUpdatedWith::ConfigCallCreation => {
                    UserLastUpdatedWith::ConfigCallCreation
                }
                DbUserLastUpdatedWith::UpdateEndpoint => UserLastUpdatedWith::UpdateEndpoint,
                DbUserLastUpdatedWith::RoleProvider => UserLastUpdatedWith::RoleProvider,
            },
            created_at,
            updated_at,
        })
    }
}

pub(crate) async fn list_users<'e, 'c: 'e, E: sqlx::Executor<'c, Database = sqlx::Postgres>>(
    filter_user_id: Option<Vec<UserId>>,
    filter_name: Option<String>,
    PaginationQuery {
        page_token,
        page_size,
    }: PaginationQuery,
    connection: E,
) -> Result<ListUsersResponse> {
    let page_size = CONFIG.page_size_or_pagination_default(page_size);
    let filter_name = filter_name.unwrap_or_default();

    let token = page_token
        .as_option()
        .map(PaginateToken::try_from)
        .transpose()?;

    let (token_ts, token_id): (_, Option<&String>) = token
        .as_ref()
        .map(PaginateToken::v1_parts)
        .transpose()?
        .unzip();

    // The name filter matches the raw `name` column. A nameless role-provider stub
    // (`name IS NULL`) has no name to match, so a name search never returns it
    // (`NULL ILIKE ...` is NULL → excluded) — by design: such a stub is surfaced by
    // the unfiltered list or fetched by its id via the `$3/$4` id filter below, not
    // by a username search. The display placeholder ("Nameless User with id <id>",
    // see `display_user_name`) is a read-time render only, deliberately NOT a search
    // key — matching it would leak the presentation string into this query (and its
    // index) and couple them to the placeholder wording.
    // The trailing `(u.created_at, u.id)` predicate is the keyset pagination cursor.
    let users: Vec<User> = sqlx::query_as!(
        UserRow,
        r#"
        SELECT
            id,
            name,
            last_updated_with as "last_updated_with: DbUserLastUpdatedWith",
            user_type as "user_type: DbUserType",
            email,
            created_at,
            updated_at
        FROM users u
        where (deleted_at is null)
            AND ($1 OR name ILIKE ('%' || $2 || '%'))
            AND ($3 OR id = any($4))
            AND ((u.created_at > $5 OR $5 IS NULL) OR (u.created_at = $5 AND u.id > $6))
        ORDER BY u.created_at, u.id ASC
        LIMIT $7
        "#,
        filter_name.is_empty(),
        filter_name.clone(),
        filter_user_id.is_none(),
        filter_user_id
            .unwrap_or_default()
            .into_iter()
            .map(|u| u.to_string())
            .collect::<Vec<String>>() as Vec<String>,
        token_ts,
        token_id,
        page_size,
    )
    .fetch_all(connection)
    .await
    .map_err(|e| e.into_error_model("Error fetching users".to_string()))?
    .into_iter()
    .map(User::try_from)
    .collect::<Result<_>>()?;

    let next_page_token = users.last().map(|u| {
        PaginateToken::V1(V1PaginateToken {
            created_at: u.created_at,
            id: u.id.clone(),
        })
        .to_string()
    });

    Ok(ListUsersResponse {
        users,
        next_page_token,
    })
}

/// Soft-deletes a user (scrubs PII, sets `deleted_at`) **and** removes the
/// user's role assignments (`role_assignment`) and provider sync log
/// (`role_assignment_sync`), so a deleted user is no longer a member of any
/// role. This matches the OpenFGA authorizer, whose `delete_user` drops all of
/// the user's tuples — keeping the two backends consistent on delete.
///
/// Only acts on an *active* row (`deleted_at IS NULL`). Returns `None` if no
/// active user with this id exists — including re-deleting an already
/// soft-deleted user, which is a no-op that preserves the original `deleted_at`
/// (consistent with `get`/`list`, which hide soft-deleted users). Otherwise
/// returns the (possibly empty) set of roles the user was assigned to. The caller
/// evicts the user's effective-roles cache after commit.
///
/// Takes its locks in separate statements, in the order of the role-assignment
/// writers: the user row, then the user's assignments, then the user's sync
/// records. Each statement after the user lock reads a snapshot taken after that
/// lock was granted, so it sees every row a concurrent sync of the user committed
/// while this waited.
pub(crate) async fn delete_user(
    id: UserId,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Option<DeletedUser>> {
    let id = id.to_string();
    let map_err = |e: sqlx::Error| e.into_error_model("Error deleting user".to_string());

    // Also the row as it is before the delete scrubs its name and email: the identity
    // the user lifecycle event reports.
    let before = sqlx::query_as!(
        UserRow,
        r#"
        SELECT
            id,
            name,
            email,
            last_updated_with AS "last_updated_with: DbUserLastUpdatedWith",
            user_type AS "user_type: DbUserType",
            created_at,
            updated_at
        FROM users
        WHERE id = $1
        FOR NO KEY UPDATE
        "#,
        id,
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(map_err)?;

    let deleted_user = sqlx::query_scalar!(
        r#"
        UPDATE users
        SET deleted_at = now(),
            name = 'Deleted User',
            email = null
        WHERE id = $1 AND deleted_at IS NULL
        RETURNING id
        "#,
        id,
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(map_err)?;

    let affected_roles = sqlx::query_scalar!(
        r#"DELETE FROM role_assignment WHERE user_id = $1 RETURNING role_id"#,
        id,
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(map_err)?;

    sqlx::query!(r#"DELETE FROM role_assignment_sync WHERE user_id = $1"#, id)
        .execute(&mut **transaction)
        .await
        .map_err(map_err)?;

    let (Some(_), Some(before)) = (deleted_user, before) else {
        return Ok(None);
    };
    Ok(Some(DeletedUser {
        user: User::try_from(before)?,
        affected_roles: affected_roles.into_iter().map(RoleId::new).collect(),
    }))
}

/// Upserts a user and reports what the write did: created, updated with the row as it
/// was before, or unchanged.
///
/// Locks the row first, like `delete_user` and the role-provider syncs, so the row read
/// before the write is the row the write replaces. A row another transaction inserts
/// after that read has no previous row to report.
pub(crate) async fn create_or_update_user(
    id: &UserId,
    name: &str,
    email: Option<&str>,
    last_updated_with: UserLastUpdatedWith,
    user_type: UserType,
    mode: UserUpsertMode,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<CreateOrUpdateUserResponse> {
    let db_last_updated_with: DbUserLastUpdatedWith = last_updated_with.into();
    let backfill_only = matches!(mode, UserUpsertMode::BackfillUnnamedStub);
    let map_err =
        |e: sqlx::Error| e.into_error_model("Error creating or updating user".to_string());

    let before = sqlx::query!(
        r#"
        SELECT
            id,
            name,
            email,
            last_updated_with AS "last_updated_with: DbUserLastUpdatedWith",
            user_type AS "user_type: DbUserType",
            created_at,
            updated_at,
            deleted_at IS NOT NULL AS "deleted!"
        FROM users
        WHERE id = $1
        FOR NO KEY UPDATE
        "#,
        id.to_string(),
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(map_err)?
    .map(|row| {
        (
            UserRow {
                id: row.id,
                name: row.name,
                email: row.email,
                last_updated_with: row.last_updated_with,
                user_type: row.user_type,
                created_at: row.created_at,
                updated_at: row.updated_at,
            },
            row.deleted,
        )
    });

    // One statement covers both modes. The `DO UPDATE` fires unconditionally for
    // `Overwrite` (`NOT $6`), but for `BackfillUnnamedStub` only when the row is
    // still an un-named role-provider stub — so a real name is never clobbered,
    // atomically against a concurrent sync. In backfill a NULL incoming email keeps
    // the stub's existing (provider-synced) email rather than clearing it; Overwrite
    // stays an unconditional replace. The `UNION ALL` fallback returns the unchanged
    // row when the guard skips the update, so a no-op still yields a row (and
    // `fetch_one` holds).
    //
    // query_as doesn't respect FromRow: https://github.com/launchbadge/sqlx/issues/2584
    let user = sqlx::query!(
        r#"
        WITH upserted AS (
            INSERT INTO users (id, name, email, last_updated_with, user_type)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (id) DO UPDATE
                SET name = $2,
                    email = CASE WHEN $6 THEN COALESCE($3, users.email) ELSE $3 END,
                    last_updated_with = $4, user_type = $5, deleted_at = null
                WHERE NOT $6
                   OR (users.name IS NULL
                       AND users.last_updated_with = 'role-provider'::user_last_updated_with)
            RETURNING (xmax = 0) AS created, id, name, email, created_at, updated_at, last_updated_with, user_type, deleted_at
        )
        SELECT
            u.created AS "created!",
            u.id AS "id!",
            u.name,
            u.email,
            u.created_at AS "created_at!",
            u.updated_at,
            u.last_updated_with AS "last_updated_with!: DbUserLastUpdatedWith",
            u.user_type AS "user_type!: DbUserType",
            u.deleted_at IS NOT NULL AS "deleted!"
        FROM upserted u
        UNION ALL
        SELECT
            false AS "created!",
            e.id AS "id!",
            e.name,
            e.email,
            e.created_at AS "created_at!",
            e.updated_at,
            e.last_updated_with AS "last_updated_with!: DbUserLastUpdatedWith",
            e.user_type AS "user_type!: DbUserType",
            e.deleted_at IS NOT NULL AS "deleted!"
        FROM users e
        WHERE e.id = $1 AND NOT EXISTS (SELECT 1 FROM upserted)
        "#,
        id.to_string(),
        name,
        email,
        db_last_updated_with as _,
        DbUserType::from(user_type) as _,
        backfill_only,
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(map_err)?;
    let created = user.created;
    let deleted = user.deleted;
    let user = UserRow {
        id: user.id,
        name: user.name,
        email: user.email,
        user_type: user.user_type,
        last_updated_with: user.last_updated_with,
        created_at: user.created_at,
        updated_at: user.updated_at,
    };

    if created {
        return Ok(CreateOrUpdateUserResponse::Created(User::try_from(user)?));
    }
    Ok(match before {
        Some((previous, was_deleted))
            if previous.identity(was_deleted) == user.identity(deleted) =>
        {
            CreateOrUpdateUserResponse::Unchanged(User::try_from(user)?)
        }
        before => CreateOrUpdateUserResponse::Updated {
            user: User::try_from(user)?,
            previous: before
                .map(|(previous, _)| User::try_from(previous))
                .transpose()?,
        },
    })
}

pub(crate) async fn search_user<'e, 'c: 'e, E: sqlx::Executor<'c, Database = sqlx::Postgres>>(
    search_term: &str,
    connection: E,
) -> Result<SearchUserResponse> {
    // Split into two legs so the fuzzy leg's ORDER BY is the bare KNN distance — that
    // lets it use the `users_name_email_coalesce_gist_idx` GiST index instead of
    // scanning + sorting every row (a leading `CASE` in the ORDER BY defeats KNN). The
    // exact-id match is unioned in so it still ranks first.
    let users = sqlx::query!(
        r#"
        SELECT id AS "id!", name, email, user_type AS "user_type!: DbUserType"
        FROM (
            ( SELECT id, name, email, user_type, 0 AS rank, 0::real AS dist
              FROM users
              WHERE id = $1 AND deleted_at IS NULL )
          UNION ALL
            ( SELECT id, name, email, user_type, 1 AS rank,
                     (COALESCE(name, '') || ' ' || COALESCE(email, '')) <-> $1 AS dist
              FROM users
              WHERE id <> $1 AND deleted_at IS NULL
              ORDER BY (COALESCE(name, '') || ' ' || COALESCE(email, '')) <-> $1
              LIMIT 10 )
        ) ranked
        ORDER BY rank, dist
        LIMIT 10
        "#,
        search_term,
    )
    .fetch_all(connection)
    .await
    .map_err(|e| e.into_error_model("Error searching user".to_string()))?
    .into_iter()
    .map(|row| {
        Ok(SearchUser {
            name: display_user_name(&row.id, row.name),
            id: row.id.try_into()?,
            user_type: row.user_type.into(),
            email: row.email,
        })
    })
    .collect::<Result<_>>()?;

    Ok(SearchUserResponse { users })
}

#[cfg(test)]
mod test {
    use lakekeeper::api::iceberg::types::PageToken;

    use super::*;
    use crate::CatalogState;

    async fn delete_user_committed(state: &CatalogState, user_id: UserId) -> Option<DeletedUser> {
        let mut t = state.read_write.write_pool.begin().await.unwrap();
        let result = delete_user(user_id, &mut t).await.unwrap();
        t.commit().await.unwrap();
        result
    }

    async fn create_or_update_user_committed(
        id: &UserId,
        name: &str,
        email: Option<&str>,
        last_updated_with: UserLastUpdatedWith,
        user_type: UserType,
        mode: UserUpsertMode,
        state: &CatalogState,
    ) -> CreateOrUpdateUserResponse {
        let mut t = state.read_write.write_pool.begin().await.unwrap();
        let result =
            create_or_update_user(id, name, email, last_updated_with, user_type, mode, &mut t)
                .await
                .unwrap();
        t.commit().await.unwrap();
        result
    }

    #[sqlx::test]
    async fn test_create_or_update_user(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let user_id = UserId::new_unchecked("oidc", "test_user_1");
        let user_name = "Test User 1";

        create_or_update_user_committed(
            &user_id,
            user_name,
            None,
            UserLastUpdatedWith::CreateEndpoint,
            UserType::Human,
            UserUpsertMode::Overwrite,
            &state,
        )
        .await;

        let users = list_users(
            None,
            None,
            PaginationQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(10),
            },
            &state.read_write.read_pool,
        )
        .await
        .unwrap();

        assert_eq!(users.users.len(), 1);
        assert_eq!(users.users[0].id, user_id);
        assert_eq!(users.users[0].name, user_name);
        assert_eq!(users.users[0].email, None);
        assert_eq!(users.users[0].user_type, UserType::Human);

        // Update
        let user_name = "Test User 1 Updated";
        create_or_update_user_committed(
            &user_id,
            user_name,
            None,
            UserLastUpdatedWith::CreateEndpoint,
            UserType::Human,
            UserUpsertMode::Overwrite,
            &state,
        )
        .await;

        let users = list_users(
            None,
            None,
            PaginationQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(10),
            },
            &state.read_write.read_pool,
        )
        .await
        .unwrap();

        assert_eq!(users.users.len(), 1);
        assert_eq!(users.users[0].id, user_id);
        assert_eq!(users.users[0].name, user_name);
        assert_eq!(users.users[0].email, None);
    }

    /// What a write reports is what the user lifecycle events fire on: created,
    /// updated with the previous row, or unchanged when only `last_updated_with` moved.
    #[sqlx::test]
    async fn create_or_update_user_reports_what_the_write_did(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let user_id = UserId::new_unchecked("oidc", "lifecycle");
        let upsert = |name: &'static str, email: Option<&'static str>, with| {
            let state = state.clone();
            let user_id = user_id.clone();
            async move {
                create_or_update_user_committed(
                    &user_id,
                    name,
                    email,
                    with,
                    UserType::Human,
                    UserUpsertMode::Overwrite,
                    &state,
                )
                .await
            }
        };

        let created = upsert(
            "Alice",
            Some("alice@example.com"),
            UserLastUpdatedWith::CreateEndpoint,
        )
        .await;
        assert!(matches!(&created, CreateOrUpdateUserResponse::Created(u) if u.name == "Alice"));

        let unchanged = upsert(
            "Alice",
            Some("alice@example.com"),
            UserLastUpdatedWith::UpdateEndpoint,
        )
        .await;
        assert!(matches!(
            unchanged,
            CreateOrUpdateUserResponse::Unchanged(_)
        ));
        assert!(unchanged.write().is_none());

        let updated = upsert(
            "Alice",
            Some("alice@new.example.com"),
            UserLastUpdatedWith::UpdateEndpoint,
        )
        .await;
        let CreateOrUpdateUserResponse::Updated { user, previous } = updated else {
            panic!("expected an update, got {updated:?}");
        };
        assert_eq!(user.email.as_deref(), Some("alice@new.example.com"));
        assert_eq!(
            previous.and_then(|p| p.email).as_deref(),
            Some("alice@example.com")
        );
    }

    /// The delete reports the row as it was, before it scrubbed the name and email.
    /// Creating the user again reads as an update of the deleted row.
    #[sqlx::test]
    async fn delete_user_reports_the_row_before_the_delete(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let user_id = UserId::new_unchecked("oidc", "deleted");
        create_or_update_user_committed(
            &user_id,
            "Bob",
            Some("bob@example.com"),
            UserLastUpdatedWith::CreateEndpoint,
            UserType::Human,
            UserUpsertMode::Overwrite,
            &state,
        )
        .await;

        let deleted = delete_user_committed(&state, user_id.clone())
            .await
            .unwrap();
        assert_eq!(deleted.user.name, "Bob");
        assert_eq!(deleted.user.email.as_deref(), Some("bob@example.com"));

        let again = create_or_update_user_committed(
            &user_id,
            "Bob",
            Some("bob@example.com"),
            UserLastUpdatedWith::CreateEndpoint,
            UserType::Human,
            UserUpsertMode::Overwrite,
            &state,
        )
        .await;
        let CreateOrUpdateUserResponse::Updated { previous, .. } = again else {
            panic!("expected an update of the deleted row, got {again:?}");
        };
        assert_eq!(previous.map(|p| p.name).as_deref(), Some("Deleted User"));
    }

    /// The first-login backfill leaves a named row alone, and says so.
    #[sqlx::test]
    async fn a_skipped_backfill_is_unchanged(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let user_id = UserId::new_unchecked("oidc", "named");
        create_or_update_user_committed(
            &user_id,
            "Carol",
            None,
            UserLastUpdatedWith::CreateEndpoint,
            UserType::Human,
            UserUpsertMode::Overwrite,
            &state,
        )
        .await;
        let backfill = create_or_update_user_committed(
            &user_id,
            "Someone Else",
            None,
            UserLastUpdatedWith::ConfigCallCreation,
            UserType::Human,
            UserUpsertMode::BackfillUnnamedStub,
            &state,
        )
        .await;
        assert!(matches!(&backfill, CreateOrUpdateUserResponse::Unchanged(u) if u.name == "Carol"));
    }

    #[sqlx::test]
    async fn test_search_user(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let user_id = UserId::new_unchecked("kubernetes", "test_user_1");
        let user_name = "Test User 1";

        create_or_update_user_committed(
            &user_id,
            user_name,
            None,
            UserLastUpdatedWith::UpdateEndpoint,
            UserType::Application,
            UserUpsertMode::Overwrite,
            &state,
        )
        .await;

        let search_result = search_user("Test", &state.read_write.read_pool)
            .await
            .unwrap();
        assert_eq!(search_result.users.len(), 1);
        assert_eq!(search_result.users[0].id, user_id);
        assert_eq!(search_result.users[0].name, user_name);
        assert_eq!(search_result.users[0].user_type, UserType::Application);

        // A soft-deleted user must not surface in search. delete_user tombstones the
        // row (deleted_at set, name -> 'Deleted User'); search must exclude it both by
        // its former name and by the 'Deleted User' tombstone name.
        delete_user_committed(&state, user_id.clone()).await;
        assert_eq!(
            search_user("Test", &state.read_write.read_pool)
                .await
                .unwrap()
                .users
                .len(),
            0
        );
        assert_eq!(
            search_user("Deleted User", &state.read_write.read_pool)
                .await
                .unwrap()
                .users
                .len(),
            0
        );
    }

    #[sqlx::test]
    async fn test_delete_user(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let user_id = UserId::new_unchecked("oidc", "test_user_1");
        let user_name = "Test User 1";

        create_or_update_user_committed(
            &user_id,
            user_name,
            None,
            UserLastUpdatedWith::ConfigCallCreation,
            UserType::Application,
            UserUpsertMode::Overwrite,
            &state,
        )
        .await;

        delete_user_committed(&state, user_id).await;

        let users = list_users(
            None,
            None,
            PaginationQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(10),
            },
            &state.read_write.read_pool,
        )
        .await
        .unwrap();

        assert_eq!(users.users.len(), 0);

        // Delete non-existent user
        let user_id = UserId::new_unchecked("oidc", "test_user_2");
        let result = delete_user_committed(&state, user_id).await;
        assert!(result.is_none());
    }

    /// Re-deleting an already soft-deleted user is a no-op: it returns `None`
    /// (consistent with `get`/`list`, which hide soft-deleted users) rather than
    /// matching the tombstone row and resetting its `deleted_at`. A `None` return
    /// means the `deleted_at IS NULL` guard matched zero rows, so the original
    /// tombstone is left untouched.
    #[sqlx::test]
    async fn test_delete_user_already_deleted_is_noop(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let user_id = UserId::new_unchecked("oidc", "test_user_1");

        create_or_update_user_committed(
            &user_id,
            "Test User 1",
            None,
            UserLastUpdatedWith::ConfigCallCreation,
            UserType::Application,
            UserUpsertMode::Overwrite,
            &state,
        )
        .await;

        // First delete acts on the active row.
        let first = delete_user_committed(&state, user_id.clone()).await;
        assert!(first.is_some());

        // Second delete finds no active row → no-op, no tombstone reset.
        let second = delete_user_committed(&state, user_id).await;
        assert!(second.is_none());
    }

    #[sqlx::test]
    async fn test_paginate_user(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        for i in 0..10 {
            let user_id = UserId::new_unchecked("oidc", &format!("test_user_{i}"));
            let user_name = &format!("test user {i}");

            create_or_update_user_committed(
                &user_id,
                user_name,
                None,
                UserLastUpdatedWith::ConfigCallCreation,
                UserType::Application,
                UserUpsertMode::Overwrite,
                &state,
            )
            .await;
        }
        let users = list_users(
            None,
            None,
            PaginationQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(10),
            },
            &state.read_write.read_pool,
        )
        .await
        .unwrap();

        assert_eq!(users.users.len(), 10);

        let users = list_users(
            None,
            None,
            PaginationQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(5),
            },
            &state.read_write.read_pool,
        )
        .await
        .unwrap();
        assert_eq!(users.users.len(), 5);

        for (uidx, u) in users.users.iter().enumerate() {
            let user_id = UserId::new_unchecked("oidc", &format!("test_user_{uidx}"));
            let user_name = format!("test user {uidx}");
            assert_eq!(u.id, user_id);
            assert_eq!(u.name, user_name);
        }

        let users = list_users(
            None,
            None,
            PaginationQuery {
                page_token: users.next_page_token.into(),
                page_size: Some(5),
            },
            &state.read_write.read_pool,
        )
        .await
        .unwrap();

        assert_eq!(users.users.len(), 5);

        for (uidx, u) in users.users.iter().enumerate() {
            let uidx = uidx + 5;
            let user_id = UserId::new_unchecked("oidc", &format!("test_user_{uidx}"));
            let user_name = format!("test user {uidx}");
            assert_eq!(u.id, user_id);
            assert_eq!(u.name, user_name);
        }

        // last page is empty
        let users = list_users(
            None,
            None,
            PaginationQuery {
                page_token: users.next_page_token.into(),
                page_size: Some(5),
            },
            &state.read_write.read_pool,
        )
        .await
        .unwrap();
        assert_eq!(users.users.len(), 0);
        assert!(users.next_page_token.is_none());
    }
}
