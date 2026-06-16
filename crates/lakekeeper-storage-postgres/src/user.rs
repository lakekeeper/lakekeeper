use lakekeeper::{
    CONFIG,
    api::{
        iceberg::v1::PaginationQuery,
        management::v1::user::{
            ListUsersResponse, SearchUser, SearchUserResponse, User, UserLastUpdatedWith, UserType,
        },
    },
    service::{CreateOrUpdateUserResponse, Result, RoleId, UserId, UserUpsertMode},
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

#[derive(sqlx::Type, Debug, Clone, Copy)]
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
struct UserRow {
    id: String,
    name: Option<String>,
    email: Option<String>,
    last_updated_with: DbUserLastUpdatedWith,
    user_type: DbUserType,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: Option<chrono::DateTime<chrono::Utc>>,
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
        .map(|PaginateToken::V1(V1PaginateToken { created_at, id })| (created_at, id))
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
/// returns the (possibly empty) set of roles the user was assigned to, so the
/// caller can evict those roles' member caches and the user's effective-roles
/// cache. Done in one round-trip.
pub(crate) async fn delete_user<'c, 'e: 'c, E: sqlx::Executor<'c, Database = sqlx::Postgres>>(
    id: UserId,
    connection: E,
) -> Result<Option<Vec<RoleId>>> {
    let row = sqlx::query!(
        r#"
        WITH
        deleted_user AS (
            UPDATE users
            SET deleted_at = now(),
                name = 'Deleted User',
                email = null
            WHERE id = $1 AND deleted_at IS NULL
            RETURNING id
        ),
        deleted_assignments AS (
            DELETE FROM role_assignment WHERE user_id = $1 RETURNING role_id
        ),
        deleted_sync AS (
            DELETE FROM role_assignment_sync WHERE user_id = $1
        )
        SELECT
            (SELECT id FROM deleted_user) AS "user_id?",
            COALESCE((SELECT array_agg(role_id) FROM deleted_assignments), ARRAY[]::uuid[])
                AS "affected_roles!: Vec<uuid::Uuid>"
        "#,
        id.to_string(),
    )
    .fetch_one(connection)
    .await
    .map_err(|e| e.into_error_model("Error deleting user".to_string()))?;

    Ok(row
        .user_id
        .map(|_| row.affected_roles.into_iter().map(RoleId::new).collect()))
}

pub(crate) async fn create_or_update_user<
    'c,
    'e: 'c,
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
>(
    id: &UserId,
    name: &str,
    email: Option<&str>,
    last_updated_with: UserLastUpdatedWith,
    user_type: UserType,
    mode: UserUpsertMode,
    connection: E,
) -> Result<CreateOrUpdateUserResponse> {
    let db_last_updated_with: DbUserLastUpdatedWith = last_updated_with.into();
    let backfill_only = matches!(mode, UserUpsertMode::BackfillUnnamedStub);

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
            RETURNING (xmax = 0) AS created, id, name, email, created_at, updated_at, last_updated_with, user_type
        )
        SELECT
            u.created AS "created!",
            u.id AS "id!",
            u.name,
            u.email,
            u.created_at AS "created_at!",
            u.updated_at,
            u.last_updated_with AS "last_updated_with!: DbUserLastUpdatedWith",
            u.user_type AS "user_type!: DbUserType"
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
            e.user_type AS "user_type!: DbUserType"
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
    .fetch_one(connection)
    .await
    .map_err(|e| e.into_error_model("Error creating or updating user".to_string()))?;
    let created = user.created;
    let user = UserRow {
        id: user.id,
        name: user.name,
        email: user.email,
        user_type: user.user_type,
        last_updated_with: user.last_updated_with,
        created_at: user.created_at,
        updated_at: user.updated_at,
    };

    Ok(if created {
        CreateOrUpdateUserResponse::Created(User::try_from(user)?)
    } else {
        CreateOrUpdateUserResponse::Updated(User::try_from(user)?)
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

    #[sqlx::test]
    async fn test_create_or_update_user(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let user_id = UserId::new_unchecked("oidc", "test_user_1");
        let user_name = "Test User 1";

        create_or_update_user(
            &user_id,
            user_name,
            None,
            UserLastUpdatedWith::CreateEndpoint,
            UserType::Human,
            UserUpsertMode::Overwrite,
            &state.read_write.write_pool,
        )
        .await
        .unwrap();

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
        create_or_update_user(
            &user_id,
            user_name,
            None,
            UserLastUpdatedWith::CreateEndpoint,
            UserType::Human,
            UserUpsertMode::Overwrite,
            &state.read_write.write_pool,
        )
        .await
        .unwrap();

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

    #[sqlx::test]
    async fn test_search_user(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let user_id = UserId::new_unchecked("kubernetes", "test_user_1");
        let user_name = "Test User 1";

        create_or_update_user(
            &user_id,
            user_name,
            None,
            UserLastUpdatedWith::UpdateEndpoint,
            UserType::Application,
            UserUpsertMode::Overwrite,
            &state.read_write.write_pool,
        )
        .await
        .unwrap();

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
        delete_user(user_id.clone(), &state.read_write.write_pool)
            .await
            .unwrap();
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

        create_or_update_user(
            &user_id,
            user_name,
            None,
            UserLastUpdatedWith::ConfigCallCreation,
            UserType::Application,
            UserUpsertMode::Overwrite,
            &state.read_write.write_pool,
        )
        .await
        .unwrap();

        delete_user(user_id, &state.read_write.write_pool)
            .await
            .unwrap();

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
        let result = delete_user(user_id, &state.read_write.write_pool)
            .await
            .unwrap();
        assert_eq!(result, None);
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

        create_or_update_user(
            &user_id,
            "Test User 1",
            None,
            UserLastUpdatedWith::ConfigCallCreation,
            UserType::Application,
            UserUpsertMode::Overwrite,
            &state.read_write.write_pool,
        )
        .await
        .unwrap();

        // First delete acts on the active row.
        let first = delete_user(user_id.clone(), &state.read_write.write_pool)
            .await
            .unwrap();
        assert!(first.is_some());

        // Second delete finds no active row → no-op, no tombstone reset.
        let second = delete_user(user_id, &state.read_write.write_pool)
            .await
            .unwrap();
        assert_eq!(second, None);
    }

    #[sqlx::test]
    async fn test_paginate_user(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        for i in 0..10 {
            let user_id = UserId::new_unchecked("oidc", &format!("test_user_{i}"));
            let user_name = &format!("test user {i}");

            create_or_update_user(
                &user_id,
                user_name,
                None,
                UserLastUpdatedWith::ConfigCallCreation,
                UserType::Application,
                UserUpsertMode::Overwrite,
                &state.read_write.write_pool,
            )
            .await
            .unwrap();
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
