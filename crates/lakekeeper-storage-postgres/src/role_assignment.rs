use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use lakekeeper::{
    ProjectId,
    service::{
        AddRoleMembersError, AddRoleMembersResult, AddUserRoleAssignmentsError,
        AddUserRoleAssignmentsResult, ArcProjectId, ArcRoleIdent, AssignedRole, AssignedUser,
        CatalogBackendError, CatalogRoleForAssignment, CatalogUserRoleAssignmentUser,
        DatabaseIntegrityError, ErrorModel, InvalidPaginationToken, LAKEKEEPER_ROLE_PROVIDER_NAME,
        ListRoleMembersResult, ListRolesPage, ListUserRoleAssignmentsResult,
        RemoveRoleMembersError, RemoveRoleMembersResult, RemoveUserRoleAssignmentsError,
        RemoveUserRoleAssignmentsResult, RoleAssignmentUserNotFound, RoleId,
        RoleIdNotFoundInProject, RoleIdent, RoleMemberKind, RoleMembershipCycle,
        RoleMembershipDepthExceeded, RoleMembershipDirection, RoleMembershipEntry,
        RoleMembershipLockTimeout, RoleNameAlreadyExists, RoleNotManuallyAssignable,
        RoleProviderId, SYSTEM_ROLE_PROVIDER_NAME, SyncRoleMembersError, SyncRoleMembersResult,
        SyncUserRoleAssignmentsError, SyncUserRoleAssignmentsResult, UniqueMembers, UniqueRoles,
        UserProviderSyncInfo,
        authn::UserId,
        authz::{ListRoleAssignmentsResultPage, RoleAssignmentRow, UserOrRoleId},
    },
};
use uuid::Uuid;

use super::{
    dbutils::DBErrorHandler,
    pagination::{PaginateToken, V1PaginateToken},
    user::{DbUserLastUpdatedWith, DbUserType},
};

// ─── helpers ──────────────────────────────────────────────────────────────────

/// Maps a sqlx error to `RoleNameAlreadyExists` when the unique name constraint
/// fires, otherwise falls back to the generic backend error conversion.
fn map_role_upsert_error<E>(e: sqlx::Error) -> E
where
    E: From<RoleNameAlreadyExists> + From<CatalogBackendError>,
{
    if let sqlx::Error::Database(ref db) = e
        && db.is_unique_violation()
        && db.constraint() == Some("unique_role_name_in_project")
    {
        return RoleNameAlreadyExists::new()
            .append_detail(db.message())
            .into();
    }
    e.into_catalog_backend_error().into()
}

fn user_id_from_db(s: &str) -> Result<UserId, DatabaseIntegrityError> {
    UserId::try_from(s).map_err(|e| DatabaseIntegrityError::new(e.message))
}

// ─── Row type shared by the two role-members list queries ─────────────────────

#[derive(sqlx::FromRow)]
struct RoleMembersRow {
    role_id: Uuid,
    source_id: String,
    provider_id: String,
    project_id: String,
    user_id: Option<String>,
    last_synced_at: Option<chrono::DateTime<chrono::Utc>>,
}

fn rows_to_list_role_members_result(
    rows: Vec<RoleMembersRow>,
) -> Result<Option<ListRoleMembersResult>, DatabaseIntegrityError> {
    let Some(first) = rows.first() else {
        return Ok(None);
    };

    let role_id = RoleId::new(first.role_id);
    let role_ident: ArcRoleIdent = Arc::new(RoleIdent::from_db_unchecked(
        first.provider_id.clone(),
        first.source_id.clone(),
    ));
    let project_id = Arc::new(ProjectId::from_db_unchecked(first.project_id.clone()));
    let last_synced_at = first.last_synced_at;

    let members: Vec<AssignedUser> = rows
        .into_iter()
        .filter_map(|r| r.user_id)
        .map(|id| {
            user_id_from_db(&id).map(|user_id| AssignedUser {
                user_id: Arc::new(user_id),
            })
        })
        .collect::<Result<_, _>>()?;

    Ok(Some(ListRoleMembersResult {
        role_id,
        project_id,
        role_ident,
        members,
        last_synced_at,
    }))
}

// ─── sync_role_members_by_ident ───────────────────────────────────────────────
//
// Single CTE round-trip (PG15-compatible):
//   1. Upsert role                               (upserted_role)
//   2. Upsert desired users                      (upserted_users)
//   3. DELETE stale members                      (removed_members)
//   4. INSERT new members ON CONFLICT DO NOTHING (added_members)
//   5. Record sync time                          (sync_ts)
#[allow(clippy::too_many_lines)]
pub(crate) async fn sync_role_members_by_ident(
    project_id: &ProjectId,
    role: &CatalogRoleForAssignment<'_>,
    members: UniqueMembers<'_, '_>,
    transaction: &mut sqlx::Transaction<'static, sqlx::Postgres>,
) -> Result<SyncRoleMembersResult, SyncRoleMembersError> {
    let user_ids: Vec<String> = members.iter().map(|m| m.user_id.to_string()).collect();
    let user_names: Vec<Option<&str>> = members.iter().map(|m| m.name).collect();
    let user_emails: Vec<Option<&str>> = members.iter().map(|m| m.email).collect();
    let user_types: Vec<Option<DbUserType>> = members
        .iter()
        .map(|m| m.user_type.map(DbUserType::from))
        .collect();
    let user_updated_withs: Vec<DbUserLastUpdatedWith> = members
        .iter()
        .map(|m| DbUserLastUpdatedWith::from(m.updated_with))
        .collect();
    // Capture the timestamp on the client so the value written to Postgres
    // and the value returned to the caller are identical, avoiding any
    // clock skew between the DB server and the application server.
    let synced_at = chrono::Utc::now();

    let row = sqlx::query!(
        r#"
        WITH
        upserted_role AS (
            INSERT INTO "role" (id, name, description, source_id, provider_id, project_id)
            VALUES (gen_random_uuid(), COALESCE($1, $3 || ' in Provider ' || $4), NULLIF($2, ''), $3, $4, $5)
            ON CONFLICT ON CONSTRAINT unique_role_provider_source_in_project
            DO UPDATE SET name        = COALESCE($1, "role".name),
                          description = CASE WHEN $2 IS NULL THEN "role".description ELSE NULLIF($2, '') END
            WHERE "role".name IS DISTINCT FROM COALESCE($1, "role".name)
               OR "role".description IS DISTINCT FROM
                  CASE WHEN $2 IS NULL THEN "role".description ELSE NULLIF($2, '') END
            RETURNING id
        ),
        -- Fallback: when the upsert WHERE was false (nothing changed) upserted_role
        -- returns no rows.  Union with a direct lookup so downstream CTEs always
        -- have exactly one role id regardless of whether the row was written.
        role_id AS (
            SELECT id FROM upserted_role
            UNION ALL
            SELECT id FROM "role"
            WHERE source_id = $3 AND provider_id = $4 AND project_id = $5
              AND NOT EXISTS (SELECT 1 FROM upserted_role)
        ),
        user_input AS (
            SELECT u.id,
                   u.name,
                   COALESCE(u.name, 'Nameless User with id ' || u.id) AS effective_name,
                   u.email,
                   NULLIF(u.email, '') AS effective_email,
                   u.utype,
                   COALESCE(u.utype, 'human'::user_type) AS effective_utype,
                   u.updated_with
            FROM UNNEST($6::TEXT[], $7::TEXT[], $8::TEXT[], $9::user_type[], $10::user_last_updated_with[])
                AS u(id, name, email, utype, updated_with)
        ),
        upserted_users AS (
            INSERT INTO users (id, name, email, last_updated_with, user_type)
            SELECT id, effective_name, effective_email, updated_with, effective_utype
            FROM user_input
            ON CONFLICT (id) DO UPDATE SET
                name              = COALESCE(
                                        (SELECT name FROM user_input WHERE id = EXCLUDED.id),
                                        users.name),
                email             = CASE
                                        WHEN (SELECT email FROM user_input WHERE id = EXCLUDED.id) IS NULL
                                            THEN users.email
                                        ELSE NULLIF(
                                                (SELECT email FROM user_input WHERE id = EXCLUDED.id),
                                                '')
                                    END,
                last_updated_with = EXCLUDED.last_updated_with,
                user_type         = COALESCE(
                                        (SELECT utype FROM user_input WHERE id = EXCLUDED.id),
                                        users.user_type),
                deleted_at        = null
            WHERE users.name IS DISTINCT FROM COALESCE(
                                                 (SELECT name FROM user_input WHERE id = EXCLUDED.id),
                                                 users.name)
               OR users.email IS DISTINCT FROM
                  CASE WHEN (SELECT email FROM user_input WHERE id = EXCLUDED.id) IS NULL
                           THEN users.email
                       ELSE NULLIF((SELECT email FROM user_input WHERE id = EXCLUDED.id), '')
                  END
               OR users.user_type IS DISTINCT FROM COALESCE(
                                                        (SELECT utype FROM user_input WHERE id = EXCLUDED.id),
                                                        users.user_type)
               OR users.deleted_at IS NOT NULL
        ),
        -- Remove members no longer in the desired set.
        -- References user_input (all requested users) rather than upserted_users
        -- so that unchanged rows skipped by the ON CONFLICT WHERE clause are
        -- still treated as members-to-keep.
        removed_members AS (
            DELETE FROM role_assignment
            WHERE role_id = (SELECT id FROM role_id)
              AND user_id NOT IN (SELECT id FROM user_input)
            RETURNING user_id
        ),
        added_members AS (
            INSERT INTO role_assignment (user_id, role_id)
            SELECT id, (SELECT id FROM role_id)
            FROM user_input
            ON CONFLICT (user_id, role_id) DO NOTHING
            RETURNING user_id
        ),
        sync_ts AS (
            INSERT INTO role_members_sync (role_id, synced_at)
            VALUES ((SELECT id FROM role_id), $11)
            ON CONFLICT (role_id) DO UPDATE SET synced_at = $11
            RETURNING synced_at
        )
        SELECT
            (SELECT id        FROM role_id)                                                       AS "role_id!: Uuid",
            (SELECT synced_at FROM sync_ts)                                                       AS "synced_at!",
            (SELECT COALESCE(array_agg(user_id), ARRAY[]::TEXT[]) FROM added_members)   AS "added_ids!: Vec<String>",
            (SELECT COALESCE(array_agg(user_id), ARRAY[]::TEXT[]) FROM removed_members) AS "removed_ids!: Vec<String>"
        "#,
        role.name as Option<&str>,
        role.description as Option<&str>,
        role.ident.source_id().as_str(),
        role.ident.provider_id().as_str(),
        &**project_id,
        &user_ids as &[String],
        &user_names as &[Option<&str>],
        &user_emails as &[Option<&str>],
        &user_types as &[Option<DbUserType>],
        &user_updated_withs as &[DbUserLastUpdatedWith],
        synced_at,
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(map_role_upsert_error::<SyncRoleMembersError>)?;

    let role_id = RoleId::new(row.role_id);

    let added = row
        .added_ids
        .into_iter()
        .map(|id| {
            user_id_from_db(&id).map(|uid| AssignedUser {
                user_id: Arc::new(uid),
            })
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(SyncRoleMembersError::from)?;

    let removed = row
        .removed_ids
        .into_iter()
        .map(|id| {
            user_id_from_db(&id).map(|uid| AssignedUser {
                user_id: Arc::new(uid),
            })
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(SyncRoleMembersError::from)?;

    Ok(SyncRoleMembersResult {
        role_id,
        added,
        removed,
        synced_at: row.synced_at,
    })
}

// ─── sync_user_role_assignments_by_provider ───────────────────────────────────
//
// Two queries, one transaction:
//   Query 1 – single CTE (PG15-compatible):
//     1. Upsert the user                          (upserted_user)
//     2. Bulk upsert desired roles                 (upserted_roles)
//     3. DELETE stale assignments                  (removed_assignments)
//     4. INSERT new assignments ON CONFLICT DO NOTHING (added_assignments)
//     5. Upsert sync record                        (sync_ts)
//   Query 2 – plain SELECT via list_role_assignments_for_user:
//     Reads the post-sync state (writes from query 1 are visible to subsequent
//     statements in the same transaction).
#[allow(clippy::too_many_lines)]
pub(crate) async fn sync_user_role_assignments_by_provider(
    user: &CatalogUserRoleAssignmentUser<'_>,
    project_id: &ProjectId,
    provider_id: &RoleProviderId,
    roles: UniqueRoles<'_, '_>,
    transaction: &mut sqlx::Transaction<'static, sqlx::Postgres>,
) -> Result<SyncUserRoleAssignmentsResult, SyncUserRoleAssignmentsError> {
    let role_names: Vec<Option<&str>> = roles.iter().map(|r| r.name).collect();
    let role_descs: Vec<Option<&str>> = roles.iter().map(|r| r.description).collect();
    let role_src_ids: Vec<&str> = roles.iter().map(|r| r.ident.source_id().as_str()).collect();
    // Capture the timestamp on the client so the value written to Postgres
    // and the value returned to the caller are identical, avoiding any
    // clock skew between the DB server and the application server.
    let synced_at = chrono::Utc::now();

    let row = sqlx::query!(
        r#"
        WITH
        upserted_user AS (
            INSERT INTO users (id, name, email, last_updated_with, user_type)
            VALUES ($1, COALESCE($2, 'Nameless User with id ' || $1), NULLIF($3, ''), $4, COALESCE($5, 'human'::user_type))
            ON CONFLICT (id) DO UPDATE SET
                name              = COALESCE($2, users.name),
                email             = CASE WHEN $3 IS NULL THEN users.email ELSE NULLIF($3, '') END,
                last_updated_with = EXCLUDED.last_updated_with,
                user_type         = COALESCE($5, users.user_type),
                deleted_at        = null
            WHERE users.name IS DISTINCT FROM COALESCE($2, users.name)
               OR users.email IS DISTINCT FROM
                  CASE WHEN $3 IS NULL THEN users.email ELSE NULLIF($3, '') END
               OR users.user_type IS DISTINCT FROM COALESCE($5, users.user_type)
               OR users.deleted_at IS NOT NULL
        ),
        role_input AS (
            SELECT u.name,
                   COALESCE(u.name, u.source_id || ' in Provider ' || $6) AS effective_name,
                   u.description,
                   NULLIF(u.description, '') AS effective_description,
                   u.source_id
            FROM UNNEST($8::TEXT[], $9::TEXT[], $10::TEXT[]) AS u(name, description, source_id)
        ),
        upserted_roles AS (
            INSERT INTO "role" (id, name, description, source_id, provider_id, project_id)
            SELECT gen_random_uuid(), effective_name, effective_description, source_id, $6, $7
            FROM role_input
            ON CONFLICT ON CONSTRAINT unique_role_provider_source_in_project DO UPDATE SET
                name        = COALESCE(
                                  (SELECT name FROM role_input WHERE source_id = EXCLUDED.source_id),
                                  "role".name),
                description = CASE
                                  WHEN (SELECT description FROM role_input WHERE source_id = EXCLUDED.source_id) IS NULL
                                      THEN "role".description
                                  ELSE NULLIF(
                                          (SELECT description FROM role_input WHERE source_id = EXCLUDED.source_id),
                                          '')
                              END
            WHERE "role".name IS DISTINCT FROM COALESCE(
                                                  (SELECT name FROM role_input WHERE source_id = EXCLUDED.source_id),
                                                  "role".name)
               OR "role".description IS DISTINCT FROM
                  CASE WHEN (SELECT description FROM role_input WHERE source_id = EXCLUDED.source_id) IS NULL
                           THEN "role".description
                       ELSE NULLIF(
                               (SELECT description FROM role_input WHERE source_id = EXCLUDED.source_id),
                               '')
                  END
            RETURNING id
        ),
        -- Fallback: when the upsert WHERE was false (nothing changed) upserted_roles
        -- returns no rows for those entries.  RETURNING id is available directly
        -- (unlike reading the "role" table, which uses the pre-statement snapshot
        -- and cannot see rows inserted by upserted_roles in the same query).
        -- Arm 1: newly inserted roles and updated roles come from RETURNING.
        -- Arm 2: roles that existed but were unchanged (WHERE skipped them) are
        --        found in the old "role" snapshot; they are absent from upserted_roles.
        role_ids AS (
            SELECT id FROM upserted_roles
            UNION ALL
            SELECT r.id
            FROM "role" r
            JOIN role_input ri ON r.source_id = ri.source_id
              AND r.provider_id = $6 AND r.project_id = $7
            WHERE NOT EXISTS (
                SELECT 1 FROM upserted_roles ur WHERE ur.id = r.id
            )
        ),
        removed_assignments AS (
            DELETE FROM role_assignment
            WHERE user_id = $1
              AND role_id IN (
                  SELECT id FROM "role" WHERE provider_id = $6 AND project_id = $7
              )
              AND role_id NOT IN (SELECT id FROM role_ids)
            RETURNING role_id
        ),
        added_assignments AS (
            INSERT INTO role_assignment (user_id, role_id)
            SELECT $1, id FROM role_ids
            ON CONFLICT (user_id, role_id) DO NOTHING
            RETURNING role_id
        ),
        sync_ts AS (
            INSERT INTO role_assignment_sync (user_id, project_id, provider_id, synced_at)
            VALUES ($1, $7, $6, $11)
            ON CONFLICT (user_id, project_id, provider_id) DO UPDATE SET synced_at = $11
            RETURNING synced_at
        )
        SELECT
            (SELECT synced_at FROM sync_ts)                                                                AS "synced_at!",
            (SELECT COALESCE(array_agg(role_id), ARRAY[]::UUID[]) FROM added_assignments)   AS "added_ids!: Vec<Uuid>",
            (SELECT COALESCE(array_agg(role_id), ARRAY[]::UUID[]) FROM removed_assignments) AS "removed_ids!: Vec<Uuid>"
        "#,
        user.user_id.to_string(),
        user.name as Option<&str>,
        user.email,
        DbUserLastUpdatedWith::from(user.updated_with.clone()) as _,
        user.user_type.map(DbUserType::from) as Option<DbUserType>,
        provider_id.as_str(),
        &**project_id,
        &role_names as &[Option<&str>],
        &role_descs as &[Option<&str>],
        &role_src_ids as &[&str],
        synced_at,
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(map_role_upsert_error::<SyncUserRoleAssignmentsError>)?;

    // Second query within the same transaction: the CTE-driven INSERT/DELETE
    // (added_assignments / removed_assignments) and sync_ts upsert writes are
    // now visible, so a plain SELECT gives the authoritative post-sync state
    // without any snapshot-visibility gymnastics.
    let all_assignments = list_role_assignments_for_user(user.user_id, &mut **transaction)
        .await
        .map_err(SyncUserRoleAssignmentsError::from)?;

    Ok(SyncUserRoleAssignmentsResult {
        added: row.added_ids.into_iter().map(RoleId::new).collect(),
        removed: row.removed_ids.into_iter().map(RoleId::new).collect(),
        synced_at: row.synced_at,
        all_roles: all_assignments.roles,
        provider_sync_times: all_assignments.provider_sync_times,
    })
}

// ─── list_role_assignments_for_user ──────────────────────────────────────────

pub(crate) async fn list_role_assignments_for_user<
    'c,
    'e: 'c,
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
>(
    user_id: &UserId,
    connection: E,
) -> Result<ListUserRoleAssignmentsResult, CatalogBackendError> {
    let rows = sqlx::query!(
        r#"
        WITH RECURSIVE effective_roles(role_id) AS (
                SELECT ur.role_id FROM role_assignment ur WHERE ur.user_id = $1
            UNION
                SELECT rm.parent_role_id
                FROM role_membership rm
                JOIN effective_roles er ON rm.member_role_id = er.role_id
        ) CYCLE role_id SET is_cycle USING path,
        assigned AS (
            SELECT
                r.id          AS role_id,
                r.source_id,
                r.provider_id,
                r.project_id,
                s.project_id  AS sync_project_id,
                s.provider_id AS sync_provider_id,
                s.synced_at
            FROM effective_roles er
            JOIN "role" r ON r.id = er.role_id
            LEFT JOIN role_assignment_sync s
                ON  s.user_id     = $1
                AND s.provider_id = r.provider_id
                AND s.project_id  = r.project_id
        )
        SELECT
            role_id          AS "role_id?: Uuid",
            source_id        AS "source_id?",
            provider_id      AS "provider_id?",
            project_id       AS "project_id?",
            sync_project_id  AS "sync_project_id?",
            sync_provider_id AS "sync_provider_id?",
            synced_at        AS "synced_at?"
        FROM assigned

        UNION ALL

        -- Providers that have been synced but currently have no assignments.
        -- These rows carry only sync metadata; all role columns are NULL.
        -- The NOT EXISTS check reuses the already-computed `assigned` CTE
        -- instead of re-joining role_assignment + role.
        SELECT
            NULL::UUID AS "role_id?: Uuid",
            NULL::TEXT AS "source_id?",
            NULL::TEXT AS "provider_id?",
            NULL::TEXT AS "project_id?",
            s.project_id,
            s.provider_id,
            s.synced_at
        FROM role_assignment_sync s
        WHERE s.user_id = $1
          AND NOT EXISTS (
              SELECT 1
              FROM assigned
              WHERE assigned.provider_id = s.provider_id
                AND assigned.project_id  = s.project_id
          )
        "#,
        user_id.to_string(),
    )
    .fetch_all(connection)
    .await
    .map_err(super::dbutils::DBErrorHandler::into_catalog_backend_error)?;

    let mut roles = Vec::with_capacity(rows.len());
    let mut seen_sync = HashSet::new();
    let mut provider_sync_times: Vec<UserProviderSyncInfo> = Vec::new();

    for row in rows {
        if let (Some(role_id), Some(source_id), Some(provider_id_r), Some(project_id_r)) =
            (row.role_id, row.source_id, row.provider_id, row.project_id)
        {
            roles.push(AssignedRole {
                role_id: RoleId::new(role_id),
                role_ident: Arc::new(RoleIdent::from_db_unchecked(provider_id_r, source_id)),
                project_id: Arc::new(ProjectId::from_db_unchecked(project_id_r)),
            });
        }

        if let (Some(sp), Some(prov), Some(sat)) =
            (row.sync_project_id, row.sync_provider_id, row.synced_at)
            && seen_sync.insert((sp.clone(), prov.clone()))
        {
            provider_sync_times.push(UserProviderSyncInfo {
                project_id: Arc::new(ProjectId::from_db_unchecked(sp)),
                provider_id: RoleProviderId::new_unchecked(prov),
                synced_at: sat,
            });
        }
    }

    Ok(ListUserRoleAssignmentsResult {
        roles,
        provider_sync_times,
    })
}

// ─── list_role_assignments_for_role ──────────────────────────────────────────

pub(crate) async fn list_role_assignments_for_role<
    'c,
    'e: 'c,
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
>(
    role_id: RoleId,
    connection: E,
) -> Result<Option<ListRoleMembersResult>, CatalogBackendError> {
    let rows = sqlx::query_as!(
        RoleMembersRow,
        r#"
        SELECT
            r.id          AS role_id,
            r.source_id,
            r.provider_id,
            r.project_id,
            CASE WHEN ur.role_id IS NOT NULL THEN u.id END AS user_id,
            rms.synced_at AS last_synced_at
        FROM "role" r
        LEFT JOIN role_assignment     ur  ON ur.role_id  = r.id
        LEFT JOIN users         u   ON u.id         = ur.user_id
        LEFT JOIN role_members_sync rms ON rms.role_id = r.id
        WHERE r.id = $1
        "#,
        *role_id,
    )
    .fetch_all(connection)
    .await
    .map_err(super::dbutils::DBErrorHandler::into_catalog_backend_error)?;

    rows_to_list_role_members_result(rows).map_err(CatalogBackendError::new_unexpected)
}

// ─── list_role_assignments_for_role_by_ident ──────────────────────────────────

pub(crate) async fn list_role_assignments_for_role_by_ident<
    'c,
    'e: 'c,
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
>(
    project_id: &ProjectId,
    role_ident: &RoleIdent,
    connection: E,
) -> Result<Option<ListRoleMembersResult>, CatalogBackendError> {
    let rows = sqlx::query_as!(
        RoleMembersRow,
        r#"
        SELECT
            r.id          AS role_id,
            r.source_id,
            r.provider_id,
            r.project_id,
            CASE WHEN ur.role_id IS NOT NULL THEN u.id END AS user_id,
            rms.synced_at AS last_synced_at
        FROM "role" r
        LEFT JOIN role_assignment     ur  ON ur.role_id  = r.id
        LEFT JOIN users         u   ON u.id         = ur.user_id
        LEFT JOIN role_members_sync rms ON rms.role_id = r.id
        WHERE r.project_id  = $1
          AND r.provider_id = $2
          AND r.source_id   = $3
        "#,
        &**project_id,
        role_ident.provider_id().as_str(),
        role_ident.source_id().as_str(),
    )
    .fetch_all(connection)
    .await
    .map_err(super::dbutils::DBErrorHandler::into_catalog_backend_error)?;

    rows_to_list_role_members_result(rows).map_err(CatalogBackendError::new_unexpected)
}

/// Seed for the per-project role-membership advisory-lock key. The lock key is
/// `hashtextextended(project_id, SEED)` — a 64-bit, seeded hash. The seed keeps
/// this key from colliding with the migrator's advisory lock (a different int8
/// key) or any other future advisory-lock purpose, and using the 64-bit
/// `hashtextextended` (rather than 32-bit `hashtext`) makes cross-project
/// false-sharing negligible. (`rolembrs` in ASCII.)
const ROLE_MEMBERSHIP_LOCK_SEED: i64 = 0x726F_6C65_6D62_7273;

/// Map a Postgres error to [`AddRoleMembersError`]: a `lock_timeout` expiry
/// (SQLSTATE `55P03`) becomes the retriable [`RoleMembershipLockTimeout`], anything
/// else a backend error. Used for the lock-bounded statements (advisory lock, `FOR SHARE`).
fn map_lock_timeout(project_id: &ArcProjectId) -> impl FnOnce(sqlx::Error) -> AddRoleMembersError {
    let project_id = project_id.clone();
    move |e| match e.as_database_error().and_then(|db| db.code()) {
        // 55P03 = lock_not_available: the `lock_timeout` elapsed waiting on a lock.
        Some(code) if code.as_ref() == "55P03" => RoleMembershipLockTimeout::new(project_id).into(),
        _ => super::dbutils::DBErrorHandler::into_catalog_backend_error(e).into(),
    }
}

// ─── add_role_members ─────────────────────────────────────────────────────────
//
// 0. Take a per-project advisory lock so concurrent adds serialize (otherwise two
//    adds with different parents — e.g. (A,B) and (B,A) — could each pass their
//    cycle check against pre-insert state and together close a cycle).
// 1. Validate parent + every member: exists, in `project_id`, catalog-managed.
// 2. Reject cycles per member (member already a transitive ancestor of parent,
//    or member == parent).
// 3. Idempotently insert the edges.
pub(crate) async fn add_role_members(
    project_id: &ArcProjectId,
    parent_role_id: RoleId,
    member_role_ids: &[RoleId],
    max_depth: usize,
    transaction: &mut sqlx::Transaction<'static, sqlx::Postgres>,
) -> Result<AddRoleMembersResult, AddRoleMembersError> {
    // Serialize membership writes per project via a transaction-scoped advisory
    // lock. It is auto-released on commit/rollback/error or when a dead backend
    // is reaped — no manual unlock, no leak. `lock_timeout` bounds the wait so a
    // stuck lock fails fast with a typed, retriable error instead of hanging;
    // legitimate concurrent writers just queue for a few ms and both succeed.
    // Reset to DEFAULT after the lock+FOR SHARE window so the bound doesn't leak
    // onto a caller's later statements when this composes into their transaction.
    sqlx::query("SET LOCAL lock_timeout = '3s'")
        .execute(&mut **transaction)
        .await
        .map_err(super::dbutils::DBErrorHandler::into_catalog_backend_error)?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, $2))")
        .bind(project_id.as_str())
        .bind(ROLE_MEMBERSHIP_LOCK_SEED)
        .execute(&mut **transaction)
        .await
        .map_err(map_lock_timeout(project_id))?;

    // Deduplicate members up front: duplicates would only trigger redundant
    // per-member recursive cycle queries and the insert is idempotent anyway.
    // Order is irrelevant for the validation/cycle/insert work below.
    let member_role_ids: Vec<RoleId> = {
        let mut seen: HashSet<Uuid> = HashSet::with_capacity(member_role_ids.len());
        member_role_ids
            .iter()
            .filter(|r| seen.insert(***r))
            .copied()
            .collect()
    };
    let member_role_ids: &[RoleId] = &member_role_ids;

    // Fetch provider_id for the parent and every distinct member that exists in
    // `project_id`. Rows missing from the result are either non-existent or
    // belong to a different project — both map to RoleIdNotFoundInProject.
    let mut wanted: HashSet<Uuid> = member_role_ids.iter().map(|r| **r).collect();
    wanted.insert(*parent_role_id);
    let wanted_ids: Vec<Uuid> = wanted.into_iter().collect();

    // `FOR SHARE` locks the role rows so a concurrent delete can't land between this
    // read and the INSERT below (FK references `role` ON DELETE CASCADE). A competing
    // delete waits for our commit, bounded by `lock_timeout` → `RoleMembershipLockTimeout`.
    let rows = sqlx::query!(
        r#"
        SELECT id, provider_id
        FROM "role"
        WHERE project_id = $1
          AND id = ANY($2::uuid[])
        FOR SHARE
        "#,
        project_id.as_str(),
        &wanted_ids,
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(map_lock_timeout(project_id))?;

    // End of the lock-contended window: restore the default lock_timeout for the
    // rest of the transaction. The advisory lock serializes writers and we now hold
    // `FOR SHARE` on the roles, so the cycle check and INSERT below don't wait on
    // locks; bounding them is unnecessary and would otherwise leak to the caller.
    sqlx::query("SET LOCAL lock_timeout = DEFAULT")
        .execute(&mut **transaction)
        .await
        .map_err(super::dbutils::DBErrorHandler::into_catalog_backend_error)?;

    let mut provider_by_id: HashMap<Uuid, String> = HashMap::with_capacity(rows.len());
    for row in rows {
        provider_by_id.insert(row.id, row.provider_id);
    }

    // Validate parent then every member: present in project + catalog-managed.
    let validate = |role_id: RoleId| -> Result<(), AddRoleMembersError> {
        let Some(provider) = provider_by_id.get(&*role_id) else {
            return Err(RoleIdNotFoundInProject::new(role_id, project_id.clone()).into());
        };
        let provider = RoleProviderId::new_unchecked(provider.clone());
        if !(provider.is_lakekeeper() || provider.is_system()) {
            return Err(RoleNotManuallyAssignable::new(role_id, provider).into());
        }
        Ok(())
    };
    validate(parent_role_id)?;

    // Empty input is a no-op — but only once the parent has been validated above
    // (so a bad parent is still rejected). Read back the current direct members so
    // `members` reflects the parent's state.
    if member_role_ids.is_empty() {
        let members = list_role_memberships(
            parent_role_id,
            RoleMembershipDirection::Members,
            &mut **transaction,
        )
        .await?;
        return Ok(AddRoleMembersResult {
            added: Vec::new(),
            members,
        });
    }

    for member in member_role_ids {
        validate(*member)?;
    }

    // Cycle check before any insert: a member equal to, or already a transitive
    // ancestor of, the parent would close a cycle. A direct self-edge is a cheap
    // in-memory check; the transitive case computes the parent's ancestor closure
    // once and intersects it with all members in a single query.
    //
    // No `project_id` predicate: validation above rejects cross-project endpoints,
    // so the graph never spans projects. Add a project scope if that's relaxed.
    let member_uuids: Vec<Uuid> = member_role_ids.iter().map(|r| **r).collect();
    if let Some(member) = member_role_ids.iter().find(|m| ***m == *parent_role_id) {
        return Err(RoleMembershipCycle::new(parent_role_id, *member).into());
    }
    if let Some(cycle_member) = sqlx::query_scalar!(
        r#"
        WITH RECURSIVE ancestors(role_id) AS (
            SELECT parent_role_id FROM role_membership WHERE member_role_id = $1
            UNION
            SELECT rm.parent_role_id FROM role_membership rm
            JOIN ancestors a ON rm.member_role_id = a.role_id
        )
        SELECT role_id AS "role_id!" FROM ancestors WHERE role_id = ANY($2::uuid[]) LIMIT 1
        "#,
        *parent_role_id,
        &member_uuids,
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(super::dbutils::DBErrorHandler::into_catalog_backend_error)?
    {
        return Err(RoleMembershipCycle::new(parent_role_id, RoleId::new(cycle_member)).into());
    }

    // Depth check before any insert: an edge that would make some role-nesting
    // chain longer than `max_depth` (number of role→role edges) is rejected. The
    // longest chain through a new edge `parent -> member` is
    // `longest_chain_above(parent) + 1 + longest_chain_below(member)`. We compute
    // the parent's upward depth once and each candidate member's downward depth,
    // and reject the first member for which the sum exceeds the bound. Cycles were
    // already ruled out above, so both walks terminate; the `d < $3` guards bound
    // the recursion defensively. Saturating the bound to i32 avoids overflow/panic
    // for pathologically large configured limits (the depth columns are `int4`).
    //
    // Catalog-path invariant only: the OpenFGA path tolerates depth and relies on
    // its own resolution limits, the same asymmetry as cycle prevention.
    let max_depth_bound = i32::try_from(max_depth).unwrap_or(i32::MAX);
    if let Some(member) = sqlx::query_scalar!(
        r#"
        WITH RECURSIVE
        up(role_id, d) AS (
            SELECT $1::uuid, 0
          UNION ALL
            SELECT rm.parent_role_id, up.d + 1
            FROM role_membership rm JOIN up ON rm.member_role_id = up.role_id
            WHERE up.d < $3
        ),
        down(seed, role_id, d) AS (
            SELECT m, m, 0 FROM unnest($2::uuid[]) AS m
          UNION ALL
            SELECT down.seed, rm.member_role_id, down.d + 1
            FROM role_membership rm JOIN down ON rm.parent_role_id = down.role_id
            WHERE down.d < $3
        )
        SELECT down.seed AS "member_role_id!"
        FROM down
        CROSS JOIN (SELECT max(d) AS up_max FROM up) u
        GROUP BY down.seed, u.up_max
        HAVING u.up_max + 1 + max(down.d) > $3
        LIMIT 1
        "#,
        *parent_role_id,
        &member_uuids,
        max_depth_bound,
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(super::dbutils::DBErrorHandler::into_catalog_backend_error)?
    {
        return Err(RoleMembershipDepthExceeded::new(
            parent_role_id,
            RoleId::new(member),
            max_depth,
        )
        .into());
    }

    // `ON CONFLICT DO NOTHING` skips already-present edges, so `RETURNING` yields
    // exactly the members that were newly added. `role.version` (ETag) is
    // intentionally NOT bumped: edges live in `role_membership`, and effective-role
    // freshness is handled by `USER_ASSIGNMENTS_CACHE` invalidation, not the role row.
    let added = sqlx::query_scalar!(
        r#"
        INSERT INTO role_membership (parent_role_id, member_role_id)
        SELECT $1, m FROM unnest($2::uuid[]) AS m
        ON CONFLICT (parent_role_id, member_role_id) DO NOTHING
        RETURNING member_role_id
        "#,
        *parent_role_id,
        &member_uuids,
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(super::dbutils::DBErrorHandler::into_catalog_backend_error)?
    .into_iter()
    .map(RoleId::new)
    .collect();

    // Read the parent's updated direct members back on the SAME transaction, so the
    // returned state reflects this write (a follow-up query — possibly on a lagging
    // read replica — would not be guaranteed to).
    let members = list_role_memberships(
        parent_role_id,
        RoleMembershipDirection::Members,
        &mut **transaction,
    )
    .await?;
    Ok(AddRoleMembersResult { added, members })
}

// ─── remove_role_members ──────────────────────────────────────────────────────

pub(crate) async fn remove_role_members(
    parent_role_id: RoleId,
    member_role_ids: &[RoleId],
    transaction: &mut sqlx::Transaction<'static, sqlx::Postgres>,
) -> Result<RemoveRoleMembersResult, RemoveRoleMembersError> {
    // No advisory lock (unlike `add_role_members`): a delete can't create a cycle.
    // A remove racing a concurrent add may make that add's cycle check spuriously
    // reject (harmless, retriable) — don't "fix" it with a lock.
    let removed = if member_role_ids.is_empty() {
        Vec::new()
    } else {
        let member_uuids: Vec<Uuid> = member_role_ids.iter().map(|r| **r).collect();
        // `RETURNING` yields exactly the edges that existed and were deleted.
        sqlx::query_scalar!(
            r#"
            DELETE FROM role_membership
            WHERE parent_role_id = $1
              AND member_role_id = ANY($2::uuid[])
            RETURNING member_role_id
            "#,
            *parent_role_id,
            &member_uuids,
        )
        .fetch_all(&mut **transaction)
        .await
        .map_err(super::dbutils::DBErrorHandler::into_catalog_backend_error)?
        .into_iter()
        .map(RoleId::new)
        .collect()
    };

    // Read the parent's updated direct members back on the SAME transaction (see
    // `add_role_members`) — read-your-writes, immune to read-replica lag.
    let members = list_role_memberships(
        parent_role_id,
        RoleMembershipDirection::Members,
        &mut **transaction,
    )
    .await?;
    Ok(RemoveRoleMembersResult { removed, members })
}

// ─── add_user_role_assignments ────────────────────────────────────────────────
//
// Additive user→role assignment (the catalog persistence path for the management
// API's user members). Bipartite, so — unlike `add_role_members` — no cycle is
// possible and no advisory lock is taken. `ON CONFLICT DO NOTHING` makes the
// insert idempotent at the row level, so concurrent identical adds are safe.

pub(crate) async fn add_user_role_assignments(
    project_id: &ArcProjectId,
    role_id: RoleId,
    user_ids: &[UserId],
    transaction: &mut sqlx::Transaction<'static, sqlx::Postgres>,
) -> Result<AddUserRoleAssignmentsResult, AddUserRoleAssignmentsError> {
    // `user_ids` is already deduplicated by the `*Ops` layer (backend-independent),
    // so `added` below never double-counts. We do NOT short-circuit on empty input:
    // the role must still be validated (exists in project + catalog-managed) so an
    // add to an unknown/unmanaged role fails identically regardless of member count
    // — mirroring `add_role_members`, which validates the parent before its
    // empty-input return.
    let user_id_strings: Vec<String> = user_ids.iter().map(ToString::to_string).collect();

    // Validate AND insert in a single round-trip. The FK on `role_assignment`
    // cannot carry any of the three checks below: `role_id → role(id)` is global
    // (not project-scoped, so a role in another project would satisfy it),
    // catalog-managedness is a column value (not a constraint), and the user FK
    // is satisfied by a soft-deleted row. So we validate explicitly — but in one
    // statement, which keeps the timing uniform across rejection reasons and
    // avoids extra round-trips.
    //
    // The `inserted` CTE is gated on full validity (role present, managed, and
    // every user existing & not soft-deleted), so nothing is written on any
    // error path — the SELECT then returns the state needed to raise the precise
    // typed error. `managed_providers` ($4) is the allowlist, passed from the
    // Rust constants so there is no SQL/Rust drift. `ON CONFLICT DO NOTHING` +
    // `RETURNING` makes the add idempotent and reports exactly the new rows.
    //
    // The role/user CTEs lock their rows `FOR UPDATE` so a concurrent role-delete
    // or `delete_user` soft-delete can't race the INSERT: the user FK passes
    // against a soft-deleted row, so without the lock we'd leave a dangling
    // assignment instead of the typed not-found.
    let managed_providers: [&str; 2] = [LAKEKEEPER_ROLE_PROVIDER_NAME, SYSTEM_ROLE_PROVIDER_NAME];
    let result = sqlx::query!(
        r#"
        WITH
        target_role AS (
            SELECT provider_id FROM "role" WHERE id = $1 AND project_id = $2 FOR UPDATE
        ),
        existing_users AS (
            SELECT id FROM users WHERE id = ANY($3::text[]) AND deleted_at IS NULL FOR UPDATE
        ),
        inserted AS (
            INSERT INTO role_assignment (user_id, role_id)
            SELECT u, $1 FROM unnest($3::text[]) AS u
            WHERE EXISTS (SELECT 1 FROM target_role WHERE provider_id = ANY($4::text[]))
              AND NOT EXISTS (
                  SELECT 1 FROM unnest($3::text[]) AS u2
                  WHERE u2 NOT IN (SELECT id FROM existing_users)
              )
            ON CONFLICT (user_id, role_id) DO NOTHING
            RETURNING user_id
        )
        SELECT
            (SELECT provider_id FROM target_role) AS "role_provider?",
            COALESCE((SELECT array_agg(id) FROM existing_users), ARRAY[]::text[])
                AS "existing_users!: Vec<String>",
            COALESCE((SELECT array_agg(user_id) FROM inserted), ARRAY[]::text[])
                AS "inserted_users!: Vec<String>"
        "#,
        *role_id,
        project_id.as_str(),
        &user_id_strings,
        &managed_providers as &[&str],
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(super::dbutils::DBErrorHandler::into_catalog_backend_error)?;

    // Decode the validation state into the precise typed error (nothing was
    // inserted unless every check passed, so these early returns leave no write).
    let Some(role_provider) = result.role_provider else {
        return Err(RoleIdNotFoundInProject::new(role_id, project_id.clone()).into());
    };
    let provider = RoleProviderId::new_unchecked(role_provider);
    if !(provider.is_lakekeeper() || provider.is_system()) {
        return Err(RoleNotManuallyAssignable::new(role_id, provider).into());
    }
    // Reuse the already-computed `user_id_strings` (same order as `user_ids`)
    // rather than re-`to_string()`ing each id; the lookup sets borrow `&str`.
    let existing: HashSet<&str> = result.existing_users.iter().map(String::as_str).collect();
    if let Some((missing, _)) = user_ids
        .iter()
        .zip(&user_id_strings)
        .find(|(_, s)| !existing.contains(s.as_str()))
    {
        return Err(RoleAssignmentUserNotFound::new(missing.clone()).into());
    }

    let inserted: HashSet<&str> = result.inserted_users.iter().map(String::as_str).collect();
    let added: Vec<UserId> = user_ids
        .iter()
        .zip(&user_id_strings)
        .filter(|(_, s)| inserted.contains(s.as_str()))
        .map(|(u, _)| u.clone())
        .collect();
    Ok(AddUserRoleAssignmentsResult { added })
}

// ─── remove_user_role_assignments ─────────────────────────────────────────────

/// `user_ids` is assumed unique (the `*Ops` layer deduplicates before calling):
/// `removed` is rebuilt from the input matched against `RETURNING`, so a duplicate
/// would otherwise be reported twice — same contract as `add_user_role_assignments`.
pub(crate) async fn remove_user_role_assignments(
    role_id: RoleId,
    user_ids: &[UserId],
    transaction: &mut sqlx::Transaction<'static, sqlx::Postgres>,
) -> Result<RemoveUserRoleAssignmentsResult, RemoveUserRoleAssignmentsError> {
    if user_ids.is_empty() {
        return Ok(RemoveUserRoleAssignmentsResult {
            removed: Vec::new(),
        });
    }
    let user_id_strings: Vec<String> = user_ids.iter().map(ToString::to_string).collect();
    // `RETURNING` yields exactly the assignments that existed and were deleted.
    let returned_rows = sqlx::query_scalar!(
        r#"
        DELETE FROM role_assignment
        WHERE role_id = $1 AND user_id = ANY($2::text[])
        RETURNING user_id
        "#,
        *role_id,
        &user_id_strings,
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(super::dbutils::DBErrorHandler::into_catalog_backend_error)?;
    // `RETURNING` yields exactly the assignments that existed and were deleted.
    let returned: HashSet<&str> = returned_rows.iter().map(String::as_str).collect();

    let removed: Vec<UserId> = user_ids
        .iter()
        .zip(&user_id_strings)
        .filter(|(_, s)| returned.contains(s.as_str()))
        .map(|(u, _)| u.clone())
        .collect();
    Ok(RemoveUserRoleAssignmentsResult { removed })
}

// ─── affected_users_for_membership_edge ───────────────────────────────────────
//
// After a `role_membership` edge `(parent, member)` is added or removed, the set
// of users whose EFFECTIVE roles changed is exactly the users directly assigned
// (in `role_assignment`) to `member` OR to any role in the DESCENDANT closure of
// `member` — i.e. roles that reach `member` by climbing member→parent edges.
//
// `parent` is intentionally not consulted: descendants of `member` already
// capture every affected user regardless of which parent the edge attached to.
pub(crate) async fn affected_users_for_membership_edge<
    'c,
    'e: 'c,
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
>(
    member_role_id: RoleId,
    connection: E,
) -> Result<Vec<UserId>, CatalogBackendError> {
    let rows = sqlx::query_scalar!(
        r#"
        WITH RECURSIVE descendants(role_id) AS (
                SELECT $1::uuid
            UNION
                SELECT rm.member_role_id
                FROM role_membership rm
                JOIN descendants d ON rm.parent_role_id = d.role_id
        )
        SELECT DISTINCT ra.user_id
        FROM role_assignment ra
        WHERE ra.role_id IN (SELECT role_id FROM descendants)
        "#,
        *member_role_id,
    )
    .fetch_all(connection)
    .await
    .map_err(super::dbutils::DBErrorHandler::into_catalog_backend_error)?;

    rows.iter()
        .map(|id| user_id_from_db(id))
        .collect::<Result<Vec<_>, _>>()
        .map_err(CatalogBackendError::new_unexpected)
}

// ─── list_role_memberships ────────────────────────────────────────────────────

pub(crate) async fn list_role_memberships<
    'c,
    'e: 'c,
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
>(
    role_id: RoleId,
    direction: RoleMembershipDirection,
    connection: E,
) -> Result<Vec<RoleMembershipEntry>, CatalogBackendError> {
    // Two static queries rather than a single CASE-based one: each keeps its
    // WHERE column sargable so the (parent,member) / (member,parent) indexes are
    // usable. `Members` walks parent→member; `Parents` walks member→parent.
    fn entry(
        id: Uuid,
        source_id: String,
        provider_id: String,
        created_at: chrono::DateTime<chrono::Utc>,
    ) -> RoleMembershipEntry {
        RoleMembershipEntry {
            role_id: RoleId::new(id),
            role_ident: Arc::new(RoleIdent::from_db_unchecked(provider_id, source_id)),
            created_at,
        }
    }

    let entries = match direction {
        RoleMembershipDirection::Members => sqlx::query!(
            r#"
            SELECT r.id, r.source_id, r.provider_id, rm.created_at
            FROM role_membership rm
            JOIN "role" r ON r.id = rm.member_role_id
            WHERE rm.parent_role_id = $1
            ORDER BY rm.member_role_id
            "#,
            *role_id,
        )
        .fetch_all(connection)
        .await
        .map_err(super::dbutils::DBErrorHandler::into_catalog_backend_error)?
        .into_iter()
        .map(|row| entry(row.id, row.source_id, row.provider_id, row.created_at))
        .collect(),
        RoleMembershipDirection::Parents => sqlx::query!(
            r#"
            SELECT r.id, r.source_id, r.provider_id, rm.created_at
            FROM role_membership rm
            JOIN "role" r ON r.id = rm.parent_role_id
            WHERE rm.member_role_id = $1
            ORDER BY rm.parent_role_id
            "#,
            *role_id,
        )
        .fetch_all(connection)
        .await
        .map_err(super::dbutils::DBErrorHandler::into_catalog_backend_error)?
        .into_iter()
        .map(|row| entry(row.id, row.source_id, row.provider_id, row.created_at))
        .collect(),
    };
    Ok(entries)
}

// ─── list_direct_role_members_page ───────────────────────────────────────────────────

/// Direct (depth-1) members of `role_id`, in `project_id`: user members (from
/// `role_assignment`) and member roles (from `role_membership`) merged into one
/// listing under a single opaque cursor. `type_filter` optionally restricts to
/// one kind. Ordered/keyset on `(created_at, member_type, member_id)` — a stable
/// total order across the two heterogeneous sources, so a cursor minted on a
/// `user` row resumes correctly into the `role` rows and vice versa.
pub(crate) async fn list_direct_role_members_page<
    'c,
    'e: 'c,
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
>(
    project_id: &ProjectId,
    role_id: RoleId,
    type_filter: Option<RoleMemberKind>,
    pagination: lakekeeper::api::iceberg::v1::PaginationQuery,
    connection: E,
) -> lakekeeper::service::Result<ListRoleAssignmentsResultPage> {
    let lakekeeper::api::iceberg::v1::PaginationQuery {
        page_token,
        page_size,
    } = pagination;
    let page_size = lakekeeper::CONFIG.page_size_or_pagination_default(page_size);

    let (want_users, want_roles) = match type_filter {
        None => (true, true),
        Some(RoleMemberKind::User) => (true, false),
        Some(RoleMemberKind::Role) => (false, true),
    };

    // The opaque cursor is `(created_at, "<member_type>:<member_id>")`. The id half
    // carries a type discriminator so a token minted on a `user` row keyset-resumes
    // correctly into the `role` rows under the `(created_at, member_type, member_id)`
    // ordering. `splitn(3, '&')` in the token codec keeps the ':' intact.
    let token = page_token
        .as_option()
        .map(PaginateToken::<String>::try_from)
        .transpose()?;
    let (token_ts, token_type, token_id): (Option<&chrono::DateTime<chrono::Utc>>, _, _) =
        match token.as_ref() {
            Some(PaginateToken::V1(V1PaginateToken { created_at, id })) => {
                let (member_type, member_id) = id.split_once(':').ok_or_else(|| {
                    InvalidPaginationToken::new("Invalid role-members page token payload", id)
                })?;
                (Some(created_at), Some(member_type), Some(member_id))
            }
            None => (None, None, None),
        };

    let rows = sqlx::query!(
        r#"
        SELECT
            m.created_at  AS "created_at!",
            m.member_type AS "member_type!",
            m.member_id   AS "member_id!"
        FROM (
            SELECT ra.created_at, 'user'::text AS member_type, ra.user_id AS member_id
            FROM role_assignment ra
            JOIN "role" r ON r.id = ra.role_id
            WHERE ra.role_id = $1 AND r.project_id = $2 AND $3
          UNION ALL
            SELECT rm.created_at, 'role'::text AS member_type, rm.member_role_id::text AS member_id
            FROM role_membership rm
            JOIN "role" r ON r.id = rm.member_role_id
            WHERE rm.parent_role_id = $1 AND r.project_id = $2 AND $4
        ) m
        WHERE ($5::timestamptz IS NULL)
           OR (m.created_at, m.member_type, m.member_id) > ($5, $6, $7)
        ORDER BY m.created_at, m.member_type, m.member_id
        LIMIT $8
        "#,
        *role_id,
        project_id.as_str(),
        want_users,
        want_roles,
        token_ts,
        token_type,
        token_id,
        page_size,
    )
    .fetch_all(connection)
    .await
    .map_err(|e| e.into_error_model("Error listing the members of a role".to_string()))?;

    let mut assignments: Vec<RoleAssignmentRow> = Vec::with_capacity(rows.len());
    for row in &rows {
        let subject = match row.member_type.as_str() {
            "user" => UserOrRoleId::User(user_id_from_db(&row.member_id).map_err(|e| {
                ErrorModel::internal(
                    "Stored role member has an unparseable user id",
                    "RoleMemberUserIdInvalid",
                    Some(Box::new(e)),
                )
            })?),
            "role" => {
                let id = Uuid::parse_str(&row.member_id).map_err(|e| {
                    ErrorModel::internal(
                        "Stored role member has an unparseable role id",
                        "RoleMemberRoleIdInvalid",
                        Some(Box::new(e)),
                    )
                })?;
                UserOrRoleId::Role(RoleId::new(id))
            }
            other => {
                return Err(ErrorModel::internal(
                    format!("Unexpected role member type '{other}'"),
                    "RoleMemberTypeInvalid",
                    None,
                )
                .into());
            }
        };
        assignments.push(RoleAssignmentRow {
            subject,
            role_id,
            created_at: Some(row.created_at),
        });
    }

    let next_page_token = rows.last().map(|row| {
        PaginateToken::V1(V1PaginateToken {
            created_at: row.created_at,
            id: format!("{}:{}", row.member_type, row.member_id),
        })
        .to_string()
    });

    Ok(ListRoleAssignmentsResultPage {
        assignments,
        next_page_token,
    })
}

// ─── list_direct_user_roles_page ─────────────────────────────────────────────────────

/// Direct (depth-1) roles a user is assigned to, in `project_id`, keyset-paginated
/// by `(role_assignment.created_at, role_id)`. Project-scoped via a JOIN on `role`
/// so a `role_assignment` row whose role lives in another project is excluded.
/// A malformed `page_token` surfaces as a 400 (`InvalidPaginationToken`).
pub(crate) async fn list_direct_user_roles_page<
    'c,
    'e: 'c,
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
>(
    project_id: &ProjectId,
    user_id: &UserId,
    pagination: lakekeeper::api::iceberg::v1::PaginationQuery,
    connection: E,
) -> lakekeeper::service::Result<ListRolesPage> {
    let lakekeeper::api::iceberg::v1::PaginationQuery {
        page_token,
        page_size,
    } = pagination;
    let page_size = lakekeeper::CONFIG.page_size_or_pagination_default(page_size);

    let token = page_token
        .as_option()
        .map(PaginateToken::<Uuid>::try_from)
        .transpose()?;
    let (token_ts, token_id): (_, Option<&Uuid>) = token
        .as_ref()
        .map(|PaginateToken::V1(V1PaginateToken { created_at, id })| (created_at, id))
        .unzip();

    let entries: Vec<RoleMembershipEntry> = sqlx::query!(
        r#"
        SELECT r.id, r.source_id, r.provider_id, ra.created_at
        FROM role_assignment ra
        JOIN "role" r ON r.id = ra.role_id
        WHERE ra.user_id = $1
          AND r.project_id = $2
          AND ((ra.created_at > $3 OR $3 IS NULL) OR (ra.created_at = $3 AND r.id > $4))
        ORDER BY ra.created_at, r.id ASC
        LIMIT $5
        "#,
        user_id.to_string(),
        project_id.as_str(),
        token_ts,
        token_id,
        page_size,
    )
    .fetch_all(connection)
    .await
    .map_err(|e| e.into_error_model("Error listing the roles a user is assigned to".to_string()))?
    .into_iter()
    .map(|row| RoleMembershipEntry {
        role_id: RoleId::new(row.id),
        role_ident: Arc::new(RoleIdent::from_db_unchecked(row.provider_id, row.source_id)),
        created_at: row.created_at,
    })
    .collect();

    let next_page_token = entries.last().map(|e| {
        PaginateToken::V1(V1PaginateToken {
            created_at: e.created_at,
            id: *e.role_id,
        })
        .to_string()
    });

    Ok(ListRolesPage {
        entries,
        next_page_token,
    })
}

// ─── list_direct_role_parents_page ───────────────────────────────────────────────────

/// Direct (depth-1) parent roles of `role_id`, in `project_id`, keyset-paginated
/// by `(role_membership.created_at, parent_role_id)`.
pub(crate) async fn list_direct_role_parents_page<
    'c,
    'e: 'c,
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
>(
    project_id: &ProjectId,
    role_id: RoleId,
    pagination: lakekeeper::api::iceberg::v1::PaginationQuery,
    connection: E,
) -> lakekeeper::service::Result<ListRolesPage> {
    let lakekeeper::api::iceberg::v1::PaginationQuery {
        page_token,
        page_size,
    } = pagination;
    let page_size = lakekeeper::CONFIG.page_size_or_pagination_default(page_size);

    let token = page_token
        .as_option()
        .map(PaginateToken::<Uuid>::try_from)
        .transpose()?;
    let (token_ts, token_id): (_, Option<&Uuid>) = token
        .as_ref()
        .map(|PaginateToken::V1(V1PaginateToken { created_at, id })| (created_at, id))
        .unzip();

    let entries: Vec<RoleMembershipEntry> = sqlx::query!(
        r#"
        SELECT r.id, r.source_id, r.provider_id, rm.created_at
        FROM role_membership rm
        JOIN "role" r ON r.id = rm.parent_role_id
        WHERE rm.member_role_id = $1
          AND r.project_id = $2
          AND ((rm.created_at > $3 OR $3 IS NULL) OR (rm.created_at = $3 AND r.id > $4))
        ORDER BY rm.created_at, r.id ASC
        LIMIT $5
        "#,
        *role_id,
        project_id.as_str(),
        token_ts,
        token_id,
        page_size,
    )
    .fetch_all(connection)
    .await
    .map_err(|e| e.into_error_model("Error listing the parents of a role".to_string()))?
    .into_iter()
    .map(|row| RoleMembershipEntry {
        role_id: RoleId::new(row.id),
        role_ident: Arc::new(RoleIdent::from_db_unchecked(row.provider_id, row.source_id)),
        created_at: row.created_at,
    })
    .collect();

    let next_page_token = entries.last().map(|e| {
        PaginateToken::V1(V1PaginateToken {
            created_at: e.created_at,
            id: *e.role_id,
        })
        .to_string()
    });

    Ok(ListRolesPage {
        entries,
        next_page_token,
    })
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use lakekeeper::{
        ProjectId,
        api::{
            iceberg::{types::PageToken, v1::PaginationQuery},
            management::v1::user::{UserLastUpdatedWith, UserType},
        },
        service::{
            AddRoleMembersError, ArcProjectId, CatalogCreateRoleRequest, CatalogRoleAssignmentOps,
            CatalogRoleForAssignment, CatalogRoleOps, CatalogStore, CatalogUserRoleAssignmentUser,
            RoleId, RoleIdent, RoleProviderId, RoleSourceId, SyncRoleMembersError,
            SyncUserRoleAssignmentsError, Transaction, UniqueMembers, UniqueRoles,
            authn::{UserId, UserIdRef},
        },
    };

    use super::*;
    use crate::{CatalogState, PostgresBackend, PostgresTransaction};

    fn um<'s, 'd>(s: &'s [CatalogUserRoleAssignmentUser<'d>]) -> UniqueMembers<'s, 'd> {
        UniqueMembers::from_unchecked(s)
    }

    fn ur<'s, 'd>(s: &'s [CatalogRoleForAssignment<'d>]) -> UniqueRoles<'s, 'd> {
        UniqueRoles::from_unchecked(s)
    }

    // ── helpers ────────────────────────────────────────────────────────────

    fn make_user<'a>(user_id: &'a UserIdRef, name: &'a str) -> CatalogUserRoleAssignmentUser<'a> {
        CatalogUserRoleAssignmentUser {
            user_id,
            name: Some(name),
            email: None,
            user_type: Some(UserType::Human),
            updated_with: UserLastUpdatedWith::RoleProvider,
        }
    }

    fn make_role<'a>(ident: &'a Arc<RoleIdent>, name: &'a str) -> CatalogRoleForAssignment<'a> {
        CatalogRoleForAssignment {
            ident,
            name: Some(name),
            description: None,
        }
    }

    async fn make_project(state: &CatalogState) -> ProjectId {
        let project_id = ProjectId::new_random();
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        PostgresBackend::create_project(
            &project_id,
            format!("Project {project_id}"),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();
        project_id
    }

    /// Create a catalog-managed (`lakekeeper` provider) role in `project_id`
    /// and return its freshly minted [`RoleId`].
    async fn create_managed_role(
        state: &CatalogState,
        project_id: &ProjectId,
        name: &str,
    ) -> RoleId {
        let provider_id = RoleProviderId::lakekeeper();
        let source_id = RoleSourceId::try_new(name).unwrap();
        let role_id = RoleId::new_random();
        let request = CatalogCreateRoleRequest::builder()
            .role_id(role_id)
            .role_name(name)
            .source_id(&source_id)
            .provider_id(&provider_id)
            .build();
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let role = PostgresBackend::create_role(project_id, request, t.transaction())
            .await
            .unwrap();
        t.commit().await.unwrap();
        role.id()
    }

    /// Create a role with an EXTERNAL (non-catalog-managed) provider, e.g. an
    /// LDAP-synced group. Such roles are NOT manually assignable via the API and
    /// must be rejected from `role_membership` edges.
    async fn create_external_role(
        state: &CatalogState,
        project_id: &ProjectId,
        provider: &str,
        source: &str,
    ) -> RoleId {
        let provider_id = RoleProviderId::new_unchecked(provider);
        let source_id = RoleSourceId::try_new(source).unwrap();
        let role_id = RoleId::new_random();
        let request = CatalogCreateRoleRequest::builder()
            .role_id(role_id)
            .role_name(source)
            .source_id(&source_id)
            .provider_id(&provider_id)
            .build();
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let role = PostgresBackend::create_role(project_id, request, t.transaction())
            .await
            .unwrap();
        t.commit().await.unwrap();
        role.id()
    }

    /// Provision a user in the `users` table (no role assignment), so it can be
    /// referenced by the management-API assignment writes (which require the user
    /// to pre-exist).
    async fn provision_user(state: &CatalogState, user_id: &UserId, name: &str) {
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        PostgresBackend::create_or_update_user(
            user_id,
            name,
            None,
            UserLastUpdatedWith::RoleProvider,
            UserType::Human,
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();
    }

    // ── user→role assignment writes (management API) ───────────────────────

    /// Adding then removing a user assignment is idempotent: the first add reports
    /// the user, a repeat add reports nothing; the first remove reports the user,
    /// a repeat remove reports nothing. The assignment is visible in the merged
    /// members listing in between.
    #[sqlx::test]
    async fn user_role_assignments_add_remove_idempotent(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let role = create_managed_role(&state, &project_id, "R").await;
        let user_id = UserId::new_unchecked("oidc", "alice");
        provision_user(&state, &user_id, "Alice").await;
        let arc_project: ArcProjectId = Arc::new(project_id.clone());

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let added = add_user_role_assignments(
            &arc_project,
            role,
            std::slice::from_ref(&user_id),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();
        assert_eq!(added.added, vec![user_id.clone()]);

        // Visible as a member.
        let page = list_direct_role_members_page(
            &project_id,
            role,
            Some(RoleMemberKind::User),
            PaginationQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(50),
            },
            &pool,
        )
        .await
        .unwrap();
        assert_eq!(page.assignments.len(), 1);
        assert_eq!(
            page.assignments[0].subject,
            UserOrRoleId::User(user_id.clone())
        );

        // Repeat add is idempotent.
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let again = add_user_role_assignments(
            &arc_project,
            role,
            std::slice::from_ref(&user_id),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();
        assert_eq!(again.added, Vec::<UserId>::new());

        // Remove, then repeat-remove is idempotent.
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let removed =
            remove_user_role_assignments(role, std::slice::from_ref(&user_id), t.transaction())
                .await
                .unwrap();
        t.commit().await.unwrap();
        assert_eq!(removed.removed, vec![user_id.clone()]);

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let removed_again =
            remove_user_role_assignments(role, std::slice::from_ref(&user_id), t.transaction())
                .await
                .unwrap();
        t.commit().await.unwrap();
        assert_eq!(removed_again.removed, Vec::<UserId>::new());
    }

    /// Provision-then-assign: assigning an unknown user is rejected with
    /// `RoleAssignmentUserNotFound` (→404) as a typed error.
    #[sqlx::test]
    async fn user_role_assignments_add_unknown_user_rejected(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let role = create_managed_role(&state, &project_id, "R").await;
        let arc_project: ArcProjectId = Arc::new(project_id.clone());
        let ghost = UserId::new_unchecked("oidc", "ghost");

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let err = add_user_role_assignments(
            &arc_project,
            role,
            std::slice::from_ref(&ghost),
            t.transaction(),
        )
        .await
        .unwrap_err();
        let AddUserRoleAssignmentsError::RoleAssignmentUserNotFound(e) = err else {
            panic!("expected RoleAssignmentUserNotFound, got {err:?}");
        };
        assert_eq!(e.user_id, ghost);
    }

    /// All-or-nothing: a batch mixing a valid and an unknown user is rejected,
    /// and the valid user is NOT partially assigned (the single-statement insert
    /// is gated on the whole batch being valid, so nothing is written on the
    /// error path).
    #[sqlx::test]
    async fn user_role_assignments_add_partial_invalid_writes_nothing(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let role = create_managed_role(&state, &project_id, "R").await;
        let alice = UserId::new_unchecked("oidc", "alice");
        provision_user(&state, &alice, "Alice").await;
        let ghost = UserId::new_unchecked("oidc", "ghost");
        let arc_project: ArcProjectId = Arc::new(project_id.clone());

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let err = add_user_role_assignments(
            &arc_project,
            role,
            &[alice.clone(), ghost.clone()],
            t.transaction(),
        )
        .await
        .unwrap_err();
        t.commit().await.unwrap();
        assert!(
            matches!(
                err,
                AddUserRoleAssignmentsError::RoleAssignmentUserNotFound(_)
            ),
            "got {err:?}"
        );

        // Alice must NOT have been assigned despite being valid — the whole batch
        // was rejected.
        let page = list_direct_role_members_page(
            &project_id,
            role,
            Some(RoleMemberKind::User),
            PaginationQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(50),
            },
            &pool,
        )
        .await
        .unwrap();
        assert_eq!(page.assignments.len(), 0, "no partial write");
    }

    /// Assigning to a role that does not exist in the project is rejected with
    /// `RoleIdNotFoundInProject` (→404).
    #[sqlx::test]
    async fn user_role_assignments_add_unknown_role_rejected(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let user_id = UserId::new_unchecked("oidc", "alice");
        provision_user(&state, &user_id, "Alice").await;
        let arc_project: ArcProjectId = Arc::new(project_id.clone());
        let bogus_role = RoleId::new_random();

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let err = add_user_role_assignments(&arc_project, bogus_role, &[user_id], t.transaction())
            .await
            .unwrap_err();
        let AddUserRoleAssignmentsError::RoleIdNotFoundInProject(e) = err else {
            panic!("expected RoleIdNotFoundInProject, got {err:?}");
        };
        assert_eq!(e.role_id, bogus_role);
    }

    /// A user cannot be manually assigned to an externally-provided (e.g. LDAP)
    /// role — its membership is provider-driven. Rejected with
    /// `RoleNotManuallyAssignable`.
    #[sqlx::test]
    async fn user_role_assignments_add_external_role_rejected(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let ext_role = create_external_role(&state, &project_id, "ldap", "group-x").await;
        let user_id = UserId::new_unchecked("oidc", "alice");
        provision_user(&state, &user_id, "Alice").await;
        let arc_project: ArcProjectId = Arc::new(project_id.clone());

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let err = add_user_role_assignments(&arc_project, ext_role, &[user_id], t.transaction())
            .await
            .unwrap_err();
        let AddUserRoleAssignmentsError::RoleNotManuallyAssignable(e) = err else {
            panic!("expected RoleNotManuallyAssignable, got {err:?}");
        };
        assert_eq!(e.role_id, ext_role);
    }

    /// Empty input still validates the role (consistent with `add_role_members`):
    /// adding to an unknown role with no members is a 404, not a silent success.
    #[sqlx::test]
    async fn user_role_assignments_add_empty_still_validates_role(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let arc_project: ArcProjectId = Arc::new(project_id.clone());

        // Empty input + unknown role → still 404, not Ok.
        let bogus_role = RoleId::new_random();
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let err = add_user_role_assignments(&arc_project, bogus_role, &[], t.transaction())
            .await
            .unwrap_err();
        assert!(
            matches!(err, AddUserRoleAssignmentsError::RoleIdNotFoundInProject(_)),
            "expected RoleIdNotFoundInProject, got {err:?}"
        );

        // Empty input + valid role → no members added, no error.
        let role = create_managed_role(&state, &project_id, "R").await;
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let res = add_user_role_assignments(&arc_project, role, &[], t.transaction())
            .await
            .unwrap();
        t.commit().await.unwrap();
        assert!(res.added.is_empty());
    }

    /// Duplicate user ids in one request insert one row and are reported once.
    /// Dedup lives in the backend-independent `*Ops` layer, so this goes through
    /// the `*Ops` method (not the storage free fn, which trusts a unique set).
    #[sqlx::test]
    async fn user_role_assignments_add_deduplicates(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let role = create_managed_role(&state, &project_id, "R").await;
        let alice = UserId::new_unchecked("oidc", "alice");
        provision_user(&state, &alice, "Alice").await;
        let arc_project: ArcProjectId = Arc::new(project_id.clone());

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let res = PostgresBackend::add_user_role_assignments(
            &arc_project,
            role,
            &[alice.clone(), alice.clone()],
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();
        assert_eq!(res.added, vec![alice], "duplicate input reported once");
    }

    /// Duplicate user ids in one remove request delete the row once and are
    /// reported once. Dedup lives in the `*Ops` layer (like the add path), so this
    /// goes through the `*Ops` method, not the storage free fn.
    #[sqlx::test]
    async fn user_role_assignments_remove_deduplicates(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let role = create_managed_role(&state, &project_id, "R").await;
        let alice = UserId::new_unchecked("oidc", "alice");
        provision_user(&state, &alice, "Alice").await;
        let arc_project: ArcProjectId = Arc::new(project_id.clone());

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        PostgresBackend::add_user_role_assignments(
            &arc_project,
            role,
            std::slice::from_ref(&alice),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let res = PostgresBackend::remove_user_role_assignments(
            role,
            &[alice.clone(), alice.clone()],
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();
        assert_eq!(res.removed, vec![alice], "duplicate input reported once");
    }

    /// Deleting a user removes their role assignments (matching the OpenFGA
    /// authorizer): `delete_user` returns the affected roles, and the user no
    /// longer appears as a member.
    #[sqlx::test]
    async fn delete_user_removes_role_assignments(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let role = create_managed_role(&state, &project_id, "R").await;
        let user_id = UserId::new_unchecked("oidc", "alice");
        provision_user(&state, &user_id, "Alice").await;
        let arc_project: ArcProjectId = Arc::new(project_id.clone());

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        add_user_role_assignments(
            &arc_project,
            role,
            std::slice::from_ref(&user_id),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        // Sanity: the user is a member before deletion.
        let members_query = |kind| {
            list_direct_role_members_page(
                &project_id,
                role,
                kind,
                PaginationQuery {
                    page_token: PageToken::NotSpecified,
                    page_size: Some(50),
                },
                &pool,
            )
        };
        assert_eq!(
            members_query(Some(RoleMemberKind::User))
                .await
                .unwrap()
                .assignments
                .len(),
            1
        );

        // Delete the user — the affected role is reported.
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let affected = PostgresBackend::delete_user(user_id.clone(), t.transaction())
            .await
            .unwrap();
        t.commit().await.unwrap();
        assert_eq!(affected, Some(vec![role]));

        // The assignment is gone — the user is no longer a member.
        assert_eq!(
            members_query(Some(RoleMemberKind::User))
                .await
                .unwrap()
                .assignments
                .len(),
            0,
            "deleted user is no longer a role member"
        );
    }

    /// `remove_user_role_assignments_and_invalidate` evicts the user's cached
    /// effective-roles entry, so a subsequent read on THIS replica reflects the
    /// change immediately. (The cache is per-process moka; other replicas converge
    /// only within the cache TTL — which is exactly why management read endpoints
    /// bypass the cache and query the DB directly.)
    #[sqlx::test]
    async fn user_role_assignments_and_invalidate_evicts_cache(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let role = create_managed_role(&state, &project_id, "R").await;
        let user_id = UserId::new_unchecked("oidc", "cache-user");
        provision_user(&state, &user_id, "Cache User").await;
        let arc_project: ArcProjectId = Arc::new(project_id.clone());

        PostgresBackend::add_user_role_assignments_and_invalidate(
            &arc_project,
            role,
            std::slice::from_ref(&user_id),
            state.clone(),
        )
        .await
        .unwrap();

        // Warm the per-user effective-roles cache: U → {R}.
        let warmed = PostgresBackend::list_role_assignments_for_user(&user_id, state.clone())
            .await
            .unwrap();
        assert_eq!(
            warmed
                .roles
                .iter()
                .map(|r| r.role_id)
                .collect::<HashSet<_>>(),
            HashSet::from([role])
        );

        // Remove + invalidate: the cache entry must be evicted, and U has no roles.
        PostgresBackend::remove_user_role_assignments_and_invalidate(
            role,
            std::slice::from_ref(&user_id),
            state.clone(),
        )
        .await
        .unwrap();
        let after = PostgresBackend::list_role_assignments_for_user(&user_id, state.clone())
            .await
            .unwrap();
        assert!(
            !Arc::ptr_eq(&warmed, &after),
            "cache must be invalidated on assignment removal"
        );
        assert!(after.roles.is_empty(), "U has no roles after removal");
    }

    /// External provider sync must refuse the reserved `system`/`lakekeeper`
    /// providers (they are catalog-managed). Enforced in the backend-independent
    /// `*Ops` layer, so it holds for every sync entry point.
    #[sqlx::test]
    async fn sync_rejects_reserved_providers(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let user_id = Arc::new(UserId::new_unchecked("oidc", "alice"));

        for reserved in ["system", "lakekeeper"] {
            let ident = Arc::new(RoleIdent::new_unchecked(reserved, "group"));

            // Role-members sync: the role's ident is a reserved provider.
            let mut t = PostgresTransaction::begin_write(state.clone())
                .await
                .unwrap();
            let err = PostgresBackend::sync_role_members_by_ident(
                &project_id,
                &make_role(&ident, "Group"),
                &[make_user(&user_id, "Alice")],
                t.transaction(),
            )
            .await
            .unwrap_err();
            assert!(
                matches!(err, SyncRoleMembersError::ReservedRoleProvider(_)),
                "members sync for '{reserved}': {err:?}"
            );

            // User-assignment sync: the provider scope is reserved.
            let provider = RoleProviderId::new_unchecked(reserved);
            let mut t = PostgresTransaction::begin_write(state.clone())
                .await
                .unwrap();
            let err = PostgresBackend::sync_user_role_assignments_by_provider(
                make_user(&user_id, "Alice"),
                &project_id,
                &provider,
                &[make_role(&ident, "Group")],
                t.transaction(),
            )
            .await
            .unwrap_err();
            assert!(
                matches!(err, SyncUserRoleAssignmentsError::ReservedRoleProvider(_)),
                "user sync for '{reserved}': {err:?}"
            );
        }
    }

    // ── paginated cold reads (management API) ──────────────────────────────

    /// `list_direct_user_roles_page` keyset-paginates a user's direct role assignments:
    /// each assigned role appears exactly once across pages, page sizes are exact,
    /// and the final page carries no continuation token.
    #[sqlx::test]
    async fn list_direct_user_roles_page_paginates(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let provider = RoleProviderId::new_unchecked("ldap");
        let user_id = Arc::new(UserId::new_unchecked("oidc", "alice"));
        let user = make_user(&user_id, "Alice");

        // Assign the user to three roles in the project.
        let idents = [
            Arc::new(RoleIdent::new_unchecked("ldap", "group-1")),
            Arc::new(RoleIdent::new_unchecked("ldap", "group-2")),
            Arc::new(RoleIdent::new_unchecked("ldap", "group-3")),
        ];
        let roles: Vec<_> = idents
            .iter()
            .enumerate()
            .map(|(n, i)| make_role(i, ["Group 1", "Group 2", "Group 3"][n]))
            .collect();
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        sync_user_role_assignments_by_provider(
            &user,
            &project_id,
            &provider,
            ur(&roles),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        // Ground truth: the full set of assigned role ids (the cached reader is
        // transitive, but here every assignment is direct so the sets coincide).
        let ground_truth: std::collections::HashSet<Uuid> =
            list_role_assignments_for_user(&user_id, &pool)
                .await
                .unwrap()
                .roles
                .into_iter()
                .map(|r| *r.role_id)
                .collect();
        assert_eq!(ground_truth.len(), 3);

        // Page 1: two entries + a continuation token.
        let page1 = list_direct_user_roles_page(
            &project_id,
            &user_id,
            PaginationQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(2),
            },
            &pool,
        )
        .await
        .unwrap();
        assert_eq!(page1.entries.len(), 2);
        let token = page1.next_page_token.clone().expect("page 1 has a token");

        // Page 2: the remaining entry. Per the house keyset convention
        // (`list_users`), a non-empty page always carries a token — the client
        // pages until it receives an EMPTY page, not until the token is absent
        // on a partial page.
        let page2 = list_direct_user_roles_page(
            &project_id,
            &user_id,
            PaginationQuery {
                page_token: PageToken::Present(token),
                page_size: Some(2),
            },
            &pool,
        )
        .await
        .unwrap();
        assert_eq!(page2.entries.len(), 1);
        let token = page2
            .next_page_token
            .clone()
            .expect("page 2 still has a token");

        // Page 3: empty, and now the token is absent — pagination is drained.
        let page3 = list_direct_user_roles_page(
            &project_id,
            &user_id,
            PaginationQuery {
                page_token: PageToken::Present(token),
                page_size: Some(2),
            },
            &pool,
        )
        .await
        .unwrap();
        assert_eq!(page3.entries.len(), 0);
        assert_eq!(page3.next_page_token, None);

        // Union across the non-empty pages == ground truth, with no duplicates.
        let union: std::collections::HashSet<Uuid> = page1
            .entries
            .iter()
            .chain(&page2.entries)
            .map(|e| *e.role_id)
            .collect();
        assert_eq!(union.len(), 3, "no role appears on two pages");
        assert_eq!(union, ground_truth);
    }

    /// `list_direct_role_members_page` merges user members (`role_assignment`) and role
    /// members (`role_membership`) into one keyset-paginated listing: every member
    /// of either kind appears exactly once across pages — including across the
    /// user/role type boundary mid-pagination — and the union equals the inserted
    /// set.
    #[sqlx::test]
    async fn list_direct_role_members_page_merges_users_and_roles(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let parent = create_managed_role(&state, &project_id, "R").await;
        let ma = create_managed_role(&state, &project_id, "ma").await;
        let mb = create_managed_role(&state, &project_id, "mb").await;
        let arc_project: ArcProjectId = Arc::new(project_id.clone());

        // Two role members of R.
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        add_role_members(&arc_project, parent, &[ma, mb], usize::MAX, t.transaction())
            .await
            .unwrap();
        t.commit().await.unwrap();

        // Two user members of R (matched to the existing managed role by ident).
        let r_ident = Arc::new(RoleIdent::new_unchecked("lakekeeper", "R"));
        let u1 = Arc::new(UserId::new_unchecked("oidc", "u1"));
        let u2 = Arc::new(UserId::new_unchecked("oidc", "u2"));
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        sync_role_members_by_ident(
            &project_id,
            &make_role(&r_ident, "R"),
            um(&[make_user(&u1, "U1"), make_user(&u2, "U2")]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        let expected: std::collections::HashSet<UserOrRoleId> = [
            UserOrRoleId::User((*u1).clone()),
            UserOrRoleId::User((*u2).clone()),
            UserOrRoleId::Role(ma),
            UserOrRoleId::Role(mb),
        ]
        .into_iter()
        .collect();

        // Drain all pages at page_size 2 (4 members → 2 full pages + 1 empty).
        let mut got: Vec<UserOrRoleId> = Vec::new();
        let mut token = PageToken::NotSpecified;
        let mut pages = 0;
        loop {
            let page = list_direct_role_members_page(
                &project_id,
                parent,
                None,
                PaginationQuery {
                    page_token: token,
                    page_size: Some(2),
                },
                &pool,
            )
            .await
            .unwrap();
            pages += 1;
            if page.assignments.is_empty() {
                assert_eq!(page.next_page_token, None, "drained page has no token");
                break;
            }
            for row in &page.assignments {
                assert_eq!(
                    row.role_id, parent,
                    "every row is scoped to the parent role"
                );
                assert!(row.created_at.is_some(), "catalog path sets created_at");
                got.push(row.subject.clone());
            }
            token = PageToken::Present(page.next_page_token.expect("non-empty page has a token"));
        }

        assert_eq!(pages, 3, "two full pages of 2 then one empty page");
        assert_eq!(got.len(), 4, "no member appears on two pages");
        let union: std::collections::HashSet<UserOrRoleId> = got.into_iter().collect();
        assert_eq!(union, expected);
    }

    /// `?type=` restricts the merged listing to one member kind: `User` returns
    /// only user members, `Role` only member roles.
    #[sqlx::test]
    async fn list_direct_role_members_page_type_filter(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let parent = create_managed_role(&state, &project_id, "R").await;
        let ma = create_managed_role(&state, &project_id, "ma").await;
        let arc_project: ArcProjectId = Arc::new(project_id.clone());

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        add_role_members(&arc_project, parent, &[ma], usize::MAX, t.transaction())
            .await
            .unwrap();
        t.commit().await.unwrap();

        let r_ident = Arc::new(RoleIdent::new_unchecked("lakekeeper", "R"));
        let u1 = Arc::new(UserId::new_unchecked("oidc", "u1"));
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        sync_role_members_by_ident(
            &project_id,
            &make_role(&r_ident, "R"),
            um(&[make_user(&u1, "U1")]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        let users_only = list_direct_role_members_page(
            &project_id,
            parent,
            Some(RoleMemberKind::User),
            PaginationQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(50),
            },
            &pool,
        )
        .await
        .unwrap();
        assert_eq!(users_only.assignments.len(), 1);
        assert_eq!(
            users_only.assignments[0].subject,
            UserOrRoleId::User((*u1).clone())
        );

        let roles_only = list_direct_role_members_page(
            &project_id,
            parent,
            Some(RoleMemberKind::Role),
            PaginationQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(50),
            },
            &pool,
        )
        .await
        .unwrap();
        assert_eq!(roles_only.assignments.len(), 1);
        assert_eq!(roles_only.assignments[0].subject, UserOrRoleId::Role(ma));
    }

    /// Project scoping: listing members of a role while passing a different
    /// project's id returns nothing — the role does not belong to that project.
    #[sqlx::test]
    async fn list_direct_role_members_page_scoped_to_project(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_a = make_project(&state).await;
        let project_b = make_project(&state).await;
        let parent = create_managed_role(&state, &project_a, "R").await;
        let ma = create_managed_role(&state, &project_a, "ma").await;
        let arc_a: ArcProjectId = Arc::new(project_a.clone());

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        add_role_members(&arc_a, parent, &[ma], usize::MAX, t.transaction())
            .await
            .unwrap();
        t.commit().await.unwrap();

        // Correct project: the member is visible.
        let in_a = list_direct_role_members_page(
            &project_a,
            parent,
            None,
            PaginationQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(50),
            },
            &pool,
        )
        .await
        .unwrap();
        assert_eq!(in_a.assignments.len(), 1);

        // Wrong project: the role is not in B, so the listing is empty.
        let in_b = list_direct_role_members_page(
            &project_b,
            parent,
            None,
            PaginationQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(50),
            },
            &pool,
        )
        .await
        .unwrap();
        assert_eq!(in_b.assignments.len(), 0);
        assert_eq!(in_b.next_page_token, None);
    }

    /// `list_direct_user_roles_page` is project-scoped: a user assigned to roles in two
    /// projects sees only the roles of the requested project.
    #[sqlx::test]
    async fn list_direct_user_roles_page_scoped_to_project(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_a = make_project(&state).await;
        let project_b = make_project(&state).await;
        let provider = RoleProviderId::new_unchecked("ldap");
        let user_id = Arc::new(UserId::new_unchecked("oidc", "alice"));
        let user = make_user(&user_id, "Alice");
        let a_ident = Arc::new(RoleIdent::new_unchecked("ldap", "group-a"));
        let b_ident = Arc::new(RoleIdent::new_unchecked("ldap", "group-b"));

        for (proj, ident, name) in [
            (&project_a, &a_ident, "Group A"),
            (&project_b, &b_ident, "Group B"),
        ] {
            let mut t = PostgresTransaction::begin_write(state.clone())
                .await
                .unwrap();
            sync_user_role_assignments_by_provider(
                &user,
                proj,
                &provider,
                ur(&[make_role(ident, name)]),
                t.transaction(),
            )
            .await
            .unwrap();
            t.commit().await.unwrap();
        }

        let in_a = list_direct_user_roles_page(
            &project_a,
            &user_id,
            PaginationQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(50),
            },
            &pool,
        )
        .await
        .unwrap();
        assert_eq!(in_a.entries.len(), 1, "only the project-A role is returned");
    }

    /// `list_direct_role_parents_page` keyset-paginates the direct parents of a role:
    /// each parent appears once across pages, the final non-empty page still
    /// carries a token, and the drained page is empty with no token.
    #[sqlx::test]
    async fn list_direct_role_parents_page_paginates(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let child = create_managed_role(&state, &project_id, "child").await;
        let p1 = create_managed_role(&state, &project_id, "p1").await;
        let p2 = create_managed_role(&state, &project_id, "p2").await;
        let p3 = create_managed_role(&state, &project_id, "p3").await;
        let arc_project: ArcProjectId = Arc::new(project_id.clone());

        // child becomes a member of all three parents → child has 3 parents.
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        for p in [p1, p2, p3] {
            add_role_members(&arc_project, p, &[child], usize::MAX, t.transaction())
                .await
                .unwrap();
        }
        t.commit().await.unwrap();

        let expected: std::collections::HashSet<Uuid> = [p1, p2, p3].iter().map(|r| **r).collect();

        let page1 = list_direct_role_parents_page(
            &project_id,
            child,
            PaginationQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(2),
            },
            &pool,
        )
        .await
        .unwrap();
        assert_eq!(page1.entries.len(), 2);
        let token = page1.next_page_token.clone().expect("page 1 has a token");

        let page2 = list_direct_role_parents_page(
            &project_id,
            child,
            PaginationQuery {
                page_token: PageToken::Present(token),
                page_size: Some(2),
            },
            &pool,
        )
        .await
        .unwrap();
        assert_eq!(page2.entries.len(), 1);
        let token = page2
            .next_page_token
            .clone()
            .expect("page 2 still has a token");

        let page3 = list_direct_role_parents_page(
            &project_id,
            child,
            PaginationQuery {
                page_token: PageToken::Present(token),
                page_size: Some(2),
            },
            &pool,
        )
        .await
        .unwrap();
        assert_eq!(page3.entries.len(), 0);
        assert_eq!(page3.next_page_token, None);

        let union: std::collections::HashSet<Uuid> = page1
            .entries
            .iter()
            .chain(&page2.entries)
            .map(|e| *e.role_id)
            .collect();
        assert_eq!(union.len(), 3, "no parent appears on two pages");
        assert_eq!(union, expected);
    }

    // ── role_membership graph ──────────────────────────────────────────────

    #[sqlx::test]
    async fn role_membership_add_is_idempotent(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let parent = create_managed_role(&state, &project_id, "parent").await;
        let child = create_managed_role(&state, &project_id, "child").await;
        let project_id: ArcProjectId = Arc::new(project_id);

        // First add — the child is the actually-added member.
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let first =
            PostgresBackend::add_role_members(&project_id, parent, &[child], t.transaction())
                .await
                .unwrap();
        t.commit().await.unwrap();
        assert_eq!(first.added, vec![child]);
        // The result carries the parent's post-op direct members, read back in-txn.
        assert_eq!(
            first.members.iter().map(|m| m.role_id).collect::<Vec<_>>(),
            vec![child]
        );

        // Second add — a no-op: nothing was newly added, so the delta is empty,
        // but `members` still reflects the (unchanged) current state.
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let second =
            PostgresBackend::add_role_members(&project_id, parent, &[child], t.transaction())
                .await
                .unwrap();
        t.commit().await.unwrap();
        assert_eq!(second.added, Vec::<RoleId>::new());
        assert_eq!(
            second.members.iter().map(|m| m.role_id).collect::<Vec<_>>(),
            vec![child]
        );

        let members = PostgresBackend::list_role_memberships(
            parent,
            RoleMembershipDirection::Members,
            state.clone(),
        )
        .await
        .unwrap();
        assert_eq!(
            members.iter().map(|m| m.role_id).collect::<Vec<_>>(),
            vec![child]
        );
        // The child is a lakekeeper-managed role, so it is manually assignable.
        assert_eq!(members.len(), 1);
        assert!(members[0].manually_assignable());
        // The edge's creation timestamp is populated from `role_membership.created_at`.
        // Use a wide window (not `<= now()`) to tolerate DB-vs-host clock skew.
        assert!(
            (chrono::Utc::now() - members[0].created_at)
                .num_seconds()
                .abs()
                < 3600,
            "created_at should be a recent timestamp, got {}",
            members[0].created_at
        );
    }

    #[sqlx::test]
    async fn role_membership_direct_cycle_rejected(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let a = create_managed_role(&state, &project_id, "a").await;
        let b = create_managed_role(&state, &project_id, "b").await;
        let project_id: ArcProjectId = Arc::new(project_id);

        // a -> b
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        PostgresBackend::add_role_members(&project_id, a, &[b], t.transaction())
            .await
            .unwrap();
        t.commit().await.unwrap();

        // b -> a must be rejected as a cycle.
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let err = PostgresBackend::add_role_members(&project_id, b, &[a], t.transaction())
            .await
            .unwrap_err();
        let AddRoleMembersError::RoleMembershipCycle(cycle) = err else {
            panic!("expected RoleMembershipCycle, got {err:?}");
        };
        assert_eq!(cycle.parent_role_id, b);
        assert_eq!(cycle.member_role_id, a);
    }

    #[sqlx::test]
    async fn role_membership_self_cycle_rejected(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let a = create_managed_role(&state, &project_id, "a").await;
        let project_id: ArcProjectId = Arc::new(project_id);

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let err = PostgresBackend::add_role_members(&project_id, a, &[a], t.transaction())
            .await
            .unwrap_err();
        let AddRoleMembersError::RoleMembershipCycle(cycle) = err else {
            panic!("expected RoleMembershipCycle, got {err:?}");
        };
        assert_eq!(cycle.parent_role_id, a);
        assert_eq!(cycle.member_role_id, a);
    }

    #[sqlx::test]
    async fn role_membership_transitive_cycle_rejected(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let a = create_managed_role(&state, &project_id, "a").await;
        let b = create_managed_role(&state, &project_id, "b").await;
        let c = create_managed_role(&state, &project_id, "c").await;
        let project_id: ArcProjectId = Arc::new(project_id);

        // a -> b, b -> c
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        PostgresBackend::add_role_members(&project_id, a, &[b], t.transaction())
            .await
            .unwrap();
        PostgresBackend::add_role_members(&project_id, b, &[c], t.transaction())
            .await
            .unwrap();
        t.commit().await.unwrap();

        // c -> a closes a depth-3 cycle and must be rejected.
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let err = PostgresBackend::add_role_members(&project_id, c, &[a], t.transaction())
            .await
            .unwrap_err();
        let AddRoleMembersError::RoleMembershipCycle(cycle) = err else {
            panic!("expected RoleMembershipCycle, got {err:?}");
        };
        assert_eq!(cycle.parent_role_id, c);
        assert_eq!(cycle.member_role_id, a);
    }

    /// A chain whose longest path through the new edge is *exactly* `max_depth`
    /// edges is allowed. With `max_depth = 3`: r0→r1→r2 already exists (2 edges),
    /// adding r2→r3 makes the longest chain r0→r1→r2→r3 = 3 edges = the limit.
    #[sqlx::test]
    async fn role_membership_depth_at_limit_allowed(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let r0 = create_managed_role(&state, &project_id, "r0").await;
        let r1 = create_managed_role(&state, &project_id, "r1").await;
        let r2 = create_managed_role(&state, &project_id, "r2").await;
        let r3 = create_managed_role(&state, &project_id, "r3").await;
        let project_id: ArcProjectId = Arc::new(project_id);

        // Build r0 -> r1 -> r2 (2 edges) without tripping the limit during setup.
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        add_role_members(&project_id, r0, &[r1], usize::MAX, t.transaction())
            .await
            .unwrap();
        add_role_members(&project_id, r1, &[r2], usize::MAX, t.transaction())
            .await
            .unwrap();
        t.commit().await.unwrap();

        // r2 -> r3: longest chain becomes exactly 3 edges → allowed at max_depth=3.
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let result = add_role_members(&project_id, r2, &[r3], 3, t.transaction())
            .await
            .unwrap();
        t.commit().await.unwrap();
        assert_eq!(result.added, vec![r3]);
    }

    /// One edge deeper than the limit is rejected. With `max_depth = 3`:
    /// r0→r1→r2→r3 already exists (3 edges); adding r3→r4 would make a 4-edge chain.
    #[sqlx::test]
    async fn role_membership_depth_exceeded(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let r0 = create_managed_role(&state, &project_id, "r0").await;
        let r1 = create_managed_role(&state, &project_id, "r1").await;
        let r2 = create_managed_role(&state, &project_id, "r2").await;
        let r3 = create_managed_role(&state, &project_id, "r3").await;
        let r4 = create_managed_role(&state, &project_id, "r4").await;
        let project_id: ArcProjectId = Arc::new(project_id);

        // Build r0 -> r1 -> r2 -> r3 (3 edges) without tripping the limit.
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        add_role_members(&project_id, r0, &[r1], usize::MAX, t.transaction())
            .await
            .unwrap();
        add_role_members(&project_id, r1, &[r2], usize::MAX, t.transaction())
            .await
            .unwrap();
        add_role_members(&project_id, r2, &[r3], usize::MAX, t.transaction())
            .await
            .unwrap();
        t.commit().await.unwrap();

        // r3 -> r4 would be a 4-edge chain → rejected at max_depth=3.
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let err = add_role_members(&project_id, r3, &[r4], 3, t.transaction())
            .await
            .unwrap_err();
        let AddRoleMembersError::RoleMembershipDepthExceeded(e) = err else {
            panic!("expected RoleMembershipDepthExceeded, got {err:?}");
        };
        assert_eq!(e.parent_role_id, r3);
        assert_eq!(e.member_role_id, r4);
        assert_eq!(e.max_depth, 3);
    }

    /// Depth counts edges on *both* sides of the new edge. With `max_depth = 3`,
    /// neither side alone exceeds it: the parent has 1 edge above it (g→p) and the
    /// member has 2 edges below it (m→m1→m2). Adding p→m makes the longest chain
    /// g→p→m→m1→m2 = 4 edges, so it must be rejected even though each side is small.
    #[sqlx::test]
    async fn role_membership_depth_counts_both_directions(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let g = create_managed_role(&state, &project_id, "g").await;
        let p = create_managed_role(&state, &project_id, "p").await;
        let m = create_managed_role(&state, &project_id, "m").await;
        let m1 = create_managed_role(&state, &project_id, "m1").await;
        let m2 = create_managed_role(&state, &project_id, "m2").await;
        let project_id: ArcProjectId = Arc::new(project_id);

        // Upward of the parent: g -> p (1 edge above p).
        // Downward of the member: m -> m1 -> m2 (2 edges below m).
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        add_role_members(&project_id, g, &[p], usize::MAX, t.transaction())
            .await
            .unwrap();
        add_role_members(&project_id, m, &[m1], usize::MAX, t.transaction())
            .await
            .unwrap();
        add_role_members(&project_id, m1, &[m2], usize::MAX, t.transaction())
            .await
            .unwrap();
        t.commit().await.unwrap();

        // p -> m: 1 (above p) + 1 (new edge) + 2 (below m) = 4 edges → rejected.
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let err = add_role_members(&project_id, p, &[m], 3, t.transaction())
            .await
            .unwrap_err();
        let AddRoleMembersError::RoleMembershipDepthExceeded(e) = err else {
            panic!("expected RoleMembershipDepthExceeded, got {err:?}");
        };
        assert_eq!(e.parent_role_id, p);
        assert_eq!(e.member_role_id, m);
        assert_eq!(e.max_depth, 3);
    }

    /// A multi-member add is all-or-nothing: if any one member would exceed the
    /// limit, the whole batch is rejected (naming that member) and no edge —
    /// not even the in-limit ones — is written. With `max_depth = 2`, adding
    /// `[shallow, deep]` to a parent rejects because `deep` has a 2-edge subtree
    /// (`deep→d1→d2`), so `p→deep→d1→d2` = 3 edges; `p→shallow` (1 edge) is fine
    /// but must not be persisted either.
    #[sqlx::test]
    async fn role_membership_depth_batch_is_all_or_nothing(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let p = create_managed_role(&state, &project_id, "p").await;
        let shallow = create_managed_role(&state, &project_id, "shallow").await;
        let deep = create_managed_role(&state, &project_id, "deep").await;
        let d1 = create_managed_role(&state, &project_id, "d1").await;
        let d2 = create_managed_role(&state, &project_id, "d2").await;
        let project_id: ArcProjectId = Arc::new(project_id);

        // deep -> d1 -> d2 (2 edges below `deep`), built without tripping the limit.
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        add_role_members(&project_id, deep, &[d1], usize::MAX, t.transaction())
            .await
            .unwrap();
        add_role_members(&project_id, d1, &[d2], usize::MAX, t.transaction())
            .await
            .unwrap();
        t.commit().await.unwrap();

        // Batch p -> [shallow, deep] at max_depth=2: only `deep` exceeds, but the
        // whole call is rejected and it names `deep`.
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let err = add_role_members(&project_id, p, &[shallow, deep], 2, t.transaction())
            .await
            .unwrap_err();
        let AddRoleMembersError::RoleMembershipDepthExceeded(e) = err else {
            panic!("expected RoleMembershipDepthExceeded, got {err:?}");
        };
        assert_eq!(e.parent_role_id, p);
        assert_eq!(e.member_role_id, deep);
        assert_eq!(e.max_depth, 2);
        drop(t); // roll back the rejected transaction

        // All-or-nothing: the in-limit `p -> shallow` edge was NOT written either.
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let members =
            list_role_memberships(p, RoleMembershipDirection::Members, &mut **t.transaction())
                .await
                .unwrap();
        t.commit().await.unwrap();
        assert_eq!(members, Vec::new());
    }

    /// The default limit (10) is enforced through the public trait path
    /// (`PostgresBackend::add_role_members`), which reads
    /// `CONFIG.role.max_nesting_depth` — proving the config is wired end-to-end,
    /// not just the free fn. A 10-edge chain is allowed; the 11th edge is rejected.
    #[sqlx::test]
    async fn role_membership_default_depth_enforced_via_trait_path(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let mut roles = Vec::new();
        for i in 0..=11 {
            roles.push(create_managed_role(&state, &project_id, &format!("r{i}")).await);
        }
        let project_id: ArcProjectId = Arc::new(project_id);

        // r0 -> r1 -> ... -> r10 == 10 edges, exactly the default limit → all allowed.
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        for i in 0..10 {
            PostgresBackend::add_role_members(
                &project_id,
                roles[i],
                &[roles[i + 1]],
                t.transaction(),
            )
            .await
            .unwrap();
        }
        t.commit().await.unwrap();

        // r10 -> r11 would be the 11th edge → rejected by the default max of 10.
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let err = PostgresBackend::add_role_members(
            &project_id,
            roles[10],
            &[roles[11]],
            t.transaction(),
        )
        .await
        .unwrap_err();
        let AddRoleMembersError::RoleMembershipDepthExceeded(e) = err else {
            panic!("expected RoleMembershipDepthExceeded, got {err:?}");
        };
        assert_eq!(e.parent_role_id, roles[10]);
        assert_eq!(e.member_role_id, roles[11]);
        assert_eq!(e.max_depth, 10);
    }

    /// `max_depth = 0` disables role nesting entirely: any role→role edge is a
    /// 1-edge chain (`0 + 1 + 0 > 0`), so it is rejected. This is the documented
    /// flat-roles-only policy — `0` is a valid configured value, not clamped away.
    #[sqlx::test]
    async fn role_membership_depth_zero_disables_nesting(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let p = create_managed_role(&state, &project_id, "p").await;
        let m = create_managed_role(&state, &project_id, "m").await;
        let project_id: ArcProjectId = Arc::new(project_id);

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let err = add_role_members(&project_id, p, &[m], 0, t.transaction())
            .await
            .unwrap_err();
        let AddRoleMembersError::RoleMembershipDepthExceeded(e) = err else {
            panic!("expected RoleMembershipDepthExceeded, got {err:?}");
        };
        assert_eq!(e.parent_role_id, p);
        assert_eq!(e.member_role_id, m);
        assert_eq!(e.max_depth, 0);
    }

    #[sqlx::test]
    async fn effective_roles_resolve_ancestor_closure_depth_3(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let grandparent = create_managed_role(&state, &project_id, "grandparent").await;
        let parent = create_managed_role(&state, &project_id, "parent").await;
        let leaf = create_managed_role(&state, &project_id, "leaf").await;
        let arc_project_id: ArcProjectId = Arc::new(project_id.clone());

        // Membership edges (member -> parent):
        //   grandparent has member parent
        //   parent      has member leaf
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        PostgresBackend::add_role_members(&arc_project_id, grandparent, &[parent], t.transaction())
            .await
            .unwrap();
        PostgresBackend::add_role_members(&arc_project_id, parent, &[leaf], t.transaction())
            .await
            .unwrap();
        t.commit().await.unwrap();

        // Assign a user DIRECTLY to leaf only. `sync_role_members_by_ident`
        // upserts by (provider_id, source_id, project_id); the leaf role was
        // created via `create_managed_role` with the lakekeeper provider and
        // source_id == name, so this matches the existing role and inserts a
        // real role_assignment(user, leaf) row (provisioning the user too).
        let leaf_ident = Arc::new(RoleIdent::new_unchecked("lakekeeper", "leaf"));
        let leaf_role = make_role(&leaf_ident, "leaf");
        let user_id = Arc::new(UserId::new_unchecked("oidc", "alice"));
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let sync = sync_role_members_by_ident(
            &project_id,
            &leaf_role,
            um(&[make_user(&user_id, "Alice")]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();
        // Sanity: the upsert matched the existing leaf role, not a new one.
        assert_eq!(sync.added.len(), 1, "exactly one member added to leaf");

        let result = list_role_assignments_for_user(&user_id, &pool)
            .await
            .unwrap();

        let actual_set: HashSet<RoleId> = result.roles.iter().map(|r| r.role_id).collect();
        assert_eq!(actual_set, HashSet::from([leaf, parent, grandparent]));
    }

    /// Regression lock: transitively-acquired (ancestor) roles must NOT inject
    /// phantom `provider_sync_times` entries.
    ///
    /// `provider_sync_times` is built solely from the user's own
    /// `role_assignment_sync` rows (the `UNION ALL` block plus the
    /// `LEFT JOIN role_assignment_sync` in the `assigned` CTE). Ancestor roles
    /// are reached purely via `role_membership` edges, which Task 2 restricts to
    /// catalog-managed (lakekeeper) roles — these are never written to
    /// `role_assignment_sync`. So even though the user gains `parent` and
    /// `grandparent` transitively, no sync metadata is fabricated for them.
    ///
    /// Here the user is provisioned + assigned to `leaf` via
    /// `sync_role_members_by_ident`, which writes the per-role `role_members_sync`
    /// log, NOT the per-(user, project, provider) `role_assignment_sync` log that
    /// `provider_sync_times` reads from. Hence `provider_sync_times` is exactly
    /// empty.
    #[sqlx::test]
    async fn effective_roles_transitive_roles_add_no_phantom_sync_times(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let grandparent = create_managed_role(&state, &project_id, "grandparent").await;
        let parent = create_managed_role(&state, &project_id, "parent").await;
        let leaf = create_managed_role(&state, &project_id, "leaf").await;
        let arc_project_id: ArcProjectId = Arc::new(project_id.clone());

        // Membership edges (member -> parent), all catalog-managed roles:
        //   grandparent has member parent
        //   parent      has member leaf
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        PostgresBackend::add_role_members(&arc_project_id, grandparent, &[parent], t.transaction())
            .await
            .unwrap();
        PostgresBackend::add_role_members(&arc_project_id, parent, &[leaf], t.transaction())
            .await
            .unwrap();
        t.commit().await.unwrap();

        // Assign the user DIRECTLY to leaf only (matches the existing
        // lakekeeper-managed leaf role created above). `sync_role_members_by_ident`
        // writes `role_members_sync` (per-role), never `role_assignment_sync`
        // (per-user/project/provider), so the user has NO direct provider-sync state.
        let leaf_ident = Arc::new(RoleIdent::new_unchecked("lakekeeper", "leaf"));
        let leaf_role = make_role(&leaf_ident, "leaf");
        let user_id = Arc::new(UserId::new_unchecked("oidc", "alice"));
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let sync = sync_role_members_by_ident(
            &project_id,
            &leaf_role,
            um(&[make_user(&user_id, "Alice")]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();
        // Sanity: the upsert matched the existing leaf role, not a new one.
        assert_eq!(sync.added.len(), 1, "exactly one member added to leaf");

        let result = list_role_assignments_for_user(&user_id, &pool)
            .await
            .unwrap();

        // The effective role set is EXACTLY the full ancestor closure.
        let actual_set: HashSet<RoleId> = result.roles.iter().map(|r| r.role_id).collect();
        assert_eq!(actual_set, HashSet::from([leaf, parent, grandparent]));

        // The transitive roles add NO provider-sync entries. The user has no
        // `role_assignment_sync` rows at all, so the result is exactly empty.
        assert!(
            result.provider_sync_times.is_empty(),
            "transitive roles must not inject phantom provider_sync_times; got {:?}",
            result.provider_sync_times
        );
    }

    #[sqlx::test]
    async fn role_membership_remove_is_idempotent(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let parent = create_managed_role(&state, &project_id, "parent").await;
        let child = create_managed_role(&state, &project_id, "child").await;
        let project_id: ArcProjectId = Arc::new(project_id);

        // Removing a non-existent edge is Ok (no-op): nothing was removed, so the delta is empty.
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let noop = PostgresBackend::remove_role_members(parent, &[child], t.transaction())
            .await
            .unwrap();
        t.commit().await.unwrap();
        assert_eq!(noop.removed, Vec::<RoleId>::new());

        // Add the edge, then remove it.
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        PostgresBackend::add_role_members(&project_id, parent, &[child], t.transaction())
            .await
            .unwrap();
        t.commit().await.unwrap();

        assert_eq!(
            PostgresBackend::list_role_memberships(
                parent,
                RoleMembershipDirection::Members,
                state.clone()
            )
            .await
            .unwrap()
            .iter()
            .map(|m| m.role_id)
            .collect::<Vec<_>>(),
            vec![child]
        );

        // Removing the present edge — the child is the actually-removed member.
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let removed = PostgresBackend::remove_role_members(parent, &[child], t.transaction())
            .await
            .unwrap();
        t.commit().await.unwrap();
        assert_eq!(removed.removed, vec![child]);
        // The post-op members read back on the same transaction are now empty.
        assert_eq!(removed.members, Vec::new());

        assert_eq!(
            PostgresBackend::list_role_memberships(
                parent,
                RoleMembershipDirection::Members,
                state.clone()
            )
            .await
            .unwrap()
            .iter()
            .map(|m| m.role_id)
            .collect::<Vec<_>>(),
            Vec::<RoleId>::new()
        );

        // Removing again is a no-op: the delta is empty.
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let second = PostgresBackend::remove_role_members(parent, &[child], t.transaction())
            .await
            .unwrap();
        t.commit().await.unwrap();
        assert_eq!(second.removed, Vec::<RoleId>::new());
    }

    #[sqlx::test]
    async fn role_membership_cross_project_rejected(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_a = make_project(&state).await;
        let project_b = make_project(&state).await;
        let parent = create_managed_role(&state, &project_a, "parent").await;
        // Member lives in a different project than `parent`.
        let foreign_member = create_managed_role(&state, &project_b, "member").await;
        let project_a: ArcProjectId = Arc::new(project_a);

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let err = PostgresBackend::add_role_members(
            &project_a,
            parent,
            &[foreign_member],
            t.transaction(),
        )
        .await
        .unwrap_err();
        let AddRoleMembersError::RoleIdNotFoundInProject(e) = err else {
            panic!("expected RoleIdNotFoundInProject, got {err:?}");
        };
        // The foreign-project MEMBER is the role reported as not-found (not the parent).
        assert_eq!(e.role_id, foreign_member);
        assert_eq!(e.project_id.as_ref(), project_a.as_ref());
    }

    #[sqlx::test]
    async fn role_membership_external_provider_member_rejected(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let parent = create_managed_role(&state, &project_id, "parent").await;
        // An externally-provided (LDAP) role is not catalog-managed, so it cannot
        // be added as a member — only lakekeeper/system roles may nest.
        let external = create_external_role(&state, &project_id, "ldap", "ext-group").await;
        let project_id: ArcProjectId = Arc::new(project_id);

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let err =
            PostgresBackend::add_role_members(&project_id, parent, &[external], t.transaction())
                .await
                .unwrap_err();
        let AddRoleMembersError::RoleNotManuallyAssignable(e) = err else {
            panic!("expected RoleNotManuallyAssignable, got {err:?}");
        };
        assert_eq!(e.role_id, external);
        assert_eq!(e.provider_id, RoleProviderId::new_unchecked("ldap"));
    }

    #[sqlx::test]
    async fn role_membership_add_dedups_batch(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let parent = create_managed_role(&state, &project_id, "parent").await;
        let b = create_managed_role(&state, &project_id, "b").await;
        let c = create_managed_role(&state, &project_id, "c").await;
        let project_id: ArcProjectId = Arc::new(project_id);

        // Batch containing a duplicate member id — deduped, each edge inserted once.
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        PostgresBackend::add_role_members(&project_id, parent, &[b, c, b], t.transaction())
            .await
            .unwrap();
        t.commit().await.unwrap();

        let members: HashSet<RoleId> = PostgresBackend::list_role_memberships(
            parent,
            RoleMembershipDirection::Members,
            state.clone(),
        )
        .await
        .unwrap()
        .into_iter()
        .map(|m| m.role_id)
        .collect();
        assert_eq!(members, HashSet::from([b, c]));
    }

    #[sqlx::test]
    async fn role_membership_list_parents_direct(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let parent1 = create_managed_role(&state, &project_id, "parent1").await;
        let parent2 = create_managed_role(&state, &project_id, "parent2").await;
        let member = create_managed_role(&state, &project_id, "member").await;
        let project_id: ArcProjectId = Arc::new(project_id);

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        PostgresBackend::add_role_members(&project_id, parent1, &[member], t.transaction())
            .await
            .unwrap();
        PostgresBackend::add_role_members(&project_id, parent2, &[member], t.transaction())
            .await
            .unwrap();
        t.commit().await.unwrap();

        let parents: HashSet<RoleId> = PostgresBackend::list_role_memberships(
            member,
            RoleMembershipDirection::Parents,
            state.clone(),
        )
        .await
        .unwrap()
        .into_iter()
        .map(|m| m.role_id)
        .collect();
        assert_eq!(parents, HashSet::from([parent1, parent2]));
    }

    #[sqlx::test]
    async fn role_membership_parent_not_found_rejected(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let member = create_managed_role(&state, &project_id, "member").await;
        // A parent role id that does not exist in this project.
        let missing_parent = RoleId::new_random();
        let project_id: ArcProjectId = Arc::new(project_id);

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let err = PostgresBackend::add_role_members(
            &project_id,
            missing_parent,
            &[member],
            t.transaction(),
        )
        .await
        .unwrap_err();
        let AddRoleMembersError::RoleIdNotFoundInProject(e) = err else {
            panic!("expected RoleIdNotFoundInProject, got {err:?}");
        };
        // The MISSING PARENT is the role reported as not-found.
        assert_eq!(e.role_id, missing_parent);
    }

    #[sqlx::test]
    async fn role_membership_add_empty_members_still_validates_parent(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let missing_parent = RoleId::new_random();
        let project_id: ArcProjectId = Arc::new(project_id);

        // An empty member list is a no-op, but the parent must still be validated:
        // an unknown parent is rejected, not silently accepted.
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let err =
            PostgresBackend::add_role_members(&project_id, missing_parent, &[], t.transaction())
                .await
                .unwrap_err();
        let AddRoleMembersError::RoleIdNotFoundInProject(e) = err else {
            panic!("expected RoleIdNotFoundInProject, got {err:?}");
        };
        assert_eq!(e.role_id, missing_parent);
    }

    #[sqlx::test]
    async fn role_membership_empty_input_is_noop(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let parent = create_managed_role(&state, &project_id, "parent").await;
        let project_id: ArcProjectId = Arc::new(project_id);

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        PostgresBackend::add_role_members(&project_id, parent, &[], t.transaction())
            .await
            .unwrap();
        PostgresBackend::remove_role_members(parent, &[], t.transaction())
            .await
            .unwrap();
        t.commit().await.unwrap();

        assert!(
            PostgresBackend::list_role_memberships(
                parent,
                RoleMembershipDirection::Members,
                state.clone()
            )
            .await
            .unwrap()
            .is_empty()
        );
    }

    #[sqlx::test]
    async fn edge_change_invalidates_deep_descendant_user(pool: sqlx::PgPool) {
        // top <- mid <- leaf; user assigned to leaf. Adding the top<-mid edge must
        // invalidate the user even though they are a DEPTH-2 descendant of `mid`,
        // exercising the recursive descendant closure (not just depth-1).
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let top = create_managed_role(&state, &project_id, "top").await;
        let mid = create_managed_role(&state, &project_id, "mid").await;
        let leaf = create_managed_role(&state, &project_id, "leaf").await;
        let arc_project_id: ArcProjectId = Arc::new(project_id.clone());

        // mid has member leaf.
        PostgresBackend::add_role_members_and_invalidate(
            &arc_project_id,
            mid,
            &[leaf],
            state.clone(),
        )
        .await
        .unwrap();

        // Assign user to leaf (matches the lakekeeper-managed `leaf` role above).
        let leaf_ident = Arc::new(RoleIdent::new_unchecked("lakekeeper", "leaf"));
        let leaf_role = make_role(&leaf_ident, "leaf");
        let user_id = Arc::new(UserId::new_unchecked("oidc", "deep-descendant-user"));
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        sync_role_members_by_ident(
            &project_id,
            &leaf_role,
            um(&[make_user(&user_id, "Deep")]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        // Warm: U effective = {leaf, mid}.
        let warmed = PostgresBackend::list_role_assignments_for_user(&user_id, state.clone())
            .await
            .unwrap();
        assert_eq!(
            warmed
                .roles
                .iter()
                .map(|r| r.role_id)
                .collect::<HashSet<_>>(),
            HashSet::from([leaf, mid])
        );

        // Add top <- mid. descendants(mid) = {mid, leaf}; U (on leaf) must be invalidated.
        PostgresBackend::add_role_members_and_invalidate(
            &arc_project_id,
            top,
            &[mid],
            state.clone(),
        )
        .await
        .unwrap();

        let after = PostgresBackend::list_role_assignments_for_user(&user_id, state.clone())
            .await
            .unwrap();
        assert!(
            !Arc::ptr_eq(&warmed, &after),
            "deep (depth-2) descendant user must be invalidated"
        );
        assert_eq!(
            after
                .roles
                .iter()
                .map(|r| r.role_id)
                .collect::<HashSet<_>>(),
            HashSet::from([leaf, mid, top])
        );
    }

    #[sqlx::test]
    async fn role_membership_remove_and_invalidate_evicts(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let parent = create_managed_role(&state, &project_id, "parent").await;
        let child = create_managed_role(&state, &project_id, "child").await;
        let arc_project_id: ArcProjectId = Arc::new(project_id.clone());

        PostgresBackend::add_role_members_and_invalidate(
            &arc_project_id,
            parent,
            &[child],
            state.clone(),
        )
        .await
        .unwrap();

        let child_ident = Arc::new(RoleIdent::new_unchecked("lakekeeper", "child"));
        let child_role = make_role(&child_ident, "child");
        let user_id = Arc::new(UserId::new_unchecked("oidc", "remove-invalidation-user"));
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        sync_role_members_by_ident(
            &project_id,
            &child_role,
            um(&[make_user(&user_id, "U")]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        // Warm: U effective = {child, parent}.
        let warmed = PostgresBackend::list_role_assignments_for_user(&user_id, state.clone())
            .await
            .unwrap();
        assert_eq!(
            warmed
                .roles
                .iter()
                .map(|r| r.role_id)
                .collect::<HashSet<_>>(),
            HashSet::from([child, parent])
        );

        // Remove the edge + invalidate. U loses the transitively-acquired `parent`.
        PostgresBackend::remove_role_members_and_invalidate(parent, &[child], state.clone())
            .await
            .unwrap();

        let after = PostgresBackend::list_role_assignments_for_user(&user_id, state.clone())
            .await
            .unwrap();
        assert!(
            !Arc::ptr_eq(&warmed, &after),
            "cache must be invalidated on edge removal"
        );
        assert_eq!(
            after
                .roles
                .iter()
                .map(|r| r.role_id)
                .collect::<HashSet<_>>(),
            HashSet::from([child])
        );
    }

    #[sqlx::test]
    async fn role_membership_add_lock_timeout(pool: sqlx::PgPool) {
        // Hold the per-project advisory lock on a separate connection, then a
        // concurrent `add_role_members` must wait `lock_timeout` (3s) and fail
        // with the typed `RoleMembershipLockTimeout` rather than hang.
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let parent = create_managed_role(&state, &project_id, "parent").await;
        let child = create_managed_role(&state, &project_id, "child").await;
        let arc_project_id: ArcProjectId = Arc::new(project_id);

        // Holder: open a transaction on its own connection and grab the exact
        // same project lock key the impl uses, keeping it held.
        let mut holder = pool.begin().await.unwrap();
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, $2))")
            .bind(arc_project_id.as_str())
            .bind(super::ROLE_MEMBERSHIP_LOCK_SEED)
            .execute(&mut *holder)
            .await
            .unwrap();

        // Concurrent add on a different connection: blocked → 3s timeout → typed error.
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let err =
            PostgresBackend::add_role_members(&arc_project_id, parent, &[child], t.transaction())
                .await
                .unwrap_err();
        let AddRoleMembersError::RoleMembershipLockTimeout(e) = err else {
            panic!("expected RoleMembershipLockTimeout, got {err:?}");
        };
        assert_eq!(e.project_id.as_ref(), arc_project_id.as_ref());

        // Release the held lock.
        holder.rollback().await.unwrap();
    }

    // ── sync_role_members_by_ident ─────────────────────────────────────────

    #[sqlx::test]
    async fn role_members_initial_sync(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let role_ident = Arc::new(RoleIdent::new_unchecked("ldap", "group-engineers"));
        let role = make_role(&role_ident, "Engineers");
        let u1 = Arc::new(UserId::new_unchecked("oidc", "alice"));
        let u2 = Arc::new(UserId::new_unchecked("oidc", "bob"));

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let result = sync_role_members_by_ident(
            &project_id,
            &role,
            um(&[make_user(&u1, "Alice"), make_user(&u2, "Bob")]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        assert_eq!(
            result.added.len(),
            2,
            "both members should be added on first sync"
        );
        assert_eq!(result.removed.len(), 0);
        assert!(result.synced_at <= chrono::Utc::now());

        let added_ids: HashSet<_> = result.added.iter().map(|u| &u.user_id).collect();
        assert!(added_ids.contains(&u1));
        assert!(added_ids.contains(&u2));

        let list = list_role_assignments_for_role_by_ident(&project_id, &role_ident, &pool)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(list.members.len(), 2);
        assert!(list.last_synced_at.is_some());
    }

    #[sqlx::test]
    async fn role_members_incremental_add_and_remove(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let role_ident = Arc::new(RoleIdent::new_unchecked("ldap", "group-ops"));
        let role = make_role(&role_ident, "Ops");
        let u1 = Arc::new(UserId::new_unchecked("oidc", "alice"));
        let u2 = Arc::new(UserId::new_unchecked("oidc", "bob"));
        let u3 = Arc::new(UserId::new_unchecked("oidc", "carol"));

        // First sync: [u1, u2]
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        sync_role_members_by_ident(
            &project_id,
            &role,
            um(&[make_user(&u1, "Alice"), make_user(&u2, "Bob")]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        // Second sync: [u2, u3] — removes u1, adds u3
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let result = sync_role_members_by_ident(
            &project_id,
            &role,
            um(&[make_user(&u2, "Bob"), make_user(&u3, "Carol")]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        assert_eq!(result.added.len(), 1);
        assert_eq!(result.added[0].user_id, u3);
        assert_eq!(result.removed.len(), 1);
        assert_eq!(result.removed[0].user_id, u1);

        let list = list_role_assignments_for_role_by_ident(&project_id, &role_ident, &pool)
            .await
            .unwrap()
            .unwrap();
        let member_ids: HashSet<_> = list.members.iter().map(|u| &u.user_id).collect();
        assert!(member_ids.contains(&u2));
        assert!(member_ids.contains(&u3));
        assert!(!member_ids.contains(&u1));
    }

    #[sqlx::test]
    async fn role_members_empty_clears_all(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let role_ident = Arc::new(RoleIdent::new_unchecked("ldap", "group-empty"));
        let role = make_role(&role_ident, "Group");
        let u1 = Arc::new(UserId::new_unchecked("oidc", "alice"));
        let u2 = Arc::new(UserId::new_unchecked("oidc", "bob"));

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        sync_role_members_by_ident(
            &project_id,
            &role,
            um(&[make_user(&u1, "Alice"), make_user(&u2, "Bob")]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let result = sync_role_members_by_ident(&project_id, &role, um(&[]), t.transaction())
            .await
            .unwrap();
        t.commit().await.unwrap();

        assert_eq!(result.added.len(), 0);
        assert_eq!(result.removed.len(), 2);

        let list = list_role_assignments_for_role_by_ident(&project_id, &role_ident, &pool)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(list.members.len(), 0);
        // The sync record must survive even when the member list is empty —
        // callers need to know "we synced and found no members".
        assert_eq!(
            list.last_synced_at,
            Some(result.synced_at),
            "last_synced_at must be present after clearing all members"
        );
    }

    #[sqlx::test]
    async fn role_members_idempotent(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let role_ident = Arc::new(RoleIdent::new_unchecked("ldap", "group-stable"));
        let u1 = Arc::new(UserId::new_unchecked("oidc", "alice"));

        for _ in 0..3 {
            let mut t = PostgresTransaction::begin_write(state.clone())
                .await
                .unwrap();
            let result = sync_role_members_by_ident(
                &project_id,
                &make_role(&role_ident, "Stable"),
                um(&[make_user(&u1, "Alice")]),
                t.transaction(),
            )
            .await
            .unwrap();
            t.commit().await.unwrap();
            assert_eq!(result.removed.len(), 0);
        }

        let list = list_role_assignments_for_role_by_ident(&project_id, &role_ident, &pool)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(list.members.len(), 1);
    }

    #[sqlx::test]
    async fn role_members_updates_role_name_on_sync(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let role_ident = Arc::new(RoleIdent::new_unchecked("ldap", "group-renamed"));
        let u1 = Arc::new(UserId::new_unchecked("oidc", "alice"));

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        sync_role_members_by_ident(
            &project_id,
            &make_role(&role_ident, "Old Name"),
            um(&[make_user(&u1, "Alice")]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        // Re-sync with updated role name — must not error
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let result = sync_role_members_by_ident(
            &project_id,
            &make_role(&role_ident, "New Name"),
            um(&[make_user(&u1, "Alice")]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        assert_eq!(result.added.len(), 0);
        assert_eq!(result.removed.len(), 0);
    }

    /// When the role's name and description are unchanged, the role row must
    /// not be written (no `updated_at` bump).  The `updated_at` trigger fires
    /// only when a row is actually written, so comparing timestamps before and
    /// after an identical re-sync is the precise observable test.
    #[sqlx::test]
    async fn role_members_no_write_when_role_unchanged(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let role_ident = Arc::new(RoleIdent::new_unchecked("ldap", "group-stable-role"));
        let u1 = Arc::new(UserId::new_unchecked("oidc", "alice"));

        // Initial sync — creates the role row.
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let first = sync_role_members_by_ident(
            &project_id,
            &make_role(&role_ident, "Stable"),
            um(&[make_user(&u1, "Alice")]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        // Capture version immediately after the first sync.
        // updated_at is NULL after INSERT (trigger only fires on UPDATE), so we
        // use version (BIGINT NOT NULL DEFAULT 0) which increments on every
        // actual write and stays at 0 when the ON CONFLICT WHERE skips the update.
        let version_after_first: i64 = sqlx::query_scalar!(
            r#"SELECT version FROM "role" WHERE id = $1"#,
            *first.role_id,
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        // Identical re-sync — nothing should change.
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let second = sync_role_members_by_ident(
            &project_id,
            &make_role(&role_ident, "Stable"),
            um(&[make_user(&u1, "Alice")]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        assert_eq!(first.role_id, second.role_id);
        assert_eq!(second.added.len(), 0);
        assert_eq!(second.removed.len(), 0);

        let version_after_second: i64 = sqlx::query_scalar!(
            r#"SELECT version FROM "role" WHERE id = $1"#,
            *first.role_id,
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(
            version_after_first, version_after_second,
            "role row must not be written when nothing changed"
        );
    }

    /// Counter-test: when the role name changes, `updated_at` MUST advance.
    #[sqlx::test]
    async fn role_members_write_when_role_name_changes(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let role_ident = Arc::new(RoleIdent::new_unchecked("ldap", "group-changing-name"));
        let u1 = Arc::new(UserId::new_unchecked("oidc", "alice"));

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let first = sync_role_members_by_ident(
            &project_id,
            &make_role(&role_ident, "Original Name"),
            um(&[make_user(&u1, "Alice")]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        let version_before: i64 = sqlx::query_scalar!(
            r#"SELECT version FROM "role" WHERE id = $1"#,
            *first.role_id,
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        sync_role_members_by_ident(
            &project_id,
            &make_role(&role_ident, "New Name"),
            um(&[make_user(&u1, "Alice")]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        let version_after: i64 = sqlx::query_scalar!(
            r#"SELECT version FROM "role" WHERE id = $1"#,
            *first.role_id,
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert!(
            version_after > version_before,
            "role row must be written when name changes"
        );
    }

    #[sqlx::test]
    async fn role_members_empty_from_start(pool: sqlx::PgPool) {
        // First sync is empty — member_changes returns 0 rows; tests that
        // added_ids! / removed_ids! / role_id! / synced_at! are all non-null.
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let role_ident = Arc::new(RoleIdent::new_unchecked("ldap", "group-never-had-members"));
        let role = make_role(&role_ident, "Empty Group");

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let result = sync_role_members_by_ident(&project_id, &role, um(&[]), t.transaction())
            .await
            .unwrap();
        t.commit().await.unwrap();

        assert_eq!(result.added.len(), 0);
        assert_eq!(result.removed.len(), 0);

        let list = list_role_assignments_for_role_by_ident(&project_id, &role_ident, &pool)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(list.members.len(), 0);
        // The sync record must be present even for a role that has never had
        // any members — callers need to know "we synced and found nothing".
        assert_eq!(
            list.last_synced_at,
            Some(result.synced_at),
            "last_synced_at must be present even on a first-ever empty sync"
        );
    }

    #[sqlx::test]
    async fn role_members_empty_twice(pool: sqlx::PgPool) {
        // Two consecutive empty syncs — second sync has 0 rows in member_changes.
        // Core invariant: `last_synced_at` returned by the list function must
        // be present and must strictly advance on every sync, even when the
        // member set stays empty.
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let role_ident = Arc::new(RoleIdent::new_unchecked("ldap", "group-empty-twice"));
        let role = make_role(&role_ident, "Twice Empty");
        let u1 = Arc::new(UserId::new_unchecked("oidc", "alice"));

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        sync_role_members_by_ident(
            &project_id,
            &role,
            um(&[make_user(&u1, "Alice")]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        // First clear
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let first_clear = sync_role_members_by_ident(&project_id, &role, um(&[]), t.transaction())
            .await
            .unwrap();
        t.commit().await.unwrap();

        assert_eq!(first_clear.added.len(), 0);
        assert_eq!(first_clear.removed.len(), 1, "alice removed on first clear");

        let list = list_role_assignments_for_role_by_ident(&project_id, &role_ident, &pool)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(list.members.len(), 0, "no members after first clear");
        assert_eq!(
            list.last_synced_at,
            Some(first_clear.synced_at),
            "last_synced_at must equal synced_at from first clear"
        );

        // Second clear — member_changes produces 0 rows
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let result = sync_role_members_by_ident(&project_id, &role, um(&[]), t.transaction())
            .await
            .unwrap();
        t.commit().await.unwrap();

        assert_eq!(result.added.len(), 0);
        assert_eq!(result.removed.len(), 0);
        // sync_ts must be written even on a no-op empty sync, advancing the timestamp.
        assert!(
            result.synced_at > first_clear.synced_at,
            "synced_at must strictly advance on each sync: first={:?} second={:?}",
            first_clear.synced_at,
            result.synced_at,
        );

        let list2 = list_role_assignments_for_role_by_ident(&project_id, &role_ident, &pool)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            list2.members.len(),
            0,
            "still no members after second clear"
        );
        assert_eq!(
            list2.last_synced_at,
            Some(result.synced_at),
            "last_synced_at must reflect the second sync"
        );
    }

    // ── list_role_assignments_for_role / list_role_assignments_for_role_by_ident ──

    /// Both list functions return `None` when no role with the given ident / id
    /// exists in the database.
    #[sqlx::test]
    async fn list_role_by_ident_returns_none_for_unknown(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let role_ident = RoleIdent::new_unchecked("ldap", "does-not-exist");

        let result = list_role_assignments_for_role_by_ident(&project_id, &role_ident, &pool)
            .await
            .unwrap();
        assert!(
            result.is_none(),
            "must return None when role does not exist"
        );
    }

    #[sqlx::test]
    async fn list_role_by_id_returns_none_for_unknown(pool: sqlx::PgPool) {
        let pool2 = pool.clone();
        let result = list_role_assignments_for_role(RoleId::new(Uuid::new_v4()), &pool2)
            .await
            .unwrap();
        assert!(
            result.is_none(),
            "must return None when role id does not exist"
        );
    }

    /// Listing by ID and listing by ident return equivalent results: same
    /// `role_id`, same members, same `last_synced_at`.
    #[sqlx::test]
    async fn list_role_by_id_matches_list_by_ident(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let role_ident = Arc::new(RoleIdent::new_unchecked("ldap", "group-engineers"));
        let role = make_role(&role_ident, "Engineers");
        let u1 = Arc::new(UserId::new_unchecked("oidc", "alice"));
        let u2 = Arc::new(UserId::new_unchecked("oidc", "bob"));

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let sync = sync_role_members_by_ident(
            &project_id,
            &role,
            um(&[make_user(&u1, "Alice"), make_user(&u2, "Bob")]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        let by_ident = list_role_assignments_for_role_by_ident(&project_id, &role_ident, &pool)
            .await
            .unwrap()
            .expect("role must exist");
        let by_id = list_role_assignments_for_role(sync.role_id, &pool)
            .await
            .unwrap()
            .expect("role must exist");

        assert_eq!(by_id.role_id, by_ident.role_id, "role_id must match");
        let ids_by_ident: HashSet<_> = by_ident.members.iter().map(|m| &m.user_id).collect();
        let ids_by_id: HashSet<_> = by_id.members.iter().map(|m| &m.user_id).collect();
        assert_eq!(ids_by_ident, ids_by_id, "member sets must match");
        assert_eq!(
            by_id.last_synced_at, by_ident.last_synced_at,
            "last_synced_at must match"
        );
    }

    /// `last_synced_at` on the list result must equal the `synced_at` returned
    /// by the most recent sync call.
    #[sqlx::test]
    async fn list_role_last_synced_at_matches_sync_result(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let role_ident = Arc::new(RoleIdent::new_unchecked("ldap", "group-timed"));
        let role = make_role(&role_ident, "Timed Group");
        let u1 = Arc::new(UserId::new_unchecked("oidc", "alice"));

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let first_sync = sync_role_members_by_ident(
            &project_id,
            &role,
            um(&[make_user(&u1, "Alice")]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        let list = list_role_assignments_for_role_by_ident(&project_id, &role_ident, &pool)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            list.last_synced_at,
            Some(first_sync.synced_at),
            "last_synced_at must equal synced_at from the sync result"
        );

        // A second sync advances the timestamp.
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let second_sync = sync_role_members_by_ident(
            &project_id,
            &role,
            um(&[make_user(&u1, "Alice")]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        let list2 = list_role_assignments_for_role_by_ident(&project_id, &role_ident, &pool)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            list2.last_synced_at,
            Some(second_sync.synced_at),
            "last_synced_at must reflect the most recent sync"
        );
        assert!(
            second_sync.synced_at > first_sync.synced_at,
            "second sync timestamp must be strictly later"
        );
    }

    // ── sync_user_role_assignments_by_provider ─────────────────────────────

    #[sqlx::test]
    async fn user_assignments_initial_sync(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let provider = RoleProviderId::new_unchecked("oidc");
        let r1_ident = Arc::new(RoleIdent::new_unchecked("oidc", "admin"));
        let r2_ident = Arc::new(RoleIdent::new_unchecked("oidc", "viewer"));
        let user_id = Arc::new(UserId::new_unchecked("oidc", "alice"));
        let user = make_user(&user_id, "Alice");

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let result = sync_user_role_assignments_by_provider(
            &user,
            &project_id,
            &provider,
            ur(&[
                make_role(&r1_ident, "Admin"),
                make_role(&r2_ident, "Viewer"),
            ]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        assert_eq!(result.added.len(), 2);
        assert_eq!(result.removed.len(), 0);
        // all_roles reflects the complete post-sync state (same as list result).
        assert_eq!(
            result.all_roles.len(),
            2,
            "all_roles must contain both assigned roles"
        );
        assert_eq!(
            result.provider_sync_times.len(),
            1,
            "one sync record for the provider"
        );
        assert_eq!(result.provider_sync_times[0].provider_id, provider);

        let list = list_role_assignments_for_user(&user_id, &pool)
            .await
            .unwrap();
        assert_eq!(list.roles.len(), 2);
        assert_eq!(list.provider_sync_times.len(), 1);
        assert_eq!(list.provider_sync_times[0].provider_id, provider);
    }

    #[sqlx::test]
    async fn user_assignments_incremental_add_and_remove(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let provider = RoleProviderId::new_unchecked("oidc");
        let r1_ident = Arc::new(RoleIdent::new_unchecked("oidc", "admin"));
        let r2_ident = Arc::new(RoleIdent::new_unchecked("oidc", "viewer"));
        let r3_ident = Arc::new(RoleIdent::new_unchecked("oidc", "editor"));
        let user_id = Arc::new(UserId::new_unchecked("oidc", "alice"));
        let user = make_user(&user_id, "Alice");

        // First sync: [r1, r2]
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let first = sync_user_role_assignments_by_provider(
            &user,
            &project_id,
            &provider,
            ur(&[
                make_role(&r1_ident, "Admin"),
                make_role(&r2_ident, "Viewer"),
            ]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();
        let added_first: HashSet<RoleId> = first.added.iter().copied().collect();

        // Second sync: [r2, r3] — removes r1, adds r3
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let result = sync_user_role_assignments_by_provider(
            &user,
            &project_id,
            &provider,
            ur(&[
                make_role(&r2_ident, "Viewer"),
                make_role(&r3_ident, "Editor"),
            ]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        assert_eq!(result.added.len(), 1);
        assert_eq!(result.removed.len(), 1);
        assert!(
            added_first.contains(&result.removed[0]),
            "removed ID must be from first sync"
        );
        // all_roles reflects the complete post-sync assignment list.
        assert_eq!(result.all_roles.len(), 2, "viewer + editor");
        let result_src_ids: HashSet<&str> = result
            .all_roles
            .iter()
            .map(|r| r.role_ident.source_id().as_str())
            .collect();
        assert!(result_src_ids.contains("viewer"));
        assert!(result_src_ids.contains("editor"));
        assert!(!result_src_ids.contains("admin"));

        let list = list_role_assignments_for_user(&user_id, &pool)
            .await
            .unwrap();
        assert_eq!(list.roles.len(), 2);
        let source_ids: HashSet<&str> = list
            .roles
            .iter()
            .map(|r| r.role_ident.source_id().as_str())
            .collect();
        assert!(source_ids.contains("viewer"));
        assert!(source_ids.contains("editor"));
        assert!(!source_ids.contains("admin"));
    }

    #[sqlx::test]
    async fn user_assignments_empty_removes_all(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let provider = RoleProviderId::new_unchecked("oidc");
        let r1_ident = Arc::new(RoleIdent::new_unchecked("oidc", "admin"));
        let user_id = Arc::new(UserId::new_unchecked("oidc", "alice"));
        let user = make_user(&user_id, "Alice");

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        sync_user_role_assignments_by_provider(
            &user,
            &project_id,
            &provider,
            ur(&[make_role(&r1_ident, "Admin")]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let result = sync_user_role_assignments_by_provider(
            &user,
            &project_id,
            &provider,
            ur(&[]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        assert_eq!(result.added.len(), 0);
        assert_eq!(result.removed.len(), 1);
        assert_eq!(result.all_roles.len(), 0, "all role assignments removed");
        // Even after all assignments are removed, the provider sync record must
        // be retrievable so callers know "we synced and found nothing".
        assert_eq!(
            result.provider_sync_times.len(),
            1,
            "sync record must still be surfaced after clearing all assignments"
        );
        assert_eq!(result.provider_sync_times[0].provider_id, provider);

        let list = list_role_assignments_for_user(&user_id, &pool)
            .await
            .unwrap();
        assert_eq!(list.roles.len(), 0);
        assert_eq!(
            list.provider_sync_times.len(),
            1,
            "list must surface the sync record even when the user has no roles"
        );
        assert_eq!(list.provider_sync_times[0].provider_id, provider);
    }

    #[sqlx::test]
    async fn user_assignments_isolates_by_provider(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let provider_a = RoleProviderId::new_unchecked("ldap");
        let provider_b = RoleProviderId::new_unchecked("oidc");
        let ra_ident = Arc::new(RoleIdent::new_unchecked("ldap", "group-a"));
        let rb_ident = Arc::new(RoleIdent::new_unchecked("oidc", "group-b"));
        let user_id = Arc::new(UserId::new_unchecked("oidc", "alice"));
        let user = make_user(&user_id, "Alice");

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        sync_user_role_assignments_by_provider(
            &user,
            &project_id,
            &provider_a,
            ur(&[make_role(&ra_ident, "Group A")]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        sync_user_role_assignments_by_provider(
            &user,
            &project_id,
            &provider_b,
            ur(&[make_role(&rb_ident, "Group B")]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        // Sync provider_a with [] — only provider_a's role removed
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let result = sync_user_role_assignments_by_provider(
            &user,
            &project_id,
            &provider_a,
            ur(&[]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        assert_eq!(result.removed.len(), 1);
        assert_eq!(result.added.len(), 0);
        assert_eq!(
            result.all_roles.len(),
            1,
            "only provider_b's role remains in all_roles"
        );
        assert_eq!(
            result.all_roles[0].role_ident.provider_id(),
            &provider_b,
            "surviving role belongs to provider_b"
        );

        let list = list_role_assignments_for_user(&user_id, &pool)
            .await
            .unwrap();
        assert_eq!(list.roles.len(), 1, "provider_b's role survives");
        assert_eq!(list.roles[0].role_ident.provider_id(), &provider_b);
    }

    #[sqlx::test]
    async fn user_assignments_isolates_by_project(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let p1 = make_project(&state).await;
        let p2 = make_project(&state).await;
        let provider = RoleProviderId::new_unchecked("ldap");
        let r1_ident = Arc::new(RoleIdent::new_unchecked("ldap", "group-1"));
        let r2_ident = Arc::new(RoleIdent::new_unchecked("ldap", "group-2"));
        let user_id = Arc::new(UserId::new_unchecked("oidc", "alice"));
        let user = make_user(&user_id, "Alice");

        for (proj, role_ident, role_name) in
            [(&p1, &r1_ident, "Group 1"), (&p2, &r2_ident, "Group 2")]
        {
            let mut t = PostgresTransaction::begin_write(state.clone())
                .await
                .unwrap();
            sync_user_role_assignments_by_provider(
                &user,
                proj,
                &provider,
                ur(&[make_role(role_ident, role_name)]),
                t.transaction(),
            )
            .await
            .unwrap();
            t.commit().await.unwrap();
        }

        // Sync p1 with [] — only p1's role removed
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let result =
            sync_user_role_assignments_by_provider(&user, &p1, &provider, ur(&[]), t.transaction())
                .await
                .unwrap();
        t.commit().await.unwrap();

        assert_eq!(result.removed.len(), 1);

        let list = list_role_assignments_for_user(&user_id, &pool)
            .await
            .unwrap();
        assert_eq!(list.roles.len(), 1, "p2's role survives");
        assert_eq!(*list.roles[0].project_id, p2);
    }

    #[sqlx::test]
    async fn user_assignments_empty_from_start(pool: sqlx::PgPool) {
        // First sync with no roles — assignment_changes returns 0 rows; tests
        // that added_ids! / removed_ids! / synced_at! are all non-null.
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let provider = RoleProviderId::new_unchecked("oidc");
        let user_id = Arc::new(UserId::new_unchecked("oidc", "alice"));
        let user = make_user(&user_id, "Alice");

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let result = sync_user_role_assignments_by_provider(
            &user,
            &project_id,
            &provider,
            ur(&[]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        assert_eq!(result.added.len(), 0);
        assert_eq!(result.removed.len(), 0);
        assert_eq!(result.all_roles.len(), 0);
        // sync_ts always writes a record even for an empty sync; the provider
        // sync time must be surfaced even when there are no role assignments.
        assert_eq!(
            result.provider_sync_times.len(),
            1,
            "sync record must be surfaced even when there are no role assignments"
        );
        assert_eq!(result.provider_sync_times[0].provider_id, provider);

        let list = list_role_assignments_for_user(&user_id, &pool)
            .await
            .unwrap();
        assert_eq!(list.roles.len(), 0);
        assert_eq!(
            list.provider_sync_times.len(),
            1,
            "list must also surface the sync record for the provider"
        );
        assert_eq!(list.provider_sync_times[0].provider_id, provider);
    }

    #[sqlx::test]
    async fn user_assignments_empty_twice(pool: sqlx::PgPool) {
        // Two consecutive empty syncs — second sync has 0 rows in assignment_changes.
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let provider = RoleProviderId::new_unchecked("oidc");
        let r1_ident = Arc::new(RoleIdent::new_unchecked("oidc", "admin"));
        let user_id = Arc::new(UserId::new_unchecked("oidc", "alice"));
        let user = make_user(&user_id, "Alice");

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        sync_user_role_assignments_by_provider(
            &user,
            &project_id,
            &provider,
            ur(&[make_role(&r1_ident, "Admin")]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        // First clear
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let first_clear = sync_user_role_assignments_by_provider(
            &user,
            &project_id,
            &provider,
            ur(&[]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        assert_eq!(first_clear.added.len(), 0);
        assert_eq!(
            first_clear.removed.len(),
            1,
            "admin role removed on first clear"
        );

        let list = list_role_assignments_for_user(&user_id, &pool)
            .await
            .unwrap();
        assert_eq!(list.roles.len(), 0, "no roles after first clear");

        // Second clear — assignment_changes produces 0 rows
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let result = sync_user_role_assignments_by_provider(
            &user,
            &project_id,
            &provider,
            ur(&[]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        assert_eq!(result.added.len(), 0);
        assert_eq!(result.removed.len(), 0);
        // sync_ts must be written even on a no-op empty sync, advancing the timestamp.
        assert!(
            result.synced_at > first_clear.synced_at,
            "synced_at must strictly advance on each sync: first={:?} second={:?}",
            first_clear.synced_at,
            result.synced_at,
        );

        // The provider sync record must be retrievable even when the user has
        // no role assignments — this is the core invariant being tested.
        let list = list_role_assignments_for_user(&user_id, &pool)
            .await
            .unwrap();
        assert_eq!(list.roles.len(), 0, "still no roles after second clear");
        assert_eq!(
            list.provider_sync_times.len(),
            1,
            "provider sync record must be retrievable even with no role assignments"
        );
        assert_eq!(list.provider_sync_times[0].provider_id, provider);
        assert_eq!(
            list.provider_sync_times[0].synced_at, result.synced_at,
            "retrieved synced_at must match the value returned by the sync call"
        );
    }

    /// When the role's name and description are unchanged, the role row must
    /// not be written (no `updated_at` bump).
    #[sqlx::test]
    async fn user_assignments_no_write_when_role_unchanged(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let provider = RoleProviderId::new_unchecked("oidc");
        let r1_ident = Arc::new(RoleIdent::new_unchecked("oidc", "stable-role"));
        let user_id = Arc::new(UserId::new_unchecked("oidc", "alice"));
        let user = make_user(&user_id, "Alice");

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let first = sync_user_role_assignments_by_provider(
            &user,
            &project_id,
            &provider,
            ur(&[make_role(&r1_ident, "Stable Role")]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        let role_id = *first.added[0];
        let version_after_first: i64 =
            sqlx::query_scalar!(r#"SELECT version FROM "role" WHERE id = $1"#, role_id,)
                .fetch_one(&pool)
                .await
                .unwrap();

        // Identical re-sync.
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        sync_user_role_assignments_by_provider(
            &user,
            &project_id,
            &provider,
            ur(&[make_role(&r1_ident, "Stable Role")]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        let version_after_second: i64 =
            sqlx::query_scalar!(r#"SELECT version FROM "role" WHERE id = $1"#, role_id,)
                .fetch_one(&pool)
                .await
                .unwrap();

        assert_eq!(
            version_after_first, version_after_second,
            "role row must not be written when nothing changed"
        );
    }

    /// Counter-test: when the role name changes, `updated_at` MUST advance.
    #[sqlx::test]
    async fn user_assignments_write_when_role_name_changes(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let provider = RoleProviderId::new_unchecked("oidc");
        let r1_ident = Arc::new(RoleIdent::new_unchecked("oidc", "changing-role"));
        let user_id = Arc::new(UserId::new_unchecked("oidc", "alice"));
        let user = make_user(&user_id, "Alice");

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let first = sync_user_role_assignments_by_provider(
            &user,
            &project_id,
            &provider,
            ur(&[make_role(&r1_ident, "Original Name")]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        let role_id = *first.added[0];
        let version_before: i64 =
            sqlx::query_scalar!(r#"SELECT version FROM "role" WHERE id = $1"#, role_id,)
                .fetch_one(&pool)
                .await
                .unwrap();

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        sync_user_role_assignments_by_provider(
            &user,
            &project_id,
            &provider,
            ur(&[make_role(&r1_ident, "New Name")]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        let version_after: i64 =
            sqlx::query_scalar!(r#"SELECT version FROM "role" WHERE id = $1"#, role_id,)
                .fetch_one(&pool)
                .await
                .unwrap();

        assert!(
            version_after > version_before,
            "role row must be written when name changes"
        );
    }

    // ── all_roles / provider_sync_times cross-provider ─────────────────────

    /// `all_roles` must include roles from *all* providers, not just the one
    /// being synced.  This is the core invariant that lets the trait's default
    /// impl build `ListUserRoleAssignmentsResult` without an extra DB query.
    #[sqlx::test]
    async fn user_assignments_all_roles_across_providers(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let provider_a = RoleProviderId::new_unchecked("ldap");
        let provider_b = RoleProviderId::new_unchecked("oidc");
        let ra_ident = Arc::new(RoleIdent::new_unchecked("ldap", "group-a"));
        let rb_ident = Arc::new(RoleIdent::new_unchecked("oidc", "group-b"));
        let user_id = Arc::new(UserId::new_unchecked("oidc", "alice"));
        let user = make_user(&user_id, "Alice");

        // Sync provider_a first.
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        sync_user_role_assignments_by_provider(
            &user,
            &project_id,
            &provider_a,
            ur(&[make_role(&ra_ident, "Group A")]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        // Sync provider_b — all_roles must include provider_a's role.
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let result = sync_user_role_assignments_by_provider(
            &user,
            &project_id,
            &provider_b,
            ur(&[make_role(&rb_ident, "Group B")]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        assert_eq!(result.all_roles.len(), 2, "roles from both providers");
        let src_ids: HashSet<&str> = result
            .all_roles
            .iter()
            .map(|r| r.role_ident.source_id().as_str())
            .collect();
        assert!(src_ids.contains("group-a"), "provider_a role present");
        assert!(src_ids.contains("group-b"), "provider_b role present");

        let prov_ids: HashSet<&RoleProviderId> = result
            .provider_sync_times
            .iter()
            .map(|s| &s.provider_id)
            .collect();
        assert_eq!(prov_ids.len(), 2, "one sync record per provider");
        assert!(prov_ids.contains(&provider_a));
        assert!(prov_ids.contains(&provider_b));
    }

    /// After clearing `provider_a`'s roles, `all_roles` must still contain `provider_b`'s roles — clearing one provider must not affect others.
    #[sqlx::test]
    async fn user_assignments_all_roles_after_clear_one_provider(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let provider_a = RoleProviderId::new_unchecked("ldap");
        let provider_b = RoleProviderId::new_unchecked("oidc");
        let ra_ident = Arc::new(RoleIdent::new_unchecked("ldap", "group-a"));
        let rb_ident = Arc::new(RoleIdent::new_unchecked("oidc", "group-b"));
        let user_id = Arc::new(UserId::new_unchecked("oidc", "alice"));
        let user = make_user(&user_id, "Alice");

        for (prov, ident, name) in [
            (&provider_a, &ra_ident, "Group A"),
            (&provider_b, &rb_ident, "Group B"),
        ] {
            let mut t = PostgresTransaction::begin_write(state.clone())
                .await
                .unwrap();
            sync_user_role_assignments_by_provider(
                &user,
                &project_id,
                prov,
                ur(&[make_role(ident, name)]),
                t.transaction(),
            )
            .await
            .unwrap();
            t.commit().await.unwrap();
        }

        // Clear provider_a.
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let result = sync_user_role_assignments_by_provider(
            &user,
            &project_id,
            &provider_a,
            ur(&[]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        assert_eq!(result.removed.len(), 1);
        assert_eq!(result.all_roles.len(), 1, "only provider_b's role remains");
        assert_eq!(
            result.all_roles[0].role_ident.provider_id(),
            &provider_b,
            "surviving role is from provider_b"
        );
        // provider_a was synced with an empty list, provider_b still has an
        // active assignment — both sync records must be surfaced.
        assert_eq!(
            result.provider_sync_times.len(),
            2,
            "sync records for both providers must be surfaced"
        );
        let sync_prov_ids: HashSet<&RoleProviderId> = result
            .provider_sync_times
            .iter()
            .map(|s| &s.provider_id)
            .collect();
        assert!(
            sync_prov_ids.contains(&provider_a),
            "provider_a sync record present"
        );
        assert!(
            sync_prov_ids.contains(&provider_b),
            "provider_b sync record present"
        );
    }

    // ── cache invalidation on membership edge change ───────────────────────

    /// Adding an edge `(parent, child)` must invalidate the cached effective
    /// roles of every user reachable through `child`'s descendant closure —
    /// here the user assigned directly to `child`.
    ///
    /// The `USER_ASSIGNMENTS_CACHE` lives in the `lakekeeper` crate behind a
    /// `pub(crate)` module, so it cannot be poked directly from this crate.
    /// Instead the invalidation is observed through the public cached read path
    /// `list_role_assignments_for_user`:
    ///   * Warming it returns an `Arc` that is then stored in the cache.
    ///   * If the entry were NOT invalidated, the second call would be a cache
    ///     HIT and return the very same `Arc` (pointer-equal) with the stale
    ///     `{child}` role set.
    ///   * Because the edge change invalidated the entry, the second call
    ///     re-fetches from the DB, yielding a fresh, pointer-distinct `Arc`
    ///     whose effective set has grown to `{child, parent}`.
    #[sqlx::test]
    async fn edge_change_invalidates_descendant_member_users(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = make_project(&state).await;
        let parent = create_managed_role(&state, &project_id, "parent").await;
        let child = create_managed_role(&state, &project_id, "child").await;
        let arc_project_id: ArcProjectId = Arc::new(project_id.clone());

        // Assign user U directly to `child` only (matches the existing
        // lakekeeper-managed `child` role created above).
        let child_ident = Arc::new(RoleIdent::new_unchecked("lakekeeper", "child"));
        let child_role = make_role(&child_ident, "child");
        let user_id = Arc::new(UserId::new_unchecked("oidc", "edge-invalidation-user"));
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let sync = sync_role_members_by_ident(
            &project_id,
            &child_role,
            um(&[make_user(&user_id, "Edge User")]),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();
        assert_eq!(sync.added.len(), 1, "exactly one member added to child");

        // Warm the USER_ASSIGNMENTS_CACHE via the cached read path. Before the
        // edge exists the effective set is exactly {child}.
        let warmed = PostgresBackend::list_role_assignments_for_user(&user_id, state.clone())
            .await
            .unwrap();
        let warmed_set: HashSet<RoleId> = warmed.roles.iter().map(|r| r.role_id).collect();
        assert_eq!(
            warmed_set,
            HashSet::from([child]),
            "before the edge, U's effective roles are exactly {{child}}"
        );

        // Add edge (parent has member child) and invalidate. child is in the
        // descendant closure of itself, U is assigned to child → U is stale.
        PostgresBackend::add_role_members_and_invalidate(
            &arc_project_id,
            parent,
            &[child],
            state.clone(),
        )
        .await
        .unwrap();

        // Second read: the entry was invalidated, so this is a cache MISS that
        // re-fetches the DB. The Arc must be pointer-distinct from the warmed
        // one, proving the stale entry was evicted rather than served.
        let after = PostgresBackend::list_role_assignments_for_user(&user_id, state.clone())
            .await
            .unwrap();
        assert!(
            !Arc::ptr_eq(&warmed, &after),
            "cache entry must have been invalidated (fresh Arc), not served stale"
        );
        let after_set: HashSet<RoleId> = after.roles.iter().map(|r| r.role_id).collect();
        assert_eq!(
            after_set,
            HashSet::from([child, parent]),
            "after the edge, U transitively gains `parent`"
        );
    }
}
