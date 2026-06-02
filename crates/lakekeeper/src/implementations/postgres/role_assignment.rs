use std::{collections::HashSet, sync::Arc};

use uuid::Uuid;

use super::{
    dbutils::DBErrorHandler,
    user::{DbUserLastUpdatedWith, DbUserType},
};
use crate::{
    ProjectId,
    service::{
        ArcRoleIdent, AssignedRole, AssignedUser, CatalogBackendError, CatalogRoleForAssignment,
        CatalogUserRoleAssignmentUser, DatabaseIntegrityError, ListRoleMembersResult,
        ListUserRoleAssignmentsResult, RoleId, RoleIdent, RoleNameAlreadyExists, RoleProviderId,
        SyncRoleMembersError, SyncRoleMembersResult, SyncUserRoleAssignmentsError,
        SyncUserRoleAssignmentsResult, UniqueMembers, UniqueRoles, UserProviderSyncInfo,
        authn::UserId,
    },
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
        WITH assigned AS (
            SELECT
                r.id          AS role_id,
                r.source_id,
                r.provider_id,
                r.project_id,
                s.project_id  AS sync_project_id,
                s.provider_id AS sync_provider_id,
                s.synced_at
            FROM role_assignment ur
            JOIN "role" r ON r.id = ur.role_id
            LEFT JOIN role_assignment_sync s
                ON  s.user_id     = ur.user_id
                AND s.provider_id = r.provider_id
                AND s.project_id  = r.project_id
            WHERE ur.user_id = $1
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

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ProjectId,
        api::management::v1::user::{UserLastUpdatedWith, UserType},
        implementations::postgres::{CatalogState, PostgresBackend, PostgresTransaction},
        service::{
            CatalogRoleForAssignment, CatalogStore, CatalogUserRoleAssignmentUser, RoleIdent,
            RoleProviderId, Transaction, UniqueMembers, UniqueRoles,
            authn::{UserId, UserIdRef},
        },
    };

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
}
