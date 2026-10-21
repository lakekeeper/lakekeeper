use std::{collections::HashSet, ops::Deref, sync::Arc};

use iceberg::spec::FormatVersion;
use lakekeeper::{
    CONFIG, ProjectId, SecretId, WarehouseId,
    api::{
        ErrorModel,
        iceberg::v1::PaginationQuery,
        management::v1::{
            DeleteWarehouseQuery,
            warehouse::{TabularDeleteProfile, WarehouseStatistics, WarehouseStatisticsResponse},
        },
    },
    service::{
        AllowedFormatVersions, CatalogCreateWarehouseError, CatalogCreateWarehouseRequest,
        CatalogDeleteWarehouseError, CatalogGetWarehouseByIdError, CatalogGetWarehouseByNameError,
        CatalogListWarehousesError, CatalogRenameWarehouseError, DatabaseIntegrityError,
        DeletedWarehouse, EnsureWarehouseSpecMutableError, GetProjectResponse, ManagedBy,
        OnRoleConflict, ProjectIdNotFoundError, RenamedWarehouse, ResolvedWarehouse,
        RollbackCompactionPolicy, SetWarehouseDeletionProfileError,
        SetWarehouseFormatVersionPolicyError, SetWarehouseManagedByError,
        SetWarehouseProtectedError, SetWarehouseRollbackCompactionPolicyError,
        SetWarehouseStatusError, StorageProfileSerializationError,
        UpdateWarehouseStorageProfileError, UpdatedWarehouseStorage, UserId,
        WarehouseAlreadyExists, WarehouseFormatVersionPolicy, WarehouseHasUnfinishedTasks,
        WarehouseIdAlreadyExists, WarehouseIdNotFound, WarehouseNotEmpty, WarehouseProtected,
        WarehouseSpecLocked, WarehouseStatus, WarehouseVersion, registered_system_roles,
        storage::StorageProfile, system_role_requests,
    },
};
use sqlx::{PgPool, types::Json};

use super::CatalogState;
use crate::{
    dbutils::{DBErrorHandler, user_id_from_db},
    pagination::{PaginateToken, V1PaginateToken},
};

pub(super) async fn set_warehouse_deletion_profile<
    'c,
    'e: 'c,
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
>(
    warehouse_id: WarehouseId,
    deletion_profile: &TabularDeleteProfile,
    connection: E,
) -> Result<ResolvedWarehouse, SetWarehouseDeletionProfileError> {
    let num_secs = deletion_profile
        .expiration_seconds()
        .map(|dur| dur.num_seconds());
    let prof = DbTabularDeleteProfile::from(*deletion_profile);

    let row_count = sqlx::query_as!(
        WarehouseRecord,
        r#"
            UPDATE warehouse
            SET tabular_expiration_seconds = $1, tabular_delete_mode = $2
            WHERE warehouse_id = $3
            AND status = 'active'
            RETURNING 
                project_id,
                warehouse_id,
                warehouse_name,
                storage_profile as "storage_profile: Json<StorageProfile>",
                storage_secret_id,
                status AS "status: WarehouseStatus",
                tabular_delete_mode as "tabular_delete_mode: DbTabularDeleteProfile",
                tabular_expiration_seconds,
                protected,
                allowed_format_versions,
                default_format_version,
                rollback_compaction_on_conflict,
                rollback_compaction_max_age_seconds,
                rollback_compaction_fence_seconds,
                managed_by as "managed_by: ManagedBy",
                updated_at,
                version
            "#,
        num_secs,
        prof as _,
        *warehouse_id
    )
    .fetch_optional(connection)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;

    let Some(warehouse) = row_count else {
        return Err(WarehouseIdNotFound::new(warehouse_id).into());
    };

    Ok(warehouse.try_into()?)
}

pub(crate) async fn create_warehouse(
    project_id: &ProjectId,
    request: CatalogCreateWarehouseRequest,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<ResolvedWarehouse, CatalogCreateWarehouseError> {
    let CatalogCreateWarehouseRequest {
        warehouse_name,
        warehouse_id,
        storage_profile,
        storage_secret_id,
        delete_profile: tabular_delete_profile,
        format_version_policy,
        managed_by,
    } = request;
    let storage_profile_ser =
        serde_json::to_value(storage_profile).map_err(StorageProfileSerializationError::from)?;

    let num_secs = tabular_delete_profile
        .expiration_seconds()
        .map(|dur| dur.num_seconds());
    let prof = DbTabularDeleteProfile::from(tabular_delete_profile);

    let allowed_format_versions_db =
        format_version_versions_to_db(&format_version_policy.allowed_format_versions);
    let default_format_version_db = format_version_policy
        .default_format_version
        .map(format_version_to_db);

    let warehouse = sqlx::query_as!(
        WarehouseRecord,
        r#"WITH
            whi AS (INSERT INTO warehouse (
                                   warehouse_id,
                                   warehouse_name,
                                   project_id,
                                   storage_profile,
                                   storage_secret_id,
                                   status,
                                   tabular_expiration_seconds,
                                   tabular_delete_mode,
                                   allowed_format_versions,
                                   default_format_version,
                                   managed_by)
                                -- COALESCE keeps the column's own default as the
                                -- fallback for callers that do not pick an id.
                                VALUES (COALESCE($10, uuid_generate_v1mc()), $1, $2, $3, $4, 'active', $5, $6, $7, $8, $9)
                                RETURNING
                                    project_id,
                                    warehouse_id,
                                    warehouse_name,
                                    storage_profile as "storage_profile: Json<StorageProfile>",
                                    storage_secret_id,
                                    status AS "status: WarehouseStatus",
                                    tabular_delete_mode as "tabular_delete_mode: DbTabularDeleteProfile",
                                    tabular_expiration_seconds,
                                    protected,
                                    allowed_format_versions,
                                    default_format_version,
                                    rollback_compaction_on_conflict,
                                    rollback_compaction_max_age_seconds,
                                    rollback_compaction_fence_seconds,
                                    managed_by as "managed_by: ManagedBy",
                                    updated_at,
                                    version),
            whs AS (INSERT INTO warehouse_statistics (number_of_views,
                                                      number_of_tables,
                                                      warehouse_id)
                     VALUES (0, 0, (SELECT warehouse_id FROM whi)))
            SELECT
                *
            FROM whi"#,
        warehouse_name,
        project_id,
        storage_profile_ser,
        storage_secret_id.map(|id| id.into_uuid()),
        num_secs,
        prof as _,
        &allowed_format_versions_db,
        default_format_version_db,
        managed_by as ManagedBy,
        warehouse_id.map(|id| *id)
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| match &e {
        sqlx::Error::Database(db_err) => match db_err.constraint() {
            // ToDo: Get constraint name from const
            Some("unique_warehouse_name_in_project") => CatalogCreateWarehouseError::from(
                WarehouseAlreadyExists::new(warehouse_name, project_id.clone()),
            ),
            // Reachable only for a caller-supplied id: the column default cannot
            // collide in practice.
            Some("warehouse_pkey") => warehouse_id
                .map_or_else(
                    || e.into_catalog_backend_error().into(),
                    |id| CatalogCreateWarehouseError::from(WarehouseIdAlreadyExists::new(id)),
                ),
            Some("warehouse_project_id_fk") => {
                ProjectIdNotFoundError::new(project_id.clone()).into()
            }
            _ => e.into_catalog_backend_error().into(),
        },
        _ => e.into_catalog_backend_error().into(),
    })?;

    Ok(warehouse.try_into()?)
}

pub(crate) async fn rename_project(
    project_id: &ProjectId,
    new_name: &str,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> lakekeeper::service::Result<()> {
    let row_count = sqlx::query!(
        "UPDATE project
            SET project_name = $1
            WHERE project_id = $2",
        new_name,
        project_id
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| e.into_error_model("Error renaming project"))?
    .rows_affected();

    if row_count == 0 {
        return Err(ErrorModel::not_found("Project not found", "ProjectNotFound", None).into());
    }

    Ok(())
}

// `'static` on the inner Transaction lifetime is required so the call to
// `PostgresBackend::upsert_system_roles` below matches the trait's
// `Transaction<'_>` GAT, which for `PostgresBackend` resolves to
// `&mut sqlx::Transaction<'static, sqlx::Postgres>`. The only caller
// (`<PostgresBackend as CatalogStore>::create_project` in catalog.rs)
// already passes a `'static`-conn transaction, so this tightening is a
// no-op at every call site.
pub async fn create_project(
    project_id: &ProjectId,
    project_name: String,
    transaction: &mut sqlx::Transaction<'static, sqlx::Postgres>,
) -> lakekeeper::service::Result<()> {
    let Some(_project_id) = sqlx::query_scalar!(
        r#"
        INSERT INTO project (project_name, project_id)
        VALUES ($1, $2)
        ON CONFLICT DO NOTHING
        RETURNING project_id
        "#,
        project_name,
        project_id
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| e.into_error_model("Error creating Project"))?
    else {
        return Err(ErrorModel::conflict(
            "Project with this id already exists",
            "ProjectIdAlreadyExists",
            None,
        )
        .into());
    };

    // Seed system roles from the process-wide registry, if any. Empty in
    // default OSS (no extension registered). Atomic with the project
    // insert, and built from the same requests as the post-migration backfill.
    // The project is new, so no instance caches its roles.
    let specs = registered_system_roles();
    if !specs.is_empty() {
        let seeded = async {
            crate::role::create_roles(
                project_id,
                system_role_requests(specs)?,
                OnRoleConflict::UpdateMetadata,
                &mut **transaction,
            )
            .await
        }
        .await;
        seeded.map_err(|e| {
            ErrorModel::internal(
                "Failed to seed registered system roles",
                "SystemRoleSeedFailed",
                Some(Box::new(e)),
            )
        })?;
    }

    Ok(())
}

pub(crate) async fn get_project(
    project_id: &ProjectId,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> lakekeeper::service::Result<Option<GetProjectResponse>> {
    let project = sqlx::query!(
        r#"
        SELECT
            project_name,
            project_id
        FROM project
        WHERE project_id = $1
        "#,
        project_id
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| {
        ErrorModel::internal(
            "Error fetching project",
            "ProjectFetchError",
            Some(Box::new(e)),
        )
    })?;

    if let Some(project) = project {
        Ok(Some(GetProjectResponse {
            project_id: Arc::new(ProjectId::from_db_unchecked(project.project_id)),
            name: project.project_name,
        }))
    } else {
        Ok(None)
    }
}

/// Deletes a project and returns the users whose role-provider sync records the delete
/// removed.
///
/// Locks the project row first: a sync record insert checks its project row with a
/// key-share lock, so the read after this lock sees every record a concurrent sync
/// committed while this waited, and none can follow.
pub(crate) async fn delete_project(
    project_id: &ProjectId,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> lakekeeper::service::Result<Vec<UserId>> {
    // Delete schedules before the project row is locked: the materializer holds a schedule row
    // while its task insert waits for the project row.
    sqlx::query!(
        "DELETE FROM task_schedule WHERE project_id = $1",
        project_id
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| e.into_error_model("Error deleting project task schedules"))?;

    let locked = sqlx::query_scalar!(
        r#"SELECT 1 AS "locked!" FROM project WHERE project_id = $1 FOR UPDATE"#,
        project_id
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| e.into_error_model("Error deleting project"))?;
    if locked.is_none() {
        return Err(ErrorModel::not_found("Project not found", "ProjectNotFound", None).into());
    }

    let synced_users = sqlx::query_scalar!(
        r#"SELECT DISTINCT user_id FROM role_assignment_sync WHERE project_id = $1"#,
        project_id
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(|e| e.into_error_model("Error deleting project"))?;

    sqlx::query!(r#"DELETE FROM project WHERE project_id = $1"#, project_id)
        .execute(&mut **transaction)
        .await
        .map_err(|e| match &e {
            sqlx::Error::Database(db_error) => {
                if db_error.is_foreign_key_violation() {
                    ErrorModel::conflict(
                        "Project is not empty",
                        "ProjectNotEmpty",
                        Some(Box::new(e)),
                    )
                } else {
                    e.into_error_model("Error deleting project")
                }
            }
            _ => e.into_error_model("Error deleting project"),
        })?;

    synced_users
        .iter()
        .map(|user_id| user_id_from_db(user_id).map_err(|e| ErrorModel::from(e).into()))
        .collect()
}

#[derive(sqlx::FromRow, Debug, PartialEq)]
struct WarehouseRecord {
    project_id: String,
    warehouse_id: uuid::Uuid,
    warehouse_name: String,
    storage_profile: Json<StorageProfile>,
    storage_secret_id: Option<uuid::Uuid>,
    status: WarehouseStatus,
    tabular_delete_mode: DbTabularDeleteProfile,
    tabular_expiration_seconds: Option<i64>,
    protected: bool,
    managed_by: ManagedBy,
    allowed_format_versions: Vec<i16>,
    default_format_version: Option<i16>,
    rollback_compaction_on_conflict: bool,
    rollback_compaction_max_age_seconds: i64,
    rollback_compaction_fence_seconds: i64,
    updated_at: Option<chrono::DateTime<chrono::Utc>>,
    version: i64,
}

impl TryFrom<WarehouseRecord> for ResolvedWarehouse {
    type Error = DatabaseIntegrityError;

    fn try_from(value: WarehouseRecord) -> Result<Self, Self::Error> {
        let tabular_delete_profile = db_to_api_tabular_delete_profile(
            value.tabular_delete_mode,
            value.tabular_expiration_seconds,
        )?;

        let allowed_format_versions = db_to_allowed_format_versions(value.allowed_format_versions)?;
        let default_format_version = value
            .default_format_version
            .map(format_version_from_db)
            .transpose()?;

        Ok(ResolvedWarehouse {
            warehouse_id: value.warehouse_id.into(),
            name: value.warehouse_name,
            project_id: Arc::new(ProjectId::from_db_unchecked(value.project_id)),
            storage_profile: value.storage_profile.deref().clone(),
            storage_secret_id: value.storage_secret_id.map(Into::into),
            status: value.status,
            tabular_delete_profile,
            protected: value.protected,
            managed_by: value.managed_by,
            allowed_format_versions,
            default_format_version,
            rollback_compaction_policy: RollbackCompactionPolicy {
                enabled: value.rollback_compaction_on_conflict,
                max_compaction_age_seconds: u64::try_from(
                    value.rollback_compaction_max_age_seconds,
                )
                .map_err(|_| {
                    DatabaseIntegrityError::new(format!(
                        "Invalid rollback_compaction_max_age_seconds '{}' stored for warehouse.",
                        value.rollback_compaction_max_age_seconds
                    ))
                })?,
                snapshot_removal_fence_seconds: u64::try_from(
                    value.rollback_compaction_fence_seconds,
                )
                .map_err(|_| {
                    DatabaseIntegrityError::new(format!(
                        "Invalid rollback_compaction_fence_seconds '{}' stored for warehouse.",
                        value.rollback_compaction_fence_seconds
                    ))
                })?,
            },
            updated_at: value.updated_at,
            version: WarehouseVersion::from(value.version),
        })
    }
}

pub(crate) async fn list_warehouses<
    'e,
    'c: 'e,
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
>(
    project_id: &ProjectId,
    include_status: Option<Vec<WarehouseStatus>>,
    catalog_state: E,
) -> Result<Vec<ResolvedWarehouse>, CatalogListWarehousesError> {
    let include_status = include_status.unwrap_or_else(|| vec![WarehouseStatus::Active]);
    let warehouses = sqlx::query_as!(
        WarehouseRecord,
        r#"
            SELECT 
                project_id,
                warehouse_id,
                warehouse_name,
                storage_profile as "storage_profile: Json<StorageProfile>",
                storage_secret_id,
                status AS "status: WarehouseStatus",
                tabular_delete_mode as "tabular_delete_mode: DbTabularDeleteProfile",
                tabular_expiration_seconds,
                protected,
                allowed_format_versions,
                default_format_version,
                rollback_compaction_on_conflict,
                rollback_compaction_max_age_seconds,
                rollback_compaction_fence_seconds,
                managed_by as "managed_by: ManagedBy",
                updated_at,
                version
            FROM warehouse
            WHERE project_id = $1
            AND status = ANY($2)
            "#,
        project_id,
        include_status as Vec<WarehouseStatus>
    )
    .fetch_all(catalog_state)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;

    warehouses
        .into_iter()
        .map(|warehouse| warehouse.try_into().map_err(Into::into))
        .collect()
}

pub(super) async fn get_warehouse_by_name(
    warehouse_name: &str,
    project_id: &ProjectId,
    catalog_state: CatalogState,
) -> Result<Option<ResolvedWarehouse>, CatalogGetWarehouseByNameError> {
    let warehouse = sqlx::query_as!(
        WarehouseRecord,
        r#"
        SELECT
            project_id,
            warehouse_id,
            warehouse_name,
            storage_profile as "storage_profile: Json<StorageProfile>",
            storage_secret_id,
            status AS "status: WarehouseStatus",
            tabular_delete_mode as "tabular_delete_mode: DbTabularDeleteProfile",
            tabular_expiration_seconds,
            protected,
            allowed_format_versions,
            default_format_version,
            rollback_compaction_on_conflict,
            rollback_compaction_max_age_seconds,
            rollback_compaction_fence_seconds,
            managed_by as "managed_by: ManagedBy",
            updated_at,
            version
        FROM warehouse
        WHERE warehouse_name = $1 AND project_id = $2
        "#,
        warehouse_name.to_string(),
        project_id
    )
    .fetch_optional(&catalog_state.read_pool())
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;

    if let Some(warehouse) = warehouse {
        Ok(Some(warehouse.try_into()?))
    } else {
        Ok(None)
    }
}

pub(crate) async fn get_warehouse_by_id<
    'e,
    'c: 'e,
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
>(
    warehouse_id: WarehouseId,
    catalog_state: E,
) -> Result<Option<ResolvedWarehouse>, CatalogGetWarehouseByIdError> {
    let warehouse = sqlx::query_as!(
        WarehouseRecord,
        r#"
        SELECT 
            project_id,
            warehouse_id,
            warehouse_name,
            storage_profile as "storage_profile: Json<StorageProfile>",
            storage_secret_id,
            status AS "status: WarehouseStatus",
            tabular_delete_mode as "tabular_delete_mode: DbTabularDeleteProfile",
            tabular_expiration_seconds,
            protected,
            allowed_format_versions,
            default_format_version,
            rollback_compaction_on_conflict,
            rollback_compaction_max_age_seconds,
            rollback_compaction_fence_seconds,
            managed_by as "managed_by: ManagedBy",
            updated_at,
            version
        FROM warehouse
        WHERE warehouse_id = $1
        "#,
        *warehouse_id
    )
    .fetch_optional(catalog_state)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;

    if let Some(warehouse) = warehouse {
        Ok(Some(warehouse.try_into()?))
    } else {
        Ok(None)
    }
}

pub(crate) async fn list_projects<'e, 'c: 'e, E: sqlx::Executor<'c, Database = sqlx::Postgres>>(
    project_ids: Option<HashSet<ProjectId>>,
    connection: E,
) -> lakekeeper::service::Result<Vec<GetProjectResponse>> {
    let return_all = project_ids.is_none();
    let projects = sqlx::query!(
        r#"
        SELECT project_id, project_name FROM project WHERE project_id = ANY($1) or $2
        "#,
        project_ids
            .map(|ids| ids.into_iter().map(|i| i.to_string()).collect::<Vec<_>>())
            .unwrap_or_default() as Vec<String>,
        return_all
    )
    .fetch_all(connection)
    .await
    .map_err(|e| e.into_error_model("Error fetching projects"))?;

    Ok(projects
        .into_iter()
        .map(|project| GetProjectResponse {
            project_id: Arc::new(ProjectId::from_db_unchecked(project.project_id)),
            name: project.project_name,
        })
        .collect())
}

pub(crate) async fn delete_warehouse(
    warehouse_id: WarehouseId,
    DeleteWarehouseQuery { force }: DeleteWarehouseQuery,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<DeletedWarehouse, CatalogDeleteWarehouseError> {
    let unfinished_task_counts_per_queue = sqlx::query!(
        r#"WITH active_tasks as (SELECT task_id, queue_name, status from task WHERE warehouse_id = $1)
            SELECT COUNT(task_id) as "task_count!", queue_name FROM active_tasks GROUP BY queue_name"#,
        *warehouse_id,
    ).fetch_all(&mut **transaction).await.map_err(|e| e.into_catalog_backend_error().append_detail("Error fetching active tasks for warehouse"))?;
    if !unfinished_task_counts_per_queue.is_empty() {
        let task_descriptions = unfinished_task_counts_per_queue
            .iter()
            .map(|row| format!("{} Tasks in queue '{}'", row.task_count, row.queue_name))
            .collect::<Vec<_>>()
            .join(", ");

        return Err(WarehouseHasUnfinishedTasks {
            stack: vec![format!("Unfinished tasks: {task_descriptions}")],
        }
        .into());
    }

    // The row stays locked until the transaction ends, so the protection checked here and
    // the project, name and secret the DELETE returns all describe the row it removes.
    let protected = sqlx::query_scalar!(
        "SELECT protected FROM warehouse WHERE warehouse_id = $1 FOR UPDATE",
        *warehouse_id
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?
    .ok_or_else(|| WarehouseIdNotFound::new(warehouse_id))?;
    if protected && !force {
        return Err(WarehouseProtected::new().into());
    }

    let deleted = sqlx::query!(
        r#"DELETE FROM warehouse WHERE warehouse_id = $1
           RETURNING project_id, warehouse_name, storage_secret_id"#,
        *warehouse_id
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| delete_warehouse_error(e, warehouse_id))?;

    Ok(DeletedWarehouse {
        project_id: ProjectId::from_db_unchecked(deleted.project_id),
        name: deleted.warehouse_name,
        storage_secret_id: deleted.storage_secret_id.map(Into::into),
    })
}

/// A task inserted after [`delete_warehouse`] checked for unfinished tasks fails the DELETE
/// on `task_warehouse_id_fk`, and gets the same error as that check.
fn delete_warehouse_error(
    e: sqlx::Error,
    warehouse_id: WarehouseId,
) -> CatalogDeleteWarehouseError {
    match &e {
        sqlx::Error::RowNotFound => WarehouseIdNotFound::new(warehouse_id).into(),
        sqlx::Error::Database(db_error) if db_error.is_foreign_key_violation() => {
            if db_error.constraint() == Some("task_warehouse_id_fk") {
                WarehouseHasUnfinishedTasks::new().into()
            } else {
                WarehouseNotEmpty::new().into()
            }
        }
        _ => e.into_catalog_backend_error().into(),
    }
}

pub(crate) async fn rename_warehouse(
    warehouse_id: WarehouseId,
    new_name: &str,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<RenamedWarehouse, CatalogRenameWarehouseError> {
    let previous_name = sqlx::query_scalar!(
        "SELECT warehouse_name FROM warehouse WHERE warehouse_id = $1 AND status = 'active' FOR UPDATE",
        *warehouse_id
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?
    .ok_or_else(|| WarehouseIdNotFound::new(warehouse_id))?;

    let warehouse = sqlx::query_as!(
        WarehouseRecord,
        r#"UPDATE warehouse
            SET warehouse_name = $1
            WHERE warehouse_id = $2
            AND status = 'active'
        RETURNING
            project_id,
            warehouse_id,
            warehouse_name,
            storage_profile as "storage_profile: Json<StorageProfile>",
            storage_secret_id,
            status AS "status: WarehouseStatus",
            tabular_delete_mode as "tabular_delete_mode: DbTabularDeleteProfile",
            tabular_expiration_seconds,
            protected,
            allowed_format_versions,
            default_format_version,
            rollback_compaction_on_conflict,
            rollback_compaction_max_age_seconds,
            rollback_compaction_fence_seconds,
            managed_by as "managed_by: ManagedBy",
            updated_at,
            version
        "#,
        new_name,
        *warehouse_id
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;

    let Some(warehouse) = warehouse else {
        return Err(WarehouseIdNotFound::new(warehouse_id).into());
    };

    Ok(RenamedWarehouse {
        warehouse: warehouse.try_into()?,
        previous_name,
    })
}

pub(crate) async fn set_warehouse_status(
    warehouse_id: WarehouseId,
    status: WarehouseStatus,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<ResolvedWarehouse, SetWarehouseStatusError> {
    let row_count = sqlx::query_as!(
        WarehouseRecord,
        r#"UPDATE warehouse
            SET status = $1
            WHERE warehouse_id = $2
            RETURNING                 
                project_id,
                warehouse_id,
                warehouse_name,
                storage_profile as "storage_profile: Json<StorageProfile>",
                storage_secret_id,
                status AS "status: WarehouseStatus",
                tabular_delete_mode as "tabular_delete_mode: DbTabularDeleteProfile",
                tabular_expiration_seconds,
                protected,
                allowed_format_versions,
                default_format_version,
                rollback_compaction_on_conflict,
                rollback_compaction_max_age_seconds,
                rollback_compaction_fence_seconds,
                managed_by as "managed_by: ManagedBy",
                updated_at,
                version
        "#,
        status as _,
        *warehouse_id
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;

    let Some(warehouse) = row_count else {
        return Err(WarehouseIdNotFound::new(warehouse_id).into());
    };

    Ok(warehouse.try_into()?)
}

pub(crate) async fn set_warehouse_protection(
    warehouse_id: WarehouseId,
    protected: bool,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<ResolvedWarehouse, SetWarehouseProtectedError> {
    let warehouse = sqlx::query_as!(
        WarehouseRecord,
        r#"UPDATE warehouse
            SET protected = $1
            WHERE warehouse_id = $2
            RETURNING 
                project_id,
                warehouse_id,
                warehouse_name,
                storage_profile as "storage_profile: Json<StorageProfile>",
                storage_secret_id,
                status AS "status: WarehouseStatus",
                tabular_delete_mode as "tabular_delete_mode: DbTabularDeleteProfile",
                tabular_expiration_seconds,
                protected,
                allowed_format_versions,
                default_format_version,
                rollback_compaction_on_conflict,
                rollback_compaction_max_age_seconds,
                rollback_compaction_fence_seconds,
                managed_by as "managed_by: ManagedBy",
                updated_at,
                version
            "#,
        protected,
        *warehouse_id
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;

    let Some(warehouse) = warehouse else {
        return Err(WarehouseIdNotFound::new(warehouse_id).into());
    };

    Ok(warehouse.try_into()?)
}

pub(crate) async fn set_warehouse_managed_by(
    warehouse_id: WarehouseId,
    managed_by: ManagedBy,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<ResolvedWarehouse, SetWarehouseManagedByError> {
    let warehouse = sqlx::query_as!(
        WarehouseRecord,
        r#"UPDATE warehouse
            SET managed_by = $1
            WHERE warehouse_id = $2
            RETURNING
                project_id,
                warehouse_id,
                warehouse_name,
                storage_profile as "storage_profile: Json<StorageProfile>",
                storage_secret_id,
                status AS "status: WarehouseStatus",
                tabular_delete_mode as "tabular_delete_mode: DbTabularDeleteProfile",
                tabular_expiration_seconds,
                protected,
                allowed_format_versions,
                default_format_version,
                rollback_compaction_on_conflict,
                rollback_compaction_max_age_seconds,
                rollback_compaction_fence_seconds,
                managed_by as "managed_by: ManagedBy",
                updated_at,
                version
            "#,
        managed_by as ManagedBy,
        *warehouse_id
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;

    let Some(warehouse) = warehouse else {
        return Err(WarehouseIdNotFound::new(warehouse_id).into());
    };

    Ok(warehouse.try_into()?)
}

pub(crate) async fn set_warehouse_rollback_compaction_policy(
    warehouse_id: WarehouseId,
    policy: RollbackCompactionPolicy,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<ResolvedWarehouse, SetWarehouseRollbackCompactionPolicyError> {
    let max_compaction_age_seconds =
        i64::try_from(policy.max_compaction_age_seconds).unwrap_or(i64::MAX);
    let fence_seconds = i64::try_from(policy.snapshot_removal_fence_seconds).unwrap_or(i64::MAX);
    let warehouse = sqlx::query_as!(
        WarehouseRecord,
        r#"UPDATE warehouse
            SET rollback_compaction_on_conflict = $1,
                rollback_compaction_max_age_seconds = $3,
                rollback_compaction_fence_seconds = $4
            WHERE warehouse_id = $2
            RETURNING
                project_id,
                warehouse_id,
                warehouse_name,
                storage_profile as "storage_profile: Json<StorageProfile>",
                storage_secret_id,
                status AS "status: WarehouseStatus",
                tabular_delete_mode as "tabular_delete_mode: DbTabularDeleteProfile",
                tabular_expiration_seconds,
                protected,
                allowed_format_versions,
                default_format_version,
                rollback_compaction_on_conflict,
                rollback_compaction_max_age_seconds,
                rollback_compaction_fence_seconds,
                managed_by as "managed_by: ManagedBy",
                updated_at,
                version
            "#,
        policy.enabled,
        *warehouse_id,
        max_compaction_age_seconds,
        fence_seconds
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;

    let Some(warehouse) = warehouse else {
        return Err(WarehouseIdNotFound::new(warehouse_id).into());
    };

    Ok(warehouse.try_into()?)
}

/// Re-read the `managed_by` marker `FOR UPDATE` within the active transaction and
/// reject the in-flight spec mutation when the warehouse is managed and the caller
/// does not hold bypass privileges (instance admin / in-process). Reading inside
/// the write transaction with a row lock makes the lock an enforced invariant,
/// immune to stale caches and concurrent marker changes (TOCTOU-safe).
///
/// A missing warehouse is not an error here — the subsequent mutation reports
/// not-found through its own path.
pub(crate) async fn ensure_warehouse_spec_mutable(
    warehouse_id: WarehouseId,
    bypass: bool,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), EnsureWarehouseSpecMutableError> {
    if bypass {
        return Ok(());
    }

    let managed_by = sqlx::query_scalar!(
        r#"SELECT managed_by as "managed_by: ManagedBy"
           FROM warehouse WHERE warehouse_id = $1 FOR UPDATE"#,
        *warehouse_id
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;

    if let Some(managed_by) = managed_by
        && managed_by.is_externally_managed()
    {
        return Err(WarehouseSpecLocked::new(managed_by).into());
    }

    Ok(())
}

pub(crate) async fn set_warehouse_format_version_policy(
    warehouse_id: WarehouseId,
    policy: &WarehouseFormatVersionPolicy,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<ResolvedWarehouse, SetWarehouseFormatVersionPolicyError> {
    let allowed_format_versions_db = format_version_versions_to_db(&policy.allowed_format_versions);
    let default_format_version_db = policy.default_format_version.map(format_version_to_db);

    let warehouse = sqlx::query_as!(
        WarehouseRecord,
        r#"UPDATE warehouse
            SET allowed_format_versions = $1, default_format_version = $2
            WHERE warehouse_id = $3
            RETURNING
                project_id,
                warehouse_id,
                warehouse_name,
                storage_profile as "storage_profile: Json<StorageProfile>",
                storage_secret_id,
                status AS "status: WarehouseStatus",
                tabular_delete_mode as "tabular_delete_mode: DbTabularDeleteProfile",
                tabular_expiration_seconds,
                protected,
                allowed_format_versions,
                default_format_version,
                rollback_compaction_on_conflict,
                rollback_compaction_max_age_seconds,
                rollback_compaction_fence_seconds,
                managed_by as "managed_by: ManagedBy",
                updated_at,
                version
            "#,
        &allowed_format_versions_db,
        default_format_version_db,
        *warehouse_id
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;

    let Some(warehouse) = warehouse else {
        return Err(WarehouseIdNotFound::new(warehouse_id).into());
    };

    Ok(warehouse.try_into()?)
}

pub(crate) async fn update_storage_profile(
    warehouse_id: WarehouseId,
    storage_profile: StorageProfile,
    storage_secret_id: Option<SecretId>,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<UpdatedWarehouseStorage, UpdateWarehouseStorageProfileError> {
    let storage_profile_ser =
        serde_json::to_value(storage_profile).map_err(StorageProfileSerializationError::from)?;

    let previous_secret_id = sqlx::query_scalar!(
        "SELECT storage_secret_id FROM warehouse WHERE warehouse_id = $1 AND status = 'active' FOR NO KEY UPDATE",
        *warehouse_id
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?
    .ok_or_else(|| WarehouseIdNotFound::new(warehouse_id))?;

    let warehouse = sqlx::query_as!(
        WarehouseRecord,
        r#"
            UPDATE warehouse
            SET storage_profile = $1, storage_secret_id = $2
            WHERE warehouse_id = $3
            AND status = 'active'
            RETURNING
                project_id,
                warehouse_id,
                warehouse_name,
                storage_profile as "storage_profile: Json<StorageProfile>",
                storage_secret_id,
                status AS "status: WarehouseStatus",
                tabular_delete_mode as "tabular_delete_mode: DbTabularDeleteProfile",
                tabular_expiration_seconds,
                protected,
                allowed_format_versions,
                default_format_version,
                rollback_compaction_on_conflict,
                rollback_compaction_max_age_seconds,
                rollback_compaction_fence_seconds,
                managed_by as "managed_by: ManagedBy",
                updated_at,
                version
        "#,
        storage_profile_ser,
        storage_secret_id.map(|id| id.into_uuid()),
        *warehouse_id
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;

    let Some(warehouse) = warehouse else {
        return Err(WarehouseIdNotFound::new(warehouse_id).into());
    };

    Ok(UpdatedWarehouseStorage {
        warehouse: warehouse.try_into()?,
        previous_secret_id: previous_secret_id.map(Into::into),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "tabular_delete_mode", rename_all = "kebab-case")]
enum DbTabularDeleteProfile {
    Soft,
    Hard,
}

impl From<TabularDeleteProfile> for DbTabularDeleteProfile {
    fn from(value: TabularDeleteProfile) -> Self {
        match value {
            TabularDeleteProfile::Soft { .. } => DbTabularDeleteProfile::Soft,
            TabularDeleteProfile::Hard {} => DbTabularDeleteProfile::Hard,
        }
    }
}

/// Convert a database tabular delete profile to the API tabular delete profile
fn db_to_api_tabular_delete_profile(
    mode: DbTabularDeleteProfile,
    expiration_seconds: Option<i64>,
) -> Result<TabularDeleteProfile, DatabaseIntegrityError> {
    match mode {
        DbTabularDeleteProfile::Soft => {
            let seconds = expiration_seconds.ok_or(DatabaseIntegrityError::new(
                "Did not find `expiration_seconds` for warehouse with soft deletion enabled.",
            ))?;
            Ok(TabularDeleteProfile::Soft {
                expiration_seconds: chrono::Duration::seconds(seconds),
            })
        }
        DbTabularDeleteProfile::Hard => Ok(TabularDeleteProfile::Hard {}),
    }
}

/// Convert a stored `smallint` to an Iceberg [`FormatVersion`].
fn format_version_from_db(value: i16) -> Result<FormatVersion, DatabaseIntegrityError> {
    match value {
        1 => Ok(FormatVersion::V1),
        2 => Ok(FormatVersion::V2),
        3 => Ok(FormatVersion::V3),
        other => Err(DatabaseIntegrityError::new(format!(
            "Invalid table format version '{other}' stored for warehouse."
        ))),
    }
}

/// Convert a stored `smallint[]` to [`AllowedFormatVersions`].
fn db_to_allowed_format_versions(
    values: Vec<i16>,
) -> Result<AllowedFormatVersions, DatabaseIntegrityError> {
    let versions = values
        .into_iter()
        .map(format_version_from_db)
        .collect::<Result<Vec<_>, _>>()?;
    AllowedFormatVersions::try_new(versions).map_err(|_| {
        DatabaseIntegrityError::new("Stored allowed_format_versions for warehouse is empty.")
    })
}

/// Convert an Iceberg [`FormatVersion`] to a `smallint` for storage.
fn format_version_to_db(version: FormatVersion) -> i16 {
    version as i16
}

/// Convert an [`AllowedFormatVersions`] set to a `smallint[]` for storage.
fn format_version_versions_to_db(allowed: &AllowedFormatVersions) -> Vec<i16> {
    allowed
        .as_slice()
        .iter()
        .copied()
        .map(format_version_to_db)
        .collect()
}

pub(crate) async fn get_warehouse_stats(
    conn: PgPool,
    warehouse_id: WarehouseId,
    PaginationQuery {
        page_size,
        page_token,
    }: PaginationQuery,
) -> lakekeeper::api::Result<WarehouseStatisticsResponse> {
    let page_size = CONFIG.page_size_or_pagination_default(page_size);

    let token = page_token
        .as_option()
        .map(PaginateToken::try_from)
        .transpose()?;

    let (token_ts, _): (_, Option<&String>) = token
        .as_ref()
        .map(PaginateToken::v1_parts)
        .transpose()?
        .unzip();

    let stats = sqlx::query!(
        r#"
        SELECT
            number_of_views as "number_of_views!",
            number_of_tables as "number_of_tables!",
            created_at as "created_at!",
            updated_at,
            timestamp as "timestamp!"
        FROM (
            (SELECT number_of_views, number_of_tables, created_at, updated_at, timestamp
            FROM warehouse_statistics
            WHERE warehouse_id = $1
            AND (timestamp < $2 OR $2 IS NULL))

            UNION ALL

            (SELECT number_of_views, number_of_tables, created_at, updated_at, timestamp
            FROM warehouse_statistics_history
            WHERE warehouse_id = $1
            AND (timestamp < $2 OR $2 IS NULL))
        ) AS ww
        ORDER BY timestamp DESC
        LIMIT $3
        "#,
        *warehouse_id,
        token_ts,
        page_size
    )
    .fetch_all(&conn)
    .await
    .map_err(|e| {
        tracing::error!(error=?e, "Error fetching warehouse stats");
        e.into_error_model("failed to get stats")
    })?;

    let next_page_token = stats.last().map(|s| {
        PaginateToken::V1(V1PaginateToken {
            created_at: s.timestamp,
            id: String::new(),
        })
        .to_string()
    });

    let stats = stats
        .into_iter()
        .map(|s| WarehouseStatistics {
            number_of_tables: s.number_of_tables,
            number_of_views: s.number_of_views,
            timestamp: s.timestamp,
            updated_at: s.updated_at.unwrap_or(s.created_at),
        })
        .collect();
    Ok(WarehouseStatisticsResponse {
        warehouse_ident: *warehouse_id,
        stats,
        next_page_token,
    })
}

#[cfg(any(test, feature = "test-utils"))]
#[allow(unused_imports, dead_code)]
pub mod test {
    use http::StatusCode;
    use lakekeeper::{
        api::iceberg::types::PageToken,
        service::{
            CatalogStore as _, CatalogWarehouseOps as _, Transaction, WriteTransaction,
            authz::CatalogWarehouseAction,
            cache_invalidation::CacheKey,
            storage::{S3Flavor, S3Profile},
        },
    };

    use super::*;
    use crate::{
        PostgresBackend, PostgresTransaction,
        test_utils::{change, change_log_head, committed_changes},
    };

    /// Simulate the post-migration steady state (Release N+1): unfreeze schema writes and make the
    /// legacy `table_schema.schema` column nullable, so the normalized-schema write path is
    /// exercisable in tests. Idempotent. The freeze trigger only exists after the data-migration
    /// hook runs (which `#[sqlx::test]` does not run), so the `DROP TRIGGER` is a no-op there.
    pub async fn advance_to_steady_state(pool: &sqlx::PgPool) {
        sqlx::query("DROP TRIGGER IF EXISTS table_schema_freeze_jsonb ON table_schema")
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("ALTER TABLE table_schema ALTER COLUMN schema DROP NOT NULL")
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("DROP TRIGGER IF EXISTS view_schema_freeze_jsonb ON view_schema")
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("ALTER TABLE view_schema ALTER COLUMN schema DROP NOT NULL")
            .execute(pool)
            .await
            .unwrap();
    }

    pub async fn initialize_warehouse(
        state: CatalogState,
        storage_profile: Option<StorageProfile>,
        project_id: Option<&ProjectId>,
        secret_id: Option<SecretId>,
        create_project: bool,
    ) -> (lakekeeper::service::ArcProjectId, lakekeeper::WarehouseId) {
        let project_id = project_id.map_or(Arc::new(ProjectId::from(uuid::Uuid::nil())), |id| {
            Arc::new(id.clone())
        });
        let mut t = WriteTransaction::<PostgresBackend>::begin(state.clone())
            .await
            .unwrap();

        if create_project {
            PostgresBackend::create_project(
                &project_id,
                format!("Project {project_id}"),
                t.transaction(),
            )
            .await
            .unwrap();
        }

        let storage_profile = storage_profile.unwrap_or(StorageProfile::S3(
            S3Profile::builder()
                .bucket("test_bucket".to_string())
                .region("us-east-1".to_string())
                .flavor(S3Flavor::S3Compat)
                .sts_enabled(false)
                .build(),
        ));

        let warehouse = PostgresBackend::create_warehouse(
            &project_id,
            CatalogCreateWarehouseRequest::builder()
                .warehouse_name("test_warehouse".to_string())
                .storage_profile(storage_profile)
                .storage_secret_id(secret_id)
                .delete_profile(TabularDeleteProfile::Soft {
                    expiration_seconds: chrono::Duration::seconds(5),
                })
                .build(),
            &mut t,
        )
        .await
        .unwrap();

        t.commit().await.unwrap();

        // Every table-touching test needs the normalized-schema write path, which requires the
        // post-migration steady state; applied here so tests don't each have to repeat it.
        advance_to_steady_state(&state.write_pool()).await;

        (project_id, warehouse.warehouse_id)
    }

    #[sqlx::test]
    async fn test_get_warehouse_by_name(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        // A project of its own: the warehouse caches are shared by every test in the process.
        let project_id = ProjectId::from(uuid::Uuid::new_v4());
        let (_, warehouse_id) =
            initialize_warehouse(state.clone(), None, Some(&project_id), None, true).await;

        let fetched_warehouse = PostgresBackend::get_warehouse_by_name(
            "test_warehouse",
            &Arc::new(project_id),
            WarehouseStatus::active(),
            state.clone(),
        )
        .await
        .unwrap();

        assert_eq!(
            Some(warehouse_id),
            fetched_warehouse.map(|w| w.warehouse_id)
        );
    }

    #[sqlx::test]
    async fn test_managed_by_locks_spec(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        // Default is self-managed: the guard allows mutation even without bypass.
        {
            let mut t = PostgresTransaction::begin_write(state.clone())
                .await
                .unwrap();
            PostgresBackend::ensure_warehouse_spec_mutable(
                warehouse_id,
                &CatalogWarehouseAction::Delete { force: false },
                false,
                t.transaction(),
            )
            .await
            .expect("self-managed warehouse spec is mutable by anyone");
            t.commit().await.unwrap();
        }

        // Mark the warehouse as managed by the instance.
        {
            let mut t = WriteTransaction::<PostgresBackend>::begin(state.clone())
                .await
                .unwrap();
            let wh = PostgresBackend::set_warehouse_managed_by(
                warehouse_id,
                ManagedBy::InstanceAdmin,
                &mut t,
            )
            .await
            .unwrap();
            assert_eq!(wh.managed_by, ManagedBy::InstanceAdmin);
            t.commit().await.unwrap();
        }

        // The marker persists (fresh read, cache skipped).
        let fetched = PostgresBackend::get_warehouse_by_id_cache_aware(
            warehouse_id,
            WarehouseStatus::active_and_inactive(),
            lakekeeper::service::CachePolicy::Skip,
            state.clone(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(fetched.managed_by, ManagedBy::InstanceAdmin);

        // A non-bypass caller is blocked with a 403 `WarehouseManaged`.
        {
            let mut t = PostgresTransaction::begin_write(state.clone())
                .await
                .unwrap();
            let err = PostgresBackend::ensure_warehouse_spec_mutable(
                warehouse_id,
                &CatalogWarehouseAction::Delete { force: false },
                false,
                t.transaction(),
            )
            .await
            .expect_err("managed warehouse blocks non-bypass spec mutation");
            let model = ErrorModel::from(err);
            assert_eq!(model.code, StatusCode::FORBIDDEN.as_u16());
            assert_eq!(model.r#type, "WarehouseManaged");
            t.commit().await.unwrap();
        }

        // A bypass caller (instance admin / in-process) passes.
        {
            let mut t = PostgresTransaction::begin_write(state.clone())
                .await
                .unwrap();
            PostgresBackend::ensure_warehouse_spec_mutable(
                warehouse_id,
                &CatalogWarehouseAction::Delete { force: false },
                true,
                t.transaction(),
            )
            .await
            .expect("instance-admin bypass may mutate a managed warehouse");
            t.commit().await.unwrap();

            // A non-spec action (e.g. a read) is never locked, even on a managed
            // warehouse without bypass — the gate short-circuits on
            // `is_spec_mutation`.
            let mut t = PostgresTransaction::begin_write(state.clone())
                .await
                .unwrap();
            PostgresBackend::ensure_warehouse_spec_mutable(
                warehouse_id,
                &CatalogWarehouseAction::GetMetadata,
                false,
                t.transaction(),
            )
            .await
            .expect("non-spec actions are not subject to the managed-by lock");
            t.commit().await.unwrap();
        }

        // Clearing the marker restores normal mutability (recovery path).
        {
            let mut t = WriteTransaction::<PostgresBackend>::begin(state.clone())
                .await
                .unwrap();
            let wh = PostgresBackend::set_warehouse_managed_by(
                warehouse_id,
                ManagedBy::SelfManaged,
                &mut t,
            )
            .await
            .unwrap();
            assert_eq!(wh.managed_by, ManagedBy::SelfManaged);
            t.commit().await.unwrap();
        }
        {
            let mut t = PostgresTransaction::begin_write(state.clone())
                .await
                .unwrap();
            PostgresBackend::ensure_warehouse_spec_mutable(
                warehouse_id,
                &CatalogWarehouseAction::Delete { force: false },
                false,
                t.transaction(),
            )
            .await
            .expect("cleared warehouse is mutable again");
            t.commit().await.unwrap();
        }
    }

    #[sqlx::test]
    async fn test_list_projects(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let project_id_1 = Arc::new(ProjectId::from(uuid::Uuid::new_v4()));
        initialize_warehouse(state.clone(), None, Some(&project_id_1), None, true).await;

        let mut trx = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();

        let projects = PostgresBackend::list_projects(None, trx.transaction())
            .await
            .unwrap()
            .into_iter()
            .map(|p| p.project_id)
            .collect::<Vec<_>>();
        trx.commit().await.unwrap();
        assert_eq!(projects.len(), 1);
        assert!(projects.contains(&project_id_1));

        let project_id_2 = Arc::new(ProjectId::from(uuid::Uuid::new_v4()));
        initialize_warehouse(state.clone(), None, Some(&project_id_2), None, true).await;

        let mut trx = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();

        let projects = PostgresBackend::list_projects(None, trx.transaction())
            .await
            .unwrap()
            .into_iter()
            .map(|p| p.project_id)
            .collect::<Vec<_>>();
        trx.commit().await.unwrap();
        assert_eq!(projects.len(), 2);
        assert!(projects.contains(&project_id_1));
        assert!(projects.contains(&project_id_2));
        let mut trx = PostgresTransaction::begin_read(state).await.unwrap();

        let projects = PostgresBackend::list_projects(
            Some(HashSet::from_iter(vec![(*project_id_1).clone()])),
            trx.transaction(),
        )
        .await
        .unwrap()
        .into_iter()
        .map(|p| p.project_id)
        .collect::<Vec<_>>();
        trx.commit().await.unwrap();

        assert_eq!(projects.len(), 1);
        assert!(projects.contains(&project_id_1));
    }

    #[sqlx::test]
    async fn test_list_warehouses(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectId::from(uuid::Uuid::new_v4());
        let (_, warehouse_id_1) =
            initialize_warehouse(state.clone(), None, Some(&project_id), None, true).await;
        let warehouses = PostgresBackend::list_warehouses(&project_id, None, state)
            .await
            .unwrap();
        assert_eq!(warehouses.len(), 1);
        // Check ids
        assert!(warehouses.iter().any(|w| w.warehouse_id == warehouse_id_1));
    }

    #[sqlx::test]
    async fn test_list_warehouses_active_filter(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectId::from(uuid::Uuid::new_v4());
        let (_, warehouse_id_1) =
            initialize_warehouse(state.clone(), None, Some(&project_id), None, true).await;

        // Rename warehouse 1
        let mut transaction = WriteTransaction::<PostgresBackend>::begin(state.clone())
            .await
            .unwrap();
        PostgresBackend::rename_warehouse(warehouse_id_1, "new_name", &mut transaction)
            .await
            .unwrap();
        PostgresBackend::set_warehouse_status(
            warehouse_id_1,
            WarehouseStatus::Inactive,
            &mut transaction,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        // Create warehouse 2
        let (_, warehouse_id_2) =
            initialize_warehouse(state.clone(), None, Some(&project_id), None, false).await;

        // Assert active whs
        let warehouses = PostgresBackend::list_warehouses(
            &project_id,
            Some(vec![WarehouseStatus::Active, WarehouseStatus::Inactive]),
            state.clone(),
        )
        .await
        .unwrap();
        assert_eq!(warehouses.len(), 2);
        assert!(warehouses.iter().any(|w| w.warehouse_id == warehouse_id_1));
        assert!(warehouses.iter().any(|w| w.warehouse_id == warehouse_id_2));

        // Assert only active whs

        let warehouses = PostgresBackend::list_warehouses(&project_id, None, state)
            .await
            .unwrap();
        assert_eq!(warehouses.len(), 1);
        assert!(warehouses.iter().any(|w| w.warehouse_id == warehouse_id_2));
    }

    #[sqlx::test]
    async fn test_rename_warehouse(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectId::from(uuid::Uuid::new_v4());
        let (_, warehouse_id) =
            initialize_warehouse(state.clone(), None, Some(&project_id), None, true).await;

        let mut transaction = WriteTransaction::<PostgresBackend>::begin(state.clone())
            .await
            .unwrap();
        PostgresBackend::rename_warehouse(warehouse_id, "new_name", &mut transaction)
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        let warehouse =
            PostgresBackend::get_warehouse_by_id(warehouse_id, WarehouseStatus::active(), state)
                .await
                .unwrap();
        assert_eq!(warehouse.unwrap().name, "new_name");
    }

    #[sqlx::test]
    async fn test_rename_project(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectId::from(uuid::Uuid::new_v4());
        {
            let mut t = PostgresTransaction::begin_write(state.clone())
                .await
                .unwrap();
            PostgresBackend::create_project(&project_id, "old_name".to_string(), t.transaction())
                .await
                .unwrap();
            t.commit().await.unwrap();
        }

        {
            let mut t = PostgresTransaction::begin_write(state.clone())
                .await
                .unwrap();
            PostgresBackend::rename_project(&project_id, "new_name", t.transaction())
                .await
                .unwrap();
            t.commit().await.unwrap();
        }

        let mut read_transaction = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();
        let project = PostgresBackend::get_project(&project_id, read_transaction.transaction())
            .await
            .unwrap();
        assert_eq!(project.unwrap().name, "new_name");
    }

    #[sqlx::test]
    async fn test_same_project_id(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectId::from(uuid::Uuid::new_v4());
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        PostgresBackend::create_project(&project_id, "old_name".to_string(), t.transaction())
            .await
            .unwrap();
        let err =
            PostgresBackend::create_project(&project_id, "other_name".to_string(), t.transaction())
                .await
                .unwrap_err();
        assert_eq!(err.error.code, StatusCode::CONFLICT);
        t.commit().await.unwrap();
    }

    #[sqlx::test]
    async fn test_cannot_drop_protected_warehouse(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectId::from(uuid::Uuid::new_v4());
        let (_, warehouse_id) =
            initialize_warehouse(state.clone(), None, Some(&project_id), None, true).await;
        let mut trx = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        set_warehouse_protection(warehouse_id, true, trx.transaction())
            .await
            .unwrap();
        let e = delete_warehouse(
            warehouse_id,
            DeleteWarehouseQuery { force: false },
            trx.transaction(),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            e,
            CatalogDeleteWarehouseError::WarehouseProtected(_)
        ));
        set_warehouse_protection(warehouse_id, false, trx.transaction())
            .await
            .unwrap();
        delete_warehouse(
            warehouse_id,
            DeleteWarehouseQuery { force: false },
            trx.transaction(),
        )
        .await
        .unwrap();

        trx.commit().await.unwrap();
    }

    #[sqlx::test]
    async fn test_rollback_compaction_policy_round_trips(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let created = get_warehouse_by_id(warehouse_id, &pool)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            created.rollback_compaction_policy,
            RollbackCompactionPolicy::default()
        );

        let policy = RollbackCompactionPolicy {
            enabled: true,
            max_compaction_age_seconds: 600,
            snapshot_removal_fence_seconds: 30,
        };
        let mut trx = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let updated =
            set_warehouse_rollback_compaction_policy(warehouse_id, policy, trx.transaction())
                .await
                .unwrap();
        trx.commit().await.unwrap();
        assert_eq!(updated.rollback_compaction_policy, policy);

        let reloaded = get_warehouse_by_id(warehouse_id, &pool)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reloaded.rollback_compaction_policy, policy);
        assert!(reloaded.version > created.version);
    }

    #[sqlx::test]
    async fn test_can_force_drop_protected_warehouse(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectId::from(uuid::Uuid::new_v4());
        let (_, warehouse_id) =
            initialize_warehouse(state.clone(), None, Some(&project_id), None, true).await;
        let mut trx = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        set_warehouse_protection(warehouse_id, true, trx.transaction())
            .await
            .unwrap();
        delete_warehouse(
            warehouse_id,
            DeleteWarehouseQuery { force: true },
            trx.transaction(),
        )
        .await
        .unwrap();

        trx.commit().await.unwrap();
    }

    #[sqlx::test]
    async fn test_warehouse_statistics_pagination(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectId::from(uuid::Uuid::new_v4());
        let (_, warehouse_id) =
            initialize_warehouse(state.clone(), None, Some(&project_id), None, true).await;

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        for i in 0..10 {
            sqlx::query!(
                r#"
                INSERT INTO warehouse_statistics_history (number_of_views, number_of_tables, warehouse_id, timestamp)
                VALUES ($1, $2, $3, $4)
                "#,
                i,
                i,
                *warehouse_id,
                chrono::Utc::now() - chrono::Duration::hours(i)
            )
            .execute(&mut **t.transaction())
            .await
            .unwrap();
        }
        t.commit().await.unwrap();

        let stats = PostgresBackend::get_warehouse_stats(
            warehouse_id,
            PaginationQuery {
                page_size: None,
                page_token: PageToken::NotSpecified,
            },
            state.clone(),
        )
        .await
        .unwrap();

        assert_eq!(stats.stats.len(), 11);

        let stats = PostgresBackend::get_warehouse_stats(
            warehouse_id,
            PaginationQuery {
                page_size: Some(3),
                page_token: PageToken::NotSpecified,
            },
            state.clone(),
        )
        .await
        .unwrap();

        assert_eq!(stats.stats.len(), 3);
        assert!(stats.next_page_token.is_some());

        let stats = PostgresBackend::get_warehouse_stats(
            warehouse_id,
            PaginationQuery {
                page_size: Some(5),
                page_token: stats.next_page_token.into(),
            },
            state.clone(),
        )
        .await
        .unwrap();

        assert_eq!(stats.stats.len(), 5);
        assert!(stats.next_page_token.is_some());

        let stats = PostgresBackend::get_warehouse_stats(
            warehouse_id,
            PaginationQuery {
                page_size: Some(5),
                page_token: stats.next_page_token.into(),
            },
            state.clone(),
        )
        .await
        .unwrap();

        assert_eq!(stats.stats.len(), 3);
        assert!(stats.next_page_token.is_some());

        // last page is empty
        let stats = PostgresBackend::get_warehouse_stats(
            warehouse_id,
            PaginationQuery {
                page_size: Some(5),
                page_token: stats.next_page_token.into(),
            },
            state,
        )
        .await
        .unwrap();

        assert_eq!(stats.stats.len(), 0);
        assert!(stats.next_page_token.is_none());
    }

    /// The delete reports the secret of the row it removed, so the caller cleans up
    /// the right credential. Sourced from the DELETE's own RETURNING rather than a
    /// prior read, which a concurrent credential rotation could invalidate.
    #[sqlx::test]
    async fn test_delete_warehouse_reports_the_deleted_rows_secret(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectId::from(uuid::Uuid::new_v4());
        let (_, without_secret) =
            initialize_warehouse(state.clone(), None, Some(&project_id), None, true).await;

        let secret_id = SecretId::from(uuid::Uuid::now_v7());
        let mut trx = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let with_secret = create_warehouse(
            &project_id,
            CatalogCreateWarehouseRequest::builder()
                .warehouse_name(format!("secret-owner-{}", uuid::Uuid::now_v7()))
                .storage_profile(crate::tests::memory_io_profile())
                .storage_secret_id(Some(secret_id))
                .delete_profile(TabularDeleteProfile::Hard {})
                .build(),
            trx.transaction(),
        )
        .await
        .unwrap();

        assert_eq!(
            delete_warehouse(
                with_secret.warehouse_id,
                DeleteWarehouseQuery { force: false },
                trx.transaction(),
            )
            .await
            .unwrap()
            .storage_secret_id,
            Some(secret_id)
        );
        // A warehouse without a credential reports nothing to clean up.
        assert_eq!(
            delete_warehouse(
                without_secret,
                DeleteWarehouseQuery { force: false },
                trx.transaction(),
            )
            .await
            .unwrap()
            .storage_secret_id,
            None
        );
        trx.commit().await.unwrap();
    }

    /// Every warehouse write records the cache keys it changes: the warehouse, the name it
    /// had, and the storage secret it replaced or removed.
    #[sqlx::test]
    async fn warehouse_writes_record_their_cache_keys(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectId::from(uuid::Uuid::new_v4());
        let (_, warehouse_id) =
            initialize_warehouse(state.clone(), None, Some(&project_id), None, true).await;
        let id = warehouse_id.to_string();
        let name = |name: &str| {
            CacheKey::WarehouseName {
                project_id: project_id.clone(),
                name: name.to_string(),
            }
            .encode()
        };
        let first_secret = SecretId::from(uuid::Uuid::now_v7());
        let second_secret = SecretId::from(uuid::Uuid::now_v7());
        let begin = || WriteTransaction::<PostgresBackend>::begin(state.clone());
        let mut logged = Vec::new();

        let mut t = begin().await.unwrap();
        let created = PostgresBackend::create_warehouse(
            &project_id,
            CatalogCreateWarehouseRequest::builder()
                .warehouse_name("second".to_string())
                .storage_profile(crate::tests::memory_io_profile())
                .delete_profile(TabularDeleteProfile::Hard {})
                .build(),
            &mut t,
        )
        .await
        .unwrap();
        logged.push(committed_changes(t, &pool).await);

        let mut t = begin().await.unwrap();
        PostgresBackend::rename_warehouse(warehouse_id, "renamed", &mut t)
            .await
            .unwrap();
        logged.push(committed_changes(t, &pool).await);

        let mut t = begin().await.unwrap();
        PostgresBackend::set_warehouse_protected(warehouse_id, true, &mut t)
            .await
            .unwrap();
        logged.push(committed_changes(t, &pool).await);

        let mut t = begin().await.unwrap();
        PostgresBackend::set_warehouse_managed_by(warehouse_id, ManagedBy::InstanceAdmin, &mut t)
            .await
            .unwrap();
        logged.push(committed_changes(t, &pool).await);

        let mut t = begin().await.unwrap();
        PostgresBackend::set_warehouse_deletion_profile(
            warehouse_id,
            &TabularDeleteProfile::Hard {},
            &mut t,
        )
        .await
        .unwrap();
        logged.push(committed_changes(t, &pool).await);

        let mut t = begin().await.unwrap();
        PostgresBackend::set_warehouse_format_version_policy(
            warehouse_id,
            &WarehouseFormatVersionPolicy::default(),
            &mut t,
        )
        .await
        .unwrap();
        logged.push(committed_changes(t, &pool).await);

        let mut t = begin().await.unwrap();
        PostgresBackend::set_warehouse_status(warehouse_id, WarehouseStatus::Inactive, &mut t)
            .await
            .unwrap();
        logged.push(committed_changes(t, &pool).await);

        let mut t = begin().await.unwrap();
        PostgresBackend::set_warehouse_status(warehouse_id, WarehouseStatus::Active, &mut t)
            .await
            .unwrap();
        logged.push(committed_changes(t, &pool).await);

        let mut t = begin().await.unwrap();
        let first = PostgresBackend::update_storage_profile(
            warehouse_id,
            crate::tests::memory_io_profile(),
            Some(first_secret),
            &mut t,
        )
        .await
        .unwrap();
        logged.push(committed_changes(t, &pool).await);

        let mut t = begin().await.unwrap();
        let second = PostgresBackend::update_storage_profile(
            warehouse_id,
            crate::tests::memory_io_profile(),
            Some(second_secret),
            &mut t,
        )
        .await
        .unwrap();
        logged.push(committed_changes(t, &pool).await);

        let mut t = begin().await.unwrap();
        PostgresBackend::delete_warehouse(
            warehouse_id,
            DeleteWarehouseQuery { force: true },
            &mut t,
        )
        .await
        .unwrap();
        logged.push(committed_changes(t, &pool).await);

        assert_eq!(
            (first.previous_secret_id, second.previous_secret_id),
            (None, Some(first_secret))
        );
        let warehouse_only = vec![change("warehouse", &[&id])];
        assert_eq!(
            logged,
            vec![
                vec![change("warehouse", &[&created.warehouse_id.to_string()])],
                vec![change("warehouse", &[&id, &name("test_warehouse")])],
                warehouse_only.clone(),
                warehouse_only.clone(),
                warehouse_only.clone(),
                warehouse_only.clone(),
                warehouse_only.clone(),
                warehouse_only.clone(),
                warehouse_only.clone(),
                vec![
                    change("warehouse", &[&id]),
                    change("secret", &[&first_secret.to_string()]),
                ],
                vec![
                    change("warehouse", &[&id, &name("renamed")]),
                    change("secret", &[&second_secret.to_string()]),
                ],
            ]
        );
    }

    /// Committing a write evicts the changed warehouse from this instance's cache; a rollback
    /// records nothing and keeps the cached entry.
    #[sqlx::test]
    async fn committed_writes_evict_locally_and_rolled_back_writes_do_not(pool: sqlx::PgPool) {
        use lakekeeper::service::warehouse_cache::WAREHOUSE_CACHE;

        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectId::from(uuid::Uuid::new_v4());
        let (_, warehouse_id) =
            initialize_warehouse(state.clone(), None, Some(&project_id), None, true).await;
        let warm = || async {
            PostgresBackend::get_warehouse_by_id(
                warehouse_id,
                WarehouseStatus::active(),
                state.clone(),
            )
            .await
            .unwrap()
            .unwrap();
            WAREHOUSE_CACHE.get(&warehouse_id).await.is_some()
        };

        assert!(warm().await);
        let mut t = WriteTransaction::<PostgresBackend>::begin(state.clone())
            .await
            .unwrap();
        PostgresBackend::rename_warehouse(warehouse_id, "rolled-back", &mut t)
            .await
            .unwrap();
        let before = change_log_head(&pool).await;
        t.rollback().await.unwrap();
        let after = change_log_head(&pool).await;
        let cached_after_rollback = WAREHOUSE_CACHE.get(&warehouse_id).await.is_some();

        let mut t = WriteTransaction::<PostgresBackend>::begin(state.clone())
            .await
            .unwrap();
        PostgresBackend::rename_warehouse(warehouse_id, "committed", &mut t)
            .await
            .unwrap();
        t.commit().await.unwrap();

        assert_eq!(
            (
                after - before,
                cached_after_rollback,
                WAREHOUSE_CACHE.get(&warehouse_id).await.is_some()
            ),
            (0, true, false)
        );
    }

    /// A commit that fails also evicts the written warehouse locally: whether the change
    /// landed is unknown, and an eviction only costs a reload.
    #[sqlx::test]
    async fn a_failed_commit_still_evicts_locally(pool: sqlx::PgPool) {
        use lakekeeper::service::warehouse_cache::WAREHOUSE_CACHE;

        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectId::from(uuid::Uuid::new_v4());
        let (_, warehouse_id) =
            initialize_warehouse(state.clone(), None, Some(&project_id), None, true).await;
        sqlx::raw_sql(
            r"
            CREATE FUNCTION test_refuse_commit() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                RAISE EXCEPTION 'commit refused';
            END $$;
            CREATE CONSTRAINT TRIGGER refuse_commit AFTER UPDATE ON warehouse
                DEFERRABLE INITIALLY DEFERRED
                FOR EACH ROW EXECUTE FUNCTION test_refuse_commit();
            ",
        )
        .execute(&pool)
        .await
        .unwrap();
        PostgresBackend::get_warehouse_by_id(
            warehouse_id,
            WarehouseStatus::active(),
            state.clone(),
        )
        .await
        .unwrap()
        .unwrap();
        let cached_before = WAREHOUSE_CACHE.get(&warehouse_id).await.is_some();

        let mut t = WriteTransaction::<PostgresBackend>::begin(state.clone())
            .await
            .unwrap();
        PostgresBackend::rename_warehouse(warehouse_id, "refused", &mut t)
            .await
            .unwrap();
        let committed = t.commit().await;

        assert_eq!(
            (
                cached_before,
                committed.is_err(),
                WAREHOUSE_CACHE.get(&warehouse_id).await.is_some()
            ),
            (true, true, false)
        );
    }

    /// A task committed after the unfinished-task check, while the DELETE waits for the
    /// warehouse row, fails the DELETE on `task_warehouse_id_fk`.
    #[sqlx::test]
    async fn test_delete_warehouse_with_concurrently_inserted_task(pool: sqlx::PgPool) {
        use lakekeeper::service::tasks::{
            ScheduleTaskMetadata, TaskEntity, TaskInput, TaskQueueName, WarehouseTaskEntityId,
        };

        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (project_id, warehouse_id) =
            initialize_warehouse(state.clone(), None, None, None, true).await;

        // Until it commits, the inserting transaction holds a key-share lock on the warehouse row.
        let mut inserter = pool.begin().await.unwrap();
        crate::tasks::queue_task_batch(
            &mut inserter,
            &TaskQueueName::from("concurrent-queue"),
            vec![TaskInput {
                task_metadata: ScheduleTaskMetadata {
                    resource_demand: None,
                    project_id,
                    parent_task_id: None,
                    scheduled_for: None,
                    entity: TaskEntity::EntityInWarehouse {
                        warehouse_id,
                        entity_id: WarehouseTaskEntityId::Table {
                            table_id: uuid::Uuid::now_v7().into(),
                        },
                        entity_name: vec!["table".to_string()],
                    },
                },
                payload: serde_json::json!({}),
            }],
        )
        .await
        .unwrap();

        let delete = tokio::spawn({
            let pool = pool.clone();
            async move {
                let mut trx = pool.begin().await.unwrap();
                delete_warehouse(
                    warehouse_id,
                    DeleteWarehouseQuery { force: false },
                    &mut trx,
                )
                .await
            }
        });
        wait_for_a_lock_wait(&pool).await;
        inserter.commit().await.unwrap();

        let err = delete.await.unwrap().unwrap_err();
        let CatalogDeleteWarehouseError::WarehouseHasUnfinishedTasks(err) = err else {
            panic!("{err:?}");
        };
        // The unfinished-task check would have added an "Unfinished tasks: ..." entry.
        assert_eq!(
            err.stack,
            vec!["Error deleting warehouse in catalog".to_string()]
        );
        assert_eq!(ErrorModel::from(err).code, StatusCode::CONFLICT.as_u16());
    }

    /// A transaction that sets `protected` while the delete waits for the warehouse row makes
    /// the delete see the protection; the warehouse is kept and the caller gets a 409.
    #[sqlx::test]
    async fn test_delete_warehouse_protected_concurrently(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        let mut protector = pool.begin().await.unwrap();
        sqlx::query("UPDATE warehouse SET protected = true WHERE warehouse_id = $1")
            .bind(*warehouse_id)
            .execute(&mut *protector)
            .await
            .unwrap();

        let delete = tokio::spawn({
            let pool = pool.clone();
            async move {
                let mut trx = pool.begin().await.unwrap();
                delete_warehouse(
                    warehouse_id,
                    DeleteWarehouseQuery { force: false },
                    &mut trx,
                )
                .await
            }
        });
        wait_for_a_lock_wait(&pool).await;
        protector.commit().await.unwrap();

        let err = delete.await.unwrap().unwrap_err();
        let CatalogDeleteWarehouseError::WarehouseProtected(err) = err else {
            panic!("{err:?}");
        };
        assert_eq!(ErrorModel::from(err).code, StatusCode::CONFLICT.as_u16());
        let protected: Vec<bool> =
            sqlx::query_scalar("SELECT protected FROM warehouse WHERE warehouse_id = $1")
                .bind(*warehouse_id)
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(protected, vec![true]);
    }

    /// Returns once a session of this test's database waits for a lock.
    #[cfg(test)]
    pub(crate) async fn wait_for_a_lock_wait(pool: &sqlx::PgPool) {
        let waiting = async {
            loop {
                let waiting: i64 = sqlx::query_scalar(
                    "SELECT count(*) FROM pg_stat_activity \
                     WHERE datname = current_database() AND wait_event_type = 'Lock'",
                )
                .fetch_one(pool)
                .await
                .unwrap();
                if waiting > 0 {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        };
        tokio::time::timeout(std::time::Duration::from_secs(10), waiting)
            .await
            .expect("a session waits for a lock");
    }

    /// Schedules are deleted before the project row is locked, so a materializer that holds a
    /// schedule row and then inserts its task finishes without deadlocking.
    #[sqlx::test]
    async fn test_delete_project_deletes_schedules_before_locking_the_project(pool: sqlx::PgPool) {
        use lakekeeper::service::{
            CatalogTaskOps as _,
            tasks::{
                ScheduleTaskMetadata, TaskEntity, TaskInput, TaskQueueName, TaskScheduleUpsert,
            },
        };

        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = Arc::new(ProjectId::new_random());
        let queue_name = TaskQueueName::from("scheduled-queue");
        let mut trx = PostgresTransaction::begin_write(state).await.unwrap();
        PostgresBackend::create_project(&project_id, "project".to_string(), trx.transaction())
            .await
            .unwrap();
        let schedule = PostgresBackend::upsert_task_schedule(
            TaskScheduleUpsert::builder()
                .project_id(project_id.clone())
                .queue_name(queue_name.clone())
                .schedule_key("nightly")
                .interval(chrono::Duration::hours(1))
                .build(),
            trx.transaction(),
        )
        .await
        .unwrap();
        trx.commit().await.unwrap();

        // The materializer locks the schedule row until it commits.
        let mut materializer = pool.begin().await.unwrap();
        sqlx::query("SELECT 1 FROM task_schedule WHERE schedule_id = $1 FOR UPDATE")
            .bind(*schedule.schedule_id)
            .execute(&mut *materializer)
            .await
            .unwrap();

        let delete = tokio::spawn({
            let pool = pool.clone();
            let project_id = project_id.clone();
            async move {
                let mut trx = pool.begin().await.unwrap();
                delete_project(&project_id, &mut trx).await.unwrap();
                trx.commit().await.unwrap();
            }
        });
        wait_for_a_lock_wait(&pool).await;

        // The task's foreign-key check needs the project row.
        let tick = TaskInput {
            task_metadata: ScheduleTaskMetadata {
                resource_demand: None,
                project_id: project_id.clone(),
                parent_task_id: None,
                scheduled_for: None,
                entity: TaskEntity::Project,
            },
            payload: serde_json::json!({}),
        };
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            crate::tasks::queue_task_batch(&mut materializer, &queue_name, vec![tick]),
        )
        .await
        .expect("inserting does not wait for the delete")
        .unwrap();
        materializer.commit().await.unwrap();

        delete.await.unwrap();
        let left: (i64, i64) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM task WHERE project_id = $1), \
                    (SELECT count(*) FROM task_schedule WHERE project_id = $1)",
        )
        .bind(project_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(left, (0, 0));
    }

    #[sqlx::test]
    async fn test_delete_warehouse_with_namespace_is_not_empty(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        crate::namespace::tests::initialize_namespace(
            state.clone(),
            warehouse_id,
            &iceberg::NamespaceIdent::new("ns".to_string()),
            None,
        )
        .await;

        let mut trx = PostgresTransaction::begin_write(state).await.unwrap();
        let err = delete_warehouse(
            warehouse_id,
            DeleteWarehouseQuery { force: false },
            trx.transaction(),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, CatalogDeleteWarehouseError::WarehouseNotEmpty(_)),
            "{err:?}"
        );
    }

    #[sqlx::test]
    async fn test_delete_non_existing_warehouse(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectId::from(uuid::Uuid::new_v4());
        let (_, warehouse_id) =
            initialize_warehouse(state.clone(), None, Some(&project_id), None, true).await;
        let mut trx = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        delete_warehouse(
            warehouse_id,
            DeleteWarehouseQuery { force: false },
            trx.transaction(),
        )
        .await
        .unwrap();
        let e = delete_warehouse(
            warehouse_id,
            DeleteWarehouseQuery { force: false },
            trx.transaction(),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            e,
            CatalogDeleteWarehouseError::WarehouseIdNotFound(_)
        ));
    }

    // ── create_roles `OnRoleConflict::UpdateMetadata` semantics ────────────

    #[sqlx::test]
    async fn test_create_roles_update_metadata_is_noop_when_unchanged(pool: sqlx::PgPool) {
        // The IS DISTINCT FROM guard in the ON CONFLICT clause must keep
        // re-runs with identical values from bumping `version` (which the
        // `set_updated_at_and_increment_version` trigger would otherwise
        // increment on any UPDATE that touched a row tuple). The returned
        // Vec must also be empty — no rows were inserted or changed.
        use lakekeeper::service::{
            CatalogCreateRoleRequest, OnRoleConflict, RoleId, RoleSourceId, SYSTEM_ROLE_PROVIDER_ID,
        };

        use crate::role::create_roles;

        let project_id = ProjectId::new_random();
        let mut t = pool.begin().await.unwrap();
        create_project(&project_id, "noop-test".to_string(), &mut t)
            .await
            .unwrap();
        t.commit().await.unwrap();

        // First seed: insert one system role.
        let source: RoleSourceId = "example_role".parse().unwrap();
        let initial = CatalogCreateRoleRequest::builder()
            .role_id(RoleId::new_random())
            .role_name("Example Role")
            .description(Some("Example description"))
            .source_id(&source)
            .provider_id(&SYSTEM_ROLE_PROVIDER_ID)
            .build();
        let mut t = pool.begin().await.unwrap();
        let seeded = create_roles(
            &project_id,
            vec![initial],
            OnRoleConflict::UpdateMetadata,
            &mut *t,
        )
        .await
        .unwrap();
        t.commit().await.unwrap();
        assert_eq!(seeded.len(), 1);

        let original = fetch_system_role(&pool, &project_id, &source).await;

        // Re-run with identical values — must be a no-op.
        let again = CatalogCreateRoleRequest::builder()
            .role_id(RoleId::new_random())
            .role_name("Example Role")
            .description(Some("Example description"))
            .source_id(&source)
            .provider_id(&SYSTEM_ROLE_PROVIDER_ID)
            .build();
        let mut t = pool.begin().await.unwrap();
        let upserted = create_roles(
            &project_id,
            vec![again],
            OnRoleConflict::UpdateMetadata,
            &mut *t,
        )
        .await
        .unwrap();
        t.commit().await.unwrap();
        assert_eq!(
            upserted.len(),
            0,
            "no-op upsert must return an empty Vec, not the unchanged row"
        );

        let after = fetch_system_role(&pool, &project_id, &source).await;
        assert_eq!(after.id, original.id, "row id must be unchanged");
        assert_eq!(
            after.version, original.version,
            "version must not bump on no-op upsert"
        );
        assert_eq!(
            after.updated_at, original.updated_at,
            "updated_at must not move on no-op upsert"
        );
    }

    #[sqlx::test]
    async fn test_create_roles_update_metadata_refreshes_existing_row(pool: sqlx::PgPool) {
        // Locks in `OnRoleConflict::UpdateMetadata`: re-running create_roles
        // with the same `(project, provider, source_id)` but a new
        // name/description must update the existing row in place,
        // preserving its id and bumping its version via the trigger.
        use lakekeeper::service::{
            CatalogCreateRoleRequest, OnRoleConflict, RoleId, RoleSourceId, SYSTEM_ROLE_PROVIDER_ID,
        };

        use crate::role::create_roles;

        let project_id = ProjectId::new_random();
        let mut t = pool.begin().await.unwrap();
        create_project(&project_id, "upsert-test".to_string(), &mut t)
            .await
            .unwrap();
        t.commit().await.unwrap();

        let source: RoleSourceId = "example_role".parse().unwrap();
        let initial = CatalogCreateRoleRequest::builder()
            .role_id(RoleId::new_random())
            .role_name("Original Name")
            .description(Some("Original description"))
            .source_id(&source)
            .provider_id(&SYSTEM_ROLE_PROVIDER_ID)
            .build();
        let mut t = pool.begin().await.unwrap();
        create_roles(
            &project_id,
            vec![initial],
            OnRoleConflict::UpdateMetadata,
            &mut *t,
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        let original = fetch_system_role(&pool, &project_id, &source).await;
        assert_eq!(original.name, "Original Name");

        // Upsert with a different name+description; row id must be
        // preserved, name/description and version change.
        let refreshed_request = CatalogCreateRoleRequest::builder()
            .role_id(RoleId::new_random())
            .role_name("Refreshed Name")
            .description(Some("Refreshed description"))
            .source_id(&source)
            .provider_id(&SYSTEM_ROLE_PROVIDER_ID)
            .build();
        let mut t = pool.begin().await.unwrap();
        let upserted = create_roles(
            &project_id,
            vec![refreshed_request],
            OnRoleConflict::UpdateMetadata,
            &mut *t,
        )
        .await
        .unwrap();
        t.commit().await.unwrap();
        assert_eq!(upserted.len(), 1);

        let refreshed = fetch_system_role(&pool, &project_id, &source).await;
        assert_eq!(refreshed.id, original.id, "row id must be preserved");
        assert_eq!(refreshed.name, "Refreshed Name");
        assert_eq!(
            refreshed.description.as_deref(),
            Some("Refreshed description")
        );
        assert!(
            *refreshed.version > *original.version,
            "version must be bumped by the trigger ({:?} -> {:?})",
            original.version,
            refreshed.version
        );
    }

    /// Lookup helper used by upsert tests — fetches the single system role
    /// row identified by `(provider_id = "system", source_id)`.
    async fn fetch_system_role(
        pool: &sqlx::PgPool,
        project_id: &ProjectId,
        source: &lakekeeper::service::RoleSourceId,
    ) -> std::sync::Arc<lakekeeper::service::Role> {
        use lakekeeper::service::{CatalogListRolesByIdFilter, SYSTEM_ROLE_PROVIDER_ID};

        use crate::role::list_roles;
        let provider = &*SYSTEM_ROLE_PROVIDER_ID;
        let providers = [provider];
        let sources = [source];
        let filter = CatalogListRolesByIdFilter::builder()
            .provider_ids(Some(&providers))
            .source_ids(Some(&sources))
            .build();
        let response = list_roles(
            Some(project_id),
            filter,
            PaginationQuery {
                page_size: Some(10),
                page_token: PageToken::Empty,
            },
            pool,
        )
        .await
        .unwrap();
        assert_eq!(response.roles.len(), 1, "expected exactly one matching row");
        response.roles[0].clone()
    }
}
