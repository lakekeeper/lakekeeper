use std::{collections::HashSet, ops::Deref, sync::Arc};

use iceberg::spec::FormatVersion;
use sqlx::{PgPool, types::Json};

use super::CatalogState;
use crate::{
    CONFIG, ProjectId, SecretId, WarehouseId,
    api::{
        ErrorModel,
        iceberg::v1::PaginationQuery,
        management::v1::{
            DeleteWarehouseQuery,
            warehouse::{TabularDeleteProfile, WarehouseStatistics, WarehouseStatisticsResponse},
        },
    },
    implementations::postgres::{
        PostgresBackend,
        dbutils::DBErrorHandler,
        pagination::{PaginateToken, V1PaginateToken},
    },
    service::{
        AllowedFormatVersions, CatalogCreateWarehouseError, CatalogDeleteWarehouseError,
        CatalogGetWarehouseByIdError, CatalogGetWarehouseByNameError, CatalogListWarehousesError,
        CatalogRenameWarehouseError, CatalogRoleOps, DatabaseIntegrityError, GetProjectResponse,
        ProjectIdNotFoundError, ResolvedWarehouse, SetWarehouseDeletionProfileError,
        SetWarehouseFormatVersionPolicyError, SetWarehouseProtectedError, SetWarehouseStatusError,
        StorageProfileSerializationError, SystemRoleSeederCap, UpdateWarehouseStorageProfileError,
        WarehouseAlreadyExists, WarehouseFormatVersionPolicy, WarehouseHasUnfinishedTasks,
        WarehouseIdNotFound, WarehouseNotEmpty, WarehouseProtected, WarehouseStatus,
        WarehouseVersion, registered_system_roles, storage::StorageProfile,
    },
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
    warehouse_name: String,
    project_id: &ProjectId,
    storage_profile: StorageProfile,
    tabular_delete_profile: TabularDeleteProfile,
    storage_secret_id: Option<SecretId>,
    format_version_policy: WarehouseFormatVersionPolicy,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<ResolvedWarehouse, CatalogCreateWarehouseError> {
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
                                   warehouse_name,
                                   project_id,
                                   storage_profile,
                                   storage_secret_id,
                                   status,
                                   tabular_expiration_seconds,
                                   tabular_delete_mode,
                                   allowed_format_versions,
                                   default_format_version)
                                VALUES ($1, $2, $3, $4, 'active', $5, $6, $7, $8)
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
        default_format_version_db
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| match &e {
        sqlx::Error::Database(db_err) => match db_err.constraint() {
            // ToDo: Get constraint name from const
            Some("unique_warehouse_name_in_project") => CatalogCreateWarehouseError::from(
                WarehouseAlreadyExists::new(warehouse_name, project_id.clone()),
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
) -> crate::service::Result<()> {
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
pub(crate) async fn create_project(
    project_id: &ProjectId,
    project_name: String,
    transaction: &mut sqlx::Transaction<'static, sqlx::Postgres>,
) -> crate::service::Result<()> {
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
    // insert; the seeder path goes through the cap-gated trait method so
    // `create_project` and the post-migration backfill share one code path.
    let specs = registered_system_roles();
    if !specs.is_empty() {
        let cap = SystemRoleSeederCap::new();
        PostgresBackend::upsert_system_roles(project_id, specs, cap, transaction)
            .await
            .map_err(|e| {
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
) -> crate::service::Result<Option<GetProjectResponse>> {
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

pub(crate) async fn delete_project(
    project_id: &ProjectId,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> crate::service::Result<()> {
    let row_count = sqlx::query_scalar!(r#"DELETE FROM project WHERE project_id = $1"#, project_id)
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
        })?
        .rows_affected();

    if row_count == 0 {
        return Err(ErrorModel::not_found("Project not found", "ProjectNotFound", None).into());
    }

    Ok(())
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
    allowed_format_versions: Vec<i16>,
    default_format_version: Option<i16>,
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
            allowed_format_versions,
            default_format_version,
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
) -> crate::service::Result<Vec<GetProjectResponse>> {
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
) -> Result<(), CatalogDeleteWarehouseError> {
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

    let protected = sqlx::query_scalar!(
        r#"WITH delete_info as (
               SELECT protected FROM warehouse w WHERE w.warehouse_id = $1
           ),
           deleted as (DELETE FROM warehouse WHERE warehouse_id = $1 AND (not protected OR $2))
           SELECT protected as "protected!" FROM delete_info"#,
        *warehouse_id,
        force
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| match &e {
        sqlx::Error::RowNotFound => {
            CatalogDeleteWarehouseError::from(WarehouseIdNotFound::new(warehouse_id))
        }
        sqlx::Error::Database(db_error) => {
            if db_error.is_foreign_key_violation() {
                WarehouseNotEmpty::new().into()
            } else {
                e.into_catalog_backend_error().into()
            }
        }
        _ => e.into_catalog_backend_error().into(),
    })?;

    if protected && !force {
        return Err(WarehouseProtected::new().into());
    }

    Ok(())
}

pub(crate) async fn rename_warehouse(
    warehouse_id: WarehouseId,
    new_name: &str,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<ResolvedWarehouse, CatalogRenameWarehouseError> {
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

    Ok(warehouse.try_into()?)
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
) -> Result<ResolvedWarehouse, UpdateWarehouseStorageProfileError> {
    let storage_profile_ser =
        serde_json::to_value(storage_profile).map_err(StorageProfileSerializationError::from)?;

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

    Ok(warehouse.try_into()?)
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
) -> crate::api::Result<WarehouseStatisticsResponse> {
    let page_size = CONFIG.page_size_or_pagination_default(page_size);

    let token = page_token
        .as_option()
        .map(PaginateToken::try_from)
        .transpose()?;

    let (token_ts, _): (_, Option<String>) = token
        .map(|PaginateToken::V1(V1PaginateToken { created_at, id })| (created_at, id))
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

#[cfg(test)]
pub(crate) mod test {
    use http::StatusCode;

    use super::*;
    use crate::{
        api::iceberg::types::PageToken,
        implementations::postgres::{PostgresBackend, PostgresTransaction},
        service::{
            CatalogStore as _, CatalogWarehouseOps as _, Transaction,
            storage::{S3Flavor, S3Profile},
        },
    };

    pub(crate) async fn initialize_warehouse(
        state: CatalogState,
        storage_profile: Option<StorageProfile>,
        project_id: Option<&ProjectId>,
        secret_id: Option<SecretId>,
        create_project: bool,
    ) -> (crate::service::ArcProjectId, crate::WarehouseId) {
        let project_id = project_id.map_or(Arc::new(ProjectId::from(uuid::Uuid::nil())), |id| {
            Arc::new(id.clone())
        });
        let mut t = PostgresTransaction::begin_write(state.clone())
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
            "test_warehouse".to_string(),
            &project_id,
            storage_profile,
            TabularDeleteProfile::Soft {
                expiration_seconds: chrono::Duration::seconds(5),
            },
            secret_id,
            WarehouseFormatVersionPolicy::default(),
            t.transaction(),
        )
        .await
        .unwrap();

        t.commit().await.unwrap();
        (project_id, warehouse.warehouse_id)
    }

    #[sqlx::test]
    async fn test_get_warehouse_by_name(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        let fetched_warehouse = PostgresBackend::get_warehouse_by_name(
            "test_warehouse",
            &Arc::new(ProjectId::from(uuid::Uuid::nil())),
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
        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        PostgresBackend::rename_warehouse(warehouse_id_1, "new_name", transaction.transaction())
            .await
            .unwrap();
        PostgresBackend::set_warehouse_status(
            warehouse_id_1,
            WarehouseStatus::Inactive,
            transaction.transaction(),
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

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        PostgresBackend::rename_warehouse(warehouse_id, "new_name", transaction.transaction())
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
        use crate::{
            implementations::postgres::role::create_roles,
            service::{
                CatalogCreateRoleRequest, OnRoleConflict, RoleId, RoleSourceId,
                SYSTEM_ROLE_PROVIDER_ID,
            },
        };

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
        use crate::{
            implementations::postgres::role::create_roles,
            service::{
                CatalogCreateRoleRequest, OnRoleConflict, RoleId, RoleSourceId,
                SYSTEM_ROLE_PROVIDER_ID,
            },
        };

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
        source: &crate::service::RoleSourceId,
    ) -> std::sync::Arc<crate::service::Role> {
        use crate::{
            implementations::postgres::role::list_roles,
            service::{CatalogListRolesByIdFilter, SYSTEM_ROLE_PROVIDER_ID},
        };
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
