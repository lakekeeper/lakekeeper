use std::{collections::HashSet, ops::Deref};

use sqlx::{types::Json, PgPool};

use super::{dbutils::DBErrorHandler as _, CatalogState};
use crate::{
    api::{
        iceberg::v1::{PaginationQuery, MAX_PAGE_SIZE},
        management::v1::{
            warehouse::{TabularDeleteProfile, WarehouseStatistics, WarehouseStatisticsResponse},
            DeleteWarehouseQuery, ProtectionResponse,
        },
        CatalogConfig, ErrorModel, Result,
    },
    implementations::postgres::pagination::{PaginateToken, V1PaginateToken},
    request_metadata::RequestMetadata,
    service::{storage::StorageProfile, GetProjectResponse, GetWarehouseResponse, WarehouseStatus},
    ProjectId, SecretIdent, WarehouseIdent,
};

pub(super) async fn get_warehouse_by_name(
    warehouse_name: &str,
    project_id: &ProjectId,
    catalog_state: CatalogState,
) -> Result<Option<WarehouseIdent>> {
    let warehouse_id = sqlx::query_scalar!(
        r#"
            SELECT
                warehouse_id
            FROM warehouse
            WHERE warehouse_name = $1 AND project_id = $2
            AND status = 'active'
            "#,
        warehouse_name.to_string(),
        project_id
    )
    .fetch_optional(&catalog_state.read_pool())
    .await
    .map_err(map_select_warehouse_err)?;

    Ok(warehouse_id.map(Into::into))
}

pub(super) async fn set_warehouse_deletion_profile<
    'c,
    'e: 'c,
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
>(
    warehouse_id: WarehouseIdent,
    deletion_profile: &TabularDeleteProfile,
    connection: E,
) -> Result<()> {
    let num_secs = deletion_profile
        .expiration_seconds()
        .map(|dur| dur.num_seconds());
    let prof = DbTabularDeleteProfile::from(*deletion_profile);

    let row_count = sqlx::query!(
        r#"
            UPDATE warehouse
            SET tabular_expiration_seconds = $1, tabular_delete_mode = $2
            WHERE warehouse_id = $3
            AND status = 'active'
            "#,
        num_secs,
        prof as _,
        *warehouse_id
    )
    .execute(connection)
    .await
    .map_err(|e| e.into_error_model("Error setting warehouse deletion profile"))?
    .rows_affected();

    if row_count == 0 {
        return Err(ErrorModel::not_found("Warehouse not found", "WarehouseNotFound", None).into());
    }

    Ok(())
}

pub(super) async fn get_config_for_warehouse(
    warehouse_id: WarehouseIdent,
    catalog_state: CatalogState,
    request_metadata: &RequestMetadata,
) -> Result<Option<CatalogConfig>> {
    let storage_profile = sqlx::query_scalar!(
        r#"
            SELECT
                storage_profile as "storage_profile: Json<StorageProfile>"
            FROM warehouse
            WHERE warehouse_id = $1
            AND status = 'active'
            "#,
        *warehouse_id
    )
    .fetch_optional(&catalog_state.read_pool())
    .await
    .map_err(map_select_warehouse_err)?;

    Ok(storage_profile.map(|p| p.generate_catalog_config(warehouse_id, request_metadata)))
}

pub(crate) async fn create_warehouse(
    warehouse_name: String,
    project_id: &ProjectId,
    storage_profile: StorageProfile,
    tabular_delete_profile: TabularDeleteProfile,
    storage_secret_id: Option<SecretIdent>,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<WarehouseIdent> {
    let storage_profile_ser = serde_json::to_value(storage_profile).map_err(|e| {
        ErrorModel::internal(
            "Error serializing storage profile",
            "StorageProfileSerializationError",
            Some(Box::new(e)),
        )
    })?;

    let num_secs = tabular_delete_profile
        .expiration_seconds()
        .map(|dur| dur.num_seconds());
    let prof = DbTabularDeleteProfile::from(tabular_delete_profile);

    let warehouse_id = sqlx::query_scalar!(
        r#"WITH
            whi AS (INSERT INTO warehouse (
                                   warehouse_name,
                                   project_id,
                                   storage_profile,
                                   storage_secret_id,
                                   status,
                                   tabular_expiration_seconds,
                                   tabular_delete_mode)
                                VALUES ($1, $2, $3, $4, 'active', $5, $6)
                                RETURNING warehouse_id),
            whs AS (INSERT INTO warehouse_statistics (number_of_views,
                                                      number_of_tables,
                                                      warehouse_id)
                     VALUES (0, 0, (SELECT warehouse_id FROM whi)))
            SELECT warehouse_id FROM whi"#,
        warehouse_name,
        project_id,
        storage_profile_ser,
        storage_secret_id.map(|id| id.into_uuid()),
        num_secs,
        prof as _
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| match &e {
        sqlx::Error::Database(db_err) => match db_err.constraint() {
            // ToDo: Get constraint name from const
            Some("unique_warehouse_name_in_project") => ErrorModel::conflict(
                "Warehouse with this name already exists in the project.",
                "WarehouseNameAlreadyExists",
                Some(Box::new(e)),
            ),
            Some("warehouse_project_id_fk") => {
                ErrorModel::not_found("Project not found", "ProjectNotFound", Some(Box::new(e)))
            }
            _ => e.into_error_model("Error creating Warehouse"),
        },
        _ => e.into_error_model("Error creating Warehouse"),
    })?;

    Ok(warehouse_id.into())
}

pub(crate) async fn rename_project(
    project_id: &ProjectId,
    new_name: &str,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
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

pub(crate) async fn create_project(
    project_id: &ProjectId,
    project_name: String,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
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

    Ok(())
}

pub(crate) async fn get_project(
    project_id: &ProjectId,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Option<GetProjectResponse>> {
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
            project_id: ProjectId::from_db_unchecked(project.project_id),
            name: project.project_name,
        }))
    } else {
        Ok(None)
    }
}

pub(crate) async fn delete_project(
    project_id: &ProjectId,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
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

pub(crate) async fn list_warehouses<
    'e,
    'c: 'e,
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
>(
    project_id: &ProjectId,
    include_status: Option<Vec<WarehouseStatus>>,
    catalog_state: E,
) -> Result<Vec<GetWarehouseResponse>> {
    #[derive(sqlx::FromRow, Debug, PartialEq)]
    struct WarehouseRecord {
        warehouse_id: uuid::Uuid,
        warehouse_name: String,
        storage_profile: Json<StorageProfile>,
        storage_secret_id: Option<uuid::Uuid>,
        status: WarehouseStatus,
        tabular_delete_mode: DbTabularDeleteProfile,
        tabular_expiration_seconds: Option<i64>,
        protected: bool,
    }

    let include_status = include_status.unwrap_or_else(|| vec![WarehouseStatus::Active]);
    let warehouses = sqlx::query_as!(
        WarehouseRecord,
        r#"
            SELECT 
                warehouse_id,
                warehouse_name,
                storage_profile as "storage_profile: Json<StorageProfile>",
                storage_secret_id,
                status AS "status: WarehouseStatus",
                tabular_delete_mode as "tabular_delete_mode: DbTabularDeleteProfile",
                tabular_expiration_seconds,
                protected
            FROM warehouse
            WHERE project_id = $1
            AND status = ANY($2)
            "#,
        project_id,
        include_status as Vec<WarehouseStatus>
    )
    .fetch_all(catalog_state)
    .await
    .map_err(|e| e.into_error_model("Error fetching warehouses"))?;

    warehouses
        .into_iter()
        .map(|warehouse| {
            let tabular_delete_profile = match warehouse.tabular_delete_mode {
                DbTabularDeleteProfile::Soft => TabularDeleteProfile::Soft {
                    expiration_seconds: chrono::Duration::seconds(
                        warehouse
                            .tabular_expiration_seconds
                            .ok_or(ErrorModel::internal(
                                "Tabular expiration seconds not found",
                                "TabularExpirationSecondsNotFound",
                                None,
                            ))?,
                    ),
                },
                DbTabularDeleteProfile::Hard => TabularDeleteProfile::Hard {},
            };

            Ok(GetWarehouseResponse {
                id: warehouse.warehouse_id.into(),
                name: warehouse.warehouse_name,
                project_id: project_id.clone(),
                storage_profile: warehouse.storage_profile.deref().clone(),
                storage_secret_id: warehouse.storage_secret_id.map(std::convert::Into::into),
                status: warehouse.status,
                tabular_delete_profile,
                protected: warehouse.protected,
            })
        })
        .collect::<Result<Vec<_>>>()
}

pub(crate) async fn get_warehouse(
    warehouse_id: WarehouseIdent,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Option<GetWarehouseResponse>> {
    let warehouse = sqlx::query!(
        r#"
        SELECT 
            warehouse_name,
            project_id,
            storage_profile as "storage_profile: Json<StorageProfile>",
            storage_secret_id,
            status AS "status: WarehouseStatus",
            tabular_delete_mode as "tabular_delete_mode: DbTabularDeleteProfile",
            tabular_expiration_seconds,
            protected
        FROM warehouse
        WHERE warehouse_id = $1
        "#,
        *warehouse_id
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(map_select_warehouse_err)?;

    if let Some(warehouse) = warehouse {
        let tabular_delete_profile = match warehouse.tabular_delete_mode {
            DbTabularDeleteProfile::Soft => TabularDeleteProfile::Soft {
                expiration_seconds: chrono::Duration::seconds(
                    warehouse
                        .tabular_expiration_seconds
                        .ok_or(ErrorModel::internal(
                            "Tabular expiration seconds not found",
                            "TabularExpirationSecondsNotFound",
                            None,
                        ))?,
                ),
            },
            DbTabularDeleteProfile::Hard => TabularDeleteProfile::Hard {},
        };

        Ok(Some(GetWarehouseResponse {
            id: warehouse_id,
            name: warehouse.warehouse_name,
            project_id: ProjectId::from_db_unchecked(warehouse.project_id),
            storage_profile: warehouse.storage_profile.deref().clone(),
            storage_secret_id: warehouse.storage_secret_id.map(std::convert::Into::into),
            status: warehouse.status,
            tabular_delete_profile,
            protected: warehouse.protected,
        }))
    } else {
        Ok(None)
    }
}

pub(crate) async fn list_projects<'e, 'c: 'e, E: sqlx::Executor<'c, Database = sqlx::Postgres>>(
    project_ids: Option<HashSet<ProjectId>>,
    connection: E,
) -> Result<Vec<GetProjectResponse>> {
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
            project_id: ProjectId::from_db_unchecked(project.project_id),
            name: project.project_name,
        })
        .collect())
}

pub(crate) async fn delete_warehouse(
    warehouse_id: WarehouseIdent,
    DeleteWarehouseQuery { force }: DeleteWarehouseQuery,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    let protected = sqlx::query_scalar!(
        r#"WITH delete_info as (
               SELECT
                   protected
               FROM warehouse
               WHERE warehouse_id = $1
           ),
           deleted as (DELETE FROM warehouse WHERE warehouse_id = $1 AND (not protected OR $2))
           SELECT protected as "protected!" FROM delete_info"#,
        *warehouse_id,
        force
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| match &e {
        sqlx::Error::RowNotFound => ErrorModel::not_found(
            format!("Warehouse '{warehouse_id}' not found"),
            "WarehouseNotFound",
            Some(Box::new(e)),
        ),
        sqlx::Error::Database(db_error) => {
            if db_error.is_foreign_key_violation() {
                ErrorModel::conflict(
                    "Warehouse is not empty",
                    "WarehouseNotEmpty",
                    Some(Box::new(e)),
                )
            } else {
                e.into_error_model("Error deleting warehouse")
            }
        }
        _ => e.into_error_model("Error deleting warehouse"),
    })?;

    if protected && !force {
        return Err(
            ErrorModel::conflict("Warehouse is protected", "WarehouseProtected", None).into(),
        );
    }

    Ok(())
}

pub(crate) async fn rename_warehouse(
    warehouse_id: WarehouseIdent,
    new_name: &str,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    let row_count = sqlx::query!(
        "UPDATE warehouse
            SET warehouse_name = $1
            WHERE warehouse_id = $2
            AND status = 'active'",
        new_name,
        *warehouse_id
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| e.into_error_model("Error renaming warehouse"))?
    .rows_affected();

    if row_count == 0 {
        return Err(ErrorModel::not_found("Warehouse not found", "WarehouseNotFound", None).into());
    }

    Ok(())
}

pub(crate) async fn set_warehouse_status(
    warehouse_id: WarehouseIdent,
    status: WarehouseStatus,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    let row_count = sqlx::query!(
        "UPDATE warehouse
            SET status = $1
            WHERE warehouse_id = $2",
        status as _,
        *warehouse_id
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| e.into_error_model("Error setting warehouse status"))?
    .rows_affected();

    if row_count == 0 {
        return Err(ErrorModel::not_found("Warehouse not found", "WarehouseNotFound", None).into());
    }

    Ok(())
}

pub(crate) async fn set_warehouse_protection(
    warehouse_id: WarehouseIdent,
    protected: bool,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<ProtectionResponse> {
    let row = sqlx::query!(
        "UPDATE warehouse
            SET protected = $1
            WHERE warehouse_id = $2
            returning protected, updated_at",
        protected,
        *warehouse_id
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| {
        if let sqlx::Error::RowNotFound = e {
            return ErrorModel::not_found(
                format!("Warehouse '{warehouse_id}' not found"),
                "WarehouseNotFound",
                Some(Box::new(e)),
            );
        }
        e.into_error_model("Error setting warehouse protection")
    })?;

    Ok(ProtectionResponse {
        protected: row.protected,
        updated_at: row.updated_at,
    })
}

pub(crate) async fn update_storage_profile(
    warehouse_id: WarehouseIdent,
    storage_profile: StorageProfile,
    storage_secret_id: Option<SecretIdent>,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    let storage_profile_ser = serde_json::to_value(storage_profile).map_err(|e| {
        ErrorModel::internal(
            "Error serializing storage profile",
            "StorageProfileSerializationError",
            Some(Box::new(e)),
        )
    })?;

    let row_count = sqlx::query!(
        r#"
            UPDATE warehouse
            SET storage_profile = $1, storage_secret_id = $2
            WHERE warehouse_id = $3
            AND status = 'active'
        "#,
        storage_profile_ser,
        storage_secret_id.map(|id| id.into_uuid()),
        *warehouse_id
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| e.into_error_model("Error updating storage profile"))?
    .rows_affected();

    if row_count == 0 {
        return Err(ErrorModel::not_found("Warehouse not found", "WarehouseNotFound", None).into());
    }

    Ok(())
}

fn map_select_warehouse_err(e: sqlx::Error) -> ErrorModel {
    ErrorModel::internal(
        "Error fetching warehouse",
        "WarehouseFetchError",
        Some(Box::new(e)),
    )
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

pub(crate) async fn get_warehouse_stats(
    conn: PgPool,
    warehouse_ident: WarehouseIdent,
    PaginationQuery {
        page_size,
        page_token,
    }: PaginationQuery,
) -> crate::api::Result<WarehouseStatisticsResponse> {
    let page_size = page_size.map_or(MAX_PAGE_SIZE, |i| i.clamp(1, MAX_PAGE_SIZE));

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
        warehouse_ident.0,
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
        warehouse_ident: *warehouse_ident,
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
        implementations::postgres::{PostgresCatalog, PostgresTransaction},
        service::{
            storage::{S3Flavor, S3Profile},
            Catalog as _, Transaction,
        },
    };

    pub(crate) async fn initialize_warehouse(
        state: CatalogState,
        storage_profile: Option<StorageProfile>,
        project_id: Option<&ProjectId>,
        secret_id: Option<SecretIdent>,
        create_project: bool,
    ) -> crate::WarehouseIdent {
        let project_id = project_id.map_or(
            ProjectId::from(uuid::Uuid::nil()),
            std::borrow::ToOwned::to_owned,
        );
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        if create_project {
            PostgresCatalog::create_project(
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

        let warehouse_id = PostgresCatalog::create_warehouse(
            "test_warehouse".to_string(),
            &project_id,
            storage_profile,
            TabularDeleteProfile::Soft {
                expiration_seconds: chrono::Duration::seconds(5),
            },
            secret_id,
            t.transaction(),
        )
        .await
        .unwrap();

        t.commit().await.unwrap();
        warehouse_id
    }

    #[sqlx::test]
    async fn test_get_warehouse_by_name(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;

        let fetched_warehouse_id = PostgresCatalog::get_warehouse_by_name(
            "test_warehouse",
            &ProjectId::from(uuid::Uuid::nil()),
            state.clone(),
        )
        .await
        .unwrap();

        assert_eq!(Some(warehouse_id), fetched_warehouse_id);
    }

    #[sqlx::test]
    async fn test_list_projects(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let project_id_1 = ProjectId::from(uuid::Uuid::new_v4());
        initialize_warehouse(state.clone(), None, Some(&project_id_1), None, true).await;

        let mut trx = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();

        let projects = PostgresCatalog::list_projects(None, trx.transaction())
            .await
            .unwrap()
            .into_iter()
            .map(|p| p.project_id)
            .collect::<Vec<_>>();
        trx.commit().await.unwrap();
        assert_eq!(projects.len(), 1);
        assert!(projects.contains(&project_id_1));

        let project_id_2 = ProjectId::from(uuid::Uuid::new_v4());
        initialize_warehouse(state.clone(), None, Some(&project_id_2), None, true).await;

        let mut trx = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();

        let projects = PostgresCatalog::list_projects(None, trx.transaction())
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

        let projects = PostgresCatalog::list_projects(
            Some(HashSet::from_iter(vec![project_id_1.clone()])),
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
        let warehouse_id_1 =
            initialize_warehouse(state.clone(), None, Some(&project_id), None, true).await;
        let mut trx = PostgresTransaction::begin_read(state).await.unwrap();

        let warehouses = PostgresCatalog::list_warehouses(&project_id, None, trx.transaction())
            .await
            .unwrap();
        trx.commit().await.unwrap();
        assert_eq!(warehouses.len(), 1);
        // Check ids
        assert!(warehouses.iter().any(|w| w.id == warehouse_id_1));
    }

    #[sqlx::test]
    async fn test_list_warehouses_active_filter(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectId::from(uuid::Uuid::new_v4());
        let warehouse_id_1 =
            initialize_warehouse(state.clone(), None, Some(&project_id), None, true).await;

        // Rename warehouse 1
        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        PostgresCatalog::rename_warehouse(warehouse_id_1, "new_name", transaction.transaction())
            .await
            .unwrap();
        PostgresCatalog::set_warehouse_status(
            warehouse_id_1,
            WarehouseStatus::Inactive,
            transaction.transaction(),
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        // Create warehouse 2
        let warehouse_id_2 =
            initialize_warehouse(state.clone(), None, Some(&project_id), None, false).await;
        let mut trx = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();

        // Assert active whs
        let warehouses = PostgresCatalog::list_warehouses(
            &project_id,
            Some(vec![WarehouseStatus::Active, WarehouseStatus::Inactive]),
            trx.transaction(),
        )
        .await
        .unwrap();
        trx.commit().await.unwrap();
        assert_eq!(warehouses.len(), 2);
        assert!(warehouses.iter().any(|w| w.id == warehouse_id_1));
        assert!(warehouses.iter().any(|w| w.id == warehouse_id_2));

        // Assert only active whs
        let mut trx = PostgresTransaction::begin_read(state).await.unwrap();

        let warehouses = PostgresCatalog::list_warehouses(&project_id, None, trx.transaction())
            .await
            .unwrap();
        trx.commit().await.unwrap();
        assert_eq!(warehouses.len(), 1);
        assert!(warehouses.iter().any(|w| w.id == warehouse_id_2));
    }

    #[sqlx::test]
    async fn test_rename_warehouse(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectId::from(uuid::Uuid::new_v4());
        let warehouse_id =
            initialize_warehouse(state.clone(), None, Some(&project_id), None, true).await;

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        PostgresCatalog::rename_warehouse(warehouse_id, "new_name", transaction.transaction())
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        let mut read_transaction = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();
        let warehouse =
            PostgresCatalog::get_warehouse(warehouse_id, read_transaction.transaction())
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
            PostgresCatalog::create_project(&project_id, "old_name".to_string(), t.transaction())
                .await
                .unwrap();
            t.commit().await.unwrap();
        }

        {
            let mut t = PostgresTransaction::begin_write(state.clone())
                .await
                .unwrap();
            PostgresCatalog::rename_project(&project_id, "new_name", t.transaction())
                .await
                .unwrap();
            t.commit().await.unwrap();
        }

        let mut read_transaction = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();
        let project = PostgresCatalog::get_project(&project_id, read_transaction.transaction())
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
        PostgresCatalog::create_project(&project_id, "old_name".to_string(), t.transaction())
            .await
            .unwrap();
        let err =
            PostgresCatalog::create_project(&project_id, "other_name".to_string(), t.transaction())
                .await
                .unwrap_err();
        assert_eq!(err.error.code, StatusCode::CONFLICT);
        t.commit().await.unwrap();
    }

    #[sqlx::test]
    async fn test_cannot_drop_protected_warehouse(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectId::from(uuid::Uuid::new_v4());
        let warehouse_id =
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
        assert_eq!(e.error.code, StatusCode::CONFLICT);
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
        let warehouse_id =
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
        let warehouse_id =
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
                warehouse_id.0,
                chrono::Utc::now() - chrono::Duration::hours(i)
            )
            .execute(&mut **t.transaction())
            .await
            .unwrap();
        }
        t.commit().await.unwrap();

        let stats = PostgresCatalog::get_warehouse_stats(
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

        let stats = PostgresCatalog::get_warehouse_stats(
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

        let stats = PostgresCatalog::get_warehouse_stats(
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

        let stats = PostgresCatalog::get_warehouse_stats(
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
        let stats = PostgresCatalog::get_warehouse_stats(
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
}
