use super::dbutils::DBErrorHandler as _;
use crate::api::{CatalogConfig, ErrorModel, Result};
use crate::service::{GetProjectResponse, GetWarehouseResponse, WarehouseStatus};
use crate::{service::storage::StorageProfile, ProjectIdent, SecretIdent, WarehouseIdent};
use http::StatusCode;
use sqlx::Error;
use std::collections::HashSet;
use std::ops::Deref;

use super::CatalogState;
use crate::api::management::v1::warehouse::TabularDeleteProfile;
use sqlx::types::Json;

pub(super) async fn get_warehouse_by_name(
    warehouse_name: &str,
    project_id: ProjectIdent,
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
        *project_id
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
    .map_err(|e| e.into_error_model("Error setting warehouse deletion profile".into()))?
    .rows_affected();

    if row_count == 0 {
        return Err(ErrorModel::not_found("Warehouse not found", "WarehouseNotFound", None).into());
    }

    Ok(())
}

pub(super) async fn get_config_for_warehouse(
    warehouse_id: WarehouseIdent,
    catalog_state: CatalogState,
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

    Ok(storage_profile.map(|p| p.generate_catalog_config(warehouse_id)))
}

pub(crate) async fn create_warehouse<'a>(
    warehouse_name: String,
    project_id: ProjectIdent,
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
        r#"
        INSERT INTO warehouse (warehouse_name, project_id, storage_profile, storage_secret_id, "status", tabular_expiration_seconds, tabular_delete_mode)
        VALUES ($1, $2, $3, $4, 'active', $5, $6)
        RETURNING warehouse_id
        "#,
        warehouse_name,
        *project_id,
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
            Some("unique_warehouse_name_in_project") => ErrorModel::conflict("Warehouse with this name already exists in the project.",
                "WarehouseNameAlreadyExists", Some(Box::new(e)))
                ,
            Some("warehouse_project_id_fk") => ErrorModel::not_found("Project not found", "ProjectNotFound", Some(Box::new(e))),
            _ => e.into_error_model("Error creating Warehouse".into()),
        },
        _ => e.into_error_model("Error creating Warehouse".into()),
    })?;

    Ok(warehouse_id.into())
}

pub(crate) async fn rename_project<'a>(
    project_id: ProjectIdent,
    new_name: &str,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    let row_count = sqlx::query!(
        "UPDATE project
            SET project_name = $1
            WHERE project_id = $2",
        new_name,
        *project_id
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| e.into_error_model("Error renaming project".into()))?
    .rows_affected();

    if row_count == 0 {
        return Err(ErrorModel::not_found("Project not found", "ProjectNotFound", None).into());
    }

    Ok(())
}

pub(crate) async fn create_project<'a>(
    project_id: ProjectIdent,
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
        *project_id
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| e.into_error_model("Error creating Project".into()))?
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

pub(crate) async fn get_project<'a>(
    project_id: ProjectIdent,
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
        *project_id
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
            project_id: project.project_id.into(),
            name: project.project_name,
        }))
    } else {
        Ok(None)
    }
}

pub(crate) async fn delete_project<'a>(
    project_id: ProjectIdent,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    let row_count =
        sqlx::query_scalar!(r#"DELETE FROM project WHERE project_id = $1"#, *project_id)
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
                        e.into_error_model("Error deleting project".into())
                    }
                }
                _ => e.into_error_model("Error deleting project".into()),
            })?
            .rows_affected();

    if row_count == 0 {
        return Err(ErrorModel::not_found("Project not found", "ProjectNotFound", None).into());
    }

    Ok(())
}

pub(crate) async fn list_warehouses(
    project_id: ProjectIdent,
    include_status: Option<Vec<WarehouseStatus>>,
    catalog_state: CatalogState,
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
                tabular_expiration_seconds
            FROM warehouse
            WHERE project_id = $1
            AND status = ANY($2)
            "#,
        *project_id,
        include_status as Vec<WarehouseStatus>
    )
    .fetch_all(&catalog_state.read_pool())
    .await
    .map_err(|e| e.into_error_model("Error fetching warehouses".into()))?;

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
                project_id,
                storage_profile: warehouse.storage_profile.deref().clone(),
                storage_secret_id: warehouse.storage_secret_id.map(std::convert::Into::into),
                status: warehouse.status,
                tabular_delete_profile,
            })
        })
        .collect::<Result<Vec<_>>>()
}

pub(crate) async fn get_warehouse<'a>(
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
            tabular_expiration_seconds
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
            project_id: ProjectIdent::from(warehouse.project_id),
            storage_profile: warehouse.storage_profile.deref().clone(),
            storage_secret_id: warehouse.storage_secret_id.map(std::convert::Into::into),
            status: warehouse.status,
            tabular_delete_profile,
        }))
    } else {
        Ok(None)
    }
}

pub(crate) async fn list_projects(
    project_ids: Option<HashSet<ProjectIdent>>,
    catalog_state: CatalogState,
) -> Result<Vec<GetProjectResponse>> {
    let return_all = project_ids.is_none();
    let projects = sqlx::query!(
        r#"
        SELECT project_id, project_name FROM project WHERE project_id = ANY($1) or $2
        "#,
        project_ids
            .map(|ids| ids.into_iter().map(|i| *i).collect::<Vec<_>>())
            .unwrap_or_default() as Vec<uuid::Uuid>,
        return_all
    )
    .fetch_all(&catalog_state.read_pool())
    .await
    .map_err(|e| e.into_error_model("Error fetching projects".into()))?;

    Ok(projects
        .into_iter()
        .map(|project| GetProjectResponse {
            project_id: ProjectIdent::from(project.project_id),
            name: project.project_name,
        })
        .collect())
}

pub(crate) async fn delete_warehouse<'a>(
    warehouse_id: WarehouseIdent,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    let row_count = sqlx::query_scalar!(
        r#"DELETE FROM warehouse WHERE warehouse_id = $1"#,
        *warehouse_id
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| match &e {
        sqlx::Error::Database(db_error) => {
            if db_error.is_foreign_key_violation() {
                ErrorModel::conflict(
                    "Warehouse is not empty",
                    "WarehouseNotEmpty",
                    Some(Box::new(e)),
                )
            } else {
                e.into_error_model("Error deleting warehouse".into())
            }
        }
        _ => e.into_error_model("Error deleting warehouse".into()),
    })?
    .rows_affected();

    if row_count == 0 {
        return Err(ErrorModel::not_found("Warehouse not found", "WarehouseNotFound", None).into());
    }

    Ok(())
}

pub(crate) async fn rename_warehouse<'a>(
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
    .map_err(|e| e.into_error_model("Error renaming warehouse".into()))?
    .rows_affected();

    if row_count == 0 {
        return Err(ErrorModel::not_found("Warehouse not found", "WarehouseNotFound", None).into());
    }

    Ok(())
}

pub(crate) async fn set_warehouse_status<'a>(
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
    .map_err(|e| e.into_error_model("Error setting warehouse status".into()))?
    .rows_affected();

    if row_count == 0 {
        return Err(ErrorModel::not_found("Warehouse not found", "WarehouseNotFound", None).into());
    }

    Ok(())
}

pub(crate) async fn update_storage_profile<'a>(
    warehouse_id: WarehouseIdent,
    storage_profile: StorageProfile,
    storage_secret_id: Option<SecretIdent>,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    let storage_profile_ser = serde_json::to_value(storage_profile).map_err(|e| {
        ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message("Error serializing storage profile".to_string())
            .r#type("StorageProfileSerializationError".to_string())
            .source(Some(Box::new(e)))
            .build()
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
    .map_err(|e| e.into_error_model("Error updating storage profile".into()))?
    .rows_affected();

    if row_count == 0 {
        return Err(ErrorModel::not_found("Warehouse not found", "WarehouseNotFound", None).into());
    }

    Ok(())
}

fn map_select_warehouse_err(e: Error) -> ErrorModel {
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

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::implementations::postgres::PostgresCatalog;
    use crate::service::storage::S3Flavor;
    use crate::service::Catalog as _;
    use crate::{
        implementations::postgres::PostgresTransaction,
        service::{storage::S3Profile, Transaction as _},
    };

    pub(crate) async fn initialize_warehouse(
        state: CatalogState,
        storage_profile: Option<StorageProfile>,
        project_id: Option<&ProjectIdent>,
        secret_id: Option<SecretIdent>,
        create_project: bool,
    ) -> crate::WarehouseIdent {
        let project_id = project_id.map_or(
            ProjectIdent::from(uuid::Uuid::nil()),
            std::borrow::ToOwned::to_owned,
        );
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        if create_project {
            PostgresCatalog::create_project(
                project_id,
                format!("Project {project_id}"),
                t.transaction(),
            )
            .await
            .unwrap();
        }

        let storage_profile = storage_profile.unwrap_or(StorageProfile::S3(S3Profile {
            bucket: "test_bucket".to_string(),
            endpoint: None,
            region: "us-east-1".to_string(),
            assume_role_arn: None,
            path_style_access: None,
            key_prefix: None,
            sts_role_arn: None,
            sts_enabled: false,
            flavor: S3Flavor::Minio,
        }));

        let warehouse_id = PostgresCatalog::create_warehouse(
            "test_warehouse".to_string(),
            project_id,
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
            ProjectIdent::from(uuid::Uuid::nil()),
            state.clone(),
        )
        .await
        .unwrap();

        assert_eq!(Some(warehouse_id), fetched_warehouse_id);
    }

    #[sqlx::test]
    async fn test_list_projects(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id_1 = ProjectIdent::from(uuid::Uuid::new_v4());
        initialize_warehouse(state.clone(), None, Some(&project_id_1), None, true).await;

        let projects = PostgresCatalog::list_projects(None, state.clone())
            .await
            .unwrap()
            .into_iter()
            .map(|p| p.project_id)
            .collect::<Vec<_>>();
        assert_eq!(projects.len(), 1);
        assert!(projects.contains(&project_id_1));

        let project_id_2 = ProjectIdent::from(uuid::Uuid::new_v4());
        initialize_warehouse(state.clone(), None, Some(&project_id_2), None, true).await;

        let projects = PostgresCatalog::list_projects(None, state.clone())
            .await
            .unwrap()
            .into_iter()
            .map(|p| p.project_id)
            .collect::<Vec<_>>();
        assert_eq!(projects.len(), 2);
        assert!(projects.contains(&project_id_1));
        assert!(projects.contains(&project_id_2));

        let projects = PostgresCatalog::list_projects(
            Some(HashSet::from_iter(vec![project_id_1])),
            state.clone(),
        )
        .await
        .unwrap()
        .into_iter()
        .map(|p| p.project_id)
        .collect::<Vec<_>>();

        assert_eq!(projects.len(), 1);
        assert!(projects.contains(&project_id_1));
    }

    #[sqlx::test]
    async fn test_list_warehouses(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectIdent::from(uuid::Uuid::new_v4());
        let warehouse_id_1 =
            initialize_warehouse(state.clone(), None, Some(&project_id), None, true).await;

        let warehouses = PostgresCatalog::list_warehouses(project_id, None, state.clone())
            .await
            .unwrap();
        assert_eq!(warehouses.len(), 1);
        // Check ids
        assert!(warehouses.iter().any(|w| w.id == warehouse_id_1));
    }

    #[sqlx::test]
    async fn test_list_warehouses_active_filter(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectIdent::from(uuid::Uuid::new_v4());
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

        // Assert active whs
        let warehouses = PostgresCatalog::list_warehouses(
            project_id,
            Some(vec![WarehouseStatus::Active, WarehouseStatus::Inactive]),
            state.clone(),
        )
        .await
        .unwrap();
        assert_eq!(warehouses.len(), 2);
        assert!(warehouses.iter().any(|w| w.id == warehouse_id_1));
        assert!(warehouses.iter().any(|w| w.id == warehouse_id_2));

        // Assert only active whs
        let warehouses = PostgresCatalog::list_warehouses(project_id, None, state.clone())
            .await
            .unwrap();
        assert_eq!(warehouses.len(), 1);
        assert!(warehouses.iter().any(|w| w.id == warehouse_id_2));
    }

    #[sqlx::test]
    async fn test_rename_warehouse(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectIdent::from(uuid::Uuid::new_v4());
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
        let project_id = ProjectIdent::from(uuid::Uuid::new_v4());
        {
            let mut t = PostgresTransaction::begin_write(state.clone())
                .await
                .unwrap();
            PostgresCatalog::create_project(project_id, "old_name".to_string(), t.transaction())
                .await
                .unwrap();
            t.commit().await.unwrap();
        }

        {
            let mut t = PostgresTransaction::begin_write(state.clone())
                .await
                .unwrap();
            PostgresCatalog::rename_project(project_id, "new_name", t.transaction())
                .await
                .unwrap();
            t.commit().await.unwrap();
        }

        let mut read_transaction = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();
        let project = PostgresCatalog::get_project(project_id, read_transaction.transaction())
            .await
            .unwrap();
        assert_eq!(project.unwrap().name, "new_name");
    }

    #[sqlx::test]
    async fn test_same_project_id(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectIdent::from(uuid::Uuid::new_v4());
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        PostgresCatalog::create_project(project_id, "old_name".to_string(), t.transaction())
            .await
            .unwrap();
        let err =
            PostgresCatalog::create_project(project_id, "other_name".to_string(), t.transaction())
                .await
                .unwrap_err();
        assert_eq!(err.error.code, StatusCode::CONFLICT);
        t.commit().await.unwrap();
    }
}
