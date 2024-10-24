use crate::api::iceberg::v1::PaginationQuery;
use crate::api::management::v1::role::{ListRolesResponse, Role, SearchRoleResponse};
use crate::implementations::postgres::dbutils::DBErrorHandler;
use crate::service::{Result, RoleId};
use crate::ProjectIdent;
use iceberg_ext::catalog::rest::ErrorModel;
use uuid::Uuid;

#[derive(sqlx::FromRow, Debug)]
struct RoleRow {
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub project_id: ProjectIdent,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl From<RoleRow> for Role {
    fn from(
        RoleRow {
            id,
            name,
            description,
            project_id,
            created_at,
            updated_at,
        }: RoleRow,
    ) -> Self {
        Self {
            id: RoleId::new(id),
            name,
            description,
            project_id,
            created_at,
            updated_at,
        }
    }
}

pub(crate) async fn create_role<'e, 'c: 'e, E: sqlx::Executor<'c, Database = sqlx::Postgres>>(
    role_id: RoleId,
    project_id: ProjectIdent,
    role_name: &str,
    description: Option<&str>,
    connection: E,
) -> Result<Role> {
    let role = sqlx::query_as!(
        RoleRow,
        r#"
        INSERT INTO role (id, name, description, project_id)
        VALUES ($1, $2, $3, $4)
        RETURNING id, name, description, project_id, created_at, updated_at
        "#,
        uuid::Uuid::from(role_id),
        role_name,
        description,
        uuid::Uuid::from(project_id)
    )
    .fetch_one(connection)
    .await
    .map_err(|e| match e {
        sqlx::Error::Database(db_error) => {
            if db_error.is_unique_violation() {
                ErrorModel::conflict(
                    format!("A role with this name or id already exists in project {project_id}")
                        .to_string(),
                    "RoleAlreadyExists".to_string(),
                    Some(Box::new(db_error)),
                )
            } else if db_error.is_foreign_key_violation() {
                ErrorModel::not_found(
                    format!("Project {project_id} not found").to_string(),
                    "ProjectNotFound".to_string(),
                    Some(Box::new(db_error)),
                )
            } else {
                ErrorModel::internal(
                    "Error creating Role".to_string(),
                    "RoleCreationFailed",
                    Some(Box::new(db_error)),
                )
            }
        }
        _ => e.into_error_model("Error creating Role".into()),
    })?;

    Ok(Role::from(role))
}

pub(crate) async fn update_role<'e, 'c: 'e, E: sqlx::Executor<'c, Database = sqlx::Postgres>>(
    role_id: RoleId,
    role_name: &str,
    description: Option<&str>,
    connection: E,
) -> Result<Option<Role>> {
    let role = sqlx::query_as!(
        RoleRow,
        r#"
        UPDATE role
        SET name = $2, description = $3
        WHERE id = $1
        RETURNING id, name, description, project_id, created_at, updated_at
        "#,
        uuid::Uuid::from(role_id),
        role_name,
        description
    )
    .fetch_one(connection)
    .await;

    match role {
        Err(sqlx::Error::RowNotFound) => Ok(None),
        Err(e) => Err(e.into_error_model("Error updating Role".to_string()).into()),
        Ok(role) => Ok(Some(Role::from(role))),
    }
}

pub(crate) async fn search_role<'e, 'c: 'e, E: sqlx::Executor<'c, Database = sqlx::Postgres>>(
    search_term: &str,
    connection: E,
) -> Result<SearchRoleResponse> {
    let roles = sqlx::query_as!(
        RoleRow,
        r#"
        SELECT id, name, description, project_id, created_at, updated_at
        FROM role
        ORDER BY name <-> $1 ASC
        LIMIT 10
        "#,
        search_term,
    )
    .fetch_all(connection)
    .await
    .map_err(|e| e.into_error_model("Error searching role".to_string()))?
    .into_iter()
    .map(Into::into)
    .collect();

    Ok(SearchRoleResponse { roles })
}

pub(crate) async fn list_roles<'e, 'c: 'e, E: sqlx::Executor<'c, Database = sqlx::Postgres>>(
    filter_project_id: Option<ProjectIdent>,
    filter_role_id: Option<Vec<RoleId>>,
    filter_name: Option<String>,
    _pagination: PaginationQuery,
    connection: E,
) -> Result<ListRolesResponse> {
    // TODO: impl pagination
    let filter_name = filter_name.unwrap_or_default();

    let roles = sqlx::query_as!(
        RoleRow,
        r#"
        SELECT
            id,
            name,
            description,
            project_id,
            created_at,
            updated_at
        FROM role
        WHERE ($1 OR project_id = $2)
            AND ($3 OR id = any($4))
            AND ($5 OR name ILIKE ('%' || $6 || '%'))
        "#,
        filter_project_id.is_none(),
        uuid::Uuid::from(filter_project_id.unwrap_or_default()),
        filter_role_id.is_none(),
        filter_role_id
            .unwrap_or_default()
            .into_iter()
            .map(|id| Uuid::from(id))
            .collect::<Vec<uuid::Uuid>>() as Vec<Uuid>,
        filter_name.is_empty(),
        filter_name.to_string()
    )
    .fetch_all(connection)
    .await
    .map_err(|e| e.into_error_model("Error fetching roles".to_string()))?
    .into_iter()
    .map(Role::from)
    .collect();

    Ok(ListRolesResponse {
        roles,
        next_page_token: None,
    })
}

pub(crate) async fn delete_role<'e, 'c: 'e, E: sqlx::Executor<'c, Database = sqlx::Postgres>>(
    role_id: RoleId,
    connection: E,
) -> Result<Option<()>> {
    let role = sqlx::query!(
        r#"
        DELETE FROM role
        WHERE id = $1
        RETURNING id
        "#,
        uuid::Uuid::from(role_id)
    )
    .fetch_optional(connection)
    .await
    .map_err(|e| e.into_error_model("Error deleting Role".to_string()))?;

    Ok(role.map(|_| ()))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::api::iceberg::v1::PageToken;
    use crate::implementations::postgres::{CatalogState, PostgresCatalog, PostgresTransaction};
    use crate::service::{Catalog, Transaction};

    #[sqlx::test]
    async fn test_create_role(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = uuid::Uuid::now_v7().into();
        let role_id = RoleId::default();
        let role_name = "Role 1";

        // Yield 404 on project not found
        let err = create_role(
            role_id,
            project_id,
            role_name,
            Some("Role 1 description"),
            &state.write_pool(),
        )
        .await
        .unwrap_err();
        assert_eq!(err.error.code, 404);

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        PostgresCatalog::create_project(
            project_id,
            format!("Project {project_id}"),
            t.transaction(),
        )
        .await
        .unwrap();

        t.commit().await.unwrap();

        let role = create_role(
            role_id,
            project_id,
            role_name,
            Some("Role 1 description"),
            &state.write_pool(),
        )
        .await
        .unwrap();

        assert_eq!(role.name, "Role 1");
        assert_eq!(role.description, Some("Role 1 description".to_string()));
        assert_eq!(role.project_id, project_id);

        // Duplicate name yields conflict (case-insensitive) (409)
        let new_role_id = RoleId::default();
        let err = create_role(
            new_role_id,
            project_id,
            role_name.to_lowercase().as_str(),
            Some("Role 1 description"),
            &state.write_pool(),
        )
        .await
        .unwrap_err();
        assert_eq!(err.error.code, 409);
    }

    #[sqlx::test]
    async fn test_rename_role(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = Uuid::now_v7().into();
        let role_id = RoleId::default();
        let role_name = "Role 1";

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        PostgresCatalog::create_project(
            project_id,
            format!("Project {project_id}"),
            t.transaction(),
        )
        .await
        .unwrap();

        t.commit().await.unwrap();

        let role = create_role(
            role_id,
            project_id,
            role_name,
            Some("Role 1 description"),
            &state.write_pool(),
        )
        .await
        .unwrap();

        assert_eq!(role.name, "Role 1");
        assert_eq!(role.description, Some("Role 1 description".to_string()));
        assert_eq!(role.project_id, project_id);

        let _updated_role = update_role(
            role_id,
            "Role 2",
            Some("Role 2 description"),
            &state.write_pool(),
        )
        .await
        .unwrap()
        .unwrap();
    }

    #[sqlx::test]
    async fn test_rename_role_conflicts(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = Uuid::now_v7().into();
        let role_id = RoleId::default();
        let role_name = "Role 1";
        let role_name_2 = "Role 2";

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        PostgresCatalog::create_project(
            project_id,
            format!("Project {project_id}"),
            t.transaction(),
        )
        .await
        .unwrap();

        t.commit().await.unwrap();

        let _role = create_role(
            role_id,
            project_id,
            role_name,
            Some("Role 1 description"),
            &state.write_pool(),
        )
        .await
        .unwrap();

        let role = create_role(
            RoleId::default(),
            project_id,
            role_name_2,
            Some("Role 2 description"),
            &state.write_pool(),
        )
        .await
        .unwrap();

        assert_eq!(role.name, "Role 2");
        assert_eq!(role.description, Some("Role 2 description".to_string()));
        assert_eq!(role.project_id, project_id);

        let err = update_role(
            role_id,
            role_name_2,
            Some("Role 2 description"),
            &state.write_pool(),
        )
        .await
        .unwrap_err();
        assert_eq!(err.error.code, 409);
    }

    #[sqlx::test]
    async fn test_list_roles(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project1_id = Uuid::now_v7().into();
        let project2_id = Uuid::now_v7().into();

        let role1_id = RoleId::default();
        let role2_id = RoleId::default();

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        PostgresCatalog::create_project(
            project1_id,
            format!("Project {project1_id}"),
            t.transaction(),
        )
        .await
        .unwrap();

        PostgresCatalog::create_project(
            project2_id,
            format!("Project {project2_id}"),
            t.transaction(),
        )
        .await
        .unwrap();

        t.commit().await.unwrap();

        create_role(
            role1_id,
            project1_id,
            "Role 1",
            Some("Role 1 description"),
            &state.write_pool(),
        )
        .await
        .unwrap();

        create_role(
            role2_id,
            project2_id,
            "Role 2",
            Some("Role 2 description"),
            &state.write_pool(),
        )
        .await
        .unwrap();

        let roles = list_roles(
            None,
            None,
            None,
            PaginationQuery {
                page_size: Some(10),
                page_token: PageToken::Empty,
            },
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert_eq!(roles.roles.len(), 2);

        // Project filter
        let roles = list_roles(
            Some(project1_id),
            None,
            None,
            PaginationQuery {
                page_size: Some(10),
                page_token: PageToken::Empty,
            },
            &state.read_pool(),
        )
        .await
        .unwrap();

        assert_eq!(roles.roles.len(), 1);
        assert_eq!(roles.roles[0].id, role1_id);

        // Role filter
        let roles = list_roles(
            None,
            Some(vec![role2_id]),
            None,
            PaginationQuery {
                page_size: Some(10),
                page_token: PageToken::Empty,
            },
            &state.read_pool(),
        )
        .await
        .unwrap();

        assert_eq!(roles.roles.len(), 1);
        assert_eq!(roles.roles[0].id, role2_id);

        // Name filter
        let roles = list_roles(
            None,
            None,
            Some("Role 1".to_string()),
            PaginationQuery {
                page_size: Some(10),
                page_token: PageToken::Empty,
            },
            &state.read_pool(),
        )
        .await
        .unwrap();

        assert_eq!(roles.roles.len(), 1);
        assert_eq!(roles.roles[0].id, role1_id);
    }

    #[sqlx::test]
    async fn test_delete_role(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = Uuid::now_v7().into();
        let role_id = RoleId::default();
        let role_name = "Role 1";

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        PostgresCatalog::create_project(
            project_id,
            format!("Project {project_id}"),
            t.transaction(),
        )
        .await
        .unwrap();

        t.commit().await.unwrap();

        create_role(
            role_id,
            project_id,
            role_name,
            Some("Role 1 description"),
            &state.write_pool(),
        )
        .await
        .unwrap();

        delete_role(role_id, &state.write_pool())
            .await
            .unwrap()
            .unwrap();

        let roles = list_roles(
            None,
            None,
            None,
            PaginationQuery {
                page_size: Some(10),
                page_token: PageToken::Empty,
            },
            &state.read_pool(),
        )
        .await
        .unwrap();

        assert_eq!(roles.roles.len(), 0);
    }

    #[sqlx::test]
    async fn test_search_role(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = Uuid::now_v7().into();
        let role_id = RoleId::default();
        let role_name = "Role 1";

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        PostgresCatalog::create_project(
            project_id,
            format!("Project {project_id}"),
            t.transaction(),
        )
        .await
        .unwrap();

        t.commit().await.unwrap();

        create_role(
            role_id,
            project_id,
            role_name,
            Some("Role 1 description"),
            &state.write_pool(),
        )
        .await
        .unwrap();

        let search_result = search_role("ro 1", &state.read_pool()).await.unwrap();
        assert_eq!(search_result.roles.len(), 1);
        assert_eq!(search_result.roles[0].name, role_name);
    }
}
