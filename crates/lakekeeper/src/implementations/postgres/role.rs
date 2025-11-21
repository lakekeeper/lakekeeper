use std::sync::Arc;

use uuid::Uuid;

use crate::{
    api::{
        iceberg::v1::PaginationQuery,
        management::v1::role::{ListRolesResponse, Role, SearchRoleResponse},
    },
    implementations::postgres::{
        dbutils::DBErrorHandler,
        pagination::{PaginateToken, V1PaginateToken},
    },
    service::{
        CreateRoleError, DeleteRoleError, ListRolesError, ProjectIdNotFoundError, Result,
        RoleExternalIdAlreadyExists, RoleId, RoleIdNotFound, RoleNameAlreadyExists,
        SearchRolesError, UpdateRoleError,
    },
    ProjectId, CONFIG,
};

#[derive(sqlx::FromRow, Debug)]
struct RoleRow {
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub project_id: String,
    pub external_id: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl From<RoleRow> for Role {
    fn from(
        RoleRow {
            id,
            name,
            description,
            external_id,
            project_id,
            created_at,
            updated_at,
        }: RoleRow,
    ) -> Self {
        Self {
            id: RoleId::new(id),
            name,
            description,
            project_id: ProjectId::from_db_unchecked(project_id),
            external_id,
            created_at,
            updated_at,
        }
    }
}

pub(crate) async fn create_role<'e, 'c: 'e, E: sqlx::Executor<'c, Database = sqlx::Postgres>>(
    role_id: RoleId,
    project_id: &ProjectId,
    role_name: &str,
    description: Option<&str>,
    external_id: Option<&str>,
    connection: E,
) -> Result<Role, CreateRoleError> {
    let role = sqlx::query_as!(
        RoleRow,
        r#"
        INSERT INTO role (id, name, description, project_id, external_id)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id, name, description, project_id, external_id, created_at, updated_at
        "#,
        uuid::Uuid::from(role_id),
        role_name,
        description,
        project_id,
        external_id
    )
    .fetch_one(connection)
    .await
    .map_err(|e| match &e {
        sqlx::Error::Database(db_error) => {
            if db_error.is_unique_violation() {
                match db_error.constraint() {
                    Some("unique_role_external_id_in_project") => {
                        let external_id = external_id.unwrap_or("");
                        CreateRoleError::from(RoleExternalIdAlreadyExists::new(
                            external_id,
                            project_id.clone(),
                        ))
                    }
                    Some("unique_role_name_in_project") => {
                        RoleNameAlreadyExists::new(role_name, project_id.clone()).into()
                    }
                    _ => e.into_catalog_backend_error().into(),
                }
            } else if db_error.is_foreign_key_violation() {
                ProjectIdNotFoundError::new(project_id.clone()).into()
            } else {
                e.into_catalog_backend_error().into()
            }
        }
        _ => e.into_catalog_backend_error().into(),
    })?;

    Ok(Role::from(role))
}

pub(crate) async fn update_role<'e, 'c: 'e, E: sqlx::Executor<'c, Database = sqlx::Postgres>>(
    project_id: &ProjectId,
    role_id: RoleId,
    role_name: &str,
    description: Option<&str>,
    connection: E,
) -> Result<Role, UpdateRoleError> {
    let role = sqlx::query_as!(
        RoleRow,
        r#"
        UPDATE role
        SET name = $2, description = $3
        WHERE id = $1 AND project_id = $4
        RETURNING id, name, description, project_id, external_id, created_at, updated_at
        "#,
        uuid::Uuid::from(role_id),
        role_name,
        description,
        project_id,
    )
    .fetch_one(connection)
    .await;

    match role {
        Err(sqlx::Error::RowNotFound) => Err(UpdateRoleError::from(RoleIdNotFound::new(
            role_id,
            project_id.clone(),
        ))),
        Err(e) => match &e {
            sqlx::Error::Database(db_error) => {
                if db_error.is_unique_violation() {
                    match db_error.constraint() {
                        Some("unique_role_name_in_project") => Err(UpdateRoleError::from(
                            RoleNameAlreadyExists::new(role_name, project_id.clone()),
                        )),
                        _ => Err(e.into_catalog_backend_error().into()),
                    }
                } else {
                    Err(e.into_catalog_backend_error().into())
                }
            }
            _ => Err(e.into_catalog_backend_error().into()),
        },
        Ok(role) => Ok(Role::from(role)),
    }
}

pub(crate) async fn update_role_external_id<
    'e,
    'c: 'e,
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
>(
    project_id: &ProjectId,
    role_id: RoleId,
    external_id: Option<&str>,
    connection: E,
) -> Result<Role, UpdateRoleError> {
    let role = sqlx::query_as!(
        RoleRow,
        r#"
        UPDATE role
        SET external_id = $2
        WHERE id = $1 AND project_id = $3
        RETURNING id, name, description, project_id, external_id, created_at, updated_at
        "#,
        uuid::Uuid::from(role_id),
        external_id,
        project_id,
    )
    .fetch_one(connection)
    .await;

    match role {
        Err(sqlx::Error::RowNotFound) => Err(UpdateRoleError::from(RoleIdNotFound::new(
            role_id,
            project_id.clone(),
        ))),
        Err(e) => match &e {
            sqlx::Error::Database(db_error) => {
                if db_error.is_unique_violation() {
                    match db_error.constraint() {
                        Some("unique_role_external_id_in_project") => {
                            Err(UpdateRoleError::from(RoleExternalIdAlreadyExists::new(
                                external_id.unwrap_or(""),
                                project_id.clone(),
                            )))
                        }
                        _ => Err(e.into_catalog_backend_error().into()),
                    }
                } else {
                    Err(e.into_catalog_backend_error().into())
                }
            }
            _ => Err(e.into_catalog_backend_error().into()),
        },
        Ok(role) => Ok(Role::from(role)),
    }
}

pub(crate) async fn search_role<'e, 'c: 'e, E: sqlx::Executor<'c, Database = sqlx::Postgres>>(
    project_id: &ProjectId,
    search_term: &str,
    connection: E,
) -> Result<SearchRoleResponse, SearchRolesError> {
    let roles = sqlx::query_as!(
        RoleRow,
        r#"
        SELECT id, name, description, project_id, external_id, created_at, updated_at
        FROM role
        WHERE project_id = $2
        ORDER BY 
            CASE 
                WHEN id::text = $1 THEN 1
                WHEN external_id = $1 THEN 2
                ELSE 3
            END,
            name <-> $1 ASC
        LIMIT 10
        "#,
        search_term,
        project_id
    )
    .fetch_all(connection)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?
    .into_iter()
    .map(|r| Arc::new(r.into()))
    .collect();

    Ok(SearchRoleResponse { roles })
}

pub(crate) async fn list_roles<'e, 'c: 'e, E: sqlx::Executor<'c, Database = sqlx::Postgres>>(
    filter_project_id: Option<ProjectId>,
    filter_role_id: Option<Vec<RoleId>>,
    filter_name: Option<String>,
    PaginationQuery {
        page_size,
        page_token,
    }: PaginationQuery,
    connection: E,
) -> Result<ListRolesResponse, ListRolesError> {
    let page_size = CONFIG.page_size_or_pagination_default(page_size);
    let filter_name = filter_name.unwrap_or_default();

    let token = page_token
        .as_option()
        .map(PaginateToken::try_from)
        .transpose()?;

    let (token_ts, token_id) = token
        .as_ref()
        .map(
            |PaginateToken::V1(V1PaginateToken { created_at, id }): &PaginateToken<Uuid>| {
                (created_at, id)
            },
        )
        .unzip();

    let roles = sqlx::query_as!(
        RoleRow,
        r#"
        SELECT
            id,
            name,
            description,
            project_id,
            external_id,
            created_at,
            updated_at
        FROM role r
        WHERE ($1 OR project_id = $2)
            AND ($3 OR id = any($4))
            AND ($5 OR name ILIKE ('%' || $6 || '%'))
            --- PAGINATION
            AND ((r.created_at > $7 OR $7 IS NULL) OR (r.created_at = $7 AND r.id > $8))
        ORDER BY r.created_at, r.id ASC
        LIMIT $9
        "#,
        filter_project_id.is_none(),
        &filter_project_id.unwrap_or(ProjectId::new_random()),
        filter_role_id.is_none(),
        filter_role_id
            .unwrap_or_default()
            .into_iter()
            .map(|id| Uuid::from(id))
            .collect::<Vec<uuid::Uuid>>() as Vec<Uuid>,
        filter_name.is_empty(),
        filter_name.clone(),
        token_ts,
        token_id,
        page_size,
    )
    .fetch_all(connection)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?
    .into_iter()
    .map(|r| Arc::new(Role::from(r)))
    .collect::<Vec<_>>();

    let next_page_token = roles.last().map(|r| {
        PaginateToken::V1(V1PaginateToken::<Uuid> {
            created_at: r.created_at,
            id: r.id.into(),
        })
        .to_string()
    });

    Ok(ListRolesResponse {
        roles,
        next_page_token,
    })
}

pub(crate) async fn delete_role<'e, 'c: 'e, E: sqlx::Executor<'c, Database = sqlx::Postgres>>(
    project_id: &ProjectId,
    role_id: RoleId,
    connection: E,
) -> Result<(), DeleteRoleError> {
    let role = sqlx::query!(
        r#"
        DELETE FROM role
        WHERE id = $1 AND project_id = $2
        RETURNING id
        "#,
        uuid::Uuid::from(role_id),
        project_id
    )
    .fetch_optional(connection)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;

    if role.is_none() {
        Err(DeleteRoleError::from(RoleIdNotFound::new(
            role_id,
            project_id.clone(),
        )))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        api::iceberg::v1::PageToken,
        implementations::postgres::{CatalogState, PostgresBackend, PostgresTransaction},
        service::{CatalogStore, Transaction},
    };

    #[sqlx::test]
    async fn test_create_role(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectId::new_random();
        let role_id = RoleId::new_random();
        let role_name = "Role 1";

        // Yield 404 on project not found
        let err = create_role(
            role_id,
            &project_id,
            role_name,
            Some("Role 1 description"),
            None,
            &state.write_pool(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, CreateRoleError::ProjectIdNotFoundError(_)));

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

        let role = create_role(
            role_id,
            &project_id,
            role_name,
            Some("Role 1 description"),
            Some("external-1"),
            &state.write_pool(),
        )
        .await
        .unwrap();

        assert_eq!(role.name, "Role 1");
        assert_eq!(role.description, Some("Role 1 description".to_string()));
        assert_eq!(role.project_id, project_id);
        assert_eq!(role.external_id, Some("external-1".to_string()));

        // Duplicate name yields conflict (case-insensitive) (409)
        let new_role_id = RoleId::new_random();
        let err = create_role(
            new_role_id,
            &project_id,
            role_name.to_lowercase().as_str(),
            Some("Role 1 description"),
            None,
            &state.write_pool(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, CreateRoleError::RoleNameAlreadyExists(_)));
    }

    #[sqlx::test]
    async fn test_rename_role(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectId::new_random();
        let role_id = RoleId::new_random();
        let role_name = "Role 1";

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

        let role = create_role(
            role_id,
            &project_id,
            role_name,
            Some("Role 1 description"),
            None,
            &state.write_pool(),
        )
        .await
        .unwrap();

        assert_eq!(role.name, "Role 1");
        assert_eq!(role.description, Some("Role 1 description".to_string()));
        assert_eq!(role.project_id, project_id);
        assert_eq!(role.external_id, None);

        let updated_role = update_role(
            &project_id,
            role_id,
            "Role 2",
            Some("Role 2 description"),
            &state.write_pool(),
        )
        .await
        .unwrap();
        assert_eq!(updated_role.name, "Role 2");
        assert_eq!(
            updated_role.description,
            Some("Role 2 description".to_string())
        );
        assert_eq!(updated_role.project_id, project_id);
    }

    #[sqlx::test]
    async fn test_rename_role_conflicts(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectId::new_random();
        let role_id = RoleId::new_random();
        let role_name = "Role 1";
        let role_name_2 = "Role 2";

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

        let _role = create_role(
            role_id,
            &project_id,
            role_name,
            Some("Role 1 description"),
            Some("external-1"),
            &state.write_pool(),
        )
        .await
        .unwrap();

        let role = create_role(
            RoleId::new_random(),
            &project_id,
            role_name_2,
            Some("Role 2 description"),
            Some("external-2"),
            &state.write_pool(),
        )
        .await
        .unwrap();

        assert_eq!(role.name, "Role 2");
        assert_eq!(role.description, Some("Role 2 description".to_string()));
        assert_eq!(role.project_id, project_id);

        let err = update_role(
            &project_id,
            role_id,
            role_name_2,
            Some("Role 2 description"),
            &state.write_pool(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, UpdateRoleError::RoleNameAlreadyExists(_)));
    }

    #[sqlx::test]
    async fn test_set_role_external_id(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectId::new_random();
        let role_id = RoleId::new_random();
        let role_name = "Role 1";

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

        let role = create_role(
            role_id,
            &project_id,
            role_name,
            Some("Role 1 description"),
            None,
            &state.write_pool(),
        )
        .await
        .unwrap();

        assert_eq!(role.name, "Role 1");
        assert_eq!(role.description, Some("Role 1 description".to_string()));
        assert_eq!(role.project_id, project_id);
        assert_eq!(role.external_id, None);

        let updated_role = update_role_external_id(
            &project_id,
            role_id,
            Some("external-2"),
            &state.write_pool(),
        )
        .await
        .unwrap();
        assert_eq!(updated_role.name, "Role 1");
        assert_eq!(
            updated_role.description,
            Some("Role 1 description".to_string())
        );
        assert_eq!(updated_role.project_id, project_id);
        assert_eq!(updated_role.external_id, Some("external-2".to_string()));

        // Create new role with same external id yields conflict
        let new_role_id = RoleId::new_random();
        let err = create_role(
            new_role_id,
            &project_id,
            "Role 2",
            Some("Role 2 description"),
            Some("external-2"),
            &state.write_pool(),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            CreateRoleError::RoleExternalIdAlreadyExists(_)
        ));

        // Create a new role with different external id and set to existing external id yields conflict
        let another_role_id = RoleId::new_random();
        let _another_role = create_role(
            another_role_id,
            &project_id,
            "Role 3",
            Some("Role 3 description"),
            Some("external-3"),
            &state.write_pool(),
        )
        .await
        .unwrap();
        let err = update_role_external_id(
            &project_id,
            another_role_id,
            Some("external-2"),
            &state.write_pool(),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            UpdateRoleError::RoleExternalIdAlreadyExists(_)
        ));
    }

    #[sqlx::test]
    async fn test_list_roles(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project1_id = ProjectId::new_random();
        let project2_id = ProjectId::new_random();

        let role1_id = RoleId::new_random();
        let role2_id = RoleId::new_random();

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        PostgresBackend::create_project(
            &project1_id,
            format!("Project {project1_id}"),
            t.transaction(),
        )
        .await
        .unwrap();

        PostgresBackend::create_project(
            &project2_id,
            format!("Project {project2_id}"),
            t.transaction(),
        )
        .await
        .unwrap();

        t.commit().await.unwrap();

        create_role(
            role1_id,
            &project1_id,
            "Role 1",
            Some("Role 1 description"),
            None,
            &state.write_pool(),
        )
        .await
        .unwrap();

        create_role(
            role2_id,
            &project2_id,
            "Role 2",
            Some("Role 2 description"),
            None,
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
    async fn test_paginate_roles(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project1_id = ProjectId::new_random();

        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        PostgresBackend::create_project(
            &project1_id,
            format!("Project {project1_id}"),
            t.transaction(),
        )
        .await
        .unwrap();

        t.commit().await.unwrap();

        for i in 0..10 {
            create_role(
                RoleId::new_random(),
                &project1_id,
                &format!("Role-{i}"),
                Some(&format!("Role-{i} description")),
                None,
                &state.write_pool(),
            )
            .await
            .unwrap();
        }

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
        assert_eq!(roles.roles.len(), 10);

        let roles = list_roles(
            None,
            None,
            None,
            PaginationQuery {
                page_size: Some(5),
                page_token: PageToken::Empty,
            },
            &state.read_pool(),
        )
        .await
        .unwrap();

        assert_eq!(roles.roles.len(), 5);

        for (idx, r) in roles.roles.iter().enumerate() {
            assert_eq!(r.name, format!("Role-{idx}"));
        }

        let roles = list_roles(
            None,
            None,
            None,
            PaginationQuery {
                page_size: Some(5),
                page_token: roles.next_page_token.into(),
            },
            &state.read_pool(),
        )
        .await
        .unwrap();

        assert_eq!(roles.roles.len(), 5);
        for (idx, r) in roles.roles.iter().enumerate() {
            assert_eq!(r.name, format!("Role-{}", idx + 5));
        }

        let roles = list_roles(
            None,
            None,
            None,
            PaginationQuery {
                page_size: Some(5),
                page_token: roles.next_page_token.into(),
            },
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert_eq!(roles.roles.len(), 0);
        assert!(roles.next_page_token.is_none());
    }

    #[sqlx::test]
    async fn test_delete_role(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectId::new_random();
        let role_id = RoleId::new_random();
        let role_name = "Role 1";

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

        create_role(
            role_id,
            &project_id,
            role_name,
            Some("Role 1 description"),
            None,
            &state.write_pool(),
        )
        .await
        .unwrap();

        delete_role(&project_id, role_id, &state.write_pool())
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

        assert_eq!(roles.roles.len(), 0);
    }

    #[sqlx::test]
    async fn test_search_role(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectId::new_random();
        let role_id = RoleId::new_random();
        let role_name = "Role 1";

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

        create_role(
            role_id,
            &project_id,
            role_name,
            Some("Role 1 description"),
            None,
            &state.write_pool(),
        )
        .await
        .unwrap();

        let search_result = search_role(&project_id, "ro 1", &state.read_pool())
            .await
            .unwrap();
        assert_eq!(search_result.roles.len(), 1);
        assert_eq!(search_result.roles[0].name, role_name);
    }
}
