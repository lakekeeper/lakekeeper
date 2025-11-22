use std::sync::Arc;

use itertools::Itertools;
use uuid::Uuid;

use crate::{
    api::{
        iceberg::v1::PaginationQuery,
        management::v1::role::{
            ListRolesResponse, Role, SearchRoleResponse, UpdateRoleSourceSystemRequest,
        },
    },
    implementations::postgres::{
        dbutils::DBErrorHandler,
        pagination::{PaginateToken, V1PaginateToken},
    },
    service::{
        CatalogBackendError, CatalogCreateRoleRequest, CatalogListRolesFilter, CreateRoleError,
        ListRolesError, ProjectIdNotFoundError, Result, RoleId, RoleIdNotFound,
        RoleNameAlreadyExists, RoleSourceIdConflict, SearchRolesError, UpdateRoleError,
    },
    ProjectId, CONFIG,
};

#[derive(sqlx::FromRow, Debug)]
struct RoleRow {
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub project_id: String,
    pub source_id: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl From<RoleRow> for Role {
    fn from(
        RoleRow {
            id,
            name,
            description,
            source_id,
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
            source_id,
            created_at,
            updated_at,
        }
    }
}

pub(crate) async fn create_roles<'e, 'c: 'e, E: sqlx::Executor<'c, Database = sqlx::Postgres>>(
    project_id: &ProjectId,
    roles_to_create: Vec<CatalogCreateRoleRequest<'_>>,
    connection: E,
) -> Result<Vec<Role>, CreateRoleError> {
    if roles_to_create.is_empty() {
        return Ok(Vec::new());
    }

    #[allow(clippy::type_complexity)]
    let (role_ids, role_names, descriptions, source_ids): (
        Vec<Uuid>,
        Vec<&str>,
        Vec<Option<&str>>,
        Vec<Option<&str>>,
    ) = roles_to_create
        .into_iter()
        .map(
            |CatalogCreateRoleRequest {
                 role_id,
                 role_name,
                 description,
                 source_id,
             }| { (*role_id, role_name, description, source_id) },
        )
        .multiunzip();

    let roles = sqlx::query_as!(
        RoleRow,
        r#"
        INSERT INTO role (id, name, description, source_id, project_id)
        SELECT u.*, $5 FROM UNNEST($1::UUID[], $2::TEXT[], $3::TEXT[], $4::TEXT[]) u
        RETURNING id, name, description, project_id, source_id,created_at, updated_at
        "#,
        &role_ids,
        &role_names as &Vec<_>,
        &descriptions as &Vec<_>,
        &source_ids as &Vec<_>,
        &*project_id,
    )
    .fetch_all(connection)
    .await
    .map_err(|e| match &e {
        sqlx::Error::Database(db_error) => {
            if db_error.is_unique_violation() {
                match db_error.constraint() {
                    Some("unique_role_source_id_per_project") => {
                        CreateRoleError::from(RoleSourceIdConflict::new())
                    }
                    Some("unique_role_name_in_project") => RoleNameAlreadyExists::new().into(),
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

    Ok(roles.into_iter().map(Role::from).collect())
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
        RETURNING id, name, description, project_id, source_id, created_at, updated_at
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
                        Some("unique_role_name_in_project") => {
                            Err(UpdateRoleError::from(RoleNameAlreadyExists::new()))
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

pub(crate) async fn update_role_source_system<
    'e,
    'c: 'e,
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
>(
    project_id: &ProjectId,
    role_id: RoleId,
    request: &UpdateRoleSourceSystemRequest,
    connection: E,
) -> Result<Role, UpdateRoleError> {
    let role = sqlx::query_as!(
        RoleRow,
        r#"
        UPDATE role
        SET source_id = $3
        WHERE id = $1 AND project_id = $2
        RETURNING id, name, description, project_id, source_id, created_at, updated_at
        "#,
        uuid::Uuid::from(role_id),
        project_id,
        request.source_id
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
                        Some("unique_role_source_id_per_project") => {
                            Err(UpdateRoleError::from(RoleSourceIdConflict::new()))
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
        SELECT id, name, description, project_id, source_id, created_at, updated_at
        FROM role
        WHERE project_id = $2
        ORDER BY 
            CASE 
                WHEN id::text = $1 THEN 1
                WHEN source_id = $1 THEN 2
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
    project_id: &ProjectId,
    filter: CatalogListRolesFilter<'_>,
    PaginationQuery {
        page_size,
        page_token,
    }: PaginationQuery,
    connection: E,
) -> Result<ListRolesResponse, ListRolesError> {
    let page_size = CONFIG.page_size_or_pagination_default(page_size);

    let CatalogListRolesFilter {
        role_ids,
        source_ids,
    } = filter;

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

    let role_id_filter = role_ids.map(|ids| ids.iter().map(|r| **r).collect::<Vec<Uuid>>());

    let roles = sqlx::query_as!(
        RoleRow,
        r#"
        SELECT
            id,
            name,
            description,
            project_id,
            source_id,
            created_at,
            updated_at
        FROM role r
        WHERE project_id = $1
            AND ($2 OR id = any($3))
            AND ($4 OR source_id = any($5))
            --- PAGINATION
            AND ((r.created_at > $6 OR $6 IS NULL) OR (r.created_at = $6 AND r.id > $7))
        ORDER BY r.created_at, r.id ASC
        LIMIT $8
        "#,
        &project_id,
        role_id_filter.is_none(),
        &role_id_filter.unwrap_or_default(),
        source_ids.is_none(),
        source_ids.unwrap_or(&[]) as &[&str],
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

pub(crate) async fn delete_roles<'e, 'c: 'e, E: sqlx::Executor<'c, Database = sqlx::Postgres>>(
    project_id: &ProjectId,
    role_id_filter: Option<&[RoleId]>,
    source_id_filter: Option<&[&str]>,
    connection: E,
) -> Result<Vec<RoleId>, CatalogBackendError> {
    let role_id_filter = role_id_filter.map(|ids| ids.iter().map(|r| **r).collect::<Vec<Uuid>>());

    let deleted_ids = sqlx::query_scalar!(
        r#"
        DELETE FROM role
        WHERE project_id = $1
        AND ($2 OR id = ANY($3::UUID[]))
        AND ($4 OR source_id = ANY($5::TEXT[]))
        RETURNING id
        "#,
        project_id,
        role_id_filter.is_none(),
        &role_id_filter.unwrap_or_default(),
        source_id_filter.is_none(),
        &source_id_filter.unwrap_or(&[]) as &[&str]
    )
    .fetch_all(connection)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;

    Ok(deleted_ids.into_iter().map(Into::into).collect())
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

        let err = create_roles(
            &project_id,
            vec![CatalogCreateRoleRequest::builder()
                .role_id(role_id)
                .role_name(role_name)
                .description(Some("Role 1 description"))
                .build()],
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

        let roles = create_roles(
            &project_id,
            vec![CatalogCreateRoleRequest::builder()
                .role_id(role_id)
                .role_name(role_name)
                .description(Some("Role 1 description"))
                .source_id(Some("source-id"))
                .build()],
            &state.write_pool(),
        )
        .await
        .unwrap();
        let role = &roles[0];

        assert_eq!(role.name, "Role 1");
        assert_eq!(role.description, Some("Role 1 description".to_string()));
        assert_eq!(role.project_id, project_id);
        assert_eq!(role.source_id, Some("source-id".to_string()));

        // Duplicate name yields conflict (case-insensitive) (409)
        let new_role_id = RoleId::new_random();
        let err = create_roles(
            &project_id,
            vec![CatalogCreateRoleRequest::builder()
                .role_id(new_role_id)
                .role_name(&role_name.to_lowercase())
                .description(Some("Role 1 description"))
                .build()],
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

        let roles = create_roles(
            &project_id,
            vec![CatalogCreateRoleRequest::builder()
                .role_id(role_id)
                .role_name(role_name)
                .description(Some("Role 1 description"))
                .build()],
            &state.write_pool(),
        )
        .await
        .unwrap();
        let role = &roles[0];

        assert_eq!(role.name, "Role 1");
        assert_eq!(role.description, Some("Role 1 description".to_string()));
        assert_eq!(role.project_id, project_id);
        assert_eq!(role.source_id, None);

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

        create_roles(
            &project_id,
            vec![CatalogCreateRoleRequest::builder()
                .role_id(role_id)
                .role_name(role_name)
                .description(Some("Role 1 description"))
                .source_id(Some("external-1"))
                .build()],
            &state.write_pool(),
        )
        .await
        .unwrap();

        let roles = create_roles(
            &project_id,
            vec![CatalogCreateRoleRequest::builder()
                .role_id(RoleId::new_random())
                .role_name(role_name_2)
                .description(Some("Role 2 description"))
                .source_id(Some("external-2"))
                .build()],
            &state.write_pool(),
        )
        .await
        .unwrap();
        let role = &roles[0];

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
    async fn test_set_role_source_system(pool: sqlx::PgPool) {
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

        let roles = create_roles(
            &project_id,
            vec![CatalogCreateRoleRequest::builder()
                .role_id(role_id)
                .role_name(role_name)
                .description(Some("Role 1 description"))
                .build()],
            &state.write_pool(),
        )
        .await
        .unwrap();
        let role = &roles[0];

        assert_eq!(role.name, "Role 1");
        assert_eq!(role.description, Some("Role 1 description".to_string()));
        assert_eq!(role.project_id, project_id);
        assert_eq!(role.source_id, None);

        let updated_role = update_role_source_system(
            &project_id,
            role_id,
            &UpdateRoleSourceSystemRequest {
                source_id: "external-2".to_string(),
            },
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
        assert_eq!(updated_role.source_id, Some("external-2".to_string()));

        // Create new role with same external id yields conflict
        let new_role_id = RoleId::new_random();
        let err = create_roles(
            &project_id,
            vec![CatalogCreateRoleRequest::builder()
                .role_id(new_role_id)
                .role_name("Role 2")
                .description(Some("Role 2 description"))
                .source_id(Some("external-2"))
                .build()],
            &state.write_pool(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, CreateRoleError::RoleSourceIdConflict(_)));

        // Create a new role with different external id and set to existing external id yields conflict
        let another_role_id = RoleId::new_random();
        create_roles(
            &project_id,
            vec![CatalogCreateRoleRequest::builder()
                .role_id(another_role_id)
                .role_name("Role 3")
                .description(Some("Role 3 description"))
                .source_id(Some("external-3"))
                .build()],
            &state.write_pool(),
        )
        .await
        .unwrap();
        let err = update_role_source_system(
            &project_id,
            another_role_id,
            &UpdateRoleSourceSystemRequest {
                source_id: "external-2".to_string(),
            },
            &state.write_pool(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, UpdateRoleError::RoleSourceIdConflict(_)));
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

        create_roles(
            &project1_id,
            vec![CatalogCreateRoleRequest::builder()
                .role_id(role1_id)
                .role_name("Role 1")
                .description(None)
                .build()],
            &state.write_pool(),
        )
        .await
        .unwrap();

        create_roles(
            &project2_id,
            vec![CatalogCreateRoleRequest::builder()
                .role_id(role2_id)
                .role_name("Role 2")
                .description(Some("Role 2 description"))
                .build()],
            &state.write_pool(),
        )
        .await
        .unwrap();

        let roles = list_roles(
            &project1_id,
            CatalogListRolesFilter::builder().build(),
            PaginationQuery {
                page_size: Some(10),
                page_token: PageToken::Empty,
            },
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert_eq!(roles.roles.len(), 1);

        let roles = list_roles(
            &project2_id,
            CatalogListRolesFilter::builder().build(),
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
            create_roles(
                &project1_id,
                vec![CatalogCreateRoleRequest::builder()
                    .role_id(RoleId::new_random())
                    .role_name(&format!("Role-{i}"))
                    .description(Some(&format!("Role-{i} description")))
                    .build()],
                &state.write_pool(),
            )
            .await
            .unwrap();
        }

        let roles = list_roles(
            &project1_id,
            CatalogListRolesFilter::builder().build(),
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
            &project1_id,
            CatalogListRolesFilter::builder().build(),
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
            &project1_id,
            CatalogListRolesFilter::builder().build(),
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
            &project1_id,
            CatalogListRolesFilter::builder().build(),
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

        create_roles(
            &project_id,
            vec![CatalogCreateRoleRequest::builder()
                .role_id(role_id)
                .role_name(role_name)
                .description(Some("Role 1 description"))
                .build()],
            &state.write_pool(),
        )
        .await
        .unwrap();

        delete_roles(&project_id, Some(&[role_id]), None, &state.write_pool())
            .await
            .unwrap();

        let roles = list_roles(
            &project_id,
            CatalogListRolesFilter::builder().build(),
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
    async fn test_delete_roles_by_source_id(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
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

        // Create roles with different source IDs
        create_roles(
            &project_id,
            vec![
                CatalogCreateRoleRequest::builder()
                    .role_id(RoleId::new_random())
                    .role_name("Role 1")
                    .source_id(Some("external-1"))
                    .build(),
                CatalogCreateRoleRequest::builder()
                    .role_id(RoleId::new_random())
                    .role_name("Role 2")
                    .source_id(Some("external-2"))
                    .build(),
                CatalogCreateRoleRequest::builder()
                    .role_id(RoleId::new_random())
                    .role_name("Role 3")
                    .source_id(Some("external-3"))
                    .build(),
            ],
            &state.write_pool(),
        )
        .await
        .unwrap();

        // Delete roles with specific source IDs
        let deleted = delete_roles(
            &project_id,
            None,
            Some(&["external-1", "external-3"]),
            &state.write_pool(),
        )
        .await
        .unwrap();

        assert_eq!(deleted.len(), 2);

        // Verify only role with external-2 remains
        let roles = list_roles(
            &project_id,
            CatalogListRolesFilter::builder().build(),
            PaginationQuery {
                page_size: Some(10),
                page_token: PageToken::Empty,
            },
            &state.read_pool(),
        )
        .await
        .unwrap();

        assert_eq!(roles.roles.len(), 1);
        assert_eq!(roles.roles[0].source_id, Some("external-2".to_string()));
    }

    #[sqlx::test]
    async fn test_delete_roles_with_role_id_and_source_filters(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectId::new_random();
        let role_id_1 = RoleId::new_random();
        let role_id_2 = RoleId::new_random();

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

        // Create roles
        create_roles(
            &project_id,
            vec![
                CatalogCreateRoleRequest::builder()
                    .role_id(role_id_1)
                    .role_name("Role 1")
                    .source_id(Some("id-1"))
                    .build(),
                CatalogCreateRoleRequest::builder()
                    .role_id(role_id_2)
                    .role_name("Role 2")
                    .source_id(Some("id-2"))
                    .build(),
            ],
            &state.write_pool(),
        )
        .await
        .unwrap();

        // Delete with both role_id and source_system filter (both must match)
        let deleted = delete_roles(&project_id, Some(&[role_id_1]), None, &state.write_pool())
            .await
            .unwrap();

        assert_eq!(deleted.len(), 1);
        assert_eq!(deleted[0], role_id_1);

        // Verify role 2 remains
        let roles = list_roles(
            &project_id,
            CatalogListRolesFilter::builder().build(),
            PaginationQuery {
                page_size: Some(10),
                page_token: PageToken::Empty,
            },
            &state.read_pool(),
        )
        .await
        .unwrap();

        assert_eq!(roles.roles.len(), 1);
        assert_eq!(roles.roles[0].id, role_id_2);
    }

    #[sqlx::test]
    async fn test_delete_roles_respects_project_boundary(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project1_id = ProjectId::new_random();
        let project2_id = ProjectId::new_random();

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

        // Create roles in both projects with same source system
        create_roles(
            &project1_id,
            vec![CatalogCreateRoleRequest::builder()
                .role_id(RoleId::new_random())
                .role_name("Role 1")
                .build()],
            &state.write_pool(),
        )
        .await
        .unwrap();

        create_roles(
            &project2_id,
            vec![CatalogCreateRoleRequest::builder()
                .role_id(RoleId::new_random())
                .role_name("Role 2")
                .build()],
            &state.write_pool(),
        )
        .await
        .unwrap();

        // Delete from project 1
        let deleted = delete_roles(&project1_id, None, None, &state.write_pool())
            .await
            .unwrap();

        assert_eq!(deleted.len(), 1);

        // Verify project 2 role still exists
        let roles = list_roles(
            &project2_id,
            CatalogListRolesFilter::builder().build(),
            PaginationQuery {
                page_size: Some(10),
                page_token: PageToken::Empty,
            },
            &state.read_pool(),
        )
        .await
        .unwrap();

        assert_eq!(roles.roles.len(), 1);
        assert_eq!(roles.roles[0].name, "Role 2");
    }

    #[sqlx::test]
    async fn test_list_roles_filter_by_source_ids(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
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

        // Create roles with different source IDs
        create_roles(
            &project_id,
            vec![
                CatalogCreateRoleRequest::builder()
                    .role_id(RoleId::new_random())
                    .role_name("Role 1")
                    .source_id(Some("external-1"))
                    .build(),
                CatalogCreateRoleRequest::builder()
                    .role_id(RoleId::new_random())
                    .role_name("Role 2")
                    .source_id(Some("external-2"))
                    .build(),
                CatalogCreateRoleRequest::builder()
                    .role_id(RoleId::new_random())
                    .role_name("Role 3")
                    .source_id(Some("external-3"))
                    .build(),
            ],
            &state.write_pool(),
        )
        .await
        .unwrap();

        // Filter by specific source IDs
        let roles = list_roles(
            &project_id,
            CatalogListRolesFilter::builder()
                .source_ids(Some(&["external-1", "external-3"]))
                .build(),
            PaginationQuery {
                page_size: Some(10),
                page_token: PageToken::Empty,
            },
            &state.read_pool(),
        )
        .await
        .unwrap();

        assert_eq!(roles.roles.len(), 2);
        assert!(roles
            .roles
            .iter()
            .any(|r| r.name == "Role 1" && r.source_id == Some("external-1".to_string())));
        assert!(roles
            .roles
            .iter()
            .any(|r| r.name == "Role 3" && r.source_id == Some("external-3".to_string())));

        // Filter by single source ID
        let roles = list_roles(
            &project_id,
            CatalogListRolesFilter::builder()
                .source_ids(Some(&["external-2"]))
                .build(),
            PaginationQuery {
                page_size: Some(10),
                page_token: PageToken::Empty,
            },
            &state.read_pool(),
        )
        .await
        .unwrap();

        assert_eq!(roles.roles.len(), 1);
        assert_eq!(roles.roles[0].name, "Role 2");
        assert_eq!(roles.roles[0].source_id, Some("external-2".to_string()));
    }

    #[sqlx::test]
    async fn test_list_roles_filter_by_role_ids_and_source_filters(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectId::new_random();
        let role1_id = RoleId::new_random();
        let role2_id = RoleId::new_random();
        let role3_id = RoleId::new_random();

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

        // Create roles
        create_roles(
            &project_id,
            vec![
                CatalogCreateRoleRequest::builder()
                    .role_id(role1_id)
                    .role_name("Role 1")
                    .source_id(Some("id-1"))
                    .build(),
                CatalogCreateRoleRequest::builder()
                    .role_id(role2_id)
                    .role_name("Role 2")
                    .source_id(Some("id-2"))
                    .build(),
                CatalogCreateRoleRequest::builder()
                    .role_id(role3_id)
                    .role_name("Role 3")
                    .source_id(Some("id-3"))
                    .build(),
            ],
            &state.write_pool(),
        )
        .await
        .unwrap();

        // Filter by role_ids and source_system (both must match)
        let roles = list_roles(
            &project_id,
            CatalogListRolesFilter::builder()
                .source_ids(Some(&["id-1", "id-2"]))
                .build(),
            PaginationQuery {
                page_size: Some(10),
                page_token: PageToken::Empty,
            },
            &state.read_pool(),
        )
        .await
        .unwrap();

        assert_eq!(roles.roles.len(), 2);
        assert!(roles.roles.iter().any(|r| r.id == role1_id));
        assert!(roles.roles.iter().any(|r| r.id == role2_id));

        // Filter by role_ids and source_ids
        let roles = list_roles(
            &project_id,
            CatalogListRolesFilter::builder()
                .role_ids(Some(&[role1_id, role3_id]))
                .source_ids(Some(&["id-1"]))
                .build(),
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

        // Filter with all three filters
        let roles = list_roles(
            &project_id,
            CatalogListRolesFilter::builder()
                .role_ids(Some(&[role1_id, role2_id, role3_id]))
                .source_ids(Some(&["id-2"]))
                .build(),
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

        create_roles(
            &project_id,
            vec![CatalogCreateRoleRequest::builder()
                .role_id(role_id)
                .role_name(role_name)
                .description(Some("Role 1 description"))
                .build()],
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

    #[sqlx::test]
    async fn test_batch_create_roles_with_mixed_optional_fields(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
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

        // Create multiple roles with different combinations of optional fields
        let roles = create_roles(
            &project_id,
            vec![
                // All fields present
                CatalogCreateRoleRequest::builder()
                    .role_id(RoleId::new_random())
                    .role_name("Role 1")
                    .description(Some("Description 1"))
                    .source_id(Some("external-1"))
                    .build(),
                // No description, has source_id
                CatalogCreateRoleRequest::builder()
                    .role_id(RoleId::new_random())
                    .role_name("Role 2")
                    .source_id(Some("external-2"))
                    .build(),
                // Has description, no source_id
                CatalogCreateRoleRequest::builder()
                    .role_id(RoleId::new_random())
                    .role_name("Role 3")
                    .description(Some("Description 3"))
                    .build(),
                // No description, no source_id (all optional fields None)
                CatalogCreateRoleRequest::builder()
                    .role_id(RoleId::new_random())
                    .role_name("Role 4")
                    .build(),
            ],
            &state.write_pool(),
        )
        .await
        .unwrap();

        assert_eq!(roles.len(), 4);

        // Verify first role (all fields present)
        assert_eq!(roles[0].name, "Role 1");
        assert_eq!(roles[0].description, Some("Description 1".to_string()));
        assert_eq!(roles[0].source_id, Some("external-1".to_string()));

        // Verify second role (no description)
        assert_eq!(roles[1].name, "Role 2");
        assert_eq!(roles[1].description, None);
        assert_eq!(roles[1].source_id, Some("external-2".to_string()));

        // Verify third role (no source_id)
        assert_eq!(roles[2].name, "Role 3");
        assert_eq!(roles[2].description, Some("Description 3".to_string()));
        assert_eq!(roles[2].source_id, None);

        // Verify fourth role (all optional fields None)
        assert_eq!(roles[3].name, "Role 4");
        assert_eq!(roles[3].description, None);
        assert_eq!(roles[3].source_id, None);
    }

    #[sqlx::test]
    async fn test_batch_create_roles_all_none_fields(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
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

        // Create multiple roles with all optional fields as None
        let roles = create_roles(
            &project_id,
            vec![
                CatalogCreateRoleRequest::builder()
                    .role_id(RoleId::new_random())
                    .role_name("MinimalRole1")
                    .build(),
                CatalogCreateRoleRequest::builder()
                    .role_id(RoleId::new_random())
                    .role_name("MinimalRole2")
                    .build(),
                CatalogCreateRoleRequest::builder()
                    .role_id(RoleId::new_random())
                    .role_name("MinimalRole3")
                    .build(),
            ],
            &state.write_pool(),
        )
        .await
        .unwrap();

        assert_eq!(roles.len(), 3);

        for (i, role) in roles.iter().enumerate() {
            assert_eq!(role.name, format!("MinimalRole{}", i + 1));
            assert_eq!(role.description, None);
            assert_eq!(role.source_id, None);
            assert_eq!(role.project_id, project_id);
        }
    }

    #[sqlx::test]
    async fn test_batch_create_roles_conflict_in_batch(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
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

        // Try to create roles with duplicate names in the same batch
        let err = create_roles(
            &project_id,
            vec![
                CatalogCreateRoleRequest::builder()
                    .role_id(RoleId::new_random())
                    .role_name("DuplicateName")
                    .build(),
                CatalogCreateRoleRequest::builder()
                    .role_id(RoleId::new_random())
                    .role_name("DuplicateName")
                    .build(),
            ],
            &state.write_pool(),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, CreateRoleError::RoleNameAlreadyExists(_)));
    }

    #[sqlx::test]
    async fn test_batch_create_roles_duplicate_source_id_in_batch(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
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

        // Try to create roles with duplicate source_id in the same batch
        let err = create_roles(
            &project_id,
            vec![
                CatalogCreateRoleRequest::builder()
                    .role_id(RoleId::new_random())
                    .role_name("Role1")
                    .source_id(Some("duplicate-external-id"))
                    .build(),
                CatalogCreateRoleRequest::builder()
                    .role_id(RoleId::new_random())
                    .role_name("Role2")
                    .source_id(Some("duplicate-external-id"))
                    .build(),
            ],
            &state.write_pool(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, CreateRoleError::RoleSourceIdConflict(_)));
    }

    #[sqlx::test]
    async fn test_batch_create_single_role(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
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

        // Batch create with single role (edge case)
        let roles = create_roles(
            &project_id,
            vec![CatalogCreateRoleRequest::builder()
                .role_id(RoleId::new_random())
                .role_name("SingleRole")
                .build()],
            &state.write_pool(),
        )
        .await
        .unwrap();

        assert_eq!(roles.len(), 1);
        assert_eq!(roles[0].name, "SingleRole");
        assert_eq!(roles[0].description, None);
        assert_eq!(roles[0].source_id, None);
    }
}
