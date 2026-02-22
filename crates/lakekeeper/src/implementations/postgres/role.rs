use std::sync::Arc;

use itertools::Itertools;
use uuid::Uuid;

use crate::{
    CONFIG, ProjectId,
    api::{iceberg::v1::PaginationQuery, management::v1::role::UpdateRoleSourceSystemRequest},
    implementations::postgres::{
        dbutils::DBErrorHandler,
        pagination::{PaginateToken, V1PaginateToken},
    },
    service::{
        CatalogBackendError, CatalogCreateRoleRequest, CatalogListRolesByIdFilter, CreateRoleError,
        ListRolesError, ListRolesResponse, ProjectIdNotFoundError, Result, Role, RoleId,
        RoleIdNotFoundInProject, RoleIdent, RoleNameAlreadyExists, RoleSourceIdConflict,
        RoleVersion, SearchRoleResponse, SearchRolesError, UpdateRoleError,
    },
};

#[derive(sqlx::FromRow, Debug)]
struct RoleRow {
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub project_id: String,
    pub provider_id: String,
    pub source_id: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
    pub version: i64,
}

impl From<RoleRow> for Role {
    fn from(
        RoleRow {
            id,
            name,
            description,
            source_id,
            provider_id,
            project_id,
            created_at,
            updated_at,
            version,
        }: RoleRow,
    ) -> Self {
        Self {
            id: RoleId::new(id),
            name,
            description,
            project_id: Arc::new(ProjectId::from_db_unchecked(project_id)),
            ident: Arc::new(RoleIdent::from_db_unchecked(provider_id, source_id)),
            created_at,
            updated_at,
            version: RoleVersion::from(version),
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
    let (role_ids, role_names, descriptions, source_ids, provider_ids): (
        Vec<Uuid>,
        Vec<&str>,
        Vec<Option<&str>>,
        Vec<&str>,
        Vec<&str>,
    ) = roles_to_create
        .into_iter()
        .map(
            |CatalogCreateRoleRequest {
                 role_id,
                 role_name,
                 description,
                 source_id,
                 provider_id,
             }| {
                (
                    *role_id,
                    role_name,
                    description,
                    source_id.as_str(),
                    provider_id.as_str(),
                )
            },
        )
        .multiunzip();

    let roles = sqlx::query_as!(
        RoleRow,
        r#"
        INSERT INTO role (id, name, description, source_id, provider_id, project_id)
        SELECT u.*, $6 FROM UNNEST($1::UUID[], $2::TEXT[], $3::TEXT[], $4::TEXT[], $5::TEXT[]) u
        RETURNING id, name, description, project_id, provider_id, source_id, created_at, updated_at, version
        "#,
        &role_ids,
        &role_names as &Vec<_>,
        &descriptions as &Vec<_>,
        &source_ids as &Vec<_>,
        &provider_ids as &Vec<_>,
        &*project_id,
    )
    .fetch_all(connection)
    .await
    .map_err(|e| match &e {
        sqlx::Error::Database(db_error) => {
            if db_error.is_unique_violation() {
                match db_error.constraint() {
                    Some("unique_role_provider_source_in_project") => {
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
        RETURNING id, name, description, project_id, provider_id, source_id, created_at, updated_at, version
        "#,
        uuid::Uuid::from(role_id),
        role_name,
        description,
        project_id,
    )
    .fetch_one(connection)
    .await;

    match role {
        Err(sqlx::Error::RowNotFound) => Err(UpdateRoleError::from(RoleIdNotFoundInProject::new(
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
    let UpdateRoleSourceSystemRequest {
        source_id,
        provider_id,
    } = request;

    let role = sqlx::query_as!(
        RoleRow,
        r#"
        UPDATE role
        SET source_id = $3, provider_id = $4
        WHERE id = $1 AND project_id = $2
        RETURNING id, name, description, project_id, provider_id, source_id, created_at, updated_at, version
        "#,
        uuid::Uuid::from(role_id),
        project_id,
        source_id.as_str(),
        provider_id.as_str()
    )
    .fetch_one(connection)
    .await;

    match role {
        Err(sqlx::Error::RowNotFound) => Err(UpdateRoleError::from(RoleIdNotFoundInProject::new(
            role_id,
            project_id.clone(),
        ))),
        Err(e) => match &e {
            sqlx::Error::Database(db_error) => {
                if db_error.is_unique_violation() {
                    match db_error.constraint() {
                        Some("unique_role_provider_source_in_project") => {
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
        SELECT id, name, description, project_id, provider_id, source_id, created_at, updated_at, version
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
    project_id: Option<&ProjectId>,
    filter: CatalogListRolesByIdFilter<'_>,
    PaginationQuery {
        page_size,
        page_token,
    }: PaginationQuery,
    connection: E,
) -> Result<ListRolesResponse, ListRolesError> {
    let page_size = CONFIG.page_size_or_pagination_default(page_size);

    let CatalogListRolesByIdFilter {
        role_ids,
        source_ids,
        provider_ids,
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
    let source_ids_filter = source_ids
        .map(|ids| ids.iter().map(|i| i.as_str()).collect::<Vec<_>>())
        .unwrap_or_default();
    let provider_ids_filter = provider_ids
        .map(|ids| ids.iter().map(|i| i.as_str()).collect::<Vec<_>>())
        .unwrap_or_default();

    let roles = sqlx::query_as!(
        RoleRow,
        r#"
        SELECT
            id,
            name,
            description,
            project_id,
            provider_id,
            source_id,
            created_at,
            updated_at,
            version
        FROM role r
        WHERE ($9 or project_id = $1)
            AND ($2 OR id = any($3))
            AND ($4 OR source_id = any($5))
            AND ($10 OR provider_id = any($11))
            --- PAGINATION
            AND ((r.created_at > $6 OR $6 IS NULL) OR (r.created_at = $6 AND r.id > $7))
        ORDER BY r.created_at, r.id ASC
        LIMIT $8
        "#,
        &project_id.map(ProjectId::as_str).unwrap_or_default(),
        role_id_filter.is_none(),
        &role_id_filter.unwrap_or_default(),
        source_ids.is_none(),
        &source_ids_filter as &[&str],
        token_ts,
        token_id,
        page_size,
        project_id.is_none(),
        provider_ids.is_none(),
        &provider_ids_filter as &[&str]
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
    connection: E,
) -> Result<Vec<RoleId>, CatalogBackendError> {
    let role_id_filter = role_id_filter.map(|ids| ids.iter().map(|r| **r).collect::<Vec<Uuid>>());

    let deleted_ids = sqlx::query_scalar!(
        r#"
        DELETE FROM role
        WHERE project_id = $1
        AND ($2 OR id = ANY($3::UUID[]))
        RETURNING id
        "#,
        project_id,
        role_id_filter.is_none(),
        &role_id_filter.unwrap_or_default()
    )
    .fetch_all(connection)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;

    Ok(deleted_ids.into_iter().map(Into::into).collect())
}

pub(crate) async fn list_roles_by_idents<
    'e,
    'c: 'e,
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
>(
    project_id: &ProjectId,
    idents: &[&RoleIdent],
    connection: E,
) -> Result<Vec<Role>, CatalogBackendError> {
    if idents.is_empty() {
        return Ok(Vec::new());
    }

    let providers: Vec<&str> = idents.iter().map(|i| i.provider_id().as_str()).collect();
    let source_ids: Vec<&str> = idents.iter().map(|i| i.source_id().as_str()).collect();

    sqlx::query_as!(
        RoleRow,
        r#"
        SELECT id, name, description, project_id, provider_id, source_id, created_at, updated_at, version
        FROM role
        WHERE project_id = $1
          AND EXISTS (
              SELECT 1 FROM UNNEST($2::TEXT[], $3::TEXT[]) AS u(p, s)
              WHERE u.p = provider_id AND u.s = source_id
          )
        "#,
        project_id,
        &providers as &[&str],
        &source_ids as &[&str],
    )
    .fetch_all(connection)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)
    .map(|rows| rows.into_iter().map(Role::from).collect())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        api::iceberg::v1::PageToken,
        implementations::postgres::{CatalogState, PostgresBackend, PostgresTransaction},
        service::{CatalogStore, RoleProviderId, RoleSourceId, Transaction},
    };

    #[sqlx::test]
    async fn test_create_role(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectId::new_random();
        let role_id = RoleId::new_random();
        let role_name = "Role 1";

        let source_id = RoleSourceId::new_from_role_id(role_id);
        let provider_id = RoleProviderId::default();
        let err = create_roles(
            &project_id,
            vec![
                CatalogCreateRoleRequest::builder()
                    .role_id(role_id)
                    .role_name(role_name)
                    .description(Some("Role 1 description"))
                    .source_id(&source_id)
                    .provider_id(&provider_id)
                    .build(),
            ],
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

        let source_id: RoleSourceId = "source-id".parse().unwrap();
        let provider_id = RoleProviderId::default();
        let roles = create_roles(
            &project_id,
            vec![
                CatalogCreateRoleRequest::builder()
                    .role_id(role_id)
                    .role_name(role_name)
                    .description(Some("Role 1 description"))
                    .source_id(&source_id)
                    .provider_id(&provider_id)
                    .build(),
            ],
            &state.write_pool(),
        )
        .await
        .unwrap();
        let role = &roles[0];

        assert_eq!(role.name, "Role 1");
        assert_eq!(role.description, Some("Role 1 description".to_string()));
        assert_eq!(&*role.project_id, &project_id);
        assert_eq!(role.source_id(), &source_id);

        // Duplicate name yields conflict (case-insensitive) (409)
        let new_role_id = RoleId::new_random();
        let new_source_id = RoleSourceId::new_from_role_id(new_role_id);
        let err = create_roles(
            &project_id,
            vec![
                CatalogCreateRoleRequest::builder()
                    .role_id(new_role_id)
                    .role_name(&role_name.to_lowercase())
                    .description(Some("Role 1 description"))
                    .source_id(&new_source_id)
                    .provider_id(&provider_id)
                    .build(),
            ],
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

        let source_id = RoleSourceId::new_from_role_id(role_id);
        let provider_id = RoleProviderId::default();
        let roles = create_roles(
            &project_id,
            vec![
                CatalogCreateRoleRequest::builder()
                    .role_id(role_id)
                    .role_name(role_name)
                    .description(Some("Role 1 description"))
                    .source_id(&source_id)
                    .provider_id(&provider_id)
                    .build(),
            ],
            &state.write_pool(),
        )
        .await
        .unwrap();
        let role = &roles[0];

        assert_eq!(role.name, "Role 1");
        assert_eq!(role.description, Some("Role 1 description".to_string()));
        assert_eq!(&*role.project_id, &project_id);
        assert_eq!(role.source_id().as_str(), role_id.to_string());

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
        assert_eq!(&*updated_role.project_id, &project_id);
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

        let source_id: RoleSourceId = "source-id".parse().unwrap();
        let provider_id = RoleProviderId::default();
        create_roles(
            &project_id,
            vec![
                CatalogCreateRoleRequest::builder()
                    .role_id(role_id)
                    .role_name(role_name)
                    .description(Some("Role 1 description"))
                    .source_id(&source_id)
                    .provider_id(&provider_id)
                    .build(),
            ],
            &state.write_pool(),
        )
        .await
        .unwrap();

        let source_id_2: RoleSourceId = "source-id-2".parse().unwrap();
        let roles = create_roles(
            &project_id,
            vec![
                CatalogCreateRoleRequest::builder()
                    .role_id(RoleId::new_random())
                    .role_name(role_name_2)
                    .description(Some("Role 2 description"))
                    .source_id(&source_id_2)
                    .provider_id(&provider_id)
                    .build(),
            ],
            &state.write_pool(),
        )
        .await
        .unwrap();
        let role = &roles[0];

        assert_eq!(role.name, "Role 2");
        assert_eq!(role.description, Some("Role 2 description".to_string()));
        assert_eq!(&*role.project_id, &project_id);

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

        let source_id = RoleSourceId::new_from_role_id(role_id);
        let provider_id = RoleProviderId::default();
        let roles = create_roles(
            &project_id,
            vec![
                CatalogCreateRoleRequest::builder()
                    .role_id(role_id)
                    .role_name(role_name)
                    .description(Some("Role 1 description"))
                    .source_id(&source_id)
                    .provider_id(&provider_id)
                    .build(),
            ],
            &state.write_pool(),
        )
        .await
        .unwrap();
        let role = &roles[0];

        assert_eq!(role.name, "Role 1");
        assert_eq!(role.description, Some("Role 1 description".to_string()));
        assert_eq!(&*role.project_id, &project_id);
        assert_eq!(role.source_id().as_str(), role_id.to_string());

        let external_source_id: RoleSourceId = "external-2".parse().unwrap();
        let external_provider_id: RoleProviderId = "external".parse().unwrap();
        let updated_role = update_role_source_system(
            &project_id,
            role_id,
            &UpdateRoleSourceSystemRequest {
                source_id: external_source_id.clone(),
                provider_id: external_provider_id.clone(),
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
        assert_eq!(&*updated_role.project_id, &project_id);
        assert_eq!(updated_role.source_id(), &external_source_id);

        // Create new role with same external id yields conflict
        let new_role_id = RoleId::new_random();
        let err = create_roles(
            &project_id,
            vec![
                CatalogCreateRoleRequest::builder()
                    .role_id(new_role_id)
                    .role_name("Role 2")
                    .description(Some("Role 2 description"))
                    .source_id(&external_source_id)
                    .provider_id(&external_provider_id)
                    .build(),
            ],
            &state.write_pool(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, CreateRoleError::RoleSourceIdConflict(_)));

        // Create a new role with different external id and set to existing external id yields conflict
        let another_role_id = RoleId::new_random();
        let another_source_id: RoleSourceId = "external-3".parse().unwrap();
        create_roles(
            &project_id,
            vec![
                CatalogCreateRoleRequest::builder()
                    .role_id(another_role_id)
                    .role_name("Role 3")
                    .description(Some("Role 3 description"))
                    .source_id(&another_source_id)
                    .provider_id(&external_provider_id)
                    .build(),
            ],
            &state.write_pool(),
        )
        .await
        .unwrap();
        let err = update_role_source_system(
            &project_id,
            another_role_id,
            &UpdateRoleSourceSystemRequest {
                source_id: external_source_id.clone(),
                provider_id: external_provider_id.clone(),
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

        let source1 = RoleSourceId::new_from_role_id(role1_id);
        let source2 = RoleSourceId::new_from_role_id(role2_id);
        let provider_id = RoleProviderId::default();
        create_roles(
            &project1_id,
            vec![
                CatalogCreateRoleRequest::builder()
                    .role_id(role1_id)
                    .role_name("Role 1")
                    .description(None)
                    .source_id(&source1)
                    .provider_id(&provider_id)
                    .build(),
            ],
            &state.write_pool(),
        )
        .await
        .unwrap();

        create_roles(
            &project2_id,
            vec![
                CatalogCreateRoleRequest::builder()
                    .role_id(role2_id)
                    .role_name("Role 2")
                    .description(Some("Role 2 description"))
                    .source_id(&source2)
                    .provider_id(&provider_id)
                    .build(),
            ],
            &state.write_pool(),
        )
        .await
        .unwrap();

        let roles = list_roles(
            Some(&project1_id),
            CatalogListRolesByIdFilter::builder().build(),
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
            Some(&project2_id),
            CatalogListRolesByIdFilter::builder().build(),
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
    async fn test_list_roles_across_all_projects(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project1_id = ProjectId::new_random();
        let project2_id = ProjectId::new_random();
        let project3_id = ProjectId::new_random();

        let role1_id = RoleId::new_random();
        let role2_id = RoleId::new_random();
        let role3_id = RoleId::new_random();

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

        PostgresBackend::create_project(
            &project3_id,
            format!("Project {project3_id}"),
            t.transaction(),
        )
        .await
        .unwrap();

        t.commit().await.unwrap();

        // Create roles in multiple projects
        let source1 = RoleSourceId::new_from_role_id(role1_id);
        let source2 = RoleSourceId::new_from_role_id(role2_id);
        let source3 = RoleSourceId::new_from_role_id(role3_id);
        let provider_id = RoleProviderId::default();
        create_roles(
            &project1_id,
            vec![
                CatalogCreateRoleRequest::builder()
                    .role_id(role1_id)
                    .role_name("Role 1")
                    .description(Some("Role in project 1"))
                    .source_id(&source1)
                    .provider_id(&provider_id)
                    .build(),
            ],
            &state.write_pool(),
        )
        .await
        .unwrap();

        create_roles(
            &project2_id,
            vec![
                CatalogCreateRoleRequest::builder()
                    .role_id(role2_id)
                    .role_name("Role 2")
                    .description(Some("Role in project 2"))
                    .source_id(&source2)
                    .provider_id(&provider_id)
                    .build(),
            ],
            &state.write_pool(),
        )
        .await
        .unwrap();

        create_roles(
            &project3_id,
            vec![
                CatalogCreateRoleRequest::builder()
                    .role_id(role3_id)
                    .role_name("Role 3")
                    .description(Some("Role in project 3"))
                    .source_id(&source3)
                    .provider_id(&provider_id)
                    .build(),
            ],
            &state.write_pool(),
        )
        .await
        .unwrap();

        // List all roles across all projects with project_id = None
        let roles = list_roles(
            None,
            CatalogListRolesByIdFilter::builder().build(),
            PaginationQuery {
                page_size: Some(10),
                page_token: PageToken::Empty,
            },
            &state.read_pool(),
        )
        .await
        .unwrap();

        // Should return all 3 roles
        assert_eq!(roles.roles.len(), 3);
        let role_ids: Vec<RoleId> = roles.roles.iter().map(|r| r.id).collect();
        assert!(role_ids.contains(&role1_id));
        assert!(role_ids.contains(&role2_id));
        assert!(role_ids.contains(&role3_id));

        // Verify that filtering by role_ids works across projects
        let roles = list_roles(
            None,
            CatalogListRolesByIdFilter::builder()
                .role_ids(Some(&[role1_id, role3_id]))
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
        let role_ids: Vec<RoleId> = roles.roles.iter().map(|r| r.id).collect();
        assert!(role_ids.contains(&role1_id));
        assert!(role_ids.contains(&role3_id));
        assert!(!role_ids.contains(&role2_id));
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

        let provider_id = RoleProviderId::default();
        for i in 0..10 {
            let role_id_i = RoleId::new_random();
            let source_id_i = RoleSourceId::new_from_role_id(role_id_i);
            create_roles(
                &project1_id,
                vec![
                    CatalogCreateRoleRequest::builder()
                        .role_id(role_id_i)
                        .role_name(&format!("Role-{i}"))
                        .description(Some(&format!("Role-{i} description")))
                        .source_id(&source_id_i)
                        .provider_id(&provider_id)
                        .build(),
                ],
                &state.write_pool(),
            )
            .await
            .unwrap();
        }

        let roles = list_roles(
            Some(&project1_id),
            CatalogListRolesByIdFilter::builder().build(),
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
            Some(&project1_id),
            CatalogListRolesByIdFilter::builder().build(),
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
            Some(&project1_id),
            CatalogListRolesByIdFilter::builder().build(),
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
            Some(&project1_id),
            CatalogListRolesByIdFilter::builder().build(),
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

        let source_id = RoleSourceId::new_from_role_id(role_id);
        let provider_id = RoleProviderId::default();
        create_roles(
            &project_id,
            vec![
                CatalogCreateRoleRequest::builder()
                    .role_id(role_id)
                    .role_name(role_name)
                    .description(Some("Role 1 description"))
                    .source_id(&source_id)
                    .provider_id(&provider_id)
                    .build(),
            ],
            &state.write_pool(),
        )
        .await
        .unwrap();

        delete_roles(&project_id, Some(&[role_id]), &state.write_pool())
            .await
            .unwrap();

        let roles = list_roles(
            Some(&project_id),
            CatalogListRolesByIdFilter::builder().build(),
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
    async fn test_delete_roles_by_id(pool: sqlx::PgPool) {
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

        // Create roles with different IDs
        let role1_id = RoleId::new_random();
        let role2_id = RoleId::new_random();
        let source_id_1 = RoleSourceId::new_from_role_id(role1_id);
        let source_id_2 = RoleSourceId::new_from_role_id(role2_id);
        let provider_id = RoleProviderId::default();
        create_roles(
            &project_id,
            vec![
                CatalogCreateRoleRequest::builder()
                    .role_id(role1_id)
                    .role_name("Role 1")
                    .source_id(&source_id_1)
                    .provider_id(&provider_id)
                    .build(),
                CatalogCreateRoleRequest::builder()
                    .role_id(role2_id)
                    .role_name("Role 2")
                    .source_id(&source_id_2)
                    .provider_id(&provider_id)
                    .build(),
            ],
            &state.write_pool(),
        )
        .await
        .unwrap();

        // Delete role 1 by ID
        let deleted = delete_roles(&project_id, Some(&[role1_id]), &state.write_pool())
            .await
            .unwrap();

        assert_eq!(deleted.len(), 1);
        assert_eq!(deleted[0], role1_id);

        // Verify only role 2 remains
        let roles = list_roles(
            Some(&project_id),
            CatalogListRolesByIdFilter::builder().build(),
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
    async fn test_delete_all_roles_in_project(pool: sqlx::PgPool) {
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
        let source_id_1 = RoleSourceId::new_from_role_id(role_id_1);
        let source_id_2 = RoleSourceId::new_from_role_id(role_id_2);
        let provider_id = RoleProviderId::default();
        create_roles(
            &project_id,
            vec![
                CatalogCreateRoleRequest::builder()
                    .role_id(role_id_1)
                    .role_name("Role 1")
                    .source_id(&source_id_1)
                    .provider_id(&provider_id)
                    .build(),
                CatalogCreateRoleRequest::builder()
                    .role_id(role_id_2)
                    .role_name("Role 2")
                    .source_id(&source_id_2)
                    .provider_id(&provider_id)
                    .build(),
            ],
            &state.write_pool(),
        )
        .await
        .unwrap();

        // Delete role 1 by ID
        let deleted = delete_roles(&project_id, Some(&[role_id_1]), &state.write_pool())
            .await
            .unwrap();

        assert_eq!(deleted.len(), 1);
        assert_eq!(deleted[0], role_id_1);

        // Verify role 2 remains
        let roles = list_roles(
            Some(&project_id),
            CatalogListRolesByIdFilter::builder().build(),
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
        let role1_id = RoleId::new_random();
        let role2_id = RoleId::new_random();
        let source_id_1 = RoleSourceId::new_from_role_id(role1_id);
        let source_id_2 = RoleSourceId::new_from_role_id(role2_id);
        let provider_id = RoleProviderId::default();
        create_roles(
            &project1_id,
            vec![
                CatalogCreateRoleRequest::builder()
                    .role_id(role1_id)
                    .role_name("Role 1")
                    .source_id(&source_id_1)
                    .provider_id(&provider_id)
                    .build(),
            ],
            &state.write_pool(),
        )
        .await
        .unwrap();

        create_roles(
            &project2_id,
            vec![
                CatalogCreateRoleRequest::builder()
                    .role_id(role2_id)
                    .role_name("Role 2")
                    .source_id(&source_id_2)
                    .provider_id(&provider_id)
                    .build(),
            ],
            &state.write_pool(),
        )
        .await
        .unwrap();

        // Delete from project 1
        let deleted = delete_roles(&project1_id, None, &state.write_pool())
            .await
            .unwrap();

        assert_eq!(deleted.len(), 1);

        // Verify project 2 role still exists
        let roles = list_roles(
            Some(&project2_id),
            CatalogListRolesByIdFilter::builder().build(),
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
        let ext_source_1: RoleSourceId = "external-1".parse().unwrap();
        let ext_source_2: RoleSourceId = "external-2".parse().unwrap();
        let ext_source_3: RoleSourceId = "external-3".parse().unwrap();
        let ext_provider: RoleProviderId = "external".parse().unwrap();
        create_roles(
            &project_id,
            vec![
                CatalogCreateRoleRequest::builder()
                    .role_id(RoleId::new_random())
                    .role_name("Role 1")
                    .source_id(&ext_source_1)
                    .provider_id(&ext_provider)
                    .build(),
                CatalogCreateRoleRequest::builder()
                    .role_id(RoleId::new_random())
                    .role_name("Role 2")
                    .source_id(&ext_source_2)
                    .provider_id(&ext_provider)
                    .build(),
                CatalogCreateRoleRequest::builder()
                    .role_id(RoleId::new_random())
                    .role_name("Role 3")
                    .source_id(&ext_source_3)
                    .provider_id(&ext_provider)
                    .build(),
            ],
            &state.write_pool(),
        )
        .await
        .unwrap();

        // Filter by specific source IDs
        let roles = list_roles(
            Some(&project_id),
            CatalogListRolesByIdFilter::builder()
                .source_ids(Some(&[&ext_source_1, &ext_source_3]))
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
        assert!(
            roles
                .roles
                .iter()
                .any(|r| r.name == "Role 1" && r.source_id() == &ext_source_1)
        );
        assert!(
            roles
                .roles
                .iter()
                .any(|r| r.name == "Role 3" && r.source_id() == &ext_source_3)
        );

        // Filter by single source ID
        let roles = list_roles(
            Some(&project_id),
            CatalogListRolesByIdFilter::builder()
                .source_ids(Some(&[&ext_source_2]))
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
        assert_eq!(roles.roles[0].source_id(), &ext_source_2);
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
        let source_1 = RoleSourceId::new_from_role_id(role1_id);
        let source_2 = RoleSourceId::new_from_role_id(role2_id);
        let source_3 = RoleSourceId::new_from_role_id(role3_id);
        let provider_id = RoleProviderId::default();
        create_roles(
            &project_id,
            vec![
                CatalogCreateRoleRequest::builder()
                    .role_id(role1_id)
                    .role_name("Role 1")
                    .source_id(&source_1)
                    .provider_id(&provider_id)
                    .build(),
                CatalogCreateRoleRequest::builder()
                    .role_id(role2_id)
                    .role_name("Role 2")
                    .source_id(&source_2)
                    .provider_id(&provider_id)
                    .build(),
                CatalogCreateRoleRequest::builder()
                    .role_id(role3_id)
                    .role_name("Role 3")
                    .source_id(&source_3)
                    .provider_id(&provider_id)
                    .build(),
            ],
            &state.write_pool(),
        )
        .await
        .unwrap();

        // Filter by role_ids and source_system (both must match)
        let roles = list_roles(
            Some(&project_id),
            CatalogListRolesByIdFilter::builder()
                .source_ids(Some(&[&source_1, &source_2]))
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
            Some(&project_id),
            CatalogListRolesByIdFilter::builder()
                .role_ids(Some(&[role1_id, role3_id]))
                .source_ids(Some(&[&source_1]))
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
            Some(&project_id),
            CatalogListRolesByIdFilter::builder()
                .role_ids(Some(&[role1_id, role2_id, role3_id]))
                .source_ids(Some(&[&source_2]))
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

        let role_id = RoleId::new_random();
        let role_name = "Role 1";
        let source_id = RoleSourceId::new_from_role_id(role_id);
        let provider_id = RoleProviderId::default();
        create_roles(
            &project_id,
            vec![
                CatalogCreateRoleRequest::builder()
                    .role_id(role_id)
                    .role_name(role_name)
                    .description(Some("Role 1 description"))
                    .source_id(&source_id)
                    .provider_id(&provider_id)
                    .build(),
            ],
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

        let provider_id = RoleProviderId::default();
        let ext_source_1 = "external-1".parse::<RoleSourceId>().unwrap();
        let ext_source_2 = "external-2".parse::<RoleSourceId>().unwrap();
        let role3_id = RoleId::new_random();
        let source_id_3 = RoleSourceId::new_from_role_id(role3_id);
        let role4_id = RoleId::new_random();
        let source_id_4 = RoleSourceId::new_from_role_id(role4_id);

        // Create multiple roles with different combinations of optional fields
        let roles = create_roles(
            &project_id,
            vec![
                // All fields present
                CatalogCreateRoleRequest::builder()
                    .role_id(RoleId::new_random())
                    .role_name("Role 1")
                    .description(Some("Description 1"))
                    .source_id(&ext_source_1)
                    .provider_id(&provider_id)
                    .build(),
                // No description
                CatalogCreateRoleRequest::builder()
                    .role_id(RoleId::new_random())
                    .role_name("Role 2")
                    .source_id(&ext_source_2)
                    .provider_id(&provider_id)
                    .build(),
                // Has description
                CatalogCreateRoleRequest::builder()
                    .role_id(role3_id)
                    .role_name("Role 3")
                    .description(Some("Description 3"))
                    .source_id(&source_id_3)
                    .provider_id(&provider_id)
                    .build(),
                // No description (all optional fields None)
                CatalogCreateRoleRequest::builder()
                    .role_id(role4_id)
                    .role_name("Role 4")
                    .source_id(&source_id_4)
                    .provider_id(&provider_id)
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
        assert_eq!(roles[0].source_id(), &ext_source_1);

        // Verify second role (no description)
        assert_eq!(roles[1].name, "Role 2");
        assert_eq!(roles[1].description, None);
        assert_eq!(roles[1].source_id(), &ext_source_2);

        // Verify third role (has description)
        assert_eq!(roles[2].name, "Role 3");
        assert_eq!(roles[2].description, Some("Description 3".to_string()));
        assert_eq!(roles[2].source_id().as_str(), role3_id.to_string());

        // Verify fourth role (lakekeeper-managed)
        assert_eq!(roles[3].name, "Role 4");
        assert_eq!(roles[3].description, None);
        assert_eq!(roles[3].source_id().as_str(), role4_id.to_string());
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

        let provider_id = RoleProviderId::default();
        let role_id_1 = RoleId::new_random();
        let role_id_2 = RoleId::new_random();
        let role_id_3 = RoleId::new_random();
        let source_id_1 = RoleSourceId::new_from_role_id(role_id_1);
        let source_id_2 = RoleSourceId::new_from_role_id(role_id_2);
        let source_id_3 = RoleSourceId::new_from_role_id(role_id_3);

        // Create multiple roles with all optional fields as None
        let roles = create_roles(
            &project_id,
            vec![
                CatalogCreateRoleRequest::builder()
                    .role_id(role_id_1)
                    .role_name("MinimalRole1")
                    .source_id(&source_id_1)
                    .provider_id(&provider_id)
                    .build(),
                CatalogCreateRoleRequest::builder()
                    .role_id(role_id_2)
                    .role_name("MinimalRole2")
                    .source_id(&source_id_2)
                    .provider_id(&provider_id)
                    .build(),
                CatalogCreateRoleRequest::builder()
                    .role_id(role_id_3)
                    .role_name("MinimalRole3")
                    .source_id(&source_id_3)
                    .provider_id(&provider_id)
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
            assert_eq!(&*role.project_id, &project_id);
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

        let provider_id = RoleProviderId::default();
        let role_id_1 = RoleId::new_random();
        let role_id_2 = RoleId::new_random();
        let source_id_1 = RoleSourceId::new_from_role_id(role_id_1);
        let source_id_2 = RoleSourceId::new_from_role_id(role_id_2);

        // Try to create roles with duplicate names in the same batch
        let err = create_roles(
            &project_id,
            vec![
                CatalogCreateRoleRequest::builder()
                    .role_id(role_id_1)
                    .role_name("DuplicateName")
                    .source_id(&source_id_1)
                    .provider_id(&provider_id)
                    .build(),
                CatalogCreateRoleRequest::builder()
                    .role_id(role_id_2)
                    .role_name("DuplicateName")
                    .source_id(&source_id_2)
                    .provider_id(&provider_id)
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
        let source_id = "duplicate-external-id".parse::<RoleSourceId>().unwrap();
        let provider_id = RoleProviderId::default();
        let err = create_roles(
            &project_id,
            vec![
                CatalogCreateRoleRequest::builder()
                    .role_id(RoleId::new_random())
                    .role_name("Role1")
                    .source_id(&source_id)
                    .provider_id(&provider_id)
                    .build(),
                CatalogCreateRoleRequest::builder()
                    .role_id(RoleId::new_random())
                    .role_name("Role2")
                    .source_id(&source_id)
                    .provider_id(&provider_id)
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
        let single_role_id = RoleId::new_random();
        let single_source = RoleSourceId::new_from_role_id(single_role_id);
        let provider_id = RoleProviderId::default();
        let roles = create_roles(
            &project_id,
            vec![
                CatalogCreateRoleRequest::builder()
                    .role_id(single_role_id)
                    .role_name("SingleRole")
                    .source_id(&single_source)
                    .provider_id(&provider_id)
                    .build(),
            ],
            &state.write_pool(),
        )
        .await
        .unwrap();

        assert_eq!(roles.len(), 1);
        assert_eq!(roles[0].name, "SingleRole");
        assert_eq!(roles[0].description, None);
        assert_eq!(roles[0].source_id().as_str(), single_role_id.to_string());
    }
}
