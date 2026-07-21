//! Postgres storage for governance tags. Mirrors the role module: free functions
//! taking a connection/transaction, with constraint-name -> typed-error mapping.

use lakekeeper::{
    CONFIG, ProjectId,
    api::iceberg::v1::PaginationQuery,
    service::{
        ApplyTagError, CatalogBackendError, CatalogCreateTagDefinitionRequest,
        CreateTagDefinitionError, DeleteTagDefinitionError, ListTagDefinitionsError,
        ListTagDefinitionsResponse, ProjectIdNotFoundError, RemoveTagError, Result, Tag,
        TagDefinition, TagDefinitionId, TagDefinitionIdNotFound, TagDefinitionInUse, TagId,
        TagNameAlreadyExists, TagNotFound, TagScope, TagSource, TagTarget, TagTargetNotFound,
        TagValueKind, UpdateTagDefinitionError, UpdateTagDefinitionRequest,
    },
};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::{
    dbutils::DBErrorHandler,
    pagination::{PaginateToken, V1PaginateToken},
};

#[derive(sqlx::FromRow, Debug)]
struct TagDefinitionRow {
    tag_definition_id: Uuid,
    project_id: String,
    name: String,
    description: Option<String>,
    scope: Vec<String>,
    value_kind: TagValueKind,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: Option<chrono::DateTime<chrono::Utc>>,
    updated_by: String,
}

fn unknown_enum(kind: &str, value: &str) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("Unknown {kind} '{value}' encountered in database"),
    )
}

impl TryFrom<TagDefinitionRow> for TagDefinition {
    type Error = CatalogBackendError;

    fn try_from(row: TagDefinitionRow) -> std::result::Result<Self, Self::Error> {
        let scope = row
            .scope
            .iter()
            .map(|s| {
                TagScope::parse(s)
                    .ok_or_else(|| CatalogBackendError::new_unexpected(unknown_enum("tag scope", s)))
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(TagDefinition {
            tag_definition_id: TagDefinitionId::new(row.tag_definition_id),
            project_id: ProjectId::from_db_unchecked(row.project_id),
            name: row.name,
            description: row.description,
            scope,
            value_kind: row.value_kind,
            created_at: row.created_at,
            updated_at: row.updated_at,
            updated_by: row.updated_by,
        })
    }
}

/// Insert a new tag definition (and, for enumerated definitions, its allowed
/// values) atomically. Value validation (name, scope, value-kind/allowed-values
/// consistency) is the caller's responsibility; this maps only structural DB
/// violations to typed errors.
pub(crate) async fn create_tag_definition(
    project_id: &ProjectId,
    request: CatalogCreateTagDefinitionRequest<'_>,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<TagDefinition, CreateTagDefinitionError> {
    let CatalogCreateTagDefinitionRequest {
        tag_definition_id,
        name,
        description,
        scope,
        value_spec,
        updated_by,
    } = request;
    let value_kind = value_spec.kind();
    let allowed_values = value_spec.allowed_values();

    let scope: Vec<String> = scope.iter().map(|s| s.as_str().to_string()).collect();

    let row = sqlx::query_as!(
        TagDefinitionRow,
        r#"
        INSERT INTO tag_definition
            (tag_definition_id, project_id, name, description, scope, value_kind, updated_by)
        VALUES ($1, $2, $3, $4, $5::text[], $6, $7)
        RETURNING
            tag_definition_id,
            project_id,
            name,
            description,
            scope,
            value_kind AS "value_kind: TagValueKind",
            created_at,
            updated_at,
            updated_by
        "#,
        *tag_definition_id,
        &**project_id,
        name,
        description,
        &scope,
        value_kind as _,
        updated_by,
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| -> CreateTagDefinitionError {
        match &e {
            sqlx::Error::Database(db_error) => {
                if db_error.is_unique_violation() {
                    match db_error.constraint() {
                        Some("tag_definition_name_idx") => TagNameAlreadyExists::new().into(),
                        _ => e.into_catalog_backend_error().into(),
                    }
                } else if db_error.is_foreign_key_violation() {
                    ProjectIdNotFoundError::new(project_id.clone()).into()
                } else {
                    e.into_catalog_backend_error().into()
                }
            }
            _ => e.into_catalog_backend_error().into(),
        }
    })?;

    // Enumerated definitions carry their permitted values; insert them in the same
    // transaction so the definition and its allowed values commit atomically.
    if !allowed_values.is_empty() {
        sqlx::query!(
            r#"
            INSERT INTO tag_allowed_value (tag_definition_id, value)
            SELECT $1, v FROM UNNEST($2::text[]) v
            "#,
            *tag_definition_id,
            allowed_values as &[&str],
        )
        .execute(&mut **transaction)
        .await
        .map_err(|e| CreateTagDefinitionError::from(e.into_catalog_backend_error()))?;
    }

    Ok(TagDefinition::try_from(row)?)
}

/// Fetch a single tag definition scoped to its project. Returns `None` if no
/// definition with that id exists in the project (including when it exists in a
/// different project). Scalar only — allowed values are fetched separately.
pub(crate) async fn get_tag_definition<'e, 'c: 'e, E>(
    project_id: &ProjectId,
    tag_definition_id: TagDefinitionId,
    connection: E,
) -> Result<Option<TagDefinition>, CatalogBackendError>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let row = sqlx::query_as!(
        TagDefinitionRow,
        r#"
        SELECT
            tag_definition_id,
            project_id,
            name,
            description,
            scope,
            value_kind AS "value_kind: TagValueKind",
            created_at,
            updated_at,
            updated_by
        FROM tag_definition
        WHERE project_id = $1 AND tag_definition_id = $2
        "#,
        &**project_id,
        *tag_definition_id,
    )
    .fetch_optional(connection)
    .await
    .map_err(|e| e.into_catalog_backend_error())?;

    row.map(TagDefinition::try_from).transpose()
}

/// List a project's tag definitions, keyset-paginated by `(created_at, tag_definition_id)`.
/// Scalar only — the allowed-value child table is never joined here.
pub(crate) async fn list_tag_definitions<'e, 'c: 'e, E>(
    project_id: &ProjectId,
    PaginationQuery {
        page_size,
        page_token,
    }: PaginationQuery,
    connection: E,
) -> Result<ListTagDefinitionsResponse, ListTagDefinitionsError>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let page_size = CONFIG.page_size_or_pagination_default(page_size);

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

    let rows = sqlx::query_as!(
        TagDefinitionRow,
        r#"
        SELECT
            tag_definition_id,
            project_id,
            name,
            description,
            scope,
            value_kind AS "value_kind: TagValueKind",
            created_at,
            updated_at,
            updated_by
        FROM tag_definition
        WHERE project_id = $1
            AND ((created_at > $2 OR $2 IS NULL) OR (created_at = $2 AND tag_definition_id > $3))
        ORDER BY created_at, tag_definition_id ASC
        LIMIT $4
        "#,
        &**project_id,
        token_ts,
        token_id,
        page_size,
    )
    .fetch_all(connection)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;

    let tag_definitions = rows
        .into_iter()
        .map(TagDefinition::try_from)
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let next_page_token = tag_definitions.last().map(|d| {
        PaginateToken::V1(V1PaginateToken::<Uuid> {
            created_at: d.created_at,
            id: *d.tag_definition_id,
        })
        .to_string()
    });

    Ok(ListTagDefinitionsResponse {
        tag_definitions,
        next_page_token,
    })
}

/// The permitted values of an enumerated definition, sorted. Empty for a
/// non-enumerated definition or an unknown id. Fetched lazily: the caller has
/// already resolved the definition (and its project scope) via [`get_tag_definition`].
pub(crate) async fn get_tag_allowed_values<'e, 'c: 'e, E>(
    tag_definition_id: TagDefinitionId,
    connection: E,
) -> Result<Vec<String>, CatalogBackendError>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    sqlx::query_scalar!(
        r#"SELECT value FROM tag_allowed_value WHERE tag_definition_id = $1 ORDER BY value"#,
        *tag_definition_id,
    )
    .fetch_all(connection)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)
}

/// Update a tag definition's mutable fields: replace name/description/scope and add
/// (never remove) allowed values, atomically. Widen-only scope and kind-immutability
/// are enforced by the caller; this maps only the rename conflict and not-found.
pub(crate) async fn update_tag_definition(
    project_id: &ProjectId,
    tag_definition_id: TagDefinitionId,
    request: UpdateTagDefinitionRequest<'_>,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<TagDefinition, UpdateTagDefinitionError> {
    let UpdateTagDefinitionRequest {
        name,
        description,
        scope,
        add_allowed_values,
        updated_by,
    } = request;
    let scope: Vec<String> = scope.iter().map(|s| s.as_str().to_string()).collect();

    let row = sqlx::query_as!(
        TagDefinitionRow,
        r#"
        UPDATE tag_definition
        SET name = $3, description = $4, scope = $5::text[], updated_by = $6
        WHERE project_id = $1 AND tag_definition_id = $2
        RETURNING
            tag_definition_id,
            project_id,
            name,
            description,
            scope,
            value_kind AS "value_kind: TagValueKind",
            created_at,
            updated_at,
            updated_by
        "#,
        &**project_id,
        *tag_definition_id,
        name,
        description,
        &scope,
        updated_by,
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| -> UpdateTagDefinitionError {
        match &e {
            sqlx::Error::Database(db_error)
                if db_error.is_unique_violation()
                    && db_error.constraint() == Some("tag_definition_name_idx") =>
            {
                TagNameAlreadyExists::new().into()
            }
            _ => e.into_catalog_backend_error().into(),
        }
    })?
    .ok_or_else(|| TagDefinitionIdNotFound::new(tag_definition_id))?;

    // Allowed values are add-only: insert the requested ones, ignoring any already present.
    if !add_allowed_values.is_empty() {
        sqlx::query!(
            r#"
            INSERT INTO tag_allowed_value (tag_definition_id, value)
            SELECT $1, v FROM UNNEST($2::text[]) v
            ON CONFLICT (tag_definition_id, value) DO NOTHING
            "#,
            *tag_definition_id,
            add_allowed_values as &[&str],
        )
        .execute(&mut **transaction)
        .await
        .map_err(|e| UpdateTagDefinitionError::from(e.into_catalog_backend_error()))?;
    }

    Ok(TagDefinition::try_from(row)?)
}

/// Delete a tag definition (and, via cascade, its allowed values). Fails with
/// [`TagDefinitionInUse`] if any tag still references it (the attachment FK is `RESTRICT`), and
/// with [`TagDefinitionIdNotFound`] if no such definition exists in the project.
pub(crate) async fn delete_tag_definition(
    project_id: &ProjectId,
    tag_definition_id: TagDefinitionId,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), DeleteTagDefinitionError> {
    let result = sqlx::query!(
        "DELETE FROM tag_definition WHERE project_id = $1 AND tag_definition_id = $2",
        &**project_id,
        *tag_definition_id,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| -> DeleteTagDefinitionError {
        match &e {
            sqlx::Error::Database(db_error)
                if db_error.is_foreign_key_violation()
                    && db_error.constraint() == Some("tag_definition_id_fkey") =>
            {
                TagDefinitionInUse::new().into()
            }
            _ => e.into_catalog_backend_error().into(),
        }
    })?;

    if result.rows_affected() == 0 {
        return Err(TagDefinitionIdNotFound::new(tag_definition_id).into());
    }
    Ok(())
}

/// The `tag` table's nullable target columns, projected from a domain [`TagTarget`]. One-way
/// (write / query-key) only: the table stores a bare `tabular_id` with no Table/View/GenericTable
/// discriminator, so a row cannot rebuild the `TabularId` subtype — reads recover the full target
/// from the caller-supplied `TagTarget`. Column shape belongs here, in the storage layer.
struct TagTargetColumns {
    warehouse_id: Uuid,
    namespace_id: Option<Uuid>,
    tabular_id: Option<Uuid>,
    field_id: Option<i32>,
}

impl TagTargetColumns {
    fn from_target(target: TagTarget) -> Self {
        // warehouse_id via the typed domain accessor; the remaining slots encode the single-target
        // invariant (exactly one of namespace/tabular; field only for a column).
        let warehouse_id = *target.warehouse_id();
        match target {
            TagTarget::Warehouse(_) => Self {
                warehouse_id,
                namespace_id: None,
                tabular_id: None,
                field_id: None,
            },
            TagTarget::Namespace { namespace_id, .. } => Self {
                warehouse_id,
                namespace_id: Some(*namespace_id),
                tabular_id: None,
                field_id: None,
            },
            TagTarget::Tabular { tabular_id, .. } => Self {
                warehouse_id,
                namespace_id: None,
                tabular_id: Some(*tabular_id.as_ref()),
                field_id: None,
            },
            TagTarget::Column {
                tabular_id,
                field_id,
                ..
            } => Self {
                warehouse_id,
                namespace_id: None,
                tabular_id: Some(*tabular_id.as_ref()),
                field_id: Some(field_id),
            },
        }
    }
}

/// Attach a tag definition to a target. Idempotent per (target, definition, source): re-applying
/// updates the value. Maps a missing definition / missing target to typed errors; value legality
/// (marker carries none, enumerated is one of the allowed set) is validated by the caller.
pub(crate) async fn apply_tag(
    tag_id: TagId,
    tag_definition_id: TagDefinitionId,
    target: TagTarget,
    value: Option<&str>,
    source: TagSource,
    updated_by: &str,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<Tag, ApplyTagError> {
    let cols = TagTargetColumns::from_target(target);

    let row = sqlx::query!(
        r#"
        INSERT INTO tag
            (tag_id, tag_definition_id, warehouse_id, namespace_id, tabular_id, field_id,
             value, source, updated_by)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT ON CONSTRAINT tag_unique_target_definition_source
        DO UPDATE SET value = EXCLUDED.value, updated_by = EXCLUDED.updated_by
        RETURNING tag_id, value, created_at, updated_at, updated_by
        "#,
        *tag_id,
        *tag_definition_id,
        cols.warehouse_id,
        cols.namespace_id,
        cols.tabular_id,
        cols.field_id,
        value,
        source as _,
        updated_by,
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| -> ApplyTagError {
        match &e {
            sqlx::Error::Database(db_error) if db_error.is_foreign_key_violation() => {
                match db_error.constraint() {
                    Some("tag_definition_id_fkey") => {
                        TagDefinitionIdNotFound::new(tag_definition_id).into()
                    }
                    Some("tag_warehouse_id_fkey")
                    | Some("tag_namespace_fkey")
                    | Some("tag_tabular_fkey")
                    | Some("tag_field_fkey") => TagTargetNotFound::new().into(),
                    _ => e.into_catalog_backend_error().into(),
                }
            }
            _ => e.into_catalog_backend_error().into(),
        }
    })?;

    Ok(Tag {
        tag_id: TagId::new(row.tag_id),
        tag_definition_id,
        target,
        value: row.value,
        source,
        created_at: row.created_at,
        updated_at: row.updated_at,
        updated_by: row.updated_by,
    })
}

/// Remove a tag attachment by its id. Returns [`TagNotFound`] if no such tag exists.
pub(crate) async fn remove_tag(
    tag_id: TagId,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), RemoveTagError> {
    let result = sqlx::query!("DELETE FROM tag WHERE tag_id = $1", *tag_id)
        .execute(&mut **transaction)
        .await
        .map_err(|e| RemoveTagError::from(e.into_catalog_backend_error()))?;
    if result.rows_affected() == 0 {
        return Err(TagNotFound::new(tag_id).into());
    }
    Ok(())
}

/// List the tags attached to exactly `target`, ordered by `(created_at, tag_id)`.
pub(crate) async fn list_tags_for_target<'e, 'c: 'e, E>(
    target: TagTarget,
    connection: E,
) -> Result<Vec<Tag>, CatalogBackendError>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let cols = TagTargetColumns::from_target(target);
    let rows = sqlx::query!(
        r#"
        SELECT
            tag_id,
            tag_definition_id,
            value,
            source AS "source: TagSource",
            created_at,
            updated_at,
            updated_by
        FROM tag
        WHERE warehouse_id = $1
          AND namespace_id IS NOT DISTINCT FROM $2
          AND tabular_id IS NOT DISTINCT FROM $3
          AND field_id IS NOT DISTINCT FROM $4
        ORDER BY created_at, tag_id
        "#,
        cols.warehouse_id,
        cols.namespace_id,
        cols.tabular_id,
        cols.field_id,
    )
    .fetch_all(connection)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;

    Ok(rows
        .into_iter()
        .map(|r| Tag {
            tag_id: TagId::new(r.tag_id),
            tag_definition_id: TagDefinitionId::new(r.tag_definition_id),
            target,
            value: r.value,
            source: r.source,
            created_at: r.created_at,
            updated_at: r.updated_at,
            updated_by: r.updated_by,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use lakekeeper::{
        api::iceberg::types::PageToken,
        service::{CatalogStore, TabularId, TagScope, TagValueKind, TagValueSpec, Transaction as _},
    };

    use super::*;
    use crate::{
        CatalogState, PostgresBackend, PostgresTransaction,
        tabular::table::tests::create_table_with_schema, warehouse::test::initialize_warehouse,
    };

    fn two_col_schema() -> Schema {
        Schema::builder()
            .with_schema_id(0)
            .with_identifier_field_ids(vec![1])
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap()
    }

    async fn create_project(state: &CatalogState, project_id: &ProjectId) {
        let mut t = PostgresTransaction::begin_write(state.clone()).await.unwrap();
        PostgresBackend::create_project(project_id, format!("Project {project_id}"), t.transaction())
            .await
            .unwrap();
        t.commit().await.unwrap();
    }

    #[sqlx::test]
    async fn test_create_tag_definition(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectId::new_random();

        // Missing project -> ProjectIdNotFoundError (FK violation).
        let mut txn = pool.begin().await.unwrap();
        let err = create_tag_definition(
            &project_id,
            CatalogCreateTagDefinitionRequest::builder()
                .tag_definition_id(TagDefinitionId::new_random())
                .name("pii.classification")
                .description(Some("PII classification"))
                .scope(&[TagScope::Column, TagScope::Table])
                .value_spec(TagValueSpec::Marker)
                .updated_by("alice")
                .build(),
            &mut txn,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, CreateTagDefinitionError::ProjectIdNotFoundError(_)));
        drop(txn);

        create_project(&state, &project_id).await;

        // Create a marker definition.
        let mut txn = pool.begin().await.unwrap();
        let def = create_tag_definition(
            &project_id,
            CatalogCreateTagDefinitionRequest::builder()
                .tag_definition_id(TagDefinitionId::new_random())
                .name("pii.classification")
                .description(Some("PII classification"))
                .scope(&[TagScope::Column, TagScope::Table])
                .value_spec(TagValueSpec::Marker)
                .updated_by("alice")
                .build(),
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        assert_eq!(def.name, "pii.classification");
        assert_eq!(def.description, Some("PII classification".to_string()));
        assert_eq!(&def.project_id, &project_id);
        assert_eq!(def.scope, vec![TagScope::Column, TagScope::Table]);
        assert_eq!(def.value_kind, TagValueKind::Marker);
        assert_eq!(def.updated_by, "alice");
        assert_eq!(def.updated_at, None);

        // Duplicate name (case-insensitive) -> TagNameAlreadyExists.
        let mut txn = pool.begin().await.unwrap();
        let err = create_tag_definition(
            &project_id,
            CatalogCreateTagDefinitionRequest::builder()
                .tag_definition_id(TagDefinitionId::new_random())
                .name("PII.Classification")
                .scope(&[TagScope::Column])
                .value_spec(TagValueSpec::Marker)
                .updated_by("bob")
                .build(),
            &mut txn,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, CreateTagDefinitionError::TagNameAlreadyExists(_)));
    }

    #[sqlx::test]
    async fn test_create_enumerated_tag_definition(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectId::new_random();
        create_project(&state, &project_id).await;

        let tag_definition_id = TagDefinitionId::new_random();
        let mut txn = pool.begin().await.unwrap();
        let def = create_tag_definition(
            &project_id,
            CatalogCreateTagDefinitionRequest::builder()
                .tag_definition_id(tag_definition_id)
                .name("sensitivity")
                .scope(&[TagScope::Table, TagScope::Column])
                .value_spec(TagValueSpec::Enumerated {
                    allowed_values: &["restricted", "public", "internal"],
                })
                .updated_by("alice")
                .build(),
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        assert_eq!(def.value_kind, TagValueKind::Enumerated);

        // Allowed values are persisted in the child table, atomically with the definition.
        let stored: Vec<String> = sqlx::query_scalar!(
            "SELECT value FROM tag_allowed_value WHERE tag_definition_id = $1 ORDER BY value",
            *tag_definition_id,
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            stored,
            vec![
                "internal".to_string(),
                "public".to_string(),
                "restricted".to_string()
            ]
        );
    }

    #[sqlx::test]
    async fn test_get_tag_definition(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectId::new_random();
        create_project(&state, &project_id).await;

        // Absent id -> None.
        let absent = get_tag_definition(&project_id, TagDefinitionId::new_random(), &pool)
            .await
            .unwrap();
        assert_eq!(absent, None);

        // Create, then fetch it back verbatim.
        let id = TagDefinitionId::new_random();
        let mut txn = pool.begin().await.unwrap();
        let created = create_tag_definition(
            &project_id,
            CatalogCreateTagDefinitionRequest::builder()
                .tag_definition_id(id)
                .name("pii.classification")
                .description(Some("PII classification"))
                .scope(&[TagScope::Column, TagScope::Table])
                .value_spec(TagValueSpec::Marker)
                .updated_by("alice")
                .build(),
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        let got = get_tag_definition(&project_id, id, &pool).await.unwrap();
        assert_eq!(got, Some(created));

        // A definition is invisible from a different project (no cross-project reads).
        let other_project = ProjectId::new_random();
        create_project(&state, &other_project).await;
        let cross = get_tag_definition(&other_project, id, &pool).await.unwrap();
        assert_eq!(cross, None);
    }

    #[sqlx::test]
    async fn test_list_tag_definitions(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectId::new_random();
        create_project(&state, &project_id).await;

        // Empty project.
        let empty = list_tag_definitions(
            &project_id,
            PaginationQuery { page_size: Some(10), page_token: PageToken::Empty },
            &pool,
        )
        .await
        .unwrap();
        assert!(empty.tag_definitions.is_empty());
        assert_eq!(empty.next_page_token, None);

        // Create three in order; uuid-v7 ids and created_at both ascend, so
        // (created_at, tag_definition_id) ordering == creation order.
        for name in ["a.one", "b.two", "c.three"] {
            let mut txn = pool.begin().await.unwrap();
            create_tag_definition(
                &project_id,
                CatalogCreateTagDefinitionRequest::builder()
                    .tag_definition_id(TagDefinitionId::new_random())
                    .name(name)
                    .scope(&[TagScope::Table])
                    .value_spec(TagValueSpec::Marker)
                    .updated_by("alice")
                    .build(),
                &mut txn,
            )
            .await
            .unwrap();
            txn.commit().await.unwrap();
        }

        let all = list_tag_definitions(
            &project_id,
            PaginationQuery { page_size: Some(10), page_token: PageToken::Empty },
            &pool,
        )
        .await
        .unwrap();
        let got: Vec<&str> = all.tag_definitions.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(got, vec!["a.one", "b.two", "c.three"]);

        // Page size 2: first page + cursor, then the remaining one.
        let page1 = list_tag_definitions(
            &project_id,
            PaginationQuery { page_size: Some(2), page_token: PageToken::Empty },
            &pool,
        )
        .await
        .unwrap();
        let p1: Vec<&str> = page1.tag_definitions.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(p1, vec!["a.one", "b.two"]);
        assert!(page1.next_page_token.is_some());

        let page2 = list_tag_definitions(
            &project_id,
            PaginationQuery { page_size: Some(2), page_token: page1.next_page_token.into() },
            &pool,
        )
        .await
        .unwrap();
        let p2: Vec<&str> = page2.tag_definitions.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(p2, vec!["c.three"]);
    }

    #[sqlx::test]
    async fn test_get_tag_allowed_values(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectId::new_random();
        create_project(&state, &project_id).await;

        let enumerated_id = TagDefinitionId::new_random();
        let marker_id = TagDefinitionId::new_random();
        let mut txn = pool.begin().await.unwrap();
        create_tag_definition(
            &project_id,
            CatalogCreateTagDefinitionRequest::builder()
                .tag_definition_id(enumerated_id)
                .name("sensitivity")
                .scope(&[TagScope::Table])
                .value_spec(TagValueSpec::Enumerated {
                    allowed_values: &["restricted", "public", "internal"],
                })
                .updated_by("alice")
                .build(),
            &mut txn,
        )
        .await
        .unwrap();
        create_tag_definition(
            &project_id,
            CatalogCreateTagDefinitionRequest::builder()
                .tag_definition_id(marker_id)
                .name("pii")
                .scope(&[TagScope::Column])
                .value_spec(TagValueSpec::Marker)
                .updated_by("alice")
                .build(),
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        // Enumerated -> sorted values.
        let values = get_tag_allowed_values(enumerated_id, &pool).await.unwrap();
        assert_eq!(
            values,
            vec![
                "internal".to_string(),
                "public".to_string(),
                "restricted".to_string()
            ]
        );

        // Marker -> empty; unknown id -> empty.
        assert!(get_tag_allowed_values(marker_id, &pool).await.unwrap().is_empty());
        assert!(
            get_tag_allowed_values(TagDefinitionId::new_random(), &pool)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[sqlx::test]
    async fn test_update_tag_definition(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectId::new_random();
        create_project(&state, &project_id).await;

        // Unknown id -> TagDefinitionIdNotFound.
        let mut txn = pool.begin().await.unwrap();
        let err = update_tag_definition(
            &project_id,
            TagDefinitionId::new_random(),
            UpdateTagDefinitionRequest::builder()
                .name("whatever")
                .scope(&[TagScope::Table])
                .updated_by("bob")
                .build(),
            &mut txn,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, UpdateTagDefinitionError::TagDefinitionIdNotFound(_)));
        drop(txn);

        // Create an enumerated definition, then widen it: rename, set description,
        // broaden scope, add an allowed value.
        let id = TagDefinitionId::new_random();
        let mut txn = pool.begin().await.unwrap();
        create_tag_definition(
            &project_id,
            CatalogCreateTagDefinitionRequest::builder()
                .tag_definition_id(id)
                .name("sensitivity")
                .scope(&[TagScope::Table])
                .value_spec(TagValueSpec::Enumerated {
                    allowed_values: &["public", "internal"],
                })
                .updated_by("alice")
                .build(),
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        let mut txn = pool.begin().await.unwrap();
        let updated = update_tag_definition(
            &project_id,
            id,
            UpdateTagDefinitionRequest::builder()
                .name("data.sensitivity")
                .description(Some("Sensitivity level"))
                .scope(&[TagScope::Table, TagScope::Column])
                .add_allowed_values(&["restricted"])
                .updated_by("bob")
                .build(),
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        assert_eq!(updated.name, "data.sensitivity");
        assert_eq!(updated.description, Some("Sensitivity level".to_string()));
        assert_eq!(updated.scope, vec![TagScope::Table, TagScope::Column]);
        assert_eq!(updated.value_kind, TagValueKind::Enumerated);
        assert_eq!(updated.updated_by, "bob");
        assert!(updated.updated_at.is_some());

        let values = get_tag_allowed_values(id, &pool).await.unwrap();
        assert_eq!(
            values,
            vec![
                "internal".to_string(),
                "public".to_string(),
                "restricted".to_string()
            ]
        );

        // Renaming onto another definition's name (case-insensitive) -> conflict.
        let other = TagDefinitionId::new_random();
        let mut txn = pool.begin().await.unwrap();
        create_tag_definition(
            &project_id,
            CatalogCreateTagDefinitionRequest::builder()
                .tag_definition_id(other)
                .name("pii")
                .scope(&[TagScope::Column])
                .value_spec(TagValueSpec::Marker)
                .updated_by("alice")
                .build(),
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        let mut txn = pool.begin().await.unwrap();
        let err = update_tag_definition(
            &project_id,
            other,
            UpdateTagDefinitionRequest::builder()
                .name("DATA.Sensitivity")
                .scope(&[TagScope::Column])
                .updated_by("bob")
                .build(),
            &mut txn,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, UpdateTagDefinitionError::TagNameAlreadyExists(_)));
    }

    #[sqlx::test]
    async fn test_apply_tag(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (project_id, warehouse_id) =
            initialize_warehouse(state.clone(), None, None, None, true).await;

        // A definition applicable to warehouse / table / column.
        let def_id = TagDefinitionId::new_random();
        let mut txn = pool.begin().await.unwrap();
        create_tag_definition(
            &project_id,
            CatalogCreateTagDefinitionRequest::builder()
                .tag_definition_id(def_id)
                .name("pii")
                .scope(&[TagScope::Warehouse, TagScope::Table, TagScope::Column])
                .value_spec(TagValueSpec::Marker)
                .updated_by("alice")
                .build(),
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        // Apply to the warehouse.
        let mut txn = pool.begin().await.unwrap();
        let tag = apply_tag(
            TagId::new_random(),
            def_id,
            TagTarget::Warehouse(warehouse_id),
            None,
            TagSource::Manual,
            "alice",
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();
        assert_eq!(tag.tag_definition_id, def_id);
        assert_eq!(tag.target, TagTarget::Warehouse(warehouse_id));
        assert_eq!(tag.value, None);
        assert_eq!(tag.source, TagSource::Manual);
        assert_eq!(tag.updated_by, "alice");

        // Apply to a column — exercises tag_field_fkey against the tabular_field spine.
        let (table_id, _schema) =
            create_table_with_schema(state.clone(), warehouse_id, two_col_schema()).await;
        let column = TagTarget::Column {
            warehouse_id,
            tabular_id: TabularId::Table(table_id),
            field_id: 1,
        };
        let mut txn = pool.begin().await.unwrap();
        let ctag = apply_tag(
            TagId::new_random(),
            def_id,
            column,
            None,
            TagSource::Manual,
            "alice",
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();
        assert_eq!(ctag.target, column);

        // A column that is not a live field -> TagTargetNotFound (tag_field_fkey).
        let ghost = TagTarget::Column {
            warehouse_id,
            tabular_id: TabularId::Table(table_id),
            field_id: 999,
        };
        let mut txn = pool.begin().await.unwrap();
        let err = apply_tag(
            TagId::new_random(),
            def_id,
            ghost,
            None,
            TagSource::Manual,
            "alice",
            &mut txn,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ApplyTagError::TagTargetNotFound(_)));
        drop(txn);

        // A missing definition -> TagDefinitionIdNotFound.
        let mut txn = pool.begin().await.unwrap();
        let err = apply_tag(
            TagId::new_random(),
            TagDefinitionId::new_random(),
            TagTarget::Warehouse(warehouse_id),
            None,
            TagSource::Manual,
            "alice",
            &mut txn,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ApplyTagError::TagDefinitionIdNotFound(_)));
    }

    #[sqlx::test]
    async fn test_list_and_remove_tags(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (project_id, warehouse_id) =
            initialize_warehouse(state.clone(), None, None, None, true).await;

        let def_pii = TagDefinitionId::new_random();
        let def_tier = TagDefinitionId::new_random();
        let mut txn = pool.begin().await.unwrap();
        create_tag_definition(
            &project_id,
            CatalogCreateTagDefinitionRequest::builder()
                .tag_definition_id(def_pii)
                .name("pii")
                .scope(&[TagScope::Warehouse])
                .value_spec(TagValueSpec::Marker)
                .updated_by("alice")
                .build(),
            &mut txn,
        )
        .await
        .unwrap();
        create_tag_definition(
            &project_id,
            CatalogCreateTagDefinitionRequest::builder()
                .tag_definition_id(def_tier)
                .name("tier")
                .scope(&[TagScope::Warehouse])
                .value_spec(TagValueSpec::FreeText)
                .updated_by("alice")
                .build(),
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        let wh = TagTarget::Warehouse(warehouse_id);

        // Nothing applied yet.
        assert!(list_tags_for_target(wh, &pool).await.unwrap().is_empty());

        // Apply pii, then tier (separate transactions -> strictly increasing created_at).
        let pii_tag_id = TagId::new_random();
        let mut txn = pool.begin().await.unwrap();
        apply_tag(pii_tag_id, def_pii, wh, None, TagSource::Manual, "alice", &mut txn)
            .await
            .unwrap();
        txn.commit().await.unwrap();

        let mut txn = pool.begin().await.unwrap();
        apply_tag(
            TagId::new_random(),
            def_tier,
            wh,
            Some("gold"),
            TagSource::Manual,
            "alice",
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        let listed = list_tags_for_target(wh, &pool).await.unwrap();
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].tag_definition_id, def_pii);
        assert_eq!(listed[0].value, None);
        assert_eq!(listed[1].tag_definition_id, def_tier);
        assert_eq!(listed[1].value, Some("gold".to_string()));

        // Re-applying pii (same target/definition/source) upserts in place — no new row,
        // and the original tag_id is preserved.
        let mut txn = pool.begin().await.unwrap();
        let reapplied = apply_tag(
            TagId::new_random(),
            def_pii,
            wh,
            None,
            TagSource::Manual,
            "bob",
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();
        assert_eq!(reapplied.tag_id, pii_tag_id);
        assert_eq!(reapplied.updated_by, "bob");
        assert_eq!(list_tags_for_target(wh, &pool).await.unwrap().len(), 2);

        // Remove pii; tier remains.
        let mut txn = pool.begin().await.unwrap();
        remove_tag(pii_tag_id, &mut txn).await.unwrap();
        txn.commit().await.unwrap();
        let remaining = list_tags_for_target(wh, &pool).await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].tag_definition_id, def_tier);

        // Removing an unknown tag -> TagNotFound.
        let mut txn = pool.begin().await.unwrap();
        let err = remove_tag(TagId::new_random(), &mut txn).await.unwrap_err();
        assert!(matches!(err, RemoveTagError::TagNotFound(_)));
    }

    #[sqlx::test]
    async fn test_delete_tag_definition(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (project_id, warehouse_id) =
            initialize_warehouse(state.clone(), None, None, None, true).await;

        // Unknown id -> not found.
        let mut txn = pool.begin().await.unwrap();
        let err = delete_tag_definition(&project_id, TagDefinitionId::new_random(), &mut txn)
            .await
            .unwrap_err();
        assert!(matches!(err, DeleteTagDefinitionError::TagDefinitionIdNotFound(_)));
        drop(txn);

        // Enumerated definition deletes, cascading its allowed values.
        let enum_def = TagDefinitionId::new_random();
        let mut txn = pool.begin().await.unwrap();
        create_tag_definition(
            &project_id,
            CatalogCreateTagDefinitionRequest::builder()
                .tag_definition_id(enum_def)
                .name("sensitivity")
                .scope(&[TagScope::Warehouse])
                .value_spec(TagValueSpec::Enumerated {
                    allowed_values: &["a", "b"],
                })
                .updated_by("alice")
                .build(),
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        let mut txn = pool.begin().await.unwrap();
        delete_tag_definition(&project_id, enum_def, &mut txn).await.unwrap();
        txn.commit().await.unwrap();
        assert_eq!(get_tag_definition(&project_id, enum_def, &pool).await.unwrap(), None);
        assert!(get_tag_allowed_values(enum_def, &pool).await.unwrap().is_empty());

        // A definition with an attachment cannot be deleted (RESTRICT).
        let used_def = TagDefinitionId::new_random();
        let mut txn = pool.begin().await.unwrap();
        create_tag_definition(
            &project_id,
            CatalogCreateTagDefinitionRequest::builder()
                .tag_definition_id(used_def)
                .name("pii")
                .scope(&[TagScope::Warehouse])
                .value_spec(TagValueSpec::Marker)
                .updated_by("alice")
                .build(),
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        let tag_id = TagId::new_random();
        let mut txn = pool.begin().await.unwrap();
        apply_tag(
            tag_id,
            used_def,
            TagTarget::Warehouse(warehouse_id),
            None,
            TagSource::Manual,
            "alice",
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        let mut txn = pool.begin().await.unwrap();
        let err = delete_tag_definition(&project_id, used_def, &mut txn)
            .await
            .unwrap_err();
        assert!(matches!(err, DeleteTagDefinitionError::TagDefinitionInUse(_)));
        drop(txn);

        // Remove the attachment, then the definition deletes (same transaction).
        let mut txn = pool.begin().await.unwrap();
        remove_tag(tag_id, &mut txn).await.unwrap();
        delete_tag_definition(&project_id, used_def, &mut txn).await.unwrap();
        txn.commit().await.unwrap();
        assert_eq!(get_tag_definition(&project_id, used_def, &pool).await.unwrap(), None);
    }
}
