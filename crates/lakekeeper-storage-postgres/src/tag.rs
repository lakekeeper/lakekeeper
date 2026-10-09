//! Postgres storage for governance tags. Mirrors the role module: free functions
//! taking a connection/transaction, with constraint-name -> typed-error mapping.

use std::collections::HashMap;

use lakekeeper::{
    CONFIG, ProjectId,
    api::iceberg::v1::PaginationQuery,
    service::{
        ApplyTagError, CatalogBackendError, CatalogCreateTagDefinitionRequest,
        CreateTagDefinitionError, DatasetId, DeleteTagDefinitionError, EffectiveTagCandidate,
        EffectiveTagSource, GenericTableId, ListTagAttachmentsError, ListTagAttachmentsResponse,
        ListTagDefinitionsError, ListTagDefinitionsResponse, NamespaceId, ProjectIdNotFoundError,
        RemoveTagError, Result, TableId, TabularId, Tag, TagAttachmentFilter, TagDefinition,
        TagDefinitionId, TagDefinitionIdNotFound, TagDefinitionInUse, TagId, TagNameAlreadyExists,
        TagNotFound, TagScope, TagSource, TagTarget, TagTargetNotFound, TagValueKind, TagWithName,
        UpdateTagDefinitionError, UpdateTagDefinitionRequest, ViewId, WarehouseId,
    },
};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::{
    dbutils::DBErrorHandler,
    pagination::{PaginateToken, V1PaginateToken},
    tabular::TabularType,
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
                TagScope::parse(s).ok_or_else(|| {
                    CatalogBackendError::new_unexpected(unknown_enum("tag scope", s))
                })
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
    } = request;
    let value_kind = value_spec.kind();
    let allowed_values = value_spec.allowed_values();

    let scope: Vec<String> = scope.iter().map(|s| s.as_str().to_string()).collect();

    let row = sqlx::query_as!(
        TagDefinitionRow,
        r#"
        INSERT INTO tag_definition
            (tag_definition_id, project_id, name, description, scope, value_kind)
        VALUES ($1, $2, $3, $4, $5::text[], $6)
        RETURNING
            tag_definition_id,
            project_id,
            name,
            description,
            scope,
            value_kind AS "value_kind: TagValueKind",
            created_at,
            updated_at
        "#,
        *tag_definition_id,
        &**project_id,
        name,
        description,
        &scope,
        value_kind as _,
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
            updated_at
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

/// Fetch a single tag definition by case-insensitive name within the project
/// (matches the `lower(name)` unique index). Returns `None` if no definition with
/// that name exists in the project. Scalar only — allowed values are fetched
/// separately.
pub(crate) async fn get_tag_definition_by_name<'e, 'c: 'e, E>(
    project_id: &ProjectId,
    name: &str,
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
            updated_at
        FROM tag_definition
        WHERE project_id = $1 AND lower(name) = lower($2)
        "#,
        &**project_id,
        name,
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

    let token: Option<PaginateToken<Uuid>> = page_token
        .as_option()
        .map(PaginateToken::try_from)
        .transpose()?;
    let (token_ts, token_id) = token
        .as_ref()
        .map(PaginateToken::v1_parts)
        .transpose()?
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
            updated_at
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
) -> Result<(TagDefinition, Vec<String>), UpdateTagDefinitionError> {
    let UpdateTagDefinitionRequest {
        name,
        description,
        scope,
        add_allowed_values,
    } = request;
    let scope: Vec<String> = scope.iter().map(|s| s.as_str().to_string()).collect();

    let row = sqlx::query_as!(
        TagDefinitionRow,
        r#"
        UPDATE tag_definition
        SET name = $3, description = $4, scope = $5::text[]
        WHERE project_id = $1 AND tag_definition_id = $2
        RETURNING
            tag_definition_id,
            project_id,
            name,
            description,
            scope,
            value_kind AS "value_kind: TagValueKind",
            created_at,
            updated_at
        "#,
        &**project_id,
        *tag_definition_id,
        name,
        description,
        &scope,
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

    // Read the merged allowed values back in the same transaction so the caller can
    // echo them without a post-commit read (which would hit a possibly-lagging
    // replica). Empty for non-enumerated definitions.
    let allowed_values = sqlx::query_scalar!(
        r#"SELECT value FROM tag_allowed_value WHERE tag_definition_id = $1 ORDER BY value"#,
        *tag_definition_id,
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(|e| UpdateTagDefinitionError::from(e.into_catalog_backend_error()))?;

    Ok((TagDefinition::try_from(row)?, allowed_values))
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
            // Deleting a `tag_definition` that still has attachments trips the `tag`
            // FK's ON DELETE RESTRICT. The constraint name is stable across versions;
            // the SQLSTATE is not: Postgres 18 raises RESTRICT as `restrict_violation`
            // (23001), whereas <=17 used `foreign_key_violation` (23503).
            sqlx::Error::Database(db_error)
                if matches!(db_error.code().as_deref(), Some("23001" | "23503"))
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

/// Attach a tag definition to a target. Idempotent per (target, definition, source). Returns the
/// attachment and whether it actually changed: re-applying an **identical** value is a true no-op —
/// no `UPDATE` (so `updated_at` is not bumped) and `changed = false` (so the caller skips the
/// change event). Applying a new/different value returns `changed = true`. Maps a missing
/// definition / missing target to typed errors; value legality is validated by the caller.
pub(crate) async fn apply_tag(
    tag_id: TagId,
    tag_definition_id: TagDefinitionId,
    target: TagTarget,
    value: Option<&str>,
    source: TagSource,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(Tag, bool), ApplyTagError> {
    let cols = TagTargetColumns::from_target(target);

    // The `DO UPDATE ... WHERE value differs` makes a same-value re-apply a no-op: Postgres
    // performs no update and RETURNING yields no row (so the `updated_at` trigger never fires).
    let updated = sqlx::query!(
        r#"
        INSERT INTO tag
            (tag_id, tag_definition_id, warehouse_id, namespace_id, tabular_id, field_id,
             value, source)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT ON CONSTRAINT tag_unique_target_definition_source
        DO UPDATE SET value = EXCLUDED.value
            WHERE tag.value IS DISTINCT FROM EXCLUDED.value
        RETURNING tag_id, value, created_at, updated_at
        "#,
        *tag_id,
        *tag_definition_id,
        cols.warehouse_id,
        cols.namespace_id,
        cols.tabular_id,
        cols.field_id,
        value,
        source as _,
    )
    .fetch_optional(&mut **transaction)
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

    if let Some(row) = updated {
        return Ok((
            Tag {
                tag_id: TagId::new(row.tag_id),
                tag_definition_id,
                target,
                value: row.value,
                source,
                created_at: row.created_at,
                updated_at: row.updated_at,
            },
            true,
        ));
    }

    // No-op: an attachment with this exact value already exists. Read it back (by the same
    // unique key the ON CONFLICT targets) and report it unchanged.
    let existing = sqlx::query!(
        r#"
        SELECT tag_id, value, created_at, updated_at
        FROM tag
        WHERE warehouse_id = $1
          AND namespace_id IS NOT DISTINCT FROM $2
          AND tabular_id IS NOT DISTINCT FROM $3
          AND field_id IS NOT DISTINCT FROM $4
          AND tag_definition_id = $5
          AND source = $6
        "#,
        cols.warehouse_id,
        cols.namespace_id,
        cols.tabular_id,
        cols.field_id,
        *tag_definition_id,
        source as _,
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| ApplyTagError::from(e.into_catalog_backend_error()))?;

    Ok((
        Tag {
            tag_id: TagId::new(existing.tag_id),
            tag_definition_id,
            target,
            value: existing.value,
            source,
            created_at: existing.created_at,
            updated_at: existing.updated_at,
        },
        false,
    ))
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

/// Atomically delete the `(target, definition, source)` attachment and return it,
/// or `None` if it was not attached. A single `DELETE ... RETURNING` in the caller's
/// write transaction: it reads its own primary (no replica lag between "find" and
/// "delete") and is safe under concurrent deletes — the row is locked, so a losing
/// racer simply sees no row and gets `None` (idempotent) rather than an error. The
/// full unique key is matched, so at most one row is affected.
pub(crate) async fn remove_tag_for_target(
    target: TagTarget,
    tag_definition_id: TagDefinitionId,
    source: TagSource,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<Option<Tag>, RemoveTagError> {
    let cols = TagTargetColumns::from_target(target);
    let removed = sqlx::query!(
        r#"
        DELETE FROM tag
        WHERE warehouse_id = $1
          AND namespace_id IS NOT DISTINCT FROM $2
          AND tabular_id IS NOT DISTINCT FROM $3
          AND field_id IS NOT DISTINCT FROM $4
          AND tag_definition_id = $5
          AND source = $6
        RETURNING tag_id, value, created_at, updated_at
        "#,
        cols.warehouse_id,
        cols.namespace_id,
        cols.tabular_id,
        cols.field_id,
        *tag_definition_id,
        source as _,
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| RemoveTagError::from(e.into_catalog_backend_error()))?;

    Ok(removed.map(|r| Tag {
        tag_id: TagId::new(r.tag_id),
        tag_definition_id,
        target,
        value: r.value,
        source,
        created_at: r.created_at,
        updated_at: r.updated_at,
    }))
}

struct TagWithNameRow {
    tag_id: Uuid,
    tag_definition_id: Uuid,
    value: Option<String>,
    source: TagSource,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: Option<chrono::DateTime<chrono::Utc>>,
    name: String,
}

/// List the tags attached to exactly `target` (with each definition's name),
/// ordered by `(created_at, tag_id)`. Branches per target shape so the WHERE uses
/// sargable `= $n` / `IS NULL` predicates — `IS NOT DISTINCT FROM $n` is not
/// index-usable and would scan every tag in the warehouse — and joins
/// `tag_definition` so the caller needs no per-row name lookup.
pub(crate) async fn list_tags_for_target<'e, 'c: 'e, E>(
    target: TagTarget,
    connection: E,
) -> Result<Vec<TagWithName>, CatalogBackendError>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let wh = *target.warehouse_id();
    let rows = match target {
        TagTarget::Warehouse(_) => {
            sqlx::query_as!(
                TagWithNameRow,
                r#"SELECT t.tag_id, t.tag_definition_id, t.value,
                        t.source AS "source: TagSource", t.created_at, t.updated_at, td.name
                   FROM tag t JOIN tag_definition td USING (tag_definition_id)
                   WHERE t.warehouse_id = $1 AND t.namespace_id IS NULL
                     AND t.tabular_id IS NULL AND t.field_id IS NULL
                   ORDER BY t.created_at, t.tag_id"#,
                wh,
            )
            .fetch_all(connection)
            .await
        }
        TagTarget::Namespace { namespace_id, .. } => {
            sqlx::query_as!(
                TagWithNameRow,
                r#"SELECT t.tag_id, t.tag_definition_id, t.value,
                        t.source AS "source: TagSource", t.created_at, t.updated_at, td.name
                   FROM tag t JOIN tag_definition td USING (tag_definition_id)
                   WHERE t.warehouse_id = $1 AND t.namespace_id = $2
                     AND t.tabular_id IS NULL AND t.field_id IS NULL
                   ORDER BY t.created_at, t.tag_id"#,
                wh,
                *namespace_id,
            )
            .fetch_all(connection)
            .await
        }
        TagTarget::Tabular { tabular_id, .. } => {
            sqlx::query_as!(
                TagWithNameRow,
                r#"SELECT t.tag_id, t.tag_definition_id, t.value,
                        t.source AS "source: TagSource", t.created_at, t.updated_at, td.name
                   FROM tag t JOIN tag_definition td USING (tag_definition_id)
                   WHERE t.warehouse_id = $1 AND t.namespace_id IS NULL
                     AND t.tabular_id = $2 AND t.field_id IS NULL
                   ORDER BY t.created_at, t.tag_id"#,
                wh,
                *tabular_id.as_ref(),
            )
            .fetch_all(connection)
            .await
        }
        TagTarget::Column {
            tabular_id,
            field_id,
            ..
        } => {
            sqlx::query_as!(
                TagWithNameRow,
                r#"SELECT t.tag_id, t.tag_definition_id, t.value,
                        t.source AS "source: TagSource", t.created_at, t.updated_at, td.name
                   FROM tag t JOIN tag_definition td USING (tag_definition_id)
                   WHERE t.warehouse_id = $1 AND t.namespace_id IS NULL
                     AND t.tabular_id = $2 AND t.field_id = $3
                   ORDER BY t.created_at, t.tag_id"#,
                wh,
                *tabular_id.as_ref(),
                field_id,
            )
            .fetch_all(connection)
            .await
        }
    }
    .map_err(DBErrorHandler::into_catalog_backend_error)?;

    Ok(rows
        .into_iter()
        .map(|r| TagWithName {
            tag: Tag {
                tag_id: TagId::new(r.tag_id),
                tag_definition_id: TagDefinitionId::new(r.tag_definition_id),
                target,
                value: r.value,
                source: r.source,
                created_at: r.created_at,
                updated_at: r.updated_at,
            },
            definition_name: r.name,
        })
        .collect())
}

/// The direct tags on every target in `targets`, echoing the caller's own `TagTarget`.
///
/// The batched form of [`list_tags_for_target`]. One arm per target shape, so each arm keeps
/// the sargable predicates the single-target read needs: the unique index serves the
/// warehouse and namespace arms, `tag_tabular_idx` the tabular and column arms.
///
/// No join back to `tabular`, so a tabular's kind is echoed from the caller's entry and tags
/// on soft-deleted tabulars are included. Two entries naming one tabular under different
/// kinds collide, and the later one takes every row.
///
/// Unordered — the caller buckets per target anyway.
///
/// Row count is the sum over `targets` of the definitions on each. Tag definitions are
/// customer data with no cap, so a large batch can return thousands of rows.
///
/// Every arm with more than one array over-matches: the namespace and tabular arms cross
/// two arrays, the column arm three. The maps below drop what was not asked for, so the
/// result is exact, but more rows may be fetched than returned.
pub(crate) async fn list_tags_on_targets<'e, 'c: 'e, E>(
    targets: &[TagTarget],
    connection: E,
) -> Result<Vec<TagWithName>, CatalogBackendError>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    if targets.is_empty() {
        return Ok(Vec::new());
    }

    // The arrays match as a cross product, and the maps tighten it afterwards. They key on
    // the full tuple because a tabular id can exist in two warehouses, so a bare id would
    // echo one warehouse's tag onto the other's object.
    let mut warehouse_ids: Vec<Uuid> = Vec::new();
    let mut namespace_warehouses: Vec<Uuid> = Vec::new();
    let mut namespace_ids: Vec<Uuid> = Vec::new();
    let mut tabular_warehouses: Vec<Uuid> = Vec::new();
    let mut tabular_ids: Vec<Uuid> = Vec::new();
    let mut column_warehouses: Vec<Uuid> = Vec::new();
    let mut column_tabulars: Vec<Uuid> = Vec::new();
    let mut column_fields: Vec<i32> = Vec::new();
    let mut warehouses: HashMap<Uuid, TagTarget> = HashMap::new();
    let mut namespaces: HashMap<(Uuid, Uuid), TagTarget> = HashMap::new();
    let mut tabulars: HashMap<(Uuid, Uuid), TagTarget> = HashMap::new();
    let mut columns: HashMap<(Uuid, Uuid, i32), TagTarget> = HashMap::new();
    for target in targets {
        let wh = *target.warehouse_id();
        match target {
            TagTarget::Warehouse(_) => {
                warehouse_ids.push(wh);
                warehouses.insert(wh, *target);
            }
            TagTarget::Namespace { namespace_id, .. } => {
                namespace_warehouses.push(wh);
                namespace_ids.push(**namespace_id);
                namespaces.insert((wh, **namespace_id), *target);
            }
            TagTarget::Tabular { tabular_id, .. } => {
                tabular_warehouses.push(wh);
                tabular_ids.push(*tabular_id.as_ref());
                tabulars.insert((wh, *tabular_id.as_ref()), *target);
            }
            TagTarget::Column {
                tabular_id,
                field_id,
                ..
            } => {
                column_warehouses.push(wh);
                column_tabulars.push(*tabular_id.as_ref());
                column_fields.push(*field_id);
                columns.insert((wh, *tabular_id.as_ref(), *field_id), *target);
            }
        }
    }

    // Needed for the query plan, not for the result: duplicates change no row. Postgres
    // costs `= ANY` as one index descent per element, so repeating the warehouse once per
    // target inflates the estimate by the number of targets. With enough tabulars the
    // planner then picks a sequential scan of `tag`, measured ~30x slower than the index
    // scan. The arrays are independent `= ANY` sets, not zipped by position. Do not remove.
    for ids in [
        &mut warehouse_ids,
        &mut namespace_warehouses,
        &mut namespace_ids,
        &mut tabular_warehouses,
        &mut tabular_ids,
        &mut column_warehouses,
        &mut column_tabulars,
    ] {
        ids.sort_unstable();
        ids.dedup();
    }
    column_fields.sort_unstable();
    column_fields.dedup();

    let rows = sqlx::query!(
        r#"
        SELECT
            t.tag_id, t.tag_definition_id,
            t.warehouse_id, t.namespace_id, t.tabular_id, t.field_id,
            t.value, t.source AS "source: TagSource",
            t.created_at, t.updated_at,
            -- Looked up per row, not joined: a join lets the planner hash or merge every
            -- project's definitions, measured ~20 ms against 0.04 ms for a table load.
            -- `!`: the foreign key guarantees the definition exists.
            (SELECT td.name FROM tag_definition td
             WHERE td.tag_definition_id = t.tag_definition_id) AS "name!"
        FROM tag t
        WHERE (t.warehouse_id = ANY($1) AND t.namespace_id IS NULL
               AND t.tabular_id IS NULL AND t.field_id IS NULL)
           OR (t.warehouse_id = ANY($2) AND t.namespace_id = ANY($3)
               AND t.tabular_id IS NULL AND t.field_id IS NULL)
           -- field_id IS NULL keeps a table's column tags out of the table's own rows.
           -- The match below would drop them anyway, so this is selectivity, not
           -- correctness -- unlike in `list_tags_for_target`, which has no post-filter.
           OR (t.warehouse_id = ANY($4) AND t.tabular_id = ANY($5)
               AND t.namespace_id IS NULL AND t.field_id IS NULL)
           OR (t.warehouse_id = ANY($6) AND t.tabular_id = ANY($7)
               AND t.namespace_id IS NULL AND t.field_id = ANY($8))
        "#,
        &warehouse_ids,
        &namespace_warehouses,
        &namespace_ids,
        &tabular_warehouses,
        &tabular_ids,
        &column_warehouses,
        &column_tabulars,
        &column_fields,
    )
    .fetch_all(connection)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;

    Ok(rows
        .into_iter()
        .filter_map(|r| {
            // Most specific first: field_id means a column, tabular_id a tabular.
            let target = match (r.namespace_id, r.tabular_id, r.field_id) {
                (_, Some(tabular_id), Some(field_id)) => {
                    columns.get(&(r.warehouse_id, tabular_id, field_id))
                }
                (_, Some(tabular_id), None) => tabulars.get(&(r.warehouse_id, tabular_id)),
                (Some(namespace_id), None, _) => namespaces.get(&(r.warehouse_id, namespace_id)),
                (None, None, _) => warehouses.get(&r.warehouse_id),
            };
            // A miss is the cross product over-matching: not asked for, so drop the row.
            let target = *target?;
            Some(TagWithName {
                tag: Tag {
                    tag_id: TagId::new(r.tag_id),
                    tag_definition_id: TagDefinitionId::new(r.tag_definition_id),
                    target,
                    value: r.value,
                    source: r.source,
                    created_at: r.created_at,
                    updated_at: r.updated_at,
                },
                definition_name: r.name,
            })
        })
        .collect())
}

/// All governance tags attached to any column of `tabular_id` (`field_id IS NOT NULL`),
/// each carrying its column's field-id in the reconstructed `Column` target. Ordered by
/// field-id so the caller can group per column. Unpaginated, like the other forward
/// per-target tag lists.
pub(crate) async fn list_column_tags_for_tabular<'e, 'c: 'e, E>(
    warehouse_id: WarehouseId,
    tabular_id: TabularId,
    connection: E,
) -> Result<Vec<TagWithName>, CatalogBackendError>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let wh = *warehouse_id;
    let rows = sqlx::query!(
        r#"SELECT t.tag_id, t.tag_definition_id, t.value,
                t.source AS "source: TagSource", t.created_at, t.updated_at, td.name,
                t.field_id AS "field_id!"
           FROM tag t JOIN tag_definition td USING (tag_definition_id)
           WHERE t.warehouse_id = $1 AND t.namespace_id IS NULL
             AND t.tabular_id = $2 AND t.field_id IS NOT NULL
           ORDER BY t.field_id, t.created_at, t.tag_id"#,
        wh,
        *tabular_id.as_ref(),
    )
    .fetch_all(connection)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;

    Ok(rows
        .into_iter()
        .map(|r| TagWithName {
            tag: Tag {
                tag_id: TagId::new(r.tag_id),
                tag_definition_id: TagDefinitionId::new(r.tag_definition_id),
                target: TagTarget::Column {
                    warehouse_id,
                    tabular_id,
                    field_id: r.field_id,
                },
                value: r.value,
                source: r.source,
                created_at: r.created_at,
                updated_at: r.updated_at,
            },
            definition_name: r.name,
        })
        .collect())
}

/// Rebuild a domain [`TagTarget`] from a `tag` row's nullable target columns plus the
/// tabular subtype recovered from the `tabular` join. Inverse of [`TagTargetColumns::from_target`]:
/// the row stores a bare `tabular_id`, so the Table/View/GenericTable discriminator comes from the
/// join. A set `tabular_id` with no subtype means the `tabular` row is gone — impossible under the
/// `tag_tabular_fkey`/`tag_field_fkey` constraints, so it is an unexpected backend error.
fn reconstruct_target(
    warehouse_id: Uuid,
    namespace_id: Option<Uuid>,
    tabular_id: Option<Uuid>,
    field_id: Option<i32>,
    tabular_type: Option<TabularType>,
) -> Result<TagTarget, CatalogBackendError> {
    let warehouse_id = WarehouseId::new(warehouse_id);
    if let Some(namespace_id) = namespace_id {
        return Ok(TagTarget::Namespace {
            warehouse_id,
            namespace_id: NamespaceId::new(namespace_id),
        });
    }
    let Some(tabular_id) = tabular_id else {
        return Ok(TagTarget::Warehouse(warehouse_id));
    };
    let tabular_id = match tabular_type {
        Some(TabularType::Table) => TabularId::Table(TableId::new(tabular_id)),
        Some(TabularType::View) => TabularId::View(ViewId::new(tabular_id)),
        Some(TabularType::GenericTable) => TabularId::GenericTable(GenericTableId::new(tabular_id)),
        Some(TabularType::Dataset) => TabularId::Dataset(DatasetId::new(tabular_id)),
        None => {
            return Err(CatalogBackendError::new_unexpected(unknown_enum(
                "tabular type for tag target",
                "missing",
            )));
        }
    };
    Ok(match field_id {
        Some(field_id) => TagTarget::Column {
            warehouse_id,
            tabular_id,
            field_id,
        },
        None => TagTarget::Tabular {
            warehouse_id,
            tabular_id,
        },
    })
}

/// Reverse lookup: the targets a definition is directly attached to, optionally filtered to a
/// single `value`, keyset-paginated by `(created_at, tag_id)`. Joins `tabular` to recover the
/// Table/View/GenericTable subtype the `tag` row does not store. Direct attachments only — no
/// hierarchy expansion.
pub(crate) async fn list_tag_attachments<'e, 'c: 'e, E>(
    tag_definition_id: TagDefinitionId,
    filter: &TagAttachmentFilter,
    PaginationQuery {
        page_size,
        page_token,
    }: PaginationQuery,
    connection: E,
) -> Result<ListTagAttachmentsResponse, ListTagAttachmentsError>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let page_size = CONFIG.page_size_or_pagination_default(page_size);

    let token: Option<PaginateToken<Uuid>> = page_token
        .as_option()
        .map(PaginateToken::try_from)
        .transpose()?;
    let (token_ts, token_id) = token
        .as_ref()
        .map(PaginateToken::v1_parts)
        .transpose()?
        .unzip();

    let rows = sqlx::query!(
        r#"
        SELECT
            t.tag_id,
            t.warehouse_id,
            t.namespace_id,
            t.tabular_id,
            t.field_id,
            t.value,
            t.source AS "source: TagSource",
            t.created_at,
            t.updated_at,
            tab.typ AS "tabular_type?: TabularType"
        FROM tag t
        LEFT JOIN tabular tab
            ON tab.warehouse_id = t.warehouse_id AND tab.tabular_id = t.tabular_id
        WHERE t.tag_definition_id = $1
            -- Exclude attachments on soft-deleted (recycle-bin) tabulars: a "which
            -- objects carry this tag" view lists live objects only. Warehouse/namespace
            -- rows have no tabular (tab.tabular_id IS NULL) and are always kept.
            AND (tab.tabular_id IS NULL OR tab.deleted_at IS NULL)
            AND ($5::text IS NULL OR t.value = $5)
            AND ($6::uuid IS NULL OR t.warehouse_id = $6)
            AND ($7::timestamptz IS NULL OR t.created_at >= $7)
            AND ($8::timestamptz IS NULL OR t.created_at <= $8)
            -- Target-type filter: derive each row's object type from its column shape
            -- (columns/namespaces/warehouses) and the joined tabular subtype. `tab.typ`
            -- and `TagScope::as_str` are both kebab-case, so the strings line up.
            AND ($9::text IS NULL OR $9 = CASE
                WHEN t.field_id IS NOT NULL THEN 'column'
                WHEN t.tabular_id IS NOT NULL THEN tab.typ::text
                WHEN t.namespace_id IS NOT NULL THEN 'namespace'
                ELSE 'warehouse' END)
            AND ((t.created_at > $2 OR $2 IS NULL) OR (t.created_at = $2 AND t.tag_id > $3))
        ORDER BY t.created_at, t.tag_id ASC
        LIMIT $4
        "#,
        *tag_definition_id,
        token_ts,
        token_id,
        page_size,
        filter.value.as_deref(),
        filter.warehouse_id.map(|w| *w),
        filter.created_after,
        filter.created_before,
        filter.target_type.map(TagScope::as_str),
    )
    .fetch_all(connection)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;

    let tags = rows
        .into_iter()
        .map(|r| {
            let target = reconstruct_target(
                r.warehouse_id,
                r.namespace_id,
                r.tabular_id,
                r.field_id,
                r.tabular_type,
            )?;
            Ok(Tag {
                tag_id: TagId::new(r.tag_id),
                tag_definition_id,
                target,
                value: r.value,
                source: r.source,
                created_at: r.created_at,
                updated_at: r.updated_at,
            })
        })
        .collect::<std::result::Result<Vec<_>, CatalogBackendError>>()?;

    let next_page_token = tags.last().map(|t| {
        PaginateToken::V1(V1PaginateToken::<Uuid> {
            created_at: t.created_at,
            id: *t.tag_id,
        })
        .to_string()
    });

    Ok(ListTagAttachmentsResponse {
        tags,
        next_page_token,
    })
}

/// Build an [`EffectiveTagSource`] from a gather row's `kind`/`origin_namespace_id`.
fn effective_origin(
    warehouse_id: WarehouseId,
    kind: &str,
    origin_namespace_id: Option<Uuid>,
) -> Result<EffectiveTagSource, CatalogBackendError> {
    Ok(match kind {
        "direct" => EffectiveTagSource::Direct,
        "warehouse" => EffectiveTagSource::Warehouse { warehouse_id },
        "namespace" => {
            let namespace_id = origin_namespace_id.ok_or_else(|| {
                CatalogBackendError::new_unexpected(unknown_enum(
                    "effective tag origin",
                    "namespace without id",
                ))
            })?;
            EffectiveTagSource::Namespace {
                warehouse_id,
                namespace_id: NamespaceId::new(namespace_id),
            }
        }
        other => {
            return Err(CatalogBackendError::new_unexpected(unknown_enum(
                "effective tag kind",
                other,
            )));
        }
    })
}

/// Gather candidate effective tags for `target`: the target's own direct tags plus tags on its
/// ancestors (parent namespaces + warehouse), each annotated with a containment `distance` and
/// origin. One statement per target type. `field_id IS NULL` is applied throughout so a table's
/// own column tags never leak into its effective set. Unresolved/unfiltered — the caller applies
/// visibility and most-specific-wins. Callers must have resolved/authorized the target first.
pub(crate) async fn list_effective_tag_candidates<'e, 'c: 'e, E>(
    target: TagTarget,
    connection: E,
) -> Result<Vec<EffectiveTagCandidate>, CatalogBackendError>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let warehouse_id = target.warehouse_id();
    let wh = *warehouse_id;

    // Every arm SELECTs the same columns so one mapper handles all shapes:
    // tag_id, tag_definition_id, name, value, source, created_at, updated_at,
    // origin_namespace_id, distance, kind.
    match target {
        TagTarget::Warehouse(_) => {
            let rows = sqlx::query!(
                r#"
                SELECT t.tag_id, t.tag_definition_id, td.name,
                       t.value, t.source AS "source: TagSource", t.created_at, t.updated_at,
                       NULL::uuid AS origin_namespace_id, 0 AS "distance!", 'direct' AS "kind!"
                FROM tag t
                JOIN tag_definition td ON td.tag_definition_id = t.tag_definition_id
                WHERE t.warehouse_id = $1
                  AND t.namespace_id IS NULL AND t.tabular_id IS NULL AND t.field_id IS NULL
                "#,
                wh,
            )
            .fetch_all(connection)
            .await
            .map_err(DBErrorHandler::into_catalog_backend_error)?;
            rows.into_iter()
                .map(|r| {
                    Ok(EffectiveTagCandidate {
                        tag_id: TagId::new(r.tag_id),
                        tag_definition_id: TagDefinitionId::new(r.tag_definition_id),
                        name: r.name,
                        value: r.value,
                        source: r.source,
                        created_at: r.created_at,
                        updated_at: r.updated_at,
                        distance: r.distance,
                        origin: effective_origin(warehouse_id, &r.kind, r.origin_namespace_id)?,
                    })
                })
                .collect()
        }
        TagTarget::Column {
            tabular_id,
            field_id,
            ..
        } => {
            let rows = sqlx::query!(
                r#"
                SELECT t.tag_id, t.tag_definition_id, td.name,
                       t.value, t.source AS "source: TagSource", t.created_at, t.updated_at,
                       NULL::uuid AS origin_namespace_id, 0 AS "distance!", 'direct' AS "kind!"
                FROM tag t
                JOIN tag_definition td ON td.tag_definition_id = t.tag_definition_id
                WHERE t.warehouse_id = $1 AND t.tabular_id = $2 AND t.field_id = $3
                "#,
                wh,
                *tabular_id.as_ref(),
                field_id,
            )
            .fetch_all(connection)
            .await
            .map_err(DBErrorHandler::into_catalog_backend_error)?;
            rows.into_iter()
                .map(|r| {
                    Ok(EffectiveTagCandidate {
                        tag_id: TagId::new(r.tag_id),
                        tag_definition_id: TagDefinitionId::new(r.tag_definition_id),
                        name: r.name,
                        value: r.value,
                        source: r.source,
                        created_at: r.created_at,
                        updated_at: r.updated_at,
                        distance: r.distance,
                        origin: effective_origin(warehouse_id, &r.kind, r.origin_namespace_id)?,
                    })
                })
                .collect()
        }
        TagTarget::Namespace { namespace_id, .. } => {
            // `ancestors` = the target namespace itself + every strict-prefix parent.
            // Prefixes are materialised with `generate_series` and matched by equality
            // (`IN`) so the lookup uses the namespace-name index (repo idiom, sargable);
            // NOT a per-row positional slice. Own row -> distance 0 (direct); parent at
            // depth d -> D - d; warehouse -> D. `CROSS JOIN anchor` yields zero rows (not
            // a NULL distance) if the target namespace was concurrently dropped.
            let rows = sqlx::query!(
                r#"
                WITH anchor AS (
                    SELECT namespace_name AS path, array_length(namespace_name, 1) AS depth
                    FROM namespace WHERE warehouse_id = $1 AND namespace_id = $2
                ),
                prefixes AS (
                    SELECT DISTINCT path[1:generate_series(1, depth)] AS name FROM anchor
                ),
                ancestors AS (
                    SELECT n.namespace_id, array_length(n.namespace_name, 1) AS depth
                    FROM namespace n
                    WHERE n.warehouse_id = $1
                      AND n.namespace_name IN (SELECT name FROM prefixes)
                )
                SELECT t.tag_id, t.tag_definition_id, td.name,
                       t.value, t.source AS "source: TagSource", t.created_at, t.updated_at,
                       t.namespace_id AS origin_namespace_id,
                       (CASE
                          WHEN t.namespace_id = $2 THEN 0
                          WHEN t.namespace_id IS NOT NULL THEN a.depth - anc.depth
                          ELSE a.depth
                        END) AS "distance!",
                       (CASE
                          WHEN t.namespace_id = $2 THEN 'direct'
                          WHEN t.namespace_id IS NOT NULL THEN 'namespace'
                          ELSE 'warehouse'
                        END) AS "kind!"
                FROM tag t
                JOIN tag_definition td ON td.tag_definition_id = t.tag_definition_id
                LEFT JOIN ancestors anc ON anc.namespace_id = t.namespace_id
                CROSS JOIN anchor a
                WHERE t.warehouse_id = $1
                  AND t.tabular_id IS NULL AND t.field_id IS NULL
                  AND (t.namespace_id IN (SELECT namespace_id FROM ancestors)
                       OR t.namespace_id IS NULL)
                "#,
                wh,
                *namespace_id,
            )
            .fetch_all(connection)
            .await
            .map_err(DBErrorHandler::into_catalog_backend_error)?;
            rows.into_iter()
                .map(|r| {
                    Ok(EffectiveTagCandidate {
                        tag_id: TagId::new(r.tag_id),
                        tag_definition_id: TagDefinitionId::new(r.tag_definition_id),
                        name: r.name,
                        value: r.value,
                        source: r.source,
                        created_at: r.created_at,
                        updated_at: r.updated_at,
                        distance: r.distance,
                        origin: effective_origin(warehouse_id, &r.kind, r.origin_namespace_id)?,
                    })
                })
                .collect()
        }
        TagTarget::Tabular { tabular_id, .. } => {
            // Anchor = the tabular's containing namespace; `ancestors` = that namespace +
            // its parents (materialised prefixes matched by `IN`, index-friendly). Direct
            // tabular row -> distance 0; namespace at depth d -> D - d + 1; warehouse ->
            // D + 1. `CROSS JOIN anchor` yields zero rows (not a NULL distance) if the
            // tabular was concurrently dropped.
            let rows = sqlx::query!(
                r#"
                WITH anchor AS (
                    SELECT ns.namespace_name AS path, array_length(ns.namespace_name, 1) AS depth
                    FROM tabular tb
                    JOIN namespace ns ON ns.namespace_id = tb.namespace_id
                    WHERE tb.warehouse_id = $1 AND tb.tabular_id = $2
                ),
                prefixes AS (
                    SELECT DISTINCT path[1:generate_series(1, depth)] AS name FROM anchor
                ),
                ancestors AS (
                    SELECT n.namespace_id, array_length(n.namespace_name, 1) AS depth
                    FROM namespace n
                    WHERE n.warehouse_id = $1
                      AND n.namespace_name IN (SELECT name FROM prefixes)
                )
                SELECT t.tag_id, t.tag_definition_id, td.name,
                       t.value, t.source AS "source: TagSource", t.created_at, t.updated_at,
                       t.namespace_id AS origin_namespace_id,
                       (CASE
                          WHEN t.tabular_id IS NOT NULL THEN 0
                          WHEN t.namespace_id IS NOT NULL THEN a.depth - anc.depth + 1
                          ELSE a.depth + 1
                        END) AS "distance!",
                       (CASE
                          WHEN t.tabular_id IS NOT NULL THEN 'direct'
                          WHEN t.namespace_id IS NOT NULL THEN 'namespace'
                          ELSE 'warehouse'
                        END) AS "kind!"
                FROM tag t
                JOIN tag_definition td ON td.tag_definition_id = t.tag_definition_id
                LEFT JOIN ancestors anc ON anc.namespace_id = t.namespace_id
                CROSS JOIN anchor a
                WHERE t.warehouse_id = $1
                  AND t.field_id IS NULL
                  AND ((t.tabular_id = $2)
                       OR (t.tabular_id IS NULL
                           AND t.namespace_id IN (SELECT namespace_id FROM ancestors))
                       OR (t.tabular_id IS NULL AND t.namespace_id IS NULL))
                "#,
                wh,
                *tabular_id.as_ref(),
            )
            .fetch_all(connection)
            .await
            .map_err(DBErrorHandler::into_catalog_backend_error)?;
            rows.into_iter()
                .map(|r| {
                    Ok(EffectiveTagCandidate {
                        tag_id: TagId::new(r.tag_id),
                        tag_definition_id: TagDefinitionId::new(r.tag_definition_id),
                        name: r.name,
                        value: r.value,
                        source: r.source,
                        created_at: r.created_at,
                        updated_at: r.updated_at,
                        distance: r.distance,
                        origin: effective_origin(warehouse_id, &r.kind, r.origin_namespace_id)?,
                    })
                })
                .collect()
        }
    }
}

#[cfg(test)]
mod tests {
    use iceberg::{
        NamespaceIdent,
        spec::{NestedField, PrimitiveType, Schema, Type},
    };
    use lakekeeper::{
        api::iceberg::types::PageToken,
        service::{
            CatalogStore, TabularId, TagScope, TagValueKind, TagValueSpec, Transaction as _,
            resolve_effective_tags, resolve_effective_tags_from_chain,
        },
    };

    use super::*;
    use crate::{
        CatalogState, PostgresBackend, PostgresTransaction,
        namespace::tests::initialize_namespace,
        tabular::table::tests::{create_table_with_schema, initialize_table},
        warehouse::test::initialize_warehouse,
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
        let mut t = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        PostgresBackend::create_project(
            project_id,
            format!("Project {project_id}"),
            t.transaction(),
        )
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
                .build(),
            &mut txn,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            CreateTagDefinitionError::ProjectIdNotFoundError(_)
        ));
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
                .build(),
            &mut txn,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            CreateTagDefinitionError::TagNameAlreadyExists(_)
        ));
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
    async fn test_get_tag_definition_by_name(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let project_id = ProjectId::new_random();
        create_project(&state, &project_id).await;

        let id = TagDefinitionId::new_random();
        let mut txn = pool.begin().await.unwrap();
        let created = create_tag_definition(
            &project_id,
            CatalogCreateTagDefinitionRequest::builder()
                .tag_definition_id(id)
                .name("PII.Email")
                .description(Some("Email addresses"))
                .scope(&[TagScope::Column])
                .value_spec(TagValueSpec::Marker)
                .build(),
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        // Lookup is case-insensitive.
        let got = get_tag_definition_by_name(&project_id, "pii.email", &pool)
            .await
            .unwrap();
        assert_eq!(got, Some(created));

        // Unknown name -> None.
        let absent = get_tag_definition_by_name(&project_id, "pii.phone", &pool)
            .await
            .unwrap();
        assert_eq!(absent, None);

        // A definition is invisible from a different project (no cross-project reads).
        let other_project = ProjectId::new_random();
        create_project(&state, &other_project).await;
        let cross = get_tag_definition_by_name(&other_project, "PII.Email", &pool)
            .await
            .unwrap();
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
            PaginationQuery {
                page_size: Some(10),
                page_token: PageToken::Empty,
            },
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
                    .build(),
                &mut txn,
            )
            .await
            .unwrap();
            txn.commit().await.unwrap();
        }

        let all = list_tag_definitions(
            &project_id,
            PaginationQuery {
                page_size: Some(10),
                page_token: PageToken::Empty,
            },
            &pool,
        )
        .await
        .unwrap();
        let got: Vec<&str> = all
            .tag_definitions
            .iter()
            .map(|d| d.name.as_str())
            .collect();
        assert_eq!(got, vec!["a.one", "b.two", "c.three"]);

        // Page size 2: first page + cursor, then the remaining one.
        let page1 = list_tag_definitions(
            &project_id,
            PaginationQuery {
                page_size: Some(2),
                page_token: PageToken::Empty,
            },
            &pool,
        )
        .await
        .unwrap();
        let p1: Vec<&str> = page1
            .tag_definitions
            .iter()
            .map(|d| d.name.as_str())
            .collect();
        assert_eq!(p1, vec!["a.one", "b.two"]);
        assert!(page1.next_page_token.is_some());

        let page2 = list_tag_definitions(
            &project_id,
            PaginationQuery {
                page_size: Some(2),
                page_token: page1.next_page_token.into(),
            },
            &pool,
        )
        .await
        .unwrap();
        let p2: Vec<&str> = page2
            .tag_definitions
            .iter()
            .map(|d| d.name.as_str())
            .collect();
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
        assert!(
            get_tag_allowed_values(marker_id, &pool)
                .await
                .unwrap()
                .is_empty()
        );
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
                .build(),
            &mut txn,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            UpdateTagDefinitionError::TagDefinitionIdNotFound(_)
        ));
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
                .build(),
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        let mut txn = pool.begin().await.unwrap();
        let (updated, updated_allowed_values) = update_tag_definition(
            &project_id,
            id,
            UpdateTagDefinitionRequest::builder()
                .name("data.sensitivity")
                .description(Some("Sensitivity level"))
                .scope(&[TagScope::Table, TagScope::Column])
                .add_allowed_values(&["restricted"])
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
        assert!(updated.updated_at.is_some());

        let expected_values = vec![
            "internal".to_string(),
            "public".to_string(),
            "restricted".to_string(),
        ];
        // The in-transaction read returns the merged values (no separate get needed)...
        assert_eq!(updated_allowed_values, expected_values);
        // ...and they match a subsequent independent read.
        let values = get_tag_allowed_values(id, &pool).await.unwrap();
        assert_eq!(values, expected_values);

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
                .build(),
            &mut txn,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            UpdateTagDefinitionError::TagNameAlreadyExists(_)
        ));
    }

    #[sqlx::test]
    async fn test_list_column_tags_for_tabular(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (project_id, warehouse_id) =
            initialize_warehouse(state.clone(), None, None, None, true).await;

        let def_id = TagDefinitionId::new_random();
        let mut txn = pool.begin().await.unwrap();
        create_tag_definition(
            &project_id,
            CatalogCreateTagDefinitionRequest::builder()
                .tag_definition_id(def_id)
                .name("pii")
                .scope(&[TagScope::Column])
                .value_spec(TagValueSpec::Marker)
                .build(),
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        let (table_id, _schema) =
            create_table_with_schema(state.clone(), warehouse_id, two_col_schema()).await;
        let tabular_id = TabularId::Table(table_id);

        // Tag both columns (field-ids 1 and 2 in `two_col_schema`).
        for field_id in [1, 2] {
            let mut txn = pool.begin().await.unwrap();
            apply_tag(
                TagId::new_random(),
                def_id,
                TagTarget::Column {
                    warehouse_id,
                    tabular_id,
                    field_id,
                },
                None,
                TagSource::Manual,
                &mut txn,
            )
            .await
            .unwrap();
            txn.commit().await.unwrap();
        }

        // A table-level tag on the same table must NOT show up (columns only).
        let table_def = TagDefinitionId::new_random();
        let mut txn = pool.begin().await.unwrap();
        create_tag_definition(
            &project_id,
            CatalogCreateTagDefinitionRequest::builder()
                .tag_definition_id(table_def)
                .name("owner")
                .scope(&[TagScope::Table])
                .value_spec(TagValueSpec::Marker)
                .build(),
            &mut txn,
        )
        .await
        .unwrap();
        apply_tag(
            TagId::new_random(),
            table_def,
            TagTarget::Tabular {
                warehouse_id,
                tabular_id,
            },
            None,
            TagSource::Manual,
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        let tags = list_column_tags_for_tabular(warehouse_id, tabular_id, &pool)
            .await
            .unwrap();

        // Exactly the two column tags, ordered by field-id; the table tag is excluded.
        assert_eq!(
            tags.iter().map(|t| t.tag.target).collect::<Vec<_>>(),
            vec![
                TagTarget::Column {
                    warehouse_id,
                    tabular_id,
                    field_id: 1,
                },
                TagTarget::Column {
                    warehouse_id,
                    tabular_id,
                    field_id: 2,
                },
            ]
        );
        assert_eq!(
            tags.iter()
                .map(|t| t.definition_name.as_str())
                .collect::<Vec<_>>(),
            vec!["pii", "pii"]
        );
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
                .build(),
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        // Apply to the warehouse.
        let mut txn = pool.begin().await.unwrap();
        let (tag, _) = apply_tag(
            TagId::new_random(),
            def_id,
            TagTarget::Warehouse(warehouse_id),
            None,
            TagSource::Manual,
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();
        assert_eq!(tag.tag_definition_id, def_id);
        assert_eq!(tag.target, TagTarget::Warehouse(warehouse_id));
        assert_eq!(tag.value, None);
        assert_eq!(tag.source, TagSource::Manual);

        // Apply to a column — exercises tag_field_fkey against the tabular_field spine.
        let (table_id, _schema) =
            create_table_with_schema(state.clone(), warehouse_id, two_col_schema()).await;
        let column = TagTarget::Column {
            warehouse_id,
            tabular_id: TabularId::Table(table_id),
            field_id: 1,
        };
        let mut txn = pool.begin().await.unwrap();
        let (ctag, _) = apply_tag(
            TagId::new_random(),
            def_id,
            column,
            None,
            TagSource::Manual,
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
        apply_tag(pii_tag_id, def_pii, wh, None, TagSource::Manual, &mut txn)
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
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        let listed = list_tags_for_target(wh, &pool).await.unwrap();
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].tag.tag_definition_id, def_pii);
        assert_eq!(listed[0].definition_name, "pii"); // joined, not looked up per-row
        assert_eq!(listed[0].tag.value, None);
        assert_eq!(listed[1].tag.tag_definition_id, def_tier);
        assert_eq!(listed[1].definition_name, "tier");
        assert_eq!(listed[1].tag.value, Some("gold".to_string()));

        // Re-applying pii with the SAME value is a no-op: the original row (tag_id) is
        // preserved, `changed` is false, and `updated_at` is not bumped.
        let before = list_tags_for_target(wh, &pool)
            .await
            .unwrap()
            .into_iter()
            .map(|t| t.tag)
            .find(|t| t.tag_id == pii_tag_id)
            .unwrap();
        let mut txn = pool.begin().await.unwrap();
        let (reapplied, changed) = apply_tag(
            TagId::new_random(),
            def_pii,
            wh,
            None,
            TagSource::Manual,
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();
        assert_eq!(reapplied.tag_id, pii_tag_id);
        assert!(!changed, "same-value re-apply must be a no-op");
        assert_eq!(reapplied.updated_at, before.updated_at);
        assert_eq!(list_tags_for_target(wh, &pool).await.unwrap().len(), 2);

        // Re-applying tier with a DIFFERENT value updates in place: the attachment
        // row is kept (same tag_id), `changed` is true, and the new value is both
        // returned and persisted.
        let tier_before = list_tags_for_target(wh, &pool)
            .await
            .unwrap()
            .into_iter()
            .map(|t| t.tag)
            .find(|t| t.tag_definition_id == def_tier)
            .unwrap();
        let mut txn = pool.begin().await.unwrap();
        let (retagged, changed) = apply_tag(
            TagId::new_random(),
            def_tier,
            wh,
            Some("silver"),
            TagSource::Manual,
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();
        assert!(changed, "changing the value must report changed");
        assert_eq!(
            retagged.tag_id, tier_before.tag_id,
            "a value change keeps the same attachment row"
        );
        assert_eq!(retagged.value, Some("silver".to_string()));
        let tier_after = list_tags_for_target(wh, &pool)
            .await
            .unwrap()
            .into_iter()
            .map(|t| t.tag)
            .find(|t| t.tag_definition_id == def_tier)
            .unwrap();
        assert_eq!(
            tier_after.value,
            Some("silver".to_string()),
            "the new value must be persisted, not just returned"
        );
        assert_eq!(tier_after.tag_id, tier_before.tag_id);

        // Remove pii; tier remains.
        let mut txn = pool.begin().await.unwrap();
        remove_tag(pii_tag_id, &mut txn).await.unwrap();
        txn.commit().await.unwrap();
        let remaining = list_tags_for_target(wh, &pool).await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].tag.tag_definition_id, def_tier);

        // Removing an unknown tag by id -> TagNotFound.
        let mut txn = pool.begin().await.unwrap();
        let err = remove_tag(TagId::new_random(), &mut txn).await.unwrap_err();
        assert!(matches!(err, RemoveTagError::TagNotFound(_)));

        // remove_tag_for_target atomically deletes and returns the attachment (tier is
        // still attached); a second call is an idempotent no-op returning `None`.
        let mut txn = pool.begin().await.unwrap();
        let removed = remove_tag_for_target(wh, def_tier, TagSource::Manual, &mut txn)
            .await
            .unwrap();
        txn.commit().await.unwrap();
        assert_eq!(removed.map(|t| t.tag_definition_id), Some(def_tier));

        let mut txn = pool.begin().await.unwrap();
        let removed_again = remove_tag_for_target(wh, def_tier, TagSource::Manual, &mut txn)
            .await
            .unwrap();
        txn.commit().await.unwrap();
        assert!(removed_again.is_none());
        assert!(list_tags_for_target(wh, &pool).await.unwrap().is_empty());
    }

    #[sqlx::test]
    async fn test_list_tag_attachments(pool: sqlx::PgPool) {
        use lakekeeper::api::iceberg::types::PageToken;

        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (project_id, warehouse_id) =
            initialize_warehouse(state.clone(), None, None, None, true).await;

        let def_id = TagDefinitionId::new_random();
        let mut txn = pool.begin().await.unwrap();
        create_tag_definition(
            &project_id,
            CatalogCreateTagDefinitionRequest::builder()
                .tag_definition_id(def_id)
                .name("location")
                .scope(&[TagScope::Warehouse, TagScope::Table, TagScope::Column])
                .value_spec(TagValueSpec::FreeText)
                .build(),
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        let (table_id, _schema) =
            create_table_with_schema(state.clone(), warehouse_id, two_col_schema()).await;
        let table_target = TagTarget::Tabular {
            warehouse_id,
            tabular_id: TabularId::Table(table_id),
        };
        let column_target = TagTarget::Column {
            warehouse_id,
            tabular_id: TabularId::Table(table_id),
            field_id: 1,
        };

        let no_filter = TagAttachmentFilter::builder().build();
        let eu_filter = TagAttachmentFilter::builder()
            .value(Some("eu".to_string()))
            .build();
        let apac_filter = TagAttachmentFilter::builder()
            .value(Some("apac".to_string()))
            .build();

        // Empty before anything is applied.
        let empty = list_tag_attachments(
            def_id,
            &no_filter,
            PaginationQuery {
                page_size: Some(10),
                page_token: PageToken::Empty,
            },
            &pool,
        )
        .await
        .unwrap();
        assert!(empty.tags.is_empty());
        assert_eq!(empty.next_page_token, None);

        // Apply to warehouse ("eu"), table ("eu"), column ("us") in order — separate
        // transactions give strictly increasing created_at, so listing order is fixed.
        for (target, value) in [
            (TagTarget::Warehouse(warehouse_id), "eu"),
            (table_target, "eu"),
            (column_target, "us"),
        ] {
            let mut txn = pool.begin().await.unwrap();
            apply_tag(
                TagId::new_random(),
                def_id,
                target,
                Some(value),
                TagSource::Manual,
                &mut txn,
            )
            .await
            .unwrap();
            txn.commit().await.unwrap();
        }

        // All three, with the target subtype reconstructed from the tabular join.
        let all = list_tag_attachments(
            def_id,
            &no_filter,
            PaginationQuery {
                page_size: Some(10),
                page_token: PageToken::Empty,
            },
            &pool,
        )
        .await
        .unwrap();
        let targets: Vec<TagTarget> = all.tags.iter().map(|t| t.target).collect();
        assert_eq!(
            targets,
            vec![
                TagTarget::Warehouse(warehouse_id),
                table_target,
                column_target
            ]
        );
        let values: Vec<Option<&str>> = all.tags.iter().map(|t| t.value.as_deref()).collect();
        assert_eq!(values, vec![Some("eu"), Some("eu"), Some("us")]);

        // Value filter: only the two "eu" attachments (warehouse + table).
        let eu = list_tag_attachments(
            def_id,
            &eu_filter,
            PaginationQuery {
                page_size: Some(10),
                page_token: PageToken::Empty,
            },
            &pool,
        )
        .await
        .unwrap();
        let eu_targets: Vec<TagTarget> = eu.tags.iter().map(|t| t.target).collect();
        assert_eq!(
            eu_targets,
            vec![TagTarget::Warehouse(warehouse_id), table_target]
        );

        // A value nothing carries -> empty.
        let none = list_tag_attachments(
            def_id,
            &apac_filter,
            PaginationQuery {
                page_size: Some(10),
                page_token: PageToken::Empty,
            },
            &pool,
        )
        .await
        .unwrap();
        assert!(none.tags.is_empty());

        // Keyset pagination: page of 2, then the remaining 1.
        let page1 = list_tag_attachments(
            def_id,
            &no_filter,
            PaginationQuery {
                page_size: Some(2),
                page_token: PageToken::Empty,
            },
            &pool,
        )
        .await
        .unwrap();
        assert_eq!(page1.tags.len(), 2);
        assert!(page1.next_page_token.is_some());
        let page2 = list_tag_attachments(
            def_id,
            &no_filter,
            PaginationQuery {
                page_size: Some(2),
                page_token: page1.next_page_token.into(),
            },
            &pool,
        )
        .await
        .unwrap();
        assert_eq!(page2.tags.len(), 1);
        assert_eq!(page2.tags[0].target, column_target);
        // Trailing token even on the non-full last page; following it terminates.
        let page3 = list_tag_attachments(
            def_id,
            &no_filter,
            PaginationQuery {
                page_size: Some(2),
                page_token: page2.next_page_token.into(),
            },
            &pool,
        )
        .await
        .unwrap();
        assert!(page3.tags.is_empty());
        assert_eq!(page3.next_page_token, None);

        // Value filter AND keyset pagination together: the two "eu" attachments
        // (warehouse, then table) paged one at a time.
        let eu_page1 = list_tag_attachments(
            def_id,
            &eu_filter,
            PaginationQuery {
                page_size: Some(1),
                page_token: PageToken::Empty,
            },
            &pool,
        )
        .await
        .unwrap();
        assert_eq!(eu_page1.tags.len(), 1);
        assert_eq!(eu_page1.tags[0].target, TagTarget::Warehouse(warehouse_id));
        let eu_page2 = list_tag_attachments(
            def_id,
            &eu_filter,
            PaginationQuery {
                page_size: Some(1),
                page_token: eu_page1.next_page_token.into(),
            },
            &pool,
        )
        .await
        .unwrap();
        assert_eq!(eu_page2.tags.len(), 1);
        assert_eq!(eu_page2.tags[0].target, table_target);
        // Following the token past the last "eu" row yields an empty page (the "us"
        // attachment is correctly excluded by the value filter, not just paged past).
        let eu_page3 = list_tag_attachments(
            def_id,
            &eu_filter,
            PaginationQuery {
                page_size: Some(1),
                page_token: eu_page2.next_page_token.into(),
            },
            &pool,
        )
        .await
        .unwrap();
        assert!(eu_page3.tags.is_empty());
        assert_eq!(eu_page3.next_page_token, None);

        // Target-type filter: each object type in isolation.
        let by_type = |ty: TagScope| TagAttachmentFilter::builder().target_type(Some(ty)).build();
        let full = || PaginationQuery {
            page_size: Some(10),
            page_token: PageToken::Empty,
        };
        let wh = list_tag_attachments(def_id, &by_type(TagScope::Warehouse), full(), &pool)
            .await
            .unwrap();
        assert_eq!(
            wh.tags.iter().map(|t| t.target).collect::<Vec<_>>(),
            vec![TagTarget::Warehouse(warehouse_id)]
        );
        let tbl = list_tag_attachments(def_id, &by_type(TagScope::Table), full(), &pool)
            .await
            .unwrap();
        assert_eq!(
            tbl.tags.iter().map(|t| t.target).collect::<Vec<_>>(),
            vec![table_target]
        );
        let col = list_tag_attachments(def_id, &by_type(TagScope::Column), full(), &pool)
            .await
            .unwrap();
        assert_eq!(
            col.tags.iter().map(|t| t.target).collect::<Vec<_>>(),
            vec![column_target]
        );
        // A type nothing is attached under -> empty.
        let ns = list_tag_attachments(def_id, &by_type(TagScope::Namespace), full(), &pool)
            .await
            .unwrap();
        assert!(ns.tags.is_empty());

        // Warehouse filter: all three live in this warehouse; a different one -> empty.
        let in_wh = list_tag_attachments(
            def_id,
            &TagAttachmentFilter::builder()
                .warehouse_id(Some(warehouse_id))
                .build(),
            full(),
            &pool,
        )
        .await
        .unwrap();
        assert_eq!(in_wh.tags.len(), 3);
        let other_wh = list_tag_attachments(
            def_id,
            &TagAttachmentFilter::builder()
                .warehouse_id(Some(WarehouseId::new(Uuid::nil())))
                .build(),
            full(),
            &pool,
        )
        .await
        .unwrap();
        assert!(other_wh.tags.is_empty());

        // created_after / created_before: unbounded window returns all; impossible ones none.
        let epoch = chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0).unwrap();
        let year_2100 = chrono::DateTime::<chrono::Utc>::from_timestamp(4_102_444_800, 0).unwrap();
        let since_epoch = list_tag_attachments(
            def_id,
            &TagAttachmentFilter::builder()
                .created_after(Some(epoch))
                .build(),
            full(),
            &pool,
        )
        .await
        .unwrap();
        assert_eq!(since_epoch.tags.len(), 3);
        let after_2100 = list_tag_attachments(
            def_id,
            &TagAttachmentFilter::builder()
                .created_after(Some(year_2100))
                .build(),
            full(),
            &pool,
        )
        .await
        .unwrap();
        assert!(after_2100.tags.is_empty());
        let before_epoch = list_tag_attachments(
            def_id,
            &TagAttachmentFilter::builder()
                .created_before(Some(epoch))
                .build(),
            full(),
            &pool,
        )
        .await
        .unwrap();
        assert!(before_epoch.tags.is_empty());

        // Unknown definition -> empty, no token.
        let unknown = list_tag_attachments(
            TagDefinitionId::new_random(),
            &no_filter,
            PaginationQuery {
                page_size: Some(10),
                page_token: PageToken::Empty,
            },
            &pool,
        )
        .await
        .unwrap();
        assert!(unknown.tags.is_empty());
        assert_eq!(unknown.next_page_token, None);
    }

    /// Each row lands on the target it belongs to, with the definition name joined, and a
    /// tabular's column tags stay out of the tabular's own set.
    #[sqlx::test]
    async fn test_list_tags_on_targets(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (project_id, warehouse_id) =
            initialize_warehouse(state.clone(), None, None, None, true).await;
        let (table_id, _schema) =
            create_table_with_schema(state.clone(), warehouse_id, two_col_schema()).await;

        let namespace_id = sqlx::query_scalar!(
            "SELECT namespace_id FROM tabular WHERE warehouse_id = $1 AND tabular_id = $2",
            *warehouse_id,
            *table_id,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let namespace_id = NamespaceId::new(namespace_id);

        let def_pii = TagDefinitionId::new_random();
        let def_tier = TagDefinitionId::new_random();
        let mut txn = pool.begin().await.unwrap();
        for (id, name, spec) in [
            (def_pii, "pii", TagValueSpec::Marker),
            (def_tier, "tier", TagValueSpec::FreeText),
        ] {
            create_tag_definition(
                &project_id,
                CatalogCreateTagDefinitionRequest::builder()
                    .tag_definition_id(id)
                    .name(name)
                    .scope(&[
                        TagScope::Warehouse,
                        TagScope::Namespace,
                        TagScope::Table,
                        TagScope::Column,
                    ])
                    .value_spec(spec)
                    .build(),
                &mut txn,
            )
            .await
            .unwrap();
        }
        txn.commit().await.unwrap();

        let wh_target = TagTarget::Warehouse(warehouse_id);
        let ns_target = TagTarget::Namespace {
            warehouse_id,
            namespace_id,
        };
        let table_target = TagTarget::Tabular {
            warehouse_id,
            tabular_id: TabularId::Table(table_id),
        };
        let column_target = TagTarget::Column {
            warehouse_id,
            tabular_id: TabularId::Table(table_id),
            field_id: 1,
        };

        // `pii` at every level, plus a valued `tier` on the table only.
        let mut txn = pool.begin().await.unwrap();
        for target in [wh_target, ns_target, table_target, column_target] {
            apply_tag(
                TagId::new_random(),
                def_pii,
                target,
                None,
                TagSource::Manual,
                &mut txn,
            )
            .await
            .unwrap();
        }
        apply_tag(
            TagId::new_random(),
            def_tier,
            table_target,
            Some("gold"),
            TagSource::Manual,
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        // No targets: answered without a query.
        assert!(list_tags_on_targets(&[], &pool).await.unwrap().is_empty());

        // The chain an authorizer would ask for. The column tag is not in it: nobody
        // named that target.
        let rows = list_tags_on_targets(&[wh_target, ns_target, table_target], &pool)
            .await
            .unwrap();
        let mut got: Vec<(TagTarget, &str, Option<&str>)> = rows
            .iter()
            .map(|r| {
                (
                    r.tag.target,
                    r.definition_name.as_str(),
                    r.tag.value.as_deref(),
                )
            })
            .collect();
        got.sort_by_key(|(target, name, _)| {
            let level = match target {
                TagTarget::Warehouse(_) => 0,
                TagTarget::Namespace { .. } => 1,
                TagTarget::Tabular { .. } => 2,
                TagTarget::Column { .. } => 3,
            };
            (level, *name)
        });
        assert_eq!(
            got,
            vec![
                (wh_target, "pii", None),
                (ns_target, "pii", None),
                (table_target, "pii", None),
                (table_target, "tier", Some("gold")),
            ]
        );

        // Named explicitly, the column returns its own tag and nothing else.
        let rows = list_tags_on_targets(&[column_target], &pool).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].tag.target, column_target);
        assert_eq!(rows[0].definition_name, "pii");

        // A target that does not exist gives no rows and no error, like the per-target read.
        let untagged = TagTarget::Warehouse(WarehouseId::from(Uuid::now_v7()));
        assert!(
            list_tags_on_targets(&[untagged], &pool)
                .await
                .unwrap()
                .is_empty()
        );

        // The kind is echoed from the caller's entry, never re-read: the row holds a bare
        // `tabular_id`. This is what lets the statement skip the join to `tabular`.
        let as_view = TagTarget::Tabular {
            warehouse_id,
            tabular_id: TabularId::View(ViewId::from(*table_id)),
        };
        let rows = list_tags_on_targets(&[as_view], &pool).await.unwrap();
        let mut names: Vec<&str> = rows.iter().map(|r| r.definition_name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(names, vec!["pii", "tier"]);
        assert!(rows.iter().all(|r| r.tag.target == as_view));
    }

    /// Column targets match as a cross product of three arrays, so a tag on a pair nobody
    /// asked for can satisfy the SQL. The per-tuple tightening drops it.
    #[sqlx::test]
    async fn test_list_tags_on_targets_drops_unrequested_column_pairs(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (project_id, warehouse_id) =
            initialize_warehouse(state.clone(), None, None, None, true).await;
        let (table_one, _) =
            create_table_with_schema(state.clone(), warehouse_id, two_col_schema()).await;
        let (table_two, _) =
            create_table_with_schema(state.clone(), warehouse_id, two_col_schema()).await;

        let def = TagDefinitionId::new_random();
        let mut txn = pool.begin().await.unwrap();
        create_tag_definition(
            &project_id,
            CatalogCreateTagDefinitionRequest::builder()
                .tag_definition_id(def)
                .name("pii")
                .scope(&[TagScope::Column])
                .value_spec(TagValueSpec::Marker)
                .build(),
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        let column = |table: TableId, field_id: i32| TagTarget::Column {
            warehouse_id,
            tabular_id: TabularId::Table(table),
            field_id,
        };

        // Tagged (table_one, field 2). Requested (table_one, field 1) and (table_two,
        // field 2), so the tagged pair is in both arrays but is not a requested target.
        let mut txn = pool.begin().await.unwrap();
        apply_tag(
            TagId::new_random(),
            def,
            column(table_one, 2),
            None,
            TagSource::Manual,
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        let rows = list_tags_on_targets(&[column(table_one, 1), column(table_two, 2)], &pool)
            .await
            .unwrap();
        assert!(rows.is_empty(), "unrequested (tabular, field) pair leaked");

        // Asking for the pair that is actually tagged returns it.
        let rows = list_tags_on_targets(&[column(table_one, 2)], &pool)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].tag.target, column(table_one, 2));
    }

    /// One marker definition, scoped to warehouse, namespace, table and column.
    async fn marker_definition(
        pool: &sqlx::PgPool,
        project_id: &ProjectId,
        name: &str,
    ) -> TagDefinitionId {
        let id = TagDefinitionId::new_random();
        let mut txn = pool.begin().await.unwrap();
        create_tag_definition(
            project_id,
            CatalogCreateTagDefinitionRequest::builder()
                .tag_definition_id(id)
                .name(name)
                .scope(&[
                    TagScope::Warehouse,
                    TagScope::Namespace,
                    TagScope::Table,
                    TagScope::Column,
                ])
                .value_spec(TagValueSpec::Marker)
                .build(),
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();
        id
    }

    /// Two requested targets can together name a pair neither of them is. Only the
    /// tuple-keyed map stops it being reported, and a tabular id can be reused across
    /// warehouses, so that pair can really exist.
    #[sqlx::test]
    async fn test_list_tags_on_targets_drops_cross_warehouse_tabular_pairs(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_a) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let other_project = ProjectId::new_random();
        let (project_b, warehouse_b) =
            initialize_warehouse(state.clone(), None, Some(&other_project), None, true).await;
        let (table_x, _) =
            create_table_with_schema(state.clone(), warehouse_b, two_col_schema()).await;
        let def = marker_definition(&pool, &project_b, "pii").await;

        let tagged = TagTarget::Tabular {
            warehouse_id: warehouse_b,
            tabular_id: TabularId::Table(table_x),
        };
        let mut txn = pool.begin().await.unwrap();
        apply_tag(
            TagId::new_random(),
            def,
            tagged,
            None,
            TagSource::Manual,
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        // `(A, X)` shares only the id, `(B, Y)` only the warehouse. Together they
        // cross-match the tagged `(B, X)`, which neither of them is.
        let requested = [
            TagTarget::Tabular {
                warehouse_id: warehouse_a,
                tabular_id: TabularId::Table(table_x),
            },
            TagTarget::Tabular {
                warehouse_id: warehouse_b,
                tabular_id: TabularId::Table(TableId::from(Uuid::now_v7())),
            },
        ];
        assert_eq!(
            list_tags_on_targets(&requested, &pool).await.unwrap(),
            Vec::new()
        );

        // The target that really carries it still gets it.
        let rows = list_tags_on_targets(&[tagged], &pool).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].tag.target, tagged);
    }

    /// The namespace arm cross-matches the same way, through separate code. A namespace id
    /// is unique today, so the crossed pair cannot exist — but the map keying is still the
    /// only thing enforcing it.
    #[sqlx::test]
    async fn test_list_tags_on_targets_drops_cross_warehouse_namespace_pairs(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_a) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let other_project = ProjectId::new_random();
        let (project_b, warehouse_b) =
            initialize_warehouse(state.clone(), None, Some(&other_project), None, true).await;
        let (table_x, _) =
            create_table_with_schema(state.clone(), warehouse_b, two_col_schema()).await;
        let namespace_b = NamespaceId::new(
            sqlx::query_scalar!(
                "SELECT namespace_id FROM tabular WHERE warehouse_id = $1 AND tabular_id = $2",
                *warehouse_b,
                *table_x,
            )
            .fetch_one(&pool)
            .await
            .unwrap(),
        );
        let def = marker_definition(&pool, &project_b, "pii").await;

        let tagged = TagTarget::Namespace {
            warehouse_id: warehouse_b,
            namespace_id: namespace_b,
        };
        let mut txn = pool.begin().await.unwrap();
        apply_tag(
            TagId::new_random(),
            def,
            tagged,
            None,
            TagSource::Manual,
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        let requested = [
            TagTarget::Namespace {
                warehouse_id: warehouse_a,
                namespace_id: namespace_b,
            },
            TagTarget::Namespace {
                warehouse_id: warehouse_b,
                namespace_id: NamespaceId::new(Uuid::now_v7()),
            },
        ];
        assert_eq!(
            list_tags_on_targets(&requested, &pool).await.unwrap(),
            Vec::new()
        );
    }

    /// These four WHERE arms duplicate four in `list_tags_for_target` and will drift. For a
    /// single target both must answer the same, as a set: only the per-target read orders.
    #[sqlx::test]
    async fn test_list_tags_on_targets_agrees_with_the_per_target_read(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (project_id, warehouse_id) =
            initialize_warehouse(state.clone(), None, None, None, true).await;
        let (table_id, _) =
            create_table_with_schema(state.clone(), warehouse_id, two_col_schema()).await;
        let namespace_id = NamespaceId::new(
            sqlx::query_scalar!(
                "SELECT namespace_id FROM tabular WHERE warehouse_id = $1 AND tabular_id = $2",
                *warehouse_id,
                *table_id,
            )
            .fetch_one(&pool)
            .await
            .unwrap(),
        );
        let def = marker_definition(&pool, &project_id, "pii").await;

        let targets = [
            TagTarget::Warehouse(warehouse_id),
            TagTarget::Namespace {
                warehouse_id,
                namespace_id,
            },
            TagTarget::Tabular {
                warehouse_id,
                tabular_id: TabularId::Table(table_id),
            },
            TagTarget::Column {
                warehouse_id,
                tabular_id: TabularId::Table(table_id),
                field_id: 1,
            },
        ];
        let mut txn = pool.begin().await.unwrap();
        for target in targets {
            apply_tag(
                TagId::new_random(),
                def,
                target,
                None,
                TagSource::Manual,
                &mut txn,
            )
            .await
            .unwrap();
        }
        txn.commit().await.unwrap();

        for target in targets {
            let mut batched = list_tags_on_targets(&[target], &pool).await.unwrap();
            let mut single = list_tags_for_target(target, &pool).await.unwrap();
            batched.sort_by_key(|r| *r.tag.tag_id);
            single.sort_by_key(|r| *r.tag.tag_id);
            assert_eq!(batched, single, "divergence on {target:?}");
            assert_eq!(
                batched.len(),
                1,
                "fixture should tag {target:?} exactly once"
            );
        }
    }

    /// Nothing joins back to `tabular`, so a soft-deleted table keeps its tags — like the
    /// per-target read and the grant fetch, which keep a dropped table's state for undrop.
    /// A `deleted_at` filter added here would break it silently.
    #[sqlx::test]
    async fn test_list_tags_on_targets_includes_soft_deleted_tabulars(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (project_id, warehouse_id) =
            initialize_warehouse(state.clone(), None, None, None, true).await;
        let (table_id, _) =
            create_table_with_schema(state.clone(), warehouse_id, two_col_schema()).await;
        let def = marker_definition(&pool, &project_id, "pii").await;

        let target = TagTarget::Tabular {
            warehouse_id,
            tabular_id: TabularId::Table(table_id),
        };
        let mut txn = pool.begin().await.unwrap();
        apply_tag(
            TagId::new_random(),
            def,
            target,
            None,
            TagSource::Manual,
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        sqlx::query!(
            "UPDATE tabular SET deleted_at = now() WHERE warehouse_id = $1 AND tabular_id = $2",
            *warehouse_id,
            *table_id,
        )
        .execute(&pool)
        .await
        .unwrap();

        let rows = list_tags_on_targets(&[target], &pool).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].tag.target, target);
        assert_eq!(rows[0].definition_name, "pii");
    }

    /// The batched read plus [`resolve_effective_tags_from_chain`] must answer what the API
    /// answers for the same object, or what a user is shown and what a policy enforces
    /// drift apart.
    ///
    /// The fixture: a table under `x.y`, a sibling branch `x.z` in the same batch, one
    /// definition at every level, one on the two `x` namespaces only, one on the warehouse
    /// only, and a column tag.
    #[sqlx::test]
    async fn test_folded_chain_matches_the_effective_tags_api(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (project_id, warehouse_id) =
            initialize_warehouse(state.clone(), None, None, None, true).await;

        let ident = |parts: &[&str]| {
            NamespaceIdent::from_vec(parts.iter().map(ToString::to_string).collect()).unwrap()
        };
        let (outer, inner, sibling) = (ident(&["x"]), ident(&["x", "y"]), ident(&["x", "z"]));
        let mut namespace_ids = Vec::new();
        for namespace in [&outer, &inner, &sibling] {
            namespace_ids.push(
                initialize_namespace(state.clone(), warehouse_id, namespace, None)
                    .await
                    .namespace_id(),
            );
        }
        let [outer_id, inner_id, sibling_id]: [NamespaceId; 3] = namespace_ids.try_into().unwrap();
        let table = initialize_table(
            warehouse_id,
            state.clone(),
            false,
            Some(inner.clone()),
            None,
            None,
        )
        .await;
        let sibling_table = initialize_table(
            warehouse_id,
            state.clone(),
            false,
            Some(sibling.clone()),
            None,
            None,
        )
        .await;

        let namespace_target = |namespace_id| TagTarget::Namespace {
            warehouse_id,
            namespace_id,
        };
        let tabular_target = |table_id| TagTarget::Tabular {
            warehouse_id,
            tabular_id: TabularId::Table(table_id),
        };
        let wh_target = TagTarget::Warehouse(warehouse_id);
        let outer_target = namespace_target(outer_id);
        let inner_target = namespace_target(inner_id);
        let sibling_target = namespace_target(sibling_id);
        let table_target = tabular_target(table.table_id);
        let sibling_table_target = tabular_target(sibling_table.table_id);
        let column_target = TagTarget::Column {
            warehouse_id,
            tabular_id: TabularId::Table(table.table_id),
            field_id: 1,
        };

        let sensitivity = TagDefinitionId::new_random();
        let owner = TagDefinitionId::new_random();
        let wh_only = marker_definition(&pool, &project_id, "wh_only").await;
        let pii = marker_definition(&pool, &project_id, "pii").await;
        let mut txn = pool.begin().await.unwrap();
        for (id, name) in [(sensitivity, "sensitivity"), (owner, "owner")] {
            create_tag_definition(
                &project_id,
                CatalogCreateTagDefinitionRequest::builder()
                    .tag_definition_id(id)
                    .name(name)
                    .scope(&[TagScope::Warehouse, TagScope::Namespace, TagScope::Table])
                    .value_spec(TagValueSpec::FreeText)
                    .build(),
                &mut txn,
            )
            .await
            .unwrap();
        }
        txn.commit().await.unwrap();

        // At every level, so only the nearest may win. The sibling's copy must never leak
        // into the `x.y` branch.
        let mut applied = vec![
            (sensitivity, wh_target, Some("from-warehouse")),
            (sensitivity, outer_target, Some("from-outer")),
            (sensitivity, inner_target, Some("from-inner")),
            (sensitivity, sibling_target, Some("from-sibling")),
            (sensitivity, table_target, Some("from-table")),
        ];
        // On the two `x` namespaces only: for the table nothing sits at distance 0, so the
        // winner turns purely on which ancestor is nearer.
        applied.push((owner, outer_target, Some("outer-owner")));
        applied.push((owner, inner_target, Some("inner-owner")));
        applied.push((wh_only, wh_target, None));
        applied.push((pii, column_target, None));
        let mut txn = pool.begin().await.unwrap();
        for (definition, target, value) in applied {
            apply_tag(
                TagId::new_random(),
                definition,
                target,
                value,
                TagSource::Manual,
                &mut txn,
            )
            .await
            .unwrap();
        }
        txn.commit().await.unwrap();

        // One read serves every object below, as an authorizer would use it.
        let rows = list_tags_on_targets(
            &[
                table_target,
                inner_target,
                outer_target,
                wh_target,
                sibling_target,
                sibling_table_target,
                column_target,
            ],
            &pool,
        )
        .await
        .unwrap();

        // Each object's own line of descent, nearest first.
        let table_ancestors = vec![inner_target, outer_target, wh_target];
        for (target, ancestors) in [
            (table_target, table_ancestors.clone()),
            (inner_target, vec![outer_target, wh_target]),
            (outer_target, vec![wh_target]),
            (sibling_target, vec![outer_target, wh_target]),
            (
                sibling_table_target,
                vec![sibling_target, outer_target, wh_target],
            ),
            (wh_target, Vec::new()),
            // Given ancestors on purpose: a column must ignore them.
            (
                column_target,
                vec![table_target, inner_target, outer_target, wh_target],
            ),
        ] {
            let folded = resolve_effective_tags_from_chain(target, &ancestors, &rows);
            let from_api =
                resolve_effective_tags(list_effective_tag_candidates(target, &pool).await.unwrap());
            assert_eq!(
                folded, from_api,
                "folded chain diverges from the API for {target:?}"
            );
        }

        // And the answer is the right one, not just the same on both paths.
        let shape = |target, ancestors: &[TagTarget]| {
            let mut shape: Vec<(String, Option<String>, i32, EffectiveTagSource)> =
                resolve_effective_tags_from_chain(target, ancestors, &rows)
                    .into_iter()
                    .map(|c| (c.name, c.value, c.distance, c.origin))
                    .collect();
            shape.sort_unstable_by(|a, b| a.0.cmp(&b.0));
            shape
        };
        let text = |s: &str| Some(s.to_string());
        assert_eq!(
            shape(table_target, &table_ancestors),
            vec![
                // The nearer of two namespaces wins.
                (
                    "owner".to_string(),
                    text("inner-owner"),
                    1,
                    EffectiveTagSource::Namespace {
                        warehouse_id,
                        namespace_id: inner_id,
                    }
                ),
                // The table's own tag beats its ancestors; the sibling's never appears.
                (
                    "sensitivity".to_string(),
                    text("from-table"),
                    0,
                    EffectiveTagSource::Direct
                ),
                // Survives from the far end, at the full chain length.
                (
                    "wh_only".to_string(),
                    None,
                    3,
                    EffectiveTagSource::Warehouse { warehouse_id }
                ),
            ]
        );
        // A column carries only its own tag, never the table's.
        assert_eq!(
            shape(column_target, &[table_target, inner_target]),
            vec![("pii".to_string(), None, 0, EffectiveTagSource::Direct)]
        );
    }

    #[sqlx::test]
    async fn test_list_effective_tag_candidates(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (project_id, warehouse_id) =
            initialize_warehouse(state.clone(), None, None, None, true).await;
        let (table_id, _schema) =
            create_table_with_schema(state.clone(), warehouse_id, two_col_schema()).await;

        // The table's containing namespace.
        let namespace_id = sqlx::query_scalar!(
            "SELECT namespace_id FROM tabular WHERE warehouse_id = $1 AND tabular_id = $2",
            *warehouse_id,
            *table_id,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let namespace_id = NamespaceId::new(namespace_id);

        let def = TagDefinitionId::new_random();
        let mut txn = pool.begin().await.unwrap();
        create_tag_definition(
            &project_id,
            CatalogCreateTagDefinitionRequest::builder()
                .tag_definition_id(def)
                .name("pii")
                .scope(&[
                    TagScope::Warehouse,
                    TagScope::Namespace,
                    TagScope::Table,
                    TagScope::Column,
                ])
                .value_spec(TagValueSpec::Marker)
                .build(),
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        let table_target = TagTarget::Tabular {
            warehouse_id,
            tabular_id: TabularId::Table(table_id),
        };
        let column_target = TagTarget::Column {
            warehouse_id,
            tabular_id: TabularId::Table(table_id),
            field_id: 1,
        };
        // Attach at warehouse, the table's namespace, the table, and a column.
        for target in [
            TagTarget::Warehouse(warehouse_id),
            TagTarget::Namespace {
                warehouse_id,
                namespace_id,
            },
            table_target,
            column_target,
        ] {
            let mut txn = pool.begin().await.unwrap();
            apply_tag(
                TagId::new_random(),
                def,
                target,
                None,
                TagSource::Manual,
                &mut txn,
            )
            .await
            .unwrap();
            txn.commit().await.unwrap();
        }

        // TABLE effective candidates: direct table (0), its namespace (1), warehouse
        // (2). The column tag must NOT appear (field_id exclusion).
        let mut cands = list_effective_tag_candidates(table_target, &pool)
            .await
            .unwrap();
        cands.sort_by_key(|c| c.distance);
        let shape: Vec<(i32, EffectiveTagSource)> =
            cands.iter().map(|c| (c.distance, c.origin)).collect();
        assert_eq!(
            shape,
            vec![
                (0, EffectiveTagSource::Direct),
                (
                    1,
                    EffectiveTagSource::Namespace {
                        warehouse_id,
                        namespace_id
                    }
                ),
                (2, EffectiveTagSource::Warehouse { warehouse_id }),
            ]
        );
        assert!(cands.iter().all(|c| c.name == "pii"));

        // COLUMN effective candidates: direct-only (its own tag), distance 0.
        let col = list_effective_tag_candidates(column_target, &pool)
            .await
            .unwrap();
        assert_eq!(col.len(), 1);
        assert_eq!(col[0].distance, 0);
        assert_eq!(col[0].origin, EffectiveTagSource::Direct);

        // WAREHOUSE effective candidates: direct-only, distance 0.
        let wh = list_effective_tag_candidates(TagTarget::Warehouse(warehouse_id), &pool)
            .await
            .unwrap();
        assert_eq!(wh.len(), 1);
        assert_eq!(wh[0].origin, EffectiveTagSource::Direct);

        // NAMESPACE effective candidates: direct (0) + warehouse (1).
        let mut ns = list_effective_tag_candidates(
            TagTarget::Namespace {
                warehouse_id,
                namespace_id,
            },
            &pool,
        )
        .await
        .unwrap();
        ns.sort_by_key(|c| c.distance);
        let ns_shape: Vec<(i32, EffectiveTagSource)> =
            ns.iter().map(|c| (c.distance, c.origin)).collect();
        assert_eq!(
            ns_shape,
            vec![
                (0, EffectiveTagSource::Direct),
                (1, EffectiveTagSource::Warehouse { warehouse_id }),
            ]
        );
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
        assert!(matches!(
            err,
            DeleteTagDefinitionError::TagDefinitionIdNotFound(_)
        ));
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
                .build(),
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        let mut txn = pool.begin().await.unwrap();
        delete_tag_definition(&project_id, enum_def, &mut txn)
            .await
            .unwrap();
        txn.commit().await.unwrap();
        assert_eq!(
            get_tag_definition(&project_id, enum_def, &pool)
                .await
                .unwrap(),
            None
        );
        assert!(
            get_tag_allowed_values(enum_def, &pool)
                .await
                .unwrap()
                .is_empty()
        );

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
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        let mut txn = pool.begin().await.unwrap();
        let err = delete_tag_definition(&project_id, used_def, &mut txn)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            DeleteTagDefinitionError::TagDefinitionInUse(_)
        ));
        drop(txn);

        // Remove the attachment, then the definition deletes (same transaction).
        let mut txn = pool.begin().await.unwrap();
        remove_tag(tag_id, &mut txn).await.unwrap();
        delete_tag_definition(&project_id, used_def, &mut txn)
            .await
            .unwrap();
        txn.commit().await.unwrap();
        assert_eq!(
            get_tag_definition(&project_id, used_def, &pool)
                .await
                .unwrap(),
            None
        );
    }
}
