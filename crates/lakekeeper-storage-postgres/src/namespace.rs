use std::{collections::HashMap, sync::Arc};

use iceberg::TableIdent;
use itertools::izip;
use lakekeeper::{
    CONFIG, WarehouseId,
    api::iceberg::v1::{PaginatedMapping, namespace::NamespaceDropFlags},
    server::namespace::MAX_NAMESPACE_DEPTH,
    service::{
        CatalogBackendError, CatalogCreateNamespaceError, CatalogGetNamespaceError,
        CatalogListNamespaceError, CatalogListNamespacesResponse, CatalogMoveNamespaceError,
        CatalogNamespaceDropError, CatalogSetNamespaceProtectedError,
        CatalogUpdateNamespacePropertiesError, ChildNamespaceProtected, ChildTabularProtected,
        CreateNamespaceRequest, InternalParseLocationError, InvalidNamespaceIdentifier,
        ListNamespacesQuery, MovedNamespace, Namespace, NamespaceAlreadyExists,
        NamespaceCannotMoveIntoSelf, NamespaceDropInfo, NamespaceHasChildren,
        NamespaceHasRunningTabularExpirations, NamespaceId, NamespaceIdent, NamespaceNotEmpty,
        NamespaceNotFound, NamespacePropertiesSerializationError, NamespaceProtected,
        NamespaceWithParent, Result, SerializationError, TabularId, WarehouseIdNotFound,
        storage::join_location, tasks::TaskId,
    },
};
use sqlx::types::Json;
use uuid::Uuid;

use super::dbutils::DBErrorHandler;
use crate::{
    pagination::{PaginateToken, V1PaginateToken},
    tabular::TabularType,
};

#[derive(Debug)]
struct NamespaceRow {
    namespace_id: NamespaceId,
    namespace_name: Vec<String>,
    warehouse_id: WarehouseId,
    protected: bool,
    properties: Json<Option<HashMap<String, String>>>,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: Option<chrono::DateTime<chrono::Utc>>,
    version: i64,
}

impl NamespaceRow {
    fn into_namespace(
        self,
        warehouse_id: WarehouseId,
    ) -> std::result::Result<Namespace, InvalidNamespaceIdentifier> {
        Ok(Namespace {
            namespace_ident: parse_namespace_identifier_from_vec(
                &self.namespace_name,
                warehouse_id,
                Some(self.namespace_id),
            )?,
            protected: self.protected,
            properties: self.properties.0.filter(|p| !p.is_empty()),
            namespace_id: self.namespace_id,
            warehouse_id: self.warehouse_id,
            created_at: self.created_at,
            updated_at: self.updated_at,
            version: self.version.into(),
        })
    }
}

#[derive(Debug)]
struct NamespaceWithParentVersionRow {
    namespace_id: NamespaceId,
    /// Canonical (stored) namespace name.
    namespace_name: Vec<String>,
    /// User-requested namespace name. Equals `namespace_name` when the row was not
    /// matched to a user input (e.g. for id-based lookups, or internal parents that
    /// the user did not explicitly reference).
    requested_name: Vec<String>,
    warehouse_id: WarehouseId,
    protected: bool,
    properties: Json<Option<HashMap<String, String>>>,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: Option<chrono::DateTime<chrono::Utc>>,
    version: i64,
    parent_namespace_id: Option<Uuid>,
    parent_version: Option<i64>,
}

impl NamespaceWithParentVersionRow {
    fn into_namespace_with_parent_version(
        self,
        warehouse_id: WarehouseId,
    ) -> std::result::Result<NamespaceWithParent, InvalidNamespaceIdentifier> {
        let parent = if let (Some(parent_id), Some(parent_version)) =
            (self.parent_namespace_id, self.parent_version)
        {
            Some((parent_id.into(), parent_version.into()))
        } else {
            None
        };

        // Only set requested_ident if it actually differs from canonical.
        // This keeps the invariant "requested_ident = None ⇒ canonical case" clean.
        let requested_ident = if self.requested_name == self.namespace_name {
            None
        } else {
            Some(
                NamespaceIdent::from_vec(self.requested_name.clone()).map_err(|_| {
                    InvalidNamespaceIdentifier::new(
                        warehouse_id,
                        format!("{:?}", self.requested_name),
                    )
                })?,
            )
        };

        let namespace = NamespaceRow {
            namespace_id: self.namespace_id,
            namespace_name: self.namespace_name,
            warehouse_id: self.warehouse_id,
            protected: self.protected,
            properties: self.properties,
            created_at: self.created_at,
            updated_at: self.updated_at,
            version: self.version,
        }
        .into_namespace(warehouse_id)?;

        Ok(NamespaceWithParent {
            namespace: Arc::new(namespace),
            parent,
            requested_ident,
        })
    }
}

pub(crate) async fn get_namespaces_by_id<
    'c,
    'e: 'c,
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
>(
    warehouse_id: WarehouseId,
    namespace_ids: &[NamespaceId],
    connection: E,
) -> std::result::Result<Vec<NamespaceWithParent>, CatalogGetNamespaceError> {
    let rows = sqlx::query_as!(
        NamespaceWithParentVersionRow,
        r#"
        with selected_ns as (
            select namespace_name
            from namespace
            where warehouse_id = $1 AND namespace_id = ANY($2)
        ),
        parent_paths as (
            SELECT DISTINCT namespace_name[1:generate_series(1, array_length(namespace_name, 1))] as parent_name
            FROM selected_ns
        ),
        relevant_namespaces AS (
            SELECT
                n.namespace_id,
                n.namespace_name,
                n.warehouse_id,
                n.protected,
                n.namespace_properties,
                n.created_at,
                n.updated_at,
                n.version
            FROM namespace n
            INNER JOIN warehouse w ON w.warehouse_id = $1
            WHERE n.warehouse_id = $1
            AND w.status = 'active'
            AND n.namespace_name IN (SELECT parent_name FROM parent_paths)
        )
        SELECT
                n.namespace_id,
                n.namespace_name as "namespace_name: Vec<String>",
                -- Id-based lookup: no user-requested name, return canonical.
                n.namespace_name as "requested_name!: Vec<String>",
                n.warehouse_id,
                n.protected,
                n.namespace_properties as "properties: Json<Option<HashMap<String, String>>>",
                n.created_at,
                n.updated_at,
                n.version,
                p.namespace_id as "parent_namespace_id?",
                p.version as "parent_version?"
        FROM relevant_namespaces n
        LEFT JOIN relevant_namespaces p ON array_length(n.namespace_name, 1) = array_length(p.namespace_name, 1) + 1
            AND n.namespace_name[1:array_length(p.namespace_name, 1)] = p.namespace_name
        "#,
        *warehouse_id,
        &namespace_ids.iter().copied().map(Into::into).collect::<Vec<Uuid>>()
    )
    .fetch_all(connection)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;

    rows.into_iter()
        .map(|row| row.into_namespace_with_parent_version(warehouse_id))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

pub(crate) async fn get_namespaces_by_name<
    'c,
    'e: 'c,
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
>(
    warehouse_id: WarehouseId,
    namespace: &[&NamespaceIdent],
    connection: E,
) -> std::result::Result<Vec<NamespaceWithParent>, CatalogGetNamespaceError> {
    // Encoding `ns_names` as json is a workaround for `sqlx` not supporting `Vec<Vec<String>>`.

    let ns_names_json = namespace
        .iter()
        .map(|ns| serde_json::to_value(*ns).map_err(|e| SerializationError::new("namespace", e)))
        .collect::<Result<Vec<_>, _>>()?;

    let rows = sqlx::query_as!(
        NamespaceWithParentVersionRow,
        r#"
        with requested_namespaces as (
            -- Not collated to `case_insensitive` like the column: `SELECT DISTINCT`
            -- in `requested_parent_paths` then keeps `FOO` and `foo` apart, and the
            -- `LEFT JOIN` onto `rpp` below returns a row per requested spelling per
            -- level of depth. That is what callers need -- `requested_name` is how
            -- they map an ident they asked about back to an id, and collating here
            -- would leave one arbitrary spelling with an entry and the rest without.
            select array(select jsonb_array_elements_text(r))::text[] as namespace_name
            from unnest($2::jsonb[]) as r
        ),
        requested_parent_paths as (
            SELECT DISTINCT namespace_name[1:generate_series(1, array_length(namespace_name, 1))] as parent_name
            FROM requested_namespaces
        ),
        selected_ns as (
            select namespace_name
            from namespace
            where warehouse_id = $1 AND namespace_name = ANY(SELECT namespace_name FROM requested_namespaces)
        ),
        parent_paths as (
            SELECT DISTINCT namespace_name[1:generate_series(1, array_length(namespace_name, 1))] as parent_name
            FROM selected_ns
        ),
        relevant_namespaces AS (
            SELECT
                n.namespace_id,
                n.namespace_name,
                n.warehouse_id,
                n.protected,
                n.namespace_properties,
                n.created_at,
                n.updated_at,
                n.version
            FROM namespace n
            INNER JOIN warehouse w ON w.warehouse_id = $1
            WHERE n.warehouse_id = $1
            AND w.status = 'active'
            -- Every `parent_paths` entry is a prefix of a stored name that matched a
            -- requested one, so it is also a `requested_parent_paths` entry and this
            -- conjunct removes no rows. It is here because the planner can estimate
            -- it: `requested_parent_paths` comes from the parameter array, while
            -- `parent_paths` comes from a lookup in this same table, which the
            -- planner cannot see through -- it assumes far more rows than there are
            -- and resolves the join by scanning every namespace in the warehouse.
            AND n.namespace_name IN (SELECT parent_name FROM requested_parent_paths)
            AND n.namespace_name IN (SELECT parent_name FROM parent_paths)
        )
        SELECT
                n.namespace_id as "namespace_id!: uuid::Uuid",
                -- Canonical (stored) name: what's written to cache for case-deterministic id lookups.
                n.namespace_name as "namespace_name!: Vec<String>",
                -- User-requested name: the caller's case. Matches canonical when the row
                -- is an internal parent not referenced by any user input (COALESCE fallback).
                -- The `=` join uses the case-insensitive ICU collation on namespace_name,
                -- so `['foo']` from the user matches stored `['Foo']`.
                COALESCE(rpp.parent_name, n.namespace_name) as "requested_name!: Vec<String>",
                n.warehouse_id as "warehouse_id!: uuid::Uuid",
                n.protected as "protected!: bool",
                n.namespace_properties as "properties!: Json<Option<HashMap<String, String>>>",
                n.created_at as "created_at!: chrono::DateTime<chrono::Utc>",
                n.updated_at as "updated_at?: chrono::DateTime<chrono::Utc>",
                n.version as "version!: i64",
                p.namespace_id as "parent_namespace_id?",
                p.version as "parent_version?"
        FROM relevant_namespaces n
        LEFT JOIN requested_parent_paths rpp ON n.namespace_name = rpp.parent_name
        LEFT JOIN relevant_namespaces p ON array_length(n.namespace_name, 1) = array_length(p.namespace_name, 1) + 1
            AND n.namespace_name[1:array_length(p.namespace_name, 1)] = p.namespace_name
        "#,
        *warehouse_id,
        &ns_names_json
    )
    .fetch_all(connection)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;

    rows.into_iter()
        .map(|row| row.into_namespace_with_parent_version(warehouse_id))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

struct ListNamespaceRow {
    namespace_id: NamespaceId,
    warehouse_id: WarehouseId,
    namespace_name: Vec<String>,
    protected: bool,
    properties: Json<Option<HashMap<String, String>>>,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: Option<chrono::DateTime<chrono::Utc>>,
    parent_namespace_id: Option<Uuid>,
    parent_version: Option<i64>,
    version: i64,
    include_in_list: bool,
}

impl From<ListNamespaceRow> for NamespaceWithParentVersionRow {
    fn from(row: ListNamespaceRow) -> Self {
        NamespaceWithParentVersionRow {
            namespace_id: row.namespace_id,
            // Listing has no user-requested case; both fields use the stored (canonical) name.
            requested_name: row.namespace_name.clone(),
            namespace_name: row.namespace_name,
            warehouse_id: row.warehouse_id,
            protected: row.protected,
            properties: row.properties,
            created_at: row.created_at,
            updated_at: row.updated_at,
            parent_version: row.parent_version,
            parent_namespace_id: row.parent_namespace_id,
            version: row.version,
        }
    }
}

fn list_rows_into_hierarchy(
    rows: Vec<ListNamespaceRow>,
    warehouse_id: WarehouseId,
) -> std::result::Result<CatalogListNamespacesResponse, InvalidNamespaceIdentifier> {
    if rows.is_empty() {
        return Ok(CatalogListNamespacesResponse {
            parent_namespaces: HashMap::new(),
            namespaces: PaginatedMapping::with_capacity(0),
        });
    }

    let mut namespace_by_id: HashMap<NamespaceId, NamespaceWithParent> =
        HashMap::with_capacity(rows.len());

    // Track which namespaces should be included in the result, in order
    let mut result = PaginatedMapping::new();

    for row in rows {
        let include_this_row_in_list = row.include_in_list;

        let namespace = NamespaceWithParentVersionRow::from(row)
            .into_namespace_with_parent_version(warehouse_id)?;

        if include_this_row_in_list {
            let namespace_id = namespace.namespace_id();
            let created_at = namespace.created_at();

            let token = PaginateToken::V1(V1PaginateToken {
                id: namespace_id,
                created_at,
            })
            .to_string();

            result.insert(namespace_id, namespace.clone(), token);
        }

        namespace_by_id.insert(namespace.namespace_id(), namespace);
    }

    Ok(CatalogListNamespacesResponse {
        parent_namespaces: namespace_by_id,
        namespaces: result,
    })
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn list_namespaces(
    warehouse_id: WarehouseId,
    ListNamespacesQuery {
        page_token,
        page_size,
        parent,
        return_uuids: _,
        return_protection_status: _,
    }: &ListNamespacesQuery,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> std::result::Result<CatalogListNamespacesResponse, CatalogListNamespaceError> {
    let page_size = CONFIG.page_size_or_pagination_max(*page_size);

    // Treat empty parent as None
    let parent = parent
        .as_ref()
        .and_then(|p| if p.is_empty() { None } else { Some(p.clone()) });
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

    let namespaces = if let Some(parent) = parent {
        // If it doesn't fit in a i32 it is way too large. Validation would have failed
        // already in the catalog.
        let parent_len: i32 = parent.len().try_into().unwrap_or(MAX_NAMESPACE_DEPTH + 1);

        // Namespace name field is an array.
        // Get all namespaces where the "name" array has
        // length(parent) + 1 elements, and the first length(parent)
        // elements are equal to parent.
        sqlx::query_as!(
            ListNamespaceRow,
            r#"
            WITH list_entries AS (
                SELECT
                    n.namespace_id,
                    n.namespace_name
                FROM namespace n
                INNER JOIN warehouse w ON w.warehouse_id = $1
                WHERE n.warehouse_id = $1
                AND w.status = 'active'
                AND n.depth = $2 + 1
                AND "namespace_name"[1:$2] = $3
                --- PAGINATION
                AND ((n.created_at > $4 OR $4 IS NULL) OR (n.created_at = $4 AND n.namespace_id > $5))
                ORDER BY n.created_at, n.namespace_id ASC
                LIMIT $6
            ),
            parent_paths AS (
                SELECT DISTINCT
                    tn.namespace_name[1:generate_series(1, array_length(tn.namespace_name, 1))] as parent_name
                FROM list_entries tn
            ),
            relevant_namespaces AS (
                SELECT
                    n.namespace_id,
                    n.namespace_name,
                    n.warehouse_id,
                    n.protected,
                    n.namespace_properties,
                    n.created_at,
                    n.updated_at,
                    n.version,
                    n.namespace_id in (SELECT namespace_id FROM list_entries) AS "include_in_list"
                FROM namespace n
                WHERE n.warehouse_id = $1
                AND n.namespace_name IN (SELECT parent_name FROM parent_paths)
            )
            SELECT
                n.namespace_id,
                n.namespace_name as "namespace_name: Vec<String>",
                n.warehouse_id,
                n.protected,
                n.namespace_properties as "properties: Json<Option<HashMap<String, String>>>",
                n.created_at,
                n.updated_at,
                n.version,
                n.include_in_list AS "include_in_list!",
                p.namespace_id as "parent_namespace_id?",
                p.version as "parent_version?"
            FROM relevant_namespaces n
            LEFT JOIN relevant_namespaces p ON array_length(n.namespace_name, 1) = array_length(p.namespace_name, 1) + 1
                AND n.namespace_name[1:array_length(p.namespace_name, 1)] = p.namespace_name
            ORDER BY n.created_at, n.namespace_id ASC
            "#,
            *warehouse_id,
            parent_len,
            &*parent,
            token_ts,
            token_id,
            page_size
        )
        .fetch_all(&mut **transaction)
        .await
        .map_err(DBErrorHandler::into_catalog_backend_error)?
        .into_iter()
        .collect::<Vec<_>>()
    } else {
        sqlx::query_as!(
            ListNamespaceRow,
            r#"
            WITH list_entries AS (
                SELECT
                    n.namespace_id,
                    n.namespace_name
                FROM namespace n
                INNER JOIN warehouse w ON w.warehouse_id = $1
                WHERE n.warehouse_id = $1
                AND n.depth = 1
                AND w.status = 'active'
                AND ((n.created_at > $2 OR $2 IS NULL) OR (n.created_at = $2 AND n.namespace_id > $3))
                ORDER BY n.created_at, n.namespace_id ASC
                LIMIT $4
            ),
            parent_paths AS (
                SELECT DISTINCT
                    tn.namespace_name[1:generate_series(1, array_length(tn.namespace_name, 1))] as parent_name
                FROM list_entries tn
            ),
            relevant_namespaces AS (
                SELECT
                    n.namespace_id,
                    n.namespace_name,
                    n.warehouse_id,
                    n.protected,
                    n.namespace_properties,
                    n.created_at,
                    n.updated_at,
                    n.version,
                    n.namespace_id in (SELECT namespace_id FROM list_entries) AS "include_in_list"
                FROM namespace n
                WHERE n.warehouse_id = $1
                AND n.namespace_name IN (SELECT parent_name FROM parent_paths)
            )
            SELECT
                n.namespace_id,
                n.namespace_name as "namespace_name: Vec<String>",
                n.warehouse_id,
                n.protected,
                n.namespace_properties as "properties: Json<Option<HashMap<String, String>>>",
                n.created_at,
                n.updated_at,
                n.version,
                n.include_in_list AS "include_in_list!",
                p.namespace_id as "parent_namespace_id?",
                p.version as "parent_version?"
            FROM relevant_namespaces n
            LEFT JOIN relevant_namespaces p ON array_length(n.namespace_name, 1) = array_length(p.namespace_name, 1) + 1
                AND n.namespace_name[1:array_length(p.namespace_name, 1)] = p.namespace_name
            ORDER BY n.created_at, n.namespace_id ASC
            "#,
            *warehouse_id,
            token_ts,
            token_id,
            page_size
        )
        .fetch_all(&mut **transaction)
        .await
        .map_err(DBErrorHandler::into_catalog_backend_error)?
        .into_iter()
        .collect()
    };

    let namespace_map = list_rows_into_hierarchy(namespaces, warehouse_id)?;

    Ok(namespace_map)
}

/// The parent row a namespace is about to be written under, locked `FOR KEY SHARE` for the
/// remainder of the transaction.
#[derive(Debug)]
struct LockedParentNamespace {
    namespace_id: NamespaceId,
    /// The parent's **stored** spelling. The ancestor segments of any row written under it must be
    /// these bytes, never the caller's.
    namespace_name: Vec<String>,
}

/// Resolve a namespace path's parent by name, hold a key share lock on it, and return its id
/// together with its **stored** spelling.
///
/// Both writers that place a namespace under a parent need this — `create_namespace` and
/// `move_namespace` — and neither may write the caller's spelling of the ancestor segments.
/// `namespace_name` is `text[] collate "case_insensitive"`, so `["PARENT"]` matches a row stored as
/// `["parent"]`. Writing the caller's array verbatim stores a child whose prefix does not
/// byte-match the parent row it points at. The catalog stays correct — the parent id is right, and
/// every SQL comparison is collated — but `is_parent_ident` in the namespace cache compares
/// `child[..len - 1]` against the parent's ident with plain equality. A mismatch makes
/// `build_hierarchy_from_cache` invalidate and return `None` on *every* subsequent lookup, by id
/// and by name alike, and the reload re-inserts the same canonical bytes and fails identically — a
/// permanent miss plus eviction churn for that row and its whole subtree, not a stale read.
///
/// Callers must therefore build the written path as `namespace_name` plus the caller's leaf. Only
/// the leaf keeps the caller's casing, because a case-only change to the leaf is a genuine rename.
///
/// `FOR KEY SHARE` is the weakest lock that works: it conflicts with the `FOR UPDATE` that a DELETE
/// and a rename of the parent take (`namespace_name` is a key column of
/// `unique_namespace_per_warehouse`), so neither can commit before us. It deliberately does not
/// conflict with the `FOR NO KEY UPDATE` that property and protection updates take — nothing here
/// reads either, so excluding them would only add contention.
///
/// Deliberately returns no `version`. The parent's version belongs in the `parent_ns` CTE of each
/// caller's write statement, which reads it in the same statement as the write. Because
/// `FOR NO KEY UPDATE` is *not* excluded, a property or protection update on the parent can commit
/// between this lock and that write and bump `version` via `set_updated_at_and_increment_version`;
/// a version captured here would then be strictly older than the truth at write time.
///
/// Issued as its own statement rather than folded into a CTE of the write, because the caller needs
/// the stored name in Rust *before* the write is built.
///
/// `Ok(None)` means the parent does not exist; callers map it to their own `NamespaceNotFound`,
/// because their error types differ.
async fn lock_parent_namespace(
    warehouse_id: WarehouseId,
    parent: &NamespaceIdent,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> std::result::Result<Option<LockedParentNamespace>, CatalogBackendError> {
    Ok(sqlx::query!(
        r#"
            SELECT namespace_id, namespace_name
            FROM namespace
            WHERE warehouse_id = $1 AND namespace_name = $2
            FOR KEY SHARE
            "#,
        *warehouse_id,
        &**parent,
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?
    .map(|r| LockedParentNamespace {
        namespace_id: r.namespace_id.into(),
        namespace_name: r.namespace_name,
    }))
}

pub(crate) async fn create_namespace(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    request: CreateNamespaceRequest,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> std::result::Result<NamespaceWithParent, CatalogCreateNamespaceError> {
    let CreateNamespaceRequest {
        namespace,
        properties,
    } = request;
    let parent = namespace.parent();

    // See `lock_parent_namespace`: the parent is locked here, and its *stored* spelling is what the
    // ancestor segments of the new row are built from.
    let locked_parent = if let Some(ref parent) = parent {
        Some(
            lock_parent_namespace(warehouse_id, parent, transaction)
                .await?
                .ok_or_else(|| NamespaceNotFound::new(warehouse_id, parent.clone()))?,
        )
    } else {
        None
    };

    // What actually gets written: the parent's stored spelling of the ancestor segments, plus the
    // caller's spelling of the leaf. The parent was matched under the case-insensitive collation,
    // so the caller's spelling of the ancestors carries no information; only the leaf keeps it,
    // because a case-only leaf difference is the name itself. Identical construction to
    // `move_namespace`'s `written_name`.
    let written_name: Vec<String> = match locked_parent.as_ref() {
        Some(parent_row) => parent_row
            .namespace_name
            .iter()
            .cloned()
            .chain(namespace.as_ref().last().cloned())
            .collect(),
        // Creating at the warehouse root: a single element, nothing to canonicalise.
        None => namespace.as_ref().clone(),
    };

    // The parent's stored name, so `parent_ns` below matches byte-exactly rather than relying on
    // the collation a second time.
    let canonical_parent = locked_parent.as_ref().map(|r| r.namespace_name.as_slice());
    let has_parent = canonical_parent.is_some();

    let row = sqlx::query_as!(
        NamespaceWithParentVersionRow,
        r#"
        WITH inserted_ns AS (
            INSERT INTO namespace (warehouse_id, namespace_id, namespace_name, namespace_properties)
            (
                SELECT $1, $2, $3, $4
                WHERE EXISTS (
                    SELECT 1
                    FROM warehouse
                    WHERE warehouse_id = $1
                    AND status = 'active'
            ))
            RETURNING
                namespace_id,
                namespace_name,
                warehouse_id,
                protected,
                namespace_properties,
                created_at,
                updated_at,
                version
        ),
        parent_ns AS (
            SELECT
                namespace_id,
                version
            FROM namespace
            WHERE warehouse_id = $1
            AND $6
            AND namespace_name = $5
        )
        SELECT
            i.namespace_id as "namespace_id!",
            i.namespace_name as "namespace_name!",
            -- The ancestor segments were taken from the locked parent row, so what was inserted
            -- is canonical by construction; there is no separate requested spelling to carry.
            i.namespace_name as "requested_name!",
            i.warehouse_id as "warehouse_id!",
            i.protected as "protected!",
            i.namespace_properties as "properties!: Json<Option<HashMap<String, String>>>",
            i.created_at as "created_at!",
            i.updated_at,
            i.version as "version!",
            p.namespace_id as "parent_namespace_id?",
            p.version as "parent_version?"
        FROM inserted_ns i
        LEFT JOIN parent_ns p ON $6
        "#,
        *warehouse_id,
        *namespace_id,
        &written_name[..],
        serde_json::to_value(properties.clone()).map_err(|e| {
            NamespacePropertiesSerializationError::new(warehouse_id, namespace.clone(), e)
        })?,
        canonical_parent,
        has_parent
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| match e {
        sqlx::Error::Database(ref db_error) if db_error.is_unique_violation() => {
            tracing::debug!("Namespace already exists: {db_error:?}");
            CatalogCreateNamespaceError::from(NamespaceAlreadyExists::new(
                warehouse_id,
                namespace.clone(),
            ))
        }
        sqlx::Error::Database(ref db_error) if db_error.is_foreign_key_violation() => {
            tracing::debug!("Namespace foreign key violation: {db_error:?}");
            WarehouseIdNotFound::new(warehouse_id).into()
        }
        e @ sqlx::Error::RowNotFound => {
            tracing::debug!("Warehouse not found: {e:?}");
            WarehouseIdNotFound::new(warehouse_id).into()
        }
        _ => {
            tracing::error!("Internal error creating namespace: {e:?}");
            e.into_catalog_backend_error().into()
        }
    })?;

    // Check if parent was expected but not found
    if let Some(parent) = parent
        && row.parent_namespace_id.is_none()
    {
        return Err(CatalogCreateNamespaceError::from(NamespaceNotFound::new(
            warehouse_id,
            parent,
        )));
    }

    row.into_namespace_with_parent_version(warehouse_id)
        .map_err(Into::into)
}

/// Rewrite namespace path prefixes that disagree with their parent row's spelling, returning the
/// number of rows changed.
///
/// Maintenance, run from the post-migration hooks rather than as a migration, and gated there on the
/// migration it is pinned to having just been applied — so it runs once per upgrade, not on every
/// startup. Deliberately not a migration itself: migrations run inside one transaction under
/// `SET LOCAL statement_timeout = '60min'` while holding the advisory lock, with `serve` waiting on
/// them, so a slow data rewrite there can abort an upgrade and keep the server down. As a hook it
/// runs outside that transaction, and its failure is logged rather than fatal unless an operator
/// asked for it explicitly.
///
/// Written to be idempotent regardless, because both recovery paths depend on it:
/// `migrate --force-idempotent-post-migration-hooks` re-runs it after a failure, and re-pinning it to
/// a later migration re-runs it for a newly found hole.
///
/// `create_namespace`
/// used to insert the caller's whole path verbatim while resolving the parent under the
/// `case_insensitive` collation, so creating `a/b/c` under a parent stored `a/B` stored the child as
/// {a,b,c}. Nothing in the database is broken by that — the parent id is right, every SQL comparison
/// is collated — but `is_parent_ident` in the namespace cache compares the prefix byte-wise, so such
/// a row and every descendant can never be served from cache: each lookup invalidates, reloads the
/// same bytes and fails identically. `lock_parent_namespace` stops new drift; this repairs what is
/// already stored.
///
/// One statement per depth, ascending, is a performance requirement rather than a style choice. The
/// natural recursive form joins on `child.namespace_name[1:parent.depth] = parent.stored`, whose
/// left operand references both relations, so Postgres can never use it as a join key — it demotes
/// to a per-row `Join Filter` over the cross product of each depth level with the next, which is
/// quadratic. Measured with nothing to repair: 4.4 s at 28k namespaces, 18.6 s at 66k. Pinning the
/// depth per statement makes the slice bound depend only on the child, yielding
/// `Hash Cond: ((c.warehouse_id = p.warehouse_id) AND (c.namespace_name[1:2] = p.namespace_name))`
/// — 154 ms at 128k for the whole loop. Same trap `move_namespace`'s `has_children` guard documents.
///
/// Ascending order is what makes it correct: each statement reads parents one level up that the
/// previous statement already canonicalised, so a repair at depth 2 propagates downwards. Bounded by
/// the deepest row present rather than `MAX_NAMESPACE_DEPTH`, so rows written while that limit was
/// higher are still repaired. Rows whose ancestor is missing are left alone.
///
/// Collision-free by construction: every rewritten value is collation-equal to the value it
/// replaces, and `unique_namespace_per_warehouse` is over that collation. `ON UPDATE CASCADE`
/// carries the new spelling to `tabular.tabular_namespace_name`. `version` and `updated_at` are
/// untouched, because the trigger's `WHEN` compares the collated column and a case-only rewrite is
/// NOT DISTINCT there — replicas do not need the bump, since an affected row could never have been
/// cached anyway.
pub(crate) async fn repair_namespace_path_casing(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> std::result::Result<u64, CatalogBackendError> {
    let max_depth: Option<i32> = sqlx::query_scalar!(r#"SELECT max(depth) FROM namespace"#)
        .fetch_one(&mut **transaction)
        .await
        .map_err(DBErrorHandler::into_catalog_backend_error)?;

    let mut repaired = 0;
    for depth in 2..=max_depth.unwrap_or(1) {
        repaired += sqlx::query!(
            r#"
            UPDATE namespace c
            SET namespace_name = p.namespace_name || c.namespace_name[$1:]
            FROM namespace p
            WHERE p.warehouse_id = c.warehouse_id
                AND c.depth = $1
                AND p.depth = $1 - 1
                -- Collated: finds the parent case-insensitively.
                AND p.namespace_name = c.namespace_name[1:$1 - 1]
                -- Byte-wise: rewrites only rows that actually differ, which makes this idempotent.
                AND (c.namespace_name[1:$1 - 1]::text) COLLATE "C"
                    <> (p.namespace_name::text) COLLATE "C"
            "#,
            depth,
        )
        .execute(&mut **transaction)
        .await
        .map_err(DBErrorHandler::into_catalog_backend_error)?
        .rows_affected();
    }
    Ok(repaired)
}

pub(crate) async fn move_namespace(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    destination: &NamespaceIdent,
    force: bool,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> std::result::Result<MovedNamespace, CatalogMoveNamespaceError> {
    // Lock the row we are about to move for the remainder of the transaction, so that a
    // concurrent move/drop/create cannot invalidate the checks below. `FOR UPDATE OF n`
    // locks the namespace row only — the warehouse row is merely inspected, and the
    // LEFT JOINed parent must not be locked (Postgres rejects locking the nullable side
    // of an outer join).
    //
    // Selects the full row, not just what the guards need, so that the no-op path below
    // can answer from it without a second read.
    let source = sqlx::query_as!(
        NamespaceWithParentVersionRow,
        r#"
        SELECT
            n.namespace_id as "namespace_id!",
            n.namespace_name as "namespace_name!",
            -- Addressed by id, so there is no user-requested case to preserve.
            n.namespace_name as "requested_name!",
            n.warehouse_id as "warehouse_id!",
            n.protected as "protected!",
            n.namespace_properties as "properties!: Json<Option<HashMap<String, String>>>",
            n.created_at as "created_at!",
            n.updated_at,
            n.version as "version!",
            p.namespace_id as "parent_namespace_id?",
            p.version as "parent_version?"
        FROM namespace n
            INNER JOIN warehouse w
                ON n.warehouse_id = w.warehouse_id AND w.status = 'active'
            LEFT JOIN namespace p
                ON p.warehouse_id = n.warehouse_id
                AND array_length(n.namespace_name, 1) > 1
                AND p.namespace_name = n.namespace_name[1:array_length(n.namespace_name, 1) - 1]
        WHERE n.warehouse_id = $1 AND n.namespace_id = $2
        FOR UPDATE OF n
        "#,
        *warehouse_id,
        *namespace_id,
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?
    .ok_or_else(|| NamespaceNotFound::new(warehouse_id, namespace_id))?;

    // A destination identical to the current path changes nothing, so answer with the row
    // just read. Returned before any guard, so that retrying a completed move succeeds
    // rather than colliding with itself or tripping a guard that no longer matters.
    // Comparison is byte-exact, so if source != destination this might be
    // - a "genuine move" to a different parent (possibly including a rename)
    // - a "genuine rename", including one that changes only the leaf's casing
    // - a no-op where the difference is only in the *casing* of the ancestors, which the
    //   case-insensitive parent lookup makes meaningless
    // The last case is handled below, after the guard, once the parent's stored spelling is known.
    if source.namespace_name == destination.as_ref()[..] {
        let previous_parent: Option<NamespaceId> = source.parent_namespace_id.map(Into::into);
        let namespace = source.into_namespace_with_parent_version(warehouse_id)?;
        let previous_ident = namespace.canonical_ident().clone();
        return Ok(MovedNamespace {
            namespace,
            previous_ident,
            previous_parent,
        });
    }

    // Captured before the UPDATE: the cache must evict the old ident and the authorizer
    // must delete the tuples pointing at the old parent.
    let previous_ident = parse_namespace_identifier_from_vec(
        &source.namespace_name,
        warehouse_id,
        Some(namespace_id),
    )?;
    let previous_parent: Option<NamespaceId> = source.parent_namespace_id.map(Into::into);

    if source.protected && !force {
        return Err(NamespaceProtected::new(warehouse_id, namespace_id).into());
    }

    // If it doesn't fit in an i32 it is way too large; validation would have rejected it
    // in the catalog layer already. Matches `list_namespaces`.
    let source_depth: i32 = source
        .namespace_name
        .len()
        .try_into()
        .unwrap_or(MAX_NAMESPACE_DEPTH + 1);

    // Moving a namespace P that has descendants is not implemented yet: the UPDATE below
    // rewrites only this row, so descendants would keep their old absolute paths and stop
    // resolving. Check if the moving/renaming namespace has children.
    //
    // Any descendant counts, not just direct children. Descendants of P are contiguous in the
    // `unique_namespace_per_warehouse` btree immediately after P.
    //
    // Deliberately a separate statement, issued only after the `FOR UPDATE` above returned:
    // under READ COMMITTED each statement takes a fresh snapshot, so a concurrent
    // `create_namespace` that held the source row's lock while we were blocked becomes visible
    // here as soon as it commits. Folded into the locking SELECT, this guard would run against
    // the snapshot taken *before* we blocked and would miss that child.
    let has_children = sqlx::query_scalar!(
        r#"
        SELECT coalesce((
            SELECT namespace_name[1:$2] = $3
            FROM namespace
            WHERE warehouse_id = $1
                AND namespace_name > $3
            ORDER BY namespace_name
            LIMIT 1
        ), false) AS "has_children!"
        "#,
        *warehouse_id,
        source_depth,
        &source.namespace_name,
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;

    if has_children {
        return Err(NamespaceHasChildren::new(warehouse_id, namespace_id).into());
    }

    // See `lock_parent_namespace` for the lock strength and why the stored spelling is read.
    //
    // What that lock does *not* stop is the parent being dropped immediately after our commit:
    // `drop_namespace` evaluates its emptiness guard in an unlocked statement and then deletes by
    // an id list frozen from that snapshot, so it neither sees our new child nor re-checks after
    // waiting on this lock. That is a pre-existing gap in `drop_namespace`, not one this lock can
    // close; closing it needs a `FOR UPDATE` pre-lock there — and it must be `FOR UPDATE`, not
    // `FOR NO KEY UPDATE`, or it would not conflict with `FOR KEY SHARE` and the mutual exclusion
    // would silently vanish.
    let destination_parent = destination.parent();
    let destination_parent_row = if let Some(ref parent) = destination_parent {
        let parent_row = lock_parent_namespace(warehouse_id, parent, transaction)
            .await?
            .ok_or_else(|| NamespaceNotFound::new(warehouse_id, parent.clone()))?;

        // The destination's parent cannot be the namespace itself. A *deeper* descendant
        // is impossible to reach here: it would have to exist as the parent, and we
        // already rejected any namespace with descendants above.
        if parent_row.namespace_id == namespace_id {
            return Err(NamespaceCannotMoveIntoSelf::new(warehouse_id, namespace_id).into());
        }
        Some(parent_row)
    } else {
        None
    };
    let has_destination_parent = destination_parent_row.is_some();

    // What actually gets written: the parent's *stored* spelling of the ancestor segments, plus the
    // caller's spelling of the leaf. See `lock_parent_namespace` for why the prefix must come from
    // the parent row. Identical construction to `create_namespace`.
    //
    // A destination that differs only in ancestor casing canonicalises to the path this row
    // already has; the check below answers it as the no-op it is.
    let written_name: Vec<String> = match destination_parent_row.as_ref() {
        Some(parent_row) => parent_row
            .namespace_name
            .iter()
            .cloned()
            .chain(destination.as_ref().last().cloned())
            .collect(),
        // Moving to the warehouse root: a single element, nothing to canonicalise.
        None => destination.as_ref().clone(),
    };

    // The parent resolved case-insensitively, so a destination differing from the current path
    // only in ancestor casing canonicalises back to the path this row already has. Answer it as
    // the no-op it is, rather than issuing an UPDATE that rewrites the same bytes while bumping
    // `version` and `updated_at` — which would leave `is_noop()` reporting true while the row's
    // version had in fact moved, suppressing the event that would tell any replica about it.
    //
    // Deliberately after the guards, unlike the byte-exact check at the top: this input already
    // reaches them today, so answering it here changes no error, only the redundant write.
    // Resolving the parent earlier instead would take its lock on paths that hold none today and
    // would let `NamespaceNotFound` outrank `NamespaceProtected`.
    if written_name == source.namespace_name {
        let namespace = source.into_namespace_with_parent_version(warehouse_id)?;
        return Ok(MovedNamespace {
            namespace,
            previous_ident,
            previous_parent,
        });
    }

    let canonical_destination_parent = destination_parent_row
        .as_ref()
        .map(|r| r.namespace_name.as_slice());

    // Collisions are caught by the `unique_namespace_per_warehouse` constraint rather
    // than by a probe, so there is no window between checking and writing. Renaming a row
    // to a name only it holds — e.g. a case-only rename — does not violate it.
    //
    // `version` and `updated_at` are set explicitly rather than left to the
    // `set_updated_at_and_increment_version` trigger: its `WHEN` clause compares
    // `namespace_name`, which is `text[] collate "case_insensitive"`, so a case-only rename
    // compares equal and the trigger never fires. The trigger *assigns*
    // `NEW.version = OLD.version + 1` rather than incrementing `NEW`, so setting both here
    // cannot double-bump when it does fire. Both no-op checks — byte-exact at the top of this
    // function, and canonical once the parent is known — return before this point, so a request
    // that changes nothing still bumps nothing.
    let row = sqlx::query_as!(
        NamespaceWithParentVersionRow,
        r#"
        WITH updated_ns AS (
            UPDATE namespace
            SET namespace_name = $3,
                version = version + 1,
                updated_at = now()
            WHERE warehouse_id = $1 AND namespace_id = $2
            RETURNING
                namespace_id,
                namespace_name,
                warehouse_id,
                protected,
                namespace_properties,
                created_at,
                updated_at,
                version
        ),
        parent_ns AS (
            SELECT namespace_id, version
            FROM namespace
            WHERE warehouse_id = $1
                AND $5
                AND namespace_name = $4
        )
        SELECT
            u.namespace_id as "namespace_id!",
            u.namespace_name as "namespace_name!",
            -- What was written is canonical by construction (see `written_name`), so there is
            -- no separate requested spelling to carry.
            u.namespace_name as "requested_name!",
            u.warehouse_id as "warehouse_id!",
            u.protected as "protected!",
            u.namespace_properties as "properties!: Json<Option<HashMap<String, String>>>",
            u.created_at as "created_at!",
            u.updated_at,
            u.version as "version!",
            p.namespace_id as "parent_namespace_id?",
            p.version as "parent_version?"
        FROM updated_ns u
        LEFT JOIN parent_ns p ON $5
        "#,
        *warehouse_id,
        *namespace_id,
        &written_name[..],
        // The parent's stored name, so `parent_ns` matches byte-exactly rather than relying on
        // the collation a second time.
        canonical_destination_parent,
        has_destination_parent,
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| match e {
        sqlx::Error::Database(ref db_error) if db_error.is_unique_violation() => {
            tracing::debug!("Move destination already exists: {db_error:?}");
            CatalogMoveNamespaceError::from(NamespaceAlreadyExists::new(
                warehouse_id,
                destination.clone(),
            ))
        }
        _ => {
            tracing::error!("Internal error moving namespace {namespace_id}: {e:?}");
            e.into_catalog_backend_error().into()
        }
    })?
    // The row was locked above, so this only fires if the warehouse or namespace was
    // removed by this same transaction between the lock and here.
    .ok_or_else(|| NamespaceNotFound::new(warehouse_id, namespace_id))?;

    Ok(MovedNamespace {
        namespace: row.into_namespace_with_parent_version(warehouse_id)?,
        previous_ident,
        previous_parent,
    })
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn drop_namespace(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    NamespaceDropFlags {
        force,
        purge: _purge,
        recursive,
    }: NamespaceDropFlags,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> std::result::Result<NamespaceDropInfo, CatalogNamespaceDropError> {
    let info = sqlx::query!(r#"
        WITH namespace_info AS (
            SELECT namespace_name, namespace_id, protected
            FROM namespace
            WHERE warehouse_id = $1 AND namespace_id = $2
        ),
        child_namespaces AS (
            SELECT n.protected, n.namespace_id, n.namespace_name
            FROM namespace n
            INNER JOIN namespace_info ni ON n.namespace_name[1:array_length(ni.namespace_name, 1)] = ni.namespace_name
            WHERE n.warehouse_id = $1 AND n.namespace_id != $2
        ),
        tabulars AS (
            SELECT ta.tabular_id, ta.name as table_name, COALESCE(ni.namespace_name, cn.namespace_name) as namespace_name, fs_location, fs_protocol, ta.typ, ta.protected, deleted_at
            FROM tabular ta
            LEFT JOIN namespace_info ni ON ta.namespace_id = ni.namespace_id
            LEFT JOIN child_namespaces cn ON ta.namespace_id = cn.namespace_id
            WHERE warehouse_id = $1 AND (metadata_location IS NOT NULL OR ta.typ = 'generic-table') AND (ta.namespace_id = $2 OR (ta.namespace_id = ANY (SELECT namespace_id FROM child_namespaces)))
        ),
        tasks AS (
            SELECT t.task_id, t.queue_name, t.status as task_status from task t
            WHERE t.entity_id = ANY (SELECT tabular_id FROM tabulars) AND t.warehouse_id = $1 AND t.entity_type in ('table', 'view', 'generic-table')
        )
        SELECT
            ni.protected AS "is_protected!",
            ni.namespace_name AS "namespace_name: Vec<String>",
            EXISTS (SELECT 1 FROM child_namespaces WHERE protected = true) AS "has_protected_namespaces!",
            EXISTS (SELECT 1 FROM tabulars WHERE protected = true) AS "has_protected_tabulars!",
            EXISTS (SELECT 1 FROM tasks WHERE task_status = 'running' AND queue_name IN ('soft_deletion', 'tabular_expiration')) AS "has_running_expiration!",
            ARRAY(SELECT tabular_id FROM tabulars where deleted_at is NULL) AS "child_tabulars!",
            ARRAY(SELECT to_jsonb(namespace_name) FROM tabulars where deleted_at is NULL) AS "child_tabulars_namespace_names!: Vec<serde_json::Value>",
            ARRAY(SELECT table_name FROM tabulars where deleted_at is NULL) AS "child_tabulars_table_names!",
            ARRAY(SELECT fs_protocol FROM tabulars where deleted_at is NULL) AS "child_tabular_fs_protocol!",
            ARRAY(SELECT fs_location FROM tabulars where deleted_at is NULL) AS "child_tabular_fs_location!",
            ARRAY(SELECT typ FROM tabulars where deleted_at is NULL) AS "child_tabular_typ!: Vec<TabularType>",
            ARRAY(SELECT tabular_id FROM tabulars where deleted_at is not NULL) AS "child_tabulars_deleted!",
            ARRAY(SELECT namespace_id FROM child_namespaces) AS "child_namespaces!",
            ARRAY(SELECT task_id FROM tasks) AS "child_tabular_task_id!: Vec<Uuid>"
        FROM namespace_info ni
"#,
        *warehouse_id,
        *namespace_id,
    ).fetch_one(&mut **transaction).await.map_err(|e|
        if let sqlx::Error::RowNotFound = e {
            CatalogNamespaceDropError::from(NamespaceNotFound::new(warehouse_id, namespace_id))
         } else {
            e.into_catalog_backend_error().into()
        }
    )?;
    let namespace_ident = parse_namespace_identifier_from_vec(
        &info.namespace_name,
        warehouse_id,
        Some(namespace_id),
    )?;

    if !recursive
        && (!info.child_tabulars.is_empty()
            || !info.child_tabulars_deleted.is_empty()
            || !info.child_namespaces.is_empty())
    {
        return Err(
            NamespaceNotEmpty::new(warehouse_id, namespace_ident.clone()).append_detail(format!("Contains {} tables/views/generic tables, {} soft-deleted tables/views/generic tables and {} child namespaces.",
                info.child_tabulars.len(),
                info.child_tabulars_deleted.len(),
                info.child_namespaces.len()
        )

    ).append_detail("Use 'recursive' flag to delete all content.").into()
        );
    }

    if !force && info.is_protected {
        return Err(NamespaceProtected::new(warehouse_id, namespace_ident.clone()).into());
    }

    if !force && info.has_protected_namespaces {
        return Err(ChildNamespaceProtected::new(warehouse_id, namespace_ident.clone()).into());
    }

    if !force && info.has_protected_tabulars {
        return Err(ChildTabularProtected::new(warehouse_id, namespace_ident.clone()).into());
    }

    if info.has_running_expiration {
        return Err(NamespaceHasRunningTabularExpirations::new(
            warehouse_id,
            namespace_ident.clone(),
        )
        .into());
    }

    let record = sqlx::query!(
        r#"
        DELETE FROM namespace
            WHERE warehouse_id = $1
            -- If recursive is true, delete all child namespaces...
            AND (namespace_id = any($2) or namespace_id = $3)
            AND warehouse_id IN (
                SELECT warehouse_id FROM warehouse WHERE status = 'active'
                AND warehouse_id = $1
            )
        "#,
        *warehouse_id,
        &info.child_namespaces,
        *namespace_id,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| match &e {
        sqlx::Error::Database(db_error) if db_error.is_foreign_key_violation() => {
            CatalogNamespaceDropError::from(NamespaceNotEmpty::new(
                warehouse_id,
                namespace_ident.clone(),
            ))
        }
        _ => e.into_catalog_backend_error().into(),
    })?;

    tracing::debug!(
        "Deleted {deleted_count} namespaces while dropping namespace {namespace_ident} with id {namespace_id} in warehouse {warehouse_id}",
        deleted_count = record.rows_affected()
    );

    if record.rows_affected() == 0 {
        return Err(NamespaceNotFound::new(warehouse_id, namespace_ident.clone()).into());
    }

    Ok(NamespaceDropInfo {
        child_namespaces: info.child_namespaces.into_iter().map(Into::into).collect(),
        child_tables: izip!(
            info.child_tabulars,
            info.child_tabular_fs_protocol,
            info.child_tabular_fs_location,
            info.child_tabular_typ,
            info.child_tabulars_namespace_names,
            info.child_tabulars_table_names
        )
        .map(
            |(tabular_id, protocol, fs_location, typ, ns_name, t_name)| {
                let ns_ident = json_value_to_namespace_ident(warehouse_id, &ns_name)?;
                let table_ident = TableIdent::new(ns_ident, t_name);
                Ok::<_, CatalogNamespaceDropError>((
                    match typ {
                        TabularType::Table => TabularId::Table(tabular_id.into()),
                        TabularType::View => TabularId::View(tabular_id.into()),
                        TabularType::GenericTable => TabularId::GenericTable(tabular_id.into()),
                    },
                    join_location(protocol.as_str(), fs_location.as_str())
                        .map_err(InternalParseLocationError::from)?,
                    table_ident,
                ))
            },
        )
        .collect::<std::result::Result<Vec<_>, _>>()?,
        open_tasks: info
            .child_tabular_task_id
            .into_iter()
            .map(TaskId::from)
            .collect(),
    })
}

pub(super) fn parse_namespace_identifier_from_vec(
    namespace: &[String],
    warehouse_id: WarehouseId,
    namespace_id: Option<impl Into<NamespaceId>>,
) -> std::result::Result<NamespaceIdent, InvalidNamespaceIdentifier> {
    let namespace_id = namespace_id.map(Into::into);
    NamespaceIdent::from_vec(namespace.to_owned()).map_err(|_e| {
        let err = InvalidNamespaceIdentifier::new(warehouse_id, format!("{namespace:?}"))
            .append_detail("Namespace identifier can't be empty");
        if let Some(id) = namespace_id {
            err.with_id(id)
        } else {
            err
        }
    })
}

fn json_value_to_namespace_ident(
    warehouse_id: WarehouseId,
    v: &serde_json::Value,
) -> Result<NamespaceIdent, InvalidNamespaceIdentifier> {
    if let serde_json::Value::Array(arr) = v.clone() {
        let str_vec: Result<Vec<String>, InvalidNamespaceIdentifier> = arr
            .into_iter()
            .map(|item| {
                if let serde_json::Value::String(s) = item {
                    Ok(s)
                } else {
                    Err(
                        InvalidNamespaceIdentifier::new(warehouse_id, format!("{v:?}"))
                            .append_detail("Expected array of strings for namespace identifier"),
                    )
                }
            })
            .collect();
        NamespaceIdent::from_vec(str_vec?).map_err(|_e| {
            InvalidNamespaceIdentifier::new(warehouse_id, format!("{v:?}"))
                .append_detail("Namespace identifier can't be empty")
        })
    } else {
        Err(
            InvalidNamespaceIdentifier::new(warehouse_id, format!("{v:?}"))
                .append_detail("Expected array for namespace identifier"),
        )
    }
}

pub(crate) async fn set_namespace_protected(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    protect: bool,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> std::result::Result<NamespaceWithParent, CatalogSetNamespaceProtectedError> {
    let row = sqlx::query_as!(
        NamespaceWithParentVersionRow,
        r#"
        WITH updated_ns AS (
            UPDATE namespace
            SET protected = $1
            WHERE namespace_id = $2 AND warehouse_id IN (
                SELECT warehouse_id FROM warehouse WHERE status = 'active'
            )
            RETURNING
                namespace_id,
                namespace_name,
                warehouse_id,
                protected,
                namespace_properties,
                created_at,
                updated_at,
                version
        ),
        parent_ns AS (
            SELECT
                p.namespace_id,
                p.version
            FROM updated_ns u
            INNER JOIN namespace p ON p.warehouse_id = u.warehouse_id
                AND p.namespace_name = u.namespace_name[1:array_length(u.namespace_name, 1) - 1]
            WHERE array_length(u.namespace_name, 1) > 1
        )
        SELECT
            u.namespace_id as "namespace_id!",
            u.namespace_name as "namespace_name!",
            -- No user-requested case in protection update path; return canonical.
            u.namespace_name as "requested_name!",
            u.warehouse_id as "warehouse_id!",
            u.protected as "protected!",
            u.namespace_properties as "properties!: Json<Option<HashMap<String, String>>>",
            u.created_at as "created_at!",
            u.updated_at,
            u.version as "version!",
            p.namespace_id as "parent_namespace_id?",
            p.version as "parent_version?"
        FROM updated_ns u
        LEFT JOIN parent_ns p ON TRUE
        "#,
        protect,
        *namespace_id
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| {
        if let sqlx::Error::RowNotFound = e {
            CatalogSetNamespaceProtectedError::from(NamespaceNotFound::new(
                warehouse_id,
                namespace_id,
            ))
        } else {
            tracing::error!("Error setting namespace protection: {e:?}");
            e.into_catalog_backend_error().into()
        }
    })?;

    row.into_namespace_with_parent_version(warehouse_id)
        .map_err(Into::into)
}

pub(crate) async fn update_namespace_properties(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    properties: HashMap<String, String>,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> std::result::Result<NamespaceWithParent, CatalogUpdateNamespacePropertiesError> {
    let properties = serde_json::to_value(properties)
        .map_err(|e| NamespacePropertiesSerializationError::new(warehouse_id, namespace_id, e))?;

    let row = sqlx::query_as!(
        NamespaceWithParentVersionRow,
        r#"
        WITH updated_ns AS (
            UPDATE namespace
            SET namespace_properties = $1
            WHERE warehouse_id = $2 AND namespace_id = $3
            AND warehouse_id IN (
                SELECT warehouse_id FROM warehouse WHERE status = 'active'
            )
            RETURNING
                namespace_id,
                namespace_name,
                warehouse_id,
                protected,
                namespace_properties,
                created_at,
                updated_at,
                version
        ),
        parent_ns AS (
            SELECT
                p.namespace_id,
                p.version
            FROM updated_ns u
            INNER JOIN namespace p ON p.warehouse_id = u.warehouse_id
                AND p.namespace_name = u.namespace_name[1:array_length(u.namespace_name, 1) - 1]
            WHERE array_length(u.namespace_name, 1) > 1
        )
        SELECT
            u.namespace_id as "namespace_id!",
            u.namespace_name as "namespace_name!",
            -- No user-requested case in property update path; return canonical.
            u.namespace_name as "requested_name!",
            u.warehouse_id as "warehouse_id!",
            u.protected as "protected!",
            u.namespace_properties as "properties!: Json<Option<HashMap<String, String>>>",
            u.created_at as "created_at!",
            u.updated_at,
            u.version as "version!",
            p.namespace_id as "parent_namespace_id?",
            p.version as "parent_version?"
        FROM updated_ns u
        LEFT JOIN parent_ns p ON TRUE
        "#,
        properties,
        *warehouse_id,
        *namespace_id
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => CatalogUpdateNamespacePropertiesError::from(
            NamespaceNotFound::new(warehouse_id, namespace_id),
        ),
        _ => e.into_catalog_backend_error().into(),
    })?;

    row.into_namespace_with_parent_version(warehouse_id)
        .map_err(Into::into)
}

#[cfg(any(test, feature = "test-utils"))]
#[allow(unused_imports, dead_code)]
pub mod tests {
    use std::str::FromStr;

    use lakekeeper::{
        api::iceberg::{types::PageToken, v1::tables::LoadTableFilters},
        service::{
            CachePolicy, CatalogNamespaceOps, Transaction as _,
            is_same_namespace_path_ignoring_ascii_case,
        },
    };

    use super::{
        super::{PostgresBackend, warehouse::test::initialize_warehouse},
        *,
    };
    use crate::{
        CatalogState, PostgresTransaction,
        tabular::{
            mark_tabular_as_deleted, set_tabular_protected,
            table::{load_tables, tests::initialize_table},
        },
    };

    pub async fn initialize_namespace(
        state: CatalogState,
        warehouse_id: WarehouseId,
        namespace: &NamespaceIdent,
        properties: Option<HashMap<String, String>>,
    ) -> NamespaceWithParent {
        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        let namespace_id = NamespaceId::new_random();

        let response = PostgresBackend::create_namespace(
            warehouse_id,
            namespace_id,
            CreateNamespaceRequest {
                namespace: namespace.clone(),
                properties: properties.clone(),
            },
            transaction.transaction(),
        )
        .await
        .unwrap();

        transaction.commit().await.unwrap();

        response
    }

    #[sqlx::test]
    async fn test_namespace_lifecycle(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        let namespace = NamespaceIdent::from_vec(vec!["test".to_string()]).unwrap();
        let properties = HashMap::from_iter(vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]);

        let namespace_info = initialize_namespace(
            state.clone(),
            warehouse_id,
            &namespace,
            Some(properties.clone()),
        )
        .await;

        let namespace_hierarchy_by_name = PostgresBackend::get_namespace_cache_aware(
            warehouse_id,
            &namespace,
            CachePolicy::Skip,
            state.clone(),
        )
        .await
        .unwrap()
        .expect("Namespace should exist");
        assert_eq!(
            namespace_hierarchy_by_name.root(),
            &namespace_hierarchy_by_name.namespace
        );
        assert_eq!(namespace_hierarchy_by_name.depth(), 0);
        assert_eq!(*namespace_hierarchy_by_name.version(), 0);
        assert_eq!(namespace_hierarchy_by_name.parent(), None);
        assert_eq!(namespace_hierarchy_by_name.namespace.parent, None);
        let namespace_id = namespace_hierarchy_by_name.namespace_id();

        assert_eq!(&namespace_hierarchy_by_name.namespace, &namespace_info);

        let namespace_hierarchy_by_id = PostgresBackend::get_namespace_cache_aware(
            warehouse_id,
            namespace_id,
            CachePolicy::Skip,
            state.clone(),
        )
        .await
        .unwrap()
        .expect("Namespace should exist");

        assert_eq!(namespace_hierarchy_by_id, namespace_hierarchy_by_name);

        let mut transaction = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();

        let _response = PostgresBackend::get_namespace_cache_aware(
            warehouse_id,
            &namespace,
            CachePolicy::Skip,
            state.clone(),
        )
        .await
        .unwrap()
        .expect("Namespace should exist");

        let response = PostgresBackend::list_namespaces(
            warehouse_id,
            &ListNamespacesQuery {
                page_token: lakekeeper::api::iceberg::v1::PageToken::NotSpecified,
                page_size: None,
                parent: None,
                return_uuids: false,
                return_protection_status: false,
            },
            transaction.transaction(),
        )
        .await
        .unwrap()
        .namespaces
        .into_hashmap();

        assert_eq!(response.len(), 1);
        assert_eq!(response[&namespace_id].namespace_ident(), &namespace);

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        let new_props = HashMap::from_iter(vec![
            ("key2".to_string(), "updated_value".to_string()),
            ("new_key".to_string(), "new_value".to_string()),
        ]);
        PostgresBackend::update_namespace_properties(
            warehouse_id,
            namespace_id,
            new_props.clone(),
            transaction.transaction(),
        )
        .await
        .unwrap();

        transaction.commit().await.unwrap();

        let response = PostgresBackend::get_namespace_cache_aware(
            warehouse_id,
            namespace_id,
            CachePolicy::Skip,
            state.clone(),
        )
        .await
        .unwrap()
        .expect("Namespace should exist");
        assert_eq!(response.properties().unwrap(), &new_props);

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        PostgresBackend::drop_namespace(
            warehouse_id,
            namespace_id,
            NamespaceDropFlags::default(),
            transaction.transaction(),
        )
        .await
        .expect("Error dropping namespace");
    }

    #[sqlx::test]
    async fn test_pagination(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["test".to_string()]).unwrap();
        let properties = Some(HashMap::from_iter(vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]));

        let namespace_info_1 =
            initialize_namespace(state.clone(), warehouse_id, &namespace, properties.clone()).await;

        let namespace = NamespaceIdent::from_vec(vec!["test2".to_string()]).unwrap();
        let properties = Some(HashMap::from_iter(vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]));
        let namespace_info_2 =
            initialize_namespace(state.clone(), warehouse_id, &namespace, properties.clone()).await;
        let namespace = NamespaceIdent::from_vec(vec!["test3".to_string()]).unwrap();
        let properties = Some(HashMap::from_iter(vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]));
        let namespace_info_3 =
            initialize_namespace(state.clone(), warehouse_id, &namespace, properties.clone()).await;

        let mut t = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();

        let namespaces = PostgresBackend::list_namespaces(
            warehouse_id,
            &ListNamespacesQuery {
                page_token: lakekeeper::api::iceberg::v1::PageToken::NotSpecified,
                page_size: Some(1),
                parent: None,
                return_uuids: false,
                return_protection_status: false,
            },
            t.transaction(),
        )
        .await
        .unwrap()
        .namespaces;
        let next_page_token = namespaces.next_token().map(ToString::to_string);
        assert_eq!(namespaces.len(), 1);
        let namespaces = namespaces.into_hashmap();
        assert_eq!(
            namespaces[&namespace_info_1.namespace_id()].namespace_ident(),
            namespace_info_1.namespace_ident()
        );
        assert!(!namespaces[&namespace_info_1.namespace_id()].is_protected());
        // Root namespaces should have no parents
        assert!(namespaces[&namespace_info_1.namespace_id()].is_root());

        let mut t = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();

        let namespaces = PostgresBackend::list_namespaces(
            warehouse_id,
            &ListNamespacesQuery {
                page_token: next_page_token.map_or(
                    lakekeeper::api::iceberg::v1::PageToken::Empty,
                    lakekeeper::api::iceberg::v1::PageToken::Present,
                ),
                page_size: Some(2),
                parent: None,
                return_uuids: false,
                return_protection_status: false,
            },
            t.transaction(),
        )
        .await
        .unwrap()
        .namespaces;
        let next_page_token = namespaces.next_token().map(ToString::to_string);
        assert_eq!(namespaces.len(), 2);
        assert!(next_page_token.is_some());
        let namespaces = namespaces.into_hashmap();

        assert_eq!(
            namespaces[&namespace_info_2.namespace_id()].namespace_ident(),
            namespace_info_2.namespace_ident()
        );
        assert!(!namespaces[&namespace_info_2.namespace_id()].is_protected());
        assert!(namespaces[&namespace_info_2.namespace_id()].is_root());
        assert_eq!(
            namespaces[&namespace_info_3.namespace_id()].namespace_ident(),
            namespace_info_3.namespace_ident()
        );
        assert!(!namespaces[&namespace_info_3.namespace_id()].is_protected());
        assert!(namespaces[&namespace_info_3.namespace_id()].is_root());

        // last page is empty
        let namespaces = PostgresBackend::list_namespaces(
            warehouse_id,
            &ListNamespacesQuery {
                page_token: next_page_token.map_or(
                    lakekeeper::api::iceberg::v1::PageToken::Empty,
                    lakekeeper::api::iceberg::v1::PageToken::Present,
                ),
                page_size: Some(3),
                parent: None,
                return_uuids: false,
                return_protection_status: false,
            },
            t.transaction(),
        )
        .await
        .unwrap()
        .namespaces;

        assert_eq!(namespaces.next_token(), None);
        assert_eq!(namespaces.into_hashmap(), HashMap::new());
    }

    #[sqlx::test]
    async fn test_get_nested_namespace(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        let parent_namespace_ident = NamespaceIdent::from_vec(vec!["parent".to_string()]).unwrap();
        let parent_namespace: NamespaceWithParent =
            initialize_namespace(state.clone(), warehouse_id, &parent_namespace_ident, None).await;

        let child_namespace_ident =
            NamespaceIdent::from_vec(vec!["parent".to_string(), "child".to_string()]).unwrap();
        let child_namespace =
            initialize_namespace(state.clone(), warehouse_id, &child_namespace_ident, None).await;

        let result = PostgresBackend::get_namespace_cache_aware(
            warehouse_id,
            &child_namespace_ident,
            CachePolicy::Skip,
            state.clone(),
        )
        .await
        .unwrap()
        .expect("Namespace should exist");
        assert_eq!(&result.namespace, &child_namespace);
        assert_eq!(result.depth(), 1);
        assert_eq!(result.root(), &parent_namespace);
        assert_eq!(result.parents.len(), 1);
        assert_eq!(&result.parents[0], &parent_namespace);
    }

    #[sqlx::test]
    async fn test_get_nonexistent_namespace(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        let result = PostgresBackend::get_namespace_cache_aware(
            warehouse_id,
            NamespaceId::new_random(),
            CachePolicy::Skip,
            state.clone(),
        )
        .await
        .unwrap();
        assert_eq!(result, None);
    }

    #[sqlx::test]
    async fn test_drop_nonexistent_namespace(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let result = drop_namespace(
            warehouse_id,
            NamespaceId::new_random(),
            NamespaceDropFlags::default(),
            transaction.transaction(),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            result,
            CatalogNamespaceDropError::NamespaceNotFound(_)
        ));
    }

    #[sqlx::test]
    async fn test_cannot_drop_nonempty_namespace(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let staged = false;
        let table = initialize_table(warehouse_id, state.clone(), staged, None, None, None).await;

        let namespace_id = PostgresBackend::get_namespace_cache_aware(
            warehouse_id,
            Into::<NamespaceIdent>::into(table.namespace),
            CachePolicy::Skip,
            state.clone(),
        )
        .await
        .unwrap()
        .expect("Namespace should exist")
        .namespace_id();
        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let result = drop_namespace(
            warehouse_id,
            namespace_id,
            NamespaceDropFlags::default(),
            transaction.transaction(),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            result,
            CatalogNamespaceDropError::NamespaceNotEmpty(_)
        ));
    }

    // Regression test: a namespace containing only soft-deleted tabulars (pending
    // expiration) must not be droppable non-recursively.  Before the fix the
    // non-recursive guard checked only active tabulars, so the FK CASCADE on
    // namespace deletion would hard-delete the soft-deleted rows and orphan their
    // expiration tasks.
    #[sqlx::test]
    async fn test_cannot_drop_namespace_with_soft_deleted_tabulars(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let staged = false;
        let table = initialize_table(warehouse_id, state.clone(), staged, None, None, None).await;

        let namespace_id = PostgresBackend::get_namespace_cache_aware(
            warehouse_id,
            Into::<NamespaceIdent>::into(table.namespace.clone()),
            CachePolicy::Skip,
            state.clone(),
        )
        .await
        .unwrap()
        .expect("Namespace should exist")
        .namespace_id();

        // Soft-delete the table — it stays in the tabular table with deleted_at set.
        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        mark_tabular_as_deleted(
            warehouse_id,
            TabularId::Table(table.table_id),
            false,
            None,
            transaction.transaction(),
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        // Non-recursive drop must fail: the namespace still contains a soft-deleted tabular.
        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let result = drop_namespace(
            warehouse_id,
            namespace_id,
            NamespaceDropFlags::default(),
            transaction.transaction(),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(result, CatalogNamespaceDropError::NamespaceNotEmpty(_)),
            "expected NamespaceNotEmpty, got {result:?}"
        );
    }

    // Non-recursive drop must fail when the namespace contains a generic table.
    // Exercises the `entity_type IN ('table', 'view', 'generic-table')` branch in
    // the drop_namespace SQL.
    #[sqlx::test]
    async fn test_cannot_drop_namespace_with_generic_tables(pool: sqlx::PgPool) {
        use lakekeeper::service::{
            CatalogGenericTableOps as _, GenericTableCreation, GenericTableFormat, GenericTableId,
        };

        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let ns_ident =
            NamespaceIdent::from_vec(vec![format!("ns_{}", uuid::Uuid::now_v7())]).unwrap();
        let ns = initialize_namespace(state.clone(), warehouse_id, &ns_ident, None).await;
        let namespace_id = ns.namespace_id();

        let mut trx = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        PostgresBackend::create_generic_table(
            GenericTableCreation {
                generic_table_id: GenericTableId::from(uuid::Uuid::now_v7()),
                namespace_id,
                warehouse_id,
                name: "gt".to_string(),
                format: GenericTableFormat::Unknown("lance".to_string()),
                location: lakekeeper_io::Location::from_str(&format!(
                    "memory://test/{warehouse_id}/gt"
                ))
                .unwrap(),
                doc: None,
                schema: None,
                statistics: None,
                properties: HashMap::default(),
            },
            trx.transaction(),
        )
        .await
        .unwrap();
        trx.commit().await.unwrap();

        let mut trx = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let result = drop_namespace(
            warehouse_id,
            namespace_id,
            NamespaceDropFlags::default(),
            trx.transaction(),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(result, CatalogNamespaceDropError::NamespaceNotEmpty(_)),
            "expected NamespaceNotEmpty for namespace with generic-table child, got {result:?}"
        );
    }

    // Recursive drop must succeed and surface the generic table as a child.
    #[sqlx::test]
    async fn test_can_recursive_drop_namespace_with_generic_tables(pool: sqlx::PgPool) {
        use lakekeeper::service::{
            CatalogGenericTableOps as _, GenericTableCreation, GenericTableFormat, GenericTableId,
        };

        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let ns_ident =
            NamespaceIdent::from_vec(vec![format!("ns_{}", uuid::Uuid::now_v7())]).unwrap();
        let ns = initialize_namespace(state.clone(), warehouse_id, &ns_ident, None).await;
        let namespace_id = ns.namespace_id();

        let gt_id = GenericTableId::from(uuid::Uuid::now_v7());
        let gt_name = "gt-recursive";
        let mut trx = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        PostgresBackend::create_generic_table(
            GenericTableCreation {
                generic_table_id: gt_id,
                namespace_id,
                warehouse_id,
                name: gt_name.to_string(),
                format: GenericTableFormat::Unknown("lance".to_string()),
                location: lakekeeper_io::Location::from_str(&format!(
                    "memory://test/{warehouse_id}/{gt_id}"
                ))
                .unwrap(),
                doc: None,
                schema: None,
                statistics: None,
                properties: HashMap::default(),
            },
            trx.transaction(),
        )
        .await
        .unwrap();
        trx.commit().await.unwrap();

        let mut trx = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let drop_info = drop_namespace(
            warehouse_id,
            namespace_id,
            NamespaceDropFlags {
                force: false,
                purge: false,
                recursive: true,
            },
            trx.transaction(),
        )
        .await
        .unwrap();

        assert_eq!(drop_info.child_namespaces.len(), 0);
        assert_eq!(drop_info.child_tables.len(), 1);
        let (child_id, _, child_ident) = &drop_info.child_tables[0];
        assert_eq!(*child_id, TabularId::GenericTable(gt_id));
        assert_eq!(child_ident.name, gt_name);
        trx.commit().await.unwrap();
    }

    #[sqlx::test]
    async fn test_can_recursive_drop_nonempty_namespace(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let staged = false;
        let table = initialize_table(warehouse_id, state.clone(), staged, None, None, None).await;

        let namespace_id = PostgresBackend::get_namespace_cache_aware(
            warehouse_id,
            Into::<NamespaceIdent>::into(table.namespace),
            CachePolicy::Skip,
            state.clone(),
        )
        .await
        .unwrap()
        .expect("Namespace should exist")
        .namespace_id();

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let drop_info = drop_namespace(
            warehouse_id,
            namespace_id,
            NamespaceDropFlags {
                force: false,
                purge: false,
                recursive: true,
            },
            transaction.transaction(),
        )
        .await
        .unwrap();

        assert_eq!(drop_info.child_namespaces.len(), 0);
        assert_eq!(drop_info.child_tables.len(), 1);
        assert_eq!(drop_info.open_tasks.len(), 0);
        let r0 = &drop_info.child_tables[0];
        assert_eq!(r0.0, TabularId::Table(table.table_id));
        assert_eq!(r0.2, table.table_ident);

        transaction.commit().await.unwrap();

        let mut transaction = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();
        let tables = load_tables(
            warehouse_id,
            [table.table_id].into_iter(),
            true,
            &LoadTableFilters::default(),
            transaction.transaction(),
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        assert_eq!(tables.len(), 0);
    }

    #[sqlx::test]
    async fn test_cannot_drop_namespace_with_sub_namespaces(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["test".to_string()]).unwrap();

        let response = initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;

        let namespace =
            NamespaceIdent::from_vec(vec!["test".to_string(), "test2".to_string()]).unwrap();
        let response2 = initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        let result = drop_namespace(
            warehouse_id,
            response.namespace_id(),
            NamespaceDropFlags::default(),
            transaction.transaction(),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            result,
            CatalogNamespaceDropError::NamespaceNotEmpty(_)
        ));

        drop_namespace(
            warehouse_id,
            response2.namespace_id(),
            NamespaceDropFlags::default(),
            transaction.transaction(),
        )
        .await
        .unwrap();

        drop_namespace(
            warehouse_id,
            response.namespace_id(),
            NamespaceDropFlags::default(),
            transaction.transaction(),
        )
        .await
        .unwrap();
    }

    #[sqlx::test]
    async fn test_can_recursive_drop_namespace_with_sub_namespaces(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["test".to_string()]).unwrap();

        let response = initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;

        let namespace =
            NamespaceIdent::from_vec(vec!["test".to_string(), "test2".to_string()]).unwrap();
        let _ = initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        let drop_info = drop_namespace(
            warehouse_id,
            response.namespace_id(),
            NamespaceDropFlags {
                force: false,
                purge: false,
                recursive: true,
            },
            transaction.transaction(),
        )
        .await
        .unwrap();

        assert_eq!(drop_info.child_namespaces.len(), 1);
        assert_eq!(drop_info.child_tables.len(), 0);
        assert_eq!(drop_info.open_tasks.len(), 0);

        transaction.commit().await.unwrap();

        let mut transaction = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();
        let ns = list_namespaces(
            warehouse_id,
            &ListNamespacesQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(100),
                parent: None,
                return_uuids: true,
                return_protection_status: false,
            },
            transaction.transaction(),
        )
        .await
        .unwrap()
        .namespaces;
        transaction.commit().await.unwrap();

        assert_eq!(ns.len(), 0);
    }

    #[sqlx::test]
    async fn test_case_insensitive_but_preserve_case(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace_1 = NamespaceIdent::from_vec(vec!["Test".to_string()]).unwrap();
        let namespace_2 = NamespaceIdent::from_vec(vec!["test".to_string()]).unwrap();

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        let response = PostgresBackend::create_namespace(
            warehouse_id,
            NamespaceId::new_random(),
            CreateNamespaceRequest {
                namespace: namespace_1.clone(),
                properties: None,
            },
            transaction.transaction(),
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        // Check that the namespace is created with the correct case
        assert_eq!(response.namespace_ident(), &namespace_1);

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        let response = PostgresBackend::create_namespace(
            warehouse_id,
            NamespaceId::new_random(),
            CreateNamespaceRequest {
                namespace: namespace_2.clone(),
                properties: None,
            },
            transaction.transaction(),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            response,
            CatalogCreateNamespaceError::NamespaceAlreadyExists(_)
        ));
    }

    #[sqlx::test]
    async fn test_cannot_drop_protected_namespace(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["test".to_string()]).unwrap();

        let response = initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;
        assert_eq!(*response.version(), 0);

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        let protected_response = PostgresBackend::set_namespace_protected(
            warehouse_id,
            response.namespace_id(),
            true,
            transaction.transaction(),
        )
        .await
        .unwrap();
        assert_eq!(*protected_response.version(), 1);

        let result = drop_namespace(
            warehouse_id,
            response.namespace_id(),
            NamespaceDropFlags::default(),
            transaction.transaction(),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            result,
            CatalogNamespaceDropError::NamespaceProtected(_)
        ));
    }

    #[sqlx::test]
    async fn test_can_force_drop_protected_namespace(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["test".to_string()]).unwrap();

        let response = initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        PostgresBackend::set_namespace_protected(
            warehouse_id,
            response.namespace_id(),
            true,
            transaction.transaction(),
        )
        .await
        .unwrap();

        let result = drop_namespace(
            warehouse_id,
            response.namespace_id(),
            NamespaceDropFlags {
                force: true,
                purge: false,
                recursive: false,
            },
            transaction.transaction(),
        )
        .await
        .unwrap();

        assert!(result.child_namespaces.is_empty());
        assert!(result.child_tables.is_empty());
        assert!(result.open_tasks.is_empty());
    }

    #[sqlx::test]
    async fn test_can_recursive_force_drop_nonempty_protected_namespace(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let outer_namespace = NamespaceIdent::from_vec(vec!["test".to_string()]).unwrap();

        let response =
            initialize_namespace(state.clone(), warehouse_id, &outer_namespace, None).await;
        let namespace_id = response.namespace_id();

        let namespace =
            NamespaceIdent::from_vec(vec!["test".to_string(), "test2".to_string()]).unwrap();
        let _ = initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        set_namespace_protected(warehouse_id, namespace_id, true, transaction.transaction())
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let err = drop_namespace(
            warehouse_id,
            namespace_id,
            NamespaceDropFlags {
                force: false,
                purge: false,
                recursive: true,
            },
            transaction.transaction(),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            CatalogNamespaceDropError::NamespaceProtected(_)
        ));

        let drop_info = drop_namespace(
            warehouse_id,
            namespace_id,
            NamespaceDropFlags {
                force: true,
                recursive: true,
                purge: false,
            },
            transaction.transaction(),
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        assert_eq!(drop_info.child_namespaces.len(), 1);
        assert_eq!(drop_info.child_tables.len(), 0);
        assert_eq!(drop_info.open_tasks.len(), 0);
    }

    #[sqlx::test]
    async fn test_can_recursive_force_drop_namespace_with_protected_table(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let outer_namespace = NamespaceIdent::from_vec(vec!["test".to_string()]).unwrap();

        let response =
            initialize_namespace(state.clone(), warehouse_id, &outer_namespace, None).await;
        let namespace_id = response.namespace_id();
        let tab = initialize_table(
            warehouse_id,
            state.clone(),
            false,
            Some(outer_namespace),
            None,
            None,
        )
        .await;

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        set_tabular_protected(
            warehouse_id,
            TabularId::Table(tab.table_id),
            true,
            transaction.transaction(),
        )
        .await
        .unwrap();

        let err = drop_namespace(
            warehouse_id,
            namespace_id,
            NamespaceDropFlags {
                force: false,
                purge: false,
                recursive: true,
            },
            transaction.transaction(),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            CatalogNamespaceDropError::ChildTabularProtected(_)
        ));

        let drop_info = drop_namespace(
            warehouse_id,
            namespace_id,
            NamespaceDropFlags {
                force: true,
                recursive: true,
                purge: false,
            },
            transaction.transaction(),
        )
        .await
        .unwrap();

        assert_eq!(drop_info.child_namespaces.len(), 0);
        assert_eq!(drop_info.child_tables.len(), 1);
        assert_eq!(drop_info.open_tasks.len(), 0);

        transaction.commit().await.unwrap();
    }

    #[sqlx::test]
    async fn test_list_namespaces_with_hierarchy(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        // Create a hierarchy: root, root.child, root.child.grandchild
        let root = NamespaceIdent::from_vec(vec!["root".to_string()]).unwrap();
        let root_ns = initialize_namespace(state.clone(), warehouse_id, &root, None).await;

        let child =
            NamespaceIdent::from_vec(vec!["root".to_string(), "child".to_string()]).unwrap();
        let child_ns = initialize_namespace(state.clone(), warehouse_id, &child, None).await;

        let grandchild = NamespaceIdent::from_vec(vec![
            "root".to_string(),
            "child".to_string(),
            "grandchild".to_string(),
        ])
        .unwrap();
        let grandchild_ns =
            initialize_namespace(state.clone(), warehouse_id, &grandchild, None).await;

        // List all root namespaces (no parent filter)
        let mut transaction = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();

        let result = PostgresBackend::list_namespaces(
            warehouse_id,
            &ListNamespacesQuery {
                page_token: PageToken::NotSpecified,
                page_size: None,
                parent: None,
                return_uuids: false,
                return_protection_status: false,
            },
            transaction.transaction(),
        )
        .await
        .unwrap();

        // Should only return root namespace
        assert_eq!(result.parent_namespaces.len(), 1);
        let result = result.namespaces;
        assert_eq!(result.len(), 1);
        let result_map = result.into_hashmap();

        let root_hierarchy = &result_map[&root_ns.namespace_id()];
        assert_eq!(root_hierarchy.namespace_ident(), &root);
        assert!(root_hierarchy.is_root());

        // List children of root
        let mut transaction = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();

        let result = PostgresBackend::list_namespaces(
            warehouse_id,
            &ListNamespacesQuery {
                page_token: PageToken::NotSpecified,
                page_size: None,
                parent: Some(root.clone()),
                return_uuids: false,
                return_protection_status: false,
            },
            transaction.transaction(),
        )
        .await
        .unwrap();

        // Should return child with root as parent
        assert_eq!(result.parent_namespaces.len(), 2);
        let result = result.namespaces;
        assert_eq!(result.len(), 1);
        let result_map = result.into_hashmap();

        let child_hierarchy = &result_map[&child_ns.namespace_id()];
        assert_eq!(child_hierarchy.namespace_ident(), &child);
        assert!(!child_hierarchy.is_root());

        // List children of root.child
        let mut transaction = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();

        let result = PostgresBackend::list_namespaces(
            warehouse_id,
            &ListNamespacesQuery {
                page_token: PageToken::NotSpecified,
                page_size: None,
                parent: Some(child.clone()),
                return_uuids: false,
                return_protection_status: false,
            },
            transaction.transaction(),
        )
        .await
        .unwrap();

        // Should return grandchild with full hierarchy
        assert_eq!(result.parent_namespaces.len(), 3);
        let result = result.namespaces;
        assert_eq!(result.len(), 1);
        let result_map = result.into_hashmap();

        let grandchild_hierarchy = &result_map[&grandchild_ns.namespace_id()];
        assert_eq!(grandchild_hierarchy.namespace_ident(), &grandchild);
        assert!(!grandchild_hierarchy.is_root());
    }

    #[sqlx::test]
    async fn test_list_namespaces_multiple_hierarchies(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        // Create multiple root namespaces with children
        // Root A with child A.1
        let root_a = NamespaceIdent::from_vec(vec!["a".to_string()]).unwrap();
        let root_a_ns = initialize_namespace(state.clone(), warehouse_id, &root_a, None).await;

        let child_a1 = NamespaceIdent::from_vec(vec!["a".to_string(), "1".to_string()]).unwrap();
        let child_a1_ns = initialize_namespace(state.clone(), warehouse_id, &child_a1, None).await;

        // Root B with child B.1
        let root_b = NamespaceIdent::from_vec(vec!["b".to_string()]).unwrap();
        let root_b_ns = initialize_namespace(state.clone(), warehouse_id, &root_b, None).await;

        let child_b1 = NamespaceIdent::from_vec(vec!["b".to_string(), "1".to_string()]).unwrap();
        let child_b1_ns = initialize_namespace(state.clone(), warehouse_id, &child_b1, None).await;

        // List all root namespaces
        let mut transaction = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();

        let result = PostgresBackend::list_namespaces(
            warehouse_id,
            &ListNamespacesQuery {
                page_token: PageToken::NotSpecified,
                page_size: None,
                parent: None,
                return_uuids: false,
                return_protection_status: false,
            },
            transaction.transaction(),
        )
        .await
        .unwrap();

        // Should return both roots, both with no parents
        assert_eq!(result.parent_namespaces.len(), 2);
        let result = result.namespaces;
        assert_eq!(result.len(), 2);
        let result_map = result.into_hashmap();

        assert!(result_map[&root_a_ns.namespace_id()].is_root());
        assert!(result_map[&root_b_ns.namespace_id()].is_root());

        // List children of root A
        let mut transaction = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();

        let result = PostgresBackend::list_namespaces(
            warehouse_id,
            &ListNamespacesQuery {
                page_token: PageToken::NotSpecified,
                page_size: None,
                parent: Some(root_a.clone()),
                return_uuids: false,
                return_protection_status: false,
            },
            transaction.transaction(),
        )
        .await
        .unwrap();

        // Should only return A.1 with correct parent
        assert_eq!(result.parent_namespaces.len(), 2);
        let result = result.namespaces;
        assert_eq!(result.len(), 1);
        let result_map = result.into_hashmap();

        let a1_hierarchy = &result_map[&child_a1_ns.namespace_id()];
        assert_eq!(
            a1_hierarchy.parent.unwrap(),
            (root_a_ns.namespace_id(), root_a_ns.version())
        );

        // List children of root B
        let mut transaction = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();

        let result = PostgresBackend::list_namespaces(
            warehouse_id,
            &ListNamespacesQuery {
                page_token: PageToken::NotSpecified,
                page_size: None,
                parent: Some(root_b.clone()),
                return_uuids: false,
                return_protection_status: false,
            },
            transaction.transaction(),
        )
        .await
        .unwrap();

        // Should only return B.1 with correct parent
        assert_eq!(result.parent_namespaces.len(), 2);
        let result = result.namespaces;
        assert_eq!(result.len(), 1);
        let result_map = result.into_hashmap();

        let b1_hierarchy = &result_map[&child_b1_ns.namespace_id()];
        assert_eq!(
            b1_hierarchy.parent.unwrap(),
            (root_b_ns.namespace_id(), root_b_ns.version())
        );
    }

    #[sqlx::test]
    async fn test_list_namespaces_pagination_with_hierarchy(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        // Create parent and multiple children
        let parent = NamespaceIdent::from_vec(vec!["parent".to_string()]).unwrap();
        let parent_ns = initialize_namespace(state.clone(), warehouse_id, &parent, None).await;

        let child1 =
            NamespaceIdent::from_vec(vec!["parent".to_string(), "child1".to_string()]).unwrap();
        let child1_ns = initialize_namespace(state.clone(), warehouse_id, &child1, None).await;

        let child2 =
            NamespaceIdent::from_vec(vec!["parent".to_string(), "child2".to_string()]).unwrap();
        let child2_ns = initialize_namespace(state.clone(), warehouse_id, &child2, None).await;

        let child3 =
            NamespaceIdent::from_vec(vec!["parent".to_string(), "child3".to_string()]).unwrap();
        let child3_ns = initialize_namespace(state.clone(), warehouse_id, &child3, None).await;

        // List children with pagination (page_size = 2)
        let mut transaction = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();

        let result = PostgresBackend::list_namespaces(
            warehouse_id,
            &ListNamespacesQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(2),
                parent: Some(parent.clone()),
                return_uuids: false,
                return_protection_status: false,
            },
            transaction.transaction(),
        )
        .await
        .unwrap();

        // First page: 2 children
        assert_eq!(result.parent_namespaces.len(), 3);
        let result = result.namespaces;
        assert_eq!(result.len(), 2);
        let next_token = result.next_token().map(ToString::to_string);
        assert!(next_token.is_some());

        let result_map = result.into_hashmap();

        // All returned children should have parent hierarchy
        assert!(
            result_map.contains_key(&child1_ns.namespace_id())
                || result_map.contains_key(&child2_ns.namespace_id())
                || result_map.contains_key(&child3_ns.namespace_id())
        );

        for hierarchy in result_map.values() {
            assert_eq!(
                hierarchy.parent.unwrap(),
                (parent_ns.namespace_id(), parent_ns.version())
            );
        }

        // Get second page
        let mut transaction = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();

        let result = PostgresBackend::list_namespaces(
            warehouse_id,
            &ListNamespacesQuery {
                page_token: next_token.map_or(PageToken::Empty, PageToken::Present),
                page_size: Some(2),
                parent: Some(parent.clone()),
                return_uuids: false,
                return_protection_status: false,
            },
            transaction.transaction(),
        )
        .await
        .unwrap()
        .namespaces;

        // Second page: 1 child
        assert_eq!(result.len(), 1);
        let result_map = result.into_hashmap();

        // This child should also have parent hierarchy
        for hierarchy in result_map.values() {
            assert_eq!(
                hierarchy.parent.unwrap(),
                (parent_ns.namespace_id(), parent_ns.version())
            );
        }
    }

    #[sqlx::test]
    async fn test_list_namespaces_deep_hierarchy(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        // Create a 4-level deep hierarchy
        let level1 = NamespaceIdent::from_vec(vec!["level1".to_string()]).unwrap();
        let _level1_ns = initialize_namespace(state.clone(), warehouse_id, &level1, None).await;

        let level2 =
            NamespaceIdent::from_vec(vec!["level1".to_string(), "level2".to_string()]).unwrap();
        let _level2_ns = initialize_namespace(state.clone(), warehouse_id, &level2, None).await;

        let level3 = NamespaceIdent::from_vec(vec![
            "level1".to_string(),
            "level2".to_string(),
            "level3".to_string(),
        ])
        .unwrap();
        let _level3_ns = initialize_namespace(state.clone(), warehouse_id, &level3, None).await;

        let level4 = NamespaceIdent::from_vec(vec![
            "level1".to_string(),
            "level2".to_string(),
            "level3".to_string(),
            "level4".to_string(),
        ])
        .unwrap();
        let level4_ns = initialize_namespace(state.clone(), warehouse_id, &level4, None).await;

        // List at level 4 (deepest)
        let mut transaction = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();

        let result = PostgresBackend::list_namespaces(
            warehouse_id,
            &ListNamespacesQuery {
                page_token: PageToken::NotSpecified,
                page_size: None,
                parent: Some(level3.clone()),
                return_uuids: false,
                return_protection_status: false,
            },
            transaction.transaction(),
        )
        .await
        .unwrap();

        let parents = result.parent_namespaces;
        let result = result.namespaces;
        assert_eq!(result.len(), 1);
        let result_map = result.into_hashmap();

        let level4_hierarchy = &result_map[&level4_ns.namespace_id()];

        // Verify parent chain: level3 -> level2 -> level1
        assert_eq!(parents.len(), 4);
        let parent3 = parents
            .get(&level4_hierarchy.parent_namespaces_id().unwrap())
            .unwrap();
        assert_eq!(parent3.namespace_ident(), &level3);
        let parent2 = parents
            .get(&parent3.parent_namespaces_id().unwrap())
            .unwrap();
        assert_eq!(parent2.namespace_ident(), &level2);
        let parent1 = parents
            .get(&parent2.parent_namespaces_id().unwrap())
            .unwrap();
        assert_eq!(parent1.namespace_ident(), &level1);
        assert!(parent1.is_root());
    }

    #[sqlx::test]
    async fn test_list_namespaces_preserves_case(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        let ns_mixed = NamespaceIdent::from_vec(vec!["Analytics".to_string()]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &ns_mixed, None).await;

        let mut transaction = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();

        let result = list_namespaces(
            warehouse_id,
            &ListNamespacesQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(100),
                parent: None,
                return_uuids: false,
                return_protection_status: false,
            },
            transaction.transaction(),
        )
        .await
        .unwrap();

        assert_eq!(result.namespaces.len(), 1);
        let ns = result.namespaces.into_hashmap();
        let stored = ns.values().next().unwrap();
        assert_eq!(stored.namespace_ident(), &ns_mixed);
    }

    #[sqlx::test]
    async fn test_get_namespace_case_insensitive_lookup(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        let ns_mixed = NamespaceIdent::from_vec(vec!["Analytics".to_string()]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &ns_mixed, None).await;

        // Lookup with different case should succeed
        let ns_upper = NamespaceIdent::from_vec(vec!["ANALYTICS".to_string()]).unwrap();
        let found = PostgresBackend::get_namespace_cache_aware(
            warehouse_id,
            &ns_upper,
            CachePolicy::Skip,
            state.clone(),
        )
        .await
        .unwrap();
        assert!(
            found.is_some(),
            "Namespace should be found case-insensitively"
        );

        let ns_lower = NamespaceIdent::from_vec(vec!["analytics".to_string()]).unwrap();
        let found = PostgresBackend::get_namespace_cache_aware(
            warehouse_id,
            &ns_lower,
            CachePolicy::Skip,
            state.clone(),
        )
        .await
        .unwrap();
        assert!(
            found.is_some(),
            "Namespace should be found case-insensitively"
        );
    }

    /// Regression test: an ident-based lookup with a different case than stored
    /// must not contaminate the id-cache. A subsequent id-based lookup must
    /// return the canonical (stored) case, not whatever case the first caller
    /// happened to use.
    ///
    /// Without a proper fix, by-name lookups populate the cache with the
    /// requester's case, and subsequent by-id lookups return that case — making
    /// id-based lookups non-deterministic (depending on whoever populated the
    /// cache first).
    #[sqlx::test]
    async fn test_id_lookup_returns_canonical_case(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        let canonical_ident = NamespaceIdent::from_vec(vec!["Mixed_Case".to_string()]).unwrap();
        let ns = initialize_namespace(state.clone(), warehouse_id, &canonical_ident, None).await;
        let namespace_id = ns.namespace_id();

        // Caller A does an ident-based lookup with a different case.
        // This populates the cache.
        let upper_ident = NamespaceIdent::from_vec(vec!["MIXED_CASE".to_string()]).unwrap();
        let via_upper_ident = PostgresBackend::get_namespace_cache_aware(
            warehouse_id,
            &upper_ident,
            CachePolicy::Use,
            state.clone(),
        )
        .await
        .unwrap()
        .expect("namespace should be found");
        // Response should carry the caller's requested case.
        assert_eq!(via_upper_ident.namespace_ident(), &upper_ident);

        // Caller B does an id-based lookup. The id has no case context, so the
        // canonical (stored) case must be returned — NOT whatever case caller A
        // used to populate the cache.
        let via_id = PostgresBackend::get_namespace_cache_aware(
            warehouse_id,
            namespace_id,
            CachePolicy::Use,
            state.clone(),
        )
        .await
        .unwrap()
        .expect("namespace should be found by id");
        assert_eq!(
            via_id.namespace_ident(),
            &canonical_ident,
            "id-based lookup must return canonical case, not the case from a prior ident-based caller"
        );
    }

    /// Cache hits must carry the current caller's case, not whichever case
    /// populated the cache first. Two different-case lookups should each get
    /// their own case back.
    #[sqlx::test]
    async fn test_cache_hit_returns_callers_case(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        let canonical = NamespaceIdent::from_vec(vec!["CacheHit".to_string()]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &canonical, None).await;

        // Caller A populates the cache with uppercase.
        let upper = NamespaceIdent::from_vec(vec!["CACHEHIT".to_string()]).unwrap();
        let a = PostgresBackend::get_namespace_cache_aware(
            warehouse_id,
            &upper,
            CachePolicy::Use,
            state.clone(),
        )
        .await
        .unwrap()
        .expect("found");
        assert_eq!(a.namespace_ident(), &upper);

        // Caller B looks up with lowercase. The cache entry exists (canonical-case id
        // cache entry + uppercase ident cache entry). Lowercase ident cache misses →
        // DB hit → populates lowercase ident cache. Response carries lowercase.
        let lower = NamespaceIdent::from_vec(vec!["cachehit".to_string()]).unwrap();
        let b = PostgresBackend::get_namespace_cache_aware(
            warehouse_id,
            &lower,
            CachePolicy::Use,
            state.clone(),
        )
        .await
        .unwrap()
        .expect("found");
        assert_eq!(b.namespace_ident(), &lower);

        // Caller C looks up with lowercase again. This time the ident cache hits.
        // The response still carries lowercase, not whatever case caller A used.
        let c = PostgresBackend::get_namespace_cache_aware(
            warehouse_id,
            &lower,
            CachePolicy::Use,
            state.clone(),
        )
        .await
        .unwrap()
        .expect("found");
        assert_eq!(c.namespace_ident(), &lower);
    }

    /// Multi-level lookup must return the caller's case for the leaf AND all
    /// ancestor levels of the hierarchy.
    #[sqlx::test]
    async fn test_hierarchy_lookup_applies_case_to_all_levels(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        let ns_root = NamespaceIdent::from_vec(vec!["Foo".to_string()]).unwrap();
        let ns_mid = NamespaceIdent::from_vec(vec!["Foo".to_string(), "Bar".to_string()]).unwrap();
        let ns_leaf = NamespaceIdent::from_vec(vec![
            "Foo".to_string(),
            "Bar".to_string(),
            "Baz".to_string(),
        ])
        .unwrap();
        initialize_namespace(state.clone(), warehouse_id, &ns_root, None).await;
        initialize_namespace(state.clone(), warehouse_id, &ns_mid, None).await;
        initialize_namespace(state.clone(), warehouse_id, &ns_leaf, None).await;

        // Lookup with all-uppercase case.
        let upper = NamespaceIdent::from_vec(vec![
            "FOO".to_string(),
            "BAR".to_string(),
            "BAZ".to_string(),
        ])
        .unwrap();
        let hier = PostgresBackend::get_namespace_cache_aware(
            warehouse_id,
            &upper,
            CachePolicy::Skip,
            state.clone(),
        )
        .await
        .unwrap()
        .expect("found");

        // Leaf carries caller's case.
        assert_eq!(hier.namespace_ident(), &upper);
        // Parents carry corresponding prefix in caller's case.
        assert_eq!(hier.parents.len(), 2);
        assert_eq!(
            hier.parents[0].namespace_ident(),
            &NamespaceIdent::from_vec(vec!["FOO".to_string(), "BAR".to_string()]).unwrap()
        );
        assert_eq!(
            hier.parents[1].namespace_ident(),
            &NamespaceIdent::from_vec(vec!["FOO".to_string()]).unwrap()
        );

        // Canonical ident (underneath the requested_ident overlay) is still the
        // stored case — internal code can access it if needed.
        assert_eq!(
            hier.namespace.canonical_ident(),
            &ns_leaf,
            "canonical_ident should remain the stored case"
        );
    }

    /// Batch ident lookup must apply caller's case to every returned namespace,
    /// regardless of whether an individual entry came from cache or DB.
    /// Every spelling a caller asks about gets its own row, all pointing at the same
    /// namespace.
    ///
    /// `requested_name` is how a caller maps an ident it asked about back to an id,
    /// and the keys it builds from it are case-sensitive. Deduplicating the requested
    /// arrays case-insensitively would leave one arbitrary spelling with a row and
    /// the others with none, which reads downstream as "namespace not found".
    #[sqlx::test]
    async fn every_requested_spelling_maps_to_the_same_namespace(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        let parent = NamespaceIdent::from_vec(vec!["Foo".to_string()]).unwrap();
        let child = NamespaceIdent::from_vec(vec!["Foo".to_string(), "Bar".to_string()]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &parent, None).await;
        initialize_namespace(state.clone(), warehouse_id, &child, None).await;

        let upper = NamespaceIdent::from_vec(vec!["FOO".to_string(), "BAR".to_string()]).unwrap();
        let lower = NamespaceIdent::from_vec(vec!["foo".to_string(), "bar".to_string()]).unwrap();

        let rows = get_namespaces_by_name(warehouse_id, &[&upper, &lower], &state.read_pool())
            .await
            .unwrap();

        let ids_for = |ident: &NamespaceIdent| {
            rows.iter()
                .filter(|r| r.namespace_ident() == ident)
                .map(|r| r.namespace_id())
                .collect::<Vec<_>>()
        };
        let upper_ids = ids_for(&upper);
        let lower_ids = ids_for(&lower);

        assert_eq!(
            upper_ids.len(),
            1,
            "no row came back for the spelling {upper:?}: {rows:#?}"
        );
        assert_eq!(
            lower_ids.len(),
            1,
            "no row came back for the spelling {lower:?}: {rows:#?}"
        );
        assert_eq!(
            upper_ids, lower_ids,
            "two spellings of one namespace resolved to different ids"
        );
    }

    #[sqlx::test]
    async fn test_batch_ident_lookup_applies_case(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        let ns_a = NamespaceIdent::from_vec(vec!["Alpha".to_string()]).unwrap();
        let ns_b = NamespaceIdent::from_vec(vec!["Beta".to_string()]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &ns_a, None).await;
        initialize_namespace(state.clone(), warehouse_id, &ns_b, None).await;

        // Pre-populate cache for ns_a via a single lookup.
        let _ = PostgresBackend::get_namespace_cache_aware(
            warehouse_id,
            &ns_a,
            CachePolicy::Use,
            state.clone(),
        )
        .await
        .unwrap();

        // Now batch-lookup with a DIFFERENT case — ns_a will cache-hit (and needs
        // the substitution callback), ns_b will cache-miss and go to DB.
        let upper_a = NamespaceIdent::from_vec(vec!["ALPHA".to_string()]).unwrap();
        let upper_b = NamespaceIdent::from_vec(vec!["BETA".to_string()]).unwrap();
        let results = PostgresBackend::get_namespaces_by_ident(
            warehouse_id,
            &[&upper_a, &upper_b],
            state.clone(),
        )
        .await
        .unwrap();

        // Both entries should carry the uppercase case the caller requested.
        let uppercase_idents: std::collections::HashSet<_> = results
            .values()
            .map(|ns| ns.namespace_ident().clone())
            .collect();
        assert!(
            uppercase_idents.contains(&upper_a),
            "ALPHA should be in results (cache-hit path), got: {uppercase_idents:?}"
        );
        assert!(
            uppercase_idents.contains(&upper_b),
            "BETA should be in results (DB path), got: {uppercase_idents:?}"
        );
    }

    /// After populating the cache with a specific case, subsequent lookups with
    /// the *same* case hit without a DB call. Different-case lookups miss the
    /// ident-cache, go to DB, and populate a separate ident-cache entry.
    #[sqlx::test]
    async fn test_canonical_lookup_hits_cache_after_creation(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        let canonical = NamespaceIdent::from_vec(vec!["DualKey".to_string()]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &canonical, None).await;

        // First lookup populates the cache with the canonical case
        // (both as ident cache key AND the id cache data).
        let first = PostgresBackend::get_namespace_cache_aware(
            warehouse_id,
            &canonical,
            CachePolicy::Use,
            state.clone(),
        )
        .await
        .unwrap()
        .expect("found");
        assert_eq!(first.namespace_ident(), &canonical);

        // Second lookup with SAME case: cache hit path, same result.
        let second = PostgresBackend::get_namespace_cache_aware(
            warehouse_id,
            &canonical,
            CachePolicy::Use,
            state.clone(),
        )
        .await
        .unwrap()
        .expect("found");
        assert_eq!(second.namespace_ident(), &canonical);

        // Third lookup with different case: response still correct (caller's case),
        // but this path went through DB first time → cache now has both entries.
        let other = NamespaceIdent::from_vec(vec!["dualkey".to_string()]).unwrap();
        let third = PostgresBackend::get_namespace_cache_aware(
            warehouse_id,
            &other,
            CachePolicy::Use,
            state.clone(),
        )
        .await
        .unwrap()
        .expect("found");
        assert_eq!(third.namespace_ident(), &other);
    }

    /// The transaction path of `get_namespace` must NOT warm the shared
    /// `NAMESPACE_CACHE`. A transaction can observe its own uncommitted writes, so
    /// publishing what it reads into the cross-request cache could expose state
    /// that later rolls back. Cache warming is reserved for the pooled-`State`
    /// path, which only ever reads committed data. The contrast assertion (State
    /// path *does* warm) keeps this honest: it proves the cache is enabled, so the
    /// "not warmed" assertion cannot pass vacuously.
    #[sqlx::test]
    async fn get_namespace_transaction_path_does_not_warm_shared_cache(pool: sqlx::PgPool) {
        use lakekeeper::service::namespace_cache::NAMESPACE_CACHE;

        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        let namespace = NamespaceIdent::from_vec(vec!["txn_no_warm".to_string()]).unwrap();
        let ns = initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;
        let namespace_id = ns.namespace_id();

        // Clean slate: creation may have warmed the cache for this id.
        NAMESPACE_CACHE.invalidate(&namespace_id).await;
        assert!(
            NAMESPACE_CACHE.get(&namespace_id).await.is_none(),
            "precondition: namespace must not be cached"
        );

        // Transaction path: read through an active (read) transaction.
        let mut txn = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();
        let found = PostgresBackend::get_namespace(warehouse_id, namespace_id, txn.transaction())
            .await
            .unwrap();
        assert!(found.is_some(), "the committed namespace must be returned");
        txn.rollback().await.unwrap();

        assert!(
            NAMESPACE_CACHE.get(&namespace_id).await.is_none(),
            "transaction-path get_namespace must NOT warm the shared NAMESPACE_CACHE"
        );

        // Contrast: the pooled-`State` path DOES warm the cache. This proves the
        // cache is enabled, so the assertion above is meaningful (not vacuous).
        let found = PostgresBackend::get_namespace(warehouse_id, namespace_id, state.clone())
            .await
            .unwrap();
        assert!(found.is_some());
        assert!(
            NAMESPACE_CACHE.get(&namespace_id).await.is_some(),
            "State-path get_namespace must warm the shared NAMESPACE_CACHE"
        );
    }

    // ------------------------ create_namespace prefix casing ------------------------

    /// The parent is matched under the case-insensitive collation, so a caller may spell ancestor
    /// segments differently from how they are stored. What lands in the table must use the parent's
    /// stored spelling: the namespace cache compares a child's path minus its leaf against its
    /// parent's ident byte-wise, and a mismatch makes every later lookup of that row — and of its
    /// whole subtree — miss the cache, reload the same bytes and fail identically.
    #[sqlx::test]
    async fn test_create_namespace_canonicalises_parent_casing(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        // The issue's own repro: parent stored `a/B`, child created as `a/b/c`.
        initialize_namespace(state.clone(), warehouse_id, &ident(&["a"]), None).await;
        initialize_namespace(state.clone(), warehouse_id, &ident(&["a", "B"]), None).await;
        let child =
            initialize_namespace(state.clone(), warehouse_id, &ident(&["a", "b", "c"]), None).await;

        assert_eq!(
            stored_path(&pool, child.namespace_id()).await,
            vec!["a".to_string(), "B".to_string(), "c".to_string()],
            "the ancestor segments must be stored with the parent's casing, not the caller's"
        );
        assert_eq!(
            child.canonical_ident(),
            &ident(&["a", "B", "c"]),
            "the canonical ident is what was stored"
        );
        assert_eq!(
            child.namespace_ident(),
            &ident(&["a", "B", "c"]),
            "create reports the stored path, not the requested one — no requested-case overlay, \
             matching `move_namespace`"
        );
        assert_eq!(
            child.parent_namespaces_id(),
            Some(
                get_namespace_ident(&pool, warehouse_id, &ident(&["a", "B"]))
                    .await
                    .into()
            ),
            "the parent id must still resolve to the stored parent row"
        );

        // The other direction: the caller upper-cases where the parent is stored lower-case.
        initialize_namespace(state.clone(), warehouse_id, &ident(&["d"]), None).await;
        initialize_namespace(state.clone(), warehouse_id, &ident(&["d", "e"]), None).await;
        let reverse =
            initialize_namespace(state.clone(), warehouse_id, &ident(&["d", "E", "f"]), None).await;
        assert_eq!(
            stored_path(&pool, reverse.namespace_id()).await,
            vec!["d".to_string(), "e".to_string(), "f".to_string()],
            "canonicalisation is not direction-specific"
        );

        // The leaf is the name the caller owns, so its casing is kept verbatim.
        let leaf = initialize_namespace(
            state.clone(),
            warehouse_id,
            &ident(&["a", "b", "CeE"]),
            None,
        )
        .await;
        assert_eq!(
            stored_path(&pool, leaf.namespace_id()).await,
            vec!["a".to_string(), "B".to_string(), "CeE".to_string()],
            "only the ancestors are rewritten; the leaf keeps the caller's casing"
        );
    }

    /// Discriminates against a fix that only canonicalises the immediate parent segment — which a
    /// depth-2 test would pass. Every ancestor segment comes from the parent row, and the parent
    /// row's own prefix is canonical by induction.
    #[sqlx::test]
    async fn test_create_namespace_canonicalises_every_ancestor_segment(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        initialize_namespace(state.clone(), warehouse_id, &ident(&["x"]), None).await;
        initialize_namespace(state.clone(), warehouse_id, &ident(&["x", "Y"]), None).await;
        initialize_namespace(state.clone(), warehouse_id, &ident(&["x", "Y", "Z"]), None).await;

        // Every ancestor segment is spelled differently from how it is stored.
        let deep = initialize_namespace(
            state.clone(),
            warehouse_id,
            &ident(&["X", "y", "z", "leaf"]),
            None,
        )
        .await;

        assert_eq!(
            stored_path(&pool, deep.namespace_id()).await,
            vec![
                "x".to_string(),
                "Y".to_string(),
                "Z".to_string(),
                "leaf".to_string()
            ],
            "all ancestor segments must come from the parent row, not just the last one"
        );
    }

    /// The `None` arm: at the warehouse root there is no parent row to take a spelling from, so the
    /// caller's casing is the stored casing. Guards against over-canonicalisation.
    #[sqlx::test]
    async fn test_create_namespace_at_warehouse_root_keeps_callers_casing(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        let root =
            initialize_namespace(state.clone(), warehouse_id, &ident(&["RootCase"]), None).await;

        assert_eq!(
            stored_path(&pool, root.namespace_id()).await,
            vec!["RootCase".to_string()]
        );
        assert_eq!(root.canonical_ident(), &ident(&["RootCase"]));
    }

    /// Canonicalising the prefix must not change collision behaviour: the unique index is over the
    /// whole array under the case-insensitive collation, so a case-variant path still collides.
    #[sqlx::test]
    async fn test_create_namespace_rejects_case_variant_prefix_collision(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        initialize_namespace(state.clone(), warehouse_id, &ident(&["a"]), None).await;
        initialize_namespace(state.clone(), warehouse_id, &ident(&["a", "B"]), None).await;
        initialize_namespace(state.clone(), warehouse_id, &ident(&["a", "B", "C"]), None).await;

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let err = PostgresBackend::create_namespace(
            warehouse_id,
            NamespaceId::new_random(),
            CreateNamespaceRequest {
                namespace: ident(&["a", "b", "c"]),
                properties: None,
            },
            transaction.transaction(),
        )
        .await
        .expect_err("a case-variant of an existing path must still collide");
        assert!(
            matches!(err, CatalogCreateNamespaceError::NamespaceAlreadyExists(_)),
            "unexpected error: {err:?}"
        );
    }

    /// The write-side fix stops new drift; it cannot repair rows already stored. This pins the
    /// repair migration, which is the only remedy for them: `move_namespace` refuses any namespace
    /// with descendants, so a drifted row with children cannot be fixed through the API at all.
    ///
    /// Runs the migration's own statement via `include_str!` so the SQL stays single-sourced.
    /// `#[sqlx::test]` applies migrations before the body, so the drift has to be created after —
    /// which also means this exercises the re-run path, and the statement is idempotent.
    #[sqlx::test]
    async fn test_repair_namespace_path_casing_canonicalises_existing_rows(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        // Insert the drift directly: `create_namespace` can no longer produce it. The depth-5 row
        // pins the loop bound — it is derived from the deepest row present, not a constant, so a
        // chain deeper than expected is still repaired.
        let ids: Vec<NamespaceId> = (0..6).map(|_| NamespaceId::new_random()).collect();
        for (id, path) in ids.iter().zip([
            vec!["a"],
            vec!["a", "B"],
            vec!["a", "b", "c"],
            vec!["a", "b", "c", "D"],
            vec!["a", "b", "c", "d", "E"],
            // No depth-1 ancestor, so the loop never reaches it and it must be left alone.
            vec!["orphan", "x"],
        ]) {
            let path: Vec<String> = path.iter().map(ToString::to_string).collect();
            sqlx::query!(
                r#"
                INSERT INTO namespace (warehouse_id, namespace_id, namespace_name, namespace_properties)
                VALUES ($1, $2, $3, '{}'::jsonb)
                "#,
                *warehouse_id,
                **id,
                &path[..],
            )
            .execute(&pool)
            .await
            .unwrap();
        }

        let repaired = {
            let mut t = PostgresTransaction::begin_write(state.clone())
                .await
                .unwrap();
            let n = repair_namespace_path_casing(t.transaction()).await.unwrap();
            t.commit().await.unwrap();
            n
        };
        assert_eq!(
            repaired, 3,
            "the three drifted rows below depth 1 are rewritten"
        );

        assert_eq!(stored_path(&pool, ids[0]).await, vec!["a".to_string()]);
        assert_eq!(
            stored_path(&pool, ids[1]).await,
            vec!["a".to_string(), "B".to_string()]
        );
        assert_eq!(
            stored_path(&pool, ids[2]).await,
            vec!["a".to_string(), "B".to_string(), "c".to_string()],
            "the depth-3 row must adopt its parent's stored casing"
        );
        assert_eq!(
            stored_path(&pool, ids[3]).await,
            vec![
                "a".to_string(),
                "B".to_string(),
                "c".to_string(),
                "D".to_string()
            ],
            "and the depth-4 row must follow the repaired depth-3 prefix — a single-pass UPDATE \
             would leave this one broken, which is why the statement recurses"
        );
        assert_eq!(
            stored_path(&pool, ids[4]).await,
            vec![
                "a".to_string(),
                "B".to_string(),
                "c".to_string(),
                "D".to_string(),
                "E".to_string()
            ],
            "the depth-5 row proves the loop runs to the deepest row present, not to a constant"
        );
        assert_eq!(
            stored_path(&pool, ids[5]).await,
            vec!["orphan".to_string(), "x".to_string()],
            "a row with no depth-1 ancestor is left untouched"
        );

        // A case-only rewrite does not fire `set_updated_at_and_increment_version`, so replicas are
        // not forced to re-read rows that could never have been cached anyway.
        let (version, updated_at): (i64, Option<chrono::DateTime<chrono::Utc>>) =
            sqlx::query_as("SELECT version, updated_at FROM namespace WHERE namespace_id = $1")
                .bind(*ids[2])
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(version, 0, "a case-only repair must not bump the version");
        assert!(updated_at.is_none(), "nor stamp updated_at");

        // Idempotent: `--force-idempotent-post-migration-hooks` re-runs this to recover from a
        // failure, and re-pinning it re-runs it for a new hole, so a second pass must change nothing.
        let repaired_again = {
            let mut t = PostgresTransaction::begin_write(state.clone())
                .await
                .unwrap();
            let n = repair_namespace_path_casing(t.transaction()).await.unwrap();
            t.commit().await.unwrap();
            n
        };
        assert_eq!(repaired_again, 0, "a second pass must rewrite nothing");
        assert_eq!(
            stored_path(&pool, ids[4]).await,
            vec![
                "a".to_string(),
                "B".to_string(),
                "c".to_string(),
                "D".to_string(),
                "E".to_string()
            ],
            "re-running the hook must be a no-op"
        );
    }

    // ------------------------------ move_namespace ------------------------------

    fn ident(parts: &[&str]) -> NamespaceIdent {
        NamespaceIdent::from_vec(parts.iter().map(ToString::to_string).collect()).unwrap()
    }

    /// Run `move_namespace` in its own committed transaction.
    async fn move_ns(
        state: &CatalogState,
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        destination: &NamespaceIdent,
        force: bool,
    ) -> std::result::Result<MovedNamespace, CatalogMoveNamespaceError> {
        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let result = move_namespace(
            warehouse_id,
            namespace_id,
            destination,
            force,
            transaction.transaction(),
        )
        .await;
        if result.is_ok() {
            transaction.commit().await.unwrap();
        }
        result
    }

    /// Read a namespace's canonical stored path straight from the table, bypassing
    /// every cache — the assertions must observe the DB, not a memoised view.
    async fn stored_path(pool: &sqlx::PgPool, namespace_id: NamespaceId) -> Vec<String> {
        sqlx::query_scalar!(
            r#"SELECT namespace_name FROM namespace WHERE namespace_id = $1"#,
            *namespace_id
        )
        .fetch_one(pool)
        .await
        .unwrap()
    }

    #[sqlx::test]
    async fn test_move_namespace_to_warehouse_root(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        initialize_namespace(state.clone(), warehouse_id, &ident(&["parent"]), None).await;
        let child = initialize_namespace(
            state.clone(),
            warehouse_id,
            &ident(&["parent", "child"]),
            None,
        )
        .await;

        let moved = move_ns(
            &state,
            warehouse_id,
            child.namespace_id(),
            &ident(&["child"]),
            false,
        )
        .await
        .unwrap();

        assert_eq!(moved.namespace.canonical_ident(), &ident(&["child"]));
        assert_eq!(
            moved.namespace.parent, None,
            "moving to root clears the parent"
        );
        assert_eq!(
            stored_path(&pool, child.namespace_id()).await,
            vec!["child".to_string()]
        );
    }

    #[sqlx::test]
    async fn test_move_namespace_under_new_parent(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        let old_parent =
            initialize_namespace(state.clone(), warehouse_id, &ident(&["old"]), None).await;
        let new_parent =
            initialize_namespace(state.clone(), warehouse_id, &ident(&["new"]), None).await;
        let child =
            initialize_namespace(state.clone(), warehouse_id, &ident(&["old", "child"]), None)
                .await;
        assert_eq!(
            child.parent_namespaces_id(),
            Some(old_parent.namespace_id())
        );

        let moved = move_ns(
            &state,
            warehouse_id,
            child.namespace_id(),
            &ident(&["new", "child"]),
            false,
        )
        .await
        .unwrap();

        assert_eq!(moved.namespace.canonical_ident(), &ident(&["new", "child"]));
        assert_eq!(
            moved.namespace.parent_namespaces_id(),
            Some(new_parent.namespace_id()),
            "the returned parent must be the destination parent"
        );

        // The pre-move identity drives cache eviction and the authorizer's tuple deletion.
        assert_eq!(moved.previous_ident, ident(&["old", "child"]));
        assert_eq!(moved.previous_parent, Some(old_parent.namespace_id()));
        assert!(!moved.is_noop());
        assert!(moved.changed_parent());
    }

    #[sqlx::test]
    async fn test_move_namespace_renames_in_place(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        let parent =
            initialize_namespace(state.clone(), warehouse_id, &ident(&["parent"]), None).await;
        let child = initialize_namespace(
            state.clone(),
            warehouse_id,
            &ident(&["parent", "before"]),
            None,
        )
        .await;

        let moved = move_ns(
            &state,
            warehouse_id,
            child.namespace_id(),
            &ident(&["parent", "after"]),
            false,
        )
        .await
        .unwrap();

        assert_eq!(
            moved.namespace.canonical_ident(),
            &ident(&["parent", "after"])
        );
        assert_eq!(
            moved.namespace.parent_namespaces_id(),
            Some(parent.namespace_id()),
            "a rename keeps the parent"
        );
        assert_eq!(moved.previous_ident, ident(&["parent", "before"]));
        assert!(
            !moved.changed_parent(),
            "a rename must not report a parent change — the authorizer would rewrite tuples for nothing"
        );
        assert!(
            *moved.namespace.version() > *child.version(),
            "renaming must bump the version (got {} -> {})",
            *child.version(),
            *moved.namespace.version()
        );
    }

    #[sqlx::test]
    async fn test_move_namespace_case_only_rename(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        let ns = initialize_namespace(state.clone(), warehouse_id, &ident(&["casing"]), None).await;

        // The unique index collates case-insensitively, so this must NOT be reported as a
        // collision with the row being renamed.
        let moved = move_ns(
            &state,
            warehouse_id,
            ns.namespace_id(),
            &ident(&["CaSiNg"]),
            false,
        )
        .await
        .unwrap();

        assert_eq!(moved.namespace.canonical_ident(), &ident(&["CaSiNg"]));
        assert_eq!(
            stored_path(&pool, ns.namespace_id()).await,
            vec!["CaSiNg".to_string()],
            "the stored path must carry the new casing"
        );
        // The `set_updated_at_and_increment_version` trigger compares `namespace_name` under
        // the case-insensitive collation, so a case-only rename does not fire it. The UPDATE
        // sets both columns explicitly; without that, caches and the authz version fence would
        // never learn the path changed.
        assert!(
            *moved.namespace.version() > *ns.version(),
            "a case-only rename is a real rename and must bump the version"
        );
        assert!(
            moved.namespace.updated_at().is_some(),
            "a case-only rename must stamp updated_at"
        );
        assert!(!moved.is_noop());
    }

    /// The destination parent is matched case-insensitively, so a caller may spell ancestor
    /// segments differently from how they are stored. What lands in the table must use the
    /// parent's stored spelling: the namespace cache's `is_parent_ident` compares a child's
    /// path minus its leaf against the parent's ident byte-wise, and a mismatch turns every
    /// later by-id lookup of that namespace into a cache miss plus an eviction.
    #[sqlx::test]
    async fn test_move_namespace_canonicalises_parent_casing(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        initialize_namespace(state.clone(), warehouse_id, &ident(&["parent"]), None).await;
        let movable =
            initialize_namespace(state.clone(), warehouse_id, &ident(&["movable"]), None).await;

        // Caller spells the parent "PARENT"; the stored row is "parent".
        let moved = move_ns(
            &state,
            warehouse_id,
            movable.namespace_id(),
            &ident(&["PARENT", "child"]),
            false,
        )
        .await
        .unwrap();

        assert_eq!(
            stored_path(&pool, movable.namespace_id()).await,
            vec!["parent".to_string(), "child".to_string()],
            "the ancestor segments must be stored with the parent's casing, not the caller's"
        );
        assert_eq!(
            moved.namespace.canonical_ident(),
            &ident(&["parent", "child"]),
            "the returned ident must match what was stored"
        );
        // The leaf keeps the caller's casing — that is what a rename is for.
        let moved = move_ns(
            &state,
            warehouse_id,
            movable.namespace_id(),
            &ident(&["parent", "ChIlD"]),
            false,
        )
        .await
        .unwrap();
        assert_eq!(
            stored_path(&pool, movable.namespace_id()).await,
            vec!["parent".to_string(), "ChIlD".to_string()],
            "the leaf must keep the caller's casing"
        );
        assert_eq!(
            moved.namespace.parent_namespaces_id(),
            Some(
                get_namespace_ident(&pool, warehouse_id, &ident(&["parent"]))
                    .await
                    .into()
            ),
            "the parent id must still resolve"
        );
    }

    /// A destination that differs from the current path only in the casing of *ancestor*
    /// segments names the path the row already has: the parent is matched case-insensitively, so
    /// the caller's spelling of it carries no information. It must therefore be a no-op, not a
    /// write — a redundant UPDATE would bump `version` and `updated_at` while `is_noop()` still
    /// reported true, so the event that tells other replicas about the bump is suppressed.
    ///
    /// Contrast `test_move_namespace_case_only_rename`: a case-only difference in the *leaf* is a
    /// real rename, because the leaf is the name itself.
    #[sqlx::test]
    async fn test_move_namespace_ancestor_case_only_destination_is_noop(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        initialize_namespace(state.clone(), warehouse_id, &ident(&["parent"]), None).await;
        let child = initialize_namespace(
            state.clone(),
            warehouse_id,
            &ident(&["parent", "child"]),
            None,
        )
        .await;

        let moved = move_ns(
            &state,
            warehouse_id,
            child.namespace_id(),
            &ident(&["PARENT", "child"]),
            false,
        )
        .await
        .unwrap();

        assert_eq!(
            stored_path(&pool, child.namespace_id()).await,
            vec!["parent".to_string(), "child".to_string()],
            "the stored path must be untouched"
        );
        assert_eq!(
            *moved.namespace.version(),
            *child.version(),
            "an ancestor-case-only destination must not bump the version"
        );
        assert_eq!(
            moved.namespace.updated_at(),
            child.updated_at(),
            "nor stamp updated_at"
        );
        assert!(
            moved.is_noop(),
            "callers rely on is_noop() to skip event emission"
        );
        assert!(!moved.changed_parent());
    }

    /// Guards the approximation in `is_same_namespace_path_ignoring_ascii_case` against the *live* collation.
    ///
    /// That helper decides whether a move re-parents a namespace, using ASCII case folding as a stand-in for
    /// how Postgres compares `namespace_name`. The two need not agree in both directions. When the
    /// database considers two paths equal and `UniCase` does not, a move is refused that could have
    /// been allowed — annoying, safe. The reverse is the dangerous one: if `UniCase` says "same"
    /// where the database says "different", the paths name *different* parents, and the
    /// storage-layout guard would wave through a genuine re-parent and desync locations.
    ///
    /// So the invariant is one-directional — unicase-equal must imply collation-equal. A migration
    /// that made the collation stricter, case-sensitive being the obvious way, breaks it here
    /// rather than silently weakening the guard.
    ///
    /// The probe table copies its column type *and collation* from the real `namespace` table with
    /// `CREATE TABLE AS ... LIMIT 0`, so it tracks the migration instead of restating it. All
    /// queries run on one connection because a temp table belongs to its connection.
    #[sqlx::test]
    async fn test_namespace_path_comparison_is_no_looser_than_the_collation(pool: sqlx::PgPool) {
        let mut conn = pool.acquire().await.unwrap();

        sqlx::query(
            "CREATE TEMP TABLE collation_probe AS
             SELECT 0::int AS i, namespace_name AS a, namespace_name AS b
             FROM namespace LIMIT 0",
        )
        .execute(&mut *conn)
        .await
        .unwrap();

        // Straddling the boundary on purpose: plain case, non-ASCII case, accents, a ligature,
        // ignorable characters, sharp s, dotless i, prefix-similar names, differing depth.
        let pairs: Vec<(Vec<String>, Vec<String>)> = vec![
            (vec!["parent".into()], vec!["PARENT".into()]),
            (vec!["a".into(), "b".into()], vec!["A".into(), "B".into()]),
            (vec!["ärger".into()], vec!["ÄRGER".into()]),
            (vec!["türkçe".into()], vec!["TÜRKÇE".into()]),
            (vec!["straße".into()], vec!["STRASSE".into()]),
            (vec!["straße".into()], vec!["STRAßE".into()]),
            (vec!["resume".into()], vec!["résumé".into()]),
            (vec!["ﬁle".into()], vec!["file".into()]),
            (vec!["a\u{200d}b".into()], vec!["ab".into()]),
            (vec!["sun\u{ad}day".into()], vec!["sunday".into()]),
            (vec!["ıstanbul".into()], vec!["Istanbul".into()]),
            (vec!["child_7".into()], vec!["child_7x".into()]),
            (vec!["a".into()], vec!["a".into(), "b".into()]),
        ];

        for (i, (left, right)) in pairs.iter().enumerate() {
            sqlx::query("INSERT INTO collation_probe (i, a, b) VALUES ($1, $2, $3)")
                .bind(i32::try_from(i).unwrap())
                .bind(left)
                .bind(right)
                .execute(&mut *conn)
                .await
                .unwrap();
        }

        let collation: Vec<(i32, bool)> =
            sqlx::query_as("SELECT i, (a = b) FROM collation_probe ORDER BY i")
                .fetch_all(&mut *conn)
                .await
                .unwrap();
        assert_eq!(collation.len(), pairs.len(), "every pair must be probed");

        let mut looser = Vec::new();
        for (i, collation_same) in collation {
            let (left, right) = &pairs[usize::try_from(i).unwrap()];
            if is_same_namespace_path_ignoring_ascii_case(left, right) && !collation_same {
                looser.push(format!("{left:?} vs {right:?}"));
            }
        }

        assert!(
            looser.is_empty(),
            "is_same_namespace_path_ignoring_ascii_case is looser than the database collation for these \
             paths, so the storage-layout guard could miss a real re-parent:\n  {}",
            looser.join("\n  ")
        );
    }

    /// Helper: the id of a namespace by its exact stored path.
    async fn get_namespace_ident(
        pool: &sqlx::PgPool,
        warehouse_id: WarehouseId,
        ns: &NamespaceIdent,
    ) -> uuid::Uuid {
        sqlx::query_scalar!(
            r#"SELECT namespace_id FROM namespace WHERE warehouse_id = $1 AND namespace_name = $2"#,
            *warehouse_id,
            &ns.as_ref()[..],
        )
        .fetch_one(pool)
        .await
        .unwrap()
    }

    #[sqlx::test]
    async fn test_move_namespace_has_children_probe_adversarial_names(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        // The guard answers "has descendants" from the first row after the source in the
        // unique index. These names all sort immediately around `child_7` without being its
        // descendants, so a bound that is even slightly too wide reports children that do not
        // exist — and a case-variant descendant must still be caught.
        let seven =
            initialize_namespace(state.clone(), warehouse_id, &ident(&["child_7"]), None).await;
        for path in [vec!["child_7x"], vec!["child_7.a"], vec!["child_8"]] {
            initialize_namespace(state.clone(), warehouse_id, &ident(&path), None).await;
        }

        move_ns(
            &state,
            warehouse_id,
            seven.namespace_id(),
            &ident(&["moved_7"]),
            false,
        )
        .await
        .expect("child_7x, child_7.a and child_8 are siblings, not descendants of child_7");

        // A descendant differing only in case must still be caught: the index collates
        // case-insensitively, so it sorts inside the descendant range.
        let cased =
            initialize_namespace(state.clone(), warehouse_id, &ident(&["Parent"]), None).await;
        initialize_namespace(
            state.clone(),
            warehouse_id,
            &ident(&["Parent", "kid"]),
            None,
        )
        .await;
        let err = move_ns(
            &state,
            warehouse_id,
            cased.namespace_id(),
            &ident(&["Elsewhere"]),
            false,
        )
        .await
        .expect_err("a descendant must block the move regardless of casing");
        assert!(
            matches!(err, CatalogMoveNamespaceError::NamespaceHasChildren(_)),
            "unexpected error: {err:?}"
        );

        // A leaf element above every sentinel candidate must not escape the range.
        let high = initialize_namespace(state.clone(), warehouse_id, &ident(&["high"]), None).await;
        initialize_namespace(
            state.clone(),
            warehouse_id,
            &ident(&["high", "\u{ffff}"]),
            None,
        )
        .await;
        let err = move_ns(
            &state,
            warehouse_id,
            high.namespace_id(),
            &ident(&["high_moved"]),
            false,
        )
        .await
        .expect_err("a U+FFFF descendant must block the move");
        assert!(
            matches!(err, CatalogMoveNamespaceError::NamespaceHasChildren(_)),
            "unexpected error: {err:?}"
        );
    }

    #[sqlx::test]
    async fn test_move_namespace_to_current_path_is_noop(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        initialize_namespace(state.clone(), warehouse_id, &ident(&["parent"]), None).await;
        let child = initialize_namespace(
            state.clone(),
            warehouse_id,
            &ident(&["parent", "child"]),
            None,
        )
        .await;

        // Retrying a completed move must succeed rather than collide with itself.
        let moved = move_ns(
            &state,
            warehouse_id,
            child.namespace_id(),
            &ident(&["parent", "child"]),
            false,
        )
        .await
        .unwrap();

        assert_eq!(
            moved.namespace.canonical_ident(),
            &ident(&["parent", "child"])
        );
        assert_eq!(
            *moved.namespace.version(),
            *child.version(),
            "a no-op must not bump the version"
        );
        assert!(
            moved.is_noop(),
            "callers rely on is_noop() to skip event emission and tuple rewrites"
        );
        assert!(!moved.changed_parent());
    }

    #[sqlx::test]
    async fn test_move_namespace_noop_succeeds_even_with_children(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        let parent =
            initialize_namespace(state.clone(), warehouse_id, &ident(&["parent"]), None).await;
        initialize_namespace(
            state.clone(),
            warehouse_id,
            &ident(&["parent", "child"]),
            None,
        )
        .await;

        // The no-op is answered before the has-children guard: nothing moves, so the
        // guard is irrelevant and a retry must still succeed.
        move_ns(
            &state,
            warehouse_id,
            parent.namespace_id(),
            &ident(&["parent"]),
            false,
        )
        .await
        .unwrap();
    }

    #[sqlx::test]
    async fn test_move_namespace_rejects_existing_destination(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        let source = initialize_namespace(state.clone(), warehouse_id, &ident(&["a"]), None).await;
        initialize_namespace(state.clone(), warehouse_id, &ident(&["b"]), None).await;

        let err = move_ns(
            &state,
            warehouse_id,
            source.namespace_id(),
            &ident(&["b"]),
            false,
        )
        .await
        .unwrap_err();

        assert!(
            matches!(err, CatalogMoveNamespaceError::NamespaceAlreadyExists(_)),
            "expected NamespaceAlreadyExists, got {err:?}"
        );
        assert_eq!(
            stored_path(&pool, source.namespace_id()).await,
            vec!["a".to_string()],
            "a rejected move must not have changed the source"
        );
    }

    #[sqlx::test]
    async fn test_move_namespace_rejects_existing_destination_different_case(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        let source = initialize_namespace(state.clone(), warehouse_id, &ident(&["a"]), None).await;
        initialize_namespace(state.clone(), warehouse_id, &ident(&["taken"]), None).await;

        // Distinct rows collate equal, so this is a genuine collision.
        let err = move_ns(
            &state,
            warehouse_id,
            source.namespace_id(),
            &ident(&["TAKEN"]),
            false,
        )
        .await
        .unwrap_err();

        assert!(
            matches!(err, CatalogMoveNamespaceError::NamespaceAlreadyExists(_)),
            "expected NamespaceAlreadyExists, got {err:?}"
        );
    }

    #[sqlx::test]
    async fn test_move_namespace_rejects_namespace_with_children(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        let parent =
            initialize_namespace(state.clone(), warehouse_id, &ident(&["parent"]), None).await;
        initialize_namespace(
            state.clone(),
            warehouse_id,
            &ident(&["parent", "child"]),
            None,
        )
        .await;

        let err = move_ns(
            &state,
            warehouse_id,
            parent.namespace_id(),
            &ident(&["moved"]),
            false,
        )
        .await
        .unwrap_err();

        assert!(
            matches!(err, CatalogMoveNamespaceError::NamespaceHasChildren(_)),
            "expected NamespaceHasChildren, got {err:?}"
        );
    }

    #[sqlx::test]
    async fn test_move_namespace_rejects_namespace_with_grandchildren_only(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        let root = initialize_namespace(state.clone(), warehouse_id, &ident(&["root"]), None).await;
        let mid =
            initialize_namespace(state.clone(), warehouse_id, &ident(&["root", "mid"]), None).await;
        initialize_namespace(
            state.clone(),
            warehouse_id,
            &ident(&["root", "mid", "leaf"]),
            None,
        )
        .await;

        // Remove the intermediate namespace at the DB level. The public API does not produce
        // this state on purpose — a non-recursive `drop_namespace` refuses while a child
        // exists — though its unlocked emptiness guard can still race a concurrent move into
        // it. This is exactly why the guard tests for *any* descendant rather than direct
        // children only: with a direct-children-only predicate, `root` would move here and
        // silently strand `root.mid.leaf` under a path that no longer resolves.
        sqlx::query!(
            r#"DELETE FROM namespace WHERE namespace_id = $1"#,
            *mid.namespace_id()
        )
        .execute(&pool)
        .await
        .unwrap();

        let err = move_ns(
            &state,
            warehouse_id,
            root.namespace_id(),
            &ident(&["moved"]),
            false,
        )
        .await
        .unwrap_err();

        assert!(
            matches!(err, CatalogMoveNamespaceError::NamespaceHasChildren(_)),
            "expected NamespaceHasChildren for a grandchild-only descendant, got {err:?}"
        );
    }

    #[sqlx::test]
    async fn test_move_namespace_rejects_protected_without_force(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        let ns = initialize_namespace(state.clone(), warehouse_id, &ident(&["prot"]), None).await;

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        PostgresBackend::set_namespace_protected(
            warehouse_id,
            ns.namespace_id(),
            true,
            transaction.transaction(),
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        let err = move_ns(
            &state,
            warehouse_id,
            ns.namespace_id(),
            &ident(&["moved"]),
            false,
        )
        .await
        .unwrap_err();

        assert!(
            matches!(err, CatalogMoveNamespaceError::NamespaceProtected(_)),
            "expected NamespaceProtected, got {err:?}"
        );
    }

    #[sqlx::test]
    async fn test_move_namespace_force_moves_protected(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        let ns = initialize_namespace(state.clone(), warehouse_id, &ident(&["prot"]), None).await;

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        PostgresBackend::set_namespace_protected(
            warehouse_id,
            ns.namespace_id(),
            true,
            transaction.transaction(),
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        let moved = move_ns(
            &state,
            warehouse_id,
            ns.namespace_id(),
            &ident(&["moved"]),
            true,
        )
        .await
        .unwrap();

        assert_eq!(moved.namespace.canonical_ident(), &ident(&["moved"]));
        assert!(
            moved.namespace.is_protected(),
            "force moves the namespace but must not clear protection"
        );
    }

    #[sqlx::test]
    async fn test_move_namespace_rejects_missing_destination_parent(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        let ns = initialize_namespace(state.clone(), warehouse_id, &ident(&["a"]), None).await;

        let err = move_ns(
            &state,
            warehouse_id,
            ns.namespace_id(),
            &ident(&["nonexistent", "a"]),
            false,
        )
        .await
        .unwrap_err();

        assert!(
            matches!(err, CatalogMoveNamespaceError::NamespaceNotFound(_)),
            "expected NamespaceNotFound for the destination parent, got {err:?}"
        );
    }

    #[sqlx::test]
    async fn test_move_namespace_rejects_move_into_self(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        let ns = initialize_namespace(state.clone(), warehouse_id, &ident(&["a"]), None).await;

        let err = move_ns(
            &state,
            warehouse_id,
            ns.namespace_id(),
            &ident(&["a", "a"]),
            false,
        )
        .await
        .unwrap_err();

        assert!(
            matches!(
                err,
                CatalogMoveNamespaceError::NamespaceCannotMoveIntoSelf(_)
            ),
            "expected NamespaceCannotMoveIntoSelf, got {err:?}"
        );
    }

    #[sqlx::test]
    async fn test_move_namespace_unknown_namespace(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        let err = move_ns(
            &state,
            warehouse_id,
            NamespaceId::new_random(),
            &ident(&["a"]),
            false,
        )
        .await
        .unwrap_err();

        assert!(
            matches!(err, CatalogMoveNamespaceError::NamespaceNotFound(_)),
            "expected NamespaceNotFound, got {err:?}"
        );
    }

    #[sqlx::test]
    async fn test_move_namespace_cascades_tabular_namespace_name(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        initialize_namespace(state.clone(), warehouse_id, &ident(&["old"]), None).await;
        initialize_namespace(state.clone(), warehouse_id, &ident(&["new"]), None).await;
        let source =
            initialize_namespace(state.clone(), warehouse_id, &ident(&["old", "child"]), None)
                .await;

        let table = initialize_table(
            warehouse_id,
            state.clone(),
            false,
            Some(source.namespace_ident().clone()),
            None,
            Some("tbl".to_string()),
        )
        .await;

        move_ns(
            &state,
            warehouse_id,
            source.namespace_id(),
            &ident(&["new", "child"]),
            false,
        )
        .await
        .unwrap();

        // `tabular.tabular_namespace_name` is a denormalised copy of the namespace path
        // with an ON UPDATE CASCADE foreign key, so the single UPDATE above must have
        // repaired every contained tabular.
        let cascaded = sqlx::query_scalar!(
            r#"SELECT tabular_namespace_name FROM tabular WHERE tabular_id = $1"#,
            *table.table_id
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(
            cascaded,
            vec!["new".to_string(), "child".to_string()],
            "the contained table's denormalised namespace path must follow the move"
        );
    }
}
