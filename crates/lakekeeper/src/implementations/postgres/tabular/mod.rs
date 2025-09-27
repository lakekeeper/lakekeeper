pub mod table;
pub(crate) mod view;

use std::{
    collections::{HashMap, HashSet},
    default::Default,
    fmt::Debug,
};

use chrono::Utc;
use http::StatusCode;
use iceberg::ErrorKind;
use iceberg_ext::NamespaceIdent;
use lakekeeper_io::Location;
use sqlx::FromRow;
use uuid::Uuid;

use super::dbutils::DBErrorHandler as _;
use crate::{
    api::{
        iceberg::v1::{PaginatedMapping, PaginationQuery},
        management::v1::{
            tabular::{SearchTabular, SearchTabularResponse},
            ProtectionResponse,
        },
    },
    catalog::tables::CONCURRENT_UPDATE_ERROR_TYPE,
    implementations::postgres::pagination::{PaginateToken, V1PaginateToken},
    service::{
        storage::{join_location, split_location},
        task_queue::TaskId,
        DeletionDetails, ErrorModel, NamespaceId, Result, TableId, TableIdent, TabularId,
        TabularIdentBorrowed, TabularIdentOwned, TabularInfo, UndropTabularResponse,
    },
    WarehouseId, CONFIG,
};

#[derive(Debug, sqlx::Type, Copy, Clone, strum::Display)]
#[sqlx(type_name = "tabular_type", rename_all = "kebab-case")]
pub(crate) enum TabularType {
    Table,
    View,
}

pub(crate) async fn set_tabular_protected(
    warehouse_id: WarehouseId,
    tabular_id: TabularId,
    protected: bool,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<ProtectionResponse> {
    tracing::debug!(
        "Setting tabular protection for {} ({}) to {}",
        tabular_id,
        tabular_id.typ_str(),
        protected
    );
    let row = sqlx::query!(
        r#"
        UPDATE tabular
        SET protected = $3
        WHERE warehouse_id = $1 AND tabular_id = $2
        RETURNING protected, updated_at
        "#,
        *warehouse_id,
        *tabular_id,
        protected
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| {
        if let sqlx::Error::RowNotFound = e {
            ErrorModel::not_found(
                format!("{} not found", tabular_id.typ_str()),
                "NoSuchTabularError".to_string(),
                Some(Box::new(e)),
            )
        } else {
            tracing::warn!("Error setting tabular as protected: {}", e);
            e.into_error_model(format!(
                "Error setting {} as protected",
                tabular_id.typ_str()
            ))
        }
    })?;
    Ok(ProtectionResponse {
        protected: row.protected,
        updated_at: row.updated_at,
    })
}

pub(crate) async fn get_tabular_protected(
    warehouse_id: WarehouseId,
    tabular_id: TabularId,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<ProtectionResponse> {
    tracing::debug!(
        "Getting tabular protection status for {tabular_id} ({}) in {warehouse_id}",
        tabular_id.typ_str()
    );

    let row = sqlx::query!(
        r#"
        SELECT protected, updated_at
        FROM tabular
        WHERE warehouse_id = $1 AND tabular_id = $2
        "#,
        *warehouse_id,
        *tabular_id
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| {
        if let sqlx::Error::RowNotFound = e {
            ErrorModel::not_found(
                format!("{} not found", tabular_id.typ_str()),
                "NoSuchTabularError".to_string(),
                Some(Box::new(e)),
            )
        } else {
            tracing::warn!("Error getting tabular protection status: {}", e);
            e.into_error_model(format!(
                "Error getting protection status for {}",
                tabular_id.typ_str()
            ))
        }
    })?;

    Ok(ProtectionResponse {
        protected: row.protected,
        updated_at: row.updated_at,
    })
}

pub(crate) async fn tabular_ident_to_id<'a, 'e, 'c: 'e, E>(
    warehouse_id: WarehouseId,
    table: &TabularIdentBorrowed<'a>,
    list_flags: crate::service::ListFlags,
    transaction: E,
) -> Result<Option<(TabularId, String)>>
where
    E: 'e + sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let t = table.to_table_ident_tuple();
    let typ: TabularType = table.into();

    let rows = sqlx::query!(
        r#"
        SELECT t.tabular_id, t.typ as "typ: TabularType", fs_protocol, fs_location
        FROM tabular t
        INNER JOIN namespace n
            ON n.warehouse_id = $3 AND t.namespace_id = n.namespace_id
        INNER JOIN warehouse w ON w.warehouse_id = $3
        WHERE t.warehouse_id = $3 
        AND n.namespace_name = $1 
        AND t.name = $2
        AND w.status = 'active'
        AND t.typ = $4
        AND (t.deleted_at IS NULL OR $5)
        AND (t.metadata_location IS NOT NULL OR $6)
        "#,
        t.namespace.as_ref(),
        t.name,
        *warehouse_id,
        typ as _,
        list_flags.include_deleted,
        list_flags.include_staged
    )
    .fetch_one(transaction)
    .await
    .map(|r| {
        let location = join_location(&r.fs_protocol, &r.fs_location);
        Some(match r.typ {
            TabularType::Table => (TabularId::Table(r.tabular_id.into()), location),
            TabularType::View => (TabularId::View(r.tabular_id.into()), location),
        })
    });

    match rows {
        Err(e) => match e {
            sqlx::Error::RowNotFound => Ok(None),
            _ => Err(e
                .into_error_model(format!("Error fetching {}", table.typ_str()))
                .into()),
        },
        Ok(opt) => Ok(opt),
    }
}

#[derive(Debug, FromRow)]
struct TabularRow {
    tabular_id: Uuid,
    // Despite `IS NOT NULL` filter sqlx thinks column selected from input is nullable.
    namespace: Option<Vec<String>>,
    // Despite `IS NOT NULL` filter sqlx thinks column selected from input is nullable.
    tabular_name: Option<String>,
    // apparently this is needed, we need 'as "typ: TabularType"' in the query else the select won't
    // work, but that apparently aliases the whole column to "typ: TabularType"
    #[sqlx(rename = "typ: TabularType")]
    typ: TabularType,
}

/// The keys in the returned map correspond to the input identifiers in the `tables` parameter.
///
/// These may differ in case from identifiers stored in the db, since case insensitivity is achieved
/// by collation. For example:
///
/// - Table name in the db is `table1`
/// - The input parameter is `TABLE1`
/// - `table1` and `TABLE1` match due to collation and the key in the returned map is `TABLE1`
///
/// In line with that, querying both `table1` and `TABLE1` returns a map with two entries,
/// both mapping to the same table id.
#[allow(clippy::too_many_lines)]
pub(crate) async fn tabular_idents_to_ids<'e, 'c: 'e, E>(
    warehouse_id: WarehouseId,
    tables: HashSet<TabularIdentBorrowed<'_>>,
    list_flags: crate::service::ListFlags,
    catalog_state: E,
) -> Result<HashMap<TabularIdentOwned, Option<TabularId>>>
where
    E: 'e + sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    if tables.is_empty() {
        return Ok(HashMap::new());
    }
    let (ns_names, t_names, t_typs) = tables.iter().fold(
        (
            Vec::with_capacity(tables.len()),
            Vec::with_capacity(tables.len()),
            Vec::with_capacity(tables.len()),
        ),
        |(mut ns_names, mut t_names, mut t_typs), t| {
            let TableIdent { namespace, name } = t.to_table_ident_tuple();
            let typ: TabularType = t.into();
            ns_names.push(namespace.as_ref());
            t_names.push(name);
            t_typs.push(typ);
            (ns_names, t_names, t_typs)
        },
    );

    // Encoding `ns_names` as json is a workaround for `sqlx` not supporting `Vec<Vec<String>>`.
    let ns_names_json = serde_json::to_value(&ns_names).map_err(|e| {
        ErrorModel::internal(
            "Error json encoding namespace names",
            "EncodingError",
            Some(Box::new(e)),
        )
    })?;

    // For columns with collation, the query must return the value as in input `tables`.
    let rows = sqlx::query_as!(
        TabularRow,
        r#"
        SELECT t.tabular_id,
            in_ns.name as "namespace",
            in_t.name as tabular_name,
            t.typ as "typ: TabularType"
        FROM LATERAL (
            SELECT (
                SELECT array_agg(val ORDER BY ord)
                FROM jsonb_array_elements_text(x.name) WITH ORDINALITY AS e(val, ord)
            ) AS name, x.idx
            FROM jsonb_array_elements($2) WITH ORDINALITY AS x(name, idx)
        ) in_ns
        INNER JOIN LATERAL UNNEST($3::text[], $4::tabular_type[])
            WITH ORDINALITY AS in_t(name, typ, idx)
            ON in_ns.idx = in_t.idx
        INNER JOIN tabular t ON t.warehouse_id = $1 AND
            t.name = in_t.name AND t.typ = in_t.typ
        INNER JOIN namespace n ON n.warehouse_id = $1
            AND t.namespace_id = n.namespace_id AND n.namespace_name = in_ns.name
        INNER JOIN warehouse w ON w.warehouse_id = $1
        WHERE in_t.name IS NOT NULL AND in_ns.name IS NOT NULL
            AND w.status = 'active'
            AND (t.deleted_at is NULL OR $5)
            AND (t.metadata_location is not NULL OR $6) "#,
        *warehouse_id,
        ns_names_json as _,
        t_names.as_slice() as _,
        t_typs.as_slice() as _,
        list_flags.include_deleted,
        list_flags.include_staged
    )
    .fetch_all(catalog_state)
    .await
    .map_err(|e| e.into_error_model("Error fetching tables or views".to_string()))?;

    let mut table_map = HashMap::with_capacity(tables.len());
    for TabularRow {
        tabular_id,
        namespace: in_namespace,
        tabular_name: in_tabular_name,
        typ,
    } in rows
    {
        let namespace = in_namespace.ok_or_else(|| {
            ErrorModel::internal(
                "Namespace name should not be null",
                "InternalDatabaseError",
                None,
            )
        })?;
        let name = in_tabular_name.ok_or_else(|| {
            ErrorModel::internal(
                "Tabular name should not be null",
                "InternalDatabaseError",
                None,
            )
        })?;
        let namespace = try_parse_namespace_ident(namespace)?;

        match typ {
            TabularType::Table => {
                table_map.insert(
                    TabularIdentOwned::Table(TableIdent { namespace, name }),
                    Some(TabularId::Table(tabular_id.into())),
                );
            }
            TabularType::View => {
                table_map.insert(
                    TabularIdentOwned::View(TableIdent { namespace, name }),
                    Some(TabularId::View(tabular_id.into())),
                );
            }
        }
    }

    // Missing tables are added with None
    if table_map.len() < tables.len() {
        for table in tables {
            table_map.entry(table.into()).or_insert(None);
        }
    }

    Ok(table_map)
}

pub(crate) struct CreateTabular<'a> {
    pub(crate) id: Uuid,
    pub(crate) name: &'a str,
    pub(crate) namespace_id: Uuid,
    pub(crate) warehouse_id: Uuid,
    pub(crate) typ: TabularType,
    pub(crate) metadata_location: Option<&'a Location>,
    pub(crate) location: &'a Location,
}

pub(crate) fn get_partial_fs_locations(location: &Location) -> Result<Vec<String>> {
    location
        .partial_locations()
        .into_iter()
        // Keep only the last part of the location
        .map(|l| {
            split_location(l)
                .map_err(Into::into)
                .map(|(_, p)| p.to_string())
        })
        .collect::<Result<Vec<_>>>()
}

pub(crate) async fn create_tabular(
    CreateTabular {
        id,
        name,
        namespace_id,
        warehouse_id,
        typ,
        metadata_location,
        location,
    }: CreateTabular<'_>,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Uuid> {
    let (fs_protocol, fs_location) = split_location(location.as_str())?;
    let partial_locations = get_partial_fs_locations(location)?;

    let tabular_id = sqlx::query_scalar!(
        r#"
        INSERT INTO tabular (tabular_id, name, namespace_id, namespace_name, warehouse_id, typ, metadata_location, fs_protocol, fs_location)
        SELECT $1, $2, $3, n.namespace_name, $4, $5, $6, $7, $8
        FROM namespace n
        WHERE n.namespace_id = $3
        RETURNING tabular_id
        "#,
        id,
        name,
        namespace_id,
        warehouse_id,
        typ as _,
        metadata_location.map(Location::as_str),
        fs_protocol,
        fs_location
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| {
        tracing::warn!(?e, "Error creating new {typ}");
        e.into_error_model(format!("Error creating {typ}"))
    })?;

    let location_is_taken = sqlx::query_scalar!(
        r#"SELECT EXISTS (
               SELECT 1
               FROM tabular ta
               WHERE ta.warehouse_id = $1 AND (fs_location = ANY($2) OR
                      -- TODO: revisit this after knowing performance impact, may need an index
                      (length($4) < length(fs_location) AND ((TRIM(TRAILING '/' FROM fs_location) || '/') LIKE $4 || '/%'))
               ) AND tabular_id != $3
           ) as "exists!""#,
        warehouse_id,
        &partial_locations,
        id,
        fs_location
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| {
        tracing::warn!(?e, "Error checking for conflicting locations");
        e.into_error_model("Error checking for conflicting locations".to_string())
    })?;

    if location_is_taken {
        return Err(ErrorModel::bad_request(
            "Location is already taken by another table or view",
            "LocationAlreadyTaken",
            None,
        )
        .into());
    }

    Ok(tabular_id)
}

#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
pub(crate) async fn list_tabulars<'e, 'c, E>(
    warehouse_id: WarehouseId,
    namespace: Option<&NamespaceIdent>,
    namespace_id: Option<NamespaceId>,
    list_flags: crate::service::ListFlags,
    catalog_state: E,
    typ: Option<TabularType>,
    pagination_query: PaginationQuery,
) -> Result<PaginatedMapping<TabularId, TabularInfo>>
where
    E: 'e + sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let page_size = CONFIG.page_size_or_pagination_max(pagination_query.page_size);

    let token = pagination_query
        .page_token
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

    let tables = sqlx::query!(
        r#"
        SELECT
            t.tabular_id,
            t.name as "tabular_name",
            t.namespace_name,
            t.typ as "typ: TabularType",
            t.created_at,
            t.deleted_at,
            tt.scheduled_for as "cleanup_at?",
            tt.task_id as "cleanup_task_id?",
            t.protected
        FROM tabular t
        INNER JOIN warehouse w ON w.warehouse_id = $1
        LEFT JOIN task tt ON (t.tabular_id = tt.entity_id AND tt.entity_type in ('table', 'view') AND queue_name = 'tabular_expiration' AND tt.warehouse_id = $1)
        WHERE t.warehouse_id = $1 AND (tt.queue_name = 'tabular_expiration' OR tt.queue_name is NULL)
            AND (t.namespace_name = $2 OR $2 IS NULL)
            AND (t.namespace_id = $10 OR $10 IS NULL)
            AND w.status = 'active'
            AND (t.typ = $3 OR $3 IS NULL)
            -- active tables are tables that are not staged and not deleted
            AND ((t.deleted_at IS NOT NULL OR t.metadata_location IS NULL) OR $4)
            AND (t.deleted_at IS NULL OR $5)
            AND (t.metadata_location IS NOT NULL OR $6)
            AND ((t.created_at > $7 OR $7 IS NULL) OR (t.created_at = $7 AND t.tabular_id > $8))
            ORDER BY t.created_at, t.tabular_id ASC
            LIMIT $9
        "#,
        *warehouse_id,
        namespace.as_deref().map(|n| n.as_ref().as_slice()),
        typ as _,
        list_flags.include_active,
        list_flags.include_deleted,
        list_flags.include_staged,
        token_ts,
        token_id,
        page_size,
        namespace_id.map(|n| *n),
    )
    .fetch_all(catalog_state)
    .await
    .map_err(|e| e.into_error_model("Error fetching tables or views".to_string()))?;

    let mut tabulars = PaginatedMapping::with_capacity(tables.len());
    for table in tables {
        let namespace = try_parse_namespace_ident(table.namespace_name)?;
        let name = table.tabular_name;

        let deletion_details = if let Some(deleted_at) = table.deleted_at {
            Some(DeletionDetails {
                expiration_date: table.cleanup_at.ok_or(ErrorModel::internal(
                    "Cleanup date missing for deleted tabular",
                    "InternalDatabaseError",
                    None,
                ))?,
                expiration_task_id: table.cleanup_task_id.ok_or(ErrorModel::internal(
                    "Cleanup task ID missing for deleted tabular",
                    "InternalDatabaseError",
                    None,
                ))?,
                deleted_at,
                created_at: table.created_at,
            })
        } else {
            None
        };

        match table.typ {
            TabularType::Table => {
                tabulars.insert(
                    TabularId::Table(table.tabular_id.into()),
                    TabularInfo {
                        table_ident: TabularIdentOwned::Table(TableIdent { namespace, name }),
                        deletion_details,
                        protected: table.protected,
                    },
                    PaginateToken::V1(V1PaginateToken {
                        created_at: table.created_at,
                        id: table.tabular_id,
                    })
                    .to_string(),
                );
            }
            TabularType::View => {
                tabulars.insert(
                    TabularId::View(table.tabular_id.into()),
                    TabularInfo {
                        table_ident: TabularIdentOwned::View(TableIdent { namespace, name }),
                        deletion_details,
                        protected: table.protected,
                    },
                    PaginateToken::V1(V1PaginateToken {
                        created_at: table.created_at,
                        id: table.tabular_id,
                    })
                    .to_string(),
                );
            }
        }
    }

    Ok(tabulars)
}

/// If search term corresponds to an uuid, it searches for the corresponding `TabularId`. Otherwise
/// it searches for similarly named tables, taking namespace name and table name into account.
pub(crate) async fn search_tabular<'e, 'c: 'e, E: sqlx::Executor<'c, Database = sqlx::Postgres>>(
    warehouse_id: WarehouseId,
    search_term: &str,
    connection: E,
) -> Result<SearchTabularResponse> {
    let tabulars = match Uuid::try_parse(search_term) {
        // Search string corresponds to uuid.
        Ok(id) => sqlx::query!(
            r#"
                SELECT tabular_id,
                    namespace_name,
                    name,
                    typ as "typ: TabularType"
                FROM tabular
                WHERE warehouse_id = $1 AND tabular_id = $2
                LIMIT 1
                "#,
            *warehouse_id,
            id,
        )
        .fetch_optional(connection)
        .await
        .map_err(|e| e.into_error_model("Error searching tabular by uuid".to_string()))?
        .map(|row| SearchTabular {
            namespace_name: row.namespace_name,
            tabular_name: row.name,
            id: row.tabular_id,
            tabular_type: row.typ.into(),
            dist: Some(0f32), // ids match so it's a perfect match
        })
        .into_iter()
        .collect::<Vec<_>>(),

        // Search string is not an uuid
        Err(_) => sqlx::query!(
            r#"
                SELECT tabular_id,
                    namespace_name,
                    name,
                    typ as "typ: TabularType",
                    concat_namespace_name_tabular_name(namespace_name, name) <-> $2 AS dist
                FROM tabular
                WHERE warehouse_id = $1
                ORDER BY dist ASC
                LIMIT 10
                "#,
            *warehouse_id,
            search_term,
        )
        .fetch_all(connection)
        .await
        .map_err(|e| e.into_error_model("Error searching tabular by search term".to_string()))?
        .into_iter()
        .map(|row| SearchTabular {
            namespace_name: row.namespace_name,
            tabular_name: row.name,
            id: row.tabular_id,
            tabular_type: row.typ.into(),
            dist: row.dist,
        })
        .collect::<Vec<_>>(),
    };

    Ok(SearchTabularResponse { tabulars })
}

/// Rename a tabular. Tabulars may be moved across namespaces.
#[allow(clippy::too_many_lines)]
pub(crate) async fn rename_tabular(
    warehouse_id: WarehouseId,
    source_id: TabularId,
    source: &TableIdent,
    destination: &TableIdent,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    if source == destination {
        return Ok(());
    }

    let TableIdent {
        namespace: source_namespace,
        name: source_name,
    } = source;
    let TableIdent {
        namespace: dest_namespace,
        name: dest_name,
    } = destination;

    if source_namespace == dest_namespace {
        let _ = sqlx::query_scalar!(
            r#"
            WITH locked_tabular AS (
                SELECT tabular_id, name, namespace_id
                FROM tabular
                WHERE tabular_id = $2
                    AND warehouse_id = $4
                    AND typ = $3
                    AND metadata_location IS NOT NULL
                    AND deleted_at IS NULL
                FOR UPDATE
            ),
            locked_source_namespace AS ( -- source namespace of the tabular
                SELECT n.namespace_id
                FROM namespace n
                JOIN locked_tabular lt ON lt.namespace_id = n.namespace_id
                WHERE n.warehouse_id = $4
                FOR UPDATE
            ),
            warehouse_check AS (
                SELECT warehouse_id
                FROM warehouse
                WHERE warehouse_id = $4 AND status = 'active'
            ),
            conflict_check AS (
                SELECT 1
                FROM tabular t
                JOIN locked_source_namespace ln ON t.namespace_id = ln.namespace_id AND t.warehouse_id = $4
                WHERE t.name = $1
                FOR UPDATE
            )
            UPDATE tabular
            SET name = $1
            FROM locked_tabular lt, warehouse_check wc, locked_source_namespace lsn
            WHERE tabular.tabular_id = lt.tabular_id
                AND tabular.warehouse_id = $4
                AND wc.warehouse_id = $4
                AND lsn.namespace_id IS NOT NULL
                AND NOT EXISTS (SELECT 1 FROM conflict_check)
            RETURNING tabular.tabular_id
            "#,
            &**dest_name,
            *source_id,
            TabularType::from(source_id) as _,
            *warehouse_id,
        )
        .fetch_one(&mut **transaction)
        .await
        .map_err(|e| match e {
            sqlx::Error::RowNotFound => ErrorModel::builder()
                .code(StatusCode::NOT_FOUND.into())
                .message(format!("ID of {} to rename not found", source_id.typ_str()))
                .r#type(format!("Rename{}IdNotFound", source_id.typ_str()))
                .build(),
            _ => e.into_error_model(format!("Error renaming {}", source_id.typ_str())),
        })?;
    } else {
        let _ = sqlx::query_scalar!(
            r#"
            WITH locked_tabular AS (
                SELECT tabular_id, name, namespace_id
                FROM tabular
                WHERE tabular_id = $4
                    AND warehouse_id = $2
                    AND typ = $5
                    AND metadata_location IS NOT NULL
                    AND name = $6
                    AND deleted_at IS NULL
                FOR UPDATE
            ),
            locked_namespace AS ( -- target namespace
                SELECT namespace_id
                FROM namespace
                WHERE warehouse_id = $2 AND namespace_name = $3
                FOR UPDATE
            ),
            locked_source_namespace AS ( -- source namespace of the tabular
                SELECT n.namespace_id
                FROM namespace n
                JOIN locked_tabular lt ON lt.namespace_id = n.namespace_id
                WHERE n.warehouse_id = $2
                FOR UPDATE
            ),
            warehouse_check AS (
                SELECT warehouse_id FROM warehouse
                WHERE warehouse_id = $2 AND status = 'active'
            ),
            conflict_check AS (
                SELECT 1
                FROM tabular t
                JOIN locked_namespace ln ON t.namespace_id = ln.namespace_id AND t.warehouse_id = $2
                WHERE t.name = $1
                FOR UPDATE
            )
            UPDATE tabular t
            SET name = $1, namespace_id = ln.namespace_id
            FROM locked_tabular lt, locked_namespace ln, locked_source_namespace lsn, warehouse_check wc
            WHERE t.tabular_id = lt.tabular_id
            AND t.warehouse_id = $2
            AND ln.namespace_id IS NOT NULL
            AND wc.warehouse_id = $2
            AND lsn.namespace_id IS NOT NULL
            AND NOT EXISTS (SELECT 1 FROM conflict_check)
            RETURNING t.tabular_id;
            "#,
            &**dest_name,
            *warehouse_id,
            &**dest_namespace,
            *source_id,
            TabularType::from(source_id) as _,
            &**source_name,
        )
        .fetch_one(&mut **transaction)
        .await
        .map_err(|e| match e {
            sqlx::Error::RowNotFound => ErrorModel::builder()
                .code(StatusCode::NOT_FOUND.into())
                .message(format!(
                    "ID of {} to rename not found or destination namespace not found",
                    source_id.typ_str()
                ))
                .r#type(format!(
                    "Rename{}IdOrNamespaceNotFound",
                    source_id.typ_str()
                ))
                .build(),
            _ => e.into_error_model(format!("Error renaming {}", source_id.typ_str())),
        })?;
    }

    Ok(())
}

#[derive(Debug, Copy, Clone, sqlx::Type, PartialEq, Eq)]
#[sqlx(type_name = "deletion_kind", rename_all = "kebab-case")]
pub enum DeletionKind {
    Default,
    Purge,
}

impl From<DeletionKind> for crate::api::management::v1::DeleteKind {
    fn from(kind: DeletionKind) -> Self {
        match kind {
            DeletionKind::Default => crate::api::management::v1::DeleteKind::Default,
            DeletionKind::Purge => crate::api::management::v1::DeleteKind::Purge,
        }
    }
}

impl From<TabularType> for crate::api::management::v1::TabularType {
    fn from(typ: TabularType) -> Self {
        match typ {
            TabularType::Table => crate::api::management::v1::TabularType::Table,
            TabularType::View => crate::api::management::v1::TabularType::View,
        }
    }
}

pub(crate) async fn clear_tabular_deleted_at(
    tabular_ids: &[TabularId],
    warehouse_id: WarehouseId,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Vec<UndropTabularResponse>> {
    let tabular_ids: Vec<Uuid> = tabular_ids.iter().map(|id| **id).collect();
    let undrop_tabular_informations = sqlx::query!(
        r#"WITH locked_tabulars AS (
            SELECT t.tabular_id, t.name, t.namespace_id, n.namespace_name
            FROM tabular t 
            JOIN namespace n ON t.namespace_id = n.namespace_id
            WHERE n.warehouse_id = $2
                AND t.warehouse_id = $2
                AND t.tabular_id = ANY($1::uuid[])
            FOR UPDATE OF t
        ),
        validation AS (
            SELECT NOT EXISTS (
                SELECT 1 FROM unnest($1::uuid[]) AS id
                WHERE id NOT IN (SELECT tabular_id FROM locked_tabulars)
            ) AS all_found
        ),
        locked_tasks AS (
            SELECT ta.task_id, ta.entity_id
            FROM task ta
            JOIN locked_tabulars lt ON ta.entity_id = lt.tabular_id
            WHERE ta.entity_type in ('table', 'view')
                AND ta.warehouse_id = $2
                AND ta.queue_name = 'tabular_expiration'
            FOR UPDATE OF ta
        )
        UPDATE tabular
        SET deleted_at = NULL
        FROM locked_tabulars lt
        LEFT JOIN locked_tasks lta ON lt.tabular_id = lta.entity_id
        WHERE tabular.tabular_id = lt.tabular_id AND tabular.warehouse_id = $2
        RETURNING
            tabular.name,
            tabular.tabular_id,
            lta.task_id as "task_id?",
            lt.namespace_name,
            (SELECT all_found FROM validation) as "all_found!";"#,
        &tabular_ids,
        *warehouse_id,
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(|e| {
        tracing::warn!("Error marking tabular as undeleted: {e}");
        match &e {
            sqlx::Error::Database(db_err) => match db_err.constraint() {
                Some("unique_name_per_namespace_id") => ErrorModel::bad_request(
                    "Tabular with the same name already exists in the namespace.",
                    "TabularNameAlreadyExists",
                    Some(Box::new(e)),
                ),
                _ => e.into_error_model("Error marking tabulars as undeleted".to_string()),
            },
            _ => e.into_error_model("Error marking tabulars as undeleted".to_string()),
        }
    })?;

    let all_found = undrop_tabular_informations
        .first()
        .map_or(tabular_ids.is_empty(), |r| r.all_found);
    if !all_found {
        return Err(ErrorModel::not_found(
            "One or more tabular IDs to undrop not found",
            "NoSuchTabularError",
            None,
        )
        .into());
    }

    let undrop_tabular_informations = undrop_tabular_informations
        .into_iter()
        .map(|undrop_tabular_information| UndropTabularResponse {
            table_id: TableId::from(undrop_tabular_information.tabular_id),
            expiration_task_id: undrop_tabular_information.task_id.map(TaskId::from),
            name: undrop_tabular_information.name,
            namespace: NamespaceIdent::from_vec(undrop_tabular_information.namespace_name)
                .unwrap_or(NamespaceIdent::new("unknown".into())),
        })
        .collect::<Vec<UndropTabularResponse>>();

    Ok(undrop_tabular_informations)
}

pub(crate) async fn mark_tabular_as_deleted(
    warehouse_id: WarehouseId,
    tabular_id: TabularId,
    force: bool,
    delete_date: Option<chrono::DateTime<Utc>>,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    let r = sqlx::query!(
        r#"
        WITH locked_tabular AS (
            SELECT tabular_id, protected
            FROM tabular
            WHERE tabular_id = $2 AND warehouse_id = $1
            FOR UPDATE
        ),
        marked AS (
            UPDATE tabular
            SET deleted_at = $3
            FROM locked_tabular lt
            WHERE tabular.tabular_id = lt.tabular_id
                AND tabular.warehouse_id = $1
                AND ((NOT lt.protected) OR $4)
            RETURNING tabular.tabular_id
        )
        SELECT 
            lt.protected as "protected!",
            (SELECT tabular_id FROM marked) IS NOT NULL as "was_marked!"
        FROM locked_tabular lt
        "#,
        *warehouse_id,
        *tabular_id,
        delete_date.unwrap_or(Utc::now()),
        force,
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| {
        if let sqlx::Error::RowNotFound = e {
            ErrorModel::not_found(
                format!("Table with id {} not found", tabular_id.typ_str()),
                "NoSuchTabularError".to_string(),
                Some(Box::new(e)),
            )
        } else {
            tracing::warn!("Error marking tabular as deleted: {}", e);
            e.into_error_model(format!(
                "Error marking {} in {warehouse_id} as deleted",
                tabular_id.typ_str()
            ))
        }
    })?;

    if r.protected && !force {
        return Err(ErrorModel::conflict(
            format!(
                "{} in warehouse {warehouse_id} is protected and cannot be deleted",
                tabular_id.typ_str()
            ),
            "ProtectedTabularError",
            None,
        )
        .into());
    }

    Ok(())
}

pub(crate) async fn drop_tabular(
    warehouse_id: WarehouseId,
    tabular_id: TabularId,
    force: bool,
    required_metadata_location: Option<&Location>,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<String> {
    let location = sqlx::query!(
        r#"WITH locked_tabular AS (
            SELECT tabular_id, protected, metadata_location, fs_location, fs_protocol
            FROM tabular
            WHERE tabular_id = $2
                AND warehouse_id = $1
                AND typ = $3
                AND tabular_id in (SELECT tabular_id FROM active_tabulars WHERE warehouse_id = $1 AND tabular_id = $2)
            FOR UPDATE
        ),
        deleted AS (
            DELETE FROM tabular
            WHERE tabular_id IN (
                SELECT tabular_id FROM locked_tabular 
                WHERE ((NOT protected) OR $4)
                AND ($5::text IS NULL OR metadata_location = $5)
            )
            AND warehouse_id = $1
            RETURNING tabular_id
        )
        SELECT 
            lt.protected as "protected!",
            lt.metadata_location,
            lt.fs_protocol,
            lt.fs_location,
            (SELECT tabular_id FROM deleted) IS NOT NULL as "was_deleted!"
        FROM locked_tabular lt"#,
        *warehouse_id,
        *tabular_id,
        TabularType::from(tabular_id) as _,
        force,
        required_metadata_location.map(ToString::to_string)
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| {
        if let sqlx::Error::RowNotFound = e {
            ErrorModel::not_found(
                format!(
                    "{} with ID {} not found",
                    tabular_id.typ_str(),
                    tabular_id.as_ref()
                ),
                ErrorKind::TableNotFound.to_string(),
                Some(Box::new(e)),
            )
        } else {
            tracing::warn!("Error dropping tabular: {}", e);
            e.into_error_model(format!("Error dropping {}", tabular_id.typ_str()))
        }
    })?;

    tracing::trace!(
        "Dropped Tabular with ID {tabular_id}. Protected: {}, Location: {:?}, Protocol: {:?}",
        location.protected,
        location.fs_location,
        location.fs_protocol
    );

    if location.protected && !force {
        return Err(ErrorModel::conflict(
            format!(
                "{} is protected and cannot be dropped",
                tabular_id.typ_str()
            ),
            "ProtectedTabularError",
            None,
        )
        .into());
    }

    if let Some(required_metadata_location) = required_metadata_location {
        if location.metadata_location != Some(required_metadata_location.to_string()) {
            return Err(ErrorModel::bad_request(
                format!("Concurrent update on tabular with id {tabular_id}"),
                CONCURRENT_UPDATE_ERROR_TYPE,
                None,
            )
            .into());
        }
    }

    debug_assert!(
        location.was_deleted,
        "If we didn't delete anything, we should have errored out earlier"
    );

    Ok(join_location(&location.fs_protocol, &location.fs_location))
}

fn try_parse_namespace_ident(namespace: Vec<String>) -> Result<NamespaceIdent> {
    NamespaceIdent::from_vec(namespace).map_err(|e| {
        ErrorModel::internal(
            "Error parsing namespace",
            "NamespaceParseError",
            Some(Box::new(e)),
        )
        .into()
    })
}

impl<'a, 'b> From<&'b TabularIdentBorrowed<'a>> for TabularType {
    fn from(ident: &'b TabularIdentBorrowed<'a>) -> Self {
        match ident {
            TabularIdentBorrowed::Table(_) => TabularType::Table,
            TabularIdentBorrowed::View(_) => TabularType::View,
        }
    }
}

impl<'a> From<&'a TabularId> for TabularType {
    fn from(ident: &'a TabularId) -> Self {
        match ident {
            TabularId::Table(_) => TabularType::Table,
            TabularId::View(_) => TabularType::View,
        }
    }
}

impl From<TabularId> for TabularType {
    fn from(ident: TabularId) -> Self {
        match ident {
            TabularId::Table(_) => TabularType::Table,
            TabularId::View(_) => TabularType::View,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr as _;

    use iceberg::ErrorKind;
    use lakekeeper_io::Location;
    use uuid::Uuid;

    use super::*;
    use crate::{
        catalog::tables::CONCURRENT_UPDATE_ERROR_TYPE,
        implementations::postgres::{
            namespace::tests::initialize_namespace, warehouse::test::initialize_warehouse,
            CatalogState,
        },
        service::NamespaceId,
    };

    async fn setup_test_table(
        pool: &sqlx::PgPool,
        protected: bool,
    ) -> (WarehouseId, TabularId, Location, NamespaceId) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace =
            iceberg_ext::NamespaceIdent::from_vec(vec!["test_namespace".to_string()]).unwrap();
        let (namespace_id, _) =
            initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;

        let table_name = format!("test_table_{}", Uuid::now_v7());
        let location = Location::from_str(&format!("s3://test-bucket/{table_name}/")).unwrap();
        let metadata_location =
            Location::from_str(&format!("s3://test-bucket/{table_name}/metadata/v1.json")).unwrap();

        let mut transaction = pool.begin().await.unwrap();

        let table_id = Uuid::now_v7();
        let tabular_id = create_tabular(
            CreateTabular {
                id: table_id,
                name: &table_name,
                namespace_id: *namespace_id,
                warehouse_id: *warehouse_id,
                typ: TabularType::Table,
                metadata_location: Some(&metadata_location),
                location: &location,
            },
            &mut transaction,
        )
        .await
        .unwrap();

        // Set protection status if needed
        if protected {
            set_tabular_protected(
                warehouse_id,
                TabularId::Table(tabular_id.into()),
                true,
                &mut transaction,
            )
            .await
            .unwrap();
        }

        transaction.commit().await.unwrap();

        (
            warehouse_id,
            TabularId::Table(tabular_id.into()),
            metadata_location,
            namespace_id,
        )
    }

    #[sqlx::test]
    async fn test_drop_tabular_table_not_found_returns_404(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;

        let mut transaction = pool.begin().await.unwrap();
        let nonexistent_table_id = TabularId::Table(Uuid::now_v7().into());

        let result = drop_tabular(
            warehouse_id,
            nonexistent_table_id,
            false,
            None,
            &mut transaction,
        )
        .await;

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.error.code, 404);
        assert_eq!(error.error.r#type, ErrorKind::TableNotFound.to_string());
        assert!(error.error.message.contains("Table with ID"));
        assert!(error.error.message.contains("not found"));
    }

    #[sqlx::test]
    async fn test_drop_tabular_protected_table_without_force_returns_protected_error(
        pool: sqlx::PgPool,
    ) {
        let (warehouse_id, tabular_id, metadata_location, _) = setup_test_table(&pool, true).await;

        let mut transaction = pool.begin().await.unwrap();

        let result = drop_tabular(
            warehouse_id,
            tabular_id,
            false, // force = false
            Some(&metadata_location),
            &mut transaction,
        )
        .await;

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.error.code, 409);
        assert_eq!(error.error.r#type, "ProtectedTabularError");
        assert!(error
            .error
            .message
            .contains("is protected and cannot be dropped"));
    }

    #[sqlx::test]
    async fn test_drop_tabular_protected_table_with_force_succeeds(pool: sqlx::PgPool) {
        let (warehouse_id, tabular_id, metadata_location, _) = setup_test_table(&pool, true).await;

        let mut transaction = pool.begin().await.unwrap();

        let result = drop_tabular(
            warehouse_id,
            tabular_id,
            true, // force = true
            Some(&metadata_location),
            &mut transaction,
        )
        .await;

        assert!(result.is_ok());
        let location = result.unwrap();
        assert!(location.starts_with("s3://test-bucket/"));
    }

    #[sqlx::test]
    async fn test_drop_tabular_concurrent_update_error_wrong_metadata_location(pool: sqlx::PgPool) {
        let (warehouse_id, tabular_id, _actual_metadata_location, _) =
            setup_test_table(&pool, false).await;

        let wrong_metadata_location =
            Location::from_str("s3://wrong-bucket/wrong/metadata/v1.json").unwrap();

        let mut transaction = pool.begin().await.unwrap();

        let result = drop_tabular(
            warehouse_id,
            tabular_id,
            false,
            Some(&wrong_metadata_location),
            &mut transaction,
        )
        .await;

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.error.code, 400);
        assert_eq!(error.error.r#type, CONCURRENT_UPDATE_ERROR_TYPE);
        assert!(error
            .error
            .message
            .contains("Concurrent update on tabular with id"));
    }

    #[sqlx::test]
    async fn test_drop_tabular_with_correct_metadata_location_succeeds(pool: sqlx::PgPool) {
        let (warehouse_id, tabular_id, metadata_location, _) = setup_test_table(&pool, false).await;

        let mut transaction = pool.begin().await.unwrap();

        let result = drop_tabular(
            warehouse_id,
            tabular_id,
            false,
            Some(&metadata_location),
            &mut transaction,
        )
        .await;

        assert!(result.is_ok());
        let location = result.unwrap();
        assert!(location.starts_with("s3://test-bucket/"));
    }

    #[sqlx::test]
    async fn test_drop_tabular_without_metadata_location_check_succeeds(pool: sqlx::PgPool) {
        let (warehouse_id, tabular_id, _metadata_location, _) =
            setup_test_table(&pool, false).await;

        let mut transaction = pool.begin().await.unwrap();

        let result = drop_tabular(
            warehouse_id,
            tabular_id,
            false,
            None, // No metadata location check
            &mut transaction,
        )
        .await;

        assert!(result.is_ok());
        let location = result.unwrap();
        assert!(location.starts_with("s3://test-bucket/"));
    }

    #[sqlx::test]
    async fn test_drop_tabular_view_not_found_returns_404(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;

        let mut transaction = pool.begin().await.unwrap();
        let nonexistent_view_id = TabularId::View(Uuid::now_v7().into());

        let result = drop_tabular(
            warehouse_id,
            nonexistent_view_id,
            false,
            None,
            &mut transaction,
        )
        .await;

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.error.code, 404);
        assert_eq!(error.error.r#type, ErrorKind::TableNotFound.to_string());
        assert!(error.error.message.contains("View with ID"));
        assert!(error.error.message.contains("not found"));
    }

    #[sqlx::test]
    async fn test_drop_tabular_inactive_warehouse_returns_404(pool: sqlx::PgPool) {
        let (warehouse_id, tabular_id, metadata_location, _) = setup_test_table(&pool, false).await;

        // Deactivate the warehouse
        let mut transaction = pool.begin().await.unwrap();
        crate::implementations::postgres::warehouse::set_warehouse_status(
            warehouse_id,
            crate::api::management::v1::warehouse::WarehouseStatus::Inactive,
            &mut transaction,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = pool.begin().await.unwrap();

        let result = drop_tabular(
            warehouse_id,
            tabular_id,
            false,
            Some(&metadata_location),
            &mut transaction,
        )
        .await;

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.error.code, 404);
        assert_eq!(error.error.r#type, ErrorKind::TableNotFound.to_string());
    }

    #[sqlx::test]
    async fn test_search_tabular(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace1 = iceberg_ext::NamespaceIdent::from_vec(vec!["hr_ns".to_string()]).unwrap();
        let (namespace1_id, _) =
            initialize_namespace(state.clone(), warehouse_id, &namespace1, None).await;
        let namespace2 =
            iceberg_ext::NamespaceIdent::from_vec(vec!["finance_ns".to_string()]).unwrap();
        let (namespace2_id, _) =
            initialize_namespace(state.clone(), warehouse_id, &namespace2, None).await;

        let table_names = [10, 101, 1011, 42, 420]
            .into_iter()
            .map(|i| format!("test_region_{i}"))
            .collect::<Vec<_>>();

        let mut best_match_id = None; // will store id of the tabular we'll search for
        for nsid in [namespace1_id, namespace2_id] {
            for tn in &table_names {
                let mut transaction = pool.begin().await.unwrap();
                let table_id = Uuid::now_v7();
                let location =
                    Location::from_str(&format!("s3://test-bucket/{nsid}/{tn}/")).unwrap();
                let metadata_location =
                    Location::from_str(&format!("s3://test-bucket/{nsid}/{tn}/metadata/v1.json"))
                        .unwrap();
                let tabular_id = create_tabular(
                    CreateTabular {
                        id: table_id,
                        name: tn.as_ref(),
                        namespace_id: *nsid,
                        warehouse_id: *warehouse_id,
                        typ: TabularType::Table,
                        metadata_location: Some(&metadata_location),
                        location: &location,
                    },
                    &mut transaction,
                )
                .await
                .unwrap();
                transaction.commit().await.unwrap();
                if nsid == namespace2_id && tn == "test_region_42" {
                    best_match_id = Some(tabular_id);
                }
            }
        }

        let res = search_tabular(warehouse_id, "finance.table42", &state.read_write.read_pool)
            .await
            .unwrap()
            .tabulars[0]
            .clone();

        // Assert the best match is returned as first result.
        assert_eq!(res.id, best_match_id.unwrap());
        assert_eq!(res.namespace_name, vec!["finance_ns".to_string()]);
        assert_eq!(res.tabular_name, "test_region_42");
        assert_eq!(res.tabular_type, TabularType::Table.into());
    }

    #[sqlx::test]
    async fn test_search_tabular_by_uuid(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = iceberg_ext::NamespaceIdent::from_vec(vec!["hr_ns".to_string()]).unwrap();
        let (namespace_id, _) =
            initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;

        let table_names = [10, 101, 1011, 42, 420]
            .into_iter()
            .map(|i| format!("test_region_{i}"))
            .collect::<Vec<_>>();

        let mut id_to_search = None; // will store id of the tabular we'll search for
        for tn in &table_names {
            let mut transaction = pool.begin().await.unwrap();
            let table_id = Uuid::now_v7();
            let location =
                Location::from_str(&format!("s3://test-bucket/{namespace_id}/{tn}/")).unwrap();
            let metadata_location = Location::from_str(&format!(
                "s3://test-bucket/{namespace_id}/{tn}/metadata/v1.json"
            ))
            .unwrap();
            let tabular_id = create_tabular(
                CreateTabular {
                    id: table_id,
                    name: tn.as_ref(),
                    namespace_id: *namespace_id,
                    warehouse_id: *warehouse_id,
                    typ: TabularType::Table,
                    metadata_location: Some(&metadata_location),
                    location: &location,
                },
                &mut transaction,
            )
            .await
            .unwrap();
            transaction.commit().await.unwrap();
            if tn == "test_region_42" {
                id_to_search = Some(tabular_id);
            }
        }

        let results = search_tabular(
            warehouse_id,
            id_to_search.unwrap().to_string().as_str(),
            &state.read_write.read_pool,
        )
        .await
        .unwrap()
        .tabulars;
        assert_eq!(results.len(), 1);

        // Assert the tabular with matching uuid is returned
        let res = results[0].clone();
        assert_eq!(res.id, id_to_search.unwrap());
        assert_eq!(res.namespace_name, vec!["hr_ns".to_string()]);
        assert_eq!(res.tabular_name, "test_region_42");
        assert_eq!(res.tabular_type, TabularType::Table.into());
    }
}
