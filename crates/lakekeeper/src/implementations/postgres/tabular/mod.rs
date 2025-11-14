mod load_by_location;
mod protection;
pub mod table;
pub(crate) mod view;

use std::{collections::HashMap, default::Default, fmt::Debug, str::FromStr as _};

use chrono::Utc;
use lakekeeper_io::Location;
pub(crate) use load_by_location::*;
pub(crate) use protection::set_tabular_protected;
use sqlx::FromRow;
use uuid::Uuid;

use super::dbutils::DBErrorHandler as _;
use crate::{
    api::iceberg::v1::{PaginatedMapping, PaginationQuery},
    implementations::postgres::{
        namespace::parse_namespace_identifier_from_vec,
        pagination::{PaginateToken, V1PaginateToken},
    },
    service::{
        storage::join_location, CatalogSearchTabularInfo, CatalogSearchTabularResponse,
        ClearTabularDeletedAtError, ConcurrentUpdateError, CreateTabularError, DropTabularError,
        ExpirationTaskInfo, GetTabularInfoError, InternalParseLocationError,
        InvalidNamespaceIdentifier, ListTabularsError, LocationAlreadyTaken,
        MarkTabularAsDeletedError, NamespaceId, ProtectedTabularDeletionWithoutForce,
        RenameTabularError, SearchTabularError, SerializationError, TableDeletionInfo, TableIdent,
        TableInfo, TabularAlreadyExists, TabularId, TabularIdentBorrowed, TabularNotFound,
        ViewDeletionInfo, ViewInfo, ViewOrTableDeletionInfo, ViewOrTableInfo,
    },
    WarehouseId, CONFIG,
};

#[derive(Debug, sqlx::Type, Copy, Clone, strum::Display)]
#[sqlx(type_name = "tabular_type", rename_all = "kebab-case")]
pub(crate) enum TabularType {
    Table,
    View,
}

impl From<crate::api::management::v1::TabularType> for TabularType {
    fn from(typ: crate::api::management::v1::TabularType) -> Self {
        match typ {
            crate::api::management::v1::TabularType::Table => TabularType::Table,
            crate::api::management::v1::TabularType::View => TabularType::View,
        }
    }
}

#[derive(Debug, derive_more::From)]
pub enum FromTabularRowError {
    InvalidNamespaceIdentifier(InvalidNamespaceIdentifier),
    InternalParseLocationError(InternalParseLocationError),
}

impl From<FromTabularRowError> for GetTabularInfoError {
    fn from(err: FromTabularRowError) -> Self {
        match err {
            FromTabularRowError::InvalidNamespaceIdentifier(e) => e.into(),
            FromTabularRowError::InternalParseLocationError(e) => e.into(),
        }
    }
}

#[derive(Debug, FromRow)]
struct TabularRow {
    tabular_id: Uuid,
    warehouse_version: i64,
    namespace_name: Vec<String>,
    namespace_version: i64,
    namespace_id: Uuid,
    tabular_name: String,
    updated_at: Option<chrono::DateTime<Utc>>,
    metadata_location: Option<String>,
    protected: bool,
    // apparently this is needed, we need 'as "typ: TabularType"' in the query else the select won't
    // work, but that apparently aliases the whole column to "typ: TabularType"
    #[sqlx(rename = "typ: TabularType")]
    typ: TabularType,
    fs_location: String,
    fs_protocol: String,
    view_properties_keys: Option<Vec<String>>,
    view_properties_values: Option<Vec<String>>,
    table_properties_keys: Option<Vec<String>>,
    table_properties_values: Option<Vec<String>>,
}

impl TabularRow {
    fn try_into_table_or_view(
        self,
        warehouse_id: WarehouseId,
    ) -> Result<ViewOrTableInfo, FromTabularRowError> {
        let namespace = parse_namespace_identifier_from_vec(
            &self.namespace_name,
            warehouse_id,
            Some(self.namespace_id),
        )?;
        let name = self.tabular_name;

        let tabular_ident = TableIdent { namespace, name };
        let location = join_location(&self.fs_protocol, &self.fs_location)
            .map_err(InternalParseLocationError::from)?;
        let metadata_location = self
            .metadata_location
            .map(|s| Location::from_str(&s))
            .transpose()
            .map_err(InternalParseLocationError::from)?;
        let view_or_table_info = match self.typ {
            TabularType::Table => ViewOrTableInfo::Table(TableInfo {
                namespace_id: self.namespace_id.into(),
                tabular_ident,
                warehouse_id,
                tabular_id: self.tabular_id.into(),
                protected: self.protected,
                metadata_location,
                updated_at: self.updated_at,
                location,
                properties: prepare_properties(
                    self.table_properties_keys,
                    self.table_properties_values,
                ),
                namespace_version: self.namespace_version.into(),
                warehouse_version: self.warehouse_version.into(),
            }),
            TabularType::View => ViewOrTableInfo::View(ViewInfo {
                namespace_id: self.namespace_id.into(),
                tabular_ident,
                warehouse_id,
                tabular_id: self.tabular_id.into(),
                protected: self.protected,
                metadata_location,
                updated_at: self.updated_at,
                location,
                properties: prepare_properties(
                    self.view_properties_keys,
                    self.view_properties_values,
                ),
                namespace_version: self.namespace_version.into(),
                warehouse_version: self.warehouse_version.into(),
            }),
        };

        Ok(view_or_table_info)
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn get_tabular_infos_by_ids<'e, 'c: 'e, E>(
    warehouse_id: WarehouseId,
    tabulars: &[TabularId],
    list_flags: crate::service::TabularListFlags,
    catalog_state: E,
) -> Result<Vec<ViewOrTableInfo>, GetTabularInfoError>
where
    E: 'e + sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    if tabulars.is_empty() {
        return Ok(Vec::new());
    }
    let (t_ids, t_typs) = tabulars.iter().fold(
        (
            Vec::with_capacity(tabulars.len()),
            Vec::with_capacity(tabulars.len()),
        ),
        |(mut t_ids, mut t_typs), t| {
            match t {
                TabularId::Table(id) => {
                    t_ids.push(**id);
                    t_typs.push(TabularType::Table);
                }
                TabularId::View(id) => {
                    t_ids.push(**id);
                    t_typs.push(TabularType::View);
                }
            }
            (t_ids, t_typs)
        },
    );

    let rows = sqlx::query_as!(
        TabularRow,
        r#"
        WITH q AS (
            SELECT id, typ FROM UNNEST($2::uuid[], $3::tabular_type[]) u(id, typ)
        ),
        selected_tabulars AS (
            SELECT t.tabular_id,
                t.namespace_id,
                t.name as tabular_name,
                t.tabular_namespace_name as namespace_name,
                t.typ,
                t.metadata_location,
                t.updated_at,
                t.protected,
                t.fs_location,
                t.fs_protocol,
                w.version as warehouse_version,
                n.version as namespace_version
            FROM tabular t 
            INNER JOIN q ON t.warehouse_id = $1 AND t.tabular_id = q.id AND t.typ = q.typ
            INNER JOIN warehouse w ON w.warehouse_id = $1
            INNER JOIN namespace n ON n.namespace_id = t.namespace_id AND n.warehouse_id = $1
            WHERE w.status = 'active'
                AND (t.deleted_at is NULL OR $4)
                AND (t.metadata_location is not NULL OR $5)
        ),
        selected_views AS (
            SELECT tabular_id FROM selected_tabulars WHERE typ = 'view'
        ),
        selected_tables AS (
            SELECT tabular_id FROM selected_tabulars WHERE typ = 'table'
        )
        SELECT st.tabular_id,
               st.warehouse_version,
               st.namespace_name,
               st.namespace_version,
               st.namespace_id,
               st.tabular_name,
               st.updated_at,
               st.metadata_location,
               st.protected,
               st.typ as "typ: TabularType",
               st.fs_location,
               st.fs_protocol,
               vp.view_properties_keys,
               vp.view_properties_values,
               tp.keys as table_properties_keys,
               tp.values as table_properties_values
        FROM selected_tabulars st
        LEFT JOIN (SELECT view_id,
                    ARRAY_AGG(key)   AS view_properties_keys,
                    ARRAY_AGG(value) AS view_properties_values
            FROM view_properties
            WHERE warehouse_id = $1 and view_id in (SELECT tabular_id FROM selected_views)
            GROUP BY view_id) vp ON st.tabular_id = vp.view_id
        LEFT JOIN (SELECT table_id,
                    ARRAY_AGG(key) as keys,
                    ARRAY_AGG(value) as values
                FROM table_properties
                WHERE warehouse_id = $1 AND table_id in (SELECT tabular_id FROM selected_tables)
                GROUP BY table_id) tp ON st.tabular_id = tp.table_id
        "#,
        *warehouse_id,
        t_ids.as_slice() as _,
        t_typs.as_slice() as _,
        list_flags.include_deleted,
        list_flags.include_staged
    )
    .fetch_all(catalog_state)
    .await
    .map_err(super::dbutils::DBErrorHandler::into_catalog_backend_error)?;

    let result = rows
        .into_iter()
        .map(|row| {
            let view_or_table_info = row.try_into_table_or_view(warehouse_id)?;
            Ok(view_or_table_info)
        })
        .collect::<Result<_, GetTabularInfoError>>()?;
    Ok(result)
}

/// The returned tabulars have the same case (name and namespace) as the input identifiers.
///
/// These may differ from identifiers stored in the db, since case insensitivity is achieved
/// by collation. For example:
///
/// - Table name in the db is `table1`
/// - The input parameter is `TABLE1`
/// - `table1` and `TABLE1` match due to collation and the key in the returned map is `TABLE1`
///
/// In line with that, querying both `table1` and `TABLE1` returns a map with two entries,
/// both mapping to the same table id.
#[allow(clippy::too_many_lines)]
pub(crate) async fn get_tabular_infos_by_idents<'e, 'c: 'e, E>(
    warehouse_id: WarehouseId,
    tabulars: &[TabularIdentBorrowed<'_>],
    list_flags: crate::service::TabularListFlags,
    catalog_state: E,
) -> Result<Vec<ViewOrTableInfo>, GetTabularInfoError>
where
    E: 'e + sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    if tabulars.is_empty() {
        return Ok(Vec::new());
    }
    let (ns_names, t_names, t_typs) = tabulars.iter().fold(
        (
            Vec::with_capacity(tabulars.len()),
            Vec::with_capacity(tabulars.len()),
            Vec::with_capacity(tabulars.len()),
        ),
        |(mut ns_names, mut t_names, mut t_typs), t| {
            let TableIdent { namespace, name } = t.as_table_ident();
            let typ: TabularType = t.into();
            ns_names.push(namespace.as_ref());
            t_names.push(name);
            t_typs.push(typ);
            (ns_names, t_names, t_typs)
        },
    );

    // Encoding `ns_names` as json is a workaround for `sqlx` not supporting `Vec<Vec<String>>`.
    let ns_names_json =
        serde_json::to_value(&ns_names).map_err(|e| SerializationError::new("namespace", e))?;

    // For columns with collation, the query must return the value as in input `tables`.
    let rows = sqlx::query_as!(
        TabularRow,
        r#"
        WITH selected_tabulars AS (
            SELECT t.tabular_id,
                in_ns.name as namespace_name,
                in_t.name as tabular_name,
                t.namespace_id,
                t.typ,
                t.metadata_location,
                t.updated_at,
                t.protected,
                t.fs_location,
                t.fs_protocol,
                w.version as warehouse_version,
                n.version as namespace_version
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
                AND (t.metadata_location is not NULL OR $6)
        ),
        selected_views AS (
            SELECT tabular_id FROM selected_tabulars WHERE typ = 'view'
        ),
        selected_tables AS (
            SELECT tabular_id FROM selected_tabulars WHERE typ = 'table'
        )
        SELECT st.tabular_id,
               st.warehouse_version,
               st.namespace_name as "namespace_name!",
               st.namespace_version,
               st.namespace_id,
               st.tabular_name as "tabular_name!",
               st.updated_at,
               st.metadata_location,
               st.protected as "protected!",
               st.typ as "typ: TabularType",
               st.fs_location,
               st.fs_protocol,
               vp.view_properties_keys,
               vp.view_properties_values,
               tp.keys as table_properties_keys,
               tp.values as table_properties_values
        FROM selected_tabulars st
        LEFT JOIN (SELECT view_id,
                    ARRAY_AGG(key)   AS view_properties_keys,
                    ARRAY_AGG(value) AS view_properties_values
            FROM view_properties
            WHERE warehouse_id = $1 and view_id in (SELECT tabular_id FROM selected_views)
            GROUP BY view_id) vp ON st.tabular_id = vp.view_id
        LEFT JOIN (SELECT table_id,
                    ARRAY_AGG(key) as keys,
                    ARRAY_AGG(value) as values
                FROM table_properties
                WHERE warehouse_id = $1 AND table_id in (SELECT tabular_id FROM selected_tables)
                GROUP BY table_id) tp ON st.tabular_id = tp.table_id
        "#,
        *warehouse_id,
        ns_names_json as _,
        t_names.as_slice() as _,
        t_typs.as_slice() as _,
        list_flags.include_deleted,
        list_flags.include_staged
    )
    .fetch_all(catalog_state)
    .await
    .map_err(super::dbutils::DBErrorHandler::into_catalog_backend_error)?;

    let result = rows
        .into_iter()
        .map(|row| {
            let view_or_table_info = row.try_into_table_or_view(warehouse_id)?;
            Ok(view_or_table_info)
        })
        .collect::<Result<_, GetTabularInfoError>>()?;
    Ok(result)
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

pub(crate) fn get_partial_fs_locations(
    location: &Location,
) -> Result<Vec<String>, InternalParseLocationError> {
    location
        .partial_locations()
        .into_iter()
        // Keep only the last part of the location
        .map(|l| {
            let location = Location::from_str(l)?;
            Ok(location.authority_and_path().to_string())
        })
        .collect()
}

impl From<FromTabularRowError> for CreateTabularError {
    fn from(err: FromTabularRowError) -> Self {
        match err {
            FromTabularRowError::InvalidNamespaceIdentifier(e) => e.into(),
            FromTabularRowError::InternalParseLocationError(e) => e.into(),
        }
    }
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
) -> Result<ViewOrTableInfo, CreateTabularError> {
    let fs_protocol = location.scheme();
    let fs_location = location.authority_and_path();
    let partial_locations = get_partial_fs_locations(location)?;

    let tabular_id = sqlx::query_as!(
        TabularRow,
        r#"
        WITH inserted AS (
            INSERT INTO tabular (tabular_id, name, namespace_id, tabular_namespace_name, warehouse_id, typ, metadata_location, fs_protocol, fs_location)
            SELECT $1, $2, $3, n.namespace_name, $4, $5, $6, $7, $8
            FROM namespace n
            WHERE n.namespace_id = $3 AND n.warehouse_id = $4
            RETURNING 
                tabular_id,
                namespace_id,
                name as tabular_name,
                tabular_namespace_name as namespace_name,
                typ,
                metadata_location,
                updated_at,
                protected,
                fs_location,
                fs_protocol
        )
        SELECT i.tabular_id,
               w.version as warehouse_version,
               i.namespace_name,
               n.version as namespace_version,
               i.namespace_id,
               i.tabular_name,
               i.updated_at,
               i.metadata_location,
               i.protected,
               i.typ as "typ: TabularType",
               i.fs_location,
               i.fs_protocol,
               NULL::text[] as view_properties_keys,
               NULL::text[] as view_properties_values,
               NULL::text[] as table_properties_keys,
               NULL::text[] as table_properties_values
        FROM inserted i
        INNER JOIN warehouse w ON w.warehouse_id = $4
        INNER JOIN namespace n ON n.namespace_id = $3 AND n.warehouse_id = $4
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
        match e {
            sqlx::Error::Database(db_err)
                if [Some("unique_name_per_namespace_id"), Some("tabular_pkey")].contains(&db_err.constraint()) =>
            {
                CreateTabularError::from(TabularAlreadyExists::new())
            }
            _ => e.into_catalog_backend_error().into(),
        }
    })?;

    let location_is_taken = sqlx::query_scalar!(
        r#"SELECT EXISTS (
               SELECT 1
               FROM tabular ta
               WHERE ta.warehouse_id = $1 AND (fs_location = ANY($2) OR
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
        e.into_catalog_backend_error().append_detail("Error checking for conflicting locations")
    })?;

    if location_is_taken {
        return Err(LocationAlreadyTaken::new(location.clone()).into());
    }

    let tabular_info = tabular_id.try_into_table_or_view(warehouse_id.into())?;

    Ok(tabular_info)
}

#[derive(Debug, FromRow)]
struct TabularRowWithDeletion {
    tabular_id: Uuid,
    namespace_name: Vec<String>,
    namespace_id: Uuid,
    tabular_name: String,
    updated_at: Option<chrono::DateTime<Utc>>,
    metadata_location: Option<String>,
    protected: bool,
    // apparently this is needed, we need 'as "typ: TabularType"' in the query else the select won't
    // work, but that apparently aliases the whole column to "typ: TabularType"
    #[sqlx(rename = "typ: TabularType")]
    typ: TabularType,
    fs_location: String,
    fs_protocol: String,
    created_at: chrono::DateTime<Utc>,
    deleted_at: Option<chrono::DateTime<Utc>>,
    deletion_scheduled_for: Option<chrono::DateTime<Utc>>,
    deletion_task_id: Option<Uuid>,
    namespace_version: i64,
    warehouse_version: i64,
    view_properties_keys: Option<Vec<String>>,
    view_properties_values: Option<Vec<String>>,
    table_properties_keys: Option<Vec<String>>,
    table_properties_values: Option<Vec<String>>,
}

impl TabularRowWithDeletion {
    fn try_into_table_or_view(
        self,
        warehouse_id: WarehouseId,
    ) -> Result<ViewOrTableDeletionInfo, FromTabularRowError> {
        let row = TabularRow {
            tabular_id: self.tabular_id,
            namespace_name: self.namespace_name,
            namespace_id: self.namespace_id,
            tabular_name: self.tabular_name,
            updated_at: self.updated_at,
            metadata_location: self.metadata_location,
            protected: self.protected,
            typ: self.typ,
            fs_location: self.fs_location,
            fs_protocol: self.fs_protocol,
            warehouse_version: self.warehouse_version,
            namespace_version: self.namespace_version,
            view_properties_keys: self.view_properties_keys,
            view_properties_values: self.view_properties_values,
            table_properties_keys: self.table_properties_keys,
            table_properties_values: self.table_properties_values,
        };

        let tabular_info = row.try_into_table_or_view(warehouse_id)?;
        let expiration_task = if let (Some(expiration_task_id), Some(expiration_date)) =
            (self.deletion_task_id, self.deletion_scheduled_for)
        {
            Some(ExpirationTaskInfo {
                task_id: expiration_task_id.into(),
                expiration_date,
            })
        } else {
            None
        };

        let tabular_deletion_info = match tabular_info {
            ViewOrTableInfo::Table(table_info) => TableDeletionInfo {
                tabular: table_info,
                expiration_task,
                deleted_at: self.deleted_at,
                created_at: self.created_at,
            }
            .into(),
            ViewOrTableInfo::View(view_info) => ViewDeletionInfo {
                tabular: view_info,
                expiration_task,
                deleted_at: self.deleted_at,
                created_at: self.created_at,
            }
            .into(),
        };

        Ok(tabular_deletion_info)
    }
}

impl From<FromTabularRowError> for ListTabularsError {
    fn from(err: FromTabularRowError) -> Self {
        match err {
            FromTabularRowError::InvalidNamespaceIdentifier(e) => e.into(),
            FromTabularRowError::InternalParseLocationError(e) => e.into(),
        }
    }
}

#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
pub(crate) async fn list_tabulars<'e, 'c, E>(
    warehouse_id: WarehouseId,
    namespace_id: Option<NamespaceId>,
    list_flags: crate::service::TabularListFlags,
    catalog_state: E,
    typ: Option<TabularType>,
    pagination_query: PaginationQuery,
) -> Result<PaginatedMapping<TabularId, ViewOrTableDeletionInfo>, ListTabularsError>
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

    let tables = sqlx::query_as!(
        TabularRowWithDeletion,
        r#"
        WITH selected_tabulars AS (
            SELECT
                t.tabular_id,
                t.name as tabular_name,
                t.tabular_namespace_name as namespace_name,
                t.namespace_id,
                t.metadata_location,
                t.typ,
                t.updated_at,
                t.created_at,
                t.deleted_at,
                tt.scheduled_for as deletion_scheduled_for,
                tt.task_id as deletion_task_id,
                t.protected,
                t.fs_location,
                t.fs_protocol,
                w.version as warehouse_version,
                n.version as namespace_version
            FROM tabular t
            INNER JOIN warehouse w ON w.warehouse_id = $1
            INNER JOIN namespace n ON n.namespace_id = t.namespace_id AND n.warehouse_id = $1
            LEFT JOIN task tt ON (t.tabular_id = tt.entity_id AND tt.entity_type in ('table', 'view') AND queue_name = 'tabular_expiration' AND tt.warehouse_id = $1)
            WHERE t.warehouse_id = $1 AND (tt.queue_name = 'tabular_expiration' OR tt.queue_name is NULL)
                AND (t.namespace_id = $2 OR $2 IS NULL)
                AND w.status = 'active'
                AND (t.typ = $3 OR $3 IS NULL)
                -- active tables are tables that are not staged (metadata_location is set) and not deleted
                AND (
                    (t.deleted_at IS NULL AND t.metadata_location IS NOT NULL AND $4) OR   -- include_active
                    (t.deleted_at IS NOT NULL AND $5) OR                                   -- include_deleted  
                    (t.metadata_location IS NULL AND $6)                                   -- include_staged
                )
                AND ((t.created_at > $7 OR $7 IS NULL) OR (t.created_at = $7 AND t.tabular_id > $8))
            ORDER BY t.created_at, t.tabular_id ASC
            LIMIT $9
        ),
        selected_views AS (
            SELECT tabular_id FROM selected_tabulars WHERE typ = 'view'
        ),
        selected_tables AS (
            SELECT tabular_id FROM selected_tabulars WHERE typ = 'table'
        )
        SELECT st.tabular_id,
               st.tabular_name,
               st.namespace_name,
               st.namespace_id,
               st.metadata_location,
               st.typ as "typ: TabularType",
               st.updated_at,
               st.created_at,
               st.deleted_at,
               st.deletion_scheduled_for as "deletion_scheduled_for?",
               st.deletion_task_id as "deletion_task_id?",
               st.protected,
               st.fs_location,
               st.fs_protocol,
               st.namespace_version,
               st.warehouse_version,
               vp.view_properties_keys,
               vp.view_properties_values,
               tp.keys as table_properties_keys,
               tp.values as table_properties_values
        FROM selected_tabulars st
        LEFT JOIN (SELECT view_id,
                    ARRAY_AGG(key)   AS view_properties_keys,
                    ARRAY_AGG(value) AS view_properties_values
            FROM view_properties
            WHERE warehouse_id = $1 and view_id in (SELECT tabular_id FROM selected_views)
            GROUP BY view_id) vp ON st.tabular_id = vp.view_id
        LEFT JOIN (SELECT table_id,
                    ARRAY_AGG(key) as keys,
                    ARRAY_AGG(value) as values
                FROM table_properties
                WHERE warehouse_id = $1 AND table_id in (SELECT tabular_id FROM selected_tables)
                GROUP BY table_id) tp ON st.tabular_id = tp.table_id
        "#,
        *warehouse_id,
        namespace_id.map(|n| *n),
        typ as _,
        list_flags.include_active,
        list_flags.include_deleted,
        list_flags.include_staged,
        token_ts,
        token_id,
        page_size
    )
    .fetch_all(catalog_state)
    .await
    .map_err(super::dbutils::DBErrorHandler::into_catalog_backend_error)?;

    let mut tabulars = PaginatedMapping::with_capacity(tables.len());
    for table in tables {
        let deletion_info = table.try_into_table_or_view(warehouse_id)?;
        let tabular_id = deletion_info.tabular_id();
        let created_at = deletion_info.created_at();

        tabulars.insert(
            tabular_id,
            deletion_info,
            PaginateToken::V1(V1PaginateToken {
                created_at,
                id: tabular_id,
            })
            .to_string(),
        );
    }

    Ok(tabulars)
}

struct PostgresSearchTabularInfo {
    tabular_id: Uuid,
    namespace_id: Uuid,
    namespace_name: Vec<String>,
    namespace_version: i64,
    tabular_name: String,
    typ: TabularType,
    metadata_location: Option<String>,
    updated_at: Option<chrono::DateTime<Utc>>,
    protected: bool,
    distance: Option<f32>,
    fs_location: String,
    fs_protocol: String,
    warehouse_version: i64,
    view_properties_keys: Option<Vec<String>>,
    view_properties_values: Option<Vec<String>>,
    table_properties_keys: Option<Vec<String>>,
    table_properties_values: Option<Vec<String>>,
}

impl PostgresSearchTabularInfo {
    fn into_search_tabular(
        self,
        warehouse_id: WarehouseId,
    ) -> Result<CatalogSearchTabularInfo, SearchTabularError> {
        let namespace = parse_namespace_identifier_from_vec(
            &self.namespace_name,
            warehouse_id,
            Some(self.namespace_id),
        )?;
        let tabular_ident = TableIdent {
            namespace: namespace.clone(),
            name: self.tabular_name.clone(),
        };
        let location = join_location(&self.fs_protocol, &self.fs_location)
            .map_err(InternalParseLocationError::from)?;
        let metadata_location = self
            .metadata_location
            .map(|s| Location::from_str(&s))
            .transpose()
            .map_err(InternalParseLocationError::from)?;
        let tabular = match self.typ {
            TabularType::Table => ViewOrTableInfo::Table(TableInfo {
                namespace_id: self.namespace_id.into(),
                tabular_ident,
                warehouse_id,
                tabular_id: self.tabular_id.into(),
                protected: self.protected,
                metadata_location,
                updated_at: self.updated_at,
                location,
                namespace_version: self.namespace_version.into(),
                warehouse_version: self.warehouse_version.into(),
                properties: prepare_properties(
                    self.table_properties_keys,
                    self.table_properties_values,
                ),
            }),
            TabularType::View => ViewOrTableInfo::View(ViewInfo {
                namespace_id: self.namespace_id.into(),
                tabular_ident,
                warehouse_id,
                tabular_id: self.tabular_id.into(),
                protected: self.protected,
                metadata_location,
                updated_at: self.updated_at,
                location,
                namespace_version: self.namespace_version.into(),
                warehouse_version: self.warehouse_version.into(),
                properties: prepare_properties(
                    self.view_properties_keys,
                    self.view_properties_values,
                ),
            }),
        };

        Ok(CatalogSearchTabularInfo {
            tabular,
            distance: self.distance,
        })
    }
}

/// Searches for similarly named tables, taking namespace name and table name into account.
///
/// If the search term corresponds to an uuid, it instead searches for a table or namespace
/// with that uuid. If a namespace with that uuid exists, the response contains tabulars inside the
/// namespace.
#[allow(clippy::too_many_lines)]
pub(crate) async fn search_tabular<'e, 'c: 'e, E: sqlx::Executor<'c, Database = sqlx::Postgres>>(
    warehouse_id: WarehouseId,
    search_term: &str,
    connection: E,
) -> Result<CatalogSearchTabularResponse, SearchTabularError> {
    let tabulars = match Uuid::try_parse(search_term) {
        // Search string corresponds to uuid.
        Ok(id) => sqlx::query_as!(
            PostgresSearchTabularInfo,
            r#"
            WITH selected_tabulars AS (
                SELECT tabular_id,
                    t.namespace_id,
                    tabular_namespace_name as namespace_name,
                    name as tabular_name,
                    typ,
                    metadata_location,
                    t.updated_at,
                    t.protected,
                    t.fs_location,
                    t.fs_protocol,
                    w.version as warehouse_version,
                    n.version as namespace_version,
                    NULL::float4 as distance
                FROM tabular t
                INNER JOIN warehouse w ON w.warehouse_id = t.warehouse_id
                INNER JOIN namespace n ON n.namespace_id = t.namespace_id AND n.warehouse_id = t.warehouse_id
                WHERE t.warehouse_id = $1
                    AND w.status = 'active'
                    AND t.deleted_at IS NULL
                    AND t.metadata_location IS NOT NULL
                    AND (t.tabular_id = $2 OR t.namespace_id = $2)
                ORDER BY (t.tabular_id = $2) DESC
                LIMIT 10
            ),
            selected_views AS (
                SELECT tabular_id FROM selected_tabulars WHERE typ = 'view'
            ),
            selected_tables AS (
                SELECT tabular_id FROM selected_tabulars WHERE typ = 'table'
            )
            SELECT st.tabular_id,
                st.namespace_id,
                st.namespace_name,
                st.namespace_version,
                st.tabular_name,
                st.typ as "typ: TabularType",
                st.metadata_location,
                st.updated_at,
                st.protected,
                st.distance,
                st.fs_location,
                st.fs_protocol,
                st.warehouse_version,
                vp.view_properties_keys,
                vp.view_properties_values,
                tp.keys as table_properties_keys,
                tp.values as table_properties_values
            FROM selected_tabulars st
            LEFT JOIN (SELECT view_id,
                        ARRAY_AGG(key)   AS view_properties_keys,
                        ARRAY_AGG(value) AS view_properties_values
                FROM view_properties
                WHERE warehouse_id = $1 and view_id in (SELECT tabular_id FROM selected_views)
                GROUP BY view_id) vp ON st.tabular_id = vp.view_id
            LEFT JOIN (SELECT table_id,
                        ARRAY_AGG(key) as keys,
                        ARRAY_AGG(value) as values
                    FROM table_properties
                    WHERE warehouse_id = $1 AND table_id in (SELECT tabular_id FROM selected_tables)
                    GROUP BY table_id) tp ON st.tabular_id = tp.table_id
            "#,
            *warehouse_id,
            id,
        )
        .fetch_all(connection)
        .await
        .map_err(super::dbutils::DBErrorHandler::into_catalog_backend_error)?
        .into_iter()
        .map(|row| row.into_search_tabular(warehouse_id))
        .collect::<Result<Vec<_>, _>>()?,

        // Search string is not an uuid
        Err(_) => sqlx::query_as!(
            PostgresSearchTabularInfo,
            r#"
            WITH selected_tabulars AS (
                SELECT  tabular_id,
                    t.namespace_id,
                    tabular_namespace_name as namespace_name,
                    name as tabular_name,
                    typ,
                    metadata_location,
                    t.updated_at,
                    t.protected,
                    t.fs_location,
                    t.fs_protocol,
                    w.version as warehouse_version,
                    n.version as namespace_version,
                    concat_namespace_name_tabular_name(tabular_namespace_name, name) <-> $2 AS distance
                FROM tabular t
                INNER JOIN warehouse w ON w.warehouse_id = t.warehouse_id
                INNER JOIN namespace n ON n.namespace_id = t.namespace_id AND n.warehouse_id = t.warehouse_id
                WHERE t.warehouse_id = $1
                    AND w.status = 'active'
                    AND t.deleted_at IS NULL
                    AND t.metadata_location IS NOT NULL
                ORDER BY distance ASC
                LIMIT 10
            ),
            filtered_tabulars AS (
                SELECT * FROM selected_tabulars
                WHERE distance < 1.0
            ),
            selected_views AS (
                SELECT tabular_id FROM filtered_tabulars WHERE typ = 'view'
            ),
            selected_tables AS (
                SELECT tabular_id FROM filtered_tabulars WHERE typ = 'table'
            )
            SELECT st.tabular_id,
                st.namespace_id,
                st.namespace_name,
                st.namespace_version,
                st.tabular_name,
                st.typ as "typ: TabularType",
                st.metadata_location,
                st.updated_at,
                st.protected,
                st.distance,
                st.fs_location,
                st.fs_protocol,
                st.warehouse_version,
                vp.view_properties_keys,
                vp.view_properties_values,
                tp.keys as table_properties_keys,
                tp.values as table_properties_values
            FROM filtered_tabulars st
            LEFT JOIN (SELECT view_id,
                        ARRAY_AGG(key)   AS view_properties_keys,
                        ARRAY_AGG(value) AS view_properties_values
                FROM view_properties
                WHERE warehouse_id = $1 and view_id in (SELECT tabular_id FROM selected_views)
                GROUP BY view_id) vp ON st.tabular_id = vp.view_id
            LEFT JOIN (SELECT table_id,
                        ARRAY_AGG(key) as keys,
                        ARRAY_AGG(value) as values
                    FROM table_properties
                    WHERE warehouse_id = $1 AND table_id in (SELECT tabular_id FROM selected_tables)
                    GROUP BY table_id) tp ON st.tabular_id = tp.table_id
            ORDER BY distance ASC
            "#,
            *warehouse_id,
            search_term,
        )
        .fetch_all(connection)
        .await
        .map_err(super::dbutils::DBErrorHandler::into_catalog_backend_error)?
        .into_iter()
        .map(|row| row.into_search_tabular(warehouse_id))
        .collect::<Result<Vec<_>, _>>()?,
    };

    Ok(CatalogSearchTabularResponse {
        search_results: tabulars,
    })
}

impl From<FromTabularRowError> for RenameTabularError {
    fn from(err: FromTabularRowError) -> Self {
        match err {
            FromTabularRowError::InvalidNamespaceIdentifier(e) => e.into(),
            FromTabularRowError::InternalParseLocationError(e) => e.into(),
        }
    }
}

/// Rename a tabular. Tabulars may be moved across namespaces.
#[allow(clippy::too_many_lines)]
pub(crate) async fn rename_tabular(
    warehouse_id: WarehouseId,
    source_id: TabularId,
    source: &TableIdent,
    destination: &TableIdent,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<ViewOrTableInfo, RenameTabularError> {
    let TableIdent {
        namespace: source_namespace,
        name: source_name,
    } = source;
    let TableIdent {
        namespace: dest_namespace,
        name: dest_name,
    } = destination;

    let row = if source_namespace == dest_namespace {
        sqlx::query_as!(
            TabularRow,
            r#"
            WITH locked_tabular AS (
                SELECT tabular_id, name, namespace_id, typ
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
            ),
            updated AS (
                UPDATE tabular t
                SET name = $1
                FROM locked_tabular lt, warehouse_check wc, locked_source_namespace lsn
                WHERE t.tabular_id = lt.tabular_id
                    AND t.warehouse_id = $4
                    AND wc.warehouse_id = $4
                    AND lsn.namespace_id IS NOT NULL
                    AND NOT EXISTS (SELECT 1 FROM conflict_check)
                RETURNING 
                    t.tabular_id,
                    t.namespace_id,
                    t.name as tabular_name,
                    t.tabular_namespace_name as namespace_name,
                    t.typ,
                    t.metadata_location,
                    t.updated_at,
                    t.protected,
                    t.fs_location,
                    t.fs_protocol
            ),
            selected_views AS (
                SELECT tabular_id FROM updated WHERE typ = 'view'
            ),
            selected_tables AS (
                SELECT tabular_id FROM updated WHERE typ = 'table'
            )
            SELECT u.tabular_id,
                w.version as warehouse_version,
                u.namespace_name,
                n.version as namespace_version,
                u.namespace_id,
                u.tabular_name,
                u.updated_at,
                u.metadata_location,
                u.protected,
                u.typ as "typ: TabularType",
                u.fs_location,
                u.fs_protocol,
                vp.view_properties_keys,
                vp.view_properties_values,
                tp.keys as table_properties_keys,
                tp.values as table_properties_values
            FROM updated u
            INNER JOIN warehouse w ON w.warehouse_id = $4
            INNER JOIN namespace n ON n.namespace_id = u.namespace_id AND n.warehouse_id = $4
            LEFT JOIN (SELECT view_id,
                        ARRAY_AGG(key)   AS view_properties_keys,
                        ARRAY_AGG(value) AS view_properties_values
                FROM view_properties
                WHERE warehouse_id = $4 and view_id in (SELECT tabular_id FROM selected_views)
                GROUP BY view_id) vp ON u.tabular_id = vp.view_id
            LEFT JOIN (SELECT table_id,
                        ARRAY_AGG(key) as keys,
                        ARRAY_AGG(value) as values
                    FROM table_properties
                    WHERE warehouse_id = $4 AND table_id in (SELECT tabular_id FROM selected_tables)
                    GROUP BY table_id) tp ON u.tabular_id = tp.table_id
            "#,
            &**dest_name,
            *source_id,
            TabularType::from(source_id) as _,
            *warehouse_id,
        )
        .fetch_one(&mut **transaction)
        .await
        .map_err(|e| match e {
            sqlx::Error::RowNotFound => RenameTabularError::from(TabularNotFound::new(
            warehouse_id, source_id
        )),
            _ => e.into_catalog_backend_error().into(),
        })?
    } else {
        sqlx::query_as!(
            TabularRow,
            r#"
            WITH locked_tabular AS (
                SELECT tabular_id, name, namespace_id, typ
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
            ),
            updated AS (
                UPDATE tabular t
                SET name = $1, namespace_id = ln.namespace_id, tabular_namespace_name = $3
                FROM locked_tabular lt, locked_namespace ln, locked_source_namespace lsn, warehouse_check wc
                    WHERE t.tabular_id = lt.tabular_id
                    AND t.warehouse_id = $2
                    AND ln.namespace_id IS NOT NULL
                    AND wc.warehouse_id = $2
                    AND lsn.namespace_id IS NOT NULL
                    AND NOT EXISTS (SELECT 1 FROM conflict_check)
                RETURNING t.tabular_id,
                    t.namespace_id,
                    t.name as tabular_name,
                    t.tabular_namespace_name as namespace_name,
                    t.typ,
                    t.metadata_location,
                    t.updated_at,
                    t.protected,
                    t.fs_location,
                    t.fs_protocol
            ),
            selected_views AS (
                SELECT tabular_id FROM updated WHERE typ = 'view'
            ),
            selected_tables AS (
                SELECT tabular_id FROM updated WHERE typ = 'table'
            )
            SELECT u.tabular_id,
                w.version as warehouse_version,
                u.namespace_name,
                n.version as namespace_version,
                u.namespace_id,
                u.tabular_name,
                u.updated_at,
                u.metadata_location,
                u.protected,
                u.typ as "typ: TabularType",
                u.fs_location,
                u.fs_protocol,
                vp.view_properties_keys,
                vp.view_properties_values,
                tp.keys as table_properties_keys,
                tp.values as table_properties_values
            FROM updated u
            INNER JOIN warehouse w ON w.warehouse_id = $2
            INNER JOIN namespace n ON n.namespace_id = u.namespace_id AND n.warehouse_id = $2
            LEFT JOIN (SELECT view_id,
                        ARRAY_AGG(key)   AS view_properties_keys,
                        ARRAY_AGG(value) AS view_properties_values
                FROM view_properties
                WHERE warehouse_id = $2 and view_id in (SELECT tabular_id FROM selected_views)
                GROUP BY view_id) vp ON u.tabular_id = vp.view_id
            LEFT JOIN (SELECT table_id,
                        ARRAY_AGG(key) as keys,
                        ARRAY_AGG(value) as values
                    FROM table_properties
                    WHERE warehouse_id = $2 AND table_id in (SELECT tabular_id FROM selected_tables)
                    GROUP BY table_id) tp ON u.tabular_id = tp.table_id
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
            sqlx::Error::RowNotFound => RenameTabularError::from(TabularNotFound::new(
            warehouse_id, source_id
        ).append_detail("Either the source tabular or the destination namespace could not be found.")),
            _ => e.into_catalog_backend_error().into(),
        })?
    };

    let tabular_info = row.try_into_table_or_view(warehouse_id)?;

    Ok(tabular_info)
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

impl From<FromTabularRowError> for ClearTabularDeletedAtError {
    fn from(err: FromTabularRowError) -> Self {
        match err {
            FromTabularRowError::InvalidNamespaceIdentifier(e) => e.into(),
            FromTabularRowError::InternalParseLocationError(e) => e.into(),
        }
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn clear_tabular_deleted_at(
    tabular_ids: &[TabularId],
    warehouse_id: WarehouseId,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Vec<ViewOrTableDeletionInfo>, ClearTabularDeletedAtError> {
    let tabular_ids_uuid: Vec<Uuid> = tabular_ids.iter().map(|id| **id).collect();
    let undrop_tabular_informations = sqlx::query_as!(
        TabularRowWithDeletion,
        r#"WITH locked_tabulars AS (
            SELECT t.tabular_id, t.name, t.namespace_id, n.namespace_name, t.typ
            FROM tabular t 
            JOIN namespace n ON t.namespace_id = n.namespace_id
            WHERE n.warehouse_id = $2
                AND t.warehouse_id = $2
                AND t.tabular_id = ANY($1::uuid[])
            FOR UPDATE OF t
        ),
        locked_tasks AS (
            SELECT task_id, entity_id, scheduled_for
            FROM task ta
            JOIN locked_tabulars lt ON ta.entity_id = lt.tabular_id
            WHERE ta.entity_type in ('table', 'view')
                AND ta.warehouse_id = $2
                AND ta.queue_name = 'tabular_expiration'
            FOR UPDATE OF ta
        ),
        updated AS (
            UPDATE tabular t
            SET deleted_at = NULL
            FROM locked_tabulars lt
            LEFT JOIN locked_tasks lta ON lt.tabular_id = lta.entity_id
            WHERE t.tabular_id = lt.tabular_id AND t.warehouse_id = $2
            RETURNING
                t.tabular_id,
                t.name as tabular_name,
                t.tabular_namespace_name as namespace_name,
                t.namespace_id,
                t.metadata_location,
                t.typ,
                t.updated_at,
                t.created_at,
                t.deleted_at,
                lta.scheduled_for as deletion_scheduled_for,
                lta.task_id as deletion_task_id,
                t.protected,
                t.fs_location,
                t.fs_protocol
        ),
        selected_views AS (
            SELECT tabular_id FROM updated WHERE typ = 'view'
        ),
        selected_tables AS (
            SELECT tabular_id FROM updated WHERE typ = 'table'
        )
        SELECT u.tabular_id,
            u.namespace_name,
            u.namespace_id,
            u.tabular_name,
            u.updated_at,
            u.metadata_location,
            u.protected,
            u.typ as "typ: TabularType",
            u.fs_location,
            u.fs_protocol,
            u.created_at,
            u.deleted_at,
            u.deletion_scheduled_for as "deletion_scheduled_for?",
            u.deletion_task_id as "deletion_task_id?",
            n.version as namespace_version,
            w.version as warehouse_version,
            vp.view_properties_keys,
            vp.view_properties_values,
            tp.keys as table_properties_keys,
            tp.values as table_properties_values
        FROM updated u
        INNER JOIN warehouse w ON w.warehouse_id = $2
        INNER JOIN namespace n ON n.namespace_id = u.namespace_id AND n.warehouse_id = $2
        LEFT JOIN (SELECT view_id,
                    ARRAY_AGG(key)   AS view_properties_keys,
                    ARRAY_AGG(value) AS view_properties_values
            FROM view_properties
            WHERE warehouse_id = $2 and view_id in (SELECT tabular_id FROM selected_views)
            GROUP BY view_id) vp ON u.tabular_id = vp.view_id
        LEFT JOIN (SELECT table_id,
                    ARRAY_AGG(key) as keys,
                    ARRAY_AGG(value) as values
                FROM table_properties
                WHERE warehouse_id = $2 AND table_id in (SELECT tabular_id FROM selected_tables)
                GROUP BY table_id) tp ON u.tabular_id = tp.table_id
        "#,
        &tabular_ids_uuid,
        *warehouse_id,
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(|e| {
        tracing::warn!("Error marking tabular as undeleted: {e}");
        match &e {
            sqlx::Error::Database(db_err)
                if db_err.constraint() == Some("unique_name_per_namespace_id") =>
            {
                ClearTabularDeletedAtError::from(TabularAlreadyExists::new())
            }
            _ => e.into_catalog_backend_error().into(),
        }
    })?;

    let found_ids = undrop_tabular_informations
        .iter()
        .map(|r| r.tabular_id)
        .collect::<std::collections::HashSet<Uuid>>();
    if let Some(missing_id) = tabular_ids.iter().find(|id| !found_ids.contains(&**id)) {
        return Err(TabularNotFound::new(warehouse_id, *missing_id).into());
    }

    undrop_tabular_informations
        .into_iter()
        .map(|undrop_tabular_information| {
            undrop_tabular_information
                .try_into_table_or_view(warehouse_id)
                .map_err(Into::into)
        })
        .collect()
}

impl From<FromTabularRowError> for MarkTabularAsDeletedError {
    fn from(err: FromTabularRowError) -> Self {
        match err {
            FromTabularRowError::InvalidNamespaceIdentifier(e) => e.into(),
            FromTabularRowError::InternalParseLocationError(e) => e.into(),
        }
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn mark_tabular_as_deleted(
    warehouse_id: WarehouseId,
    tabular_id: TabularId,
    force: bool,
    delete_date: Option<chrono::DateTime<Utc>>,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<ViewOrTableInfo, MarkTabularAsDeletedError> {
    let r = sqlx::query_as!(
        TabularRow,
        r#"
        WITH locked_tabular AS (
            SELECT 
                tabular_id,
                namespace_id,
                name,
                tabular_namespace_name,
                typ,
                metadata_location,
                updated_at,
                protected,
                fs_location,
                fs_protocol
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
        ),
        result_tabulars AS (
            SELECT 
                lt.tabular_id,
                lt.namespace_id,
                lt.name as tabular_name,
                lt.tabular_namespace_name as namespace_name,
                lt.typ,
                lt.metadata_location,
                lt.updated_at,
                lt.protected,
                lt.fs_location,
                lt.fs_protocol,
                (SELECT tabular_id FROM marked) IS NOT NULL as was_marked
            FROM locked_tabular lt
        ),
        selected_views AS (
            SELECT tabular_id FROM result_tabulars WHERE typ = 'view'
        ),
        selected_tables AS (
            SELECT tabular_id FROM result_tabulars WHERE typ = 'table'
        )
        SELECT 
            rt.tabular_id,
            w.version as warehouse_version,
            rt.namespace_name,
            n.version as namespace_version,
            rt.namespace_id,
            rt.tabular_name,
            rt.updated_at,
            rt.metadata_location,
            rt.protected,
            rt.typ as "typ: TabularType",
            rt.fs_location,
            rt.fs_protocol,
            vp.view_properties_keys,
            vp.view_properties_values,
            tp.keys as table_properties_keys,
            tp.values as table_properties_values
        FROM result_tabulars rt
        INNER JOIN warehouse w ON w.warehouse_id = $1
        INNER JOIN namespace n ON n.namespace_id = rt.namespace_id AND n.warehouse_id = $1
        LEFT JOIN (SELECT view_id,
                    ARRAY_AGG(key)   AS view_properties_keys,
                    ARRAY_AGG(value) AS view_properties_values
            FROM view_properties
            WHERE warehouse_id = $1 and view_id in (SELECT tabular_id FROM selected_views)
            GROUP BY view_id) vp ON rt.tabular_id = vp.view_id
        LEFT JOIN (SELECT table_id,
                    ARRAY_AGG(key) as keys,
                    ARRAY_AGG(value) as values
                FROM table_properties
                WHERE warehouse_id = $1 AND table_id in (SELECT tabular_id FROM selected_tables)
                GROUP BY table_id) tp ON rt.tabular_id = tp.table_id
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
            MarkTabularAsDeletedError::from(TabularNotFound::new(warehouse_id, tabular_id))
        } else {
            e.into_catalog_backend_error().into()
        }
    })?;
    if r.protected && !force {
        return Err(ProtectedTabularDeletionWithoutForce::new(warehouse_id, tabular_id).into());
    }

    let tabular_info = r.try_into_table_or_view(warehouse_id)?;
    Ok(tabular_info)
}

pub(crate) async fn drop_tabular(
    warehouse_id: WarehouseId,
    tabular_id: TabularId,
    force: bool,
    required_metadata_location: Option<&Location>,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Location, DropTabularError> {
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
            DropTabularError::from(TabularNotFound::new(warehouse_id, tabular_id))
        } else {
            e.into_catalog_backend_error().into()
        }
    })?;

    tracing::debug!(
        "Dropped Tabular with ID {tabular_id}. Protected: {}, Location: {:?}, Protocol: {:?}",
        location.protected,
        location.fs_location,
        location.fs_protocol
    );

    if location.protected && !force {
        return Err(ProtectedTabularDeletionWithoutForce::new(warehouse_id, tabular_id).into());
    }

    if let Some(required_metadata_location) = required_metadata_location {
        if location.metadata_location != Some(required_metadata_location.to_string()) {
            return Err(ConcurrentUpdateError::new(warehouse_id, tabular_id).into());
        }
    }

    debug_assert!(
        location.was_deleted,
        "If we didn't delete anything, we should have errored out earlier"
    );
    let location = join_location(&location.fs_protocol, &location.fs_location)
        .map_err(InternalParseLocationError::from)?;
    Ok(location)
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

fn prepare_properties(
    keys: Option<Vec<String>>,
    values: Option<Vec<String>>,
) -> HashMap<String, String> {
    if let (Some(keys), Some(values)) = (keys, values) {
        keys.into_iter().zip(values).collect()
    } else {
        HashMap::new()
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr as _;

    use lakekeeper_io::Location;
    use uuid::Uuid;

    use super::*;
    use crate::{
        implementations::postgres::{
            namespace::tests::initialize_namespace, warehouse::test::initialize_warehouse,
            CatalogState,
        },
        service::AuthZTableInfo,
    };

    pub(super) async fn setup_test_tabular(pool: &sqlx::PgPool, protected: bool) -> TableInfo {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace =
            iceberg_ext::NamespaceIdent::from_vec(vec!["test_namespace".to_string()]).unwrap();
        let response = initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;
        let namespace_id = response.namespace_id();

        let table_name = format!("test_table_{}", Uuid::now_v7());
        let location = Location::from_str(&format!("s3://test-bucket/{table_name}/")).unwrap();
        let metadata_location =
            Location::from_str(&format!("s3://test-bucket/{table_name}/metadata/v1.json")).unwrap();

        let mut transaction = pool.begin().await.unwrap();

        let table_id = Uuid::now_v7();
        let tabular_info = create_tabular(
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
        assert_eq!(tabular_info.tabular_id(), TabularId::Table(table_id.into()));

        // Set protection status if needed
        if protected {
            set_tabular_protected(
                warehouse_id,
                tabular_info.tabular_id(),
                true,
                &mut transaction,
            )
            .await
            .unwrap();
        }

        transaction.commit().await.unwrap();

        tabular_info.into_table_info().unwrap()
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
        assert!(matches!(error, DropTabularError::TabularNotFound(_)));
    }

    #[sqlx::test]
    async fn test_drop_tabular_protected_table_without_force_returns_protected_error(
        pool: sqlx::PgPool,
    ) {
        let table_info = setup_test_tabular(&pool, true).await;

        let mut transaction = pool.begin().await.unwrap();

        let result = drop_tabular(
            table_info.warehouse_id,
            table_info.table_id().into(),
            false, // force = false
            Some(&table_info.metadata_location.unwrap()),
            &mut transaction,
        )
        .await;

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(matches!(
            error,
            DropTabularError::ProtectedTabularDeletionWithoutForce(_)
        ));
    }

    #[sqlx::test]
    async fn test_drop_tabular_protected_table_with_force_succeeds(pool: sqlx::PgPool) {
        let table_info = setup_test_tabular(&pool, true).await;

        let mut transaction = pool.begin().await.unwrap();

        let result = drop_tabular(
            table_info.warehouse_id,
            table_info.table_id().into(),
            true, // force = true
            Some(&table_info.metadata_location.unwrap()),
            &mut transaction,
        )
        .await;

        assert!(result.is_ok());
        let location = result.unwrap();
        assert!(location.as_str().starts_with("s3://test-bucket/"));
    }

    #[sqlx::test]
    async fn test_drop_tabular_concurrent_update_error_wrong_metadata_location(pool: sqlx::PgPool) {
        let table_info = setup_test_tabular(&pool, false).await;

        let wrong_metadata_location =
            Location::from_str("s3://wrong-bucket/wrong/metadata/v1.json").unwrap();

        let mut transaction = pool.begin().await.unwrap();

        let result = drop_tabular(
            table_info.warehouse_id,
            table_info.table_id().into(),
            false,
            Some(&wrong_metadata_location),
            &mut transaction,
        )
        .await;

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(matches!(error, DropTabularError::ConcurrentUpdateError(_)));
    }

    #[sqlx::test]
    async fn test_drop_tabular_with_correct_metadata_location_succeeds(pool: sqlx::PgPool) {
        let table_info = setup_test_tabular(&pool, false).await;

        let mut transaction = pool.begin().await.unwrap();

        let result = drop_tabular(
            table_info.warehouse_id,
            table_info.table_id().into(),
            false,
            Some(&table_info.metadata_location.unwrap()),
            &mut transaction,
        )
        .await;

        assert!(result.is_ok());
        let location = result.unwrap();
        assert!(location.as_str().starts_with("s3://test-bucket/"));
    }

    #[sqlx::test]
    async fn test_drop_tabular_without_metadata_location_check_succeeds(pool: sqlx::PgPool) {
        let table_info = setup_test_tabular(&pool, false).await;

        let mut transaction = pool.begin().await.unwrap();

        let result = drop_tabular(
            table_info.warehouse_id,
            table_info.table_id().into(),
            false,
            None, // No metadata location check
            &mut transaction,
        )
        .await;

        assert!(result.is_ok());
        let location = result.unwrap();
        assert!(location.as_str().starts_with("s3://test-bucket/"));
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
        assert!(matches!(error, DropTabularError::TabularNotFound(_)));
    }

    #[sqlx::test]
    async fn test_drop_tabular_inactive_warehouse_returns_404(pool: sqlx::PgPool) {
        let table_info = setup_test_tabular(&pool, false).await;

        // Deactivate the warehouse
        let mut transaction = pool.begin().await.unwrap();
        crate::implementations::postgres::warehouse::set_warehouse_status(
            table_info.warehouse_id,
            crate::api::management::v1::warehouse::WarehouseStatus::Inactive,
            &mut transaction,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = pool.begin().await.unwrap();

        let result = drop_tabular(
            table_info.warehouse_id,
            table_info.table_id().into(),
            false,
            Some(&table_info.metadata_location.unwrap()),
            &mut transaction,
        )
        .await;

        let error = result.unwrap_err();
        assert!(matches!(error, DropTabularError::TabularNotFound(_)));
    }

    #[sqlx::test]
    async fn test_search_tabular_no_results(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;

        let res = search_tabular(
            warehouse_id,
            "non_existent_table",
            &state.read_write.read_pool,
        )
        .await
        .unwrap();

        assert!(res.search_results.is_empty());
    }

    #[sqlx::test]
    async fn test_search_tabular(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace1 = iceberg_ext::NamespaceIdent::from_vec(vec!["hr_ns".to_string()]).unwrap();
        let namespace1_id = initialize_namespace(state.clone(), warehouse_id, &namespace1, None)
            .await
            .namespace_id();
        let namespace2 =
            iceberg_ext::NamespaceIdent::from_vec(vec!["finance_ns".to_string()]).unwrap();
        let namespace2_id = initialize_namespace(state.clone(), warehouse_id, &namespace2, None)
            .await
            .namespace_id();

        let table_names = [10, 101, 1011, 42, 420]
            .into_iter()
            .map(|i| format!("test_region_{i}"))
            .collect::<Vec<_>>();

        let mut best_match_info = None; // will store id of the tabular we'll search for
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
                    best_match_info = Some(tabular_id);
                }
            }
        }

        let best_match_info = best_match_info.unwrap();
        let res = search_tabular(warehouse_id, "finance.table42", &state.read_write.read_pool)
            .await
            .unwrap()
            .search_results[0]
            .clone();

        // Assert the best match is returned as first result.
        assert_eq!(res.tabular.tabular_id(), best_match_info.tabular_id());
        assert_eq!(
            res.tabular.tabular_ident().namespace.clone().inner(),
            vec!["finance_ns".to_string()]
        );
        assert_eq!(res.tabular.tabular_ident().name, "test_region_42");
    }

    #[sqlx::test]
    async fn test_search_tabular_by_uuid(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = iceberg_ext::NamespaceIdent::from_vec(vec!["hr_ns".to_string()]).unwrap();
        let namespace_id = initialize_namespace(state.clone(), warehouse_id, &namespace, None)
            .await
            .namespace_id();

        let table_names = [10, 101, 1011, 42, 420]
            .into_iter()
            .map(|i| format!("test_region_{i}"))
            .collect::<Vec<_>>();

        let mut to_search = None; // will store id of the tabular we'll search for
        for tn in &table_names {
            let mut transaction = pool.begin().await.unwrap();
            let table_id = Uuid::now_v7();
            let location =
                Location::from_str(&format!("s3://test-bucket/{namespace_id}/{tn}/")).unwrap();
            let metadata_location = Location::from_str(&format!(
                "s3://test-bucket/{namespace_id}/{tn}/metadata/v1.json"
            ))
            .unwrap();
            let tabular_info = create_tabular(
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
                to_search = Some(tabular_info);
            }
        }

        let to_search = to_search.unwrap();
        let results = search_tabular(
            warehouse_id,
            &(*to_search.tabular_id()).to_string(),
            &state.read_write.read_pool,
        )
        .await
        .unwrap()
        .search_results;
        assert_eq!(results.len(), 1);
        let res = &results[0];

        // Assert the tabular with matching uuid is returned
        assert_eq!(res.tabular.tabular_id(), to_search.tabular_id());
        assert_eq!(
            res.tabular.tabular_ident().namespace.clone().inner(),
            vec!["hr_ns".to_string()]
        );
        assert_eq!(res.tabular.tabular_ident().name, "test_region_42");
    }
}
