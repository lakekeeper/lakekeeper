pub mod generic_table;
mod load_by_location;
mod protection;
pub mod table;
pub mod view;

use std::{collections::HashMap, default::Default, fmt::Debug, str::FromStr as _};

use chrono::Utc;
use lakekeeper::{
    CONFIG, WarehouseId,
    api::iceberg::v1::{PaginatedMapping, PaginationQuery},
    service::{
        CatalogSearchTabularInfo, CatalogSearchTabularResponse, ClearTabularDeletedAtError,
        ConcurrentUpdateError, CreateTabularError, DropTabularError, ExpirationTaskInfo,
        GenericTableDeletionInfo, GenericTabularInfo, GetTabularInfoError,
        InternalParseLocationError, InvalidNamespaceIdentifier, ListTabularsError,
        LocationAlreadyTaken, MarkTabularAsDeletedError, NamespaceId,
        ProtectedTabularDeletionWithoutForce, RenameTabularError, SearchTabularError,
        SerializationError, TableDeletionInfo, TableIdent, TableInfo, TabularAlreadyExists,
        TabularId, TabularIdentBorrowed, TabularNotFound, ViewDeletionInfo, ViewInfo,
        ViewOrTableDeletionInfo, ViewOrTableInfo, storage::join_location,
    },
};
use lakekeeper_io::Location;
pub(crate) use load_by_location::*;
pub(crate) use protection::set_tabular_protected;
use sqlx::FromRow;
use uuid::Uuid;
use xxhash_rust::xxh3::Xxh3Default;

use super::dbutils::DBErrorHandler as _;
use crate::{
    namespace::parse_namespace_identifier_from_vec,
    pagination::{PaginateToken, V1PaginateToken},
};

#[derive(Debug, sqlx::Type, Copy, Clone, PartialEq, Eq, strum::Display)]
#[sqlx(type_name = "tabular_type", rename_all = "kebab-case")]
pub(crate) enum TabularType {
    Table,
    View,
    GenericTable,
}

impl From<lakekeeper::api::management::v1::TabularType> for TabularType {
    fn from(typ: lakekeeper::api::management::v1::TabularType) -> Self {
        match typ {
            lakekeeper::api::management::v1::TabularType::Table => TabularType::Table,
            lakekeeper::api::management::v1::TabularType::View => TabularType::View,
            lakekeeper::api::management::v1::TabularType::GenericTable => TabularType::GenericTable,
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

/// Tabular row without per-tabular-type properties.
///
/// Used by writes (create / commit / rename) that already know the new
/// property map from the in-memory metadata and would otherwise have to pad
/// the SELECT with `NULL::text[]` columns just to satisfy the row decoder.
/// Reads that genuinely return properties from the DB use
/// [`TabularRowWithProperties`] instead.
#[derive(Debug, FromRow)]
pub(super) struct TabularRowCore {
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
}

impl TabularRowCore {
    pub(super) fn try_into_table_or_view(
        self,
        warehouse_id: WarehouseId,
    ) -> Result<ViewOrTableInfo, FromTabularRowError> {
        self.try_into_table_or_view_with_properties(warehouse_id, HashMap::new())
    }

    fn try_into_table_or_view_with_properties(
        self,
        warehouse_id: WarehouseId,
        properties: HashMap<String, String>,
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
                properties,
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
                properties,
                namespace_version: self.namespace_version.into(),
                warehouse_version: self.warehouse_version.into(),
            }),
            TabularType::GenericTable => ViewOrTableInfo::GenericTable(GenericTabularInfo {
                namespace_id: self.namespace_id.into(),
                tabular_ident,
                warehouse_id,
                tabular_id: self.tabular_id.into(),
                protected: self.protected,
                metadata_location,
                updated_at: self.updated_at,
                location,
                properties,
                namespace_version: self.namespace_version.into(),
                warehouse_version: self.warehouse_version.into(),
            }),
        };

        Ok(view_or_table_info)
    }
}

/// Tabular row that also carries the table/view property arrays selected via
/// LEFT JOIN. Use this for queries that need to hydrate properties from the DB
/// (e.g. listing / lookup / rename). Writes that overlay properties from
/// in-memory metadata should use [`TabularRowCore`] instead.
#[derive(Debug, FromRow)]
pub(super) struct TabularRowWithProperties {
    tabular_id: Uuid,
    warehouse_version: i64,
    namespace_name: Vec<String>,
    namespace_version: i64,
    namespace_id: Uuid,
    tabular_name: String,
    updated_at: Option<chrono::DateTime<Utc>>,
    metadata_location: Option<String>,
    protected: bool,
    #[sqlx(rename = "typ: TabularType")]
    typ: TabularType,
    fs_location: String,
    fs_protocol: String,
    view_properties_keys: Option<Vec<String>>,
    view_properties_values: Option<Vec<String>>,
    table_properties_keys: Option<Vec<String>>,
    table_properties_values: Option<Vec<String>>,
    generic_table_properties_keys: Option<Vec<String>>,
    generic_table_properties_values: Option<Vec<String>>,
}

impl TabularRowWithProperties {
    pub(super) fn try_into_table_or_view(
        self,
        warehouse_id: WarehouseId,
    ) -> Result<ViewOrTableInfo, FromTabularRowError> {
        let properties = match self.typ {
            TabularType::Table => {
                prepare_properties(self.table_properties_keys, self.table_properties_values)
            }
            TabularType::View => {
                prepare_properties(self.view_properties_keys, self.view_properties_values)
            }
            TabularType::GenericTable => prepare_properties(
                self.generic_table_properties_keys,
                self.generic_table_properties_values,
            ),
        };
        let core = TabularRowCore {
            tabular_id: self.tabular_id,
            warehouse_version: self.warehouse_version,
            namespace_name: self.namespace_name,
            namespace_version: self.namespace_version,
            namespace_id: self.namespace_id,
            tabular_name: self.tabular_name,
            updated_at: self.updated_at,
            metadata_location: self.metadata_location,
            protected: self.protected,
            typ: self.typ,
            fs_location: self.fs_location,
            fs_protocol: self.fs_protocol,
        };
        core.try_into_table_or_view_with_properties(warehouse_id, properties)
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn get_tabular_infos_by_ids<'e, 'c: 'e, E>(
    warehouse_id: WarehouseId,
    tabulars: &[TabularId],
    list_flags: lakekeeper::service::TabularListFlags,
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
                TabularId::GenericTable(id) => {
                    t_ids.push(**id);
                    t_typs.push(TabularType::GenericTable);
                }
            }
            (t_ids, t_typs)
        },
    );

    let rows = sqlx::query_as!(
        TabularRowWithProperties,
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
                AND (t.metadata_location is not NULL OR $5 OR t.typ = 'generic-table')
        ),
        selected_views AS (
            SELECT tabular_id FROM selected_tabulars WHERE typ = 'view'
        ),
        selected_tables AS (
            SELECT tabular_id FROM selected_tabulars WHERE typ = 'table'
        ),
        selected_generic_tables AS (
            SELECT tabular_id FROM selected_tabulars WHERE typ = 'generic-table'
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
               tp.values as table_properties_values,
               gtp.keys as generic_table_properties_keys,
               gtp.values as generic_table_properties_values
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
        LEFT JOIN (SELECT generic_table_id,
                    ARRAY_AGG(key) as keys,
                    ARRAY_AGG(value) as values
                FROM generic_table_properties
                WHERE warehouse_id = $1 AND generic_table_id in (SELECT tabular_id FROM selected_generic_tables)
                GROUP BY generic_table_id) gtp ON st.tabular_id = gtp.generic_table_id
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
    list_flags: lakekeeper::service::TabularListFlags,
    catalog_state: E,
) -> Result<HashMap<TableIdent, ViewOrTableInfo>, GetTabularInfoError>
where
    E: 'e + sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    if tabulars.is_empty() {
        return Ok(HashMap::new());
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
        TabularRowWithProperties,
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
                AND (t.metadata_location is not NULL OR $6 OR t.typ = 'generic-table')
        ),
        selected_views AS (
            SELECT tabular_id FROM selected_tabulars WHERE typ = 'view'
        ),
        selected_tables AS (
            SELECT tabular_id FROM selected_tabulars WHERE typ = 'table'
        ),
        selected_generic_tables AS (
            SELECT tabular_id FROM selected_tabulars WHERE typ = 'generic-table'
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
               tp.values as table_properties_values,
               gtp.keys as generic_table_properties_keys,
               gtp.values as generic_table_properties_values
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
        LEFT JOIN (SELECT generic_table_id,
                    ARRAY_AGG(key) as keys,
                    ARRAY_AGG(value) as values
                FROM generic_table_properties
                WHERE warehouse_id = $1 AND generic_table_id in (SELECT tabular_id FROM selected_generic_tables)
                GROUP BY generic_table_id) gtp ON st.tabular_id = gtp.generic_table_id
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
            let info = row.try_into_table_or_view(warehouse_id)?;
            Ok((info.tabular_ident().clone(), info))
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

/// Advisory-lock keys covering `location` and every path above it inside its
/// bucket, given the partial locations from [`get_partial_fs_locations`].
///
/// Two locations collide only when one is a path prefix of the other, and in that
/// case the shorter one's full path is among the longer one's prefixes -- so
/// locking a location together with its prefixes makes any two colliding creates
/// share a key and serialize. Without that they do not: the check is a read, and
/// two transactions can both pass it and both commit, leaving one tabular's data
/// inside another's location. Nothing else prevents this; no index on
/// `fs_location` is unique.
///
/// The bare authority is left out, and no collision needs it: a location is
/// admitted only as a strict sublocation of the warehouse's base location, and that
/// base names at least a bucket, so no tabular sits at a bare authority and the
/// authority is the ancestor of none of them. Including it would instead give every
/// create in a bucket one key in common and serialize them all. Locations that
/// share only a bucket do not block each other, ones that share a directory do.
///
/// Sorted and deduplicated so two transactions holding overlapping sets acquire
/// them in the same order and cannot deadlock. `xxh3` because the value has to be
/// the same in every replica and across versions; a collision only makes two
/// unrelated creates wait for each other.
fn location_lock_keys(warehouse_id: Uuid, partial_locations: &[String]) -> Vec<i64> {
    let mut keys: Vec<i64> = partial_locations
        .iter()
        .filter(|location| location.contains('/'))
        .map(|location| {
            let mut hasher = Xxh3Default::new();
            hasher.update(warehouse_id.as_bytes());
            hasher.update(&[0]);
            hasher.update(location.as_bytes());
            i64::from_ne_bytes(hasher.digest().to_ne_bytes())
        })
        .collect();
    keys.sort_unstable();
    keys.dedup();
    keys
}

/// The value the `fs_location` column holds for `location`.
///
/// No writer produces a trailing slash today: the create paths normalize through
/// `determine_tabular_location`, and iceberg's own metadata types trim a location
/// on their way in (`TableMetadata::try_normalize`,
/// `ViewMetadataBuilder::set_location`). So this changes nothing at runtime -- it
/// is here because `Location` still permits one, and the column's two comparisons
/// fail differently if one arrives: a slash on the stored side leaves the row
/// unequal to every trimmed candidate, hiding the ancestor match, and a slash on
/// the probe side shifts the descendant range to `["X//", "X/0")`, which nothing
/// falls in. Enforced for stored values by
/// `tabular_fs_location_no_trailing_slash`.
pub(crate) fn fs_location_of(location: &Location) -> &str {
    location.authority_and_path().trim_end_matches('/')
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
            Ok(fs_location_of(&location).to_string())
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

/// Errors with `LocationAlreadyTaken` if any other tabular in `warehouse_id`
/// occupies `location`, sits under it, or contains it.
///
/// This check is on its own: no index on `fs_location` is unique, so nothing at
/// the schema level backstops it. It is also an unlocked read, so two concurrent
/// creates can both pass it and both commit.
///
/// Runs on create and on every view commit -- a view can move to a new location,
/// where a table's `SetLocation` to a different value is refused.
pub(crate) async fn ensure_location_available(
    warehouse_id: Uuid,
    self_tabular_id: Uuid,
    location: &Location,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), CreateTabularError> {
    let partial_locations = get_partial_fs_locations(location)?;

    // Held until this transaction ends, so a concurrent create at a colliding
    // location waits here and then sees the committed row below instead of passing
    // the same check against the same absent row.
    let lock_keys = location_lock_keys(warehouse_id, &partial_locations);
    sqlx::query!(
        "SELECT pg_advisory_xact_lock(key) FROM unnest($1::bigint[]) AS key",
        &lock_keys
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| {
        e.into_catalog_backend_error()
            .append_detail("Error locking the location for a conflict check")
    })?;

    // A trailing slash on either side hides a match: it would make the descendant
    // range `["X//", "X/0")`, which no stored location can fall in since empty
    // path segments are rejected, and it would leave a stored ancestor unequal to
    // every entry of `$2`. Both sides go through `fs_location_of` so neither can
    // carry one -- `$2` via `get_partial_fs_locations`.
    let fs_location = fs_location_of(location);
    let taken = sqlx::query_scalar!(
        // Two `EXISTS` rather than one over an `OR`, which is the same predicate:
        // the planner does not build a `BitmapOr` in a generic plan, and this
        // statement is prepared and reused, so the `OR` form degraded to a
        // sequential scan of every tabular in the warehouse once a connection had
        // executed it enough times to switch. Split, both branches index-scan
        // under a generic plan as well as a custom one.
        //
        // `~>=~` and `~<~` compare bytes, ignoring collation, so the descendant
        // half is the range `[$4 || '/', $4 || '0')` -- '0' being the byte after
        // '/'. Every location under `$4` falls in it and nothing else does, and
        // it is what the `text_pattern_ops` index from 20260830000000 serves.
        //
        // Byte comparison also reads `$4` literally, where `LIKE` read it as a
        // pattern: a location containing `\` escaped the character after it and
        // hid a real collision, and one containing `%` or `_` matched unrelated
        // siblings.
        //
        // The first branch covers exact duplicates: `$2` carries the location
        // and its parents, so equality is enough.
        r#"SELECT (
               EXISTS (
                   SELECT 1 FROM tabular ta
                   WHERE ta.warehouse_id = $1 AND ta.fs_location = ANY($2)
                     AND ta.tabular_id != $3
               )
               OR EXISTS (
                   SELECT 1 FROM tabular ta
                   WHERE ta.warehouse_id = $1
                     AND ta.fs_location ~>=~ ($4 || '/')
                     AND ta.fs_location ~<~ ($4 || '0')
                     AND ta.tabular_id != $3
               )
           ) as "exists!""#,
        warehouse_id,
        &partial_locations,
        self_tabular_id,
        fs_location,
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| {
        e.into_catalog_backend_error()
            .append_detail("Error checking for conflicting locations")
    })?;
    if taken {
        return Err(LocationAlreadyTaken::new(location.clone()).into());
    }
    Ok(())
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
    let fs_location = fs_location_of(location);

    // Check location availability before the INSERT so a collision raises
    // `LocationAlreadyTaken` cleanly instead of inserting a row we'll have to
    // rely on transaction rollback to undo.
    ensure_location_available(warehouse_id, id, location, transaction).await?;

    let tabular_id = sqlx::query_as!(
        TabularRowCore,
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
               i.fs_protocol
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
    generic_table_properties_keys: Option<Vec<String>>,
    generic_table_properties_values: Option<Vec<String>>,
}

impl TabularRowWithDeletion {
    fn try_into_table_or_view(
        self,
        warehouse_id: WarehouseId,
    ) -> Result<ViewOrTableDeletionInfo, FromTabularRowError> {
        let row = TabularRowWithProperties {
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
            generic_table_properties_keys: self.generic_table_properties_keys,
            generic_table_properties_values: self.generic_table_properties_values,
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
            ViewOrTableInfo::GenericTable(generic_table_info) => GenericTableDeletionInfo {
                tabular: generic_table_info,
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
    list_flags: lakekeeper::service::TabularListFlags,
    catalog_state: E,
    typ: Option<TabularType>,
    pagination_query: PaginationQuery,
) -> Result<PaginatedMapping<TabularId, ViewOrTableDeletionInfo>, ListTabularsError>
where
    E: 'e + sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let page_size = CONFIG.page_size_or_pagination_max(pagination_query.page_size);

    let token: Option<PaginateToken<Uuid>> = pagination_query
        .page_token
        .as_option()
        .map(PaginateToken::try_from)
        .transpose()?;

    let (token_ts, token_id) = token
        .as_ref()
        .map(PaginateToken::v1_parts)
        .transpose()?
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
            LEFT JOIN task tt ON (t.tabular_id = tt.entity_id AND tt.entity_type in ('table', 'view', 'generic-table') AND tt.queue_name IN ('soft_deletion', 'tabular_expiration') AND tt.warehouse_id = $1 AND tt.project_id = w.project_id)
            -- Deliberately NOT filtering on tt.queue_name here. The predicate used to be:
            --     AND (tt.queue_name IN ('soft_deletion', 'tabular_expiration') OR tt.queue_name is NULL)
            -- It can never exclude a row: the LEFT JOIN's ON clause already restricts matches to
            -- those two queues, so a matched row always satisfies the IN, and an unmatched row
            -- is NULL-extended by the outer join and always satisfies the IS NULL.
            -- Postgres cannot reason about the LEFT JOIN, so the planner can get confused
            -- and decides to not use the index, degrading query performance.
            WHERE t.warehouse_id = $1
                AND (t.namespace_id = $2 OR $2 IS NULL)
                AND w.status = 'active'
                AND (t.typ = $3 OR $3 IS NULL)
                -- active tabulars: not deleted AND (has metadata_location OR is generic-table)
                AND (
                    (t.deleted_at IS NULL AND (t.metadata_location IS NOT NULL OR t.typ = 'generic-table') AND $4) OR   -- include_active
                    (t.deleted_at IS NOT NULL AND $5) OR                                   -- include_deleted
                    (t.metadata_location IS NULL AND t.typ != 'generic-table' AND $6)      -- include_staged
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
        ),
        selected_generic_tables AS (
            SELECT tabular_id FROM selected_tabulars WHERE typ = 'generic-table'
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
               tp.values as table_properties_values,
               gtp.keys as generic_table_properties_keys,
               gtp.values as generic_table_properties_values
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
        LEFT JOIN (SELECT generic_table_id,
                    ARRAY_AGG(key) as keys,
                    ARRAY_AGG(value) as values
                FROM generic_table_properties
                WHERE warehouse_id = $1 AND generic_table_id in (SELECT tabular_id FROM selected_generic_tables)
                GROUP BY generic_table_id) gtp ON st.tabular_id = gtp.generic_table_id
        ORDER BY st.created_at, st.tabular_id ASC
        "#,
        // The CTE has ORDER BY but PostgreSQL does not preserve row order through
        // JOINs. Without the outer ORDER BY, the last row (used to derive the
        // next-page cursor) may not be the maximum (created_at, tabular_id),
        // causing the next page to re-fetch already-returned rows.
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
    generic_table_properties_keys: Option<Vec<String>>,
    generic_table_properties_values: Option<Vec<String>>,
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
            TabularType::GenericTable => ViewOrTableInfo::GenericTable(GenericTabularInfo {
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
                    self.generic_table_properties_keys,
                    self.generic_table_properties_values,
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
                    AND (t.metadata_location IS NOT NULL OR t.typ = 'generic-table')
                    AND (t.tabular_id = $2 OR t.namespace_id = $2)
                ORDER BY (t.tabular_id = $2) DESC
                LIMIT 10
            ),
            selected_views AS (
                SELECT tabular_id FROM selected_tabulars WHERE typ = 'view'
            ),
            selected_tables AS (
                SELECT tabular_id FROM selected_tabulars WHERE typ = 'table'
            ),
            selected_generic_tables AS (
                SELECT tabular_id FROM selected_tabulars WHERE typ = 'generic-table'
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
                tp.values as table_properties_values,
                gtp.keys as generic_table_properties_keys,
                gtp.values as generic_table_properties_values
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
            LEFT JOIN (SELECT generic_table_id,
                        ARRAY_AGG(key) as keys,
                        ARRAY_AGG(value) as values
                    FROM generic_table_properties
                    WHERE warehouse_id = $1 AND generic_table_id in (SELECT tabular_id FROM selected_generic_tables)
                    GROUP BY generic_table_id) gtp ON st.tabular_id = gtp.generic_table_id
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
                    AND (t.metadata_location IS NOT NULL OR t.typ = 'generic-table')
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
            ),
            selected_generic_tables AS (
                SELECT tabular_id FROM filtered_tabulars WHERE typ = 'generic-table'
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
                tp.values as table_properties_values,
                gtp.keys as generic_table_properties_keys,
                gtp.values as generic_table_properties_values
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
            LEFT JOIN (SELECT generic_table_id,
                        ARRAY_AGG(key) as keys,
                        ARRAY_AGG(value) as values
                    FROM generic_table_properties
                    WHERE warehouse_id = $1 AND generic_table_id in (SELECT tabular_id FROM selected_generic_tables)
                    GROUP BY generic_table_id) gtp ON st.tabular_id = gtp.generic_table_id
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

/// Map a failed rename onto its error.
///
/// An occupied destination name is detected by the `unique_name_per_namespace_id`
/// index rather than by a pre-check inside the statement. The index key is
/// `(warehouse_id, namespace_id, name, deleted_at)` with `NULLS NOT DISTINCT`, and
/// the row being renamed is always live, so it can only collide with another live
/// row. A soft-deleted namesake therefore does not block the rename — the same
/// name is already freely reusable via create. Letting the failing statement pick
/// its own error also means the sequential case and a concurrent insert of the
/// destination name are reported identically.
///
/// Like every constraint violation, a conflict aborts the caller's transaction.
/// All callers propagate it and drop the transaction; anything that swallows this
/// error would make the next statement fail with `25P02`.
fn rename_tabular_error(
    e: sqlx::Error,
    warehouse_id: WarehouseId,
    source_id: TabularId,
    not_found_detail: &str,
) -> RenameTabularError {
    match e {
        sqlx::Error::Database(db_err)
            if db_err.constraint() == Some("unique_name_per_namespace_id") =>
        {
            TabularAlreadyExists::new().into()
        }
        sqlx::Error::RowNotFound => TabularNotFound::new(warehouse_id, source_id)
            .append_detail(not_found_detail)
            .into(),
        _ => e.into_catalog_backend_error().into(),
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
            TabularRowWithProperties,
            r#"
            WITH locked_tabular AS (
                SELECT tabular_id, name, namespace_id, typ
                FROM tabular
                WHERE tabular_id = $2
                    AND warehouse_id = $4
                    AND typ = $3
                    AND (metadata_location IS NOT NULL OR typ = 'generic-table')
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
            updated AS (
                UPDATE tabular t
                SET name = $1
                FROM locked_tabular lt, warehouse_check wc, locked_source_namespace lsn
                WHERE t.tabular_id = lt.tabular_id
                    AND t.warehouse_id = $4
                    AND wc.warehouse_id = $4
                    AND lsn.namespace_id IS NOT NULL
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
            ),
            selected_generic_tables AS (
                SELECT tabular_id FROM updated WHERE typ = 'generic-table'
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
                tp.values as table_properties_values,
                gtp.keys as generic_table_properties_keys,
                gtp.values as generic_table_properties_values
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
            LEFT JOIN (SELECT generic_table_id,
                        ARRAY_AGG(key) as keys,
                        ARRAY_AGG(value) as values
                    FROM generic_table_properties
                    WHERE warehouse_id = $4 AND generic_table_id in (SELECT tabular_id FROM selected_generic_tables)
                    GROUP BY generic_table_id) gtp ON u.tabular_id = gtp.generic_table_id
            "#,
            &**dest_name,
            *source_id,
            TabularType::from(source_id) as _,
            *warehouse_id,
        )
        .fetch_one(&mut **transaction)
        .await
        .map_err(|e| {
            rename_tabular_error(
                e,
                warehouse_id,
                source_id,
                "The source tabular could not be found.",
            )
        })?
    } else {
        sqlx::query_as!(
            TabularRowWithProperties,
            r#"
            WITH locked_tabular AS (
                SELECT tabular_id, name, namespace_id, typ
                FROM tabular
                WHERE tabular_id = $4
                    AND warehouse_id = $2
                    AND typ = $5
                    AND (metadata_location IS NOT NULL OR typ = 'generic-table')
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
            updated AS (
                UPDATE tabular t
                SET name = $1, namespace_id = ln.namespace_id, tabular_namespace_name = $3
                FROM locked_tabular lt, locked_namespace ln, locked_source_namespace lsn, warehouse_check wc
                    WHERE t.tabular_id = lt.tabular_id
                    AND t.warehouse_id = $2
                    AND ln.namespace_id IS NOT NULL
                    AND wc.warehouse_id = $2
                    AND lsn.namespace_id IS NOT NULL
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
            ),
            selected_generic_tables AS (
                SELECT tabular_id FROM updated WHERE typ = 'generic-table'
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
                tp.values as table_properties_values,
                gtp.keys as generic_table_properties_keys,
                gtp.values as generic_table_properties_values
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
            LEFT JOIN (SELECT generic_table_id,
                        ARRAY_AGG(key) as keys,
                        ARRAY_AGG(value) as values
                    FROM generic_table_properties
                    WHERE warehouse_id = $2 AND generic_table_id in (SELECT tabular_id FROM selected_generic_tables)
                    GROUP BY generic_table_id) gtp ON u.tabular_id = gtp.generic_table_id
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
        .map_err(|e| {
            rename_tabular_error(
                e,
                warehouse_id,
                source_id,
                "Either the source tabular or the destination namespace could not be found.",
            )
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

impl From<DeletionKind> for lakekeeper::api::management::v1::DeleteKind {
    fn from(kind: DeletionKind) -> Self {
        match kind {
            DeletionKind::Default => lakekeeper::api::management::v1::DeleteKind::Default,
            DeletionKind::Purge => lakekeeper::api::management::v1::DeleteKind::Purge,
        }
    }
}

impl From<TabularType> for lakekeeper::api::management::v1::TabularType {
    fn from(typ: TabularType) -> Self {
        match typ {
            TabularType::Table => lakekeeper::api::management::v1::TabularType::Table,
            TabularType::View => lakekeeper::api::management::v1::TabularType::View,
            TabularType::GenericTable => lakekeeper::api::management::v1::TabularType::GenericTable,
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
            WHERE ta.entity_type in ('table', 'view', 'generic-table')
                AND ta.warehouse_id = $2
                AND ta.queue_name IN ('soft_deletion', 'tabular_expiration')
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
        ),
        selected_generic_tables AS (
            SELECT tabular_id FROM updated WHERE typ = 'generic-table'
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
            tp.values as table_properties_values,
            gtp.keys as generic_table_properties_keys,
            gtp.values as generic_table_properties_values
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
        LEFT JOIN (SELECT generic_table_id,
                    ARRAY_AGG(key) as keys,
                    ARRAY_AGG(value) as values
                FROM generic_table_properties
                WHERE warehouse_id = $2 AND generic_table_id in (SELECT tabular_id FROM selected_generic_tables)
                GROUP BY generic_table_id) gtp ON u.tabular_id = gtp.generic_table_id
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
        TabularRowWithProperties,
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
        ),
        selected_generic_tables AS (
            SELECT tabular_id FROM result_tabulars WHERE typ = 'generic-table'
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
            tp.values as table_properties_values,
            gtp.keys as generic_table_properties_keys,
            gtp.values as generic_table_properties_values
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
        LEFT JOIN (SELECT generic_table_id,
                    ARRAY_AGG(key) as keys,
                    ARRAY_AGG(value) as values
                FROM generic_table_properties
                WHERE warehouse_id = $1 AND generic_table_id in (SELECT tabular_id FROM selected_generic_tables)
                GROUP BY generic_table_id) gtp ON rt.tabular_id = gtp.generic_table_id
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

    if let Some(required_metadata_location) = required_metadata_location
        && location.metadata_location != Some(required_metadata_location.to_string())
    {
        return Err(ConcurrentUpdateError::new(warehouse_id, tabular_id).into());
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
            TabularIdentBorrowed::GenericTable(_) => TabularType::GenericTable,
        }
    }
}

impl<'a> From<&'a TabularId> for TabularType {
    fn from(ident: &'a TabularId) -> Self {
        match ident {
            TabularId::Table(_) => TabularType::Table,
            TabularId::View(_) => TabularType::View,
            TabularId::GenericTable(_) => TabularType::GenericTable,
        }
    }
}

impl From<TabularId> for TabularType {
    fn from(ident: TabularId) -> Self {
        match ident {
            TabularId::Table(_) => TabularType::Table,
            TabularId::View(_) => TabularType::View,
            TabularId::GenericTable(_) => TabularType::GenericTable,
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

    use lakekeeper::service::AuthZTableInfo;
    use lakekeeper_io::Location;
    use uuid::Uuid;

    use super::*;
    use crate::{
        CatalogState, namespace::tests::initialize_namespace, warehouse::test::initialize_warehouse,
    };

    /// Creates a tabular at `location`, returning its id.
    async fn plant_tabular(
        pool: &sqlx::PgPool,
        warehouse_id: Uuid,
        namespace_id: Uuid,
        location: &str,
    ) -> Uuid {
        let id = Uuid::now_v7();
        let location = Location::from_str(location).unwrap();
        let metadata_location =
            Location::from_str(&format!("s3://metadata-bucket/{id}/v1.json")).unwrap();
        let name = format!("t_{id}");

        let mut transaction = pool.begin().await.unwrap();
        create_tabular(
            CreateTabular {
                id,
                name: &name,
                namespace_id,
                warehouse_id,
                typ: TabularType::Table,
                metadata_location: Some(&metadata_location),
                location: &location,
            },
            &mut transaction,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();
        id
    }

    /// A create holds locks that a colliding create cannot take, so the two cannot
    /// run their checks against the same absent row.
    ///
    /// The check is a read and no index on `fs_location` is unique, so without this
    /// both transactions pass and both commit -- one tabular's data ends up inside
    /// the other's location, and purging either then destroys the other's files.
    ///
    /// Asserted on the locks rather than on two racing creates, because a create
    /// that is correctly blocked never returns: the second transaction would have to
    /// be woken by committing the first, and then it sees the committed row and is
    /// refused for a reason that has nothing to do with locking. The keys come from
    /// `location_lock_keys` itself, so narrowing what it covers -- dropping the path
    /// prefixes, say -- shows up here as an overlap that is no longer there.
    ///
    /// A parent and a location under it are different values, so a unique index
    /// expresses neither pair; only the prefix keys make them collide. The second
    /// pair sits directly under the bucket, as shallow as a tabular can be, and is
    /// the pair a key set that dropped one segment too many would miss.
    #[sqlx::test]
    async fn a_create_locks_out_a_colliding_one(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let ident = iceberg_ext::NamespaceIdent::from_vec(vec!["ns".to_string()]).unwrap();
        let namespace_id = *initialize_namespace(state.clone(), warehouse_id, &ident, None)
            .await
            .namespace_id();

        for (name, parent, child) in [
            ("deep", "s3://bkt/race/parent", "s3://bkt/race/parent/child"),
            ("shallow", "s3://bkt/shallow", "s3://bkt/shallow/child"),
        ] {
            let parent = Location::from_str(parent).unwrap();
            let child = Location::from_str(child).unwrap();

            // Creates `parent` and stays open, so its locks are still held and the row
            // it wrote is not visible to anyone else.
            let mut first = pool.begin().await.unwrap();
            create_tabular(
                CreateTabular {
                    id: Uuid::now_v7(),
                    name,
                    namespace_id,
                    warehouse_id: *warehouse_id,
                    typ: TabularType::Table,
                    metadata_location: None,
                    location: &parent,
                },
                &mut first,
            )
            .await
            .expect("the uncontended create was refused");

            // What a create of `child` would have to take, on another connection.
            let child_keys =
                location_lock_keys(*warehouse_id, &get_partial_fs_locations(&child).unwrap());
            assert!(!child_keys.is_empty(), "{child} produced no lock keys");

            let mut acquired = 0;
            let mut contender = pool.begin().await.unwrap();
            for key in &child_keys {
                let free: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1)")
                    .bind(key)
                    .fetch_one(&mut *contender)
                    .await
                    .unwrap();
                if free {
                    acquired += 1;
                }
            }
            contender.rollback().await.unwrap();
            first.commit().await.unwrap();

            assert_ne!(
                acquired,
                child_keys.len(),
                "a create at {child} could take every lock the create of {parent} holds, \
                 so both would pass the check and both commit"
            );
        }
    }

    /// A location arriving with a trailing slash is stored without one, so the
    /// collision check finds it like any other.
    ///
    /// Stored verbatim, `bkt/dir/` is never equal to a candidate in `$2`, because
    /// those are trimmed -- so the equality half, which is the half that finds an
    /// ancestor, misses it and a tabular can be created at `bkt/dir/child`. That is
    /// what this asserts is no longer possible. The range half is unaffected: it
    /// still catches an exact duplicate at `bkt/dir`, since `bkt/dir/` falls inside
    /// `["bkt/dir/", "bkt/dir0")`.
    #[sqlx::test]
    async fn a_trailing_slash_does_not_hide_a_location(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let ident = iceberg_ext::NamespaceIdent::from_vec(vec!["ns".to_string()]).unwrap();
        let namespace_id = *initialize_namespace(state.clone(), warehouse_id, &ident, None)
            .await
            .namespace_id();

        let id = plant_tabular(&pool, *warehouse_id, namespace_id, "s3://bkt/dir/").await;

        let stored: String =
            sqlx::query_scalar("SELECT fs_location FROM tabular WHERE tabular_id = $1")
                .bind(id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(stored, "bkt/dir", "the trailing slash reached the column");

        let child = Location::from_str("s3://bkt/dir/child").unwrap();
        let mut transaction = pool.begin().await.unwrap();
        let result =
            ensure_location_available(*warehouse_id, Uuid::now_v7(), &child, &mut transaction)
                .await;
        transaction.rollback().await.unwrap();
        assert!(
            result.is_err(),
            "a tabular could be created inside s3://bkt/dir/"
        );

        // The constraint is what keeps the column that way, whatever a future writer
        // forgets to trim.
        let mut transaction = pool.begin().await.unwrap();
        let err = sqlx::query("UPDATE tabular SET fs_location = 'bkt/dir/' WHERE tabular_id = $1")
            .bind(id)
            .execute(&mut *transaction)
            .await
            .expect_err("a trailing slash was accepted into the column");
        assert!(
            err.to_string()
                .contains("tabular_fs_location_no_trailing_slash"),
            "rejected, but not by the constraint: {err}"
        );
    }

    /// `ensure_location_available` refuses a location that duplicates another
    /// tabular's, sits under it, or contains it.
    ///
    /// Driven through the function against real rows, so the trim applied to the
    /// location before the query runs is covered as well -- `s3://bkt/nested/parent/`
    /// is only found without it if the range is built from the trimmed form.
    ///
    /// The `taken` cases are the load-bearing half: missing a collision lets two
    /// tabulars write into one prefix. The rest guard the other direction, where a
    /// location is refused that merely resembles an occupied one -- which is what a
    /// pattern match did, reading `\`, `%` and `_` as metacharacters.
    #[sqlx::test]
    async fn ensure_location_available_finds_collisions_reading_locations_literally(
        pool: sqlx::PgPool,
    ) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        // A second project, since a warehouse name is unique within one.
        let other_project = lakekeeper::service::ProjectId::from(Uuid::now_v7());
        let (_, other_warehouse_id) =
            initialize_warehouse(state.clone(), None, Some(&other_project), None, true).await;

        let ident = iceberg_ext::NamespaceIdent::from_vec(vec!["ns".to_string()]).unwrap();
        let namespace_id = *initialize_namespace(state.clone(), warehouse_id, &ident, None)
            .await
            .namespace_id();

        // Only the child of `w\dir` is planted, never `w\dir` itself: probing the
        // parent is the one direction the `= ANY($2)` half cannot answer, because
        // `$2` carries a location's parents and not its children.
        //
        // `near-miss` and `wildcard` hold the rows a pattern built from `\` or `_`
        // would wrongly reach, and they sit under prefixes of their own. Under
        // `backslash` they would mask the case above -- a pattern match finds
        // `wdir/t` there and reports a collision, which is the right answer for the
        // wrong reason.
        for location in [
            r"s3://bkt/backslash/w\dir/t",
            r"s3://bkt/near-miss/wdir/t",
            "s3://bkt/wildcard/undXr/t",
            "s3://bkt/nested/parent/child",
            "s3://bkt/exact/tbl",
            // Siblings straddling the range's own boundary: `/` is 0x2F, so `a0`
            // (0x30) is the first byte outside `["a/", "a0")` and `ab` is further
            // out. Neither sits under `a`, and an off-by-one endpoint says they do.
            "s3://bkt/boundary/a0",
            "s3://bkt/boundary/ab",
        ] {
            plant_tabular(&pool, *warehouse_id, namespace_id, location).await;
        }
        let occupied_id =
            plant_tabular(&pool, *warehouse_id, namespace_id, "s3://bkt/self/occupied").await;
        let moved_up_id =
            plant_tabular(&pool, *warehouse_id, namespace_id, "s3://bkt/moveup/sub").await;

        let cases = [
            (
                r"s3://bkt/backslash/w\dir",
                true,
                r"contains a stored location holding a `\`, where a pattern reached the escaped spelling instead",
            ),
            (
                r"s3://bkt/backslash/w\dir/t/deeper",
                true,
                "sits under a stored location",
            ),
            ("s3://bkt/exact/tbl", true, "exact duplicate"),
            (
                "s3://bkt/nested/parent/",
                true,
                "contains a stored location, and the trailing slash must not shift the range",
            ),
            (
                r"s3://bkt/near-miss/w\dir",
                false,
                r"`\` must not escape the `d` and reach the stored `wdir`",
            ),
            (
                "s3://bkt/wildcard/und_r",
                false,
                "`_` must not match the `X` in the stored `undXr`",
            ),
            (
                "s3://bkt/nested/parent-sibling",
                false,
                "shares a prefix with an occupied location but no path segment",
            ),
            ("s3://bkt/unoccupied/x", false, "nothing near it"),
            (
                "s3://bkt/boundary/a",
                false,
                "`a0` and `ab` sort past the range's upper endpoint, so neither is under it",
            ),
        ];

        // Collected rather than asserted in the loop, so a break shows every case it
        // affects instead of only the first.
        let mut wrong = Vec::new();
        for (candidate, expect_taken, why) in cases {
            let location = Location::from_str(candidate).unwrap();
            let mut transaction = pool.begin().await.unwrap();
            let taken = ensure_location_available(
                *warehouse_id,
                Uuid::now_v7(),
                &location,
                &mut transaction,
            )
            .await
            .is_err();
            transaction.rollback().await.unwrap();

            if taken != expect_taken {
                let direction = if expect_taken {
                    "missed a collision"
                } else {
                    "refused a free location"
                };
                wrong.push(format!("{candidate}: {direction} -- it {why}"));
            }
        }
        assert!(wrong.is_empty(), "{}", wrong.join("\n"));

        // A tabular does not collide with itself -- a view commit re-checks the
        // location it already occupies.
        let location = Location::from_str("s3://bkt/self/occupied").unwrap();
        let mut transaction = pool.begin().await.unwrap();
        ensure_location_available(*warehouse_id, occupied_id, &location, &mut transaction)
            .await
            .expect("a tabular's own location is available to it");

        // Locations are scoped to a warehouse.
        ensure_location_available(
            *other_warehouse_id,
            Uuid::now_v7(),
            &location,
            &mut transaction,
        )
        .await
        .expect("an occupied location in another warehouse does not collide");

        // Both again against the descendant half, which the two probes above cannot
        // reach: `s3://bkt/self/occupied` has nothing under it, so they only ever
        // exercise the equality half. A view moving up to a location that contains
        // only its own row has to find itself excluded there too, and a probe from
        // another warehouse has to not see this row at all.
        let parent = Location::from_str("s3://bkt/moveup").unwrap();
        ensure_location_available(*warehouse_id, moved_up_id, &parent, &mut transaction)
            .await
            .expect("a tabular moving up to its own parent collides with itself");
        ensure_location_available(
            *other_warehouse_id,
            Uuid::now_v7(),
            &parent,
            &mut transaction,
        )
        .await
        .expect("a location whose only descendant is in another warehouse does not collide");
        transaction.rollback().await.unwrap();
    }

    pub(super) async fn setup_test_tabular(pool: &sqlx::PgPool, protected: bool) -> TableInfo {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
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
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

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
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

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
        crate::warehouse::set_warehouse_status(
            table_info.warehouse_id,
            lakekeeper::api::management::v1::warehouse::WarehouseStatus::Inactive,
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
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

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
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
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
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
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

    /// `list_tabulars` joins `task` only to decorate rows with soft-deletion
    /// info. The join is restricted to the soft-deletion queues by its ON clause
    /// *alone*: the WHERE clause that used to repeat that restriction was
    /// removed because it is a tautology that wrecks the planner's row estimate
    /// (see the comment in `list_tabulars`).
    ///
    /// That makes the ON clause the only thing preventing a tabular with tasks
    /// in other queues from being joined more than once. Duplicate joined rows
    /// do not surface as duplicate entries -- the result is keyed by tabular id,
    /// so they collapse -- they surface as *short pages*, because `LIMIT` counts
    /// joined rows, not tabulars. This test would catch that: one tabular
    /// carries two tasks in unrelated queues, and a page of two must still
    /// return both tabulars.
    #[sqlx::test]
    async fn test_list_tabulars_unaffected_by_tasks_in_unrelated_queues(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace =
            iceberg_ext::NamespaceIdent::from_vec(vec!["unrelated_queue_ns".to_string()]).unwrap();
        let namespace_id = initialize_namespace(state.clone(), warehouse_id, &namespace, None)
            .await
            .namespace_id();

        let mut transaction = pool.begin().await.unwrap();
        let mut tabular_ids = Vec::new();
        for name in ["table_one", "table_two"] {
            let id = Uuid::now_v7();
            let location = Location::from_str(&format!("s3://test-bucket/{name}/")).unwrap();
            let metadata_location =
                Location::from_str(&format!("s3://test-bucket/{name}/metadata/v1.json")).unwrap();
            create_tabular(
                CreateTabular {
                    id,
                    name,
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
            tabular_ids.push(id);
        }
        transaction.commit().await.unwrap();

        // Two tasks on the *first* tabular, both in queues the list query does
        // not join. If the ON clause ever stops filtering on queue_name, this
        // tabular joins twice and consumes both slots of the page below.
        for queue_name in ["tabular_purge", "statistics"] {
            sqlx::query(
                r#"
                INSERT INTO task (task_id, warehouse_id, queue_name, status, scheduled_for,
                                  task_data, entity_id, entity_type, entity_name, project_id)
                SELECT gen_random_uuid(), w.warehouse_id, $1, 'scheduled', now(), '{}'::jsonb,
                       $2, 'table', ARRAY['table_one'], w.project_id
                FROM warehouse w WHERE w.warehouse_id = $3
                "#,
            )
            .bind(queue_name)
            .bind(tabular_ids[0])
            .bind(*warehouse_id)
            .execute(&pool)
            .await
            .unwrap();
        }

        let listed = list_tabulars(
            warehouse_id,
            Some(namespace_id),
            lakekeeper::service::TabularListFlags::active(),
            &pool,
            None,
            lakekeeper::api::iceberg::v1::PaginationQuery {
                page_token: lakekeeper::api::iceberg::v1::PageToken::NotSpecified,
                page_size: Some(2),
            },
        )
        .await
        .unwrap();

        assert_eq!(
            listed.len(),
            2,
            "a page of 2 must return both tabulars; tasks in unrelated queues must not \
             join and consume page slots"
        );
        for (_, info) in listed.iter() {
            assert!(
                info.expiration_task().is_none(),
                "a task in an unrelated queue must not be reported as a pending deletion"
            );
        }
    }
}
