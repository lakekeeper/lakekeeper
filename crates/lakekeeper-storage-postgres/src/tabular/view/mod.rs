mod load;

use std::{
    collections::{HashMap, HashSet},
    default::Default,
    str::FromStr as _,
};

use chrono::{DateTime, Utc};
use iceberg::spec::{SchemaRef, ViewMetadata, ViewRepresentation, ViewVersionId, ViewVersionRef};
use lakekeeper::{
    WarehouseId,
    service::{
        CatalogBackendError, CommitViewError, ConcurrentUpdateError, ConversionError,
        CreateViewError, CreateViewVersionError, InternalParseLocationError, NamespaceId,
        SerializationError, TabularNotFound, UnexpectedTabularInResponse, ViewId, ViewInfo,
    },
};
use lakekeeper_io::Location;
pub(crate) use load::load_view;
use serde::Deserialize;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::{
    dbutils::DBErrorHandler as _,
    tabular::{CreateTabular, TabularType, create_tabular},
};

pub(crate) async fn create_view(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    metadata_location: &Location,
    transaction: &mut Transaction<'_, Postgres>,
    name: &str,
    metadata: &ViewMetadata,
) -> Result<ViewInfo, CreateViewError> {
    let location =
        Location::from_str(metadata.location()).map_err(InternalParseLocationError::from)?;

    let tabular_info = create_tabular(
        CreateTabular {
            id: metadata.uuid(),
            name,
            namespace_id: *namespace_id,
            warehouse_id: *warehouse_id,
            typ: TabularType::View,
            metadata_location: Some(metadata_location),
            location: &location,
        },
        &mut *transaction,
    )
    .await?;

    let Some(view_info) = tabular_info.into_view_info() else {
        return Err(UnexpectedTabularInResponse::new()
            .append_detail("Expected created tabular to be of type view")
            .into());
    };

    let view_id = sqlx::query_scalar!(
        r#"
        INSERT INTO view (warehouse_id, view_id, view_format_version)
        VALUES ($1, $2, $3)
        returning view_id
        "#,
        *warehouse_id,
        metadata.uuid(),
        ViewFormatVersion::from(metadata.format_version()) as _,
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(super::super::dbutils::DBErrorHandler::into_catalog_backend_error)?;

    tracing::debug!("Inserted base view and tabular.");
    populate_view_metadata(warehouse_id, view_id, metadata, transaction).await?;

    // `view_info` came from `create_tabular` and has no properties (the row
    // hadn't been populated yet); refresh them from the metadata we just
    // wrote so the caller doesn't see an empty `properties` HashMap.
    Ok(finalize_view_info(view_info, metadata))
}

pub(crate) async fn commit_existing_view(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    metadata_location: &Location,
    previous_metadata_location: &Location,
    transaction: &mut Transaction<'_, Postgres>,
    metadata: &ViewMetadata,
) -> Result<ViewInfo, CommitViewError> {
    let location =
        Location::from_str(metadata.location()).map_err(InternalParseLocationError::from)?;
    let view_id = ViewId::from(metadata.uuid());
    let fs_location = location.authority_and_path();
    let fs_protocol = location.scheme();

    // Compile-time guard: the `tabular` UPDATE below does not touch
    // `view.view_format_version`. When iceberg introduces V2, this `From`
    // impl breaks to compile — at that point, also add an UPDATE on
    // `view.view_format_version` here.
    let _ = ViewFormatVersion::from(metadata.format_version());

    // Lock the tabular row + classify. Existence-and-authz check
    // (namespace_id matches what authz authorized against) + row-lock for the
    // unconditional UPDATE below + read of `metadata_location` for the
    // concurrent-update guard. 0 rows → TabularNotFound; mismatch →
    // ConcurrentUpdateError. The DB check constraint `tabular_check`
    // guarantees views always have non-NULL `metadata_location`, so the
    // unwrap-into-Some below cannot misfire on a staged row.
    let current_metadata_location: Option<String> = sqlx::query_scalar!(
        r#"
        SELECT metadata_location
        FROM tabular
        WHERE warehouse_id = $1
          AND tabular_id = $2
          AND typ = 'view'
          AND deleted_at IS NULL
          AND namespace_id = $3
        FOR UPDATE
        "#,
        *warehouse_id,
        *view_id,
        *namespace_id,
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| {
        e.into_catalog_backend_error()
            .append_detail("Error locking view row for commit.")
    })?
    .ok_or_else(|| TabularNotFound::new(warehouse_id, view_id))?;

    if current_metadata_location.as_deref() != Some(previous_metadata_location.as_str()) {
        return Err(ConcurrentUpdateError::new(warehouse_id, view_id).into());
    }

    super::ensure_location_available(*warehouse_id, *view_id, &location, &mut *transaction).await?;

    // We hold the row lock — this UPDATE always matches the one row above.
    // Properties aren't read back from the DB here — `finalize_view_info`
    // overlays them from the in-memory `ViewMetadata` after this returns.
    let row = sqlx::query_as!(
        super::TabularRowCore,
        r#"
        WITH updated AS (
            UPDATE tabular
            SET metadata_location = $3,
                fs_protocol = $4,
                fs_location = $5
            WHERE warehouse_id = $1 AND tabular_id = $2
            RETURNING tabular_id,
                      namespace_id,
                      name AS tabular_name,
                      tabular_namespace_name AS namespace_name,
                      typ,
                      metadata_location,
                      updated_at,
                      protected,
                      fs_location,
                      fs_protocol
        )
        SELECT u.tabular_id,
               u.namespace_id,
               u.tabular_name,
               u.namespace_name,
               u.typ AS "typ: TabularType",
               u.metadata_location,
               u.updated_at,
               u.protected,
               u.fs_location,
               u.fs_protocol,
               w.version AS warehouse_version,
               n.version AS namespace_version
        FROM updated u
        INNER JOIN warehouse w ON w.warehouse_id = $1
        INNER JOIN namespace n ON n.namespace_id = u.namespace_id AND n.warehouse_id = $1
        "#,
        *warehouse_id,
        *view_id,
        metadata_location.to_string(),
        fs_protocol,
        fs_location,
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| {
        e.into_catalog_backend_error()
            .append_detail("Error updating tabular row during view commit.")
    })?;

    clear_view_metadata(warehouse_id, *view_id, transaction).await?;
    populate_view_metadata(warehouse_id, *view_id, metadata, transaction).await?;

    let info = row
        .try_into_table_or_view(warehouse_id)
        .map_err(|e| match e {
            super::FromTabularRowError::InvalidNamespaceIdentifier(e) => CommitViewError::from(e),
            super::FromTabularRowError::InternalParseLocationError(e) => CommitViewError::from(e),
        })?;
    let Some(view_info) = info.into_view_info() else {
        return Err(UnexpectedTabularInResponse::new()
            .append_detail("Expected committed tabular to be of type view")
            .into());
    };
    Ok(finalize_view_info(view_info, metadata))
}

/// Overlays properties from the in-memory metadata onto `view_info` so the
/// returned value reflects the just-written `view_properties` rows without a
/// re-query. Used by both `create_view` and `commit_existing_view` after
/// `populate_view_metadata`.
fn finalize_view_info(mut view_info: ViewInfo, metadata: &ViewMetadata) -> ViewInfo {
    view_info.properties.clone_from(metadata.properties());
    view_info
}

async fn populate_view_metadata(
    warehouse_id: WarehouseId,
    view_id: Uuid,
    metadata: &ViewMetadata,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), CreateViewError> {
    // schemas first (FK target for view_version)
    batch_insert_view_schemas(
        warehouse_id,
        view_id,
        metadata.schemas_iter(),
        &mut *transaction,
    )
    .await?;

    // versions (FK to schemas, FK target for representations/log/current)
    batch_insert_view_versions(
        warehouse_id,
        view_id,
        metadata.versions(),
        &mut *transaction,
    )
    .await?;

    batch_insert_view_representations(
        warehouse_id,
        view_id,
        metadata.versions(),
        &mut *transaction,
    )
    .await?;

    set_current_view_metadata_version(
        warehouse_id,
        metadata.uuid(),
        metadata.current_version_id(),
        transaction,
    )
    .await?;

    batch_insert_view_version_log(warehouse_id, view_id, metadata.history(), &mut *transaction)
        .await?;

    set_view_properties(warehouse_id, view_id, metadata.properties(), transaction).await?;

    Ok(())
}

// Removes all view sub-metadata so a commit can repopulate it from `ViewMetadata`.
//
// Only two DELETEs are needed because of the FK chain set up in migration
// `20250904142650_reusable_table_id.sql`:
//
//   - `view_version` REFERENCES `view_schema` ON DELETE CASCADE
//     → deleting `view_schema` cascades to all `view_version` rows.
//   - `view_version_log`, `view_representation`, and
//     `current_view_metadata_version` all REFERENCE `view_version` ON DELETE
//     CASCADE → cascade transitively from the `view_schema` delete.
//
// If a future migration weakens any of those CASCADE constraints, this
// function must be updated to delete from the affected tables explicitly,
// or `populate_view_metadata` will hit PK collisions.
async fn clear_view_metadata(
    warehouse_id: WarehouseId,
    view_id: Uuid,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), CatalogBackendError> {
    sqlx::query!(
        r#"
        DELETE FROM view_properties
        WHERE warehouse_id = $1 AND view_id = $2
        "#,
        *warehouse_id,
        view_id,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| {
        e.into_catalog_backend_error()
            .append_detail("Error clearing view properties before commit.")
    })?;

    sqlx::query!(
        r#"
        DELETE FROM view_schema
        WHERE warehouse_id = $1 AND view_id = $2
        "#,
        *warehouse_id,
        view_id,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| {
        e.into_catalog_backend_error()
            .append_detail("Error clearing view metadata before commit.")
    })?;

    Ok(())
}

async fn batch_insert_view_version_log(
    warehouse_id: WarehouseId,
    view_id: Uuid,
    log: &[iceberg::spec::ViewVersionLog],
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), CreateViewError> {
    if log.is_empty() {
        return Ok(());
    }
    let mut version_ids: Vec<ViewVersionId> = Vec::with_capacity(log.len());
    let mut timestamps: Vec<DateTime<Utc>> = Vec::with_capacity(log.len());
    for entry in log {
        version_ids.push(entry.version_id());
        timestamps.push(
            entry
                .timestamp()
                .map_err(|e| ConversionError::new("view_version_log.timestamp", e))?,
        );
    }
    sqlx::query!(
        r#"
        INSERT INTO view_version_log (warehouse_id, view_id, version_id, timestamp)
        SELECT $1, $2, u.version_id, u.timestamp
        FROM UNNEST($3::int[], $4::timestamptz[]) AS u(version_id, timestamp)
        "#,
        *warehouse_id,
        view_id,
        &version_ids,
        &timestamps,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| {
        e.into_catalog_backend_error()
            .append_detail("Error inserting view version log.")
    })?;
    Ok(())
}

async fn set_view_properties(
    warehouse_id: WarehouseId,
    view_id: Uuid,
    properties: &HashMap<String, String>,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), CatalogBackendError> {
    let (keys, vals): (Vec<String>, Vec<String>) = properties
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .unzip();
    sqlx::query!(
        r#"INSERT INTO view_properties (warehouse_id, view_id, key, value)
           SELECT $1, $2, u.* FROM UNNEST($3::text[], $4::text[]) u
              ON CONFLICT (warehouse_id, view_id, key)
                DO UPDATE SET value = EXCLUDED.value
           ;"#,
        *warehouse_id,
        view_id,
        &keys,
        &vals
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| {
        e.into_catalog_backend_error()
            .append_detail("Error setting view properties.")
    })?;
    Ok(())
}

async fn batch_insert_view_schemas<'a>(
    warehouse_id: WarehouseId,
    view_id: Uuid,
    schemas: impl IntoIterator<Item = &'a SchemaRef>,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), CreateViewError> {
    let (schema_ids, schema_jsons): (Vec<i32>, Vec<serde_json::Value>) = schemas
        .into_iter()
        .map(|s| {
            serde_json::to_value(s)
                .map(|json| (s.schema_id(), json))
                .map_err(|e| SerializationError::new("schema", e))
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .unzip();

    if schema_ids.is_empty() {
        return Ok(());
    }

    sqlx::query!(
        r#"
        INSERT INTO view_schema (warehouse_id, view_id, schema_id, schema)
        SELECT $1, $2, u.schema_id, u.schema
        FROM UNNEST($3::int[], $4::jsonb[]) AS u(schema_id, schema)
        "#,
        *warehouse_id,
        view_id,
        &schema_ids,
        &schema_jsons,
    )
    .execute(&mut **transaction)
    .await
    .map_err(super::super::dbutils::DBErrorHandler::into_catalog_backend_error)?;
    Ok(())
}

/// Resolves a set of namespace paths to their surrogate `namespace_id` UUIDs
/// in a single query. Paths that don't exist in the warehouse are absent from
/// the returned map (write-side counterpart of the warning-and-empty path in
/// `load_view::get_default_namespace_ident`).
async fn resolve_namespace_paths(
    warehouse_id: WarehouseId,
    paths: &HashSet<Vec<String>>,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<HashMap<Vec<String>, Uuid>, CatalogBackendError> {
    if paths.is_empty() {
        return Ok(HashMap::new());
    }
    // Pass each path as a JSON-encoded text array, then decode back to
    // `text[]` server-side — postgres doesn't accept ragged `text[][]`.
    let path_jsons: Vec<serde_json::Value> = paths.iter().map(|p| serde_json::json!(p)).collect();
    let rows = sqlx::query!(
        r#"
        WITH requested AS (
            SELECT ARRAY(SELECT jsonb_array_elements_text(r))::text[] AS namespace_name
            FROM UNNEST($2::jsonb[]) AS r
        )
        SELECT n.namespace_name AS "namespace_name!", n.namespace_id AS "namespace_id!"
        FROM namespace n
        INNER JOIN requested r ON r.namespace_name = n.namespace_name
        WHERE n.warehouse_id = $1
        "#,
        *warehouse_id,
        &path_jsons,
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(|e| {
        e.into_catalog_backend_error()
            .append_detail("Error resolving default-namespace paths during view commit.")
    })?;
    Ok(rows
        .into_iter()
        .map(|r| (r.namespace_name, r.namespace_id))
        .collect())
}

/// Inserts every `view_version` from the metadata in a single batched INSERT.
///
/// Unique default-namespace paths are pre-resolved in one batch query
/// (iceberg view-spec requires the field; Spark sometimes sends an empty
/// path, which we store as NULL; unresolvable paths are also stored as NULL,
/// symmetric to the warning-and-empty behavior in `load_view`).
async fn batch_insert_view_versions<'a>(
    warehouse_id: WarehouseId,
    view_id: Uuid,
    versions: impl IntoIterator<Item = &'a ViewVersionRef>,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), CreateViewVersionError> {
    let versions: Vec<&ViewVersionRef> = versions.into_iter().collect();
    if versions.is_empty() {
        return Ok(());
    }

    let unique_ns: HashSet<Vec<String>> = versions
        .iter()
        .map(|v| v.default_namespace().clone().inner())
        .filter(|ns| !ns.is_empty())
        .collect();
    let ns_map = resolve_namespace_paths(warehouse_id, &unique_ns, &mut *transaction).await?;

    let mut version_ids: Vec<ViewVersionId> = Vec::with_capacity(versions.len());
    let mut schema_ids: Vec<i32> = Vec::with_capacity(versions.len());
    let mut timestamps: Vec<DateTime<Utc>> = Vec::with_capacity(versions.len());
    let mut default_namespace_ids: Vec<Option<Uuid>> = Vec::with_capacity(versions.len());
    let mut default_catalogs: Vec<Option<String>> = Vec::with_capacity(versions.len());
    let mut summaries: Vec<serde_json::Value> = Vec::with_capacity(versions.len());

    for v in &versions {
        version_ids.push(v.version_id());
        schema_ids.push(v.schema_id());
        timestamps.push(
            v.timestamp()
                .map_err(|e| ConversionError::new("view_version.timestamp", e))?,
        );
        let ns_path = v.default_namespace().clone().inner();
        let ns_id = if ns_path.is_empty() {
            None
        } else {
            ns_map.get(&ns_path).copied()
        };
        default_namespace_ids.push(ns_id);
        default_catalogs.push(v.default_catalog().cloned());
        summaries.push(
            serde_json::to_value(v.summary())
                .map_err(|e| SerializationError::new("view_version.summary", e))?,
        );
    }

    sqlx::query!(
        r#"
        INSERT INTO view_version (
            warehouse_id, view_id, version_id, schema_id, timestamp,
            default_namespace_id, default_catalog, summary
        )
        SELECT $1, $2, u.version_id, u.schema_id, u.timestamp,
               u.default_namespace_id, u.default_catalog, u.summary
        FROM UNNEST(
            $3::int[], $4::int[], $5::timestamptz[],
            $6::uuid[], $7::text[], $8::jsonb[]
        ) AS u(version_id, schema_id, timestamp,
               default_namespace_id, default_catalog, summary)
        "#,
        *warehouse_id,
        view_id,
        &version_ids,
        &schema_ids,
        &timestamps,
        &default_namespace_ids as &[Option<Uuid>],
        &default_catalogs as &[Option<String>],
        &summaries,
    )
    .execute(&mut **transaction)
    .await
    .map_err(super::super::dbutils::DBErrorHandler::into_catalog_backend_error)?;
    Ok(())
}

async fn set_current_view_metadata_version(
    warehouse_id: WarehouseId,
    view_id: Uuid,
    version_id: ViewVersionId,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), CatalogBackendError> {
    sqlx::query!(
        r#"
        INSERT INTO current_view_metadata_version (warehouse_id, view_id, version_id)
        VALUES ($1, $2, $3)
        ON CONFLICT (warehouse_id, view_id)
        DO UPDATE SET version_id = $3
        WHERE current_view_metadata_version.view_id = $2
        AND current_view_metadata_version.warehouse_id = $1
        "#,
        *warehouse_id,
        view_id,
        version_id,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| {
        e.into_catalog_backend_error()
            .append_detail("Error setting current view metadata version.")
    })?;

    tracing::debug!("Successfully set current view metadata version");
    Ok(())
}

async fn batch_insert_view_representations<'a>(
    warehouse_id: WarehouseId,
    view_id: Uuid,
    versions: impl IntoIterator<Item = &'a ViewVersionRef>,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), CreateViewVersionError> {
    let mut version_ids: Vec<ViewVersionId> = Vec::new();
    let mut sqls: Vec<String> = Vec::new();
    let mut dialects: Vec<String> = Vec::new();

    for v in versions {
        for rep in v.representations().iter() {
            let ViewRepresentation::Sql(repr) = rep;
            version_ids.push(v.version_id());
            sqls.push(repr.sql.clone());
            dialects.push(repr.dialect.clone());
        }
    }

    if version_ids.is_empty() {
        return Ok(());
    }

    // `view_representation_type` has a single variant (`sql`) — hardcode the
    // cast rather than threading a typed enum array through the bind layer.
    sqlx::query!(
        r#"
        INSERT INTO view_representation (
            warehouse_id, view_id, view_version_id, typ, sql, dialect
        )
        SELECT $1, $2, u.view_version_id, 'sql'::view_representation_type, u.sql, u.dialect
        FROM UNNEST($3::int[], $4::text[], $5::text[])
             AS u(view_version_id, sql, dialect)
        "#,
        *warehouse_id,
        view_id,
        &version_ids,
        &sqls,
        &dialects,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| {
        e.into_catalog_backend_error()
            .append_detail("Error inserting view representations.")
    })?;
    Ok(())
}

#[derive(Debug, sqlx::Type)]
#[sqlx(type_name = "view_format_version", rename_all = "kebab-case")]
pub(crate) enum ViewFormatVersion {
    V1,
}

#[derive(sqlx::Type, Debug, Deserialize)]
#[sqlx(type_name = "view_representation_type", rename_all = "kebab-case")]
#[serde(rename_all = "kebab-case")]
pub(crate) enum ViewRepresentationType {
    Sql,
}

impl From<iceberg::spec::ViewFormatVersion> for ViewFormatVersion {
    fn from(value: iceberg::spec::ViewFormatVersion) -> Self {
        match value {
            iceberg::spec::ViewFormatVersion::V1 => Self::V1,
        }
    }
}

#[cfg(any(test, feature = "test-utils"))]
#[allow(unused_imports, dead_code)]
pub mod tests {
    use iceberg::{
        NamespaceIdent, TableIdent,
        spec::{ViewMetadata, ViewMetadataBuilder},
    };
    use iceberg_ext::configs::ParseFromStr;
    use lakekeeper::{
        WarehouseId,
        api::{iceberg::v1::PaginationQuery, management::v1::DeleteKind},
        service::{
            ArcProjectId, CommitViewError, CreateViewError, DropTabularError, LoadViewError,
            TabularId, TabularIdentBorrowed, TabularListFlags, ViewId,
            tasks::{
                ScheduleTaskMetadata, TaskEntity, WarehouseTaskEntityId,
                tabular_expiration_queue::{TabularExpirationPayload, TabularExpirationTask},
            },
        },
    };
    use lakekeeper_io::Location;
    use serde_json::json;
    use sqlx::PgPool;
    use uuid::Uuid;

    use crate::{
        CatalogState, PostgresBackend,
        namespace::tests::initialize_namespace,
        tabular::{TabularType, mark_tabular_as_deleted, view::load_view},
        warehouse::test::initialize_warehouse,
    };

    pub fn view_request(view_id: Option<Uuid>, location: &Location) -> ViewMetadata {
        serde_json::from_value(json!({
  "format-version": 1,
  "view-uuid": view_id.unwrap_or_else(Uuid::now_v7).to_string(),
  "location": location.as_str(),
  "current-version-id": 2,
  "versions": [
    {
      "version-id": 1,
      "schema-id": 0,
      "timestamp-ms": 1_719_559_079_091_usize,
      "summary": {
        "engine-name": "spark",
        "iceberg-version": "Apache Iceberg 1.5.2 (commit cbb853073e681b4075d7c8707610dceecbee3a82)",
        "engine-version": "3.5.1",
        "app-id": "local-1719559068458"
      },
      "representations": [
        {
          "type": "sql",
          "sql": "select id, strings from spark_demo.my_table",
          "dialect": "spark"
        }
      ],
      "default-namespace": []
    },
    {
      "version-id": 2,
      "schema-id": 1,
      "timestamp-ms": 1_719_559_081_510_usize,
      "summary": {
        "app-id": "local-1719559068458",
        "engine-version": "3.5.1",
        "iceberg-version": "Apache Iceberg 1.5.2 (commit cbb853073e681b4075d7c8707610dceecbee3a82)",
        "engine-name": "spark"
      },
      "representations": [
        {
          "type": "sql",
          "sql": "select id from spark_demo.my_table",
          "dialect": "spark"
        }
      ],
      "default-namespace": []
    }
  ],
  "version-log": [
    {
      "version-id": 1,
      "timestamp-ms": 1_719_559_079_095_usize
    }
  ],
  "schemas": [
    {
      "schema-id": 1,
      "type": "struct",
      "fields": [
        {
          "id": 0,
          "name": "id",
          "required": false,
          "type": "long",
          "doc": "id of thing"
        }
      ]
    },
    {
      "schema-id": 0,
      "type": "struct",
      "fields": [
        {
          "id": 0,
          "name": "id",
          "required": false,
          "type": "long"
        },
        {
          "id": 1,
          "name": "strings",
          "required": false,
          "type": "string"
        }
      ]
    }
  ],
  "properties": {
    "create_engine_version": "Spark 3.5.1",
    "spark.query-column-names": "id",
    "engine_version": "Spark 3.5.1"
  }
}
 )).unwrap()
    }

    #[sqlx::test]
    async fn create_view(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;
        let namespace_id =
            crate::tabular::table::tests::get_namespace_id(state.clone(), warehouse_id, &namespace)
                .await;
        let view_uuid = ViewId::from(Uuid::now_v7());
        let location = "s3://my_bucket/my_table/metadata/bar"
            .parse::<Location>()
            .unwrap();
        let request = view_request(Some(*view_uuid), &location);
        let mut tx = pool.begin().await.unwrap();
        super::create_view(
            warehouse_id,
            namespace_id,
            &format!(
                "s3://my_bucket/my_table/metadata/bar/metadata-{}.gz.json",
                Uuid::now_v7()
            )
            .parse()
            .unwrap(),
            &mut tx,
            "myview",
            &request,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();

        let mut tx = pool.begin().await.unwrap();
        // recreate with same uuid should fail
        let new_location = "s3://my_bucket/my_table/metadata/new-location"
            .parse::<Location>()
            .unwrap();
        let new_request = view_request(Some(*view_uuid), &new_location);
        let created_view_err = super::create_view(
            warehouse_id,
            namespace_id,
            &format!("{new_location}/metadata-{}.gz.json", Uuid::now_v7())
                .parse()
                .unwrap(),
            &mut tx,
            "myview2",
            &new_request,
        )
        .await
        .expect_err("recreation should fail");
        assert!(
            matches!(created_view_err, CreateViewError::TabularAlreadyExists(_)),
            "created_view_err: {created_view_err:?}"
        );
        tx.commit().await.unwrap();

        // recreate with other uuid but same name should fail
        let mut tx = pool.begin().await.unwrap();
        let created_view = super::create_view(
            warehouse_id,
            namespace_id,
            &format!("{new_location}/metadata-{}.gz.json", Uuid::now_v7())
                .parse()
                .unwrap(),
            &mut tx,
            "myview",
            &ViewMetadataBuilder::new_from_metadata(new_request.clone())
                .assign_uuid(Uuid::now_v7())
                .build()
                .unwrap()
                .metadata,
        )
        .await
        .expect_err("recreation should fail");
        assert!(matches!(
            created_view,
            CreateViewError::TabularAlreadyExists(_)
        ));
        tx.commit().await.unwrap();

        let views = super::super::list_tabulars(
            warehouse_id,
            Some(namespace_id),
            TabularListFlags::active(),
            &state.read_pool(),
            Some(TabularType::View),
            PaginationQuery::empty(),
        )
        .await
        .unwrap();
        assert_eq!(views.len(), 1);
        let (list_view_uuid, view) = views.into_iter().next().unwrap();
        assert_eq!(list_view_uuid, TabularId::View(view_uuid));
        assert_eq!(view.tabular_ident().name, "myview");

        // New name and uuid should succeed
        let mut tx = pool.begin().await.unwrap();
        let _created_view = super::create_view(
            warehouse_id,
            namespace_id,
            &format!("{new_location}/metadata-{}.gz.json", Uuid::now_v7())
                .parse()
                .unwrap(),
            &mut tx,
            "myview2",
            &ViewMetadataBuilder::new_from_metadata(new_request.clone())
                .assign_uuid(Uuid::now_v7())
                .build()
                .unwrap()
                .metadata,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();

        let mut tx = pool.begin().await.unwrap();
        let metadata = load_view(warehouse_id, view_uuid, false, &mut tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(&*metadata.metadata, &request);
    }

    #[sqlx::test]
    async fn drop_view_unconditionally(pool: sqlx::PgPool) {
        let (state, created_meta, warehouse_id, _, _, _, _) = prepare_view(pool).await;
        let mut tx: sqlx::Transaction<'_, sqlx::Postgres> =
            state.write_pool().begin().await.unwrap();
        super::super::drop_tabular(
            warehouse_id,
            ViewId::from(created_meta.uuid()).into(),
            false,
            None,
            &mut tx,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();

        let mut tx = state.write_pool().begin().await.unwrap();
        let err = load_view(warehouse_id, created_meta.uuid().into(), false, &mut tx)
            .await
            .expect_err("dropped view should not be loadable");
        tx.commit().await.unwrap();

        assert!(
            matches!(err, LoadViewError::TabularNotFound(_)),
            "err: {err:?}"
        );
    }

    #[sqlx::test]
    async fn drop_view_correct_location(pool: sqlx::PgPool) {
        let (state, created_meta, warehouse_id, _, _, metadata_location, _) =
            prepare_view(pool).await;
        let mut tx = state.write_pool().begin().await.unwrap();
        super::super::drop_tabular(
            warehouse_id,
            ViewId::from(created_meta.uuid()).into(),
            false,
            Some(&metadata_location),
            &mut tx,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        let mut tx = state.write_pool().begin().await.unwrap();
        let err = load_view(warehouse_id, created_meta.uuid().into(), false, &mut tx)
            .await
            .expect_err("dropped view should not be loadable");
        tx.commit().await.unwrap();

        assert!(
            matches!(err, LoadViewError::TabularNotFound(_)),
            "err: {err:?}"
        );
    }

    #[sqlx::test]
    async fn test_drop_view_metadata_mismatch(pool: sqlx::PgPool) {
        let (state, created_meta, warehouse_id, _, _, _, _) = prepare_view(pool).await;
        let mut tx = state.write_pool().begin().await.unwrap();
        let err = super::super::drop_tabular(
            warehouse_id,
            ViewId::from(created_meta.uuid()).into(),
            false,
            Some(&Location::parse_value("s3://not-the/old-location").unwrap()),
            &mut tx,
        )
        .await
        .expect_err("dropping view with wrong metadata location should fail");
        tx.commit().await.unwrap();

        assert!(matches!(err, DropTabularError::ConcurrentUpdateError(_)));
    }

    #[sqlx::test]
    async fn soft_drop_view(pool: sqlx::PgPool) {
        let (state, created_meta, warehouse_id, _, _, _, project_id) = prepare_view(pool).await;
        let mut tx = state.write_pool().begin().await.unwrap();

        let _ = TabularExpirationTask::schedule_task::<PostgresBackend>(
            ScheduleTaskMetadata {
                project_id,
                parent_task_id: None,
                scheduled_for: Some(chrono::Utc::now() + chrono::Duration::seconds(1)),
                entity: TaskEntity::EntityInWarehouse {
                    entity_name: vec!["myview".to_string()],
                    entity_id: WarehouseTaskEntityId::View {
                        view_id: created_meta.uuid().into(),
                    },
                    warehouse_id,
                },
            },
            TabularExpirationPayload::new(DeleteKind::Purge),
            &mut tx,
        )
        .await
        .unwrap();
        mark_tabular_as_deleted(
            warehouse_id,
            TabularId::View(created_meta.uuid().into()),
            false,
            None,
            &mut tx,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        let mut tx = state.write_pool().begin().await.unwrap();
        load_view(warehouse_id, created_meta.uuid().into(), true, &mut tx)
            .await
            .expect("soft-dropped view should loadable");
        tx.commit().await.unwrap();

        let mut tx = state.write_pool().begin().await.unwrap();
        super::super::drop_tabular(
            warehouse_id,
            ViewId::from(created_meta.uuid()).into(),
            false,
            None,
            &mut tx,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();

        let mut tx = state.write_pool().begin().await.unwrap();
        load_view(warehouse_id, created_meta.uuid().into(), true, &mut tx)
            .await
            .expect_err("hard-delete view should not be loadable");
        tx.commit().await.unwrap();
    }

    #[sqlx::test]
    async fn view_exists(pool: sqlx::PgPool) {
        let (state, _created_meta, warehouse_id, namespace, name, _, _) = prepare_view(pool).await;
        let view_ident = TableIdent {
            namespace: namespace.clone(),
            name: name.clone(),
        };
        let view_ident_borrowed = TabularIdentBorrowed::View(&view_ident);
        let exists = super::super::get_tabular_infos_by_idents(
            warehouse_id,
            &[view_ident_borrowed],
            TabularListFlags::all(),
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert_eq!(exists.len(), 1);

        let non_existing_view_ident = TableIdent {
            namespace: namespace.clone(),
            name: "non_existing".to_string(),
        };
        let non_existing_view_ident_borrowed = TabularIdentBorrowed::View(&non_existing_view_ident);
        let non_exists = super::super::get_tabular_infos_by_idents(
            warehouse_id,
            &[non_existing_view_ident_borrowed],
            TabularListFlags::all(),
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert_eq!(non_exists.len(), 0);
    }

    #[sqlx::test]
    async fn drop_view_not_existing(pool: sqlx::PgPool) {
        let (state, _, warehouse_id, _, _, _, _) = prepare_view(pool).await;
        let mut tx = state.write_pool().begin().await.unwrap();
        let e = super::super::drop_tabular(
            warehouse_id,
            ViewId::new_random().into(),
            false,
            None,
            &mut tx,
        )
        .await
        .expect_err("dropping random uuid should not succeed");
        tx.commit().await.unwrap();
        assert!(matches!(e, DropTabularError::TabularNotFound(_)));
    }

    #[sqlx::test]
    async fn test_view_case_insensitive_lookup(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;
        let namespace_id =
            crate::tabular::table::tests::get_namespace_id(state.clone(), warehouse_id, &namespace)
                .await;

        let location = "s3://my_bucket/my_view/metadata/bar"
            .parse::<Location>()
            .unwrap();
        let request = view_request(None, &location);
        let mut tx = pool.begin().await.unwrap();
        super::create_view(
            warehouse_id,
            namespace_id,
            &format!(
                "s3://my_bucket/my_view/metadata/bar/metadata-{}.gz.json",
                Uuid::now_v7()
            )
            .parse()
            .unwrap(),
            &mut tx,
            "my_view",
            &request,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();

        // Lookup with uppercase name and namespace
        let upper_namespace = NamespaceIdent::from_vec(vec!["MY_NAMESPACE".to_string()]).unwrap();
        let upper_ident = TableIdent {
            namespace: upper_namespace,
            name: "MY_VIEW".to_string(),
        };
        let infos = super::super::get_tabular_infos_by_idents(
            warehouse_id,
            &[TabularIdentBorrowed::View(&upper_ident)],
            TabularListFlags::active(),
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert_eq!(infos.len(), 1);

        // Creating a duplicate view with different case should fail on the
        // name uniqueness constraint. Use a distinct storage location so
        // `ensure_location_available` doesn't short-circuit this with
        // `LocationAlreadyTaken`.
        let second_location = "s3://my_bucket/my_view_v2/metadata"
            .parse::<Location>()
            .unwrap();
        let mut tx = pool.begin().await.unwrap();
        let err = super::create_view(
            warehouse_id,
            namespace_id,
            &format!("{second_location}/metadata-{}.gz.json", Uuid::now_v7())
                .parse()
                .unwrap(),
            &mut tx,
            "MY_VIEW",
            &view_request(Some(Uuid::now_v7()), &second_location),
        )
        .await
        .expect_err("duplicate view name with different case should fail");
        assert!(matches!(err, CreateViewError::TabularAlreadyExists(_)));
    }

    #[sqlx::test]
    async fn commit_existing_view_detects_stale_previous_metadata_location(pool: PgPool) {
        let (state, metadata, warehouse_id, namespace, _, metadata_location, _) =
            prepare_view(pool).await;
        let namespace_id =
            crate::tabular::table::tests::get_namespace_id(state.clone(), warehouse_id, &namespace)
                .await;

        let stale_metadata_location: Location = format!(
            "s3://my_bucket/my_table/metadata/bar/metadata-{}.gz.json",
            Uuid::now_v7()
        )
        .parse()
        .unwrap();
        assert_ne!(stale_metadata_location, metadata_location);

        let new_metadata_location: Location = format!(
            "s3://my_bucket/my_table/metadata/bar/metadata-{}.gz.json",
            Uuid::now_v7()
        )
        .parse()
        .unwrap();

        let mut tx = state.write_pool().begin().await.unwrap();
        let err = super::commit_existing_view(
            warehouse_id,
            namespace_id,
            &new_metadata_location,
            &stale_metadata_location,
            &mut tx,
            &metadata,
        )
        .await
        .expect_err("commit with stale previous_metadata_location should fail");
        assert!(
            matches!(err, CommitViewError::ConcurrentUpdateError(_)),
            "expected ConcurrentUpdateError, got: {err:?}"
        );
    }

    #[sqlx::test]
    async fn commit_existing_view_returns_tabular_not_found_when_view_absent(pool: PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;
        let namespace_id =
            crate::tabular::table::tests::get_namespace_id(state.clone(), warehouse_id, &namespace)
                .await;

        // A view UUID that was never created.
        let absent_view_uuid = Uuid::now_v7();
        let location = "s3://my_bucket/missing_view/metadata"
            .parse::<Location>()
            .unwrap();
        let metadata = view_request(Some(absent_view_uuid), &location);
        let new_metadata_location: Location = format!(
            "s3://my_bucket/missing_view/metadata/metadata-{}.gz.json",
            Uuid::now_v7()
        )
        .parse()
        .unwrap();
        let previous_metadata_location: Location = format!(
            "s3://my_bucket/missing_view/metadata/metadata-{}.gz.json",
            Uuid::now_v7()
        )
        .parse()
        .unwrap();

        let mut tx = state.write_pool().begin().await.unwrap();
        let err = super::commit_existing_view(
            warehouse_id,
            namespace_id,
            &new_metadata_location,
            &previous_metadata_location,
            &mut tx,
            &metadata,
        )
        .await
        .expect_err("commit against absent view should fail");
        assert!(
            matches!(err, CommitViewError::TabularNotFound(_)),
            "expected TabularNotFound, got: {err:?}"
        );
    }

    #[sqlx::test]
    async fn commit_existing_view_rejects_colliding_location(pool: PgPool) {
        // First view at the location used by `prepare_view`
        // (`s3://my_bucket/my_table/metadata/bar`).
        let (state, _, warehouse_id, namespace, _, _, _) = prepare_view(pool.clone()).await;
        let namespace_id =
            crate::tabular::table::tests::get_namespace_id(state.clone(), warehouse_id, &namespace)
                .await;

        // Second view at a distinct location.
        let other_location = "s3://my_bucket/other_view/metadata"
            .parse::<Location>()
            .unwrap();
        let other_metadata_location: Location = format!("{other_location}/metadata-init.gz.json")
            .parse()
            .unwrap();
        let other_view_uuid = Uuid::now_v7();
        let other_request = view_request(Some(other_view_uuid), &other_location);
        let mut tx = state.write_pool().begin().await.unwrap();
        super::create_view(
            warehouse_id,
            namespace_id,
            &other_metadata_location,
            &mut tx,
            "other_view",
            &other_request,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();

        // Build metadata for `other_view` whose `location()` collides with the
        // first view's storage path — the location-collision check must reject
        // it. The fixture's `view_request` constructs the metadata JSON
        // directly with the provided location, so reuse it with the same UUID.
        let stolen_location = "s3://my_bucket/my_table/metadata/bar"
            .parse::<Location>()
            .unwrap();
        let stolen_metadata = view_request(Some(other_view_uuid), &stolen_location);

        let new_metadata_location: Location = format!("{stolen_location}/metadata-2.gz.json")
            .parse()
            .unwrap();
        let mut tx = state.write_pool().begin().await.unwrap();
        let err = super::commit_existing_view(
            warehouse_id,
            namespace_id,
            &new_metadata_location,
            &other_metadata_location,
            &mut tx,
            &stolen_metadata,
        )
        .await
        .expect_err("commit at a colliding location should fail");
        assert!(
            matches!(err, CommitViewError::LocationAlreadyTaken(_)),
            "expected LocationAlreadyTaken, got: {err:?}"
        );
    }

    #[sqlx::test]
    async fn commit_existing_view_cleans_old_sub_metadata(pool: PgPool) {
        // `clear_view_metadata` only deletes from `view_properties` and
        // `view_schema`; everything else relies on ON DELETE CASCADE. If a
        // future migration weakens any CASCADE link, the second
        // `populate_view_metadata` call below will hit a PK collision because
        // the prior version/representation/log rows weren't cleared. This
        // test asserts the cascade chain still works end-to-end.
        let (state, metadata, warehouse_id, namespace, _, metadata_location, _) =
            prepare_view(pool.clone()).await;
        let namespace_id =
            crate::tabular::table::tests::get_namespace_id(state.clone(), warehouse_id, &namespace)
                .await;
        let view_uuid = metadata.uuid();
        let expected_schemas = i64::try_from(metadata.schemas_iter().len()).unwrap();
        let expected_versions = i64::try_from(metadata.versions().len()).unwrap();
        let expected_reps = i64::try_from(
            metadata
                .versions()
                .map(|v| v.representations().iter().count())
                .sum::<usize>(),
        )
        .unwrap();

        let new_metadata_location: Location = format!(
            "s3://my_bucket/my_table/metadata/bar/metadata-{}.gz.json",
            Uuid::now_v7()
        )
        .parse()
        .unwrap();
        let mut tx = state.write_pool().begin().await.unwrap();
        super::commit_existing_view(
            warehouse_id,
            namespace_id,
            &new_metadata_location,
            &metadata_location,
            &mut tx,
            &metadata,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();

        let schema_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM view_schema WHERE view_id = $1")
                .bind(view_uuid)
                .fetch_one(&state.read_pool())
                .await
                .unwrap();
        let version_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM view_version WHERE view_id = $1")
                .bind(view_uuid)
                .fetch_one(&state.read_pool())
                .await
                .unwrap();
        let rep_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM view_representation WHERE view_id = $1")
                .bind(view_uuid)
                .fetch_one(&state.read_pool())
                .await
                .unwrap();

        assert_eq!(schema_count, expected_schemas, "view_schema not cleared");
        assert_eq!(version_count, expected_versions, "view_version not cleared");
        assert_eq!(rep_count, expected_reps, "view_representation not cleared");
    }

    async fn prepare_view(
        pool: PgPool,
    ) -> (
        CatalogState,
        ViewMetadata,
        WarehouseId,
        NamespaceIdent,
        String,
        Location,
        ArcProjectId,
    ) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (project_id, warehouse_id) =
            initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;
        let namespace_id =
            crate::tabular::table::tests::get_namespace_id(state.clone(), warehouse_id, &namespace)
                .await;
        let location = "s3://my_bucket/my_table/metadata/bar"
            .parse::<Location>()
            .unwrap();
        let metadata_location = format!(
            "s3://my_bucket/my_table/metadata/bar/metadata-{}.gz.json",
            Uuid::now_v7()
        )
        .parse()
        .unwrap();
        let request = view_request(None, &location);
        let mut tx = pool.begin().await.unwrap();
        super::create_view(
            warehouse_id,
            namespace_id,
            &metadata_location,
            &mut tx,
            "myview",
            &request,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();

        (
            state,
            request,
            warehouse_id,
            namespace,
            "myview".into(),
            metadata_location,
            project_id,
        )
    }
}
