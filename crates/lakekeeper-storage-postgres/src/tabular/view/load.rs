use std::{
    collections::HashMap,
    str::FromStr as _,
    sync::{Arc, LazyLock},
};

use chrono::{DateTime, Utc};
use iceberg::{
    NamespaceIdent,
    spec::{
        SqlViewRepresentation, ViewMetadata, ViewMetadataParts, ViewRepresentation,
        ViewRepresentations, ViewVersion, ViewVersionId, ViewVersionLog,
    },
};
use itertools::izip;
use lakekeeper::{
    WarehouseId,
    service::{
        CatalogBackendError, CatalogGetNamespaceError, CatalogNamespaceOps, CatalogView,
        InternalParseLocationError, InvalidViewRepresentationsInternal, LoadViewError, NamespaceId,
        RequiredViewComponentMissing, TabularNotFound, ViewId,
        ViewMetadataValidationFailedInternal, storage::join_location,
    },
};
use lakekeeper_io::Location;
use sqlx::{FromRow, PgConnection, types::Json};
use uuid::Uuid;

use crate::{
    PostgresBackend, PostgresTransactionType,
    dbutils::DBErrorHandler,
    tabular::{
        prepare_properties,
        table::normalized_schema,
        view::{ViewFormatVersion, ViewRepresentationType},
    },
};

pub(crate) async fn load_view(
    warehouse_id: WarehouseId,
    view_id: ViewId,
    include_deleted: bool,
    conn: PostgresTransactionType<'_>,
) -> Result<CatalogView, LoadViewError> {
    let Query {
        view_id,
        view_format_version,
        view_fs_location,
        view_fs_protocol,
        metadata_location,
        current_version_id,
        view_properties_keys,
        view_properties_values,
        version_ids,
        version_schema_ids,
        version_timestamps,
        version_default_namespace_ids,
        version_default_catalogs,
        version_metadata_summaries,
        version_log_ids,
        version_log_timestamps,
        view_representation_typ,
        view_representation_sql,
        view_representation_dialect,
        warehouse_version,
    } = query(warehouse_id, *view_id, include_deleted, &mut *conn)
        .await?
        .ok_or_else(|| TabularNotFound::new(warehouse_id, view_id))?;

    let view_id: ViewId = view_id.into();

    let schema_field_rows = sqlx::query!(
        r#"SELECT schema_id, field_id, parent_field_id, ordinal, name, required, doc,
                  type_kind::text as "type_kind!", type_params, initial_default, write_default,
                  is_identifier
           FROM schema_field
           WHERE warehouse_id = $1 AND tabular_id = $2
           ORDER BY schema_id, parent_field_id, ordinal"#,
        *warehouse_id,
        *view_id,
    )
    .fetch_all(&mut **conn)
    .await
    .map_err(|e| e.into_catalog_backend_error())?;

    let rows: Vec<normalized_schema::SchemaFieldRow> = schema_field_rows
        .into_iter()
        .map(|r| normalized_schema::SchemaFieldRow {
            schema_id: r.schema_id,
            field_id: r.field_id,
            parent_field_id: r.parent_field_id,
            ordinal: r.ordinal,
            name: r.name,
            required: r.required,
            doc: r.doc,
            type_kind: r.type_kind,
            type_params: r.type_params,
            initial_default: r.initial_default,
            write_default: r.write_default,
            is_identifier: r.is_identifier,
        })
        .collect();
    // Seed EVERY anchor, not just the current schema's: `try_from_parts` only validates the current
    // version's schema id, so a missing non-current anchor would otherwise silently vanish. Such
    // non-current schemas load as empty (inert); an empty current schema is rejected below.
    let schema_anchor_rows = sqlx::query!(
        r#"SELECT schema_id FROM view_schema WHERE warehouse_id = $1 AND view_id = $2"#,
        *warehouse_id,
        *view_id,
    )
    .fetch_all(&mut **conn)
    .await
    .map_err(|e| e.into_catalog_backend_error())?;
    let seed_empty_schema_ids: Vec<i32> = schema_anchor_rows
        .into_iter()
        .map(|r| r.schema_id)
        .collect();

    let schemas =
        normalized_schema::assemble_schemas(rows, &seed_empty_schema_ids).map_err(|e| {
            RequiredViewComponentMissing::new(warehouse_id, view_id).append_detail(format!(
                "Failed to assemble view schemas from schema_field rows: {e}"
            ))
        })?;

    let properties = prepare_properties(view_properties_keys, view_properties_values);
    let version_log = prepare_version_log(version_log_ids, version_log_timestamps);

    let versions = prepare_versions(
        &mut *conn,
        warehouse_id,
        view_id,
        VersionsPrep {
            version_ids,
            version_schema_ids,
            version_timestamps,
            version_default_namespace_ids,
            version_default_catalogs,
            version_metadata_summaries,
            view_representation_typ,
            view_representation_sql,
            view_representation_dialect,
        },
    )
    .await?;

    let metadata_location =
        Location::from_str(&metadata_location).map_err(InternalParseLocationError::from)?;
    let location = join_location(&view_fs_protocol, &view_fs_location)
        .map_err(InternalParseLocationError::from)?;
    let metadata = ViewMetadata::try_from_parts(ViewMetadataParts {
        format_version: match view_format_version {
            ViewFormatVersion::V1 => iceberg::spec::ViewFormatVersion::V1,
        },
        view_uuid: *view_id,
        location: location.to_string(),
        current_version_id,
        versions,
        version_log,
        schemas,
        properties,
    })
    .map(Arc::new)
    .map_err(|e| {
        ViewMetadataValidationFailedInternal::new(warehouse_id, view_id).append_detail(e.message())
    })?;

    // The current schema must be non-empty: a view's schema is its SQL query's output (always >=1
    // column, unlike a zero-column table), so zero rows here means lost field rows — fail loud.
    if metadata.current_schema().as_struct().fields().is_empty() {
        return Err(RequiredViewComponentMissing::new(warehouse_id, view_id)
            .append_detail(format!(
                "Current view version {} schema {} has no fields (schema_field rows missing).",
                metadata.current_version_id(),
                metadata.current_schema().schema_id()
            ))
            .into());
    }

    Ok(CatalogView {
        metadata_location,
        warehouse_version: warehouse_version.into(),
        metadata,
        location,
    })
}

async fn query(
    warehouse_id: WarehouseId,
    view_id: Uuid,
    include_deleted: bool,
    conn: &mut PgConnection,
) -> Result<Option<Query>, CatalogBackendError> {
    let rs = sqlx::query_as!(Query,
            r#"
SELECT v.view_id,
       v.view_format_version             AS "view_format_version: ViewFormatVersion",
       ta.fs_location                    AS view_fs_location,
       ta.fs_protocol                    AS view_fs_protocol,
       ta.metadata_location              AS "metadata_location!",
       cvv.version_id                    AS current_version_id,
       vp.view_properties_keys,
       vp.view_properties_values,
       vvr.version_ids                   AS "version_ids!: Vec<ViewVersionId>",
       vvr.version_schema_ids,
       vvr.version_timestamps,
       vvr.version_default_namespace_ids AS "version_default_namespace_ids!: Vec<Option<Uuid>>",
       vvr.version_default_catalogs      AS "version_default_catalogs!: Vec<Option<String>>",
       vvr.summaries                     AS "version_metadata_summaries: Vec<Json<HashMap<String, String>>>",
       vvl.version_log_ids,
       vvl.version_log_timestamps,
       vvr.typ                           AS "view_representation_typ: Json<Vec<Vec<ViewRepresentationType>>>",
       vvr.sql                           AS "view_representation_sql: Json<Vec<Vec<String>>>",
       vvr.dialect                       AS "view_representation_dialect: Json<Vec<Vec<String>>>",
       w.version                         AS warehouse_version
FROM view v
         INNER JOIN tabular ta ON ta.warehouse_id = $1 AND ta.tabular_id = v.view_id
         INNER JOIN warehouse w ON w.warehouse_id = $1
         INNER JOIN current_view_metadata_version cvv
             ON cvv.warehouse_id = $1 AND v.view_id = cvv.view_id
         LEFT JOIN (SELECT view_id,
                           ARRAY_AGG(version_id) AS version_log_ids,
                           ARRAY_AGG(timestamp)  AS version_log_timestamps
                    FROM view_version_log
                    WHERE warehouse_id = $1 and view_id = $2
                    GROUP BY view_id) vvl
                    ON v.view_id = vvl.view_id
         LEFT JOIN (SELECT view_id,
                           ARRAY_AGG(key)   AS view_properties_keys,
                           ARRAY_AGG(value) AS view_properties_values
                    FROM view_properties
                    WHERE warehouse_id = $1 and view_id = $2
                    GROUP BY view_id) vp
                    ON v.view_id = vp.view_id
         LEFT JOIN (SELECT vv.view_id,
                           ARRAY_AGG(version_id)           AS version_ids,
                           ARRAY_AGG(summary)              AS summaries,
                           ARRAY_AGG(schema_id)            AS version_schema_ids,
                           ARRAY_AGG(timestamp)            AS version_timestamps,
                           ARRAY_AGG(default_namespace_id) AS version_default_namespace_ids,
                           ARRAY_AGG(default_catalog)      AS version_default_catalogs,
                           JSONB_AGG(typ)                  as "typ",
                           JSONB_AGG(sql)                  as "sql",
                           JSONB_AGG(dialect)              as "dialect"
                    FROM view_version vv
                             LEFT JOIN (SELECT view_id,
                                               view_version_id,
                                               ARRAY_AGG(typ)     as typ,
                                               ARRAY_AGG(sql)     as sql,
                                               ARRAY_AGG(dialect) as dialect
                                        FROM view_representation
                                        WHERE warehouse_id = $1 and view_id = $2
                                        GROUP BY view_version_id, view_id) vr
                                        ON vv.version_id = vr.view_version_id AND vv.view_id = vr.view_id
                    WHERE vv.warehouse_id = $1 and vv.view_id = $2
                    GROUP BY vv.view_id) vvr ON v.view_id = vvr.view_id
         WHERE v.warehouse_id = $1 AND v.view_id = $2 AND (ta.deleted_at is NULL OR $3)"#,
            *warehouse_id,
            view_id,
            include_deleted
        )
        .fetch_optional(&mut *conn)
        .await.map_err(|e| {
        e.into_catalog_backend_error()
    })?;
    Ok(rs)
}

async fn prepare_versions(
    conn: PostgresTransactionType<'_>,
    warehouse_id: WarehouseId,
    view_id: ViewId,
    VersionsPrep {
        version_ids,
        version_schema_ids,
        version_timestamps,
        version_default_namespace_ids,
        version_default_catalogs,
        version_metadata_summaries,
        view_representation_typ,
        view_representation_sql,
        view_representation_dialect,
    }: VersionsPrep,
) -> Result<HashMap<ViewVersionId, Arc<ViewVersion>>, LoadViewError> {
    let version_schema_ids = version_schema_ids.ok_or_else(|| {
        RequiredViewComponentMissing::new(warehouse_id, view_id)
            .append_detail("Version Schema IDs missing")
    })?;
    let version_timestamps = version_timestamps.ok_or_else(|| {
        RequiredViewComponentMissing::new(warehouse_id, view_id)
            .append_detail("Version Timestamps missing")
    })?;
    let version_metadata_summary = version_metadata_summaries.ok_or_else(|| {
        RequiredViewComponentMissing::new(warehouse_id, view_id)
            .append_detail("Version Metadata Summaries missing")
    })?;
    let version_representation_typ = view_representation_typ
        .ok_or_else(|| {
            RequiredViewComponentMissing::new(warehouse_id, view_id)
                .append_detail("Version Representation Types missing")
        })?
        .0;
    let version_representation_sql = view_representation_sql
        .ok_or_else(|| {
            RequiredViewComponentMissing::new(warehouse_id, view_id)
                .append_detail("Version Representation SQLs missing")
        })?
        .0;
    let version_representation_dialect = view_representation_dialect
        .ok_or_else(|| {
            RequiredViewComponentMissing::new(warehouse_id, view_id)
                .append_detail("Version Representation Dialects missing")
        })?
        .0;

    let mut versions = HashMap::new();
    for (
        version_id,
        timestamp,
        version_default_cat,
        version_default_ns,
        version_meta_summary,
        schema_id,
        typs,
        dialects,
        sqls,
    ) in izip!(
        version_ids,
        version_timestamps,
        version_default_catalogs,
        version_default_namespace_ids,
        version_metadata_summary,
        version_schema_ids,
        version_representation_typ,
        version_representation_dialect,
        version_representation_sql,
    ) {
        let default_namespace_ident =
            get_default_namespace_ident(warehouse_id, version_default_ns.map(Into::into), conn)
                .await?;
        let reps: Vec<ViewRepresentation> = izip!(typs, dialects, sqls)
            .map(|(typ, dialect, sql)| match typ {
                ViewRepresentationType::Sql => {
                    ViewRepresentation::Sql(SqlViewRepresentation { sql, dialect })
                }
            })
            .collect();

        let builder = ViewVersion::builder()
            .with_timestamp_ms(timestamp.timestamp_millis())
            .with_version_id(version_id)
            .with_default_namespace(default_namespace_ident)
            .with_default_catalog(version_default_cat)
            .with_schema_id(schema_id)
            .with_summary(version_meta_summary.0)
            .with_representations(
                ViewRepresentations::builder()
                    .add_all_representations(reps)
                    .build()
                    .map_err(|e| {
                        InvalidViewRepresentationsInternal::new(warehouse_id, view_id)
                            .append_detail(e.message())
                    })?,
            )
            .build();

        versions.insert(version_id, Arc::new(builder));
    }
    Ok(versions)
}

fn prepare_version_log(
    version_log_ids: Option<Vec<ViewVersionId>>,
    version_log_timestamps: Option<Vec<DateTime<Utc>>>,
) -> Vec<ViewVersionLog> {
    if let (Some(log_ids), Some(log_timestamps)) = (version_log_ids, version_log_timestamps) {
        izip!(log_ids, log_timestamps)
            .map(|(id, ts)| ViewVersionLog::new(id, ts.timestamp_millis()))
            .collect()
    } else {
        vec![]
    }
}

// Default Namespace is a required field. Yet, some query engines (e.g. Spark) may not send
// any value for it. In this case, we should return an empty `NamespaceIdent`.
// `NamespaceIdent` does not allow empty vecs, hence this workaround.
static EMPTY_NAMESPACE_IDENT: LazyLock<NamespaceIdent> =
    LazyLock::new(|| serde_json::from_value(serde_json::Value::Array(vec![])).unwrap());

async fn get_default_namespace_ident(
    warehouse_id: WarehouseId,
    default_namespace: Option<NamespaceId>,
    conn: PostgresTransactionType<'_>,
) -> Result<NamespaceIdent, CatalogGetNamespaceError> {
    let Some(default_namespace) = default_namespace else {
        return Ok(EMPTY_NAMESPACE_IDENT.clone());
    };

    let namespace = PostgresBackend::get_namespace(warehouse_id, default_namespace, conn).await?;
    let namespace_ident = namespace.map_or_else(
        || {
            tracing::warn!(
                "Default namespace id '{default_namespace}' not found; returning empty default namespace."
            );
            EMPTY_NAMESPACE_IDENT.clone()
        },
        |n| n.namespace_ident().clone(),
    );
    Ok(namespace_ident)
}

#[derive(FromRow)]
struct Query {
    view_id: Uuid,
    view_format_version: ViewFormatVersion,
    view_fs_location: String,
    view_fs_protocol: String,
    metadata_location: String,
    current_version_id: ViewVersionId,
    view_properties_keys: Option<Vec<String>>,
    view_properties_values: Option<Vec<String>>,
    version_ids: Vec<ViewVersionId>,
    version_schema_ids: Option<Vec<i32>>,
    version_timestamps: Option<Vec<chrono::DateTime<Utc>>>,
    version_default_namespace_ids: Vec<Option<Uuid>>,
    version_default_catalogs: Vec<Option<String>>,
    version_metadata_summaries: Option<Vec<Json<HashMap<String, String>>>>,
    version_log_ids: Option<Vec<ViewVersionId>>,
    version_log_timestamps: Option<Vec<chrono::DateTime<Utc>>>,
    view_representation_typ: Option<Json<Vec<Vec<ViewRepresentationType>>>>,
    view_representation_sql: Option<Json<Vec<Vec<String>>>>,
    view_representation_dialect: Option<Json<Vec<Vec<String>>>>,
    warehouse_version: i64,
}

struct VersionsPrep {
    version_ids: Vec<ViewVersionId>,
    version_schema_ids: Option<Vec<i32>>,
    version_timestamps: Option<Vec<DateTime<Utc>>>,
    version_default_namespace_ids: Vec<Option<Uuid>>,
    version_default_catalogs: Vec<Option<String>>,
    version_metadata_summaries: Option<Vec<Json<HashMap<String, String>>>>,
    view_representation_typ: Option<Json<Vec<Vec<ViewRepresentationType>>>>,
    view_representation_sql: Option<Json<Vec<Vec<String>>>>,
    view_representation_dialect: Option<Json<Vec<Vec<String>>>>,
}
