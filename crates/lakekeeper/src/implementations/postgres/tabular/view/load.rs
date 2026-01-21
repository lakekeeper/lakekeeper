use std::{
    collections::HashMap,
    str::FromStr as _,
    sync::{Arc, LazyLock},
};

use chrono::{DateTime, Utc};
use iceberg::{
    NamespaceIdent,
    spec::{
        Schema, SqlViewRepresentation, ViewMetadata, ViewMetadataParts, ViewRepresentation,
        ViewRepresentations, ViewVersion, ViewVersionId, ViewVersionLog,
    },
};
use itertools::izip;
use lakekeeper_io::Location;
use sqlx::{FromRow, PgConnection, types::Json};
use uuid::Uuid;

use crate::{
    WarehouseId,
    implementations::postgres::{
        PostgresBackend, PostgresTransactionType,
        dbutils::DBErrorHandler,
        tabular::{
            prepare_properties,
            view::{ViewFormatVersion, ViewRepresentationType},
        },
    },
    service::{
        CatalogBackendError, CatalogGetNamespaceError, CatalogNamespaceOps, CatalogView,
        InternalParseLocationError, InvalidViewRepresentationsInternal, LoadViewError, NamespaceId,
        RequiredViewComponentMissing, TabularNotFound, ViewId,
        ViewMetadataValidationFailedInternal, storage::join_location,
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
        schema_ids,
        schemas,
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

    let view_id = view_id.into();
    let schemas = prepare_schemas(warehouse_id, view_id, schema_ids, schemas)?;
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
    Ok(CatalogView {
        metadata_location,
        warehouse_version: warehouse_version.into(),
        metadata: ViewMetadata::try_from_parts(ViewMetadataParts {
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
            ViewMetadataValidationFailedInternal::new(warehouse_id, view_id)
                .append_detail(e.message())
        })?,
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
       vs.schema_ids,
       vs.schemas                        AS "schemas: Vec<Json<Schema>>",
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
                           ARRAY_AGG(schema_id) AS schema_ids,
                           ARRAY_AGG(schema)    AS schemas
                    FROM view_schema
                    WHERE warehouse_id = $1 and view_id = $2
                    GROUP BY view_id) vs
                    ON v.view_id = vs.view_id
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

fn prepare_schemas(
    warehouse_id: WarehouseId,
    view_id: ViewId,
    schema_ids: Option<Vec<i32>>,
    schemas: Option<Vec<Json<Schema>>>,
) -> Result<HashMap<i32, Arc<Schema>>, RequiredViewComponentMissing> {
    let schema_ids = schema_ids.ok_or_else(|| {
        RequiredViewComponentMissing::new(warehouse_id, view_id).append_detail("Schema IDs missing")
    })?;
    let schemas = schemas.ok_or_else(|| {
        RequiredViewComponentMissing::new(warehouse_id, view_id).append_detail("No Schema found")
    })?;
    let schemas = schema_ids
        .into_iter()
        .zip(schemas)
        .map(|(id, schema)| Ok((id, Arc::new(schema.0))))
        .collect::<Result<HashMap<_, _>, _>>()?;
    Ok(schemas)
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
    schema_ids: Option<Vec<i32>>,
    schemas: Option<Vec<Json<Schema>>>,
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
