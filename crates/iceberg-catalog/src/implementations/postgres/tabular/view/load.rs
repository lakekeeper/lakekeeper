use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, Utc};
use iceberg::{
    spec::{
        Schema, SqlViewRepresentation, ViewMetadata, ViewMetadataParts, ViewRepresentation,
        ViewRepresentations, ViewVersion, ViewVersionId, ViewVersionLog,
    },
    NamespaceIdent,
};
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
use itertools::izip;
use sqlx::{types::Json, FromRow, PgConnection};
use uuid::Uuid;

use crate::{
    api::Result,
    implementations::postgres::{
        dbutils::DBErrorHandler,
        tabular::view::{ViewFormatVersion, ViewRepresentationType},
    },
    service::{storage::join_location, ViewId, ViewMetadataWithLocation},
};

pub(crate) async fn load_view(
    view_id: ViewId,
    include_deleted: bool,
    conn: &mut PgConnection,
) -> Result<ViewMetadataWithLocation> {
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
    } = query(*view_id, include_deleted, &mut *conn).await?;

    let schemas = prepare_schemas(schema_ids, schemas)?;
    let properties = prepare_props(view_properties_keys, view_properties_values)?;
    let version_log = prepare_version_log(version_log_ids, version_log_timestamps)?;

    let versions = prepare_versions(
        &mut *conn,
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
    Ok(ViewMetadataWithLocation {
        metadata_location,
        metadata: ViewMetadata::try_from_parts(ViewMetadataParts {
            format_version: match view_format_version {
                ViewFormatVersion::V1 => iceberg::spec::ViewFormatVersion::V1,
            },
            view_uuid: view_id,
            location: join_location(&view_fs_protocol, &view_fs_location).to_string(),
            current_version_id,
            versions,
            version_log,
            schemas,
            properties,
        })
        .map_err(|e| {
            let message = "Received invalid ViewMetadata from database";
            tracing::error!("{}: '{e:?}'", message);
            ErrorModel::internal(message, "InternalDatabaseError", Some(Box::new(e)))
        })?,
    })
}

async fn query(
    view_id: Uuid,
    include_deleted: bool,
    conn: &mut PgConnection,
) -> Result<Query, IcebergErrorResponse> {
    let rs = sqlx::query_as!(Query,
            r#"SELECT v.view_id,
       v.view_format_version            as "view_format_version: ViewFormatVersion",
       ta.fs_location                      AS view_fs_location,
       ta.fs_protocol                      AS view_fs_protocol,
       ta.metadata_location             AS "metadata_location!",
       cvv.version_id                   AS current_version_id,
       vs.schema_ids,
       vs.schemas                       as "schemas: Vec<Json<Schema>>",
       vp.view_properties_keys,
       vp.view_properties_values,
       vvr.version_ids                  as "version_ids!: Json<Vec<ViewVersionId>>",
       vvr.version_schema_ids,
       vvr.version_timestamps,
       vvr.version_default_namespace_ids AS "version_default_namespace_ids!: Vec<Option<Uuid>>",
       vvr.version_default_catalogs      AS "version_default_catalogs!: Vec<Option<String>>",
       vvr.summaries                     AS "version_metadata_summaries: Vec<Json<HashMap<String, String>>>",
       vvl.version_log_ids,
       vvl.version_log_timestamps,
       vvr.typ                           AS "view_representation_typ: Json<Vec<Vec<ViewRepresentationType>>>",
       vvr.sql                           AS "view_representation_sql: Json<Vec<Vec<String>>>",
       vvr.dialect                       AS "view_representation_dialect: Json<Vec<Vec<String>>>"
FROM view v
         INNER JOIN tabular ta ON v.view_id = ta.tabular_id
         INNER JOIN namespace n ON ta.namespace_id = n.namespace_id
         INNER JOIN warehouse w ON n.warehouse_id = w.warehouse_id
         INNER JOIN current_view_metadata_version cvv ON v.view_id = cvv.view_id
         LEFT JOIN (SELECT view_id,
                           ARRAY_AGG(schema_id) AS schema_ids,
                           ARRAY_AGG(schema)    AS schemas
                    FROM view_schema
                    GROUP BY view_id) vs ON v.view_id = vs.view_id
         LEFT JOIN (SELECT view_id,
                           ARRAY_AGG(version_id) AS version_log_ids,
                           ARRAY_AGG(timestamp)  AS version_log_timestamps
                    FROM view_version_log
                    GROUP BY view_id) vvl ON v.view_id = vvl.view_id
         LEFT JOIN (SELECT view_id,
                           ARRAY_AGG(key)   AS view_properties_keys,
                           ARRAY_AGG(value) AS view_properties_values
                    FROM view_properties
                    GROUP BY view_id) vp ON v.view_id = vp.view_id
         LEFT JOIN (SELECT vv.view_id,
                           JSONB_AGG(version_id)           AS version_ids,
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
                                        GROUP BY view_version_id, view_id) vr ON vv.version_id = vr.view_version_id AND vv.view_id = vr.view_id
                    GROUP BY vv.view_id) vvr ON v.view_id = vvr.view_id WHERE v.view_id = $1 AND (ta.deleted_at is NULL OR $2)"#,
            view_id,
            include_deleted
        )
        .fetch_one(&mut *conn)
        .await.map_err(|e| {
        let message = "Failed to fetch view metadata".to_string();
        tracing::warn!("{}", message);
        e.into_error_model(message)
    })?;
    Ok(rs)
}

async fn prepare_versions(
    conn: &mut PgConnection,
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
) -> Result<HashMap<ViewVersionId, Arc<ViewVersion>>, IcebergErrorResponse> {
    let version_ids = version_ids.0;
    let version_schema_ids =
        unwrap_or_500(version_schema_ids, "Failed to read version_schema_ids")?;
    let version_timestamps =
        unwrap_or_500(version_timestamps, "Failed to read version_timestamps")?;
    let version_metadata_summary = unwrap_or_500(
        version_metadata_summaries,
        "Failed to read version_metadata_summaries",
    )?;
    let version_representation_typ = unwrap_or_500(
        view_representation_typ,
        "failed to read view representation type",
    )?
    .0;
    let version_representation_sql = unwrap_or_500(
        view_representation_sql,
        "failed to read view representation sql",
    )?
    .0;
    let version_representation_dialect = unwrap_or_500(
        view_representation_dialect,
        "failed to read view representation dialect type",
    )?
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
        let namespace_name =
            get_namespace_ident_with_empty_support(conn, version_default_ns).await?;
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
            .with_default_namespace(namespace_name)
            .with_default_catalog(version_default_cat)
            .with_schema_id(schema_id)
            .with_summary(version_meta_summary.0)
            .with_representations(
                ViewRepresentations::builder()
                    .add_all_representations(reps)
                    .build()
                    .map_err(|e| {
                        ErrorModel::internal(
                            "Failed to build view representations",
                            "InternalDatabaseError",
                            Some(Box::new(e)),
                        )
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
) -> Result<Vec<ViewVersionLog>, IcebergErrorResponse> {
    let version_log_ids = unwrap_or_500(version_log_ids, "Failed to read version_log_ids")?;
    let version_log_timestamps = unwrap_or_500(
        version_log_timestamps,
        "Failed to read version_log_timestamps",
    )?;
    let version_log = version_log_ids
        .into_iter()
        .zip(version_log_timestamps)
        .map(|(id, ts)| ViewVersionLog::new(id, ts.timestamp_millis()))
        .collect();
    Ok(version_log)
}

fn prepare_props(
    view_properties_keys: Option<Vec<String>>,
    view_properties_values: Option<Vec<String>>,
) -> Result<HashMap<String, String>, IcebergErrorResponse> {
    let view_properties_keys =
        unwrap_or_500(view_properties_keys, "Failed to read view_properties_keys")?;
    let view_properties_values = unwrap_or_500(
        view_properties_values,
        "Failed to read view_properties_values",
    )?;
    let properties = view_properties_keys
        .into_iter()
        .zip(view_properties_values)
        .collect();
    Ok(properties)
}

fn prepare_schemas(
    schema_ids: Option<Vec<i32>>,
    schemas: Option<Vec<Json<Schema>>>,
) -> Result<HashMap<i32, Arc<Schema>>, IcebergErrorResponse> {
    let schema_ids = unwrap_or_500(schema_ids, "Failed to read schema_ids")?;
    let schemas = unwrap_or_500(schemas, "Failed to read schemas")?;
    let schemas = schema_ids
        .into_iter()
        .zip(schemas)
        .map(|(id, schema)| Ok((id, Arc::new(schema.0))))
        .collect::<Result<HashMap<_, _>>>()?;
    Ok(schemas)
}

fn unwrap_or_500<T>(val: Option<T>, message: &str) -> crate::api::Result<T> {
    Ok(val.ok_or_else(|| {
        ErrorModel::builder()
            .code(500)
            .message(message.to_string())
            .r#type("DatabaseError".to_string())
            .build()
    })?)
}

// this is a horrible function, and it's only here because NamespaceIdent doesn't allow constructing
// from empty vecs which is something that spark is handing to us.
async fn get_namespace_ident_with_empty_support(
    conn: &mut PgConnection,
    dni: Option<Uuid>,
) -> crate::api::Result<NamespaceIdent> {
    let namespace_name: NamespaceIdent = serde_json::from_value(if let Some(dni) = dni {
        serde_json::Value::Array(
            sqlx::query_scalar!(
                r#"
                    SELECT namespace_name
                    FROM namespace
                    WHERE namespace_id = $1
                "#,
                &dni
            )
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| {
                let message = "Error fetching namespace_name".to_string();
                tracing::warn!("{}", message);
                e.into_error_model(message)
            })?
            .into_iter()
            .map(serde_json::Value::String)
            .collect(),
        )
    } else {
        // TODO: NamespaceIdent doesn't allow empty vecs, otoh, spark is happily handing those to us
        serde_json::Value::Array(vec![])
    })
    .map_err(|e| {
        let message =
            "Error fetching namespace_name, failed to deserialize namespace ident.".to_string();
        tracing::warn!("{}", message);
        ErrorModel::builder()
            .code(500)
            .message(message)
            .r#type("DatabaseError".to_string())
            .source(Some(Box::new(e)))
            .build()
    })?;

    Ok(namespace_name)
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
    version_ids: Json<Vec<ViewVersionId>>,
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
}

struct VersionsPrep {
    version_ids: Json<Vec<ViewVersionId>>,
    version_schema_ids: Option<Vec<i32>>,
    version_timestamps: Option<Vec<DateTime<Utc>>>,
    version_default_namespace_ids: Vec<Option<Uuid>>,
    version_default_catalogs: Vec<Option<String>>,
    version_metadata_summaries: Option<Vec<Json<HashMap<String, String>>>>,
    view_representation_typ: Option<Json<Vec<Vec<ViewRepresentationType>>>>,
    view_representation_sql: Option<Json<Vec<Vec<String>>>>,
    view_representation_dialect: Option<Json<Vec<Vec<String>>>>,
}
