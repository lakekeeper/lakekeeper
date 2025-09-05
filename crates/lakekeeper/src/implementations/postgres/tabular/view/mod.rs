mod load;

use std::{collections::HashMap, default::Default};

use chrono::{DateTime, Utc};
use http::StatusCode;
use iceberg::{
    spec::{SchemaRef, ViewMetadata, ViewRepresentation, ViewVersionId, ViewVersionRef},
    NamespaceIdent,
};
use lakekeeper_io::Location;
pub(crate) use load::load_view;
use serde::Deserialize;
use sqlx::{FromRow, Postgres, Transaction};
use uuid::Uuid;

pub(crate) use crate::service::ViewMetadataWithLocation;
use crate::{
    api::iceberg::v1::{PaginatedMapping, PaginationQuery},
    implementations::postgres::{
        dbutils::DBErrorHandler as _,
        tabular::{
            self, create_tabular, drop_tabular, list_tabulars, CreateTabular, TabularId,
            TabularIdentBorrowed, TabularType,
        },
    },
    service::{
        ErrorModel, ListFlags, NamespaceId, Result, TableIdent, TableInfo, TabularInfo, ViewId,
    },
    WarehouseId,
};

pub(crate) async fn view_ident_to_id<'e, 'c: 'e, E>(
    warehouse_id: WarehouseId,
    table: &TableIdent,
    include_deleted: bool,
    catalog_state: E,
) -> Result<Option<ViewId>>
where
    E: 'e + sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    tabular::tabular_ident_to_id(
        warehouse_id,
        &TabularIdentBorrowed::View(table),
        ListFlags {
            include_deleted,
            include_staged: false,
            include_active: true,
        },
        catalog_state,
    )
    .await?
    .map(|(id, _)| match id {
        TabularId::Table(_) => Err(ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message("DB returned a table when filtering for views.".to_string())
            .r#type("InternalDatabaseError".to_string())
            .build()
            .into()),
        TabularId::View(view) => Ok(view.into()),
    })
    .transpose()
}

pub(crate) async fn create_view(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    metadata_location: &Location,
    transaction: &mut Transaction<'_, Postgres>,
    name: &str,
    metadata: ViewMetadata,
    location: &Location,
) -> Result<()> {
    if location.as_str() != metadata.location() {
        tracing::error!(
            "Location in ViewMetadata ('{}') does not match location ('{}') passed into create_view function, this is a bug.",
            metadata.location(),
            location.as_str()
        );
        return Err(ErrorModel::internal(
            "Location in ViewMetadata does not match location passed into create_view function.",
            "InternalServerError",
            None,
        )
        .append_details(vec![location.to_string(), metadata.location().to_string()])
        .into());
    }
    let tabular_id = create_tabular(
        CreateTabular {
            id: metadata.uuid(),
            name,
            namespace_id: *namespace_id,
            warehouse_id: *warehouse_id,
            typ: TabularType::View,
            metadata_location: Some(metadata_location),
            location,
        },
        &mut *transaction,
    )
    .await?;

    let view_id = sqlx::query_scalar!(
        r#"
        INSERT INTO view (warehouse_id, view_id, view_format_version)
        VALUES ($1, $2, $3)
        returning view_id
        "#,
        *warehouse_id,
        tabular_id,
        ViewFormatVersion::from(metadata.format_version()) as _,
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => {
            ErrorModel::internal("Error creating view", "InternalDatabaseError", None)
        }
        _ => e.into_error_model("Error creating view".to_string()),
    })?;

    tracing::debug!("Inserted base view and tabular.");
    for schema in metadata.schemas_iter() {
        let schema_id =
            create_view_schema(warehouse_id, view_id, schema.clone(), transaction).await?;
        tracing::debug!("Inserted schema with id: '{}'", schema_id);
    }

    for view_version in metadata.versions() {
        let ViewVersionResponse {
            version_id,
            view_id,
            warehouse_id,
        } = create_view_version(
            warehouse_id,
            namespace_id,
            view_id,
            view_version.clone(),
            transaction,
        )
        .await?;

        tracing::debug!(
            "Inserted view version with id: '{}' for view_id: '{}' in warehouse with id '{}'",
            version_id,
            view_id,
            warehouse_id,
        );
    }

    set_current_view_metadata_version(
        warehouse_id,
        metadata.uuid(),
        metadata.current_version_id(),
        transaction,
    )
    .await?;

    for history in metadata.history() {
        insert_view_version_log(
            warehouse_id,
            view_id,
            history.version_id(),
            Some(history.timestamp().map_err(|e| {
                ErrorModel::internal(
                    "Error converting timestamp_ms into datetime.",
                    "ViewVersionTimestampError",
                    Some(Box::new(e)),
                )
            })?),
            transaction,
        )
        .await?;
    }

    set_view_properties(warehouse_id, view_id, metadata.properties(), transaction).await?;

    tracing::debug!("Inserted view properties for view",);

    Ok(())
}

pub(crate) async fn drop_view(
    warehouse_id: WarehouseId,
    view_id: ViewId,
    force: bool,
    required_metadata_location: Option<&Location>,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<String> {
    drop_tabular(
        warehouse_id,
        TabularId::View(*view_id),
        force,
        required_metadata_location,
        transaction,
    )
    .await
}

/// Rename a table. Tables may be moved across namespaces.
pub(crate) async fn rename_view(
    warehouse_id: WarehouseId,
    source_id: ViewId,
    source: &TableIdent,
    destination: &TableIdent,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    tabular::rename_tabular(
        warehouse_id,
        TabularId::View(*source_id),
        source,
        destination,
        transaction,
    )
    .await?;

    Ok(())
}

// TODO: do we wanna do this via a trigger?
async fn insert_view_version_log(
    warehouse_id: WarehouseId,
    view_id: Uuid,
    version_id: ViewVersionId,
    timestamp_ms: Option<DateTime<Utc>>,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<()> {
    if let Some(ts) = timestamp_ms {
        sqlx::query!(
            r#"
        INSERT INTO view_version_log (warehouse_id, view_id, version_id, timestamp)
        VALUES ($1, $2, $3, $4)
        "#,
            *warehouse_id,
            view_id,
            version_id,
            ts
        )
    } else {
        sqlx::query!(
            r#"
        INSERT INTO view_version_log (warehouse_id, view_id, version_id)
        VALUES ($1, $2, $3)
        "#,
            *warehouse_id,
            view_id,
            version_id,
        )
    }
    .execute(&mut **transaction)
    .await
    .map_err(|e| {
        let message = "Error inserting view version log".to_string();
        tracing::warn!("{}", message);
        e.into_error_model(message)
    })?;
    tracing::debug!("Inserted view version log");
    Ok(())
}

pub(crate) async fn set_view_properties(
    warehouse_id: WarehouseId,
    view_id: Uuid,
    properties: &HashMap<String, String>,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<()> {
    let (keys, vals): (Vec<String>, Vec<String>) = properties
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
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
        let message = "Error inserting view property".to_string();
        tracing::warn!("{}", message);
        e.into_error_model(message)
    })?;
    Ok(())
}

pub(crate) async fn create_view_schema(
    warehouse_id: WarehouseId,
    view_id: Uuid,
    schema: SchemaRef,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<i32> {
    let schema_as_value = serde_json::to_value(&schema).map_err(|e| {
        ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message("Error serializing view schema".to_string())
            .r#type("ViewSchemaSerializationError".to_string())
            .source(Some(Box::new(e)))
            .build()
    })?;
    Ok(sqlx::query_scalar!(
        r#"
        INSERT INTO view_schema (warehouse_id, view_id, schema_id, schema)
        VALUES ($1, $2, $3, $4)
        RETURNING schema_id
        "#,
        *warehouse_id,
        view_id,
        schema.schema_id(),
        schema_as_value
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => ErrorModel::builder()
            .code(StatusCode::CONFLICT.into())
            .message("View schema already exists".to_string())
            .r#type("ViewSchemaAlreadyExists".to_string())
            .build(),
        _ => e.into_error_model("Error creating view schema".to_string()),
    })?)
}

#[derive(Debug, FromRow, Clone, Copy)]
#[allow(clippy::struct_field_names)]
struct ViewVersionResponse {
    version_id: ViewVersionId,
    view_id: Uuid,
    warehouse_id: Uuid,
}

/// Creates a `view_version` in the namespace specified by `namespace_id`.
///
/// Note that `namespace_id` is not the view's default namespace. Instead the default namespace is
/// specified separately via `view_version_request`.
#[allow(clippy::too_many_lines)]
async fn create_view_version(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    view_id: Uuid,
    view_version_request: ViewVersionRef,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<ViewVersionResponse> {
    let view_version = view_version_request;
    let version_id = view_version.version_id();
    let schema_id = view_version.schema_id();

    // According to the [iceberg spec] `view_version.default_namespace` is a required field. However
    // some query engines (e.g. Spark) may send an empty string for `default_namespace`. We
    // represent this by NULL in the `default_namespace_id` column.
    //
    // While the [iceberg spec] specifies `default_namespace` as namespace identifier, we store
    // the corresponding namespace's id as surrogate key for performance reasons.
    //
    // [iceberg spec]: https://iceberg.apache.org/view-spec/#view-metadata
    let default_ns = view_version.default_namespace();
    let default_ns = default_ns.clone().inner();
    let default_namespace_id: Option<Uuid> = sqlx::query_scalar!(
        r#"
        SELECT namespace_id
        FROM namespace n
        WHERE namespace_name = $1
        AND warehouse_id in (SELECT warehouse_id FROM namespace WHERE namespace_id = $2)
        "#,
        &default_ns,
        *namespace_id
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| {
        let message = "Error fetching namespace_id".to_string();
        tracing::warn!("{}", message);
        e.into_error_model(message)
    })?;

    let default_cat = view_version.default_catalog();
    let summary = serde_json::to_value(view_version.summary()).map_err(|e| {
        ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message("Error serializing view_version summary".to_string())
            .r#type("ViewSummarySerializationFailed".to_string())
            .source(Some(Box::new(e)))
            .build()
    })?;

    let insert_response = sqlx::query_as!(ViewVersionResponse,
                r#"
                    INSERT INTO view_version (warehouse_id, view_id, version_id, schema_id, timestamp, default_namespace_id, default_catalog, summary)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    returning warehouse_id, view_id, version_id
                "#,
                *warehouse_id,
                view_id,
                version_id,
                schema_id,
                view_version.timestamp().map_err(|e|
                    ErrorModel::builder()
                        .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                        .message("Error converting timestamp_ms into datetime.".to_string())
                        .r#type("ViewVersionTimestampError".to_string())
                        .source(Some(Box::new(e)))
                        .build()
                )?,
                default_namespace_id,
                default_cat,
                summary
            )
        .fetch_one(&mut **transaction)
        .await.map_err(|e| {
        if let sqlx::Error::RowNotFound = e {
            let message = "View version already exists";
            tracing::debug!(?e,"{}", message);
            ErrorModel::builder()
                .code(StatusCode::CONFLICT.into())
                .message(message.to_string())
                .r#type("ViewVersionAlreadyExists".to_string())
                .build()
        } else {
            let message = "Error creating view version";
            tracing::warn!(?e, "{} for: '{}'/'{}' with schema_id: '{}' due to: '{}'",
                message,
                view_id,
                version_id,
                schema_id,
                e
            );
            e.into_error_model(message.to_string())
        }
    })?;

    for rep in view_version.representations().iter() {
        insert_representation(rep, transaction, insert_response).await?;
    }

    tracing::debug!(
        "Inserted version: '{}' view metadata version for '{}'",
        version_id,
        view_id
    );

    Ok(insert_response)
}

pub(crate) async fn set_current_view_metadata_version(
    warehouse_id: WarehouseId,
    view_id: Uuid,
    version_id: ViewVersionId,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<()> {
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
        let message = "Error setting current view metadata version".to_string();
        tracing::warn!("{}", message);
        e.into_error_model(message)
    })?;

    tracing::debug!("Successfully set current view metadata version");
    Ok(())
}

pub(crate) async fn list_views<'e, 'c: 'e, E>(
    warehouse_id: WarehouseId,
    namespace: &NamespaceIdent,
    include_deleted: bool,
    transaction: E,
    paginate_query: PaginationQuery,
) -> Result<PaginatedMapping<ViewId, TableInfo>>
where
    E: 'e + sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let page = list_tabulars(
        warehouse_id,
        Some(namespace),
        None,
        ListFlags {
            include_deleted,
            include_staged: false,
            include_active: true,
        },
        transaction,
        Some(TabularType::View),
        paginate_query,
    )
    .await?;
    let views = page.map::<ViewId, TableInfo>(
        |k| match k {
            TabularId::Table(_) => Err(ErrorModel::internal(
                "DB returned a table when filtering for views.",
                "InternalDatabaseError",
                None,
            )
            .into()),
            TabularId::View(t) => Ok(t.into()),
        },
        TabularInfo::into_view_info,
    )?;
    Ok(views)
}

async fn insert_representation(
    rep: &ViewRepresentation,
    transaction: &mut Transaction<'_, Postgres>,
    view_version_response: ViewVersionResponse,
) -> Result<()> {
    let ViewRepresentation::Sql(repr) = rep;
    sqlx::query!(
        r#"
            INSERT INTO view_representation (warehouse_id, view_id, view_version_id, typ, sql, dialect)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        view_version_response.warehouse_id,
        view_version_response.view_id,
        view_version_response.version_id,
        ViewRepresentationType::from(rep) as _,
        repr.sql.as_str(),
        repr.dialect.as_str()
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| {
        let message = "Error inserting view_representation".to_string();
        tracing::warn!(?e, "{}", message);
        e.into_error_model(message)
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

impl From<&iceberg::spec::ViewRepresentation> for ViewRepresentationType {
    fn from(value: &ViewRepresentation) -> Self {
        match value {
            ViewRepresentation::Sql(_) => Self::Sql,
        }
    }
}

impl From<iceberg::spec::ViewFormatVersion> for ViewFormatVersion {
    fn from(value: iceberg::spec::ViewFormatVersion) -> Self {
        match value {
            iceberg::spec::ViewFormatVersion::V1 => Self::V1,
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use iceberg::{
        spec::{ViewMetadata, ViewMetadataBuilder},
        NamespaceIdent, TableIdent,
    };
    use iceberg_ext::configs::ParseFromStr;
    use lakekeeper_io::Location;
    use serde_json::json;
    use sqlx::PgPool;
    use uuid::Uuid;

    use crate::{
        api::{iceberg::v1::PaginationQuery, management::v1::DeleteKind},
        implementations::postgres::{
            namespace::tests::initialize_namespace,
            tabular::{mark_tabular_as_deleted, view::load_view},
            warehouse::test::initialize_warehouse,
            CatalogState, PostgresCatalog,
        },
        service::{
            task_queue::{
                tabular_expiration_queue::{TabularExpirationPayload, TabularExpirationTask},
                EntityId, TaskMetadata,
            },
            TabularId, ViewId,
        },
        WarehouseId,
    };

    pub(crate) fn view_request(view_id: Option<Uuid>, location: &Location) -> ViewMetadata {
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
        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;
        let namespace_id =
            crate::implementations::postgres::tabular::table::tests::get_namespace_id(
                state.clone(),
                warehouse_id,
                &namespace,
            )
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
            request.clone(),
            &location,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();

        let mut tx = pool.begin().await.unwrap();
        // recreate with same uuid should fail
        let created_view = super::create_view(
            warehouse_id,
            namespace_id,
            &format!(
                "s3://my_bucket/my_table/metadata/barz/metadata-{}.gz.json",
                Uuid::now_v7()
            )
            .parse()
            .unwrap(),
            &mut tx,
            "myview2",
            request.clone(),
            &"s3://my_bucket/my_table/metadata/barz".parse().unwrap(),
        )
        .await
        .expect_err("recreation should fail");
        // this is not a conflict error since uuids are not externally controlled
        assert_eq!(created_view.error.code, 500, "{}", created_view.error);
        tx.commit().await.unwrap();

        let mut tx = pool.begin().await.unwrap();

        // recreate with other uuid should fail
        let created_view = super::create_view(
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
            ViewMetadataBuilder::new_from_metadata(request.clone())
                .assign_uuid(Uuid::now_v7())
                .build()
                .unwrap()
                .metadata,
            &"s3://my_bucket/my_table/metadata/bar".parse().unwrap(),
        )
        .await
        .expect_err("recreation should fail");
        assert_eq!(created_view.error.code, 409, "{:?}", created_view.error);

        tx.commit().await.unwrap();

        let views = super::list_views(
            warehouse_id,
            &namespace,
            false,
            &state.read_pool(),
            PaginationQuery::empty(),
        )
        .await
        .unwrap();
        assert_eq!(views.len(), 1);
        let (list_view_uuid, view) = views.into_iter().next().unwrap();
        assert_eq!(list_view_uuid, view_uuid);
        assert_eq!(view.table_ident.name, "myview");

        let mut conn = state.read_pool().acquire().await.unwrap();
        let metadata = load_view(warehouse_id, view_uuid, false, &mut conn)
            .await
            .unwrap();
        assert_eq!(metadata.metadata, request.clone());
    }

    #[sqlx::test]
    async fn drop_view_unconditionally(pool: sqlx::PgPool) {
        let (state, created_meta, warehouse_id, _, _, _) = prepare_view(pool).await;
        let mut tx = state.write_pool().begin().await.unwrap();
        super::drop_view(
            warehouse_id,
            created_meta.uuid().into(),
            false,
            None,
            &mut tx,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        load_view(
            warehouse_id,
            created_meta.uuid().into(),
            false,
            &mut state.write_pool().acquire().await.unwrap(),
        )
        .await
        .expect_err("dropped view should not be loadable");
    }

    #[sqlx::test]
    async fn drop_view_correct_location(pool: sqlx::PgPool) {
        let (state, created_meta, warehouse_id, _, _, metadata_location) = prepare_view(pool).await;
        let mut tx = state.write_pool().begin().await.unwrap();
        super::drop_view(
            warehouse_id,
            created_meta.uuid().into(),
            false,
            Some(&metadata_location),
            &mut tx,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        load_view(
            warehouse_id,
            created_meta.uuid().into(),
            false,
            &mut state.write_pool().acquire().await.unwrap(),
        )
        .await
        .expect_err("dropped view should not be loadable");
    }

    #[sqlx::test]
    async fn test_drop_view_metadata_mismatch(pool: sqlx::PgPool) {
        let (state, created_meta, warehouse_id, _, _, _) = prepare_view(pool).await;
        let mut tx = state.write_pool().begin().await.unwrap();
        super::drop_view(
            warehouse_id,
            created_meta.uuid().into(),
            false,
            Some(&Location::parse_value("s3://not-the/old-location").unwrap()),
            &mut tx,
        )
        .await
        .expect_err("dropping view with wrong metadata location should fail");
        tx.commit().await.unwrap();
    }

    #[sqlx::test]
    async fn soft_drop_view(pool: sqlx::PgPool) {
        let (state, created_meta, warehouse_id, _, _, _) = prepare_view(pool).await;
        let mut tx = state.write_pool().begin().await.unwrap();

        let _ = TabularExpirationTask::schedule_task::<PostgresCatalog>(
            TaskMetadata {
                entity_id: EntityId::Tabular(created_meta.uuid()),
                warehouse_id,
                parent_task_id: None,
                schedule_for: Some(chrono::Utc::now() + chrono::Duration::seconds(1)),
            },
            TabularExpirationPayload {
                tabular_type: crate::api::management::v1::TabularType::View,
                deletion_kind: DeleteKind::Purge,
            },
            &mut tx,
        )
        .await
        .unwrap();
        mark_tabular_as_deleted(
            warehouse_id,
            TabularId::View(created_meta.uuid()),
            false,
            None,
            &mut tx,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        load_view(
            warehouse_id,
            created_meta.uuid().into(),
            true,
            &mut state.write_pool().acquire().await.unwrap(),
        )
        .await
        .expect("soft-dropped view should loadable");
        let mut tx = state.write_pool().begin().await.unwrap();

        super::drop_view(
            warehouse_id,
            created_meta.uuid().into(),
            false,
            None,
            &mut tx,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();

        load_view(
            warehouse_id,
            created_meta.uuid().into(),
            true,
            &mut state.write_pool().acquire().await.unwrap(),
        )
        .await
        .expect_err("hard-delete view should not be loadable");
    }

    #[sqlx::test]
    async fn view_exists(pool: sqlx::PgPool) {
        let (state, created_meta, warehouse_id, namespace, name, _) = prepare_view(pool).await;
        let exists = super::view_ident_to_id(
            warehouse_id,
            &TableIdent {
                namespace: namespace.clone(),
                name,
            },
            false,
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert_eq!(
            exists,
            Some(created_meta.uuid().into()),
            "view should exist"
        );

        assert_eq!(
            super::view_ident_to_id(
                warehouse_id,
                &TableIdent {
                    namespace,
                    name: "non_existing".to_string(),
                },
                false,
                &state.read_pool(),
            )
            .await
            .unwrap(),
            None,
            "non existing view should not exist"
        );
    }

    #[sqlx::test]
    async fn drop_view_not_existing(pool: sqlx::PgPool) {
        let (state, _, warehouse_id, _, _, _) = prepare_view(pool).await;
        let mut tx = state.write_pool().begin().await.unwrap();
        let e = super::drop_view(warehouse_id, Uuid::now_v7().into(), false, None, &mut tx)
            .await
            .expect_err("dropping random uuid should not succeed");
        tx.commit().await.unwrap();
        assert_eq!(e.error.code, 404);
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
    ) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;
        let namespace_id =
            crate::implementations::postgres::tabular::table::tests::get_namespace_id(
                state.clone(),
                warehouse_id,
                &namespace,
            )
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
            request.clone(),
            &location,
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
        )
    }
}
