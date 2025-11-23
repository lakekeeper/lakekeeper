mod load;

use std::{collections::HashMap, default::Default, str::FromStr as _};

use chrono::{DateTime, Utc};
use iceberg::spec::{SchemaRef, ViewMetadata, ViewRepresentation, ViewVersionId, ViewVersionRef};
use lakekeeper_io::Location;
pub(crate) use load::load_view;
use serde::Deserialize;
use sqlx::{FromRow, Postgres, Transaction};
use uuid::Uuid;

use crate::{
    WarehouseId,
    implementations::postgres::{
        dbutils::DBErrorHandler as _,
        tabular::{CreateTabular, TabularType, create_tabular},
    },
    service::{
        CatalogBackendError, ConversionError, CreateViewError, CreateViewVersionError,
        InternalParseLocationError, NamespaceId, SerializationError, UnexpectedTabularInResponse,
        ViewInfo,
    },
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
            Some(
                history
                    .timestamp()
                    .map_err(|e| ConversionError::new("view_version_log.timestamp", e))?,
            ),
            transaction,
        )
        .await?;
    }

    set_view_properties(warehouse_id, view_id, metadata.properties(), transaction).await?;

    tracing::debug!("Inserted view properties for view",);

    Ok(view_info)
}

// TODO: do we wanna do this via a trigger?
async fn insert_view_version_log(
    warehouse_id: WarehouseId,
    view_id: Uuid,
    version_id: ViewVersionId,
    timestamp_ms: Option<DateTime<Utc>>,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), CatalogBackendError> {
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
        e.into_catalog_backend_error()
            .append_detail("Error inserting view version log.")
    })?;
    tracing::debug!("Inserted view version log");
    Ok(())
}

pub(crate) async fn set_view_properties(
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

pub(crate) async fn create_view_schema(
    warehouse_id: WarehouseId,
    view_id: Uuid,
    schema: SchemaRef,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<i32, CreateViewError> {
    let schema_as_value =
        serde_json::to_value(&schema).map_err(|e| SerializationError::new("schema", e))?;
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
    .map_err(super::super::dbutils::DBErrorHandler::into_catalog_backend_error)?)
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
) -> Result<ViewVersionResponse, CreateViewVersionError> {
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
    .map_err(super::super::dbutils::DBErrorHandler::into_catalog_backend_error)?;

    let default_catalog = view_version.default_catalog();
    let summary = serde_json::to_value(view_version.summary())
        .map_err(|e| SerializationError::new("view_version.summary", e))?;

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
                    ConversionError::new(
                        "view_version.timestamp",
                        e
                    )
                )?,
                default_namespace_id,
                default_catalog,
                summary
            )
        .fetch_one(&mut **transaction)
        .await.map_err(|e| {
            e.into_catalog_backend_error()
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

async fn insert_representation(
    rep: &ViewRepresentation,
    transaction: &mut Transaction<'_, Postgres>,
    view_version_response: ViewVersionResponse,
) -> Result<(), CreateViewVersionError> {
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
        e.into_catalog_backend_error().append_detail("Error inserting view representation.")
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
        NamespaceIdent, TableIdent,
        spec::{ViewMetadata, ViewMetadataBuilder},
    };
    use iceberg_ext::configs::ParseFromStr;
    use lakekeeper_io::Location;
    use serde_json::json;
    use sqlx::PgPool;
    use uuid::Uuid;

    use crate::{
        WarehouseId,
        api::{iceberg::v1::PaginationQuery, management::v1::DeleteKind},
        implementations::postgres::{
            CatalogState, PostgresBackend,
            namespace::tests::initialize_namespace,
            tabular::{TabularType, mark_tabular_as_deleted, view::load_view},
            warehouse::test::initialize_warehouse,
        },
        service::{
            CreateViewError, DropTabularError, LoadViewError, TabularId, TabularIdentBorrowed,
            TabularListFlags, ViewId,
            tasks::{
                EntityId, TaskMetadata,
                tabular_expiration_queue::{TabularExpirationPayload, TabularExpirationTask},
            },
        },
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
        let (state, created_meta, warehouse_id, _, _, _) = prepare_view(pool).await;
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
        let (state, created_meta, warehouse_id, _, _, metadata_location) = prepare_view(pool).await;
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
        let (state, created_meta, warehouse_id, _, _, _) = prepare_view(pool).await;
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
        let (state, created_meta, warehouse_id, _, _, _) = prepare_view(pool).await;
        let mut tx = state.write_pool().begin().await.unwrap();

        let _ = TabularExpirationTask::schedule_task::<PostgresBackend>(
            TaskMetadata {
                entity_id: EntityId::View(created_meta.uuid().into()),
                warehouse_id,
                parent_task_id: None,
                schedule_for: Some(chrono::Utc::now() + chrono::Duration::seconds(1)),
                entity_name: vec!["myview".to_string()],
            },
            TabularExpirationPayload {
                deletion_kind: DeleteKind::Purge,
            },
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
        let (state, _created_meta, warehouse_id, namespace, name, _) = prepare_view(pool).await;
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
        let (state, _, warehouse_id, _, _, _) = prepare_view(pool).await;
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
        )
    }
}
