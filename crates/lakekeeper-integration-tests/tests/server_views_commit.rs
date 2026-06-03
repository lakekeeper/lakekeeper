use chrono::Utc;
use http::StatusCode;
use iceberg::TableIdent;
use iceberg_ext::catalog::rest::CommitViewRequest;
use lakekeeper::{
    WarehouseId,
    api::{
        RequestMetadata,
        iceberg::{
            types::{DropParams, Prefix},
            v1::{DataAccess, ViewParameters},
        },
        management::v1::{ApiServer as ManagementApiServer, view::ViewManagementService},
    },
    server::views::{commit::commit_view, drop::drop_view},
};
use lakekeeper_integration_tests::{
    create_view_helper, create_view_request, random_request_metadata, views_test_setup,
};
use maplit::hashmap;
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

#[sqlx::test]
async fn test_commit_view(pool: PgPool) {
    let (api_context, namespace, whi, _) = views_test_setup(pool, None).await;
    let prefix = whi.to_string();
    let view_name = "myview";
    let view = create_view_helper(
        api_context.clone(),
        namespace.clone(),
        create_view_request(Some(view_name), None),
        Some(prefix.clone()),
    )
    .await
    .unwrap();

    let rq: CommitViewRequest = spark_commit_update_request(whi, Some(view.metadata.uuid()));

    let res = Box::pin(commit_view(
        ViewParameters {
            prefix: Some(Prefix(prefix.clone())),
            view: TableIdent::from_strs(namespace.inner().into_iter().chain([view_name.into()]))
                .unwrap(),
        },
        rq,
        api_context,
        DataAccess {
            vended_credentials: true,
            remote_signing: false,
        },
        RequestMetadata::new_unauthenticated(),
    ))
    .await
    .unwrap();

    assert_eq!(res.metadata.current_version_id(), 2);
    assert_eq!(res.metadata.schemas_iter().len(), 3);
    assert_eq!(res.metadata.versions().len(), 2);
    let max_schema = res.metadata.schemas_iter().map(|s| s.schema_id()).max();
    assert_eq!(
        res.metadata.current_version().schema_id(),
        max_schema.unwrap()
    );

    assert_eq!(
        res.metadata.properties(),
        &hashmap! {
            "create_engine_version".to_string() => "Spark 3.5.1".to_string(),
            "spark.query-column-names".to_string() => "id".to_string(),
        }
    );
}

#[sqlx::test]
async fn test_commit_view_preserves_protection(pool: PgPool) {
    let (api_context, namespace, whi, _) = views_test_setup(pool, None).await;
    let prefix = whi.to_string();
    let view_name = "myview";
    let view_ident = TableIdent::from_strs(
        namespace
            .clone()
            .inner()
            .into_iter()
            .chain([view_name.into()]),
    )
    .unwrap();
    let view = create_view_helper(
        api_context.clone(),
        namespace.clone(),
        create_view_request(Some(view_name), None),
        Some(prefix.clone()),
    )
    .await
    .unwrap();

    ManagementApiServer::set_view_protection(
        view.metadata.uuid().into(),
        whi,
        true,
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let rq: CommitViewRequest = spark_commit_update_request(whi, Some(view.metadata.uuid()));
    let res = Box::pin(commit_view(
        ViewParameters {
            prefix: Some(Prefix(prefix.clone())),
            view: view_ident.clone(),
        },
        rq,
        api_context.clone(),
        DataAccess {
            vended_credentials: true,
            remote_signing: false,
        },
        RequestMetadata::new_unauthenticated(),
    ))
    .await
    .unwrap();

    let protection = ManagementApiServer::get_view_protection(
        res.metadata.uuid().into(),
        whi,
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert!(protection.protected);

    let err = drop_view(
        ViewParameters {
            prefix: Some(Prefix(prefix.clone())),
            view: view_ident,
        },
        DropParams {
            purge_requested: true,
            force: false,
        },
        api_context,
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .expect_err("protected view should remain protected after commit");
    assert_eq!(err.error.code, StatusCode::CONFLICT);
}

#[sqlx::test]
async fn test_commit_view_fails_with_wrong_assertion(pool: PgPool) {
    let (api_context, namespace, whi, _) = views_test_setup(pool, None).await;
    let prefix = whi.to_string();
    let view_name = "myview";
    let _ = create_view_helper(
        api_context.clone(),
        namespace.clone(),
        create_view_request(Some(view_name), None),
        Some(prefix.clone()),
    )
    .await
    .unwrap();

    let rq: CommitViewRequest = spark_commit_update_request(whi, Some(Uuid::now_v7()));

    let err = Box::pin(commit_view(
        ViewParameters {
            prefix: Some(Prefix(prefix.clone())),
            view: TableIdent::from_strs(namespace.inner().into_iter().chain([view_name.into()]))
                .unwrap(),
        },
        rq,
        api_context,
        DataAccess {
            vended_credentials: true,
            remote_signing: false,
        },
        RequestMetadata::new_unauthenticated(),
    ))
    .await
    .expect_err("This unexpectedly didn't fail the uuid assertion.");
    assert_eq!(err.error.code, 400);
    assert_eq!(err.error.r#type, "ViewUuidMismatch");
}

fn spark_commit_update_request(
    warehouse_id: WarehouseId,
    asserted_uuid: Option<Uuid>,
) -> CommitViewRequest {
    let uuid = asserted_uuid.map_or("019059cb-9277-7ff0-b71a-537df05b33f8".into(), |u| {
        u.to_string()
    });
    serde_json::from_value(json!({
  "requirements": [
    {
      "type": "assert-view-uuid",
      "warehouse-uuid": *warehouse_id,
      "uuid": &uuid
    }
  ],
  "updates": [
    {
      "action": "set-properties",
      "updates": {
        "create_engine_version": "Spark 3.5.1",
        "spark.query-column-names": "id",
        "engine_version": "Spark 3.5.1"
      }
    },
    {
      "action": "add-schema",
      "schema": {
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
      "last-column-id": 1
    },
    {
      "action": "add-schema",
      "schema": {
        "schema-id": 2,
        "type": "struct",
        "fields": [
          {
            "id": 0,
            "name": "idx",
            "required": false,
            "type": "long",
            "doc": "idx of thing"
          }
        ]
      },
      "last-column-id": 1
    },
    {
      "action": "add-view-version",
      "view-version": {
        "version-id": 2,
        "schema-id": -1,
        "timestamp-ms": Utc::now().timestamp_millis(),
        "summary": {
          "engine-name": "spark",
          "engine-version": "3.5.1",
          "iceberg-version": "Apache Iceberg 1.5.2 (commit cbb853073e681b4075d7c8707610dceecbee3a82)",
          "app-id": "local-1719494665567"
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
    },
    {
        "action": "remove-properties",
        "removals": ["engine_version"]
    },
    {
      "action": "set-current-view-version",
      "view-version-id": -1
    }
  ]
})).unwrap()
}
