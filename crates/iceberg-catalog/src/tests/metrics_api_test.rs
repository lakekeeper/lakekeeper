use iceberg::TableIdent;
use iceberg_ext::{spec::Metrics as ExtMetrics, NamespaceIdent};
use serde_json::json;
use sqlx::Row;
use uuid::Uuid;

use crate::{
    api::{
        iceberg::{types::Prefix, v1::TableParameters},
        management::v1::warehouse::TabularDeleteProfile,
        ApiContext,
    },
    catalog::CatalogServer,
    implementations::postgres::{CatalogState, PostgresCatalog, SecretsState},
    request_metadata::RequestMetadata,
    service::{authz::AllowAllAuthorizer, Catalog, State, UserId},
    tests::{create_ns, create_table, get_api_context, random_request_metadata, setup},
};

#[sqlx::test]
async fn test_report_table_metrics_success_and_fetch(pool: sqlx::PgPool) {
    let auth = AllowAllAuthorizer;
    let (api_context, wh_response) = setup(
        pool.clone(),
        crate::tests::test_io_profile(),
        None,
        auth,
        TabularDeleteProfile::Hard {},
        Some(UserId::new_unchecked("test_user", "test_user")),
        None,
        1,
    )
    .await;

    let ns_ident = NamespaceIdent::new("my_namespace".to_string());
    create_ns(
        api_context.clone(),
        wh_response.warehouse_id.to_string(),
        ns_ident.name().to_string(),
    )
    .await;

    let table_ident = TableIdent::new(ns_ident.clone(), "my_table".to_string());
    let create_table_response = create_table(
        api_context.clone(),
        wh_response.warehouse_id.to_string(),
        ns_ident.name().to_string(),
        table_ident.name().to_string(),
        false, // Not staged
    )
    .await
    .expect("Failed to create table");

    let table_id = create_table_response.table_metadata.table_uuid;

    let table_params = TableParameters {
        prefix: Some(Prefix(wh_response.warehouse_id.to_string())),
        table: table_ident.clone(),
    };

    let metrics_data = ExtMetrics {
        total_records: 12345,
        total_files_size_bytes: 67890,
        total_data_files: 10,
        total_delete_files: 1,
        total_position_deletes: 5,
        total_equality_deletes: 0,
    };
    let metrics_json_value =
        serde_json::to_value(&metrics_data).expect("Failed to serialize metrics to JSON");

    let report_result = CatalogServer::report_metrics(
        table_params,
        metrics_json_value,
        api_context.clone(),
        random_request_metadata(),
    )
    .await;

    assert!(report_result.is_ok(), "Report metrics failed: {:?}", report_result.err());

    // Verify data in table_metrics
    let row = sqlx::query(
        r#"
        SELECT table_id, total_records, total_files_size_bytes, total_data_files,
               total_delete_files, total_position_deletes, total_equality_deletes
        FROM table_metrics
        WHERE table_id = $1
        ORDER BY reported_at DESC
        LIMIT 1
        "#,
    )
    .bind(table_id)
    .fetch_one(&pool)
    .await
    .expect("Failed to fetch from table_metrics");

    let fetched_table_id: Uuid = row.get("table_id");
    let total_records: i64 = row.get("total_records");
    let total_files_size_bytes: i64 = row.get("total_files_size_bytes");
    let total_data_files: i64 = row.get("total_data_files");
    let total_delete_files: i64 = row.get("total_delete_files");
    let total_position_deletes: i64 = row.get("total_position_deletes");
    let total_equality_deletes: i64 = row.get("total_equality_deletes");

    assert_eq!(fetched_table_id, table_id);
    assert_eq!(total_records, metrics_data.total_records);
    assert_eq!(total_files_size_bytes, metrics_data.total_files_size_bytes);
    assert_eq!(total_data_files, metrics_data.total_data_files);
    assert_eq!(total_delete_files, metrics_data.total_delete_files);
    assert_eq!(total_position_deletes, metrics_data.total_position_deletes);
    assert_eq!(total_equality_deletes, metrics_data.total_equality_deletes);
}

#[sqlx::test]
async fn test_report_table_metrics_table_not_found(pool: sqlx::PgPool) {
    let auth = AllowAllAuthorizer;
    let (api_context, wh_response) = setup(
        pool.clone(),
        crate::tests::test_io_profile(),
        None,
        auth,
        TabularDeleteProfile::Hard {},
        Some(UserId::new_unchecked("test_user", "test_user")),
        None,
        1,
    )
    .await;

    let non_existent_table_ident = TableIdent::new(
        NamespaceIdent::new("my_namespace".to_string()),
        "non_existent_table".to_string(),
    );

    let table_params = TableParameters {
        prefix: Some(Prefix(wh_response.warehouse_id.to_string())),
        table: non_existent_table_ident,
    };

    let metrics_data = ExtMetrics {
        total_records: 100,
        total_files_size_bytes: 200,
        total_data_files: 3,
        total_delete_files: 0,
        total_position_deletes: 0,
        total_equality_deletes: 0,
    };
    let metrics_json_value =
        serde_json::to_value(&metrics_data).expect("Failed to serialize metrics to JSON");

    let report_result = CatalogServer::report_metrics(
        table_params,
        metrics_json_value,
        api_context.clone(),
        random_request_metadata(),
    )
    .await;

    assert!(report_result.is_err());
    let error = report_result.err().unwrap();
    // Error response from report_metrics is ErrorModel
    // We need to check the type and message if possible.
    // For now, checking if it's an error is sufficient for this test structure.
    // A more precise check would involve inspecting error.error.error_type or similar.
    // Based on the implementation, it should be a "TableNotFound" error.
    // Example: assert_eq!(error.error.error_type, "TableNotFound");
    // However, direct comparison of ErrorModel is complex due to its structure.
     assert_eq!(error.error.code, 404); // Not Found
     assert!(error.error.message.contains("Table"));
     assert!(error.error.message.contains("not found"));
}

#[sqlx::test]
async fn test_report_table_metrics_malformed_json(pool: sqlx::PgPool) {
    let auth = AllowAllAuthorizer;
    let (api_context, wh_response) = setup(
        pool.clone(),
        crate::tests::test_io_profile(),
        None,
        auth,
        TabularDeleteProfile::Hard {},
        Some(UserId::new_unchecked("test_user", "test_user")),
        None,
        1,
    )
    .await;
    
    let ns_ident = NamespaceIdent::new("my_namespace_json".to_string());
    create_ns(
        api_context.clone(),
        wh_response.warehouse_id.to_string(),
        ns_ident.name().to_string(),
    )
    .await;

    let table_ident = TableIdent::new(ns_ident.clone(), "my_table_json".to_string());
    let _ = create_table(
        api_context.clone(),
        wh_response.warehouse_id.to_string(),
        ns_ident.name().to_string(),
        table_ident.name().to_string(),
        false, // Not staged
    )
    .await
    .expect("Failed to create table");

    let table_params = TableParameters {
        prefix: Some(Prefix(wh_response.warehouse_id.to_string())),
        table: table_ident.clone(),
    };

    // Malformed JSON: total_records is a string instead of a number
    let malformed_json_value = json!({
        "total_records": "not_a_number",
        "total_files_size_bytes": 67890,
        "total_data_files": 10,
        "total_delete_files": 1,
        "total_position_deletes": 5,
        "total_equality_deletes": 0,
    });

    let report_result = CatalogServer::report_metrics(
        table_params,
        malformed_json_value,
        api_context.clone(),
        random_request_metadata(),
    )
    .await;

    assert!(report_result.is_err());
    let error = report_result.err().unwrap();
    // Check for bad request and specific error type related to parsing
    assert_eq!(error.error.code, 400); // Bad Request
    assert!(error.error.message.contains("Failed to parse metrics JSON"));
    assert_eq!(error.error.r#type, "MetricsParsingFailed");
}
