use std::{future::Future, sync::LazyLock};

use iceberg::{
    NamespaceIdent, TableIdent,
    spec::{NestedField, PrimitiveType, Schema, UnboundPartitionSpec},
};
use iceberg_ext::catalog::rest::{
    CreateNamespaceRequest, CreateNamespaceResponse, CreateTableRequest, CreateViewRequest,
    LoadTableResult, LoadViewResult,
};
use serde_json::json;
use tokio::runtime::Runtime;

use crate::{
    api::{
        ApiContext,
        iceberg::{
            types::Prefix,
            v1::{
                DataAccess, DropParams, NamespaceParameters, TableParameters,
                namespace::{NamespaceDropFlags, NamespaceService as _},
                tables::TablesService,
                views::ViewService,
            },
        },
    },
    implementations::postgres::{PostgresBackend, SecretsState},
    server::CatalogServer,
    service::{CatalogStore, SecretStore, State, authz::Authorizer},
    tests::random_request_metadata,
};

static COMMON_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to start Tokio runtime")
});

#[track_caller]
pub(crate) fn test_block_on<F: Future>(f: F, common_runtime: bool) -> F::Output {
    {
        if common_runtime {
            return COMMON_RUNTIME.block_on(f);
        }
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to start Tokio runtime")
            .block_on(f)
    }
}

pub(crate) async fn create_ns<T: Authorizer>(
    api_context: ApiContext<State<T, PostgresBackend, SecretsState>>,
    prefix: String,
    ns_name: String,
) -> CreateNamespaceResponse {
    CatalogServer::create_namespace(
        Some(Prefix(prefix)),
        CreateNamespaceRequest {
            namespace: NamespaceIdent::new(ns_name),
            properties: None,
        },
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap()
}

pub(crate) async fn create_table<T: Authorizer>(
    api_context: ApiContext<State<T, PostgresBackend, SecretsState>>,
    prefix: impl Into<String>,
    ns_name: impl Into<String>,
    name: impl Into<String>,
    stage: bool,
) -> crate::api::Result<LoadTableResult> {
    CatalogServer::create_table(
        NamespaceParameters {
            prefix: Some(Prefix(prefix.into())),
            namespace: NamespaceIdent::new(ns_name.into()),
        },
        create_table_request(Some(name.into()), Some(stage)),
        DataAccess::not_specified(),
        api_context,
        random_request_metadata(),
    )
    .await
}

pub(crate) async fn drop_table<T: Authorizer>(
    api_context: ApiContext<State<T, PostgresBackend, SecretsState>>,
    prefix: &str,
    ns_name: &str,
    name: &str,
    purge_requested: Option<bool>,
    force: bool,
) -> crate::api::Result<()> {
    CatalogServer::drop_table(
        TableParameters {
            prefix: Some(Prefix(prefix.to_string())),
            table: TableIdent::new(NamespaceIdent::new(ns_name.to_string()), name.to_string()),
        },
        DropParams {
            purge_requested: purge_requested.unwrap_or_default(),
            force,
        },
        api_context,
        random_request_metadata(),
    )
    .await
}

pub(crate) async fn create_view<T: Authorizer>(
    api_context: ApiContext<State<T, PostgresBackend, SecretsState>>,
    prefix: &str,
    ns_name: &str,
    name: &str,
    location: Option<&str>,
) -> crate::api::Result<LoadViewResult> {
    CatalogServer::create_view(
        NamespaceParameters {
            prefix: Some(Prefix(prefix.to_string())),
            namespace: NamespaceIdent::new(ns_name.to_string()),
        },
        create_view_request(Some(name), location),
        api_context,
        DataAccess::not_specified(),
        random_request_metadata(),
    )
    .await
}

pub(crate) async fn drop_namespace<A: Authorizer, C: CatalogStore, S: SecretStore>(
    api_context: ApiContext<State<A, C, S>>,
    flags: NamespaceDropFlags,
    namespace_parameters: NamespaceParameters,
) -> crate::api::Result<()> {
    CatalogServer::drop_namespace(
        namespace_parameters,
        flags,
        api_context,
        random_request_metadata(),
    )
    .await
}

pub(crate) fn create_view_request(name: Option<&str>, location: Option<&str>) -> CreateViewRequest {
    serde_json::from_value(json!({
    "name": name.unwrap_or("myview"),
    "location": location,
    "schema": {
      "schema-id": 0,
      "type": "struct",
      "fields": [
        {
          "id": 0,
          "name": "id",
          "required": false,
          "type": "long"
        }
      ]
    },
    "view-version": {
      "version-id": 1,
      "schema-id": 0,
      "timestamp-ms": 1_719_395_654_343_i64,
      "summary": {
        "engine-version": "3.5.1",
        "iceberg-version": "Apache Iceberg 1.5.2 (commit cbb853073e681b4075d7c8707610dceecbee3a82)",
        "engine-name": "spark",
        "app-id": "local-1719395622847"
      },
      "representations": [
        {
          "type": "sql",
          "sql": "select id, xyz from spark_demo.my_table",
          "dialect": "spark"
        }
      ],
      "default-namespace": []
    },
    "properties": {
      "create_engine_version": "Spark 3.5.1",
      "engine_version": "Spark 3.5.1",
      "spark.query-column-names": "id"
    }}))
    .unwrap()
}

pub(crate) fn create_table_request(
    table_name: Option<String>,
    stage_create: Option<bool>,
) -> CreateTableRequest {
    CreateTableRequest {
        name: table_name.unwrap_or("my_table".to_string()),
        location: None,
        schema: Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", iceberg::spec::Type::Primitive(PrimitiveType::Int))
                    .into(),
                NestedField::required(
                    2,
                    "name",
                    iceberg::spec::Type::Primitive(PrimitiveType::String),
                )
                .into(),
            ])
            .build()
            .unwrap(),
        partition_spec: Some(UnboundPartitionSpec::builder().build()),
        write_order: None,
        stage_create,
        properties: None,
    }
}
