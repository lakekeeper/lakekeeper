#[cfg(feature = "axum")]
use axum::{
    http::header::{self, HeaderMap, HeaderValue},
    response::IntoResponse,
};
use iceberg::spec::TableMetadataRef;
use typed_builder::TypedBuilder;
use xxhash_rust::xxh3::xxh3_64;

#[cfg(feature = "axum")]
use super::impl_into_response;
use crate::{
    catalog::{TableIdent, TableRequirement, TableUpdate},
    spec::{Schema, SortOrder, UnboundPartitionSpec},
};

#[derive(Debug, PartialEq, Clone, serde::Serialize, serde::Deserialize)]
pub struct StorageCredential {
    pub prefix: String,
    pub config: std::collections::HashMap<String, String>,
}
#[derive(Debug, PartialEq, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct LoadCredentialsResponse {
    pub storage_credentials: Vec<StorageCredential>,
}

/// Result used when a table is successfully loaded.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct LoadTableResult {
    /// May be null if the table is staged as part of a transaction
    pub metadata_location: Option<String>,
    pub metadata: TableMetadataRef,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<std::collections::HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_credentials: Option<Vec<StorageCredential>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CreateTableRequest {
    pub name: String,
    pub location: Option<String>,
    pub schema: Schema,
    pub partition_spec: Option<UnboundPartitionSpec>,
    pub write_order: Option<SortOrder>,
    pub stage_create: Option<bool>,
    pub properties: Option<std::collections::HashMap<String, String>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, TypedBuilder)]
#[serde(rename_all = "kebab-case")]
pub struct RegisterTableRequest {
    pub name: String,
    pub metadata_location: String,
    #[serde(default)]
    #[builder(default)]
    pub overwrite: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct RenameTableRequest {
    pub source: TableIdent,
    pub destination: TableIdent,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ListTablesResponse {
    /// An opaque token that allows clients to make use of pagination for list
    /// APIs (e.g. `ListTables`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
    pub identifiers: Vec<TableIdent>,
    /// Lakekeeper IDs of the tables.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_uuids: Option<Vec<uuid::Uuid>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protection_status: Option<Vec<bool>>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CommitTableRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier: Option<TableIdent>,
    pub requirements: Vec<TableRequirement>,
    pub updates: Vec<TableUpdate>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CommitTableResponse {
    pub metadata_location: String,
    pub metadata: TableMetadataRef,
    pub config: Option<std::collections::HashMap<String, String>>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CommitTransactionRequest {
    pub table_changes: Vec<CommitTableRequest>,
}

#[must_use]
pub fn create_etag(text: &str) -> String {
    let hash = xxh3_64(text.as_bytes());
    format!("\"{hash:x}\"")
}

#[cfg(feature = "axum")]
impl IntoResponse for LoadTableResult {
    fn into_response(self) -> axum::http::Response<axum::body::Body> {
        let mut headers = HeaderMap::new();
        let body = axum::Json(&self);

        let Some(ref metadata_location) = self.metadata_location else {
            return (headers, body).into_response();
        };
        let etag = create_etag(metadata_location);

        let Ok(header_value) = etag.parse::<HeaderValue>() else {
            tracing::error!(
                "Failed to create valid ETAG header from etag string: {}",
                etag
            );
            return (headers, body).into_response();
        };

        headers.insert(header::ETAG, header_value);
        (headers, body).into_response()
    }
}

// #[cfg(feature = "axum")]
// impl_into_response!(LoadTableResult);
#[cfg(feature = "axum")]
impl_into_response!(ListTablesResponse);
#[cfg(feature = "axum")]
impl_into_response!(CommitTableResponse);
#[cfg(feature = "axum")]
impl_into_response!(LoadCredentialsResponse);

#[cfg(test)]
#[cfg(feature = "axum")]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use iceberg::spec::{FormatVersion, Schema, TableMetadata, TableMetadataBuilder};

    use super::*;

    #[test]
    #[cfg(feature = "axum")]
    fn test_create_etag() {
        let etag = create_etag("Hello World");
        assert_eq!(etag, "\"e34615aade2e6333\"");
    }

    #[test]
    #[cfg(feature = "axum")]
    fn test_load_table_result_into_response_adds_etag_for_existing_tables() {
        let table_metadata = create_table_metadata_mock();

        let load_table_result = LoadTableResult {
            metadata_location: Some("s3://bucket/table/metadata.json".to_string()),
            metadata: table_metadata,
            config: None,
            storage_credentials: None,
        };

        let response = load_table_result.into_response();
        let headers = response.headers();

        assert_eq!(
            headers.get(header::ETAG).unwrap(),
            &create_etag("s3://bucket/table/metadata.json")
        );
    }

    #[test]
    #[cfg(feature = "axum")]
    fn test_load_table_result_into_response_returns_no_etag_when_returning_staged_table() {
        let table_metadata = create_table_metadata_mock();

        let load_table_result = LoadTableResult {
            metadata_location: None,
            metadata: table_metadata,
            config: None,
            storage_credentials: None,
        };

        let response = load_table_result.into_response();
        let headers = response.headers();

        assert!(!headers.contains_key(header::ETAG));
    }

    #[tokio::test]
    #[cfg(feature = "axum")]
    async fn test_load_table_result_into_response_returns_load_table_result_as_json_body() {
        let table_metadata = create_table_metadata_mock();

        let load_table_result = LoadTableResult {
            metadata_location: Some("s3://bucket/table/metadata.json".to_string()),
            metadata: table_metadata.clone(),
            config: None,
            storage_credentials: None,
        };

        let response = load_table_result.clone().into_response();
        let body = response.into_body();

        let body_bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        let deserialized: LoadTableResult =
            serde_json::from_slice(&body_bytes).expect("Failed to deserialize body");

        assert_eq!(deserialized, load_table_result);
    }

    fn create_table_metadata_mock() -> Arc<TableMetadata> {
        let schema = Schema::builder().with_schema_id(0).build().unwrap();

        let unbound_spec = UnboundPartitionSpec::default();

        let sort_order = SortOrder::builder()
            .with_order_id(0)
            .build(&schema)
            .unwrap();

        let props = HashMap::new();

        let mut builder = TableMetadataBuilder::new(
            schema.clone(),
            unbound_spec.clone(),
            sort_order.clone(),
            "memory://dummy".to_string(),
            FormatVersion::V2,
            props,
        )
        .unwrap();
        builder = builder.add_schema(schema.clone()).unwrap();
        builder = builder.set_current_schema(0).unwrap();
        builder = builder.add_partition_spec(unbound_spec).unwrap();
        builder = builder
            .set_default_partition_spec(TableMetadataBuilder::LAST_ADDED)
            .unwrap();
        builder = builder.add_sort_order(sort_order).unwrap();
        builder = builder
            .set_default_sort_order(i64::from(TableMetadataBuilder::LAST_ADDED))
            .unwrap();

        let build_result: TableMetadata = builder.build().unwrap().into();
        build_result.into()
    }
}
