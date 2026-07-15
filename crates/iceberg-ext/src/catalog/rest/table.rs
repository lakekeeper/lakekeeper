use std::{collections::HashMap, sync::Arc};

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

#[derive(Debug, PartialEq, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct StorageCredential {
    pub prefix: String,
    pub config: std::collections::HashMap<String, String>,
}
#[derive(Debug, PartialEq, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
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
    /// Absolute time (epoch ms) until which a conditional request may be answered
    /// with `304`, or `None` if the response vends no expiring credentials.
    /// Computed from the credentials' actual expiry; not serialized — it is the
    /// revalidation point embedded in the [`ETag`] (via [`Self::etag`]), so a 304
    /// is never served once the client's credentials leave the serve window.
    #[serde(skip)]
    pub credentials_revalidate_after_ms: Option<i64>,
}

impl LoadTableResult {
    #[must_use]
    pub fn is_staged(&self) -> bool {
        self.metadata_location.is_none()
    }

    #[must_use]
    pub fn etag(&self) -> Option<ETag> {
        let metadata_location = self.metadata_location.as_ref()?;
        Some(TableETag::new(metadata_location, self.credentials_revalidate_after_ms).into_etag())
    }
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
    pub properties: Option<HashMap<String, String>>,
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
    pub identifiers: Arc<Vec<TableIdent>>,
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

impl CommitTableResponse {
    #[must_use]
    pub fn etag(&self) -> ETag {
        create_etag(&self.metadata_location)
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CommitTransactionRequest {
    pub table_changes: Vec<CommitTableRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ETag(String);

impl ETag {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for ETag {
    fn from(value: &str) -> Self {
        ETag(value.to_string())
    }
}

impl From<String> for ETag {
    fn from(value: String) -> Self {
        ETag(value)
    }
}

/// Version prefix for structured `loadTable` [`ETag`]s. Anything not parsing
/// under this prefix (pre-upgrade or future-version values) isn't matched, so
/// the client reloads. Bump the suffix on incompatible encoding changes.
const ETAG_PREFIX: &str = "lk1";

/// Structured contents of a `loadTable` [`ETag`].
///
/// Wire form (inside the quotes): `lk1.<metadata_hash>`, or
/// `lk1.<metadata_hash>.<revalidate_after_hex>` when credentials are vended
/// (revalidate-after as epoch-ms in hex). `metadata_hash` is the xxh3-64 hex of
/// the metadata location. Embedding the revalidation point lets the server
/// decide, from the client-echoed [`ETag`] alone, whether the held credentials
/// are still within their serve window — i.e. fresh enough for a 304.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableETag {
    metadata_hash: String,
    revalidate_after_ms: Option<i64>,
}

impl TableETag {
    #[must_use]
    pub fn new(metadata_location: &str, revalidate_after_ms: Option<i64>) -> Self {
        let hash = xxh3_64(metadata_location.as_bytes());
        Self {
            metadata_hash: format!("{hash:x}"),
            // A non-positive value carries no information; drop it.
            revalidate_after_ms: revalidate_after_ms.filter(|ms| *ms > 0),
        }
    }

    #[must_use]
    pub fn metadata_hash(&self) -> &str {
        &self.metadata_hash
    }

    #[must_use]
    pub fn revalidate_after_ms(&self) -> Option<i64> {
        self.revalidate_after_ms
    }

    /// Parse a client-supplied [`ETag`] value (quotes already stripped). Returns
    /// `None` for unrecognized values so callers can reload.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        let mut parts = value.split('.');
        if parts.next()? != ETAG_PREFIX {
            return None;
        }
        let metadata_hash = parts.next().filter(|s| !s.is_empty())?.to_string();
        let revalidate_after_ms = parts
            .next()
            .map(|s| i64::from_str_radix(s, 16))
            .transpose()
            .ok()?;
        // Reject trailing junk so an unexpected shape falls back to a reload.
        if parts.next().is_some() {
            return None;
        }
        Some(Self {
            metadata_hash,
            revalidate_after_ms,
        })
    }

    /// Render the wire [`ETag`] value, quoted per HTTP `ETag` syntax.
    #[must_use]
    pub fn into_etag(self) -> ETag {
        let inner = match self.revalidate_after_ms {
            Some(ms) => format!("{ETAG_PREFIX}.{}.{ms:x}", self.metadata_hash),
            None => format!("{ETAG_PREFIX}.{}", self.metadata_hash),
        };
        format!("\"{inner}\"").into()
    }
}

#[must_use]
pub fn create_etag(text: &str) -> ETag {
    TableETag::new(text, None).into_etag()
}

#[cfg(feature = "axum")]
impl IntoResponse for LoadTableResult {
    fn into_response(self) -> axum::http::Response<axum::body::Body> {
        let mut headers = HeaderMap::new();
        let body = axum::Json(&self);

        let Some(ref etag) = self.etag() else {
            return (headers, body).into_response();
        };

        match etag.as_str().parse::<HeaderValue>() {
            Ok(header_value) => {
                headers.insert(header::ETAG, header_value);
            }
            Err(e) => {
                tracing::error!(
                    "Failed to create valid ETAG header from metadata location. Etag: {}. Metadata location: {}, error: {e}",
                    etag.as_str(),
                    self.metadata_location
                        .as_ref()
                        .unwrap_or(&"<none>".to_string())
                );
            }
        }

        (headers, body).into_response()
    }
}

#[cfg(feature = "axum")]
impl IntoResponse for CommitTableResponse {
    fn into_response(self) -> axum::http::Response<axum::body::Body> {
        let mut headers = HeaderMap::new();
        let body = axum::Json(&self);

        let etag = self.etag();
        match etag.as_str().parse::<HeaderValue>() {
            Ok(header_value) => {
                headers.insert(header::ETAG, header_value);
            }
            Err(e) => {
                tracing::error!(
                    "Failed to create valid ETAG header from metadata location after commit. Etag: {}. Metadata location: {}, error: {e}",
                    etag.as_str(),
                    self.metadata_location
                );
            }
        }

        (headers, body).into_response()
    }
}

#[cfg(feature = "axum")]
impl_into_response!(ListTablesResponse);
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
        let ETag(etag) = create_etag("Hello World");
        assert_eq!(etag, "\"lk1.e34615aade2e6333\"");
    }

    #[test]
    fn test_table_etag_round_trip_metadata_only() {
        let etag = TableETag::new("s3://bucket/table/metadata.json", None);
        let ETag(wire) = etag.clone().into_etag();
        let parsed = TableETag::parse(wire.trim_matches('"')).unwrap();
        assert_eq!(parsed, etag);
        assert_eq!(parsed.revalidate_after_ms(), None);
    }

    #[test]
    fn test_table_etag_round_trip_with_expiry() {
        let etag = TableETag::new("s3://bucket/table/metadata.json", Some(1_750_000_000_123));
        let ETag(wire) = etag.clone().into_etag();
        let parsed = TableETag::parse(wire.trim_matches('"')).unwrap();
        assert_eq!(parsed, etag);
        assert_eq!(parsed.revalidate_after_ms(), Some(1_750_000_000_123));
    }

    #[test]
    fn test_table_etag_metadata_hash_matches_legacy() {
        // The metadata component must stay byte-identical to the legacy hash so
        // a pre-upgrade client's echoed ETag still matches after upgrade.
        let location = "s3://bucket/table/metadata.json";
        let legacy_hash = format!("{:x}", xxh3_64(location.as_bytes()));
        assert_eq!(TableETag::new(location, None).metadata_hash(), legacy_hash);
    }

    #[test]
    fn test_table_etag_parse_rejects_legacy_and_junk() {
        // Legacy bare hash → not the structured format.
        assert!(TableETag::parse("e34615aade2e6333").is_none());
        // Wrong prefix, empty hash, trailing junk, non-hex expiry.
        assert!(TableETag::parse("lk2.abc").is_none());
        assert!(TableETag::parse("lk1.").is_none());
        assert!(TableETag::parse("lk1.abc.def.ghi").is_none());
        assert!(TableETag::parse("lk1.abc.zzz").is_none());
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
            credentials_revalidate_after_ms: None,
        };

        let response = load_table_result.into_response();
        let headers = response.headers();

        let ETag(etag_expected) = create_etag("s3://bucket/table/metadata.json");
        assert_eq!(headers.get(header::ETAG).unwrap(), &etag_expected);
    }

    #[test]
    #[cfg(feature = "axum")]
    fn test_load_table_result_etag_embeds_revalidate_after() {
        let table_metadata = create_table_metadata_mock();
        let load_table_result = LoadTableResult {
            metadata_location: Some("s3://bucket/table/metadata.json".to_string()),
            metadata: table_metadata,
            config: None,
            storage_credentials: None,
            credentials_revalidate_after_ms: Some(1_750_000_000_123),
        };

        let ETag(etag) = load_table_result.etag().unwrap();
        let expected =
            TableETag::new("s3://bucket/table/metadata.json", Some(1_750_000_000_123)).into_etag();
        assert_eq!(ETag(etag), expected);
        // The revalidation point must round-trip out of the wire ETag.
        let parsed = TableETag::parse(expected.as_str().trim_matches('"')).unwrap();
        assert_eq!(parsed.revalidate_after_ms(), Some(1_750_000_000_123));
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
            credentials_revalidate_after_ms: None,
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
            credentials_revalidate_after_ms: None,
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
