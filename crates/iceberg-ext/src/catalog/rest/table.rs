use std::{collections::HashMap, sync::Arc};

#[cfg(feature = "axum")]
use axum::{
    http::header::{self, HeaderMap, HeaderValue},
    response::IntoResponse,
};
use iceberg::spec::TableMetadataRef;
use typed_builder::TypedBuilder;

#[cfg(feature = "axum")]
use super::impl_into_response;
use crate::{
    catalog::{TableIdent, TableRequirement, TableUpdate, rest::RemoteSigningConfig},
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
    /// Signer settings for clients that support them, superseding the deprecated
    /// `signer.uri` / `signer.endpoint` config keys. Omitted rather than sent as
    /// `null` when remote signing is off, since a client that finds it absent
    /// falls back to those keys.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_signing_config: Option<RemoteSigningConfig>,
    /// Validator for this exact body, emitted as the `ETag` header.
    ///
    /// Not serialized, and deliberately not derivable from this struct: the tag
    /// must cover request inputs that never appear in the body, and the
    /// conditional-request path has to compute it before a body exists. The
    /// caller mints it; `None` means no validator, so a conditional request
    /// reloads rather than risking a wrong `304`.
    #[serde(skip)]
    pub etag: Option<ETag>,
}

impl LoadTableResult {
    #[must_use]
    pub fn is_staged(&self) -> bool {
        self.metadata_location.is_none()
    }

    #[must_use]
    pub fn etag(&self) -> Option<ETag> {
        self.etag.clone()
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
    /// Validator for this body, emitted as the `ETag` header. See
    /// [`LoadTableResult::etag`] for why it is minted by the caller.
    #[serde(skip)]
    pub etag: Option<ETag>,
}

impl CommitTableResponse {
    #[must_use]
    pub fn etag(&self) -> Option<ETag> {
        self.etag.clone()
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
    /// The value as held, which for a server-minted tag is the wire form —
    /// weak marker and quotes included.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The bare validator: this tag with HTTP's weak marker and surrounding
    /// quotes removed.
    ///
    /// Comparisons must go through this rather than [`Self::as_str`]. A tag
    /// arrives already bare when the HTTP layer parsed an `If-None-Match`
    /// header, but in wire form when a server-minted tag is fed back in process
    /// — and the two must not compare unequal for being spelled differently.
    #[must_use]
    pub fn validator(&self) -> &str {
        Self::strip_wire_syntax(&self.0)
    }

    /// Strip HTTP's weak marker and surrounding quotes from one `ETag` value.
    ///
    /// The single definition of that transform, so a tag cannot be normalised
    /// one way on the way in and another on the way out. Idempotent, so
    /// applying it to an already-bare value is safe.
    ///
    /// Not wildcard-aware: it is quotes that tell `If-None-Match`'s `*` apart
    /// from a tag whose opaque value is `*`, and this removes them. Ask
    /// [`Self::is_wildcard`] before normalising.
    ///
    /// Deliberately tolerant about where the `W/` sits, so a client that
    /// re-serialises a weak tag as `"W/lk3.beef"` still matches. That spelling
    /// is strictly a *strong* tag whose opaque value happens to begin with
    /// `W/`, so folding the two together is not injective over the syntax RFC
    /// 9110 8.8.3 defines — but nothing here mints an opaque value starting
    /// with `W/`, so the only source of that spelling is a mangled tag of ours,
    /// and honouring it is what the client meant. Revisit if strong tags ever
    /// get minted, or if a comparison that must reject weak validators (`If-Match`,
    /// ranges) is added.
    #[must_use]
    pub fn strip_wire_syntax(value: &str) -> &str {
        value
            .trim()
            .trim_matches('"')
            .trim_start_matches("W/")
            .trim_matches('"')
    }

    /// Whether this is `If-None-Match`'s `*` — "any current representation" —
    /// rather than a validator to compare.
    ///
    /// RFC 9110 13.1.2 admits `*` only as an alternative to the tag list, not as
    /// a member of it, and the entity-tag grammar in 8.8.3 is quoted-only — so
    /// `"*"` and `W/"*"` are ordinary tags that merely happen to have `*` as
    /// their opaque value, and must be compared like any other. The difference
    /// matters: the wildcard skips validator comparison altogether, so reading
    /// it too widely answers `304` to a request that was asking whether one
    /// specific tag is current.
    ///
    /// Tested on the value as received, so it only reports the truth for a tag
    /// that has not already been through [`Self::strip_wire_syntax`].
    #[must_use]
    pub fn is_wildcard(&self) -> bool {
        self.0.trim() == "*"
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

#[cfg(feature = "axum")]
impl IntoResponse for LoadTableResult {
    fn into_response(self) -> axum::http::Response<axum::body::Body> {
        let mut headers = HeaderMap::new();
        let body = axum::Json(&self);

        let Some(ref etag) = self.etag else {
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

        let Some(ref etag) = self.etag else {
            return (headers, body).into_response();
        };

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

    /// Both spellings of the same tag must reduce to one validator: the wire form
    /// a server mints, and the bare form header parsing yields.
    #[test]
    fn etag_validator_strips_wire_syntax_idempotently() {
        let wire = ETag::from("W/\"lk3.deadbeef\"");
        assert_eq!(wire.validator(), "lk3.deadbeef");
        assert_eq!(wire.as_str(), "W/\"lk3.deadbeef\"", "as_str stays verbatim");

        // Already-bare input is unchanged, so normalising twice is safe.
        let bare = ETag::from("lk3.deadbeef");
        assert_eq!(bare.validator(), "lk3.deadbeef");
        assert_eq!(
            wire.validator(),
            bare.validator(),
            "the two spellings must not compare unequal"
        );

        // Strong validators carry quotes but no weak marker.
        assert_eq!(ETag::from("\"lk3.deadbeef\"").validator(), "lk3.deadbeef");
        // Surrounding whitespace comes from splitting a header list.
        assert_eq!(
            ETag::from(" W/\"lk3.deadbeef\" ").validator(),
            "lk3.deadbeef"
        );
        // The wildcard survives untouched.
        assert_eq!(ETag::from("*").validator(), "*");
        // A weak tag re-serialised with the marker inside the quotes still
        // normalises to the same validator. See the note on `strip_wire_syntax`
        // for why this tolerance is chosen over the strict reading.
        assert_eq!(ETag::from("\"W/lk3.deadbeef\"").validator(), "lk3.deadbeef");
    }

    /// Only the bare token is the wildcard. A quoted `*` is a tag like any
    /// other, and treating it as "any representation" would 304 a request that
    /// asked whether that one tag is current.
    #[test]
    fn only_the_unquoted_asterisk_is_the_wildcard() {
        assert!(ETag::from("*").is_wildcard());
        // Padding is list syntax, not part of the token.
        assert!(ETag::from(" * ").is_wildcard());

        assert!(!ETag::from("\"*\"").is_wildcard());
        assert!(!ETag::from("W/\"*\"").is_wildcard());
        assert!(!ETag::from("lk3.deadbeef").is_wildcard());
        // Still a tag once normalised — one that matches nothing we mint.
        assert_eq!(ETag::from("\"*\"").validator(), "*");
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
            remote_signing_config: None,
            etag: Some(ETag::from("W/\"lk2.deadbeef\"")),
        };

        let response = load_table_result.into_response();
        let headers = response.headers();

        assert_eq!(headers.get(header::ETAG).unwrap(), "W/\"lk2.deadbeef\"");
    }

    #[test]
    #[cfg(feature = "axum")]
    fn test_load_table_result_emits_the_caller_supplied_etag_verbatim() {
        // The tag is minted by the caller, which is the only place that knows the
        // request inputs it has to cover. This type must not reinterpret it.
        let load_table_result = LoadTableResult {
            metadata_location: Some("s3://bucket/table/metadata.json".to_string()),
            metadata: create_table_metadata_mock(),
            config: None,
            storage_credentials: None,
            remote_signing_config: None,
            etag: Some(ETag::from("W/\"lk2.abc.199e1e0f9c3\"")),
        };

        let response = load_table_result.into_response();
        assert_eq!(
            response.headers().get(header::ETAG).unwrap(),
            "W/\"lk2.abc.199e1e0f9c3\""
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
            remote_signing_config: None,
            // Staged tables have no metadata location, so the caller mints no tag.
            etag: None,
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
            remote_signing_config: None,
            etag: Some(ETag::from("W/\"lk2.deadbeef\"")),
        };

        let response = load_table_result.clone().into_response();
        let body = response.into_body();

        let body_bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        let deserialized: LoadTableResult =
            serde_json::from_slice(&body_bytes).expect("Failed to deserialize body");

        // `etag` is `#[serde(skip)]` — it travels in the header, not the body — so
        // it cannot survive a round trip and must be excluded from the comparison.
        assert_eq!(deserialized.etag, None);
        let expected = LoadTableResult {
            etag: None,
            ..load_table_result
        };
        assert_eq!(deserialized, expected);
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
