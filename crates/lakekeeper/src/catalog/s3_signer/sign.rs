use std::{collections::HashMap, sync::Arc, time::SystemTime, vec};

use aws_sigv4::{
    http_request::{sign as aws_sign, SignableBody, SignableRequest, SigningSettings},
    sign::v4,
    {self},
};

use super::{super::CatalogServer, error::SignError};
use crate::{
    api::{
        iceberg::types::Prefix, ApiContext, ErrorModel, IcebergErrorResponse, Result,
        S3SignRequest, S3SignResponse,
    },
    catalog::require_warehouse_id,
    request_metadata::RequestMetadata,
    service::{
        authz::{Authorizer, CatalogTableAction, CatalogWarehouseAction},
        secrets::SecretStore,
        storage::{
            s3::S3UrlStyleDetectionMode, S3Credential, S3Location, S3Profile, StorageCredential,
        },
        Catalog, GetTableMetadataResponse, ListFlags, State, TableId, Transaction,
    },
    WarehouseId,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Operation {
    Read,
    Write,
    Delete,
}

#[async_trait::async_trait]
impl<C: Catalog, A: Authorizer + Clone, S: SecretStore>
    crate::api::iceberg::v1::s3_signer::Service<State<A, C, S>> for CatalogServer<C, A, S>
{
    #[allow(clippy::too_many_lines)]
    async fn sign(
        prefix: Option<Prefix>,
        path_table_id: Option<uuid::Uuid>,
        request: S3SignRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<S3SignResponse> {
        let warehouse_id = require_warehouse_id(prefix.clone())?;
        let authorizer = state.v1_state.authz.clone();
        authorizer
            .require_warehouse_action(
                &request_metadata,
                warehouse_id,
                CatalogWarehouseAction::CanUse,
            )
            .await?;

        let S3SignRequest {
            region: request_region,
            uri: request_url,
            method: request_method,
            headers: request_headers,
            body: request_body,
        } = request.clone();

        // Include staged tables as this might be a commit
        let include_staged = true;

        let (parsed_url, operation) = s3_utils::parse_s3_url(
            &request_url,
            s3_url_style_detection::<C>(state.v1_state.catalog.clone(), warehouse_id).await?,
            &request_method,
            request_body.as_deref(),
        )?;

        let GetTableMetadataResponse {
            table: _,
            table_id,
            namespace_id: _,
            warehouse_id: _,
            location,
            metadata_location: _,
            storage_secret_ident,
            storage_profile,
        } = if let Some(table_id) = path_table_id.map(Into::into) {
            let metadata_by_id = get_unauthorized_table_metadata_by_id(
                warehouse_id,
                table_id,
                include_staged,
                &request_url,
                &state,
            )
            .await;
            // Can't fail here before AuthZ!

            // Up to version 0.9.1 pyiceberg had a bug that did not allow table specific signer URIs.
            // Instead the first URI of the first sign call would be used for subsequent calls in the same runtime too.
            // This is fixed in 0.9.2 onward: https://github.com/apache/iceberg-python/pull/2005
            // To keep backward compatibility we move to location based lookup if the location does not match.
            // This will will be removed in a future version of Lakekeeper.
            // We only perform the fallback if:
            // 1. the requested table id is not found (probably deleted)
            // 2. the location of the table does not match the request URI

            let mut fallback_to_location = false;
            match &metadata_by_id {
                Ok(None) => {
                    fallback_to_location = true;
                }
                Ok(Some(metadata_by_id)) => {
                    if validate_uri(&parsed_url, &metadata_by_id.location).is_err() {
                        tracing::warn!("Received a table specific sign request for table {table_id} with a location {} that does not match the request URI {request_url}. Falling back to location based lookup. This is a bug in the query engine. When using PyIceberg, please update to versions > 0.9.1", metadata_by_id.location);
                        fallback_to_location = true;
                    }
                }
                _ => {}
            }

            if fallback_to_location {
                get_table_metadata_by_location(
                    warehouse_id,
                    &parsed_url,
                    include_staged,
                    &request_url,
                    &state,
                    &request_metadata,
                    authorizer.clone(),
                )
                .await?
            } else {
                authorizer
                    .require_table_action(
                        &request_metadata,
                        metadata_by_id,
                        CatalogTableAction::CanGetMetadata,
                    )
                    .await?
            }
        } else {
            get_table_metadata_by_location(
                warehouse_id,
                &parsed_url,
                include_staged,
                &request_url,
                &state,
                &request_metadata,
                authorizer.clone(),
            )
            .await?
        };

        // First check - fail fast if requested table is not allowed.
        // We also need to check later if the path matches the table location.
        authorize_operation::<A>(operation, &request_metadata, table_id, authorizer).await?;

        let extend_err = |mut e: IcebergErrorResponse| {
            e.error = e
                .error
                .append_detail(format!("Table ID: {table_id}"))
                .append_detail(format!("Request URI: {request_url}"))
                .append_detail(format!("Request Region: {request_region}"))
                .append_detail(format!("Table Location: {location}"));
            e
        };

        let storage_profile = storage_profile
            .try_into_s3()
            .map_err(|e| extend_err(IcebergErrorResponse::from(e)))?;

        validate_region(&request_region, &storage_profile).map_err(extend_err)?;
        validate_uri(&parsed_url, &location).map_err(extend_err)?;

        // If all is good, we need the storage secret
        let storage_secret = if let Some(storage_secret_ident) = storage_secret_ident {
            Some(
                state
                    .v1_state
                    .secrets
                    .get_secret_by_id::<StorageCredential>(storage_secret_ident)
                    .await?
                    .secret,
            )
        } else {
            None
        }
        .map(|secret| {
            secret
                .try_to_s3()
                .map_err(|e| extend_err(IcebergErrorResponse::from(e)))
                .cloned()
        })
        .transpose()?;

        sign(
            &storage_profile,
            storage_secret.as_ref(),
            request_body,
            &request_region,
            &request_url,
            &request_method,
            request_headers,
        )
        .await
        .map_err(extend_err)
    }
}

async fn s3_url_style_detection<C: Catalog>(
    state: C::State,
    warehouse_id: WarehouseId,
) -> Result<S3UrlStyleDetectionMode, IcebergErrorResponse> {
    let t = super::cache::WAREHOUSE_S3_URL_STYLE_CACHE
        .try_get_with(warehouse_id, async {
            tracing::trace!("No cache hit for {warehouse_id}");
            let mut tx = C::Transaction::begin_read(state).await?;
            let result = C::require_warehouse(warehouse_id, tx.transaction())
                .await
                .map(|w| {
                    w.storage_profile
                        .try_into_s3()
                        .map(|s| s.remote_signing_url_style)
                        .map_err(|e| {
                            IcebergErrorResponse::from(ErrorModel::bad_request(
                                "Warehouse storage profile is not an S3 profile",
                                "InvalidWarehouse",
                                Some(Box::new(e)),
                            ))
                        })
                })?;
            tx.commit().await?;
            result
        })
        .await
        .map_err(|e: Arc<IcebergErrorResponse>| {
            tracing::debug!("Failed to get warehouse S3 URL style detection mode from cache due to error: '{e:?}'");
            IcebergErrorResponse::from(ErrorModel::new(
                e.error.message.as_str(),
                e.error.r#type.as_str(),
                e.error.code,
                // moka Arcs errors, our errors have a non-clone backtrace, and we can't get it out
                // so we don't forward the error here. We log it above tho.
                None,
            ))
        })?;
    Ok(t)
}

async fn sign(
    storage_profile: &S3Profile,
    credentials: Option<&S3Credential>,
    request_body: Option<String>,
    request_region: &str,
    request_url: &url::Url,
    request_method: &http::Method,
    request_headers: HashMap<String, Vec<String>>,
) -> Result<S3SignResponse> {
    let body = request_body.map(std::string::String::into_bytes);
    let signable_body = if let Some(body) = &body {
        SignableBody::Bytes(body)
    } else {
        SignableBody::UnsignedPayload
    };

    let mut sign_settings = SigningSettings::default();
    sign_settings.percent_encoding_mode = aws_sigv4::http_request::PercentEncodingMode::Single;
    sign_settings.payload_checksum_kind = aws_sigv4::http_request::PayloadChecksumKind::XAmzSha256;
    let aws_credentials = storage_profile
        .get_aws_credentials_with_assumed_role(credentials)
        .await?
        .ok_or_else(|| {
            ErrorModel::precondition_failed(
                "Cannot sign requests for Warehouses without S3 credentials",
                "SignWithoutCredentials",
                None,
            )
        })?;
    let identity = aws_credentials.into();
    // let identity = credentials.into();
    let signing_params = v4::SigningParams::builder()
        .identity(&identity)
        .region(request_region)
        .name("s3")
        .time(SystemTime::now())
        .settings(sign_settings)
        .build()
        .map_err(|e| {
            ErrorModel::builder()
                .code(http::StatusCode::INTERNAL_SERVER_ERROR.into())
                .message("Failed to create signing params".to_string())
                .r#type("FailedToCreateSigningParams".to_string())
                .source(Some(Box::new(e)))
                .build()
        })?
        .into();

    let mut headers_vec: Vec<(String, String)> = Vec::new();

    for (key, values) in request_headers.clone() {
        for value in values {
            headers_vec.push((key.clone(), value));
        }
    }

    let encoded_uri = urldecode_uri_path_segments(request_url)?;
    let signable_request = SignableRequest::new(
        request_method.as_str(),
        encoded_uri.to_string(),
        headers_vec.iter().map(|(k, v)| (k.as_str(), v.as_str())),
        signable_body,
    )
    .map_err(|e| {
        ErrorModel::builder()
            .code(http::StatusCode::BAD_REQUEST.into())
            .message("Request is not signable".to_string())
            .r#type("FailedToCreateSignableRequest".to_string())
            .source(Some(Box::new(e)))
            .build()
    })?;

    let (signing_instructions, _signature) = aws_sign(signable_request, &signing_params)
        .map_err(|e| {
            ErrorModel::builder()
                .code(http::StatusCode::INTERNAL_SERVER_ERROR.into())
                .message("Failed to sign request".to_string())
                .r#type("FailedToSignRequest".to_string())
                .source(Some(Box::new(e)))
                .build()
        })?
        .into_parts();

    let mut output_uri = encoded_uri.clone();
    for (key, value) in signing_instructions.params() {
        output_uri.query_pairs_mut().append_pair(key, value);
    }

    let mut output_headers = request_headers;
    for (key, value) in signing_instructions.headers() {
        output_headers.insert(key.to_string(), vec![value.to_string()]);
    }

    let sign_response = S3SignResponse {
        uri: output_uri,
        headers: output_headers,
    };

    Ok(sign_response)
}

fn urldecode_uri_path_segments(uri: &url::Url) -> Result<url::Url> {
    // We only modify path segments. Iterate over all path segments and unr urlencoding::decode them.
    let mut new_uri = uri.clone();
    let path_segments = new_uri
        .path_segments()
        .map(std::iter::Iterator::collect::<Vec<_>>)
        .unwrap_or_default();

    let mut new_path_segments = Vec::new();
    for segment in path_segments {
        new_path_segments.push(
            urlencoding::decode(segment)
                .map(|s| {
                    aws_smithy_http::label::fmt_string(
                        s,
                        aws_smithy_http::label::EncodingStrategy::Greedy,
                    )
                })
                .map_err(|e| {
                    ErrorModel::bad_request(
                        "Failed to decode URI segment",
                        "FailedToDecodeURISegment",
                        Some(Box::new(e)),
                    )
                })?,
        );
    }

    new_uri.set_path(&new_path_segments.join("/"));
    Ok(new_uri)
}

fn validate_region(region: &str, storage_profile: &S3Profile) -> Result<()> {
    if region != storage_profile.region {
        return Err(ErrorModel::builder()
            .code(http::StatusCode::BAD_REQUEST.into())
            .message("Region does not match storage profile".to_string())
            .r#type("RegionMismatch".to_string())
            .build()
            .into());
    }

    Ok(())
}

async fn authorize_operation<A: Authorizer>(
    method: Operation,
    metadata: &RequestMetadata,
    table_id: TableId,
    authorizer: A,
) -> Result<()> {
    // First check - fail fast if requested table is not allowed.
    // We also need to check later if the path matches the table location.
    match method {
        Operation::Read => {
            authorizer
                .require_table_action(
                    metadata,
                    Ok(Some(table_id)),
                    CatalogTableAction::CanReadData,
                )
                .await?;
        }
        Operation::Write | Operation::Delete => {
            authorizer
                .require_table_action(
                    metadata,
                    Ok(Some(table_id)),
                    CatalogTableAction::CanWriteData,
                )
                .await?;
        }
    }

    Ok(())
}

/// Helper function for fetching table metadata by ID
async fn get_unauthorized_table_metadata_by_id<
    C: Catalog,
    A: Authorizer + Clone,
    S: SecretStore,
>(
    warehouse_id: WarehouseId,
    table_id: TableId,
    include_staged: bool,
    request_url: &url::Url,
    state: &ApiContext<State<A, C, S>>,
) -> Result<Option<GetTableMetadataResponse>> {
    tracing::trace!("Got S3 sign request for table {table_id} with URL {request_url}");
    C::get_table_metadata_by_id(
        warehouse_id,
        table_id,
        ListFlags {
            include_staged,
            // we were able to resolve the table to id so we know the table is not deleted
            include_deleted: false,
            include_active: true,
        },
        state.v1_state.catalog.clone(),
    )
    .await
}

/// Helper function for fetching table metadata by location
async fn get_table_metadata_by_location<C: Catalog, A: Authorizer + Clone, S: SecretStore>(
    warehouse_id: WarehouseId,
    parsed_url: &s3_utils::ParsedSignRequest,
    include_staged: bool,
    request_url: &url::Url,
    state: &ApiContext<State<A, C, S>>,
    request_metadata: &RequestMetadata,
    authorizer: A,
) -> Result<GetTableMetadataResponse> {
    tracing::trace!("Got S3 sign request for URL {request_url} without table id. Searching for table id by location");
    let first_location = parsed_url.locations.first().ok_or_else(|| {
        ErrorModel::internal(
            "Request URI does not contain a location",
            "UriNoLocation",
            None,
        )
    })?;

    let metadata = C::get_table_metadata_by_s3_location(
        warehouse_id,
        first_location.location(),
        ListFlags {
            include_staged,
            // spark iceberg drops the table and then checks for existence of metadata files
            // which in turn needs to sign HEAD requests for files reachable from the
            // dropped table.
            include_deleted: true,
            include_active: true,
        },
        state.v1_state.catalog.clone(),
    )
    .await;

    authorizer
        .require_table_action(
            request_metadata,
            metadata,
            CatalogTableAction::CanGetMetadata,
        )
        .await
}

fn validate_uri(
    // i.e. https://bucket.s3.region.amazonaws.com/key
    parsed_url: &s3_utils::ParsedSignRequest,
    // i.e. s3://bucket/key
    table_location: &str,
) -> Result<()> {
    let table_location = S3Location::try_from_str(table_location, false)?;

    for url_location in &parsed_url.locations {
        if !url_location
            .location()
            .is_sublocation_of(table_location.location())
        {
            return Err(SignError::RequestUriMismatch {
                request_uri: parsed_url.url.to_string(),
                expected_location: table_location.into_normalized_location().to_string(),
                actual_location: url_location.as_normalized_location().to_string(),
            }
            .into());
        }
    }

    Ok(())
}

pub(super) mod s3_utils {
    use lazy_regex::regex;
    use serde::{Deserialize, Serialize};

    use super::{ErrorModel, Operation, Result};
    use crate::service::storage::{s3::S3UrlStyleDetectionMode, S3Location};

    #[derive(Debug, Clone)]
    pub(super) struct ParsedSignRequest {
        pub(super) url: url::Url,
        pub(super) locations: Vec<S3Location>,
        // Used endpoint without the bucket
        #[allow(dead_code)]
        pub(super) endpoint: String,
        #[allow(dead_code)]
        pub(super) port: u16,
    }

    /// Represents the top-level S3 Delete request structure
    #[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(rename = "Delete", rename_all = "PascalCase")]
    pub(super) struct DeleteObjectsRequest {
        #[serde(rename = "Object")]
        pub(super) objects: Vec<ObjectIdentifier>,
        #[serde(rename = "Quiet")]
        pub(super) quiet: Option<bool>,
    }

    /// Individual object to delete from S3
    #[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(rename_all = "PascalCase")]
    pub(super) struct ObjectIdentifier {
        /// Object key
        pub(super) key: String,
        /// Optional version ID for versioned objects
        #[serde(rename = "VersionId")]
        pub(super) version_id: Option<String>,
    }

    /// Errors that can occur during S3 delete XML parsing
    #[derive(thiserror::Error, Debug)]
    pub(super) enum S3DeleteParseError {
        #[error("XML Body parsing error: {0}")]
        Xml(#[from] quick_xml::Error),

        #[error("XML Body deserialization error: {0}")]
        Deserialization(#[from] quick_xml::DeError),

        #[error("No objects found in delete request")]
        NoObjects,
    }

    /// Parse S3 `DeleteObjects` XML and extract all keys
    ///
    /// # Arguments
    /// * `xml` - Raw XML string from S3 `DeleteObjects` request
    ///
    /// # Returns
    /// * `Result<Vec<String>, S3DeleteParseError>` - List of object keys or an error
    pub(super) fn parse_s3_delete_xml(xml: &str) -> Result<Vec<String>, S3DeleteParseError> {
        // Approach 1: Full deserialization using serde
        let delete_request: DeleteObjectsRequest = quick_xml::de::from_str(xml)?;

        if delete_request.objects.is_empty() {
            return Err(S3DeleteParseError::NoObjects);
        }

        let keys = delete_request
            .objects
            .into_iter()
            .map(|obj| obj.key)
            .collect();

        Ok(keys)
    }

    pub(super) fn parse_s3_url(
        uri: &url::Url,
        s3_url_style_detection: S3UrlStyleDetectionMode,
        method: &http::Method,
        body: Option<&str>,
    ) -> Result<(ParsedSignRequest, Operation)> {
        let err = |t: &str, m: &str| ErrorModel::bad_request(m, t, None);

        // Require https or http
        if !matches!(uri.scheme(), "https" | "http") {
            return Err(err(
                "UriSchemeNotSupported",
                "URI to sign does not have a supported scheme. Expected https or http",
            )
            .into());
        }

        // Determine operation type based on method
        let (operation, is_post_delete_operation) = match *method {
            http::Method::GET | http::Method::HEAD => (Operation::Read, false),
            http::Method::POST | http::Method::PUT => {
                // Handle special case: DeleteObjects operation (POST with ?delete and XML body)
                if method == http::Method::POST && uri.query().is_some_and(|q| q.contains("delete"))
                {
                    (Operation::Delete, true)
                } else {
                    (Operation::Write, false)
                }
            }
            http::Method::DELETE => (Operation::Delete, false),
            _ => {
                return Err(ErrorModel::builder()
                    .code(http::StatusCode::METHOD_NOT_ALLOWED.into())
                    .message("Method not allowed".to_string())
                    .r#type("MethodNotAllowed".to_string())
                    .build()
                    .into());
            }
        };

        // Parse the base URL
        let mut parsed_request = match s3_url_style_detection {
            S3UrlStyleDetectionMode::VirtualHost => {
                virtual_host_style(uri, is_post_delete_operation, true)?
            }
            S3UrlStyleDetectionMode::Path => path_style(uri, is_post_delete_operation)?,
            S3UrlStyleDetectionMode::Auto => {
                if let Ok(parsed) = virtual_host_style(uri, is_post_delete_operation, false) {
                    parsed
                } else if let Ok(parsed) = path_style(uri, is_post_delete_operation) {
                    parsed
                } else {
                    return Err(err("UriNotS3", "URI does not match S3 host or path style").into());
                }
            }
        };

        // For DeleteObjects operation, parse the XML body for object keys
        if is_post_delete_operation {
            if let Some(xml_body) = body {
                // Get bucket from the original parsed URL
                let bucket = parsed_request
                    .locations
                    .first()
                    .ok_or_else(|| {
                        // Should not happen, as both virtual & path style set a location
                        ErrorModel::internal(
                            "URI to sign does not have a location",
                            "UriNoLocation",
                            None,
                        )
                    })?
                    .bucket_name()
                    .to_string();

                // Parse XML body to get deletion keys
                let keys = parse_s3_delete_xml(xml_body)
                    .map_err(|e| err("InvalidDeleteBody", &format!("{e}")))?;

                // Create S3 locations for each key
                let mut locations = Vec::with_capacity(keys.len());
                for key in keys {
                    let location = S3Location::new(
                        bucket.clone(),
                        key.split('/').map(ToString::to_string).collect(),
                        None,
                    )?;
                    locations.push(location);
                }

                // Replace the locations in the parsed request
                parsed_request.locations = locations;
            } else {
                return Err(err("DeleteWithoutBody", "Delete requests require a body").into());
            }
        }

        Ok((parsed_request, operation))
    }

    fn virtual_host_style(
        uri: &url::Url,
        allow_no_key: bool,
        is_known: bool,
    ) -> Result<ParsedSignRequest> {
        let host = uri.host().ok_or_else(|| {
            ErrorModel::bad_request("URI to sign does not have a host", "UriNoHost", None)
        })?;
        let path_segments = get_path_segments(uri, allow_no_key)?;
        let port = uri.port_or_known_default().unwrap_or(443);

        let host_str = host.to_string();

        let re_host_pattern = regex!(r"^((.+)\.)?(s3[.-]([a-z0-9-]+)(\..*)?)");
        let (bucket, used_endpoint) = if is_known || host_str.ends_with(".r2.cloudflarestorage.com")
        {
            known_host_style(&host_str)?
        } else if let Some((Some(bucket), Some(used_endpoint))) =
            re_host_pattern.captures(&host_str).map(|captures| {
                (
                    captures.get(2).map(|m| m.as_str()),
                    captures.get(3).map(|m| m.as_str()),
                )
            })
        {
            (bucket, used_endpoint)
        } else {
            return Err(ErrorModel::bad_request(
                "URI does not match S3 host style",
                "UriNotS3",
                None,
            )
            .into());
        };

        Ok(ParsedSignRequest {
            url: uri.clone(),
            locations: vec![S3Location::new(
                bucket.to_string(),
                path_segments,
                Some(used_endpoint.to_string()),
            )?],
            endpoint: used_endpoint.to_string(),
            port,
        })
    }

    /// Returns bucket, string
    fn known_host_style(host: &str) -> Result<(&str, &str)> {
        let (bucket, endpoint) = host.split_once('.').ok_or_else(|| {
            ErrorModel::bad_request(
                "Invalid virtual-host style URL: Expected at least one point in hostname",
                "InvalidHostStyleURL",
                None,
            )
        })?;
        Ok((bucket, endpoint))
    }

    fn path_style(uri: &url::Url, allow_no_key: bool) -> Result<ParsedSignRequest> {
        let path_segments = get_path_segments(uri, allow_no_key)?;

        let min_path_segments = if allow_no_key { 1 } else { 2 };

        if path_segments.len() < min_path_segments {
            return Err(ErrorModel::bad_request(
                format!("Path style uri needs at least {min_path_segments} path segments"),
                "UriNotS3",
                None,
            )
            .into());
        }

        Ok(ParsedSignRequest {
            url: uri.clone(),
            locations: vec![S3Location::new(
                path_segments[0].to_string(),
                if path_segments.len() > 1 {
                    path_segments[1..].to_vec()
                } else {
                    vec![]
                },
                None,
            )?],
            endpoint: uri
                .host_str()
                .ok_or_else(|| {
                    ErrorModel::bad_request("URI to sign does not have a host", "UriNoHost", None)
                })?
                .to_string(),
            port: uri.port_or_known_default().unwrap_or(443),
        })
    }

    fn get_path_segments(uri: &url::Url, allow_no_segments: bool) -> Result<Vec<String>> {
        let segments = uri
            .path_segments()
            .map(|segments| segments.map(std::string::ToString::to_string).collect());

        if let Some(segments) = segments {
            Ok(segments)
        } else if allow_no_segments {
            Ok(vec![])
        } else {
            Err(
                ErrorModel::bad_request("URI to sign does not have a path", "UriNoPath", None)
                    .into(),
            )
        }
    }
}

#[cfg(test)]
mod test_delete_body_deserialization {
    use std::collections::HashSet;

    use super::s3_utils::{parse_s3_delete_xml, DeleteObjectsRequest};

    const TEST_XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
    <Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <Object>
            <Key>initial-warehouse/01963de0-99d9-79e2-8e95-24b11d0d334c/metadata/file1.avro</Key>
        </Object>
        <Object>
            <Key>initial-warehouse/01963de0-99d9-79e2-8e95-24b11d0d334c/metadata/file2.avro</Key>
        </Object>
        <Object>
            <Key>initial-warehouse/01963de0-99d9-79e2-8e95-24b11d0d334c/metadata/file3.avro</Key>
            <VersionId>version-id-1</VersionId>
        </Object>
    </Delete>"#;

    #[test]
    fn test_parse_s3_delete_xml() {
        let keys = parse_s3_delete_xml(TEST_XML).unwrap();
        assert_eq!(keys.len(), 3);
        assert!(keys.contains(
            &"initial-warehouse/01963de0-99d9-79e2-8e95-24b11d0d334c/metadata/file1.avro"
                .to_string()
        ));
        assert!(keys.contains(
            &"initial-warehouse/01963de0-99d9-79e2-8e95-24b11d0d334c/metadata/file2.avro"
                .to_string()
        ));
        assert!(keys.contains(
            &"initial-warehouse/01963de0-99d9-79e2-8e95-24b11d0d334c/metadata/file3.avro"
                .to_string()
        ));
    }

    #[test]
    fn test_full_deserialize_2() {
        let keys = parse_s3_delete_xml("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Object><Key>initial-warehouse/01963de0-99d9-79e2-8e95-24b11d0d334c/01963e34-84b6-7313-aba0-04694cd1c8c6/metadata/snap-8699614565852557623-1-15f84829-fee3-4cd6-8691-7ea967e4f15c.avro</Key></Object><Object><Key>initial-warehouse/01963de0-99d9-79e2-8e95-24b11d0d334c/01963e34-84b6-7313-aba0-04694cd1c8c6/metadata/snap-7686961691068480281-1-d204b9d8-6b72-454a-9f67-37a6d5e6d4a5.avro</Key></Object><Object><Key>initial-warehouse/01963de0-99d9-79e2-8e95-24b11d0d334c/01963e34-84b6-7313-aba0-04694cd1c8c6/metadata/snap-1836869532246818762-1-aebc0c21-c6ac-4ef2-abd0-5a17647a4f78.avro</Key></Object><Object><Key>initial-warehouse/01963de0-99d9-79e2-8e95-24b11d0d334c/01963e34-84b6-7313-aba0-04694cd1c8c6/metadata/snap-5189981498526175103-1-91703d93-aa16-4f0f-835e-606656746aa5.avro</Key></Object><Object><Key>initial-warehouse/01963de0-99d9-79e2-8e95-24b11d0d334c/01963e34-84b6-7313-aba0-04694cd1c8c6/metadata/snap-2371629502487233412-1-9ec13408-f2a0-4f30-8560-ac7ab26611b5.avro</Key></Object></Delete>").unwrap();
        let keys = HashSet::<String>::from_iter(keys);
        let expected = HashSet::from_iter(
            vec![
                "initial-warehouse/01963de0-99d9-79e2-8e95-24b11d0d334c/01963e34-84b6-7313-aba0-04694cd1c8c6/metadata/snap-8699614565852557623-1-15f84829-fee3-4cd6-8691-7ea967e4f15c.avro".to_string(),
                "initial-warehouse/01963de0-99d9-79e2-8e95-24b11d0d334c/01963e34-84b6-7313-aba0-04694cd1c8c6/metadata/snap-7686961691068480281-1-d204b9d8-6b72-454a-9f67-37a6d5e6d4a5.avro".to_string(),
                "initial-warehouse/01963de0-99d9-79e2-8e95-24b11d0d334c/01963e34-84b6-7313-aba0-04694cd1c8c6/metadata/snap-1836869532246818762-1-aebc0c21-c6ac-4ef2-abd0-5a17647a4f78.avro".to_string(),
                "initial-warehouse/01963de0-99d9-79e2-8e95-24b11d0d334c/01963e34-84b6-7313-aba0-04694cd1c8c6/metadata/snap-5189981498526175103-1-91703d93-aa16-4f0f-835e-606656746aa5.avro".to_string(),
                "initial-warehouse/01963de0-99d9-79e2-8e95-24b11d0d334c/01963e34-84b6-7313-aba0-04694cd1c8c6/metadata/snap-2371629502487233412-1-9ec13408-f2a0-4f30-8560-ac7ab26611b5.avro".to_string(),
            ]
        );
        assert_eq!(keys, expected);
    }

    #[test]
    fn test_full_deserialize() {
        let request: DeleteObjectsRequest = quick_xml::de::from_str(TEST_XML).unwrap();
        assert_eq!(request.objects.len(), 3);

        // Check both key and version are preserved
        assert_eq!(
            request.objects[2].key,
            "initial-warehouse/01963de0-99d9-79e2-8e95-24b11d0d334c/metadata/file3.avro"
        );
        assert_eq!(
            request.objects[2].version_id,
            Some("version-id-1".to_string())
        );

        // First object has no version
        assert_eq!(request.objects[0].version_id, None);
    }

    #[test]
    fn test_empty_delete_request() {
        let empty_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
            <Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            </Delete>"#;

        assert!(parse_s3_delete_xml(empty_xml).is_err());
    }

    #[test]
    fn test_malformed_xml() {
        let malformed_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
            <Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Object>
                    <Key>file1.avro</Key>
                </Object>
                <Object>
                    <Key>file2.avro
                </Object>
            </Delete>"#;

        assert!(parse_s3_delete_xml(malformed_xml).is_err());
    }
}

#[cfg(test)]
mod test {
    use itertools::Itertools as _;

    use super::*;
    use crate::{catalog::s3_signer::sign::s3_utils::parse_s3_url, service::storage::S3Flavor};

    #[derive(Debug)]
    struct TC {
        request_uri: &'static str,
        table_location: &'static str,
        #[allow(dead_code)]
        endpoint: Option<&'static str>,
        expected_outcome: bool,
    }

    fn run_validate_uri_test(test_case: &TC) {
        let request_uri = url::Url::parse(test_case.request_uri).unwrap();
        let (request_uri, _operation) = s3_utils::parse_s3_url(
            &request_uri,
            S3UrlStyleDetectionMode::Auto,
            &http::Method::GET,
            None,
        )
        .unwrap();
        let table_location = test_case.table_location;
        let result = validate_uri(&request_uri, table_location);
        assert_eq!(
            result.is_ok(),
            test_case.expected_outcome,
            "Test case: {test_case:?}",
        );
    }

    #[test]
    fn test_parse_s3_url_config_path_style() {
        let (parsed, _operation) = parse_s3_url(
            &url::Url::parse("https://not-a-bucket.s3.region.amazonaws.com/bucket/key").unwrap(),
            S3UrlStyleDetectionMode::Path,
            &http::Method::GET,
            None,
        )
        .unwrap();
        assert_eq!(parsed.locations[0].bucket_name(), "bucket");
    }

    #[test]
    fn test_parse_s3_url_config_virtual_style() {
        let (parsed, _operation) = parse_s3_url(
            &url::Url::parse("https://bucket.s3.region.amazonaws.com/key").unwrap(),
            S3UrlStyleDetectionMode::VirtualHost,
            &http::Method::GET,
            None,
        )
        .unwrap();
        assert_eq!(parsed.locations[0].bucket_name(), "bucket");
    }

    #[test]
    fn test_parse_s3_url_config_virtual_style_minimal() {
        let (parsed, _operation) = parse_s3_url(
            &url::Url::parse("https://bucket.s3-service/key").unwrap(),
            S3UrlStyleDetectionMode::VirtualHost,
            &http::Method::GET,
            None,
        )
        .unwrap();
        assert_eq!(parsed.locations[0].bucket_name(), "bucket");
    }

    #[test]
    fn test_parse_s3_url() {
        let cases = vec![
            (
                "https://foo.s3.endpoint.com/bar/a/key",
                "s3://foo/bar/a/key",
            ),
            ("https://s3-endpoint/bar/a/key", "s3://bar/a/key"),
            ("http://localhost:9000/bar/a/key", "s3://bar/a/key"),
            ("http://192.168.1.1/bar/a/key", "s3://bar/a/key"),
            (
                "https://bucket.s3-eu-central-1.amazonaws.com/file",
                "s3://bucket/file",
            ),
            ("https://bucket.s3.amazonaws.com/file", "s3://bucket/file"),
            (
                "https://s3.us-east-1.amazonaws.com/bucket/file",
                "s3://bucket/file",
            ),
            ("https://s3.amazonaws.com/bucket/file", "s3://bucket/file"),
            (
                "https://bucket.s3.my-region.private.com:9000/file",
                "s3://bucket/file",
            ),
            (
                "https://bucket.s3.private.com:9000/file",
                "s3://bucket/file",
            ),
            (
                "https://s3.my-region.private.amazonaws.com:9000/bucket/file",
                "s3://bucket/file",
            ),
            (
                "https://s3.private.amazonaws.com:9000/bucket/file",
                "s3://bucket/file",
            ),
            (
                "https://user@bucket.s3.my-region.private.com:9000/file",
                "s3://bucket/file",
            ),
            (
                "https://user@bucket.s3-my-region.localdomain.com:9000/file",
                "s3://bucket/file",
            ),
            ("http://127.0.0.1:9000/bucket/file", "s3://bucket/file"),
            ("http://s3.foo:9000/bucket/file", "s3://bucket/file"),
            ("http://s3.localhost:9000/bucket/file", "s3://bucket/file"),
            (
                "http://s3.localhost.localdomain:9000/bucket/file",
                "s3://bucket/file",
            ),
            (
                "http://s3.localhost.localdomain:9000/bucket/file",
                "s3://bucket/file",
            ),
            (
                "https://bucket.s3-fips.dualstack.us-east-2.amazonaws.com/file",
                "s3://bucket/file",
            ),
            (
                "https://bucket.s3-fips.dualstack.us-east-2.amazonaws.com/file",
                "s3://bucket/file",
            ),
            (
                "https://s3-accesspoint.dualstack.us-gov-west-1.amazonaws.com/bucket/file",
                "s3://bucket/file",
            ),
            (
                "https://bucket.s3-accesspoint.dualstack.us-gov-west-1.amazonaws.com/file",
                "s3://bucket/file",
            ),
            // Cloudflare R2
            (
                "https://bucket.accountid123.r2.cloudflarestorage.com/file",
                "s3://bucket/file",
            ),
            (
                "https://bucket.accountid123.eu.r2.cloudflarestorage.com/file",
                "s3://bucket/file",
            ),
        ];

        for (uri, expected) in cases {
            let uri = url::Url::parse(uri).unwrap();
            let (parsed, _operation) = parse_s3_url(
                &uri,
                S3UrlStyleDetectionMode::Auto,
                &http::Method::GET,
                None,
            )
            .unwrap_or_else(|_| panic!("Failed to parse {uri}"));
            let result = parsed.locations[0]
                .clone()
                .into_normalized_location()
                .to_string();
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_parse_s3_url_delete() {
        let cases = vec![
            (
                "http://my-host:9000/examples?delete",
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Object><Key>initial-warehouse/01963de0-99d9-79e2-8e95-24b11d0d334c/01963e34-84b6-7313-aba0-04694cd1c8c6/metadata/snap-8699614565852557623-1-15f84829-fee3-4cd6-8691-7ea967e4f15c.avro</Key></Object><Object><Key>initial-warehouse/01963de0-99d9-79e2-8e95-24b11d0d334c/01963e34-84b6-7313-aba0-04694cd1c8c6/metadata/snap-7686961691068480281-1-d204b9d8-6b72-454a-9f67-37a6d5e6d4a5.avro</Key></Object><Object><Key>initial-warehouse/01963de0-99d9-79e2-8e95-24b11d0d334c/01963e34-84b6-7313-aba0-04694cd1c8c6/metadata/snap-1836869532246818762-1-aebc0c21-c6ac-4ef2-abd0-5a17647a4f78.avro</Key></Object><Object><Key>initial-warehouse/01963de0-99d9-79e2-8e95-24b11d0d334c/01963e34-84b6-7313-aba0-04694cd1c8c6/metadata/snap-5189981498526175103-1-91703d93-aa16-4f0f-835e-606656746aa5.avro</Key></Object><Object><Key>initial-warehouse/01963de0-99d9-79e2-8e95-24b11d0d334c/01963e34-84b6-7313-aba0-04694cd1c8c6/metadata/snap-2371629502487233412-1-9ec13408-f2a0-4f30-8560-ac7ab26611b5.avro</Key></Object></Delete>",
                vec![
                    "s3://examples/initial-warehouse/01963de0-99d9-79e2-8e95-24b11d0d334c/01963e34-84b6-7313-aba0-04694cd1c8c6/metadata/snap-8699614565852557623-1-15f84829-fee3-4cd6-8691-7ea967e4f15c.avro",
                    "s3://examples/initial-warehouse/01963de0-99d9-79e2-8e95-24b11d0d334c/01963e34-84b6-7313-aba0-04694cd1c8c6/metadata/snap-7686961691068480281-1-d204b9d8-6b72-454a-9f67-37a6d5e6d4a5.avro",
                    "s3://examples/initial-warehouse/01963de0-99d9-79e2-8e95-24b11d0d334c/01963e34-84b6-7313-aba0-04694cd1c8c6/metadata/snap-1836869532246818762-1-aebc0c21-c6ac-4ef2-abd0-5a17647a4f78.avro",
                    "s3://examples/initial-warehouse/01963de0-99d9-79e2-8e95-24b11d0d334c/01963e34-84b6-7313-aba0-04694cd1c8c6/metadata/snap-5189981498526175103-1-91703d93-aa16-4f0f-835e-606656746aa5.avro",
                    "s3://examples/initial-warehouse/01963de0-99d9-79e2-8e95-24b11d0d334c/01963e34-84b6-7313-aba0-04694cd1c8c6/metadata/snap-2371629502487233412-1-9ec13408-f2a0-4f30-8560-ac7ab26611b5.avro",
                ],
            ),
            (
                "http://examples.s3.my-host:9000/?delete",
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Object><Key>a/b/c.parquet</Key></Object><Object><Key>a/b/d.parquet</Key></Object></Delete>",
                vec![
                    "s3://examples/a/b/c.parquet",
                    "s3://examples/a/b/d.parquet",
                ],
            ),
            (
                "http://examples.s3.my-host:9000?delete",
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Object><Key>a/b/c.parquet</Key></Object><Object><Key>a/b/d.parquet</Key></Object></Delete>",
                vec![
                    "s3://examples/a/b/c.parquet",
                    "s3://examples/a/b/d.parquet",
                ],
            )
        ];

        for (uri, body, expected) in cases {
            let uri = url::Url::parse(uri).unwrap();
            let (parsed, operation) = parse_s3_url(
                &uri,
                S3UrlStyleDetectionMode::Auto,
                &http::Method::POST,
                Some(body),
            )
            .unwrap_or_else(|e| panic!("Failed to parse {uri}: {e:?}"));

            let result = parsed
                .locations
                .iter()
                .map(|location| location.clone().into_normalized_location().to_string())
                .collect_vec();
            assert_eq!(result, expected);
            assert_eq!(operation, Operation::Delete);
        }
    }

    #[test]
    fn test_uri_virtual_host() {
        let cases = vec![
            // Basic bucket-style
            TC {
                request_uri: "https://bucket.s3.my-region.amazonaws.com/key",
                table_location: "s3://bucket/key",
                endpoint: None,
                expected_outcome: true,
            },
            // No region
            TC {
                request_uri: "https://bucket.s3.amazonaws.com/key",
                table_location: "s3://bucket/key",
                endpoint: None,
                expected_outcome: true,
            },
            // TLD
            TC {
                request_uri: "https://bucket.s3.my-service/key",
                table_location: "s3://bucket/key",
                endpoint: None,
                expected_outcome: true,
            },
            // Allow subpaths
            TC {
                request_uri: "https://bucket.s3.my-region.amazonaws.com/key/foo/file.parquet",
                table_location: "s3://bucket/key",
                endpoint: None,
                expected_outcome: true,
            },
            // Basic bucket-style with special characters in key
            TC {
                request_uri:
                    "https://bucket.s3.my-region.amazonaws.com/key/with-special-chars%20/foo",
                table_location: "s3://bucket/key/with-special-chars%20/foo",
                endpoint: None,
                expected_outcome: true,
            },
            // Wrong key
            TC {
                request_uri: "https://bucket.s3.my-region.amazonaws.com/key-2",
                table_location: "s3://bucket/key",
                endpoint: None,
                expected_outcome: false,
            },
            // Wrong bucket
            TC {
                request_uri: "https://bucket-2.s3.my-region.amazonaws.com/key",
                table_location: "s3://bucket/key",
                endpoint: None,
                expected_outcome: false,
            },
            // Bucket with points
            TC {
                request_uri: "https://bucket.with.point.s3.my-region.amazonaws.com/key",
                table_location: "s3://bucket.with.point/key",
                endpoint: None,
                expected_outcome: true,
            },
        ];

        for tc in cases {
            run_validate_uri_test(&tc);
        }
    }

    #[test]
    fn test_uri_path_style() {
        let cases = vec![
            // Basic path-style
            TC {
                request_uri: "https://s3.my-region.amazonaws.com/bucket/key",
                table_location: "s3://bucket/key",
                endpoint: None,
                expected_outcome: true,
            },
            // Allow subpaths
            TC {
                request_uri: "https://s3.my-region.amazonaws.com/bucket/key/foo/file.parquet",
                table_location: "s3://bucket/key",
                endpoint: None,
                expected_outcome: true,
            },
            // Basic path-style with special characters in key
            TC {
                request_uri:
                    "https://s3.my-region.amazonaws.com/bucket/key/with-special-chars%20/foo",
                table_location: "s3://bucket/key/with-special-chars%20/foo",
                endpoint: None,
                expected_outcome: true,
            },
            // Wrong key
            TC {
                request_uri: "https://s3.my-region.amazonaws.com/bucket/key-2",
                table_location: "s3://bucket/key",
                endpoint: None,
                expected_outcome: false,
            },
            // Wrong bucket
            TC {
                request_uri: "https://s3.my-region.amazonaws.com/bucket-2/key",
                table_location: "s3://bucket/key",
                endpoint: None,
                expected_outcome: false,
            },
            // Bucket with points
            TC {
                request_uri: "https://s3.my-region.amazonaws.com/bucket.with.point/key",
                table_location: "s3://bucket.with.point/key",
                endpoint: None,
                expected_outcome: true,
            },
        ];

        for tc in cases {
            run_validate_uri_test(&tc);
        }
    }

    #[test]
    fn test_uri_bucket_missing() {
        parse_s3_url(
            &url::Url::parse("https://s3.my-region.amazonaws.com/key").unwrap(),
            S3UrlStyleDetectionMode::Auto,
            &http::Method::GET,
            None,
        )
        .unwrap_err();
    }

    #[test]
    fn test_uri_custom_endpoint() {
        let cases = vec![
            // Endpoint specified
            TC {
                request_uri: "https://bucket.with.point.s3.my-service.example.com/key",
                table_location: "s3://bucket.with.point/key",
                endpoint: Some("https://s3.my-service.example.com"),
                expected_outcome: true,
            },
        ];

        for tc in cases {
            run_validate_uri_test(&tc);
        }
    }

    #[test]
    fn test_validate_region() {
        let storage_profile = S3Profile::builder()
            .region("my-region".to_string())
            .flavor(S3Flavor::S3Compat)
            .sts_enabled(false)
            .bucket("should-not-be-used".to_string())
            .build();

        let result = validate_region("my-region", &storage_profile);
        assert!(result.is_ok());

        let result = validate_region("wrong-region", &storage_profile);
        assert!(result.is_err());
    }
}
