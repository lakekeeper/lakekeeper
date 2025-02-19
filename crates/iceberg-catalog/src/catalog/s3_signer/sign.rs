use std::{collections::HashMap, str::FromStr, time::SystemTime, vec};

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
        storage::{S3Location, S3Profile, StorageCredential},
        Catalog, GetTableMetadataResponse, ListFlags, State, TableIdentUuid,
    },
};

const READ_METHODS: &[&str] = &["GET", "HEAD"];
const WRITE_METHODS: &[&str] = &["PUT", "POST", "DELETE"];

#[async_trait::async_trait]
impl<C: Catalog, A: Authorizer + Clone, S: SecretStore>
    crate::api::iceberg::v1::s3_signer::Service<State<A, C, S>> for CatalogServer<C, A, S>
{
    #[allow(clippy::too_many_lines)]
    async fn sign(
        prefix: Option<Prefix>,
        _namespace: Option<String>,
        table: Option<String>,
        request: S3SignRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<S3SignResponse> {
        let warehouse_id = require_warehouse_id(prefix.clone())?;
        let authorizer = state.v1_state.authz;
        authorizer
            .require_warehouse_action(
                &request_metadata,
                warehouse_id,
                &CatalogWarehouseAction::CanUse,
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

        let parsed_url = s3_utils::parse_s3_url(&request_url)?;

        // Unfortunately there is currently no way to pass information about warehouse_id & table_id
        // to this function from a get_table or create_table process without exchanging the token.
        // Spark does not support per-table signer.uri.
        // Tabular uses a token-exchange to include the information.
        // We are looking for the path in the database, which allows us to also work with AuthN solutions
        // that do not support custom data in tokens. Perspectively, we should
        // try to get per-table signer.uri support in Spark.
        let GetTableMetadataResponse {
            table: _,
            table_id,
            namespace_id: _,
            warehouse_id: _,
            location,
            metadata_location: _,
            storage_secret_ident,
            storage_profile,
        } = if let Ok(table_id) = require_table_id(table.clone()) {
            let metadata = C::get_table_metadata_by_id(
                warehouse_id,
                table_id,
                ListFlags {
                    include_staged,
                    // we were able to resolve the table to id so we know the table is not deleted
                    include_deleted: false,
                    include_active: true,
                },
                state.v1_state.catalog,
            )
            .await;
            authorizer
                .require_table_action(
                    &request_metadata,
                    metadata,
                    &CatalogTableAction::CanGetMetadata,
                )
                .await?
        } else {
            let metadata = C::get_table_metadata_by_s3_location(
                warehouse_id,
                parsed_url.location.location(),
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
                    &request_metadata,
                    metadata,
                    &CatalogTableAction::CanGetMetadata,
                )
                .await?
        };

        // First check - fail fast if requested table is not allowed.
        // We also need to check later if the path matches the table location.
        validate_table_method::<A>(&request_method, &request_metadata, table_id, authorizer)
            .await?;

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
                    .get_secret_by_id::<StorageCredential>(&storage_secret_ident)
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

        let credentials: aws_credential_types::Credentials = storage_profile
            .get_aws_sdk_credentials(storage_secret.as_ref())
            .map_err(|e| extend_err(IcebergErrorResponse::from(e)))?;

        sign(
            credentials,
            request_body,
            &request_region,
            &request_url,
            &request_method,
            request_headers,
        )
        .map_err(extend_err)
    }
}

fn sign(
    credentials: aws_credential_types::Credentials,
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
    let identity = credentials.into();
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

fn require_table_id(table_id: Option<String>) -> Result<TableIdentUuid> {
    table_id
        .ok_or(
            ErrorModel::builder()
                .code(http::StatusCode::BAD_REQUEST.into())
                .message("A Table ID is required as part of the URL path".to_string())
                .r#type("TableIdRequired".to_string())
                .build()
                .into(),
        )
        .and_then(|table_id| TableIdentUuid::from_str(&table_id).map_err(Into::into))
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

async fn validate_table_method<A: Authorizer>(
    method: &http::Method,
    metadata: &RequestMetadata,
    table_id: TableIdentUuid,
    authorizer: A,
) -> Result<()> {
    // First check - fail fast if requested table is not allowed.
    // We also need to check later if the path matches the table location.
    if WRITE_METHODS.contains(&method.as_str()) {
        // We specify namespace as none for AuthZ check because we don't want to grant access to
        // locations not known to the catalog.
        authorizer
            .require_table_action(
                metadata,
                Ok(Some(table_id)),
                &CatalogTableAction::CanWriteData,
            )
            .await?;
    } else if READ_METHODS.contains(&method.as_str()) {
        authorizer
            .require_table_action(
                metadata,
                Ok(Some(table_id)),
                &CatalogTableAction::CanReadData,
            )
            .await?;
    } else {
        return Err(ErrorModel::builder()
            .code(http::StatusCode::METHOD_NOT_ALLOWED.into())
            .message("Method not allowed".to_string())
            .r#type("MethodNotAllowed".to_string())
            .build()
            .into());
    }

    Ok(())
}

#[allow(clippy::too_many_lines)]
fn validate_uri(
    // i.e. https://bucket.s3.region.amazonaws.com/key
    parsed_url: &s3_utils::ParsedS3Url,
    // i.e. s3://bucket/key
    table_location: &str,
) -> Result<()> {
    let table_location = S3Location::from_str(table_location)?;
    let url_location = &parsed_url.location;

    if !url_location
        .location()
        .is_sublocation_of(table_location.location())
    {
        return Err(SignError::RequestUriMismatch {
            request_uri: parsed_url.url.to_string(),
            expected_location: table_location.to_string(),
            actual_location: parsed_url.location.to_string(),
        }
        .into());
    }

    Ok(())
}

pub(super) mod s3_utils {
    use lazy_regex::regex;

    use super::{ErrorModel, Result};
    use crate::service::storage::S3Location;

    #[derive(Debug)]
    pub(super) struct ParsedS3Url {
        pub(super) url: url::Url,
        pub(super) location: S3Location,
        // Used endpoint without the bucket
        #[allow(dead_code)]
        pub(super) endpoint: String,
        #[allow(dead_code)]
        pub(super) port: u16,
    }

    pub(super) fn parse_s3_url(uri: &url::Url) -> Result<ParsedS3Url> {
        let re_host_pattern = regex!(r"^((.+)\.)?(s3[.-]([a-z0-9-]+)\..*)");

        let err = |t: &str, m: &str| {
            ErrorModel::builder()
                .code(http::StatusCode::BAD_REQUEST.into())
                .message(m.to_string())
                .r#type(t.to_string())
                .build()
        };

        let host = uri
            .host()
            .ok_or(err("UriNoHost", "URI to sign does not have a host"))?;

        // Require https or http
        if !matches!(uri.scheme(), "https" | "http") {
            return Err(err(
                "UriSchemeNotSupported",
                "URI to sign does not have a supported scheme. Expected https or http",
            )
            .into());
        }

        let path_segments: Vec<String> = uri
            .path_segments()
            .map(|segments| segments.map(std::string::ToString::to_string).collect())
            .ok_or(err(
                "UriNoPath",
                "URI to sign does not have a path. Expected a path to an object",
            ))?;

        if let Some((Some(bucket), Some(used_endpoint))) =
            re_host_pattern.captures(&host.to_string()).map(|captures| {
                (
                    captures.get(2).map(|m| m.as_str()),
                    captures.get(3).map(|m| m.as_str()),
                )
            })
        {
            // Host Style Case
            Ok(ParsedS3Url {
                url: uri.clone(),
                location: S3Location::new(bucket.to_string(), path_segments)?,
                endpoint: used_endpoint.to_string(),
                port: uri.port_or_known_default().unwrap_or(443),
            })
        } else if path_segments.len() >= 2 {
            // Path Style Case
            Ok(ParsedS3Url {
                url: uri.clone(),
                location: S3Location::new(
                    path_segments[0].to_string(),
                    path_segments[1..].to_vec(),
                )?,
                endpoint: host.to_string(),
                port: uri.port_or_known_default().unwrap_or(443),
            })
        } else {
            Err(err("UriNotS3", "URI does not match S3 host or path style").into())
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::service::storage::S3Flavor;

    #[derive(Debug)]
    struct TC {
        request_uri: &'static str,
        table_location: &'static str,
        #[allow(dead_code)]
        region: &'static str,
        #[allow(dead_code)]
        endpoint: Option<&'static str>,
        expected_outcome: bool,
    }

    fn run_validate_uri_test(test_case: &TC) {
        let request_uri = url::Url::parse(test_case.request_uri).unwrap();
        let request_uri = s3_utils::parse_s3_url(&request_uri).unwrap();
        let table_location = test_case.table_location;
        let result = validate_uri(&request_uri, table_location);
        assert_eq!(
            result.is_ok(),
            test_case.expected_outcome,
            "Test case: {test_case:?}",
        );
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
        ];

        for (uri, expected) in cases {
            let uri = url::Url::parse(uri).unwrap();
            let result = s3_utils::parse_s3_url(&uri).unwrap().location.to_string();
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_uri_virtual_host() {
        let cases = vec![
            // Basic bucket-style
            TC {
                request_uri: "https://bucket.s3.my-region.amazonaws.com/key",
                table_location: "s3://bucket/key",
                region: "my-region",
                endpoint: None,
                expected_outcome: true,
            },
            // Allow subpaths
            TC {
                request_uri: "https://bucket.s3.my-region.amazonaws.com/key/foo/file.parquet",
                table_location: "s3://bucket/key",
                region: "my-region",
                endpoint: None,
                expected_outcome: true,
            },
            // Basic bucket-style with special characters in key
            TC {
                request_uri:
                    "https://bucket.s3.my-region.amazonaws.com/key/with-special-chars%20/foo",
                table_location: "s3://bucket/key/with-special-chars%20/foo",
                region: "my-region",
                endpoint: None,
                expected_outcome: true,
            },
            // Wrong key
            TC {
                request_uri: "https://bucket.s3.my-region.amazonaws.com/key-2",
                table_location: "s3://bucket/key",
                region: "my-region",
                endpoint: None,
                expected_outcome: false,
            },
            // Wrong bucket
            TC {
                request_uri: "https://bucket-2.s3.my-region.amazonaws.com/key",
                table_location: "s3://bucket/key",
                region: "my-region",
                endpoint: None,
                expected_outcome: false,
            },
            // Bucket with points
            TC {
                request_uri: "https://bucket.with.point.s3.my-region.amazonaws.com/key",
                table_location: "s3://bucket.with.point/key",
                region: "my-region",
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
                region: "my-region",
                endpoint: None,
                expected_outcome: true,
            },
            // Allow subpaths
            TC {
                request_uri: "https://s3.my-region.amazonaws.com/bucket/key/foo/file.parquet",
                table_location: "s3://bucket/key",
                region: "my-region",
                endpoint: None,
                expected_outcome: true,
            },
            // Basic path-style with special characters in key
            TC {
                request_uri:
                    "https://s3.my-region.amazonaws.com/bucket/key/with-special-chars%20/foo",
                table_location: "s3://bucket/key/with-special-chars%20/foo",
                region: "my-region",
                endpoint: None,
                expected_outcome: true,
            },
            // Wrong key
            TC {
                request_uri: "https://s3.my-region.amazonaws.com/bucket/key-2",
                table_location: "s3://bucket/key",
                region: "my-region",
                endpoint: None,
                expected_outcome: false,
            },
            // Wrong bucket
            TC {
                request_uri: "https://s3.my-region.amazonaws.com/bucket-2/key",
                table_location: "s3://bucket/key",
                region: "my-region",
                endpoint: None,
                expected_outcome: false,
            },
            // Bucket with points
            TC {
                request_uri: "https://s3.my-region.amazonaws.com/bucket.with.point/key",
                table_location: "s3://bucket.with.point/key",
                region: "my-region",
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
        let path = "https://s3.my-region.amazonaws.com/key";
        s3_utils::parse_s3_url(&url::Url::parse(path).unwrap()).unwrap_err();
    }

    #[test]
    fn test_uri_custom_endpoint() {
        let cases = vec![
            // Endpoint specified
            TC {
                request_uri: "https://bucket.with.point.s3.my-service.example.com/key",
                table_location: "s3://bucket.with.point/key",
                region: "my-region",
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
        let storage_profile = S3Profile {
            bucket: "should-not-be-used".to_string(),
            endpoint: None,
            region: "my-region".to_string(),
            assume_role_arn: None,
            path_style_access: None,
            key_prefix: None,
            sts_role_arn: None,
            sts_enabled: false,
            flavor: S3Flavor::S3Compat,
        };

        let result = validate_region("my-region", &storage_profile);
        assert!(result.is_ok());

        let result = validate_region("wrong-region", &storage_profile);
        assert!(result.is_err());
    }
}
