use std::{collections::HashMap, sync::Arc, time::SystemTime, vec};

use aws_sigv4::{
    http_request::{SignableBody, SignableRequest, SigningSettings, sign as aws_sign},
    sign::v4,
    {self},
};
use lakekeeper_io::{Location, s3::S3Location};

use super::{super::CatalogServer, error::SignError};
use crate::{
    WarehouseId,
    api::{
        ApiContext, ErrorModel, IcebergErrorResponse, Result, S3SignRequest, S3SignResponse,
        iceberg::{
            types::Prefix,
            v1::{TableIdent, s3_signer::SignTarget},
        },
    },
    request_metadata::RequestMetadata,
    server::require_warehouse_id,
    service::{
        AuthZTableInfo, CatalogNamespaceOps, CatalogStore, CatalogTabularOps, CatalogWarehouseOps,
        GenericTabularInfo, GetTabularInfoByLocationError, ResolvedWarehouse, State, TableInfo,
        TabularListFlags, ViewOrTableInfo,
        authz::{
            AuthZCannotSeeTableLocation, AuthZError, AuthZGenericTableOps, AuthZTableOps,
            Authorizer, AuthzNamespaceOps, AuthzWarehouseOps, CatalogGenericTableAction,
            CatalogTableAction, CatalogWarehouseAction, RequireGenericTableActionError,
            RequireTableActionError,
        },
        events::{APIEventContext, context::authz_to_error_no_audit},
        secrets::SecretStore,
        storage::{
            S3Credential, S3Profile, StorageProfile, ValidationError, s3::S3UrlStyleDetectionMode,
        },
    },
};

const UNSIGNED_HEADERS: &[&str] = &[
    "range",
    "x-amz-date",
    "amz-sdk-invocation-id",
    "amz-sdk-retry",
];
const CACHEABLE_METHODS: &[http::Method] = &[http::Method::GET, http::Method::HEAD];
const CACHE_CONTROL_HEADER: &str = "Cache-Control";
const CACHE_CONTROL_NO_CACHE: &str = "no-cache";
const CACHE_CONTROL_PRIVATE: &str = "private";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Operation {
    Read,
    Write,
    Delete,
}

/// A tabular the signer can sign requests for. Views are not signable.
enum SignableTabular {
    Table(TableInfo),
    GenericTable(GenericTabularInfo),
}

impl SignableTabular {
    fn from_view_or_table(info: ViewOrTableInfo) -> Option<Self> {
        match info {
            ViewOrTableInfo::Table(info) => Some(Self::Table(info)),
            ViewOrTableInfo::GenericTable(info) => Some(Self::GenericTable(info)),
            ViewOrTableInfo::View(info) => {
                tracing::warn!(
                    "Signer resolved view {} at location {}, but views are not supported for signing",
                    info.tabular_id,
                    info.location
                );
                None
            }
        }
    }

    fn location(&self) -> &Location {
        match self {
            Self::Table(info) => &info.location,
            Self::GenericTable(info) => &info.location,
        }
    }
}

/// The resolution outcome, split by the authorization path that has to handle it.
enum ResolvedSignable {
    GenericTable(GenericTabularInfo),
    /// Also carries the not-found and lookup-error cases: both are reported as
    /// `NoSuchTableLocationException` by the table-location authorization path.
    Iceberg(Result<Option<TableInfo>, RequireTableActionError>),
}

impl From<Result<Option<SignableTabular>, RequireTableActionError>> for ResolvedSignable {
    fn from(resolved: Result<Option<SignableTabular>, RequireTableActionError>) -> Self {
        match resolved {
            Ok(Some(SignableTabular::GenericTable(info))) => Self::GenericTable(info),
            Ok(Some(SignableTabular::Table(info))) => Self::Iceberg(Ok(Some(info))),
            Ok(None) => Self::Iceberg(Ok(None)),
            Err(e) => Self::Iceberg(Err(e)),
        }
    }
}

#[async_trait::async_trait]
impl<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>
    crate::api::iceberg::v1::s3_signer::Service<State<A, C, S>> for CatalogServer<C, A, S>
{
    #[allow(clippy::too_many_lines)]
    async fn sign(
        prefix: Option<Prefix>,
        target: SignTarget,
        request: S3SignRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<S3SignResponse> {
        let warehouse_id = require_warehouse_id(prefix.as_ref())?;
        let authorizer = state.v1_state.authz.clone();

        let warehouse =
            C::get_active_warehouse_by_id(warehouse_id, state.v1_state.catalog.clone()).await;

        let request_metadata = Arc::new(request_metadata);
        let warehouse_event_ctx = APIEventContext::for_warehouse(
            request_metadata.clone(),
            state.v1_state.events.clone(),
            warehouse_id,
            CatalogWarehouseAction::Use,
        );
        let warehouse = authorizer
            .require_warehouse_action(
                warehouse_event_ctx.request_metadata(),
                warehouse_id,
                warehouse,
                warehouse_event_ctx.action().clone(),
            )
            .await
            // Too noisy otherwise
            .map_err(authz_to_error_no_audit)?;

        // Check if remote signing is enabled for this storage profile
        if let StorageProfile::S3(s3_profile) = &warehouse.storage_profile {
            if !s3_profile.remote_signing_enabled {
                return Err(IcebergErrorResponse::from(ErrorModel::forbidden(
                    "Remote signing is disabled for this storage profile",
                    "RemoteSigningDisabled",
                    None,
                )));
            }
        } else {
            return Err(IcebergErrorResponse::from(ErrorModel::bad_request(
                "Remote signing is only supported for S3 storage",
                "UnsupportedStorageType",
                None,
            )));
        }

        let S3SignRequest {
            region: request_region,
            uri: request_url,
            method: request_method,
            headers: request_headers,
            body: request_body,
            // Accepted per spec; we advertise no properties to echo back.
            properties: _,
            provider: request_provider,
        } = request;

        // Absent means S3 per the spec's backwards-compatibility rule. Anything
        // else is refused rather than signed as S3 — we reached this point only
        // because the warehouse is an S3 profile, so a different provider means
        // the client is asking for something we cannot produce.
        if let Some(provider) = request_provider.as_deref()
            && !provider.eq_ignore_ascii_case("s3")
        {
            return Err(IcebergErrorResponse::from(ErrorModel::bad_request(
                format!(
                    "Cannot sign requests for storage provider '{provider}'. Only 's3' is supported."
                ),
                "UnsupportedSignerProvider",
                None,
            )));
        }

        let (parsed_url, operation) = s3_utils::parse_s3_url(
            &s3_utils::SignRequestUri::new(request_url.clone())?,
            s3_url_style_detection(&warehouse)?,
            &request_method,
            request_body.as_deref(),
        )?;

        let first_location = parsed_url.locations.first().ok_or_else(|| {
            ErrorModel::internal(
                "Request URI does not contain a location",
                "UriNoLocation",
                None,
            )
        })?;

        let resolved = match target {
            SignTarget::TabularId(tabular_id) => {
                tracing::debug!(
                    "Got S3 sign request for tabular {tabular_id} with URL {request_url}"
                );
                resolve_signable_by_id(
                    warehouse_id,
                    tabular_id,
                    &parsed_url,
                    first_location,
                    &state,
                )
                .await
            }
            SignTarget::Table(ident) => {
                tracing::debug!("Got S3 sign request for table {ident:?} with URL {request_url}");
                resolve_signable_by_name(warehouse_id, *ident, &parsed_url, first_location, &state)
                    .await
            }
            SignTarget::FromRequestUri => {
                tracing::debug!(
                    "Got S3 sign request for URL {request_url} without tabular id. Searching for tabular by location"
                );
                resolve_signable_by_location(warehouse_id, first_location, &state)
                    .await
                    .map_err(RequireTableActionError::from)
            }
        };
        // Can't fail here before AuthZ!

        let (location, table_id) = match ResolvedSignable::from(resolved) {
            ResolvedSignable::GenericTable(gt_info) => {
                let action = match operation {
                    Operation::Read => CatalogGenericTableAction::ReadData,
                    Operation::Write | Operation::Delete => CatalogGenericTableAction::WriteData,
                };
                let event_ctx = APIEventContext::for_generic_table(
                    request_metadata,
                    state.v1_state.events,
                    warehouse_id,
                    gt_info.tabular_ident.clone(),
                    action,
                );
                let authz_result = authorize_generic_table_action_for_sign::<C, _>(
                    &warehouse,
                    gt_info,
                    event_ctx.request_metadata(),
                    event_ctx.action().clone(),
                    &authorizer,
                    state.v1_state.catalog.clone(),
                )
                .await;
                let (_event_ctx, gt_info) = event_ctx.emit_authz(authz_result)?;
                (gt_info.location, gt_info.tabular_id.to_string())
            }
            ResolvedSignable::Iceberg(table_info) => {
                let action = match operation {
                    Operation::Read => CatalogTableAction::ReadData,
                    Operation::Write | Operation::Delete => CatalogTableAction::WriteData,
                };
                let event_ctx = APIEventContext::for_table_location(
                    request_metadata,
                    state.v1_state.events,
                    warehouse_id,
                    Arc::new(first_location.clone()),
                    action,
                );
                let authz_result = authorize_table_action_for_sign::<C, _>(
                    &warehouse,
                    table_info,
                    event_ctx.user_provided_entity().table_location.clone(),
                    event_ctx.request_metadata(),
                    event_ctx.action().clone(),
                    &authorizer,
                    state.v1_state.catalog.clone(),
                )
                .await;
                let (_event_ctx, table_info) = event_ctx.emit_authz(authz_result)?;
                (table_info.location, table_info.tabular_id.to_string())
            }
        };

        let extend_err = |mut e: IcebergErrorResponse| {
            e.error = e
                .error
                .append_detail(format!("Tabular ID: {table_id}"))
                .append_detail(format!("Request URI: {request_url}"))
                .append_detail(format!("Request Region: {request_region}"))
                .append_detail(format!("Tabular Location: {location}"));
            e
        };

        let storage_profile = warehouse
            .storage_profile
            .clone()
            .try_into_s3()
            .map_err(|e| extend_err(IcebergErrorResponse::from(e)))?;

        validate_region(&request_region, &storage_profile).map_err(extend_err)?;
        validate_uri(&parsed_url, &location).map_err(extend_err)?;

        // If all is good, we need the storage secret
        let storage_secret = if let Some(storage_secret_id) = warehouse.storage_secret_id {
            Some(
                state
                    .v1_state
                    .secrets
                    .require_storage_secret_by_id(storage_secret_id)
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

fn s3_url_style_detection(
    warehouse: &ResolvedWarehouse,
) -> Result<S3UrlStyleDetectionMode, IcebergErrorResponse> {
    let storage_profile = &warehouse.storage_profile;
    if let StorageProfile::S3(s3_profile) = storage_profile {
        return Ok(s3_profile.remote_signing_url_style);
    }

    Err(IcebergErrorResponse::from(ErrorModel::bad_request(
        "Warehouse storage profile is not an S3 profile",
        "InvalidWarehouse",
        None,
    )))
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
    let identity = storage_profile.get_signing_identity(credentials).await?;
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
        if UNSIGNED_HEADERS.contains(&key.as_str()) {
            // Skip unsigned headers
            continue;
        }
        for value in values {
            headers_vec.push((key.clone(), value));
        }
    }

    let signable_request = SignableRequest::new(
        request_method.as_str(),
        request_url.to_string(),
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

    let mut output_uri = request_url.clone();
    for (key, value) in signing_instructions.params() {
        output_uri.query_pairs_mut().append_pair(key, value);
    }

    let mut output_headers = request_headers;
    for (key, value) in signing_instructions.headers() {
        output_headers.insert(key.to_string(), vec![value.to_string()]);
    }

    output_headers.insert(
        CACHE_CONTROL_HEADER.to_string(),
        vec![if CACHEABLE_METHODS.contains(request_method) {
            CACHE_CONTROL_PRIVATE.to_string()
        } else {
            CACHE_CONTROL_NO_CACHE.to_string()
        }],
    );

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
        new_path_segments.push(urlencoding::decode(segment).map_err(|e| {
            ErrorModel::bad_request(
                "Failed to decode URI segment",
                "FailedToDecodeURISegment",
                Some(Box::new(e)),
            )
        })?);
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

/// Resolve the tabular addressed by the table-scoped signer route
/// (`/v1/signer/{warehouse-id}/tabular-id/{uuid}/v1/aws/s3/sign`). That route carries a
/// bare UUID, so the id is resolved across all tabular types.
async fn resolve_signable_by_id<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    warehouse_id: WarehouseId,
    tabular_id: uuid::Uuid,
    parsed_url: &s3_utils::ParsedSignRequest,
    first_location: &S3Location,
    state: &ApiContext<State<A, C, S>>,
) -> std::result::Result<Option<SignableTabular>, RequireTableActionError> {
    let info_by_id = C::get_tabular_info_by_uuid(
        warehouse_id,
        tabular_id,
        // we were able to resolve the tabular to an id so we know it is not deleted
        TabularListFlags::active_and_staged(),
        state.v1_state.catalog.clone(),
    )
    .await
    .map_err(RequireTableActionError::from)?;

    if let Some(signable) = info_by_id.and_then(SignableTabular::from_view_or_table) {
        if validate_uri(parsed_url, signable.location()).is_ok() {
            return Ok(Some(signable));
        }

        // The id resolved, but its location does not cover the request URI.
        // Up to version 0.9.1 pyiceberg had a bug that did not allow table specific signer URIs.
        // Instead the first URI of the first sign call would be used for subsequent calls in the same runtime too.
        // This is fixed in 0.9.2 onward: https://github.com/apache/iceberg-python/pull/2005
        // To keep backward compatibility we fall back to location based lookup when the location does not match.
        // This fallback will be removed in a future version of Lakekeeper.
        tracing::warn!(
            "Received a tabular specific sign request for tabular {tabular_id} with a location {} that does not match the request URI {}. Falling back to location based lookup. This is a bug in the query engine. When using PyIceberg, please update to versions > 0.9.1",
            signable.location(),
            parsed_url.uri.received()
        );
    }

    resolve_signable_by_location(warehouse_id, first_location, state)
        .await
        .map_err(RequireTableActionError::from)
}

/// Resolve the table the spec's per-table `/sign` route names.
///
/// Funnels into [`resolve_signable_by_id`] so the name path inherits its
/// location cross-check and its fallback, rather than growing a second set.
async fn resolve_signable_by_name<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    warehouse_id: WarehouseId,
    table: TableIdent,
    parsed_url: &s3_utils::ParsedSignRequest,
    first_location: &S3Location,
    state: &ApiContext<State<A, C, S>>,
) -> std::result::Result<Option<SignableTabular>, RequireTableActionError> {
    let table_info = C::get_table_info(
        warehouse_id,
        table,
        TabularListFlags::active_and_staged(),
        state.v1_state.catalog.clone(),
    )
    .await
    .map_err(RequireTableActionError::from)?;

    if let Some(table_info) = table_info {
        return resolve_signable_by_id(
            warehouse_id,
            *table_info.table_id(),
            parsed_url,
            first_location,
            state,
        )
        .await;
    }

    resolve_signable_by_location(warehouse_id, first_location, state)
        .await
        .map_err(RequireTableActionError::from)
}

async fn resolve_signable_by_location<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    warehouse_id: WarehouseId,
    first_location: &S3Location,
    state: &ApiContext<State<A, C, S>>,
) -> std::result::Result<Option<SignableTabular>, GetTabularInfoByLocationError> {
    C::get_tabular_infos_by_s3_location(
        warehouse_id,
        first_location.location(),
        // spark iceberg drops the table and then checks for existence of metadata files
        // which in turn needs to sign HEAD requests for files reachable from the
        // dropped table.
        TabularListFlags::all(),
        state.v1_state.catalog.clone(),
    )
    .await
    .map(|opt| opt.and_then(SignableTabular::from_view_or_table))
}

async fn authorize_table_action_for_sign<C: CatalogStore, A: Authorizer + Clone>(
    warehouse: &ResolvedWarehouse,
    table_info: Result<Option<TableInfo>, RequireTableActionError>,
    table_location: Arc<S3Location>,
    request_metadata: &RequestMetadata,
    action: CatalogTableAction,
    authorizer: &A,
    catalog_state: C::State,
) -> Result<TableInfo, AuthZError> {
    let warehouse_id = warehouse.warehouse_id;
    let table_info = table_info?;

    let Some(table_info) = table_info else {
        return Err(
            AuthZCannotSeeTableLocation::new_not_found(warehouse_id, table_location).into(),
        );
    };

    // First check - fail fast if requested table is not allowed.
    // We also need to check later if the path matches the table location.
    let namespace_hierarchy = C::get_namespace(
        warehouse_id,
        table_info.table_ident().namespace.clone(),
        catalog_state,
    )
    .await;
    let namespace_hierarchy = authorizer.require_namespace_presence(
        warehouse_id,
        table_info.table_ident().namespace.clone(),
        namespace_hierarchy,
    )?;
    let table_info = authorizer
        .require_table_action(
            request_metadata,
            warehouse,
            &namespace_hierarchy,
            table_info.table_ident().clone(),
            Ok::<_, RequireTableActionError>(Some(table_info)),
            action,
        )
        .await?;

    Ok(table_info)
}

async fn authorize_generic_table_action_for_sign<C: CatalogStore, A: Authorizer + Clone>(
    warehouse: &ResolvedWarehouse,
    gt_info: GenericTabularInfo,
    request_metadata: &RequestMetadata,
    action: CatalogGenericTableAction,
    authorizer: &A,
    catalog_state: C::State,
) -> Result<GenericTabularInfo, AuthZError> {
    let warehouse_id = warehouse.warehouse_id;
    let namespace = gt_info.tabular_ident.namespace.clone();
    let ident = gt_info.tabular_ident.clone();

    // Fail fast if the namespace is not visible.
    let namespace_hierarchy =
        C::get_namespace(warehouse_id, namespace.clone(), catalog_state).await;
    let namespace_hierarchy =
        authorizer.require_namespace_presence(warehouse_id, namespace, namespace_hierarchy)?;

    let gt_info = authorizer
        .require_generic_table_action(
            request_metadata,
            warehouse,
            &namespace_hierarchy,
            ident,
            Ok::<_, RequireGenericTableActionError>(Some(gt_info)),
            action,
        )
        .await?;

    Ok(gt_info)
}

fn validate_uri(
    // i.e. https://bucket.s3.region.amazonaws.com/key
    parsed_url: &s3_utils::ParsedSignRequest,
    // i.e. s3://bucket/key
    table_location: &Location,
) -> Result<()> {
    let table_location = S3Location::try_from_location(table_location, true)
        .map_err(|e| ValidationError::from(e.with_context("Error signing request")))?;

    let normalized_table_location = if table_location.scheme() == "s3" {
        None
    } else {
        Some(table_location.clone().set_s3_scheme())
    };

    for url_location in &parsed_url.locations {
        let is_within = |table_location: &S3Location| {
            // S3 matches list prefixes as raw strings, so a prefix that stops at the table
            // location would also return the keys of same-prefixed siblings.
            if parsed_url.locations_are_list_prefixes {
                url_location
                    .location()
                    .is_prefix_within(table_location.location())
            } else {
                url_location
                    .location()
                    .is_sublocation_of(table_location.location())
            }
        };

        if !(is_within(&table_location)
            || normalized_table_location.as_ref().is_some_and(is_within))
        {
            return Err(SignError::RequestUriMismatch {
                request_uri: parsed_url.uri.received().to_string(),
                expected_location: table_location.to_string(),
                actual_location: url_location.to_string(),
            }
            .into());
        }
    }

    Ok(())
}

pub(super) mod s3_utils {
    use lakekeeper_io::s3::S3Location;
    use lazy_regex::regex;
    use percent_encoding::{AsciiSet, utf8_percent_encode};
    use serde::{Deserialize, Serialize};

    use super::{ErrorModel, Operation, Result};
    use crate::service::storage::{ValidationError, s3::S3UrlStyleDetectionMode};

    /// Query parameter that identifies a `ListObjectsV2` request, and its only valid value.
    const LIST_TYPE_QUERY_PARAM: &str = "list-type";
    const LIST_TYPE_V2: &str = "2";
    /// Query parameter carrying the key prefix a list request is scoped to.
    const PREFIX_QUERY_PARAM: &str = "prefix";

    /// The url path percent-encode set, which is what a `Location` built from an object key
    /// carries. `/` is not part of it - it separates segments in both representations. `\` is
    /// not either, and there the two representations genuinely disagree: `url` treats it as a
    /// second separator for http but not for `s3`, so an object key loses it while a list
    /// prefix keeps the byte S3 matches against. Keeping the byte is the sound half.
    const LIST_PREFIX_ENCODE_SET: &AsciiSet = &AsciiSet::EMPTY
        .add(b' ')
        .add(b'"')
        .add(b'#')
        .add(b'<')
        .add(b'>')
        .add(b'?')
        .add(b'`')
        .add(b'{')
        .add(b'}');

    /// The URI of a request to sign, both as received and with its path segments url-decoded.
    ///
    /// The signature covers the request as received, so anything that constrains the *shape*
    /// of the request has to look at [`Self::received`]. Locations are derived from
    /// [`Self::decoded`], because clients url-encode object keys. Decoding can change what a
    /// path addresses - a `%2F` hides separators from the url parser, which then collapses the
    /// `.`/`..` behind them - so the two are kept together rather than passed around as two
    /// look-alike arguments.
    #[derive(Debug, Clone)]
    pub(super) struct SignRequestUri {
        received: url::Url,
        decoded: url::Url,
    }

    impl SignRequestUri {
        pub(super) fn new(received: url::Url) -> Result<Self> {
            let decoded = super::urldecode_uri_path_segments(&received)?;
            Ok(Self { received, decoded })
        }

        pub(super) fn received(&self) -> &url::Url {
            &self.received
        }

        pub(super) fn decoded(&self) -> &url::Url {
            &self.decoded
        }

        /// `false` if url-decoding changed the path, i.e. the locations derived from
        /// [`Self::decoded`] may not describe what the signed request addresses.
        fn path_survived_decoding(&self) -> bool {
            self.received.path() == self.decoded.path()
        }
    }

    #[derive(Debug, Clone)]
    pub(super) struct ParsedSignRequest {
        pub(super) uri: SignRequestUri,
        pub(super) locations: Vec<S3Location>,
        /// `true` if `locations` are S3 list prefixes rather than object keys. S3 matches
        /// list prefixes by raw string, so they require a stricter containment check.
        pub(super) locations_are_list_prefixes: bool,
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

    /// Determine the locations a request touches.
    pub(super) fn parse_s3_url(
        uri: &SignRequestUri,
        s3_url_style_detection: S3UrlStyleDetectionMode,
        method: &http::Method,
        body: Option<&str>,
    ) -> Result<(ParsedSignRequest, Operation)> {
        let err = |t: &str, m: &str| ErrorModel::bad_request(m, t, None);

        // Require https or http
        if !matches!(uri.received().scheme(), "https" | "http") {
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
                if method == http::Method::POST
                    && uri.received().query().is_some_and(|q| q.contains("delete"))
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

        // `DeleteObjects` and `ListObjectsV2` address the bucket instead of a single object.
        // The keys that identify the table live in the request body resp. the query string.
        let is_list_operation = *method == http::Method::GET && is_list_objects_v2(uri.received());
        let allow_no_key = is_post_delete_operation || is_list_operation;

        // Parse the base URL
        let mut parsed_request = match s3_url_style_detection {
            S3UrlStyleDetectionMode::VirtualHost => virtual_host_style(uri, allow_no_key, true)?,
            S3UrlStyleDetectionMode::Path => path_style(uri, allow_no_key)?,
            S3UrlStyleDetectionMode::Auto => {
                if let Ok(parsed) = virtual_host_style(uri, allow_no_key, false) {
                    parsed
                } else if let Ok(parsed) = path_style(uri, allow_no_key) {
                    parsed
                } else {
                    return Err(err("UriNotS3", "URI does not match S3 host or path style").into());
                }
            }
        };

        // For DeleteObjects operation, parse the XML body for object keys
        if is_post_delete_operation {
            if let Some(xml_body) = body {
                let bucket = bucket_of(&parsed_request)?;
                require_bucket_addressed(&parsed_request, &bucket)?;

                // Parse XML body to get deletion keys
                let keys = parse_s3_delete_xml(xml_body)
                    .map_err(|e| err("InvalidDeleteBody", &format!("{e}")))?;

                // Create S3 locations for each key
                let mut locations = Vec::with_capacity(keys.len());
                for key in keys {
                    let location =
                        S3Location::new(&bucket, &key.split('/').collect::<Vec<_>>(), None)
                            .map_err(ValidationError::from)?;
                    locations.push(location);
                }

                // Replace the locations in the parsed request
                parsed_request.locations = locations;
            } else {
                return Err(err("DeleteWithoutBody", "Delete requests require a body").into());
            }
        } else if is_list_operation {
            // The URI path is just the bucket - the table is identified by the `prefix`.
            let bucket = bucket_of(&parsed_request)?;
            require_bucket_addressed(&parsed_request, &bucket)?;
            require_known_list_parameters(uri.received())?;
            parsed_request.locations = vec![list_prefix_location(uri.received(), &bucket)?];
            parsed_request.locations_are_list_prefixes = true;
        }

        Ok((parsed_request, operation))
    }

    /// `GET /{bucket}?list-type=2` - the `ListObjectsV2` API. Other bucket-level `GET`
    /// sub-resources (`?versions`, `?uploads`, `?location`) stay unsignable: they carry no
    /// prefix that could be authorized against a table location.
    fn is_list_objects_v2(uri: &url::Url) -> bool {
        uri.query_pairs()
            .any(|(key, value)| key == LIST_TYPE_QUERY_PARAM && value == LIST_TYPE_V2)
    }

    /// Requests whose locations are taken from the body or the query string must address the
    /// bucket itself. S3 dispatches on the path: a key in the path would turn the signed
    /// request into an object operation that ignores the parameters this authorization is
    /// based on.
    fn require_bucket_addressed(parsed_request: &ParsedSignRequest, bucket: &str) -> Result<()> {
        // The location the path resolves to is the bucket itself, in either url style ...
        let addresses_bucket = parsed_request.locations.first().is_some_and(|location| {
            location.location().as_str().trim_end_matches('/') == format!("s3://{bucket}")
        });

        // ... and url-decoding did not change the path, which would mean the bytes that get
        // signed address something else than what was just checked: `%2F` hides separators
        // from the url parser, which then collapses the `.`/`..` behind them.
        let path_survived_decoding = parsed_request.uri.path_survived_decoding();

        if !(addresses_bucket && path_survived_decoding) {
            return Err(ErrorModel::bad_request(
                "Bucket-level requests must not address an object",
                "UriNotBucket",
                None,
            )
            .into());
        }

        Ok(())
    }

    /// Parameters of the `ListObjectsV2` API, plus the `x-id` telemetry parameter that some
    /// AWS SDKs append. None of them widens the set of keys `prefix` selects.
    const LIST_QUERY_PARAMS: &[&str] = &[
        LIST_TYPE_QUERY_PARAM,
        PREFIX_QUERY_PARAM,
        "continuation-token",
        "delimiter",
        "encoding-type",
        "fetch-owner",
        "max-keys",
        "start-after",
        "x-id",
    ];

    /// Rejects list requests that carry any other parameter, or one of them twice.
    ///
    /// S3 dispatches on the query string, so a bucket sub-resource (`?policy`, `?versioning`,
    /// `?uploads`, …) alongside the list parameters would make the signed request return
    /// something else entirely - something no table location can authorize. Which of two
    /// values for the same parameter S3 applies is implementation defined, so only one of
    /// them could be authorized.
    fn require_known_list_parameters(uri: &url::Url) -> Result<()> {
        let mut seen: Vec<std::borrow::Cow<'_, str>> = Vec::new();

        for (key, _) in uri.query_pairs() {
            if !LIST_QUERY_PARAMS.contains(&key.as_ref()) {
                return Err(ErrorModel::bad_request(
                    format!("Unsupported query parameter for a list request: `{key}`"),
                    "UnsupportedListParameter",
                    None,
                )
                .into());
            }

            if seen.contains(&key) {
                return Err(ErrorModel::bad_request(
                    format!("Repeated query parameter in a list request: `{key}`"),
                    "RepeatedListParameter",
                    None,
                )
                .into());
            }

            seen.push(key);
        }

        Ok(())
    }

    fn bucket_of(parsed_request: &ParsedSignRequest) -> Result<String> {
        Ok(parsed_request
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
            .to_string())
    }

    /// The location a `ListObjectsV2` request is scoped to: `s3://{bucket}/{prefix}`.
    ///
    /// Sigv4 covers the canonical query string, so the client cannot widen the `prefix`
    /// after signing. Parsing the location from the concatenated string rather than from
    /// segments is what makes the check trustworthy: the authorized location is the exact
    /// string S3 matches keys against, and prefixes that a `Location` would not round-trip
    /// (empty segments, `.`/`..`, unsafe characters) are rejected instead of normalized.
    fn list_prefix_location(uri: &url::Url, bucket: &str) -> Result<S3Location> {
        let prefix = uri
            .query_pairs()
            .find(|(key, _)| key == PREFIX_QUERY_PARAM)
            .map(|(_, value)| value)
            .filter(|prefix| !prefix.is_empty())
            .ok_or_else(|| {
                ErrorModel::bad_request(
                    "List requests must be scoped to a prefix inside a table location",
                    "ListWithoutPrefix",
                    None,
                )
            })?;

        // `query_pairs` decodes lossily - a byte sequence that is not valid utf-8 becomes
        // U+FFFD. The location built from it would describe a different key range than the
        // one S3 matches against the bytes that get signed, so such a prefix is refused. A
        // literal U+FFFD is refused with it, because the two are indistinguishable here -
        // stricter than the object key path, which errors on malformed input but accepts the
        // literal character.
        if prefix.contains(char::REPLACEMENT_CHARACTER) {
            return Err(ErrorModel::bad_request(
                "List prefix is not valid utf-8",
                "InvalidListPrefix",
                None,
            )
            .into());
        }

        // The prefix arrives url-decoded, so it has to be encoded again to become a location.
        // Object keys reach their location through `urldecode_uri_path_segments`, which
        // re-encodes with the url path percent-encode set, so the same key has to end up as
        // the same string here - otherwise a table whose location contains one of these
        // characters can be read file by file but never listed. `%` and control characters
        // are deliberately not encoded: the parser below must keep rejecting a prefix that
        // does not round-trip - an empty segment, `.`/`..`, a control character - instead of
        // it disappearing behind an encoding of ours.
        let prefix = utf8_percent_encode(&prefix, LIST_PREFIX_ENCODE_SET);

        S3Location::try_from_str(&format!("s3://{bucket}/{prefix}"), false).map_err(|e| {
            ErrorModel::bad_request(
                format!("Invalid list prefix: {e}"),
                "InvalidListPrefix",
                Some(Box::new(e)),
            )
            .into()
        })
    }

    fn virtual_host_style(
        uri: &SignRequestUri,
        allow_no_key: bool,
        is_known: bool,
    ) -> Result<ParsedSignRequest> {
        let host = uri.decoded().host().ok_or_else(|| {
            ErrorModel::bad_request("URI to sign does not have a host", "UriNoHost", None)
        })?;
        let path_segments = get_path_segments(uri.decoded(), allow_no_key)?;
        let port = uri.decoded().port_or_known_default().unwrap_or(443);

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
            uri: uri.clone(),
            locations: vec![
                S3Location::new(
                    bucket,
                    &path_segments.iter().map(String::as_str).collect::<Vec<_>>(),
                    None,
                )
                .map_err(ValidationError::from)?,
            ],
            locations_are_list_prefixes: false,
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

    fn path_style(uri: &SignRequestUri, allow_no_key: bool) -> Result<ParsedSignRequest> {
        let path_segments = get_path_segments(uri.decoded(), allow_no_key)?;

        let min_path_segments = if allow_no_key { 1 } else { 2 };

        if path_segments.len() < min_path_segments {
            return Err(ErrorModel::bad_request(
                format!("Path style uri needs at least {min_path_segments} path segments"),
                "UriNotS3",
                None,
            )
            .into());
        }

        let path_segments_borrowed: Vec<&str> = path_segments.iter().map(String::as_str).collect();

        Ok(ParsedSignRequest {
            uri: uri.clone(),
            locations: vec![
                S3Location::new(
                    path_segments_borrowed[0],
                    if path_segments_borrowed.len() > 1 {
                        &(path_segments_borrowed[1..])
                    } else {
                        &[]
                    },
                    None,
                )
                .map_err(ValidationError::from)?,
            ],
            locations_are_list_prefixes: false,
            endpoint: uri
                .decoded()
                .host_str()
                .ok_or_else(|| {
                    ErrorModel::bad_request("URI to sign does not have a host", "UriNoHost", None)
                })?
                .to_string(),
            port: uri.decoded().port_or_known_default().unwrap_or(443),
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

    use super::s3_utils::{DeleteObjectsRequest, parse_s3_delete_xml};

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
        assert!(
            keys.contains(
                &"initial-warehouse/01963de0-99d9-79e2-8e95-24b11d0d334c/metadata/file1.avro"
                    .to_string()
            )
        );
        assert!(
            keys.contains(
                &"initial-warehouse/01963de0-99d9-79e2-8e95-24b11d0d334c/metadata/file2.avro"
                    .to_string()
            )
        );
        assert!(
            keys.contains(
                &"initial-warehouse/01963de0-99d9-79e2-8e95-24b11d0d334c/metadata/file3.avro"
                    .to_string()
            )
        );
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
    use std::str::FromStr as _;

    use itertools::Itertools as _;

    use super::*;
    use crate::service::storage::S3Flavor;

    #[derive(Debug)]
    struct TC {
        request_uri: &'static str,
        table_location: &'static str,
        #[allow(dead_code)]
        endpoint: Option<&'static str>,
        expected_outcome: bool,
    }

    fn parse_uri(
        uri: &url::Url,
        mode: S3UrlStyleDetectionMode,
        method: &http::Method,
        body: Option<&str>,
    ) -> Result<(s3_utils::ParsedSignRequest, Operation)> {
        s3_utils::parse_s3_url(
            &s3_utils::SignRequestUri::new(uri.clone())?,
            mode,
            method,
            body,
        )
    }

    fn run_validate_uri_test(test_case: &TC) {
        let request_uri = url::Url::parse(test_case.request_uri).unwrap();
        let (request_uri, _operation) = parse_uri(
            &request_uri,
            S3UrlStyleDetectionMode::Auto,
            &http::Method::GET,
            None,
        )
        .unwrap();
        let table_location = Location::from_str(test_case.table_location).unwrap();
        let result = validate_uri(&request_uri, &table_location);
        assert_eq!(
            result.is_ok(),
            test_case.expected_outcome,
            "Test case: {test_case:?}",
        );
    }

    #[test]
    fn test_parse_s3_url_config_path_style() {
        let (parsed, _operation) = parse_uri(
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
        let (parsed, _operation) = parse_uri(
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
        let (parsed, _operation) = parse_uri(
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
            let (parsed, _operation) = parse_uri(
                &uri,
                S3UrlStyleDetectionMode::Auto,
                &http::Method::GET,
                None,
            )
            .unwrap_or_else(|_| panic!("Failed to parse {uri}"));
            let result = parsed.locations[0].to_string();
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
                vec!["s3://examples/a/b/c.parquet", "s3://examples/a/b/d.parquet"],
            ),
            (
                "http://examples.s3.my-host:9000?delete",
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Object><Key>a/b/c.parquet</Key></Object><Object><Key>a/b/d.parquet</Key></Object></Delete>",
                vec!["s3://examples/a/b/c.parquet", "s3://examples/a/b/d.parquet"],
            ),
        ];

        for (uri, body, expected) in cases {
            let uri = url::Url::parse(uri).unwrap();
            let (parsed, operation) = parse_uri(
                &uri,
                S3UrlStyleDetectionMode::Auto,
                &http::Method::POST,
                Some(body),
            )
            .unwrap_or_else(|e| panic!("Failed to parse {uri}: {e:?}"));

            let result = parsed
                .locations
                .iter()
                .map(ToString::to_string)
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
                request_uri: "https://bucket.s3.my-region.amazonaws.com/key/with-special-chars%20/foo",
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
                request_uri: "https://s3.my-region.amazonaws.com/bucket/key/with-special-chars%20/foo",
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

    fn parse_list(uri: &str, mode: S3UrlStyleDetectionMode) -> s3_utils::ParsedSignRequest {
        let (parsed, operation) = parse_uri(
            &url::Url::parse(uri).unwrap(),
            mode,
            &http::Method::GET,
            None,
        )
        .unwrap_or_else(|e| panic!("Failed to parse {uri}: {e:?}"));
        // Listing reveals the keys under a prefix, which is a read.
        assert_eq!(operation, Operation::Read);
        assert!(parsed.locations_are_list_prefixes);
        parsed
    }

    fn parse_list_err(uri: &str, mode: S3UrlStyleDetectionMode) -> String {
        parse_uri(
            &url::Url::parse(uri).unwrap(),
            mode,
            &http::Method::GET,
            None,
        )
        .map(|(parsed, _)| parsed)
        .expect_err(&format!("{uri} must not be signable"))
        .error
        .r#type
    }

    /// `ListObjectsV2` addresses the bucket, so the location that identifies the table
    /// comes from the `prefix` query parameter, in both URL styles.
    #[test]
    fn test_parse_s3_url_list_objects_v2() {
        let cases = vec![
            // Path style, no key in the path at all.
            (
                "http://s3.example.com:8333/bucket?list-type=2&prefix=ns/tbl/",
                S3UrlStyleDetectionMode::Auto,
            ),
            (
                "http://s3.example.com:8333/bucket?list-type=2&prefix=ns/tbl/",
                S3UrlStyleDetectionMode::Path,
            ),
            // Path style with the trailing slash some SDKs append to the bucket.
            (
                "http://s3.example.com:8333/bucket/?list-type=2&prefix=ns/tbl/",
                S3UrlStyleDetectionMode::Auto,
            ),
            // Virtual host style.
            (
                "https://bucket.s3.my-region.amazonaws.com/?list-type=2&prefix=ns/tbl/",
                S3UrlStyleDetectionMode::Auto,
            ),
            (
                "https://bucket.s3.my-region.amazonaws.com?list-type=2&prefix=ns/tbl/",
                S3UrlStyleDetectionMode::VirtualHost,
            ),
            // The prefix is url-encoded by the AWS SDKs.
            (
                "http://s3.example.com:8333/bucket?list-type=2&prefix=ns%2Ftbl%2F",
                S3UrlStyleDetectionMode::Auto,
            ),
            // Trailing separators collapse. S3 then matches a subset of the authorized
            // directory, so this is signable rather than rejected.
            (
                "http://s3.example.com:8333/bucket?list-type=2&prefix=ns/tbl//",
                S3UrlStyleDetectionMode::Auto,
            ),
            // Pagination and other list parameters only narrow the result set.
            (
                "https://bucket.s3.my-region.amazonaws.com/?list-type=2&prefix=ns/tbl/&continuation-token=abc&max-keys=1000&encoding-type=url",
                S3UrlStyleDetectionMode::Auto,
            ),
        ];

        for (uri, mode) in cases {
            let parsed = parse_list(uri, mode);
            assert_eq!(
                parsed
                    .locations
                    .iter()
                    .map(ToString::to_string)
                    .collect_vec(),
                vec!["s3://bucket/ns/tbl/"],
                "Test case: {uri}"
            );
        }
    }

    /// Without a prefix the request would list the whole bucket, which no table can
    /// authorize.
    #[test]
    fn test_parse_s3_url_list_requires_prefix() {
        for uri in [
            "http://s3.example.com:8333/bucket?list-type=2",
            "http://s3.example.com:8333/bucket?list-type=2&prefix=",
            "https://bucket.s3.my-region.amazonaws.com/?list-type=2",
        ] {
            assert_eq!(
                parse_list_err(uri, S3UrlStyleDetectionMode::Auto),
                "ListWithoutPrefix",
                "Test case: {uri}"
            );
        }
    }

    /// A key must reach the same location whether it arrives as an object key in the path or
    /// as a list prefix in the query. Otherwise a table whose location contains the character
    /// can be read file by file but never listed - and for `#` and `?` the prefix would not
    /// even parse, because they start a fragment resp. a query string.
    ///
    /// Swept over every byte, so a change to either representation cannot pull them apart
    /// unnoticed. One of the two rejecting is fine - it just refuses to sign - so only the
    /// cases where both produce a location are compared. `\` is the one disagreement, see
    /// [`test_list_prefix_keeps_a_backslash_that_an_object_key_loses`].
    #[test]
    fn test_list_prefix_and_object_key_encode_alike() {
        let escapes = (0x00..=0xFF).map(|byte: u8| format!("%{byte:02X}")).chain([
            "%C3%A9".to_string(),
            "%2525".to_string(),
            "%2f".to_string(),
        ]);

        let mut compared = 0;

        for encoded in escapes {
            if encoded == "%5C" {
                continue;
            }

            let object = parse_uri(
                &url::Url::parse(&format!(
                    "http://s3.example.com:8333/bucket/ns/tbl{encoded}a/f.parquet"
                ))
                .unwrap(),
                S3UrlStyleDetectionMode::Auto,
                &http::Method::GET,
                None,
            );

            let list = parse_uri(
                &url::Url::parse(&format!(
                    "http://s3.example.com:8333/bucket?list-type=2&prefix=ns%2Ftbl{encoded}a%2F"
                ))
                .unwrap(),
                S3UrlStyleDetectionMode::Auto,
                &http::Method::GET,
                None,
            );

            if let (Ok((object, _)), Ok((list, _))) = (object, list) {
                assert_eq!(
                    object.locations[0].to_string(),
                    format!("{}f.parquet", list.locations[0]),
                    "object key and list prefix must encode `{encoded}` the same way"
                );
                compared += 1;
            }
        }

        // Both rejecting is the trivially passing case, so make sure the sweep did compare the
        // printable range rather than run empty.
        assert_eq!(compared, 97, "the sweep must not go vacuous");
    }

    /// `\` is the one character the two representations disagree on: `url` treats it as a
    /// second path separator for http, so the object key loses it, while the list prefix keeps
    /// the byte S3 matches keys against. Keeping it is the sound half - the authorized string
    /// is what gets listed - so this pins the list behaviour rather than the agreement.
    #[test]
    fn test_list_prefix_keeps_a_backslash_that_an_object_key_loses() {
        let list = parse_list(
            "http://s3.example.com:8333/bucket?list-type=2&prefix=ns%2Ftbl%5Ca%2F",
            S3UrlStyleDetectionMode::Auto,
        );
        assert_eq!(list.locations[0].to_string(), "s3://bucket/ns/tbl\\a/");

        let (object, _) = parse_uri(
            &url::Url::parse("http://s3.example.com:8333/bucket/ns/tbl%5Ca/f.parquet").unwrap(),
            S3UrlStyleDetectionMode::Auto,
            &http::Method::GET,
            None,
        )
        .unwrap();
        assert_eq!(
            object.locations[0].to_string(),
            "s3://bucket/ns/tbl/a/f.parquet"
        );
    }

    /// A prefix whose bytes are not valid utf-8 cannot be authorized: `query_pairs` decodes it
    /// lossily, so the location built from it describes a different key range than the one S3
    /// matches against the bytes that get signed.
    #[test]
    fn test_parse_s3_url_list_rejects_malformed_prefix_encoding() {
        for prefix in ["ns%2Ftbl%FF%FEa%2F", "ns%2Ftbl%C3%28a%2F", "ns%2Ftbl%80%2F"] {
            assert_eq!(
                parse_list_err(
                    &format!("http://s3.example.com:8333/bucket?list-type=2&prefix={prefix}"),
                    S3UrlStyleDetectionMode::Auto
                ),
                "InvalidListPrefix",
                "Test case: {prefix}"
            );
        }
    }

    /// A prefix that a location would not round-trip cannot be authorized: the string S3
    /// matches keys against would differ from the one that was checked. `ns//tbl/` is the
    /// case that matters - its keys lie outside the `ns/tbl/` directory it normalizes to.
    #[test]
    fn test_parse_s3_url_list_rejects_prefix_that_is_not_a_location() {
        for uri in [
            "http://s3.example.com:8333/bucket?list-type=2&prefix=ns//tbl/",
            "http://s3.example.com:8333/bucket?list-type=2&prefix=/ns/tbl/",
            "http://s3.example.com:8333/bucket?list-type=2&prefix=ns/tbl/../other/",
            "http://s3.example.com:8333/bucket?list-type=2&prefix=ns/tbl/%00/",
        ] {
            assert_eq!(
                parse_list_err(uri, S3UrlStyleDetectionMode::Auto),
                "InvalidListPrefix",
                "Test case: {uri}"
            );
        }
    }

    /// Which of two values for the same parameter S3 applies is implementation defined, so
    /// only one of them could be authorized.
    #[test]
    fn test_parse_s3_url_list_rejects_repeated_parameters() {
        for uri in [
            "http://s3.example.com:8333/bucket?list-type=2&prefix=ns/tbl/&prefix=other/",
            "http://s3.example.com:8333/bucket?list-type=2&list-type=2&prefix=ns/tbl/",
            "http://s3.example.com:8333/bucket?list-type=1&list-type=2&prefix=ns/tbl/",
        ] {
            assert_eq!(
                parse_list_err(uri, S3UrlStyleDetectionMode::Auto),
                "RepeatedListParameter",
                "Test case: {uri}"
            );
        }
    }

    /// Only `ListObjectsV2` gains the keyless path - all other bucket-level requests carry
    /// no prefix that could be authorized against a table location.
    #[test]
    fn test_parse_s3_url_other_bucket_requests_stay_unsignable() {
        for uri in [
            "http://s3.example.com:8333/bucket",
            "http://s3.example.com:8333/bucket?versions&prefix=ns/tbl/",
            "http://s3.example.com:8333/bucket?uploads&prefix=ns/tbl/",
            "http://s3.example.com:8333/bucket?location",
            // List Objects V1 is not implemented.
            "http://s3.example.com:8333/bucket?prefix=ns/tbl/",
        ] {
            assert_eq!(
                parse_list_err(uri, S3UrlStyleDetectionMode::Path),
                "UriNotS3",
                "Test case: {uri}"
            );
        }
    }

    /// The signed URI is the one that was received, and S3 dispatches on its path: a key in
    /// the path makes the signed request a `GetObject` for that key, which ignores the
    /// `prefix` this request was authorized against.
    #[test]
    fn test_parse_s3_url_list_must_address_the_bucket() {
        for (uri, mode) in [
            (
                "http://s3.example.com:8333/bucket/other-ns/other-tbl/metadata/v1.json?list-type=2&prefix=ns/tbl/",
                S3UrlStyleDetectionMode::Auto,
            ),
            (
                "http://s3.example.com:8333/bucket/other-ns/other-tbl/metadata/v1.json?list-type=2&prefix=ns/tbl/",
                S3UrlStyleDetectionMode::Path,
            ),
            (
                "https://bucket.s3.my-region.amazonaws.com/other-ns/other-tbl/metadata/v1.json?list-type=2&prefix=ns/tbl/",
                S3UrlStyleDetectionMode::Auto,
            ),
            (
                "https://bucket.s3.my-region.amazonaws.com/other-ns/other-tbl/metadata/v1.json?list-type=2&prefix=ns/tbl/",
                S3UrlStyleDetectionMode::VirtualHost,
            ),
            // Virtual-host style, where the key happens to be named like the bucket.
            (
                "https://bucket.s3.my-region.amazonaws.com/bucket?list-type=2&prefix=ns/tbl/",
                S3UrlStyleDetectionMode::VirtualHost,
            ),
        ] {
            assert_eq!(
                parse_list_err(uri, mode),
                "UriNotBucket",
                "Test case: {uri}"
            );
        }

        // The same holds for the bulk delete that takes its keys from the body.
        let body = "<Delete><Object><Key>ns/tbl/data/a.parquet</Key></Object></Delete>";
        let err = parse_uri(
            &url::Url::parse("http://s3.example.com:8333/bucket/other-ns/other-tbl/x.json?delete")
                .unwrap(),
            S3UrlStyleDetectionMode::Auto,
            &http::Method::POST,
            Some(body),
        )
        .map(|(parsed, _)| parsed)
        .expect_err("a bulk delete addressing an object must not be signable");
        assert_eq!(err.error.r#type, "UriNotBucket", "{err:?}");
    }

    /// Url-decoding the path reveals separators hidden in `%2F` and lets the url parser
    /// collapse the `.`/`..` behind them, so a path that addresses a key can decode to the
    /// bare bucket. The guard has to reject it, because the key is what gets signed.
    #[test]
    fn test_parse_s3_url_list_must_address_the_bucket_before_decoding() {
        let received = url::Url::parse(
            "http://s3.example.com:8333/bucket%2Fns%2Ftbl%2F%2E%2E%2F%2E%2E?list-type=2&prefix=ns/tbl/",
        )
        .unwrap();
        let uri = s3_utils::SignRequestUri::new(received).unwrap();
        assert_eq!(
            uri.decoded().path(),
            "/bucket/",
            "decoding must collapse to the bucket for this test to mean anything"
        );

        let err = s3_utils::parse_s3_url(
            &uri,
            S3UrlStyleDetectionMode::Auto,
            &http::Method::GET,
            None,
        )
        .map(|(parsed, _)| parsed)
        .expect_err("a path that only decodes to the bucket must not be signable");
        assert_eq!(err.error.r#type, "UriNotBucket", "{err:?}");
    }

    /// `+` in a query value is form-decoded to a space, by S3 as well as by the parameter
    /// reader here, so the prefix that is authorized is the one S3 matches keys against.
    #[test]
    fn test_parse_s3_url_list_prefix_plus_is_a_space() {
        let parsed = parse_list(
            "http://s3.example.com:8333/bucket?list-type=2&prefix=ns/tbl+dir/",
            S3UrlStyleDetectionMode::Auto,
        );
        assert_eq!(
            parsed
                .locations
                .iter()
                .map(ToString::to_string)
                .collect_vec(),
            vec!["s3://bucket/ns/tbl%20dir/"]
        );

        let parsed = parse_list(
            "http://s3.example.com:8333/bucket?list-type=2&prefix=ns/tbl%2Bdir/",
            S3UrlStyleDetectionMode::Auto,
        );
        assert_eq!(
            parsed
                .locations
                .iter()
                .map(ToString::to_string)
                .collect_vec(),
            vec!["s3://bucket/ns/tbl+dir/"]
        );
    }

    /// S3 dispatches on the query string too: a bucket sub-resource next to the list
    /// parameters returns something else entirely, which no table location authorizes.
    #[test]
    fn test_parse_s3_url_list_rejects_unknown_parameters() {
        for uri in [
            "http://s3.example.com:8333/bucket?list-type=2&prefix=ns/tbl/&policy",
            "http://s3.example.com:8333/bucket?list-type=2&prefix=ns/tbl/&acl",
            "http://s3.example.com:8333/bucket?list-type=2&prefix=ns/tbl/&versions",
            "http://s3.example.com:8333/bucket?list-type=2&prefix=ns/tbl/&uploads",
            "http://s3.example.com:8333/bucket?list-type=2&prefix=ns/tbl/&location",
        ] {
            assert_eq!(
                parse_list_err(uri, S3UrlStyleDetectionMode::Auto),
                "UnsupportedListParameter",
                "Test case: {uri}"
            );
        }
    }

    /// A list prefix must reach past the table's own path separator: S3 matches prefixes as
    /// raw strings, so the bare table location also returns keys of same-prefixed siblings.
    #[test]
    fn test_validate_uri_list_prefix() {
        let cases = vec![
            // The directory listing Iceberg's `FileSystemWalker` issues.
            ("ns/tbl/", "s3://bucket/ns/tbl", true),
            // Subdirectories of the table.
            ("ns/tbl/data/", "s3://bucket/ns/tbl", true),
            ("ns/tbl/metadata", "s3://bucket/ns/tbl", true),
            // Table locations may be stored with a trailing slash.
            ("ns/tbl/", "s3://bucket/ns/tbl/", true),
            // A table whose location holds an encoded `#`, addressed by the decoded prefix
            // the client sends.
            ("ns/tbl%23a/", "s3://bucket/ns/tbl%23a", true),
            ("ns/tbl%23a/", "s3://bucket/ns/tbl", false),
            // `s3a` and `s3n` table locations are normalized to `s3`.
            ("ns/tbl/", "s3a://bucket/ns/tbl", true),
            ("ns/tbl/", "s3n://bucket/ns/tbl", true),
            // The bare table location matches siblings like `s3://bucket/ns/tbl_secret/…`.
            ("ns/tbl", "s3://bucket/ns/tbl", false),
            // Siblings, whether or not they share a string prefix with the table.
            ("ns/tbl_secret/", "s3://bucket/ns/tbl", false),
            ("ns/other/", "s3://bucket/ns/tbl", false),
            // Parents of the table.
            ("ns/", "s3://bucket/ns/tbl", false),
            // Another bucket.
            ("ns/tbl/", "s3://other-bucket/ns/tbl", false),
        ];

        for (prefix, table_location, expected_outcome) in cases {
            let parsed = parse_list(
                &format!("http://s3.example.com:8333/bucket?list-type=2&prefix={prefix}"),
                S3UrlStyleDetectionMode::Auto,
            );
            let result = validate_uri(&parsed, &Location::from_str(table_location).unwrap());
            assert_eq!(
                result.is_ok(),
                expected_outcome,
                "prefix `{prefix}` against table location `{table_location}`: {result:?}"
            );
        }
    }

    #[test]
    fn test_uri_bucket_missing() {
        parse_uri(
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
