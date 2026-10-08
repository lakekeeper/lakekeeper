use std::{collections::HashMap, sync::Arc, time::SystemTime, vec};

use aws_sigv4::{
    http_request::{SignableBody, SignableRequest, SigningSettings, sign as aws_sign},
    sign::v4,
    {self},
};
use lakekeeper_io::{Location, s3::S3Location};

use super::{super::CatalogServer, error::SignError, policy};
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
        storage::{S3Credential, S3Profile, StorageProfile, ValidationError},
    },
};

const UNSIGNED_HEADERS: &[&str] = &[
    "range",
    "x-amz-date",
    "amz-sdk-invocation-id",
    "amz-sdk-retry",
];
const HOST_HEADER: &str = "host";
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

    fn tabular_id(&self) -> uuid::Uuid {
        match self {
            Self::Table(info) => *info.tabular_id,
            Self::GenericTable(info) => *info.tabular_id,
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

        let StorageProfile::S3(s3_profile) = &warehouse.storage_profile else {
            return Err(IcebergErrorResponse::from(ErrorModel::bad_request(
                "Remote signing is only supported for S3 storage",
                "UnsupportedStorageType",
                None,
            )));
        };
        if !s3_profile.remote_signing_enabled {
            return Err(IcebergErrorResponse::from(ErrorModel::forbidden(
                "Remote signing is disabled for this storage profile",
                "RemoteSigningDisabled",
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

        let (parsed_url, operation) = check_request(
            s3_profile,
            &request_url,
            &request_method,
            request_body.as_deref(),
            &request_headers,
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
                    Addressing::TabularId,
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

/// Everything about a request to sign that is checked before its table is known: the
/// locations it touches, its query parameters and its headers.
fn check_request(
    s3_profile: &S3Profile,
    request_url: &url::Url,
    request_method: &http::Method,
    request_body: Option<&str>,
    request_headers: &HashMap<String, Vec<String>>,
) -> Result<(s3_utils::ParsedSignRequest, Operation)> {
    validate_host_header(request_url, request_headers)?;
    let headers = policy::RequestHeaders::new(request_headers)?;
    policy::require_signable_headers(
        &headers,
        request_method,
        s3_profile.aws_kms_key_arn.as_deref(),
    )?;
    s3_utils::parse_sign_request(
        &s3_utils::SignRequestUri::new(request_url.clone())?,
        s3_profile.remote_signing_url_style,
        request_method,
        request_body,
        &headers,
    )
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
        if UNSIGNED_HEADERS.contains(&key.to_ascii_lowercase().as_str()) {
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

/// aws-sigv4 signs a client `host` header verbatim and derives one from the URI only when it
/// is absent, so a different host would sign a request for another bucket or endpoint.
fn validate_host_header(
    request_url: &url::Url,
    request_headers: &HashMap<String, Vec<String>>,
) -> Result<()> {
    let mut values = request_headers
        .iter()
        .filter(|(name, _)| name.to_lowercase() == HOST_HEADER)
        .flat_map(|(_, values)| values)
        .peekable();
    if values.peek().is_none() {
        return Ok(());
    }

    let signed_host = signed_host(request_url)?;
    if values.any(|value| !value.trim_matches(' ').eq_ignore_ascii_case(&signed_host)) {
        return Err(ErrorModel::bad_request(
            "The host header must match the host of the URI to sign",
            "HostHeaderMismatch",
            None,
        )
        .into());
    }

    Ok(())
}

/// The `host` aws-sigv4 derives from the URI: the host, plus the port unless it is the
/// scheme default.
fn signed_host(request_url: &url::Url) -> Result<String> {
    let uri = request_url.as_str().parse::<http::Uri>().map_err(|e| {
        ErrorModel::bad_request(
            "Request is not signable",
            "FailedToCreateSignableRequest",
            Some(Box::new(e)),
        )
    })?;
    let is_default_port = matches!(
        (uri.scheme_str(), uri.port_u16()),
        (Some("http"), Some(80)) | (Some("https"), Some(443))
    );
    let host = if is_default_port {
        uri.host()
    } else {
        uri.authority().map(http::uri::Authority::as_str)
    };
    host.map(ToString::to_string).ok_or_else(|| {
        ErrorModel::bad_request("URI to sign does not have a host", "UriNoHost", None).into()
    })
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

/// How the request named the tabular. Only affects how a location mismatch is reported.
#[derive(Clone, Copy)]
enum Addressing {
    /// Lakekeeper's `tabular-id` route, where a mismatch has one known cause.
    TabularId,
    /// The spec's per-table route.
    TableName,
}

impl Addressing {
    fn as_str(self) -> &'static str {
        match self {
            Self::TabularId => "tabular-id",
            Self::TableName => "table-name",
        }
    }
}

/// Accept a tabular the request named directly, or fall back to a location based
/// lookup when its location does not cover the request URI.
///
/// Shared by both named routes so neither grows its own cross-check.
async fn cross_check_or_fall_back<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    warehouse_id: WarehouseId,
    named: Option<SignableTabular>,
    addressing: Addressing,
    parsed_url: &s3_utils::ParsedSignRequest,
    first_location: &S3Location,
    state: &ApiContext<State<A, C, S>>,
) -> std::result::Result<Option<SignableTabular>, RequireTableActionError> {
    if let Some(signable) = named {
        if validate_uri(parsed_url, signable.location()).is_ok() {
            return Ok(Some(signable));
        }

        // The name resolved, but its location does not cover the request URI. Fall back to a
        // location based lookup; the request may still be legitimate for another tabular.
        //
        // Up to version 0.9.1 pyiceberg had a bug that did not allow table specific signer URIs.
        // Instead the first URI of the first sign call would be used for subsequent calls in the
        // same runtime too. This is fixed in 0.9.2 onward:
        // https://github.com/apache/iceberg-python/pull/2005
        // The fallback exists for that bug and will be removed in a future version of Lakekeeper,
        // so only the route that bug hits names it — the per-table route reaches this line for
        // other reasons and must not accuse the client of it.
        let hint = match addressing {
            Addressing::TabularId => {
                " This is a bug in the query engine. When using PyIceberg, please update to versions > 0.9.1"
            }
            Addressing::TableName => "",
        };
        tracing::warn!(
            addressing = addressing.as_str(),
            "Received a tabular specific sign request for tabular {} with a location {} that does not match the request URI {}. Falling back to location based lookup.{hint}",
            signable.tabular_id(),
            signable.location(),
            parsed_url.uri.received()
        );
    }

    resolve_signable_by_location(warehouse_id, first_location, state)
        .await
        .map_err(RequireTableActionError::from)
}

/// Resolve the tabular addressed by the table-scoped signer route
/// (`/v1/signer/{warehouse-id}/tabular-id/{uuid}/v1/aws/s3/sign`). That route carries a
/// bare UUID, so the id is resolved across all tabular types.
async fn resolve_signable_by_id<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    warehouse_id: WarehouseId,
    tabular_id: uuid::Uuid,
    addressing: Addressing,
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

    cross_check_or_fall_back(
        warehouse_id,
        info_by_id.and_then(SignableTabular::from_view_or_table),
        addressing,
        parsed_url,
        first_location,
        state,
    )
    .await
}

/// Resolve the table the spec's per-table `/sign` route names.
///
/// The looked-up table is used as resolved rather than re-fetched by id, so the
/// ident keeps the caller's case like every other by-name route. Only the id and
/// the location travel onward, and both are case-independent.
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

    cross_check_or_fall_back(
        warehouse_id,
        table_info.map(SignableTabular::Table),
        Addressing::TableName,
        parsed_url,
        first_location,
        state,
    )
    .await
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

    use super::{
        ErrorModel, Operation, Result,
        policy::{
            self, COPY_SOURCE_HEADER, DELETE_QUERY_PARAM, LIST_TYPE_QUERY_PARAM, LIST_TYPE_V2,
            PREFIX_QUERY_PARAM, RequestHeaders, RequestKind, VERSION_ID_QUERY_PARAM,
        },
    };
    use crate::service::storage::{ValidationError, s3::S3UrlStyleDetectionMode};

    /// The url path percent-encode set, which is what a `Location` built from an object key
    /// carries. `/` is not part of it - it separates segments in both representations. `\` is
    /// not either: `url` treats it as a second separator for http but not for `s3`, so an
    /// object key containing it is refused while a list prefix keeps the byte S3 matches
    /// against. Control characters are left out so that `Location` keeps refusing them; object
    /// keys refuse them before encoding.
    const URL_PATH_ENCODE_SET: &AsciiSet = &AsciiSet::EMPTY
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

        /// `false` unless every received path segment, url-decoded once, is the key segment S3
        /// stores and maps one to one onto a segment of [`Self::decoded`], the path locations
        /// are derived from. Url parsing resolves dot segments (also spelled `%2e`), splits on
        /// `\\`, and strips tabs and newlines, so any such rewrite shows up as a mismatch.
        ///
        /// Each decoded segment is compared re-encoded with the url path percent-encode set,
        /// not decoded a second time: a key segment holding a literal `%2F` is one segment.
        /// [`Self::path_survived_decoding`] is too strict for object keys: clients encode
        /// characters like `=` that the url parser keeps decoded.
        fn segments_survived_decoding(&self) -> bool {
            let received = self
                .received
                .path_segments()
                .map(Iterator::collect::<Vec<_>>)
                .unwrap_or_default();
            let decoded = self
                .decoded
                .path_segments()
                .map(Iterator::collect::<Vec<_>>)
                .unwrap_or_default();
            if received.len() != decoded.len() {
                return false;
            }

            let last = received.len().saturating_sub(1);
            received.iter().zip(&decoded).enumerate().all(
                |(i, (received_segment, decoded_segment))| {
                    let Ok(key) = urlencoding::decode(received_segment) else {
                        return false;
                    };
                    !is_ambiguous_key_segment(&key, i == last)
                        && !key.contains(['/', '\\'])
                        && !key.chars().any(char::is_control)
                        && utf8_percent_encode(&key, URL_PATH_ENCODE_SET).to_string()
                            == *decoded_segment
                },
            )
        }
    }

    /// `true` for a key segment that a `Location` collapses or that path normalization
    /// resolves: `.`, `..`, or an empty segment other than the trailing one of a directory
    /// marker. A location derived from such a key is not the key S3 stores.
    fn is_ambiguous_key_segment(segment: &str, is_last: bool) -> bool {
        matches!(segment, "." | "..") || (segment.is_empty() && !is_last)
    }

    /// `true` if a key S3 takes verbatim, rather than from the request path, has a segment
    /// [`is_ambiguous_key_segment`] refuses, or a `\`, which some stores read as a separator.
    /// Keys in the request path refuse `\` too.
    fn has_ambiguous_key_segment(key: &str) -> bool {
        let segments = key.split('/').collect::<Vec<_>>();
        key.contains('\\')
            || segments
                .iter()
                .enumerate()
                .any(|(i, segment)| is_ambiguous_key_segment(segment, i + 1 == segments.len()))
    }

    /// The location of the object a copy reads, taken from its `x-amz-copy-source` header:
    /// `[/]{bucket}/{key}[?versionId={version}]`, with the key url-encoded. `None` if the
    /// request does not copy.
    ///
    /// The source has to lie inside the table just like the destination, which is why it
    /// becomes one of the request's locations. S3 decodes the key once, so it is decoded
    /// once here, held to the rules of a key S3 takes verbatim, and encoded again the way
    /// the destination's key is, so that both compare to the table location alike.
    pub(super) fn copy_source_location(headers: &RequestHeaders<'_>) -> Result<Option<S3Location>> {
        let err = |m: &str| ErrorModel::bad_request(m, "InvalidCopySource", None);

        let Some(value) = headers.single(COPY_SOURCE_HEADER)? else {
            return Ok(None);
        };
        let (path, query) = value.split_once('?').unwrap_or((value, ""));
        if !query.is_empty()
            && !query
                .strip_prefix(VERSION_ID_QUERY_PARAM)
                .and_then(|rest| rest.strip_prefix('='))
                .is_some_and(|version| !version.is_empty() && !version.contains('&'))
        {
            return Err(err("A copy source may only carry a `versionId`").into());
        }

        let (bucket, key) = path
            .strip_prefix('/')
            .unwrap_or(path)
            .split_once('/')
            .filter(|(bucket, key)| !bucket.is_empty() && !key.is_empty())
            .ok_or_else(|| err("A copy source must be `{bucket}/{key}`"))?;
        // Stores differ on whether a `+` in the source is a space. Clients send `%2B`.
        if key.contains('+') {
            return Err(err("A copy source must encode `+` as `%2B`").into());
        }
        // A `%` that does not start an escape is kept by this decoder but makes others fall
        // back to the undecoded key, so the two would name different objects.
        let is_escape =
            |escape: &[u8]| escape.len() == 2 && escape.iter().all(u8::is_ascii_hexdigit);
        if key.bytes().enumerate().any(|(i, byte)| {
            byte == b'%' && !is_escape(key.as_bytes().get(i + 1..i + 3).unwrap_or_default())
        }) {
            return Err(err("The key of a copy source is not valid url-encoded utf-8").into());
        }
        let key = urlencoding::decode(key)
            .map_err(|_| err("The key of a copy source is not valid url-encoded utf-8"))?;

        if has_ambiguous_key_segment(&key) {
            return Err(
                err("A copy source must not contain `.`, `..`, empty segments or `\\`").into(),
            );
        }

        let key = utf8_percent_encode(&key, URL_PATH_ENCODE_SET);
        S3Location::try_from_str(&format!("s3://{bucket}/{key}"), false)
            .map(Some)
            .map_err(|e| {
                ErrorModel::bad_request(
                    format!("Invalid copy source: {e}"),
                    "InvalidCopySource",
                    Some(Box::new(e)),
                )
                .into()
            })
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

    /// Parse the body of an S3 `DeleteObjects` request into the objects it deletes.
    pub(super) fn parse_s3_delete_xml(
        xml: &str,
    ) -> Result<Vec<ObjectIdentifier>, S3DeleteParseError> {
        let delete_request: DeleteObjectsRequest = quick_xml::de::from_str(xml)?;

        if delete_request.objects.is_empty() {
            return Err(S3DeleteParseError::NoObjects);
        }

        Ok(delete_request.objects)
    }

    /// Determine the locations a request touches, including the object a copy reads. The
    /// destination comes first: it names the table.
    pub(super) fn parse_sign_request(
        uri: &SignRequestUri,
        s3_url_style_detection: S3UrlStyleDetectionMode,
        method: &http::Method,
        body: Option<&str>,
        headers: &RequestHeaders<'_>,
    ) -> Result<(ParsedSignRequest, Operation)> {
        let (mut parsed_request, operation) =
            parse_s3_url(uri, s3_url_style_detection, method, body)?;
        if let Some(copy_source) = copy_source_location(headers)? {
            parsed_request.locations.push(copy_source);
        }
        Ok((parsed_request, operation))
    }

    /// Determine the locations a request touches, apart from the object a copy reads.
    fn parse_s3_url(
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

        let (kind, operation) = request_kind(uri.received(), method)?;

        // `DeleteObjects` and `ListObjectsV2` address the bucket instead of a single object.
        // The keys that identify the table live in the request body resp. the query string.
        let allow_no_key = kind != RequestKind::Object;

        // Bucket-level requests are held to the stricter `require_bucket_addressed`.
        if !allow_no_key && !uri.segments_survived_decoding() {
            return Err(err(
                "AmbiguousUriPath",
                "URI path must address the same key after url-decoding its segments",
            )
            .into());
        }

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

        match kind {
            RequestKind::DeleteObjects => {
                parsed_request.locations = delete_objects_locations(&parsed_request, body)?;
            }
            RequestKind::List => {
                // The URI path is just the bucket - the table is identified by the `prefix`.
                let bucket = bucket_of(&parsed_request)?;
                require_bucket_addressed(&parsed_request, &bucket)?;
                policy::require_known_parameters(uri.received(), RequestKind::List)?;
                parsed_request.locations = vec![list_prefix_location(uri.received(), &bucket)?];
                parsed_request.locations_are_list_prefixes = true;
            }
            RequestKind::Object => {
                policy::require_known_parameters(uri.received(), RequestKind::Object)?;
                policy::require_unversioned_change(uri.received(), method)?;
            }
        }

        Ok((parsed_request, operation))
    }

    fn request_kind(uri: &url::Url, method: &http::Method) -> Result<(RequestKind, Operation)> {
        let has = |param: &str| uri.query_pairs().any(|(key, _)| key == param);
        Ok(match *method {
            http::Method::GET if is_list_objects_v2(uri) => (RequestKind::List, Operation::Read),
            http::Method::GET | http::Method::HEAD => (RequestKind::Object, Operation::Read),
            http::Method::POST if has(DELETE_QUERY_PARAM) => {
                (RequestKind::DeleteObjects, Operation::Delete)
            }
            http::Method::POST | http::Method::PUT => (RequestKind::Object, Operation::Write),
            http::Method::DELETE => (RequestKind::Object, Operation::Delete),
            _ => {
                return Err(ErrorModel::builder()
                    .code(http::StatusCode::METHOD_NOT_ALLOWED.into())
                    .message("Method not allowed".to_string())
                    .r#type("MethodNotAllowed".to_string())
                    .build()
                    .into());
            }
        })
    }

    /// The locations of the keys a `DeleteObjects` request names in its body.
    fn delete_objects_locations(
        parsed_request: &ParsedSignRequest,
        body: Option<&str>,
    ) -> Result<Vec<S3Location>> {
        let err = |t: &str, m: &str| ErrorModel::bad_request(m, t, None);
        let Some(xml_body) = body else {
            return Err(err("DeleteWithoutBody", "Delete requests require a body").into());
        };
        let bucket = bucket_of(parsed_request)?;
        require_bucket_addressed(parsed_request, &bucket)?;
        policy::require_known_parameters(
            parsed_request.uri.received(),
            RequestKind::DeleteObjects,
        )?;

        let objects =
            parse_s3_delete_xml(xml_body).map_err(|e| err("InvalidDeleteBody", &format!("{e}")))?;
        if objects.iter().any(|object| object.version_id.is_some()) {
            return Err(policy::version_change_error().into());
        }

        objects
            .into_iter()
            .map(|object| {
                if has_ambiguous_key_segment(&object.key) {
                    return Err(err(
                        "AmbiguousDeleteKey",
                        "Keys to delete must not contain `.`, `..`, empty segments or `\\`",
                    )
                    .into());
                }
                let segments = object.key.split('/').collect::<Vec<_>>();
                Ok(S3Location::new(&bucket, &segments, None).map_err(ValidationError::from)?)
            })
            .collect()
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
        let prefix = utf8_percent_encode(&prefix, URL_PATH_ENCODE_SET);

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

    /// Versions are kept, so that a delete naming one can be refused.
    #[test]
    fn test_parse_s3_delete_xml_keeps_versions() {
        let versions = parse_s3_delete_xml(TEST_XML)
            .unwrap()
            .into_iter()
            .map(|object| object.version_id)
            .collect::<Vec<_>>();
        assert_eq!(versions, vec![None, None, Some("version-id-1".to_string())]);
    }

    #[test]
    fn test_full_deserialize_2() {
        let keys = parse_s3_delete_xml("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Object><Key>initial-warehouse/01963de0-99d9-79e2-8e95-24b11d0d334c/01963e34-84b6-7313-aba0-04694cd1c8c6/metadata/snap-8699614565852557623-1-15f84829-fee3-4cd6-8691-7ea967e4f15c.avro</Key></Object><Object><Key>initial-warehouse/01963de0-99d9-79e2-8e95-24b11d0d334c/01963e34-84b6-7313-aba0-04694cd1c8c6/metadata/snap-7686961691068480281-1-d204b9d8-6b72-454a-9f67-37a6d5e6d4a5.avro</Key></Object><Object><Key>initial-warehouse/01963de0-99d9-79e2-8e95-24b11d0d334c/01963e34-84b6-7313-aba0-04694cd1c8c6/metadata/snap-1836869532246818762-1-aebc0c21-c6ac-4ef2-abd0-5a17647a4f78.avro</Key></Object><Object><Key>initial-warehouse/01963de0-99d9-79e2-8e95-24b11d0d334c/01963e34-84b6-7313-aba0-04694cd1c8c6/metadata/snap-5189981498526175103-1-91703d93-aa16-4f0f-835e-606656746aa5.avro</Key></Object><Object><Key>initial-warehouse/01963de0-99d9-79e2-8e95-24b11d0d334c/01963e34-84b6-7313-aba0-04694cd1c8c6/metadata/snap-2371629502487233412-1-9ec13408-f2a0-4f30-8560-ac7ab26611b5.avro</Key></Object></Delete>").unwrap();
        let keys = keys
            .into_iter()
            .map(|object| object.key)
            .collect::<HashSet<_>>();
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
    use crate::service::storage::{S3Flavor, s3::S3UrlStyleDetectionMode};

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
        parse_with_headers(uri, mode, method, body, &HashMap::new())
    }

    fn parse_with_headers(
        uri: &url::Url,
        mode: S3UrlStyleDetectionMode,
        method: &http::Method,
        body: Option<&str>,
        headers: &HashMap<String, Vec<String>>,
    ) -> Result<(s3_utils::ParsedSignRequest, Operation)> {
        s3_utils::parse_sign_request(
            &s3_utils::SignRequestUri::new(uri.clone())?,
            mode,
            method,
            body,
            &policy::RequestHeaders::new(headers)?,
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
    /// cases where both produce a location are compared. Object keys refuse `/` and `\`, see
    /// [`test_list_prefix_keeps_a_backslash_that_an_object_key_refuses`].
    #[test]
    fn test_list_prefix_and_object_key_encode_alike() {
        let escapes = (0x00..=0xFF).map(|byte: u8| format!("%{byte:02X}")).chain([
            "%C3%A9".to_string(),
            "%2525".to_string(),
            "%2f".to_string(),
        ]);

        let mut compared = 0;

        for encoded in escapes {
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
        assert_eq!(compared, 95, "the sweep must not go vacuous");
    }

    /// `url` treats `\` as a second path separator for http, so an object key containing it
    /// would be authorized as a different key than S3 stores and is refused. The list prefix
    /// keeps the byte S3 matches keys against - the authorized string is what gets listed.
    #[test]
    fn test_list_prefix_keeps_a_backslash_that_an_object_key_refuses() {
        let list = parse_list(
            "http://s3.example.com:8333/bucket?list-type=2&prefix=ns%2Ftbl%5Ca%2F",
            S3UrlStyleDetectionMode::Auto,
        );
        assert_eq!(list.locations[0].to_string(), "s3://bucket/ns/tbl\\a/");

        assert_eq!(
            parse_object_err(
                "http://s3.example.com:8333/bucket/ns/tbl%5Ca/f.parquet",
                &http::Method::GET
            ),
            (400, "AmbiguousUriPath".to_string())
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

        let err = s3_utils::parse_sign_request(
            &uri,
            S3UrlStyleDetectionMode::Auto,
            &http::Method::GET,
            None,
            &policy::RequestHeaders::new(&HashMap::new()).unwrap(),
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
            "http://s3.example.com:8333/bucket?list-type=2&prefix=ns/tbl/;acl",
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

    const OBJECT_URI: &str = "http://s3.example.com:8333/bucket/wh/tbl/data/x.parquet";

    const KMS_KEY: &str = "arn:aws:kms:eu-central-1:123456789012:key/abc";

    fn headers(names: &[&str]) -> HashMap<String, Vec<String>> {
        names
            .iter()
            .map(|name| ((*name).to_string(), vec!["value".to_string()]))
            .collect()
    }

    fn header_values(headers: &[(&str, &str)]) -> HashMap<String, Vec<String>> {
        headers
            .iter()
            .map(|(name, value)| ((*name).to_string(), vec![(*value).to_string()]))
            .collect()
    }

    fn s3_profile(kms_key: Option<&str>) -> S3Profile {
        let mut profile = S3Profile::builder()
            .region("my-region".to_string())
            .flavor(S3Flavor::S3Compat)
            .sts_enabled(false)
            .bucket("bucket".to_string())
            .build();
        profile.aws_kms_key_arn = kms_key.map(ToString::to_string);
        profile
    }

    /// A write that copies from inside the table, encrypted with the warehouse's key, is
    /// checked against both locations.
    #[test]
    fn test_check_request_allows_copy_within_the_table() {
        let (parsed, operation) = check_request(
            &s3_profile(Some(KMS_KEY)),
            &url::Url::parse(OBJECT_URI).unwrap(),
            &http::Method::PUT,
            None,
            &header_values(&[
                ("X-Amz-Copy-Source", "bucket/wh/tbl/data/old.parquet"),
                ("x-amz-metadata-directive", "COPY"),
                ("x-amz-server-side-encryption", "aws:kms"),
                ("x-amz-server-side-encryption-aws-kms-key-id", KMS_KEY),
                ("X-Amzn-Trace-Id", "Root=1-abc"),
                ("Content-Length", "0"),
            ]),
        )
        .unwrap();
        assert_eq!(operation, Operation::Write);
        assert_eq!(
            parsed
                .locations
                .iter()
                .map(ToString::to_string)
                .collect_vec(),
            vec![
                "s3://bucket/wh/tbl/data/x.parquet",
                "s3://bucket/wh/tbl/data/old.parquet",
            ]
        );
        validate_uri(&parsed, &Location::from_str("s3://bucket/wh/tbl").unwrap()).unwrap();
    }

    /// Every check that runs before the table is known refuses on its own.
    #[test]
    fn test_check_request_refuses() {
        let versioned = format!("{OBJECT_URI}?versionId=v1");
        let acl_query = format!("{OBJECT_URI}?x-amz-acl=public-read");
        for (method, uri, headers, expected) in [
            (
                http::Method::PUT,
                OBJECT_URI,
                vec![("x-amz-website-redirect-location", "/x")],
                (403, "HeaderNotSignable"),
            ),
            (
                http::Method::PUT,
                OBJECT_URI,
                vec![("x-amz-server-side-encryption", "AES256")],
                (403, "EncryptionNotSignable"),
            ),
            (
                http::Method::GET,
                OBJECT_URI,
                vec![("x-amz-copy-source", "bucket/wh/tbl/data/a.parquet")],
                (403, "CopyNotSignable"),
            ),
            (
                http::Method::PUT,
                OBJECT_URI,
                vec![("x-amz-copy-source", "bucket/wh/tbl/data/a+b.parquet")],
                (400, "InvalidCopySource"),
            ),
            (
                http::Method::PUT,
                versioned.as_str(),
                vec![],
                (403, "VersionChangeNotSignable"),
            ),
            (
                http::Method::PUT,
                acl_query.as_str(),
                vec![],
                (400, "UnsupportedObjectParameter"),
            ),
            (
                http::Method::PUT,
                OBJECT_URI,
                vec![("host", "other.example.com")],
                (400, "HostHeaderMismatch"),
            ),
            (
                http::Method::PUT,
                OBJECT_URI,
                vec![("x_amz_acl", "public-read")],
                (400, "InvalidHeaderName"),
            ),
        ] {
            let err = check_request(
                &s3_profile(Some(KMS_KEY)),
                &url::Url::parse(uri).unwrap(),
                &method,
                None,
                &header_values(&headers),
            )
            .map(|(parsed, _)| parsed)
            .expect_err(&format!(
                "{method} {uri} with {headers:?} must not be signable"
            ))
            .error;
            assert_eq!(
                (err.code, err.r#type.as_str()),
                expected,
                "Test case: {method} {uri} {headers:?}"
            );
        }
    }

    /// aws-sigv4 signs a client `host` header verbatim, so a different host would be a
    /// signature for another bucket or endpoint than the one validated.
    #[test]
    fn test_host_header_must_match_uri() {
        for (uri, host) in [
            (
                "https://mybucket.s3.us-east-1.amazonaws.com/wh/tbl/x.parquet",
                "victimbucket.s3.us-east-1.amazonaws.com",
            ),
            (
                "https://mybucket.s3.us-east-1.amazonaws.com/wh/tbl/x.parquet",
                "mybucket.s3.us-east-1.amazonaws.com:8443",
            ),
            (
                "http://s3.example.com:8333/bucket/wh/tbl/x.parquet",
                "s3.example.com",
            ),
            (
                "http://s3.example.com:8333/bucket/wh/tbl/x.parquet",
                "other.example.com:8333",
            ),
            ("http://s3.example.com:8333/bucket/wh/tbl/x.parquet", ""),
        ] {
            for name in ["host", "Host", "HOST"] {
                let err = validate_host_header(
                    &url::Url::parse(uri).unwrap(),
                    &header_values(&[(name, host)]),
                )
                .expect_err(&format!("{uri} with {name}: {host} must not be signable"));
                assert_eq!(
                    (err.error.code, err.error.r#type),
                    (400, "HostHeaderMismatch".to_string()),
                    "Test case: {uri} {name}: {host}"
                );
            }
        }
    }

    #[test]
    fn test_host_header_matching_uri_is_allowed() {
        for (uri, host) in [
            (
                "https://mybucket.s3.us-east-1.amazonaws.com/wh/tbl/x.parquet",
                "mybucket.s3.us-east-1.amazonaws.com",
            ),
            (
                "https://mybucket.s3.us-east-1.amazonaws.com:443/wh/tbl/x.parquet",
                "mybucket.s3.us-east-1.amazonaws.com",
            ),
            (
                "http://s3.example.com:80/bucket/wh/tbl/x.parquet",
                "s3.example.com",
            ),
            (
                "http://s3.example.com:8333/bucket/wh/tbl/x.parquet",
                "s3.example.com:8333",
            ),
            (
                "https://s3.example.com:8443/bucket/wh/tbl/x.parquet",
                "S3.Example.com:8443",
            ),
        ] {
            for name in ["host", "Host"] {
                validate_host_header(
                    &url::Url::parse(uri).unwrap(),
                    &header_values(&[(name, host)]),
                )
                .unwrap_or_else(|e| panic!("{uri} with {name}: {host} must be signable: {e:?}"));
            }
            validate_host_header(&url::Url::parse(uri).unwrap(), &headers(&["content-type"]))
                .unwrap_or_else(|e| panic!("{uri} without host must be signable: {e:?}"));
        }
    }

    fn parse_object_err(uri: &str, method: &http::Method) -> (u16, String) {
        let err = parse_uri(
            &url::Url::parse(uri).unwrap(),
            S3UrlStyleDetectionMode::Auto,
            method,
            None,
        )
        .map(|(parsed, _)| parsed)
        .expect_err(&format!("{uri} must not be signable"))
        .error;
        (err.code, err.r#type)
    }

    /// The received URI is signed, so S3 stores the key the client sent: a segment that only
    /// becomes `..`, a separator or an empty segment after decoding addresses another key than
    /// the one validated against the table location.
    #[test]
    fn test_parse_s3_url_object_path_must_survive_decoding() {
        for uri in [
            // Validated as `wh/tbl/x.parquet`, stored under the sibling `wh/tblX/`.
            "http://s3.example.com:8333/bucket/wh/tblX/..%2Ftbl/x.parquet",
            "https://bucket.s3.my-region.amazonaws.com/wh/tblX/..%2Ftbl/x.parquet",
            "http://s3.example.com:8333/bucket/wh/tbl/x%2F..%2F..%2Fother%2Fy.parquet",
            "http://s3.example.com:8333/bucket/wh/tbl/%2E%2E%2Fother/y.parquet",
            "http://s3.example.com:8333/bucket/wh/tbl/a%2Fb.parquet",
            "http://s3.example.com:8333/bucket/wh/tbl/a%2fb.parquet",
            // `\` is a separator for the url parser but part of the key for S3.
            "http://s3.example.com:8333/bucket/wh/tblX%5C..%5Ctbl/x.parquet",
            "http://s3.example.com:8333/bucket/wh/tbl%5Cx.parquet",
            // Empty segments collapse in a location but not in a key.
            "http://s3.example.com:8333/bucket/wh//tbl/x.parquet",
            "https://bucket.s3.my-region.amazonaws.com//wh/tbl/x.parquet",
            // Decoded once, these are percent-encoded dots the url parser resolves.
            "http://s3.example.com:8333/bucket/wh/other/%252E%252E/tbl/x.parquet",
            "http://s3.example.com:8333/bucket/wh/other/%252e%252e/tbl/x.parquet",
            "http://s3.example.com:8333/bucket/wh/other/.%252E/tbl/x.parquet",
            "http://s3.example.com:8333/bucket/wh/other/%252E./tbl/x.parquet",
            "http://s3.example.com:8333/bucket/wh/tbl/%252E/x.parquet",
            "https://bucket.s3.my-region.amazonaws.com/wh/other/%252E%252E/tbl/x.parquet",
            // Tab, LF and CR are stripped by the url parser.
            "http://s3.example.com:8333/bucket/wh/tb%09l/x.parquet",
            "http://s3.example.com:8333/bucket/wh/tb%0Al/x.parquet",
            "http://s3.example.com:8333/bucket/wh/tb%0Dl/x.parquet",
            "http://s3.example.com:8333/bucket/wh/other/.%09./tbl/x.parquet",
            // Other control characters are not part of a location.
            "http://s3.example.com:8333/bucket/wh/tbl/x%00.parquet",
            "http://s3.example.com:8333/bucket/wh/tbl/x%7F.parquet",
            "http://s3.example.com:8333/bucket/wh/tbl/x%C2%85.parquet",
        ] {
            for method in [http::Method::GET, http::Method::PUT, http::Method::DELETE] {
                assert_eq!(
                    parse_object_err(uri, &method),
                    (400, "AmbiguousUriPath".to_string()),
                    "Test case: {method} {uri}"
                );
            }
        }
    }

    /// Url-encoded keys whose segments stay segments are signable and stay inside the table.
    #[test]
    fn test_parse_s3_url_object_path_allows_encoded_keys() {
        let table_location = Location::from_str("s3://bucket/wh/tbl").unwrap();
        for (uri, expected) in [
            (
                "http://s3.example.com:8333/bucket/wh/tbl/data/name=a%20b/x.parquet",
                "s3://bucket/wh/tbl/data/name=a%20b/x.parquet",
            ),
            (
                "http://s3.example.com:8333/bucket/wh/tbl/data/name%3Da%20b/x.parquet",
                "s3://bucket/wh/tbl/data/name=a%20b/x.parquet",
            ),
            (
                "https://bucket.s3.my-region.amazonaws.com/wh/tbl/data/name%3Da%20b/x.parquet",
                "s3://bucket/wh/tbl/data/name=a%20b/x.parquet",
            ),
            (
                "http://s3.example.com:8333/bucket/wh/tbl/data/100%25/x.parquet",
                "s3://bucket/wh/tbl/data/100%/x.parquet",
            ),
            // A partition value containing `/` is part of the key in its encoded form.
            (
                "http://s3.example.com:8333/bucket/wh/tbl/data/name=a%252Fb/x.parquet",
                "s3://bucket/wh/tbl/data/name=a%2Fb/x.parquet",
            ),
            (
                "http://s3.example.com:8333/bucket/wh/tbl/data/caf%C3%A9/x.parquet",
                "s3://bucket/wh/tbl/data/caf%C3%A9/x.parquet",
            ),
            (
                "http://s3.example.com:8333/bucket/wh/tbl/data/a.b..c/x.parquet",
                "s3://bucket/wh/tbl/data/a.b..c/x.parquet",
            ),
            (
                "http://s3.example.com:8333/bucket/wh/tbl/data/..a/x.parquet",
                "s3://bucket/wh/tbl/data/..a/x.parquet",
            ),
            (
                "http://s3.example.com:8333/bucket/wh/tbl/data/a+b/x.parquet",
                "s3://bucket/wh/tbl/data/a+b/x.parquet",
            ),
            (
                "http://s3.example.com:8333/bucket/wh/tbl/data/a%2Bb/x.parquet",
                "s3://bucket/wh/tbl/data/a+b/x.parquet",
            ),
            (
                "http://s3.example.com:8333/bucket/wh/tbl/data/a%252E%252Eb/x.parquet",
                "s3://bucket/wh/tbl/data/a%2E%2Eb/x.parquet",
            ),
            (
                "http://s3.example.com:8333/bucket/wh/tbl/data/%E2%82%AC%20%7B%7D/x.parquet",
                "s3://bucket/wh/tbl/data/%E2%82%AC%20%7B%7D/x.parquet",
            ),
            // Directory markers end in a separator.
            (
                "http://s3.example.com:8333/bucket/wh/tbl/data/",
                "s3://bucket/wh/tbl/data/",
            ),
        ] {
            for method in [http::Method::GET, http::Method::PUT] {
                let (parsed, _) = parse_uri(
                    &url::Url::parse(uri).unwrap(),
                    S3UrlStyleDetectionMode::Auto,
                    &method,
                    None,
                )
                .unwrap_or_else(|e| panic!("{method} {uri} must be signable: {e:?}"));
                assert_eq!(
                    parsed
                        .locations
                        .iter()
                        .map(ToString::to_string)
                        .collect_vec(),
                    vec![expected],
                    "Test case: {method} {uri}"
                );
                validate_uri(&parsed, &table_location)
                    .unwrap_or_else(|e| panic!("{method} {uri} must be inside the table: {e:?}"));
            }
        }
    }

    /// The parameters a `FileIO` sends with its object requests keep the request signable and
    /// inside the table.
    #[test]
    fn test_parse_s3_url_object_allows_fileio_parameters() {
        let table_location = Location::from_str("s3://bucket/wh/tbl").unwrap();
        for (method, query) in [
            (http::Method::GET, ""),
            (http::Method::GET, "?x-id=GetObject"),
            (http::Method::GET, "?versionId=v1"),
            (http::Method::GET, "?partNumber=1"),
            (http::Method::GET, "?attributes"),
            (http::Method::GET, "?tagging"),
            (http::Method::GET, "?response-content-type=text%2Fplain"),
            (
                http::Method::GET,
                "?uploadId=abc&max-parts=10&part-number-marker=2",
            ),
            (http::Method::HEAD, "?versionId=v1"),
            (http::Method::PUT, "?x-id=PutObject"),
            (
                http::Method::PUT,
                "?partNumber=1&uploadId=abc&x-id=UploadPart",
            ),
            (http::Method::PUT, "?tagging"),
            (http::Method::POST, "?uploads"),
            (http::Method::POST, "?uploadId=abc"),
            (http::Method::DELETE, "?x-id=DeleteObject"),
            (
                http::Method::DELETE,
                "?uploadId=abc&x-id=AbortMultipartUpload",
            ),
            (http::Method::DELETE, "?tagging"),
        ] {
            let uri = format!("{OBJECT_URI}{query}");
            let (parsed, _) = parse_uri(
                &url::Url::parse(&uri).unwrap(),
                S3UrlStyleDetectionMode::Auto,
                &method,
                None,
            )
            .unwrap_or_else(|e| panic!("{method} {uri} must be signable: {e:?}"));
            validate_uri(&parsed, &table_location)
                .unwrap_or_else(|e| panic!("{method} {uri} must be inside the table: {e:?}"));
        }
    }

    /// S3 dispatches on the query string: another sub-resource on a key inside the table turns
    /// the signed request into an operation the key does not authorize, such as a rename that
    /// removes a source object outside the table.
    #[test]
    fn test_parse_s3_url_object_rejects_unknown_parameters() {
        for (method, query) in [
            (http::Method::PUT, "?renameObject"),
            (http::Method::PUT, "?x-id=RenameObject&renameObject"),
            (http::Method::POST, "?restore"),
            (http::Method::POST, "?select&select-type=2"),
            (http::Method::GET, "?torrent"),
            (http::Method::PUT, "?append&position=0"),
            (http::Method::GET, "?x-id=GetObject&policy"),
            (http::Method::POST, "?undelete"),
            // Some servers split on `;` too, which would hide a sub-resource in a value.
            (http::Method::GET, "?x-id=GetObject;retention"),
        ] {
            let uri = format!("{OBJECT_URI}{query}");
            assert_eq!(
                parse_object_err(&uri, &method),
                (400, "UnsupportedObjectParameter".to_string()),
                "Test case: {method} {uri}"
            );
        }
    }

    #[test]
    fn test_parse_s3_url_object_rejects_repeated_parameters() {
        for (method, query) in [
            (http::Method::PUT, "?partNumber=1&uploadId=abc&uploadId=def"),
            (http::Method::GET, "?versionId=v1&versionId=v2"),
        ] {
            let uri = format!("{OBJECT_URI}{query}");
            assert_eq!(
                parse_object_err(&uri, &method),
                (400, "RepeatedObjectParameter".to_string()),
                "Test case: {method} {uri}"
            );
        }
    }

    /// A `DeleteObjects` request is identified by its `delete` parameter, and carries no other
    /// sub-resource next to it.
    #[test]
    fn test_parse_s3_url_delete_objects_parameters() {
        let delete = |query: &str| {
            parse_uri(
                &url::Url::parse(&format!("http://s3.example.com:8333/bucket{query}")).unwrap(),
                S3UrlStyleDetectionMode::Auto,
                &http::Method::POST,
                Some(&delete_body(&["wh/tbl/x.parquet"])),
            )
            .map(|(_, operation)| operation)
        };

        assert_eq!(delete("?delete").unwrap(), Operation::Delete);
        assert_eq!(
            delete("?delete&x-id=DeleteObjects").unwrap(),
            Operation::Delete
        );
        for (query, expected) in [
            ("?delete&policy", "UnsupportedDeleteParameter"),
            ("?delete&delete", "RepeatedDeleteParameter"),
            // Not `DeleteObjects`: an object write to the bucket itself, which has no key.
            ("?undelete", "UriNotS3"),
        ] {
            assert_eq!(
                delete(query).unwrap_err().error.r#type,
                expected,
                "Test case: {query}"
            );
        }
    }

    fn copy_source_from(headers: &HashMap<String, Vec<String>>) -> Result<Option<S3Location>> {
        s3_utils::copy_source_location(&policy::RequestHeaders::new(headers)?)
    }

    fn copy_source(value: &str) -> Result<Option<S3Location>> {
        copy_source_from(&header_values(&[("X-Amz-Copy-Source", value)]))
    }

    #[test]
    fn test_copy_source_location() {
        assert!(
            copy_source_from(&headers(&["content-type"]))
                .unwrap()
                .is_none()
        );
        for (value, expected) in [
            (
                "/bucket/wh/tbl/data/x.parquet",
                "s3://bucket/wh/tbl/data/x.parquet",
            ),
            (
                "bucket/wh/tbl/data/x.parquet",
                "s3://bucket/wh/tbl/data/x.parquet",
            ),
            (
                "/bucket/wh/tbl/data/x.parquet?versionId=v1",
                "s3://bucket/wh/tbl/data/x.parquet",
            ),
            (
                "/bucket/wh/tbl/data/name%3Da%20b/x.parquet",
                "s3://bucket/wh/tbl/data/name=a%20b/x.parquet",
            ),
            (
                "/bucket/wh/tbl/data/a%2Bb.parquet",
                "s3://bucket/wh/tbl/data/a+b.parquet",
            ),
            // HTTP strips spaces and tabs around a header value.
            (
                " \t/bucket/wh/tbl/data/x.parquet\t ",
                "s3://bucket/wh/tbl/data/x.parquet",
            ),
            // Decoded once, as S3 does: the key holds a literal `%2F`.
            (
                "/bucket/wh/tbl/data/name=a%252Fb/x.parquet",
                "s3://bucket/wh/tbl/data/name=a%2Fb/x.parquet",
            ),
        ] {
            assert_eq!(
                copy_source(value)
                    .unwrap_or_else(|e| panic!("{value} must be a copy source: {e:?}"))
                    .map(|location| location.to_string()),
                Some(expected.to_string()),
                "Test case: {value}"
            );
        }
    }

    #[test]
    fn test_copy_source_location_rejects_malformed_sources() {
        for value in [
            "bucket",
            "/bucket/",
            "/bucket/wh/tbl/x.parquet?versionId=",
            "/bucket/wh/tbl/x.parquet?uploadId=abc",
            "/bucket/wh/tbl/x.parquet?versionId=v1&uploadId=abc",
            // Decoded once, these are separators and dot segments S3 keeps verbatim.
            "/bucket/wh/tbl%2F..%2Fother/x.parquet",
            "/bucket/wh/tbl/.%2Fx.parquet",
            "/bucket/wh//tbl/x.parquet",
            "/bucket/wh/tbl/%FF.parquet",
            "/bucket/wh/tbl/a+b.parquet",
            // Some stores read `\` as a separator.
            "/bucket/wh/tbl/x%5C..%5C..%5Cother%5Csecret",
            "/bucket/wh/tbl/x\\y.parquet",
            // Other decoders keep such a key undecoded.
            "/bucket/%77h/tbl/x%zz",
            "/bucket/wh/tbl/x%7",
            // Only spaces and tabs are stripped, so this is part of the bucket name.
            "\u{a0}/bucket/wh/tbl/x.parquet",
            // Access point ARNs are not resolved to a bucket.
            "arn:aws:s3:us-east-1:123456789012:accesspoint/ap/object/wh/tbl/x.parquet",
        ] {
            assert_eq!(
                copy_source(value)
                    .expect_err(&format!("{value} must not be a copy source"))
                    .error
                    .r#type,
                "InvalidCopySource",
                "Test case: {value}"
            );
        }

        let two_sources = HashMap::from([(
            "x-amz-copy-source".to_string(),
            vec![
                "/bucket/wh/tbl/x.parquet".to_string(),
                "/bucket/wh/other/y.parquet".to_string(),
            ],
        )]);
        let two_spellings = HashMap::from([
            (
                "x-amz-copy-source".to_string(),
                vec!["/bucket/wh/tbl/x.parquet".to_string()],
            ),
            (
                "X-Amz-Copy-Source".to_string(),
                vec!["/bucket/wh/other/y.parquet".to_string()],
            ),
        ]);
        // A store could read either value, or the second of two joined with `,`.
        let joined = header_values(&[(
            "x-amz-copy-source",
            "/bucket/wh/tbl/x.parquet,/bucket/wh/other/y.parquet",
        )]);
        for headers in [two_sources, two_spellings, joined] {
            let err = copy_source_from(&headers).unwrap_err().error;
            assert_eq!(
                (err.code, err.r#type.as_str()),
                (400, "InvalidHeaderValue"),
                "Test case: {headers:?}"
            );
        }
    }

    /// A copy reads its source, so the source has to lie inside the table like the destination.
    #[test]
    fn test_validate_uri_copy_source() {
        let table_location = Location::from_str("s3://bucket/wh/tbl").unwrap();
        for (source, inside) in [
            ("/bucket/wh/tbl/data/old.parquet", true),
            ("/bucket/wh/tbl/data/old.parquet?versionId=v1", true),
            ("/bucket/wh/other/data/x.parquet", false),
            ("/bucket/wh/tblX/data/x.parquet", false),
            ("/other-bucket/wh/tbl/data/x.parquet", false),
        ] {
            let (parsed, _) = parse_with_headers(
                &url::Url::parse(&format!("{OBJECT_URI}?partNumber=1&uploadId=abc")).unwrap(),
                S3UrlStyleDetectionMode::Auto,
                &http::Method::PUT,
                None,
                &header_values(&[("X-Amz-Copy-Source", source)]),
            )
            .unwrap_or_else(|e| panic!("{source} must be a copy source: {e:?}"));
            assert_eq!(
                parsed.locations.first().map(ToString::to_string).as_deref(),
                Some("s3://bucket/wh/tbl/data/x.parquet"),
                "The destination names the table. Test case: {source}"
            );
            assert_eq!(
                validate_uri(&parsed, &table_location).is_ok(),
                inside,
                "Test case: {source}"
            );
        }
    }

    /// Deleting one version removes it for good, past what bucket versioning keeps.
    #[test]
    fn test_parse_s3_url_rejects_versioned_deletes() {
        let err = parse_uri(
            &url::Url::parse(&format!("{OBJECT_URI}?versionId=v1")).unwrap(),
            S3UrlStyleDetectionMode::Auto,
            &http::Method::DELETE,
            None,
        )
        .map(|(parsed, _)| parsed)
        .expect_err("a versioned delete must not be signable")
        .error;
        assert_eq!(
            (err.code, err.r#type.as_str()),
            (403, "VersionChangeNotSignable")
        );
        let body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Object><Key>wh/tbl/x.parquet</Key></Object><Object><Key>wh/tbl/y.parquet</Key><VersionId>v1</VersionId></Object></Delete>";
        let err = parse_uri(
            &url::Url::parse("http://s3.example.com:8333/bucket?delete").unwrap(),
            S3UrlStyleDetectionMode::Auto,
            &http::Method::POST,
            Some(body),
        )
        .map(|(parsed, _)| parsed)
        .expect_err("a versioned delete must not be signable")
        .error;
        assert_eq!(
            (err.code, err.r#type.as_str()),
            (403, "VersionChangeNotSignable")
        );
    }

    fn delete_body(keys: &[&str]) -> String {
        let objects = keys
            .iter()
            .map(|key| format!("<Object><Key>{key}</Key></Object>"))
            .join("");
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{objects}</Delete>"
        )
    }

    /// Keys in a `DeleteObjects` body are taken verbatim by S3, while their location collapses
    /// empty segments: `wh//tbl/x` would be authorized as `wh/tbl/x`.
    #[test]
    fn test_parse_s3_url_delete_keys_must_not_be_ambiguous() {
        for key in [
            "wh//tbl/x.parquet",
            "/wh/tbl/x.parquet",
            "wh/tbl/../tblX/x.parquet",
            "wh/tbl/./x.parquet",
            "wh/tbl/..",
            "wh/tbl/x\\..\\..\\other\\y.parquet",
        ] {
            let err = parse_uri(
                &url::Url::parse("http://s3.example.com:8333/bucket?delete").unwrap(),
                S3UrlStyleDetectionMode::Auto,
                &http::Method::POST,
                Some(&delete_body(&["wh/tbl/ok.parquet", key])),
            )
            .map(|(parsed, _)| parsed)
            .expect_err(&format!("{key} must not be signable"))
            .error
            .r#type;
            assert_eq!(err, "AmbiguousDeleteKey", "Test case: {key}");
        }
    }

    #[test]
    fn test_parse_s3_url_delete_keys_allow_regular_keys() {
        let (parsed, _) = parse_uri(
            &url::Url::parse("http://s3.example.com:8333/bucket?delete").unwrap(),
            S3UrlStyleDetectionMode::Auto,
            &http::Method::POST,
            Some(&delete_body(&[
                "wh/tbl/data/a.b..c/x.parquet",
                "wh/tbl/data/name=a%2Fb/x.parquet",
                "wh/tbl/data/",
            ])),
        )
        .unwrap();
        assert_eq!(
            parsed
                .locations
                .iter()
                .map(ToString::to_string)
                .collect_vec(),
            vec![
                "s3://bucket/wh/tbl/data/a.b..c/x.parquet",
                "s3://bucket/wh/tbl/data/name=a%2Fb/x.parquet",
                "s3://bucket/wh/tbl/data/",
            ]
        );
    }
}
