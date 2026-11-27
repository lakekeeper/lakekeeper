use std::{default::Default, env::VarError, sync::LazyLock};

use lakekeeper::{
    AuthZBackend, CONFIG, X_FORWARDED_PREFIX_HEADER,
    api::iceberg::v1::tables::parse_if_none_match,
    axum,
    axum::{
        Router,
        http::{Extensions, HeaderMap, HeaderValue, StatusCode, Uri, Version, header},
        response::{IntoResponse, Redirect, Response},
        routing::get,
    },
    determine_forwarded_prefix,
    request_tracing::{MakeRequestUuid7, RestMakeSpan},
    tower,
    tower_http::{
        ServiceBuilderExt,
        catch_panic::CatchPanicLayer,
        compression::{
            CompressionLayer,
            predicate::{DefaultPredicate, NotForContentType, Predicate},
        },
        sensitive_headers::SetSensitiveHeadersLayer,
        timeout::TimeoutLayer,
        trace::{self, TraceLayer},
    },
    tracing,
};
use lakekeeper_console::{
    AssetEncoding, BasePrefix, Console, InvalidBasePrefix, LakekeeperConsoleConfig,
};

// Static configuration for UI
static UI_CONFIG: LazyLock<LakekeeperConsoleConfig> = LazyLock::new(|| {
    let default_config = LakekeeperConsoleConfig::default();
    let config = LakekeeperConsoleConfig {
        idp_authority: std::env::var("LAKEKEEPER__UI__OPENID_PROVIDER_URI")
            .ok()
            .or(CONFIG
                .openid_provider_uri
                .clone()
                .map(|uri| uri.to_string()))
            .unwrap_or(default_config.idp_authority),
        idp_client_id: std::env::var("LAKEKEEPER__UI__OPENID_CLIENT_ID")
            .unwrap_or(default_config.idp_client_id),
        idp_redirect_path: std::env::var("LAKEKEEPER__UI__OPENID_REDIRECT_PATH")
            .unwrap_or(default_config.idp_redirect_path),
        idp_scope: std::env::var("LAKEKEEPER__UI__OPENID_SCOPE")
            .unwrap_or(default_config.idp_scope),
        idp_resource: std::env::var("LAKEKEEPER__UI__OPENID_RESOURCE")
            .unwrap_or(default_config.idp_resource),
        idp_post_logout_redirect_path: std::env::var(
            "LAKEKEEPER__UI__OPENID_POST_LOGOUT_REDIRECT_PATH",
        )
        .unwrap_or(default_config.idp_post_logout_redirect_path),
        idp_post_logout_redirect_url: std::env::var(
            "LAKEKEEPER__UI__OPENID_POST_LOGOUT_REDIRECT_URL",
        )
        .unwrap_or(default_config.idp_post_logout_redirect_url),
        idp_disable_post_logout_redirect: std::env::var(
            "LAKEKEEPER__UI__OPENID_POST_LOGOUT_REDIRECT_DISABLED",
        )
        .ok()
        .and_then(|v| v.to_lowercase().parse::<bool>().ok())
        .unwrap_or(default_config.idp_disable_post_logout_redirect),
        idp_token_type: match std::env::var("LAKEKEEPER__UI__OPENID_TOKEN_TYPE").as_deref() {
            Ok("id_token") => lakekeeper_console::IdpTokenType::IdToken,
            Ok("access_token") | Err(VarError::NotPresent) => {
                lakekeeper_console::IdpTokenType::AccessToken
            }
            Ok(v) => {
                tracing::warn!(
                    "Unknown value `{v}` for LAKEKEEPER__UI__OPENID_TOKEN_TYPE, defaulting to AccessToken. Expected values are 'id_token' or 'access_token'.",
                );
                lakekeeper_console::IdpTokenType::AccessToken
            }
            Err(VarError::NotUnicode(_)) => {
                tracing::warn!(
                    "Non-Unicode value for LAKEKEEPER__UI__OPENID_TOKEN_TYPE, defaulting to AccessToken."
                );
                default_config.idp_token_type
            }
        },
        enable_authentication: CONFIG.ui_login_enabled(),
        enable_permissions: CONFIG.authz_backend != AuthZBackend::AllowAll,
        enable_user_surveys: std::env::var("LAKEKEEPER__UI__ENABLE_SURVEYS")
            .ok()
            .and_then(|v| v.to_lowercase().parse::<bool>().ok())
            .unwrap_or(default_config.enable_user_surveys),
        app_lakekeeper_url: std::env::var("LAKEKEEPER__UI__LAKEKEEPER_URL")
            .ok()
            .or(CONFIG.base_uri.as_ref().map(ToString::to_string)),
    };
    tracing::debug!("UI config: {:?}", config);
    config
});

/// The path of `LAKEKEEPER__BASE_URI`, if it has one. It is the console's path
/// prefix for every request and takes precedence over `X-Forwarded-Prefix`.
static CONFIGURED_PREFIX: LazyLock<Option<BasePrefix>> = LazyLock::new(|| {
    let path = CONFIG.base_uri.as_ref()?.path().trim_matches('/');
    if path.is_empty() {
        return None;
    }
    BasePrefix::parse(path)
        .inspect_err(|e| {
            tracing::warn!("Serving the UI without the path of LAKEKEEPER__BASE_URI: {e}");
        })
        .ok()
});

static CONSOLE: LazyLock<Console> = LazyLock::new(|| Console::new(&UI_CONFIG));

/// `index.html` depends on `X-Forwarded-Prefix`, so shared caches must not keep it.
const INDEX_CACHE_CONTROL: &str = "no-cache, private";

/// Every UI response varies by `Accept-Encoding`: a stored gzip file is sent
/// compressed or decompressed, other bodies pass the compression layer.
const VARY_ASSET: &str = "accept-encoding";
const VARY_INDEX: &str = "x-forwarded-prefix, accept-encoding";

/// Response extension: do not compress this body. Set on decompressed copies
/// of stored gzip files, whose clients do not accept gzip.
#[derive(Debug, Clone, Copy)]
struct SkipCompression;

/// The path prefix the console is mounted under for this request: the
/// configured one, else `X-Forwarded-Prefix`.
fn base_prefix(
    configured: Option<&BasePrefix>,
    headers: &HeaderMap,
) -> Result<BasePrefix, InvalidBasePrefix> {
    match configured {
        Some(prefix) => Ok(prefix.clone()),
        None => determine_forwarded_prefix(headers)
            .map_or_else(|| Ok(BasePrefix::root()), BasePrefix::parse),
    }
}

fn invalid_prefix_response(error: &InvalidBasePrefix) -> Response {
    tracing::debug!("Rejecting UI request: {X_FORWARDED_PREFIX_HEADER}: {error}");
    (StatusCode::BAD_REQUEST, "Invalid X-Forwarded-Prefix header").into_response()
}

/// `index.html`, served for `/ui/` and every client-side route below it.
pub(crate) async fn index_handler(headers: HeaderMap) -> Response {
    let prefix = match base_prefix(CONFIGURED_PREFIX.as_ref(), &headers) {
        Ok(prefix) => prefix,
        Err(e) => return invalid_prefix_response(&e),
    };
    let index = CONSOLE.index_html(&prefix);
    let mut cache_headers = HeaderMap::new();
    cache_headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(INDEX_CACHE_CONTROL),
    );
    cache_headers.insert(header::VARY, HeaderValue::from_static(VARY_INDEX));
    conditional_response(
        &headers,
        &index.etag,
        cache_headers,
        (
            [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
            index.body,
        ),
    )
}

pub(crate) async fn favicon_handler(headers: HeaderMap) -> Response {
    asset_response("favicon.ico", &headers)
}

/// Serve the built file identified by the request URI.
pub(crate) async fn static_handler(uri: Uri, headers: HeaderMap) -> Response {
    let path = uri.path().trim_start_matches('/');
    let path = path.strip_prefix("ui/").unwrap_or(path);
    asset_response(path, &headers)
}

/// A built file, unchanged: its bytes do not depend on the request. Files
/// stored gzip-compressed go out as they are to clients that accept gzip (the
/// compression layer skips bodies with a `Content-Encoding`) and decompressed,
/// but not compressed again, to all others.
fn asset_response(path: &str, req_headers: &HeaderMap) -> Response {
    let Some(asset) = CONSOLE.asset(path, accepts_gzip(req_headers)) else {
        return (StatusCode::NOT_FOUND, "404 Not Found").into_response();
    };
    let mut cache_headers = HeaderMap::new();
    cache_headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(cache_policy(path)),
    );
    cache_headers.insert(header::VARY, HeaderValue::from_static(VARY_ASSET));
    let mut content_headers = HeaderMap::new();
    if let Ok(mime) = HeaderValue::from_str(asset.mime.as_ref()) {
        content_headers.insert(header::CONTENT_TYPE, mime);
    }
    if asset.encoding == AssetEncoding::Gzip {
        content_headers.insert(header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    }
    let mut response = conditional_response(
        req_headers,
        &asset.etag,
        cache_headers,
        (content_headers, asset.body),
    );
    if asset.encoding == AssetEncoding::Decompressed {
        response.extensions_mut().insert(SkipCompression);
    }
    response
}

/// Whether the compression layer compresses a UI response: the default rules,
/// minus fonts (WOFF and WOFF2 are compressed formats) and [`SkipCompression`].
fn compress_ui_response() -> impl Predicate {
    fn not_skipped(_: StatusCode, _: Version, _: &HeaderMap, extensions: &Extensions) -> bool {
        extensions.get::<SkipCompression>().is_none()
    }
    DefaultPredicate::new()
        .and(NotForContentType::const_new("font/"))
        .and(not_skipped)
}

/// `304` if the request's `If-None-Match` matches `etag`, otherwise `200` with
/// `content`. Both carry `cache_headers` and the weak `ETag`, as RFC 9110 asks
/// for a `304`.
fn conditional_response(
    req_headers: &HeaderMap,
    etag: &str,
    mut cache_headers: HeaderMap,
    content: impl IntoResponse,
) -> Response {
    if let Ok(value) = HeaderValue::from_str(&format!("W/\"{etag}\"")) {
        cache_headers.insert(header::ETAG, value);
    }
    if if_none_match(req_headers, etag) {
        return (StatusCode::NOT_MODIFIED, cache_headers).into_response();
    }
    (cache_headers, content).into_response()
}

/// Whether the request accepts the gzip content coding (RFC 9110 12.5.3): an
/// explicit `gzip` (or `x-gzip`) entry decides by its weight, otherwise `*`.
fn accepts_gzip(headers: &HeaderMap) -> bool {
    let mut gzip = None;
    let mut wildcard = None;
    for entry in headers
        .get_all(header::ACCEPT_ENCODING)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
    {
        let mut parts = entry.split(';');
        let coding = parts.next().unwrap_or_default().trim();
        let weight = parts
            .find_map(|param| {
                let (name, value) = param.split_once('=')?;
                name.trim()
                    .eq_ignore_ascii_case("q")
                    .then(|| value.trim().parse::<f32>().unwrap_or(0.0))
            })
            .unwrap_or(1.0);
        if coding.eq_ignore_ascii_case("gzip") || coding.eq_ignore_ascii_case("x-gzip") {
            gzip = Some(weight);
        } else if coding == "*" {
            wildcard = Some(weight);
        }
    }
    gzip.or(wildcard).is_some_and(|weight| weight > 0.0)
}

/// `Cache-Control` for a built file, keyed on its (prefix-stripped) path. Every
/// response also carries a weak `ETag`, so stale entries revalidate with a `304`.
///
/// - `assets/*`: Vite puts a content hash into every file name, and no
///   configuration is written into these files, so a name always has the same
///   bytes → cache for a year without revalidation.
/// - `duckdb/*` and the worker wrapper: stable names whose bytes change with
///   the console version → revalidate on every use, so a browser never mixes an
///   old `DuckDB` core with the extension paths of a new one (a `304` avoids
///   re-downloading the multi-MB WASM).
/// - `favicon.ico`: rarely changes → cache a day.
fn cache_policy(path: &str) -> &'static str {
    if path.starts_with("assets/") {
        "public, max-age=31536000, immutable"
    } else if path.starts_with("duckdb/") || path == "duckdb-worker-wrapper.js" {
        "public, no-cache"
    } else if path == "favicon.ico" {
        "public, max-age=86400"
    } else {
        "no-cache"
    }
}

/// Whether the request's `If-None-Match` matches our bare weak `etag` under RFC
/// 9110 weak comparison, or is the `*` wildcard. Delegates parsing to the
/// catalog's [`parse_if_none_match`], which reads every field value (`get_all`);
/// each entry is then reduced to its bare opaque tag (dropping the `W/` marker
/// and quotes), so a client echoing either the weak or strong form matches.
fn if_none_match(headers: &HeaderMap, etag: &str) -> bool {
    parse_if_none_match(headers)
        .iter()
        .any(|tag| tag.is_wildcard() || tag.validator() == etag)
}

pub(crate) fn get_ui_router() -> Router {
    // Prepare at startup, so a broken console build fails here and an invalid
    // `LAKEKEEPER__BASE_URI` path is reported once.
    LazyLock::force(&CONSOLE);
    LazyLock::force(&CONFIGURED_PREFIX);
    Router::new()
        .route("/ui", get(redirect_to_ui))
        .route("/", get(redirect_to_ui))
        .route("/ui/index.html", get(redirect_to_ui))
        .route("/ui/", get(index_handler))
        .route("/ui/favicon.ico", get(favicon_handler))
        .route("/ui/duckdb-worker-wrapper.js", get(static_handler))
        .route("/ui/duckdb/{*file}", get(static_handler))
        .route("/ui/assets/{*file}", get(static_handler))
        .route("/ui/{*file}", get(index_handler))
        .layer(
            tower::ServiceBuilder::new()
                .set_x_request_id(MakeRequestUuid7)
                .layer(SetSensitiveHeadersLayer::new([
                    axum::http::header::AUTHORIZATION,
                ]))
                .layer(CompressionLayer::new().compress_when(compress_ui_response()))
                .layer(
                    TraceLayer::new_for_http()
                        .on_failure(())
                        .make_span_with(
                            RestMakeSpan::new(tracing::Level::INFO).with_log_authorization_header(
                                CONFIG.debug.log_authorization_header,
                            ),
                        )
                        .on_response(trace::DefaultOnResponse::new().level(tracing::Level::DEBUG)),
                )
                .layer(TimeoutLayer::with_status_code(
                    http::StatusCode::REQUEST_TIMEOUT,
                    std::time::Duration::from_secs(30),
                ))
                .layer(CatchPanicLayer::new())
                .propagate_x_request_id(),
        )
}

/// Redirect to the console under this request's path prefix. Temporary, so
/// that neither browsers nor shared caches keep it when the prefix changes.
async fn redirect_to_ui(headers: HeaderMap) -> Response {
    match base_prefix(CONFIGURED_PREFIX.as_ref(), &headers) {
        Ok(prefix) => Redirect::temporary(&prefix.ui_path()).into_response(),
        Err(e) => invalid_prefix_response(&e),
    }
}

#[cfg(test)]
mod test {
    use lakekeeper::{axum::body::Body, tokio, tower::ServiceExt};

    use super::*;

    async fn body_bytes(response: Response) -> axum::body::Bytes {
        axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("Failed to read response body")
    }

    fn header_str(response: &Response, name: header::HeaderName) -> Option<&str> {
        response
            .headers()
            .get(name)
            .map(|value| value.to_str().unwrap())
    }

    fn headers(entries: &[(&'static str, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in entries {
            headers.append(*name, value.parse().unwrap());
        }
        headers
    }

    /// Tests run without `LAKEKEEPER__BASE_URI`; `X-Forwarded-Prefix` counts only
    /// where x-forwarded handling is on.
    fn expected_ui_path(prefix: &str) -> &str {
        assert!(CONFIGURED_PREFIX.is_none());
        if CONFIG.use_x_forwarded_headers {
            prefix
        } else {
            "/ui/"
        }
    }

    #[tokio::test]
    async fn test_index_found() {
        let response = index_handler(HeaderMap::new()).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            header_str(&response, header::CONTENT_TYPE),
            Some("text/html; charset=utf-8")
        );
        assert_eq!(
            header_str(&response, header::CACHE_CONTROL),
            Some("no-cache, private")
        );
        assert_eq!(
            header_str(&response, header::VARY),
            Some("x-forwarded-prefix, accept-encoding")
        );
        let body = String::from_utf8(body_bytes(response).await.to_vec()).unwrap();
        assert!(body.contains(r#"<base href="/ui/">"#), "{body}");
        assert!(body.contains(r#"src="./assets/"#), "{body}");
        assert!(body.contains(r#"id="lakekeeper-config""#), "{body}");
    }

    #[tokio::test]
    async fn test_index_prefix() {
        let response = index_handler(headers(&[(X_FORWARDED_PREFIX_HEADER, "/lakekeeper")])).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = String::from_utf8(body_bytes(response).await.to_vec()).unwrap();
        let base = format!(r#"<base href="{}">"#, expected_ui_path("/lakekeeper/ui/"));
        assert!(body.contains(&base), "{body}");
    }

    #[tokio::test]
    async fn test_invalid_prefix_is_rejected() {
        if !CONFIG.use_x_forwarded_headers {
            return;
        }
        for prefix in ["\\evil.example", "a/../b", "a\"b", "<script>"] {
            let request = headers(&[(X_FORWARDED_PREFIX_HEADER, prefix)]);
            let response = index_handler(request.clone()).await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{prefix}");
            let response = redirect_to_ui(request).await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{prefix}");
        }
    }

    #[tokio::test]
    async fn test_redirect_to_ui() {
        let response = redirect_to_ui(HeaderMap::new()).await;
        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(header_str(&response, header::LOCATION), Some("/ui/"));

        let response = redirect_to_ui(headers(&[(X_FORWARDED_PREFIX_HEADER, "lakekeeper/")])).await;
        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(
            header_str(&response, header::LOCATION),
            Some(expected_ui_path("/lakekeeper/ui/"))
        );
    }

    /// No built file depends on request headers: 100 distinct `Host` and
    /// `X-Forwarded-*` values get the same bytes, so nothing is kept per value.
    /// The worker is stored gzip-compressed, the favicon plain.
    #[tokio::test]
    async fn test_assets_do_not_depend_on_request_headers() {
        for (path, accept_encoding) in [
            ("/ui/duckdb/duckdb-browser-eh.worker.js", "gzip"),
            ("/ui/duckdb/duckdb-browser-eh.worker.js", "identity"),
            ("/ui/favicon.ico", "gzip"),
        ] {
            let request = |i: usize| {
                let host = format!("host-{i}.example");
                let prefix = format!("prefix-{i}");
                headers(&[
                    ("host", &host),
                    ("x-forwarded-host", &host),
                    ("x-forwarded-proto", "https"),
                    (X_FORWARDED_PREFIX_HEADER, &prefix),
                    ("accept-encoding", accept_encoding),
                ])
            };
            let uri = path.parse::<Uri>().unwrap();
            let first = static_handler(uri.clone(), request(0)).await;
            assert_eq!(first.status(), StatusCode::OK, "{path}");
            let etag = header_str(&first, header::ETAG).unwrap().to_string();
            let body = body_bytes(first).await;
            for i in 1..100 {
                let response = static_handler(uri.clone(), request(i)).await;
                assert_eq!(header_str(&response, header::ETAG), Some(etag.as_str()));
                assert_eq!(body_bytes(response).await, body, "{path}");
            }
        }
    }

    #[tokio::test]
    async fn test_gzip_stored_asset_follows_accept_encoding() {
        let uri = "/ui/duckdb/duckdb-mvp.wasm".parse::<Uri>().unwrap();

        let gzip = static_handler(uri.clone(), headers(&[("accept-encoding", "gzip, br")])).await;
        assert_eq!(header_str(&gzip, header::CONTENT_ENCODING), Some("gzip"));
        assert_eq!(header_str(&gzip, header::VARY), Some("accept-encoding"));
        assert_eq!(
            header_str(&gzip, header::CONTENT_TYPE),
            Some("application/wasm")
        );
        let gzip_etag = header_str(&gzip, header::ETAG).unwrap().to_string();
        assert!(gzip_etag.ends_with("-gz\""), "{gzip_etag}");
        assert!(body_bytes(gzip).await.starts_with(&[0x1f, 0x8b]));

        let plain = static_handler(uri, headers(&[("accept-encoding", "gzip;q=0, *")])).await;
        assert_eq!(header_str(&plain, header::CONTENT_ENCODING), None);
        assert_eq!(header_str(&plain, header::VARY), Some("accept-encoding"));
        assert_eq!(
            header_str(&plain, header::ETAG),
            Some(gzip_etag.replace("-gz\"", "\"").as_str())
        );
        assert!(body_bytes(plain).await.starts_with(b"\0asm"));
    }

    #[test]
    fn test_configured_prefix_takes_precedence() {
        let configured = BasePrefix::parse("/from-base-uri").unwrap();
        for header_value in ["/other", "\\not-even-valid"] {
            let request = headers(&[(X_FORWARDED_PREFIX_HEADER, header_value)]);
            assert_eq!(
                base_prefix(Some(&configured), &request).unwrap().ui_path(),
                "/from-base-uri/ui/"
            );
        }
        assert_eq!(
            base_prefix(None, &HeaderMap::new()).unwrap().ui_path(),
            "/ui/"
        );
    }

    async fn through_router(path: &str, accept_encoding: &str) -> Response {
        let request = axum::http::Request::get(path)
            .header(header::ACCEPT_ENCODING, accept_encoding)
            .body(Body::empty())
            .unwrap();
        get_ui_router().oneshot(request).await.unwrap()
    }

    fn all_values(response: &Response, name: header::HeaderName) -> Vec<&str> {
        response
            .headers()
            .get_all(name)
            .iter()
            .map(|value| value.to_str().unwrap())
            .collect()
    }

    /// Through the full router, including the compression layer.
    #[tokio::test]
    async fn test_router_compression() {
        let worker = "/ui/duckdb/duckdb-browser-eh.worker.js";

        // A stored gzip file goes out as stored, compressed exactly once.
        let response = through_router(worker, "gzip, br").await;
        assert_eq!(all_values(&response, header::CONTENT_ENCODING), ["gzip"]);
        assert_eq!(all_values(&response, header::VARY), ["accept-encoding"]);
        assert!(body_bytes(response).await.starts_with(&[0x1f, 0x8b]));

        // Without gzip it is decompressed and not compressed again.
        let response = through_router(worker, "br, zstd").await;
        assert_eq!(header_str(&response, header::CONTENT_ENCODING), None);
        assert!(!body_bytes(response).await.starts_with(&[0x1f, 0x8b]));

        // A file stored plain is compressed by the layer.
        let response = through_router("/ui/duckdb-worker-wrapper.js", "br").await;
        assert_eq!(header_str(&response, header::CONTENT_ENCODING), Some("br"));
        assert_eq!(all_values(&response, header::VARY), ["accept-encoding"]);

        // index.html too, with a single Vary.
        let response = through_router("/ui/", "br").await;
        assert_eq!(header_str(&response, header::CONTENT_ENCODING), Some("br"));
        assert_eq!(
            all_values(&response, header::VARY),
            ["x-forwarded-prefix, accept-encoding"]
        );
    }

    #[test]
    fn test_accepts_gzip() {
        let accepts = |value: &str| accepts_gzip(&headers(&[("accept-encoding", value)]));
        assert!(accepts("gzip"));
        assert!(accepts("gzip, deflate, br, zstd"));
        assert!(accepts("br;q=1.0, GZIP;q=0.5"));
        assert!(accepts("x-gzip"));
        assert!(accepts("*"));
        assert!(!accepts("identity"));
        assert!(!accepts("br, zstd"));
        assert!(!accepts("gzip;q=0"));
        assert!(!accepts("gzip; q=0.0, *"));
        assert!(!accepts("gzip;q=nonsense"));
        assert!(!accepts_gzip(&HeaderMap::new()));
        // Spread across multiple header field lines.
        let mut multi = headers(&[("accept-encoding", "br")]);
        multi.append(header::ACCEPT_ENCODING, "gzip".parse().unwrap());
        assert!(accepts_gzip(&multi));
    }

    #[test]
    fn test_cache_policy() {
        assert_eq!(
            cache_policy("assets/index-abc12345.js"),
            "public, max-age=31536000, immutable"
        );
        assert_eq!(cache_policy("duckdb/duckdb-eh.wasm"), "public, no-cache");
        assert_eq!(
            cache_policy("duckdb/extensions/v1.5.5/wasm_eh/iceberg.duckdb_extension.wasm"),
            "public, no-cache"
        );
        assert_eq!(cache_policy("duckdb-worker-wrapper.js"), "public, no-cache");
        assert_eq!(cache_policy("favicon.ico"), "public, max-age=86400");
    }

    #[test]
    fn test_if_none_match() {
        // `etag` is our bare opaque tag; clients echo the weak (or strong) form.
        let with = |v: &str| {
            let mut h = HeaderMap::new();
            h.append(header::IF_NONE_MATCH, v.parse().unwrap());
            h
        };
        assert!(if_none_match(&with("W/\"deadbeef\""), "deadbeef"));
        // Weak comparison: the strong form of the same tag also matches.
        assert!(if_none_match(&with("\"deadbeef\""), "deadbeef"));
        assert!(if_none_match(&with("*"), "deadbeef"));
        // Only the bare token is the wildcard: `"*"` is a tag whose opaque value
        // is `*`, and ours never is.
        assert!(!if_none_match(&with("\"*\""), "deadbeef"));
        assert!(!if_none_match(&with("W/\"*\""), "deadbeef"));
        // Present in a comma-separated list.
        assert!(if_none_match(
            &with("W/\"other\", W/\"deadbeef\""),
            "deadbeef"
        ));
        // Spread across multiple header field lines (get_all, not just get).
        let mut multi = HeaderMap::new();
        multi.append(header::IF_NONE_MATCH, "W/\"other\"".parse().unwrap());
        multi.append(header::IF_NONE_MATCH, "W/\"deadbeef\"".parse().unwrap());
        assert!(if_none_match(&multi, "deadbeef"));
        // No / non-matching validator.
        assert!(!if_none_match(&HeaderMap::new(), "deadbeef"));
        assert!(!if_none_match(&with("W/\"other\""), "deadbeef"));
    }

    #[tokio::test]
    async fn test_static_asset_cache_headers() {
        // `duckdb-worker-wrapper.js` is a real embedded asset on a revalidating path.
        let uri = "/ui/duckdb-worker-wrapper.js".parse::<Uri>().unwrap();
        let response = static_handler(uri, HeaderMap::new()).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            header_str(&response, header::CACHE_CONTROL),
            Some("public, no-cache")
        );
        let etag = header_str(&response, header::ETAG)
            .expect("ETag header missing")
            .to_string();
        assert!(etag.starts_with("W/\""), "expected a weak ETag, got {etag}");

        // Re-requesting with the returned ETag yields a bodyless 304.
        let uri = "/ui/duckdb-worker-wrapper.js".parse::<Uri>().unwrap();
        let response = static_handler(uri, headers(&[("if-none-match", &etag)])).await;
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(header_str(&response, header::ETAG), Some(etag.as_str()));
        assert_eq!(
            header_str(&response, header::CACHE_CONTROL),
            Some("public, no-cache")
        );
        assert!(
            body_bytes(response).await.is_empty(),
            "304 response must have an empty body"
        );
    }

    #[tokio::test]
    async fn test_index_not_modified() {
        let etag = header_str(&index_handler(HeaderMap::new()).await, header::ETAG)
            .unwrap()
            .to_string();
        let response = index_handler(headers(&[("if-none-match", &etag)])).await;
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(
            header_str(&response, header::VARY),
            Some("x-forwarded-prefix, accept-encoding")
        );
        assert!(body_bytes(response).await.is_empty());
    }

    #[tokio::test]
    async fn nested_ui_path_segment_is_preserved() {
        let existing = static_handler(
            "/ui/duckdb/duckdb-eh.wasm".parse::<Uri>().unwrap(),
            HeaderMap::new(),
        )
        .await;
        assert_eq!(existing.status(), StatusCode::OK);

        let nested = static_handler(
            "/ui/duckdb/ui/duckdb-eh.wasm".parse::<Uri>().unwrap(),
            HeaderMap::new(),
        )
        .await;
        assert_eq!(nested.status(), StatusCode::NOT_FOUND);
    }
}
