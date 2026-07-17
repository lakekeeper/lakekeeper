use std::{default::Default, env::VarError, sync::LazyLock};

use lakekeeper::{
    AuthZBackend, CONFIG, X_FORWARDED_PREFIX_HEADER,
    api::iceberg::v1::tables::parse_if_none_match,
    axum,
    axum::{
        Router,
        http::{HeaderMap, StatusCode, Uri, header},
        response::{IntoResponse, Response},
        routing::get,
    },
    determine_base_uri,
    request_tracing::{MakeRequestUuid7, RestMakeSpan},
    tower,
    tower_http::{
        ServiceBuilderExt,
        catch_panic::CatchPanicLayer,
        compression::CompressionLayer,
        sensitive_headers::SetSensitiveHeadersLayer,
        timeout::TimeoutLayer,
        trace::{self, TraceLayer},
    },
    tracing,
};
use lakekeeper_console::{CacheItem, FileCache, LakekeeperConsoleConfig};

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
        base_url_prefix: CONFIG.base_uri.as_ref().and_then(|uri| {
            let path_stripped = uri.path().trim_matches('/');
            if path_stripped.is_empty() {
                None
            } else {
                Some(format!("/{path_stripped}"))
            }
        }),
    };
    tracing::debug!("UI config: {:?}", config);
    config
});

// Create a global file cache initialized with the UI config
static FILE_CACHE: LazyLock<FileCache> = LazyLock::new(|| FileCache::new(UI_CONFIG.clone()));

// We use static route matchers ("/" and "/index.html") to serve our home page
pub(crate) async fn index_handler(headers: HeaderMap) -> impl IntoResponse {
    static_handler("/index.html".parse::<Uri>().unwrap(), headers).await
}

pub(crate) async fn favicon_handler(headers: HeaderMap) -> impl IntoResponse {
    static_handler("/favicon.ico".parse::<Uri>().unwrap(), headers).await
}

// Handler for static assets
pub(crate) async fn static_handler(uri: Uri, headers: HeaderMap) -> impl IntoResponse {
    let mut path = uri.path().trim_start_matches('/').to_string();

    if path.starts_with("ui/") {
        path = path.replace("ui/", "");
    }

    let forwarded_prefix = forwarded_prefix(&headers);
    let lakekeeper_base_uri = determine_base_uri(&headers);

    tracing::trace!(
        "Serving static file: path={}, forwarded_prefix={:?}, lakekeeper_base_uri={:?}",
        path,
        forwarded_prefix,
        lakekeeper_base_uri
    );
    cache_item_to_response(
        &path,
        &headers,
        FILE_CACHE.get_file(&path, forwarded_prefix, lakekeeper_base_uri.as_deref()),
    )
}

fn forwarded_prefix(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(X_FORWARDED_PREFIX_HEADER)
        .and_then(|hv| hv.to_str().ok())
}

/// `Cache-Control` value for a static asset, keyed on its (prefix-stripped) path.
/// Every response also carries a weak `ETag`, so all of these revalidate with a
/// cheap `304` once stale.
///
/// Note we cannot mark anything `immutable`: the console rewrites config
/// placeholders (IDP settings, base URL/prefix, …) into `.js`/`.css`/`.html`
/// files — including Vite content-hashed `assets/*` — at serve time, so the
/// bytes behind a given filename change whenever the deployment's config
/// changes, without the filename changing. A bounded `max-age` bounds how long
/// a returning browser can serve stale config after a restart.
///
/// - `assets/*`, `duckdb/*` and the worker wrapper: stable filenames but
///   config-templated (or version-bumped) bytes → cache for an hour, then
///   revalidate. Within the hour there are no requests; afterwards a `304`
///   avoids re-downloading (e.g. the multi-MB WASM on a `LoQE` open).
/// - `favicon.ico`: rarely changes → cache a day.
/// - `index.html` (and the SPA fallback that serves it) is templated per request
///   → always revalidate.
fn cache_policy(path: &str) -> &'static str {
    if path.starts_with("assets/")
        || path.starts_with("duckdb/")
        || path == "duckdb-worker-wrapper.js"
    {
        "public, max-age=3600, must-revalidate"
    } else if path == "favicon.ico" {
        "public, max-age=86400"
    } else {
        "no-cache"
    }
}

/// Whether the request's `If-None-Match` matches our bare weak `etag` under RFC
/// 9110 weak comparison, or is the `*` wildcard. Delegates parsing to the
/// catalog's [`parse_if_none_match`], which reads every field value (`get_all`)
/// and normalises each entry to its bare opaque tag (dropping the `W/` marker
/// and quotes) — so a client echoing either the weak or strong form matches.
fn if_none_match(headers: &HeaderMap, etag: &str) -> bool {
    parse_if_none_match(headers)
        .iter()
        .any(|tag| tag.as_str() == "*" || tag.as_str() == etag)
}

fn cache_item_to_response(path: &str, req_headers: &HeaderMap, item: CacheItem) -> Response {
    match item {
        CacheItem::NotFound => (StatusCode::NOT_FOUND, "404 Not Found").into_response(),
        CacheItem::Found { mime, data, etag } => {
            let cache_control = cache_policy(path);
            // `etag` is the bare opaque tag, precomputed once per cache entry by
            // the console (weak — the compression layer re-encodes the body).
            let etag_header = format!("W/\"{etag}\"");
            if if_none_match(req_headers, &etag) {
                return (
                    StatusCode::NOT_MODIFIED,
                    [
                        (header::CACHE_CONTROL, cache_control),
                        (header::ETAG, etag_header.as_str()),
                    ],
                )
                    .into_response();
            }
            (
                [
                    (header::CONTENT_TYPE, mime.as_ref()),
                    (header::CACHE_CONTROL, cache_control),
                    (header::ETAG, etag_header.as_str()),
                ],
                data,
            )
                .into_response()
        }
    }
}

pub(crate) fn get_ui_router() -> Router {
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
                .layer(CompressionLayer::new())
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

async fn redirect_to_ui(headers: axum::http::HeaderMap) -> axum::response::Redirect {
    if let Some(prefix) = lakekeeper::determine_forwarded_prefix(&headers) {
        axum::response::Redirect::permanent(format!("/{prefix}/ui/").as_str())
    } else {
        axum::response::Redirect::permanent("/ui/")
    }
}

#[cfg(test)]
mod test {
    use lakekeeper::tokio;

    use super::*;

    #[tokio::test]
    async fn test_index_found() {
        let headers = HeaderMap::new();
        let response = index_handler(headers).await.into_response();
        assert_eq!(response.status(), 200);
        let body = response.into_body();
        let body_str = String::from_utf8(
            axum::body::to_bytes(body, 10000)
                .await
                .expect("Failed to read response body")
                .to_vec(),
        )
        .unwrap();
        assert!(body_str.contains("\"/ui/assets/"));
    }

    #[tokio::test]
    async fn test_index_prefix() {
        let mut headers = HeaderMap::new();
        headers.append(X_FORWARDED_PREFIX_HEADER, "/lakekeeper".parse().unwrap());
        let response = index_handler(headers).await.into_response();
        assert_eq!(response.status(), 200);
        let body = response.into_body();
        let body_str = String::from_utf8(
            axum::body::to_bytes(body, 10000)
                .await
                .expect("Failed to read response body")
                .to_vec(),
        )
        .unwrap();
        assert!(body_str.contains("\"/lakekeeper/ui/assets/"));
    }

    #[test]
    fn test_cache_policy() {
        // Config-templated / version-bumped: bounded cache + revalidate.
        assert_eq!(
            cache_policy("assets/app.config-abc123.js"),
            "public, max-age=3600, must-revalidate"
        );
        assert_eq!(
            cache_policy("duckdb/duckdb-eh.wasm"),
            "public, max-age=3600, must-revalidate"
        );
        assert_eq!(
            cache_policy("duckdb-worker-wrapper.js"),
            "public, max-age=3600, must-revalidate"
        );
        // Nothing is `immutable`: templating can change bytes under a stable name.
        assert!(!cache_policy("assets/app.config-abc123.js").contains("immutable"));
        // Favicon: longer cache.
        assert_eq!(cache_policy("favicon.ico"), "public, max-age=86400");
        // Per-request templated entry point: never cached without revalidation.
        assert_eq!(cache_policy("index.html"), "no-cache");
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
        let response = static_handler(uri, HeaderMap::new()).await.into_response();
        assert_eq!(response.status(), StatusCode::OK);

        let cache_control = response
            .headers()
            .get(header::CACHE_CONTROL)
            .expect("Cache-Control header missing")
            .to_str()
            .unwrap()
            .to_string();
        assert_eq!(cache_control, "public, max-age=3600, must-revalidate");

        let etag = response
            .headers()
            .get(header::ETAG)
            .expect("ETag header missing")
            .to_str()
            .unwrap()
            .to_string();
        assert!(etag.starts_with("W/\""), "expected a weak ETag, got {etag}");

        // Re-requesting with the returned ETag yields a bodyless 304.
        let mut headers = HeaderMap::new();
        headers.append(header::IF_NONE_MATCH, etag.parse().unwrap());
        let uri = "/ui/duckdb-worker-wrapper.js".parse::<Uri>().unwrap();
        let response = static_handler(uri, headers).await.into_response();
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(
            response
                .headers()
                .get(header::ETAG)
                .unwrap()
                .to_str()
                .unwrap(),
            etag
        );
        let body = axum::body::to_bytes(response.into_body(), 10000)
            .await
            .unwrap();
        assert!(body.is_empty(), "304 response must have an empty body");
    }
}
