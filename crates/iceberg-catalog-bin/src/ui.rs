use std::{default::Default, sync::LazyLock};

use axum::{
    http::{header, HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
};
use iceberg_catalog::{determine_base_uri, AuthZBackend, CONFIG, X_FORWARDED_PREFIX_HEADER};
use lakekeeper_console::{CacheItem, FileCache, LakekeeperConsoleConfig};

// Static configuration for UI
static UI_CONFIG: LazyLock<LakekeeperConsoleConfig> = LazyLock::new(|| {
    let default_config = LakekeeperConsoleConfig::default();
    LakekeeperConsoleConfig {
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
        enable_authentication: CONFIG.openid_provider_uri.is_some(),
        enable_permissions: CONFIG.authz_backend == AuthZBackend::OpenFGA,
        app_lakekeeper_url: std::env::var("LAKEKEEPER__UI__LAKEKEEPER_URL")
            .ok()
            .or(CONFIG.base_uri.as_ref().map(ToString::to_string)),
        base_url_prefix: CONFIG.base_uri.as_ref().and_then(|uri| {
            let path_stripped = uri.path().trim_matches('/');
            if path_stripped.is_empty() {
                None
            } else {
                Some(format!("/{}", path_stripped))
            }
        }),
    }
});

// Create a global file cache initialized with the UI config
static FILE_CACHE: LazyLock<FileCache> = LazyLock::new(|| FileCache::new(UI_CONFIG.clone()));

// We use static route matchers ("/" and "/index.html") to serve our home page
pub async fn index_handler(headers: HeaderMap) -> impl IntoResponse {
    static_handler("/index.html".parse::<Uri>().unwrap(), headers).await
}

pub async fn favicon_handler(headers: HeaderMap) -> impl IntoResponse {
    static_handler("/favicon.ico".parse::<Uri>().unwrap(), headers).await
}

// Handler for static assets
pub async fn static_handler(uri: Uri, headers: HeaderMap) -> impl IntoResponse {
    let mut path = uri.path().trim_start_matches('/').to_string();

    if path.starts_with("ui/") {
        path = path.replace("ui/", "");
    }

    let forwarded_prefix = forwarded_prefix(&headers);

    let lakekeeper_base_uri = determine_base_uri(&headers);
    cache_item_to_response(FILE_CACHE.get_file(
        &path,
        forwarded_prefix,
        lakekeeper_base_uri.as_deref(),
    ))
}

fn forwarded_prefix(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(X_FORWARDED_PREFIX_HEADER)
        .and_then(|hv| hv.to_str().ok())
}

fn cache_item_to_response(item: CacheItem) -> Response {
    match item {
        CacheItem::NotFound => (StatusCode::NOT_FOUND, "404 Not Found").into_response(),
        CacheItem::Found { mime, data } => {
            ([(header::CONTENT_TYPE, mime.as_ref())], data).into_response()
        }
    }
}

#[cfg(test)]
mod test {
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
}
