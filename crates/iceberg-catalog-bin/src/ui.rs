use axum::{
    http::{header, StatusCode, Uri},
    response::{IntoResponse, Response},
};
use iceberg_catalog::CONFIG;
use lakekeeper_console::{get_file, LakekeeperConsoleConfig};
use std::cell::LazyCell;
use std::default::Default;

// Static configuration for UI
#[allow(clippy::declare_interior_mutable_const)]
const UI_CONFIG: LazyCell<LakekeeperConsoleConfig> = LazyCell::new(|| {
    let default_config = LakekeeperConsoleConfig::default();
    LakekeeperConsoleConfig {
        idp_authority: std::env::var("LAKEKEEPER__UI__IDP_AUTHORITY")
            .ok()
            .or(CONFIG
                .openid_provider_uri
                .clone()
                .map(|uri| uri.to_string()))
            .unwrap_or(default_config.idp_authority),
        idp_client_id: std::env::var("LAKEKEEPER__UI__IDP_CLIENT_ID")
            .unwrap_or(default_config.idp_client_id),
        idp_redirect_path: std::env::var("LAKEKEEPER__UI__IDP_REDIRECT_PATH")
            .unwrap_or(default_config.idp_redirect_path),
        idp_scope: std::env::var("LAKEKEEPER__UI__IDP_SCOPE").unwrap_or(default_config.idp_scope),
        idp_resource: std::env::var("LAKEKEEPER__UI__IDP_RESOURCE")
            .unwrap_or(default_config.idp_resource),
        idp_post_logout_redirect_path: std::env::var(
            "LAKEKEEPER__UI__IDP_POST_LOGOUT_REDIRECT_PATH",
        )
        .unwrap_or(default_config.idp_post_logout_redirect_path),
        enable_authorization: CONFIG.openid_provider_uri.is_some(),
        app_iceberg_catalog_url: std::env::var("LAKEKEEPER__UI__ICEBERG_CATALOG_URL").unwrap_or(
            CONFIG
                .base_uri
                .to_string()
                .trim_end_matches('/')
                .to_string(),
        ),
    }
});

#[derive(Debug, Clone)]
enum CacheItem {
    NotFound,
    Found {
        mime: mime_guess::Mime,
        data: std::borrow::Cow<'static, [u8]>,
    },
}

const FILE_CACHE: LazyCell<moka::sync::Cache<String, CacheItem>> = LazyCell::new(|| {
    let cache = moka::sync::Cache::new(1000);
    cache
});

// We use static route matchers ("/" and "/index.html") to serve our home
// page.
pub async fn index_handler() -> impl IntoResponse {
    static_handler("/index.html".parse::<Uri>().unwrap()).await
}

// We use a wildcard matcher ("/dist/*file") to match against everything
// within our defined assets directory. This is the directory on our Asset
// struct below, where folder = "examples/public/".
pub async fn static_handler(uri: Uri) -> impl IntoResponse {
    let mut path = uri.path().trim_start_matches('/').to_string();

    if path.starts_with("ui/") {
        path = path.replace("ui/", "");
    }

    StaticFile(path)
}

fn get_file_cached(file_path: &str) -> Response {
    let cached = FILE_CACHE.get(file_path);

    if let Some(cache_item) = cached {
        cache_item.into_response()
    } else {
        let mime = mime_guess::from_path(file_path).first_or_octet_stream();
        let content = get_file(file_path, &UI_CONFIG);

        let cache_item = match content {
            Some(content) => CacheItem::Found {
                mime: mime.clone(),
                data: content.data,
            },
            None => CacheItem::NotFound,
        };
        FILE_CACHE.insert(file_path.to_string(), cache_item.clone());

        cache_item.into_response()
    }
}

pub struct StaticFile<T>(pub T);

impl IntoResponse for CacheItem {
    fn into_response(self) -> Response {
        match self {
            CacheItem::NotFound => (StatusCode::NOT_FOUND, "404 Not Found").into_response(),
            CacheItem::Found { mime, data } => {
                ([(header::CONTENT_TYPE, mime.as_ref())], data).into_response()
            }
        }
    }
}

impl<T> IntoResponse for StaticFile<T>
where
    T: Into<String>,
{
    fn into_response(self) -> Response {
        let path = self.0.into();
        get_file_cached(&path)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_index_found() {
        let response = index_handler().await.into_response();
        assert_eq!(response.status(), 200);
    }
}
