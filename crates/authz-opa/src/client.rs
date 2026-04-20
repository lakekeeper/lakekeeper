//! HTTP wiring and factory for [`OpaAuthorizer`].

use std::time::Duration;

use http::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use lakekeeper::service::ServerId;
use reqwest::Client;
use url::Url;

use crate::{AUTH_CONFIG, OpaAuthorizer, OpaError, config::OpaConfig, error::OpaResult};

/// Build an [`OpaAuthorizer`] from the globally loaded [`crate::CONFIG`].
///
/// # Errors
/// Fails if the HTTP client cannot be constructed with the configured
/// timeout / bearer token, or the decision URL is malformed.
pub async fn new_authorizer_from_default_config(server_id: ServerId) -> OpaResult<OpaAuthorizer> {
    OpaAuthorizer::new(AUTH_CONFIG.clone(), server_id)
}

impl OpaAuthorizer {
    /// Explicit constructor — tests and the default-config factory both go
    /// through this single path.
    ///
    /// # Errors
    /// Fails if the HTTP client cannot be constructed with the configured
    /// timeout / bearer token, or the decision URL is malformed.
    pub fn new(config: OpaConfig, server_id: ServerId) -> OpaResult<Self> {
        let decision_url = build_decision_url(&config.endpoint, &config.policy_path)?;

        let mut headers = HeaderMap::new();
        if let Some(token) = config.bearer_token.as_deref() {
            let mut value = HeaderValue::from_str(&format!("Bearer {token}"))?;
            value.set_sensitive(true);
            headers.insert(AUTHORIZATION, value);
        }

        let http = Client::builder()
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .default_headers(headers)
            .build()
            .map_err(OpaError::Http)?;

        Ok(Self::new_with_client(http, decision_url, config, server_id))
    }
}

fn build_decision_url(endpoint: &Url, policy_path: &str) -> OpaResult<Url> {
    let trimmed = policy_path.trim_matches('/');
    let mut base = endpoint.clone();
    // Ensure the base has a trailing slash so join() appends rather than replaces.
    if !base.path().ends_with('/') {
        let path = format!("{}/", base.path());
        base.set_path(&path);
    }
    Ok(base.join(&format!("v1/data/{trimmed}"))?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decision_url_joins_cleanly() {
        let endpoint = Url::parse("http://opa.local:8181").unwrap();
        let url = build_decision_url(&endpoint, "lakekeeper/authz/allow").unwrap();
        assert_eq!(
            url.as_str(),
            "http://opa.local:8181/v1/data/lakekeeper/authz/allow"
        );
    }

    #[test]
    fn decision_url_tolerates_path_slashes() {
        let endpoint = Url::parse("http://opa.local:8181/").unwrap();
        let url = build_decision_url(&endpoint, "/lakekeeper/authz/allow/").unwrap();
        assert_eq!(
            url.as_str(),
            "http://opa.local:8181/v1/data/lakekeeper/authz/allow"
        );
    }
}
