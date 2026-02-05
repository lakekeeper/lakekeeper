pub mod audit;

use std::fmt::{Debug, Display, Formatter, Result};

use url::Url;

/// A wrapper around `Url` that redacts credentials (username/password) when displayed.
///
/// This should be used for logging URLs that may contain sensitive information.
/// For example, `nats://user:password@host:4222` becomes `nats://[REDACTED]@host:4222`.
#[derive(Clone)]
pub struct RedactedUrl(Url);

impl Debug for RedactedUrl {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        // Delegate to Display to ensure credentials are never leaked via Debug
        write!(f, "RedactedUrl({self})")
    }
}

impl RedactedUrl {
    /// Create a new `RedactedUrl` from a `Url`.
    #[must_use]
    pub fn new(url: Url) -> Self {
        Self(url)
    }

    /// Get a reference to the inner `Url`.
    #[must_use]
    pub fn inner(&self) -> &Url {
        &self.0
    }

    /// Consume this `RedactedUrl` and return the inner `Url`.
    #[must_use]
    pub fn into_inner(self) -> Url {
        self.0
    }
}

impl From<Url> for RedactedUrl {
    fn from(url: Url) -> Self {
        Self::new(url)
    }
}

impl Display for RedactedUrl {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let url = &self.0;

        // Check if URL has any credentials
        let has_password = url.password().is_some();
        let has_username = !url.username().is_empty();

        if !has_password && !has_username {
            // No credentials, display as-is
            return write!(f, "{url}");
        }

        // Build URL without credentials
        let scheme = url.scheme();
        let host = url.host_str().unwrap_or("");
        let port = url.port().map_or(String::new(), |p| format!(":{p}"));
        let path = url.path();
        let query = url.query().map_or(String::new(), |q| format!("?{q}"));
        let fragment = url.fragment().map_or(String::new(), |fr| format!("#{fr}"));

        write!(
            f,
            "{scheme}://[REDACTED]@{host}{port}{path}{query}{fragment}"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redacted_url_no_credentials() {
        let url = Url::parse("nats://localhost:4222").unwrap();
        let redacted = RedactedUrl::new(url);
        assert_eq!(redacted.to_string(), "nats://localhost:4222");
    }

    #[test]
    fn test_redacted_url_with_password() {
        let url = Url::parse("nats://user:secret@localhost:4222").unwrap();
        let redacted = RedactedUrl::new(url);
        assert_eq!(redacted.to_string(), "nats://[REDACTED]@localhost:4222");
    }

    #[test]
    fn test_redacted_url_with_username_only() {
        let url = Url::parse("nats://token@localhost:4222").unwrap();
        let redacted = RedactedUrl::new(url);
        assert_eq!(redacted.to_string(), "nats://[REDACTED]@localhost:4222");
    }

    #[test]
    fn test_redacted_url_with_path_and_query() {
        let url = Url::parse("https://user:pass@example.com:8080/path?query=1#frag").unwrap();
        let redacted = RedactedUrl::new(url);
        assert_eq!(
            redacted.to_string(),
            "https://[REDACTED]@example.com:8080/path?query=1#frag"
        );
    }
}
