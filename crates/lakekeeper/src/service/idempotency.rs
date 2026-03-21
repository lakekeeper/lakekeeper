use http::{HeaderMap, StatusCode};
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
use uuid::Uuid;

pub const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";

/// A validated idempotency key extracted from the `Idempotency-Key` HTTP header.
#[derive(Debug, Clone, Copy)]
pub struct IdempotencyKey(Uuid);

impl IdempotencyKey {
    /// Parse an idempotency key from a header value string.
    /// Returns `None` if the string is not a valid UUID.
    pub fn parse(value: &str) -> Option<Self> {
        Uuid::parse_str(value).ok().map(Self)
    }

    #[must_use]
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }

    /// Extract an idempotency key from request headers.
    ///
    /// Returns `Ok(None)` if the header is absent (opt-in).
    /// Returns `Ok(Some(key))` if the header is present and a valid UUID.
    /// Returns `Err` (400) if the header is present but not a valid UUID.
    pub fn from_headers(headers: &HeaderMap) -> Result<Option<Self>, IcebergErrorResponse> {
        let mut values = headers.get_all(IDEMPOTENCY_KEY_HEADER).iter();
        let Some(value) = values.next() else {
            return Ok(None);
        };
        if values.next().is_some() {
            return Err(ErrorModel::bad_request(
                "Multiple Idempotency-Key headers are not allowed",
                "DuplicateIdempotencyKey",
                None,
            )
            .into());
        }

        let value_str = value.to_str().map_err(|_| {
            ErrorModel::bad_request(
                "Idempotency-Key header must be a valid UTF-8 string",
                "InvalidIdempotencyKey",
                None,
            )
        })?;

        let value_str = value_str.trim();
        if value_str.is_empty() {
            return Ok(None);
        }

        Self::parse(value_str).map(Some).ok_or_else(|| {
            ErrorModel::bad_request(
                "Idempotency-Key header must be a valid UUID (RFC 9562)",
                "InvalidIdempotencyKey",
                None,
            )
            .into()
        })
    }
}

/// Result of checking an idempotency key before the mutation.
///
/// With the in-transaction design, records only exist for committed successes.
/// There is no "in-progress" state and no stored error bodies.
#[derive(Debug)]
pub enum IdempotencyCheck {
    /// No existing record — proceed with the mutation.
    NewRequest,
    /// Finalized with success (2xx) — handler should re-derive the response.
    ReplaySuccess { http_status: StatusCode },
    /// Finalized with 204 — return 204 No Content.
    ReplayNoContent,
}

impl IdempotencyCheck {
    /// Returns `true` if this is a replay (handler should re-derive the success response
    /// instead of executing the mutation).
    #[must_use]
    pub fn is_replay(&self) -> bool {
        matches!(
            self,
            IdempotencyCheck::ReplaySuccess { .. } | IdempotencyCheck::ReplayNoContent
        )
    }
}

#[cfg(test)]
mod tests {
    use http::{HeaderMap, HeaderValue};

    use super::*;

    #[test]
    fn parse_valid_uuid_v4() {
        let key = IdempotencyKey::parse("550e8400-e29b-41d4-a716-446655440000");
        assert!(key.is_some());
        assert_eq!(
            key.unwrap().as_uuid().to_string(),
            "550e8400-e29b-41d4-a716-446655440000"
        );
    }

    #[test]
    fn parse_invalid_string() {
        assert!(IdempotencyKey::parse("not-a-uuid").is_none());
    }

    #[test]
    fn parse_empty_string() {
        assert!(IdempotencyKey::parse("").is_none());
    }

    #[test]
    fn from_headers_absent() {
        let headers = HeaderMap::new();
        let result = IdempotencyKey::from_headers(&headers).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn from_headers_valid_uuid() {
        let mut headers = HeaderMap::new();
        headers.insert(
            IDEMPOTENCY_KEY_HEADER,
            HeaderValue::from_static("550e8400-e29b-41d4-a716-446655440000"),
        );
        let result = IdempotencyKey::from_headers(&headers).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn from_headers_valid_uuid_with_whitespace() {
        let mut headers = HeaderMap::new();
        headers.insert(
            IDEMPOTENCY_KEY_HEADER,
            HeaderValue::from_static("  550e8400-e29b-41d4-a716-446655440000  "),
        );
        let result = IdempotencyKey::from_headers(&headers).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn from_headers_empty_string() {
        let mut headers = HeaderMap::new();
        headers.insert(IDEMPOTENCY_KEY_HEADER, HeaderValue::from_static(""));
        let result = IdempotencyKey::from_headers(&headers).unwrap();
        assert!(result.is_none(), "Empty header should be treated as absent");
    }

    #[test]
    fn from_headers_whitespace_only() {
        let mut headers = HeaderMap::new();
        headers.insert(IDEMPOTENCY_KEY_HEADER, HeaderValue::from_static("   "));
        let result = IdempotencyKey::from_headers(&headers).unwrap();
        assert!(
            result.is_none(),
            "Whitespace-only header should be treated as absent"
        );
    }

    #[test]
    fn from_headers_invalid_uuid() {
        let mut headers = HeaderMap::new();
        headers.insert(
            IDEMPOTENCY_KEY_HEADER,
            HeaderValue::from_static("not-a-uuid"),
        );
        let result = IdempotencyKey::from_headers(&headers);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.error.code, 400);
    }

    #[test]
    fn from_headers_duplicate_keys_rejected() {
        let mut headers = HeaderMap::new();
        headers.append(
            IDEMPOTENCY_KEY_HEADER,
            HeaderValue::from_static("550e8400-e29b-41d4-a716-446655440000"),
        );
        headers.append(
            IDEMPOTENCY_KEY_HEADER,
            HeaderValue::from_static("660e8400-e29b-41d4-a716-446655440000"),
        );
        let result = IdempotencyKey::from_headers(&headers);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.error.code, 400);
        assert!(err.error.message.contains("Multiple"));
    }

    #[test]
    fn idempotency_check_is_replay() {
        assert!(!IdempotencyCheck::NewRequest.is_replay());
        assert!(
            IdempotencyCheck::ReplaySuccess {
                http_status: StatusCode::OK
            }
            .is_replay()
        );
        assert!(IdempotencyCheck::ReplayNoContent.is_replay());
    }
}

/// Parameters for inserting an idempotency key inside a transaction.
/// Groups the key with its endpoint and success status to avoid passing
/// them as separate arguments through the call chain.
#[derive(Debug, Clone, typed_builder::TypedBuilder)]
pub struct IdempotencyInfo {
    pub key: IdempotencyKey,
    pub endpoint: crate::api::endpoints::EndpointFlat,
    pub http_status: StatusCode,
}
