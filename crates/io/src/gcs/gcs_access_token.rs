//! `OAuth2` access tokens with a known expiry, used as subject token for STS downscoping.
//!
//! The SDK authenticates service account keys with self-signed JWTs, and its access tokens
//! carry no expiry. Google STS accepts only `OAuth2` access tokens as subject token, and the
//! downscoped token expires with it. Service account keys are therefore exchanged here with
//! the `OAuth2` JWT-bearer grant (RFC 7523), and tokens of the metadata server are fetched here.

use std::{
    sync::LazyLock,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::{Engine as _, prelude::BASE64_URL_SAFE_NO_PAD};
use google_cloud_auth::credentials::service_account;
use serde::Deserialize;
use veil::Redact;

use crate::gcs::{GcsAccessTokenError, GcsServiceAccountKey, gcs_auth::CLOUD_PLATFORM_SCOPE};

/// Token endpoint for all keys. The `token_uri` of a key is not used: keys come from users.
pub(crate) const GOOGLE_TOKEN_URI: &str = "https://oauth2.googleapis.com/token";
const DEFAULT_UNIVERSE_DOMAIN: &str = "googleapis.com";
const JWT_BEARER_GRANT_TYPE: &str = "urn:ietf:params:oauth:grant-type:jwt-bearer";
/// Longest lifetime the token endpoint accepts for an assertion.
const ASSERTION_LIFETIME: Duration = Duration::from_secs(3600);
/// The assertion is dated back by this much to tolerate clocks running ahead of Google's.
const CLOCK_SKEW: Duration = Duration::from_secs(10);
const TOKEN_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Client for the token endpoint and the metadata server.
pub(crate) static TOKEN_HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .connect_timeout(crate::CONNECT_TIMEOUT)
        .timeout(TOKEN_REQUEST_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        // Only fails if the TLS backend or system DNS config can't be
        // initialized — `reqwest::Client::new()` panics on the same condition.
        .expect("Failed to build GCS token HTTP client")
});

/// `OAuth2` access token.
#[derive(Redact, Clone, PartialEq, Eq)]
pub struct GcsAccessToken {
    /// The token, without the `Bearer ` prefix.
    #[redact(partial)]
    pub token: String,
    /// When the token expires, if known.
    pub expires_at: Option<SystemTime>,
}

/// Error response of a token endpoint.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("Token endpoint returned HTTP {status}: {error}: {error_description}")]
pub struct TokenEndpointError {
    pub status: u16,
    pub error: String,
    pub error_description: String,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: Option<u64>,
}

#[derive(Deserialize, Default)]
struct TokenErrorResponse {
    #[serde(default)]
    error: String,
    #[serde(default)]
    error_description: String,
}

/// Token endpoint of `key`, which must be in the default universe domain.
pub(crate) fn token_endpoint(
    key: &GcsServiceAccountKey,
) -> Result<&'static str, GcsAccessTokenError> {
    if let Some(universe_domain) = key.str_field("universe_domain")
        && universe_domain != DEFAULT_UNIVERSE_DOMAIN
    {
        return Err(GcsAccessTokenError::permanent(
            "service account keys outside the googleapis.com universe domain are not supported for credential vending",
            None,
        ));
    }
    Ok(GOOGLE_TOKEN_URI)
}

/// Exchanges `key` at `token_uri` for an access token with the `cloud-platform` scope.
///
/// `now` dates the assertion and is the base of the expiry.
pub(crate) async fn exchange_service_account_key(
    key: &GcsServiceAccountKey,
    token_uri: &str,
    now: SystemTime,
) -> Result<GcsAccessToken, GcsAccessTokenError> {
    let assertion = signed_assertion(key, token_uri, now).await?;
    let response = TOKEN_HTTP_CLIENT
        .post(token_uri)
        .form(&[
            ("grant_type", JWT_BEARER_GRANT_TYPE),
            ("assertion", assertion.as_str()),
        ])
        .send()
        .await;
    token_from_response(response, now).await
}

/// Access token of the default service account from the metadata server at `host`.
///
/// `now` is the base of the expiry.
pub(crate) async fn metadata_server_token(
    host: &str,
    now: SystemTime,
) -> Result<GcsAccessToken, GcsAccessTokenError> {
    let response = TOKEN_HTTP_CLIENT
        .get(format!(
            "http://{host}/computeMetadata/v1/instance/service-accounts/default/token"
        ))
        .query(&[("scopes", CLOUD_PLATFORM_SCOPE)])
        .header("Metadata-Flavor", "Google")
        .send()
        .await;
    token_from_response(response, now).await
}

/// Reads an `OAuth2` token response. Server errors, 408 and 429 are transient.
async fn token_from_response(
    response: reqwest::Result<reqwest::Response>,
    now: SystemTime,
) -> Result<GcsAccessToken, GcsAccessTokenError> {
    let response = response.map_err(|e| {
        GcsAccessTokenError::transient("the token request failed", Some(Box::new(e)))
    })?;
    let status = response.status();
    let body = response.bytes().await.map_err(|e| {
        GcsAccessTokenError::transient("the token request failed", Some(Box::new(e)))
    })?;
    if !status.is_success() {
        let TokenErrorResponse {
            error,
            error_description,
        } = serde_json::from_slice(&body).unwrap_or_default();
        let source = Some(Box::new(TokenEndpointError {
            status: status.as_u16(),
            error,
            error_description,
        }) as Box<dyn std::error::Error + Send + Sync>);
        return Err(
            if status.is_server_error()
                || status == reqwest::StatusCode::TOO_MANY_REQUESTS
                || status == reqwest::StatusCode::REQUEST_TIMEOUT
            {
                GcsAccessTokenError::transient("the token endpoint is unavailable", source)
            } else {
                GcsAccessTokenError::permanent("the token endpoint rejected the request", source)
            },
        );
    }
    let TokenResponse {
        access_token,
        expires_in,
    } = serde_json::from_slice(&body).map_err(|e| {
        GcsAccessTokenError::transient(
            "the token endpoint returned an unexpected response",
            Some(Box::new(e)),
        )
    })?;
    Ok(GcsAccessToken {
        token: access_token,
        expires_at: expires_in.and_then(|secs| now.checked_add(Duration::from_secs(secs))),
    })
}

/// JWT `{header}.{claims}.{signature}` signed with the key's private key (RS256).
async fn signed_assertion(
    key: &GcsServiceAccountKey,
    token_uri: &str,
    now: SystemTime,
) -> Result<String, GcsAccessTokenError> {
    let (Some(client_email), Some(private_key_id)) = (
        key.str_field("client_email"),
        key.str_field("private_key_id"),
    ) else {
        return Err(GcsAccessTokenError::permanent(
            "the service account key lacks client_email or private_key_id",
            None,
        ));
    };
    let issued_at = now
        .checked_sub(CLOCK_SKEW)
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map_or(0, |d| d.as_secs());

    let header = serde_json::json!({
        "alg": "RS256",
        "typ": "JWT",
        "kid": private_key_id,
    });
    let claims = serde_json::json!({
        "iss": client_email,
        "scope": CLOUD_PLATFORM_SCOPE,
        "aud": token_uri,
        "iat": issued_at,
        "exp": issued_at + ASSERTION_LIFETIME.as_secs(),
    });
    let signing_input = format!(
        "{}.{}",
        BASE64_URL_SAFE_NO_PAD.encode(header.to_string()),
        BASE64_URL_SAFE_NO_PAD.encode(claims.to_string())
    );

    let signer = service_account::Builder::new(key.json().clone())
        .build_signer()
        .map_err(|e| {
            GcsAccessTokenError::permanent(
                "failed to load the service account key",
                Some(Box::new(e)),
            )
        })?;
    let signature = signer.sign(signing_input.as_bytes()).await.map_err(|e| {
        GcsAccessTokenError::permanent("failed to sign the token request", Some(Box::new(e)))
    })?;
    Ok(format!(
        "{signing_input}.{}",
        BASE64_URL_SAFE_NO_PAD.encode(signature)
    ))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        error::Error as _,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use aws_lc_rs::{
        encoding::AsDer,
        rsa::{KeyPair, KeySize},
        signature::{KeyPair as _, RSA_PKCS1_2048_8192_SHA256, UnparsedPublicKey},
    };
    use axum::{
        Router,
        body::Bytes,
        http::{HeaderMap, StatusCode, Uri},
        response::IntoResponse,
        routing::any,
    };
    use base64::prelude::BASE64_STANDARD;
    use tokio::sync::mpsc;

    use super::*;

    const NOW_SECS: u64 = 1_700_000_000;

    fn now() -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(NOW_SECS)
    }

    /// A freshly generated RSA key in the JSON shape of a service account key.
    fn generated_key() -> (GcsServiceAccountKey, KeyPair) {
        let key_pair = KeyPair::generate(KeySize::Rsa2048).unwrap();
        let der = AsDer::<aws_lc_rs::encoding::Pkcs8V1Der<'static>>::as_der(&key_pair).unwrap();
        let pem = format!(
            "-----BEGIN PRIVATE KEY-----\n{}\n-----END PRIVATE KEY-----\n",
            BASE64_STANDARD.encode(der.as_ref())
        );
        let key = GcsServiceAccountKey::new(serde_json::json!({
            "type": "service_account",
            "project_id": "example-project-0000",
            "private_key_id": "test-key-id",
            "private_key": pem,
            "client_email": "example-sa@example-project-0000.iam.gserviceaccount.com",
            "client_id": "000000000000000000000",
            "token_uri": GOOGLE_TOKEN_URI,
        }));
        (key, key_pair)
    }

    struct RecordedRequest {
        path_and_query: String,
        headers: HeaderMap,
        body: Bytes,
    }

    struct MockEndpoint {
        /// Base URL, `http://host:port`.
        base: String,
        requests: mpsc::UnboundedReceiver<RecordedRequest>,
        /// Requests to `/redirected`, the target of redirects.
        redirected: Arc<AtomicUsize>,
    }

    /// Answers every request with `response` and records it. `/redirected` only counts.
    async fn mock_endpoint(
        response: (StatusCode, Vec<(&'static str, &'static str)>, &'static str),
    ) -> MockEndpoint {
        let (sender, requests) = mpsc::unbounded_channel();
        let redirected = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route("/redirected", {
                let redirected = redirected.clone();
                any(move || {
                    redirected.fetch_add(1, Ordering::SeqCst);
                    async { StatusCode::OK }
                })
            })
            .fallback(move |uri: Uri, headers: HeaderMap, body: Bytes| {
                let sender = sender.clone();
                let (status, response_headers, response_body) = response.clone();
                async move {
                    sender
                        .send(RecordedRequest {
                            path_and_query: uri.path_and_query().unwrap().to_string(),
                            headers,
                            body,
                        })
                        .unwrap();
                    let mut response = (status, response_body).into_response();
                    for (name, value) in response_headers {
                        response.headers_mut().insert(name, value.parse().unwrap());
                    }
                    response
                }
            });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        MockEndpoint {
            base: format!("http://{address}"),
            requests,
            redirected,
        }
    }

    fn json_response(
        body: &'static str,
    ) -> (StatusCode, Vec<(&'static str, &'static str)>, &'static str) {
        (
            StatusCode::OK,
            vec![("content-type", "application/json")],
            body,
        )
    }

    fn decode_json(segment: &str) -> serde_json::Value {
        serde_json::from_slice(&BASE64_URL_SAFE_NO_PAD.decode(segment).unwrap()).unwrap()
    }

    fn endpoint_error(err: &GcsAccessTokenError) -> &TokenEndpointError {
        err.source()
            .and_then(|s| s.downcast_ref::<TokenEndpointError>())
            .unwrap()
    }

    #[tokio::test]
    async fn exchange_posts_signed_jwt_bearer_assertion() {
        let (key, key_pair) = generated_key();
        let mut mock = mock_endpoint(json_response(
            r#"{"access_token":"ya29.test","expires_in":3599,"token_type":"Bearer"}"#,
        ))
        .await;
        let uri = format!("{}/token", mock.base);

        let token = exchange_service_account_key(&key, &uri, now())
            .await
            .unwrap();
        assert_eq!(
            token,
            GcsAccessToken {
                token: "ya29.test".to_string(),
                expires_at: Some(now() + Duration::from_secs(3599)),
            }
        );

        let request = mock.requests.recv().await.unwrap();
        assert_eq!(request.path_and_query, "/token");
        let form: HashMap<String, String> = url::form_urlencoded::parse(&request.body)
            .into_owned()
            .collect();
        assert_eq!(form.len(), 2);
        assert_eq!(form["grant_type"], JWT_BEARER_GRANT_TYPE);
        let segments: Vec<&str> = form["assertion"].split('.').collect();
        assert_eq!(segments.len(), 3);

        assert_eq!(
            decode_json(segments[0]),
            serde_json::json!({"alg": "RS256", "typ": "JWT", "kid": "test-key-id"})
        );
        assert_eq!(
            decode_json(segments[1]),
            serde_json::json!({
                "iss": "example-sa@example-project-0000.iam.gserviceaccount.com",
                "scope": CLOUD_PLATFORM_SCOPE,
                "aud": uri,
                "iat": NOW_SECS - 10,
                "exp": NOW_SECS - 10 + 3600,
            })
        );
        let signature = BASE64_URL_SAFE_NO_PAD.decode(segments[2]).unwrap();
        UnparsedPublicKey::new(&RSA_PKCS1_2048_8192_SHA256, key_pair.public_key().as_ref())
            .verify(
                format!("{}.{}", segments[0], segments[1]).as_bytes(),
                &signature,
            )
            .unwrap();
    }

    #[tokio::test]
    async fn exchange_maps_token_endpoint_error() {
        let (key, _) = generated_key();
        let mock = mock_endpoint((
            StatusCode::BAD_REQUEST,
            vec![("content-type", "application/json")],
            r#"{"error":"invalid_grant","error_description":"Invalid JWT Signature."}"#,
        ))
        .await;

        let err = exchange_service_account_key(&key, &format!("{}/token", mock.base), now())
            .await
            .unwrap_err();
        assert!(!err.is_transient());
        assert_eq!(
            err.to_string(),
            "Failed to obtain a GCS access token: the token endpoint rejected the request"
        );
        assert_eq!(
            endpoint_error(&err),
            &TokenEndpointError {
                status: 400,
                error: "invalid_grant".to_string(),
                error_description: "Invalid JWT Signature.".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn exchange_unavailable_endpoint_is_transient() {
        for status in [
            StatusCode::SERVICE_UNAVAILABLE,
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::REQUEST_TIMEOUT,
        ] {
            let (key, _) = generated_key();
            let mock = mock_endpoint((status, vec![], "unavailable")).await;

            let err = exchange_service_account_key(&key, &format!("{}/token", mock.base), now())
                .await
                .unwrap_err();
            assert!(err.is_transient(), "{status}");
            assert_eq!(
                endpoint_error(&err),
                &TokenEndpointError {
                    status: status.as_u16(),
                    error: String::new(),
                    error_description: String::new(),
                }
            );
        }
    }

    #[tokio::test]
    async fn exchange_unreachable_endpoint_is_transient() {
        let (key, _) = generated_key();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let uri = format!("http://{}/token", listener.local_addr().unwrap());
        drop(listener);

        let err = exchange_service_account_key(&key, &uri, now())
            .await
            .unwrap_err();
        assert!(err.is_transient(), "{err:?}");
        let source = err
            .source()
            .and_then(|s| s.downcast_ref::<reqwest::Error>())
            .unwrap();
        assert!(source.is_connect(), "{source:?}");
    }

    #[tokio::test]
    async fn exchange_does_not_follow_redirects() {
        let (key, _) = generated_key();
        let mock = mock_endpoint((StatusCode::FOUND, vec![("location", "/redirected")], "")).await;

        let err = exchange_service_account_key(&key, &format!("{}/token", mock.base), now())
            .await
            .unwrap_err();
        assert!(!err.is_transient());
        assert_eq!(endpoint_error(&err).status, 302);
        assert_eq!(mock.redirected.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn metadata_server_token_has_expiry() {
        let mut mock = mock_endpoint(json_response(
            r#"{"access_token":"ya29.mds","expires_in":1800,"token_type":"Bearer"}"#,
        ))
        .await;
        let host = mock.base.trim_start_matches("http://").to_string();

        let token = metadata_server_token(&host, now()).await.unwrap();
        assert_eq!(
            token,
            GcsAccessToken {
                token: "ya29.mds".to_string(),
                expires_at: Some(now() + Duration::from_mins(30)),
            }
        );

        let request = mock.requests.recv().await.unwrap();
        assert_eq!(
            request.path_and_query,
            "/computeMetadata/v1/instance/service-accounts/default/token?scopes=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fcloud-platform"
        );
        assert_eq!(request.headers["metadata-flavor"], "Google");
    }

    #[tokio::test]
    async fn metadata_server_not_found_is_permanent() {
        let mock = mock_endpoint((StatusCode::NOT_FOUND, vec![], "no service account")).await;
        let host = mock.base.trim_start_matches("http://").to_string();

        let err = metadata_server_token(&host, now()).await.unwrap_err();
        assert!(!err.is_transient());
        assert_eq!(endpoint_error(&err).status, 404);
    }

    #[test]
    fn access_token_debug_redacts_token() {
        let token = GcsAccessToken {
            token: "ya29.secret-token-value".to_string(),
            expires_at: None,
        };
        assert!(!format!("{token:?}").contains("secret-token"));
    }

    fn key_with(fields: serde_json::Value) -> GcsServiceAccountKey {
        GcsServiceAccountKey::new(fields)
    }

    #[test]
    fn token_endpoint_ignores_token_uri_of_key() {
        for key_token_uri in [
            None,
            Some(GOOGLE_TOKEN_URI),
            // Keys created before 2019 name the legacy endpoint.
            Some("https://accounts.google.com/o/oauth2/token"),
            Some("https://evil.example/token"),
            Some("http://169.254.169.254/token"),
        ] {
            let key = key_with(serde_json::json!({ "token_uri": key_token_uri }));
            assert_eq!(
                token_endpoint(&key).unwrap(),
                GOOGLE_TOKEN_URI,
                "{key_token_uri:?}"
            );
        }
    }

    #[test]
    fn token_endpoint_rejects_other_universe_domains() {
        assert_eq!(
            token_endpoint(&key_with(
                serde_json::json!({"universe_domain": "googleapis.com"})
            ))
            .unwrap(),
            GOOGLE_TOKEN_URI
        );
        let err = token_endpoint(&key_with(
            serde_json::json!({"universe_domain": "example.com"}),
        ))
        .unwrap_err();
        assert!(!err.is_transient());
        assert_eq!(
            err.to_string(),
            "Failed to obtain a GCS access token: service account keys outside the googleapis.com universe domain are not supported for credential vending"
        );
    }
}
