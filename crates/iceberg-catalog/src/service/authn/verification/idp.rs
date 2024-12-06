use crate::api;
use crate::service::authn::verification::Verifier;
use crate::service::authn::Claims;
use crate::service::AuthDetails;
use anyhow::Context;
use http::StatusCode;
use iceberg_ext::catalog::rest::ErrorModel;
use jsonwebtoken::{Algorithm, DecodingKey, Header, Validation};
use jwks_client_rs::source::WebSource;
use jwks_client_rs::{JsonWebKey, JwksClient};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;
use url::Url;

#[async_trait::async_trait]
impl Verifier for IdpVerifier {
    async fn decode(&self, token: &str) -> api::Result<AuthDetails, ErrorModel> {
        let claims = IdpVerifier::decode::<Claims>(self, token).await?;
        AuthDetails::try_from_jwt_claims(claims).map_err(|e| e.error)
    }

    fn typ(&self) -> &str {
        self.main_issuer.as_str()
    }
}

#[derive(Clone)]
pub struct IdpVerifier {
    client: JwksClient<WebSource>,
    issuers: Vec<String>,
    main_issuer: String,
    /// Expected audience for the token.
    /// If None, the audience will not be validated
    audience: Option<Vec<String>>,
}

impl IdpVerifier {
    const WELL_KNOWN_CONFIG: &'static str = ".well-known/openid-configuration";
    /// Create a new verifier with the given openid configuration url and audience.
    ///
    /// # Errors
    ///
    /// This function can fail if the openid configuration cannot be fetched or parsed.
    /// This function can also fail if the `WebSource` cannot be built from the jwks uri in the
    /// fetched openid configuration
    pub async fn new(
        mut url: Url,
        audience: Option<Vec<String>>,
        additional_issuers: Option<Vec<String>>,
    ) -> anyhow::Result<Self> {
        if !url.path().ends_with('/') {
            url.set_path(&format!("{}/", url.path()));
        }

        let config = Arc::new(
            reqwest::get(url.join(Self::WELL_KNOWN_CONFIG)?)
                .await
                .context("Failed to fetch openid configuration")?
                .json::<WellKnownConfig>()
                .await
                .context("Failed to parse openid configuration")?,
        );
        let source = WebSource::builder().build(config.jwks_uri.clone())?;
        let client = JwksClient::builder().build(source);
        let main_issuer = config.issuer.clone();
        let mut issuers = additional_issuers.unwrap_or_default();
        issuers.push(main_issuer.clone());
        tracing::info!("Created IdpVerifier for issuers: {:?}", issuers);

        Ok(Self {
            client,
            issuers,
            main_issuer,
            audience,
        })
    }

    // this function is mostly lifted out of jwks_client_rs which is incompatible with azure jwks.
    async fn decode<O: DeserializeOwned>(&self, token: &str) -> api::Result<O, ErrorModel> {
        let header: Header = jsonwebtoken::decode_header(token).map_err(|e| {
            ErrorModel::builder()
                .message("Failed to decode auth token header.")
                .code(StatusCode::UNAUTHORIZED.into())
                .r#type("UnauthorizedError")
                .source(Some(Box::new(e)))
                .build()
        })?;

        if let Some(kid) = header.kid.as_ref() {
            let key: JsonWebKey = self
                .client
                .get_opt(kid)
                .await
                .map_err(|e| Self::internal_error(e, "Failed to fetch key from jwks endpoint."))?
                .ok_or_else(|| {
                    ErrorModel::builder()
                        .message("Unknown kid")
                        .r#type("UnauthorizedError")
                        .code(StatusCode::UNAUTHORIZED.into())
                        .build()
                })?;

            let validation = self.setup_validation(&header, &key)?;
            let decoding_key = Self::setup_decoding_key(key)?;

            return Ok(jsonwebtoken::decode(token, &decoding_key, &validation)
                .map_err(|e| {
                    tracing::debug!("Failed to decode token: {}", e);
                    ErrorModel::builder()
                        .message("Failed to decode token.")
                        .code(StatusCode::UNAUTHORIZED.into())
                        .r#type("UnauthorizedError")
                        .source(Some(Box::new(e)))
                        .build()
                })?
                .claims);
        }

        Err(ErrorModel::builder()
            .message("Token header does not contain a key id.")
            .code(StatusCode::UNAUTHORIZED.into())
            .r#type("UnauthorizedError")
            .build())
    }

    fn setup_decoding_key(key: JsonWebKey) -> api::Result<DecodingKey, ErrorModel> {
        let decoding_key = match key {
            JsonWebKey::Rsa(jwk) => DecodingKey::from_rsa_components(jwk.modulus(), jwk.exponent())
                .map_err(|e| {
                    Self::internal_error(
                        e,
                        "Failed to create rsa decoding key from key components.",
                    )
                })?,
            JsonWebKey::Ec(jwk) => {
                DecodingKey::from_ec_components(jwk.x(), jwk.y()).map_err(|e| {
                    Self::internal_error(e, "Failed to create ec decoding key from key components.")
                })?
            }
        };
        Ok(decoding_key)
    }

    fn setup_validation(
        &self,
        header: &Header,
        key: &JsonWebKey,
    ) -> api::Result<Validation, ErrorModel> {
        let mut validation = if let Some(alg) = key.alg() {
            Validation::new(Algorithm::from_str(alg).map_err(|e| {
                Self::internal_error(
                    e,
                    "Failed to parse algorithm from key obtained from the jwks endpoint.",
                )
            })?)
        } else {
            // We need this fallback since e.g. azure's keys at
            // https://login.microsoftonline.com/common/discovery/keys don't have the alg field
            Validation::new(header.alg)
        };

        if let Some(aud) = &self.audience {
            validation.set_audience(aud);
            validation.validate_aud = true;
        } else {
            validation.validate_aud = false;
        }
        validation.set_issuer(&self.issuers);

        Ok(validation)
    }

    fn internal_error(
        e: impl std::error::Error + Sync + Send + 'static,
        message: &str,
    ) -> ErrorModel {
        ErrorModel::builder()
            .message(message)
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .r#type("InternalServerError")
            .source(Some(Box::new(e)))
            .build()
    }
}

impl Debug for IdpVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Verifier").finish()
    }
}

#[derive(Deserialize, Clone, Debug)]
pub(crate) struct WellKnownConfig {
    #[serde(flatten)]
    #[allow(dead_code)]
    pub other: serde_json::Value,
    pub jwks_uri: Url,
    pub issuer: String,
}
