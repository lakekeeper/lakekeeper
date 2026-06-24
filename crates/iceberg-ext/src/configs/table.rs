#![allow(clippy::module_name_repetitions)]

use std::{collections::HashMap, fmt::Debug};

use super::{ConfigParseError, ConfigProperty, NotCustomProp, ParseFromStr, impl_properties};

impl_properties!(TableProperties, TableProperty);

impl TableProperties {
    /// Try to create a `TableConfig` from a list of key-value pairs.
    ///
    /// # Errors
    /// Returns an error if a known key has an incompatible value.
    pub fn try_from_props(
        props: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, ConfigParseError> {
        let mut config = TableProperties::default();
        for (key, value) in props {
            if key.starts_with("s3") {
                s3::validate(&key, &value)?;
                config.props.insert(key, value);
            } else if key.starts_with("client") {
                client::validate(&key, &value)?;
                config.props.insert(key, value);
            } else if key.starts_with("gcs") {
                gcs::validate(&key, &value)?;
                config.props.insert(key, value);
            } else if key.starts_with("signer") {
                signer::validate(&key, &value)?;
                config.props.insert(key, value);
            } else if key.starts_with("adls") {
                adls::validate(&key, &value)?;
            } else if [creds::ExpirationTimeMs::KEY].contains(&key.as_str()) {
                creds::validate(&key, &value)?;
                config.props.insert(key, value);
            } else {
                let pair = custom::CustomConfig {
                    key: key.clone(),
                    value,
                };
                config.insert(&pair);
            }
        }
        Ok(config)
    }
}

#[allow(clippy::implicit_hasher)]
impl From<TableProperties> for HashMap<String, String> {
    fn from(config: TableProperties) -> Self {
        config.props
    }
}

pub mod s3 {
    use url::Url;

    use super::{
        super::ConfigProperty, ConfigParseError, NotCustomProp, ParseFromStr, TableProperties,
        TableProperty,
    };
    use crate::configs::impl_config_values;

    impl_config_values!(
        Table,
        {
            Region, String, "s3.region", "s3_region";
            Endpoint, Url, "s3.endpoint", "s3_endpoint";
            PathStyleAccess, bool, "s3.path-style-access", "s3_path_style_access";
            SseType, String, "s3.sse.type", "s3_sse_type";
            SseKey, String, "s3.sse.key", "s3_sse_key";
            AccessKeyId, String, "s3.access-key-id", "s3_access_key_id";
            SecretAccessKey, String, "s3.secret-access-key", "s3_secret_access_key";
            SessionToken, String, "s3.session-token", "s3_session_token";
            RemoteSigningEnabled, bool, "s3.remote-signing-enabled", "s3_remote_signing_enabled";
            Signer, String, "s3.signer", "s3_signer";
            SignerUri, String, "s3.signer.uri", "s3_signer_uri";
            SignerEndpoint, String, "s3.signer.endpoint", "s3_signer_endpoint";
            SesionTokenExpiresAtMs, i64, "s3.session-token-expires-at-ms", "s3_session_token_expires_at_ms";
         }
    );
}

/// Remote-signing properties introduced in Iceberg 1.11.0. These supersede the
/// S3-namespaced `s3.signer.uri` / `s3.signer.endpoint` (deprecated, removed in Iceberg 1.12.0).
/// Lakekeeper emits both the old and new keys with identical values: new clients (>=1.11) read
/// these and avoid the deprecation warning, while older clients keep using the `s3.signer.*` keys.
pub mod signer {
    use super::{
        super::ConfigProperty, ConfigParseError, NotCustomProp, ParseFromStr, TableProperties,
        TableProperty,
    };
    use crate::configs::impl_config_values;

    impl_config_values!(
        Table,
        {
            Uri, String, "signer.uri", "signer_uri";
            Endpoint, String, "signer.endpoint", "signer_endpoint";
        }
    );
}

pub mod creds {
    use super::{
        super::ConfigProperty, ConfigParseError, NotCustomProp, ParseFromStr, TableProperties,
        TableProperty,
    };
    use crate::configs::impl_config_values;

    impl_config_values!(
        Table,
        {
            ExpirationTimeMs, i64, "expiration-time", "expiration_time";
        }
    );
}

pub mod gcs {
    use super::{
        super::ConfigProperty, ConfigParseError, NotCustomProp, ParseFromStr, TableProperties,
        TableProperty,
    };
    use crate::configs::impl_config_values;

    impl_config_values!(
        Table,
        {
            ProjectId, String, "gcs.project-id", "gcs_project_id";
            Bucket, String, "gcs.bucket", "gcs_bucket";
            Token, String, "gcs.oauth2.token", "gcs_oauth2_token";
            TokenExpiresAt, String, "gcs.oauth2.token-expires-at", "gcs_oauth2_token_expires_at";
            RefreshCredentialsEndpoint, String, "gcs.oauth2.refresh-credentials-endpoint", "gcs_oauth2_refresh_credentials_endpoint";
        }
    );
}

pub mod client {
    use super::{
        super::ConfigProperty, ConfigParseError, NotCustomProp, ParseFromStr, TableProperties,
        TableProperty,
    };
    use crate::configs::impl_config_values;
    impl_config_values!(
        Table,
        {
            Region, String, "client.region", "client_region";
            RefreshClientCredentialsEndpoint, String, "client.refresh-credentials-endpoint", "client_refresh_credentials_endpoint";
        }
    );
}

pub mod adls {
    use super::{
        super::ConfigProperty, ConfigParseError, NotCustomProp, ParseFromStr, TableProperties,
        TableProperty,
    };
    use crate::configs::impl_config_values;
    impl_config_values!(
        Table,
        {
            RefreshClientCredentialsEndpoint, String, "adls.refresh-credentials-endpoint", "adls_refresh_credentials_endpoint";
        }
    );
}

pub mod custom {
    use super::TableProperty;
    pub use crate::configs::CustomConfig;

    impl TableProperty for CustomConfig {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iceberg_io_key_match() {
        assert_eq!(iceberg::io::S3_REGION, s3::Region::KEY);
        assert_eq!(iceberg::io::S3_ENDPOINT, s3::Endpoint::KEY);
        assert_eq!(iceberg::io::S3_PATH_STYLE_ACCESS, s3::PathStyleAccess::KEY);
        assert_eq!(iceberg::io::S3_ACCESS_KEY_ID, s3::AccessKeyId::KEY);
        assert_eq!(iceberg::io::S3_SECRET_ACCESS_KEY, s3::SecretAccessKey::KEY);
        assert_eq!(iceberg::io::S3_SESSION_TOKEN, s3::SessionToken::KEY);
    }

    #[test]
    fn test_signer_keys_match_iceberg_1_11() {
        // Property names introduced in Iceberg 1.11.0 (org.apache.iceberg.rest.RESTCatalogProperties).
        assert_eq!(signer::Uri::KEY, "signer.uri");
        assert_eq!(signer::Endpoint::KEY, "signer.endpoint");
    }

    #[test]
    fn test_signer_props_round_trip_through_validate() {
        // `signer.*` keys must be dispatched to `signer::validate` and retained, not dropped as custom.
        let config = TableProperties::try_from_props([
            (
                "signer.uri".to_string(),
                "https://example.com/catalog/".to_string(),
            ),
            ("signer.endpoint".to_string(), "v1/aws/s3/sign".to_string()),
        ])
        .unwrap();
        assert_eq!(
            config.signer_uri().as_deref(),
            Some("https://example.com/catalog/")
        );
        assert_eq!(config.signer_endpoint().as_deref(), Some("v1/aws/s3/sign"));
    }
}
