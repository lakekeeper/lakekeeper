#![allow(clippy::module_name_repetitions)]

use std::{collections::HashMap, fmt::Debug};

use super::{impl_properties, ConfigParseError, ConfigProperty, NotCustomProp, ParseFromStr};

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
            AccessKeyId, String, "s3.access-key-id", "s3_access_key_id";
            SecretAccessKey, String, "s3.secret-access-key", "s3_secret_access_key";
            SessionToken, String, "s3.session-token", "s3_session_token";
            RemoteSigningEnabled, bool, "s3.remote-signing-enabled", "s3_remote_signing_enabled";
            Signer, String, "s3.signer", "s3_signer";
            SignerUri, String, "s3.signer.uri", "s3_signer_uri";
            SignerEndpoint, String, "s3.signer.endpoint", "s3_signer_endpoint";
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
    }
}
