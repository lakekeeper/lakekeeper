use std::sync::LazyLock;

use aws_config::{sts::AssumeRoleProvider, BehaviorVersion, SdkConfig};
use aws_sdk_s3::config::{
    IdentityCache, SharedCredentialsProvider, SharedHttpClient, SharedIdentityCache,
};
use veil::Redact;

mod s3_error;
mod s3_location;
mod s3_storage;
pub use s3_location::{validate_bucket_name, InvalidBucketName, S3Location};
pub use s3_storage::S3Storage;

static IDENTITY_CACHE: LazyLock<SharedIdentityCache> =
    LazyLock::new(|| IdentityCache::lazy().build());
static SMITHY_HTTP_CLIENT: LazyLock<SharedHttpClient> = LazyLock::new(|| {
    aws_smithy_http_client::Builder::new()
        .tls_provider(aws_smithy_http_client::tls::Provider::Rustls(
            aws_smithy_http_client::tls::rustls_provider::CryptoMode::AwsLc,
        ))
        .build_https()
});

const S3_CUSTOM_SCHEMES: [&str; 2] = ["s3a", "s3n"];

#[derive(Debug, Clone, PartialEq, derive_more::From)]
pub enum S3Auth {
    AccessKey(S3AccessKeyAuth),
    AwsSystemIdentity(S3AwsSystemIdentityAuth),
}

impl S3Auth {
    /// Get the external ID for the credential.
    #[must_use]
    pub fn external_id(&self) -> Option<&str> {
        match self {
            S3Auth::AccessKey(S3AccessKeyAuth { external_id, .. })
            | S3Auth::AwsSystemIdentity(S3AwsSystemIdentityAuth { external_id }) => {
                external_id.as_deref()
            }
        }
    }
}

#[derive(Redact, Clone, PartialEq)]
pub struct S3AwsSystemIdentityAuth {
    #[redact(partial)]
    pub external_id: Option<String>,
}

#[derive(Redact, Clone, PartialEq)]
pub struct S3AccessKeyAuth {
    pub aws_access_key_id: String,
    #[redact(partial)]
    pub aws_secret_access_key: String,
    #[redact(partial)]
    pub external_id: Option<String>,
}

#[derive(Debug, Eq, Clone, PartialEq, typed_builder::TypedBuilder)]
pub struct S3Settings {
    // -------- AWS Settings for multiple services --------
    #[builder(default, setter(strip_option))]
    pub assume_role_arn: Option<String>,
    #[builder(default, setter(strip_option))]
    pub endpoint: Option<url::Url>,
    pub region: String,
    // -------- S3 specific settings --------
    #[builder(default, setter(strip_option))]
    pub path_style_access: Option<bool>,
    #[builder(default, setter(strip_option))]
    pub aws_kms_key_arn: Option<String>,
}

impl S3Settings {
    pub async fn get_storage_client(&self, s3_credential: Option<&S3Auth>) -> S3Storage {
        let sdk_config = self.get_sdk_config(s3_credential).await;
        let s3_config: aws_sdk_s3::config::Config = (&sdk_config).into();
        let mut s3_builder = s3_config.to_builder();

        if self.path_style_access.unwrap_or(false) {
            s3_builder.set_force_path_style(Some(true));
        }

        let client = aws_sdk_s3::Client::from_conf(s3_builder.build());
        S3Storage::new(client, self.aws_kms_key_arn.clone())
    }

    pub async fn get_sdk_config(&self, s3_credential: Option<&S3Auth>) -> SdkConfig {
        let S3Settings {
            assume_role_arn,
            endpoint,
            region,
            // S3 specific settings
            path_style_access: _,
            aws_kms_key_arn: _,
        } = self;

        let loader = match s3_credential {
            Some(S3Auth::AccessKey(S3AccessKeyAuth {
                aws_access_key_id,
                aws_secret_access_key,
                external_id: _, // External ID handled below in assume role path
            })) => {
                let aws_credentials = aws_credential_types::Credentials::new(
                    aws_access_key_id,
                    aws_secret_access_key,
                    None,
                    None,
                    "lakekeeper-secret-storage",
                );
                aws_config::ConfigLoader::default().credentials_provider(aws_credentials)
            }
            Some(S3Auth::AwsSystemIdentity(S3AwsSystemIdentityAuth {
                external_id: _, // External ID handled below in this function in the assume role path
            })) => aws_config::from_env(),
            None => aws_config::from_env().no_credentials(),
        }
        .region(Some(aws_config::Region::new(region.to_string())))
        .behavior_version(BehaviorVersion::latest())
        .http_client((*SMITHY_HTTP_CLIENT).clone())
        .identity_cache(IDENTITY_CACHE.clone());

        let loader = if let Some(endpoint) = endpoint {
            loader.endpoint_url(endpoint.to_string())
        } else {
            loader
        };

        let sdk_config = loader.load().await;

        let sdk_config = if let Some(assume_role_arn) = assume_role_arn {
            let mut assume_role_provider = AssumeRoleProvider::builder(assume_role_arn)
                .configure(&sdk_config)
                .session_name("lakekeeper-assume-role");

            if let Some(external_id) = s3_credential.and_then(S3Auth::external_id) {
                assume_role_provider = assume_role_provider.external_id(external_id);
            }
            let assume_role_provider = assume_role_provider.build().await;

            sdk_config
                .into_builder()
                .credentials_provider(SharedCredentialsProvider::new(assume_role_provider))
                .build()
        } else {
            sdk_config
        };

        sdk_config
    }
}

/// Validate the S3 region.
///
/// # Errors
/// If the region is longer than 128 characters, an error is returned.
pub fn validate_region(region: &str) -> Result<(), String> {
    if region.len() > 128 {
        return Err("`region` must be less than 128 characters.".to_string());
    }

    Ok(())
}
