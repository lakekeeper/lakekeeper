use std::{
    collections::BTreeMap,
    sync::{Arc, LazyLock},
};

use aws_config::{
    AppName, BehaviorVersion, SdkConfig, retry::RetryConfig, sts::AssumeRoleProvider,
    timeout::TimeoutConfig,
};
use aws_sdk_s3::config::{
    IdentityCache, SharedAsyncSleep, SharedCredentialsProvider, SharedHttpClient,
    SharedIdentityCache, http::HttpRequest,
};
use aws_smithy_async::{
    rt::sleep::{self, TokioSleep},
    time::SharedTimeSource,
};
use aws_smithy_runtime_api::{
    box_error::BoxError,
    client::{
        interceptors::{Intercept, context::BeforeTransmitInterceptorContextMut},
        orchestrator::Metadata,
        runtime_components::RuntimeComponents,
    },
};
use aws_smithy_types::{base64, config_bag::ConfigBag};
use veil::Redact;

mod s3_error;
mod s3_location;
mod s3_storage;
pub use s3_location::{InvalidBucketName, S3Location, validate_bucket_name};
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

static RETRY_CONFIG: LazyLock<RetryConfig> = LazyLock::new(RetryConfig::adaptive);
static TIMEOUT_CONFIG: LazyLock<TimeoutConfig> = LazyLock::new(|| TimeoutConfig::builder().build());
static TIME_SOURCE: LazyLock<SharedTimeSource> = LazyLock::new(SharedTimeSource::default);
static TOKIO_SLEEP: LazyLock<Arc<dyn sleep::AsyncSleep>> =
    LazyLock::new(|| Arc::new(TokioSleep::new()) as Arc<dyn sleep::AsyncSleep>);
static SLEEP_IMPL: LazyLock<SharedAsyncSleep> =
    LazyLock::new(|| SharedAsyncSleep::from(TOKIO_SLEEP.clone()));

const S3_CUSTOM_SCHEMES: [&str; 2] = ["s3a", "s3n"];

/// Macro to apply common AWS configuration to any builder that supports these methods
macro_rules! apply_aws_config {
    ($builder:expr, $region:expr) => {
        $builder
            .region($region)
            .retry_config(RETRY_CONFIG.clone())
            .timeout_config(TIMEOUT_CONFIG.clone())
            .time_source(TIME_SOURCE.clone())
            .sleep_impl(SLEEP_IMPL.clone())
            .behavior_version(BehaviorVersion::latest())
            .http_client((*SMITHY_HTTP_CLIENT).clone())
            .identity_cache(IDENTITY_CACHE.clone())
            .app_name(AppName::new("lakekeeper").unwrap())
    };
}

#[derive(Debug, Hash, Clone, PartialEq, Eq, derive_more::From)]
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

#[derive(Redact, Hash, Clone, PartialEq, Eq)]
pub struct S3AwsSystemIdentityAuth {
    #[redact(partial)]
    pub external_id: Option<String>,
}

#[derive(Redact, Hash, Clone, PartialEq, Eq, typed_builder::TypedBuilder)]
pub struct S3AccessKeyAuth {
    pub aws_access_key_id: String,
    #[redact(partial)]
    pub aws_secret_access_key: String,
    #[builder(default)]
    #[redact(partial)]
    pub external_id: Option<String>,
}

#[derive(Debug, Eq, Clone, PartialEq, typed_builder::TypedBuilder)]
pub struct S3Settings {
    // -------- AWS Settings for multiple services --------
    #[builder(default)]
    pub assume_role_arn: Option<String>,
    #[builder(default)]
    /// STS Session Tags to pass when assuming a role.
    /// Each tag is a key-value pair.
    /// Only has effect if `assume_role_arn` is set.
    pub sts_session_tags: BTreeMap<String, String>,
    #[builder(default)]
    pub endpoint: Option<url::Url>,
    pub region: String,
    // -------- S3 specific settings --------
    #[builder(default)]
    pub path_style_access: Option<bool>,
    #[builder(default)]
    pub aws_kms_key_arn: Option<String>,
    #[builder(default)]
    pub legacy_md5_behavior: Option<bool>,
}

impl S3Settings {
    pub async fn get_storage_client(&self, s3_credential: Option<&S3Auth>) -> S3Storage {
        let sdk_config = self.get_sdk_config(s3_credential).await;
        let s3_config: aws_sdk_s3::config::Config = (&sdk_config).into();
        let mut s3_builder = s3_config.to_builder();

        if self.path_style_access.unwrap_or(false) {
            s3_builder.set_force_path_style(Some(true));
        }

        if self.legacy_md5_behavior.unwrap_or(false) {
            s3_builder = s3_builder.interceptor(LegacyMD5Interceptor);
        }

        let client = aws_sdk_s3::Client::from_conf(s3_builder.build());
        S3Storage::new(client, self.aws_kms_key_arn.clone())
    }

    pub async fn get_sdk_config(&self, s3_credential: Option<&S3Auth>) -> SdkConfig {
        let S3Settings {
            assume_role_arn,
            sts_session_tags,
            endpoint,
            region,
            // S3 specific settings
            path_style_access: _,
            aws_kms_key_arn: _,
            legacy_md5_behavior: _,
        } = self;

        let region = aws_config::Region::new(region.clone());

        let sdk_config = match s3_credential {
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
                let credential_provider = SharedCredentialsProvider::new(aws_credentials);

                let mut builder = apply_aws_config!(SdkConfig::builder(), region)
                    .credentials_provider(credential_provider);
                if let Some(endpoint) = endpoint {
                    builder = builder.endpoint_url(endpoint.to_string());
                }
                builder.build()
            }
            Some(S3Auth::AwsSystemIdentity(S3AwsSystemIdentityAuth {
                external_id: _, // External ID handled below in this function in the assume role path
            })) => {
                let mut builder = apply_aws_config!(aws_config::from_env(), region);
                if let Some(endpoint) = endpoint {
                    builder = builder.endpoint_url(endpoint.to_string());
                }
                builder.load().await
            }
            None => {
                let mut builder = apply_aws_config!(SdkConfig::builder(), region);
                if let Some(endpoint) = endpoint {
                    builder.set_endpoint_url(Some(endpoint.to_string()));
                }
                builder.build()
            }
        };

        if let Some(assume_role_arn) = assume_role_arn {
            let mut assume_role_provider = AssumeRoleProvider::builder(assume_role_arn)
                .configure(&sdk_config)
                .session_name("lakekeeper-assume-role");

            if let Some(external_id) = s3_credential.and_then(S3Auth::external_id) {
                assume_role_provider = assume_role_provider.external_id(external_id);
            }
            if !sts_session_tags.is_empty() {
                let tags = sts_session_tags.iter();
                assume_role_provider = assume_role_provider.tags(tags);
            }
            let assume_role_provider = assume_role_provider.build().await;

            sdk_config
                .into_builder()
                .credentials_provider(SharedCredentialsProvider::new(assume_role_provider))
                .build()
        } else {
            sdk_config
        }
    }
}

#[derive(Debug, Default)]
struct LegacyMD5Interceptor;

impl LegacyMD5Interceptor {
    /// This function mutates the request to insert a Content-MD5 header if one is not present.
    fn calculate_md5_checksum(http_request: &mut HttpRequest) {
        // Return early if a Content-MD5 header is already present
        if http_request.headers().contains_key("Content-MD5") {
            return;
        }

        // Check if the body is present if it isn't (streaming request) we skip adding the header
        if let Some(bytes) = http_request.body().bytes() {
            let md5 = md5::compute(bytes);
            let checksum_value = base64::encode(md5.as_slice());
            http_request
                .headers_mut()
                .append("Content-MD5", checksum_value);
        }
    }
}

impl Intercept for LegacyMD5Interceptor {
    fn name(&self) -> &'static str {
        "LegacyMD5Interceptor"
    }

    fn modify_before_signing(
        &self,
        ctx: &mut BeforeTransmitInterceptorContextMut<'_>,
        _runtime_components: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        if let Some(metadata) = cfg.load::<Metadata>()
            && metadata.service().eq("S3")
            && is_checksum_required(metadata.name())
        {
            Self::calculate_md5_checksum(ctx.request_mut());
        }

        Ok(())
    }
}

/// Check if a checksum is required for the given S3 operation.
/// The list of operations requiring a checksum is based on the AWS S3 model definition,
/// see `https://github.com/smithy-lang/smithy-rs/blob/main/aws/sdk/aws-models/s3.json`
pub(crate) fn is_checksum_required(operation: &str) -> bool {
    matches!(
        operation,
        "CreateBucketMetadataTableConfiguration"
            | "DeleteObjects"
            | "PutBucketAcl"
            | "PutBucketCors"
            | "PutBucketEncryption"
            | "PutBucketLifecycleConfiguration"
            | "PutBucketLogging"
            | "PutBucketOwnershipControls"
            | "PutBucketPolicy"
            | "PutBucketReplication"
            | "PutBucketRequestPayment"
            | "PutBucketTagging"
            | "PutBucketVersioning"
            | "PutBucketWebsite"
            | "PutObjectAcl"
            | "PutObjectLegalHold"
            | "PutObjectLockConfiguration"
            | "PutObjectRetention"
            | "PutObjectTagging"
            | "PutPublicAccessBlock"
            | "UpdateBucketMetadataInventoryTableConfiguration"
            | "UpdateBucketMetadataJournalTableConfiguration"
    )
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
