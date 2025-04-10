#![allow(clippy::module_name_repetitions)]

use std::{collections::HashMap, str::FromStr, sync::LazyLock};

use aws_config::{identity::IdentityCache, BehaviorVersion, SdkConfig};
use aws_sdk_sts::config::{ProvideCredentials as _, SharedIdentityCache};
use iceberg_ext::configs::{
    self,
    table::{client, custom, s3, TableProperties},
    ConfigProperty, Location,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use veil::Redact;

use super::StorageType;
use crate::{
    api::{
        iceberg::{supported_endpoints, v1::DataAccess},
        CatalogConfig,
    },
    request_metadata::RequestMetadata,
    service::storage::{
        error::{CredentialsError, FileIoError, TableConfigError, UpdateError, ValidationError},
        StoragePermissions, TableConfig,
    },
    WarehouseIdent, CONFIG,
};

static S3_HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(reqwest::Client::new);
static STS_HTTP_CLIENT: LazyLock<aws_sdk_sts::config::SharedHttpClient> = LazyLock::new(|| {
    aws_smithy_http_client::Builder::new()
        .tls_provider(aws_smithy_http_client::tls::Provider::Rustls(
            aws_smithy_http_client::tls::rustls_provider::CryptoMode::AwsLc,
        ))
        .build_https()
});
static AWS_IDENTITY_CACHE: LazyLock<SharedIdentityCache> =
    LazyLock::new(|| IdentityCache::lazy().build());

#[derive(
    Debug,
    Eq,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    utoipa::ToSchema,
    typed_builder::TypedBuilder,
)]
#[serde(rename_all = "kebab-case")]
pub struct S3Profile {
    /// Name of the S3 bucket
    pub bucket: String,
    /// Subpath in the bucket to use.
    /// The same prefix can be used for multiple warehouses.
    #[builder(default, setter(strip_option))]
    pub key_prefix: Option<String>,
    #[serde(default)]
    /// Optional ARN to assume when accessing the bucket from Lakekeeper.
    #[builder(default, setter(strip_option))]
    pub assume_role_arn: Option<String>,
    /// Optional endpoint to use for S3 requests, if not provided
    /// the region will be used to determine the endpoint.
    /// If both region and endpoint are provided, the endpoint will be used.
    /// Example: `http://s3-de.my-domain.com:9000`
    #[serde(default)]
    #[builder(default, setter(strip_option))]
    pub endpoint: Option<url::Url>,
    /// Region to use for S3 requests.
    pub region: String,
    /// Path style access for S3 requests.
    /// If the underlying S3 supports both, we recommend to not set `path_style_access`.
    #[serde(default)]
    #[builder(default, setter(strip_option))]
    pub path_style_access: Option<bool>,
    /// Optional role ARN to assume for sts vended-credentials.
    /// If not provided, `assume_role_arn` is used.
    /// Either `assume_role_arn` or `sts_role_arn` must be provided if `sts_enabled` is true.
    #[builder(default, setter(strip_option))]
    pub sts_role_arn: Option<String>,
    pub sts_enabled: bool,
    /// S3 flavor to use.
    /// Defaults to AWS
    #[serde(default)]
    pub flavor: S3Flavor,
    /// Allow `s3a://` and `s3n://` in locations.
    /// This is disabled by default. We do not recommend to use this setting
    /// except for migration of old hadoop-based tables via the register endpoint.
    /// Tables with `s3a` paths are not accessible outside the Java ecosystem.
    #[serde(default)]
    #[builder(default, setter(strip_option))]
    pub allow_alternative_protocols: Option<bool>,
    /// S3 URL style detection mode for remote signing.
    /// One of `auto`, `path-style`, `virtual-host`.
    /// Default: `auto`. When set to `auto`, Lakekeeper will first try to parse the URL as
    /// `virtual-host` and then attempt `path-style`.
    /// `path` assumes the bucket name is the first path segment in the URL. `virtual-host`
    /// assumes the bucket name is the first subdomain if it is preceding `.s3` or `.s3-`.
    ///
    /// Examples
    ///
    /// Virtual host:
    ///   - <https://bucket.s3.endpoint.com/bar/a/key>
    ///   - <https://bucket.s3-eu-central-1.amazonaws.com/file>
    ///
    /// Path style:
    ///   - <https://s3.endpoint.com/bucket/bar/a/key>
    ///   - <https://s3.us-east-1.amazonaws.com/bucket/file>
    #[serde(default, alias = "s3-url-detection-mode")]
    #[builder(default)]
    pub remote_signing_url_style: S3UrlStyleDetectionMode,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum S3UrlStyleDetectionMode {
    /// Use the path style for all requests.
    Path,
    /// Use the virtual host style for all requests.
    VirtualHost,
    /// Automatically detect the style based on the request.
    #[default]
    Auto,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
#[derive(Default)]
pub enum S3Flavor {
    #[default]
    Aws,
    #[serde(alias = "minio")]
    S3Compat,
}

#[derive(Redact, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(tag = "credential-type", rename_all = "kebab-case")]
pub enum S3Credential {
    #[serde(rename_all = "kebab-case")]
    #[schema(title = "S3CredentialAccessKey")]
    /// Authenticate to AWS using access-key and secret-key.
    AccessKey {
        aws_access_key_id: String,
        #[redact(partial)]
        aws_secret_access_key: String,
        #[redact(partial)]
        external_id: Option<String>,
    },
    #[serde(rename_all = "kebab-case")]
    #[schema(title = "S3CredentialSystemIdentity")]
    /// Authenticate to AWS using the identity configured on the system
    ///  that runs lakekeeper. The AWS SDK is used to load the credentials.
    AwsSystemIdentity {
        #[redact(partial)]
        external_id: Option<String>,
    },
}

impl S3Credential {
    /// Get the external ID for the credential.
    /// Returns `None` if the credential is not an access key.
    #[must_use]
    pub fn external_id(&self) -> Option<&str> {
        match self {
            S3Credential::AccessKey { external_id, .. }
            | S3Credential::AwsSystemIdentity { external_id, .. } => external_id.as_deref(),
        }
    }
}

impl S3Profile {
    /// Allow alternate schemes for S3 locations.
    /// This is disabled by default.
    #[must_use]
    pub fn allow_alternate_schemes(&self) -> bool {
        self.allow_alternative_protocols.unwrap_or_default()
    }

    /// Check if a s3 variant is allowed.
    /// By default, only `s3` is allowed.
    /// If `allow_variant_schemes` is set, `s3a` and `s3n` are also allowed.
    #[must_use]
    pub fn is_allowed_schema(&self, schema: &str) -> bool {
        schema == "s3" || (self.allow_alternate_schemes() && (schema == "s3a" || schema == "s3n"))
    }

    /// Create a new `FileIO` instance for S3.
    ///
    /// # Errors
    /// Fails if the `FileIO` instance cannot be created.
    pub async fn file_io(
        &self,
        credential: Option<&S3Credential>,
    ) -> Result<iceberg::io::FileIO, FileIoError> {
        let mut builder =
            iceberg::io::FileIOBuilder::new("s3").with_client((*S3_HTTP_CLIENT).clone());

        builder = builder.with_prop(iceberg::io::S3_REGION, self.region.clone());

        if let Some(endpoint) = &self.endpoint {
            builder = builder.with_prop(iceberg::io::S3_ENDPOINT, endpoint);
        }
        if let Some(path_style_access) = &self.path_style_access {
            builder = builder.with_prop(
                iceberg::io::S3_PATH_STYLE_ACCESS,
                path_style_access.to_string(),
            );
        }

        builder = builder.with_prop(
            iceberg::io::S3_PATH_STYLE_ACCESS,
            self.path_style_access.unwrap_or_default(),
        );

        let credentials = self.get_credentials_for_assume_role(credential).await?;
        let builder = if let Some(credentials) = credentials {
            let builder = builder
                .with_prop(iceberg::io::S3_ACCESS_KEY_ID, credentials.access_key_id())
                .with_prop(
                    iceberg::io::S3_SECRET_ACCESS_KEY,
                    credentials.secret_access_key(),
                )
                .with_prop(iceberg::io::S3_DISABLE_CONFIG_LOAD, "true")
                .with_prop(iceberg::io::S3_DISABLE_EC2_METADATA, "true");

            if let Some(session_token) = credentials.session_token() {
                builder.with_prop(iceberg::io::S3_SESSION_TOKEN, session_token)
            } else {
                builder
            }
        } else {
            builder
                .with_prop(iceberg::io::S3_ALLOW_ANONYMOUS, "true")
                .with_prop(iceberg::io::S3_DISABLE_CONFIG_LOAD, "true")
                .with_prop(iceberg::io::S3_DISABLE_EC2_METADATA, "true")
        };

        Ok(builder.build()?)
    }

    /// Validate the S3 profile.
    ///
    /// # Errors
    /// - Fails if the bucket name is invalid.
    /// - Fails if the region is too long.
    /// - Fails if the key prefix is too long.
    /// - Fails if the region or endpoint is missing.
    /// - Fails if the endpoint is not a valid URL.
    pub(super) fn normalize(&mut self) -> Result<(), ValidationError> {
        validate_bucket_name(&self.bucket)?;
        validate_region(&self.region)?;
        self.normalize_key_prefix()?;
        self.normalize_endpoint()?;
        self.normalize_assume_role_arn();
        self.normalize_sts_role_arn();

        if self.sts_enabled
            && matches!(self.flavor, S3Flavor::Aws)
            && self.sts_role_arn.is_none()
            && self.assume_role_arn.is_none()
        {
            return Err(ValidationError::InvalidProfile {
                source: None,
                reason: "Either `sts-role-arn` or `assume-role-arn` is required for Storage Profiles with AWS flavor if STS is enabled.".to_string(),
                entity: "sts-role-arn".to_string(),
            });
        }

        Ok(())
    }

    /// Check if the profile can be updated with the other profile.
    /// `key_prefix`, `region` and `bucket` must be the same.
    /// We enforce this to avoid issues by accidentally changing the bucket or region
    /// of a warehouse, after which all tables would not be accessible anymore.
    /// Changing an endpoint might still result in an invalid profile, but we allow it.
    ///
    /// # Errors
    /// Fails if the `bucket`, `region` or `key_prefix` is different.
    pub fn update_with(self, mut other: Self) -> Result<Self, UpdateError> {
        if self.bucket != other.bucket {
            return Err(UpdateError::ImmutableField("bucket".to_string()));
        }

        if self.region != other.region {
            return Err(UpdateError::ImmutableField("region".to_string()));
        }

        if self.key_prefix != other.key_prefix {
            return Err(UpdateError::ImmutableField("key_prefix".to_string()));
        }

        if self.allow_alternate_schemes() && other.allow_alternative_protocols.is_none() {
            // Keep previous true value if not specified explicitly in update
            other.allow_alternative_protocols = Some(true);
        }

        Ok(other)
    }

    #[must_use]
    pub fn generate_catalog_config(
        &self,
        warehouse_id: WarehouseIdent,
        request_metadata: &RequestMetadata,
    ) -> CatalogConfig {
        CatalogConfig {
            // ToDo: s3.delete-enabled?
            // if we don't do this, icebergs spark s3 attempts to sign a link that looks like /bucket?delete
            // when DROP ... PURGE-ing a table.
            defaults: HashMap::from_iter([("s3.delete-enabled".to_string(), "false".to_string())]),
            overrides: HashMap::from_iter(vec![(
                configs::table::s3::SignerUri::KEY.to_string(),
                request_metadata
                    .s3_signer_uri_for_warehouse(warehouse_id)
                    .to_string(),
            )]),
            endpoints: supported_endpoints().to_vec(),
        }
    }

    /// Base Location for this storage profile.
    ///
    /// # Errors
    /// Can fail for un-normalized profiles
    pub fn base_location(&self) -> Result<S3Location, ValidationError> {
        let prefix = self
            .key_prefix
            .as_ref()
            .map(|s| s.split('/').map(std::borrow::ToOwned::to_owned).collect())
            .unwrap_or_default();
        S3Location::new(self.bucket.clone(), prefix, None)
    }

    /// Generate the table configuration for S3.
    ///
    /// # Errors
    /// Fails if vended credentials are used - currently not supported.
    pub async fn generate_table_config(
        &self,
        DataAccess {
            vended_credentials,
            remote_signing,
        }: DataAccess,
        s3_credential: Option<&S3Credential>,
        table_location: &Location,
        storage_permissions: StoragePermissions,
    ) -> Result<TableConfig, TableConfigError> {
        // If vended_credentials is False and remote_signing is False,
        // use remote_signing.
        let mut remote_signing = !vended_credentials || remote_signing;

        let mut config = TableProperties::default();
        let mut creds = TableProperties::default();

        if let Some(true) = self.path_style_access {
            config.insert(&s3::PathStyleAccess(true));
        }

        config.insert(&s3::Region(self.region.to_string()));
        config.insert(&client::Region(self.region.to_string()));
        config.insert(&custom::CustomConfig {
            key: "region".to_string(),
            value: self.region.to_string(),
        });
        config.insert(&client::Region(self.region.to_string()));

        if let Some(endpoint) = &self.endpoint {
            config.insert(&s3::Endpoint(endpoint.clone()));
        }

        if vended_credentials {
            if self.sts_enabled {
                let aws_sdk_sts::types::Credentials {
                    access_key_id,
                    secret_access_key,
                    session_token,
                    expiration: _,
                    ..
                } = if let Some(arn) = self.sts_role_arn.as_ref().or(self.assume_role_arn.as_ref())
                {
                    self.get_aws_sts_token(table_location, s3_credential, arn, storage_permissions)
                        .await?
                } else if S3Flavor::S3Compat == self.flavor {
                    self.get_minio_sts_token(table_location, s3_credential, storage_permissions)
                        .await?
                } else {
                    return Err(TableConfigError::Misconfiguration(
                        "Either `sts-role-arn` or `assume-role-arn` is required for Storage Profiles with AWS flavor if STS is enabled.".to_string(),
                    ));
                };
                config.insert(&s3::AccessKeyId(access_key_id.clone()));
                config.insert(&s3::SecretAccessKey(secret_access_key.clone()));
                config.insert(&s3::SessionToken(session_token.clone()));
                creds.insert(&s3::AccessKeyId(access_key_id));
                creds.insert(&s3::SecretAccessKey(secret_access_key));
                creds.insert(&s3::SessionToken(session_token));
            } else {
                insert_pyiceberg_hack(&mut config);
                remote_signing = true;
            }
        }

        if remote_signing {
            config.insert(&s3::RemoteSigningEnabled(true));
            // Currently per-table signer uris are not supported by Spark.
            // The URI is cached for one table, and then re-used for another.
            // let signer_uri = CONFIG.s3_signer_uri_for_table(warehouse_id, namespace_id, table_id);
            // config.insert("s3.signer.uri".to_string(), signer_uri.to_string());
        }

        Ok(TableConfig { creds, config })
    }

    async fn get_aws_sts_token(
        &self,
        table_location: &Location,
        cred: Option<&S3Credential>,
        arn: &str,
        storage_permissions: StoragePermissions,
    ) -> Result<aws_sdk_sts::types::Credentials, CredentialsError> {
        self.get_sts_token(table_location, cred, Some(arn), storage_permissions)
            .await
    }

    async fn get_minio_sts_token(
        &self,
        table_location: &Location,
        cred: Option<&S3Credential>,
        storage_permissions: StoragePermissions,
    ) -> Result<aws_sdk_sts::types::Credentials, CredentialsError> {
        self.get_sts_token(table_location, cred, None, storage_permissions)
            .await
    }

    async fn assume_role_with_sts(
        &self,
        s3_credentials: Option<&S3Credential>,
        role_arn: Option<&str>,
        policy: Option<String>,
    ) -> Result<aws_sdk_sts::types::Credentials, CredentialsError> {
        let external_id = s3_credentials.and_then(|c| c.external_id());

        if role_arn.is_none()
            && matches!(s3_credentials, Some(S3Credential::AwsSystemIdentity { .. }))
            && !CONFIG.s3_enable_direct_system_credentials
        {
            return Err(CredentialsError::Misconfiguration(
            "This deployment of Lakekeeper requires an `assume-role-arn` to be set for system identity credentials."
                .to_string(),
        ));
        }

        if external_id.is_none()
            && role_arn.is_some()
            && CONFIG.s3_require_external_id_for_system_credentials
            && matches!(s3_credentials, Some(S3Credential::AwsSystemIdentity { .. }))
        {
            return Err(CredentialsError::Misconfiguration(
                "An `external-id` is required when using `assume-role-arn`.".to_string(),
            ));
        }

        let sdk_config = self.get_aws_sdk_config(s3_credentials).await?;

        // ToDo: Test caching
        let assume_role_builder = aws_sdk_sts::Client::new(&sdk_config)
            .assume_role()
            .role_session_name("lakekeeper-sts");

        // Attach policy if provided
        let assume_role_builder = if let Some(policy) = policy {
            assume_role_builder.policy(policy)
        } else {
            assume_role_builder
        };

        // Attach ARN if provided.
        // Some non AWS S3 implementations (e.g. MinIO) don't require an arn.
        let assume_role_builder = if let Some(role_arn) = role_arn {
            assume_role_builder.role_arn(role_arn)
        } else {
            assume_role_builder
        };

        let assume_role_builder = if let Some(external_id) = external_id {
            assume_role_builder.external_id(external_id)
        } else {
            assume_role_builder
        };

        let v = assume_role_builder.send().await.map_err(|e| {
            let err_str = format!("{e:?}");
            tracing::warn!("Failed to assume role via STS: {err_str}");
            CredentialsError::ShortTermCredential {
                source: Some(Box::new(e)),
                reason: format!("Failed to assume role via STS: {err_str}").to_string(),
            }
        })?;

        v.credentials.ok_or_else(|| {
            tracing::warn!("No credentials returned from STS");
            CredentialsError::ShortTermCredential {
                source: None,
                reason: "No credentials returned from STS".to_string(),
            }
        })
    }

    async fn get_sts_token(
        &self,
        table_location: &Location,
        s3_credential: Option<&S3Credential>,
        role_arn: Option<&str>,
        storage_permissions: StoragePermissions,
    ) -> Result<aws_sdk_sts::types::Credentials, CredentialsError> {
        let policy = Self::get_aws_policy_string(table_location, storage_permissions)?;
        self.assume_role_with_sts(s3_credential, role_arn, Some(policy))
            .await
    }

    /// Load the native AWS SDK config without assuming any role.
    async fn get_aws_sdk_config(
        &self,
        s3_credential: Option<&S3Credential>,
    ) -> Result<SdkConfig, CredentialsError> {
        if matches!(s3_credential, Some(S3Credential::AwsSystemIdentity { .. }))
            && !CONFIG.enable_aws_system_credentials
        {
            return Err(CredentialsError::Misconfiguration(
                "System identity credentials are disabled in this Lakekeeper deployment."
                    .to_string(),
            ));
        }

        let loader = match s3_credential {
            Some(S3Credential::AccessKey {
                aws_access_key_id,
                aws_secret_access_key,
                external_id: _,
            }) => {
                let aws_credentials = aws_credential_types::Credentials::new(
                    aws_access_key_id,
                    aws_secret_access_key,
                    None,
                    None,
                    "lakekeeper-secret-storage",
                );
                aws_config::ConfigLoader::default().credentials_provider(aws_credentials)
            }
            Some(S3Credential::AwsSystemIdentity { external_id: _ }) => aws_config::from_env(),
            None => aws_config::from_env().no_credentials(),
        }
        .region(Some(aws_config::Region::new(
            self.region.as_str().to_string(),
        )))
        .behavior_version(BehaviorVersion::latest())
        .http_client((*STS_HTTP_CLIENT).clone())
        .identity_cache(AWS_IDENTITY_CACHE.clone());

        let loader = if let Some(endpoint) = &self.endpoint {
            loader.endpoint_url(endpoint.to_string())
        } else {
            loader
        };

        Ok(loader.load().await)
    }

    pub(crate) async fn get_credentials_for_assume_role(
        &self,
        s3_credential: Option<&S3Credential>,
    ) -> Result<Option<aws_credential_types::Credentials>, CredentialsError> {
        if let Some(assume_role_arn) = &self.assume_role_arn {
            let aws_sts_credential = self
                .assume_role_with_sts(s3_credential, Some(assume_role_arn), None)
                .await?;
            let aws_credential = aws_credential_types::Credentials::new(
                aws_sts_credential.access_key_id(),
                aws_sts_credential.secret_access_key(),
                Some(aws_sts_credential.session_token().to_string()),
                std::time::SystemTime::try_from(*aws_sts_credential.expiration())
                    .inspect_err(|e| tracing::warn!("Failed to convert AWS smithy expiration for credentials to SystemTime: {e:?}"))
                    .ok(),
                "lakekeeper-secret-storage-assume-role",
            );
            Ok(Some(aws_credential))
        } else {
            match s3_credential {
                Some(S3Credential::AccessKey {
                    aws_access_key_id,
                    aws_secret_access_key,
                    external_id: _,
                }) => {
                    let aws_credential = aws_credential_types::Credentials::new(
                        aws_access_key_id,
                        aws_secret_access_key,
                        None,
                        None,
                        "lakekeeper-secret-storage-native",
                    );
                    Ok(Some(aws_credential))
                }
                Some(S3Credential::AwsSystemIdentity { external_id: _ }) | None => {
                    let sdk_config = self.get_aws_sdk_config(s3_credential).await?;
                    let Some(provider) = sdk_config.credentials_provider() else {
                        return Ok(None);
                    };

                    let aws_credential = provider
                        .provide_credentials()
                        .await
                        .map_err(|e| {
                            tracing::error!("Failed to obtain credentials from environment (S3Credential: {s3_credential:?}): {e:?}");
                            CredentialsError::ShortTermCredential {
                                source: Some(Box::new(e)),
                                reason: "Failed to obtain S3 credentials from environment".to_string(),
                            }
                        })?;
                    Ok(Some(aws_credential))
                }
            }
        }
    }

    fn permission_to_actions(storage_permissions: StoragePermissions) -> &'static str {
        match storage_permissions {
            StoragePermissions::Read => "\"s3:GetObject\"",
            StoragePermissions::ReadWrite => "\"s3:GetObject\", \"s3:PutObject\"",
            StoragePermissions::ReadWriteDelete => {
                "\"s3:GetObject\", \"s3:PutObject\", \"s3:DeleteObject\""
            }
        }
    }

    fn get_aws_policy_string(
        table_location: &Location,
        storage_permissions: StoragePermissions,
    ) -> Result<String, CredentialsError> {
        let table_location = S3Location::try_from_location(table_location, true).map_err(|e| {
            CredentialsError::ShortTermCredential {
                source: None,
                reason: format!("Could not generate downscoped policy for temporary credentials as location is no valid S3 location: {e}").to_string(),
            }
        })?;
        let bucket_arn = format!(
            "arn:aws:s3:::{}",
            table_location.bucket_name().trim_end_matches('/')
        );
        let key = table_location.key().join("/");
        let key = format!("{key}/");

        Ok(format!(
            r#"{{
        "Version": "2012-10-17",
        "Statement": [
            {{
                "Sid": "TableAccess",
                "Effect": "Allow",
                "Action": [
                    {}
                ],
                "Resource": [
                    "{bucket_arn}/{key}",
                    "{bucket_arn}/{key}*"
                ]
            }},
            {{
                "Sid": "ListBucketForFolder",
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": "{bucket_arn}",
                "Condition": {{
                    "StringLike": {{
                        "s3:prefix": "{key}*"
                    }}
                }}
            }}
        ]
    }}"#,
            Self::permission_to_actions(storage_permissions),
        )
        .replace('\n', "")
        .replace(' ', ""))
    }

    fn normalize_key_prefix(&mut self) -> Result<(), ValidationError> {
        if let Some(key_prefix) = self.key_prefix.as_mut() {
            *key_prefix = key_prefix.trim_matches('/').to_string();
        }

        if let Some(key_prefix) = self.key_prefix.as_ref() {
            if key_prefix.is_empty() {
                self.key_prefix = None;
            }
        }

        // Aws supports a max of 1024 chars and we need some buffer for tables.
        if let Some(key_prefix) = self.key_prefix.as_ref() {
            if key_prefix.len() > 896 {
                return Err(ValidationError::InvalidProfile {
                    source: None,
                    reason: "Storage Profile `key_prefix` must be less than 896 characters."
                        .to_string(),
                    entity: "key_prefix".to_string(),
                });
            }
        }
        Ok(())
    }

    fn normalize_endpoint(&mut self) -> Result<(), ValidationError> {
        if let Some(endpoint) = self.endpoint.as_mut() {
            if endpoint.scheme() != "http" && endpoint.scheme() != "https" {
                return Err(ValidationError::InvalidProfile {
                    source: None,
                    reason: "Storage Profile `endpoint` must have http or https protocol."
                        .to_string(),
                    entity: "S3Endpoint".to_string(),
                });
            }

            // If a non-empty path is provided, it must be a single slash which we remove.
            if !endpoint.path().is_empty() {
                if endpoint.path() != "/" {
                    return Err(ValidationError::InvalidProfile {
                        source: None,
                        reason: "Storage Profile `endpoint` must not have a path.".to_string(),
                        entity: "S3Endpoint".to_string(),
                    });
                }

                endpoint.set_path("/");
            }
        }

        Ok(())
    }

    fn normalize_assume_role_arn(&mut self) {
        if let Some(assume_role_arn) = self.assume_role_arn.as_ref() {
            if assume_role_arn.is_empty() {
                self.assume_role_arn = None;
            }
        }
    }

    fn normalize_sts_role_arn(&mut self) {
        if let Some(sts_role_arn) = self.sts_role_arn.as_ref() {
            if sts_role_arn.is_empty() {
                self.sts_role_arn = None;
            }
        }
    }
}

pub(super) fn get_file_io_from_table_config(
    config: &TableProperties,
) -> Result<iceberg::io::FileIO, FileIoError> {
    let mut builder = iceberg::io::FileIOBuilder::new("s3").with_client((*S3_HTTP_CLIENT).clone());

    for key in [
        s3::Region::KEY,
        s3::Endpoint::KEY,
        s3::AccessKeyId::KEY,
        s3::SecretAccessKey::KEY,
        s3::SessionToken::KEY,
        s3::PathStyleAccess::KEY,
    ] {
        if let Some(value) = config.get_custom_prop(key) {
            builder = builder.with_prop(key, value);
        }
    }

    Ok(builder.build()?)
}

fn validate_region(region: &str) -> Result<(), ValidationError> {
    if region.len() > 128 {
        return Err(ValidationError::InvalidProfile {
            source: None,
            reason: "`region` must be less than 128 characters.".to_string(),
            entity: "region".to_string(),
        });
    }

    Ok(())
}

fn validate_bucket_name(bucket: &str) -> Result<(), ValidationError> {
    // Bucket names must be between 3 (min) and 63 (max) characters long.
    if bucket.len() < 3 || bucket.len() > 63 {
        return Err(ValidationError::InvalidProfile {
            source: None,
            reason: "`bucket` must be between 3 and 63 characters long.".to_string(),
            entity: "BucketName".to_string(),
        });
    }

    // Bucket names can consist only of lowercase letters, numbers, dots (.), and hyphens (-).
    if !bucket
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '-')
    {
        return Err(
            ValidationError::InvalidProfile {
                source: None,
                reason: "Bucket name can consist only of lowercase letters, numbers, dots (.), and hyphens (-).".to_string(),
                entity: "BucketName".to_string(),
            }
        );
    }

    // Bucket names must begin and end with a letter or number.
    // Unwrap will not fail as the length is already checked.
    if !bucket.chars().next().unwrap().is_ascii_alphanumeric()
        || !bucket.chars().last().unwrap().is_ascii_alphanumeric()
    {
        return Err(ValidationError::InvalidProfile {
            source: None,
            reason: "Bucket name must begin and end with a letter or number.".to_string(),
            entity: "BucketName".to_string(),
        });
    }

    // Bucket names must not contain two adjacent periods.
    if bucket.contains("..") {
        return Err(ValidationError::InvalidProfile {
            source: None,
            reason: "Bucket name must not contain two adjacent periods.".to_string(),
            entity: "BucketName".to_string(),
        });
    }

    Ok(())
}

fn insert_pyiceberg_hack(config: &mut TableProperties) {
    config.insert(&s3::Signer("S3V4RestSigner".to_string()));
    config.insert(&custom::CustomConfig {
        key: "py-io-impl".to_string(),
        value: "pyiceberg.io.fsspec.FsspecFileIO".to_string(),
    });
}

// S3Location exists as part of aws_sdk_s3::types, however we don't depend on it yet
// and there is no parse() function available. The prefix is also represented as a
// String, which makes it harder to work with.
#[derive(Debug, Clone, PartialEq)]
pub struct S3Location {
    bucket_name: String,
    key: Vec<String>,
    // Location is redundant but useful for type-safe access.
    location: Location,
    custom_prefix: Option<String>,
}

impl std::fmt::Display for S3Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.location.fmt(f)
    }
}

impl S3Location {
    /// Create a new S3 location.
    ///
    /// # Errors
    /// Fails if the bucket name is invalid or the key contains unescaped slashes.
    pub fn new(
        bucket_name: String,
        key: Vec<String>,
        custom_prefix: Option<String>,
    ) -> Result<Self, ValidationError> {
        validate_bucket_name(&bucket_name)?;
        // Keys may not contain slashes
        if key.iter().any(|k| k.contains('/')) {
            return Err(ValidationError::InvalidLocation {
                source: None,
                reason: "S3 key contains unescaped slashes (/)".to_string(),
                location: format!("{key:?}"),
                storage_type: StorageType::S3,
            });
        }

        let location = format!("s3://{bucket_name}");
        let mut location =
            Location::from_str(&location).map_err(|e| ValidationError::InvalidLocation {
                reason: "Invalid S3 location.".to_string(),
                location: location.clone(),
                source: Some(e.into()),
                storage_type: StorageType::S3,
            })?;
        if !key.is_empty() {
            location.without_trailing_slash().extend(key.iter());
        }

        Ok(S3Location {
            bucket_name,
            key,
            location,
            custom_prefix,
        })
    }

    #[must_use]
    pub fn bucket_name(&self) -> &str {
        &self.bucket_name
    }

    #[must_use]
    pub fn key(&self) -> &Vec<String> {
        &self.key
    }

    #[must_use]
    pub fn location(&self) -> &Location {
        &self.location
    }

    /// Create a new S3 location from a `Location`.
    ///
    /// If `allow_variants` is set to true, `s3a://` and `s3n://` schemes are allowed.
    ///
    /// # Errors
    /// - Fails if the location is not a valid S3 location
    pub fn try_from_location(
        location: &Location,
        allow_variants: bool,
    ) -> Result<Self, ValidationError> {
        let is_custom_variant = ["s3a", "s3n"].contains(&location.url().scheme());
        // Protocol must be s3
        if (location.url().scheme() != "s3") && !(allow_variants && is_custom_variant) {
            let reason = if allow_variants {
                format!(
                    "S3 location must use s3, s3a or s3n protocol. Found: {}",
                    location.url().scheme()
                )
            } else {
                format!(
                    "S3 location must use s3 protocol. Found: {}",
                    location.url().scheme()
                )
            };
            return Err(ValidationError::InvalidLocation {
                reason,
                location: location.to_string(),
                source: None,
                storage_type: StorageType::S3,
            });
        }

        let bucket_name =
            location
                .url()
                .host_str()
                .ok_or_else(|| ValidationError::InvalidLocation {
                    reason: "S3 location does not have a bucket name.".to_string(),
                    location: location.to_string(),
                    source: None,
                    storage_type: StorageType::S3,
                })?;

        let key: Vec<String> = location
            .url()
            .path_segments()
            .map_or(Vec::new(), |segments| {
                segments.map(std::string::ToString::to_string).collect()
            });

        if is_custom_variant {
            S3Location::new(
                bucket_name.to_string(),
                key,
                Some(location.url().scheme().to_string()),
            )
        } else {
            S3Location::new(bucket_name.to_string(), key, None)
        }
    }

    /// Create a new S3 location from a string.
    ///
    /// If `allow_s3a` is set to true, `s3a://` and `s3n://` schemes are allowed.
    ///
    /// # Errors
    /// - Fails if the location is not a valid S3 location
    pub fn try_from_str(s: &str, allow_s3a: bool) -> Result<Self, ValidationError> {
        let location = Location::from_str(s).map_err(|e| ValidationError::InvalidLocation {
            reason: "Invalid S3 location.".to_string(),
            location: s.to_string(),
            source: Some(e.into()),
            storage_type: StorageType::S3,
        })?;

        Self::try_from_location(&location, allow_s3a)
    }

    /// Always returns `s3://` prefixed location.
    pub(crate) fn into_normalized_location(self) -> Location {
        self.location
    }
}

#[cfg(test)]
pub(crate) mod test {
    use needs_env_var::needs_env_var;

    use super::*;
    use crate::service::{
        storage::{StorageLocations as _, StorageProfile},
        tabular_idents::TabularIdentUuid,
        NamespaceIdentUuid,
    };

    #[test]
    fn test_storage_secret_deserialization_access_key_1() {
        let secret = serde_json::json!(
            {
                "credential-type": "access-key",
                "aws-access-key-id": "foo",
                "aws-secret-access-key": "bar",
            }
        );
        let credential: S3Credential = serde_json::from_value(secret).unwrap();
        let expected = S3Credential::AccessKey {
            aws_access_key_id: "foo".to_string(),
            aws_secret_access_key: "bar".to_string(),
            external_id: None,
        };
        assert_eq!(credential, expected);
    }

    #[test]
    fn test_storage_secret_deserialization_access_key_2() {
        let secret = serde_json::json!(
            {
                "credential-type": "access-key",
                "aws-access-key-id": "foo",
                "aws-secret-access-key": "bar",
                "external-id": "baz",
            }
        );
        let credential: S3Credential = serde_json::from_value(secret).unwrap();
        let expected = S3Credential::AccessKey {
            aws_access_key_id: "foo".to_string(),
            aws_secret_access_key: "bar".to_string(),
            external_id: Some("baz".to_string()),
        };
        assert_eq!(credential, expected);
    }

    #[test]
    fn test_storage_secret_deserialization_system_identity_1() {
        let secret = serde_json::json!(
            {
                "credential-type": "aws-system-identity",
            }
        );
        let credential: S3Credential = serde_json::from_value(secret).unwrap();
        let expected = S3Credential::AwsSystemIdentity { external_id: None };
        assert_eq!(credential, expected);
    }

    #[test]
    fn test_storage_secret_deserialization_system_identity_2() {
        let secret = serde_json::json!(
            {
                "credential-type": "aws-system-identity",
                "external-id": "baz",
            }
        );
        let credential: S3Credential = serde_json::from_value(secret).unwrap();
        let expected = S3Credential::AwsSystemIdentity {
            external_id: Some("baz".to_string()),
        };
        assert_eq!(credential, expected);
    }

    #[test]
    fn test_is_valid_bucket_name() {
        let cases = vec![
            ("foo".to_string(), true),
            ("my-bucket".to_string(), true),
            ("my.bucket".to_string(), true),
            ("my..bucket".to_string(), false),
            // 64 characters
            ("a".repeat(63), true),
            ("a".repeat(64), false),
            // 2 characters
            ("a".repeat(2), false),
            ("a".repeat(3), true),
            // Special-chars
            ("1bucket".to_string(), true),
            ("my_bucket".to_string(), false),
            ("my-ö-bucket".to_string(), false),
            // Invalid start / end chars
            (".my-bucket".to_string(), false),
            ("my-bucket.".to_string(), false),
        ];

        for (bucket, expected) in cases {
            let result = validate_bucket_name(&bucket);
            if expected {
                assert!(result.is_ok());
            } else {
                assert!(result.is_err());
            }
        }
    }

    #[test]
    fn test_default_s3_locations() {
        let profile = S3Profile {
            bucket: "test-bucket".to_string(),
            key_prefix: Some("test_prefix".to_string()),
            assume_role_arn: None,
            endpoint: None,
            region: "dummy".to_string(),
            path_style_access: Some(true),
            sts_role_arn: None,
            sts_enabled: false,
            flavor: S3Flavor::Aws,
            allow_alternative_protocols: Some(false),
            remote_signing_url_style: S3UrlStyleDetectionMode::Auto,
        };
        let sp: StorageProfile = profile.clone().into();

        let namespace_id = NamespaceIdentUuid::from(uuid::Uuid::now_v7());
        let table_id = TabularIdentUuid::Table(uuid::Uuid::now_v7());
        let namespace_location = sp.default_namespace_location(namespace_id).unwrap();

        let location = sp.default_tabular_location(&namespace_location, table_id);
        assert_eq!(
            location.to_string(),
            format!("s3://test-bucket/test_prefix/{namespace_id}/{table_id}")
        );

        let mut profile = profile.clone();
        profile.key_prefix = None;
        let sp: StorageProfile = profile.into();

        let namespace_location = sp.default_namespace_location(namespace_id).unwrap();
        let location = sp.default_tabular_location(&namespace_location, table_id);
        assert_eq!(
            location.to_string(),
            format!("s3://test-bucket/{namespace_id}/{table_id}")
        );
    }

    #[test]
    /// Tests that the tabular location is correctly generated when the namespace location
    /// independent of a trailing slash in the namespace location.
    fn test_tabular_location_trailing_slash() {
        let profile = S3Profile {
            bucket: "test-bucket".to_string(),
            key_prefix: Some("test_prefix".to_string()),
            assume_role_arn: None,
            endpoint: None,
            region: "dummy".to_string(),
            path_style_access: Some(true),
            sts_role_arn: None,
            sts_enabled: false,
            flavor: S3Flavor::Aws,
            allow_alternative_protocols: Some(false),
            remote_signing_url_style: S3UrlStyleDetectionMode::Auto,
        };

        let namespace_location = Location::from_str("s3://test-bucket/foo/").unwrap();
        let table_id = TabularIdentUuid::Table(uuid::Uuid::now_v7());
        // Prefix should be ignored as we specify the namespace_location explicitly.
        // Tabular locations should not have a trailing slash, otherwise pyiceberg fails.
        let expected = format!("s3://test-bucket/foo/{table_id}");

        let location = profile.default_tabular_location(&namespace_location, table_id);

        assert_eq!(location.to_string(), expected);

        let namespace_location = Location::from_str("s3://test-bucket/foo").unwrap();
        let location = profile.default_tabular_location(&namespace_location, table_id);
        assert_eq!(location.to_string(), expected);
    }

    #[needs_env_var(TEST_MINIO = 1)]
    pub(crate) mod minio {
        use crate::service::storage::{
            S3Credential, S3Flavor, S3Profile, StorageCredential, StorageProfile,
        };

        lazy_static::lazy_static! {
            static ref TEST_BUCKET: String = std::env::var("LAKEKEEPER_TEST__S3_BUCKET").unwrap();
            static ref TEST_REGION: String = std::env::var("LAKEKEEPER_TEST__S3_REGION").unwrap_or("local".into());
            static ref TEST_ACCESS_KEY: String = std::env::var("LAKEKEEPER_TEST__S3_ACCESS_KEY").unwrap();
            static ref TEST_SECRET_KEY: String = std::env::var("LAKEKEEPER_TEST__S3_SECRET_KEY").unwrap();
            static ref TEST_ENDPOINT: String = std::env::var("LAKEKEEPER_TEST__S3_ENDPOINT").unwrap();
        }

        pub(crate) fn storage_profile(prefix: &str) -> (S3Profile, S3Credential) {
            let profile = S3Profile {
                bucket: TEST_BUCKET.clone(),
                key_prefix: Some(prefix.to_string()),
                assume_role_arn: None,
                endpoint: Some(TEST_ENDPOINT.clone().parse().unwrap()),
                region: TEST_REGION.clone(),
                path_style_access: Some(true),
                sts_role_arn: None,
                flavor: S3Flavor::S3Compat,
                sts_enabled: true,
                allow_alternative_protocols: Some(false),
                remote_signing_url_style:
                    crate::service::storage::s3::S3UrlStyleDetectionMode::Auto,
            };
            let cred = S3Credential::AccessKey {
                aws_access_key_id: TEST_ACCESS_KEY.clone(),
                aws_secret_access_key: TEST_SECRET_KEY.clone(),
                external_id: None,
            };

            (profile, cred)
        }

        #[test]
        fn test_can_validate() {
            // we need to use a shared runtime since the static client is shared between tests here
            // and tokio::test creates a new runtime for each test. For now, we only encounter the
            // issue here, eventually, we may want to move this to a proc macro like tokio::test or
            // sqlx::test
            crate::test::test_block_on(
                async {
                    let key_prefix = format!("test_prefix-{}", uuid::Uuid::now_v7());
                    let (profile, cred) = storage_profile(&key_prefix);
                    let mut profile: StorageProfile = profile.into();
                    let cred: StorageCredential = cred.into();

                    profile.normalize().unwrap();
                    profile.validate_access(Some(&cred), None).await.unwrap();
                },
                true,
            );
        }
    }

    #[needs_env_var(TEST_AWS = 1)]
    pub(crate) mod aws {
        use super::super::*;
        use crate::service::storage::{StorageCredential, StorageProfile};

        pub(crate) fn get_storage_profile() -> (S3Profile, S3Credential) {
            let profile = S3Profile {
                bucket: std::env::var("AWS_S3_BUCKET").unwrap(),
                key_prefix: Some(uuid::Uuid::now_v7().to_string()),
                assume_role_arn: None,
                endpoint: None,
                region: std::env::var("AWS_S3_REGION").unwrap(),
                path_style_access: Some(true),
                sts_role_arn: Some(std::env::var("AWS_S3_STS_ROLE_ARN").unwrap()),
                flavor: S3Flavor::Aws,
                sts_enabled: true,
                allow_alternative_protocols: Some(false),
                remote_signing_url_style:
                    crate::service::storage::s3::S3UrlStyleDetectionMode::Auto,
            };
            let cred = S3Credential::AccessKey {
                aws_access_key_id: std::env::var("AWS_S3_ACCESS_KEY_ID").unwrap(),
                aws_secret_access_key: std::env::var("AWS_S3_SECRET_ACCESS_KEY").unwrap(),
                external_id: None,
            };

            (profile, cred)
        }

        #[test]
        fn test_can_validate() {
            // we need to use a shared runtime since the static client is shared between tests here
            // and tokio::test creates a new runtime for each test. For now, we only encounter the
            // issue here, eventually, we may want to move this to a proc macro like tokio::test or
            // sqlx::test
            crate::test::test_block_on(
                async {
                    let (profile, cred) = get_storage_profile();
                    let cred: StorageCredential = cred.into();
                    let mut profile: StorageProfile = profile.into();

                    profile.normalize().unwrap();
                    profile.validate_access(Some(&cred), None).await.unwrap();
                },
                true,
            );
        }
    }

    #[test]
    fn test_parse_s3_location() {
        let cases = vec![
            (
                "s3://test-bucket/test_prefix/namespace/table",
                "test-bucket",
                vec!["test_prefix", "namespace", "table"],
            ),
            (
                "s3://test-bucket/test_prefix/namespace/table/",
                "test-bucket",
                vec!["test_prefix", "namespace", "table", ""],
            ),
            (
                "s3://test-bucket/test_prefix",
                "test-bucket",
                vec!["test_prefix"],
            ),
            (
                "s3://test-bucket/test_prefix/",
                "test-bucket",
                vec!["test_prefix", ""],
            ),
            ("s3://test-bucket/", "test-bucket", vec![""]),
            ("s3://test-bucket", "test-bucket", vec![]),
        ];

        for (location, bucket, prefix) in cases {
            let result = S3Location::try_from_str(location, false).unwrap();
            assert_eq!(result.bucket_name, bucket);
            assert_eq!(result.key, prefix);
            assert_eq!(result.to_string(), location);
        }
    }

    #[test]
    fn parse_invalid_s3_location() {
        let cases = vec![
            // wrong prefix
            "abc://test-bucket/foo",
            "test-bucket/foo",
            "/test-bucket/foo",
            // Invalid bucket name
            "s3://test_bucket/foo",
            // S3a is not allowed
            "s3a://test-bucket/foo",
        ];

        for case in cases {
            let result = S3Location::try_from_str(case, false);
            assert!(result.is_err());
        }
    }

    #[test]
    fn policy_string_is_json() {
        let table_location = "s3://bucket-name/path/to/table";
        let policy = S3Profile::get_aws_policy_string(
            &table_location.parse().unwrap(),
            StoragePermissions::ReadWriteDelete,
        )
        .unwrap();
        let _ = serde_json::from_str::<serde_json::Value>(&policy).unwrap();
    }

    #[test]
    fn test_parse_s3_location_invalid_proto() {
        S3Location::try_from_str("adls://test-bucket/foo/", false).unwrap_err();
    }

    #[test]
    fn test_parse_s3a_location() {
        let location = S3Location::try_from_str("s3a://test-bucket/foo/", true).unwrap();
        assert_eq!(
            location.into_normalized_location().to_string(),
            "s3://test-bucket/foo/",
        );
    }

    #[test]
    fn test_s3_location_display() {
        let cases = vec![
            "s3://bucket/foo",
            "s3://bucket/foo/bar",
            "s3://bucket/foo/bar/",
        ];
        for case in cases {
            let location = S3Location::try_from_str(case, false).unwrap();
            let printed = location.to_string();
            assert_eq!(printed, case);
        }
    }
}
