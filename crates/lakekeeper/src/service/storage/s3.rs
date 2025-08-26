#![allow(clippy::module_name_repetitions)]

use std::{collections::HashMap, sync::LazyLock};

use aws_config::SdkConfig;
use aws_sdk_sts::config::ProvideCredentials;
use aws_smithy_runtime_api::client::identity::Identity;
use iceberg_ext::{
    catalog::rest::ErrorModel,
    configs::{
        table::{client, custom, s3, TableProperties},
        ConfigProperty,
    },
};
use lakekeeper_io::{
    s3::{
        validate_bucket_name, S3AccessKeyAuth, S3Auth, S3AwsSystemIdentityAuth, S3Location,
        S3Settings, S3Storage,
    },
    InvalidLocationError, Location,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use veil::Redact;

use crate::{
    api::{
        iceberg::{
            supported_endpoints,
            v1::{tables::DataAccessMode, DataAccess},
        },
        management::v1::warehouse::TabularDeleteProfile,
        CatalogConfig,
    },
    request_metadata::RequestMetadata,
    service::{
        storage::{
            error::{
                CredentialsError, IcebergFileIoError, InvalidProfileError, TableConfigError,
                UpdateError, ValidationError,
            },
            StoragePermissions, TableConfig,
        },
        TabularId,
    },
    WarehouseId, CONFIG,
};

static S3_HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(reqwest::Client::new);

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
    /// The validity of the sts tokens in seconds. Default is 3600
    #[builder(default = 3600)]
    #[serde(default = "fn_3600")]
    pub sts_token_validity_seconds: u64,
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
    /// Controls whether the `s3.delete-enabled=false` flag is sent to clients.
    ///
    /// In all Iceberg 1.x versions, when Spark executes `DROP TABLE xxx PURGE`, it directly
    /// deletes files from S3, bypassing the catalog's soft-deletion mechanism.
    /// Other query engines properly delegate this operation to the catalog.
    /// This Spark behavior is expected to change in Iceberg 2.0.
    ///
    /// Setting this to `true` pushes the `s3.delete-enabled=false` flag to clients,
    /// which discourages Spark from directly deleting files during `DROP TABLE xxx PURGE` operations.
    /// Note that clients may override this setting, and it affects other Spark operations
    /// that require file deletion, such as removing snapshots.
    ///
    /// For more details, refer to Lakekeeper's
    /// [Soft-Deletion documentation](https://docs.lakekeeper.io/docs/nightly/concepts/#soft-deletion).
    /// This flag has no effect if Soft-Deletion is disabled for the warehouse.
    #[serde(default = "fn_true")]
    #[builder(default = true)]
    pub push_s3_delete_disabled: bool,
    /// ARN of the KMS key used to encrypt the S3 bucket, if any.
    #[serde(default)]
    #[builder(default, setter(strip_option))]
    pub aws_kms_key_arn: Option<String>,
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
    // CloudflareR2,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(tag = "credential-type", rename_all = "kebab-case")]
pub enum S3Credential {
    /// Authenticate to AWS using access-key and secret-key.
    AccessKey(S3AccessKeyCredential),
    /// Authenticate to AWS using the identity configured on the system
    ///  that runs lakekeeper. The AWS SDK is used to load the credentials.
    AwsSystemIdentity(S3AwsSystemIdentityCredential),
    CloudflareR2(S3CloudflareR2Credential),
}

#[derive(Redact, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
#[schema(title = "S3CredentialAccessKey")]
pub struct S3AccessKeyCredential {
    pub aws_access_key_id: String,
    #[redact(partial)]
    pub aws_secret_access_key: String,
    #[redact(partial)]
    pub external_id: Option<String>,
}

#[derive(Redact, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
#[schema(title = "S3CredentialSystemIdentity")]
pub struct S3AwsSystemIdentityCredential {
    #[redact(partial)]
    pub external_id: Option<String>,
}

#[derive(Redact, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
#[schema(title = "CloudflareR2Credential")]
pub struct S3CloudflareR2Credential {
    /// Access key ID used for IO operations of Lakekeeper
    pub access_key_id: String,
    #[redact(partial)]
    /// Secret key associated with the access key ID.
    pub secret_access_key: String,
    #[redact(partial)]
    /// Token associated with the access key ID.
    /// This is used to fetch downscoped temporary credentials for vended credentials.
    pub token: String,
    /// Cloudflare account ID, used to determine the temporary credentials endpoint.
    pub account_id: String,
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

    #[must_use]
    /// Check whether the location of this storage profile is overlapping
    /// with the given storage profile.
    pub fn is_overlapping_location(&self, other: &Self) -> bool {
        // Different bucket, region, or endpoint means no overlap
        if self.bucket != other.bucket
            || self.region != other.region
            || self.endpoint != other.endpoint
        {
            return false;
        }

        // If key prefixes are identical, they overlap
        if self.key_prefix == other.key_prefix {
            return true;
        }

        match (&self.key_prefix, &other.key_prefix) {
            // both have Some key_prefix values - check if one is a prefix of the other
            (Some(key_prefix), Some(other_key_prefix)) => {
                let kp1 = format!("{key_prefix}/");
                let kp2 = format!("{other_key_prefix}/");
                kp1.starts_with(&kp2) || kp2.starts_with(&kp1)
            }
            // If either has no key prefix, it can access the entire bucket
            (None, _) | (_, None) => true,
        }
    }

    /// Create a new S3 client with the provided authentication method.
    ///
    /// # Errors
    /// - If system identity is requested but disabled in the configuration
    /// - If the client cannot be initialized
    pub async fn lakekeeper_io(
        &self,
        credential: Option<&S3Credential>,
    ) -> Result<S3Storage, CredentialsError> {
        let s3_settings = storage_profile_to_s3_settings(self);
        let auth = credential
            .map(|c| S3Auth::try_from(c.clone()))
            .transpose()?;
        Ok(s3_settings.get_storage_client(auth.as_ref()).await)
    }

    /// Validate the S3 profile.
    ///
    /// # Errors
    /// - Fails if the bucket name is invalid.
    /// - Fails if the region is too long.
    /// - Fails if the key prefix is too long.
    /// - Fails if the region or endpoint is missing.
    /// - Fails if the endpoint is not a valid URL.
    pub(super) fn normalize(
        &mut self,
        s3_credential: Option<&S3Credential>,
    ) -> Result<(), ValidationError> {
        validate_bucket_name(&self.bucket).map_err(|e| InvalidProfileError {
            source: None,
            reason: e.to_string(),
            entity: "bucket".to_string(),
        })?;
        validate_region(&self.region)?;
        self.normalize_key_prefix()?;
        self.normalize_endpoint()?;
        self.normalize_assume_role_arn();
        self.normalize_sts_role_arn();

        if let Some(S3Credential::CloudflareR2(cloudflare_r2_credential)) = s3_credential {
            self.normalize_r2(cloudflare_r2_credential)?;
        }

        if self.sts_enabled
            && matches!(self.flavor, S3Flavor::Aws)
            && self.sts_role_arn.is_none()
            && self.assume_role_arn.is_none()
        {
            return Err(InvalidProfileError {
                source: None,
                reason: "Either `sts-role-arn` or `assume-role-arn` is required for Storage Profiles with AWS flavor if STS is enabled.".to_string(),
                entity: "sts-role-arn".to_string(),
            }.into());
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
        _warehouse_id: WarehouseId,
        _request_metadata: &RequestMetadata,
        delete_profile: TabularDeleteProfile,
    ) -> CatalogConfig {
        let mut defaults = HashMap::new();

        if self.push_s3_delete_disabled
            && matches!(delete_profile, TabularDeleteProfile::Soft { .. })
        {
            defaults.insert("s3.delete-enabled".to_string(), "false".to_string());
        }

        CatalogConfig {
            defaults,
            overrides: HashMap::new(),
            endpoints: supported_endpoints().to_vec(),
        }
    }

    /// Base Location for this storage profile.
    ///
    /// # Errors
    /// Can fail for un-normalized profiles
    pub fn base_location(&self) -> Result<S3Location, InvalidLocationError> {
        let prefix = self
            .key_prefix
            .as_ref()
            .map(|s| s.split('/').collect::<Vec<_>>())
            .unwrap_or_default();
        S3Location::new(&self.bucket, &prefix, None)
    }

    /// Generate the table configuration for S3.
    ///
    /// # Errors
    /// Fails if vended credentials are used - currently not supported.
    #[allow(clippy::too_many_arguments)]
    pub async fn generate_table_config(
        &self,
        data_access: DataAccessMode,
        s3_credential: Option<&S3Credential>,
        table_location: &Location,
        storage_permissions: StoragePermissions,
        request_metadata: &RequestMetadata,
        warehouse_id: WarehouseId,
        tabular_id: TabularId,
    ) -> Result<TableConfig, TableConfigError> {
        let (remote_signing, vended_credentials) = match data_access {
            DataAccessMode::ServerDelegated(DataAccess {
                vended_credentials,
                remote_signing,
            }) => {
                // If vended_credentials is False and remote_signing is False,
                // use remote_signing. This avoids generating costly STS credentials
                // for clients that don't need them.
                let remote_signing = !vended_credentials || remote_signing;
                (remote_signing, vended_credentials)
            }
            DataAccessMode::ClientManaged => (false, false),
        };
        let mut remote_signing = remote_signing;

        let mut config = TableProperties::default();
        let mut creds = TableProperties::default();

        if let Some(true) = self.path_style_access {
            config.insert(&s3::PathStyleAccess(true));
        }

        config.insert(&s3::Region(self.region.to_string()));
        config.insert(&custom::CustomConfig {
            key: "region".to_string(),
            value: self.region.to_string(),
        });
        config.insert(&client::Region(self.region.to_string()));

        if let Some(endpoint) = &self.endpoint {
            config.insert(&s3::Endpoint(endpoint.clone()));
        }

        if vended_credentials {
            if self.sts_enabled || matches!(s3_credential, Some(S3Credential::CloudflareR2(..))) {
                let aws_sdk_sts::types::Credentials {
                    access_key_id,
                    secret_access_key,
                    session_token,
                    expiration: _,
                    ..
                } = match s3_credential.cloned() {
                    Some(S3Credential::CloudflareR2(c)) => {
                        self.get_cloudflare_r2_temporary_credentials(
                            table_location,
                            c,
                            storage_permissions,
                        )
                        .await?
                    }
                    c @ (Some(
                        S3Credential::AccessKey(..) | S3Credential::AwsSystemIdentity(..),
                    )
                    | None) => {
                        let auth = c.map(S3Auth::try_from).transpose()?;
                        let sts_arn = self
                            .sts_role_arn
                            .as_ref()
                            .or(self.assume_role_arn.as_ref())
                            .map(String::as_str);

                        if sts_arn.is_none() && S3Flavor::Aws == self.flavor {
                            return Err(TableConfigError::Misconfiguration(
                                "Either `sts-role-arn` or `assume-role-arn` is required for Storage Profiles with AWS flavor if STS is enabled.".to_string(),
                            ));
                        }

                        self.get_sts_token(
                            table_location,
                            auth.as_ref(),
                            sts_arn,
                            storage_permissions,
                        )
                        .await?
                    }
                };

                config.insert(&s3::AccessKeyId(access_key_id.clone()));
                config.insert(&s3::SecretAccessKey(secret_access_key.clone()));
                config.insert(&s3::SessionToken(session_token.clone()));
                creds.insert(&s3::AccessKeyId(access_key_id));
                creds.insert(&s3::SecretAccessKey(secret_access_key));
                creds.insert(&s3::SessionToken(session_token));
            } else {
                tracing::debug!(
                    "Falling back to remote signing: vended_credentials requested but STS is disabled for this Warehouse and the credential type is not Cloudflare R2."
                );
                push_fsspec_fileio_with_s3v4restsigner(&mut config);
                remote_signing = true;
            }
        }

        if remote_signing {
            config.insert(&s3::RemoteSigningEnabled(true));
            config.insert(&s3::SignerUri(request_metadata.s3_signer_uri(warehouse_id)));
            config.insert(&s3::SignerEndpoint(
                request_metadata.s3_signer_endpoint_for_table(warehouse_id, tabular_id),
            ));
        }

        Ok(TableConfig { creds, config })
    }

    async fn get_cloudflare_r2_temporary_credentials(
        &self,
        table_location: &Location,
        cred: S3CloudflareR2Credential,
        storage_permissions: StoragePermissions,
    ) -> Result<aws_sdk_sts::types::Credentials, CredentialsError> {
        let table_location = S3Location::try_from_location(table_location, true).map_err(|e| {
            CredentialsError::ShortTermCredential {
                source: None,
                reason: format!("Could not generate downscoped policy for temporary credentials as location is no valid S3 location: {e}").to_string(),
            }
        })?;

        let bucket_name = table_location.bucket_name();
        let key = format!("{}/", table_location.key().join("/"));

        // Prepare the request payload for Cloudflare R2 API
        let permission = match storage_permissions {
            StoragePermissions::Read => "object-read-only",
            StoragePermissions::ReadWrite | StoragePermissions::ReadWriteDelete => {
                "object-read-write"
            }
        };

        let ttl_seconds = self.sts_token_validity_seconds;
        let payload = serde_json::json!({
            "bucket": bucket_name,
            "prefixes": [key],
            "permission": permission,
            "ttlSeconds": ttl_seconds,
            "parentAccessKeyId": cred.access_key_id,
        });

        // Make the API request to Cloudflare R2
        let client = S3_HTTP_CLIENT.clone();
        let response = client
            .post(format!(
                "https://api.cloudflare.com/client/v4/accounts/{}/r2/temp-access-credentials",
                cred.account_id
            ))
            .header("Authorization", format!("Bearer {}", cred.token))
            .json(&payload)
            .send()
            .await
            .map_err(|e| CredentialsError::ShortTermCredential {
                source: Some(Box::new(e)),
                reason: "Failed to request temporary credentials from Cloudflare R2 temp-access-credentials Endpoint".to_string(),
            })?;

        // Parse the response
        if !response.status().is_success() {
            let status_code = response.status();
            let error_message = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            tracing::debug!(
                "Failed to get temporary credentials from Cloudflare R2 ({status_code}): {error_message}",
            );
            return Err(CredentialsError::ShortTermCredential {
                source: None,
                reason: format!(
                    "Failed to get temporary credentials from Cloudflare R2 ({status_code}): {error_message}",
                ),
            });
        }

        let credentials: serde_json::Value =
            response
                .json()
                .await
                .map_err(|e| CredentialsError::ShortTermCredential {
                    source: Some(Box::new(e)),
                    reason:
                        "Failed to parse temporary credentials response from Cloudflare R2 as json."
                            .to_string(),
                })?;

        // Extract credentials from the response
        let tmp_credentials: R2TemporaryCredentialsResponse = serde_json::from_value(credentials)
            .map_err(|e| {
            CredentialsError::ShortTermCredential {
                source: Some(Box::new(e)),
                reason: "Failed to parse temporary credentials response from Cloudflare R2."
                    .to_string(),
            }
        })?;

        // Return the credentials
        Ok(aws_sdk_sts::types::Credentials::builder()
            .access_key_id(tmp_credentials.result.access_key_id)
            .secret_access_key(tmp_credentials.result.secret_access_key)
            .session_token(tmp_credentials.result.session_token)
            .expiration(
                (std::time::SystemTime::now() + std::time::Duration::from_secs(ttl_seconds)).into(),
            )
            .build()
            .unwrap())
    }

    async fn get_sts_token(
        &self,
        table_location: &Location,
        s3_credential: Option<&S3Auth>,
        role_arn: Option<&str>,
        storage_permissions: StoragePermissions,
    ) -> Result<aws_sdk_sts::types::Credentials, CredentialsError> {
        let policy = self.get_aws_policy_string(table_location, storage_permissions)?;
        self.assume_role_with_sts(s3_credential, role_arn, Some(policy))
            .await
    }

    async fn assume_role_with_sts(
        &self,
        s3_credentials: Option<&S3Auth>,
        role_arn: Option<&str>,
        policy: Option<String>,
    ) -> Result<aws_sdk_sts::types::Credentials, CredentialsError> {
        let external_id = s3_credentials
            .as_ref()
            .and_then(|c| c.external_id().map(ToString::to_string));

        if role_arn.is_none()
            && matches!(s3_credentials, Some(S3Auth::AwsSystemIdentity { .. }))
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
            && matches!(s3_credentials, Some(S3Auth::AwsSystemIdentity { .. }))
        {
            return Err(CredentialsError::Misconfiguration(
                "An `external-id` is required when using `assume-role-arn`.".to_string(),
            ));
        }

        // We expect that we are able to assume the STS role directly, without needing to assume
        // the `assume-role-arn` role first.
        let sdk_config = self.get_aws_sdk_config(s3_credentials, None).await?;

        let assume_role_builder = aws_sdk_sts::Client::new(&sdk_config)
            .assume_role()
            .role_session_name("lakekeeper-sts")
            .duration_seconds(i32::try_from(self.sts_token_validity_seconds).unwrap_or(3600));

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

    /// Load the native AWS SDK config, optionally assuming a role if provided.
    /// `self.assume_role_arn` / `self.sts_role_arn` are not used directly.
    ///
    /// # Errors
    /// Returns an error if the credentials are misconfigured or if the SDK config cannot be loaded
    pub async fn get_aws_sdk_config(
        &self,
        s3_credential: Option<&S3Auth>,
        assume_role_arn: Option<&str>,
    ) -> Result<SdkConfig, CredentialsError> {
        if matches!(&s3_credential, Some(S3Auth::AwsSystemIdentity(..)))
            && !CONFIG.enable_aws_system_credentials
        {
            return Err(CredentialsError::Misconfiguration(
                "System identity credentials are disabled in this Lakekeeper deployment."
                    .to_string(),
            ));
        }

        let mut s3_settings = storage_profile_to_s3_settings(self);
        s3_settings.assume_role_arn = assume_role_arn.map(String::from);

        Ok(s3_settings.get_sdk_config(s3_credential).await)
    }

    /// Get the signing identity for the S3 credentials.
    ///
    /// # Errors
    /// - Returns an error if the credentials are misconfigured or if the signing identity cannot be obtained.
    pub async fn get_signing_identity(
        &self,
        s3_credential: Option<&S3Credential>,
    ) -> Result<Identity, ErrorModel> {
        let s3_auth = s3_credential
            .map(|e| S3Auth::try_from(e.clone()))
            .transpose()?;
        let sdk = self
            .get_aws_sdk_config(s3_auth.as_ref(), self.assume_role_arn.as_deref())
            .await?;
        let token_provider = sdk.credentials_provider().ok_or_else(|| {
            ErrorModel::precondition_failed(
                "Cannot sign requests for Warehouses without S3 credentials",
                "SignWithoutCredentials",
                None,
            )
        })?;

        let identity = token_provider
            .provide_credentials()
            .await
            .map_err(|e| {
                ErrorModel::precondition_failed(
                    "Failed to obtain signing identity from S3 credentials",
                    "SigningIdentityError",
                    Some(Box::new(e)),
                )
            })?
            .into();

        Ok(identity)
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
        &self,
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
        let key = format!("{}/", table_location.key().join("/"));

        let mut statements = format!(
            r#"
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
        "#,
            Self::permission_to_actions(storage_permissions),
        )
        .replace('\n', "")
        .replace(' ', "");

        if let Some(kms_key_arn) = self.aws_kms_key_arn.as_ref() {
            statements = format!(
                r#"
                {statements},
                {{
                    "Sid": "KmsAccess",
                    "Effect": "Allow",
                    "Action": [
                        "kms:Decrypt",
                        "kms:GenerateDataKey"
                    ],
                    "Resource": "{kms_key_arn}"
                }}"#
            );
        }

        Ok(format!(
            r#"{{
        "Version": "2012-10-17",
        "Statement": [
            {statements}
        ]
        }}"#
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
                return Err(InvalidProfileError {
                    source: None,
                    reason: "Storage Profile `key_prefix` must be less than 896 characters."
                        .to_string(),
                    entity: "key_prefix".to_string(),
                }
                .into());
            }
        }
        Ok(())
    }

    fn normalize_endpoint(&mut self) -> Result<(), ValidationError> {
        if let Some(endpoint) = self.endpoint.as_mut() {
            if endpoint.scheme() != "http" && endpoint.scheme() != "https" {
                return Err(InvalidProfileError {
                    source: None,
                    reason: "Storage Profile `endpoint` must have http or https protocol."
                        .to_string(),
                    entity: "S3Endpoint".to_string(),
                }
                .into());
            }

            // If the storage endpoint path is equal to the bucket name, remove it.
            // This is common as in the UI, cloudflare shows the S3 API with the bucket name at the end.
            if endpoint.path().ends_with(&self.bucket) {
                // Remove the bucket from the end of the path
                let path = endpoint.path();
                let new_path = path
                    .strip_suffix(&self.bucket)
                    .unwrap_or(path)
                    .trim_end_matches('/')
                    .to_string();
                endpoint.set_path(&new_path);
            }

            // If a non-empty path is provided, it must be a single slash which we remove.
            if !endpoint.path().is_empty() {
                if endpoint.path() != "/" {
                    return Err(InvalidProfileError {
                        source: None,
                        reason: "Storage Profile `endpoint` must not have a path.".to_string(),
                        entity: "S3Endpoint".to_string(),
                    }
                    .into());
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

    fn normalize_r2(
        &mut self,
        _credentials: &S3CloudflareR2Credential,
    ) -> Result<(), InvalidProfileError> {
        // No assume role ARNs are supported, set them to None
        self.assume_role_arn = None;
        self.sts_role_arn = None;
        self.sts_enabled = true;
        self.flavor = S3Flavor::S3Compat;

        // If an endpoint is specified and ends with the bucket, remove the bucket from the endpoint.
        // This is common as in the UI, cloudflare shows the S3 API with the bucket name at the end.
        if self.endpoint.is_none() {
            return Err(InvalidProfileError {
                source: None,
                reason: "Parameter `endpoint` is required for Cloudflare R2.".to_string(),
                entity: "endpoint".to_string(),
            });
        }

        Ok(())
    }
}

fn storage_profile_to_s3_settings(profile: &S3Profile) -> S3Settings {
    S3Settings {
        region: profile.region.clone(),
        endpoint: profile.endpoint.clone(),
        path_style_access: profile.path_style_access,
        assume_role_arn: profile.assume_role_arn.clone(),
        aws_kms_key_arn: profile.aws_kms_key_arn.clone(),
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct R2TemporaryCredentialsResponse {
    result: R2TemporaryCredentialsResult,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct R2TemporaryCredentialsResult {
    access_key_id: String,
    secret_access_key: String,
    session_token: String,
}

pub(super) fn get_file_io_from_table_config(
    config: &TableProperties,
) -> Result<iceberg::io::FileIO, IcebergFileIoError> {
    let mut builder = iceberg::io::FileIOBuilder::new("s3").clone();

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

fn validate_region(region: &str) -> Result<(), InvalidProfileError> {
    lakekeeper_io::s3::validate_region(region).map_err(|e| InvalidProfileError {
        source: None,
        reason: e,
        entity: "region".to_string(),
    })
}

fn push_fsspec_fileio_with_s3v4restsigner(config: &mut TableProperties) {
    config.insert(&s3::Signer("S3V4RestSigner".to_string()));
    config.insert(&custom::CustomConfig {
        key: "py-io-impl".to_string(),
        value: "pyiceberg.io.fsspec.FsspecFileIO".to_string(),
    });
}

fn fn_true() -> bool {
    true
}

fn fn_3600() -> u64 {
    3600
}

impl From<S3CloudflareR2Credential> for S3Credential {
    fn from(cloudflare_credential: S3CloudflareR2Credential) -> Self {
        S3Credential::CloudflareR2(cloudflare_credential)
    }
}

impl From<S3AccessKeyCredential> for S3AccessKeyAuth {
    fn from(access_key_credential: S3AccessKeyCredential) -> Self {
        S3AccessKeyAuth {
            aws_access_key_id: access_key_credential.aws_access_key_id,
            aws_secret_access_key: access_key_credential.aws_secret_access_key,
            external_id: access_key_credential.external_id,
        }
    }
}

impl From<S3AwsSystemIdentityCredential> for S3AwsSystemIdentityAuth {
    fn from(system_identity_credential: S3AwsSystemIdentityCredential) -> Self {
        S3AwsSystemIdentityAuth {
            external_id: system_identity_credential.external_id,
        }
    }
}

impl TryFrom<S3Credential> for S3Auth {
    type Error = CredentialsError;

    fn try_from(credential: S3Credential) -> Result<Self, Self::Error> {
        if matches!(&credential, S3Credential::AwsSystemIdentity(..))
            && !CONFIG.enable_aws_system_credentials
        {
            return Err(CredentialsError::Misconfiguration(
                "System identity credentials are disabled in this Lakekeeper deployment."
                    .to_string(),
            ));
        }

        Ok(match credential {
            S3Credential::AccessKey(c) => S3Auth::AccessKey(c.into()),
            S3Credential::AwsSystemIdentity(c) => S3Auth::AwsSystemIdentity(c.into()),
            S3Credential::CloudflareR2(S3CloudflareR2Credential {
                access_key_id,
                secret_access_key,
                token: _,
                account_id: _,
            }) => S3Auth::AccessKey(S3AccessKeyAuth {
                aws_access_key_id: access_key_id,
                aws_secret_access_key: secret_access_key,
                external_id: None, // Cloudflare R2 does not use external ID
            }),
        })
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::str::FromStr as _;

    use super::*;
    use crate::service::{
        storage::{StorageLocations as _, StorageProfile},
        tabular_idents::TabularId,
        NamespaceId,
    };

    #[test]
    fn test_deserialize_flavor() {
        let flavor: S3Flavor = serde_json::from_value(serde_json::json!("aws")).unwrap();
        assert_eq!(flavor, S3Flavor::Aws);

        let flavor: S3Flavor = serde_json::from_value(serde_json::json!("s3-compat")).unwrap();
        assert_eq!(flavor, S3Flavor::S3Compat);
    }

    #[test]
    fn test_deserialize_r2_temporary_credentials_response() {
        let response = serde_json::json!({
          "errors": [
            {
              "code": 1000,
              "message": "message",
              "documentation_url": "documentation_url",
              "source": {
                "pointer": "pointer"
              }
            }
          ],
          "messages": [
            "string"
          ],
          "result": {
            "accessKeyId": "example-access-key-id",
            "secretAccessKey": "example-secret-key",
            "sessionToken": "example-session-token"
          },
          "success": true
        });
        let response: R2TemporaryCredentialsResponse = serde_json::from_value(response).unwrap();
        let expected = R2TemporaryCredentialsResponse {
            result: R2TemporaryCredentialsResult {
                access_key_id: "example-access-key-id".to_string(),
                secret_access_key: "example-secret-key".to_string(),
                session_token: "example-session-token".to_string(),
            },
        };
        assert_eq!(response, expected);
    }

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
        let expected = S3Credential::AccessKey(S3AccessKeyCredential {
            aws_access_key_id: "foo".to_string(),
            aws_secret_access_key: "bar".to_string(),
            external_id: None,
        });
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
        let expected = S3Credential::AccessKey(S3AccessKeyCredential {
            aws_access_key_id: "foo".to_string(),
            aws_secret_access_key: "bar".to_string(),
            external_id: Some("baz".to_string()),
        });
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
        let expected =
            S3Credential::AwsSystemIdentity(S3AwsSystemIdentityCredential { external_id: None });
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
        let expected = S3Credential::AwsSystemIdentity(S3AwsSystemIdentityCredential {
            external_id: Some("baz".to_string()),
        });
        assert_eq!(credential, expected);
    }

    #[test]
    fn test_storage_secret_deserialization_r2() {
        let secret = serde_json::json!(
            {
                "credential-type": "cloudflare-r2",
                "account-id": "foo",
                "access-key-id": "bar",
                "secret-access-key": "baz",
                "token": "qux",
            }
        );
        let credential: S3Credential = serde_json::from_value(secret).unwrap();
        let expected = S3Credential::CloudflareR2(S3CloudflareR2Credential {
            account_id: "foo".to_string(),
            access_key_id: "bar".to_string(),
            secret_access_key: "baz".to_string(),
            token: "qux".to_string(),
        });
        assert_eq!(credential, expected);
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
            sts_token_validity_seconds: 3600,
            push_s3_delete_disabled: false,
            aws_kms_key_arn: None,
        };
        let sp: StorageProfile = profile.clone().into();

        let namespace_id = NamespaceId::from(uuid::Uuid::now_v7());
        let table_id = TabularId::Table(uuid::Uuid::now_v7());
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
            sts_token_validity_seconds: 3600,
            push_s3_delete_disabled: false,
            aws_kms_key_arn: None,
        };

        let namespace_location = Location::from_str("s3://test-bucket/foo/").unwrap();
        let table_id = TabularId::Table(uuid::Uuid::now_v7());
        // Prefix should be ignored as we specify the namespace_location explicitly.
        // Tabular locations should not have a trailing slash, otherwise pyiceberg fails.
        let expected = format!("s3://test-bucket/foo/{table_id}");

        let location = profile.default_tabular_location(&namespace_location, table_id);

        assert_eq!(location.to_string(), expected);

        let namespace_location = Location::from_str("s3://test-bucket/foo").unwrap();
        let location = profile.default_tabular_location(&namespace_location, table_id);
        assert_eq!(location.to_string(), expected);
    }

    pub(crate) mod minio_integration_tests {
        use std::sync::LazyLock;

        use crate::{
            api::RequestMetadata,
            service::storage::{
                s3::S3AccessKeyCredential, S3Credential, S3Flavor, S3Profile, StorageCredential,
                StorageProfile,
            },
        };

        static TEST_BUCKET: LazyLock<String> =
            LazyLock::new(|| std::env::var("LAKEKEEPER_TEST__S3_BUCKET").unwrap());
        static TEST_REGION: LazyLock<String> =
            LazyLock::new(|| std::env::var("LAKEKEEPER_TEST__S3_REGION").unwrap_or("local".into()));
        static TEST_ACCESS_KEY: LazyLock<String> =
            LazyLock::new(|| std::env::var("LAKEKEEPER_TEST__S3_ACCESS_KEY").unwrap());
        static TEST_SECRET_KEY: LazyLock<String> =
            LazyLock::new(|| std::env::var("LAKEKEEPER_TEST__S3_SECRET_KEY").unwrap());
        static TEST_ENDPOINT: LazyLock<String> =
            LazyLock::new(|| std::env::var("LAKEKEEPER_TEST__S3_ENDPOINT").unwrap());

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
                sts_token_validity_seconds: 3600,
                push_s3_delete_disabled: false,
                aws_kms_key_arn: None,
            };
            let cred = S3Credential::AccessKey(S3AccessKeyCredential {
                aws_access_key_id: TEST_ACCESS_KEY.clone(),
                aws_secret_access_key: TEST_SECRET_KEY.clone(),
                external_id: None,
            });

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

                    profile.normalize(Some(&cred)).unwrap();
                    Box::pin(profile.validate_access(
                        Some(&cred),
                        None,
                        &RequestMetadata::new_unauthenticated(),
                    ))
                    .await
                    .unwrap();
                },
                true,
            );
        }
    }

    pub(crate) mod aws_integration_tests {
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
                sts_token_validity_seconds: 3600,
                push_s3_delete_disabled: false,
                aws_kms_key_arn: None,
            };
            let cred = S3Credential::AccessKey(S3AccessKeyCredential {
                aws_access_key_id: std::env::var("AWS_S3_ACCESS_KEY_ID").unwrap(),
                aws_secret_access_key: std::env::var("AWS_S3_SECRET_ACCESS_KEY").unwrap(),
                external_id: None,
            });

            (profile, cred)
        }

        #[test]
        #[tracing_test::traced_test]
        fn test_signing_identity() {
            // we need to use a shared runtime since the static client is shared between tests here
            // and tokio::test creates a new runtime for each test. For now, we only encounter the
            // issue here, eventually, we may want to move this to a proc macro like tokio::test or
            // sqlx::test
            crate::test::test_block_on(
                async {
                    let (profile, cred) = get_storage_profile();
                    let mut profile = profile;
                    profile.normalize(Some(&cred)).unwrap();
                    let _identity = profile.get_signing_identity(Some(&cred)).await.unwrap();
                },
                true,
            );
        }

        #[test]
        #[tracing_test::traced_test]
        fn test_signing_identity_with_assumed_role() {
            // we need to use a shared runtime since the static client is shared between tests here
            // and tokio::test creates a new runtime for each test. For now, we only encounter the
            // issue here, eventually, we may want to move this to a proc macro like tokio::test or
            // sqlx::test
            crate::test::test_block_on(
                async {
                    let (profile, cred) = get_storage_profile();
                    let mut profile = profile;
                    profile.normalize(Some(&cred)).unwrap();
                    profile.assume_role_arn = profile.sts_role_arn.clone();
                    let _identity = profile.get_signing_identity(Some(&cred)).await.unwrap();
                },
                true,
            );
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

                    profile.normalize(Some(&cred)).unwrap();
                    Box::pin(profile.validate_access(
                        Some(&cred),
                        None,
                        &RequestMetadata::new_unauthenticated(),
                    ))
                    .await
                    .unwrap();
                },
                true,
            );
        }
    }

    pub(crate) mod aws_kms_integration_tests {
        use super::super::*;
        use crate::service::storage::{StorageCredential, StorageProfile};

        pub(crate) fn get_storage_profile() -> (S3Profile, S3Credential) {
            let profile = S3Profile {
                bucket: std::env::var("AWS_KMS_S3_BUCKET").unwrap(),
                key_prefix: Some(uuid::Uuid::now_v7().to_string()),
                assume_role_arn: Some(std::env::var("AWS_S3_STS_ROLE_ARN").unwrap()),
                endpoint: None,
                region: std::env::var("AWS_S3_REGION").unwrap(),
                path_style_access: Some(true),
                sts_role_arn: None,
                flavor: S3Flavor::Aws,
                sts_enabled: true,
                allow_alternative_protocols: Some(false),
                remote_signing_url_style:
                    crate::service::storage::s3::S3UrlStyleDetectionMode::Auto,
                sts_token_validity_seconds: 3600,
                push_s3_delete_disabled: false,
                aws_kms_key_arn: Some(std::env::var("AWS_S3_KMS_ARN").unwrap()),
            };
            let cred = S3Credential::AccessKey(S3AccessKeyCredential {
                aws_access_key_id: std::env::var("AWS_S3_ACCESS_KEY_ID").unwrap(),
                aws_secret_access_key: std::env::var("AWS_S3_SECRET_ACCESS_KEY").unwrap(),
                external_id: None,
            });

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

                    profile.normalize(Some(&cred)).unwrap();
                    Box::pin(profile.validate_access(
                        Some(&cred),
                        None,
                        &RequestMetadata::new_unauthenticated(),
                    ))
                    .await
                    .unwrap();
                },
                true,
            );
        }
    }

    pub(crate) mod r2_integration_tests {
        use super::super::*;
        use crate::service::storage::{StorageCredential, StorageProfile};

        pub(crate) fn get_storage_profile() -> (S3Profile, S3Credential) {
            let profile = S3Profile {
                bucket: std::env::var("LAKEKEEPER_TEST__R2_BUCKET").unwrap(),
                key_prefix: Some(uuid::Uuid::now_v7().to_string()),
                assume_role_arn: None,
                endpoint: Some(
                    std::env::var("LAKEKEEPER_TEST__R2_ENDPOINT")
                        .unwrap()
                        .parse()
                        .unwrap(),
                ),
                region: "auto".to_string(),
                path_style_access: Some(true),
                sts_role_arn: None,
                flavor: S3Flavor::S3Compat,
                sts_enabled: true,
                allow_alternative_protocols: Some(false),
                remote_signing_url_style:
                    crate::service::storage::s3::S3UrlStyleDetectionMode::Auto,
                sts_token_validity_seconds: 3600,
                push_s3_delete_disabled: false,
                aws_kms_key_arn: None,
            };
            let cred = S3Credential::CloudflareR2(S3CloudflareR2Credential {
                access_key_id: std::env::var("LAKEKEEPER_TEST__R2_ACCESS_KEY_ID").unwrap(),
                secret_access_key: std::env::var("LAKEKEEPER_TEST__R2_SECRET_ACCESS_KEY").unwrap(),
                token: std::env::var("LAKEKEEPER_TEST__R2_TOKEN").unwrap(),
                account_id: std::env::var("LAKEKEEPER_TEST__R2_ACCOUNT_ID").unwrap(),
            });

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

                    profile.normalize(Some(&cred)).unwrap();
                    Box::pin(profile.validate_access(
                        Some(&cred),
                        None,
                        &RequestMetadata::new_unauthenticated(),
                    ))
                    .await
                    .unwrap();
                },
                true,
            );
        }
    }

    #[test]
    fn policy_string_is_json() {
        let table_location = "s3://bucket-name/path/to/table";
        let profile = S3Profile::builder()
            .bucket("bucket-name".to_string())
            .key_prefix("path/to/table".to_string())
            .region("us-east-1".to_string())
            .flavor(S3Flavor::S3Compat)
            .sts_enabled(true)
            .build();
        let policy = profile
            .get_aws_policy_string(
                &table_location.parse().unwrap(),
                StoragePermissions::ReadWriteDelete,
            )
            .unwrap();
        println!("Policy: {policy}");
        let _ = serde_json::from_str::<serde_json::Value>(&policy).unwrap();
    }
}

#[cfg(test)]
mod is_overlapping_location_tests {
    use super::*;

    fn create_profile(
        bucket: &str,
        region: &str,
        endpoint: Option<&str>,
        key_prefix: Option<&str>,
    ) -> S3Profile {
        S3Profile {
            bucket: bucket.to_string(),
            key_prefix: key_prefix.map(ToString::to_string),
            region: region.to_string(),
            endpoint: endpoint.map(|e| e.parse().unwrap()),
            assume_role_arn: None,
            path_style_access: None,
            sts_role_arn: None,
            sts_enabled: false,
            flavor: S3Flavor::Aws,
            allow_alternative_protocols: None,
            remote_signing_url_style: S3UrlStyleDetectionMode::Auto,
            sts_token_validity_seconds: 3600,
            push_s3_delete_disabled: true,
            aws_kms_key_arn: None,
        }
    }

    #[test]
    fn test_non_overlapping_different_bucket() {
        let profile1 = create_profile("bucket1", "us-east-1", None, Some("prefix"));
        let profile2 = create_profile("bucket2", "us-east-1", None, Some("prefix"));

        assert!(!profile1.is_overlapping_location(&profile2));
    }

    #[test]
    fn test_non_overlapping_different_region() {
        let profile1 = create_profile("bucket1", "us-east-1", None, Some("prefix"));
        let profile2 = create_profile("bucket1", "us-west-1", None, Some("prefix"));

        assert!(!profile1.is_overlapping_location(&profile2));
    }

    #[test]
    fn test_non_overlapping_different_endpoint() {
        let profile1 = create_profile(
            "bucket1",
            "us-east-1",
            Some("http://endpoint1.com"),
            Some("prefix"),
        );
        let profile2 = create_profile(
            "bucket1",
            "us-east-1",
            Some("http://endpoint2.com"),
            Some("prefix"),
        );

        assert!(!profile1.is_overlapping_location(&profile2));
    }

    #[test]
    fn test_overlapping_identical_key_prefix() {
        let profile1 = create_profile("bucket1", "us-east-1", None, Some("prefix"));
        let profile2 = create_profile("bucket1", "us-east-1", None, Some("prefix"));

        assert!(profile1.is_overlapping_location(&profile2));
    }

    #[test]
    fn test_overlapping_one_prefix_of_other() {
        let profile1 = create_profile("bucket1", "us-east-1", None, Some("prefix"));
        let profile2 = create_profile("bucket1", "us-east-1", None, Some("prefix/subpath"));

        assert!(profile1.is_overlapping_location(&profile2));
        assert!(profile2.is_overlapping_location(&profile1)); // Test symmetry
    }

    #[test]
    fn test_overlapping_no_key_prefix() {
        let profile1 = create_profile("bucket1", "us-east-1", None, None);
        let profile2 = create_profile("bucket1", "us-east-1", None, Some("prefix"));

        assert!(profile1.is_overlapping_location(&profile2));
        assert!(profile2.is_overlapping_location(&profile1)); // Test symmetry
    }

    #[test]
    fn test_non_overlapping_unrelated_key_prefixes() {
        let profile1 = create_profile("bucket1", "us-east-1", None, Some("prefix1"));
        let profile2 = create_profile("bucket1", "us-east-1", None, Some("prefix2"));

        // These don't overlap as neither is a prefix of the other
        assert!(!profile1.is_overlapping_location(&profile2));
    }

    #[test]
    fn test_overlapping_both_no_key_prefix() {
        let profile1 = create_profile("bucket1", "us-east-1", None, None);
        let profile2 = create_profile("bucket1", "us-east-1", None, None);

        assert!(profile1.is_overlapping_location(&profile2));
    }

    #[test]
    fn test_complex_key_prefix_scenarios() {
        // Prefix with slash at the end should be handled the same
        let profile1 = create_profile("bucket1", "us-east-1", None, Some("prefix"));
        let profile2 = create_profile("bucket1", "us-east-1", None, Some("prefix-extra"));

        // Not overlapping since "prefix" is not a prefix of "prefix-extra"
        // (they share characters but one is not a prefix of the other)
        assert!(!profile1.is_overlapping_location(&profile2));

        // Actual prefix case
        let profile3 = create_profile("bucket1", "us-east-1", None, Some("prefix"));
        let profile4 = create_profile("bucket1", "us-east-1", None, Some("prefix/sub"));

        assert!(profile3.is_overlapping_location(&profile4));
    }
}
