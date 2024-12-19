#![allow(clippy::module_name_repetitions)]

use crate::WarehouseIdent;

use crate::api::{iceberg::v1::DataAccess, CatalogConfig};
use crate::service::storage::error::{
    CredentialsError, FileIoError, TableConfigError, UpdateError, ValidationError,
};
use crate::service::storage::{StoragePermissions, TableConfig};

use super::StorageType;
use crate::api::iceberg::supported_endpoints;
use base64::Engine;
use iceberg_ext::configs::table::{gcs, TableProperties};
use iceberg_ext::configs::Location;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use veil::Redact;

mod sts;

#[derive(Debug, Eq, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct GcsProfile {
    /// Name of the GCS bucket
    pub bucket: String,
    /// Subpath in the bucket to use.
    /// The same prefix can be used for multiple warehouses.
    pub key_prefix: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(tag = "credential-type", rename_all = "kebab-case")]
/// GCS Credentials
///
/// Currently only supports Service Account Key
/// Example of a key:
/// ```json
///     {
///       "type": "service_account",
///       "project_id": "example-project-1234",
///       "private_key_id": "....",
///       "private_key": "-----BEGIN PRIVATE KEY-----\n.....\n-----END PRIVATE KEY-----\n",
///       "client_email": "abc@example-project-1234.iam.gserviceaccount.com",
///       "client_id": "123456789012345678901",
///       "auth_uri": "https://accounts.google.com/o/oauth2/auth",
///       "token_uri": "https://oauth2.googleapis.com/token",
///       "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
///       "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/abc%example-project-1234.iam.gserviceaccount.com",
///       "universe_domain": "googleapis.com"
///     }
/// ```
pub enum GcsCredential {
    /// Service Account Key
    ///
    /// The key is the JSON object obtained when creating a service account key in the GCP console.
    #[schema(title = "GcsCredentialServiceAccountKey")]
    ServiceAccountKey { key: GcsServiceKey },
}

#[derive(Redact, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct GcsServiceKey {
    pub r#type: String,
    pub project_id: String,
    pub private_key_id: String,
    #[redact(partial)]
    pub private_key: String,
    pub client_email: String,
    pub client_id: String,
    pub auth_uri: String,
    pub token_uri: String,
    pub auth_provider_x509_cert_url: String,
    pub client_x509_cert_url: String,
    pub universe_domain: String,
}

impl GcsProfile {
    /// Create a new `FileIO` instance for GCS.
    ///
    /// # Errors
    /// Fails if the `FileIO` instance cannot be created.
    #[allow(clippy::unused_self)]
    pub fn file_io(
        &self,
        credential: Option<&GcsCredential>,
    ) -> Result<iceberg::io::FileIO, FileIoError> {
        let mut builder = iceberg::io::FileIOBuilder::new("gcs");

        if let Some(GcsCredential::ServiceAccountKey { key }) = credential {
            builder = builder.with_prop(
                iceberg::io::GCS_CREDENTIALS_JSON,
                // guess we're doing base64 now ¯\_(._.)_/¯
                base64::prelude::BASE64_STANDARD.encode(
                    serde_json::to_string(key)
                        .map_err(CredentialsError::from)?
                        .as_bytes(),
                ),
            );
        }

        Ok(builder.build()?)
    }

    /// Validate the GCS profile.
    ///
    /// # Errors
    /// - Fails if the bucket name is invalid.
    /// - Fails if the key prefix is too long.
    pub(super) fn normalize(&mut self) -> Result<(), ValidationError> {
        validate_bucket_name(&self.bucket)?;
        self.normalize_key_prefix()?;

        Ok(())
    }

    /// Check if the profile can be updated with the other profile.
    /// `key_prefix` and `bucket` must be the same.
    /// We enforce this to avoid issues by accidentally changing the bucket of a warehouse,
    /// after which all tables would not be accessible anymore.
    ///
    /// # Errors
    /// Fails if the `bucket` or `key_prefix` is different.
    pub fn can_be_updated_with(&self, other: &Self) -> Result<(), UpdateError> {
        if self.bucket != other.bucket {
            return Err(UpdateError::ImmutableField("bucket".to_string()));
        }

        if self.key_prefix != other.key_prefix {
            return Err(UpdateError::ImmutableField("key_prefix".to_string()));
        }

        Ok(())
    }

    #[must_use]
    #[allow(clippy::unused_self)]
    pub fn generate_catalog_config(&self, _: WarehouseIdent) -> CatalogConfig {
        CatalogConfig {
            defaults: HashMap::with_capacity(0),
            overrides: HashMap::with_capacity(0),
            endpoints: supported_endpoints(),
        }
    }

    /// Base Location for this storage profile.
    ///
    /// # Errors
    /// Can fail for un-normalized profiles
    pub fn base_location(&self) -> Result<Location, ValidationError> {
        let prefix: Vec<String> = self
            .key_prefix
            .as_ref()
            .map(|s| s.split('/').map(std::borrow::ToOwned::to_owned).collect())
            .unwrap_or_default();
        Location::from_str(&format!("gs://{}/", self.bucket))
            .map(|mut l| {
                l.extend(prefix.iter());
                l
            })
            .map_err(|e| ValidationError::InvalidLocation {
                reason: "Invalid GCS location.".to_string(),
                location: format!("gs://{}/", self.bucket),
                source: Some(e.into()),
                storage_type: StorageType::Gcs,
            })
    }

    /// Generate the table configuration for GCS.
    pub(crate) async fn generate_table_config(
        &self,
        _: &DataAccess,
        cred: Option<&GcsCredential>,
        table_location: &Location,
        storage_permissions: StoragePermissions,
    ) -> Result<TableConfig, TableConfigError> {
        let mut creds = TableProperties::default();
        if let Some(GcsCredential::ServiceAccountKey { key }) = cred {
            let token = sts::downscope(
                key,
                &self.bucket,
                table_location.clone(),
                storage_permissions,
            )
            .await?;

            creds.insert(&gcs::Token(token.access_token));
            creds.insert(&gcs::ProjectId(key.project_id.clone()));

            if let Some(expiry) = token.expires_in {
                creds.insert(&gcs::TokenExpiresAt(
                    (std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis()
                        + (expiry * 1000) as u128)
                        .to_string(),
                ));
            }
        }

        Ok(TableConfig {
            // Due to backwards compat reasons we still return creds within config too
            config: creds.clone(),
            creds,
        })
    }

    fn normalize_key_prefix(&mut self) -> Result<(), ValidationError> {
        if let Some(key_prefix) = self.key_prefix.as_mut() {
            *key_prefix = key_prefix.trim_matches('/').to_string();
            if key_prefix.starts_with(".well-known/acme-challenge/") {
                return Err(ValidationError::InvalidProfile {
                    source: None,
                    reason: "Storage Profile `key_prefix` cannot start with `.well-known/acme-challenge/`.".to_string(),
                    entity: "key_prefix".to_string(),
                });
            }
        }

        if let Some(key_prefix) = self.key_prefix.as_ref() {
            if key_prefix.is_empty() {
                self.key_prefix = None;
            }
        }

        // GCS supports a max of 1024 chars and we need some buffer for tables.
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
}

pub(super) fn get_file_io_from_table_config(
    config: &TableProperties,
) -> Result<iceberg::io::FileIO, FileIoError> {
    Ok(iceberg::io::FileIOBuilder::new("gcs")
        .with_props(config.inner())
        .build()?)
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

    // Bucket names cannot be represented as an IP address in dotted-decimal notation.
    if bucket.parse::<std::net::Ipv4Addr>().is_ok() {
        return Err(ValidationError::InvalidProfile {
            source: None,
            reason:
                "Bucket name cannot be represented as an IP address in dotted-decimal notation."
                    .to_string(),
            entity: "BucketName".to_string(),
        });
    }

    // Bucket names cannot begin with the "goog" prefix.
    if bucket.starts_with("goog") {
        return Err(ValidationError::InvalidProfile {
            source: None,
            reason: "Bucket name cannot begin with the \"goog\" prefix.".to_string(),
            entity: "BucketName".to_string(),
        });
    }

    // Bucket names cannot contain "google" or close misspellings.
    let lower_bucket = bucket.to_lowercase();
    if lazy_regex::regex!(r"(g[0o][0o]+g[l1]e)").is_match(&lower_bucket) {
        return Err(ValidationError::InvalidProfile {
            source: None,
            reason:
                "Bucket name cannot contain \"google\" or close misspellings, such as \"g00gle\"."
                    .to_string(),
            entity: "BucketName".to_string(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use crate::service::storage::gcs::validate_bucket_name;
    use needs_env_var::needs_env_var;

    // Bucket names: Your bucket names must meet the following requirements:
    //
    // Bucket names can only contain lowercase letters, numeric characters, dashes (-), underscores (_), and dots (.). Spaces are not allowed. Names containing dots require verification.
    // Bucket names must start and end with a number or letter.
    // Bucket names must contain 3-63 characters. Names containing dots can contain up to 222 characters, but each dot-separated component can be no longer than 63 characters.
    // Bucket names cannot be represented as an IP address in dotted-decimal notation (for example, 192.168.5.4).
    // Bucket names cannot begin with the "goog" prefix.
    // Bucket names cannot contain "google" or close misspellings, such as "g00gle".
    #[test]
    fn test_valid_bucket_names() {
        // Valid bucket names
        assert!(validate_bucket_name("valid-bucket-name").is_ok());
        assert!(validate_bucket_name("valid.bucket.name").is_ok());
        assert!(validate_bucket_name("valid-bucket-name-123").is_ok());
        assert!(validate_bucket_name("123-valid-bucket-name").is_ok());
        assert!(validate_bucket_name("valid-bucket-name-123").is_ok());
        assert!(validate_bucket_name("valid.bucket.name.123").is_ok());

        // Invalid bucket names
        assert!(validate_bucket_name("Invalid-Bucket-Name").is_err()); // Uppercase letters
        assert!(validate_bucket_name("invalid_bucket_name").is_err()); // Underscores
        assert!(validate_bucket_name("invalid bucket name").is_err()); // Spaces
        assert!(validate_bucket_name("invalid..bucket..name").is_err()); // Adjacent periods
        assert!(validate_bucket_name("invalid-bucket-name-").is_err()); // Ends with hyphen
        assert!(validate_bucket_name("-invalid-bucket-name").is_err()); // Starts with hyphen
        assert!(validate_bucket_name("gooogle-bucket-name").is_err()); // Contains "gooogle"
        assert!(validate_bucket_name("192.168.5.4").is_err()); // IP address format
        assert!(validate_bucket_name("goog-bucket-name").is_err()); // Begins with "goog"
        assert!(validate_bucket_name("a").is_err()); // Less than 3 characters
        assert!(validate_bucket_name("a".repeat(64).as_str()).is_err()); // More than 63 characters
    }

    #[needs_env_var(TEST_GCS = 1)]
    mod cloud_tests {
        use crate::service::storage::gcs::{GcsCredential, GcsProfile, GcsServiceKey};
        use crate::service::storage::StorageCredential;
        use crate::service::storage::StorageProfile;

        #[tokio::test]
        async fn test_can_validate() {
            let cred: StorageCredential = std::env::var("GCS_CREDENTIAL")
                .map(|s| GcsCredential::ServiceAccountKey {
                    key: serde_json::from_str::<GcsServiceKey>(&s).unwrap(),
                })
                .map_err(|_| ())
                .expect("Missing cred")
                .into();
            let s = &serde_json::to_string(&cred).unwrap();
            serde_json::from_str::<StorageCredential>(s).expect("json roundtrip failed");

            let bucket = std::env::var("GCS_BUCKET").expect("Missing bucket");

            let mut profile: StorageProfile = GcsProfile {
                bucket,
                key_prefix: Some("test_prefix".to_string()),
            }
            .into();

            profile.normalize().expect("Failed to normalize profile");
            profile.validate_access(Some(&cred), None).await.unwrap();
        }
    }
}
