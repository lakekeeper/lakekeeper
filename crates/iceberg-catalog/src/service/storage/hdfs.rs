use std::collections::HashMap;

use iceberg_ext::configs::{table::TableProperties, Location, ParseFromStr};
use serde::{Deserialize, Serialize};

use super::error::{CredentialsError, FileIoError};
use crate::{
    api::iceberg::v1::DataAccess,
    service::storage::{
        error::TableConfigError, StoragePermissions, StorageType, TableConfig, ValidationError,
    },
    CONFIG,
};

#[derive(Debug, Eq, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
/// HDFS Storage Profile
pub struct HdfsProfile {
    /// HDFS URL, i.e. `hdfs://ns`
    pub url: url::Url,
    /// Path in the filesystem which is used for this
    /// profile.
    pub key_prefix: String,
    #[serde(default)]
    /// Additional configuration options.
    /// Please check the [`rust-native` Documentation](https://github.com/Kimahriman/hdfs-native?tab=readme-ov-file#supported-hdfs-settings)
    /// for possible keys.
    pub config: HashMap<String, String>,
}

impl HdfsProfile {
    #[allow(clippy::unnecessary_wraps)]
    #[allow(clippy::unused_self)]
    pub fn generate_table_config(
        &self,
        _data_access: DataAccess,
        _table_location: &Location,
        _storage_permissions: StoragePermissions,
    ) -> Result<TableConfig, TableConfigError> {
        Ok(TableConfig {
            creds: TableProperties::default(),
            config: TableProperties::default(),
        })
    }

    #[allow(clippy::unnecessary_wraps)]
    #[allow(clippy::unused_self)]
    pub fn normalize(&mut self) -> Result<(), ValidationError> {
        Ok(())
    }

    pub fn base_location(&self) -> Result<Location, ValidationError> {
        Location::parse_value(&format!(
            "{}/{}",
            self.url.as_str().trim_end_matches('/'),
            self.key_prefix.as_str().trim_start_matches('/')
        ))
        .map_err(|e| ValidationError::InvalidLocation {
            source: Some(Box::new(e)),
            reason: "Failed to create location for storage profile.".to_string(),
            storage_type: StorageType::Hdfs,
            location: self.key_prefix.clone(),
        })
    }

    #[must_use]
    /// Check whether the location of this storage profile is overlapping
    /// with the given storage profile.
    pub fn is_overlapping_location(&self, other: &Self) -> bool {
        if self.url != other.url {
            return false;
        }

        // If key prefixes are identical, they overlap
        if self.key_prefix == other.key_prefix {
            return true;
        }

        let kp1 = format!("{}/", self.key_prefix);
        let kp2 = format!("{}/", other.key_prefix);
        kp1.starts_with(&kp2) || kp2.starts_with(&kp1)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, utoipa::ToSchema, PartialEq)]
pub struct HdfsCredential {}

impl HdfsProfile {
    pub fn file_io(
        &self,
        _credential: Option<&HdfsCredential>,
    ) -> Result<hdfs_native::Client, FileIoError> {
        if !CONFIG.enable_hdfs_with_system_credentials {
            return Err(FileIoError::Credentials(
                CredentialsError::Misconfiguration(
                    "HDFS is disabled in this Lakekeeper instance.".to_string(),
                ),
            ));
        }
        hdfs_native::Client::new_with_config(self.url.as_str(), self.config.clone())
            .map_err(|e| FileIoError::FileIoCreationFailed(Box::new(e)))
    }
}
