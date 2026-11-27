//! Table health: sizes and findings of the Iceberg tables in a warehouse.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::service::TableId;

/// Sort order of the table health ranking.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub enum TableHealthOrderBy {
    /// Highest severity first, then most live data bytes.
    #[default]
    Severity,
}

/// Whether the last run measured the table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub enum TableHealthStatus {
    /// The table was measured. Sizes and findings are set.
    Measured,
    /// The table cannot be measured. `not-measurable-reason` says why. Sizes are not set.
    NotMeasurable,
    /// The table has no result yet. Only the table health detail returns this status.
    NotMeasured,
}

/// Why a table cannot be measured.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case", tag = "reason")]
pub enum NotMeasurableReason {
    /// The table is encrypted.
    Encrypted,
    /// The scan ran over the run time limit.
    TimeLimit,
    /// A data or delete file in the current snapshot has no file size.
    MissingFileSize {
        /// The file without a size
        path: String,
    },
    /// A manifest list or manifest cannot be read.
    MetadataUnreadable {
        /// The file that cannot be read
        path: String,
    },
    /// The metadata file, the manifest list or a manifest does not exist.
    MetadataMissing {
        /// The missing file
        path: String,
    },
}

/// Severity of a finding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub enum FindingSeverity {
    Info,
    Warning,
    Critical,
}

/// One table in the health ranking of a warehouse.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct TableHealthSummary {
    #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
    pub table_id: TableId,
    /// Namespace parts of the table
    pub namespace: Vec<String>,
    /// Name of the table
    pub name: String,
    /// `measured` or `not-measurable`
    pub status: TableHealthStatus,
    /// Set if `status` is `not-measurable`
    pub not_measurable_reason: Option<NotMeasurableReason>,
    /// Highest severity of the table's findings. `warning` for a table that cannot be measured.
    /// Not set if the table has no findings.
    pub max_severity: Option<FindingSeverity>,
    pub finding_count: i32,
    /// Bytes of the data files of the current snapshot. Not set if `status` is `not-measurable`.
    pub live_data_bytes: Option<i64>,
    /// Bytes of the delete files of the current snapshot. Not set if `status` is
    /// `not-measurable`.
    pub live_delete_bytes: Option<i64>,
    /// Bytes of the metadata files of the current snapshot. Not set if `status` is
    /// `not-measurable`.
    pub live_metadata_bytes: Option<i64>,
    /// End of the last run
    pub measured_at: DateTime<Utc>,
    /// Version of the measurement code that produced these values
    pub measurement_version: i32,
}
