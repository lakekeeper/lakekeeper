//! Table health: sizes and findings of the Iceberg tables in a warehouse.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::tasks::TaskStatus;
use crate::service::{TableId, tasks::TaskId};

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

/// A problem health found in a table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct TableHealthFinding {
    /// Kind of the finding, e.g. `manifest-length-mismatch`
    pub kind: String,
    pub severity: FindingSeverity,
    /// Kind-specific data that shows the problem
    #[serde(default)]
    pub evidence: serde_json::Value,
    /// The values the decision used
    #[serde(default)]
    pub thresholds: serde_json::Value,
}

/// The `table_health` task of a table.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct TableHealthTask {
    /// The active task. Not set if no task is scheduled or running.
    #[cfg_attr(feature = "open-api", schema(value_type = Option<uuid::Uuid>))]
    pub task_id: Option<TaskId>,
    /// Status of the active task
    pub status: Option<TaskStatus>,
    /// When the active task runs
    pub scheduled_for: Option<DateTime<Utc>>,
    /// Outcome of the last finished attempt
    pub last_outcome: Option<TaskStatus>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct GetTableHealthResponse {
    #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
    pub table_id: TableId,
    /// Namespace parts of the table
    pub namespace: Vec<String>,
    /// Name of the table
    pub name: String,
    pub status: TableHealthStatus,
    /// Set if `status` is `not-measurable`
    pub not_measurable_reason: Option<NotMeasurableReason>,
    /// Highest severity of the table's findings. `warning` for a table that cannot be measured.
    /// Not set if the table has no findings.
    pub max_severity: Option<FindingSeverity>,
    pub finding_count: i32,
    /// Bytes of the data files of the current snapshot. Set if `status` is `measured`.
    pub live_data_bytes: Option<i64>,
    /// Bytes of the delete files of the current snapshot. Set if `status` is `measured`.
    pub live_delete_bytes: Option<i64>,
    /// Bytes of the metadata files of the current snapshot. Set if `status` is `measured`.
    pub live_metadata_bytes: Option<i64>,
    /// End of the last run. Not set if `status` is `not-measured`.
    pub measured_at: Option<DateTime<Utc>>,
    /// Version of the measurement code that produced these values. Not set if `status` is
    /// `not-measured`.
    pub measurement_version: Option<i32>,
    pub findings: Vec<TableHealthFinding>,
    /// Duration of the last run
    pub scan_duration_ms: Option<i64>,
    /// Metadata file that was measured
    pub metadata_location: Option<String>,
    /// Current snapshot that was measured. Not set for a table without a snapshot or a table
    /// that cannot be measured.
    pub snapshot_id: Option<i64>,
    /// Number of manifests read. Set if `status` is `measured`.
    pub manifests_read: Option<i64>,
    /// Earliest time of the next run. Not set if `status` is `not-measured`.
    pub next_not_before: Option<DateTime<Utc>>,
    /// Only set if the caller may see the table's tasks
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task: Option<TableHealthTask>,
}

/// The part of the health totals of a warehouse that comes from soft-deleted tables.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct SoftDeletedHealthTotals {
    /// Number of soft-deleted Iceberg tables
    pub tables: i64,
    pub live_data_bytes: i64,
    pub live_delete_bytes: i64,
    pub live_metadata_bytes: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct MeasurementVersionCount {
    pub version: i32,
    pub tables: i64,
}

/// Health totals of the Iceberg tables of a warehouse. Sums include soft-deleted tables.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct WarehouseHealthTotalsResponse {
    /// Number of Iceberg tables, soft-deleted tables included
    pub tables: i64,
    pub tables_measured: i64,
    pub tables_not_measurable: i64,
    /// Tables never measured or not measurable
    pub tables_without_value: i64,
    /// Sum over the measured tables
    pub live_data_bytes: i64,
    /// Sum over the measured tables
    pub live_delete_bytes: i64,
    /// Sum over the measured tables
    pub live_metadata_bytes: i64,
    /// The part of the counts and sums that comes from soft-deleted tables
    pub soft_deleted: SoftDeletedHealthTotals,
    /// Measurement versions in use, with the number of tables measured by each
    pub measurement_versions: Vec<MeasurementVersionCount>,
}
