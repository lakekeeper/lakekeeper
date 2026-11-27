//! Table health: the sizes of an Iceberg table and the problems found in its metadata.
//!
//! A measurement reads only Iceberg metadata (metadata.json, the current snapshot's manifest list
//! and its manifests). It never lists storage and never estimates: a table that can't be measured
//! gets a [`NotMeasurableReason`] and no sizes.

mod findings;
mod measure;

use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use lakekeeper_io::Location;
pub use measure::{MeasureError, measure};
use serde::{Deserialize, Serialize};

use crate::{WarehouseId, service::TableId};

/// Version of the measurement code. Stored with every result.
///
/// Raise it in any change to what a size means or how a finding decides. It is a plain number so
/// that servers of the same release agree on it.
pub const MEASUREMENT_VERSION: i32 = 1;

/// Sizes of the current snapshot of a table, in bytes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Sizes {
    /// Data files of the alive manifest entries.
    pub live_data_bytes: i64,
    /// Position-delete, equality-delete and deletion-vector files of the alive manifest entries.
    /// A Puffin file that holds several deletion vectors counts once.
    pub live_delete_bytes: i64,
    /// metadata.json, the manifest list, the manifests, and the statistics and partition
    /// statistics files of the current snapshot.
    pub live_metadata_bytes: i64,
}

/// Kind of a [`Finding`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, strum::Display)]
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
pub enum FindingKind {
    /// The `manifest_length` in the manifest list differs from the bytes of the manifest, or is
    /// missing.
    ManifestLengthMismatch,
}

/// Severity of a [`Finding`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Severity {
    Info,
    Warning,
    Critical,
}

/// A problem found in the metadata of a table. It does not stop the measurement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Finding {
    pub kind: FindingKind,
    pub severity: Severity,
    /// Kind-specific data that shows the problem.
    pub evidence: serde_json::Value,
    /// The values the decision used.
    pub thresholds: serde_json::Value,
}

/// Why a table can't be measured.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "reason")]
pub enum NotMeasurableReason {
    /// The table is encrypted.
    Encrypted,
    /// The scan ran over the run time limit of the queue.
    TimeLimit,
    /// An alive manifest entry has no file size.
    MissingFileSize { path: String },
    /// A manifest list or manifest can't be parsed.
    MetadataUnreadable { path: String },
    /// metadata.json, the manifest list or a manifest doesn't exist.
    MetadataMissing { path: String },
}

/// Result of measuring one table.
#[derive(Debug, Clone, PartialEq)]
pub enum MeasureOutcome {
    Measured(Measurement),
    NotMeasurable(NotMeasurableReason),
}

/// Sizes and findings of a measured table.
#[derive(Debug, Clone, PartialEq)]
pub struct Measurement {
    pub sizes: Sizes,
    pub findings: Vec<Finding>,
    /// The measured snapshot. `None` for a table without a current snapshot.
    pub snapshot_id: Option<i64>,
    /// Number of manifests read.
    pub manifests_read: u64,
}

/// The details a run stores for a table: findings, the reason the table could not be measured,
/// and the scan.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct HealthResult<'a> {
    pub not_measurable: Option<&'a NotMeasurableReason>,
    pub findings: &'a [Finding],
    pub metadata_location: &'a str,
    /// `None` if the table has no current snapshot or could not be measured.
    pub snapshot_id: Option<i64>,
    pub scan_duration_ms: i64,
    /// `None` if the table could not be measured.
    pub manifests_read: Option<u64>,
}

/// The result of one run on one table, as it is stored.
#[derive(Debug, Clone, PartialEq)]
pub struct TableHealthWrite {
    pub warehouse_id: WarehouseId,
    pub table_id: TableId,
    pub measurement_version: i32,
    /// Time from the end of the run to the earliest start of the next run.
    pub next_run_delay: TimeDelta,
    pub outcome: MeasureOutcome,
    /// The metadata location that was measured.
    pub metadata_location: Location,
    pub scan_duration: Duration,
}

impl TableHealthWrite {
    /// Highest severity of the findings. Warning for a table that could not be measured, so that
    /// it ranks among the tables to look at.
    #[must_use]
    pub fn max_severity(&self) -> Option<Severity> {
        match &self.outcome {
            MeasureOutcome::Measured(measurement) => measurement
                .findings
                .iter()
                .map(|finding| finding.severity)
                .max(),
            MeasureOutcome::NotMeasurable(_) => Some(Severity::Warning),
        }
    }

    #[must_use]
    pub fn findings(&self) -> &[Finding] {
        match &self.outcome {
            MeasureOutcome::Measured(measurement) => &measurement.findings,
            MeasureOutcome::NotMeasurable(_) => &[],
        }
    }

    /// `None` if the table could not be measured.
    #[must_use]
    pub fn sizes(&self) -> Option<Sizes> {
        match &self.outcome {
            MeasureOutcome::Measured(measurement) => Some(measurement.sizes),
            MeasureOutcome::NotMeasurable(_) => None,
        }
    }

    #[must_use]
    pub fn result(&self) -> HealthResult<'_> {
        let (not_measurable, snapshot_id, manifests_read) = match &self.outcome {
            MeasureOutcome::Measured(measurement) => (
                None,
                measurement.snapshot_id,
                Some(measurement.manifests_read),
            ),
            MeasureOutcome::NotMeasurable(reason) => (Some(reason), None, None),
        };
        HealthResult {
            not_measurable,
            findings: self.findings(),
            metadata_location: self.metadata_location.as_str(),
            snapshot_id,
            scan_duration_ms: i64::try_from(self.scan_duration.as_millis()).unwrap_or(i64::MAX),
            manifests_read,
        }
    }
}

/// The next run of a table, as a write sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NextRun {
    /// The end of the run, taken after commits to the table are held off, plus
    /// [`TableHealthWrite::next_run_delay`].
    pub not_before: DateTime<Utc>,
    /// The table's metadata location differs from the measured one: a commit changed the table
    /// during the run.
    pub location_changed: bool,
}

/// Whether a run's result was stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableHealthWriteOutcome {
    /// The table does not exist or is staged. Nothing was written.
    TableGone,
    /// A file was missing and a commit changed the table during the run: the commit can have
    /// deleted the file. Nothing was written.
    Raced(NextRun),
    Written(NextRun),
    /// The table's stored result comes from a newer measurement version and was kept.
    NewerExists(NextRun),
}
