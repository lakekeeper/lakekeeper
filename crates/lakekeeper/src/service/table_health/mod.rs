//! Table health: the sizes of an Iceberg table and the problems found in its metadata.
//!
//! A measurement reads only Iceberg metadata (metadata.json, the current snapshot's manifest list
//! and its manifests). It never lists storage and never estimates: a table that can't be measured
//! gets a [`NotMeasurableReason`] and no sizes.

mod measure;

pub use measure::{MeasureError, measure};
use serde::{Deserialize, Serialize};

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
