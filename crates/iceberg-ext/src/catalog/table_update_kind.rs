use iceberg::TableUpdate;
use serde::{Deserialize, Serialize};

/// The kind of a [`TableUpdate`], independent of its payload.
///
/// `iceberg-rust`'s [`TableUpdate`] is a `#[serde(tag = "action")]` enum with no
/// companion discriminant type, and we cannot derive one onto a foreign type.
/// This mirrors its variants so callers can reason about *what kinds* of changes
/// a commit contains — e.g. to decide whether a commit only moves a branch ref or
/// also carries table-global changes — without cloning payloads or matching on
/// stringly-typed names.
///
/// The `serde` representation is kept identical to [`TableUpdate`]'s `action`
/// tag (kebab-case), so a kind serializes to exactly the action string a client
/// sent. This is enforced by a test.
#[derive(
    Clone,
    Copy,
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Serialize,
    Deserialize,
    strum_macros::Display,
    utoipa::ToSchema,
)]
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
pub enum TableUpdateKind {
    UpgradeFormatVersion,
    AssignUuid,
    AddSchema,
    SetCurrentSchema,
    AddSpec,
    SetDefaultSpec,
    AddSortOrder,
    SetDefaultSortOrder,
    AddSnapshot,
    SetSnapshotRef,
    RemoveSnapshots,
    RemoveSnapshotRef,
    SetLocation,
    SetProperties,
    RemoveProperties,
    RemovePartitionSpecs,
    SetStatistics,
    RemoveStatistics,
    SetPartitionStatistics,
    RemovePartitionStatistics,
    RemoveSchemas,
    AddEncryptionKey,
    RemoveEncryptionKey,
}

impl From<&TableUpdate> for TableUpdateKind {
    fn from(update: &TableUpdate) -> Self {
        match update {
            TableUpdate::UpgradeFormatVersion { .. } => Self::UpgradeFormatVersion,
            TableUpdate::AssignUuid { .. } => Self::AssignUuid,
            TableUpdate::AddSchema { .. } => Self::AddSchema,
            TableUpdate::SetCurrentSchema { .. } => Self::SetCurrentSchema,
            TableUpdate::AddSpec { .. } => Self::AddSpec,
            TableUpdate::SetDefaultSpec { .. } => Self::SetDefaultSpec,
            TableUpdate::AddSortOrder { .. } => Self::AddSortOrder,
            TableUpdate::SetDefaultSortOrder { .. } => Self::SetDefaultSortOrder,
            TableUpdate::AddSnapshot { .. } => Self::AddSnapshot,
            TableUpdate::SetSnapshotRef { .. } => Self::SetSnapshotRef,
            TableUpdate::RemoveSnapshots { .. } => Self::RemoveSnapshots,
            TableUpdate::RemoveSnapshotRef { .. } => Self::RemoveSnapshotRef,
            TableUpdate::SetLocation { .. } => Self::SetLocation,
            TableUpdate::SetProperties { .. } => Self::SetProperties,
            TableUpdate::RemoveProperties { .. } => Self::RemoveProperties,
            TableUpdate::RemovePartitionSpecs { .. } => Self::RemovePartitionSpecs,
            TableUpdate::SetStatistics { .. } => Self::SetStatistics,
            TableUpdate::RemoveStatistics { .. } => Self::RemoveStatistics,
            TableUpdate::SetPartitionStatistics { .. } => Self::SetPartitionStatistics,
            TableUpdate::RemovePartitionStatistics { .. } => Self::RemovePartitionStatistics,
            TableUpdate::RemoveSchemas { .. } => Self::RemoveSchemas,
            TableUpdate::AddEncryptionKey { .. } => Self::AddEncryptionKey,
            TableUpdate::RemoveEncryptionKey { .. } => Self::RemoveEncryptionKey,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use iceberg::{
        TableUpdate,
        spec::{
            FormatVersion, Schema, SnapshotReference, SnapshotRetention, SortOrder,
            UnboundPartitionSpec,
        },
    };

    use super::TableUpdateKind;

    fn sample_updates() -> Vec<TableUpdate> {
        // A broad, cheap-to-construct subset covering multi-word kebab names and
        // every ref/snapshot-related kind. The four variants with heavy payloads
        // (AddSnapshot, SetStatistics, SetPartitionStatistics, AddEncryptionKey)
        // are omitted here — the `From` match is exhaustive so the compiler
        // guarantees they are mapped.
        let reference = SnapshotReference {
            snapshot_id: 1,
            retention: SnapshotRetention::Branch {
                min_snapshots_to_keep: None,
                max_snapshot_age_ms: None,
                max_ref_age_ms: None,
            },
        };
        vec![
            TableUpdate::UpgradeFormatVersion {
                format_version: FormatVersion::V2,
            },
            TableUpdate::AssignUuid {
                uuid: uuid::Uuid::nil(),
            },
            TableUpdate::AddSchema {
                schema: Schema::builder().build().unwrap(),
            },
            TableUpdate::SetCurrentSchema { schema_id: 1 },
            TableUpdate::AddSpec {
                spec: UnboundPartitionSpec::builder().build(),
            },
            TableUpdate::SetDefaultSpec { spec_id: 0 },
            TableUpdate::AddSortOrder {
                sort_order: SortOrder::unsorted_order(),
            },
            TableUpdate::SetDefaultSortOrder { sort_order_id: 0 },
            TableUpdate::SetSnapshotRef {
                ref_name: "main".to_string(),
                reference,
            },
            TableUpdate::RemoveSnapshots {
                snapshot_ids: vec![1],
            },
            TableUpdate::RemoveSnapshotRef {
                ref_name: "dev".to_string(),
            },
            TableUpdate::SetLocation {
                location: "s3://bucket/table".to_string(),
            },
            TableUpdate::SetProperties {
                updates: HashMap::new(),
            },
            TableUpdate::RemoveProperties { removals: vec![] },
            TableUpdate::RemovePartitionSpecs { spec_ids: vec![1] },
            TableUpdate::RemoveStatistics { snapshot_id: 1 },
            TableUpdate::RemovePartitionStatistics { snapshot_id: 1 },
            TableUpdate::RemoveSchemas {
                schema_ids: vec![1],
            },
            TableUpdate::RemoveEncryptionKey {
                key_id: "k".to_string(),
            },
        ]
    }

    /// Each kind must serialize to exactly the `action` tag its `TableUpdate`
    /// serializes to — the kind is a faithful discriminant of the wire type.
    /// This catches both a wrong `From` arm and any drift from iceberg's names.
    #[test]
    fn kind_serializes_to_iceberg_action_tag() {
        for update in sample_updates() {
            let update_json = serde_json::to_value(&update).unwrap();
            let action = update_json
                .get("action")
                .expect("TableUpdate serializes with an `action` tag");
            let kind = TableUpdateKind::from(&update);
            let kind_json = serde_json::to_value(kind).unwrap();
            assert_eq!(&kind_json, action, "kind mismatch for update {update:?}");
        }
    }

    /// The ref/global classification the authorizer relies on must be stable.
    #[test]
    fn from_maps_ref_and_global_kinds() {
        let set_ref = TableUpdate::SetSnapshotRef {
            ref_name: "main".to_string(),
            reference: SnapshotReference {
                snapshot_id: 1,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            },
        };
        assert_eq!(
            TableUpdateKind::from(&set_ref),
            TableUpdateKind::SetSnapshotRef
        );
        assert_eq!(
            TableUpdateKind::from(&TableUpdate::RemoveSnapshotRef {
                ref_name: "dev".to_string()
            }),
            TableUpdateKind::RemoveSnapshotRef
        );
        assert_eq!(
            TableUpdateKind::from(&TableUpdate::RemoveSnapshots {
                snapshot_ids: vec![1]
            }),
            TableUpdateKind::RemoveSnapshots
        );
        assert_eq!(
            TableUpdateKind::from(&TableUpdate::SetProperties {
                updates: HashMap::new()
            }),
            TableUpdateKind::SetProperties
        );
    }
}
