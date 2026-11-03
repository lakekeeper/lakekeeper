use std::collections::BTreeSet;

use iceberg::TableUpdate;
use serde::{Deserialize, Serialize};

/// The kind of a table metadata update, identified by its Iceberg action name
/// (for example `set-snapshot-ref`, `add-schema`, or `remove-snapshots`).
//
// Implementation note (not part of the public schema): iceberg's `TableUpdate`
// is a foreign `#[serde(tag = "action")]` enum with no discriminant type, and a
// derive can't be added to a foreign type — so this mirrors its variants. The
// serde/`Display` names are kept identical to `TableUpdate`'s `action` tag
// (kebab-case), test-locked, so a kind maps 1:1 to the action a client sent.
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
    strum_macros::EnumCount,
    strum_macros::VariantNames,
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

impl TableUpdateKind {
    /// Whether a commit made of `kinds` only changes the table's data: it adds snapshots,
    /// moves branches and tags, and records statistics. Anything that changes the table's
    /// structure, properties or history is not. An empty set names no update, so it says
    /// nothing about the commit and does not count.
    #[must_use]
    pub fn changes_only_data(kinds: &BTreeSet<TableUpdateKind>) -> bool {
        !kinds.is_empty()
            && kinds.iter().all(|kind| match kind {
                Self::AddSnapshot
                | Self::SetSnapshotRef
                | Self::SetStatistics
                | Self::RemoveStatistics
                | Self::SetPartitionStatistics
                | Self::RemovePartitionStatistics => true,
                // Writers to an encrypted table add the key of each snapshot they add.
                Self::AddEncryptionKey => kinds.contains(&Self::AddSnapshot),
                Self::UpgradeFormatVersion
                | Self::AssignUuid
                | Self::AddSchema
                | Self::SetCurrentSchema
                | Self::AddSpec
                | Self::SetDefaultSpec
                | Self::AddSortOrder
                | Self::SetDefaultSortOrder
                | Self::RemoveSnapshots
                | Self::RemoveSnapshotRef
                | Self::SetLocation
                | Self::SetProperties
                | Self::RemoveProperties
                | Self::RemovePartitionSpecs
                | Self::RemoveSchemas
                | Self::RemoveEncryptionKey => false,
            })
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
    ///
    /// Also pins `Display` (strum) to the same string as serde: the two are
    /// independent kebab-case encodings, and both feed authorization (the action
    /// descriptor uses `Display`, the wire contract uses serde), so they must not
    /// drift apart.
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
            assert_eq!(
                serde_json::Value::String(kind.to_string()),
                *action,
                "Display disagrees with serde for {kind:?}"
            );
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

    fn kinds(kinds: &[TableUpdateKind]) -> std::collections::BTreeSet<TableUpdateKind> {
        kinds.iter().copied().collect()
    }

    /// What ordinary writers send, including statistics written with the data and the keys
    /// of encrypted tables.
    #[test]
    fn data_commits_change_only_data() {
        use TableUpdateKind::*;
        for commit in [
            vec![AddSnapshot],
            vec![AddSnapshot, SetSnapshotRef],
            vec![SetSnapshotRef],
            vec![AddSnapshot, SetSnapshotRef, SetStatistics],
            vec![RemoveStatistics],
            vec![SetPartitionStatistics, RemovePartitionStatistics],
            vec![AddEncryptionKey, AddSnapshot, SetSnapshotRef],
        ] {
            assert!(
                TableUpdateKind::changes_only_data(&kinds(&commit)),
                "{commit:?} only changes data"
            );
        }
    }

    #[test]
    fn other_commits_are_not_data_commits() {
        use TableUpdateKind::*;
        for commit in [
            vec![AddSchema, SetCurrentSchema],
            vec![AddSnapshot, SetSnapshotRef, SetProperties],
            vec![RemoveProperties],
            vec![RemoveSnapshots],
            vec![RemoveSnapshotRef],
            vec![AddSpec, SetDefaultSpec],
            vec![AddSortOrder, SetDefaultSortOrder],
            vec![UpgradeFormatVersion],
            vec![AssignUuid],
            vec![SetLocation],
            vec![RemovePartitionSpecs, RemoveSchemas],
            vec![RemoveEncryptionKey],
            // A key without a snapshot to encrypt is not part of a data commit.
            vec![AddEncryptionKey],
            // Names no update, so says nothing about the commit.
            vec![],
        ] {
            assert!(
                !TableUpdateKind::changes_only_data(&kinds(&commit)),
                "{commit:?} is not a data commit"
            );
        }
    }

    /// Pins the data kinds over every variant, alone and together with an added
    /// snapshot, so a structural kind cannot hide behind a companion kind.
    #[test]
    fn data_kinds_are_exactly_these() {
        use TableUpdateKind::*;
        use strum::{EnumCount as _, VariantNames as _};

        let all: Vec<TableUpdateKind> = TableUpdateKind::VARIANTS
            .iter()
            .map(|name| serde_json::from_value(serde_json::Value::from(*name)).unwrap())
            .collect();
        assert_eq!(all.len(), TableUpdateKind::COUNT);

        let alone: Vec<_> = all
            .iter()
            .copied()
            .filter(|kind| TableUpdateKind::changes_only_data(&kinds(&[*kind])))
            .collect();
        assert_eq!(
            alone,
            vec![
                AddSnapshot,
                SetSnapshotRef,
                SetStatistics,
                RemoveStatistics,
                SetPartitionStatistics,
                RemovePartitionStatistics,
            ]
        );

        let with_snapshot: Vec<_> = all
            .iter()
            .copied()
            .filter(|kind| TableUpdateKind::changes_only_data(&kinds(&[*kind, AddSnapshot])))
            .collect();
        assert_eq!(
            with_snapshot,
            vec![
                AddSnapshot,
                SetSnapshotRef,
                SetStatistics,
                RemoveStatistics,
                SetPartitionStatistics,
                RemovePartitionStatistics,
                AddEncryptionKey,
            ]
        );
    }
}
