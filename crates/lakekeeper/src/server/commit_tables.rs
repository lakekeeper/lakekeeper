use std::{
    collections::{BTreeSet, HashMap},
    str::FromStr as _,
};

use iceberg::{TableRequirement, TableUpdate, spec::TableMetadata};
use iceberg_ext::{
    catalog::TableUpdateKind,
    spec::{TableMetadataBuildResult, TableMetadataBuilder},
};
use lakekeeper_io::Location;

use crate::{
    server::tables::create_table::ensure_format_version_allowed,
    service::{AllowedFormatVersions, ErrorModel, Result},
};

/// Table properties that must not be modified or removed once set.
///
/// Per the Iceberg spec, catalogs must ensure these properties are immutable
/// after table creation. See: <https://iceberg.apache.org/docs/nightly/encryption/#catalog-security-requirements>
const IMMUTABLE_TABLE_PROPERTIES: &[&str] = &["encryption.key-id"];

/// Reject any `UpgradeFormatVersion` update whose target version is not permitted
/// by the warehouse policy. Only the upgrade action is checked, so tightening a
/// policy does not retroactively block writes to existing tables.
pub(crate) fn ensure_format_version_upgrades_allowed(
    updates: &[TableUpdate],
    allowed_format_versions: &AllowedFormatVersions,
) -> Result<()> {
    for update in updates {
        if let TableUpdate::UpgradeFormatVersion { format_version } = update {
            ensure_format_version_allowed(*format_version, allowed_format_versions)?;
        }
    }
    Ok(())
}

/// Apply the commits to table metadata.
pub(super) fn apply_commit(
    metadata: TableMetadata,
    metadata_location: Option<&Location>,
    requirements: &[TableRequirement],
    updates: Vec<TableUpdate>,
) -> Result<TableMetadataBuildResult> {
    // Check requirements
    requirements
        .iter()
        .map(|r| {
            r.check(metadata_location.map(|_| &metadata)).map_err(|e| {
                ErrorModel::conflict(e.to_string(), e.kind().to_string(), Some(Box::new(e))).into()
            })
        })
        .collect::<Result<Vec<_>>>()?;

    // Store data of current metadata to prevent disallowed changes
    let previous_location = Location::from_str(metadata.location()).map_err(|e| {
        ErrorModel::internal(
            format!("Invalid table location in DB: {e}"),
            "InvalidTableLocation",
            Some(Box::new(e)),
        )
    })?;
    let previous_uuid = metadata.uuid();
    let previous_immutable_properties: HashMap<&'static str, String> = IMMUTABLE_TABLE_PROPERTIES
        .iter()
        .filter_map(|&key| metadata.properties().get(key).map(|val| (key, val.clone())))
        .collect();

    let mut builder = TableMetadataBuilder::new_from_metadata(
        metadata,
        metadata_location.map(std::string::ToString::to_string),
    );

    // Update!
    for update in updates {
        tracing::debug!("Applying update: '{}'", TableUpdateKind::from(&update));
        match &update {
            TableUpdate::AssignUuid { uuid } => {
                if uuid != &previous_uuid {
                    return Err(ErrorModel::bad_request(
                        "Cannot assign a new UUID",
                        "AssignUuidNotAllowed",
                        None,
                    )
                    .into());
                }
            }
            TableUpdate::SetLocation { location } => {
                if location != &previous_location.to_string() {
                    return Err(ErrorModel::bad_request(
                        "Cannot change table location",
                        "SetLocationNotAllowed",
                        None,
                    )
                    .into());
                }
            }
            TableUpdate::SetProperties { updates } => {
                check_immutable_properties_not_modified(&previous_immutable_properties, updates)?;
                builder = TableUpdate::apply(update, builder).map_err(|e| {
                    let msg = e.message().to_string();
                    ErrorModel::bad_request(msg, "InvalidTableUpdate", Some(Box::new(e)))
                })?;
            }
            TableUpdate::RemoveProperties { removals } => {
                check_immutable_properties_not_removed(&previous_immutable_properties, removals)?;
                builder = TableUpdate::apply(update, builder).map_err(|e| {
                    let msg = e.message().to_string();
                    ErrorModel::bad_request(msg, "InvalidTableUpdate", Some(Box::new(e)))
                })?;
            }
            _ => {
                builder = TableUpdate::apply(update, builder).map_err(|e| {
                    let msg = e.message().to_string();
                    ErrorModel::bad_request(msg, "InvalidTableUpdate", Some(Box::new(e)))
                })?;
            }
        }
    }
    builder
        .build()
        .map_err(|e| {
            tracing::debug!("Table metadata build failed: {}", e);
            let msg = e.message().to_string();
            ErrorModel::conflict(msg, "CommitFailedException", Some(Box::new(e))).into()
        })
        .inspect(|r| {
            tracing::debug!(
                "Table metadata updated, at: {}",
                r.metadata.last_updated_ms()
            );
        })
}

/// Collect the branch/tag ref names a commit explicitly writes, from its
/// `SetSnapshotRef` / `RemoveSnapshotRef` updates.
///
/// A `RemoveSnapshots` update can drop a ref by cascade without naming it here.
/// That case is deliberately NOT resolved to a ref name (doing so precisely
/// needs the pre-commit metadata, unavailable at authorization time). Instead it
/// is covered by [`update_kinds`]: a commit whose kinds are not purely ref/data
/// moves is escalated by policy to require table-wide authority, so an unnamed
/// cascade cannot slip past a protected-ref rule.
pub(super) fn refs_from_updates(updates: &[TableUpdate]) -> BTreeSet<String> {
    updates
        .iter()
        .filter_map(|update| match update {
            TableUpdate::SetSnapshotRef { ref_name, .. }
            | TableUpdate::RemoveSnapshotRef { ref_name } => Some(ref_name.clone()),
            _ => None,
        })
        .collect()
}

/// Collect the distinct kinds of updates present in a commit.
///
/// Passed to the authorizer so a policy can require table-wide authority when a
/// commit contains table-global changes (schema/spec/sort-order/properties)
/// rather than only moving a branch ref.
pub(super) fn update_kinds(updates: &[TableUpdate]) -> BTreeSet<TableUpdateKind> {
    updates.iter().map(TableUpdateKind::from).collect()
}

/// Return an error if any immutable property that already exists on the table
/// is being changed to a different value.
fn check_immutable_properties_not_modified(
    previous_immutable_properties: &HashMap<&str, String>,
    updates: &HashMap<String, String>,
) -> Result<()> {
    for (&prop, previous_value) in previous_immutable_properties {
        if let Some(new_value) = updates.get(prop)
            && *new_value != *previous_value
        {
            return Err(ErrorModel::bad_request(
                format!("Cannot modify immutable property '{prop}'"),
                "ImmutablePropertyModification",
                None,
            )
            .into());
        }
    }
    Ok(())
}

/// Return an error if any immutable property that exists on the table
/// is being removed.
fn check_immutable_properties_not_removed(
    previous_immutable_properties: &HashMap<&str, String>,
    removals: &[String],
) -> Result<()> {
    for &prop in previous_immutable_properties.keys() {
        if removals.iter().any(|r| r == prop) {
            return Err(ErrorModel::bad_request(
                format!("Cannot remove immutable property '{prop}'"),
                "ImmutablePropertyRemoval",
                None,
            )
            .into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};

    use iceberg::{
        TableUpdate,
        spec::{
            FormatVersion, NestedField, PrimitiveType, Schema, SnapshotReference,
            SnapshotRetention, SortOrder, UnboundPartitionSpec,
        },
    };
    use iceberg_ext::{catalog::TableUpdateKind, spec::TableMetadataBuilder};

    use super::{
        AllowedFormatVersions, apply_commit, ensure_format_version_upgrades_allowed,
        refs_from_updates, update_kinds,
    };

    fn test_metadata_with_properties(
        props: HashMap<String, String>,
    ) -> iceberg::spec::TableMetadata {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", iceberg::spec::Type::Primitive(PrimitiveType::Int))
                    .into(),
            ])
            .build()
            .unwrap();

        TableMetadataBuilder::new(
            schema,
            UnboundPartitionSpec::builder().build(),
            SortOrder::unsorted_order(),
            "s3://bucket/table".to_string(),
            FormatVersion::V2,
            props,
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata
    }

    fn branch_ref(snapshot_id: i64) -> SnapshotReference {
        SnapshotReference {
            snapshot_id,
            retention: SnapshotRetention::Branch {
                min_snapshots_to_keep: None,
                max_snapshot_age_ms: None,
                max_ref_age_ms: None,
            },
        }
    }

    #[test]
    fn refs_from_updates_collects_set_and_remove_snapshot_ref() {
        let updates = vec![
            TableUpdate::SetSnapshotRef {
                ref_name: "dev".to_string(),
                reference: branch_ref(1),
            },
            TableUpdate::RemoveSnapshotRef {
                ref_name: "old".to_string(),
            },
            TableUpdate::UpgradeFormatVersion {
                format_version: FormatVersion::V2,
            },
        ];
        assert_eq!(
            refs_from_updates(&updates),
            BTreeSet::from(["dev".to_string(), "old".to_string()])
        );
    }

    #[test]
    fn refs_from_updates_ignores_removesnapshots_cascade() {
        // `RemoveSnapshots` names no ref; the cascade is handled via
        // `update_kinds` escalation, so no ref name is extracted here.
        let updates = vec![TableUpdate::RemoveSnapshots {
            snapshot_ids: vec![1],
        }];
        assert!(refs_from_updates(&updates).is_empty());
    }

    #[test]
    fn update_kinds_collects_distinct_update_types() {
        let updates = vec![
            TableUpdate::SetSnapshotRef {
                ref_name: "dev".to_string(),
                reference: branch_ref(1),
            },
            TableUpdate::UpgradeFormatVersion {
                format_version: FormatVersion::V2,
            },
            TableUpdate::SetProperties {
                updates: HashMap::from([("k".to_string(), "v".to_string())]),
            },
        ];
        assert_eq!(
            update_kinds(&updates),
            BTreeSet::from([
                TableUpdateKind::SetSnapshotRef,
                TableUpdateKind::UpgradeFormatVersion,
                TableUpdateKind::SetProperties,
            ])
        );
    }

    #[test]
    fn test_immutable_property_cannot_be_modified() {
        let metadata = test_metadata_with_properties(HashMap::from([(
            "encryption.key-id".to_string(),
            "key-1".to_string(),
        )]));

        let result = apply_commit(
            metadata,
            None,
            &[],
            vec![TableUpdate::SetProperties {
                updates: HashMap::from([("encryption.key-id".to_string(), "key-2".to_string())]),
            }],
        );

        let err = result.unwrap_err();
        assert_eq!(err.error.r#type, "ImmutablePropertyModification");
    }

    #[test]
    fn test_immutable_property_cannot_be_removed() {
        let metadata = test_metadata_with_properties(HashMap::from([(
            "encryption.key-id".to_string(),
            "key-1".to_string(),
        )]));

        let result = apply_commit(
            metadata,
            None,
            &[],
            vec![TableUpdate::RemoveProperties {
                removals: vec!["encryption.key-id".to_string()],
            }],
        );

        let err = result.unwrap_err();
        assert_eq!(err.error.r#type, "ImmutablePropertyRemoval");
    }

    #[test]
    fn test_immutable_property_can_be_set_to_same_value() {
        let metadata = test_metadata_with_properties(HashMap::from([(
            "encryption.key-id".to_string(),
            "key-1".to_string(),
        )]));

        let result = apply_commit(
            metadata,
            None,
            &[],
            vec![TableUpdate::SetProperties {
                updates: HashMap::from([("encryption.key-id".to_string(), "key-1".to_string())]),
            }],
        );

        assert!(result.is_ok());
    }

    #[test]
    fn test_immutable_property_can_be_set_initially() {
        let metadata = test_metadata_with_properties(HashMap::new());

        let result = apply_commit(
            metadata,
            None,
            &[],
            vec![TableUpdate::SetProperties {
                updates: HashMap::from([("encryption.key-id".to_string(), "key-1".to_string())]),
            }],
        );

        assert!(result.is_ok());
    }

    #[test]
    fn test_removing_nonexistent_immutable_property_is_ok() {
        let metadata = test_metadata_with_properties(HashMap::new());

        let result = apply_commit(
            metadata,
            None,
            &[],
            vec![TableUpdate::RemoveProperties {
                removals: vec!["encryption.key-id".to_string()],
            }],
        );

        assert!(result.is_ok());
    }

    #[test]
    fn test_other_properties_remain_mutable() {
        let metadata = test_metadata_with_properties(HashMap::from([(
            "some.other.prop".to_string(),
            "old-value".to_string(),
        )]));

        let result = apply_commit(
            metadata,
            None,
            &[],
            vec![TableUpdate::SetProperties {
                updates: HashMap::from([("some.other.prop".to_string(), "new-value".to_string())]),
            }],
        );

        assert!(result.is_ok());
    }

    #[test]
    fn test_upgrade_to_allowed_format_version_succeeds() {
        let allowed = AllowedFormatVersions::try_new([FormatVersion::V2, FormatVersion::V3])
            .expect("non-empty");

        ensure_format_version_upgrades_allowed(
            &[TableUpdate::UpgradeFormatVersion {
                format_version: FormatVersion::V3,
            }],
            &allowed,
        )
        .expect("V3 upgrade is allowed");
    }

    #[test]
    fn test_upgrade_to_disallowed_format_version_is_rejected() {
        let allowed = AllowedFormatVersions::try_new([FormatVersion::V2]).expect("non-empty");

        let err = ensure_format_version_upgrades_allowed(
            &[TableUpdate::UpgradeFormatVersion {
                format_version: FormatVersion::V3,
            }],
            &allowed,
        )
        .unwrap_err();

        assert_eq!(err.error.r#type, "FormatVersionNotAllowed");
    }
}
