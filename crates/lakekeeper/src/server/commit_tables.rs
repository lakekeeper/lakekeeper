use std::{collections::HashMap, str::FromStr as _};

use iceberg::{TableRequirement, TableUpdate, spec::TableMetadata};
use iceberg_ext::spec::{TableMetadataBuildResult, TableMetadataBuilder};
use lakekeeper_io::Location;

use crate::service::{ErrorModel, Result};

/// Table properties that must not be modified or removed once set.
///
/// Per the Iceberg spec, catalogs must ensure these properties are immutable
/// after table creation. See: <https://iceberg.apache.org/docs/nightly/encryption/#catalog-security-requirements>
const IMMUTABLE_TABLE_PROPERTIES: &[&str] = &["encryption.key-id"];

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
        tracing::debug!("Applying update: '{}'", table_update_as_str(&update));
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

fn table_update_as_str(update: &TableUpdate) -> &str {
    match update {
        TableUpdate::UpgradeFormatVersion { .. } => "upgrade_format_version",
        TableUpdate::AssignUuid { .. } => "assign_uuid",
        TableUpdate::AddSchema { .. } => "add_schema",
        TableUpdate::SetCurrentSchema { .. } => "set_current_schema",
        TableUpdate::AddSpec { .. } => "add_spec",
        TableUpdate::SetDefaultSpec { .. } => "set_default_spec",
        TableUpdate::AddSortOrder { .. } => "add_sort_order",
        TableUpdate::SetDefaultSortOrder { .. } => "set_default_sort_order",
        TableUpdate::AddSnapshot { .. } => "add_snapshot",
        TableUpdate::SetSnapshotRef { .. } => "set_snapshot_ref",
        TableUpdate::RemoveSnapshots { .. } => "remove_snapshots",
        TableUpdate::RemoveSnapshotRef { .. } => "remove_snapshot_ref",
        TableUpdate::SetLocation { .. } => "set_location",
        TableUpdate::SetProperties { .. } => "set_properties",
        TableUpdate::RemoveProperties { .. } => "remove_properties",
        TableUpdate::RemovePartitionSpecs { .. } => "remove_partition_specs",
        TableUpdate::SetStatistics { .. } => "set_statistics",
        TableUpdate::RemoveStatistics { .. } => "remove_statistics",
        TableUpdate::SetPartitionStatistics { .. } => "set_partition_statistics",
        TableUpdate::RemovePartitionStatistics { .. } => "remove_partition_statistics",
        TableUpdate::RemoveSchemas { .. } => "remove_schemas",
        TableUpdate::AddEncryptionKey { .. } => "add_encryption_key",
        TableUpdate::RemoveEncryptionKey { .. } => "remove_encryption_key",
    }
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
    use std::collections::HashMap;

    use iceberg::{
        TableUpdate,
        spec::{
            FormatVersion, NestedField, PrimitiveType, Schema, SortOrder, UnboundPartitionSpec,
        },
    };
    use iceberg_ext::spec::TableMetadataBuilder;

    use super::apply_commit;

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
}
