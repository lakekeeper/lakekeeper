use std::{collections::HashMap, str::FromStr as _};

use iceberg::{
    TableRequirement, TableUpdate,
    spec::{SchemaRef, TableMetadata},
};
use iceberg_ext::spec::{TableMetadataBuildResult, TableMetadataBuilder};
use lakekeeper_io::Location;

use crate::{
    server::tables::create_table::ensure_format_version_allowed,
    service::{AllowedFormatVersions, ErrorModel, IcebergErrorResponse, Result},
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

/// Reject a commit that would rebind an existing schema id to different content.
///
/// Iceberg treats a schema id as an immutable handle to a fixed set of columns, and the
/// normalized schema store diffs stored schemas by id — so silently changing the content of a
/// shared id would leave stale columns persisted (and desync any identity keyed on them).
/// Ordinary schema evolution never trips this (new content always gets a fresh id), but a commit
/// that removes a schema and adds another in the same request can make the builder recycle the
/// freed id onto different content. Compare by structural content (fields + identifier ids),
/// ignoring the id itself — mirrors iceberg's own `is_same_schema`.
pub(crate) fn ensure_schema_content_stable<'a>(
    previous: impl Iterator<Item = &'a SchemaRef>,
    new: impl Iterator<Item = &'a SchemaRef>,
) -> Result<()> {
    let previous: HashMap<i32, &SchemaRef> = previous.map(|s| (s.schema_id(), s)).collect();
    for n in new {
        let Some(p) = previous.get(&n.schema_id()) else {
            continue;
        };
        // Compare identifier fields as a SET: `identifier_field_ids()` iterates a randomized HashSet,
        // so an order-sensitive comparison would spuriously differ for the same set. Sort both.
        let (mut p_ids, mut n_ids): (Vec<i32>, Vec<i32>) = (
            p.identifier_field_ids().collect(),
            n.identifier_field_ids().collect(),
        );
        p_ids.sort_unstable();
        n_ids.sort_unstable();
        if p.as_struct() != n.as_struct() || p_ids != n_ids {
            return Err(ErrorModel::bad_request(
                format!(
                    "Commit would reassign schema id {} to different content; schema ids are immutable.",
                    n.schema_id()
                ),
                "SchemaIdContentChanged",
                None,
            )
            .into());
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
    // Snapshot the persisted schemas before the builder consumes `metadata`, to guard against a
    // commit that recycles a schema id onto different content (see `ensure_schema_content_stable`).
    let previous_schemas: Vec<SchemaRef> = metadata.schemas_iter().cloned().collect();

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
    let build_result = builder.build().map_err(|e| {
        tracing::debug!("Table metadata build failed: {}", e);
        let msg = e.message().to_string();
        IcebergErrorResponse::from(ErrorModel::conflict(
            msg,
            "CommitFailedException",
            Some(Box::new(e)),
        ))
    })?;
    ensure_schema_content_stable(
        previous_schemas.iter(),
        build_result.metadata.schemas_iter(),
    )?;
    tracing::debug!(
        "Table metadata updated, at: {}",
        build_result.metadata.last_updated_ms()
    );
    Ok(build_result)
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

    use super::{
        AllowedFormatVersions, apply_commit, ensure_format_version_upgrades_allowed,
        ensure_schema_content_stable,
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

    #[test]
    fn adding_a_new_schema_preserves_existing_content_ok() {
        // Base metadata has schema id 0 (current) = [1: id int]. Adding a structurally different
        // schema must NOT trip the stability guard — shared id 0 is unchanged.
        let metadata = test_metadata_with_properties(HashMap::new());
        let schema_b = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", iceberg::spec::Type::Primitive(PrimitiveType::Int))
                    .into(),
                NestedField::optional(
                    2,
                    "name",
                    iceberg::spec::Type::Primitive(PrimitiveType::String),
                )
                .into(),
            ])
            .build()
            .unwrap();

        let result = apply_commit(
            metadata,
            None,
            &[],
            vec![TableUpdate::AddSchema { schema: schema_b }],
        )
        .unwrap();
        // Builder assigned the new schema id 1; current stays 0.
        assert!(result.metadata.schema_by_id(1).is_some());
        assert_eq!(result.metadata.current_schema_id(), 0);
    }

    #[test]
    fn recycling_a_schema_id_onto_different_content_is_rejected() {
        // Base: schema 0 (current) = [1: id int].
        let metadata = test_metadata_with_properties(HashMap::new());
        let schema_b = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", iceberg::spec::Type::Primitive(PrimitiveType::Int))
                    .into(),
                NestedField::optional(
                    2,
                    "name",
                    iceberg::spec::Type::Primitive(PrimitiveType::String),
                )
                .into(),
            ])
            .build()
            .unwrap();
        // Add schema B: builder assigns it id 1 (highest+1), current stays 0.
        let m1 = apply_commit(
            metadata,
            None,
            &[],
            vec![TableUpdate::AddSchema { schema: schema_b }],
        )
        .unwrap()
        .metadata;
        assert!(m1.schema_by_id(1).is_some());
        assert_eq!(m1.current_schema_id(), 0);

        // In ONE commit: remove schema 1 and add a structurally different schema. Because 1 was the
        // highest (non-current) id, the builder recycles id 1 onto the new content. The normalized
        // store diffs schemas by id, so it would leave schema 1's old columns persisted — the guard
        // must reject this outright.
        let schema_c = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", iceberg::spec::Type::Primitive(PrimitiveType::Int))
                    .into(),
                NestedField::optional(
                    3,
                    "value",
                    iceberg::spec::Type::Primitive(PrimitiveType::Long),
                )
                .into(),
            ])
            .build()
            .unwrap();

        let err = apply_commit(
            m1,
            None,
            &[],
            vec![
                TableUpdate::RemoveSchemas {
                    schema_ids: vec![1],
                },
                TableUpdate::AddSchema { schema: schema_c },
            ],
        )
        .unwrap_err();

        assert_eq!(err.error.r#type, "SchemaIdContentChanged");
    }

    #[test]
    fn identical_composite_identifier_schema_is_not_flagged() {
        // Regression: identifier fields must compare as a SET. `identifier_field_ids()` iterates a
        // randomized HashSet, so an order-sensitive comparison would spuriously differ (~50% for a
        // 2-field identifier) and wrongly reject a legitimate remove-and-re-add of an identical
        // schema. Rebuild both schemas each iteration (fresh HashSet orders) and repeat so an
        // order-sensitive regression fails with overwhelming probability.
        let build = || -> iceberg::spec::SchemaRef {
            std::sync::Arc::new(
                Schema::builder()
                    .with_schema_id(1)
                    .with_identifier_field_ids(vec![1, 2])
                    .with_fields(vec![
                        NestedField::required(
                            1,
                            "a",
                            iceberg::spec::Type::Primitive(PrimitiveType::Int),
                        )
                        .into(),
                        NestedField::required(
                            2,
                            "b",
                            iceberg::spec::Type::Primitive(PrimitiveType::Int),
                        )
                        .into(),
                    ])
                    .build()
                    .unwrap(),
            )
        };
        for _ in 0..64 {
            let (p, n) = (build(), build());
            ensure_schema_content_stable(std::iter::once(&p), std::iter::once(&n))
                .expect("identical schemas with the same identifier set must not be flagged");
        }
    }
}
