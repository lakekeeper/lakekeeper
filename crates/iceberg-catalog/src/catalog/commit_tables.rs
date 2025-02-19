use std::str::FromStr as _;

use iceberg::{spec::TableMetadata, TableRequirement, TableUpdate};
use iceberg_ext::{
    configs::Location,
    spec::{TableMetadataBuildResult, TableMetadataBuilder},
};

use crate::service::{ErrorModel, Result};

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
            ErrorModel::bad_request(msg, "TableMetadataBuildFailed", Some(Box::new(e))).into()
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
    }
}
