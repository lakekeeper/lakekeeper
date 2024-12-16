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

    builder.build().map_err(|e| {
        let msg = e.message().to_string();
        ErrorModel::bad_request(msg, "TableMetadataBuildFailed", Some(Box::new(e))).into()
    })
}
