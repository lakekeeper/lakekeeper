use crate::catalog::{TableIdent, TableRequirement, TableUpdate};
use crate::spec::{Schema, SortOrder, TableMetadata, UnboundPartitionSpec};

#[cfg(feature = "axum")]
use super::impl_into_response;
use super::{ErrorModel, IcebergErrorResponse};

/// Result used when a table is successfully loaded.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct LoadTableResult {
    /// May be null if the table is staged as part of a transaction
    pub metadata_location: Option<String>,
    #[serde(rename = "metadata")]
    pub metadata: TableMetadata,
    pub config: Option<std::collections::HashMap<String, String>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CreateTableRequest {
    pub name: String,
    pub location: Option<String>,
    pub schema: Schema,
    pub partition_spec: Option<UnboundPartitionSpec>,
    pub write_order: Option<SortOrder>,
    pub stage_create: Option<bool>,
    pub properties: Option<std::collections::HashMap<String, String>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct RegisterTableRequest {
    pub name: String,
    pub metadata_location: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct RenameTableRequest {
    pub source: TableIdent,
    pub destination: TableIdent,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ListTablesResponse {
    /// An opaque token that allows clients to make use of pagination for list
    /// APIs (e.g. `ListTables`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
    pub identifiers: Vec<TableIdent>,
    /// Lakekeeper IDs of the tables.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_uuids: Option<Vec<uuid::Uuid>>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CommitTableRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier: Option<TableIdent>,
    pub requirements: Vec<TableRequirement>,
    pub updates: Vec<TableUpdate>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CommitTableResponse {
    pub metadata_location: String,
    pub metadata: TableMetadata,
    pub config: Option<std::collections::HashMap<String, String>>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CommitTransactionRequest {
    pub table_changes: Vec<CommitTableRequest>,
}

#[cfg(feature = "axum")]
impl_into_response!(LoadTableResult);
#[cfg(feature = "axum")]
impl_into_response!(ListTablesResponse);
#[cfg(feature = "axum")]
impl_into_response!(CommitTableResponse);

pub trait TableRequirementExt {
    /// Assert if requirements match the given metadata.
    ///
    /// # Errors
    /// Fails if the requirements are not met.
    fn assert(&self, metadata: &TableMetadata, exists: bool) -> Result<(), IcebergErrorResponse>;
}

impl TableRequirementExt for TableRequirement {
    #[allow(clippy::too_many_lines)]
    fn assert(&self, metadata: &TableMetadata, exists: bool) -> Result<(), IcebergErrorResponse> {
        match self {
            TableRequirement::NotExist => {
                if exists {
                    return Err(ErrorModel::conflict(
                        "assert-not-exist Table Requirement violated",
                        "TableRequirementNotExist",
                        None,
                    )
                    .into());
                }
            }
            TableRequirement::UuidMatch { uuid } => {
                if &metadata.uuid() != uuid {
                    return Err(ErrorModel::conflict(
                        "assert-uuid Table Requirement violated",
                        "TableRequirementUuidMatch",
                        None,
                    )
                    .append_detail(format!("Expected: {uuid}, Found: {}", metadata.uuid()))
                    .into());
                }
            }
            TableRequirement::CurrentSchemaIdMatch { current_schema_id } => {
                // ToDo: Harmonize the types of current_schema_id
                if i64::from(metadata.current_schema_id) != *current_schema_id {
                    return Err(ErrorModel::conflict(
                        "assert-current-schema-id Table Requirement violated",
                        "TableRequirementCurrentSchemaIdMatch",
                        None,
                    )
                    .append_detail(format!(
                        "Expected: {current_schema_id}, Found: {}",
                        metadata.current_schema_id
                    ))
                    .into());
                }
            }
            TableRequirement::DefaultSortOrderIdMatch {
                default_sort_order_id,
            } => {
                if metadata.default_sort_order_id != *default_sort_order_id {
                    return Err(ErrorModel::conflict(
                        "assert-default-sort-order-id Table Requirement violated",
                        "TableRequirementDefaultSortOrderIdMatch",
                        None,
                    )
                    .append_detail(format!(
                        "Expected: {default_sort_order_id}, Found: {}",
                        metadata.default_sort_order_id
                    ))
                    .into());
                }
            }
            TableRequirement::RefSnapshotIdMatch { r#ref, snapshot_id } => {
                if let Some(snapshot_id) = snapshot_id {
                    let snapshot_ref = metadata.refs.get(r#ref).ok_or(ErrorModel::not_found(
                        "Snapshot reference not found",
                        "TableRequirementRefSnapshotIdMatch",
                        None,
                    ))?;
                    if snapshot_ref.snapshot_id != *snapshot_id {
                        return Err(ErrorModel::conflict(
                            "assert-ref-snapshot-id Branch points to different snapshot.",
                            "TableRequirementRefSnapshotIdMatch",
                            None,
                        )
                        .append_detail(format!(
                            "Expected: {snapshot_id}, Found: {}",
                            snapshot_ref.snapshot_id
                        ))
                        .into());
                    }
                } else if metadata.refs.contains_key(r#ref) {
                    return Err(ErrorModel::conflict(
                        "assert-ref-snapshot-id Branch points a snapshot.",
                        "TableRequirementRefSnapshotIdMatch",
                        None,
                    )
                    .append_detail(format!(
                        "Expected: None, Found: {}",
                        metadata.refs.get(r#ref).unwrap().snapshot_id
                    ))
                    .into());
                }
            }
            TableRequirement::DefaultSpecIdMatch { default_spec_id } => {
                // ToDo: Harmonize the types of default_spec_id
                if i64::from(metadata.default_partition_spec_id()) != *default_spec_id {
                    return Err(ErrorModel::conflict(
                        "assert-default-spec-id Table Requirement violated",
                        "TableRequirementDefaultSpecIdMatch",
                        None,
                    )
                    .append_detail(format!(
                        "Expected: {default_spec_id}, Found: {}",
                        metadata.default_partition_spec_id()
                    ))
                    .into());
                }
            }
            TableRequirement::LastAssignedPartitionIdMatch {
                last_assigned_partition_id,
            } => {
                if i64::from(metadata.last_partition_id) != *last_assigned_partition_id {
                    return Err(ErrorModel::conflict(
                        "assert-last-assigned-partition-id Table Requirement violated",
                        "TableRequirementLastAssignedPartitionIdMatch",
                        None,
                    )
                    .append_detail(format!(
                        "Expected: {last_assigned_partition_id}, Found: {}",
                        metadata.last_partition_id
                    ))
                    .into());
                }
            }
            TableRequirement::LastAssignedFieldIdMatch {
                last_assigned_field_id,
            } => {
                // ToDo: Harmonize types
                let last_column_id: i64 = metadata.last_column_id.into();
                if &last_column_id != last_assigned_field_id {
                    return Err(ErrorModel::conflict(
                        "assert-last-assigned-field-id Table Requirement violated",
                        "TableRequirementLastAssignedFieldIdMatch",
                        None,
                    )
                    .append_detail(format!(
                        "Expected: {last_assigned_field_id}, Found: {}",
                        metadata.last_column_id
                    ))
                    .into());
                }
            }
        };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_table_request_minimal() {
        let j = serde_json::json!(
        {
            "name": "tbl_name",
            "schema": {
                "schema-id": 1,
                "type" : "struct",
                "fields" : [ {
                  "id" : 1,
                  "name" : "event_count",
                  "required" : false,
                  "type" : "int",
                  "doc" : "Count of events"
                }, {
                  "id" : 2,
                  "name" : "event_date",
                  "required" : false,
                  "type" : "date"
                } ]
              }
        });

        let r: CreateTableRequest = serde_json::from_value(j).unwrap();
        assert_eq!(r.name, "tbl_name");
        assert_eq!(r.location, None);
        assert_eq!(r.schema.schema_id(), 1);
    }
}
