use std::{
    collections::{BTreeSet, HashMap},
    str::FromStr as _,
    sync::Arc,
};

use iceberg::{
    TableRequirement, TableUpdate,
    spec::{MAIN_BRANCH, SchemaRef, TableMetadata},
};
use iceberg_ext::{
    catalog::TableUpdateKind,
    spec::{TableMetadataBuildResult, TableMetadataBuilder},
};
use lakekeeper_io::Location;

use crate::{
    server::tables::{create_table::ensure_format_version_allowed, parse_table_property_updates},
    service::{
        AllowedFormatVersions, ErrorModel, IcebergErrorResponse, Result,
        authz::{CatalogTableAction, CommitFacts},
    },
};

/// Table properties that must not be modified or removed once set.
///
/// Per the Iceberg spec, catalogs must ensure these properties are immutable
/// after table creation. See: <https://iceberg.apache.org/docs/nightly/encryption/#catalog-security-requirements>
const IMMUTABLE_TABLE_PROPERTIES: &[&str] = &["encryption.key-id"];

/// Clock skew Iceberg tolerates between the times in table metadata.
const ONE_MINUTE_MS: i64 = 60_000;

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
///
/// `now_ms` is the server clock, read once per commit attempt; it becomes the new
/// `last-updated-ms`.
pub(super) fn apply_commit(
    metadata: TableMetadata,
    metadata_location: Option<&Location>,
    requirements: &[TableRequirement],
    updates: Vec<TableUpdate>,
    now_ms: i64,
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
    let refs_to_keep = refs_not_removed_by_name(&metadata, &updates);
    let commit_times = CommitTimes::new(&metadata, metadata_location, &updates);

    let mut builder = TableMetadataBuilder::new_from_metadata(
        metadata,
        metadata_location.map(std::string::ToString::to_string),
    )
    .set_last_updated_ms(now_ms);

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
                // Trailing slashes trimmed on both sides: the stored spelling never
                // carries one, so a client echoing back its own location with a
                // slash would otherwise be refused a commit it is entitled to.
                if location.trim_end_matches('/')
                    != previous_location.to_string().trim_end_matches('/')
                {
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
                builder = TableUpdate::apply(update, builder).map_err(update_error)?;
            }
            TableUpdate::RemoveProperties { removals } => {
                check_immutable_properties_not_removed(&previous_immutable_properties, removals)?;
                builder = TableUpdate::apply(update, builder).map_err(update_error)?;
            }
            _ => {
                builder = TableUpdate::apply(update, builder).map_err(update_error)?;
            }
        }
    }
    let build_result = builder.build().map_err(|e| {
        tracing::debug!("Table metadata build failed: {}", e);
        commit_times.build_error(e, now_ms)
    })?;
    ensure_schema_content_stable(
        previous_schemas.iter(),
        build_result.metadata.schemas_iter(),
    )?;
    ensure_refs_kept(refs_to_keep, &build_result.metadata)?;
    tracing::debug!(
        "Table metadata updated, at: {}",
        build_result.metadata.last_updated_ms()
    );
    Ok(build_result)
}

/// Answer an update the builder refused.
///
/// A retryable error means the update was built on outdated table metadata, for example a
/// sequence number another branch already used: the client must reload and retry, so 409.
fn update_error(e: iceberg::Error) -> IcebergErrorResponse {
    let msg = e.message().to_string();
    if e.retryable() {
        ErrorModel::conflict(msg, "CommitFailedException", Some(Box::new(e))).into()
    } else {
        ErrorModel::bad_request(msg, "InvalidTableUpdate", Some(Box::new(e))).into()
    }
}

/// The times, in ms, that tell whether a failed build is a clock problem.
struct CommitTimes {
    /// The previous `last-updated-ms`, which becomes the last metadata-log entry.
    previous_last_updated: Option<i64>,
    /// The last snapshot-log entry before the commit.
    last_logged: Option<i64>,
    /// The `main` snapshot-log entries the commit appends, in order: the time of a snapshot the
    /// commit adds, or `None` for an existing snapshot, which is logged at the server clock.
    new_main_entries: Vec<Option<i64>>,
}

impl CommitTimes {
    fn new(
        metadata: &TableMetadata,
        metadata_location: Option<&Location>,
        updates: &[TableUpdate],
    ) -> Self {
        let mut added = HashMap::new();
        let mut new_main_entries = Vec::new();
        for update in updates {
            match update {
                TableUpdate::AddSnapshot { snapshot } => {
                    added.insert(snapshot.snapshot_id(), snapshot.timestamp_ms());
                }
                TableUpdate::SetSnapshotRef {
                    ref_name,
                    reference,
                } if ref_name == MAIN_BRANCH => {
                    new_main_entries.push(added.get(&reference.snapshot_id).copied());
                }
                _ => {}
            }
        }
        Self {
            previous_last_updated: metadata_location.map(|_| metadata.last_updated_ms()),
            last_logged: metadata.history().last().map(|entry| entry.timestamp_ms),
            new_main_entries,
        }
    }

    /// Answer a failed build.
    ///
    /// The builder refuses metadata whose logs step back by more than one minute, or whose
    /// `last-updated-ms`, the server clock, is more than one minute before the last entry of
    /// either log. Such a build fails until the clocks agree. Iceberg Java refuses the same
    /// metadata with an `IllegalArgumentException`, which its reference server answers with 400.
    /// Every other build error is a conflict.
    fn build_error(&self, e: iceberg::Error, now_ms: i64) -> IcebergErrorResponse {
        let message = e.message().to_string();
        let source = Some(Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>);
        let main_entries = self
            .new_main_entries
            .iter()
            .map(|entry| entry.unwrap_or(now_ms));
        let ahead = self
            .previous_last_updated
            .into_iter()
            .chain(self.last_logged)
            .chain(main_entries.clone())
            .max()
            .filter(|latest| latest.saturating_sub(now_ms) > ONE_MINUTE_MS);
        if let Some(latest) = ahead {
            return ErrorModel::bad_request(
                format!(
                    "The table or this commit holds the time {latest} ms, which is more than one minute after the server clock ({now_ms} ms). Synchronize the clocks of Lakekeeper and its clients."
                ),
                "TimestampAheadOfServerClock",
                source,
            )
            .into();
        }
        let mut latest = self.last_logged;
        for entry in main_entries {
            if let Some(earlier) = latest
                && earlier.saturating_sub(entry) > ONE_MINUTE_MS
            {
                return ErrorModel::bad_request(
                    format!(
                        "The new main snapshot has the time {entry} ms, which is more than one minute before the table's snapshot log ({earlier} ms). Synchronize the clocks of Lakekeeper and its clients."
                    ),
                    "TimestampBeforeTableHistory",
                    source,
                )
                .into();
            }
            latest = latest.max(Some(entry));
        }
        ErrorModel::conflict(message, "CommitFailedException", source).into()
    }
}

/// Names of the refs of `metadata` that no `RemoveSnapshotRef` in `updates` removes.
fn refs_not_removed_by_name(metadata: &TableMetadata, updates: &[TableUpdate]) -> Vec<String> {
    let removed_by_name: BTreeSet<&str> = updates
        .iter()
        .filter_map(|update| match update {
            TableUpdate::RemoveSnapshotRef { ref_name } => Some(ref_name.as_str()),
            _ => None,
        })
        .collect();
    metadata
        .refs()
        .keys()
        .filter(|name| !removed_by_name.contains(name.as_str()))
        .cloned()
        .collect()
}

/// Refuse a commit that drops a ref without removing it by name.
///
/// `RemoveSnapshots` silently drops every ref that points to a removed snapshot. A ref created
/// or moved after the client planned the removal would be lost, and the client's file cleanup
/// would delete files only its snapshot uses, so the client must reload and plan again.
fn ensure_refs_kept(refs_to_keep: Vec<String>, new_metadata: &TableMetadata) -> Result<()> {
    let dropped: Vec<String> = refs_to_keep
        .into_iter()
        .filter(|name| !new_metadata.refs().contains_key(name))
        .collect();
    if dropped.is_empty() {
        return Ok(());
    }
    Err(ErrorModel::conflict(
        format!(
            "Cannot remove snapshots that the refs [{}] point to unless the commit also removes these refs. Reload the table and retry.",
            dropped.join(", ")
        ),
        "CommitFailedException",
        None,
    )
    .into())
}

/// The action to authorize for one table's commit request, built from its updates.
///
/// The only place that marks an action's `update_kinds` as listing every update of a commit
/// request; see [`CommitFacts`].
pub(crate) fn commit_action(updates: &[TableUpdate]) -> CatalogTableAction {
    let (updated_properties, removed_properties) = parse_table_property_updates(updates);
    CatalogTableAction::Commit {
        updated_properties: Arc::new(updated_properties),
        removed_properties: Arc::new(removed_properties),
        target_refs: Arc::new(refs_from_updates(updates)),
        update_kinds: Arc::new(update_kinds(updates)),
        commit_facts: CommitFacts::of_request(),
    }
}

/// Collect the branch/tag ref names a commit explicitly writes, from its
/// `SetSnapshotRef` / `RemoveSnapshotRef` updates.
///
/// A `RemoveSnapshots` update names no ref. [`apply_commit`] refuses one that would
/// drop a ref the commit does not remove by name, and [`update_kinds`] lets a policy
/// require table-wide authority for it.
fn refs_from_updates(updates: &[TableUpdate]) -> BTreeSet<String> {
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
fn update_kinds(updates: &[TableUpdate]) -> BTreeSet<TableUpdateKind> {
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
    use std::{
        collections::{BTreeSet, HashMap},
        sync::Arc,
    };

    use iceberg::{
        TableRequirement, TableUpdate,
        spec::{
            FormatVersion, MAIN_BRANCH, MetadataLog, NestedField, Operation, PrimitiveType, Schema,
            Snapshot, SnapshotLog, SnapshotReference, SnapshotRetention, SortOrder, Summary,
            TableMetadata, UnboundPartitionSpec,
        },
    };
    use iceberg_ext::{catalog::TableUpdateKind, spec::TableMetadataBuilder};

    use super::{
        AllowedFormatVersions, apply_commit, commit_action, ensure_format_version_upgrades_allowed,
        ensure_schema_content_stable, refs_from_updates, update_kinds,
    };
    use crate::service::authz::{CatalogTableAction, CommitFacts};

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

    fn now_ms() -> i64 {
        chrono::Utc::now().timestamp_millis()
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

    /// A `set-location` naming the table's own location, with a trailing slash, is
    /// not a relocation and must not be refused.
    ///
    /// The stored spelling never carries a trailing slash, so comparing raw strings
    /// rejected a commit the client was entitled to make -- permanently, for any
    /// client that appends one.
    #[test]
    fn set_location_differing_only_by_a_trailing_slash_is_accepted() {
        let metadata = test_metadata_with_properties(HashMap::new());
        let result = apply_commit(
            metadata,
            None,
            &[],
            vec![TableUpdate::SetLocation {
                location: "s3://bucket/table/".to_string(),
            }],
            now_ms(),
        );
        assert!(
            result.is_ok(),
            "the table's own location was refused: {:?}",
            result.err()
        );
    }

    /// Moving a table to a genuinely different location is still refused.
    #[test]
    fn set_location_to_a_different_location_is_refused() {
        let metadata = test_metadata_with_properties(HashMap::new());
        let result = apply_commit(
            metadata,
            None,
            &[],
            vec![TableUpdate::SetLocation {
                location: "s3://bucket/elsewhere".to_string(),
            }],
            now_ms(),
        );
        assert!(result.is_err(), "a real relocation was accepted");
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
        // `RemoveSnapshots` names no ref; `apply_commit` refuses one that drops a ref.
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

    /// The action of a commit request without updates lists no update kinds, and lists
    /// them as every update of the request.
    #[test]
    fn commit_action_of_a_request_without_updates() {
        assert_eq!(
            commit_action(&[]),
            CatalogTableAction::Commit {
                updated_properties: Arc::default(),
                removed_properties: Arc::default(),
                target_refs: Arc::default(),
                update_kinds: Arc::default(),
                commit_facts: CommitFacts::of_request(),
            }
        );
    }

    #[test]
    fn commit_action_lists_properties_refs_and_kinds() {
        let action = commit_action(&[
            TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
                reference: branch_ref(1),
            },
            TableUpdate::SetProperties {
                updates: HashMap::from([("owner".to_string(), "team".to_string())]),
            },
            TableUpdate::RemoveProperties {
                removals: vec!["comment".to_string()],
            },
        ]);
        assert_eq!(
            action,
            CatalogTableAction::Commit {
                updated_properties: Arc::new([("owner".to_string(), "team".to_string())].into()),
                removed_properties: Arc::new(vec!["comment".to_string()]),
                target_refs: Arc::new(BTreeSet::from([MAIN_BRANCH.to_string()])),
                update_kinds: Arc::new(BTreeSet::from([
                    TableUpdateKind::SetSnapshotRef,
                    TableUpdateKind::SetProperties,
                    TableUpdateKind::RemoveProperties,
                ])),
                commit_facts: CommitFacts::of_request(),
            }
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
            now_ms(),
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
            now_ms(),
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
            now_ms(),
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
            now_ms(),
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
            now_ms(),
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
            now_ms(),
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
            now_ms(),
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
            now_ms(),
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
            now_ms(),
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
            Arc::new(
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

    fn tag_ref(snapshot_id: i64) -> SnapshotReference {
        SnapshotReference {
            snapshot_id,
            retention: SnapshotRetention::Tag {
                max_ref_age_ms: None,
            },
        }
    }

    /// Snapshots 1 -> 2 -> 3 with `main` at 3 and the tag `keep` at 2.
    fn metadata_with_tagged_snapshot() -> TableMetadata {
        let mut metadata = test_metadata_with_properties(HashMap::new());
        for snapshot_id in 1..=3 {
            let snapshot = Snapshot::builder()
                .with_snapshot_id(snapshot_id)
                .with_parent_snapshot_id((snapshot_id > 1).then_some(snapshot_id - 1))
                .with_timestamp_ms(metadata.last_updated_ms() + 1)
                .with_sequence_number(snapshot_id)
                .with_schema_id(0)
                .with_manifest_list(format!(
                    "s3://bucket/table/metadata/snap-{snapshot_id}.avro"
                ))
                .with_summary(Summary {
                    operation: Operation::Append,
                    additional_properties: HashMap::new(),
                })
                .build();
            metadata = apply_commit(
                metadata,
                None,
                &[],
                vec![
                    TableUpdate::AddSnapshot { snapshot },
                    TableUpdate::SetSnapshotRef {
                        ref_name: MAIN_BRANCH.to_string(),
                        reference: branch_ref(snapshot_id),
                    },
                ],
                now_ms(),
            )
            .unwrap()
            .metadata;
        }
        apply_commit(
            metadata,
            None,
            &[],
            vec![TableUpdate::SetSnapshotRef {
                ref_name: "keep".to_string(),
                reference: tag_ref(2),
            }],
            now_ms(),
        )
        .unwrap()
        .metadata
    }

    /// A commit may drop a ref only by removing it by name; a ref that `RemoveSnapshots` would
    /// drop silently, for example one created after the client planned the removal, fails the
    /// commit with a conflict so the client plans again.
    #[test]
    fn removing_snapshots_never_drops_a_ref_silently() {
        // Refs (name, snapshot id) after the commit, or the error type.
        type Expected = Result<Vec<(&'static str, i64)>, &'static str>;
        let remove = |ids: &[i64]| TableUpdate::RemoveSnapshots {
            snapshot_ids: ids.to_vec(),
        };
        let remove_keep = || TableUpdate::RemoveSnapshotRef {
            ref_name: "keep".to_string(),
        };
        let cases: Vec<(&str, Vec<TableUpdate>, Expected)> = vec![
            (
                "unreferenced snapshot",
                vec![remove(&[1])],
                Ok(vec![("keep", 2), ("main", 3)]),
            ),
            (
                "tagged snapshot",
                vec![remove(&[1, 2])],
                Err("CommitFailedException"),
            ),
            (
                "tag removed by name before",
                vec![remove_keep(), remove(&[1, 2])],
                Ok(vec![("main", 3)]),
            ),
            (
                "tag removed by name after",
                vec![remove(&[1, 2]), remove_keep()],
                Ok(vec![("main", 3)]),
            ),
            (
                "tag moved away",
                vec![
                    TableUpdate::SetSnapshotRef {
                        ref_name: "keep".to_string(),
                        reference: tag_ref(3),
                    },
                    remove(&[1, 2]),
                ],
                Ok(vec![("keep", 3), ("main", 3)]),
            ),
        ];
        for (case, updates, expected) in cases {
            let result = apply_commit(
                metadata_with_tagged_snapshot(),
                None,
                &[],
                updates,
                now_ms(),
            );
            match (result, expected) {
                (Ok(result), Ok(expected_refs)) => {
                    let mut refs: Vec<(&str, i64)> = result
                        .metadata
                        .refs()
                        .iter()
                        .map(|(name, r)| (name.as_str(), r.snapshot_id))
                        .collect();
                    refs.sort_unstable();
                    assert_eq!(refs, expected_refs, "{case}");
                }
                (Err(e), Err(expected_type)) => {
                    assert_eq!(e.error.code, 409, "{case}");
                    assert_eq!(e.error.r#type, expected_type, "{case}");
                }
                (result, expected) => {
                    panic!(
                        "{case}: expected {expected:?}, got {:?}",
                        result.map(|_| ())
                    );
                }
            }
        }
    }

    const HOUR_MS: i64 = 60 * 60 * 1000;

    /// Location of the stored metadata file a commit replaces.
    fn previous_file() -> lakekeeper_io::Location {
        "s3://bucket/table/metadata/00000-previous.metadata.json"
            .parse()
            .unwrap()
    }

    fn snapshot_at(
        snapshot_id: i64,
        parent: Option<i64>,
        sequence_number: i64,
        timestamp_ms: i64,
    ) -> Snapshot {
        Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(parent)
            .with_timestamp_ms(timestamp_ms)
            .with_sequence_number(sequence_number)
            .with_schema_id(0)
            .with_manifest_list(format!(
                "s3://bucket/table/metadata/snap-{snapshot_id}.avro"
            ))
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build()
    }

    fn set_property(value: &str) -> TableUpdate {
        TableUpdate::SetProperties {
            updates: HashMap::from([("k".to_string(), value.to_string())]),
        }
    }

    /// Commit to a stored table with the server clock at `now`; the error's code and type.
    fn commit_at(
        metadata: TableMetadata,
        updates: Vec<TableUpdate>,
        now: i64,
    ) -> Result<TableMetadata, (u16, String)> {
        let previous_file = previous_file();
        apply_commit(metadata, Some(&previous_file), &[], updates, now)
            .map(|result| result.metadata)
            .map_err(|e| (e.error.code, e.error.r#type))
    }

    /// `metadata` as a client parses it.
    fn reparse(metadata: &TableMetadata) -> TableMetadata {
        serde_json::from_value(serde_json::to_value(metadata).unwrap())
            .expect("clients must be able to load the stored metadata")
    }

    /// A table with snapshot 1 on `main`, committed with the server clock at the returned time.
    fn table_with_main_snapshot() -> (TableMetadata, i64) {
        let base = test_metadata_with_properties(HashMap::new());
        let now = base.last_updated_ms() + 1000;
        let metadata = commit_at(
            base,
            vec![
                TableUpdate::AddSnapshot {
                    snapshot: snapshot_at(1, None, 1, now - 5),
                },
                TableUpdate::SetSnapshotRef {
                    ref_name: MAIN_BRANCH.to_string(),
                    reference: branch_ref(1),
                },
            ],
            now,
        )
        .unwrap();
        (metadata, now)
    }

    #[test]
    fn last_updated_ms_is_the_server_clock() {
        let (metadata, now) = table_with_main_snapshot();
        assert_eq!(metadata.last_updated_ms(), now);
        assert_eq!(metadata.snapshot_by_id(1).unwrap().timestamp_ms(), now - 5);
        assert_eq!(
            metadata.history(),
            &[SnapshotLog {
                snapshot_id: 1,
                timestamp_ms: now - 5,
            }]
        );

        let metadata = commit_at(metadata, vec![set_property("v")], now + 10).unwrap();
        assert_eq!(metadata.last_updated_ms(), now + 10);
        assert_eq!(
            metadata.metadata_log().last(),
            Some(&MetadataLog {
                metadata_file: previous_file().to_string(),
                timestamp_ms: now,
            })
        );
    }

    /// Updates that leave the metadata as it is record no change, which is how a commit that
    /// changes nothing is told apart. Setting a property to its current value records one.
    #[test]
    fn updates_that_change_nothing_record_no_change() {
        let (base, now) = table_with_main_snapshot();
        let metadata = commit_at(base, vec![set_property("v")], now + 10).unwrap();
        let changes = |updates: Vec<TableUpdate>| {
            apply_commit(
                metadata.clone(),
                Some(&previous_file()),
                &[TableRequirement::UuidMatch {
                    uuid: metadata.uuid(),
                }],
                updates,
                now + 20,
            )
            .unwrap()
            .changes
        };

        assert_eq!(changes(vec![]), vec![]);
        assert_eq!(
            changes(vec![
                TableUpdate::AssignUuid {
                    uuid: metadata.uuid(),
                },
                TableUpdate::UpgradeFormatVersion {
                    format_version: metadata.format_version(),
                },
                TableUpdate::SetLocation {
                    location: metadata.location().to_string(),
                },
                TableUpdate::SetSnapshotRef {
                    ref_name: MAIN_BRANCH.to_string(),
                    reference: branch_ref(1),
                },
                TableUpdate::RemoveStatistics { snapshot_id: 1 },
                TableUpdate::RemovePartitionStatistics { snapshot_id: 1 },
                TableUpdate::RemoveSnapshots {
                    snapshot_ids: vec![42],
                },
                TableUpdate::RemoveSnapshotRef {
                    ref_name: "absent".to_string(),
                },
            ]),
            vec![]
        );
        assert_eq!(changes(vec![set_property("v")]), vec![set_property("v")]);
    }

    /// A client clock two hours ahead on a branch or a staged (WAP) snapshot moves no
    /// table time, so every following commit stores metadata that clients load.
    #[test]
    fn fast_client_on_a_branch_or_staged_snapshot_keeps_the_table_loadable() {
        let (base, now) = table_with_main_snapshot();
        let fast = now + 2 * HOUR_MS;
        let branch_b = || TableUpdate::SetSnapshotRef {
            ref_name: "b".to_string(),
            reference: branch_ref(2),
        };

        let on_branch = commit_at(
            base.clone(),
            vec![
                TableUpdate::AddSnapshot {
                    snapshot: snapshot_at(2, Some(1), 2, fast),
                },
                branch_b(),
            ],
            now + 1000,
        )
        .unwrap();
        assert_eq!(on_branch.last_updated_ms(), now + 1000);
        assert_eq!(on_branch.snapshot_by_id(2).unwrap().timestamp_ms(), fast);
        let staged = commit_at(
            base,
            vec![TableUpdate::AddSnapshot {
                snapshot: snapshot_at(2, Some(1), 2, fast),
            }],
            now + 1000,
        )
        .unwrap();
        assert_eq!(staged.last_updated_ms(), now + 1000);

        let remove_snapshot_2 = || TableUpdate::RemoveSnapshots {
            snapshot_ids: vec![2],
        };
        let remove_branch_b = || TableUpdate::RemoveSnapshotRef {
            ref_name: "b".to_string(),
        };
        let sequences: Vec<(&str, TableMetadata, Vec<Vec<TableUpdate>>)> = vec![
            (
                "branch, then property, then expire",
                on_branch,
                vec![
                    vec![set_property("v")],
                    vec![remove_branch_b(), remove_snapshot_2()],
                ],
            ),
            (
                "staged, then property, then publish",
                staged.clone(),
                vec![
                    vec![set_property("v")],
                    vec![TableUpdate::SetSnapshotRef {
                        ref_name: MAIN_BRANCH.to_string(),
                        reference: branch_ref(2),
                    }],
                ],
            ),
            (
                "staged, then expire, then property",
                staged,
                vec![vec![remove_snapshot_2()], vec![set_property("v")]],
            ),
        ];
        for (case, mut metadata, commits) in sequences {
            let mut clock = now + 1000;
            for updates in commits {
                clock += 1000;
                metadata = commit_at(metadata, updates, clock).unwrap();
                assert_eq!(metadata.last_updated_ms(), clock, "{case}");
                assert_eq!(reparse(&metadata), metadata, "{case}");
            }
            // A client with a correct clock commits next.
            clock += 1000;
            let parent = metadata.current_snapshot_id();
            let next = commit_at(
                metadata.clone(),
                vec![
                    TableUpdate::AddSnapshot {
                        snapshot: snapshot_at(
                            3,
                            parent,
                            metadata.last_sequence_number() + 1,
                            clock - 5,
                        ),
                    },
                    TableUpdate::SetSnapshotRef {
                        ref_name: MAIN_BRANCH.to_string(),
                        reference: branch_ref(3),
                    },
                ],
                clock,
            )
            .unwrap();
            assert_eq!(reparse(&next), next, "{case}");
        }
    }

    /// A snapshot that becomes `main`'s current one with a time more than a minute after the
    /// server clock would log a time after `last-updated-ms`: refused with 400, as Java does.
    #[test]
    fn fast_client_on_main_is_refused() {
        let (base, now) = table_with_main_snapshot();
        let commit_main = |timestamp_ms: i64| {
            commit_at(
                base.clone(),
                vec![
                    TableUpdate::AddSnapshot {
                        snapshot: snapshot_at(2, Some(1), 2, timestamp_ms),
                    },
                    TableUpdate::SetSnapshotRef {
                        ref_name: MAIN_BRANCH.to_string(),
                        reference: branch_ref(2),
                    },
                ],
                now + 1000,
            )
        };
        assert_eq!(
            commit_main(now + 2 * HOUR_MS).unwrap_err(),
            (400, "TimestampAheadOfServerClock".to_string())
        );
        let within_tolerance = commit_main(now + 1000 + 30_000).unwrap();
        assert_eq!(within_tolerance.last_updated_ms(), now + 1000);
        assert_eq!(reparse(&within_tolerance), within_tolerance);
    }

    fn add_main_snapshot(snapshot: Snapshot) -> Vec<TableUpdate> {
        let snapshot_id = snapshot.snapshot_id();
        vec![
            TableUpdate::AddSnapshot { snapshot },
            TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
                reference: branch_ref(snapshot_id),
            },
        ]
    }

    /// Writers whose clock is behind Lakekeeper's commit as with Iceberg Java's REST catalog:
    /// on any branch, and as the only writer on `main`.
    #[test]
    fn slow_client_commits() {
        let (base, now) = table_with_main_snapshot();
        let behind = commit_at(
            base.clone(),
            add_main_snapshot(snapshot_at(2, Some(1), 2, now - 30_000)),
            now + 1000,
        )
        .unwrap();
        assert_eq!(behind.last_updated_ms(), now + 1000);
        assert_eq!(
            behind.snapshot_by_id(2).unwrap().timestamp_ms(),
            now - 30_000
        );
        assert_eq!(reparse(&behind), behind);

        // Two minutes slow on a branch, after a correct `main` snapshot, then a property change
        // and a correct `main` snapshot.
        let slow_ms = now + 1000 - 2 * 60_000;
        let mut metadata = commit_at(
            base,
            vec![
                TableUpdate::AddSnapshot {
                    snapshot: snapshot_at(2, Some(1), 2, slow_ms),
                },
                TableUpdate::SetSnapshotRef {
                    ref_name: "b".to_string(),
                    reference: branch_ref(2),
                },
            ],
            now + 1000,
        )
        .unwrap();
        assert_eq!(metadata.last_updated_ms(), now + 1000);
        assert_eq!(reparse(&metadata), metadata);
        metadata = commit_at(metadata, vec![set_property("v")], now + 2000).unwrap();
        assert_eq!(reparse(&metadata), metadata);
        metadata = commit_at(
            metadata,
            add_main_snapshot(snapshot_at(3, Some(1), 3, now + 2995)),
            now + 3000,
        )
        .unwrap();
        assert_eq!(reparse(&metadata), metadata);

        // The only writer on `main`, always two minutes slow.
        let created = test_metadata_with_properties(HashMap::new());
        let clock = created.last_updated_ms() + 1000;
        let first = commit_at(
            created,
            add_main_snapshot(snapshot_at(1, None, 1, clock - 2 * 60_000)),
            clock,
        )
        .unwrap();
        let second = commit_at(
            first,
            add_main_snapshot(snapshot_at(2, Some(1), 2, clock + 10 - 2 * 60_000)),
            clock + 10,
        )
        .unwrap();
        assert_eq!(second.last_updated_ms(), clock + 10);
        assert_eq!(
            second.history(),
            &[
                SnapshotLog {
                    snapshot_id: 1,
                    timestamp_ms: clock - 2 * 60_000,
                },
                SnapshotLog {
                    snapshot_id: 2,
                    timestamp_ms: clock + 10 - 2 * 60_000,
                },
            ]
        );
        assert_eq!(reparse(&second), second);
    }

    /// A `main` snapshot more than a minute before the table's last snapshot-log entry would
    /// leave the snapshot log out of order: refused with 400, as Java does.
    #[test]
    fn slow_client_after_a_correct_main_snapshot_is_refused() {
        let (base, now) = table_with_main_snapshot();
        assert_eq!(
            commit_at(
                base,
                add_main_snapshot(snapshot_at(2, Some(1), 2, now + 1000 - 2 * 60_000)),
                now + 1000,
            )
            .unwrap_err(),
            (400, "TimestampBeforeTableHistory".to_string())
        );
    }

    /// A `main` snapshot-log entry more than a minute after this replica's clock, accepted by a
    /// replica whose clock is ahead, would log a time after `last-updated-ms`: refused with 400,
    /// as Java does.
    #[test]
    fn snapshot_log_ahead_of_a_lagging_replica_is_refused() {
        let (base, now) = table_with_main_snapshot();
        let metadata = commit_at(
            base,
            add_main_snapshot(snapshot_at(2, Some(1), 2, now + 59_000)),
            now + 1000,
        )
        .unwrap();
        let lagging = now - 29_000;
        for (commit, updates) in [
            ("property change", vec![set_property("v")]),
            (
                "branch snapshot",
                vec![
                    TableUpdate::AddSnapshot {
                        snapshot: snapshot_at(3, Some(2), 3, lagging),
                    },
                    TableUpdate::SetSnapshotRef {
                        ref_name: "b".to_string(),
                        reference: branch_ref(3),
                    },
                ],
            ),
            (
                "main snapshot",
                add_main_snapshot(snapshot_at(3, Some(2), 3, lagging)),
            ),
            (
                "main rollback",
                vec![TableUpdate::SetSnapshotRef {
                    ref_name: MAIN_BRANCH.to_string(),
                    reference: branch_ref(1),
                }],
            ),
        ] {
            assert_eq!(
                commit_at(metadata.clone(), updates, lagging).unwrap_err(),
                (400, "TimestampAheadOfServerClock".to_string()),
                "{commit}"
            );
        }
    }

    /// A commit that moves `main` to an existing snapshot, logged at the server clock, and then
    /// adds a `main` snapshot more than a minute older leaves the snapshot log out of order:
    /// refused with 400, as Java does.
    #[test]
    fn main_snapshot_before_a_rollback_in_the_same_commit_is_refused() {
        let (base, now) = table_with_main_snapshot();
        let with_branch = commit_at(
            base,
            vec![
                TableUpdate::AddSnapshot {
                    snapshot: snapshot_at(2, Some(1), 2, now + 995),
                },
                TableUpdate::SetSnapshotRef {
                    ref_name: "b".to_string(),
                    reference: branch_ref(2),
                },
            ],
            now + 1000,
        )
        .unwrap();
        let clock = now + 5 * 60_000;
        let mut updates = vec![TableUpdate::SetSnapshotRef {
            ref_name: MAIN_BRANCH.to_string(),
            reference: branch_ref(2),
        }];
        updates.extend(add_main_snapshot(snapshot_at(
            3,
            Some(2),
            3,
            clock - 2 * 60_000,
        )));
        assert_eq!(
            commit_at(with_branch, updates, clock).unwrap_err(),
            (400, "TimestampBeforeTableHistory".to_string())
        );
    }

    /// A build error that is not about times stays a conflict, for example a parentless snapshot
    /// below the sequence number of a retained snapshot (lakekeeper/lakekeeper#949).
    #[test]
    fn build_error_without_a_time_problem_is_a_conflict() {
        let (base, now) = table_with_main_snapshot();
        let metadata = commit_at(
            base,
            add_main_snapshot(snapshot_at(2, Some(1), 2, now + 995)),
            now + 1000,
        )
        .unwrap();
        assert_eq!(
            commit_at(
                metadata,
                add_main_snapshot(snapshot_at(3, None, 1, now + 1995)),
                now + 2000,
            )
            .unwrap_err(),
            (409, "CommitFailedException".to_string())
        );
    }

    /// A request that adds a snapshot on another branch and moves `main` to an existing snapshot
    /// logs the server clock for `main`, as Java 1.11 does, whatever the branch snapshot's time.
    #[test]
    fn main_moved_to_an_existing_snapshot_logs_the_server_clock() {
        let created = test_metadata_with_properties(HashMap::new());
        let server = created.last_updated_ms() + 1000;
        let mut base = created;
        for (tick, snapshot_id) in [(1, 1), (2, 2)] {
            let clock = server + tick * 10 * 60_000;
            base = commit_at(
                base,
                add_main_snapshot(snapshot_at(
                    snapshot_id,
                    (snapshot_id > 1).then_some(snapshot_id - 1),
                    snapshot_id,
                    clock,
                )),
                clock,
            )
            .unwrap();
        }
        let now = server + 3 * 10 * 60_000;
        // The snapshot on `b` is two minutes fast, or five minutes slow.
        for offset in [2 * 60_000, -5 * 60_000] {
            let metadata = commit_at(
                base.clone(),
                vec![
                    TableUpdate::AddSnapshot {
                        snapshot: snapshot_at(3, Some(2), 3, now + offset),
                    },
                    TableUpdate::SetSnapshotRef {
                        ref_name: "b".to_string(),
                        reference: branch_ref(3),
                    },
                    TableUpdate::SetSnapshotRef {
                        ref_name: MAIN_BRANCH.to_string(),
                        reference: branch_ref(1),
                    },
                ],
                now,
            )
            .unwrap();
            assert_eq!(metadata.last_updated_ms(), now, "{offset}");
            assert_eq!(
                metadata.history().last(),
                Some(&SnapshotLog {
                    snapshot_id: 1,
                    timestamp_ms: now,
                }),
                "{offset}"
            );
            assert_eq!(reparse(&metadata), metadata, "{offset}");
        }
    }

    /// Stored times more than a minute ahead of the server clock, for example from registered
    /// metadata, would put a later entry in the metadata log than the new `last-updated-ms`:
    /// refused with 400, and the stored metadata stays as it is.
    #[test]
    fn table_times_ahead_of_the_server_clock_are_refused() {
        let base = test_metadata_with_properties(HashMap::new());
        let now = base.last_updated_ms() + 1000;
        let ahead = now + 2 * HOUR_MS;
        let last_updated_ahead =
            commit_at(base.clone(), vec![set_property("ahead")], ahead).unwrap();
        let all_times_ahead = commit_at(
            base.clone(),
            vec![
                TableUpdate::AddSnapshot {
                    snapshot: snapshot_at(1, None, 1, ahead),
                },
                TableUpdate::SetSnapshotRef {
                    ref_name: MAIN_BRANCH.to_string(),
                    reference: branch_ref(1),
                },
            ],
            ahead,
        )
        .unwrap();
        let commits = || {
            vec![
                ("property change", vec![set_property("v")]),
                (
                    "main snapshot",
                    add_main_snapshot(snapshot_at(2, Some(1), 2, now - 5)),
                ),
                (
                    "branch snapshot",
                    vec![
                        TableUpdate::AddSnapshot {
                            snapshot: snapshot_at(2, Some(1), 2, now - 5),
                        },
                        TableUpdate::SetSnapshotRef {
                            ref_name: "b".to_string(),
                            reference: branch_ref(2),
                        },
                    ],
                ),
                (
                    "staged snapshot",
                    vec![TableUpdate::AddSnapshot {
                        snapshot: snapshot_at(2, Some(1), 2, now - 5),
                    }],
                ),
            ]
        };
        for (table, metadata) in [
            ("last-updated-ms ahead", last_updated_ahead),
            ("all times ahead", all_times_ahead),
        ] {
            for (commit, updates) in commits() {
                assert_eq!(
                    commit_at(metadata.clone(), updates, now).unwrap_err(),
                    (400, "TimestampAheadOfServerClock".to_string()),
                    "{table}, {commit}"
                );
            }
        }

        let within_tolerance = commit_at(base, vec![set_property("ahead")], now + 30_000).unwrap();
        let metadata = commit_at(within_tolerance, vec![set_property("v")], now).unwrap();
        assert_eq!(metadata.last_updated_ms(), now);
        assert_eq!(reparse(&metadata), metadata);
    }

    /// A snapshot whose time is after the table's history.
    fn snapshot(
        metadata: &TableMetadata,
        snapshot_id: i64,
        parent: Option<i64>,
        sequence_number: i64,
        row_range: Option<(u64, u64)>,
    ) -> Snapshot {
        let builder = Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(parent)
            .with_timestamp_ms(metadata.last_updated_ms() + 1)
            .with_sequence_number(sequence_number)
            .with_schema_id(0)
            .with_manifest_list(format!(
                "s3://bucket/table/metadata/snap-{snapshot_id}.avro"
            ))
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            });
        match row_range {
            Some((first_row_id, added_rows)) => {
                builder.with_row_range(first_row_id, added_rows).build()
            }
            None => builder.build(),
        }
    }

    fn add_snapshot(snapshot: Snapshot) -> TableUpdate {
        TableUpdate::AddSnapshot { snapshot }
    }

    fn set_branch(ref_name: &str, snapshot_id: i64) -> TableUpdate {
        TableUpdate::SetSnapshotRef {
            ref_name: ref_name.to_string(),
            reference: branch_ref(snapshot_id),
        }
    }

    fn commit(metadata: TableMetadata, updates: Vec<TableUpdate>) -> TableMetadata {
        apply_commit(metadata, None, &[], updates, now_ms())
            .unwrap()
            .metadata
    }

    /// Snapshot 1 on `main` and its child 2 on the branch `audit`; `last-sequence-number` is 2.
    /// On v3, `next-row-id` is 15.
    fn metadata_with_branch_commit(format_version: FormatVersion) -> TableMetadata {
        let mut metadata = test_metadata_with_properties(HashMap::new());
        let v3 = format_version == FormatVersion::V3;
        if v3 {
            metadata = commit(
                metadata,
                vec![TableUpdate::UpgradeFormatVersion { format_version }],
            );
        }
        let first = snapshot(&metadata, 1, None, 1, v3.then_some((0, 10)));
        metadata = commit(
            metadata,
            vec![add_snapshot(first), set_branch(MAIN_BRANCH, 1)],
        );
        let second = snapshot(&metadata, 2, Some(1), 2, v3.then_some((10, 5)));
        commit(metadata, vec![add_snapshot(second), set_branch("audit", 2)])
    }

    fn assert_error(result: super::Result<impl std::fmt::Debug>, code: u16, r#type: &str) {
        let error = result.expect_err("the commit must be refused").error;
        assert_eq!(
            (error.code, error.r#type.as_str()),
            (code, r#type),
            "{error:?}"
        );
    }

    /// A snapshot computed from metadata that another branch's commit has since outdated is a
    /// conflict: the client reloads and its retry commits.
    #[test]
    fn stale_sequence_number_after_a_branch_commit_is_a_conflict() {
        let base = metadata_with_branch_commit(FormatVersion::V2);
        let main_at_1 = [TableRequirement::RefSnapshotIdMatch {
            r#ref: MAIN_BRANCH.to_string(),
            snapshot_id: Some(1),
        }];

        let stored: lakekeeper_io::Location = "s3://bucket/table/metadata/00002.metadata.json"
            .parse()
            .unwrap();

        let stale = snapshot(&base, 3, Some(1), 2, None);
        let result = apply_commit(
            base.clone(),
            Some(&stored),
            &main_at_1,
            vec![add_snapshot(stale), set_branch(MAIN_BRANCH, 3)],
            now_ms(),
        );
        assert_error(result, 409, "CommitFailedException");

        let retried = snapshot(&base, 3, Some(1), 3, None);
        let metadata = apply_commit(
            base,
            Some(&stored),
            &main_at_1,
            vec![add_snapshot(retried), set_branch(MAIN_BRANCH, 3)],
            now_ms(),
        )
        .unwrap()
        .metadata;
        assert_eq!(metadata.current_snapshot_id(), Some(3));
        assert_eq!(metadata.last_sequence_number(), 3);
    }

    /// A staged (WAP) snapshot carries no ref and so no requirement; a stale one is a conflict too.
    #[test]
    fn stale_staged_snapshot_is_a_conflict() {
        let base = metadata_with_branch_commit(FormatVersion::V2);
        let stale = snapshot(&base, 3, Some(1), 2, None);
        assert_error(
            apply_commit(base, None, &[], vec![add_snapshot(stale)], now_ms()),
            409,
            "CommitFailedException",
        );
    }

    #[test]
    fn stale_first_row_id_is_a_conflict() {
        let base = metadata_with_branch_commit(FormatVersion::V3);
        assert_eq!(base.next_row_id(), 15);

        let stale = snapshot(&base, 3, Some(1), 3, Some((10, 5)));
        assert_error(
            apply_commit(
                base.clone(),
                None,
                &[],
                vec![add_snapshot(stale), set_branch(MAIN_BRANCH, 3)],
                now_ms(),
            ),
            409,
            "CommitFailedException",
        );

        let retried = snapshot(&base, 3, Some(1), 3, Some((15, 5)));
        let metadata = commit(
            base,
            vec![add_snapshot(retried), set_branch(MAIN_BRANCH, 3)],
        );
        assert_eq!(metadata.next_row_id(), 20);
    }

    /// A resend of a snapshot that already landed is not a conflict: a retry cannot succeed.
    #[test]
    fn resent_snapshot_is_refused_as_invalid() {
        let base = metadata_with_branch_commit(FormatVersion::V2);
        let resent = snapshot(&base, 2, Some(1), 2, None);
        assert_error(
            apply_commit(base, None, &[], vec![add_snapshot(resent)], now_ms()),
            400,
            "InvalidTableUpdate",
        );
    }

    /// A parentless snapshot below the sequence number of a retained snapshot fails the build,
    /// which stays a conflict: a replace racing another commit heals with a reload
    /// (lakekeeper/lakekeeper#949).
    #[test]
    fn parentless_stale_snapshot_stays_a_conflict() {
        let base = metadata_with_branch_commit(FormatVersion::V2);
        let parentless = snapshot(&base, 3, None, 1, None);
        assert_error(
            apply_commit(
                base,
                None,
                &[],
                vec![add_snapshot(parentless), set_branch(MAIN_BRANCH, 3)],
                now_ms(),
            ),
            409,
            "CommitFailedException",
        );
    }
}
