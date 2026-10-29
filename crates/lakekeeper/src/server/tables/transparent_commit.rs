//! Transparent commit — catalog-brokered yield that lets a writer win over compaction.
//!
//! Iceberg optimistic concurrency is symmetric: whoever commits second loses. When a
//! compaction (a `REPLACE` snapshot) lands first, a concurrent writer's commit fails
//! validation even though the compaction changed no logical table data. "Transparent commit"
//! engineers the asymmetry into the catalog: a compaction commit lands as an ordinary durable
//! snapshot carrying a summary marker declaring it *rollbackable*; only when a writer's commit
//! would otherwise fail does the catalog excise the marked snapshot(s) and rebase the writer
//! onto its expected base — folding the revert into the writer's own commit.
//!
//! This module holds the *policy* half: [`plan_compaction_reap`] decides, from a failed
//! commit's requirements and updates, whether a reap is safe and which snapshots to excise.
//! The structural *mechanism* — actually excising the snapshots and rewinding the branch —
//! lives in `iceberg`'s [`TableMetadata::rollback_branch_to_snapshot`].
//!
//! Both halves are guarded by two independent opt-ins: a per-warehouse feature flag
//! ([`crate::service::RollbackCompactionPolicy`], default off) and a per-snapshot summary marker set by
//! the compaction engine. The marker makes the feature work for external compaction engines
//! too, not just Lakekeeper's own maintenance.

use iceberg::{
    TableRequirement, TableUpdate,
    spec::{FormatVersion, Operation, Snapshot, SnapshotReference, TableMetadata},
};

use crate::service::SnapshotRemovalFence;

/// Primary marker a compaction stamps on its `REPLACE` snapshot summary to opt into being
/// rolled back on conflict. Expected value: `"true"` (compared case-insensitively).
pub(crate) const ROLLBACKABLE_SNAPSHOT_MARKER: &str = "lakekeeper.rollbackable";

/// Value expected for [`ROLLBACKABLE_SNAPSHOT_MARKER`].
pub(crate) const ROLLBACKABLE_SNAPSHOT_MARKER_VALUE: &str = "true";

/// Apache Polaris's equivalent marker. Honoured for drop-in compatibility with compaction
/// engines already integrated against Polaris's `rollback.compaction.on-conflicts` feature.
/// Expected value: `"rollback"` (compared case-insensitively).
pub(crate) const POLARIS_ROLLBACK_MARKER: &str =
    "polaris.internal.conflict-resolution.by-operation-type.replace";

/// Value expected for [`POLARIS_ROLLBACK_MARKER`].
pub(crate) const POLARIS_ROLLBACK_MARKER_VALUE: &str = "rollback";

/// Upper bound on how many stacked compaction snapshots a single reap will excise. A conflict
/// gap longer than this is almost certainly not a simple compaction-vs-writer race; such a gap
/// bails to a normal OCC failure, so the reap never rewinds an unbounded chain.
pub(crate) const MAX_REAP_CHAIN_LENGTH: usize = 20;

/// A validated decision to excise compaction snapshots and rebase a writer's commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReapPlan {
    /// The branch the writer is committing to and that will be rewound.
    pub(crate) branch: String,
    /// The snapshot the writer expected as its base — the rewind target.
    pub(crate) expected_base_snapshot_id: i64,
    /// The snapshots to excise, ordered tip-first.
    pub(crate) reaped_snapshot_ids: Vec<i64>,
}

/// Why a reap was declined. Surfaced only in `debug` traces; every variant maps to a normal
/// OCC failure for the writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReapRejection {
    /// The request does not carry exactly one `assert-ref-snapshot-id` with a concrete id.
    RefAssertionNotUnique,
    /// The asserted reference is unknown, or names a tag; only a branch can be rebased.
    NotABranch,
    /// The asserted reference already matches — the conflict came from elsewhere.
    NoConflict,
    /// The writer's updates are not a single clean snapshot append to the asserted branch.
    UnexpectedUpdateShape,
    /// The writer's snapshot is an `append` or `replace`. Neither depends on the files a
    /// compaction rewrote, so the writer succeeds by retrying on top of the compaction.
    WriterOperationRetriesCleanly,
    /// A snapshot in the gap is not a rollbackable compaction snapshot (stop at first real commit).
    NonRollbackableInGap,
    /// The gap is empty, broken, or the expected base is not an ancestor of the tip.
    GapNotResolvable,
    /// A snapshot in the gap is reachable from another branch or tag.
    ReferencedByOtherRef,
    /// The gap exceeds [`MAX_REAP_CHAIN_LENGTH`].
    GapTooLong,
    /// Rewinding would leave the writer's snapshot with a stale sequence number.
    SequenceNumberConflict,
    /// A gap snapshot carries row-lineage row ranges (V3); not yet supported.
    RowLineageUnsupported,
    /// A gap snapshot is older than the warehouse's maximum compaction age.
    CompactionTooOld,
}

/// How old a compaction snapshot may be and still be rolled back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReapAgeLimit {
    /// The time of the commit, in milliseconds since the Unix epoch.
    pub(crate) now_ms: i64,
    /// The maximum age of a reaped snapshot, by its `timestamp-ms`.
    pub(crate) max_compaction_age_seconds: u64,
}

impl ReapAgeLimit {
    fn admits(self, snapshot: &Snapshot) -> bool {
        let age_ms = self.now_ms.saturating_sub(snapshot.timestamp_ms());
        u64::try_from(age_ms).map_or(true, |age_ms| {
            age_ms <= self.max_compaction_age_seconds.saturating_mul(1000)
        })
    }
}

/// Decide whether a failed commit can be satisfied by excising rollbackable compaction
/// snapshots and rebasing the writer onto its expected base.
///
/// Pure and conservative: returns `Some(plan)` only when the reap is provably safe, and `None`
/// (the writer takes a normal OCC failure) at the slightest doubt. `metadata` is the *current*
/// stored table metadata against which the writer's commit just failed.
pub(crate) fn plan_compaction_reap(
    metadata: &TableMetadata,
    requirements: &[TableRequirement],
    updates: &[TableUpdate],
    age_limit: ReapAgeLimit,
) -> Option<ReapPlan> {
    match plan_compaction_reap_inner(metadata, requirements, updates, age_limit) {
        Ok(plan) => Some(plan),
        Err(reason) => {
            tracing::debug!(?reason, "transparent-commit: declining to reap compaction");
            None
        }
    }
}

fn plan_compaction_reap_inner(
    metadata: &TableMetadata,
    requirements: &[TableRequirement],
    updates: &[TableUpdate],
    age_limit: ReapAgeLimit,
) -> Result<ReapPlan, ReapRejection> {
    // 1. Exactly one `assert-ref-snapshot-id` with a concrete expected snapshot.
    let mut assertions = requirements.iter().filter_map(|r| match r {
        TableRequirement::RefSnapshotIdMatch {
            r#ref,
            snapshot_id: Some(expected),
        } => Some((r#ref.clone(), *expected)),
        _ => None,
    });
    let (branch, expected_base) = assertions
        .next()
        .ok_or(ReapRejection::RefAssertionNotUnique)?;
    if assertions.next().is_some() {
        return Err(ReapRejection::RefAssertionNotUnique);
    }

    // The asserted reference must exist and be a branch. Its tip must actually differ from the
    // expectation, otherwise this requirement passed and the conflict lies elsewhere.
    let branch_ref = metadata
        .refs()
        .get(&branch)
        .ok_or(ReapRejection::NotABranch)?;
    if !branch_ref.is_branch() {
        return Err(ReapRejection::NotABranch);
    }
    let tip = branch_ref.snapshot_id;
    if tip == expected_base {
        return Err(ReapRejection::NoConflict);
    }

    // 2. The writer's updates must be a single clean snapshot append to the asserted branch.
    let added_snapshot = clean_writer_snapshot(updates, &branch, expected_base)?;

    // 3. Walk the gap from the tip back to the expected base. Every snapshot in between must be
    //    a rollbackable compaction snapshot; the first non-rollbackable snapshot (a real data
    //    commit) stops the reap.
    let mut reaped = Vec::new();
    let mut cursor = Some(tip);
    loop {
        let current = cursor.ok_or(ReapRejection::GapNotResolvable)?;
        if current == expected_base {
            break;
        }
        if reaped.len() >= MAX_REAP_CHAIN_LENGTH {
            return Err(ReapRejection::GapTooLong);
        }
        let snapshot = metadata
            .snapshot_by_id(current)
            .ok_or(ReapRejection::GapNotResolvable)?;
        if !is_rollbackable_snapshot(snapshot) {
            return Err(ReapRejection::NonRollbackableInGap);
        }
        if !age_limit.admits(snapshot) {
            return Err(ReapRejection::CompactionTooOld);
        }
        // V3 row lineage: excising a snapshot that assigned row ids would leave `next-row-id`
        // ahead of the retained snapshots, breaking the rebased writer's row-id validation, so
        // such a snapshot is not reapable and the reap bails conservatively.
        if metadata.format_version() >= FormatVersion::V3 && snapshot.row_range().is_some() {
            return Err(ReapRejection::RowLineageUnsupported);
        }
        reaped.push(current);
        cursor = snapshot.parent_snapshot_id();
    }
    if reaped.is_empty() {
        return Err(ReapRejection::GapNotResolvable);
    }

    // 4. Never reap a snapshot another branch or tag can reach (even transitively): its
    //    manifests still reference the reaped snapshots' data files, so excising them would let
    //    orphan cleanup delete files that ref still depends on.
    for (ref_name, reference) in metadata.refs() {
        if ref_name == &branch {
            continue;
        }
        let mut cursor = Some(reference.snapshot_id);
        while let Some(current) = cursor {
            if reaped.contains(&current) {
                return Err(ReapRejection::ReferencedByOtherRef);
            }
            cursor = metadata
                .snapshot_by_id(current)
                .and_then(|s| s.parent_snapshot_id());
        }
    }

    // 5. The writer's snapshot must legitimately sit on top of the rewound base. After excising
    //    the gap, `last_sequence_number` drops to the highest remaining snapshot's sequence
    //    number; the writer's pre-assigned sequence number must exceed it (V2+). If another
    //    branch advanced the sequence past the writer's, we cannot rebase — bail.
    if metadata.format_version() != FormatVersion::V1 {
        let reaped_set = &reaped;
        let new_last_sequence_number = metadata
            .snapshots()
            .filter(|s| !reaped_set.contains(&s.snapshot_id()))
            .map(|s| s.sequence_number())
            .max()
            .unwrap_or(0);
        if added_snapshot.sequence_number() <= new_last_sequence_number {
            return Err(ReapRejection::SequenceNumberConflict);
        }
    }

    Ok(ReapPlan {
        branch,
        expected_base_snapshot_id: expected_base,
        reaped_snapshot_ids: reaped,
    })
}

/// Returns the writer's added snapshot if `updates` are a single clean snapshot append to
/// `branch`: exactly one `AddSnapshot` whose parent is `expected_base`, exactly one
/// `SetSnapshotRef` that keeps `branch` a branch and points it at that snapshot, and no snapshot
/// or ref removals. The added snapshot must be an `overwrite` or `delete`. Anything else is not
/// a shape that can be safely rebased.
fn clean_writer_snapshot<'a>(
    updates: &'a [TableUpdate],
    branch: &str,
    expected_base: i64,
) -> Result<&'a Snapshot, ReapRejection> {
    let mut added_snapshot: Option<&Snapshot> = None;
    let mut set_ref: Option<(&str, &SnapshotReference)> = None;
    let mut set_ref_count = 0usize;
    for update in updates {
        match update {
            TableUpdate::AddSnapshot { snapshot } => {
                if added_snapshot.is_some() {
                    return Err(ReapRejection::UnexpectedUpdateShape);
                }
                added_snapshot = Some(snapshot);
            }
            TableUpdate::SetSnapshotRef {
                ref_name,
                reference,
            } => {
                set_ref_count += 1;
                set_ref = Some((ref_name, reference));
            }
            TableUpdate::RemoveSnapshots { .. } | TableUpdate::RemoveSnapshotRef { .. } => {
                return Err(ReapRejection::UnexpectedUpdateShape);
            }
            _ => {}
        }
    }
    let added_snapshot = added_snapshot.ok_or(ReapRejection::UnexpectedUpdateShape)?;
    let clean_branch_update = set_ref.is_some_and(|(ref_name, reference)| {
        ref_name == branch
            && reference.is_branch()
            && reference.snapshot_id == added_snapshot.snapshot_id()
    });
    if set_ref_count != 1 || !clean_branch_update {
        return Err(ReapRejection::UnexpectedUpdateShape);
    }
    // The writer snapshot must have `expected_base` as its parent, so the rebased branch is a clean
    // append to the rewound base. This rejects a missing parent and any other parent. The iceberg
    // builder accepts a `parent_snapshot_id` that names no existing snapshot, so a parent that
    // names a reaped compaction snapshot would leave the rebased branch with a dangling parent.
    if added_snapshot.parent_snapshot_id() != Some(expected_base) {
        return Err(ReapRejection::UnexpectedUpdateShape);
    }
    // Only an `overwrite` or `delete` can reference files the compaction rewrote (row-level
    // deletes, merge-on-read updates) and so fail validation on every retry.
    match added_snapshot.summary().operation {
        Operation::Overwrite | Operation::Delete => {}
        Operation::Append | Operation::Replace => {
            return Err(ReapRejection::WriterOperationRetriesCleanly);
        }
    }
    Ok(added_snapshot)
}

/// A snapshot may be rolled back on conflict iff it is a `REPLACE` (compaction) snapshot whose
/// summary opts in via either the Lakekeeper or the Polaris marker.
fn is_rollbackable_snapshot(snapshot: &Snapshot) -> bool {
    let summary = snapshot.summary();
    if summary.operation != Operation::Replace {
        return false;
    }
    let has_marker = |key: &str, value: &str| {
        summary
            .additional_properties
            .get(key)
            .is_some_and(|v| v.eq_ignore_ascii_case(value))
    };
    has_marker(
        ROLLBACKABLE_SNAPSHOT_MARKER,
        ROLLBACKABLE_SNAPSHOT_MARKER_VALUE,
    ) || has_marker(POLARIS_ROLLBACK_MARKER, POLARIS_ROLLBACK_MARKER_VALUE)
}

/// Whether the client's own updates remove snapshots, as an expire-snapshots run does.
pub(crate) fn removes_snapshots(updates: &[TableUpdate]) -> bool {
    updates
        .iter()
        .any(|u| matches!(u, TableUpdate::RemoveSnapshots { .. }))
}

/// Whether a commit that removes snapshots must wait because a compaction was rolled back
/// within `window`.
///
/// A client that expires snapshots, such as Java's `ExpireSnapshots`, loads the table before
/// and after its commit and deletes the files of every snapshot missing from the second load
/// as if it had expired that snapshot. A rolled-back compaction missing from the second load
/// would make it delete files the new branch tip still reads. The conflict makes such a client
/// retry from a load that no longer contains the rolled-back compaction.
pub(crate) fn snapshot_removal_fenced(
    fence: &SnapshotRemovalFence,
    window: chrono::Duration,
) -> bool {
    fence.since_last_reap.is_some_and(|since| since < window)
}

/// Whether a rollback must be skipped because a commit removed snapshots within `window`.
/// The client of that commit may not have loaded the table a second time yet; see
/// [`snapshot_removal_fenced`].
pub(crate) fn reap_fenced(fence: &SnapshotRemovalFence, window: chrono::Duration) -> bool {
    fence
        .since_last_snapshot_removal
        .is_some_and(|since| since < window)
}

/// The updates synthesized by a reap, to be recorded alongside the writer's own updates so that
/// events, contract verifiers, and update-kind flags observe a truthful change list.
pub(crate) fn synthesized_reap_updates(plan: &ReapPlan) -> Vec<TableUpdate> {
    vec![TableUpdate::RemoveSnapshots {
        snapshot_ids: plan.reaped_snapshot_ids.clone(),
    }]
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use iceberg::spec::{
        NestedField, PrimitiveType, Schema, Snapshot, SnapshotReference, SnapshotRetention,
        SortOrder, Summary, TableMetadataBuilder, Type, UnboundPartitionSpec,
    };

    use super::*;

    const MAIN: &str = "main";

    /// The writer commits at 3000 ms; every test snapshot is younger than an hour.
    const AGE_LIMIT: ReapAgeLimit = ReapAgeLimit {
        now_ms: 3000,
        max_compaction_age_seconds: 3600,
    };

    fn plan_inner(
        metadata: &TableMetadata,
        requirements: &[TableRequirement],
        updates: &[TableUpdate],
    ) -> Result<ReapPlan, ReapRejection> {
        plan_compaction_reap_inner(metadata, requirements, updates, AGE_LIMIT)
    }

    fn plan(
        metadata: &TableMetadata,
        requirements: &[TableRequirement],
        updates: &[TableUpdate],
    ) -> Option<ReapPlan> {
        plan_compaction_reap(metadata, requirements, updates, AGE_LIMIT)
    }

    fn base_builder() -> TableMetadataBuilder {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();
        TableMetadataBuilder::new(
            schema,
            UnboundPartitionSpec::builder().build(),
            SortOrder::unsorted_order(),
            "s3://bucket/table".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
    }

    fn snapshot(
        id: i64,
        parent: Option<i64>,
        seq: i64,
        ts: i64,
        op: Operation,
        markers: &[(&str, &str)],
    ) -> Snapshot {
        Snapshot::builder()
            .with_snapshot_id(id)
            .with_parent_snapshot_id(parent)
            .with_sequence_number(seq)
            .with_timestamp_ms(ts)
            .with_schema_id(0)
            .with_manifest_list(format!("/snap-{id}.avro"))
            .with_summary(Summary {
                operation: op,
                additional_properties: markers
                    .iter()
                    .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                    .collect(),
            })
            .build()
    }

    fn commit(
        metadata: iceberg::spec::TableMetadata,
        snapshot: Snapshot,
    ) -> iceberg::spec::TableMetadata {
        metadata
            .into_builder(Some("s3://bucket/table/metadata/m.json".to_string()))
            .set_branch_snapshot(snapshot, MAIN)
            .unwrap()
            .build()
            .unwrap()
            .metadata
    }

    const ROLLBACK_MARKER: &[(&str, &str)] = &[(ROLLBACKABLE_SNAPSHOT_MARKER, "true")];

    /// Table at snapshot A, then a rollbackable compaction C landed on top.
    fn table_a_then_compaction(
        markers: &[(&str, &str)],
        c_op: Operation,
    ) -> iceberg::spec::TableMetadata {
        let after_a = base_builder()
            .add_snapshot(snapshot(1, None, 1, 1000, Operation::Append, &[]))
            .unwrap()
            .set_ref(
                MAIN,
                SnapshotReference {
                    snapshot_id: 1,
                    retention: SnapshotRetention::Branch {
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                },
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        commit(after_a, snapshot(2, Some(1), 2, 2000, c_op, markers))
    }

    /// A clean single-snapshot row-level delete asserting base snapshot `expected` on `main`.
    fn writer(expected: i64, new_id: i64, seq: i64) -> (Vec<TableRequirement>, Vec<TableUpdate>) {
        let reqs = vec![TableRequirement::RefSnapshotIdMatch {
            r#ref: MAIN.to_string(),
            snapshot_id: Some(expected),
        }];
        let updates = vec![
            TableUpdate::AddSnapshot {
                snapshot: snapshot(new_id, Some(expected), seq, 3000, Operation::Delete, &[]),
            },
            TableUpdate::SetSnapshotRef {
                ref_name: MAIN.to_string(),
                reference: SnapshotReference {
                    snapshot_id: new_id,
                    retention: SnapshotRetention::Branch {
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                },
            },
        ];
        (reqs, updates)
    }

    #[test]
    fn reaps_single_rollbackable_compaction() {
        let md = table_a_then_compaction(ROLLBACK_MARKER, Operation::Replace);
        let (reqs, updates) = writer(1, 3, 2);
        let plan = plan(&md, &reqs, &updates).expect("should reap");
        assert_eq!(plan.branch, MAIN);
        assert_eq!(plan.expected_base_snapshot_id, 1);
        assert_eq!(plan.reaped_snapshot_ids, vec![2]);
    }

    #[test]
    fn honours_polaris_marker() {
        let md =
            table_a_then_compaction(&[(POLARIS_ROLLBACK_MARKER, "rollback")], Operation::Replace);
        let (reqs, updates) = writer(1, 3, 2);
        assert!(plan(&md, &reqs, &updates).is_some());
    }

    #[test]
    fn declines_unmarked_replace() {
        let md = table_a_then_compaction(&[], Operation::Replace);
        let (reqs, updates) = writer(1, 3, 2);
        assert!(plan(&md, &reqs, &updates).is_none());
    }

    #[test]
    fn declines_marked_non_replace() {
        // Append carrying the marker is not a compaction snapshot.
        let md = table_a_then_compaction(ROLLBACK_MARKER, Operation::Append);
        let (reqs, updates) = writer(1, 3, 2);
        assert!(plan(&md, &reqs, &updates).is_none());
    }

    #[test]
    fn declines_real_commit_in_gap() {
        // A <- C(rollbackable replace) <- D(plain append). Writer asserts base A.
        let with_c = table_a_then_compaction(ROLLBACK_MARKER, Operation::Replace);
        let md = commit(
            with_c,
            snapshot(3, Some(2), 3, 2500, Operation::Append, &[]),
        );
        let (reqs, updates) = writer(1, 4, 2);
        assert!(plan(&md, &reqs, &updates).is_none());
    }

    #[test]
    fn declines_when_referenced_by_tag() {
        let md = base_builder()
            .add_snapshot(snapshot(1, None, 1, 1000, Operation::Append, &[]))
            .unwrap()
            .set_ref(
                MAIN,
                SnapshotReference {
                    snapshot_id: 1,
                    retention: SnapshotRetention::Branch {
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                },
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let md = md
            .into_builder(Some("s3://bucket/table/metadata/m.json".to_string()))
            .set_branch_snapshot(
                snapshot(2, Some(1), 2, 2000, Operation::Replace, ROLLBACK_MARKER),
                MAIN,
            )
            .unwrap()
            .set_ref(
                "t",
                SnapshotReference {
                    snapshot_id: 2,
                    retention: SnapshotRetention::Tag {
                        max_ref_age_ms: None,
                    },
                },
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let (reqs, updates) = writer(1, 3, 2);
        assert!(plan(&md, &reqs, &updates).is_none());
    }

    #[test]
    fn declines_without_ref_assertion() {
        let md = table_a_then_compaction(ROLLBACK_MARKER, Operation::Replace);
        let (_reqs, updates) = writer(1, 3, 2);
        assert!(plan(&md, &[], &updates).is_none());
    }

    #[test]
    fn declines_with_two_ref_assertions() {
        let md = table_a_then_compaction(ROLLBACK_MARKER, Operation::Replace);
        let (mut reqs, updates) = writer(1, 3, 2);
        reqs.push(TableRequirement::RefSnapshotIdMatch {
            r#ref: MAIN.to_string(),
            snapshot_id: Some(1),
        });
        assert!(plan(&md, &reqs, &updates).is_none());
    }

    #[test]
    fn declines_writer_with_snapshot_removal() {
        let md = table_a_then_compaction(ROLLBACK_MARKER, Operation::Replace);
        let (reqs, mut updates) = writer(1, 3, 2);
        updates.push(TableUpdate::RemoveSnapshots {
            snapshot_ids: vec![2],
        });
        assert!(plan(&md, &reqs, &updates).is_none());
    }

    #[test]
    fn declines_set_ref_not_targeting_added_snapshot() {
        // The branch update points at the existing snapshot 1, which would leave the writer's
        // snapshot 3 unreferenced.
        let md = table_a_then_compaction(ROLLBACK_MARKER, Operation::Replace);
        let (reqs, mut updates) = writer(1, 3, 2);
        let TableUpdate::SetSnapshotRef { reference, .. } = &mut updates[1] else {
            unreachable!("writer() puts SetSnapshotRef second");
        };
        reference.snapshot_id = 1;
        assert!(plan(&md, &reqs, &updates).is_none());
    }

    #[test]
    fn declines_set_ref_converting_branch_to_tag() {
        let md = table_a_then_compaction(ROLLBACK_MARKER, Operation::Replace);
        let (reqs, mut updates) = writer(1, 3, 2);
        let TableUpdate::SetSnapshotRef { reference, .. } = &mut updates[1] else {
            unreachable!("writer() puts SetSnapshotRef second");
        };
        reference.retention = SnapshotRetention::Tag {
            max_ref_age_ms: None,
        };
        assert!(plan(&md, &reqs, &updates).is_none());
    }

    #[test]
    fn declines_writer_snapshot_not_parented_on_base() {
        // Writer asserts base 1 but its snapshot parents off the reaped compaction 2. Accepting it
        // would leave the rebased branch with a dangling parent.
        let md = table_a_then_compaction(ROLLBACK_MARKER, Operation::Replace);
        let (reqs, mut updates) = writer(1, 3, 2);
        updates[0] = TableUpdate::AddSnapshot {
            snapshot: snapshot(3, Some(2), 2, 3000, Operation::Delete, &[]),
        };
        assert!(plan(&md, &reqs, &updates).is_none());
    }

    #[test]
    fn declines_when_ref_matches_no_conflict() {
        let md = table_a_then_compaction(ROLLBACK_MARKER, Operation::Replace);
        // Writer asserts the current tip (2) — no conflict, nothing to reap.
        let (reqs, updates) = writer(2, 3, 3);
        assert!(plan(&md, &reqs, &updates).is_none());
    }

    #[test]
    fn declines_on_stale_sequence_number() {
        let md = table_a_then_compaction(ROLLBACK_MARKER, Operation::Replace);
        // Writer's snapshot reuses seq 1, which is not greater than the rewound base's seq.
        let (reqs, updates) = writer(1, 3, 1);
        assert!(plan(&md, &reqs, &updates).is_none());
    }

    #[test]
    fn declines_writer_append_or_replace() {
        let md = table_a_then_compaction(ROLLBACK_MARKER, Operation::Replace);
        for op in [Operation::Append, Operation::Replace] {
            let (reqs, mut updates) = writer(1, 3, 2);
            updates[0] = TableUpdate::AddSnapshot {
                snapshot: snapshot(3, Some(1), 2, 3000, op.clone(), &[]),
            };
            assert_eq!(
                plan_inner(&md, &reqs, &updates),
                Err(ReapRejection::WriterOperationRetriesCleanly),
                "{op:?}"
            );
        }
    }

    #[test]
    fn reaps_for_writer_overwrite() {
        let md = table_a_then_compaction(ROLLBACK_MARKER, Operation::Replace);
        let (reqs, mut updates) = writer(1, 3, 2);
        updates[0] = TableUpdate::AddSnapshot {
            snapshot: snapshot(3, Some(1), 2, 3000, Operation::Overwrite, &[]),
        };
        assert!(plan(&md, &reqs, &updates).is_some());
    }

    #[test]
    fn reaps_compaction_exactly_at_max_age() {
        // The compaction snapshot was taken at 2000 ms.
        let md = table_a_then_compaction(ROLLBACK_MARKER, Operation::Replace);
        let (reqs, updates) = writer(1, 3, 2);
        let limit = ReapAgeLimit {
            now_ms: 2000 + 3_600_000,
            max_compaction_age_seconds: 3600,
        };
        assert!(plan_compaction_reap(&md, &reqs, &updates, limit).is_some());
    }

    #[test]
    fn declines_compaction_older_than_max_age() {
        let md = table_a_then_compaction(ROLLBACK_MARKER, Operation::Replace);
        let (reqs, updates) = writer(1, 3, 2);
        let limit = ReapAgeLimit {
            now_ms: 2000 + 3_600_001,
            max_compaction_age_seconds: 3600,
        };
        assert_eq!(
            plan_compaction_reap_inner(&md, &reqs, &updates, limit),
            Err(ReapRejection::CompactionTooOld)
        );
    }

    #[test]
    fn fence_blocks_only_inside_window() {
        let window = chrono::Duration::seconds(60);
        let at = |secs| SnapshotRemovalFence {
            since_last_reap: Some(chrono::Duration::seconds(secs)),
            since_last_snapshot_removal: Some(chrono::Duration::seconds(secs)),
        };
        assert!(snapshot_removal_fenced(&at(59), window));
        assert!(reap_fenced(&at(59), window));
        // Written by a transaction that started after the reading one.
        assert!(snapshot_removal_fenced(&at(-1), window));
        assert!(reap_fenced(&at(-1), window));
        assert!(!snapshot_removal_fenced(&at(60), window));
        assert!(!reap_fenced(&at(60), window));
        let never = SnapshotRemovalFence::default();
        assert!(!snapshot_removal_fenced(&never, window));
        assert!(!reap_fenced(&never, window));
    }

    #[test]
    fn recognizes_snapshot_removal_updates() {
        assert!(removes_snapshots(&[TableUpdate::RemoveSnapshots {
            snapshot_ids: vec![1]
        }]));
        let (_, writer_updates) = writer(1, 3, 2);
        assert!(!removes_snapshots(&writer_updates));
    }
}
