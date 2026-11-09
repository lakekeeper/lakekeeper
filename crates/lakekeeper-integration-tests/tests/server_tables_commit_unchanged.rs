//! A table commit that changes nothing writes nothing: no metadata file and no change in the
//! catalog, and Lakekeeper does not call `transaction_committed` for it. A commit that changes
//! one table writes every table of the request.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use futures::TryStreamExt as _;
use iceberg::{
    TableIdent, TableRequirement, TableUpdate,
    spec::{
        MAIN_BRANCH, Operation, Snapshot, SnapshotReference, SnapshotRetention, Summary,
        TableMetadata, ViewMetadata,
    },
};
use iceberg_ext::catalog::rest::{
    CommitTableRequest, CommitTableResponse, CommitTransactionRequest, ErrorModel,
    IcebergErrorResponse, LoadTableResult, ViewUpdate,
};
use lakekeeper::{
    api::{
        ApiContext, RequestMetadata,
        endpoints::EndpointFlat,
        iceberg::{
            types::Prefix,
            v1::{
                DataAccess, NamespaceParameters, TableParameters,
                tables::{LoadTableRequest, LoadTableResultOrNotModified, TablesService as _},
            },
        },
        management::v1::warehouse::TabularDeleteProfile,
    },
    server::CatalogServer,
    service::{
        State, TabularId,
        authz::AllowAllAuthorizer,
        contract_verification::{
            ContractVerification, ContractVerificationOutcome, ContractVerifiers,
        },
        events::{CommitTransactionEvent, EventListener},
        idempotency::IdempotencyKey,
    },
};
use lakekeeper_integration_tests::{
    create_ns, create_table_request, memory_io_profile, setup_simple,
};
use lakekeeper_io::LakekeeperStorage as _;
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;

type Ctx = ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>;

async fn setup(pool: PgPool) -> (Ctx, NamespaceParameters) {
    let (ctx, warehouse) = setup_simple(
        pool,
        memory_io_profile(),
        None,
        AllowAllAuthorizer::default(),
        TabularDeleteProfile::Hard {},
        None,
    )
    .await;
    let prefix = warehouse.warehouse_id.to_string();
    let namespace = create_ns(ctx.clone(), prefix.clone(), "ns".to_string())
        .await
        .namespace;
    (
        ctx,
        NamespaceParameters {
            prefix: Some(Prefix(prefix)),
            namespace,
        },
    )
}

async fn create_table(
    ctx: &Ctx,
    ns_params: &NamespaceParameters,
    name: &str,
    staged: bool,
) -> LoadTableResult {
    CatalogServer::create_table(
        ns_params.clone(),
        create_table_request(Some(name.to_string()), Some(staged)),
        DataAccess::not_specified(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap()
}

fn table_params(ns_params: &NamespaceParameters, name: &str) -> TableParameters {
    TableParameters {
        prefix: ns_params.prefix.clone(),
        table: TableIdent::new(ns_params.namespace.clone(), name.to_string()),
    }
}

/// Commit to one table with `updateTable`.
async fn commit_as(
    ctx: &Ctx,
    table: TableParameters,
    requirements: Vec<TableRequirement>,
    updates: Vec<TableUpdate>,
    request_metadata: RequestMetadata,
) -> Result<CommitTableResponse, IcebergErrorResponse> {
    CatalogServer::commit_table(
        table,
        CommitTableRequest {
            identifier: None,
            requirements,
            updates,
        },
        ctx.clone(),
        request_metadata,
    )
    .await
}

async fn commit(
    ctx: &Ctx,
    ns_params: &NamespaceParameters,
    name: &str,
    requirements: Vec<TableRequirement>,
    updates: Vec<TableUpdate>,
) -> Result<CommitTableResponse, IcebergErrorResponse> {
    commit_as(
        ctx,
        table_params(ns_params, name),
        requirements,
        updates,
        RequestMetadata::new_unauthenticated(),
    )
    .await
}

/// Commit to several tables with `commitTransaction`.
async fn commit_transaction(
    ctx: &Ctx,
    ns_params: &NamespaceParameters,
    changes: Vec<(&str, Vec<TableRequirement>, Vec<TableUpdate>)>,
    request_metadata: RequestMetadata,
) -> Result<(), IcebergErrorResponse> {
    CatalogServer::commit_transaction(
        ns_params.prefix.clone(),
        CommitTransactionRequest {
            table_changes: changes
                .into_iter()
                .map(|(name, requirements, updates)| CommitTableRequest {
                    identifier: Some(TableIdent::new(
                        ns_params.namespace.clone(),
                        name.to_string(),
                    )),
                    requirements,
                    updates,
                })
                .collect(),
        },
        ctx.clone(),
        request_metadata,
    )
    .await
}

async fn load(ctx: &Ctx, ns_params: &NamespaceParameters, name: &str) -> LoadTableResult {
    let LoadTableResultOrNotModified::LoadTableResult(loaded) = CatalogServer::load_table(
        table_params(ns_params, name),
        LoadTableRequest::builder().build(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap() else {
        panic!("a load without ETags returns the table");
    };
    loaded
}

/// Files in the `metadata` directory of the table at `table_location`, sorted.
async fn metadata_files(table_location: &str) -> Vec<String> {
    let io = memory_io_profile().file_io(None).await.unwrap();
    let pages: Vec<_> = io
        .list(&format!("{table_location}/metadata/"), None)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut files: Vec<String> = pages
        .into_iter()
        .flatten()
        .map(|file| file.location().to_string())
        .collect();
    files.sort();
    files
}

fn set_property(value: &str) -> TableUpdate {
    TableUpdate::SetProperties {
        updates: HashMap::from([("k".to_string(), value.to_string())]),
    }
}

fn main_at(snapshot_id: i64) -> TableUpdate {
    TableUpdate::SetSnapshotRef {
        ref_name: MAIN_BRANCH.to_string(),
        reference: SnapshotReference {
            snapshot_id,
            retention: SnapshotRetention::Branch {
                min_snapshots_to_keep: None,
                max_snapshot_age_ms: None,
                max_ref_age_ms: None,
            },
        },
    }
}

fn require_main_at(snapshot_id: Option<i64>) -> TableRequirement {
    TableRequirement::RefSnapshotIdMatch {
        r#ref: MAIN_BRANCH.to_string(),
        snapshot_id,
    }
}

/// Add the snapshot `snapshot_id` and make it current on `main`.
fn append(metadata: &TableMetadata, snapshot_id: i64) -> Vec<TableUpdate> {
    let snapshot = Snapshot::builder()
        .with_snapshot_id(snapshot_id)
        .with_parent_snapshot_id(metadata.current_snapshot_id())
        .with_timestamp_ms(chrono::Utc::now().timestamp_millis())
        .with_sequence_number(metadata.last_sequence_number() + 1)
        .with_schema_id(metadata.current_schema_id())
        .with_manifest_list(format!("/snap-{snapshot_id}.avro"))
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: HashMap::new(),
        })
        .build();
    vec![TableUpdate::AddSnapshot { snapshot }, main_at(snapshot_id)]
}

fn new_key() -> IdempotencyKey {
    IdempotencyKey::parse(&uuid::Uuid::now_v7().to_string()).unwrap()
}

fn with_key(key: IdempotencyKey) -> RequestMetadata {
    let mut metadata = RequestMetadata::new_unauthenticated();
    metadata.with_idempotency_key(key);
    metadata
}

/// An empty commit returns the stored metadata and its location and writes no metadata file.
#[sqlx::test]
async fn test_empty_commit_returns_the_stored_metadata_and_writes_nothing(pool: PgPool) {
    let (ctx, ns_params) = setup(pool).await;
    let created = create_table(&ctx, &ns_params, "t", false).await;
    let changed = commit(&ctx, &ns_params, "t", vec![], vec![set_property("v")])
        .await
        .unwrap();

    let unchanged = commit(
        &ctx,
        &ns_params,
        "t",
        vec![TableRequirement::UuidMatch {
            uuid: created.metadata.uuid(),
        }],
        vec![],
    )
    .await
    .unwrap();

    assert_eq!(unchanged.metadata_location, changed.metadata_location);
    assert_eq!(unchanged.etag, changed.etag);
    // Includes the metadata log and `last-updated-ms`.
    assert_eq!(unchanged.metadata, changed.metadata);
    assert_eq!(
        metadata_files(created.metadata.location()).await,
        vec![
            created.metadata_location.clone().unwrap(),
            changed.metadata_location.clone(),
        ]
    );
    let loaded = load(&ctx, &ns_params, "t").await;
    assert_eq!(loaded.metadata_location, Some(changed.metadata_location));
    assert_eq!(loaded.metadata, changed.metadata);
}

/// An update that leaves the metadata as it is writes nothing either. An update the metadata
/// builder records as a change writes, even if it sets a property to its current value.
#[sqlx::test]
async fn test_updates_that_change_nothing_write_nothing(pool: PgPool) {
    let (ctx, ns_params) = setup(pool).await;
    let created = create_table(&ctx, &ns_params, "t", false).await;
    let mut updates = append(&created.metadata, 1);
    updates.push(set_property("v"));
    let appended = commit(&ctx, &ns_params, "t", vec![], updates)
        .await
        .unwrap();

    let unchanged = commit(
        &ctx,
        &ns_params,
        "t",
        vec![require_main_at(Some(1))],
        vec![main_at(1)],
    )
    .await
    .unwrap();
    assert_eq!(unchanged.metadata_location, appended.metadata_location);
    assert_eq!(unchanged.etag, appended.etag);
    assert_eq!(unchanged.metadata, appended.metadata);

    let same_value = commit(&ctx, &ns_params, "t", vec![], vec![set_property("v")])
        .await
        .unwrap();
    assert_ne!(same_value.metadata_location, appended.metadata_location);
    assert_eq!(
        metadata_files(created.metadata.location()).await,
        vec![
            created.metadata_location.clone().unwrap(),
            appended.metadata_location,
            same_value.metadata_location,
        ]
    );
}

/// The requirements of a commit that changes nothing are still checked.
#[sqlx::test]
async fn test_empty_commit_with_a_failing_requirement_is_a_conflict(pool: PgPool) {
    let (ctx, ns_params) = setup(pool).await;
    let created = create_table(&ctx, &ns_params, "t", false).await;

    let error = commit(
        &ctx,
        &ns_params,
        "t",
        vec![require_main_at(Some(1))],
        vec![],
    )
    .await
    .unwrap_err();

    assert_eq!(
        (error.error.code, error.error.r#type.as_str()),
        (409, "CatalogCommitConflicts")
    );
    assert_eq!(
        load(&ctx, &ns_params, "t").await.metadata_location,
        created.metadata_location
    );
}

/// A commit to a staged table writes its first metadata file, even if it has no updates.
#[sqlx::test]
async fn test_empty_commit_activates_a_staged_table(pool: PgPool) {
    let (ctx, ns_params) = setup(pool).await;
    let staged = create_table(&ctx, &ns_params, "t", true).await;
    assert_eq!(staged.metadata_location, None);

    let committed = commit(&ctx, &ns_params, "t", vec![], vec![]).await.unwrap();

    let loaded = load(&ctx, &ns_params, "t").await;
    assert_eq!(
        loaded.metadata_location,
        Some(committed.metadata_location.clone())
    );
    assert_eq!(
        metadata_files(staged.metadata.location()).await,
        vec![committed.metadata_location]
    );
}

/// A contract verifier still checks a commit that changes nothing, and its refusal is the
/// answer.
#[sqlx::test]
async fn test_contract_verifier_refusing_an_empty_commit_is_the_answer(pool: PgPool) {
    let (mut ctx, ns_params) = setup(pool).await;
    let created = create_table(&ctx, &ns_params, "t", false).await;
    ctx.v1_state.contract_verifiers = ContractVerifiers::new(vec![Arc::new(TableCommitVerifier {
        refuse: true,
        ..TableCommitVerifier::default()
    })]);

    let error = commit(&ctx, &ns_params, "t", vec![], vec![])
        .await
        .unwrap_err();

    assert_eq!(
        (error.error.code, error.error.r#type.as_str()),
        (409, "ContractViolation")
    );
    assert_eq!(
        load(&ctx, &ns_params, "t").await.metadata_location,
        created.metadata_location
    );
    assert_eq!(
        metadata_files(created.metadata.location()).await,
        vec![created.metadata_location.unwrap()]
    );
}

/// A staged table in a transaction gets its first metadata file, so every table is written.
#[sqlx::test]
async fn test_transaction_with_a_staged_table_writes_every_table(pool: PgPool) {
    let (ctx, ns_params) = setup(pool).await;
    let existing = create_table(&ctx, &ns_params, "existing", false).await;
    let staged = create_table(&ctx, &ns_params, "staged", true).await;

    commit_transaction(
        &ctx,
        &ns_params,
        vec![("existing", vec![], vec![]), ("staged", vec![], vec![])],
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    let existing_location = load(&ctx, &ns_params, "existing")
        .await
        .metadata_location
        .unwrap();
    assert_eq!(
        metadata_files(existing.metadata.location()).await,
        vec![existing.metadata_location.unwrap(), existing_location]
    );
    let staged_location = load(&ctx, &ns_params, "staged")
        .await
        .metadata_location
        .unwrap();
    assert_eq!(
        metadata_files(staged.metadata.location()).await,
        vec![staged_location]
    );
}

/// A transaction without table changes is a bad request and records no idempotency key.
#[sqlx::test]
async fn test_transaction_without_table_changes_is_a_bad_request(pool: PgPool) {
    let (ctx, ns_params) = setup(pool.clone()).await;
    let key = new_key();

    let error = commit_transaction(&ctx, &ns_params, vec![], with_key(key))
        .await
        .unwrap_err();

    assert_eq!(
        (error.error.code, error.error.r#type.as_str()),
        (400, "EmptyCommitTransaction")
    );
    let records: Vec<(String, i32)> = sqlx::query_as(
        "SELECT operation, http_status FROM idempotency_record WHERE idempotency_key = $1",
    )
    .bind(key.as_uuid())
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(records, vec![]);
}

/// If one table of a transaction changes, every table is written.
#[sqlx::test]
async fn test_transaction_with_one_changed_table_writes_every_table(pool: PgPool) {
    let (ctx, ns_params) = setup(pool).await;
    let a = create_table(&ctx, &ns_params, "a", false).await;
    let b = create_table(&ctx, &ns_params, "b", false).await;

    commit_transaction(
        &ctx,
        &ns_params,
        vec![
            ("a", vec![], vec![set_property("v")]),
            ("b", vec![], vec![]),
        ],
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    for (name, created) in [("a", a), ("b", b)] {
        let loaded = load(&ctx, &ns_params, name).await;
        let location = loaded.metadata_location.unwrap();
        assert_ne!(
            Some(&location),
            created.metadata_location.as_ref(),
            "{name}"
        );
        assert_eq!(
            metadata_files(created.metadata.location()).await,
            vec![created.metadata_location.unwrap(), location],
            "{name}"
        );
    }
}

/// A commit to run, with the catalog to run it against.
type PendingCommit = (Ctx, TableParameters, Vec<TableUpdate>);

/// A contract verifier for table commits.
#[derive(Default)]
struct TableCommitVerifier {
    /// Refuse every table commit with a 409 `ContractViolation`.
    refuse: bool,
    /// Run this commit while the first commit the verifier checks is in flight: after that
    /// commit loaded its tables and before it writes them.
    commit_meanwhile: Mutex<Option<PendingCommit>>,
}

impl std::fmt::Debug for TableCommitVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableCommitVerifier")
            .field("refuse", &self.refuse)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl ContractVerification for TableCommitVerifier {
    fn name(&self) -> &'static str {
        "table-commit-verifier"
    }

    async fn check_table_updates(
        &self,
        _table_updates: &[TableUpdate],
        _current_metadata: &TableMetadata,
    ) -> Result<ContractVerificationOutcome, ErrorModel> {
        let pending = self.commit_meanwhile.lock().unwrap().take();
        if let Some((ctx, table, updates)) = pending {
            commit_as(
                &ctx,
                table,
                vec![],
                updates,
                RequestMetadata::new_unauthenticated(),
            )
            .await
            .expect("the concurrent commit succeeds");
        }
        if self.refuse {
            return Ok(ContractVerificationOutcome::Violation {
                error_model: ErrorModel::conflict("Refused", "ContractViolation", None),
            });
        }
        Ok(ContractVerificationOutcome::Clear {})
    }

    async fn check_view_updates(
        &self,
        _view_updates: &[ViewUpdate],
        _current_metadata: &ViewMetadata,
    ) -> Result<ContractVerificationOutcome, ErrorModel> {
        Ok(ContractVerificationOutcome::Clear {})
    }

    async fn check_drop(&self, _: TabularId) -> Result<ContractVerificationOutcome, ErrorModel> {
        Ok(ContractVerificationOutcome::Clear {})
    }

    async fn check_rename(
        &self,
        _: TabularId,
        _: &TableIdent,
    ) -> Result<ContractVerificationOutcome, ErrorModel> {
        Ok(ContractVerificationOutcome::Clear {})
    }
}

/// A transaction that changes `a` and only requires that `main` of `b` did not move fails if
/// another commit moves it after the tables were loaded: `b` is written too, so its change
/// is detected, and the retry finds the requirement broken.
#[sqlx::test]
async fn test_unchanged_table_of_a_transaction_still_conflicts_when_changed_meanwhile(
    pool: PgPool,
) {
    let (mut ctx, ns_params) = setup(pool).await;
    let a = create_table(&ctx, &ns_params, "a", false).await;
    let b = create_table(&ctx, &ns_params, "b", false).await;
    let b = commit(&ctx, &ns_params, "b", vec![], append(&b.metadata, 1))
        .await
        .unwrap();

    let verifier = Arc::new(TableCommitVerifier::default());
    ctx.v1_state.contract_verifiers = ContractVerifiers::new(vec![verifier.clone()]);
    *verifier.commit_meanwhile.lock().unwrap() = Some((
        ctx.clone(),
        table_params(&ns_params, "b"),
        append(&b.metadata, 2),
    ));

    let error = commit_transaction(
        &ctx,
        &ns_params,
        vec![
            ("a", vec![], vec![set_property("v")]),
            ("b", vec![require_main_at(Some(1))], vec![]),
        ],
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap_err();

    assert_eq!(
        (error.error.code, error.error.r#type.as_str()),
        (409, "CatalogCommitConflicts")
    );
    assert!(verifier.commit_meanwhile.lock().unwrap().is_none());
    assert_eq!(
        load(&ctx, &ns_params, "a").await.metadata_location,
        a.metadata_location
    );
    assert_eq!(
        load(&ctx, &ns_params, "b")
            .await
            .metadata
            .current_snapshot_id(),
        Some(2)
    );
}

/// The idempotency key of a commit that changes nothing is recorded, so a retry is a replay.
#[sqlx::test]
async fn test_retry_of_an_unchanged_commit_is_replayed(pool: PgPool) {
    let (ctx, ns_params) = setup(pool.clone()).await;
    let created = create_table(&ctx, &ns_params, "t", false).await;
    let key = new_key();
    let unchanged = || {
        commit_as(
            &ctx,
            table_params(&ns_params, "t"),
            vec![require_main_at(None)],
            vec![],
            with_key(key),
        )
    };

    let first = unchanged().await.unwrap();
    assert_eq!(
        Some(&first.metadata_location),
        created.metadata_location.as_ref()
    );
    let replayed = unchanged().await.unwrap();
    assert_eq!(replayed.metadata_location, first.metadata_location);
    assert_eq!(replayed.etag, first.etag);
    assert_eq!(replayed.metadata, first.metadata);

    // Re-running the commit now would fail its requirement.
    commit(&ctx, &ns_params, "t", vec![], append(&created.metadata, 1))
        .await
        .unwrap();
    let after_append = unchanged().await.unwrap();
    assert_eq!(after_append.metadata.current_snapshot_id(), Some(1));

    let transaction_key = new_key();
    create_table(&ctx, &ns_params, "u", false).await;
    commit_transaction(
        &ctx,
        &ns_params,
        vec![("t", vec![], vec![]), ("u", vec![], vec![])],
        with_key(transaction_key),
    )
    .await
    .unwrap();

    let mut records: Vec<(String, i32)> = sqlx::query_as(
        "SELECT operation, http_status FROM idempotency_record WHERE idempotency_key = ANY($1)",
    )
    .bind(vec![key.as_uuid(), transaction_key.as_uuid()])
    .fetch_all(&pool)
    .await
    .unwrap();
    records.sort();
    assert_eq!(
        records,
        vec![
            (
                EndpointFlat::CatalogV1CommitTransaction.name().to_string(),
                204
            ),
            (EndpointFlat::CatalogV1UpdateTable.name().to_string(), 200),
        ]
    );
}

/// Records the tables of every committed transaction.
#[derive(Debug, Default)]
struct CommitRecorder(Mutex<Vec<Vec<String>>>);

impl std::fmt::Display for CommitRecorder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CommitRecorder")
    }
}

#[async_trait::async_trait]
impl EventListener for CommitRecorder {
    async fn transaction_committed(&self, event: CommitTransactionEvent) -> anyhow::Result<()> {
        let tables = event
            .commits
            .iter()
            .map(|commit| commit.table_info.tabular_ident.name.clone())
            .collect();
        self.0.lock().unwrap().push(tables);
        Ok(())
    }
}

/// Lakekeeper does not call `transaction_committed` for a table commit that changes nothing.
#[sqlx::test]
async fn test_unchanged_commit_does_not_call_transaction_committed(pool: PgPool) {
    let (ctx, ns_params) = setup(pool).await;
    create_table(&ctx, &ns_params, "t", false).await;
    create_table(&ctx, &ns_params, "u", false).await;
    let recorder = Arc::new(CommitRecorder::default());
    ctx.v1_state.events.append(recorder.clone()).await;

    commit(&ctx, &ns_params, "t", vec![], vec![set_property("v")])
        .await
        .unwrap();
    commit(&ctx, &ns_params, "t", vec![], vec![]).await.unwrap();
    commit_transaction(
        &ctx,
        &ns_params,
        vec![("t", vec![], vec![]), ("u", vec![], vec![])],
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    commit_transaction(
        &ctx,
        &ns_params,
        vec![
            ("t", vec![], vec![]),
            ("u", vec![], vec![set_property("v")]),
        ],
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    // The dispatch runs inside the request, so every event is recorded by now.
    assert_eq!(
        *recorder.0.lock().unwrap(),
        vec![
            vec!["t".to_string()],
            vec!["t".to_string(), "u".to_string()]
        ]
    );
}
