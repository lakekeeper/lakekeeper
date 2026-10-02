//! The events a dataset's changes publish, which pipelines trigger on.
//!
//! Each test performs a change and reads back what a listener received: the
//! event, its target, and the fields a consumer keys on.
use std::{sync::Arc, time::Duration};

use iceberg::NamespaceIdent;
use lakekeeper::{
    api::{
        ApiContext,
        data::v1::datasets::{
            CommitDatasetRequest, CommitFile, CreateDatasetRefRequest, DatasetParameters,
            DatasetRefParameters, DatasetRefSource, DatasetService as _, ImportDatasetRequest,
            MoveDatasetRefRequest, UpdateDatasetSettingsRequest,
        },
        iceberg::types::Prefix,
        management::v1::warehouse::TabularDeleteProfile,
    },
    server::CatalogServer,
    service::{
        DatasetConstraints, DatasetRefType, State,
        authz::AllowAllAuthorizer,
        events::{
            CommitDatasetEvent, CreateDatasetRefEvent, DeleteDatasetRefEvent, EventListener,
            MoveDatasetRefEvent, UpdateDatasetSettingsEvent,
        },
    },
};
use lakekeeper_integration_tests::{
    create_dataset, create_ns, memory_io_profile, random_request_metadata, setup,
    spawn_build_in_queues,
};
use lakekeeper_io::LakekeeperStorage as _;
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use uuid::Uuid;

type TestApiContext = ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>;

const DS: &str = "images";

/// A dataset event as a listener saw it.
#[derive(Debug)]
enum Seen {
    Committed(CommitDatasetEvent),
    RefCreated(CreateDatasetRefEvent),
    RefMoved(MoveDatasetRefEvent),
    RefDeleted(DeleteDatasetRefEvent),
    SettingsUpdated(UpdateDatasetSettingsEvent),
}

#[derive(Debug)]
struct Capture(UnboundedSender<Seen>);

impl std::fmt::Display for Capture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Capture")
    }
}

#[async_trait::async_trait]
impl EventListener for Capture {
    async fn dataset_committed(&self, event: CommitDatasetEvent) -> anyhow::Result<()> {
        let _ = self.0.send(Seen::Committed(event));
        Ok(())
    }

    async fn dataset_ref_created(&self, event: CreateDatasetRefEvent) -> anyhow::Result<()> {
        let _ = self.0.send(Seen::RefCreated(event));
        Ok(())
    }

    async fn dataset_ref_moved(&self, event: MoveDatasetRefEvent) -> anyhow::Result<()> {
        let _ = self.0.send(Seen::RefMoved(event));
        Ok(())
    }

    async fn dataset_ref_deleted(&self, event: DeleteDatasetRefEvent) -> anyhow::Result<()> {
        let _ = self.0.send(Seen::RefDeleted(event));
        Ok(())
    }

    async fn dataset_settings_updated(
        &self,
        event: UpdateDatasetSettingsEvent,
    ) -> anyhow::Result<()> {
        let _ = self.0.send(Seen::SettingsUpdated(event));
        Ok(())
    }
}

struct Fixture {
    ctx: TestApiContext,
    prefix: String,
    ns: String,
    location: String,
    events: UnboundedReceiver<Seen>,
}

async fn make_dataset(pool: PgPool) -> Fixture {
    let (ctx, warehouse) = setup(
        pool,
        memory_io_profile(),
        None,
        AllowAllAuthorizer::default(),
        TabularDeleteProfile::Hard {},
        None,
        1,
        None,
    )
    .await;
    let prefix = warehouse.warehouse_id.to_string();
    let ns = format!("ns_{}", Uuid::now_v7());
    create_ns(ctx.clone(), prefix.clone(), ns.clone()).await;
    let created = create_dataset(ctx.clone(), prefix.clone(), ns.clone(), DS)
        .await
        .unwrap();
    let (sender, events) = unbounded_channel();
    ctx.v1_state.events.append(Arc::new(Capture(sender))).await;
    Fixture {
        ctx,
        prefix,
        ns,
        location: created.dataset.location,
        events,
    }
}

impl Fixture {
    fn ds_params(&self) -> DatasetParameters {
        DatasetParameters {
            prefix: Some(Prefix(self.prefix.clone())),
            namespace: NamespaceIdent::new(self.ns.clone()),
            dataset_name: DS.to_string(),
        }
    }

    fn ref_params(&self, r: &str) -> DatasetRefParameters {
        DatasetRefParameters {
            prefix: Some(Prefix(self.prefix.clone())),
            namespace: NamespaceIdent::new(self.ns.clone()),
            dataset_name: DS.to_string(),
            ref_name: r.to_string(),
        }
    }

    async fn next(&mut self) -> Seen {
        tokio::time::timeout(Duration::from_secs(10), self.events.recv())
            .await
            .expect("an event arrives")
            .expect("the channel is open")
    }

    /// Nothing else arrived: events are spawned, so wait a moment first.
    async fn assert_quiet(&mut self) {
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(self.events.try_recv().is_err(), "no further event");
    }

    async fn write(&self, key: &str) {
        // The warehouse's memory profile reads the same thread-local store.
        lakekeeper_io::memory::MemoryStorage::new()
            .write(
                &format!("{}/{key}", self.location),
                bytes::Bytes::from_static(b"x"),
            )
            .await
            .unwrap();
    }
}

fn file(key: &str) -> CommitFile {
    CommitFile {
        logical_key: key.to_string(),
        physical_path: None,
        etag: None,
        size: Some(1),
        content_type: None,
        checksum: None,
        version_id: None,
        last_modified: None,
    }
}

#[sqlx::test]
async fn test_a_commit_announces_its_snapshot(pool: PgPool) {
    let mut ds = make_dataset(pool).await;

    let committed = CatalogServer::commit_dataset(
        ds.ref_params("main"),
        CommitDatasetRequest {
            parent_snapshot_id: None,
            added: vec![file("a.jpg"), file("b.jpg")],
            removed: vec![],
            summary: Some(serde_json::json!({"pipeline-run": "run-1"})),
            on_constraint_violation: None,
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let Seen::Committed(event) = ds.next().await else {
        panic!("a commit event");
    };
    assert_eq!(event.dataset.dataset.name, DS);
    assert_eq!(event.published.branch, "main");
    assert_eq!(event.published.snapshot_id, committed.snapshot_id);
    assert_eq!(event.published.parent_snapshot_id, None);
    assert_eq!(event.published.changes.added, 2);
    assert_eq!(
        event.published.summary,
        Some(serde_json::json!({"pipeline-run": "run-1"}))
    );
    ds.assert_quiet().await;
}

#[sqlx::test]
async fn test_ref_operations_announce_themselves(pool: PgPool) {
    let mut ds = make_dataset(pool).await;
    let first = CatalogServer::commit_dataset(
        ds.ref_params("main"),
        CommitDatasetRequest {
            parent_snapshot_id: None,
            added: vec![file("a.jpg")],
            removed: vec![],
            summary: None,
            on_constraint_violation: None,
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap()
    .snapshot_id;
    let _ = ds.next().await;

    CatalogServer::create_dataset_ref(
        ds.ds_params(),
        CreateDatasetRefRequest {
            name: "v1".to_string(),
            typ: DatasetRefType::Tag,
            source: DatasetRefSource::Ref {
                name: "main".to_string(),
            },
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let Seen::RefCreated(created) = ds.next().await else {
        panic!("a ref creation event");
    };
    assert_eq!(created.dataset_ref.name, "v1");
    assert_eq!(created.dataset_ref.typ, DatasetRefType::Tag);
    assert_eq!(created.dataset_ref.snapshot_id, Some(first));

    CatalogServer::create_dataset_ref(
        ds.ds_params(),
        CreateDatasetRefRequest {
            name: "exp".to_string(),
            typ: DatasetRefType::Branch,
            source: DatasetRefSource::Ref {
                name: "main".to_string(),
            },
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let _ = ds.next().await;
    CatalogServer::move_dataset_ref(
        ds.ref_params("exp"),
        MoveDatasetRefRequest {
            snapshot_id: first,
            expected_snapshot_id: Some(first),
            fast_forward: false,
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let Seen::RefMoved(moved) = ds.next().await else {
        panic!("a ref move event");
    };
    assert_eq!(moved.dataset_ref.name, "exp");
    assert!(!moved.fast_forward, "a reset says so");

    CatalogServer::delete_dataset_ref(
        ds.ref_params("exp"),
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let Seen::RefDeleted(deleted) = ds.next().await else {
        panic!("a ref deletion event");
    };
    assert_eq!(deleted.ref_name, "exp");
    ds.assert_quiet().await;
}

#[sqlx::test]
async fn test_a_settings_update_is_announced(pool: PgPool) {
    let mut ds = make_dataset(pool).await;
    let constraints = DatasetConstraints {
        allowed_content_types: Some(vec!["image/jpeg".to_string()]),
        max_file_size: None,
    };

    CatalogServer::update_dataset_settings(
        ds.ds_params(),
        UpdateDatasetSettingsRequest {
            constraints: Some(constraints.clone()),
            retention: None,
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let Seen::SettingsUpdated(event) = ds.next().await else {
        panic!("a settings event");
    };
    assert_eq!(event.request.constraints, Some(constraints.clone()));
    // Resolved after the write, so it shows the dataset as the update left it.
    assert_eq!(event.dataset.dataset.constraints, constraints);
}

#[sqlx::test]
async fn test_an_import_announces_what_it_published_and_a_no_op_nothing(pool: PgPool) {
    let mut ds = make_dataset(pool).await;
    ds.write("a.jpg").await;
    ds.write("b.jpg").await;

    let imported = CatalogServer::import_dataset(
        ds.ds_params(),
        ImportDatasetRequest::default(),
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let Seen::Committed(event) = ds.next().await else {
        panic!("a commit event for the import");
    };
    assert_eq!(Some(event.published.snapshot_id), imported.snapshot_id);
    assert_eq!(event.published.changes.added, 2);

    CatalogServer::import_dataset(
        ds.ds_params(),
        ImportDatasetRequest::default(),
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    ds.assert_quiet().await;
}

#[sqlx::test]
async fn test_a_queued_import_announces_itself_as_lakekeeper(pool: PgPool) {
    let mut ds = make_dataset(pool).await;
    ds.write("a.jpg").await;
    let cancellation = lakekeeper::CancellationToken::new();
    let workers = spawn_build_in_queues(
        &ds.ctx,
        Some(Duration::from_millis(50)),
        cancellation.clone(),
    )
    .await;

    CatalogServer::import_dataset(
        ds.ds_params(),
        ImportDatasetRequest {
            queued: Some(true),
            ..Default::default()
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let Seen::Committed(event) = ds.next().await else {
        panic!("a commit event for the queued import");
    };
    assert_eq!(event.published.changes.added, 1);
    assert!(
        event.request_metadata.is_lakekeeper_internal(),
        "the worker acts as Lakekeeper, not as the caller"
    );

    cancellation.cancel();
    let _ = workers.await;
}
