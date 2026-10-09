//! The events a dataset's changes publish, which pipelines trigger on.
//!
//! Each test performs a change and reads back what a listener received: the
//! event, its target, and the fields a consumer keys on.
use std::{fmt, sync::Arc, time::Duration};

use lakekeeper::{
    CancellationToken,
    api::data::v1::datasets::{
        CommitDatasetRequest, CreateDatasetRefRequest, DatasetRefSource, DatasetService as _,
        ImportDatasetRequest, MoveDatasetRefRequest, UpdateDatasetSettingsRequest,
    },
    server::CatalogServer,
    service::{
        DatasetConstraints, DatasetRefType,
        events::{
            CommitDatasetEvent, CreateDatasetRefEvent, DeleteDatasetRefEvent, EventListener,
            MoveDatasetRefEvent, UpdateDatasetSettingsEvent,
        },
    },
};
use lakekeeper_integration_tests::{
    DATASET, TestDataset, file, random_request_metadata, spawn_build_in_queues,
};
use sqlx::PgPool;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

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

impl fmt::Display for Capture {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

/// `ds`, and the dataset events published from now on.
async fn listening(ds: TestDataset) -> (TestDataset, UnboundedReceiver<Seen>) {
    let (sender, events) = unbounded_channel();
    ds.ctx
        .v1_state
        .events
        .append(Arc::new(Capture(sender)))
        .await;
    (ds, events)
}

async fn next(events: &mut UnboundedReceiver<Seen>) -> Seen {
    tokio::time::timeout(Duration::from_secs(10), events.recv())
        .await
        .expect("an event arrives")
        .expect("the channel is open")
}

/// Nothing else arrived: events are spawned, so wait a moment first.
async fn assert_quiet(events: &mut UnboundedReceiver<Seen>) {
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(events.try_recv().is_err(), "no further event");
}

#[sqlx::test]
async fn test_a_commit_announces_its_snapshot(pool: PgPool) {
    let (ds, mut events) = listening(TestDataset::managed(pool).await).await;

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

    let Seen::Committed(event) = next(&mut events).await else {
        panic!("a commit event");
    };
    assert_eq!(event.dataset.dataset.name, DATASET);
    assert_eq!(event.published.branch, "main");
    assert_eq!(event.published.snapshot_id, committed.snapshot_id);
    assert_eq!(event.published.parent_snapshot_id, None);
    assert_eq!(event.published.changes.added, 2);
    assert_eq!(
        event.published.summary,
        Some(serde_json::json!({"pipeline-run": "run-1"}))
    );
    assert_quiet(&mut events).await;
}

#[sqlx::test]
async fn test_ref_operations_announce_themselves(pool: PgPool) {
    let (ds, mut events) = listening(TestDataset::managed(pool).await).await;
    let first = ds.commit(None, vec![file("a.jpg")], &[]).await;
    let _ = next(&mut events).await;

    CatalogServer::create_dataset_ref(
        ds.params(),
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
    let Seen::RefCreated(created) = next(&mut events).await else {
        panic!("a ref creation event");
    };
    assert_eq!(created.dataset_ref.name, "v1");
    assert_eq!(created.dataset_ref.typ, DatasetRefType::Tag);
    assert_eq!(created.dataset_ref.snapshot_id, Some(first));

    CatalogServer::create_dataset_ref(
        ds.params(),
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
    let _ = next(&mut events).await;
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
    let Seen::RefMoved(moved) = next(&mut events).await else {
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
    let Seen::RefDeleted(deleted) = next(&mut events).await else {
        panic!("a ref deletion event");
    };
    assert_eq!(deleted.ref_name, "exp");
    assert_quiet(&mut events).await;
}

#[sqlx::test]
async fn test_a_settings_update_is_announced(pool: PgPool) {
    let (ds, mut events) = listening(TestDataset::managed(pool).await).await;
    let constraints = DatasetConstraints {
        allowed_content_types: Some(vec!["image/jpeg".to_string()]),
        max_file_size: None,
    };

    CatalogServer::update_dataset_settings(
        ds.params(),
        UpdateDatasetSettingsRequest {
            constraints: Some(constraints.clone()),
            retention: None,
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let Seen::SettingsUpdated(event) = next(&mut events).await else {
        panic!("a settings event");
    };
    assert_eq!(event.request.constraints, Some(constraints.clone()));
    // Resolved after the write, so it shows the dataset as the update left it.
    assert_eq!(event.dataset.dataset.constraints, constraints);
}

#[sqlx::test]
async fn test_an_import_announces_what_it_published_and_a_no_op_nothing(pool: PgPool) {
    let (ds, mut events) = listening(TestDataset::imported(pool).await).await;
    ds.write("a.jpg", b"x").await;
    ds.write("b.jpg", b"x").await;

    let imported = CatalogServer::import_dataset(
        ds.params(),
        ImportDatasetRequest::default(),
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let Seen::Committed(event) = next(&mut events).await else {
        panic!("a commit event for the import");
    };
    assert_eq!(Some(event.published.snapshot_id), imported.snapshot_id);
    assert_eq!(event.published.changes.added, 2);

    CatalogServer::import_dataset(
        ds.params(),
        ImportDatasetRequest::default(),
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert_quiet(&mut events).await;
}

#[sqlx::test]
async fn test_a_queued_import_announces_itself_as_lakekeeper(pool: PgPool) {
    let (ds, mut events) = listening(TestDataset::imported(pool).await).await;
    ds.write("a.jpg", b"x").await;
    let cancellation = CancellationToken::new();
    let workers = spawn_build_in_queues(
        &ds.ctx,
        Some(Duration::from_millis(50)),
        cancellation.clone(),
    )
    .await;

    CatalogServer::import_dataset(
        ds.params(),
        ImportDatasetRequest {
            queued: Some(true),
            ..Default::default()
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let Seen::Committed(event) = next(&mut events).await else {
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
