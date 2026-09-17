//! Importing over files a commit named apart from their storage path.
//!
//! A commit may record `train/cat.jpg` with its bytes at `raw/0001.jpg`. A listing
//! of the prefix shows `raw/0001.jpg`; the import must recognise it as that file,
//! by where its bytes are, and neither register it a second time under its
//! storage path nor take the file for gone because nothing is stored under its
//! name.
use iceberg::NamespaceIdent;
use lakekeeper::{
    api::{
        ApiContext,
        data::v1::datasets::{
            CommitDatasetRequest, CommitFile, DatasetFile, DatasetParameters, DatasetRefParameters,
            DatasetService as _, ImportDatasetRequest, ImportDatasetResponse, ImportMode,
            ListDatasetFilesQuery,
        },
        iceberg::types::Prefix,
        management::v1::warehouse::TabularDeleteProfile,
    },
    server::CatalogServer,
    service::{DatasetSnapshotId, State, authz::AllowAllAuthorizer},
};
use lakekeeper_integration_tests::{
    create_dataset, create_ns, memory_io_profile, random_request_metadata, setup,
};
use lakekeeper_io::LakekeeperStorage as _;
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;
use uuid::Uuid;

type TestApiContext = ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>;

const DS: &str = "images";

struct Fixture {
    ctx: TestApiContext,
    prefix: String,
    ns: String,
    location: String,
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
    Fixture {
        ctx,
        prefix,
        ns,
        location: created.dataset.location,
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

    fn main_ref(&self) -> DatasetRefParameters {
        DatasetRefParameters {
            prefix: Some(Prefix(self.prefix.clone())),
            namespace: NamespaceIdent::new(self.ns.clone()),
            dataset_name: DS.to_string(),
            ref_name: "main".to_string(),
        }
    }

    async fn write(&self, key: &str, bytes: &'static [u8]) {
        // The warehouse's memory profile reads the same thread-local store.
        lakekeeper_io::memory::MemoryStorage::new()
            .write(
                &format!("{}/{key}", self.location),
                bytes::Bytes::from_static(bytes),
            )
            .await
            .unwrap();
    }

    async fn delete(&self, key: &str) {
        lakekeeper_io::memory::MemoryStorage::new()
            .delete(&format!("{}/{key}", self.location))
            .await
            .unwrap();
    }

    /// Commit `key` with its bytes at `physical_path`, on top of `main`'s head.
    async fn commit_renamed(&self, key: &str, physical_path: &str) {
        let (parent, _) = self.files().await;
        CatalogServer::commit_dataset(
            self.main_ref(),
            CommitDatasetRequest {
                parent_snapshot_id: parent,
                added: vec![CommitFile {
                    logical_key: key.to_string(),
                    physical_path: Some(physical_path.to_string()),
                    etag: None,
                    size: Some(1),
                    content_type: None,
                    checksum: None,
                    version_id: None,
                    last_modified: None,
                }],
                removed: vec![],
                summary: None,
                on_constraint_violation: None,
            },
            self.ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();
    }

    async fn import(&self, mode: ImportMode) -> ImportDatasetResponse {
        CatalogServer::import_dataset(
            self.ds_params(),
            ImportDatasetRequest {
                mode: Some(mode),
                ..Default::default()
            },
            self.ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap()
    }

    /// `main`'s head, absent while it has no commits, and its files.
    async fn files(&self) -> (Option<DatasetSnapshotId>, Vec<DatasetFile>) {
        let listed = CatalogServer::list_dataset_files(
            self.main_ref(),
            ListDatasetFilesQuery::default(),
            self.ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();
        (listed.snapshot_id, listed.files)
    }

    /// `main`'s files as `(name, where its bytes are)`.
    async fn names(&self) -> Vec<(String, String)> {
        self.files()
            .await
            .1
            .into_iter()
            .map(|f| {
                let path = f
                    .physical_path
                    .strip_prefix(&format!("{}/", self.location.trim_end_matches('/')))
                    .unwrap_or(&f.physical_path)
                    .to_string();
                (f.logical_key, path)
            })
            .collect()
    }
}

fn named(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(name, path)| ((*name).to_string(), (*path).to_string()))
        .collect()
}

/// An add-only import does not register a renamed file's object a second time
/// under its storage path.
#[sqlx::test]
async fn test_an_import_does_not_register_a_renamed_file_twice(pool: PgPool) {
    let ds = make_dataset(pool).await;
    ds.write("raw/0001.jpg", b"x").await;
    ds.commit_renamed("train/cat.jpg", "raw/0001.jpg").await;

    let imported = ds.import(ImportMode::AddOnly).await;
    assert_eq!(imported.imported, 0);
    assert_eq!(
        ds.names().await,
        named(&[("train/cat.jpg", "raw/0001.jpg")])
    );
}

/// A sync keeps a renamed file whose object is there: nothing is stored under its
/// name, but its bytes are where it says.
#[sqlx::test]
async fn test_a_sync_keeps_a_renamed_file_whose_bytes_are_there(pool: PgPool) {
    let ds = make_dataset(pool).await;
    ds.write("raw/0001.jpg", b"x").await;
    ds.commit_renamed("train/cat.jpg", "raw/0001.jpg").await;

    let synced = ds.import(ImportMode::Sync).await;
    assert_eq!((synced.imported, synced.removed), (0, 0));
    assert_eq!(
        ds.names().await,
        named(&[("train/cat.jpg", "raw/0001.jpg")])
    );
}

/// An object written again is the renamed file's new bytes: recorded as modified
/// under the file's own name, still pointing where it did.
#[sqlx::test]
async fn test_a_sync_records_a_renamed_files_new_bytes_under_its_name(pool: PgPool) {
    let ds = make_dataset(pool).await;
    ds.write("raw/0001.jpg", b"x").await;
    ds.commit_renamed("train/cat.jpg", "raw/0001.jpg").await;
    ds.write("raw/0001.jpg", b"written again").await;

    let synced = ds.import(ImportMode::Sync).await;
    assert_eq!((synced.imported, synced.modified), (0, 1));
    let (_, files) = ds.files().await;
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].logical_key, "train/cat.jpg");
    assert_eq!(files[0].size, Some(13));
    assert_eq!(
        ds.names().await,
        named(&[("train/cat.jpg", "raw/0001.jpg")])
    );
}

/// A sync removes a renamed file whose object is gone.
#[sqlx::test]
async fn test_a_sync_removes_a_renamed_file_whose_bytes_are_gone(pool: PgPool) {
    let ds = make_dataset(pool).await;
    ds.write("raw/0001.jpg", b"x").await;
    ds.write("raw/0002.jpg", b"x").await;
    ds.commit_renamed("train/cat.jpg", "raw/0001.jpg").await;
    ds.import(ImportMode::Sync).await;
    assert_eq!(
        ds.names().await,
        named(&[
            ("raw/0002.jpg", "raw/0002.jpg"),
            ("train/cat.jpg", "raw/0001.jpg"),
        ]),
        "kept under its name while its bytes are there"
    );
    ds.delete("raw/0001.jpg").await;

    let synced = ds.import(ImportMode::Sync).await;
    assert_eq!(synced.removed, 1);
    assert_eq!(ds.names().await, named(&[("raw/0002.jpg", "raw/0002.jpg")]));
}

/// An object stored under the name a renamed file goes by is another object: the
/// file keeps its name and its bytes, and the object is reported as left out.
#[sqlx::test]
async fn test_an_object_under_a_renamed_files_name_is_left_out(pool: PgPool) {
    let ds = make_dataset(pool).await;
    ds.write("raw/0001.jpg", b"x").await;
    ds.write("train/cat.jpg", b"another object").await;
    ds.commit_renamed("train/cat.jpg", "raw/0001.jpg").await;

    let imported = ds.import(ImportMode::Sync).await;
    assert_eq!(imported.imported, 0);
    assert_eq!(imported.skipped, 1);
    assert_eq!(imported.skipped_files[0].logical_key, "train/cat.jpg");
    assert_eq!(
        ds.names().await,
        named(&[("train/cat.jpg", "raw/0001.jpg")])
    );
}

/// One object can be a file under its storage path and another under a name of
/// its own: a sync keeps both.
#[sqlx::test]
async fn test_a_sync_keeps_an_object_known_under_two_names(pool: PgPool) {
    let ds = make_dataset(pool).await;
    ds.write("raw/0001.jpg", b"x").await;
    ds.import(ImportMode::AddOnly).await;
    ds.commit_renamed("train/cat.jpg", "raw/0001.jpg").await;

    let synced = ds.import(ImportMode::Sync).await;
    assert_eq!((synced.imported, synced.removed), (0, 0));
    assert_eq!(
        ds.names().await,
        named(&[
            ("raw/0001.jpg", "raw/0001.jpg"),
            ("train/cat.jpg", "raw/0001.jpg"),
        ])
    );
}

/// A renamed file whose bytes moved under its own name is that object now: a sync
/// points the file at it, removes nothing and leaves nothing out.
#[sqlx::test]
async fn test_a_sync_follows_a_renamed_files_bytes_to_its_name(pool: PgPool) {
    let ds = make_dataset(pool).await;
    ds.write("raw/0001.jpg", b"x").await;
    ds.commit_renamed("train/cat.jpg", "raw/0001.jpg").await;
    ds.write("train/cat.jpg", b"x").await;
    ds.delete("raw/0001.jpg").await;

    let synced = ds.import(ImportMode::Sync).await;
    assert_eq!(
        (
            synced.imported,
            synced.modified,
            synced.removed,
            synced.skipped
        ),
        (0, 1, 0, 0),
        "{synced:?}"
    );
    assert_eq!(
        ds.names().await,
        named(&[("train/cat.jpg", "train/cat.jpg")])
    );
}

/// Renamed files are judged a page at a time, past the first page too.
#[sqlx::test]
async fn test_a_sync_judges_renamed_files_past_one_page(pool: PgPool) {
    const FILES: usize = 1_001;
    let ds = make_dataset(pool).await;
    let added: Vec<CommitFile> = (0..FILES)
        .map(|i| CommitFile {
            logical_key: format!("train/{i:04}.jpg"),
            physical_path: Some(format!("raw/{i:04}.jpg")),
            etag: None,
            size: Some(1),
            content_type: None,
            checksum: None,
            version_id: None,
            last_modified: None,
        })
        .collect();
    for i in 0..FILES {
        ds.write(&format!("raw/{i:04}.jpg"), b"x").await;
    }
    CatalogServer::commit_dataset(
        ds.main_ref(),
        CommitDatasetRequest {
            parent_snapshot_id: None,
            added,
            removed: vec![],
            summary: None,
            on_constraint_violation: None,
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    ds.delete(&format!("raw/{:04}.jpg", FILES - 1)).await;

    // The file whose bytes are gone sorts last, on the second page.
    let synced = ds.import(ImportMode::Sync).await;
    assert_eq!((synced.imported, synced.removed), (0, 1), "{synced:?}");
}
