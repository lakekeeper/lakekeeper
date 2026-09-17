//! Importing over files a commit named apart from their storage path.
//!
//! A commit may record `train/cat.jpg` with its bytes at `raw/0001.jpg`. A listing
//! of the prefix shows `raw/0001.jpg`; the import must recognise it as that file,
//! by where its bytes are, and neither register it a second time under its
//! storage path nor take the file for gone because nothing is stored under its
//! name.
use lakekeeper::{
    api::data::v1::datasets::{
        DatasetFile, DatasetService as _, ImportDatasetRequest, ImportDatasetResponse, ImportMode,
        ListDatasetFilesQuery,
    },
    server::CatalogServer,
    service::DatasetSnapshotId,
};
use lakekeeper_integration_tests::{
    CommitFileExt as _, TestDataset, file, random_request_metadata,
};
use sqlx::PgPool;

/// Commit `key` with its bytes at `physical_path`, on top of `main`'s head.
async fn commit_renamed(ds: &TestDataset, key: &str, physical_path: &str) {
    ds.commit(
        ds.head("main").await,
        vec![file(key).at(physical_path)],
        &[],
    )
    .await;
}

async fn import(ds: &TestDataset, mode: ImportMode) -> ImportDatasetResponse {
    CatalogServer::import_dataset(
        ds.params(),
        ImportDatasetRequest {
            mode: Some(mode),
            ..Default::default()
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap()
}

/// `main`'s head, absent while it has no commits, and its files.
async fn files(ds: &TestDataset) -> (Option<DatasetSnapshotId>, Vec<DatasetFile>) {
    let listed = CatalogServer::list_dataset_files(
        ds.ref_params("main"),
        ListDatasetFilesQuery::default(),
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    (listed.snapshot_id, listed.files)
}

/// `main`'s files as `(name, where its bytes are)`.
async fn names(ds: &TestDataset) -> Vec<(String, String)> {
    files(ds)
        .await
        .1
        .into_iter()
        .map(|f| {
            let path = f
                .physical_path
                .strip_prefix(&format!("{}/", ds.location.trim_end_matches('/')))
                .unwrap_or(&f.physical_path)
                .to_string();
            (f.logical_key, path)
        })
        .collect()
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
    let ds = TestDataset::imported(pool).await;
    ds.write("raw/0001.jpg", b"x").await;
    commit_renamed(&ds, "train/cat.jpg", "raw/0001.jpg").await;

    let imported = import(&ds, ImportMode::AddOnly).await;
    assert_eq!(imported.imported, 0);
    assert_eq!(
        names(&ds).await,
        named(&[("train/cat.jpg", "raw/0001.jpg")])
    );
}

/// A sync keeps a renamed file whose object is there: nothing is stored under its
/// name, but its bytes are where it says.
#[sqlx::test]
async fn test_a_sync_keeps_a_renamed_file_whose_bytes_are_there(pool: PgPool) {
    let ds = TestDataset::imported(pool).await;
    ds.write("raw/0001.jpg", b"x").await;
    commit_renamed(&ds, "train/cat.jpg", "raw/0001.jpg").await;

    let synced = import(&ds, ImportMode::Sync).await;
    assert_eq!((synced.imported, synced.removed), (0, 0));
    assert_eq!(
        names(&ds).await,
        named(&[("train/cat.jpg", "raw/0001.jpg")])
    );
}

/// An object written again is the renamed file's new bytes: recorded as modified
/// under the file's own name, still pointing where it did.
#[sqlx::test]
async fn test_a_sync_records_a_renamed_files_new_bytes_under_its_name(pool: PgPool) {
    let ds = TestDataset::imported(pool).await;
    ds.write("raw/0001.jpg", b"x").await;
    commit_renamed(&ds, "train/cat.jpg", "raw/0001.jpg").await;
    ds.write("raw/0001.jpg", b"written again").await;

    let synced = import(&ds, ImportMode::Sync).await;
    assert_eq!((synced.imported, synced.modified), (0, 1));
    let (_, files) = files(&ds).await;
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].logical_key, "train/cat.jpg");
    assert_eq!(files[0].size, Some(13));
    assert_eq!(
        names(&ds).await,
        named(&[("train/cat.jpg", "raw/0001.jpg")])
    );
}

/// A sync removes a renamed file whose object is gone.
#[sqlx::test]
async fn test_a_sync_removes_a_renamed_file_whose_bytes_are_gone(pool: PgPool) {
    let ds = TestDataset::imported(pool).await;
    ds.write("raw/0001.jpg", b"x").await;
    ds.write("raw/0002.jpg", b"x").await;
    commit_renamed(&ds, "train/cat.jpg", "raw/0001.jpg").await;
    import(&ds, ImportMode::Sync).await;
    assert_eq!(
        names(&ds).await,
        named(&[
            ("raw/0002.jpg", "raw/0002.jpg"),
            ("train/cat.jpg", "raw/0001.jpg"),
        ]),
        "kept under its name while its bytes are there"
    );
    ds.delete("raw/0001.jpg").await;

    let synced = import(&ds, ImportMode::Sync).await;
    assert_eq!(synced.removed, 1);
    assert_eq!(names(&ds).await, named(&[("raw/0002.jpg", "raw/0002.jpg")]));
}

/// An object stored under the name a renamed file goes by is another object: the
/// file keeps its name and its bytes, and the object is reported as left out.
#[sqlx::test]
async fn test_an_object_under_a_renamed_files_name_is_left_out(pool: PgPool) {
    let ds = TestDataset::imported(pool).await;
    ds.write("raw/0001.jpg", b"x").await;
    ds.write("train/cat.jpg", b"another object").await;
    commit_renamed(&ds, "train/cat.jpg", "raw/0001.jpg").await;

    let imported = import(&ds, ImportMode::Sync).await;
    assert_eq!(imported.imported, 0);
    assert_eq!(imported.skipped, 1);
    assert_eq!(imported.skipped_files[0].logical_key, "train/cat.jpg");
    assert_eq!(
        names(&ds).await,
        named(&[("train/cat.jpg", "raw/0001.jpg")])
    );
}

/// One object can be a file under its storage path and another under a name of
/// its own: a sync keeps both.
#[sqlx::test]
async fn test_a_sync_keeps_an_object_known_under_two_names(pool: PgPool) {
    let ds = TestDataset::imported(pool).await;
    ds.write("raw/0001.jpg", b"x").await;
    import(&ds, ImportMode::AddOnly).await;
    commit_renamed(&ds, "train/cat.jpg", "raw/0001.jpg").await;

    let synced = import(&ds, ImportMode::Sync).await;
    assert_eq!((synced.imported, synced.removed), (0, 0));
    assert_eq!(
        names(&ds).await,
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
    let ds = TestDataset::imported(pool).await;
    ds.write("raw/0001.jpg", b"x").await;
    commit_renamed(&ds, "train/cat.jpg", "raw/0001.jpg").await;
    ds.write("train/cat.jpg", b"x").await;
    ds.delete("raw/0001.jpg").await;

    let synced = import(&ds, ImportMode::Sync).await;
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
        names(&ds).await,
        named(&[("train/cat.jpg", "train/cat.jpg")])
    );
}

/// Renamed files are judged a page at a time, past the first page too.
#[sqlx::test]
async fn test_a_sync_judges_renamed_files_past_one_page(pool: PgPool) {
    const FILES: usize = 1_001;
    let ds = TestDataset::imported(pool).await;
    let added = (0..FILES)
        .map(|i| file(&format!("train/{i:04}.jpg")).at(format!("raw/{i:04}.jpg")))
        .collect();
    for i in 0..FILES {
        ds.write(&format!("raw/{i:04}.jpg"), b"x").await;
    }
    ds.commit(None, added, &[]).await;
    ds.delete(&format!("raw/{:04}.jpg", FILES - 1)).await;

    // The file whose bytes are gone sorts last, on the second page.
    let synced = import(&ds, ImportMode::Sync).await;
    assert_eq!((synced.imported, synced.removed), (0, 1), "{synced:?}");
}
