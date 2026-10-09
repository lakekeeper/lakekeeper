//! Comparing two versions of a dataset.
//!
//! The property test is the one to trust: a diff between any two snapshots must
//! match what replaying the commits between them did, across checkpoints and
//! page boundaries.
use std::collections::{BTreeMap, BTreeSet};

use http::StatusCode;
use lakekeeper::{
    api::{
        Result,
        data::v1::datasets::{
            CreateDatasetRefRequest, DatasetFileChangeKind, DatasetRefSource, DatasetService as _,
            DiffDatasetQuery, DiffDatasetResponse,
        },
    },
    server::CatalogServer,
    service::{DatasetRefType, DatasetSnapshotId},
};
use lakekeeper_integration_tests::{
    CommitFileExt as _, TestDataset, file, random_request_metadata,
};
use sqlx::PgPool;
use uuid::Uuid;

/// The whole of a diff, every page read, as `(key, change)` in the order returned.
type Changes = Vec<(String, DatasetFileChangeKind)>;

async fn branch(ds: &TestDataset, name: &str, from: &str) {
    CatalogServer::create_dataset_ref(
        ds.params(),
        CreateDatasetRefRequest {
            name: name.to_string(),
            typ: DatasetRefType::Branch,
            source: DatasetRefSource::Ref {
                name: from.to_string(),
            },
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
}

async fn diff_page(ds: &TestDataset, query: DiffDatasetQuery) -> Result<DiffDatasetResponse> {
    CatalogServer::diff_dataset(
        ds.params(),
        query,
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
}

/// Every page of the diff `query` starts, read to the end.
async fn diff(ds: &TestDataset, query: DiffDatasetQuery) -> Changes {
    let mut changes = Vec::new();
    let mut query = query;
    loop {
        let page = diff_page(ds, query.clone()).await.expect("diff page");
        changes.extend(page.changes.into_iter().map(|c| (c.logical_key, c.change)));
        match page.next_page_token {
            Some(token) => query.page_token = Some(token),
            None => return changes,
        }
    }
}

fn refs(from: &str, to: &str) -> DiffDatasetQuery {
    DiffDatasetQuery {
        from: Some(from.to_string()),
        to: Some(to.to_string()),
        ..DiffDatasetQuery::default()
    }
}

fn snapshots(from: DatasetSnapshotId, to: DatasetSnapshotId, page_size: i64) -> DiffDatasetQuery {
    DiffDatasetQuery {
        from_snapshot_id: Some(from),
        to_snapshot_id: Some(to),
        page_size: Some(page_size),
        ..DiffDatasetQuery::default()
    }
}

/// The review before a promote: what the branch adds, removes and rewrites
/// against `main`, in key order, and the reverse diff mirrors it.
#[sqlx::test]
async fn test_a_diff_names_what_was_added_removed_and_changed(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;
    let base = ds
        .commit(
            None,
            vec![
                file("a").etag("1"),
                file("b").etag("1"),
                file("c").etag("1"),
            ],
            &[],
        )
        .await;
    branch(&ds, "incoming", "main").await;
    ds.commit_to(
        "incoming",
        Some(base),
        vec![file("b").etag("2"), file("d").etag("1")],
        &["a"],
    )
    .await;

    assert_eq!(
        diff(&ds, refs("main", "incoming")).await,
        [
            ("a".to_string(), DatasetFileChangeKind::Removed),
            ("b".to_string(), DatasetFileChangeKind::Modified),
            ("d".to_string(), DatasetFileChangeKind::Added)
        ]
    );
    assert_eq!(
        diff(&ds, refs("incoming", "main")).await,
        [
            ("a".to_string(), DatasetFileChangeKind::Added),
            ("b".to_string(), DatasetFileChangeKind::Modified),
            ("d".to_string(), DatasetFileChangeKind::Removed)
        ]
    );
    assert_eq!(diff(&ds, refs("main", "main")).await, []);

    let page = diff_page(&ds, refs("main", "incoming")).await.unwrap();
    let modified = page
        .changes
        .iter()
        .find(|c| c.change == DatasetFileChangeKind::Modified)
        .unwrap();
    assert_eq!(modified.from.as_ref().unwrap().etag.as_deref(), Some("1"));
    assert_eq!(modified.to.as_ref().unwrap().etag.as_deref(), Some("2"));
}

/// Before the first commit `main` names no snapshot: the diff is empty, and says
/// so without inventing one.
#[sqlx::test]
async fn test_a_diff_of_a_dataset_with_no_commits_is_empty(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;

    let page = diff_page(&ds, refs("main", "main")).await.unwrap();
    assert_eq!(page.from_snapshot_id, None);
    assert_eq!(page.to_snapshot_id, None);
    assert!(page.changes.is_empty());
    assert_eq!(page.next_page_token, None);
}

/// Pages continue the diff their first page resolved, whatever lands on the refs
/// meanwhile, and a token cannot be spent on a different comparison.
#[sqlx::test]
async fn test_a_diff_pages_are_pinned_to_their_snapshots(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;
    let base = ds.commit(None, vec![file("a").etag("1")], &[]).await;
    branch(&ds, "incoming", "main").await;
    let added: Vec<_> = (0..25)
        .map(|i| file(&format!("k{i:02}")).etag("1"))
        .collect();
    let head = ds.commit_to("incoming", Some(base), added, &[]).await;

    let mut query = refs("main", "incoming");
    query.page_size = Some(10);
    let first = diff_page(&ds, query.clone()).await.unwrap();
    assert_eq!(first.changes.len(), 10);
    let token = first.next_page_token.expect("more pages");

    // Lands mid-walk; the walk must not see it.
    ds.commit_to("incoming", Some(head), vec![file("zz").etag("1")], &[])
        .await;
    let mut rest = query.clone();
    rest.page_token = Some(token.clone());
    let mut keys: Vec<String> = first.changes.into_iter().map(|c| c.logical_key).collect();
    keys.extend(diff(&ds, rest).await.into_iter().map(|(key, _)| key));
    let expected: Vec<String> = (0..25).map(|i| format!("k{i:02}")).collect();
    assert_eq!(keys, expected);

    let mut other = refs("incoming", "main");
    other.page_token = Some(token);
    let err = diff_page(&ds, other).await.unwrap_err();
    assert_eq!(err.error.code, StatusCode::BAD_REQUEST, "{err:?}");
}

/// Each side is one ref or one snapshot; a snapshot of no dataset is not found.
#[sqlx::test]
async fn test_a_diff_names_each_side_once(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;
    let head = ds.commit(None, vec![file("a").etag("1")], &[]).await;

    for query in [
        DiffDatasetQuery {
            to: Some("main".to_string()),
            ..DiffDatasetQuery::default()
        },
        DiffDatasetQuery {
            from: Some("main".to_string()),
            from_snapshot_id: Some(head),
            to: Some("main".to_string()),
            ..DiffDatasetQuery::default()
        },
    ] {
        let err = diff_page(&ds, query).await.unwrap_err();
        assert_eq!(err.error.code, StatusCode::BAD_REQUEST, "{err:?}");
        assert_eq!(err.error.r#type, "InvalidDiffRequest");
    }

    let err = diff_page(
        &ds,
        snapshots(head, DatasetSnapshotId::from(Uuid::now_v7()), 10),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, StatusCode::NOT_FOUND, "{err:?}");
}

/// Two large, mostly equal manifests: a request compares a bounded number of
/// keys, so a change past the bound arrives on a later page.
#[sqlx::test]
async fn test_a_diff_of_mostly_equal_manifests_continues_past_empty_pages(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;
    let shared: Vec<_> = (0..12_000)
        .map(|i| file(&format!("k{i:05}")).etag("1"))
        .collect();
    let base = ds.commit(None, shared, &[]).await;
    branch(&ds, "incoming", "main").await;
    ds.commit_to("incoming", Some(base), vec![file("k11999").etag("2")], &[])
        .await;

    let first = diff_page(&ds, refs("main", "incoming")).await.unwrap();
    assert!(first.changes.is_empty(), "{:?}", first.changes);
    assert!(first.next_page_token.is_some(), "an empty page continues");
    assert_eq!(
        diff(&ds, refs("main", "incoming")).await,
        [("k11999".to_string(), DatasetFileChangeKind::Modified)]
    );
}

/// Deterministic xorshift, so a failure replays.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: usize) -> usize {
        usize::try_from(self.next() % u64::try_from(n).unwrap()).unwrap()
    }
}

/// A diff between any two snapshots matches what replaying the commits between
/// them did: seeded random adds, rewrites and removals over a small key pool, so
/// keys are removed and come back, with checkpoint folds along the chain.
#[sqlx::test]
async fn test_a_diff_matches_replaying_the_commits(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let mut state: BTreeMap<String, String> = BTreeMap::new();
    let mut history: Vec<(DatasetSnapshotId, BTreeMap<String, String>)> = Vec::new();
    let mut parent = None;
    for commit in 0..45 {
        let mut added = Vec::new();
        let mut removed = Vec::new();
        let mut touched = BTreeSet::new();
        for _ in 0..=rng.below(4) {
            let key = format!("k{:02}", rng.below(30));
            if !touched.insert(key.clone()) {
                continue;
            }
            if state.contains_key(&key) && rng.below(3) == 0 {
                state.remove(&key);
                removed.push(key);
            } else {
                let etag = format!("e{commit}");
                state.insert(key.clone(), etag.clone());
                added.push(file(&key).etag(&etag));
            }
        }
        let removed: Vec<&str> = removed.iter().map(String::as_str).collect();
        let snapshot = ds.commit(parent, added, &removed).await;
        parent = Some(snapshot);
        history.push((snapshot, state.clone()));
        if commit % 20 == 19 {
            ds.checkpoint("main").await;
        }
    }

    for _ in 0..40 {
        let (from, before) = &history[rng.below(history.len())];
        let (to, after) = &history[rng.below(history.len())];
        let mut expected = Vec::new();
        let keys: BTreeSet<&String> = before.keys().chain(after.keys()).collect();
        for key in keys {
            match (before.get(key), after.get(key)) {
                (Some(_), None) => expected.push((key.clone(), DatasetFileChangeKind::Removed)),
                (None, Some(_)) => expected.push((key.clone(), DatasetFileChangeKind::Added)),
                (Some(old), Some(new)) if old != new => {
                    expected.push((key.clone(), DatasetFileChangeKind::Modified));
                }
                _ => {}
            }
        }
        let page_size = i64::try_from(1 + rng.below(7)).unwrap();
        assert_eq!(
            diff(&ds, snapshots(*from, *to, page_size)).await,
            expected,
            "diff {from} -> {to} at page size {page_size}"
        );
    }
}
