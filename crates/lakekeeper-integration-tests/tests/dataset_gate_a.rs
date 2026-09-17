//! Gate A: does Postgres carry dataset manifests at the sizes we promise?
//!
//! The versioning design stores one manifest row per file per snapshot and
//! reconstructs a ref's file list as "nearest checkpoint plus the deltas since".
//! Two numbers decide whether that holds:
//!
//!   1. Page latency at a ref -- the hot path every reader hits.
//!   2. Checkpoint write cost -- the amortised price of keeping (1) flat.
//!
//! If page latency grows with total manifest size rather than page size, the
//! design is wrong and manifests belong in object storage. If it stays flat and
//! only checkpoint writes grow, the checkpoint interval is a tuning knob and
//! Postgres is fine.
//!
//! These are ignored by default: they seed millions of rows and take minutes.
//!
//! ```bash
//! GATE_A_FILES=1000000 cargo nextest run --all-features \
//!     -p lakekeeper-integration-tests --run-ignored all gate_a
//! ```
use std::time::Instant;

use iceberg::NamespaceIdent;
use lakekeeper::{
    api::{
        ApiContext,
        data::v1::datasets::{
            CommitDatasetRequest, CommitFile, DatasetRefParameters, DatasetService as _,
            ListDatasetFilesQuery,
        },
        iceberg::types::Prefix,
        management::v1::warehouse::TabularDeleteProfile,
    },
    server::CatalogServer,
    service::{State, authz::AllowAllAuthorizer},
};
use lakekeeper_integration_tests::{
    create_dataset, create_ns, memory_io_profile, random_request_metadata, setup,
};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;
use uuid::Uuid;

type TestApiContext = ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>;

const DS: &str = "gate_a";

/// Total manifest size to seed. Override with `GATE_A_FILES`.
fn seeded_files() -> i64 {
    std::env::var("GATE_A_FILES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000_000)
}

/// Delta snapshots stacked on top of the checkpoint. This is the number the
/// checkpoint interval controls, so the sweep below is the point of the whole
/// benchmark.
fn delta_depths() -> Vec<i64> {
    std::env::var("GATE_A_DEPTHS")
        .ok()
        .map(|v| v.split(',').filter_map(|d| d.trim().parse().ok()).collect())
        .unwrap_or_else(|| vec![0, 5, 20, 50, 100])
}

/// Files touched by each delta snapshot -- a realistic incremental commit.
const FILES_PER_DELTA: i64 = 1_000;

struct Fixture {
    ctx: TestApiContext,
    pool: PgPool,
    prefix: String,
    ns: String,
    warehouse_id: Uuid,
    dataset_id: Uuid,
    checkpoint: Uuid,
}

async fn seed(pool: PgPool, files: i64) -> Fixture {
    let (ctx, warehouse) = setup(
        pool.clone(),
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
    create_dataset(ctx.clone(), prefix.clone(), ns.clone(), DS)
        .await
        .unwrap();

    let row = sqlx::query!(r#"SELECT warehouse_id, dataset_id, location FROM dataset"#)
        .fetch_one(&pool)
        .await
        .expect("the dataset just created");
    let (warehouse_id, dataset_id, location) = (row.warehouse_id, row.dataset_id, row.location);

    // Seed the checkpoint directly rather than through commit_dataset: the API
    // path is what we are measuring, and driving a million files through it
    // would measure the seeding, not the read.
    let checkpoint = Uuid::now_v7();
    sqlx::query!(
        r#"
        INSERT INTO dataset_snapshot
            (warehouse_id, dataset_id, snapshot_id, parent_snapshot_id, location,
             status, is_checkpoint)
        VALUES ($1, $2, $3, NULL, $4, 'active', true)
        "#,
        warehouse_id,
        dataset_id,
        checkpoint,
        location,
    )
    .execute(&pool)
    .await
    .unwrap();

    // Keys are laid out the way real object stores are -- a shard prefix and a
    // zero-padded name -- so the index behaves as it would in production rather
    // than on an artificially uniform key space.
    let started = Instant::now();
    sqlx::query!(
        r#"
        INSERT INTO dataset_manifest_entry
            (warehouse_id, snapshot_id, logical_key, physical_path, change,
             etag, size, content_type)
        SELECT $1, $2,
               'shard=' || lpad((i / 10000)::text, 5, '0')
                        || '/img_' || lpad(i::text, 9, '0') || '.jpg',
               's3://bucket/data/img_' || i,
               'added', md5(i::text), 4096 + i % 1024,
               CASE WHEN i % 10 = 0 THEN 'application/json' ELSE 'image/jpeg' END
        FROM generate_series(1::bigint, $3::bigint) AS i
        "#,
        warehouse_id,
        checkpoint,
        files,
    )
    .execute(&pool)
    .await
    .unwrap();
    let seed_secs = started.elapsed().as_secs_f64();

    sqlx::query!(
        r#"UPDATE dataset_ref SET snapshot_id = $3
           WHERE warehouse_id = $1 AND dataset_id = $2 AND name = 'main'"#,
        warehouse_id,
        dataset_id,
        checkpoint,
    )
    .execute(&pool)
    .await
    .unwrap();

    // Without stats the planner will not choose the index-only path, which would
    // make every number below a measurement of a cold optimiser.
    sqlx::query("ANALYZE dataset_manifest_entry, dataset_snapshot")
        .execute(&pool)
        .await
        .unwrap();

    println!(
        "\n=== Gate A: {files} files seeded in {seed_secs:.1}s \
         ({:.0} rows/s) ===",
        files as f64 / seed_secs
    );

    Fixture {
        ctx,
        pool,
        prefix,
        ns,
        warehouse_id,
        dataset_id,
        checkpoint,
    }
}

/// Stack `depth` delta snapshots on top of `parent`, returning the new tip.
/// `applied` is how many already sit below it, so each one continues the key
/// slices rather than rewriting the same ones.
async fn stack_deltas(f: &Fixture, parent: Uuid, applied: i64, depth: i64) -> Uuid {
    let mut tip = parent;
    for d in 0..depth {
        let next = Uuid::now_v7();
        sqlx::query!(
            r#"
            INSERT INTO dataset_snapshot
                (warehouse_id, dataset_id, snapshot_id, parent_snapshot_id, location,
                 status, is_checkpoint)
            SELECT $1, $2, $3, $4, location, 'active', false
            FROM dataset_snapshot WHERE warehouse_id = $1 AND snapshot_id = $4
            "#,
            f.warehouse_id,
            f.dataset_id,
            next,
            tip,
        )
        .execute(&f.pool)
        .await
        .unwrap();

        // Each delta rewrites a distinct slice of the key space, so the
        // DISTINCT ON has real work to do at every depth rather than collapsing
        // to the same thousand keys.
        let offset = (applied + d) * FILES_PER_DELTA + 1;
        sqlx::query!(
            r#"
            INSERT INTO dataset_manifest_entry
                (warehouse_id, snapshot_id, logical_key, physical_path, change,
                 etag, size, content_type)
            SELECT $1, $2,
                   'shard=' || lpad((i / 10000)::text, 5, '0')
                            || '/img_' || lpad(i::text, 9, '0') || '.jpg',
                   's3://bucket/data/img_' || i || '_v2',
                   'added', md5((i * 7)::text), 8192, 'image/jpeg'
            FROM generate_series($3::bigint, $4::bigint) AS i
            "#,
            f.warehouse_id,
            next,
            offset,
            offset + FILES_PER_DELTA - 1,
        )
        .execute(&f.pool)
        .await
        .unwrap();
        tip = next;
    }

    sqlx::query!(
        r#"UPDATE dataset_ref SET snapshot_id = $3
           WHERE warehouse_id = $1 AND dataset_id = $2 AND name = 'main'"#,
        f.warehouse_id,
        f.dataset_id,
        tip,
    )
    .execute(&f.pool)
    .await
    .unwrap();
    tip
}

/// Median of five, through the real API surface -- authz, handler and store.
async fn time_first_page(f: &Fixture, page_size: Option<i64>, content_type: Option<&str>) -> f64 {
    let params = DatasetRefParameters {
        prefix: Some(Prefix(f.prefix.clone())),
        namespace: NamespaceIdent::new(f.ns.clone()),
        dataset_name: DS.to_string(),
        ref_name: "main".to_string(),
    };
    let mut samples = Vec::new();
    for _ in 0..5 {
        let query = ListDatasetFilesQuery {
            page_size,
            content_type: content_type.map(ToString::to_string),
            ..Default::default()
        };
        let started = Instant::now();
        let listed = CatalogServer::list_dataset_files(
            params.clone(),
            query,
            f.ctx.clone(),
            random_request_metadata(),
        )
        .await
        .expect("files list");
        samples.push(started.elapsed().as_secs_f64() * 1000.0);
        // A page may legitimately come back short or empty: the scan is bounded
        // by key range, and removals and the content_type filter are applied
        // after the fold. Only a null token means the end of the dataset.
        drop(listed);
    }
    samples.sort_by(f64::total_cmp);
    samples[2]
}

/// The headline: page latency must be a function of page size, not of how many
/// files the dataset holds or how deep the delta chain runs.
#[sqlx::test]
#[ignore = "seeds millions of rows; run explicitly for Gate A"]
async fn gate_a_page_latency_is_flat_in_manifest_size(pool: PgPool) {
    let files = seeded_files();
    let f = seed(pool, files).await;

    println!("\n  depth |  page=100 |  page=1000 | filtered(json)");
    println!("  ------+-----------+------------+---------------");

    let mut at_zero = None;
    let mut worst = 0.0_f64;
    let mut tip = f.checkpoint;
    let mut applied = 0;

    for depth in delta_depths() {
        tip = stack_deltas(&f, tip, applied, depth - applied).await;
        applied = depth;

        let p100 = time_first_page(&f, Some(100), None).await;
        let p1000 = time_first_page(&f, Some(1000), None).await;
        let filtered = time_first_page(&f, Some(100), Some("application/json")).await;

        println!("  {depth:5} | {p100:7.1}ms | {p1000:8.1}ms | {filtered:11.1}ms");
        at_zero.get_or_insert(p100);
        worst = worst.max(p100);
    }

    let base = at_zero.expect("at least one depth measured");
    println!("\n  {files} files: page=100 went {base:.1}ms -> {worst:.1}ms across the depth sweep");

    // The gate itself. A first page is a bounded index range scan over the
    // checkpoint plus a bounded merge of the deltas; neither term involves the
    // total row count. If this fails, manifests do not belong in Postgres.
    assert!(
        worst < 1_000.0,
        "first page took {worst:.1}ms at {files} files -- Gate A fails, \
         manifests must move out of Postgres"
    );
}

/// The other half: what a checkpoint costs to write, which is what the
/// checkpoint interval is trading against.
#[sqlx::test]
#[ignore = "seeds millions of rows; run explicitly for Gate A"]
async fn gate_a_checkpoint_write_cost(pool: PgPool) {
    let files = seeded_files();
    let f = seed(pool, files).await;
    let tip = stack_deltas(&f, f.checkpoint, 0, 20).await;

    let target = Uuid::now_v7();
    sqlx::query!(
        r#"
        INSERT INTO dataset_snapshot
            (warehouse_id, dataset_id, snapshot_id, parent_snapshot_id, location,
             status, is_checkpoint)
        SELECT $1, $2, $3, $4, location, 'active', true
        FROM dataset_snapshot WHERE warehouse_id = $1 AND snapshot_id = $4
        "#,
        f.warehouse_id,
        f.dataset_id,
        target,
        tip,
    )
    .execute(&f.pool)
    .await
    .unwrap();

    // Mirrors write_checkpoint: fold the chain down to newest-wins and
    // materialise it as one full manifest.
    let started = Instant::now();
    let written = sqlx::query!(
        r#"
        WITH RECURSIVE ancestry(snapshot_id, parent_snapshot_id, depth, is_checkpoint) AS (
            SELECT snapshot_id, parent_snapshot_id, 0, false
            FROM dataset_snapshot
            WHERE warehouse_id = $1 AND dataset_id = $2 AND snapshot_id = $4
          UNION ALL
            SELECT s.snapshot_id, s.parent_snapshot_id, a.depth + 1, s.is_checkpoint
            FROM dataset_snapshot s
            INNER JOIN ancestry a ON s.snapshot_id = a.parent_snapshot_id
            WHERE s.warehouse_id = $1 AND NOT a.is_checkpoint
        ),
        newest AS (
            SELECT DISTINCT ON (m.logical_key)
                m.logical_key, m.physical_path, m.change, m.etag, m.size,
                m.content_type, m.checksum, m.version_id, m.last_modified
            FROM dataset_manifest_entry m
            INNER JOIN ancestry a ON a.snapshot_id = m.snapshot_id
            WHERE m.warehouse_id = $1
            ORDER BY m.logical_key, a.depth ASC
        )
        INSERT INTO dataset_manifest_entry
            (warehouse_id, snapshot_id, logical_key, physical_path, change,
             etag, size, content_type, checksum, version_id, last_modified)
        SELECT $1, $3, logical_key, physical_path, 'added',
               etag, size, content_type, checksum, version_id, last_modified
        FROM newest
        WHERE change <> 'removed'
        ON CONFLICT DO NOTHING
        "#,
        f.warehouse_id,
        f.dataset_id,
        target,
        tip,
    )
    .execute(&f.pool)
    .await
    .unwrap()
    .rows_affected();
    let elapsed = started.elapsed().as_secs_f64();

    let (db_bytes,) = sqlx::query_as::<_, (i64,)>(
        "SELECT pg_total_relation_size('dataset_manifest_entry')::bigint",
    )
    .fetch_one(&f.pool)
    .await
    .unwrap();

    println!(
        "\n=== Gate A checkpoint: {written} rows folded from depth 20 in {elapsed:.2}s \
         ({:.0} rows/s)\n    manifest table now {:.2} GiB across {} snapshots ===",
        written as f64 / elapsed,
        db_bytes as f64 / 1024.0 / 1024.0 / 1024.0,
        22,
    );

    assert_eq!(written, files as u64, "checkpoint must be a full manifest");
}

/// How large a single commit can be before it stops being reasonable.
///
/// The manifest insert binds arrays through `UNNEST`, so it is ten bind
/// parameters no matter how many files ride along -- there is no parameter-count
/// wall. What does grow is the server-side memory holding the request plus its
/// derived column vectors, and the duration of the one transaction that stages
/// them. This measures where that starts to hurt so the batching decision is made
/// against numbers rather than a guess.
#[sqlx::test]
#[ignore = "commits up to a million files in one call; run explicitly"]
async fn gate_a_single_commit_size(pool: PgPool) {
    let (ctx, warehouse) = setup(
        pool.clone(),
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
    create_dataset(ctx.clone(), prefix.clone(), ns.clone(), DS)
        .await
        .unwrap();

    println!("\n    files |    build |   commit |    total | per 1k files");
    println!("  --------+----------+----------+----------+-------------");

    let mut parent = None;
    for count in [1_000usize, 10_000, 100_000, 1_000_000] {
        let build_started = Instant::now();
        let added = (0..count)
            .map(|i| CommitFile {
                logical_key: format!("shard={:05}/img_{i:09}.jpg", i / 10_000),
                physical_path: None,
                etag: Some(format!("{i:032x}")),
                size: Some(4096),
                content_type: Some("image/jpeg".to_string()),
                checksum: None,
                version_id: None,
                last_modified: None,
            })
            .collect::<Vec<_>>();
        let build = build_started.elapsed().as_secs_f64();

        let commit_started = Instant::now();
        let snapshot = CatalogServer::commit_dataset(
            DatasetRefParameters {
                prefix: Some(Prefix(prefix.clone())),
                namespace: NamespaceIdent::new(ns.clone()),
                dataset_name: DS.to_string(),
                ref_name: "main".to_string(),
            },
            CommitDatasetRequest {
                parent_snapshot_id: parent,
                added,
                removed: vec![],
                summary: None,
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap_or_else(|e| panic!("commit of {count} files failed: {e:?}"));
        let commit = commit_started.elapsed().as_secs_f64();
        parent = Some(snapshot.snapshot_id);

        println!(
            "  {count:7} | {build:7.2}s | {commit:7.2}s | {:7.2}s | {:9.1}ms",
            build + commit,
            (build + commit) * 1000.0 / (count as f64 / 1000.0),
        );
    }
}
