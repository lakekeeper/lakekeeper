//! Test databases copied from a migrated template.
//!
//! Applying every migration takes most of a second, so a test that migrates its own database
//! spends most of its time before its first query. The first test that runs after a migration
//! changed builds a template database with every core migration applied; each test then gets
//! a copy of it, which Postgres makes by copying files. Tests that check the migrations
//! themselves use `#[sqlx::test]` and start from an empty database.
//!
//! The test runtime, the pool limits and the cleanup match `#[sqlx::test]`.

use std::{future::Future, str::FromStr, sync::OnceLock, time::Duration};

use sqlx::{
    AssertSqlSafe, Connection, Executor, PgConnection, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use xxhash_rust::xxh3::Xxh3Default;

use crate::migrations::migrate_core_only;

/// Advisory lock key held while a template is built: the ASCII bytes of `lktmplte`.
const TEMPLATE_LOCK_KEY: i64 = 0x6c6b_746d_706c_7465;

/// Runs `test_fn` on a current-thread runtime with a pool on its own database, named after
/// `test_path` and copied from the migrated template. The database is dropped when the test
/// passes and kept for inspection when it panics; the next run of the test replaces it.
///
/// `DATABASE_URL`, also read from a `.env` file, names the server and a database to connect
/// to for creating the others.
///
/// # Panics
/// If `DATABASE_URL` is missing or invalid, or the template or the test database cannot be
/// created.
pub fn run_test<Fut: Future<Output = ()>>(test_path: &str, test_fn: impl FnOnce(PgPool) -> Fut) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build the test runtime")
        .block_on(async move {
            let master = master_pool();
            let template = template(master).await;
            let db_name = test_db_name(test_path);
            let mut conn = master
                .acquire()
                .await
                .expect("failed to connect to DATABASE_URL");
            drop_database(&mut conn, &db_name).await;
            conn.execute(AssertSqlSafe(format!(
                r#"CREATE DATABASE "{db_name}" TEMPLATE "{template}""#
            )))
            .await
            .unwrap_or_else(|e| panic!("failed to copy template {template} to {db_name}: {e}"));
            drop(conn);

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .idle_timeout(Some(Duration::from_secs(1)))
                // Counts against the 20 connections of `master`, so the tests of one process
                // stay below Postgres' connection limit. Hidden in sqlx; `#[sqlx::test]` uses it.
                .parent(master.clone())
                .connect_with(master.connect_options().as_ref().clone().database(&db_name))
                .await
                .expect("failed to connect the test pool");
            test_fn(pool.clone()).await;
            if tokio::time::timeout(Duration::from_secs(10), pool.close())
                .await
                .is_err()
            {
                eprintln!("test {test_path} held onto its pool after exiting");
            }
            match master.acquire().await {
                Ok(mut conn) => drop_database(&mut conn, &db_name).await,
                Err(e) => eprintln!("failed to drop database {db_name}: {e}"),
            }
        });
}

/// Connections to `DATABASE_URL`. Each test builds its own runtime, so a connection closes
/// when it is released.
fn master_pool() -> &'static PgPool {
    static MASTER: OnceLock<PgPool> = OnceLock::new();
    MASTER.get_or_init(|| {
        let url = dotenvy::var("DATABASE_URL").expect("DATABASE_URL must be set");
        PgPoolOptions::new()
            // Leaves room below Postgres' default limit of 100 connections for the test pools.
            .max_connections(20)
            .after_release(|_conn, _meta| Box::pin(async { Ok(false) }))
            .connect_lazy_with(
                PgConnectOptions::from_str(&url).expect("DATABASE_URL is not a valid URL"),
            )
    })
}

/// Returns the name of the template for the current migrations, built once per server.
async fn template(master: &PgPool) -> &'static str {
    static TEMPLATE: OnceLock<String> = OnceLock::new();
    if let Some(name) = TEMPLATE.get() {
        return name;
    }
    let name = template_name();
    // A session lock: concurrent test processes wait here until the first one built the
    // template.
    let mut conn = PgConnection::connect_with(master.connect_options().as_ref())
        .await
        .expect("failed to connect to DATABASE_URL");
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(TEMPLATE_LOCK_KEY)
        .execute(&mut conn)
        .await
        .expect("failed to lock the template build");
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)")
            .bind(&name)
            .fetch_one(&mut conn)
            .await
            .expect("failed to look up the template");
    if !exists {
        build_template(&mut conn, master, &name).await;
    }
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(TEMPLATE_LOCK_KEY)
        .execute(&mut conn)
        .await
        .expect("failed to unlock the template build");
    conn.close().await.ok();
    TEMPLATE.get_or_init(|| name)
}

/// Migrates a database under a build name and renames it to `name` once it is complete, so a
/// template that exists is always fully migrated.
// shortcut: templates of earlier migration sets stay on the server until it is reset; drop
// `_lk_test_tpl_*` databases by hand if they pile up.
async fn build_template(conn: &mut PgConnection, master: &PgPool, name: &str) {
    let build_name = format!("{name}_build");
    drop_database(conn, &build_name).await;
    conn.execute(AssertSqlSafe(format!(r#"CREATE DATABASE "{build_name}""#)))
        .await
        .expect("failed to create the template database");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(
            master
                .connect_options()
                .as_ref()
                .clone()
                .database(&build_name),
        )
        .await
        .expect("failed to connect to the template database");
    migrate_core_only(&pool)
        .await
        .expect("failed to migrate the template database");
    pool.close().await;
    conn.execute(AssertSqlSafe(format!(
        r#"ALTER DATABASE "{build_name}" RENAME TO "{name}""#
    )))
    .await
    .expect("failed to rename the template database");
}

/// Fails while another process is connected, so a concurrent run of the same test keeps its
/// database.
async fn drop_database(conn: &mut PgConnection, name: &str) {
    if let Err(e) = conn
        .execute(AssertSqlSafe(format!(
            r#"DROP DATABASE IF EXISTS "{name}""#
        )))
        .await
    {
        eprintln!("failed to drop database {name}: {e}");
    }
}

/// The Rust side of the migrations: a change to a hook changes the template without a new SQL
/// migration.
const MIGRATION_SOURCES: [(&str, &str); 6] = [
    ("mod.rs", include_str!("../migrations/mod.rs")),
    (
        "normalize_fs_location.rs",
        include_str!("../migrations/normalize_fs_location.rs"),
    ),
    (
        "normalize_schema.rs",
        include_str!("../migrations/normalize_schema.rs"),
    ),
    (
        "patch_migration_hash.rs",
        include_str!("../migrations/patch_migration_hash.rs"),
    ),
    (
        "split_table_metadata.rs",
        include_str!("../migrations/split_table_metadata.rs"),
    ),
    (
        "table_snapshot_key.rs",
        include_str!("../migrations/table_snapshot_key.rs"),
    ),
];

/// A new template for every change to the core migrations or to the Rust sources in
/// `src/migrations`.
// shortcut: code a hook calls outside `src/migrations` does not change the name; drop the
// `_lk_test_tpl_*` databases by hand after changing it.
fn template_name() -> String {
    let mut hasher = Xxh3Default::new();
    for migration in sqlx::migrate!().iter() {
        hasher.update(&migration.version.to_le_bytes());
        hasher.update(&migration.checksum[..]);
    }
    for (_, source) in MIGRATION_SOURCES {
        hasher.update(source.as_bytes());
    }
    format!("_lk_test_tpl_{:016x}", hasher.digest())
}

/// The same test always gets the same database, so a database a failed test left behind is
/// replaced on its next run.
fn test_db_name(test_path: &str) -> String {
    format!(
        "_lk_test_{:016x}",
        xxhash_rust::xxh3::xxh3_64(test_path.as_bytes())
    )
}

#[cfg(test)]
mod tests {
    use super::MIGRATION_SOURCES;

    #[test]
    fn the_template_name_covers_every_migration_source() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/migrations");
        let mut files: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .filter(|name| name.ends_with(".rs"))
            .collect();
        files.sort();
        let listed: Vec<&str> = MIGRATION_SOURCES.iter().map(|(name, _)| *name).collect();
        assert_eq!(
            files, listed,
            "MIGRATION_SOURCES must list every Rust file in src/migrations"
        );
    }
}
