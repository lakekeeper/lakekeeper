#![warn(
    missing_debug_implementations,
    rust_2018_idioms,
    unreachable_pub,
    clippy::pedantic
)]
#![forbid(unsafe_code)]
#![allow(clippy::module_name_repetitions, clippy::similar_names)]

// Use jemalloc as the global allocator to avoid glibc malloc fragmentation
// which causes monotonic growth of container_memory_working_set_bytes.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use clap::{Parser, Subcommand, ValueEnum};
use lakekeeper::{
    CONFIG,
    implementations::{CatalogState, postgres::PostgresBackend},
    tokio, tracing,
};
use tracing_subscriber::{EnvFilter, filter::LevelFilter};

mod authorizer;
mod config;
mod healthcheck;
mod serve;
#[cfg(feature = "ui")]
mod ui;
mod wait_for_db;

pub(crate) use config::CONFIG_BIN;
const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Migrate the database
    Migrate {},
    /// Wait for the database to be up and migrated
    WaitForDB {
        #[clap(
            default_value = "false",
            short = 'd',
            help = "Test DB connection, requires postgres env values."
        )]
        check_db: bool,
        #[clap(
            default_value = "false",
            short = 'm',
            help = "Check migrations, implies -d."
        )]
        check_migrations: bool,
        #[clap(
            default_value_t = 15,
            long,
            short,
            help = "Number of retries to connect to the database, implies -w."
        )]
        retries: u32,
        #[clap(
            default_value_t = 2,
            long,
            short,
            help = "Delay in seconds between retries to connect to the database."
        )]
        backoff: u64,
    },
    /// Run the server - The database must be migrated before running the server
    Serve {
        #[clap(
            default_value = "true",
            short = 'f',
            long = "force-start",
            help = "Start server even if DB is not up or migrations aren't complete."
        )]
        force_start: bool,
    },
    /// Check the health of the server
    Healthcheck {
        #[clap(
            default_value = "false",
            short = 'a',
            help = "Check all services, implies -d and -s."
        )]
        check_all: bool,
        #[clap(
            default_value = "false",
            short = 'd',
            help = "Only test DB connection, requires postgres env values.",
            conflicts_with("check_all")
        )]
        check_db: bool,
        #[clap(
            default_value = "false",
            short = 's',
            help = "Check health endpoint.",
            conflicts_with("check_all")
        )]
        check_server: bool,
    },
    /// Print the version of the server
    Version {},
    #[cfg(feature = "open-api")]
    /// Get the `OpenAPI` specification of the Management API as yaml
    ManagementOpenapi {},
    /// OpenFGA authorizer maintenance operations.
    Openfga {
        #[command(subcommand)]
        command: OpenfgaCommands,
    },
    /// Re-open the catalog so `/management/v1/bootstrap` can be called again.
    ///
    /// Operator-only recovery path used when switching authorizer backends
    /// (for example `AllowAll` → `OpenFGA` on an already-bootstrapped
    /// catalog) or when fixing a misconfigured first bootstrap. Flips the
    /// `open_for_bootstrap` flag in Postgres back to `true`. Does not
    /// touch the server-id, catalog data, or any existing OpenFGA tuples.
    /// After running this, an authenticated principal must call
    /// `/management/v1/bootstrap` to seed the initial admin/operator.
    ReopenBootstrap {
        #[clap(
            long,
            short = 'y',
            default_value_t = false,
            help = "Required confirmation. Without --yes the command refuses to run."
        )]
        yes: bool,
    },
}

#[derive(Subcommand)]
enum OpenfgaCommands {
    /// Reconcile structural OpenFGA hierarchy tuples against the Postgres
    /// catalog. Catalog is the source of truth: missing edges are added,
    /// and (in `add-and-delete-drift` mode) drift is removed.
    ///
    /// Run during a low-traffic window — concurrent API writes can produce
    /// transient inconsistencies that self-heal on the next run.
    Reconcile {
        #[clap(
            long,
            value_enum,
            default_value_t = ReconcileModeArg::AddMissing,
            help = "Reconcile semantics. `add-missing` is purely additive; `add-and-delete-drift` also removes structural tuples the catalog contradicts."
        )]
        mode: ReconcileModeArg,
        #[clap(
            long,
            default_value_t = false,
            help = "Compute and report the diff without writing to OpenFGA."
        )]
        dry_run: bool,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ReconcileModeArg {
    /// Add missing hierarchy edges, never delete.
    AddMissing,
    /// Add missing edges and delete drift (structural tuples the catalog contradicts).
    AddAndDeleteDrift,
}

impl From<ReconcileModeArg> for lakekeeper_authz_openfga::ReconcileMode {
    fn from(m: ReconcileModeArg) -> Self {
        match m {
            ReconcileModeArg::AddMissing => Self::AddMissingOnly,
            ReconcileModeArg::AddAndDeleteDrift => Self::AddMissingAndDeleteDrift,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .json()
        .flatten_event(true)
        .with_current_span(false)
        .with_span_list(true)
        .with_file(CONFIG_BIN.debug.extended_logs)
        .with_line_number(CONFIG_BIN.debug.extended_logs)
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();

    match cli.command {
        Some(Commands::WaitForDB {
            check_db,
            check_migrations,
            retries,
            backoff,
        }) => {
            let check_db = check_db || check_migrations;

            wait_for_db::wait_for_db(check_migrations, retries, backoff, check_db).await?;
        }
        Some(Commands::Migrate {}) => {
            print_info();
            migrate().await?;
        }
        Some(Commands::Serve { force_start }) => {
            print_info();
            serve_and_maybe_migrate(force_start).await?;
        }
        Some(Commands::Healthcheck {
            check_all,
            mut check_db,
            mut check_server,
        }) => {
            check_db |= check_all;
            check_server |= check_all;
            healthcheck::health(check_db, check_server).await?;
        }
        Some(Commands::Version {}) => {
            println!("{}", env!("CARGO_PKG_VERSION"));
        }
        Some(Commands::Openfga { command }) => match command {
            OpenfgaCommands::Reconcile { mode, dry_run } => {
                print_info();
                openfga_reconcile(mode.into(), dry_run).await?;
            }
        },
        Some(Commands::ReopenBootstrap { yes }) => {
            print_info();
            reopen_bootstrap(yes).await?;
        }
        #[cfg(feature = "open-api")]
        Some(Commands::ManagementOpenapi {}) => {
            use lakekeeper::{
                AuthZBackend, api::management::v1::api_doc, service::authz::AllowAllAuthorizer,
            };
            use lakekeeper_authz_openfga::OpenFGAAuthorizer;

            let queue_configs_ref = &lakekeeper::service::tasks::BUILT_IN_API_CONFIGS;
            let queue_configs: Vec<&_> = queue_configs_ref.iter().collect();
            let project_queue_configs_ref =
                &lakekeeper::service::tasks::BUILT_IN_PROJECT_API_CONFIGS;
            let project_queue_configs: Vec<&_> = project_queue_configs_ref.iter().collect();
            let doc = match &CONFIG.authz_backend {
                AuthZBackend::AllowAll => {
                    api_doc::<AllowAllAuthorizer>(&queue_configs, &project_queue_configs)
                }
                AuthZBackend::External(e) if e == "openfga" => {
                    api_doc::<OpenFGAAuthorizer>(&queue_configs, &project_queue_configs)
                }
                AuthZBackend::External(e) => anyhow::bail!("Unsupported authz backend `{e}`"),
            };
            println!("{}", doc.to_yaml()?);
        }
        None => {
            if CONFIG_BIN.debug.auto_serve {
                print_info();
                serve_and_maybe_migrate(true).await?;
            } else {
                // Error out if no subcommand is provided.
                eprintln!("No subcommand provided. Use --help for more information.");
                anyhow::bail!("No subcommand provided");
            }
        }
    }

    Ok(())
}

async fn serve_and_maybe_migrate(force_start: bool) -> anyhow::Result<()> {
    if CONFIG_BIN.debug.migrate_before_serve {
        wait_for_db::wait_for_db(false, 15, 2, true).await?;
        migrate().await?;
    }
    serve(force_start).await
}

async fn reopen_bootstrap(yes: bool) -> anyhow::Result<()> {
    if !yes {
        anyhow::bail!(
            "reopen-bootstrap re-allows /management/v1/bootstrap to be called. \
             Re-run with --yes to confirm."
        );
    }

    let write_pool = lakekeeper::implementations::postgres::get_writer_pool(
        CONFIG
            .to_pool_opts()
            .acquire_timeout(std::time::Duration::from_secs(CONFIG.pg_acquire_timeout)),
    )
    .await?;
    let catalog_state = CatalogState::from_pools(write_pool.clone(), write_pool);

    let server_id =
        <PostgresBackend as lakekeeper::service::CatalogStore>::reopen_for_bootstrap(catalog_state)
            .await
            .map_err(|e| anyhow::anyhow!("reopen-bootstrap failed: {e}"))?;
    tracing::info!(
        "Catalog re-opened for bootstrap (server_id={server_id}). \
         Call POST /management/v1/bootstrap to seed admin/operator."
    );
    println!();
    println!("Catalog re-opened for bootstrap.");
    println!("  server_id: {server_id}");
    println!("  next:      POST /management/v1/bootstrap (with an authenticated principal)");

    Ok(())
}

async fn openfga_reconcile(
    mode: lakekeeper_authz_openfga::ReconcileMode,
    dry_run: bool,
) -> anyhow::Result<()> {
    if !lakekeeper_authz_openfga::CONFIG.is_openfga_enabled() {
        anyhow::bail!(
            "openfga reconcile requires LAKEKEEPER__AUTHZ_BACKEND=openfga; current backend is {:?}",
            CONFIG.authz_backend
        );
    }

    let read_pool = lakekeeper::implementations::postgres::get_reader_pool(
        CONFIG
            .to_pool_opts()
            .max_connections(CONFIG.pg_read_pool_connections),
    )
    .await?;
    let write_pool = lakekeeper::implementations::postgres::get_writer_pool(
        CONFIG
            .to_pool_opts()
            .max_connections(CONFIG.pg_write_pool_connections),
    )
    .await?;
    let catalog_state = CatalogState::from_pools(read_pool, write_pool);

    let server_id = <PostgresBackend as lakekeeper::service::CatalogStore>::get_server_info(
        catalog_state.clone(),
    )
    .await?
    .server_id();

    let authorizer =
        lakekeeper_authz_openfga::new_authorizer_from_default_config(server_id).await?;

    tracing::info!("openfga reconcile: starting (mode={mode:?}, dry_run={dry_run})");
    let report = lakekeeper_authz_openfga::reconcile_hierarchy_tuples_from_catalog(
        catalog_state,
        authorizer.client(),
        server_id,
        mode,
        dry_run,
    )
    .await?;

    let action = if report.dry_run { "would" } else { "did" };
    println!();
    println!(
        "OpenFGA reconcile report ({})",
        if report.dry_run { "dry run" } else { "applied" }
    );
    println!("  mode: {mode:?}");
    println!(
        "  {action} submit {} tuple(s) in {} request(s)",
        report.tuples_submitted, report.write_requests
    );
    println!(
        "  {action} delete {} tuple(s) in {} request(s)",
        report.tuples_deleted, report.delete_requests
    );
    println!(
        "  ignored (unmanaged relation/type): {}",
        report.tuples_ignored_unmanaged
    );
    println!(
        "  ignored (both endpoints unknown):  {}",
        report.tuples_ignored_orphan
    );
    if !report.per_type.is_empty() {
        println!("  per-type submitted:");
        for (ty, n) in &report.per_type {
            println!("    {ty:<10} {n}");
        }
    }

    Ok(())
}

async fn migrate() -> anyhow::Result<()> {
    tracing::info!("Migrating database...");
    let write_pool = lakekeeper::implementations::postgres::get_writer_pool(
        CONFIG
            .to_pool_opts()
            .acquire_timeout(std::time::Duration::from_secs(CONFIG.pg_acquire_timeout)),
    )
    .await?;

    // This embeds database migrations in the application binary so we can ensure the database
    // is migrated correctly on startup
    let server_id = lakekeeper::implementations::postgres::migrations::migrate(&write_pool).await?;
    tracing::info!("Database migration complete.");

    tracing::info!("Migrating authorizer...");
    authorizer::migrate(server_id).await?;
    tracing::info!("Authorizer migration complete.");
    tracing::info!("Running post-migration hooks...");
    let catalog_state = CatalogState::from_pools(write_pool.clone(), write_pool.clone());
    lakekeeper::service::run_post_migration_hooks::<PostgresBackend>(catalog_state).await?;
    tracing::info!("Post-migration hooks complete.");

    Ok(())
}

async fn serve(force_start: bool) -> anyhow::Result<()> {
    tracing::info!(
        "Starting server on {}:{}...",
        CONFIG.bind_ip,
        CONFIG.listen_port
    );
    let bind_addr = std::net::SocketAddr::from((CONFIG.bind_ip, CONFIG.listen_port));
    if !force_start {
        wait_for_db::wait_for_db(true, 0, 0, true).await?;
    }
    serve::serve_default(bind_addr).await?;

    Ok(())
}

fn print_info() {
    let console_span = r" _      ___  _   _______ _   _______ ___________ ___________ 
| |    / _ \| | / |  ___| | / |  ___|  ___| ___ |  ___| ___ \
| |   / /_\ | |/ /| |__ | |/ /| |__ | |__ | |_/ | |__ | |_/ /
| |   |  _  |    \|  __||    \|  __||  __||  __/|  __||    / 
| |___| | | | |\  | |___| |\  | |___| |___| |   | |___| |\ \ 
\_____\_| |_\_| \_\____/\_| \_\____/\____/\_|   \____/\_| \_|

 _____ ___________ _____ 
/  __ |  _  | ___ |  ___|
| /  \| | | | |_/ | |__  
| |   | | | |    /|  __|
| \__/\ \_/ | |\ \| |___
 \____/\___/\_| \_\____/

Created with ❤️ by Vakamo
Docs: https://docs.lakekeeper.io
Enterprise Support: https://vakamo.com
";
    let console_span = format!("{console_span}\nLakekeeper Version: {VERSION}\n");
    println!("{console_span}");
    tracing::info!("Lakekeeper Version: {VERSION}");
}
