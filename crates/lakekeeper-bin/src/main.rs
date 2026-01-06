#![warn(
    missing_debug_implementations,
    rust_2018_idioms,
    unreachable_pub,
    clippy::pedantic
)]
#![forbid(unsafe_code)]
#![allow(clippy::module_name_repetitions, clippy::similar_names)]

use clap::{Parser, Subcommand};
use lakekeeper::{CONFIG, tokio, tracing};
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .json()
        .flatten_event(true)
        .with_current_span(true)
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
        #[cfg(feature = "open-api")]
        Some(Commands::ManagementOpenapi {}) => {
            use lakekeeper::{
                AuthZBackend, api::management::v1::api_doc, service::authz::AllowAllAuthorizer,
            };
            use lakekeeper_authz_openfga::OpenFGAAuthorizer;

            let queue_configs_ref = &lakekeeper::service::tasks::BUILT_IN_API_CONFIGS;
            let queue_configs: Vec<&_> = queue_configs_ref.iter().collect();
            let doc = match &CONFIG.authz_backend {
                AuthZBackend::AllowAll => api_doc::<AllowAllAuthorizer>(&queue_configs),
                AuthZBackend::External(e) if e == "openfga" => {
                    api_doc::<OpenFGAAuthorizer>(&queue_configs)
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

async fn migrate() -> anyhow::Result<()> {
    println!("Migrating database...");
    let write_pool = lakekeeper::implementations::postgres::get_writer_pool(
        CONFIG
            .to_pool_opts()
            .acquire_timeout(std::time::Duration::from_secs(CONFIG.pg_acquire_timeout)),
    )
    .await?;

    // This embeds database migrations in the application binary so we can ensure the database
    // is migrated correctly on startup
    let server_id = lakekeeper::implementations::postgres::migrations::migrate(&write_pool).await?;
    println!("Database migration complete.");

    println!("Migrating authorizer...");
    authorizer::migrate(server_id).await?;
    println!("Authorizer migration complete.");

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
