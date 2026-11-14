//! Postgres commits, but its reply to `COMMIT` never reaches Lakekeeper. The metadata file the
//! commit points the table or view at must stay in storage.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::TryStreamExt as _;
use iceberg::{
    NamespaceIdent, TableIdent, TableUpdate,
    spec::{TableMetadata, TableMetadataRef, ViewMetadata},
};
use iceberg_ext::catalog::rest::{
    CommitTableRequest, CommitViewRequest, IcebergErrorResponse, ViewUpdate,
};
use lakekeeper::{
    api::{
        ApiContext, RequestMetadata,
        iceberg::{
            types::Prefix,
            v1::{
                DataAccess, NamespaceParameters, TableParameters, ViewParameters,
                tables::TablesService as _, views::ViewService as _,
            },
        },
        management::v1::warehouse::TabularDeleteProfile,
    },
    server::CatalogServer,
    service::{State, authz::AllowAllAuthorizer},
};
use lakekeeper_integration_tests::{
    create_ns, create_table_request, create_view, memory_io_profile, setup_simple,
};
use lakekeeper_io::LakekeeperStorage as _;
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use serde::de::DeserializeOwned;
use sqlx::{
    PgPool,
    postgres::{PgPoolOptions, PgSslMode},
};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};

const TIMEOUT: Duration = Duration::from_secs(30);

/// `COMMIT` and `ROLLBACK` as simple-query messages, which is how sqlx sends them.
const COMMIT: &[u8] = b"Q\0\0\0\x0bCOMMIT\0";
const ROLLBACK: &[u8] = b"Q\0\0\0\x0dROLLBACK\0";
/// `CommandComplete` for `COMMIT`, then `ReadyForQuery` outside a transaction.
const COMMITTED: &[u8] = b"C\0\0\0\x0bCOMMIT\0Z\0\0\0\x05I";
const READY_FOR_QUERY_IDLE: &[u8] = b"Z\0\0\0\x05I";

/// Property whose value marks the transaction the proxy breaks.
const MARKER_KEY: &str = "test.commit-reply";
const MARKER_VALUE: &str = "dropped-6f0d1c9a";
const UNCOMPRESSED: (&str, &str) = ("write.metadata.compression-codec", "none");

type Ctx = ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>;
type ReplySender = Arc<Mutex<Option<oneshot::Sender<Vec<u8>>>>>;

/// How the proxy breaks the first transaction that sends [`MARKER_VALUE`].
#[derive(Clone, Copy)]
enum Fault {
    /// Forward its `COMMIT`, keep Postgres' reply from the client and close the connection.
    DropCommitReply,
    /// Close the connection without forwarding the bytes that carry the marker.
    CloseAtMarker,
}

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(TIMEOUT, future)
        .await
        .expect("timed out")
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

/// Starts a TCP proxy to `upstream` on an OS-assigned port that applies `fault` once. The
/// receiver gets the bytes of Postgres' reply that the proxy kept from the client.
async fn start_proxy(upstream: String, fault: Fault) -> (u16, oneshot::Receiver<Vec<u8>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (reply_tx, reply_rx) = oneshot::channel();
    let reply_tx: ReplySender = Arc::new(Mutex::new(Some(reply_tx)));
    tokio::spawn(async move {
        while let Ok((client, _)) = listener.accept().await {
            let Ok(server) = TcpStream::connect(&upstream).await else {
                continue;
            };
            tokio::spawn(relay(client, server, fault, reply_tx.clone()));
        }
    });
    (port, reply_rx)
}

async fn relay(client: TcpStream, server: TcpStream, fault: Fault, reply_tx: ReplySender) {
    let (mut client_rx, mut client_tx) = client.into_split();
    let (mut server_rx, mut server_tx) = server.into_split();
    let (drop_reply_tx, mut drop_reply_rx) = oneshot::channel::<oneshot::Sender<Vec<u8>>>();

    let to_client = tokio::spawn(async move {
        let mut buf = vec![0; 65536];
        loop {
            let n = match server_rx.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => n,
            };
            // Set before `COMMIT` is forwarded, so it is visible when its reply arrives.
            if let Ok(reply_tx) = drop_reply_rx.try_recv() {
                let mut reply = buf[..n].to_vec();
                while !reply.ends_with(READY_FOR_QUERY_IDLE) {
                    match server_rx.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => reply.extend_from_slice(&buf[..n]),
                    }
                }
                let _ = reply_tx.send(reply);
                // Dropping `client_tx` closes the client's side of the connection.
                return;
            }
            if client_tx.write_all(&buf[..n]).await.is_err() {
                return;
            }
        }
    });

    // Bytes sent in the current transaction; a message split across reads is still found.
    let mut sent = Vec::new();
    let mut marked = false;
    let mut buf = vec![0; 65536];
    loop {
        let n = match client_rx.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        sent.extend_from_slice(&buf[..n]);
        marked |= contains(&sent, MARKER_VALUE.as_bytes());
        let commit = contains(&sent, COMMIT);
        let fire = match fault {
            Fault::DropCommitReply => marked && commit,
            Fault::CloseAtMarker => marked,
        };
        let armed = fire.then(|| reply_tx.lock().unwrap().take()).flatten();
        if let Some(reply_tx) = armed {
            match fault {
                Fault::DropCommitReply => {
                    let _ = drop_reply_tx.send(reply_tx);
                    let _ = server_tx.write_all(&buf[..n]).await;
                }
                Fault::CloseAtMarker => {
                    let _ = reply_tx.send(Vec::new());
                }
            }
            break;
        }
        if commit || contains(&sent, ROLLBACK) {
            sent.clear();
            marked = false;
        }
        if server_tx.write_all(&buf[..n]).await.is_err() {
            break;
        }
    }
    // Postgres ends the session, so `to_client` sees EOF and closes the client's side.
    drop(server_tx);
    let _ = to_client.await;
}

/// Sets up a warehouse whose catalog reaches Postgres through the proxy.
async fn setup(
    pool: &PgPool,
    fault: Fault,
) -> (Ctx, NamespaceParameters, oneshot::Receiver<Vec<u8>>) {
    let options = pool.connect_options();
    let upstream = format!("{}:{}", options.get_host(), options.get_port());
    let (port, reply_rx) = start_proxy(upstream, fault).await;
    let proxied_pool = bounded(
        PgPoolOptions::new().max_connections(10).connect_with(
            (*options)
                .clone()
                .host("127.0.0.1")
                .port(port)
                // Plain text, so the proxy can read the protocol.
                .ssl_mode(PgSslMode::Disable),
        ),
    )
    .await
    .unwrap();
    let (ctx, warehouse) = bounded(setup_simple(
        proxied_pool,
        memory_io_profile(),
        None,
        AllowAllAuthorizer::default(),
        TabularDeleteProfile::Hard {},
        None,
    ))
    .await;
    let prefix = warehouse.warehouse_id.to_string();
    let namespace = bounded(create_ns(ctx.clone(), prefix.clone(), "ns".to_string()))
        .await
        .namespace;
    let ns_params = NamespaceParameters {
        prefix: Some(Prefix(prefix)),
        namespace,
    };
    (ctx, ns_params, reply_rx)
}

fn properties(marked: bool) -> HashMap<String, String> {
    let mut properties = HashMap::from([(UNCOMPRESSED.0.to_string(), UNCOMPRESSED.1.to_string())]);
    if marked {
        properties.insert(MARKER_KEY.to_string(), MARKER_VALUE.to_string());
    }
    properties
}

/// Creates table `t` with uncompressed metadata, so the test can parse the file.
async fn create_table_with_properties(
    ctx: &Ctx,
    ns_params: &NamespaceParameters,
    marked: bool,
) -> Result<TableMetadataRef, IcebergErrorResponse> {
    let mut request = create_table_request(Some("t".to_string()), Some(false));
    request.properties = Some(properties(marked));
    bounded(CatalogServer::create_table(
        ns_params.clone(),
        request,
        DataAccess::not_specified(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    ))
    .await
    .map(|result| result.metadata)
}

/// Sets the marker on table `t` with updateTable.
async fn mark_table(ctx: &Ctx, ns_params: &NamespaceParameters) -> IcebergErrorResponse {
    bounded(CatalogServer::commit_table(
        TableParameters {
            prefix: ns_params.prefix.clone(),
            table: TableIdent::new(ns_params.namespace.clone(), "t".to_string()),
        },
        CommitTableRequest {
            identifier: None,
            requirements: vec![],
            updates: vec![TableUpdate::SetProperties {
                updates: properties(true),
            }],
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    ))
    .await
    .unwrap_err()
}

/// Id and metadata location of the table or view `name` as committed in Postgres.
async fn committed_tabular(pool: &PgPool, name: &str) -> (uuid::Uuid, String) {
    bounded(
        sqlx::query_as("SELECT tabular_id, metadata_location FROM tabular WHERE name = $1")
            .bind(name)
            .fetch_one(pool),
    )
    .await
    .unwrap()
}

async fn read_metadata<T: DeserializeOwned>(location: &str) -> T {
    let io = memory_io_profile().file_io(None).await.unwrap();
    let file = bounded(io.read(location))
        .await
        .unwrap_or_else(|e| panic!("metadata file {location} is gone: {e:?}"));
    serde_json::from_slice(&file.bytes).unwrap()
}

/// Files in the `metadata` directory under `location`, sorted.
async fn list_metadata_files(location: &str) -> Vec<String> {
    let io = memory_io_profile().file_io(None).await.unwrap();
    let pages: Vec<_> = bounded(async {
        io.list(&format!("{location}/metadata/"), None)
            .await
            .unwrap()
            .try_collect()
            .await
    })
    .await
    .unwrap();
    pages
        .into_iter()
        .flatten()
        .map(|file| file.location().to_string())
        .collect()
}

fn assert_commit_state_unknown(error: &IcebergErrorResponse) {
    assert_eq!(error.error.code, 500);
    assert_eq!(error.error.r#type, "CommitStateUnknownException");
}

#[sqlx::test]
async fn update_table_keeps_metadata_file_when_commit_reply_is_lost(pool: PgPool) {
    let (ctx, ns_params, reply_rx) = setup(&pool, Fault::DropCommitReply).await;
    let created = create_table_with_properties(&ctx, &ns_params, false)
        .await
        .unwrap();
    let (table_id, old_location) = committed_tabular(&pool, "t").await;
    assert_eq!(table_id, created.uuid());

    let error = mark_table(&ctx, &ns_params).await;
    // Lets a delete spawned by the failed commit run before the files are checked.
    // `update_table_deletes_metadata_file_when_commit_fails` shows that it does.
    tokio::task::yield_now().await;

    assert_commit_state_unknown(&error);
    assert_eq!(bounded(reply_rx).await.unwrap(), COMMITTED);
    let (_, location) = committed_tabular(&pool, "t").await;
    assert_eq!(
        list_metadata_files(created.location()).await,
        [old_location, location.clone()]
    );
    let metadata: TableMetadata = read_metadata(&location).await;
    assert_eq!(metadata.uuid(), table_id);
    assert_eq!(metadata.properties().get(MARKER_KEY).unwrap(), MARKER_VALUE);
}

/// Positive control for the test above: in the same window, a failed commit's delete is seen.
#[sqlx::test]
async fn update_table_deletes_metadata_file_when_commit_fails(pool: PgPool) {
    let (ctx, ns_params, reply_rx) = setup(&pool, Fault::CloseAtMarker).await;
    let created = create_table_with_properties(&ctx, &ns_params, false)
        .await
        .unwrap();
    let (_, old_location) = committed_tabular(&pool, "t").await;

    let error = mark_table(&ctx, &ns_params).await;
    tokio::task::yield_now().await;

    assert_eq!(error.error.code, 503);
    assert_eq!(error.error.r#type, "CatalogBackendError");
    assert_eq!(bounded(reply_rx).await.unwrap(), b"");
    assert_eq!(committed_tabular(&pool, "t").await.1, old_location);
    assert_eq!(
        list_metadata_files(created.location()).await,
        [old_location]
    );
}

#[sqlx::test]
async fn create_table_keeps_metadata_file_when_commit_reply_is_lost(pool: PgPool) {
    let (ctx, ns_params, reply_rx) = setup(&pool, Fault::DropCommitReply).await;

    let error = create_table_with_properties(&ctx, &ns_params, true)
        .await
        .unwrap_err();

    assert_commit_state_unknown(&error);
    assert_eq!(bounded(reply_rx).await.unwrap(), COMMITTED);
    let (table_id, location) = committed_tabular(&pool, "t").await;
    let metadata: TableMetadata = read_metadata(&location).await;
    assert_eq!(metadata.uuid(), table_id);
    assert_eq!(metadata.properties().get(MARKER_KEY).unwrap(), MARKER_VALUE);
}

#[sqlx::test]
async fn commit_view_keeps_metadata_file_when_commit_reply_is_lost(pool: PgPool) {
    let (ctx, ns_params, reply_rx) = setup(&pool, Fault::DropCommitReply).await;
    let prefix = ns_params.prefix.clone().unwrap();
    bounded(create_view(ctx.clone(), prefix.as_str(), "ns", "v", None))
        .await
        .unwrap();
    let (view_id, old_location) = committed_tabular(&pool, "v").await;

    let error = bounded(CatalogServer::commit_view(
        ViewParameters {
            prefix: Some(prefix),
            view: TableIdent::new(NamespaceIdent::new("ns".to_string()), "v".to_string()),
        },
        CommitViewRequest {
            identifier: None,
            requirements: None,
            updates: vec![ViewUpdate::SetProperties {
                updates: properties(true),
            }],
        },
        ctx.clone(),
        DataAccess::not_specified(),
        RequestMetadata::new_unauthenticated(),
    ))
    .await
    .unwrap_err();
    tokio::task::yield_now().await;

    assert_commit_state_unknown(&error);
    assert_eq!(bounded(reply_rx).await.unwrap(), COMMITTED);
    let (_, location) = committed_tabular(&pool, "v").await;
    assert_ne!(location, old_location);
    let metadata: ViewMetadata = read_metadata(&location).await;
    assert_eq!(metadata.uuid(), view_id);
    assert_eq!(metadata.properties().get(MARKER_KEY).unwrap(), MARKER_VALUE);
}
