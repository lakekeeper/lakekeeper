use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, LazyLock};

use futures::stream::{FuturesUnordered, StreamExt};
use iceberg_ext::catalog::rest::IcebergErrorResponse;
use openfga_client::client::{
    BasicOpenFgaClient, BasicOpenFgaServiceClient, ConsistencyPreference, OpenFgaClient,
    ReadRequestTupleKey, TupleKey,
};
use strum::IntoEnumIterator;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::api::iceberg::v1::PageToken;
use crate::api::iceberg::v1::{NamespaceIdent, PaginationQuery};
use crate::service::authz::implementations::openfga::{
    NamespaceRelation, ProjectRelation, TableRelation, ViewRelation, WarehouseRelation,
};
use crate::service::{
    catalog::{ListFlags, Transaction},
    TableId,
};
use crate::service::{Catalog, ListNamespacesQuery, NamespaceId, WarehouseStatus};
use crate::{ProjectId, WarehouseId};

#[derive(Clone, Debug)]
pub(crate) struct MigrationState {
    pub store_name: String,
    pub server_id: uuid::Uuid,
}

fn openfga_user_type(inp: &str) -> Option<String> {
    inp.split(":").next().map(|user| user.to_string())
}

/// Prepends `lakekeeper_` to the type and injects `prefix` into a full OpenFGA object.
///
/// ```rust
/// let full_object = "table:t1";
/// let extended_object = "lakekeeper_table:wh1/t1";
/// assert_eq!(new_v4_tuple(full_object, "wh1"), extended_object.to_string());
/// ```
fn new_v4_tuple(full_object: &str, prefix: &str) -> anyhow::Result<String> {
    let parts: Vec<_> = full_object.split(":").collect();
    anyhow::ensure!(
        parts.len() == 2,
        "Expected full object (type:id), got {}",
        full_object
    );
    Ok(format!("lakekeeper_{}:{}/{}", parts[0], prefix, parts[1]))
}

fn extract_id_from_full_object(full_object: &str) -> anyhow::Result<String> {
    let parts: Vec<_> = full_object.split(":").collect();
    anyhow::ensure!(
        parts.len() == 2,
        "Expected full object (type:id), got {}",
        full_object
    );
    Ok(parts[1].to_string())
}

// TODO add v4 to module name as everything here is version specific

// TODO: get from config in case someone runs openfga server with lower max page size?
const OPENFGA_PAGE_SIZE: i32 = 100;

// TODO get from config, check if config value is overwritten by user's env var
const OPENFGA_WRITE_BATCH_SIZE: usize = 50;

/// Limits the number of concurrent transactions. It should be throttled as the catalog's db
/// may still be in use during the migration.
///
/// Ensure permits are dropped as soon as the tx is not needed anymore, to unblock other threads.
static DB_TX_PERMITS: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::const_new(10)));

/// Limits the number of concurrent requests to the OpenFGA server, to avoid overloading it.
///
/// Ensure the permit is dropped as soon as it's not needed anymore, to unblock other threads.
static OPENFGA_REQ_PERMITS: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::const_new(10)));

// catalog trait reingeben, nicht postgres db
pub(crate) async fn v4_push_down_warehouse_id(
    mut client: BasicOpenFgaServiceClient,
    _prev_auth_model_id: Option<String>,
    curr_auth_model_id: Option<String>,
    state: MigrationState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    println!("state in migration fn: {}", state.store_name);
    // Construct OpenFGAClient to be able to use convenience methods.
    let store = client
        .get_store_by_name(&state.store_name)
        .await?
        .expect("default store should exist");
    let curr_auth_model_id =
        curr_auth_model_id.expect("Migration hook needs current auth model's id");
    let client = client
        .into_client(&store.id, &curr_auth_model_id)
        .set_consistency(ConsistencyPreference::HigherConsistency);

    let projects = get_all_projects(&client, state.server_id).await?;
    let warehouses = get_all_warehouses(&client, projects).await?;
    let mut namespaces_per_wh: Vec<(String, Vec<String>)> = vec![];
    // TODO concurrency
    for wh in warehouses.into_iter() {
        let namespaces = get_all_namespaces(&client, wh.clone()).await?;
        namespaces_per_wh.push((wh, namespaces));
    }

    for (wh, nss) in namespaces_per_wh.into_iter() {
        // Load tabulars only inside this loop (i.e. per warehouse) and drop them at the end of it.
        // This is done to mitigate the risk of OOM during the migration of a huge catalog.
        let tabulars = get_all_tabulars(&client, &nss).await.unwrap();
        let mut new_tuples_to_write = vec![];
        let wh_id = extract_id_from_full_object(&wh)?;

        for tab in tabulars.into_iter() {
            let c1 = client.clone();
            let c2 = client.clone();
            let tab1 = tab.clone();
            let tab2 = tab.clone();
            let (tab_as_object, tab_as_user) = tokio::try_join!(
                // No need to get OPENFGA_REQ_PERMITS here as they will be acquired inside the
                // spawned functions.
                tokio::spawn(async move { get_all_tuples_with_object(&c1, tab1).await }),
                tokio::spawn(async move { get_all_tuples_with_user(&c2, tab2).await })
            )?;
            let (tab_as_object, tab_as_user) = (tab_as_object?, tab_as_user?);

            for mut tuple in tab_as_object.into_iter() {
                tuple.object = new_v4_tuple(&tuple.object, &wh_id)?;
                new_tuples_to_write.push(tuple);
            }
            for mut tuple in tab_as_user.into_iter() {
                tuple.user = new_v4_tuple(&tuple.user, &wh_id)?;
                new_tuples_to_write.push(tuple);
            }
        }

        let mut write_jobs = JoinSet::new();
        for chunk in new_tuples_to_write.chunks(OPENFGA_WRITE_BATCH_SIZE) {
            let c = client.clone();
            let semaphore = OPENFGA_REQ_PERMITS.clone();
            let tuples = chunk.to_vec();
            write_jobs.spawn(async move {
                let _permit = semaphore.acquire().await.unwrap();
                c.write(Some(tuples), None).await
            });
        }
        while let Some(res) = write_jobs.join_next().await {
            let _ = res??;
        }
    }

    let _res = add_warehouse_id_to_tables(client.clone(), &HashMap::new()).await?;
    Ok(())
}

/// Creates a new read transaction. To use it responsibly, first acquire a permit from
/// [`DB_TX_PERMITS`].
async fn new_read_transaction<C: Catalog>(state: C::State) -> crate::api::Result<C::Transaction> {
    let _permit = DB_TX_PERMITS.acquire().await.unwrap();
    C::Transaction::begin_read(state).await
}

/// Returns the ids of all warehouses, regardless of their status.
async fn all_warehouse_ids<C: Catalog>(
    catalog_state: C::State,
    project_ids: &[ProjectId],
) -> crate::api::Result<Vec<WarehouseId>> {
    let all_statuses: Vec<_> = WarehouseStatus::iter().collect();
    let mut jobs = FuturesUnordered::new();

    for pid in project_ids.iter() {
        let semaphore = DB_TX_PERMITS.clone();
        let catalog_state = catalog_state.clone();
        let all_statuses = all_statuses.clone();

        jobs.push(async move {
            let _permit = semaphore.acquire().await.unwrap();
            let mut tx = new_read_transaction::<C>(catalog_state).await?;

            // This returns all warehouses in the project since it is not (yet) possible to
            // deactivate a warehouse.
            let responses = C::list_warehouses(pid, Some(all_statuses), tx.transaction()).await?;
            drop(_permit);

            let ids = responses.into_iter().map(|res| res.id);
            Ok::<_, IcebergErrorResponse>(ids)
        });
    }

    let mut warehouse_ids = vec![];
    while let Some(res) = jobs.next().await {
        warehouse_ids.extend(res?)
    }
    Ok(warehouse_ids)
}

struct TabularQueryParams {
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    namespace_ident: NamespaceIdent,
}

// TODO check if can be more efficient by storing whid only once and mapping all ns to it
// same for table_id
struct TableParams {
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    namespace_ident: NamespaceIdent,
    table_id: TableId,
}

// TODO in catalog: implement fn that traverses all namespaces
// catalog's basic `list_namespaces` is only one level, not its children
/// Returns the query paramaters for all namespaces, which are needed to get all tabulars
/// via [`Catalog::list_tables`] and [`Catalog::list_views`].
async fn all_tabular_query_params<C: Catalog>(
    catalog_state: C::State,
    warehouse_ids: Vec<WarehouseId>,
) -> crate::api::Result<Vec<TabularQueryParams>> {
    let mut jobs = FuturesUnordered::new();

    for wid in warehouse_ids.into_iter() {
        let semaphore = DB_TX_PERMITS.clone();
        let catalog_state = catalog_state.clone();

        jobs.push(async move {
            let _permit = semaphore.acquire().await.unwrap();
            let mut tx = new_read_transaction::<C>(catalog_state).await?;

            // The function mentioned in TODO above is expected to use smaller page size + paginate
            let response = C::list_namespaces(
                wid.clone(),
                &ListNamespacesQuery {
                    page_token: PageToken::Empty,
                    page_size: Some(i64::MAX),
                    parent: None,
                    return_uuids: true,
                    return_protection_status: false,
                },
                tx.transaction(),
            )
            .await?;
            drop(_permit);

            let query_params =
                response
                    .into_iter()
                    .map(move |(nsid, ns_info)| TabularQueryParams {
                        warehouse_id: wid,
                        namespace_id: nsid,
                        namespace_ident: ns_info.namespace_ident,
                    });
            Ok::<_, IcebergErrorResponse>(query_params)
        })
    }

    let mut all_query_params = vec![];
    while let Some(res) = jobs.next().await {
        all_query_params.extend(res?);
    }

    Ok(all_query_params)
}

async fn all_tables<C: Catalog>(
    catalog_state: C::State,
    params: Vec<TabularQueryParams>,
) -> crate::api::Result<Vec<TableParams>> {
    let mut jobs = FuturesUnordered::new();

    for param in params.into_iter() {
        let semaphore = DB_TX_PERMITS.clone();
        let catalog_state = catalog_state.clone();

        jobs.push(async move {
            let _permit = semaphore.acquire().await.unwrap();
            let mut tx = new_read_transaction::<C>(catalog_state).await?;

            let response = C::list_tables(
                param.warehouse_id,
                &param.namespace_ident,
                ListFlags {
                    include_active: true,
                    include_staged: true,
                    include_deleted: true,
                },
                tx.transaction(),
                // TODO pagination with more reasonable page size
                PaginationQuery {
                    page_token: PageToken::Empty,
                    page_size: Some(i64::MAX),
                },
            )
            .await?;
            drop(_permit);

            let table_params = response.into_iter().map(move |(table_id, _)| TableParams {
                warehouse_id: param.warehouse_id,
                namespace_id: param.namespace_id,
                namespace_ident: param.namespace_ident.clone(),
                table_id,
            });
            Ok::<_, IcebergErrorResponse>(table_params)
        });
    }

    let mut all_table_params = vec![];
    while let Some(res) = jobs.next().await {
        all_table_params.extend(res?);
    }

    Ok(all_table_params)
}

async fn add_warehouse_id_to_tables<T>(
    _client: OpenFgaClient<T>,
    _warhouse_ids: &HashMap<TableId, WarehouseId>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    // let tables_
    Ok(())
}

// TODO concurrency, read with smaller page size
async fn get_all_projects(
    client: &BasicOpenFgaClient,
    server_id: uuid::Uuid,
) -> anyhow::Result<Vec<String>> {
    let request_key = ReadRequestTupleKey {
        user: format!("server:{server_id}"),
        relation: ProjectRelation::Server.to_string(),
        object: "project:".to_string(),
    };
    let tuples = client
        .read_all_pages(request_key, OPENFGA_PAGE_SIZE, u32::MAX)
        .await?;
    let projects = tuples
        .into_iter()
        .filter_map(|t| match t.key {
            None => None,
            Some(k) => Some(k.object),
        })
        .collect();
    Ok(projects)
}

async fn get_all_warehouses(
    client: &BasicOpenFgaClient,
    projects: Vec<String>,
) -> anyhow::Result<Vec<String>> {
    let mut all_warehouses = vec![];
    let mut jobs: JoinSet<anyhow::Result<Vec<String>>> = JoinSet::new();

    for p in projects.into_iter() {
        let client = client.clone();
        let semaphore = OPENFGA_REQ_PERMITS.clone();

        jobs.spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();
            let mut warehouses = vec![];
            let tuples = client
                .read_all_pages(
                    ReadRequestTupleKey {
                        user: p.to_string(),
                        relation: WarehouseRelation::Project.to_string(),
                        object: "warehouse:".to_string(),
                    },
                    OPENFGA_PAGE_SIZE,
                    u32::MAX,
                )
                .await?;
            drop(_permit);
            for t in tuples.into_iter() {
                match t.key {
                    None => {}
                    Some(k) => warehouses.push(k.object),
                }
            }
            Ok(warehouses)
        });
    }

    while let Some(whs) = jobs.join_next().await {
        all_warehouses.extend(whs??);
    }
    Ok(all_warehouses)
}

async fn get_all_namespaces(
    client: &BasicOpenFgaClient,
    warehouse: String,
) -> anyhow::Result<Vec<String>> {
    let mut namespaces = vec![];
    let mut to_process = VecDeque::from([warehouse.clone()]);

    // Breadth-first search to query namespaces at a given level in parallel.
    while !to_process.is_empty() {
        let mut jobs = tokio::task::JoinSet::new();
        let parents: Vec<String> = to_process.drain(..).collect();
        for parent in parents.iter() {
            let client = client.clone();
            let parent = parent.clone();
            let semaphore = OPENFGA_REQ_PERMITS.clone();

            jobs.spawn(async move {
                let _permit = semaphore.acquire().await.unwrap();
                let tuples = client
                    .read_all_pages(
                        ReadRequestTupleKey {
                            user: parent.clone(),
                            relation: NamespaceRelation::Parent.to_string(),
                            object: "namespace:".to_string(),
                        },
                        OPENFGA_PAGE_SIZE,
                        u32::MAX,
                    )
                    .await?;
                drop(_permit);
                let children: Vec<String> = tuples
                    .into_iter()
                    .filter_map(|t| t.key.map(|k| k.object))
                    .collect();
                Ok::<_, anyhow::Error>(children)
            });
        }
        while let Some(res) = jobs.join_next().await {
            let children = res??;
            for ns in children {
                namespaces.push(ns.clone());
                to_process.push_back(ns);
            }
        }
    }
    Ok(namespaces)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::EnumIter)]
enum TabularType {
    Table,
    View,
}

impl TabularType {
    fn object_type(&self) -> String {
        match self {
            Self::Table => "table:".to_string(),
            Self::View => "view:".to_string(),
        }
    }

    fn parent_relation_string(&self) -> String {
        match self {
            Self::Table => TableRelation::Parent.to_string(),
            Self::View => ViewRelation::Parent.to_string(),
        }
    }
}

async fn get_all_tabulars(
    client: &BasicOpenFgaClient,
    namespaces: &[String],
) -> anyhow::Result<Vec<String>> {
    let mut all_tabulars = vec![];
    let mut jobs = tokio::task::JoinSet::new();

    // Spawn one task per namespace. It will spawn nested tasks to get all tabular types.
    for ns in namespaces.iter() {
        let client = client.clone();
        let ns = ns.clone();
        jobs.spawn(async move {
            let mut tabular_jobs = tokio::task::JoinSet::new();

            for tab in TabularType::iter() {
                let client = client.clone();
                let ns = ns.clone();
                let relation = tab.parent_relation_string();
                let object_type = tab.object_type();
                let semaphore = OPENFGA_REQ_PERMITS.clone();
                tabular_jobs.spawn(async move {
                    let _permit = semaphore.acquire().await.unwrap();
                    let tuples = client
                        .read_all_pages(
                            ReadRequestTupleKey {
                                user: ns,
                                relation,
                                object: object_type,
                            },
                            OPENFGA_PAGE_SIZE,
                            u32::MAX,
                        )
                        .await?;
                    drop(_permit);
                    let tabulars: Vec<String> = tuples
                        .into_iter()
                        .filter_map(|t| t.key.map(|k| k.object))
                        .collect();
                    Ok::<_, anyhow::Error>(tabulars)
                });
            }

            let mut namespace_tabulars = vec![];
            while let Some(res) = tabular_jobs.join_next().await {
                namespace_tabulars.extend(res??);
            }

            Ok::<_, anyhow::Error>(namespace_tabulars)
        });
    }

    while let Some(res) = jobs.join_next().await {
        all_tabulars.extend(res??);
    }
    Ok(all_tabulars)
}

/// The `object` must specify both type and id (`type:id`).
///
/// The returned result contains all tuples that have the provided `object` from all relations
/// and all user types.
async fn get_all_tuples_with_object(
    client: &BasicOpenFgaClient,
    object: String,
) -> anyhow::Result<Vec<TupleKey>> {
    let tuples = client
        .read_all_pages(
            ReadRequestTupleKey {
                user: "".to_string(),
                relation: "".to_string(),
                object,
            },
            OPENFGA_PAGE_SIZE,
            u32::MAX,
        )
        .await?;
    Ok(tuples.into_iter().filter_map(|t| t.key).collect())
}

/// The `user` must specify both type and id (`type:id`)
///
/// The returned result contains all tuples that have the provided `user` from all all relations
/// and all object types.
async fn get_all_tuples_with_user(
    client: &BasicOpenFgaClient,
    user: String,
) -> anyhow::Result<Vec<TupleKey>> {
    // Querying OpenFGA's `/read` endpoint with a `TupleKey` requires at least an object type.
    // A query with `object: "user:"` is accepted while `object: ""` is not accepted.
    // These types are hardcoded as strings since we need their identifiers as of v3.4.
    let user_type =
        openfga_user_type(&user).ok_or(anyhow::anyhow!("A user type must be specified"))?;
    let object_types = match user_type.as_ref() {
        "server" => vec!["project:".to_string()],
        "user" | "role" => vec![
            "role:".to_string(),
            "server:".to_string(),
            "project:".to_string(),
            "warehouse:".to_string(),
            "namespace:".to_string(),
            "table:".to_string(),
            "view:".to_string(),
        ],
        "project" => vec!["server:".to_string(), "warehouse:".to_string()],
        "warehouse" => vec!["project:".to_string(), "namespace:".to_string()],
        "namespace" => vec![
            "warehouse:".to_string(),
            "namespace:".to_string(),
            "table:".to_string(),
            "view:".to_string(),
        ],
        "view" | "table" => vec!["namespace:".to_string()],
        "modelversion" => vec![],
        "authmodelid" => vec!["modelversion:".to_string()],
        _ => anyhow::bail!("Unexpected user type: {user_type}"),
    };

    let mut jobs = tokio::task::JoinSet::new();
    for ty in object_types.into_iter() {
        let client = client.clone();
        let user = user.clone();
        let semaphore = OPENFGA_REQ_PERMITS.clone();

        jobs.spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();
            let res = client
                .read_all_pages(
                    ReadRequestTupleKey {
                        user,
                        relation: "".to_string(),
                        object: ty,
                    },
                    OPENFGA_PAGE_SIZE,
                    u32::MAX,
                )
                .await?;
            let keys: Vec<TupleKey> = res.into_iter().filter_map(|t| t.key).collect();
            Ok::<_, anyhow::Error>(keys)
        });
    }

    let mut tuples = vec![];
    while let Some(res) = jobs.join_next().await {
        tuples.extend(res??);
    }
    Ok(tuples)
}

#[cfg(test)]
mod tests {

    mod openfga_integration_tests {
        use std::time::Instant;

        use openfga_client::{client::TupleKey, migration::TupleModelManager};
        use tokio::task::JoinSet;

        use super::super::*;
        use crate::{
            api::RequestMetadata,
            service::{
                authz::{
                    implementations::openfga::{
                        migration::{
                            add_model_v3, add_model_v4, tests::authorizer_for_empty_store,
                            V3_MODEL_VERSION,
                        },
                        new_client_from_config, OpenFGAAuthorizer, OpenFgaEntity, AUTH_CONFIG,
                        OPENFGA_SERVER,
                    },
                    Authorizer, NamespaceParent,
                },
                UserId, ViewId,
            },
            CONFIG,
        };

        // Tests must write tuples according to v3 model manually.
        // Writing through methods like `authorizer.create_*` may create tuples different from
        // what v4 migration is designed to handle.

        #[tokio::test]
        async fn test_get_all_projects() -> anyhow::Result<()> {
            let (_, authorizer) = authorizer_for_empty_store().await;

            authorizer
                .write(
                    Some(vec![
                        TupleKey {
                            user: OPENFGA_SERVER.clone(),
                            relation: ProjectRelation::Server.to_string(),
                            object: "project:p1".to_string(),
                            condition: None,
                        },
                        TupleKey {
                            user: OPENFGA_SERVER.clone(),
                            relation: ProjectRelation::Server.to_string(),
                            object: "project:p2".to_string(),
                            condition: None,
                        },
                        // Projects that must *not* be in the result.
                        // These are on a different server so they should not be returned
                        // when querying for projects on the current server.
                        TupleKey {
                            user: "server:other-server-id".to_string(),
                            relation: ProjectRelation::Server.to_string(),
                            object: "project:p-other-server".to_string(),
                            condition: None,
                        },
                        TupleKey {
                            user: "server:another-server-id".to_string(),
                            relation: ProjectRelation::Server.to_string(),
                            object: "project:p-another-server".to_string(),
                            condition: None,
                        },
                    ]),
                    None,
                )
                .await?;

            let mut projects = get_all_projects(&authorizer.client, CONFIG.server_id).await?;
            projects.sort();
            assert_eq!(
                projects,
                vec!["project:p1".to_string(), "project:p2".to_string()]
            );
            Ok(())
        }

        #[tokio::test]
        async fn test_get_all_projects_empty_server() -> anyhow::Result<()> {
            let (_, authorizer) = authorizer_for_empty_store().await;

            // Test with a server that has no projects
            let projects = get_all_projects(&authorizer.client, CONFIG.server_id).await?;
            assert!(projects.is_empty());
            Ok(())
        }

        #[tokio::test]
        async fn test_get_all_warehouses() -> anyhow::Result<()> {
            let (_, authorizer) = authorizer_for_empty_store().await;

            authorizer
                .write(
                    Some(vec![
                        TupleKey {
                            user: "project:p1".to_string(),
                            relation: WarehouseRelation::Project.to_string(),
                            object: "warehouse:w1".to_string(),
                            condition: None,
                        },
                        TupleKey {
                            user: "project:p1".to_string(),
                            relation: WarehouseRelation::Project.to_string(),
                            object: "warehouse:w2".to_string(),
                            condition: None,
                        },
                        TupleKey {
                            user: "project:p2".to_string(),
                            relation: WarehouseRelation::Project.to_string(),
                            object: "warehouse:w3".to_string(),
                            condition: None,
                        },
                        // Warehouses that must *not* be in the result.
                        // These are in projects on a different server so they should not be
                        // returned when querying for warehouses in projects p1 and p2.
                        TupleKey {
                            user: "project:p-other-server".to_string(),
                            relation: WarehouseRelation::Project.to_string(),
                            object: "warehouse:w-other-server".to_string(),
                            condition: None,
                        },
                        TupleKey {
                            user: "project:p-other-server-2".to_string(),
                            relation: WarehouseRelation::Project.to_string(),
                            object: "warehouse:w-other-server-2".to_string(),
                            condition: None,
                        },
                    ]),
                    None,
                )
                .await?;

            let projects = vec!["project:p1".to_string(), "project:p2".to_string()];
            let mut warehouses = get_all_warehouses(&authorizer.client, projects).await?;
            warehouses.sort();
            assert_eq!(
                warehouses,
                vec![
                    "warehouse:w1".to_string(),
                    "warehouse:w2".to_string(),
                    "warehouse:w3".to_string()
                ]
            );
            Ok(())
        }

        #[tokio::test]
        async fn test_get_all_warehouses_empty_project() -> anyhow::Result<()> {
            let (_, authorizer) = authorizer_for_empty_store().await;

            // Test with a project that has no warehouses
            let projects = vec!["project:empty".to_string()];
            let warehouses = get_all_warehouses(&authorizer.client, projects).await?;
            assert!(warehouses.is_empty());
            Ok(())
        }

        #[tokio::test]
        async fn test_get_all_namespaces() -> anyhow::Result<()> {
            let (_, authorizer) = authorizer_for_empty_store().await;

            // warehouse:w1 -> ns1 -> ns2 -> ns3
            //            |--> ns4
            // warehouse:w2 -> ns-other-wh -> ns-other-wh-child
            authorizer
                .write(
                    Some(vec![
                        TupleKey {
                            user: "user:actor".to_string(),
                            relation: NamespaceRelation::Ownership.to_string(),
                            object: "namespace:ns1".to_string(),
                            condition: None,
                        },
                        TupleKey {
                            user: "warehouse:w1".to_string(),
                            relation: NamespaceRelation::Parent.to_string(),
                            object: "namespace:ns1".to_string(),
                            condition: None,
                        },
                        TupleKey {
                            user: "namespace:ns1".to_string(),
                            relation: NamespaceRelation::Parent.to_string(),
                            object: "namespace:ns2".to_string(),
                            condition: None,
                        },
                        TupleKey {
                            user: "namespace:ns2".to_string(),
                            relation: NamespaceRelation::Parent.to_string(),
                            object: "namespace:ns3".to_string(),
                            condition: None,
                        },
                        TupleKey {
                            user: "warehouse:w1".to_string(),
                            relation: NamespaceRelation::Parent.to_string(),
                            object: "namespace:ns4".to_string(),
                            condition: None,
                        },
                        // Namespaces that must *not* be in the result.
                        // These are in a different warehouse (w2) so they should not be returned
                        // when querying for namespaces in warehouse:w1.
                        TupleKey {
                            user: "warehouse:w2".to_string(),
                            relation: NamespaceRelation::Parent.to_string(),
                            object: "namespace:ns-other-wh".to_string(),
                            condition: None,
                        },
                        TupleKey {
                            user: "namespace:ns-other-wh".to_string(),
                            relation: NamespaceRelation::Parent.to_string(),
                            object: "namespace:ns-other-wh-child".to_string(),
                            condition: None,
                        },
                    ]),
                    None,
                )
                .await?;

            let mut namespaces =
                get_all_namespaces(&authorizer.client, "warehouse:w1".to_string()).await?;
            namespaces.sort();
            assert_eq!(
                namespaces,
                vec![
                    "namespace:ns1".to_string(),
                    "namespace:ns2".to_string(),
                    "namespace:ns3".to_string(),
                    "namespace:ns4".to_string()
                ]
            );
            Ok(())
        }

        #[tokio::test]
        async fn test_get_all_namespaces_empty_warehouse() -> anyhow::Result<()> {
            let (_, authorizer) = authorizer_for_empty_store().await;

            let namespaces =
                get_all_namespaces(&authorizer.client, "warehouse:empty".to_string()).await?;
            assert!(namespaces.is_empty());
            Ok(())
        }

        #[tokio::test]
        async fn test_get_all_tabulars() -> anyhow::Result<()> {
            let (_, authorizer) = authorizer_for_empty_store().await;

            // Create structure:
            // namespace:ns1 -> table:t1, view:v1
            // namespace:ns2 -> table:t2, table:t3, view:v2
            // namespace:ns-other-wh -> table:table-other-wh, view:view-other-wh
            authorizer
                .write(
                    Some(vec![
                        // Tables
                        TupleKey {
                            user: "namespace:ns1".to_string(),
                            relation: TableRelation::Parent.to_string(),
                            object: "table:t1".to_string(),
                            condition: None,
                        },
                        TupleKey {
                            user: "namespace:ns2".to_string(),
                            relation: TableRelation::Parent.to_string(),
                            object: "table:t2".to_string(),
                            condition: None,
                        },
                        TupleKey {
                            user: "namespace:ns2".to_string(),
                            relation: TableRelation::Parent.to_string(),
                            object: "table:t3".to_string(),
                            condition: None,
                        },
                        // Views
                        TupleKey {
                            user: "namespace:ns1".to_string(),
                            relation: ViewRelation::Parent.to_string(),
                            object: "view:v1".to_string(),
                            condition: None,
                        },
                        TupleKey {
                            user: "namespace:ns2".to_string(),
                            relation: ViewRelation::Parent.to_string(),
                            object: "view:v2".to_string(),
                            condition: None,
                        },
                        // Tabulars that must *not* be in the result.
                        // For example because they are in a different warehouse so their namespace
                        // is not included in the list of namespaces to query.
                        TupleKey {
                            user: "namespace:ns-other-wh".to_string(),
                            relation: TableRelation::Parent.to_string(),
                            object: "table:table-other-wh".to_string(),
                            condition: None,
                        },
                        TupleKey {
                            user: "namespace:ns-other-wh".to_string(),
                            relation: ViewRelation::Parent.to_string(),
                            object: "view:view-other-wh".to_string(),
                            condition: None,
                        },
                    ]),
                    None,
                )
                .await?;

            let namespaces = vec!["namespace:ns1".to_string(), "namespace:ns2".to_string()];
            let mut tabulars = get_all_tabulars(&authorizer.client, &namespaces).await?;
            tabulars.sort();
            assert_eq!(
                tabulars,
                vec![
                    "table:t1".to_string(),
                    "table:t2".to_string(),
                    "table:t3".to_string(),
                    "view:v1".to_string(),
                    "view:v2".to_string()
                ]
            );
            Ok(())
        }

        #[tokio::test]
        async fn test_get_all_tabulars_empty_namespaces() -> anyhow::Result<()> {
            let (_, authorizer) = authorizer_for_empty_store().await;

            // Test with namespaces that have no tables or views
            let namespaces = vec![
                "namespace:empty1".to_string(),
                "namespace:empty2".to_string(),
            ];
            let tabulars = get_all_tabulars(&authorizer.client, &namespaces).await?;
            assert!(tabulars.is_empty());
            Ok(())
        }

        #[tokio::test]
        async fn test_get_all_tuples_with_object() -> anyhow::Result<()> {
            let (_, authorizer) = authorizer_for_empty_store().await;

            authorizer
                .write(
                    Some(vec![
                        // Tuples with the target object "table:target-table"
                        TupleKey {
                            user: "user:user1".to_string(),
                            relation: TableRelation::PassGrants.to_string(),
                            object: "table:target-table".to_string(),
                            condition: None,
                        },
                        TupleKey {
                            user: "namespace:ns1".to_string(),
                            relation: TableRelation::Parent.to_string(),
                            object: "table:target-table".to_string(),
                            condition: None,
                        },
                        TupleKey {
                            user: "user:user2".to_string(),
                            relation: TableRelation::Ownership.to_string(),
                            object: "table:target-table".to_string(),
                            condition: None,
                        },
                        // Tuples with different objects that should *not* be returned
                        TupleKey {
                            user: "user:user1".to_string(),
                            relation: TableRelation::Select.to_string(),
                            object: "table:other-table".to_string(),
                            condition: None,
                        },
                        TupleKey {
                            user: "namespace:ns2".to_string(),
                            relation: ViewRelation::Parent.to_string(),
                            object: "view:some-view".to_string(),
                            condition: None,
                        },
                    ]),
                    None,
                )
                .await?;

            let mut tuples =
                get_all_tuples_with_object(&authorizer.client, "table:target-table".to_string())
                    .await?;
            // Sort by user and relation for consistent comparison
            tuples.sort_by(|a, b| {
                a.user
                    .cmp(&b.user)
                    .then_with(|| a.relation.cmp(&b.relation))
            });

            assert_eq!(tuples.len(), 3);
            assert_eq!(tuples[0].user, "namespace:ns1".to_string());
            assert_eq!(tuples[0].relation, TableRelation::Parent.to_string());
            assert_eq!(tuples[0].object, "table:target-table".to_string());

            assert_eq!(tuples[1].user, "user:user1".to_string());
            assert_eq!(tuples[1].relation, TableRelation::PassGrants.to_string());
            assert_eq!(tuples[1].object, "table:target-table".to_string());

            assert_eq!(tuples[2].user, "user:user2".to_string());
            assert_eq!(tuples[2].relation, TableRelation::Ownership.to_string());
            assert_eq!(tuples[2].object, "table:target-table".to_string());

            Ok(())
        }

        #[tokio::test]
        async fn test_get_all_tuples_with_object_empty() -> anyhow::Result<()> {
            let (_, authorizer) = authorizer_for_empty_store().await;

            // Test with an object that doesn't exist
            let tuples =
                get_all_tuples_with_object(&authorizer.client, "table:nonexistent".to_string())
                    .await?;
            assert!(tuples.is_empty());
            Ok(())
        }

        /// Testing for user type `table` which can be the user in only one relation as of v3.
        #[tokio::test]
        async fn test_get_all_tuples_with_user() -> anyhow::Result<()> {
            let (_, authorizer) = authorizer_for_empty_store().await;

            // Write tuples with "table:target-table" as user and various objects
            authorizer
                .write(
                    Some(vec![
                        // Should be returned - table as user
                        TupleKey {
                            user: "table:target-table".to_string(),
                            relation: NamespaceRelation::Child.to_string(),
                            object: "namespace:parent-ns".to_string(),
                            condition: None,
                        },
                        // Should NOT be returned (different user)
                        TupleKey {
                            user: "table:other-table".to_string(),
                            relation: NamespaceRelation::Child.to_string(),
                            object: "namespace:parent-ns".to_string(),
                            condition: None,
                        },
                        TupleKey {
                            user: "user:someone".to_string(),
                            relation: TableRelation::Ownership.to_string(),
                            object: "table:target-table".to_string(),
                            condition: None,
                        },
                    ]),
                    None,
                )
                .await?;

            let tuples =
                get_all_tuples_with_user(&authorizer.client, "table:target-table".to_string())
                    .await?;

            // Only tuples with user == "table:target-table" should be returned
            assert_eq!(tuples.len(), 1);
            assert_eq!(tuples[0].user, "table:target-table");
            assert_eq!(tuples[0].relation, NamespaceRelation::Child.to_string());
            assert_eq!(tuples[0].object, "namespace:parent-ns");

            Ok(())
        }

        /// Testing for user type `namespace` which can be the user in multiple relations.
        #[tokio::test]
        async fn test_get_all_tuples_with_user_multiple_results() -> anyhow::Result<()> {
            let (_, authorizer) = authorizer_for_empty_store().await;

            // Write tuples with "namespace:target-ns" as user in multiple relations
            authorizer
                .write(
                    Some(vec![
                        // Should be returned - namespace as user in different relations
                        TupleKey {
                            user: "namespace:target-ns".to_string(),
                            relation: TableRelation::Parent.to_string(),
                            object: "table:child-table1".to_string(),
                            condition: None,
                        },
                        TupleKey {
                            user: "namespace:target-ns".to_string(),
                            relation: ViewRelation::Parent.to_string(),
                            object: "view:child-view1".to_string(),
                            condition: None,
                        },
                        TupleKey {
                            user: "namespace:target-ns".to_string(),
                            relation: NamespaceRelation::Parent.to_string(),
                            object: "namespace:child-ns1".to_string(),
                            condition: None,
                        },
                        TupleKey {
                            user: "namespace:target-ns".to_string(),
                            relation: WarehouseRelation::Namespace.to_string(),
                            object: "warehouse:parent-wh".to_string(),
                            condition: None,
                        },
                        // Should NOT be returned (different user)
                        TupleKey {
                            user: "namespace:other-ns".to_string(),
                            relation: TableRelation::Parent.to_string(),
                            object: "table:other-table".to_string(),
                            condition: None,
                        },
                        TupleKey {
                            user: "user:someone".to_string(),
                            relation: NamespaceRelation::Ownership.to_string(),
                            object: "namespace:target-ns".to_string(),
                            condition: None,
                        },
                    ]),
                    None,
                )
                .await?;

            let mut tuples =
                get_all_tuples_with_user(&authorizer.client, "namespace:target-ns".to_string())
                    .await?;

            // Sort by object for consistent comparison
            tuples.sort_by(|a, b| a.object.cmp(&b.object));

            // Only tuples with user == "namespace:target-ns" should be returned
            assert_eq!(tuples.len(), 4);

            assert_eq!(tuples[0].user, "namespace:target-ns");
            assert_eq!(tuples[0].relation, NamespaceRelation::Parent.to_string());
            assert_eq!(tuples[0].object, "namespace:child-ns1");

            assert_eq!(tuples[1].user, "namespace:target-ns");
            assert_eq!(tuples[1].relation, TableRelation::Parent.to_string());
            assert_eq!(tuples[1].object, "table:child-table1");

            assert_eq!(tuples[2].user, "namespace:target-ns");
            assert_eq!(tuples[2].relation, ViewRelation::Parent.to_string());
            assert_eq!(tuples[2].object, "view:child-view1");

            assert_eq!(tuples[3].user, "namespace:target-ns");
            assert_eq!(tuples[3].relation, WarehouseRelation::Namespace.to_string());
            assert_eq!(tuples[3].object, "warehouse:parent-wh");

            Ok(())
        }

        #[tokio::test]
        async fn test_get_all_tuples_with_user_empty() -> anyhow::Result<()> {
            let (_, authorizer) = authorizer_for_empty_store().await;

            // Test with a user that doesn't exist
            let tuples =
                get_all_tuples_with_user(&authorizer.client, "user:nonexistent".to_string())
                    .await?;
            assert!(tuples.is_empty());
            Ok(())
        }

        /// Constructs a client for a store that has been initialized and migrated to v3.
        /// Returns the client and the name of the store.
        // TODO all of the above must use this instead of authorizer_for_empty_store
        async fn v3_client_for_empty_store() -> anyhow::Result<(BasicOpenFgaClient, String)> {
            // TODO refactor openfga::migration::migrate s.t. no need to replicate it here
            let mut client = new_client_from_config().await?;
            let test_uuid = uuid::Uuid::now_v7();
            let store_name = format!("test_store_{test_uuid}");

            let model_manager = TupleModelManager::new(
                client.clone(),
                &store_name,
                &AUTH_CONFIG.authorization_model_prefix,
            );
            let mut model_manager = add_model_v3(model_manager);
            let migration_state = MigrationState {
                store_name: store_name.clone(),
                server_id: CONFIG.server_id,
            };
            model_manager.migrate(migration_state).await?;

            // TODO untangle new_authorizer from get_active_auth_model_id to use it here
            // instead of manually constructing authorizer
            // let authorizer = new_authorizer::<PostgresCatalog>(
            //     client,
            //     Some(store_name.clone()),
            //     openfga_client::client::ConsistencyPreference::HigherConsistency,
            // )
            // .await?;
            let store = client
                .get_store_by_name(&store_name)
                .await?
                .ok_or_else(|| anyhow::anyhow!("Store should exist after initialization"))?;
            let auth_model_id = model_manager
                .get_authorization_model_id(*V3_MODEL_VERSION)
                .await?
                .ok_or_else(|| anyhow::anyhow!("Auth model should be set after migration"))?;
            let client = BasicOpenFgaClient::new(client, &store.id, &auth_model_id)
                .set_consistency(ConsistencyPreference::HigherConsistency);
            Ok((client, store_name))
        }

        // Migrates the OpenFGA store to v4, which will also execute the migration function.
        async fn migrate_to_v4(
            client: &BasicOpenFgaServiceClient,
            store_name: String,
        ) -> anyhow::Result<()> {
            let model_manager = TupleModelManager::new(
                client.clone(),
                &store_name,
                &AUTH_CONFIG.authorization_model_prefix,
            );
            let mut model_manager = add_model_v4(model_manager);
            let migration_state = MigrationState {
                store_name: store_name.clone(),
                server_id: CONFIG.server_id,
            };
            println!("migrating to model v4");
            model_manager
                .migrate(migration_state)
                .await
                .map_err(|e| e.into())
        }

        #[tokio::test]
        async fn test_v4_push_down_warehouse_id() -> anyhow::Result<()> {
            let (client, store_name) = v3_client_for_empty_store().await?;

            // Create the initial tuple structure:
            //
            // Project p1 has two warehouses.
            // warehouse:wh1 -> namespace:ns1 -> table:t1, table:t2
            //                            |--> namespace:ns1_child -> view:v1, table:t3
            // warehouse:wh2 -> namespace:ns2 -> table:t4
            let initial_tuples = vec![
                // Project structure
                TupleKey {
                    user: OPENFGA_SERVER.clone(),
                    relation: ProjectRelation::Server.to_string(),
                    object: "project:p1".to_string(),
                    condition: None,
                },
                TupleKey {
                    user: "project:p1".to_string(),
                    relation: WarehouseRelation::Project.to_string(),
                    object: "warehouse:wh1".to_string(),
                    condition: None,
                },
                TupleKey {
                    user: "project:p1".to_string(),
                    relation: WarehouseRelation::Project.to_string(),
                    object: "warehouse:wh2".to_string(),
                    condition: None,
                },
                // Namespace structure for warehouse 1
                TupleKey {
                    user: "warehouse:wh1".to_string(),
                    relation: NamespaceRelation::Parent.to_string(),
                    object: "namespace:ns1".to_string(),
                    condition: None,
                },
                TupleKey {
                    user: "namespace:ns1".to_string(),
                    relation: NamespaceRelation::Parent.to_string(),
                    object: "namespace:ns1_child".to_string(),
                    condition: None,
                },
                // Namespace structure for warehouse 2
                TupleKey {
                    user: "warehouse:wh2".to_string(),
                    relation: NamespaceRelation::Parent.to_string(),
                    object: "namespace:ns2".to_string(),
                    condition: None,
                },
                // Tables in ns1 (two tables)
                TupleKey {
                    user: "namespace:ns1".to_string(),
                    relation: TableRelation::Parent.to_string(),
                    object: "table:t1".to_string(),
                    condition: None,
                },
                TupleKey {
                    user: "table:t1".to_string(),
                    relation: NamespaceRelation::Child.to_string(),
                    object: "namespace:ns1".to_string(),
                    condition: None,
                },
                TupleKey {
                    user: "namespace:ns1".to_string(),
                    relation: TableRelation::Parent.to_string(),
                    object: "table:t2".to_string(),
                    condition: None,
                },
                TupleKey {
                    user: "table:t2".to_string(),
                    relation: NamespaceRelation::Child.to_string(),
                    object: "namespace:ns1".to_string(),
                    condition: None,
                },
                // Tables and Views in ns1_child (one view and one table)
                TupleKey {
                    user: "namespace:ns1_child".to_string(),
                    relation: ViewRelation::Parent.to_string(),
                    object: "view:v1".to_string(),
                    condition: None,
                },
                TupleKey {
                    user: "view:v1".to_string(),
                    relation: NamespaceRelation::Child.to_string(),
                    object: "namespace:ns1_child".to_string(),
                    condition: None,
                },
                TupleKey {
                    user: "namespace:ns1_child".to_string(),
                    relation: TableRelation::Parent.to_string(),
                    object: "table:t3".to_string(),
                    condition: None,
                },
                TupleKey {
                    user: "table:t3".to_string(),
                    relation: NamespaceRelation::Child.to_string(),
                    object: "namespace:ns1_child".to_string(),
                    condition: None,
                },
                // Table in ns2
                TupleKey {
                    user: "namespace:ns2".to_string(),
                    relation: TableRelation::Parent.to_string(),
                    object: "table:t4".to_string(),
                    condition: None,
                },
                TupleKey {
                    user: "table:t4".to_string(),
                    relation: NamespaceRelation::Child.to_string(),
                    object: "namespace:ns2".to_string(),
                    condition: None,
                },
                // Some additional relations for tables/views (ownership, etc.)
                TupleKey {
                    user: "user:owner1".to_string(),
                    relation: TableRelation::Ownership.to_string(),
                    object: "table:t1".to_string(),
                    condition: None,
                },
                TupleKey {
                    user: "user:owner2".to_string(),
                    relation: ViewRelation::Ownership.to_string(),
                    object: "view:v1".to_string(),
                    condition: None,
                },
            ];

            // Write initial tuples
            client.write(Some(initial_tuples.clone()), None).await?;

            println!(
                "projects queried from test pre v4: {:#?}",
                get_all_projects(&client, CONFIG.server_id).await?
            );

            // Migrate to v4, which will call the migration fn.
            // TODO use migrate_to_v4, which will resolve stuff mentioned there first
            migrate_to_v4(&client.client(), store_name.clone()).await?;
            println!("authorizer is using store: {store_name}");
            println!(
                "authorizer is using auth_model_id: {}",
                client.authorization_model_id()
            );

            println!(
                "projects queried from test post v4: {:#?}",
                get_all_projects(&client, CONFIG.server_id).await?
            );

            // Read all tuples from store
            let all_tuples = client
                .read_all_pages(
                    ReadRequestTupleKey {
                        user: "".to_string(),
                        relation: "".to_string(),
                        object: "".to_string(),
                    },
                    100,
                    1000,
                )
                .await?;

            // println!("all tuples: {:#?}", all_tuples);
            let all_tuple_keys: Vec<TupleKey> =
                all_tuples.into_iter().filter_map(|t| t.key).collect();
            // println!("all tuple keys: {:#?}", all_tuple_keys);

            // Separate initial tuples from new tuples added by migration and filter out
            // tuples belonging to the store's admin relations.
            let mut new_tuples = vec![];
            for tuple in all_tuple_keys {
                if tuple.relation != "exists"
                    && tuple.relation != "openfga_id"
                    && !initial_tuples.contains(&tuple)
                {
                    new_tuples.push(tuple);
                }
            }

            // Expected new tuples should have warehouse ID prefixed to table/view IDs
            let expected_new_tuples = vec![
                // Updated object references (table/view objects with warehouse prefix)
                TupleKey {
                    user: "namespace:ns1".to_string(),
                    relation: TableRelation::Parent.to_string(),
                    object: "lakekeeper_table:wh1/t1".to_string(),
                    condition: None,
                },
                TupleKey {
                    user: "namespace:ns1".to_string(),
                    relation: TableRelation::Parent.to_string(),
                    object: "lakekeeper_table:wh1/t2".to_string(),
                    condition: None,
                },
                TupleKey {
                    user: "namespace:ns1_child".to_string(),
                    relation: ViewRelation::Parent.to_string(),
                    object: "lakekeeper_view:wh1/v1".to_string(),
                    condition: None,
                },
                TupleKey {
                    user: "namespace:ns1_child".to_string(),
                    relation: TableRelation::Parent.to_string(),
                    object: "lakekeeper_table:wh1/t3".to_string(),
                    condition: None,
                },
                TupleKey {
                    user: "namespace:ns2".to_string(),
                    relation: TableRelation::Parent.to_string(),
                    object: "lakekeeper_table:wh2/t4".to_string(),
                    condition: None,
                },
                TupleKey {
                    user: "user:owner1".to_string(),
                    relation: TableRelation::Ownership.to_string(),
                    object: "lakekeeper_table:wh1/t1".to_string(),
                    condition: None,
                },
                TupleKey {
                    user: "user:owner2".to_string(),
                    relation: ViewRelation::Ownership.to_string(),
                    object: "lakekeeper_view:wh1/v1".to_string(),
                    condition: None,
                },
                // Updated user references (table/view users with warehouse prefix)
                TupleKey {
                    user: "lakekeeper_table:wh1/t1".to_string(),
                    relation: NamespaceRelation::Child.to_string(),
                    object: "namespace:ns1".to_string(),
                    condition: None,
                },
                TupleKey {
                    user: "lakekeeper_table:wh1/t2".to_string(),
                    relation: NamespaceRelation::Child.to_string(),
                    object: "namespace:ns1".to_string(),
                    condition: None,
                },
                TupleKey {
                    user: "lakekeeper_view:wh1/v1".to_string(),
                    relation: NamespaceRelation::Child.to_string(),
                    object: "namespace:ns1_child".to_string(),
                    condition: None,
                },
                TupleKey {
                    user: "lakekeeper_table:wh1/t3".to_string(),
                    relation: NamespaceRelation::Child.to_string(),
                    object: "namespace:ns1_child".to_string(),
                    condition: None,
                },
                TupleKey {
                    user: "lakekeeper_table:wh2/t4".to_string(),
                    relation: NamespaceRelation::Child.to_string(),
                    object: "namespace:ns2".to_string(),
                    condition: None,
                },
            ];

            // Sort both for comparison
            new_tuples.sort_by(|a, b| {
                a.user
                    .cmp(&b.user)
                    .then_with(|| a.relation.cmp(&b.relation))
                    .then_with(|| a.object.cmp(&b.object))
            });

            let mut expected_sorted = expected_new_tuples.clone();
            expected_sorted.sort_by(|a, b| {
                a.user
                    .cmp(&b.user)
                    .then_with(|| a.relation.cmp(&b.relation))
                    .then_with(|| a.object.cmp(&b.object))
            });

            // Verify the new tuples match expected
            println!("new tuples: {:#?}", new_tuples);
            assert_eq!(new_tuples.len(), expected_sorted.len());
            for (actual, expected) in new_tuples.iter().zip(expected_sorted.iter()) {
                assert_eq!(actual.user, expected.user);
                assert_eq!(actual.relation, expected.relation);
                assert_eq!(actual.object, expected.object);
            }

            Ok(())
        }

        /// This is a "benchmark" for the migration of an OpenFGA store to v4.
        ///
        /// It can be executed with:
        ///
        /// ```ignore
        /// cargo test --all-features --release test_v4_push_down_warehouse_id_bench -- --ignored
        /// ```
        ///
        /// Results:
        ///
        /// * Migrating 10k tabulars takes ~25 seconds.
        /// * Migrating 20k tabulars takes ~104 seconds.
        /// * The bottleneck appears to be the OpenFGA server. During the migration lakekeeper's
        ///   CPU usage lingers around 1% to 8%.
        ///
        /// Ignored by default as it's purpose is benchmarking instead of testing. In this form
        /// it shows that the migration is fast enough (see above), so currently there would be
        /// little benefit from trying to run async code in a `bench` or using something like
        /// criterion.
        #[tokio::test(flavor = "multi_thread")]
        #[ignore]
        async fn test_v4_push_down_warehouse_id_bench() -> anyhow::Result<()> {
            const NUM_WAREHOUSES: usize = 10;
            /// equally distributed among warehouses
            const NUM_NAMESPACES: usize = 100;
            /// half tables, half views, equally distributed among namespaces
            const NUM_TABULARS: usize = 10_000;

            let (client, store_name) = v3_client_for_empty_store().await?;
            let authorizer = OpenFGAAuthorizer {
                client: client.clone(),
                health: Default::default(),
            };
            let req_meta_human =
                RequestMetadata::random_human(UserId::new_unchecked("oidc", "user"));

            println!("Populating OpenFGA store");
            let start_populating = Instant::now();
            let project_id = ProjectId::new_random();
            authorizer
                .create_project(&req_meta_human, &project_id)
                .await
                .unwrap();

            let mut warehouse_ids = Vec::with_capacity(NUM_WAREHOUSES);
            let mut wh_jobs = JoinSet::new();
            for _ in 0..NUM_WAREHOUSES {
                let wh_id = WarehouseId::new_random();
                warehouse_ids.push(wh_id.clone());

                let project_id = project_id.clone();
                let req_meta_human = req_meta_human.clone();
                let auth = authorizer.clone();
                let semaphore = OPENFGA_REQ_PERMITS.clone();

                wh_jobs.spawn(async move {
                    let _permit = semaphore.acquire().await.unwrap();
                    auth.create_warehouse(&req_meta_human, wh_id, &project_id)
                        .await
                        .unwrap();
                });
            }
            let _ = wh_jobs.join_all().await;

            let mut namespace_ids = Vec::with_capacity(NUM_NAMESPACES);
            let mut ns_jobs = JoinSet::new();
            for i in 0..NUM_NAMESPACES {
                let ns_id = NamespaceId::new_random();
                namespace_ids.push(ns_id.clone());

                let wh_id = warehouse_ids[i % warehouse_ids.len()].clone();
                let req_meta_human = req_meta_human.clone();
                let auth = authorizer.clone();
                let semaphore = OPENFGA_REQ_PERMITS.clone();

                ns_jobs.spawn(async move {
                    let _permit = semaphore.acquire().await.unwrap();
                    auth.create_namespace(
                        &req_meta_human,
                        ns_id,
                        NamespaceParent::Warehouse(wh_id),
                    )
                    .await
                    .unwrap();
                });
            }
            let _ = ns_jobs.join_all().await;

            let mut tab_jobs = JoinSet::new();
            for i in 0..NUM_TABULARS {
                let ns_id = namespace_ids[i % namespace_ids.len()].clone();
                let req_meta_human = req_meta_human.clone();
                let auth = authorizer.clone();
                let semaphore = OPENFGA_REQ_PERMITS.clone();

                tab_jobs.spawn(async move {
                    let _permit = semaphore.acquire().await.unwrap();
                    if i % 2 == 0 {
                        auth.create_table(&req_meta_human, TableId::new_random(), ns_id)
                            .await
                            .unwrap();
                    } else {
                        auth.create_view(&req_meta_human, ViewId::new_random(), ns_id)
                            .await
                            .unwrap();
                    }
                });
            }
            let _ = tab_jobs.join_all().await;
            println!(
                "Populated the OpenFGA store in {} seconds",
                start_populating.elapsed().as_secs()
            );

            println!("Migrating the OpenFGA store");
            let start_migrating = Instant::now();
            migrate_to_v4(&client.client(), store_name).await?;
            println!(
                "Migrated the OpenFGA store in {} seconds",
                start_migrating.elapsed().as_secs()
            );

            // Read a tuple generated by the migration to ensure it ran.
            let sentinel = client
                .read(
                    1,
                    ReadRequestTupleKey {
                        user: namespace_ids[0].to_openfga(),
                        relation: TableRelation::Parent.to_string(),
                        object: "lakekeeper_table:".to_string(),
                    },
                    None,
                )
                .await?
                .get_ref()
                .tuples
                .clone();
            assert!(sentinel.len() > 0, "There should be a sentinel tupel");
            println!("{:?}", sentinel);

            Ok(())
        }
    }
}
