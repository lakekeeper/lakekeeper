use std::{
    collections::VecDeque,
    sync::{Arc, LazyLock},
};

use anyhow::anyhow;
use lakekeeper::{
    service::ServerId,
    tokio,
    tokio::{sync::Semaphore, task::JoinSet},
};
use openfga_client::client::{
    BasicOpenFgaClient, BasicOpenFgaServiceClient, ConsistencyPreference, ReadRequestTupleKey,
    TupleKey,
};
use serde::Serialize;
use strum::IntoEnumIterator;

use crate::{
    relations::{
        NamespaceRelation, ProjectRelation, TableRelation, ViewRelation, WarehouseRelation,
    },
    MAX_TUPLES_PER_WRITE,
};

#[derive(Clone, Debug)]
pub(crate) struct MigrationState {
    pub store_name: String,
    pub server_id: ServerId,
}

fn openfga_user_type(inp: &str) -> Option<String> {
    inp.split(':').next().map(std::string::ToString::to_string)
}

/// Prepends `lakekeeper_` to the type and injects `prefix` into a full `OpenFGA` object.
///
/// ```rust,ignore
/// # // ignore: can't have doctest of private function
///
/// let full_object = "table:t1";
/// let extended_object = "lakekeeper_table:wh1/t1";
/// assert_eq!(new_v4_tuple(full_object, "wh1"), extended_object.to_string());
/// ```
fn new_v4_tuple(full_object: &str, prefix: &str) -> anyhow::Result<String> {
    let parts: Vec<_> = full_object.split(':').collect();
    anyhow::ensure!(
        parts.len() == 2,
        "Expected full object (type:id), got {full_object}",
    );
    Ok(format!("lakekeeper_{}:{prefix}/{}", parts[0], parts[1]))
}

fn extract_id_from_full_object(full_object: &str) -> anyhow::Result<String> {
    let parts: Vec<_> = full_object.split(':').collect();
    anyhow::ensure!(
        parts.len() == 2,
        "Expected full object (type:id), got {full_object}",
    );
    Ok(parts[1].to_string())
}

// TODO add a param to OpenFGAConfig for this?
const OPENFGA_PAGE_SIZE: i32 = 100;

static OPENFGA_WRITE_BATCH_SIZE: LazyLock<usize> =
    LazyLock::new(|| MAX_TUPLES_PER_WRITE.try_into().expect("should fit usize"));

/// Limits the number of concurrent requests to the `OpenFGA` server, to avoid overloading it.
///
/// Ensure the permit is dropped as soon as it's not needed anymore, to unblock other threads.
static OPENFGA_REQ_PERMITS: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::const_new(50)));

#[tracing::instrument(skip(client), fields(store_name = %state.store_name, server_id = %state.server_id))]
#[allow(clippy::used_underscore_binding, clippy::too_many_lines)]
pub(crate) async fn v4_push_down_warehouse_id(
    mut client: BasicOpenFgaServiceClient,
    _prev_auth_model_id: Option<String>,
    curr_auth_model_id: Option<String>,
    state: MigrationState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    tracing::info!(
        "Starting v4 warehouse ID push-down migration for store: {}",
        state.store_name
    );

    // Construct OpenFGAClient to be able to use convenience methods.
    let store = client
        .get_store_by_name(&state.store_name)
        .await?
        .ok_or_else(|| anyhow!("Store not found: {}", state.store_name))?;
    let curr_auth_model_id = curr_auth_model_id
        .ok_or_else(|| anyhow!("v4 migration is missing current authorization model id"))?;
    let client = client
        .into_client(&store.id, &curr_auth_model_id)
        .set_consistency(ConsistencyPreference::HigherConsistency);

    tracing::info!("Fetching all projects for server: {}", state.server_id);
    let projects = get_all_projects(&client, state.server_id).await?;
    tracing::info!("Found {} projects, fetching all warehouses", projects.len());
    let warehouses = get_all_warehouses(&client, projects).await?;
    tracing::info!(
        "Found {} warehouses, starting namespace and tabular processing",
        warehouses.len()
    );
    let mut namespaces_per_wh: Vec<(String, Vec<String>)> = vec![];

    let mut warehouse_jobs: JoinSet<anyhow::Result<(String, Vec<String>)>> = JoinSet::new();
    for wh in warehouses {
        let c = client.clone();
        let semaphore = OPENFGA_REQ_PERMITS.clone();

        warehouse_jobs.spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();
            let namespaces = get_all_namespaces(&c, wh.clone()).await?;
            Ok((wh, namespaces))
        });
    }
    while let Some(res) = warehouse_jobs.join_next().await {
        namespaces_per_wh.push(res??);
    }
    tracing::info!(
        "Collected {} namespaces for all warehouses.",
        namespaces_per_wh
            .iter()
            .fold(0, |acc, (_whid, namespaces)| acc + namespaces.len())
    );

    let num_warehouses = namespaces_per_wh.len();
    for (i, (wh, nss)) in namespaces_per_wh.into_iter().enumerate() {
        let processing_wh_i = i + 1;
        let wh_id = extract_id_from_full_object(&wh)?;
        tracing::info!(
            "Processing warehouse {processing_wh_i} of {num_warehouses} ({wh_id}) with {} namespaces: collecting all tabulars",
            nss.len()
        );

        // Load tabulars only inside this loop (i.e. per warehouse) and drop them at the end of it.
        // This is done to mitigate the risk of OOM during the migration of a huge catalog.
        let tabulars = get_all_tabulars(&client, &nss).await?;
        let num_tabulars_in_wh = tabulars.len();
        tracing::info!("Found {num_tabulars_in_wh} tabulars in warehouse {wh_id}");
        let mut new_tuples_to_write = vec![];

        for tab in tabulars {
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

            for mut tuple in tab_as_object {
                tuple.object = new_v4_tuple(&tuple.object, &wh_id)?;
                new_tuples_to_write.push(tuple);
            }
            for mut tuple in tab_as_user {
                tuple.user = new_v4_tuple(&tuple.user, &wh_id)?;
                new_tuples_to_write.push(tuple);
            }
        }

        tracing::info!(
            "Collected {num_tabulars_in_wh} tabulars for warehouse {wh_id}, writing {} new tuples",
            new_tuples_to_write.len()
        );

        let mut write_jobs = JoinSet::new();

        for chunk in new_tuples_to_write.chunks(*OPENFGA_WRITE_BATCH_SIZE) {
            let c = client.clone();
            let semaphore = OPENFGA_REQ_PERMITS.clone();
            let tuples = chunk.to_vec();
            write_jobs.spawn(async move {
                let _permit = semaphore.acquire().await.unwrap();
                c.write(Some(tuples), None).await
            });
        }
        while let Some(res) = write_jobs.join_next().await {
            let () = res??;
        }

        tracing::info!(
            "Completed migration for warehouse {processing_wh_i} of {num_warehouses} ({wh_id}) with {} tuples written",
            new_tuples_to_write.len()
        );
    }

    tracing::info!(
        "v4 warehouse ID push-down migration completed successfully for store: {}",
        state.store_name
    );
    Ok(())
}

async fn get_all_projects(
    client: &BasicOpenFgaClient,
    server_id: ServerId,
) -> anyhow::Result<Vec<String>> {
    let request_key = ReadRequestTupleKey {
        user: format!("server:{server_id}"),
        relation: ProjectRelation::Server.to_string(),
        object: "project:".to_string(),
    };
    let tuples = client
        .read_all_pages(Some(request_key), OPENFGA_PAGE_SIZE, u32::MAX)
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

    for p in projects {
        let client = client.clone();
        let semaphore = OPENFGA_REQ_PERMITS.clone();

        jobs.spawn(async move {
            let permit = semaphore.acquire().await.unwrap();
            let mut warehouses = vec![];
            let tuples = client
                .read_all_pages(
                    Some(ReadRequestTupleKey {
                        user: p.clone(),
                        relation: WarehouseRelation::Project.to_string(),
                        object: "warehouse:".to_string(),
                    }),
                    OPENFGA_PAGE_SIZE,
                    u32::MAX,
                )
                .await?;
            drop(permit);
            for t in tuples {
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
        for parent in &parents {
            let client = client.clone();
            let parent = parent.clone();
            let semaphore = OPENFGA_REQ_PERMITS.clone();

            jobs.spawn(async move {
                let permit = semaphore.acquire().await.unwrap();
                let tuples = client
                    .read_all_pages(
                        Some(ReadRequestTupleKey {
                            user: parent.clone(),
                            relation: NamespaceRelation::Parent.to_string(),
                            object: "namespace:".to_string(),
                        }),
                        OPENFGA_PAGE_SIZE,
                        u32::MAX,
                    )
                    .await?;
                drop(permit);
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, strum::EnumIter)]
enum TabularType {
    Table,
    View,
}

impl TabularType {
    fn object_type(self) -> String {
        match self {
            Self::Table => "table:".to_string(),
            Self::View => "view:".to_string(),
        }
    }

    fn parent_relation_string(self) -> String {
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
    for ns in namespaces {
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
                    let permit = semaphore.acquire().await.unwrap();
                    let tuples = client
                        .read_all_pages(
                            Some(ReadRequestTupleKey {
                                user: ns,
                                relation,
                                object: object_type,
                            }),
                            OPENFGA_PAGE_SIZE,
                            u32::MAX,
                        )
                        .await?;
                    drop(permit);
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
            Some(ReadRequestTupleKey {
                user: String::new(),
                relation: String::new(),
                object,
            }),
            OPENFGA_PAGE_SIZE,
            u32::MAX,
        )
        .await?;
    Ok(tuples.into_iter().filter_map(|t| t.key).collect())
}

/// The `user` must specify both type and id (`type:id`)
///
/// The returned result contains all tuples that have the provided `user` from all relations
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
    for ty in object_types {
        let client = client.clone();
        let user = user.clone();
        let semaphore = OPENFGA_REQ_PERMITS.clone();

        jobs.spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();
            let res = client
                .read_all_pages(
                    Some(ReadRequestTupleKey {
                        user,
                        relation: String::new(),
                        object: ty,
                    }),
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
mod openfga_integration_tests {
    use std::time::Instant;

    use lakekeeper::{
        api::RequestMetadata,
        service::{authz::Authorizer, NamespaceId, ServerId, TableId, UserId, ViewId},
        tokio::task::JoinSet,
        ProjectId, WarehouseId,
    };
    use openfga_client::{
        client::{CheckRequestTupleKey, TupleKey},
        migration::TupleModelManager,
    };
    use tracing_test::traced_test;

    use super::*;
    use crate::{
        client::new_client_from_default_config,
        entities::OpenFgaEntity,
        migration::{add_model_v3, add_model_v4_0, V3_MODEL_VERSION, V4_0_MODEL_VERSION},
        relations::ServerRelation,
        OpenFGAAuthorizer, AUTH_CONFIG,
    };
    // Tests must write tuples according to v3 model manually.
    // Writing through methods like `authorizer.create_*` may create tuples different from
    // what v4 migration is designed to handle.
    //
    // Code that constructs OpenFGA authorizers and clients is geared towards using the
    // default/configured model and running migrations up to there. However, in these
    // tests we exactly need v3, so that we can test the v3 -> v4 migration. Hence below some
    // functions to construct a v3 client/authorizer.

    /// Constructs a client for a store that has been initialized and migrated to v3.
    /// Returns the client, name of the store, and server id.
    async fn v3_client_for_empty_store() -> anyhow::Result<(BasicOpenFgaClient, String, ServerId)> {
        let mut client = new_client_from_default_config().await?;
        let server_id = ServerId::new_random();
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
            server_id,
        };
        model_manager.migrate(migration_state).await?;

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
        Ok((client, store_name, server_id))
    }

    async fn new_v3_authorizer_for_empty_store() -> anyhow::Result<OpenFGAAuthorizer> {
        let (client, _, server_id) = v3_client_for_empty_store().await?;
        Ok(OpenFGAAuthorizer::new(client, server_id))
    }

    /// Migrates the `OpenFGA` store to v4, which will also execute the migration function.
    /// Returns a new client set to interact with the store's v4 authorization model.
    async fn migrate_to_v4(
        client: BasicOpenFgaClient,
        store_name: String,
        server_id: ServerId,
    ) -> anyhow::Result<BasicOpenFgaClient> {
        // Migrate the store.
        let model_manager = TupleModelManager::new(
            client.client().clone(),
            &store_name,
            &AUTH_CONFIG.authorization_model_prefix,
        );
        let mut model_manager = add_model_v4_0(model_manager);
        let migration_state = MigrationState {
            store_name: store_name.clone(),
            server_id,
        };
        model_manager.migrate(migration_state).await?;

        // Construct a new client to interact with v4.
        let store_id = client.store_id();
        let auth_model_id_v4 = model_manager
            .get_authorization_model_id(*V4_0_MODEL_VERSION)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Auth model should be set after migration"))?;
        let client_v4 = BasicOpenFgaClient::new(client.client(), store_id, &auth_model_id_v4)
            .set_consistency(ConsistencyPreference::HigherConsistency);
        Ok(client_v4)
    }

    async fn new_v4_authorizer_for_empty_store() -> anyhow::Result<OpenFGAAuthorizer> {
        let (client, store_name, server_id) = v3_client_for_empty_store().await?;
        let client_v4 = migrate_to_v4(client, store_name, server_id).await?;
        Ok(OpenFGAAuthorizer::new(client_v4, server_id))
    }

    #[tokio::test]
    async fn test_get_all_projects() -> anyhow::Result<()> {
        let authorizer = new_v3_authorizer_for_empty_store().await?;

        authorizer
            .write(
                Some(vec![
                    TupleKey {
                        user: authorizer.openfga_server().clone(),
                        relation: ProjectRelation::Server.to_string(),
                        object: "project:p1".to_string(),
                        condition: None,
                    },
                    TupleKey {
                        user: authorizer.openfga_server().clone(),
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

        let mut projects = get_all_projects(&authorizer.client, authorizer.server_id()).await?;
        projects.sort();
        assert_eq!(
            projects,
            vec!["project:p1".to_string(), "project:p2".to_string()]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_get_all_projects_empty_server() -> anyhow::Result<()> {
        let authorizer = new_v3_authorizer_for_empty_store().await?;

        // Test with a server that has no projects
        let projects = get_all_projects(&authorizer.client, authorizer.server_id()).await?;
        assert!(projects.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_get_all_warehouses() -> anyhow::Result<()> {
        let authorizer = new_v3_authorizer_for_empty_store().await?;

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
        let authorizer = new_v3_authorizer_for_empty_store().await?;

        // Test with a project that has no warehouses
        let projects = vec!["project:empty".to_string()];
        let warehouses = get_all_warehouses(&authorizer.client, projects).await?;
        assert!(warehouses.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_get_all_namespaces() -> anyhow::Result<()> {
        let authorizer = new_v3_authorizer_for_empty_store().await?;

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
        let authorizer = new_v3_authorizer_for_empty_store().await?;

        let namespaces =
            get_all_namespaces(&authorizer.client, "warehouse:empty".to_string()).await?;
        assert!(namespaces.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_get_all_tabulars() -> anyhow::Result<()> {
        let authorizer = new_v3_authorizer_for_empty_store().await?;

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
        let authorizer = new_v3_authorizer_for_empty_store().await?;

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
        let authorizer = new_v3_authorizer_for_empty_store().await?;

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
                    TupleKey {
                        user: "role:some-role#assignee".to_string(),
                        relation: TableRelation::Modify.to_string(),
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

        assert_eq!(tuples.len(), 4);
        assert_eq!(tuples[0].user, "namespace:ns1".to_string());
        assert_eq!(tuples[0].relation, TableRelation::Parent.to_string());
        assert_eq!(tuples[0].object, "table:target-table".to_string());

        assert_eq!(tuples[1].user, "role:some-role#assignee".to_string());
        assert_eq!(tuples[1].relation, TableRelation::Modify.to_string());
        assert_eq!(tuples[1].object, "table:target-table".to_string());

        assert_eq!(tuples[2].user, "user:user1".to_string());
        assert_eq!(tuples[2].relation, TableRelation::PassGrants.to_string());
        assert_eq!(tuples[2].object, "table:target-table".to_string());

        assert_eq!(tuples[3].user, "user:user2".to_string());
        assert_eq!(tuples[3].relation, TableRelation::Ownership.to_string());
        assert_eq!(tuples[3].object, "table:target-table".to_string());

        Ok(())
    }

    #[tokio::test]
    async fn test_get_all_tuples_with_object_empty() -> anyhow::Result<()> {
        let authorizer = new_v3_authorizer_for_empty_store().await?;

        // Test with an object that doesn't exist
        let tuples =
            get_all_tuples_with_object(&authorizer.client, "table:nonexistent".to_string()).await?;
        assert!(tuples.is_empty());
        Ok(())
    }

    /// Testing for user type `table` which can be the user in only one relation as of v3.
    #[tokio::test]
    async fn test_get_all_tuples_with_user() -> anyhow::Result<()> {
        let authorizer = new_v3_authorizer_for_empty_store().await?;

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
                    TupleKey {
                        user: "role:some-role#assignee".to_string(),
                        relation: TableRelation::Ownership.to_string(),
                        object: "table:target-table".to_string(),
                        condition: None,
                    },
                ]),
                None,
            )
            .await?;

        let tuples =
            get_all_tuples_with_user(&authorizer.client, "table:target-table".to_string()).await?;

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
        let authorizer = new_v3_authorizer_for_empty_store().await?;

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
            get_all_tuples_with_user(&authorizer.client, "namespace:target-ns".to_string()).await?;

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
        let authorizer = new_v3_authorizer_for_empty_store().await?;

        // Test with a user that doesn't exist
        let tuples =
            get_all_tuples_with_user(&authorizer.client, "user:nonexistent".to_string()).await?;
        assert!(tuples.is_empty());
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_v4_push_down_warehouse_id() -> anyhow::Result<()> {
        let (client, store_name, server_id) = v3_client_for_empty_store().await?;
        let openfga_server = server_id.to_openfga();

        // Create the initial tuple structure:
        //
        // Project p1 has two warehouses.
        // warehouse:wh1 -> namespace:ns1 -> table:t1, table:t2
        //                            |--> namespace:ns1_child -> view:v1, table:t3
        // warehouse:wh2 -> namespace:ns2 -> table:t4
        let initial_tuples = vec![
            // Project structure
            TupleKey {
                user: openfga_server,
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
                user: "role:some-role#assignee".to_string(),
                relation: TableRelation::Select.to_string(),
                object: "table:t1".to_string(),
                condition: None,
            },
            TupleKey {
                user: "user:owner2".to_string(),
                relation: ViewRelation::Ownership.to_string(),
                object: "view:v1".to_string(),
                condition: None,
            },
            TupleKey {
                user: "role:other-role#assignee".to_string(),
                relation: TableRelation::Select.to_string(),
                object: "table:t4".to_string(),
                condition: None,
            },
        ];

        // Write initial tuples
        client.write(Some(initial_tuples.clone()), None).await?;

        // Migrate to v4, which will call the migration fn.
        let client_v4 = migrate_to_v4(client, store_name.clone(), server_id).await?;

        // Read all tuples from store
        let all_tuples = client_v4
            .read_all_pages(None::<ReadRequestTupleKey>, 100, 1000)
            .await?;

        let all_tuple_keys: Vec<TupleKey> = all_tuples.into_iter().filter_map(|t| t.key).collect();

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
            TupleKey {
                user: "role:some-role#assignee".to_string(),
                relation: TableRelation::Select.to_string(),
                object: "lakekeeper_table:wh1/t1".to_string(),
                condition: None,
            },
            TupleKey {
                user: "role:other-role#assignee".to_string(),
                relation: TableRelation::Select.to_string(),
                object: "lakekeeper_table:wh2/t4".to_string(),
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
        assert_eq!(new_tuples.len(), expected_sorted.len());
        for (actual, expected) in new_tuples.iter().zip(expected_sorted.iter()) {
            assert_eq!(actual.user, expected.user);
            assert_eq!(actual.relation, expected.relation);
            assert_eq!(actual.object, expected.object);
        }

        Ok(())
    }

    /// This is a "benchmark" for the migration of an `OpenFGA` store to v4.
    ///
    /// It can be executed with:
    ///
    /// ```ignore
    /// cargo test --all-features --release test_v4_push_down_warehouse_id_bench -- --ignored --nocapture
    /// ```
    ///
    /// Results:
    ///
    /// * Most expensive operation is writing new tuples. For each tabular at minimum 3 tuples
    ///   need to be written. Assignments involving tabulars increase that number, as for each
    ///   table/view tuple a new `lakekeeper_table/lakekeeper_view` tuple is written.
    /// * Migrating 10k tabulars takes ~25 seconds.
    /// * Migrating 20k tabulars takes ~104 seconds.
    /// * The bottleneck appears to be the `OpenFGA` server. During the migration lakekeeper's
    ///   CPU usage lingers around 1% to 8%.
    ///
    /// Ignored by default as it's purpose is benchmarking instead of testing. In this form
    /// it shows that the migration is fast enough (see above), so currently there would be
    /// little benefit from trying to run async code in a `bench` or using something like
    /// criterion.
    #[tokio::test(flavor = "multi_thread")]
    #[traced_test]
    #[ignore = "expensive benchmark, not testing functionality"]
    #[allow(clippy::too_many_lines)]
    async fn test_v4_push_down_warehouse_id_bench() -> anyhow::Result<()> {
        const NUM_WAREHOUSES: usize = 10;
        /// equally distributed among warehouses
        const NUM_NAMESPACES: usize = 100;
        /// half tables, half views, equally distributed among namespaces
        const NUM_TABULARS: usize = 10_000;

        let (client, store_name, server_id) = v3_client_for_empty_store().await?;
        let authorizer = OpenFGAAuthorizer::new(client.clone(), server_id);
        let req_meta_human = RequestMetadata::test_user(UserId::new_unchecked("oidc", "user"));

        tracing::info!("Populating OpenFGA store");
        let start_populating = Instant::now();
        let project_id = ProjectId::new_random();

        // Write tuples manually to generate a store that would have been written by a v3
        // authorizer.

        // Write project tuples manually
        let openfga_server = authorizer.openfga_server();
        let actor = req_meta_human.actor();
        let project_openfga = format!("project:{project_id}");
        authorizer
            .write(
                Some(vec![
                    TupleKey {
                        user: actor.to_openfga(),
                        relation: ProjectRelation::ProjectAdmin.to_string(),
                        object: project_openfga.clone(),
                        condition: None,
                    },
                    TupleKey {
                        user: openfga_server.clone(),
                        relation: ProjectRelation::Server.to_string(),
                        object: project_openfga.clone(),
                        condition: None,
                    },
                    TupleKey {
                        user: project_openfga,
                        relation: ServerRelation::Project.to_string(),
                        object: openfga_server.clone(),
                        condition: None,
                    },
                ]),
                None,
            )
            .await
            .unwrap();

        tracing::info!("Creating {NUM_WAREHOUSES} warehouses");
        let mut warehouse_ids = Vec::with_capacity(NUM_WAREHOUSES);
        let mut wh_jobs = JoinSet::new();
        for _ in 0..NUM_WAREHOUSES {
            let wh_id = WarehouseId::new_random();
            warehouse_ids.push(wh_id);

            let project_id = project_id.clone();
            let req_meta_human = req_meta_human.clone();
            let auth = authorizer.clone();
            let semaphore = OPENFGA_REQ_PERMITS.clone();

            wh_jobs.spawn(async move {
                let _permit = semaphore.acquire().await.unwrap();

                // Write warehouse tuples manually
                let actor = req_meta_human.actor();
                let project_openfga = project_id.to_openfga();
                let warehouse_openfga = format!("warehouse:{wh_id}");
                auth.write(
                    Some(vec![
                        TupleKey {
                            user: actor.to_openfga(),
                            relation: WarehouseRelation::Ownership.to_string(),
                            object: warehouse_openfga.clone(),
                            condition: None,
                        },
                        TupleKey {
                            user: project_openfga.clone(),
                            relation: WarehouseRelation::Project.to_string(),
                            object: warehouse_openfga.clone(),
                            condition: None,
                        },
                        TupleKey {
                            user: warehouse_openfga,
                            relation: ProjectRelation::Warehouse.to_string(),
                            object: project_openfga,
                            condition: None,
                        },
                    ]),
                    None,
                )
                .await
                .unwrap();
            });
        }
        let _ = wh_jobs.join_all().await;

        tracing::info!("Creating {NUM_NAMESPACES} namespaces in {NUM_WAREHOUSES} warehouses");
        let mut namespace_ids = Vec::with_capacity(NUM_NAMESPACES);
        let mut ns_jobs = JoinSet::new();
        for i in 0..NUM_NAMESPACES {
            let ns_id = NamespaceId::new_random();
            namespace_ids.push(ns_id);

            let wh_id = warehouse_ids[i % warehouse_ids.len()];
            let req_meta_human = req_meta_human.clone();
            let auth = authorizer.clone();
            let semaphore = OPENFGA_REQ_PERMITS.clone();

            ns_jobs.spawn(async move {
                let _permit = semaphore.acquire().await.unwrap();

                // Write namespace tuples manually
                let actor = req_meta_human.actor();
                let warehouse_openfga = format!("warehouse:{wh_id}");
                let namespace_openfga = format!("namespace:{ns_id}");
                auth.write(
                    Some(vec![
                        TupleKey {
                            user: actor.to_openfga(),
                            relation: NamespaceRelation::Ownership.to_string(),
                            object: namespace_openfga.clone(),
                            condition: None,
                        },
                        TupleKey {
                            user: warehouse_openfga.clone(),
                            relation: NamespaceRelation::Parent.to_string(),
                            object: namespace_openfga.clone(),
                            condition: None,
                        },
                        TupleKey {
                            user: namespace_openfga,
                            relation: WarehouseRelation::Namespace.to_string(),
                            object: warehouse_openfga,
                            condition: None,
                        },
                    ]),
                    None,
                )
                .await
                .unwrap();
            });
        }
        let _ = ns_jobs.join_all().await;

        tracing::info!("Creating {NUM_TABULARS} tabulars in {NUM_NAMESPACES} namespaces");
        let mut tab_jobs = JoinSet::new();
        for i in 0..NUM_TABULARS {
            let ns_id = namespace_ids[i % namespace_ids.len()];
            let req_meta_human = req_meta_human.clone();
            let auth = authorizer.clone();
            let semaphore = OPENFGA_REQ_PERMITS.clone();

            tab_jobs.spawn(async move {
                let _permit = semaphore.acquire().await.unwrap();

                // Write table or view tuples manually
                let actor = req_meta_human.actor();
                let namespace_openfga = ns_id.to_openfga();

                if i % 2 == 0 {
                    // Create table
                    let table_id = TableId::new_random();
                    let table_openfga = format!("table:{table_id}");
                    auth.write(
                        Some(vec![
                            TupleKey {
                                user: actor.to_openfga(),
                                relation: TableRelation::Ownership.to_string(),
                                object: table_openfga.clone(),
                                condition: None,
                            },
                            TupleKey {
                                user: namespace_openfga.clone(),
                                relation: TableRelation::Parent.to_string(),
                                object: table_openfga.clone(),
                                condition: None,
                            },
                            TupleKey {
                                user: table_openfga,
                                relation: NamespaceRelation::Child.to_string(),
                                object: namespace_openfga,
                                condition: None,
                            },
                        ]),
                        None,
                    )
                    .await
                    .unwrap();
                } else {
                    // Create view
                    let view_id = ViewId::new_random();
                    let view_openfga = format!("view:{view_id}");
                    auth.write(
                        Some(vec![
                            TupleKey {
                                user: actor.to_openfga(),
                                relation: ViewRelation::Ownership.to_string(),
                                object: view_openfga.clone(),
                                condition: None,
                            },
                            TupleKey {
                                user: namespace_openfga.clone(),
                                relation: ViewRelation::Parent.to_string(),
                                object: view_openfga.clone(),
                                condition: None,
                            },
                            TupleKey {
                                user: view_openfga,
                                relation: NamespaceRelation::Child.to_string(),
                                object: namespace_openfga,
                                condition: None,
                            },
                        ]),
                        None,
                    )
                    .await
                    .unwrap();
                }
            });
        }
        let _ = tab_jobs.join_all().await;
        tracing::info!(
            "Populated the OpenFGA store in {} seconds",
            start_populating.elapsed().as_secs()
        );

        tracing::info!("Migrating the OpenFGA store");
        let start_migrating = Instant::now();
        let client_v4 = migrate_to_v4(client, store_name, server_id).await?;
        tracing::info!(
            "Migrated the OpenFGA store in {} seconds",
            start_migrating.elapsed().as_secs()
        );

        // Read a tuple generated by the migration to ensure it ran.
        let sentinel = client_v4
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
        assert!(!sentinel.is_empty(), "There should be a sentinel tuple");

        Ok(())
    }

    // Tests below check v4 specific behavior, so they need an OpenFGA client/authorizer
    // migrated to v4.

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_reuse_tabular_ids_across_warehouses() -> anyhow::Result<()> {
        let authorizer = new_v4_authorizer_for_empty_store().await?;

        // Generate IDs for our test entities
        let project_id = ProjectId::new_random();
        let warehouse_id_1 = WarehouseId::new_random();
        let warehouse_id_2 = WarehouseId::new_random();
        let namespace_id_1 = NamespaceId::new_random();
        let namespace_id_2 = NamespaceId::new_random();
        let table_id = TableId::new_random();
        let view_id = ViewId::new_random();
        let user_id = UserId::new_unchecked("oidc", "privileged_user");

        // Manually construct OpenFGA identifiers instead of using `to_openfga()` as this test
        // needs the v4 representation and `to_openfga()` might divert from that in the future.
        let project_openfga = format!("project:{project_id}");
        let warehouse_1_openfga = format!("warehouse:{warehouse_id_1}");
        let warehouse_2_openfga = format!("warehouse:{warehouse_id_2}");
        let namespace_1_openfga = format!("namespace:{namespace_id_1}");
        let namespace_2_openfga = format!("namespace:{namespace_id_2}");
        let user_openfga = format!("user:{}", urlencoding::encode(&user_id.to_string()));
        let table_in_wh1 = format!("lakekeeper_table:{warehouse_id_1}/{table_id}");
        let table_in_wh2 = format!("lakekeeper_table:{warehouse_id_2}/{table_id}");
        let view_in_wh1 = format!("lakekeeper_view:{warehouse_id_1}/{view_id}");
        let view_in_wh2 = format!("lakekeeper_view:{warehouse_id_2}/{view_id}");

        // Write tuples directly instead of using methods like `authorizer.create_project()`.
        // Also here we need exactly the v4 tuples but authorizer methods might divert in the
        // future.
        authorizer
            .write(
                Some(vec![
                    // Project structure
                    TupleKey {
                        user: authorizer.openfga_server().clone(),
                        relation: ProjectRelation::Server.to_string(),
                        object: project_openfga.clone(),
                        condition: None,
                    },
                    // Warehouses in project
                    TupleKey {
                        user: project_openfga.clone(),
                        relation: WarehouseRelation::Project.to_string(),
                        object: warehouse_1_openfga.clone(),
                        condition: None,
                    },
                    TupleKey {
                        user: project_openfga.clone(),
                        relation: WarehouseRelation::Project.to_string(),
                        object: warehouse_2_openfga.clone(),
                        condition: None,
                    },
                    // Namespaces in warehouses
                    TupleKey {
                        user: warehouse_1_openfga.clone(),
                        relation: NamespaceRelation::Parent.to_string(),
                        object: namespace_1_openfga.clone(),
                        condition: None,
                    },
                    TupleKey {
                        user: warehouse_2_openfga.clone(),
                        relation: NamespaceRelation::Parent.to_string(),
                        object: namespace_2_openfga.clone(),
                        condition: None,
                    },
                    // Tables in namespaces (using lakekeeper_table format)
                    TupleKey {
                        user: namespace_1_openfga.clone(),
                        relation: TableRelation::Parent.to_string(),
                        object: table_in_wh1.clone(),
                        condition: None,
                    },
                    TupleKey {
                        user: namespace_2_openfga.clone(),
                        relation: TableRelation::Parent.to_string(),
                        object: table_in_wh2.clone(),
                        condition: None,
                    },
                    // Views in namespaces (using lakekeeper_view format)
                    TupleKey {
                        user: namespace_1_openfga.clone(),
                        relation: ViewRelation::Parent.to_string(),
                        object: view_in_wh1.clone(),
                        condition: None,
                    },
                    TupleKey {
                        user: namespace_2_openfga.clone(),
                        relation: ViewRelation::Parent.to_string(),
                        object: view_in_wh2.clone(),
                        condition: None,
                    },
                    // Child relations for tables
                    TupleKey {
                        user: table_in_wh1.clone(),
                        relation: NamespaceRelation::Child.to_string(),
                        object: namespace_1_openfga.clone(),
                        condition: None,
                    },
                    TupleKey {
                        user: table_in_wh2.clone(),
                        relation: NamespaceRelation::Child.to_string(),
                        object: namespace_2_openfga.clone(),
                        condition: None,
                    },
                    // Child relations for views
                    TupleKey {
                        user: view_in_wh1.clone(),
                        relation: NamespaceRelation::Child.to_string(),
                        object: namespace_1_openfga.clone(),
                        condition: None,
                    },
                    TupleKey {
                        user: view_in_wh2.clone(),
                        relation: NamespaceRelation::Child.to_string(),
                        object: namespace_2_openfga.clone(),
                        condition: None,
                    },
                    // Assign ownership and select privileges to table in warehouse1 only
                    TupleKey {
                        user: user_openfga.clone(),
                        relation: TableRelation::Ownership.to_string(),
                        object: table_in_wh1.clone(),
                        condition: None,
                    },
                    TupleKey {
                        user: user_openfga.clone(),
                        relation: TableRelation::Select.to_string(),
                        object: table_in_wh1.clone(),
                        condition: None,
                    },
                    // Assign ownership and select privileges to view in warehouse1 only
                    TupleKey {
                        user: user_openfga.clone(),
                        relation: ViewRelation::Ownership.to_string(),
                        object: view_in_wh1.clone(),
                        condition: None,
                    },
                    TupleKey {
                        user: user_openfga.clone(),
                        relation: ViewRelation::Describe.to_string(),
                        object: view_in_wh1.clone(),
                        condition: None,
                    },
                ]),
                None,
            )
            .await?;

        // Verify that the privileges are only assigned to the table in warehouse1
        let ownership_wh1_allowed = authorizer
            .check(CheckRequestTupleKey {
                user: user_openfga.clone(),
                relation: TableRelation::Ownership.to_string(),
                object: table_in_wh1.clone(),
            })
            .await?;

        let select_wh1_allowed = authorizer
            .check(CheckRequestTupleKey {
                user: user_openfga.clone(),
                relation: TableRelation::Select.to_string(),
                object: table_in_wh1.clone(),
            })
            .await?;

        let ownership_wh2_allowed = authorizer
            .check(CheckRequestTupleKey {
                user: user_openfga.clone(),
                relation: TableRelation::Ownership.to_string(),
                object: table_in_wh2.clone(),
            })
            .await?;

        let select_wh2_allowed = authorizer
            .check(CheckRequestTupleKey {
                user: user_openfga.clone(),
                relation: TableRelation::Select.to_string(),
                object: table_in_wh2.clone(),
            })
            .await?;

        // Verify that the privileges are only assigned to the view in warehouse1
        let view_ownership_wh1_allowed = authorizer
            .check(CheckRequestTupleKey {
                user: user_openfga.clone(),
                relation: ViewRelation::Ownership.to_string(),
                object: view_in_wh1.clone(),
            })
            .await?;

        let view_select_wh1_allowed = authorizer
            .check(CheckRequestTupleKey {
                user: user_openfga.clone(),
                relation: ViewRelation::Describe.to_string(),
                object: view_in_wh1.clone(),
            })
            .await?;

        let view_ownership_wh2_allowed = authorizer
            .check(CheckRequestTupleKey {
                user: user_openfga.clone(),
                relation: ViewRelation::Ownership.to_string(),
                object: view_in_wh2.clone(),
            })
            .await?;

        let view_select_wh2_allowed = authorizer
            .check(CheckRequestTupleKey {
                user: user_openfga.clone(),
                relation: ViewRelation::Describe.to_string(),
                object: view_in_wh2.clone(),
            })
            .await?;

        // Assert that privileges are only on warehouse1's table
        assert!(
            ownership_wh1_allowed,
            "User should have ownership on table in warehouse1"
        );
        assert!(
            select_wh1_allowed,
            "User should have select privilege on table in warehouse1"
        );
        assert!(
            !ownership_wh2_allowed,
            "User should NOT have ownership on table in warehouse2"
        );
        assert!(
            !select_wh2_allowed,
            "User should NOT have select privilege on table in warehouse2"
        );

        // Assert that privileges are only on warehouse1's view
        assert!(
            view_ownership_wh1_allowed,
            "User should have ownership on view in warehouse1"
        );
        assert!(
            view_select_wh1_allowed,
            "User should have describe privilege on view in warehouse1"
        );
        assert!(
            !view_ownership_wh2_allowed,
            "User should NOT have ownership on view in warehouse2"
        );
        assert!(
            !view_select_wh2_allowed,
            "User should NOT have describe privilege on view in warehouse2"
        );

        Ok(())
    }
}
