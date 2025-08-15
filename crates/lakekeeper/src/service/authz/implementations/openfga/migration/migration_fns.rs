use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, LazyLock};

use futures::stream::{FuturesUnordered, StreamExt};
use iceberg_ext::catalog::rest::IcebergErrorResponse;
use openfga_client::client::{
    BasicOpenFgaClient, BasicOpenFgaServiceClient, OpenFgaClient, ReadRequestTupleKey,
};
use strum::IntoEnumIterator;
use tokio::sync::Semaphore;

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
pub(crate) struct MigrationState<C: Catalog> {
    pub store_name: String,
    pub catalog: C,
    pub catalog_state: C::State,
    pub server_id: uuid::Uuid,
}

// TODO add v4 to module name as everything here is version specific

// TODO: get from config in case someone runs openfga server with lower max page size?
const OPENFGA_PAGE_SIZE: i32 = 100;

/// Limits the number of concurrent transactions. It should be throttled as the catalog's db
/// may still be in use during the migration.
///
/// Ensure permits are dropped as soon as the tx is not needed anymore, to unblock other threads.
static DB_TX_PERMITS: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::const_new(10)));

// catalog trait reingeben, nicht postgres db
pub(crate) async fn v4_push_down_warehouse_id<C: Catalog>(
    mut client: BasicOpenFgaServiceClient,
    _prev_auth_model_id: Option<String>,
    curr_auth_model_id: Option<String>,
    state: MigrationState<C>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    println!("state in migration fn: {}", state.store_name);
    // Construct OpenFGAClient to be able to use convenience methods.
    let store = client
        .get_store_by_name(&state.store_name)
        .await?
        .expect("default store should exist");
    let curr_auth_model_id =
        curr_auth_model_id.expect("Migration hook needs current auth model's id");
    let c = client.into_client(&store.id, &curr_auth_model_id);
    // TODO error conversion instead of unwrap
    let mut tx = C::Transaction::begin_read(state.catalog_state.clone())
        .await
        .map_err(|e| e.error)?;

    // - Get all table ids
    //
    // - Get map of (TableId, WarehouseId)
    //   - Via OpenFGA or retrieve from db and inject into this fn?
    //   - try via db!
    // - need to assert no table written during migration
    // - check if there's a way to lock openfga during migration
    //
    // think: how to avoid creating tables from old binary why migration is running
    //
    // - For every table id
    //   - get all tuples that have this id as user or object
    //     - propaply only for NamespaceRelation::Child table id is on user side
    //     - user: table id, object: nur `namespace:`
    //   - copy these tuples and replace table_id with warehouse_id/table_id
    //     - alte tuple erst spaeter loeschen
    //   - write the new tuples, delete the old ones only after community is off 0.9
    //
    // - Do the same for views
    //
    // All new writes must then be wareho,use_id/table_id and warehouse_id/view_id
    // checks

    // Get data required for both table and view migrations.
    // *All* table and view related tuples need to be updated, regardless of their status.
    let project_ids: Vec<_> = C::list_projects(None, tx.transaction())
        .await
        .map_err(|e| e.error)?
        .into_iter()
        .map(|response| response.project_id)
        .collect();
    let warehouse_ids = all_warehouse_ids::<C>(state.catalog_state.clone(), &project_ids)
        .await
        .map_err(|e| e.error)?;
    let tabular_query_params =
        all_tabular_query_params::<C>(state.catalog_state.clone(), warehouse_ids)
            .await
            .map_err(|e| e.error)?;
    let _tables = all_tables::<C>(state.catalog_state.clone(), tabular_query_params)
        .await
        .map_err(|e| e.error)?;

    let projects = get_all_projects(&c, state.server_id).await?;
    let warehouses = get_all_warehouses(&c, &projects).await?;

    let _res = add_warehouse_id_to_tables(c.clone(), &HashMap::new()).await?;
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
    let tuples = client
        .read_all_pages(
            ReadRequestTupleKey {
                user: format!("server:{server_id}"),
                relation: ProjectRelation::Server.to_string(),
                object: "project:".to_string(),
            },
            OPENFGA_PAGE_SIZE,
            u32::MAX,
        )
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

// TODO concurrency, read with smaller page size
async fn get_all_warehouses(
    client: &BasicOpenFgaClient,
    projects: &[String],
) -> anyhow::Result<Vec<String>> {
    let mut warehouses = vec![];
    for p in projects.iter() {
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
        for t in tuples.into_iter() {
            match t.key {
                None => {}
                Some(k) => warehouses.push(k.object),
            }
        }
    }
    Ok(warehouses)
}

// TODO concurrency, read with smaller page size
async fn get_all_namespaces(
    client: &BasicOpenFgaClient,
    warehouse: String,
) -> anyhow::Result<Vec<String>> {
    let mut namespaces = vec![];
    let mut to_process = VecDeque::from([warehouse.clone()]);

    // Breadth-first search to query namespaces at a given level in parallel.
    while let Some(parent) = to_process.pop_front() {
        // Get all namespaces that have this parent (warehouse or namespace).
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

        for tuple in tuples.into_iter() {
            if let Some(key) = tuple.key {
                let namespace = key.object;
                namespaces.push(namespace.clone());
                // Add this namespace to the processing queue to find its children
                to_process.push_back(namespace);
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

// TODO concurrency, read with smaller page size
async fn get_all_tabulars(
    client: &BasicOpenFgaClient,
    namespaces: &[String],
) -> anyhow::Result<Vec<String>> {
    let mut tabulars = vec![];
    for ns in namespaces.iter() {
        for tab in TabularType::iter() {
            let tuples = client
                .read_all_pages(
                    ReadRequestTupleKey {
                        user: ns.to_string(),
                        relation: tab.parent_relation_string(),
                        object: tab.object_type(),
                    },
                    OPENFGA_PAGE_SIZE,
                    u32::MAX,
                )
                .await?;
            for t in tuples.into_iter() {
                if let Some(k) = t.key {
                    tabulars.push(k.object)
                }
            }
        }
    }
    Ok(tabulars)
}

#[cfg(test)]
#[allow(dead_code)]
mod tests {
    use needs_env_var::needs_env_var;

    #[needs_env_var(TEST_OPENFGA = 1)]
    mod openfga {
        use openfga_client::client::TupleKey;

        use super::super::*;
        use crate::{
            service::authz::implementations::openfga::{
                migration::tests::authorizer_for_empty_store, OPENFGA_SERVER,
            },
            CONFIG,
        };

        // Tests must write tuples according to v4 model manually.
        // Writing through methods like `authorizer.create_*` may create tuples different from
        // what v4 migration is designed to handle.

        #[sqlx::test]
        async fn test_get_all_projects(pool: sqlx::PgPool) -> anyhow::Result<()> {
            let (_, authorizer, _) = authorizer_for_empty_store(pool).await;

            authorizer
                .write(
                    Some(vec![
                        TupleKey {
                            user: "user:actor".to_string(),
                            relation: ProjectRelation::ProjectAdmin.to_string(),
                            object: "project:p1".to_string(),
                            condition: None,
                        },
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

        #[sqlx::test]
        async fn test_get_all_warehouses(pool: sqlx::PgPool) -> anyhow::Result<()> {
            let (_, authorizer, _) = authorizer_for_empty_store(pool).await;

            authorizer
                .write(
                    Some(vec![
                        TupleKey {
                            user: "user:actor".to_string(),
                            relation: WarehouseRelation::Ownership.to_string(),
                            object: "warehouse:wh1".to_string(),
                            condition: None,
                        },
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
                    ]),
                    None,
                )
                .await?;

            let projects = vec!["project:p1".to_string(), "project:p2".to_string()];
            let mut warehouses = get_all_warehouses(&authorizer.client, &projects).await?;
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

        #[sqlx::test]
        async fn test_get_all_namespaces(pool: sqlx::PgPool) -> anyhow::Result<()> {
            let (_, authorizer, _) = authorizer_for_empty_store(pool).await;

            // warehouse:w1 -> ns1 -> ns2 -> ns3
            //            |--> ns4
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

        #[sqlx::test]
        async fn test_get_all_namespaces_empty_warehouse(pool: sqlx::PgPool) -> anyhow::Result<()> {
            let (_, authorizer, _) = authorizer_for_empty_store(pool).await;

            let namespaces =
                get_all_namespaces(&authorizer.client, "warehouse:empty".to_string()).await?;
            assert!(namespaces.is_empty());
            Ok(())
        }

        #[sqlx::test]
        async fn test_get_all_tabulars(pool: sqlx::PgPool) -> anyhow::Result<()> {
            let (_, authorizer, _) = authorizer_for_empty_store(pool).await;

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

        #[sqlx::test]
        async fn test_get_all_tabulars_empty_namespaces(pool: sqlx::PgPool) -> anyhow::Result<()> {
            let (_, authorizer, _) = authorizer_for_empty_store(pool).await;

            // Test with namespaces that have no tables or views
            let namespaces = vec![
                "namespace:empty1".to_string(),
                "namespace:empty2".to_string(),
            ];
            let tabulars = get_all_tabulars(&authorizer.client, &namespaces).await?;
            assert!(tabulars.is_empty());
            Ok(())
        }
    }
}
