use std::collections::HashMap;

use openfga_client::client::{BasicOpenFgaServiceClient, OpenFgaClient};
use strum::IntoEnumIterator;

use crate::service::{catalog::Transaction, TableId};
use crate::service::{Catalog, WarehouseStatus};
use crate::{ProjectId, WarehouseId};

#[derive(Clone, Debug)]
pub(crate) struct MigrationState<C: Catalog> {
    pub store_name: String,
    pub catalog: C,
    pub catalog_state: C::State,
}

// TODO(mooori): use tokio semaphore to limit the number of concurrent db txs,
// then can use something like futures unordered

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
    // currently all active
    let warehouse_ids = get_all_warehouse_ids::<C>(state.catalog_state.clone(), &project_ids)
        .await
        .map_err(|e| e.error)?;

    let _res = add_warehouse_id_to_tables(c.clone(), &HashMap::new()).await?;
    Ok(())
}

async fn read_transaction<C: Catalog>(state: C::State) -> crate::api::Result<C::Transaction> {
    C::Transaction::begin_read(state).await
}

/// Returns the ids of all warehouses, regardless of their status.
async fn get_all_warehouse_ids<C: Catalog>(
    catalog_state: C::State,
    project_ids: &[ProjectId],
) -> crate::api::Result<Vec<WarehouseId>> {
    // TODO parallelize
    let all_statuses: Vec<_> = WarehouseStatus::iter().collect();
    let mut warehouse_ids = vec![];
    for pid in project_ids.iter() {
        let mut tx = read_transaction::<C>(catalog_state.clone()).await?;
        // This returns all warehouses in the project since it is not (yet) possible to
        // deactivate a warehouse.
        let responses =
            C::list_warehouses(pid, Some(all_statuses.clone()), tx.transaction()).await?;
        let ids = responses.iter().map(|res| res.id);
        warehouse_ids.extend(ids);
    }
    Ok(warehouse_ids)
}

// TODO in catalog: implement fn that traverses all namespaces
// catalog's basic `list_namespaces` is only one level, not its children
async fn get_all_namespaces() {}

async fn add_warehouse_id_to_tables<T>(
    _client: OpenFgaClient<T>,
    _warhouse_ids: &HashMap<TableId, WarehouseId>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    // let tables_
    Ok(())
}
