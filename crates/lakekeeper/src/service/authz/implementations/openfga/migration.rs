use std::{collections::HashMap, sync::LazyLock};

use openfga_client::{
    client::{BasicAuthLayer, BasicOpenFgaServiceClient, OpenFgaClient},
    migration::{AuthorizationModelVersion, MigrationFn},
};

use crate::service::{Catalog, TableId};
use crate::WarehouseId;

use super::{OpenFGAError, OpenFGAResult, AUTH_CONFIG};

pub(super) static ACTIVE_MODEL_VERSION: LazyLock<AuthorizationModelVersion> =
    LazyLock::new(|| AuthorizationModelVersion::new(3, 4)); // <- Change this for every change in the model

#[derive(Clone, Debug)]
pub struct MigrationState<C: Catalog> {
    store_name: String,
    catalog: C,
    catalog_state: C::State,
}

fn get_model_manager<C: Catalog>(
    client: &BasicOpenFgaServiceClient,
    store_name: Option<String>,
) -> openfga_client::migration::TupleModelManager<BasicAuthLayer, MigrationState<C>> {
    openfga_client::migration::TupleModelManager::new(
        client.clone(),
        &store_name.unwrap_or(AUTH_CONFIG.store_name.clone()),
        &AUTH_CONFIG.authorization_model_prefix,
    )
    .add_model(
        // put 4 here
        serde_json::from_str(include_str!(
            // Change this for backward compatible changes.
            // For non-backward compatible changes that require tuple migrations, add another `add_model` call.
            "../../../../../../../authz/openfga/v3.4/schema.json"
        ))
        // Change also the model version in this string:
        .expect("Model v3.4 is a valid AuthorizationModel in JSON format."),
        AuthorizationModelVersion::new(3, 4),
        // For major version upgrades, this is where tuple migrations go.
        None::<MigrationFn<_, _>>,
        Some(push_down_warehouse_id),
    )
}

// catalog trait reingeben, nicht postgres db
async fn push_down_warehouse_id<C: Catalog>(
    mut client: BasicOpenFgaServiceClient,
    auth_model_id: String,
    state: MigrationState<C>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    println!("state in migration fn: {}", state.store_name);
    // Construct OpenFGAClient to be able to use convenience methods.
    let store = client
        .get_store_by_name(&state.store_name)
        .await?
        .expect("default store should exist");
    let c = client.into_client(&store.id, &auth_model_id);

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

    let _res = add_warehouse_id_to_tables(c.clone(), &HashMap::new()).await?;
    Ok(())
}

async fn add_warehouse_id_to_tables<T>(
    _client: OpenFgaClient<T>,
    _warhouse_ids: &HashMap<TableId, WarehouseId>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    // let tables_
    Ok(())
}

/// Get the active authorization model id.
/// Leave `store_name` empty to use the default store name.
///
/// Active here refers to the hardcoded model version. This might not be the version you want
/// when consecutive migrations are applied, so avoid using it in migration functions.
///
/// # Errors
/// * [`OpenFGAError::ClientError`] if the client fails to get the active model id
pub(super) async fn get_active_auth_model_id<C: Catalog>(
    client: &mut BasicOpenFgaServiceClient,
    store_name: Option<String>,
) -> OpenFGAResult<String> {
    let mut manager = get_model_manager::<C>(client, store_name);
    let model_version = super::CONFIGURED_MODEL_VERSION.unwrap_or(*ACTIVE_MODEL_VERSION);
    tracing::info!("Getting active OpenFGA Authorization Model ID for version {model_version}.");
    manager
        .get_authorization_model_id(*ACTIVE_MODEL_VERSION)
        .await
        .inspect_err(|e| {
            tracing::error!(
                "Failed to get active OpenFGA Authorization Model ID for Version {model_version}: {:?}",
                e
            );
        })?
        .ok_or(OpenFGAError::ActiveAuthModelNotFound(
            ACTIVE_MODEL_VERSION.to_string(),
        ))
}

/// Migrate the authorization model to the latest version.
///
/// After writing is finished, the following tuples will be written:
/// - `auth_model_id:<auth_model_id>:applied:model_version:<active_model_int>`
/// - `auth_model_id:*:exists:model_version:<active_model_int>`
///
/// These tuples are used to get the auth model id for the active model version and
/// to check whether a migration is needed.
///
/// # Errors
/// - Failed to read existing models
/// - Failed to write new model
/// - Failed to write new version tuples
pub(crate) async fn migrate<C: Catalog>(
    client: &BasicOpenFgaServiceClient,
    store_name: Option<String>,
    catalog: C,
    catalog_state: C::State,
) -> OpenFGAResult<()> {
    if let Some(configured_model) = *super::CONFIGURED_MODEL_VERSION {
        tracing::info!("Skipping OpenFGA Migration because a model version is explicitly is configured. Version: {configured_model}");
        return Ok(());
    }
    let store_name = store_name.unwrap_or(AUTH_CONFIG.store_name.clone());
    tracing::info!("Starting OpenFGA Migration for store {store_name}");
    let mut manager = get_model_manager(client, Some(store_name.clone()));
    let state = MigrationState {
        store_name,
        catalog,
        catalog_state,
    };
    manager.migrate(state).await?;
    tracing::info!("OpenFGA Migration finished");
    Ok(())
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) mod tests {
    use needs_env_var::needs_env_var;
    use openfga_client::client::ConsistencyPreference;

    use super::{
        super::{client::new_authorizer, OpenFGAAuthorizer},
        *,
    };
    use crate::implementations::postgres::{self, PostgresCatalog};
    use crate::service::authz::implementations::openfga::new_client_from_config;

    pub(crate) async fn authorizer_for_empty_store(
        pool: sqlx::PgPool,
    ) -> (
        BasicOpenFgaServiceClient,
        OpenFGAAuthorizer,
        postgres::CatalogState,
    ) {
        let client = new_client_from_config().await.unwrap();

        let test_uuid = uuid::Uuid::now_v7();
        let store_name = format!("test_store_{test_uuid}");
        let catalog = PostgresCatalog {};
        let catalog_state = postgres::CatalogState::from_pools(pool.clone(), pool.clone());
        migrate(
            &client,
            Some(store_name.clone()),
            catalog,
            catalog_state.clone(),
        )
        .await
        .unwrap();

        let authorizer = new_authorizer::<PostgresCatalog>(
            client.clone(),
            Some(store_name),
            ConsistencyPreference::HigherConsistency,
        )
        .await
        .unwrap();

        (client, authorizer, catalog_state)
    }

    #[needs_env_var(TEST_OPENFGA = 1)]
    mod openfga {
        use openfga_client::client::ReadAuthorizationModelsRequest;

        use super::super::*;
        use crate::{
            implementations::postgres::{self, PostgresCatalog},
            service::authz::implementations::openfga::new_client_from_config,
        };

        #[sqlx::test]
        async fn test_migrate(pool: sqlx::PgPool) {
            let catalog = PostgresCatalog {};
            let catalog_state = postgres::CatalogState::from_pools(pool.clone(), pool.clone());

            let mut client = new_client_from_config().await.unwrap();
            let store_name = format!("test_store_{}", uuid::Uuid::now_v7());

            let _model =
                get_active_auth_model_id::<PostgresCatalog>(&mut client, Some(store_name.clone()))
                    .await
                    .unwrap_err();

            // Multiple migrations should be idempotent
            migrate(
                &client,
                Some(store_name.clone()),
                catalog.clone(),
                catalog_state.clone(),
            )
            .await
            .unwrap();
            migrate(
                &client,
                Some(store_name.clone()),
                catalog.clone(),
                catalog_state.clone(),
            )
            .await
            .unwrap();
            migrate(
                &client,
                Some(store_name.clone()),
                catalog.clone(),
                catalog_state.clone(),
            )
            .await
            .unwrap();

            let store_id = client
                .get_store_by_name(&store_name)
                .await
                .unwrap()
                .unwrap()
                .id;
            let _model =
                get_active_auth_model_id::<PostgresCatalog>(&mut client, Some(store_name.clone()))
                    .await
                    .expect("Active model should exist after migration");

            // Check that there is only a single model in the store
            let models = client
                .read_authorization_models(ReadAuthorizationModelsRequest {
                    store_id,
                    page_size: Some(100),
                    continuation_token: String::new(),
                })
                .await
                .unwrap()
                .into_inner()
                .authorization_models;
            assert_eq!(models.len(), 1);
        }
    }
}
