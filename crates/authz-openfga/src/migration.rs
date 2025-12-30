use std::sync::LazyLock;

use lakekeeper::service::ServerId;
use openfga_client::{
    client::{BasicAuthLayer, BasicOpenFgaServiceClient},
    migration::{AuthorizationModelVersion, MigrationFn, TupleModelManager},
};

use super::{AUTH_CONFIG, OpenFGAError, OpenFGAResult};

pub(super) static ACTIVE_MODEL_VERSION: LazyLock<AuthorizationModelVersion> =
    LazyLock::new(|| *V4_CURRENT_MODEL_VERSION); // <- Change this for every change in the model

pub(super) static V4_0_MODEL_VERSION: LazyLock<AuthorizationModelVersion> =
    LazyLock::new(|| AuthorizationModelVersion::new(4, 0));

pub(super) static V4_CURRENT_MODEL_VERSION: LazyLock<AuthorizationModelVersion> =
    LazyLock::new(|| AuthorizationModelVersion::new(4, 3));

#[cfg(test)]
pub(super) static V3_MODEL_VERSION: LazyLock<AuthorizationModelVersion> =
    LazyLock::new(|| AuthorizationModelVersion::new(3, 4));

mod migration_fns_v4;
use migration_fns_v4::{MigrationState, v4_push_down_warehouse_id};

fn get_model_manager(
    client: &BasicOpenFgaServiceClient,
    store_name: Option<String>,
) -> TupleModelManager<BasicAuthLayer, MigrationState> {
    let manager = TupleModelManager::new(
        client.clone(),
        &store_name.unwrap_or(AUTH_CONFIG.store_name.clone()),
        &AUTH_CONFIG.authorization_model_prefix,
    );
    // Assume vX.Y has a migration function. Then vX.Y must remain here as long as migrations from
    // versions < vX.Y are supported.
    let manager = add_model_v4_0(manager);
    add_model_v4_current(manager)
}

/// Has no migration hooks.
#[cfg(test)]
pub(crate) fn add_model_v3(
    manager: TupleModelManager<BasicAuthLayer, MigrationState>,
) -> TupleModelManager<BasicAuthLayer, MigrationState> {
    manager.add_model(
        serde_json::from_str(include_str!(
            // Change this for backward compatible changes.
            // For non-backward compatible changes that require tuple migrations, add another `add_model` call.
            "../../../authz/openfga/v3.4/schema.json"
        ))
        // Change also the model version in this string:
        .expect("Model v3.4 is a valid AuthorizationModel in JSON format."),
        *V3_MODEL_VERSION,
        // For major version upgrades, this is where tuple migrations go.
        None::<MigrationFn<_, _>>,
        None::<MigrationFn<_, _>>,
    )
}

/// Does have a migration hook which may add tuples to the store.
pub(crate) fn add_model_v4_0(
    manager: TupleModelManager<BasicAuthLayer, MigrationState>,
) -> TupleModelManager<BasicAuthLayer, MigrationState> {
    manager.add_model(
        serde_json::from_str(include_str!(
            // Change this for backward compatible changes.
            // For non-backward compatible changes that require tuple migrations, add another `add_model` call.
            "../../../authz/openfga/v4.0/schema.json"
        ))
        // Change also the model version in this string:
        .expect("Model v4.0 is a valid AuthorizationModel in JSON format."),
        *V4_0_MODEL_VERSION,
        // For major version upgrades, this is where tuple migrations go.
        None::<MigrationFn<_, _>>,
        Some(v4_push_down_warehouse_id),
    )
}

/// Does not have a migration hook
pub(crate) fn add_model_v4_current(
    manager: TupleModelManager<BasicAuthLayer, MigrationState>,
) -> TupleModelManager<BasicAuthLayer, MigrationState> {
    manager.add_model(
        serde_json::from_str(include_str!(
            // Change this for backward compatible changes.
            // For non-backward compatible changes that require tuple migrations, add another `add_model` call.
            "../../../authz/openfga/v4.3/schema.json"
        ))
        // Change also the model version in this string:
        .expect("Model v4.3 is a valid AuthorizationModel in JSON format."),
        *V4_CURRENT_MODEL_VERSION,
        // For major version upgrades, this is where tuple migrations go.
        None::<MigrationFn<_, _>>,
        None::<MigrationFn<_, _>>,
    )
}

/// Get the active authorization model id.
/// Leave `store_name` empty to use the default store name.
///
/// Active here refers to the hardcoded model version. This might not be the version you want
/// when consecutive migrations are applied, so avoid using it in migration functions.
///
/// # Errors
/// * [`OpenFGAError::ClientError`] if the client fails to get the active model id
pub(crate) async fn get_active_auth_model_id(
    client: &mut BasicOpenFgaServiceClient,
    store_name: Option<String>,
) -> OpenFGAResult<String> {
    let mut manager = get_model_manager(client, store_name);
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
pub async fn migrate(
    client: &BasicOpenFgaServiceClient,
    store_name: Option<String>,
    server_id: ServerId,
) -> OpenFGAResult<()> {
    if let Some(configured_model) = *super::CONFIGURED_MODEL_VERSION {
        tracing::info!(
            "Skipping OpenFGA Migration because a model version is explicitly configured. Version: {configured_model}"
        );
        return Ok(());
    }
    let store_name = store_name.unwrap_or(AUTH_CONFIG.store_name.clone());
    tracing::info!("Starting OpenFGA Migration for store {store_name} and server {server_id}");
    let mut manager = get_model_manager(client, Some(store_name.clone()));
    let state = MigrationState {
        store_name,
        server_id,
    };
    manager.migrate(state).await?;
    tracing::info!("OpenFGA Migration finished");
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use openfga_client::client::ConsistencyPreference;

    use super::{
        super::{OpenFGAAuthorizer, client::new_authorizer},
        *,
    };
    use crate::client::new_client_from_default_config;
    pub(crate) async fn authorizer_for_empty_store()
    -> (BasicOpenFgaServiceClient, OpenFGAAuthorizer) {
        let client = new_client_from_default_config().await.unwrap();

        let server_id = ServerId::new_random();
        let test_uuid = uuid::Uuid::now_v7();
        let store_name = format!("test_store_{test_uuid}");
        migrate(&client, Some(store_name.clone()), server_id)
            .await
            .unwrap();

        let authorizer = new_authorizer(
            client.clone(),
            Some(store_name),
            ConsistencyPreference::HigherConsistency,
            server_id,
        )
        .await
        .unwrap();

        (client, authorizer)
    }

    mod openfga_integration_tests {
        use lakekeeper::tokio;
        use openfga_client::client::ReadAuthorizationModelsRequest;

        use super::super::*;
        use crate::client::new_client_from_default_config;

        #[tokio::test]
        async fn test_migrate() {
            let mut client = new_client_from_default_config().await.unwrap();
            let server_id = ServerId::new_random();
            let store_name = format!("test_store_{}", uuid::Uuid::now_v7());

            let _model = get_active_auth_model_id(&mut client, Some(store_name.clone()))
                .await
                .unwrap_err();

            // Multiple migrations should be idempotent
            migrate(&client, Some(store_name.clone()), server_id)
                .await
                .unwrap();
            migrate(&client, Some(store_name.clone()), server_id)
                .await
                .unwrap();
            migrate(&client, Some(store_name.clone()), server_id)
                .await
                .unwrap();

            let store_id = client
                .get_store_by_name(&store_name)
                .await
                .unwrap()
                .unwrap()
                .id;
            let _model = get_active_auth_model_id(&mut client, Some(store_name.clone()))
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
            assert_eq!(models.len(), 2);
        }
    }
}
