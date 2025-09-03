use std::sync::LazyLock;

use openfga_client::{
    client::{BasicAuthLayer, BasicOpenFgaServiceClient},
    migration::{AuthorizationModelVersion, MigrationFn},
};

use super::{OpenFGAError, OpenFGAResult, AUTH_CONFIG};

pub(super) static ACTIVE_MODEL_VERSION: LazyLock<AuthorizationModelVersion> =
    LazyLock::new(|| AuthorizationModelVersion::new(3, 4)); // <- Change this for every change in the model

fn get_model_manager(
    client: &BasicOpenFgaServiceClient,
    store_name: Option<String>,
) -> openfga_client::migration::TupleModelManager<BasicAuthLayer> {
    openfga_client::migration::TupleModelManager::new(
        client.clone(),
        &store_name.unwrap_or(AUTH_CONFIG.store_name.clone()),
        &AUTH_CONFIG.authorization_model_prefix,
    )
    .add_model(
        serde_json::from_str(include_str!(
            // Change this for backward compatible changes.
            // For non-backward compatible changes that require tuple migrations, add another `add_model` call.
            "../../../../../../../authz/openfga/v3.4/schema.json"
        ))
        // Change also the model version in this string:
        .expect("Model v3.4 is a valid AuthorizationModel in JSON format."),
        AuthorizationModelVersion::new(3, 4),
        // For major version upgrades, this is where tuple migrations go.
        None::<MigrationFn<_>>,
        None::<MigrationFn<_>>,
    )
}

/// Get the active authorization model id.
/// Leave `store_name` empty to use the default store name.
///
/// # Errors
/// * [`OpenFGAError::ClientError`] if the client fails to get the active model id
pub(super) async fn get_active_auth_model_id(
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
pub(crate) async fn migrate(
    client: &BasicOpenFgaServiceClient,
    store_name: Option<String>,
) -> OpenFGAResult<()> {
    if let Some(configured_model) = *super::CONFIGURED_MODEL_VERSION {
        tracing::info!("Skipping OpenFGA Migration because a model version is explicitly is configured. Version: {configured_model}");
        return Ok(());
    }
    let store_name = store_name.unwrap_or(AUTH_CONFIG.store_name.clone());
    tracing::info!("Starting OpenFGA Migration for store {store_name}");
    let mut manager = get_model_manager(client, Some(store_name.clone()));
    manager.migrate().await?;
    tracing::info!("OpenFGA Migration finished");
    Ok(())
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) mod tests {
    use openfga_client::client::ConsistencyPreference;

    use super::{
        super::{client::new_authorizer, OpenFGAAuthorizer},
        *,
    };
    use crate::service::authz::implementations::openfga::new_client_from_config;

    pub(crate) async fn authorizer_for_empty_store(
    ) -> (BasicOpenFgaServiceClient, OpenFGAAuthorizer) {
        let client = new_client_from_config().await.unwrap();

        let test_uuid = uuid::Uuid::now_v7();
        let store_name = format!("test_store_{test_uuid}");
        migrate(&client, Some(store_name.clone())).await.unwrap();

        let authorizer = new_authorizer(
            client.clone(),
            Some(store_name),
            ConsistencyPreference::HigherConsistency,
        )
        .await
        .unwrap();

        (client, authorizer)
    }

    mod openfga_integration_tests {
        use openfga_client::client::ReadAuthorizationModelsRequest;

        use super::super::*;
        use crate::service::authz::implementations::openfga::new_client_from_config;

        #[tokio::test]
        async fn test_migrate() {
            let mut client = new_client_from_config().await.unwrap();
            let store_name = format!("test_store_{}", uuid::Uuid::now_v7());

            let _model = get_active_auth_model_id(&mut client, Some(store_name.clone()))
                .await
                .unwrap_err();

            // Multiple migrations should be idempotent
            migrate(&client, Some(store_name.clone())).await.unwrap();
            migrate(&client, Some(store_name.clone())).await.unwrap();
            migrate(&client, Some(store_name.clone())).await.unwrap();

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
            assert_eq!(models.len(), 1);
        }
    }
}
