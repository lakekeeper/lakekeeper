use std::sync::LazyLock;

use lakekeeper::service::ServerId;
use openfga_client::{
    client::{
        AuthorizationModel, BasicAuthLayer, BasicOpenFgaServiceClient,
        ReadAuthorizationModelRequest, Store,
    },
    error::Error as OpenFGAClientError,
    migration::{AuthorizationModelVersion, MigrationFn, TupleModelManager},
};
use strum::IntoEnumIterator;

use super::{AUTH_CONFIG, FgaType, OpenFGAError, OpenFGAResult, relations::relations_in_use};
use crate::error::AuthModelError;

/// Prefix of the authorization models that `lakekeeper migrate` installs.
pub(crate) const OWN_MODEL_PREFIX: &str = "collaboration";

pub(super) static ACTIVE_MODEL_VERSION: LazyLock<AuthorizationModelVersion> =
    LazyLock::new(|| *V4_CURRENT_MODEL_VERSION); // <- Change this for every change in the model

pub(super) static V4_0_MODEL_VERSION: LazyLock<AuthorizationModelVersion> =
    LazyLock::new(|| AuthorizationModelVersion::new(4, 0));

pub(super) static V4_CURRENT_MODEL_VERSION: LazyLock<AuthorizationModelVersion> =
    LazyLock::new(|| AuthorizationModelVersion::new(4, 16));

const V4_0_MODEL_JSON: &str = include_str!(
    // Change this for backward compatible changes.
    // For non-backward compatible changes that require tuple migrations, add another `add_model` call.
    "../../../authz/openfga/v4.0/schema.json"
);

const V4_CURRENT_MODEL_JSON: &str = include_str!(
    // Change this for backward compatible changes.
    // For non-backward compatible changes that require tuple migrations, add another `add_model` call.
    "../../../authz/openfga/v4.16/schema.json"
);

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
        serde_json::from_str(V4_0_MODEL_JSON)
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
        serde_json::from_str(V4_CURRENT_MODEL_JSON)
            // Change also the model version in this string:
            .expect("Model v4.16 is a valid AuthorizationModel in JSON format."),
        *V4_CURRENT_MODEL_VERSION,
        // For major version upgrades, this is where tuple migrations go.
        None::<MigrationFn<_, _>>,
        None::<MigrationFn<_, _>>,
    )
}

/// Store and authorization model the server uses.
#[derive(Debug)]
pub(crate) struct ActiveAuthModel {
    pub(crate) store_id: String,
    pub(crate) model_id: String,
}

/// Get the store and the id of the active authorization model.
/// Leave `store_name` empty to use the default store name.
///
/// Active here refers to the configured model version, or to the hardcoded
/// [`ACTIVE_MODEL_VERSION`] if none is configured. This might not be the version you want
/// when consecutive migrations are applied, so avoid using it in migration functions.
///
/// # Errors
/// * [`OpenFGAError::InternalClientError`] if a request to `OpenFGA` fails
/// * [`OpenFGAError::StoreNotFound`] if the store does not exist
/// * [`OpenFGAError::ActiveAuthModelNotFound`] if the active model version is not applied
/// * [`OpenFGAError::AuthModel`] if the server cannot use the active model
pub(crate) async fn get_active_auth_model(
    client: &mut BasicOpenFgaServiceClient,
    store_name: Option<String>,
) -> OpenFGAResult<ActiveAuthModel> {
    let store_name = store_name.unwrap_or(AUTH_CONFIG.store_name.clone());
    let store = client
        .get_store_by_name(&store_name)
        .await?
        .ok_or(OpenFGAError::StoreNotFound(store_name))?;
    let model_id = get_auth_model_id(
        client,
        &store,
        &AUTH_CONFIG.authorization_model_prefix,
        *super::CONFIGURED_MODEL_VERSION,
    )
    .await?;
    Ok(ActiveAuthModel {
        store_id: store.id,
        model_id,
    })
}

/// The model version to use: `configured_version`, or [`ACTIVE_MODEL_VERSION`] if it is
/// `None`. Lakekeeper's own models change between versions under the same relation
/// names, so under [`OWN_MODEL_PREFIX`] only [`ACTIVE_MODEL_VERSION`] is accepted.
fn model_version_to_use(
    prefix: &str,
    configured_version: Option<AuthorizationModelVersion>,
) -> Result<AuthorizationModelVersion, AuthModelError> {
    match configured_version {
        None => Ok(*ACTIVE_MODEL_VERSION),
        Some(version) if prefix == OWN_MODEL_PREFIX && version != *ACTIVE_MODEL_VERSION => {
            Err(AuthModelError::OwnVersionMismatch {
                configured: version.to_string(),
                expected: ACTIVE_MODEL_VERSION.to_string(),
            })
        }
        Some(version) => Ok(version),
    }
}

/// Get the id of the model to use in `store`, and check that the model has every type
/// and relation the server checks or writes. Only names are compared: under a prefix
/// other than [`OWN_MODEL_PREFIX`] the definitions are the model owner's responsibility.
async fn get_auth_model_id(
    client: &mut BasicOpenFgaServiceClient,
    store: &Store,
    prefix: &str,
    configured_version: Option<AuthorizationModelVersion>,
) -> OpenFGAResult<String> {
    let model_version = model_version_to_use(prefix, configured_version)?;
    tracing::info!("Getting active OpenFGA Authorization Model ID for version {model_version}.");
    let model_id = TupleModelManager::<BasicAuthLayer, MigrationState>::new(
        client.clone(),
        &store.name,
        prefix,
    )
    .get_authorization_model_id(model_version)
    .await
    .inspect_err(|e| {
        tracing::error!(
            "Failed to get active OpenFGA Authorization Model ID for Version {model_version}: {:?}",
            e
        );
    })?
    .ok_or(OpenFGAError::ActiveAuthModelNotFound(
        model_version.to_string(),
    ))?;

    let model = client
        .read_authorization_model(ReadAuthorizationModelRequest {
            store_id: store.id.clone(),
            id: model_id.clone(),
        })
        .await
        .map_err(|e| OpenFGAClientError::RequestFailed(Box::new(e)))?
        .into_inner()
        .authorization_model
        .ok_or_else(|| AuthModelError::NotReturned {
            version: model_version.to_string(),
            model_id: model_id.clone(),
        })?;
    let missing = missing_model_entries(&model);
    if !missing.is_empty() {
        return Err(AuthModelError::MissingRelations {
            version: model_version.to_string(),
            missing,
        }
        .into());
    }
    Ok(model_id)
}

/// Types and relations the server checks or writes that `model` lacks: `<type>#<relation>`
/// for a missing relation, `<type>` for a missing type that the server uses without relations.
fn missing_model_entries(model: &AuthorizationModel) -> Vec<String> {
    let mut missing = Vec::new();
    for fga_type in FgaType::iter() {
        let Some(relations) = relations_in_use(&fga_type) else {
            continue;
        };
        let type_name = fga_type.to_string();
        let definition = model
            .type_definitions
            .iter()
            .find(|definition| definition.r#type == type_name);
        if relations.is_empty() {
            if definition.is_none() {
                missing.push(type_name);
            }
            continue;
        }
        missing.extend(
            relations
                .into_iter()
                .filter(|relation| {
                    !definition
                        .is_some_and(|definition| definition.relations.contains_key(relation))
                })
                .map(|relation| format!("{type_name}#{relation}")),
        );
    }
    missing
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

    fn model_json(json: &str) -> serde_json::Value {
        serde_json::from_str(json).unwrap()
    }

    fn to_model(json: serde_json::Value) -> AuthorizationModel {
        serde_json::from_value(json).unwrap()
    }

    fn type_definition<'a>(
        model: &'a mut serde_json::Value,
        type_name: &str,
    ) -> &'a mut serde_json::Value {
        model["type_definitions"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|definition| definition["type"] == type_name)
            .unwrap()
    }

    /// The current model without `warehouse#can_set_protection` and
    /// `lakekeeper_view#can_move`. No other relation refers to them.
    fn current_model_lacking_two_relations() -> serde_json::Value {
        let mut model = model_json(V4_CURRENT_MODEL_JSON);
        for (type_name, relation) in [
            ("warehouse", "can_set_protection"),
            ("lakekeeper_view", "can_move"),
        ] {
            let definition = type_definition(&mut model, type_name);
            definition["relations"]
                .as_object_mut()
                .unwrap()
                .remove(relation)
                .unwrap();
            definition["metadata"]["relations"]
                .as_object_mut()
                .unwrap()
                .remove(relation);
        }
        model
    }

    /// The current model with an extra type and an extra warehouse relation, as a
    /// model managed outside of Lakekeeper might have.
    fn current_model_with_extra_entries() -> serde_json::Value {
        let mut model = model_json(V4_CURRENT_MODEL_JSON);
        let warehouse = type_definition(&mut model, "warehouse");
        warehouse["relations"]["external_reader"] = serde_json::json!({"this": {}});
        warehouse["metadata"]["relations"]["external_reader"] =
            serde_json::json!({"directly_related_user_types": [{"type": "user"}]});
        model["type_definitions"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({
                "type": "external_report",
                "relations": {"viewer": {"this": {}}},
                "metadata": {"relations": {"viewer": {
                    "directly_related_user_types": [{"type": "user"}]
                }}}
            }));
        model
    }

    #[test]
    fn current_model_has_every_entry_in_use() {
        assert_eq!(
            missing_model_entries(&to_model(model_json(V4_CURRENT_MODEL_JSON))),
            Vec::<String>::new()
        );
    }

    #[test]
    fn missing_relations_and_types_are_listed() {
        let mut model = current_model_lacking_two_relations();
        model["type_definitions"]
            .as_array_mut()
            .unwrap()
            .retain(|definition| definition["type"] != "user");
        assert_eq!(
            missing_model_entries(&to_model(model)),
            vec![
                "user".to_string(),
                "warehouse#can_set_protection".to_string(),
                "lakekeeper_view#can_move".to_string(),
            ]
        );
    }

    #[test]
    fn own_prefix_accepts_only_the_active_version() {
        assert_eq!(
            model_version_to_use(OWN_MODEL_PREFIX, None).unwrap(),
            *ACTIVE_MODEL_VERSION
        );
        assert_eq!(
            model_version_to_use(OWN_MODEL_PREFIX, Some(*ACTIVE_MODEL_VERSION)).unwrap(),
            *ACTIVE_MODEL_VERSION
        );
        match model_version_to_use(OWN_MODEL_PREFIX, Some(*V4_0_MODEL_VERSION)) {
            Err(AuthModelError::OwnVersionMismatch {
                configured,
                expected,
            }) => {
                assert_eq!(configured, "4.0");
                assert_eq!(expected, ACTIVE_MODEL_VERSION.to_string());
            }
            other => panic!("Expected OwnVersionMismatch, got {other:?}"),
        }
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

            let _model = get_active_auth_model(&mut client, Some(store_name.clone()))
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
            let _model = get_active_auth_model(&mut client, Some(store_name.clone()))
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

        const CUSTOM_PREFIX: &str = "external";

        /// Creates a store in which only `model` is applied, under [`CUSTOM_PREFIX`].
        async fn store_with_custom_model(
            client: &mut BasicOpenFgaServiceClient,
            model: serde_json::Value,
            version: AuthorizationModelVersion,
        ) -> Store {
            let store_name = format!("test_store_{}", uuid::Uuid::now_v7());
            TupleModelManager::new(client.clone(), &store_name, CUSTOM_PREFIX)
                .add_model(
                    super::to_model(model),
                    version,
                    None::<MigrationFn<_, _>>,
                    None::<MigrationFn<_, _>>,
                )
                .migrate(MigrationState {
                    store_name: store_name.clone(),
                    server_id: ServerId::new_random(),
                })
                .await
                .unwrap();
            client
                .get_store_by_name(&store_name)
                .await
                .unwrap()
                .unwrap()
        }

        #[tokio::test]
        async fn test_custom_model_with_extra_entries_is_used() {
            let mut client = new_client_from_default_config().await.unwrap();
            let version = AuthorizationModelVersion::new(9, 2);
            let store = store_with_custom_model(
                &mut client,
                super::current_model_with_extra_entries(),
                version,
            )
            .await;
            let custom_model_id = TupleModelManager::<BasicAuthLayer, MigrationState>::new(
                client.clone(),
                &store.name,
                CUSTOM_PREFIX,
            )
            .get_authorization_model_id(version)
            .await
            .unwrap()
            .expect("Custom model should be applied");

            let model_id = get_auth_model_id(&mut client, &store, CUSTOM_PREFIX, Some(version))
                .await
                .unwrap();
            assert_eq!(model_id, custom_model_id);
        }

        #[tokio::test]
        async fn test_custom_model_lacking_relations_is_refused() {
            let mut client = new_client_from_default_config().await.unwrap();
            let version = AuthorizationModelVersion::new(9, 1);
            let store = store_with_custom_model(
                &mut client,
                super::current_model_lacking_two_relations(),
                version,
            )
            .await;

            match get_auth_model_id(&mut client, &store, CUSTOM_PREFIX, Some(version)).await {
                Err(OpenFGAError::AuthModel(AuthModelError::MissingRelations {
                    version,
                    missing,
                })) => {
                    assert_eq!(version, "9.1");
                    assert_eq!(
                        missing,
                        vec![
                            "warehouse#can_set_protection".to_string(),
                            "lakekeeper_view#can_move".to_string(),
                        ]
                    );
                }
                other => panic!("Expected MissingRelations, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn test_configured_model_version_not_applied_is_reported() {
            let mut client = new_client_from_default_config().await.unwrap();
            let store = store_with_custom_model(
                &mut client,
                super::model_json(V4_CURRENT_MODEL_JSON),
                AuthorizationModelVersion::new(9, 1),
            )
            .await;

            let err = get_auth_model_id(
                &mut client,
                &store,
                CUSTOM_PREFIX,
                Some(AuthorizationModelVersion::new(9, 0)),
            )
            .await
            .unwrap_err();
            assert!(
                matches!(&err, OpenFGAError::ActiveAuthModelNotFound(v) if v == "9.0"),
                "unexpected error: {err:?}"
            );
        }
    }
}
