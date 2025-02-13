mod v2;

use std::collections::{HashMap, HashSet};

use openfga_rs::{
    open_fga_service_client::OpenFgaServiceClient, ReadRequestTupleKey, Store, Tuple, TupleKey,
    WriteRequest, WriteRequestWrites,
};

use super::{ClientHelper, OpenFGAError, OpenFGAResult, AUTH_CONFIG};
use crate::service::authz::implementations::{
    openfga::{client::ClientConnection, ModelVersion},
    FgaType,
};

const AUTH_MODEL_ID_TYPE: &FgaType = &FgaType::AuthModelId;
const MODEL_VERSION_TYPE: &FgaType = &FgaType::ModelVersion;
const MODEL_VERSION_APPLIED_RELATION: &str = "applied";
const MODEL_VERSION_EXISTS_RELATION: &str = "exists";

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
    client: &mut OpenFgaServiceClient<ClientConnection>,
    store_name: Option<String>,
) -> OpenFGAResult<()> {
    let store_name = store_name.unwrap_or(AUTH_CONFIG.store_name.clone());
    let store = client.get_or_create_store(&store_name).await?;

    let existing_models = parse_existing_models(
        client
            .read_all_pages(
                &store.id,
                ReadRequestTupleKey {
                    user: format!("{AUTH_MODEL_ID_TYPE}:*").to_string(),
                    relation: MODEL_VERSION_EXISTS_RELATION.to_string(),
                    object: format!("{MODEL_VERSION_TYPE}:",).to_string(),
                },
            )
            .await?,
    )?;

    let (migration_needed, max_applied) = determine_migrate_from(&existing_models)?;
    if !migration_needed {
        return Ok(());
    }

    if let Some(max_applied) = max_applied {
        tracing::info!(
            "Applying OpenFGA Migration: Rolling up from {max_applied} to {}",
            ModelVersion::active()
        );
        for int_id in max_applied.as_monotonic_int()..ModelVersion::active().as_monotonic_int() {
            let model_version = ModelVersion::from_monotonic_int(int_id)
                .ok_or(OpenFGAError::UnknownModelVersionApplied(int_id))?;

            tracing::info!("Writing model version {}", model_version);

            let written_model = write_model(client, model_version, &store).await?;

            tracing::info!("Applying migration for model version {}", model_version);
            match model_version {
                ModelVersion::V1 => {
                    // no migration to be done, we start at v1
                }
                ModelVersion::V2 => v2::migrate(client, &written_model.auth_model_id, &store).await,
            }
            tracing::info!("Marking model version {} as applied", model_version);
            mark_as_applied(client, &store, written_model).await?;
        }
    } else {
        tracing::info!("No authorization models found. Applying active model version.");
        let written_model = write_model(client, ModelVersion::active(), &store).await?;
        mark_as_applied(client, &store, written_model).await?;
    }
    tracing::info!("OpenFGA Migration finished");
    Ok(())
}

async fn mark_as_applied(
    client: &mut OpenFgaServiceClient<ClientConnection>,
    store: &Store,
    written_active_model: ModelSpec,
) -> Result<(), OpenFGAError> {
    let active_int = written_active_model.model_version.as_monotonic_int();
    let authorization_model_id = written_active_model.auth_model_id.as_str();

    let write_request = WriteRequest {
        store_id: store.id.clone(),
        writes: Some(WriteRequestWrites {
            tuple_keys: vec![
                TupleKey {
                    user: format!("{AUTH_MODEL_ID_TYPE}:{authorization_model_id}"),
                    relation: MODEL_VERSION_APPLIED_RELATION.to_string(),
                    object: format!("{MODEL_VERSION_TYPE}:{active_int}"),
                    condition: None,
                },
                TupleKey {
                    user: format!("{AUTH_MODEL_ID_TYPE}:*"),
                    relation: MODEL_VERSION_EXISTS_RELATION.to_string(),
                    object: format!("{MODEL_VERSION_TYPE}:{active_int}"),
                    condition: None,
                },
            ],
        }),
        deletes: None,
        authorization_model_id: authorization_model_id.to_string(),
    };
    client
        .write(write_request.clone())
        .await
        .map_err(|e| OpenFGAError::WriteFailed {
            write_request,
            source: e,
        })?;
    Ok(())
}

struct ModelSpec {
    model_version: ModelVersion,
    auth_model_id: String,
}

async fn write_model(
    client: &mut OpenFgaServiceClient<ClientConnection>,
    model_version: ModelVersion,
    store: &Store,
) -> Result<ModelSpec, OpenFGAError> {
    let model = model_version.get_model();
    let auth_model_id = client
        .write_authorization_model(model.into_write_request(store.id.clone()))
        .await
        .map_err(OpenFGAError::write_authorization_model)?
        .into_inner()
        .authorization_model_id;
    Ok(ModelSpec {
        model_version,
        auth_model_id,
    })
}

pub(crate) async fn get_auth_model_id(
    client: &mut OpenFgaServiceClient<ClientConnection>,
    store_id: String,
    model_version: ModelVersion,
) -> OpenFGAResult<String> {
    let active_int = model_version.as_monotonic_int();
    let applied_models = parse_applied_model_versions(
        client
            .read_all_pages(
                &store_id,
                ReadRequestTupleKey {
                    user: String::new(),
                    relation: MODEL_VERSION_APPLIED_RELATION.to_string(),
                    object: format!("{MODEL_VERSION_TYPE}:{active_int}").to_string(),
                },
            )
            .await?,
    )?;

    // Must have exactly one result
    if applied_models.len() != 1 {
        return Err(OpenFGAError::AuthorizationModelIdFailed {
            reason: format!(
                "Expected exactly one auth model id for model version {}, found {}",
                model_version,
                applied_models.len()
            ),
        });
    }

    let auth_model_id =
        applied_models
            .get(&active_int)
            .ok_or(OpenFGAError::AuthorizationModelIdFailed {
                reason: format!("Failed to find auth model id for model version {model_version}"),
            })?;

    tracing::info!("Found auth model id: {}", auth_model_id);
    Ok(auth_model_id.clone())
}

fn determine_migrate_from(
    applied_models: &HashSet<u64>,
) -> OpenFGAResult<(bool, Option<ModelVersion>)> {
    let max_applied = if let Some(max_applied) = applied_models.iter().max() {
        if *max_applied >= ModelVersion::active().as_monotonic_int() {
            tracing::info!(
                "Skipping OpenFGA Migration: Active model version is lower or equal to the highest model version in openfga",
            );
            return Ok((false, None));
        }
        let max_applied = ModelVersion::from_monotonic_int(*max_applied)
            .ok_or(OpenFGAError::UnknownModelVersionApplied(*max_applied))?;
        Some(max_applied)
    } else {
        None
    };

    Ok((true, max_applied))
}

fn parse_existing_models(models: impl IntoIterator<Item = Tuple>) -> OpenFGAResult<HashSet<u64>> {
    // Make sure each string starts with "<MODELVERSION_TYPE>:".
    // Then strip that prefix and parse the rest as an integer.
    // Only the object is relevant, user is always "auth_model_id:*".

    models
        .into_iter()
        .filter_map(|t| t.key)
        .map(|t| parse_model_version_from_str(&t.object))
        .collect()
}

fn parse_model_version_from_str(model: &str) -> OpenFGAResult<u64> {
    model
        .strip_prefix(&format!("{MODEL_VERSION_TYPE}:"))
        .ok_or(OpenFGAError::unexpected_entity(
            vec![FgaType::ModelVersion],
            model.to_string(),
        ))
        .and_then(|version| {
            version.parse::<u64>().map_err(|_e| {
                OpenFGAError::unexpected_entity(vec![FgaType::ModelVersion], model.to_string())
            })
        })
}

fn parse_applied_model_versions(
    models: impl IntoIterator<Item = Tuple>,
) -> OpenFGAResult<HashMap<u64, String>> {
    // Make sure each string starts with "<MODELVERSION_TYPE>:".
    // Then strip that prefix and parse the rest as an integer.

    models
        .into_iter()
        .filter_map(|t| t.key)
        .map(|t| {
            parse_model_version_from_str(&t.object).and_then(|v| {
                t.user
                    .strip_prefix(&format!("{AUTH_MODEL_ID_TYPE}:"))
                    .ok_or(OpenFGAError::unexpected_entity(
                        vec![FgaType::AuthModelId],
                        t.user.clone(),
                    ))
                    .map(|s| (v, s.to_string()))
            })
        })
        .collect()
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) mod tests {
    use needs_env_var::needs_env_var;

    use super::{
        super::{client::new_authorizer, OpenFGAAuthorizer},
        *,
    };
    use crate::service::authz::implementations::openfga::{
        client::ClientConnection, new_client_from_config,
    };

    pub(crate) async fn authorizer_for_empty_store(
    ) -> (OpenFgaServiceClient<ClientConnection>, OpenFGAAuthorizer) {
        let mut client = new_client_from_config().await.unwrap();

        let store_name = format!("test_store_{}", uuid::Uuid::now_v7());
        migrate(&mut client, Some(store_name.clone()))
            .await
            .unwrap();

        let authorizer = new_authorizer(client.clone(), Some(store_name))
            .await
            .unwrap();

        (client, authorizer)
    }

    #[test]
    fn test_parse_applied_model_versions() {
        let expected = HashMap::from_iter(vec![
            (1, "111111".to_string()),
            (2, "222222".to_string()),
            (3, "333".to_string()),
        ]);
        let tuples = vec![
            Tuple {
                key: Some(TupleKey {
                    user: "auth_model_id:111111".to_string(),
                    relation: "applied".to_string(),
                    object: "model_version:1".to_string(),
                    condition: None,
                }),
                timestamp: None,
            },
            Tuple {
                key: Some(TupleKey {
                    user: "auth_model_id:222222".to_string(),
                    relation: "applied".to_string(),
                    object: "model_version:2".to_string(),
                    condition: None,
                }),
                timestamp: None,
            },
            Tuple {
                key: Some(TupleKey {
                    user: "auth_model_id:333".to_string(),
                    relation: "applied".to_string(),
                    object: "model_version:3".to_string(),
                    condition: None,
                }),
                timestamp: None,
            },
        ];

        let parsed = parse_applied_model_versions(tuples).unwrap();
        assert_eq!(parsed, expected);
    }

    #[needs_env_var(TEST_OPENFGA = 1)]
    mod openfga {
        use super::super::*;
        use crate::service::authz::implementations::openfga::new_client_from_config;

        #[tokio::test]
        async fn test_migrate() {
            let mut client = new_client_from_config().await.unwrap();
            let store_name = format!("test_store_{}", uuid::Uuid::now_v7());
            migrate(&mut client, Some(store_name.clone()))
                .await
                .unwrap();
            migrate(&mut client, Some(store_name.clone()))
                .await
                .unwrap();
            migrate(&mut client, Some(store_name.clone()))
                .await
                .unwrap();

            let store = client
                .get_store_by_name(&store_name)
                .await
                .unwrap()
                .unwrap();

            let authz_models = client.get_all_auth_models(store.id.clone()).await.unwrap();
            assert_eq!(authz_models.len(), 1);

            let auth_model_id_search = client
                .get_auth_model_id(store.id.clone(), ModelVersion::active().get_model_ref())
                .await
                .unwrap()
                .unwrap();

            let auth_model_id =
                get_auth_model_id(&mut client, store.id.clone(), ModelVersion::active())
                    .await
                    .unwrap();
            assert_eq!(auth_model_id, auth_model_id_search);
        }
    }
}
