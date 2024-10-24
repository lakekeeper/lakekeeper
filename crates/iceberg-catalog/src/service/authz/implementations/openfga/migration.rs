use super::{ClientHelper, OpenFGAError, OpenFGAResult, AUTH_CONFIG};
use crate::service::authz::implementations::openfga::client::ClientConnection;
use crate::service::authz::implementations::openfga::ModelVersion;
use crate::service::authz::implementations::FgaType;
use openfga_rs::open_fga_service_client::OpenFgaServiceClient;
use openfga_rs::{ReadRequestTupleKey, Tuple, TupleKey, WriteRequest, WriteRequestWrites};
use std::collections::{HashMap, HashSet};

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
    let active_model = ModelVersion::active();

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
            "Applying OpenFGA Migration: Migrating from {max_applied} to {active_model}"
        );
        match max_applied {
            ModelVersion::V1 => Ok(()),
        }
    } else {
        tracing::info!("No authorization models found. Applying active model version.");

        let active_int = active_model.as_monotonic_int();
        let model = active_model.get_model();
        let auth_model_id = client
            .write_authorization_model(model.into_write_request(store.id.clone()))
            .await
            .map_err(OpenFGAError::write_authorization_model)?
            .into_inner()
            .authorization_model_id;

        let write_request = WriteRequest {
            store_id: store.id.clone(),
            writes: Some(WriteRequestWrites {
                tuple_keys: vec![
                    TupleKey {
                        user: format!("{AUTH_MODEL_ID_TYPE}:{auth_model_id}"),
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
            authorization_model_id: auth_model_id,
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
    applied_models: &HashSet<i32>,
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

fn parse_existing_models(models: impl IntoIterator<Item = Tuple>) -> OpenFGAResult<HashSet<i32>> {
    // Make sure each string starts with "<MODELVERSION_TYPE>:".
    // Then strip that prefix and parse the rest as an integer.
    // Only the object is relevant, user is always "auth_model_id:*".

    models
        .into_iter()
        .filter_map(|t| t.key)
        .map(|t| parse_model_version_from_str(&t.object))
        .collect()
}

fn parse_model_version_from_str(model: &str) -> OpenFGAResult<i32> {
    model
        .strip_prefix(&format!("{MODEL_VERSION_TYPE}:"))
        .ok_or(OpenFGAError::unexpected_entity(
            vec![FgaType::ModelVersion],
            model.to_string(),
        ))
        .and_then(|version| {
            version.parse::<i32>().map_err(|_e| {
                OpenFGAError::unexpected_entity(vec![FgaType::ModelVersion], model.to_string())
            })
        })
}

fn parse_applied_model_versions(
    models: impl IntoIterator<Item = Tuple>,
) -> OpenFGAResult<HashMap<i32, String>> {
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
pub(crate) mod tests {
    use super::super::client::new_authorizer;
    use super::super::OpenFGAAuthorizer;
    use super::*;
    use crate::service::authz::implementations::openfga::client::ClientConnection;
    use crate::service::authz::implementations::openfga::new_client_from_config;
    use needs_env_var::needs_env_var;

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
