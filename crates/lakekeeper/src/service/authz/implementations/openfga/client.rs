//! Get `OpenFGA` clients

use openfga_client::client::{
    BasicOpenFgaClient, BasicOpenFgaServiceClient, ConsistencyPreference,
};

use super::{OpenFGAAuthorizer, OpenFGAError, OpenFGAResult, AUTH_CONFIG};
use crate::{
    service::authz::implementations::openfga::migration::get_active_auth_model_id, OpenFGAAuth,
};

pub type UnauthenticatedOpenFGAAuthorizer = OpenFGAAuthorizer;
pub type BearerOpenFGAAuthorizer = OpenFGAAuthorizer;
pub type ClientCredentialsOpenFGAAuthorizer = OpenFGAAuthorizer;

pub(crate) async fn new_client_from_config() -> OpenFGAResult<BasicOpenFgaServiceClient> {
    let endpoint = AUTH_CONFIG.endpoint.clone();

    let client = match &AUTH_CONFIG.auth {
        OpenFGAAuth::Anonymous => {
            tracing::info!("Building OpenFGA Client without Authorization.");
            BasicOpenFgaServiceClient::new_unauthenticated(endpoint)
        }
        OpenFGAAuth::ClientCredentials {
            client_id,
            client_secret,
            token_endpoint,
            scope,
        } => {
            let scopes = if let Some(scope) = scope {
                scope.split(' ').collect::<Vec<_>>()
            } else {
                vec![]
            };
            tracing::info!("Building OpenFGA Client with Client Credential Authorization. Token Endpoint: {token_endpoint}, Client ID: {client_id}, Scopes: {scopes:?}");
            BasicOpenFgaServiceClient::new_with_client_credentials(
                endpoint,
                client_id,
                client_secret,
                token_endpoint.clone(),
                &scopes,
            )
            .await
        }
        OpenFGAAuth::ApiKey(k) => {
            tracing::info!("Building OpenFGA Client with API Key Authorization.");
            BasicOpenFgaServiceClient::new_with_basic_auth(endpoint, k.as_str())
        }
    };

    Ok(client?)
}

/// Create a new `OpenFGA` authorizer from the configuration.
///
/// # Errors
/// - Server connection fails
/// - Store (name) not found (from crate Config)
/// - Active Authorization model not found
pub(crate) async fn new_authorizer_from_config(
    server_id: uuid::Uuid,
) -> OpenFGAResult<OpenFGAAuthorizer> {
    let client = new_client_from_config().await?;
    new_authorizer(
        client,
        None,
        ConsistencyPreference::MinimizeLatency,
        server_id,
    )
    .await
}

/// Create a new `OpenFGA` authorizer with the given client.
/// This must be run after migration.
///
/// # Errors
/// - Store does not exist
/// - Active Authorization model not found
/// - Server connection fails
///
pub(crate) async fn new_authorizer(
    mut service_client: BasicOpenFgaServiceClient,
    store_name: Option<String>,
    consistency: ConsistencyPreference,
    server_id: uuid::Uuid,
) -> OpenFGAResult<OpenFGAAuthorizer> {
    let store_name = store_name.unwrap_or(AUTH_CONFIG.store_name.clone());
    let auth_model_id =
        get_active_auth_model_id(&mut service_client, Some(store_name.clone())).await?;
    let store = service_client
        .get_store_by_name(&store_name)
        .await?
        .ok_or_else(|| OpenFGAError::StoreNotFound(store_name.clone()))?;

    let client = BasicOpenFgaClient::new(service_client, &store.id, &auth_model_id)
        .set_consistency(consistency);

    Ok(OpenFGAAuthorizer::new(client, server_id))
}
