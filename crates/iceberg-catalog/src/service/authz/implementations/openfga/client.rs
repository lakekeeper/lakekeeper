//! Get `OpenFGA` clients

use std::{
    collections::HashMap,
    sync::Arc,
    task::{Context, Poll},
};

use http::{HeaderMap, Request};
use openfga_rs::{
    authentication::{ClientCredentials, RefreshConfiguration},
    open_fga_service_client::OpenFgaServiceClient,
    tonic::{
        body::BoxBody,
        transport::{Channel, Endpoint},
    },
};
use tokio::sync::RwLock;
use tower::ServiceBuilder;

use super::{
    ClientHelper as _, ModelVersion, OpenFGAAuthorizer, OpenFGAError, OpenFGAResult, AUTH_CONFIG,
};
use crate::{
    service::authz::implementations::{openfga::migration::get_auth_model_id, Authorizers},
    OpenFGAAuth,
};

pub type UnauthenticatedOpenFGAAuthorizer = OpenFGAAuthorizer;
pub type BearerOpenFGAAuthorizer = OpenFGAAuthorizer;
pub type ClientCredentialsOpenFGAAuthorizer = OpenFGAAuthorizer;

pub(crate) async fn new_client_from_config(
) -> OpenFGAResult<openfga_rs::open_fga_service_client::OpenFgaServiceClient<ClientConnection>> {
    let endpoint = AUTH_CONFIG.endpoint.clone();

    let either_or_option = match &AUTH_CONFIG.auth {
        OpenFGAAuth::Anonymous => None,
        OpenFGAAuth::ClientCredentials {
            client_id,
            client_secret,
            token_endpoint,
        } => Some(tower::util::Either::Left(
            openfga_rs::tonic::service::interceptor(
                openfga_rs::authentication::ClientCredentialInterceptor::new(
                    ClientCredentials {
                        client_id: client_id.clone(),
                        client_secret: client_secret.clone(),
                        token_endpoint: token_endpoint.clone(),
                        extra_headers: HeaderMap::default(),
                        extra_oauth_params: HashMap::default(),
                    },
                    RefreshConfiguration {
                        max_retry: 10,
                        retry_interval: std::time::Duration::from_millis(5),
                    },
                ),
            ),
        )),
        OpenFGAAuth::ApiKey(k) => Some(tower::util::Either::Right(
            openfga_rs::tonic::service::interceptor(
                openfga_rs::authentication::BearerTokenInterceptor::new(k.as_str()).expect(
                    "BearerTokenInterceptor::new failed. This should not happen as the key is a valid ASCII string",
                ),
            ),
        )),
    };

    let auth_layer = tower::util::option_layer(either_or_option);
    let c = ClientService::new(
        ServiceBuilder::new()
            .layer(auth_layer)
            .service(Endpoint::new(endpoint.to_string())?.connect_lazy()),
    );

    Ok(OpenFgaServiceClient::new(c))
}

/// Create a new `OpenFGA` authorizer from the configuration.
///
/// # Errors
/// - Server connection fails
/// - Store (name) not found (from crate Config)
/// - Active Authorization model not found
pub async fn new_authorizer_from_config() -> OpenFGAResult<Authorizers> {
    let client = new_client_from_config().await?;
    Ok(Authorizers::OpenFGA(new_authorizer(client, None).await?))
}

/// Create a new `OpenFGA` authorizer with the given client.
/// This must be run after migration.
pub(crate) async fn new_authorizer(
    mut client: OpenFgaServiceClient<ClientConnection>,
    store_name: Option<String>,
) -> OpenFGAResult<OpenFGAAuthorizer> {
    let active_model_version = ModelVersion::active();
    let store_name = store_name.unwrap_or_else(|| AUTH_CONFIG.store_name.clone());

    let store_id = client
        .get_store_by_name(&store_name)
        .await?
        .ok_or(OpenFGAError::StoreNotFound { store: store_name })?
        .id;
    let authorization_model_id =
        get_auth_model_id(&mut client, store_id.clone(), active_model_version).await?;

    Ok(OpenFGAAuthorizer {
        client: Arc::new(client),
        store_id,
        authorization_model_id,
        health: Arc::new(RwLock::new(vec![])),
    })
}

pub(crate) type ClientConnection = ClientService<
    tower::util::Either<
        tower::util::Either<
            openfga_rs::tonic::service::interceptor::InterceptedService<
                Channel,
                openfga_rs::authentication::ClientCredentialInterceptor,
            >,
            openfga_rs::tonic::service::interceptor::InterceptedService<
                Channel,
                openfga_rs::authentication::BearerTokenInterceptor,
            >,
        >,
        Channel,
    >,
>;

#[derive(Clone, Debug)]
pub(crate) struct ClientService<S> {
    inner: S,
}

impl<S> ClientService<S>
where
    S: tower::Service<Request<BoxBody>>,
{
    fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S> tower::Service<Request<BoxBody>> for ClientService<S>
where
    S: tower::Service<Request<BoxBody>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<BoxBody>) -> Self::Future {
        self.inner.call(req)
    }
}
