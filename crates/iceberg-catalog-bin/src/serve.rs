use anyhow::{anyhow, Error};
use iceberg_catalog::api::router::{new_full_router, serve as service_serve, RouterArgs};
use iceberg_catalog::implementations::postgres::{CatalogState, PostgresCatalog, ReadWrite};
use iceberg_catalog::implementations::Secrets;
use iceberg_catalog::service::authz::implementations::{
    get_default_authorizer_from_config, Authorizers,
};
use iceberg_catalog::service::authz::Authorizer;
use iceberg_catalog::service::contract_verification::ContractVerifiers;
use iceberg_catalog::service::event_publisher::{
    CloudEventBackend, CloudEventsPublisher, CloudEventsPublisherBackgroundTask, Message,
    NatsBackend,
};
use iceberg_catalog::service::health::ServiceHealthProvider;
use iceberg_catalog::service::{Catalog, StartupValidationData};
use iceberg_catalog::{SecretBackend, CONFIG};
use reqwest::Url;

use iceberg_catalog::implementations::postgres::task_queues::{
    TabularExpirationQueue, TabularPurgeQueue,
};
use iceberg_catalog::service::authn::IdpVerifier;
use iceberg_catalog::service::authn::K8sVerifier;
use iceberg_catalog::service::task_queue::TaskQueues;
use std::sync::Arc;

pub(crate) async fn serve(bind_addr: std::net::SocketAddr) -> Result<(), anyhow::Error> {
    let read_pool =
        iceberg_catalog::implementations::postgres::get_reader_pool(CONFIG.to_pool_opts()).await?;
    let write_pool =
        iceberg_catalog::implementations::postgres::get_writer_pool(CONFIG.to_pool_opts()).await?;

    let catalog_state = CatalogState::from_pools(read_pool.clone(), write_pool.clone());

    let validation_data = PostgresCatalog::get_server_info(catalog_state.clone()).await?;
    match validation_data {
        StartupValidationData::NotBootstrapped => {
            tracing::info!("The catalog is not bootstrapped. Bootstrapping sets the initial administrator. Please open the Web-UI after startup or call the bootstrap endpoint directly.");
        }
        StartupValidationData::Bootstrapped {
            server_id,
            terms_accepted,
        } => {
            if !terms_accepted {
                return Err(anyhow!(
                    "The terms of service have not been accepted on bootstrap."
                ));
            }
            if server_id != CONFIG.server_id {
                return Err(anyhow!(
                    "The server ID during bootstrap {} does not match the server ID in the configuration {}.", server_id, CONFIG.server_id
                ));
            }
            tracing::info!("The catalog is bootstrapped. Server ID: {server_id}");
        }
    }

    let secrets_state: Secrets = match CONFIG.secret_backend {
        SecretBackend::KV2 => iceberg_catalog::implementations::kv2::SecretsState::from_config(
            CONFIG
                .kv2
                .as_ref()
                .ok_or_else(|| anyhow!("Need vault config to use vault as backend"))?,
        )
        .await?
        .into(),
        SecretBackend::Postgres => {
            iceberg_catalog::implementations::postgres::SecretsState::from_pools(
                read_pool.clone(),
                write_pool.clone(),
            )
            .into()
        }
    };
    let authorizer = get_default_authorizer_from_config().await?;

    let health_provider = ServiceHealthProvider::new(
        vec![
            ("catalog", Arc::new(catalog_state.clone())),
            ("secrets", Arc::new(secrets_state.clone())),
            ("auth", Arc::new(authorizer.clone())),
        ],
        CONFIG.health_check_frequency_seconds,
        CONFIG.health_check_jitter_millis,
    );
    health_provider.spawn_health_checks().await;

    let queues = TaskQueues::new(
        Arc::new(TabularExpirationQueue::from_config(
            ReadWrite::from_pools(read_pool.clone(), write_pool.clone()),
            CONFIG.queue_config.clone(),
        )?),
        Arc::new(TabularPurgeQueue::from_config(
            ReadWrite::from_pools(read_pool.clone(), write_pool.clone()),
            CONFIG.queue_config.clone(),
        )?),
    );

    let listener = tokio::net::TcpListener::bind(bind_addr).await?;

    match authorizer {
        Authorizers::AllowAll(a) => {
            serve_inner(
                a,
                catalog_state,
                secrets_state,
                queues,
                health_provider,
                listener,
            )
            .await?
        }
        Authorizers::OpenFGA(a) => {
            serve_inner(
                a,
                catalog_state,
                secrets_state,
                queues,
                health_provider,
                listener,
            )
            .await?
        }
    }

    Ok(())
}

/// Helper function to remove redundant code from matching different implementations
async fn serve_inner<A: Authorizer>(
    authorizer: A,
    catalog_state: CatalogState,
    secrets_state: Secrets,
    queues: TaskQueues,
    health_provider: ServiceHealthProvider,
    listener: tokio::net::TcpListener,
) -> Result<(), anyhow::Error> {
    let (tx, rx) = tokio::sync::mpsc::channel(1000);

    let mut cloud_event_sinks = vec![];

    if let Some(nat_addr) = &CONFIG.nats_address {
        let nats_publisher = build_nats_client(nat_addr).await?;
        cloud_event_sinks
            .push(Arc::new(nats_publisher) as Arc<dyn CloudEventBackend + Sync + Send>);
    } else {
        tracing::info!("Running without publisher.");
    };

    let x: CloudEventsPublisherBackgroundTask = CloudEventsPublisherBackgroundTask {
        source: rx,
        sinks: cloud_event_sinks,
    };

    let k8s_token_verifier = K8sVerifier::try_new()
        .await
        .map_err(|e| {
            tracing::info!(
                "Failed to create K8s authorizer: {e}, assuming we are not running on kubernetes."
            )
        })
        .ok();

    let router = new_full_router::<PostgresCatalog, _, Secrets>(RouterArgs {
        authorizer: authorizer.clone(),
        catalog_state: catalog_state.clone(),
        secrets_state: secrets_state.clone(),
        queues: queues.clone(),
        publisher: CloudEventsPublisher::new(tx.clone()),
        table_change_checkers: ContractVerifiers::new(vec![]),
        token_verifier: if let Some(uri) = CONFIG.openid_provider_uri.clone() {
            Some(IdpVerifier::new(uri).await?)
        } else {
            None
        },
        k8s_token_verifier,
        service_health_provider: health_provider,
        cors_origins: CONFIG.allow_origin.as_deref(),
        metrics_layer: Some(
            iceberg_catalog::metrics::get_axum_layer_and_install_recorder(CONFIG.metrics_port)?,
        ),
    })?;

    let publisher_handle = tokio::task::spawn(async move {
        match x.publish().await {
            Ok(_) => tracing::info!("Exiting publisher task"),
            Err(e) => tracing::error!("Publisher task failed: {e}"),
        };
    });

    tokio::select!(
        _ = queues.spawn_queues::<PostgresCatalog, _, _>(catalog_state, secrets_state, authorizer) => tracing::error!("Tabular queue task failed"),
        err = service_serve(listener, router) => tracing::error!("Service failed: {err:?}"),
    );

    tracing::debug!("Sending shutdown signal to event publisher.");
    tx.send(Message::Shutdown).await?;
    publisher_handle.await?;

    Ok(())
}

async fn build_nats_client(nat_addr: &Url) -> Result<NatsBackend, Error> {
    tracing::info!("Running with nats publisher, connecting to: {nat_addr}");
    let builder = async_nats::ConnectOptions::new();

    let builder = if let Some(file) = &CONFIG.nats_creds_file {
        builder.credentials_file(file).await?
    } else {
        builder
    };

    let builder = if let (Some(user), Some(pw)) = (&CONFIG.nats_user, &CONFIG.nats_password) {
        builder.user_and_password(user.clone(), pw.clone())
    } else {
        builder
    };

    let builder = if let Some(token) = &CONFIG.nats_token {
        builder.token(token.clone())
    } else {
        builder
    };

    let nats_publisher = NatsBackend {
        client: builder.connect(nat_addr.to_string()).await?,
        topic: CONFIG
            .nats_topic
            .clone()
            .ok_or(anyhow::anyhow!("Missing nats topic."))?,
    };
    Ok(nats_publisher)
}
