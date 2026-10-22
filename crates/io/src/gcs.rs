mod gcs_access_token;
mod gcs_auth;
mod gcs_error;
mod gcs_location;
mod gcs_storage;
mod gcs_writer;

use std::{
    sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError},
    time::Duration,
};

pub use gcs_access_token::{GcsAccessToken, TokenEndpointError};
pub use gcs_auth::{
    GcsAccessTokenError, GcsAuth, GcsBearerTokenAuth, GcsServiceAccountKey,
    system_identity_project_id,
};
pub use gcs_location::{GcsLocation, InvalidGCSBucketName, validate_bucket_name};
pub use gcs_storage::GcsStorage;
use google_cloud_auth::credentials::Credentials;
use google_cloud_gax::retry_policy::RetryPolicyExt as _;
use google_cloud_storage::{
    client::{Storage, StorageControl},
    read_resume_policy::{ReadResumePolicyExt as _, Recommended},
    retry_policy::RetryableErrors,
};
use tokio::time::Instant;

use crate::InitializeClientError;

/// Objects of at least this size are uploaded with a resumable upload.
pub(crate) const RESUMABLE_UPLOAD_THRESHOLD: usize = 25 * 1024 * 1024;
/// Size of each chunk sent in a resumable upload and of each parallel ranged read.
pub(crate) const CHUNK_SIZE: usize = 16 * 1024 * 1024;
/// Attempts per request, including the first one.
const MAX_ATTEMPTS: u32 = 4;
/// Retries of a request stop after this time. It also bounds retries the SDK throttles, which
/// do not count as attempts.
const RETRY_TIME_LIMIT: Duration = Duration::from_mins(3);
/// Retries of an upload stop after this time. One retry budget covers all chunks of a
/// resumable upload, so it is time-based.
pub(crate) const UPLOAD_RETRY_TIME_LIMIT: Duration = Duration::from_mins(30);
/// Limit for a whole request, including its retries and the download of the response. The
/// SDK has no connect timeout, and waiting for a free gRPC stream is not part of its attempt
/// timeout.
pub(crate) const REQUEST_TIMEOUT: Duration =
    RETRY_TIME_LIMIT.saturating_add(Duration::from_mins(1));
/// Time limit of one attempt of a metadata request (get, list, delete).
const CONTROL_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(60);
/// Clients are rebuilt after this time, so credentials that start failing or a rotated
/// credentials file are picked up. It is below the idle timeout of Cloud NAT (20 min): the
/// gRPC connection has no keepalive, so a connection dropped by a NAT would not be noticed.
const CLIENT_MAX_AGE: Duration = Duration::from_mins(15);
/// A metadata client that was not used for this time is replaced, so its gRPC connection,
/// which a NAT may have dropped, is not reused. It covers [`GcsStorage`] values that callers
/// hold for longer than [`CLIENT_MAX_AGE`].
const CONTROL_MAX_IDLE: Duration = Duration::from_mins(10);
const MAX_CACHED_CLIENTS: u64 = 1000;

/// Clients per credential. Each client keeps connection pools and a background task that
/// refreshes its token, so they are shared between requests.
static CLIENT_CACHE: LazyLock<moka::future::Cache<String, GcsStorage>> = LazyLock::new(|| {
    moka::future::Cache::builder()
        .max_capacity(MAX_CACHED_CLIENTS)
        .time_to_live(CLIENT_MAX_AGE)
        .build()
});

#[derive(Debug, Eq, Clone, PartialEq, typed_builder::TypedBuilder)]
pub struct GCSSettings {}

impl GCSSettings {
    /// Create a new GCS client with the provided authentication method.
    ///
    /// # Errors
    /// If the client cannot be initialized, an `InitializeClientError` is returned.
    pub async fn get_storage_client(
        &self,
        auth: &GcsAuth,
    ) -> Result<GcsStorage, InitializeClientError> {
        // The SDK builders produce large futures; boxing keeps the callers' futures small.
        let Some(cache_key) = client_cache_key(auth) else {
            return Box::pin(build_clients(auth.credentials()?, None)).await;
        };
        Box::pin(CLIENT_CACHE.try_get_with(cache_key, async {
            Box::pin(build_clients(auth.credentials()?, None)).await
        }))
        .await
        .map_err(|e: Arc<InitializeClientError>| InitializeClientError {
            reason: e.reason.clone(),
            source: Some(Box::new(e)),
        })
    }
}

/// Clients for bearer tokens are not cached: those tokens are short-lived and used once.
fn client_cache_key(auth: &GcsAuth) -> Option<String> {
    match auth {
        GcsAuth::ServiceAccountKey { key } => Some(format!("service-account:{}", key.json())),
        GcsAuth::GcpSystemIdentity {} => Some("system-identity".to_string()),
        GcsAuth::BearerToken(_) => None,
    }
}

/// Builds the data client (HTTP) and the metadata client (gRPC) of a [`GcsStorage`].
///
/// The credentials start fetching their token in the background. Object storage connections
/// are opened on first use.
pub(crate) async fn build_clients(
    credentials: Credentials,
    endpoint: Option<&str>,
) -> Result<GcsStorage, InitializeClientError> {
    let mut storage_builder = Storage::builder()
        .with_credentials(credentials.clone())
        .with_retry_policy(
            RetryableErrors
                .with_attempt_limit(MAX_ATTEMPTS)
                .with_time_limit(RETRY_TIME_LIMIT),
        )
        .with_read_resume_policy(Recommended.with_attempt_limit(MAX_ATTEMPTS))
        .with_resumable_upload_threshold(RESUMABLE_UPLOAD_THRESHOLD)
        .with_resumable_upload_buffer_size(CHUNK_SIZE);
    if let Some(endpoint) = endpoint {
        storage_builder = storage_builder.with_endpoint(endpoint);
    }
    let storage = storage_builder
        .build()
        .await
        .map_err(|e| InitializeClientError {
            reason: "Failed to build the GCS storage client".to_string(),
            source: Some(Box::new(e)),
        })?;

    let control = build_control(&credentials, endpoint).await?;
    Ok(GcsStorage::new(
        storage,
        ControlClient::new(control, credentials, endpoint.map(str::to_string)),
    ))
}

async fn build_control(
    credentials: &Credentials,
    endpoint: Option<&str>,
) -> Result<StorageControl, InitializeClientError> {
    let mut builder = StorageControl::builder()
        .with_credentials(credentials.clone())
        .with_retry_policy(
            RetryableErrors
                .with_attempt_limit(MAX_ATTEMPTS)
                .with_time_limit(RETRY_TIME_LIMIT),
        )
        .with_attempt_timeout(CONTROL_ATTEMPT_TIMEOUT);
    if let Some(endpoint) = endpoint {
        builder = builder.with_endpoint(endpoint);
    }
    builder.build().await.map_err(|e| InitializeClientError {
        reason: "Failed to build the GCS storage control client".to_string(),
        source: Some(Box::new(e)),
    })
}

/// Metadata client that is replaced after it was idle for [`CONTROL_MAX_IDLE`].
pub(crate) struct ControlClient {
    credentials: Credentials,
    endpoint: Option<String>,
    /// The client and the time it was last handed out.
    current: Mutex<(Arc<StorageControl>, Instant)>,
}

impl ControlClient {
    fn new(control: StorageControl, credentials: Credentials, endpoint: Option<String>) -> Self {
        Self {
            credentials,
            endpoint,
            current: Mutex::new((Arc::new(control), Instant::now())),
        }
    }

    /// Returns the current client, or a new one if the current one was idle for longer than
    /// [`CONTROL_MAX_IDLE`]. Concurrent callers may each build a client; the last one is kept.
    /// If no client can be built, the current one is returned.
    pub(crate) async fn get(&self) -> Arc<StorageControl> {
        {
            let mut current = self.lock();
            let now = Instant::now();
            if now.duration_since(current.1) <= CONTROL_MAX_IDLE {
                current.1 = now;
                return current.0.clone();
            }
        }
        let rebuilt = Box::pin(build_control(&self.credentials, self.endpoint.as_deref())).await;
        let mut current = self.lock();
        match rebuilt {
            Ok(control) => current.0 = Arc::new(control),
            Err(error) => {
                tracing::warn!(%error, "Keeping the idle GCS storage control client");
            }
        }
        current.1 = Instant::now();
        current.0.clone()
    }

    fn lock(&self) -> MutexGuard<'_, (Arc<StorageControl>, Instant)> {
        // The guarded section cannot panic, so a poisoned lock holds consistent data.
        self.current.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use google_cloud_auth::credentials::anonymous;

    use super::*;

    fn service_account(private_key: &str) -> GcsAuth {
        GcsAuth::ServiceAccountKey {
            key: GcsServiceAccountKey::new(serde_json::json!({
                "type": "service_account",
                "client_email": "example-sa@example-project-0000.iam.gserviceaccount.com",
                "private_key": private_key,
            })),
        }
    }

    #[test]
    fn bearer_token_clients_are_not_cached() {
        let auth = GcsAuth::BearerToken(GcsBearerTokenAuth {
            access_token: "tok".to_string(),
        });
        assert_eq!(client_cache_key(&auth), None);
    }

    #[test]
    fn system_identity_clients_share_one_cache_entry() {
        assert_eq!(
            client_cache_key(&GcsAuth::GcpSystemIdentity {}),
            Some("system-identity".to_string())
        );
    }

    #[test]
    fn service_account_clients_are_cached_per_key() {
        assert_eq!(
            client_cache_key(&service_account("key-a")),
            client_cache_key(&service_account("key-a"))
        );
        assert_ne!(
            client_cache_key(&service_account("key-a")),
            client_cache_key(&service_account("key-b"))
        );
    }

    const ENDPOINT: &str = "https://storage.googleapis.com";

    // tonic sets up TLS for https endpoints while the client is built. With rustls compiled
    // with both ring and aws-lc-rs, this panics unless tonic's `tls-aws-lc` feature is active.
    #[tokio::test]
    async fn build_clients_https_does_not_panic() {
        build_clients(anonymous::Builder::new().build(), Some(ENDPOINT))
            .await
            .unwrap();
    }

    async fn control_client(rebuild_endpoint: &str) -> ControlClient {
        let credentials = anonymous::Builder::new().build();
        let control = build_control(&credentials, Some(ENDPOINT)).await.unwrap();
        ControlClient::new(control, credentials, Some(rebuild_endpoint.to_string()))
    }

    #[tokio::test(start_paused = true)]
    async fn control_client_in_use_is_kept() {
        let client = control_client(ENDPOINT).await;
        let first = client.get().await;
        tokio::time::advance(CONTROL_MAX_IDLE).await;
        let second = client.get().await;
        tokio::time::advance(CONTROL_MAX_IDLE).await;
        let third = client.get().await;
        assert!(Arc::ptr_eq(&first, &second));
        assert!(Arc::ptr_eq(&first, &third));
    }

    #[tokio::test(start_paused = true)]
    async fn idle_control_client_is_replaced() {
        let client = control_client(ENDPOINT).await;
        let first = client.get().await;
        tokio::time::advance(CONTROL_MAX_IDLE + Duration::from_secs(1)).await;
        let second = client.get().await;
        let third = client.get().await;
        assert!(!Arc::ptr_eq(&first, &second));
        assert!(Arc::ptr_eq(&second, &third));
    }

    #[tokio::test(start_paused = true)]
    async fn idle_control_client_is_kept_if_rebuild_fails() {
        let client = control_client("not a valid endpoint").await;
        let first = client.get().await;
        tokio::time::advance(CONTROL_MAX_IDLE + Duration::from_secs(1)).await;
        let second = client.get().await;
        assert!(Arc::ptr_eq(&first, &second));
    }
}
