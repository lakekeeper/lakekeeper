use crate::api;
use crate::service::authn::verification::Verifier;
use crate::service::AuthDetails;
use iceberg_ext::catalog::rest::ErrorModel;
use k8s_openapi::api::authentication::v1::{TokenReview, TokenReviewSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::PostParams;

#[derive(Clone)]
pub struct K8sVerifier {
    client: kube::client::Client,
}

#[async_trait::async_trait]
impl Verifier for K8sVerifier {
    async fn decode(&self, token: &str) -> api::Result<AuthDetails, ErrorModel> {
        K8sVerifier::decode(self, token).await
    }

    fn typ(&self) -> &'static str {
        "kubernetes"
    }
}

impl std::fmt::Debug for K8sVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("K8sVerifier").finish()
    }
}

impl K8sVerifier {
    /// Create a new k8s verifier
    ///
    /// Assumes that the openid configuration is at the default k8s url
    ///
    /// # Errors
    /// - If the openid configuration cannot be fetched or parsed at the default k8s url (<https://kubernetes.default.svc.cluster.local>)
    pub async fn try_new() -> anyhow::Result<Self> {
        let k = kube::client::Client::try_default()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create k8s client: {e}"))?;

        Ok(Self { client: k })
    }

    async fn decode(&self, token: &str) -> api::Result<AuthDetails, ErrorModel> {
        let kapi = kube::api::Api::all(self.client.clone());
        let review = kapi
            .create(
                &PostParams::default(),
                &TokenReview {
                    metadata: ObjectMeta::default(),
                    spec: TokenReviewSpec {
                        audiences: None,
                        token: Some(token.to_string()),
                    },
                    status: None,
                },
            )
            .await
            .map_err(|e| {
                ErrorModel::internal(
                    "Failed to send token to token-review endpoint",
                    "InternalServerError",
                    Some(Box::new(e)),
                )
            })?;
        AuthDetails::try_from_kubernetes_review(review.status).map_err(|e| e.error)
    }
}
