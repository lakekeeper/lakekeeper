use std::{fmt::Debug, sync::Arc};

use axum::{
    extract::{Request, State},
    middleware::Next,
    response::{IntoResponse, Response},
    Extension,
};
use axum_extra::{
    headers::{authorization::Bearer, Authorization},
    TypedHeader,
};
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};

use crate::{api, request_metadata::RequestMetadata, service::AuthDetails};

mod idp;
mod kubernetes;

pub use idp::IdpVerifier;
pub use kubernetes::K8sVerifier;

/// `VerifierChain` chains idp and k8s verifier.
#[derive(Clone, Debug)]
pub(crate) struct VerifierChain {
    idp_verifier: Option<IdpVerifier>,
    k8s_verifier: Option<K8sVerifier>,
}

impl VerifierChain {
    /// Create a new verifier chain with the idp and k8s verifier
    ///
    /// You must provide at least one verifier. The authentication middleware will first try to
    /// decode the token using the idp provider and then the k8s provider.
    ///
    /// # Errors
    /// - If neither `idp_verifier` nor `k8s_verifier` is provided
    pub(crate) fn try_new(
        idp_verifier: Option<IdpVerifier>,
        k8s_verifier: Option<K8sVerifier>,
    ) -> anyhow::Result<Self> {
        if idp_verifier.is_none() && k8s_verifier.is_none() {
            return Err(anyhow::anyhow!("At least one verifier must be provided"));
        }
        Ok(Self {
            idp_verifier,
            k8s_verifier,
        })
    }

    fn into_vec(self) -> Vec<Arc<dyn Verifier + Send + Sync>> {
        let mut verifiers = vec![];
        if let Some(verifier) = self.idp_verifier {
            verifiers.push(Arc::new(verifier) as _);
        }
        if let Some(k8s) = self.k8s_verifier {
            verifiers.push(Arc::new(k8s) as _);
        }
        verifiers
    }
}

pub(crate) async fn auth_middleware_fn(
    State(verifiers): State<VerifierChain>,
    authorization: Option<TypedHeader<Authorization<Bearer>>>,
    Extension(mut metadata): Extension<RequestMetadata>,
    mut request: Request,
    next: Next,
) -> Response {
    let Some(authorization) = authorization else {
        tracing::debug!("Missing authorization header");
        return IcebergErrorResponse::from(ErrorModel::unauthorized(
            "Missing authorization header.",
            "UnauthorizedError",
            None,
        ))
        .into_response();
    };

    for verifier in verifiers.into_vec() {
        let Ok(details) = verifier.decode(authorization.token()).await.map_err(|e| {
            tracing::debug!(
                ?e,
                "Failed to decode token with verifier: '{}' due to: '{e}'",
                verifier.typ(),
            );
        }) else {
            continue;
        };

        metadata.auth_details = details;
        request.extensions_mut().insert(metadata);
        return next.run(request).await;
    }
    tracing::info!("No verifier could decode the token");
    IcebergErrorResponse::from(ErrorModel::unauthorized(
        "Unauthorized",
        "UnauthorizedError",
        None,
    ))
    .into_response()
}

#[async_trait::async_trait]
trait Verifier: Debug {
    async fn decode(&self, token: &str) -> api::Result<AuthDetails, ErrorModel>;
    fn typ(&self) -> &str;
}
