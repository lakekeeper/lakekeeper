use std::{
    error::Error as StdError,
    fmt::{Display, Formatter},
};

use http::StatusCode;
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};

use crate::service::{error_chain_fmt, impl_error_stack_methods};

#[derive(Debug, PartialEq, derive_more::From)]
pub enum BackendUnavailableOrCountMismatch {
    AuthorizationCountMismatch(AuthorizationCountMismatch),
    AuthorizationBackendUnavailable(AuthorizationBackendUnavailable),
}

impl From<BackendUnavailableOrCountMismatch> for ErrorModel {
    fn from(err: BackendUnavailableOrCountMismatch) -> Self {
        match err {
            BackendUnavailableOrCountMismatch::AuthorizationCountMismatch(e) => e.into(),
            BackendUnavailableOrCountMismatch::AuthorizationBackendUnavailable(e) => e.into(),
        }
    }
}
impl From<BackendUnavailableOrCountMismatch> for IcebergErrorResponse {
    fn from(err: BackendUnavailableOrCountMismatch) -> Self {
        ErrorModel::from(err).into()
    }
}

#[derive(Debug, PartialEq)]
pub struct AuthorizationCountMismatch {
    pub expected_authorizations: usize,
    pub actual_authorizations: usize,
    pub type_name: String,
}

impl AuthorizationCountMismatch {
    #[must_use]
    pub fn new(
        expected_authorizations: usize,
        actual_authorizations: usize,
        type_name: &str,
    ) -> Self {
        Self {
            expected_authorizations,
            actual_authorizations,
            type_name: type_name.to_string(),
        }
    }
}

impl From<AuthorizationCountMismatch> for ErrorModel {
    fn from(err: AuthorizationCountMismatch) -> Self {
        let AuthorizationCountMismatch {
            expected_authorizations,
            actual_authorizations,
            type_name,
        } = err;

        tracing::error!("Authorization count mismatch for {type_name} batch check: expected {expected_authorizations}, got {actual_authorizations}.");

        ErrorModel {
            r#type: "AuthorizationCountMismatch".to_string(),
            code: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
            message: "Authorization service returned invalid response".to_string(),
            source: None,
            stack: vec![],
        }
    }
}
impl From<AuthorizationCountMismatch> for IcebergErrorResponse {
    fn from(err: AuthorizationCountMismatch) -> Self {
        ErrorModel::from(err).into()
    }
}

#[derive(Debug)]
pub struct AuthorizationBackendUnavailable {
    pub stack: Vec<String>,
    pub source: Box<dyn std::error::Error + Send + Sync + 'static>,
}

impl_error_stack_methods!(AuthorizationBackendUnavailable);

impl PartialEq for AuthorizationBackendUnavailable {
    fn eq(&self, other: &Self) -> bool {
        self.stack == other.stack && self.source.to_string() == other.source.to_string()
    }
}

impl AuthorizationBackendUnavailable {
    pub fn new<E>(source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self {
            stack: Vec::new(),
            source: Box::new(source),
        }
    }
}

impl StdError for AuthorizationBackendUnavailable {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&*self.source as &(dyn StdError + 'static))
    }
}

impl Display for AuthorizationBackendUnavailable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "AuthorizationBackendError: {}", self.source)?;

        if !self.stack.is_empty() {
            writeln!(f, "Stack:")?;
            for detail in &self.stack {
                writeln!(f, "  {detail}")?;
            }
        }

        if let Some(source) = self.source.source() {
            writeln!(f, "Caused by:")?;
            // Dereference `source` to get `dyn StdError` and then take a reference to pass
            error_chain_fmt(source, f)?;
        }

        Ok(())
    }
}

impl From<AuthorizationBackendUnavailable> for ErrorModel {
    fn from(err: AuthorizationBackendUnavailable) -> Self {
        let AuthorizationBackendUnavailable { stack, source } = err;

        tracing::error!("Authorization backend error: {source}");

        ErrorModel {
            r#type: "AuthorizationBackendError".to_string(),
            code: StatusCode::SERVICE_UNAVAILABLE.as_u16(),
            message: "Authorization service is unavailable".to_string(),
            stack,
            source: None,
        }
    }
}
impl From<AuthorizationBackendUnavailable> for IcebergErrorResponse {
    fn from(err: AuthorizationBackendUnavailable) -> Self {
        ErrorModel::from(err).into()
    }
}
