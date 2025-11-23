use std::{
    error::Error as StdError,
    fmt::{Display, Formatter},
};

use http::StatusCode;
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};

// Add this macro near the top of the file, after the imports
macro_rules! impl_error_stack_methods {
    ($error_type:ty) => {
        impl $error_type {
            #[must_use]
            pub fn append_details(mut self, details: impl IntoIterator<Item = String>) -> Self {
                self.stack.extend(details);
                self
            }

            #[must_use]
            pub fn append_detail(mut self, detail: impl Into<String>) -> Self {
                self.stack.push(detail.into());
                self
            }

            pub fn append_details_mut(&mut self, details: impl IntoIterator<Item = String>) {
                self.stack.extend(details);
            }

            pub fn append_detail_mut(&mut self, detail: impl Into<String>) {
                self.stack.push(detail.into());
            }
        }
    };
}

macro_rules! impl_from_with_detail {
    ($from_type:ty => $to_type:ident::$variant:ident, $detail:expr) => {
        impl From<$from_type> for $to_type {
            fn from(err: $from_type) -> Self {
                $to_type::$variant(err.append_detail($detail))
            }
        }
    };
}

/// Defines a fully transparent error enum with automatic conversion implementations.
///
/// This macro generates:
/// - An enum with all variants marked as `#[error(transparent)]`
/// - `impl_from_with_detail!` for each variant to add stack context
/// - `From<ErrorType> for ErrorModel` that delegates to variant conversions
/// - `From<ErrorType> for IcebergErrorResponse` via `ErrorModel`
macro_rules! define_transparent_error {
    (
        $(#[$meta:meta])*
        $vis:vis enum $error_name:ident,
        stack_message: $stack_msg:expr,
        variants: [
            $($variant:ident),* $(,)?
        ]
    ) => {
        $(#[$meta])*
        #[derive(thiserror::Error, Debug)]
        $vis enum $error_name {
            $(
                #[error(transparent)]
                $variant($variant),
            )*
        }

        paste::paste! {
            const [<$error_name:snake:upper _STACK>]: &str = $stack_msg;

            $(
                impl_from_with_detail!($variant => $error_name::$variant, [<$error_name:snake:upper _STACK>]);
            )*

            impl $error_name {
                #[must_use]
                pub fn append_detail(mut self, detail: impl Into<String>) -> Self {
                    match &mut self {
                        $(
                            $error_name::$variant(e) => {
                                e.append_detail_mut(detail);
                            }
                        )*
                    }
                    self
                }

                pub fn append_detail_mut(&mut self, detail: impl Into<String>) -> &mut Self {
                    match self {
                        $(
                            $error_name::$variant(e) => {
                                e.append_detail_mut(detail);
                            }
                        )*
                    }
                    self
                }
            }

            impl From<$error_name> for ErrorModel {
                fn from(err: $error_name) -> Self {
                    match err {
                        $(
                            $error_name::$variant(e) => e.into(),
                        )*
                    }
                }
            }

            impl From<$error_name> for IcebergErrorResponse {
                fn from(err: $error_name) -> Self {
                    ErrorModel::from(err).into()
                }
            }
        }
    };
}

macro_rules! define_simple_error {
    ($error_name:ident, $error_message:literal) => {
        #[derive(thiserror::Error, Debug)]
        #[error($error_message)]
        pub struct $error_name {
            pub stack: Vec<String>,
        }

        impl Default for $error_name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl $error_name {
            #[must_use]
            pub fn new() -> Self {
                Self { stack: Vec::new() }
            }
        }

        impl_error_stack_methods!($error_name);
    };
}

pub(crate) use define_simple_error;
pub(crate) use define_transparent_error;
pub(crate) use impl_error_stack_methods;
pub(crate) use impl_from_with_detail;

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum_macros::Display)]
pub enum CatalogBackendErrorType {
    Unexpected,
    ConcurrentModification,
}

#[derive(Debug)]
pub struct CatalogBackendError {
    pub r#type: CatalogBackendErrorType,
    pub stack: Vec<String>,
    pub source: Box<dyn std::error::Error + Send + Sync + 'static>,
}

impl_error_stack_methods!(CatalogBackendError);

impl PartialEq for CatalogBackendError {
    fn eq(&self, other: &Self) -> bool {
        self.r#type == other.r#type
            && self.stack == other.stack
            && self.source.to_string() == other.source.to_string()
    }
}

impl CatalogBackendError {
    pub fn new<E>(source: E, r#type: impl Into<CatalogBackendErrorType>) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self {
            r#type: r#type.into(),
            stack: Vec::new(),
            source: Box::new(source),
        }
    }

    pub fn new_unexpected<E>(source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self {
            r#type: CatalogBackendErrorType::Unexpected,
            stack: Vec::new(),
            source: Box::new(source),
        }
    }
}

impl StdError for CatalogBackendError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&*self.source as &(dyn StdError + 'static))
    }
}

impl Display for CatalogBackendError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "CatalogBackendError ({}): {}", self.r#type, self.source)?;

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

#[derive(Debug)]
pub struct DatabaseIntegrityError {
    pub message: String,
    pub stack: Vec<String>,
}

impl_error_stack_methods!(DatabaseIntegrityError);

impl DatabaseIntegrityError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            stack: Vec::new(),
        }
    }
}

impl StdError for DatabaseIntegrityError {}

impl Display for DatabaseIntegrityError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "DatabaseIntegrityError: {}", self.message)?;

        if !self.stack.is_empty() {
            writeln!(f, "Stack:")?;
            for detail in &self.stack {
                writeln!(f, "  {detail}")?;
            }
        }
        Ok(())
    }
}

pub(crate) fn error_chain_fmt(
    e: impl StdError,
    f: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    writeln!(f, "{e}\n")?;
    let mut current = e.source();
    while let Some(cause) = current {
        writeln!(f, "Caused by:\n\t{cause}")?;
        current = cause.source();
    }
    Ok(())
}

impl From<CatalogBackendError> for ErrorModel {
    fn from(err: CatalogBackendError) -> Self {
        let CatalogBackendError {
            r#type,
            stack,
            source,
        } = err;

        let code = match r#type {
            CatalogBackendErrorType::Unexpected => StatusCode::SERVICE_UNAVAILABLE,
            CatalogBackendErrorType::ConcurrentModification => StatusCode::CONFLICT,
        }
        .as_u16();

        ErrorModel {
            r#type: "CatalogBackendError".to_string(),
            code,
            message: format!("Catalog backend error ({type}): {source}"),
            stack,
            source: None,
        }
    }
}

impl From<DatabaseIntegrityError> for ErrorModel {
    fn from(err: DatabaseIntegrityError) -> Self {
        let DatabaseIntegrityError { message, stack } = err;

        ErrorModel {
            r#type: "DatabaseIntegrityError".to_string(),
            code: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
            message: format!("Database integrity error: {message}"),
            stack,
            source: None,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct InvalidPaginationToken {
    pub message: String,
    pub value: String,
    pub stack: Vec<String>,
}

impl_error_stack_methods!(InvalidPaginationToken);

impl StdError for InvalidPaginationToken {}

impl Display for InvalidPaginationToken {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Invalid pagination token - {}. Got: `{}`",
            self.message, self.value
        )?;

        if !self.stack.is_empty() {
            writeln!(f, "Stack:")?;
            for detail in &self.stack {
                writeln!(f, "  {detail}")?;
            }
        }
        Ok(())
    }
}

impl InvalidPaginationToken {
    pub fn new(message: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            value: value.into(),
            stack: Vec::new(),
        }
    }
}

impl From<InvalidPaginationToken> for ErrorModel {
    fn from(err: InvalidPaginationToken) -> Self {
        let InvalidPaginationToken {
            message,
            value,
            stack,
        } = err;

        ErrorModel {
            r#type: "InvalidPaginationToken".to_string(),
            code: StatusCode::BAD_REQUEST.as_u16(),
            message: format!("Invalid pagination token - {message}. Got: `{value}`"),
            stack,
            source: None,
        }
    }
}
impl From<InvalidPaginationToken> for IcebergErrorResponse {
    fn from(err: InvalidPaginationToken) -> Self {
        ErrorModel::from(err).into()
    }
}

#[derive(thiserror::Error, Debug, PartialEq)]
#[error("Database service returned invalid response")]
pub struct ResultCountMismatch {
    pub expected_results: usize,
    pub actual_results: usize,
    pub type_name: String,
    pub stack: Vec<String>,
}
impl_error_stack_methods!(ResultCountMismatch);
impl ResultCountMismatch {
    #[must_use]
    pub fn new(expected_results: usize, actual_results: usize, type_name: &str) -> Self {
        Self {
            expected_results,
            actual_results,
            type_name: type_name.to_string(),
            stack: Vec::new(),
        }
    }
}
impl From<ResultCountMismatch> for ErrorModel {
    fn from(err: ResultCountMismatch) -> Self {
        let message = err.to_string();
        let ResultCountMismatch {
            expected_results,
            actual_results,
            type_name,
            stack,
        } = err;

        ErrorModel {
            r#type: "ResultCountMismatch".to_string(),
            code: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
            message,
            source: Some(Box::new(InternalErrorMessage(format!(
                "Result count mismatch for {type_name} batch operation: expected {expected_results}, got {actual_results}."
            )))),
            stack,
        }
    }
}
impl From<ResultCountMismatch> for IcebergErrorResponse {
    fn from(err: ResultCountMismatch) -> Self {
        ErrorModel::from(err).into()
    }
}

#[derive(thiserror::Error, PartialEq, Debug)]
#[error("{0}")]
pub struct InternalErrorMessage(pub String);
