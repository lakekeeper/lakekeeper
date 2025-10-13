use std::{
    error::Error as StdError,
    fmt::{Display, Formatter},
};

use http::StatusCode;
use iceberg_ext::catalog::rest::ErrorModel;

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

macro_rules! define_simple_error {
    ($error_name:ident, $error_message:literal) => {
        #[derive(thiserror::Error, Debug, PartialEq, Eq)]
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

#[derive(Debug, PartialEq)]
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

        crate::service::ErrorModel {
            r#type: "DatabaseIntegrityError".to_string(),
            code: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
            message: format!("Database integrity error: {message}"),
            stack,
            source: None,
        }
    }
}
