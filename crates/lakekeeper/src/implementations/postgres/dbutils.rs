use crate::{
    api::ErrorModel,
    service::{CatalogBackendError, CatalogBackendErrorType},
};

pub(crate) trait DBErrorHandler
where
    Self: ToString + Sized + Send + Sync + std::error::Error + 'static,
{
    fn into_error_model(self, message: impl Into<String>) -> ErrorModel {
        ErrorModel::internal(message, "DatabaseError", Some(Box::new(self)))
    }

    fn into_catalog_backend_error(self) -> CatalogBackendError;
}

impl DBErrorHandler for sqlx::Error {
    fn into_error_model(self, message: impl Into<String>) -> ErrorModel {
        match self {
            Self::Database(ref db) => {
                if db.is_unique_violation() {
                    return ErrorModel::conflict(
                        message,
                        "EntityAlreadyExists",
                        Some(Box::new(self)),
                    );
                }
                match db.code().as_deref() {
                    // https://www.postgresql.org/docs/current/errcodes-appendix.html
                    Some(
                        "2D000" | "25000" | "25001" | "25P01" | "25P02" | "25P03" | "40000"
                        | "40001" | "40002" | "40003" | "40004",
                    ) => ErrorModel::conflict(
                        "Concurrent modification failed.",
                        "TransactionFailed",
                        Some(Box::new(self)),
                    ),
                    _ => ErrorModel::internal(message, "DatabaseError", Some(Box::new(self))),
                }
            }
            _ => ErrorModel::internal(message, "DatabaseError", Some(Box::new(self))),
        }
    }

    fn into_catalog_backend_error(self) -> CatalogBackendError {
        match self {
            Self::Database(ref db) => {
                // In our new error model, entity already exists should always have
                // an explicit error variant, so we treat it as unexpected here.
                match db.code().as_deref() {
                    // https://www.postgresql.org/docs/current/errcodes-appendix.html
                    Some(
                        "2D000" | "25000" | "25001" | "25P01" | "25P02" | "25P03" | "40000"
                        | "40001" | "40002" | "40003" | "40004",
                    ) => CatalogBackendError::new(
                        self,
                        CatalogBackendErrorType::ConcurrentModification,
                    )
                    .append_detail("Database Transaction failed."),
                    _ => CatalogBackendError::new_unexpected(self),
                }
            }
            _ => CatalogBackendError::new_unexpected(self),
        }
    }
}
