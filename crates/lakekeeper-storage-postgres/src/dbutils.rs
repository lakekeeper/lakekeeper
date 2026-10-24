use lakekeeper::{
    api::ErrorModel,
    service::{CatalogBackendError, CatalogBackendErrorType, DatabaseIntegrityError, UserId},
};
use sqlx::postgres::{PgDatabaseError, PgSeverity};

pub(crate) fn user_id_from_db(s: &str) -> Result<UserId, DatabaseIntegrityError> {
    UserId::try_from(s).map_err(|e| DatabaseIntegrityError::new(e.message))
}

/// Whether a transaction whose `COMMIT` failed with `error` may have been committed. Only an ERROR
/// reply proves a rollback, and only outside SQLSTATE 40003 and the classes 08, 53, 57, 58 and
/// XX. A FATAL idle-in-transaction timeout (25P03) ends the session before `COMMIT` is read.
pub(crate) fn commit_state_unknown(error: &sqlx::Error) -> bool {
    let Some(db) = error
        .as_database_error()
        .and_then(|e| e.try_downcast_ref::<PgDatabaseError>())
    else {
        return true;
    };
    let code = db.code();
    match db.severity() {
        PgSeverity::Error => {
            code == "40003"
                || ["08", "53", "57", "58", "XX"]
                    .iter()
                    .any(|c| code.starts_with(c))
        }
        PgSeverity::Fatal => code != "25P03",
        _ => true,
    }
}

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
                        | "40001" | "40002" | "40004" | "40P01",
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
                        | "40001" | "40002" | "40004" | "40P01",
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
