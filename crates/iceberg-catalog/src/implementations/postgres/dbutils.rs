use crate::api::ErrorModel;

pub(crate) trait DBErrorHandler
where
    Self: ToString + Sized + Send + Sync + std::error::Error + 'static,
{
    fn into_error_model(self, message: String) -> ErrorModel {
        ErrorModel::internal(message, "DatabaseError", Some(Box::new(self)))
    }
}

impl DBErrorHandler for sqlx::Error {
    fn into_error_model(self, message: String) -> ErrorModel {
        match self {
            Self::Database(ref db) => {
                if db.is_unique_violation() {
                    return ErrorModel::conflict(
                        message,
                        "EntityAlreadyExists".to_string(),
                        Some(Box::new(self)),
                    );
                }
                match db.code().as_deref().map(|s| &s[..2]) {
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
}
