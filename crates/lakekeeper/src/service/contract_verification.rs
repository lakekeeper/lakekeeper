#![allow(clippy::module_name_repetitions)]
use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use iceberg::{
    spec::{TableMetadata, ViewMetadata},
    TableIdent, TableUpdate,
};
use iceberg_ext::catalog::rest::{ErrorModel, ViewUpdate};

use crate::service::TabularId;

/// A trait for checking if a table change is allowed.
///
/// This trait is used to implement custom logic for checking if a table change is allowed. One
/// possible application is the enforcement of data contracts. For example, an external system could
/// be contacted to check if the changed table columns are part of an existing contract.
///
/// # Example
///
/// ```rust
///     use async_trait::async_trait;
///     use iceberg::spec::{TableMetadata, ViewMetadata};
///     use iceberg::{TableIdent, TableUpdate};
///     use lakekeeper::service::{TabularId, contract_verification::{ContractVerification, ContractVerificationOutcome}};
///     use iceberg_ext::catalog::rest::{ErrorModel, ViewUpdate};
///
///     #[derive(Debug)]
///     pub struct AllowAllChecker;
///
///     #[async_trait]
///     impl ContractVerification for AllowAllChecker {
///         fn name(&self) -> &'static str {
///             "AllowAllChecker"
///         }
///         async fn check_table_updates(
///             &self,
///             _table_updates: &[TableUpdate],
///             _current_metadata: &TableMetadata,
///         ) -> Result<ContractVerificationOutcome, ErrorModel> {
///             Ok(ContractVerificationOutcome::Clear {})
///         }
///
///         async fn check_view_updates(&self, _view_updates: &[ViewUpdate], _current_metadata: &ViewMetadata) -> Result<ContractVerificationOutcome, ErrorModel> {
///             Ok(ContractVerificationOutcome::Clear {})
///         }
///
///         async fn check_drop(
///             &self,
///             _table_ident_uuid: TabularId,
///         ) -> Result<ContractVerificationOutcome, ErrorModel> {
///             Ok(ContractVerificationOutcome::Clear {})
///         }
///
///         async fn check_rename(&self, source: TabularId, destination: &TableIdent) -> Result<ContractVerificationOutcome, ErrorModel> {
///             Ok(ContractVerificationOutcome::Clear {})
///         }
///     }
///
///     #[derive(Debug)]
///     pub struct DenyAllChecker;
///
///     #[async_trait]
///     impl ContractVerification for DenyAllChecker {
///         fn name(&self) -> &'static str {
///             "DenyAllChecker"
///         }
///         async fn check_table_updates(
///             &self,
///             _table_updates: &[TableUpdate],
///             _current_metadata: &TableMetadata,
///         ) -> Result<ContractVerificationOutcome, ErrorModel> {
///             Ok(ContractVerificationOutcome::Violation {
///                 error_model: ErrorModel::builder()
///                     .code(409)
///                     .message("Denied")
///                     .r#type("ContractViolation".to_string())
///                     .build()
///                     .into(),
///             })
///         }
///
///         async fn check_view_updates(&self, _view_updates: &[ViewUpdate], _current_metadata: &ViewMetadata) -> Result<ContractVerificationOutcome, ErrorModel> {
///             Ok(ContractVerificationOutcome::Violation {
///                 error_model: ErrorModel::builder()
///                     .code(409)
///                     .message("Denied")
///                     .r#type("ContractViolation".to_string())
///                     .build()
///                     .into(),
///             })
///         }
///
///         async fn check_drop(
///             &self,
///             _table_ident_uuid: TabularId,
///         ) -> Result<ContractVerificationOutcome, ErrorModel> {
///             Ok(ContractVerificationOutcome::Violation {
///                 error_model: ErrorModel::builder()
///                     .code(409)
///                     .r#type("ContractViolation".to_string())
///                     .message("Denied")
///                     .build()
///                     .into(),
///             })
///         }
///
///         async fn check_rename(&self, source: TabularId, destination: &TableIdent) -> Result<ContractVerificationOutcome, ErrorModel> {
///             Ok(ContractVerificationOutcome::Violation {
///                 error_model: ErrorModel::builder()
///                     .code(409)
///                     .r#type("ContractViolation".to_string())
///                     .message("Denied")
///                     .build()
///                     .into(),
///             })
///         }
///     }
/// ```
#[async_trait]
pub trait ContractVerification: Debug {
    fn name(&self) -> &'static str;

    async fn check_table_updates(
        &self,
        table_updates: &[TableUpdate],
        current_metadata: &TableMetadata,
    ) -> Result<ContractVerificationOutcome, ErrorModel>;

    async fn check_view_updates(
        &self,
        _view_updates: &[ViewUpdate],
        _current_metadata: &ViewMetadata,
    ) -> Result<ContractVerificationOutcome, ErrorModel>;

    async fn check_drop(
        &self,
        table_ident_uuid: TabularId,
    ) -> Result<ContractVerificationOutcome, ErrorModel>;

    async fn check_rename(
        &self,
        source: TabularId,
        destination: &TableIdent,
    ) -> Result<ContractVerificationOutcome, ErrorModel>;
}

#[derive(Debug)]
pub enum ContractVerificationOutcome {
    Clear {},
    Violation { error_model: ErrorModel },
}

impl ContractVerificationOutcome {
    /// Converts `self` into a `Result<(), ErrorModel>`.
    ///
    /// When using `ContractVerificationOutcome`, we are presented with a
    /// `Result<ContractVerificationOutcome, ErrorModel>`
    /// where the outer `ErrorModel` indicates that a checker failed, that would indicate a problem
    /// with the checker itself and may be returned as an internal server error. This function here
    /// offers convenience to go from the `ContractVerificationOutcome` to a `Result<(), ErrorModel>`
    /// which can be short-circuited using the `?` operator in the handler.
    ///
    /// # Example
    ///
    /// ```rust
    ///     use lakekeeper::service::contract_verification::ContractVerificationOutcome;
    ///     use iceberg_ext::catalog::rest::ErrorModel;
    ///
    ///     fn my_handler() -> Result<(), ErrorModel> {
    ///         let result: Result<ContractVerificationOutcome, ErrorModel> = Ok(ContractVerificationOutcome::Clear {});
    ///         result?.into_result()?;
    ///         Ok(())
    ///     }
    /// ```
    ///
    /// # Errors
    ///
    /// - extracts `error_model` from `ContractVerificationOutcome::Block` and returns it as an `Err` for
    ///   convenience.
    pub fn into_result(self) -> Result<(), ErrorModel> {
        match self {
            ContractVerificationOutcome::Clear {} => Ok(()),
            ContractVerificationOutcome::Violation { error_model } => Err(error_model),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ContractVerifiers {
    checkers: Vec<Arc<dyn ContractVerification + Sync + Send>>,
}

impl ContractVerifiers {
    #[must_use]
    pub fn new(checkers: Vec<Arc<dyn ContractVerification + Sync + Send>>) -> Self {
        Self { checkers }
    }
}

#[async_trait]
impl ContractVerification for ContractVerifiers {
    fn name(&self) -> &'static str {
        "ContractVerifiers"
    }

    async fn check_table_updates(
        &self,
        table_updates: &[TableUpdate],
        current_metadata: &TableMetadata,
    ) -> Result<ContractVerificationOutcome, ErrorModel> {
        for checker in &self.checkers {
            match checker
                .check_table_updates(table_updates, current_metadata)
                .await
            {
                Ok(ContractVerificationOutcome::Clear {}) => {}
                Ok(block_result @ ContractVerificationOutcome::Violation { error_model: _ }) => {
                    tracing::info!(
                        "ContractVerifier '{}' blocked change on table '{}'",
                        checker.name(),
                        current_metadata.uuid()
                    );
                    return Ok(block_result);
                }
                Err(error) => {
                    tracing::warn!("Checker {} failed", checker.name());
                    return Err(error);
                }
            }
        }

        Ok(ContractVerificationOutcome::Clear {})
    }

    async fn check_view_updates(
        &self,
        view_updates: &[ViewUpdate],
        current_metadata: &ViewMetadata,
    ) -> Result<ContractVerificationOutcome, ErrorModel> {
        for checker in &self.checkers {
            match checker
                .check_view_updates(view_updates, current_metadata)
                .await
            {
                Ok(ContractVerificationOutcome::Clear {}) => {}
                Ok(block_result @ ContractVerificationOutcome::Violation { error_model: _ }) => {
                    tracing::info!(
                        "ContractVerifier '{}' blocked change on view '{}'",
                        checker.name(),
                        current_metadata.uuid()
                    );
                    return Ok(block_result);
                }
                Err(error) => {
                    tracing::warn!("Checker {} failed", checker.name());
                    return Err(error);
                }
            }
        }

        Ok(ContractVerificationOutcome::Clear {})
    }

    async fn check_drop(
        &self,
        table_ident_uuid: TabularId,
    ) -> Result<ContractVerificationOutcome, ErrorModel> {
        for checker in &self.checkers {
            match checker.check_drop(table_ident_uuid).await {
                Ok(ContractVerificationOutcome::Clear {}) => {}
                Ok(block_result @ ContractVerificationOutcome::Violation { error_model: _ }) => {
                    tracing::info!(
                        "ContractVerifier '{}' blocked drop on table '{}'",
                        checker.name(),
                        table_ident_uuid
                    );
                    return Ok(block_result);
                }
                Err(error) => {
                    tracing::warn!("ContractVerifier '{}' failed", checker.name());
                    return Err(error);
                }
            }
        }
        Ok(ContractVerificationOutcome::Clear {})
    }

    async fn check_rename(
        &self,
        source: TabularId,
        destination: &TableIdent,
    ) -> Result<ContractVerificationOutcome, ErrorModel> {
        for checker in &self.checkers {
            match checker.check_rename(source, destination).await {
                Ok(ContractVerificationOutcome::Clear {}) => {}
                Ok(block_result @ ContractVerificationOutcome::Violation { error_model: _ }) => {
                    tracing::info!(
                        "ContractVerifier '{}' blocked rename from '{}' to '{:?}'",
                        checker.name(),
                        source,
                        destination
                    );
                    return Ok(block_result);
                }
                Err(error) => {
                    tracing::warn!("ContractVerifier '{}' failed", checker.name());
                    return Err(error);
                }
            }
        }
        Ok(ContractVerificationOutcome::Clear {})
    }
}
