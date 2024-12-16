pub use iceberg::{NamespaceIdent, TableIdent, TableRequirement, TableUpdate};

mod view_requirement;
pub use view_requirement::{AssertViewUuid, ViewRequirement};

pub mod rest {
    mod catalog_config;
    pub use catalog_config::CatalogConfig;

    mod s3_signer;
    pub use s3_signer::{S3SignRequest, S3SignResponse};

    mod view_update;
    pub use view_update::ViewUpdate;

    mod auth;
    pub use auth::{
        OAuthAccessTokenType, OAuthClientCredentialsRequest, OAuthError, OAuthErrorType,
        OAuthTokenExchangeRequest, OAuthTokenRequest, OAuthTokenResponse, OAuthTokenType,
    };

    mod error;
    #[cfg(feature = "axum")]
    pub(crate) use error::impl_into_response;
    pub use error::{Error, ErrorModel, IcebergErrorResponse};

    mod table;
    pub use table::{
        CommitTableRequest, CommitTableResponse, CommitTransactionRequest, CreateTableRequest,
        ListTablesResponse, LoadCredentialsResponse, LoadTableResult, RegisterTableRequest,
        RenameTableRequest, StorageCredential,
    };

    mod view;
    pub use view::{CommitViewRequest, CreateViewRequest, LoadViewResult};

    mod namespace;
    pub use namespace::{
        CreateNamespaceRequest, CreateNamespaceResponse, GetNamespaceResponse,
        ListNamespacesResponse, UpdateNamespacePropertiesRequest,
        UpdateNamespacePropertiesResponse,
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_namespace_ident_serialization() {
        let j = serde_json::json!(["a!", "b~", "c "]);

        let r: NamespaceIdent = serde_json::from_value(j.clone()).unwrap();
        assert_eq!(serde_json::to_value(r).unwrap(), j);
    }

    #[test]
    fn test_table_ident_serialization() {
        let j = serde_json::json!({
            "namespace": ["ns1", "ns2"],
            "name": "tbl"
        });

        let r: TableIdent = serde_json::from_value(j.clone()).unwrap();
        assert_eq!(serde_json::to_value(r).unwrap(), j);
    }

    #[test]
    fn test_table_requirement_serialization() {
        let j = serde_json::json!({
            "type": "assert-ref-snapshot-id",
            "ref": "branch",
            "snapshot-id": 12224
        });

        let r: TableRequirement = serde_json::from_value(j.clone()).unwrap();
        match &r {
            TableRequirement::RefSnapshotIdMatch { r#ref, snapshot_id } => {
                assert_eq!(r#ref, "branch");
                assert_eq!(snapshot_id, &Some(12224));
            }
            _ => panic!("Expected TableRequirement::RefSnapshotIdMatch"),
        }
        assert_eq!(serde_json::to_value(r).unwrap(), j);
    }
}
