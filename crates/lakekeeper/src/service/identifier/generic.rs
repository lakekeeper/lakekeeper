use std::{ops::Deref, str::FromStr};

use http::StatusCode;
use iceberg::TableIdent;
use iceberg_ext::catalog::rest::ErrorModel;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub use self::named_entity::NamedEntity;
use crate::{api::iceberg::v1::Prefix, service::DatabaseIntegrityError};

mod named_entity {
    use super::TableIdent;

    pub trait NamedEntity {
        fn into_name_parts(self) -> Vec<String>;
    }

    impl NamedEntity for TableIdent {
        fn into_name_parts(self) -> Vec<String> {
            self.namespace
                .inner()
                .into_iter()
                .chain(std::iter::once(self.name))
                .collect()
        }
    }
}

macro_rules! define_id_type {
    ($name:ident, true) => {
        #[derive(Debug, Clone, Serialize, PartialEq, Eq, Hash, PartialOrd, Ord, Copy)]
        #[serde(transparent)]
        pub struct $name(uuid::Uuid);

        define_id_type_impl!($name);
    };

    ($name:ident, false) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Copy)]
        pub struct $name(uuid::Uuid);

        define_id_type_impl!($name);
    };

    ($name:ident) => {
        define_id_type!($name, true);
    };
}

macro_rules! define_id_type_impl {
    ($name:ident) => {
        impl $name {
            #[must_use]
            pub fn new(id: uuid::Uuid) -> Self {
                Self(id)
            }

            #[must_use]
            pub fn new_random() -> Self {
                Self(uuid::Uuid::now_v7())
            }

            fn from_str_with_code(s: &str, code: StatusCode) -> Result<Self, ErrorModel> {
                Ok($name(uuid::Uuid::from_str(s).map_err(|e| {
                    ErrorModel::builder()
                        .code(code.into())
                        .message(format!(concat!(
                            "Provided ",
                            stringify!($name),
                            " is not a valid UUID. Got: {{s}}"
                        )))
                        .r#type(concat!(stringify!($name), "IsNotUUID"))
                        .source(Some(Box::new(e)))
                        .build()
                })?))
            }

            /// Parses the ID from a string
            ///
            /// # Errors
            /// Returns `ErrorModel` with `BAD_REQUEST` status code if the string is not a valid UUID
            pub fn from_str_or_bad_request(s: &str) -> Result<Self, ErrorModel> {
                Self::from_str_with_code(s, StatusCode::BAD_REQUEST)
            }

            /// Parses the ID from a string
            ///
            /// # Errors
            /// Returns `ErrorModel` with `INTERNAL_SERVER_ERROR` status code if the string is not a valid UUID
            pub fn from_str_or_internal(s: &str) -> Result<Self, ErrorModel> {
                Self::from_str_with_code(s, StatusCode::INTERNAL_SERVER_ERROR)
            }
        }

        impl Deref for $name {
            type Target = uuid::Uuid;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl From<uuid::Uuid> for $name {
            fn from(value: uuid::Uuid) -> Self {
                Self(value)
            }
        }

        impl From<&uuid::Uuid> for $name {
            fn from(value: &uuid::Uuid) -> Self {
                Self(*value)
            }
        }

        impl From<$name> for uuid::Uuid {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl AsRef<uuid::Uuid> for $name {
            fn as_ref(&self) -> &uuid::Uuid {
                &self.0
            }
        }

        // Deserialize is separately implemented to provide better error messages
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> std::result::Result<$name, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let s = String::deserialize(deserializer)?;
                Ok($name::from(uuid::Uuid::from_str(&s).map_err(|_| {
                    serde::de::Error::custom(format!(
                        "Provided {} is not a valid UUID. Got {s}.",
                        stringify!($name),
                    ))
                })?))
            }
        }
    };
}

// Generate all your ID types
define_id_type!(ServerId, true);
define_id_type!(WarehouseId, true);
define_id_type!(ViewId, true);
define_id_type!(TableId, true);
define_id_type!(NamespaceId, true);
define_id_type!(RoleId, true);

impl TryFrom<Prefix> for WarehouseId {
    type Error = ErrorModel;

    fn try_from(value: Prefix) -> Result<Self, Self::Error> {
        let prefix = uuid::Uuid::parse_str(value.as_str()).map_err(|e| {
            ErrorModel::builder()
                .code(StatusCode::BAD_REQUEST.into())
                .message(format!(
                    "Provided prefix is not a warehouse id. Expected UUID, got: {}",
                    value.as_str()
                ))
                .r#type("PrefixIsNotWarehouseID".to_string())
                .source(Some(Box::new(e)))
                .build()
        })?;
        Ok(WarehouseId(prefix))
    }
}

impl TryFrom<Option<Uuid>> for WarehouseId {
    type Error = ErrorModel;

    fn try_from(value: Option<Uuid>) -> Result<Self, Self::Error> {
        match value {
            Some(id) => Ok(WarehouseId(id)),
            None => Err(DatabaseIntegrityError::new("WarehouseId must not be null.").into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serde() {
        let id = TableId::new_random();
        let serialized = serde_json::to_value(id).unwrap();
        assert_eq!(serialized, serde_json::json!(id.0.to_string()));
        let deserialized: TableId = serde_json::from_value(serialized).unwrap();
        assert_eq!(id, deserialized);
    }

    #[test]
    fn test_type_name_in_error() {
        let invalid_uuid = "not-a-uuid";
        let err = TableId::from_str_or_bad_request(invalid_uuid).unwrap_err();
        assert_eq!(err.code, StatusCode::BAD_REQUEST);
        assert_eq!(err.r#type, "TableIdIsNotUUID");
        assert!(err.message.contains("TableId"));
        assert!(err.source.is_some());
    }

    #[test]
    fn test_try_from_option_uuid_to_warehouse_id_with_valid_uuid() {
        let uuid = Uuid::new_v4();
        let warehouse_id = WarehouseId::try_from(Some(uuid)).unwrap();
        assert_eq!(warehouse_id, WarehouseId::new(uuid));
    }

    #[test]
    fn test_try_from_option_uuid_to_warehouse_id_with_none_uuid() {
        let warehouse_id_error = WarehouseId::try_from(None).unwrap_err();
        assert_eq!(warehouse_id_error.r#type, "DatabaseIntegrityError");
    }
}
