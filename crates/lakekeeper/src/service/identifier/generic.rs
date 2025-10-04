use std::{ops::Deref, str::FromStr};

use http::StatusCode;
use iceberg::TableIdent;
use iceberg_ext::catalog::rest::ErrorModel;
use serde::{Deserialize, Serialize};

pub use self::named_entity::NamedEntity;
use crate::api::iceberg::v1::Prefix;

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

            /// Parses the ID from a string
            ///
            /// # Errors
            /// Returns `ErrorModel` with `BAD_REQUEST` status code if the string is not a valid UUID
            pub fn from_str_or_bad_request(s: &str) -> Result<Self, ErrorModel> {
                Ok($name(uuid::Uuid::from_str(s).map_err(|e| {
                    ErrorModel::builder()
                        .code(StatusCode::BAD_REQUEST.into())
                        .message(concat!(
                            "Provided ",
                            stringify!($name),
                            " is not a valid UUID"
                        ))
                        .r#type(concat!(stringify!($name), "IsNotUUID"))
                        .source(Some(Box::new(e)))
                        .build()
                })?))
            }

            /// Parses the ID from a string
            ///
            /// # Errors
            /// Returns `ErrorModel` with `INTERNAL_SERVER_ERROR` status code if the string is not a valid UUID
            pub fn from_str_or_internal(s: &str) -> Result<Self, ErrorModel> {
                Ok($name(uuid::Uuid::from_str(s).map_err(|e| {
                    ErrorModel::builder()
                        .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                        .message(concat!(
                            "Provided ",
                            stringify!($name),
                            " is not a valid UUID"
                        ))
                        .r#type(concat!(stringify!($name), "IsNotUUID"))
                        .source(Some(Box::new(e)))
                        .build()
                })?))
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

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> std::result::Result<$name, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let s = String::deserialize(deserializer)?;
                Ok($name::from(uuid::Uuid::from_str(&s).map_err(|e| {
                    serde::de::Error::custom(format!(
                        "Provided {} is not a valid UUID: {}",
                        stringify!($name),
                        e
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
