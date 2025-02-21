use heck::ToUpperCamelCase;

pub mod namespace;
mod primitives;
pub mod table;

pub use custom::CustomConfig;
pub use primitives::Location;

pub trait NotCustomProp {}

pub trait ConfigProperty {
    const KEY: &'static str;
    type Type: ToString + ParseFromStr;

    fn key(&self) -> &str {
        Self::KEY
    }

    fn value(&self) -> &Self::Type;

    fn into_value(self) -> Self::Type;

    fn value_to_string(&self) -> String {
        self.value().to_string()
    }

    /// Parse the value from a string.
    ///
    /// # Errors
    /// Returns a `ParseError` if the value is incompatible with the type.
    fn parse_value(value: &str) -> Result<Self::Type, ConfigParseError>
    where
        Self::Type: ParseFromStr,
    {
        ParseFromStr::parse_value(value).map_err(|e| e.for_key(Self::KEY))
    }
}

#[derive(thiserror::Error, Debug)]
#[error("Failed to parse value '{value}' to '{typ}'. {reasoning}")]
pub struct ParseError {
    value: String,
    typ: String,
    reasoning: String,
}

impl ParseError {
    #[must_use]
    pub fn for_key(self, key: &str) -> ConfigParseError {
        ConfigParseError {
            value: self.value,
            typ: self.typ,
            reasoning: self.reasoning,
            key: key.to_string(),
        }
    }
}

#[derive(thiserror::Error, Debug)]
#[error("Failed to parse config '{key}' with value '{value}' to '{typ}'. {reasoning}")]
pub struct ConfigParseError {
    value: String,
    typ: String,
    key: String,
    reasoning: String,
}

impl ConfigParseError {
    #[must_use]
    pub fn err_type(&self) -> String {
        format!("Config{}ParseError", self.key.to_upper_camel_case())
    }
}

/// `ParseFromStr` is a trait that needs to be implemented for the associated type of `ConfigValue`.
///
/// In most cases, it can be a `FromStr` implementation, in other cases, like `bool` we implement it
/// to handle more bool-ish variants such as "1", "t", "true", "f", "false", etc.
pub trait ParseFromStr {
    /// # Errors
    /// Returns a `ParseError` if the value cannot be parsed.
    fn parse_value(value: &str) -> Result<Self, ParseError>
    where
        Self: Sized;
}

impl ParseFromStr for String {
    fn parse_value(value: &str) -> Result<Self, ParseError> {
        Ok(value.to_string())
    }
}

impl ParseFromStr for bool {
    fn parse_value(value: &str) -> Result<Self, ParseError> {
        match value {
            "1" | "t" | "T" | "TRUE" | "true" | "True" => Ok(true),
            "0" | "f" | "F" | "FALSE" | "false" | "False" => Ok(false),
            value => Err(ParseError {
                value: value.to_string(),
                typ: "bool".to_string(),
                reasoning: String::new(),
            }),
        }
    }
}

impl ParseFromStr for url::Url {
    fn parse_value(value: &str) -> Result<Self, ParseError> {
        value.parse().map_err(|_| ParseError {
            value: value.to_string(),
            typ: "Url".to_string(),
            reasoning: String::new(),
        })
    }
}

mod custom {
    use super::ConfigProperty;

    #[derive(Debug, PartialEq, Clone)]

    pub struct CustomConfig {
        pub key: String,
        pub value: String,
    }

    impl ConfigProperty for CustomConfig {
        const KEY: &'static str = "custom";
        type Type = String;

        fn key(&self) -> &str {
            self.key.as_str()
        }

        fn value(&self) -> &Self::Type {
            &self.value
        }

        fn into_value(self) -> Self::Type {
            self.value
        }
    }
}

/// A macro to implement accessors and mutators for property structs
macro_rules! impl_properties {
    ($name:ident, $prop_type:ident) => {
        #[derive(Debug, Clone, PartialEq, Default)]
        pub struct $name {
            props: std::collections::HashMap<String, String>,
        }

        pub trait $prop_type: ConfigProperty {}

        impl $name {
            /// Inserts a property into the configuration.
            ///
            /// If the property already exists, the previous value is returned.
            pub fn insert<S: $prop_type>(&mut self, pair: &S) -> Option<S::Type> {
                let prev = self
                    .props
                    .insert(pair.key().to_string(), pair.value_to_string());
                prev.and_then(|v| S::parse_value(v.as_str()).ok())
            }

            /// Removes a property from the configuration.
            ///
            /// If the property exists, the value is returned.
            /// If the property exists but cannot be parsed to the expected type, `None` is returned.
            pub fn remove<S: $prop_type + NotCustomProp>(&mut self) -> Option<S::Type> {
                self.props
                    .remove(S::KEY)
                    .and_then(|v| S::parse_value(v.as_str()).ok())
            }

            /// Removes a property from the configuration.
            ///
            /// If the property exists, the value is returned.
            pub fn remove_untyped(&mut self, key: &str) -> Option<String> {
                self.props.remove(key)
            }

            #[must_use]
            /// Get a property from the configuration.
            ///
            /// If the property exists but cannot be parsed to the expected type, `None` is returned.
            /// If you want to fail on unparsable values, use `get_prop_fallible`.
            pub fn get_prop_opt<C: $prop_type + NotCustomProp>(&self) -> Option<C::Type> {
                self.props
                    .get(C::KEY)
                    .and_then(|v| ParseFromStr::parse_value(v.as_str()).ok())
            }

            #[must_use]
            /// Get a property from the configuration.
            ///
            /// If the property does not exist `None` is returend
            ///
            /// # Errors
            ///
            /// If the property exists but cannot be parsed to the expected type, a `ConfigParseError` is returned.
            pub fn get_prop_fallible<C: $prop_type + NotCustomProp>(
                &self,
            ) -> Option<Result<C::Type, ConfigParseError>> {
                self.props
                    .get(C::KEY)
                    .map(|v| ParseFromStr::parse_value(v.as_str()))
                    .map(|r| r.map_err(|e| e.for_key(C::KEY)))
            }

            #[must_use]
            /// Get a custom property from the configuration.
            ///
            /// A custom property is a property that is either dynamic in its key or a property that
            /// is not part of the standard interface. As such it is not bound to a specific type.
            /// Both key and value are strings.
            pub fn get_custom_prop(&self, key: &str) -> Option<String> {
                self.props.get(key).cloned()
            }

            /// Convenience constructor to create a new configuration from an optional list of
            /// properties.
            ///
            /// # Errors
            ///
            /// Returns an error if a known key has an incompatible value.
            pub fn try_from_maybe_props(
                props: Option<impl IntoIterator<Item = (String, String)>>,
            ) -> Result<Self, ConfigParseError> {
                match props {
                    Some(props) => Self::try_from_props(props),
                    None => Ok(Self::default()),
                }
            }

            #[must_use]
            /// Non validating constructor to create a new configuration from a list of properties.
            ///
            /// Useful when going from wire format (`HashMap<String, String>`) to this type when
            /// failing on a single invalid property is not desired. `get_prop_fallible` still
            /// offers the possibility to have runtime errors above unexpected failures elsewhere.
            pub fn from_props_unchecked(props: impl IntoIterator<Item = (String, String)>) -> Self {
                let mut config = Self::default();
                for (key, value) in props {
                    config.props.insert(key, value);
                }
                config
            }

            #[must_use]
            pub fn inner(&self) -> &std::collections::HashMap<String, String> {
                &self.props
            }
        }
    };
}

macro_rules! impl_config_value {
    ($struct_name:ident, $prop_type:ident, $typ:ident, $key:expr, $accessor:expr) => {
        #[derive(Debug, PartialEq, Clone)]
        pub struct $struct_name(pub $typ);

        impl ConfigProperty for $struct_name {
            const KEY: &'static str = $key;
            type Type = $typ;

            fn value(&self) -> &Self::Type {
                &self.0
            }

            fn into_value(self) -> Self::Type {
                self.0
            }

            fn parse_value(value: &str) -> Result<Self::Type, ConfigParseError>
            where
                Self::Type: ParseFromStr,
            {
                Self::Type::parse_value(value).map_err(|e| e.for_key(Self::KEY))
            }
        }

        impl NotCustomProp for $struct_name {}

        paste::paste! {
            impl [<$prop_type Property>] for $struct_name {}
        }

        paste::paste! {
            impl [<$prop_type Properties>] {
                #[must_use]
                pub fn [<$accessor:snake>](&self) -> Option<$typ> {
                    self.get_prop_opt::<$struct_name>()
                }

                pub fn [<insert_ $accessor:snake>](&mut self, value: $typ) -> Option<$typ> {
                    self.insert(&$struct_name(value))
                }
            }
        }
    };
}
macro_rules! impl_config_values {
    ($prefix:ident, { $($struct_name:ident, $typ:ident, $key:expr, $accessor:expr);+ $(;)? }) => {
        use crate::configs::impl_config_value;

        $(
            impl_config_value!($struct_name, $prefix, $typ, $key, $accessor);
        )+

        pub(crate) fn validate(
            key: &str,
            value: &str,
        ) -> Result<(), ConfigParseError> {
            Ok(match key {
                $(
                    $struct_name::KEY => {
                        _ = $struct_name::parse_value(value)?;
                    }
                )+
                _ => {},
            })
        }
    };
}

use impl_config_value;
use impl_config_values;
use impl_properties;
