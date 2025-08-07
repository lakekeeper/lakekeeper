use std::{collections::HashMap, fmt::Debug};

use lakekeeper_io::Location;

use super::{impl_properties, ConfigParseError, ConfigProperty, NotCustomProp, ParseFromStr};

impl_properties!(NamespaceProperties, NamespaceProperty);

impl NamespaceProperty for Location {}

impl NamespaceProperties {
    #[must_use]
    pub fn get_location(&self) -> Option<Location> {
        self.get_prop_opt::<Location>()
    }

    /// Try to create a `NamespaceProperties` from a list of key-value pairs.
    ///
    /// # Errors
    ///
    /// Returns an error if a known key has an incompatible value.
    pub fn try_from_props(
        props: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, ConfigParseError> {
        let mut config = Self::default();
        for (key, value) in props {
            validate(&key, &value)?;
            config.props.insert(key, value);
        }
        Ok(config)
    }
}

impl IntoIterator for NamespaceProperties {
    type Item = (String, String);
    type IntoIter = std::collections::hash_map::IntoIter<String, String>;

    fn into_iter(self) -> Self::IntoIter {
        self.props.into_iter()
    }
}

#[allow(clippy::implicit_hasher)]
impl From<NamespaceProperties> for HashMap<String, String> {
    fn from(config: NamespaceProperties) -> Self {
        config.props
    }
}

pub(crate) fn validate(key: &str, value: &str) -> Result<(), ConfigParseError> {
    match key {
        Location::KEY => <Location as ConfigProperty>::parse_value(value).map(|_| ()),
        _ => Ok(()),
    }
}
