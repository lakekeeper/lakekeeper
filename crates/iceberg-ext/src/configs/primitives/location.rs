use std::str::FromStr;

use lakekeeper_io::Location;

use crate::configs::{ConfigProperty, NotCustomProp, ParseError, ParseFromStr};

impl NotCustomProp for Location {}

impl ConfigProperty for Location {
    const KEY: &'static str = "location";
    type Type = Self;

    fn key(&self) -> &str {
        Self::KEY
    }

    fn value(&self) -> &Self::Type {
        self
    }

    fn into_value(self) -> Self::Type {
        self
    }
}

impl ParseFromStr for Location {
    fn parse_value(value: &str) -> Result<Self, ParseError> {
        Location::from_str(value).map_err(|e| ParseError {
            typ: "Location".to_string(),
            value: e.value,
            reasoning: e.reason,
        })
    }
}
