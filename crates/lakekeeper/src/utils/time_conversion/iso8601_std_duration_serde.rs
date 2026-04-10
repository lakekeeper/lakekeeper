//! Serde support for serializing `std::time::Duration` as ISO 8601 duration strings.
//!
//! Use with `#[serde(with = "iso8601_std_duration_serde")]` on `std::time::Duration` fields.

use std::time::Duration;

use serde::{Deserializer, Serializer};

use crate::utils::time_conversion::{
    duration_serde_visitor::StdDurationVisitor, std_duration_to_iso_8601_string,
};

pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&std_duration_to_iso_8601_string(duration))
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_str(StdDurationVisitor)
}
