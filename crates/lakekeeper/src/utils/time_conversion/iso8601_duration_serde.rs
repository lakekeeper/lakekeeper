use chrono::Duration;
use serde::{Deserializer, Serializer};

use crate::utils::time_conversion::{
    chrono_to_iso_8601_duration, duration_serde_visitor::ChronoDurationVisitor,
};

pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    // Convert chrono::Duration to iso8601::Duration
    let iso_duration = chrono_to_iso_8601_duration(duration).map_err(serde::ser::Error::custom)?;

    // Serialize to string
    serializer.serialize_str(&iso_duration.to_string())
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_str(ChronoDurationVisitor::default())
}
