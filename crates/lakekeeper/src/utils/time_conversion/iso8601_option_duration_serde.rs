use chrono::Duration;
use serde::{Deserialize, Deserializer, Serializer};

use crate::utils::time_conversion::iso8601_duration_serde;

#[allow(clippy::ref_option)]
pub fn serialize<S>(duration: &Option<Duration>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match duration {
        Some(d) => iso8601_duration_serde::serialize(d, serializer),
        None => serializer.serialize_none(),
    }
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt: Option<String> = Option::deserialize(deserializer)?;
    match opt {
        Some(duration_str) => {
            let duration = iso8601_duration_serde::deserialize(
                serde::de::value::StrDeserializer::new(&duration_str),
            )?;
            Ok(Some(duration))
        }
        None => Ok(None),
    }
}
