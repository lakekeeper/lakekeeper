//! Serde support for serializing `Option<chrono::Duration>` as ISO 8601 duration strings or null.
//!
//! Similar to [`iso8601_duration_serde`](crate::utils::time_conversion::iso8601_duration_serde),
//! but handles optional durations where `None` is serialized as `null`.
//!
//! # Examples
//!
//! ```
//! use chrono::Duration;
//! use serde::{Deserialize, Serialize};
//! use lakekeeper::utils::time_conversion::iso8601_option_duration_serde;
//!
//! #[derive(Serialize, Deserialize)]
//! struct OptionalConfig {
//!     #[serde(with = "iso8601_option_duration_serde")]
//!     max_duration: Option<Duration>,
//! }
//!
//! // Some value
//! let json = r#"{"max_duration":"P1D"}"#;
//! let config: OptionalConfig = serde_json::from_str(json).unwrap();
//! assert_eq!(config.max_duration, Some(Duration::days(1)));
//!
//! // None value
//! let json = r#"{"max_duration":null}"#;
//! let config: OptionalConfig = serde_json::from_str(json).unwrap();
//! assert_eq!(config.max_duration, None);
//! ```

use chrono::Duration;
use serde::{Deserialize, Deserializer, Serializer};

use crate::utils::time_conversion::iso8601_duration_serde;

/// Serializes an `Option<chrono::Duration>` as an ISO 8601 duration string or null.
///
/// - `Some(duration)` is serialized as an ISO 8601 string (e.g., `"P1DT2H"`)
/// - `None` is serialized as `null`
///
/// Suitable for use with `#[serde(with = "iso8601_option_duration_serde")]`.
///
/// # Errors
///
/// Returns a serialization error if a contained duration is negative or would overflow.
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

/// Deserializes an optional ISO 8601 duration string into `Option<chrono::Duration>`.
///
/// - String values are parsed as ISO 8601 durations
/// - `null` values result in `None`
/// - Missing fields default to `None` (when used with `#[serde(default)]`)
///
/// Suitable for use with `#[serde(with = "iso8601_option_duration_serde")]`.
///
/// # Errors
///
/// Returns a deserialization error if:
/// - A string value is not in valid ISO 8601 format
/// - The duration contains years or months (not supported)
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
