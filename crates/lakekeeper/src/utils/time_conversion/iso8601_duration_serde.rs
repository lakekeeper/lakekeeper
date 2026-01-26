//! Serde support for serializing `chrono::Duration` as ISO 8601 duration strings.
//!
//! This module provides `serialize` and `deserialize` functions designed to be used with
//! the `#[serde(with = "...")]` attribute on `Duration` fields.
//!
//! # Examples
//!
//! ```
//! use chrono::Duration;
//! use serde::{Deserialize, Serialize};
//! use lakekeeper::utils::time_conversion::iso8601_duration_serde;
//!
//! #[derive(Serialize, Deserialize)]
//! struct TimeoutConfig {
//!     #[serde(with = "iso8601_duration_serde")]
//!     timeout: Duration,
//! }
//!
//! let json = r#"{"timeout":"P1DT2H30M"}"#;
//! let config: TimeoutConfig = serde_json::from_str(json).unwrap();
//! assert_eq!(config.timeout.num_hours(), 26);
//! ```

use chrono::Duration;
use serde::{Deserializer, Serializer};

use crate::utils::time_conversion::{
    chrono_to_iso_8601_duration, duration_serde_visitor::ChronoDurationVisitor,
};

/// Serializes a `chrono::Duration` as an ISO 8601 duration string.
///
/// Converts the duration to ISO 8601 format and serializes it as a string.
/// Suitable for use with `#[serde(with = "iso8601_duration_serde")]`.
///
/// # Errors
///
/// Returns a serialization error if the duration is negative or would overflow in ISO 8601 format.
pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    // Convert chrono::Duration to iso8601::Duration
    let iso_duration = chrono_to_iso_8601_duration(duration).map_err(serde::ser::Error::custom)?;

    // Serialize to string
    serializer.serialize_str(&iso_duration.to_string())
}

/// Deserializes an ISO 8601 duration string into a `chrono::Duration`.
///
/// Expects a string in ISO 8601 format (e.g., `P3DT4H5M6S` or `P2W`) and converts it
/// to a `chrono::Duration`.
///
/// Suitable for use with `#[serde(with = "iso8601_duration_serde")]`.
///
/// # Errors
///
/// Returns a deserialization error if:
/// - The string is not in valid ISO 8601 format
/// - The duration contains years or months (not supported)
pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_str(ChronoDurationVisitor)
}
