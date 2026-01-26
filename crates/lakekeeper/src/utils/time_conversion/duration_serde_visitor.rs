use std::{fmt::Formatter, str::FromStr};

use serde::de::{Error, Visitor};

use crate::utils::time_conversion::iso_8601_duration_to_chrono;

#[derive(Debug, Default)]
pub struct ISO8601DurationVisitor;

impl<'de> Visitor<'de> for ISO8601DurationVisitor {
    type Value = iso8601::Duration;

    fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a duration string in ISO 8601 format")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        iso8601::Duration::from_str(value).map_err(|e| E::custom(e))
    }
}

#[derive(Debug, Default)]
pub struct ChronoDurationVisitor;

impl<'de> Visitor<'de> for ChronoDurationVisitor {
    type Value = chrono::Duration;

    fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a duration string in ISO 8601 format")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        let iso8601_duration_visitor = ISO8601DurationVisitor::default();
        let duration = iso8601_duration_visitor.visit_str::<E>(value)?;
        iso_8601_duration_to_chrono(&duration).map_err(|e| E::custom(e))
    }
}

#[cfg(test)]
mod test {
    use serde_json::error::Error;

    use super::*;

    #[test]
    fn test_iso8601_duration_visitor_can_parse_iso_8601_duration() {
        let iso_duration_str = "P3DT4H";
        let duration: iso8601::Duration = ISO8601DurationVisitor
            .visit_str::<Error>(iso_duration_str)
            .unwrap();
        assert_eq!(
            duration,
            iso8601::Duration::YMDHMS {
                year: 0,
                month: 0,
                day: 3,
                hour: 4,
                minute: 0,
                second: 0,
                millisecond: 0
            }
        );
    }

    #[test]
    fn test_iso8601_duration_visitor_throws_error_with_invalid_format() {
        let iso_duration_str = "InvalidDuration";
        let result = ISO8601DurationVisitor.visit_str::<Error>(iso_duration_str);
        assert!(result.is_err());
    }

    #[test]
    fn test_chrono_duration_visitor_can_parse_iso_8601_duration() {
        let iso_duration_str = "P3DT4H";
        let duration: chrono::Duration = ChronoDurationVisitor
            .visit_str::<Error>(iso_duration_str)
            .unwrap();
        assert_eq!(
            duration,
            chrono::Duration::days(3) + chrono::Duration::hours(4)
        );
    }

    #[test]
    fn test_chrono_duration_visitor_throws_error_with_invalid_format() {
        let iso_duration_str = "InvalidDuration";
        let result = ChronoDurationVisitor.visit_str::<Error>(iso_duration_str);
        assert!(result.is_err());
    }

    #[test]
    fn test_chrono_duration_visitor_returns_error_if_it_contains_month() {
        let iso_duration_str = "P1MT2H";
        let result = ChronoDurationVisitor.visit_str::<Error>(iso_duration_str);
        assert!(result.is_err());
    }

    #[test]
    fn test_chrono_duration_visitor_returns_error_if_it_contains_year() {
        let iso_duration_str = "P1YT2H";
        let result = ChronoDurationVisitor.visit_str::<Error>(iso_duration_str);
        assert!(result.is_err());
    }
}
