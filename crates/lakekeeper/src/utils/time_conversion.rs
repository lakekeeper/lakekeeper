use iceberg_ext::catalog::rest::ErrorModel;

pub(crate) fn iso_8601_duration_to_chrono(
    duration: &iso8601::Duration,
) -> Result<chrono::Duration, ErrorModel> {
    match duration {
        iso8601::Duration::YMDHMS {
            year,
            month,
            day,
            hour,
            minute,
            second,
            millisecond,
        } => {
            if *year != 0 || *month != 0 {
                return Err(ErrorModel::bad_request(
                    "Invalid duration: May not contain year & month".to_string(),
                    "InvalidDuration".to_string(),
                    None,
                ));
            }
            Ok(chrono::Duration::days(i64::from(*day))
                + chrono::Duration::hours(i64::from(*hour))
                + chrono::Duration::minutes(i64::from(*minute))
                + chrono::Duration::seconds(i64::from(*second))
                + chrono::Duration::milliseconds(i64::from(*millisecond)))
        }
        iso8601::Duration::Weeks(w) => Ok(chrono::Duration::weeks(i64::from(*w))),
    }
}

pub(crate) fn chrono_to_iso_8601_duration(
    duration: &chrono::Duration,
) -> Result<iso8601::Duration, crate::api::ErrorModel> {
    // Check for negative duration
    if duration.num_milliseconds() < 0 {
        return Err(crate::api::ErrorModel::bad_request(
            "Negative durations not supported for ISO8601 format".to_string(),
            "InvalidDuration".to_string(),
            None,
        ));
    }

    // Extract time components
    let total_seconds = duration.num_seconds();

    // Safe conversion now that we know it's non-negative
    let milliseconds = u32::try_from(duration.num_milliseconds() % 1000).map_err(|_| {
        crate::api::ErrorModel::bad_request(
            "Duration milliseconds too large for ISO8601".to_string(),
            "InvalidDuration".to_string(),
            None,
        )
    })?;

    // If duration is exactly divisible by weeks (7 days), use weeks representation, except for zero
    if (total_seconds != 0 && total_seconds % (7 * 24 * 60 * 60) == 0) && milliseconds == 0 {
        let weeks = total_seconds / (7 * 24 * 60 * 60);
        let weeks_u32 = u32::try_from(weeks).map_err(|_| {
            crate::api::ErrorModel::bad_request(
                "Duration weeks too large for ISO8601".to_string(),
                "InvalidDuration".to_string(),
                None,
            )
        })?;
        return Ok(iso8601::Duration::Weeks(weeks_u32));
    }

    // Otherwise use YMDHMS representation
    let days = u32::try_from(duration.num_days()).map_err(|_| {
        crate::api::ErrorModel::bad_request(
            "Duration days too large for ISO8601".to_string(),
            "InvalidDuration".to_string(),
            None,
        )
    })?;

    let hours = u32::try_from(duration.num_hours() % 24).map_err(|_| {
        crate::api::ErrorModel::bad_request(
            "Duration hours calculation error".to_string(),
            "InvalidDuration".to_string(),
            None,
        )
    })?;

    let minutes = u32::try_from(duration.num_minutes() % 60).map_err(|_| {
        crate::api::ErrorModel::bad_request(
            "Duration minutes calculation error".to_string(),
            "InvalidDuration".to_string(),
            None,
        )
    })?;

    let seconds = u32::try_from(total_seconds % 60).map_err(|_| {
        crate::api::ErrorModel::bad_request(
            "Duration seconds calculation error".to_string(),
            "InvalidDuration".to_string(),
            None,
        )
    })?;

    Ok(iso8601::Duration::YMDHMS {
        year: 0,
        month: 0,
        day: days,
        hour: hours,
        minute: minutes,
        second: seconds,
        millisecond: milliseconds,
    })
}

/// Module for serializing `chrono::Duration` as ISO8601 duration strings
pub(crate) mod iso8601_duration_serde {
    use std::str::FromStr;

    use chrono::Duration;
    use serde::{Deserialize, Deserializer, Serializer};

    use super::{chrono_to_iso_8601_duration, iso_8601_duration_to_chrono};

    pub(crate) fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Convert chrono::Duration to iso8601::Duration
        let iso_duration =
            chrono_to_iso_8601_duration(duration).map_err(serde::ser::Error::custom)?;

        // Serialize to string
        serializer.serialize_str(&iso_duration.to_string())
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let duration_str = String::deserialize(deserializer)?;

        // Parse string into iso8601::Duration
        let iso_duration = iso8601::Duration::from_str(&duration_str)
            .map_err(|e| serde::de::Error::custom(format!("Invalid ISO8601 duration: {e}")))?;

        // Convert to chrono::Duration
        iso_8601_duration_to_chrono(&iso_duration).map_err(|e| serde::de::Error::custom(e.message))
    }
}

pub(crate) mod iso8601_option_duration_serde {
    use chrono::Duration;
    use serde::{Deserialize, Deserializer, Serializer};

    use super::iso8601_duration_serde;

    #[allow(clippy::ref_option)]
    pub(crate) fn serialize<S>(
        duration: &Option<Duration>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match duration {
            Some(d) => iso8601_duration_serde::serialize(d, serializer),
            None => serializer.serialize_none(),
        }
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
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
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_iso8601_to_chrono_duration() {
        // Test YMDHMS format
        let iso_duration = iso8601::Duration::from_str("P3DT4H5M6.789S").unwrap();
        let chrono_duration = iso_8601_duration_to_chrono(&iso_duration).unwrap();

        assert_eq!(chrono_duration.num_days(), 3);
        assert_eq!(chrono_duration.num_hours() % 24, 4);
        assert_eq!(chrono_duration.num_minutes() % 60, 5);
        assert_eq!(chrono_duration.num_seconds() % 60, 6);
        assert_eq!(chrono_duration.num_milliseconds() % 1000, 789);

        // Test Weeks format
        let iso_duration = iso8601::Duration::from_str("P2W").unwrap();
        let chrono_duration = iso_8601_duration_to_chrono(&iso_duration).unwrap();

        assert_eq!(chrono_duration.num_weeks(), 2);

        // Test rejection of year/month
        let iso_duration = iso8601::Duration::from_str("P1Y2M").unwrap();
        let result = iso_8601_duration_to_chrono(&iso_duration);
        assert!(result.is_err());
    }

    #[test]
    fn test_chrono_to_iso8601_duration() {
        // Test day/hour/minute/second conversion
        let chrono_duration = chrono::Duration::days(3)
            + chrono::Duration::hours(4)
            + chrono::Duration::minutes(5)
            + chrono::Duration::seconds(6)
            + chrono::Duration::milliseconds(789);

        let iso_duration = chrono_to_iso_8601_duration(&chrono_duration).unwrap();

        match iso_duration {
            iso8601::Duration::YMDHMS {
                year,
                month,
                day,
                hour,
                minute,
                second,
                millisecond,
            } => {
                assert_eq!(year, 0);
                assert_eq!(month, 0);
                assert_eq!(day, 3);
                assert_eq!(hour, 4);
                assert_eq!(minute, 5);
                assert_eq!(second, 6);
                assert_eq!(millisecond, 789);
            }
            iso8601::Duration::Weeks(_) => panic!("Expected YMDHMS format"),
        }

        // Test week-based conversion
        let chrono_duration = chrono::Duration::weeks(2);
        let iso_duration = chrono_to_iso_8601_duration(&chrono_duration).unwrap();

        match iso_duration {
            iso8601::Duration::Weeks(weeks) => {
                assert_eq!(weeks, 2);
            }
            iso8601::Duration::YMDHMS { .. } => panic!("Expected Weeks format"),
        }
    }

    #[test]
    fn test_roundtrip_conversion() {
        // Test YMDHMS roundtrip
        let original = chrono::Duration::days(3)
            + chrono::Duration::hours(4)
            + chrono::Duration::minutes(5)
            + chrono::Duration::seconds(6);

        let iso = chrono_to_iso_8601_duration(&original).unwrap();
        let roundtrip = iso_8601_duration_to_chrono(&iso).unwrap();

        assert_eq!(original, roundtrip);

        // Test Weeks roundtrip
        let original = chrono::Duration::weeks(2);
        let iso = chrono_to_iso_8601_duration(&original).unwrap();
        let roundtrip = iso_8601_duration_to_chrono(&iso).unwrap();

        assert_eq!(original, roundtrip);
    }
}

#[cfg(test)]
mod iso8601_duration_serde_tests {
    use chrono::Duration;
    use serde::{Deserialize, Serialize};

    use super::iso8601_duration_serde;

    // Test struct with a Duration field using our serializer
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    pub(super) struct TestDuration {
        #[serde(with = "iso8601_duration_serde")]
        pub(super) duration: Duration,
    }

    #[test]
    fn test_serialize_durations() {
        // Simple duration - 1 day
        let test = TestDuration {
            duration: Duration::days(1),
        };
        let json = serde_json::to_string(&test).unwrap();
        assert_eq!(json, r#"{"duration":"P1D"}"#);

        // Complex duration with multiple components
        let test = TestDuration {
            duration: Duration::days(2) + Duration::hours(3) + Duration::minutes(45),
        };
        let json = serde_json::to_string(&test).unwrap();
        assert_eq!(json, r#"{"duration":"P2DT3H45M"}"#);

        // Duration using weeks
        let test = TestDuration {
            duration: Duration::weeks(3),
        };
        let json = serde_json::to_string(&test).unwrap();
        assert_eq!(json, r#"{"duration":"P3W"}"#);

        // Zero duration
        let test = TestDuration {
            duration: Duration::seconds(0),
        };
        let json = serde_json::to_string(&test).unwrap();
        assert_eq!(json, r#"{"duration":"P0D"}"#);

        // Only hours duration
        let test = TestDuration {
            duration: Duration::hours(12),
        };
        let json = serde_json::to_string(&test).unwrap();
        assert_eq!(json, r#"{"duration":"PT12H"}"#);
    }

    #[test]
    fn test_deserialize_durations() {
        // Simple period - 1 day
        let json = r#"{"duration":"P1D"}"#;
        let test: TestDuration = serde_json::from_str(json).unwrap();
        assert_eq!(test.duration, Duration::days(1));

        // Complex duration
        let json = r#"{"duration":"P2DT3H45M"}"#;
        let test: TestDuration = serde_json::from_str(json).unwrap();
        assert_eq!(
            test.duration,
            Duration::days(2) + Duration::hours(3) + Duration::minutes(45)
        );

        // Weeks format
        let json = r#"{"duration":"P3W"}"#;
        let test: TestDuration = serde_json::from_str(json).unwrap();
        assert_eq!(test.duration, Duration::weeks(3));

        // With fractional seconds
        let json = r#"{"duration":"PT1H30M45.5S"}"#;
        let test: TestDuration = serde_json::from_str(json).unwrap();
        assert_eq!(
            test.duration,
            Duration::hours(1)
                + Duration::minutes(30)
                + Duration::seconds(45)
                + Duration::milliseconds(500)
        );
    }

    #[test]
    fn test_roundtrip_serialization() {
        let durations = vec![
            Duration::days(2) + Duration::hours(12) + Duration::minutes(30),
            Duration::weeks(1),
            Duration::minutes(90),
            Duration::seconds(3600),
            Duration::milliseconds(5000),
        ];

        for original in durations {
            let test = TestDuration { duration: original };
            let json = serde_json::to_string(&test).unwrap();
            let roundtrip: TestDuration = serde_json::from_str(&json).unwrap();

            assert_eq!(
                original, roundtrip.duration,
                "Failed roundtrip for {original:?}"
            );
        }
    }

    #[test]
    fn test_deserialize_errors() {
        // Invalid format - missing P
        let json = r#"{"duration":"1D"}"#;
        let result = serde_json::from_str::<TestDuration>(json);
        assert!(result.is_err());

        // Contains year and month (not supported)
        let json = r#"{"duration":"P1Y2M"}"#;
        let result = serde_json::from_str::<TestDuration>(json);
        assert!(result.is_err());

        // Completely invalid string
        let json = r#"{"duration":"not-a-duration"}"#;
        let result = serde_json::from_str::<TestDuration>(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_compare_with_direct_conversion() {
        // Verify that our serde module produces the same results as direct conversion
        let duration = Duration::days(3) + Duration::hours(5) + Duration::minutes(30);

        // Direct conversion
        let iso_duration = super::chrono_to_iso_8601_duration(&duration).unwrap();
        let iso_string = iso_duration.to_string();

        // Through serde
        let test = TestDuration { duration };
        let json = serde_json::to_string(&test).unwrap();
        let json_value: serde_json::Value = serde_json::from_str(&json).unwrap();
        let serde_string = json_value["duration"].as_str().unwrap();

        assert_eq!(iso_string, serde_string);
    }
}

#[cfg(test)]
mod iso8601_option_duration_serde_tests {
    use chrono::Duration;
    use serde::{Deserialize, Serialize};

    use super::iso8601_option_duration_serde;

    // Test struct with an Option<Duration> field using our serializer
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct TestOptionalDuration {
        #[serde(with = "iso8601_option_duration_serde")]
        duration: Option<Duration>,
    }

    #[test]
    fn test_serialize_some_durations() {
        // Some duration - 1 day
        let test = TestOptionalDuration {
            duration: Some(Duration::days(1)),
        };
        let json = serde_json::to_string(&test).unwrap();
        assert_eq!(json, r#"{"duration":"P1D"}"#);

        // Some complex duration
        let test = TestOptionalDuration {
            duration: Some(Duration::days(2) + Duration::hours(3) + Duration::minutes(45)),
        };
        let json = serde_json::to_string(&test).unwrap();
        assert_eq!(json, r#"{"duration":"P2DT3H45M"}"#);

        // Some duration using weeks
        let test = TestOptionalDuration {
            duration: Some(Duration::weeks(3)),
        };
        let json = serde_json::to_string(&test).unwrap();
        assert_eq!(json, r#"{"duration":"P3W"}"#);
    }

    #[test]
    fn test_serialize_none_duration() {
        let test = TestOptionalDuration { duration: None };
        let json = serde_json::to_string(&test).unwrap();
        assert_eq!(json, r#"{"duration":null}"#);
    }

    #[test]
    fn test_deserialize_some_durations() {
        // Some duration - 1 day
        let json = r#"{"duration":"P1D"}"#;
        let test: TestOptionalDuration = serde_json::from_str(json).unwrap();
        assert_eq!(test.duration, Some(Duration::days(1)));

        // Some complex duration
        let json = r#"{"duration":"P2DT3H45M"}"#;
        let test: TestOptionalDuration = serde_json::from_str(json).unwrap();
        assert_eq!(
            test.duration,
            Some(Duration::days(2) + Duration::hours(3) + Duration::minutes(45))
        );

        // Some weeks format
        let json = r#"{"duration":"P3W"}"#;
        let test: TestOptionalDuration = serde_json::from_str(json).unwrap();
        assert_eq!(test.duration, Some(Duration::weeks(3)));

        // Some duration with fractional seconds
        let json = r#"{"duration":"PT1H30M45.5S"}"#;
        let test: TestOptionalDuration = serde_json::from_str(json).unwrap();
        assert_eq!(
            test.duration,
            Some(
                Duration::hours(1)
                    + Duration::minutes(30)
                    + Duration::seconds(45)
                    + Duration::milliseconds(500)
            )
        );
    }

    #[test]
    fn test_deserialize_none_duration() {
        // Explicit null
        let json = r#"{"duration":null}"#;
        let test: TestOptionalDuration = serde_json::from_str(json).unwrap();
        assert_eq!(test.duration, None);
    }

    #[test]
    fn test_roundtrip_serialization() {
        let test_cases = vec![
            None,
            Some(Duration::days(2) + Duration::hours(12) + Duration::minutes(30)),
            Some(Duration::weeks(1)),
            Some(Duration::minutes(90)),
            Some(Duration::seconds(3600)),
            Some(Duration::milliseconds(5000)),
        ];

        for original in test_cases {
            let test = TestOptionalDuration { duration: original };
            let json = serde_json::to_string(&test).unwrap();
            let roundtrip: TestOptionalDuration = serde_json::from_str(&json).unwrap();

            assert_eq!(
                original, roundtrip.duration,
                "Failed roundtrip for {original:?}"
            );
        }
    }

    #[test]
    fn test_deserialize_errors() {
        // Invalid format - missing P (when Some)
        let json = r#"{"duration":"1D"}"#;
        let result = serde_json::from_str::<TestOptionalDuration>(json);
        assert!(result.is_err());

        // Contains year and month (not supported) when Some
        let json = r#"{"duration":"P1Y2M"}"#;
        let result = serde_json::from_str::<TestOptionalDuration>(json);
        assert!(result.is_err());

        // Completely invalid string when Some
        let json = r#"{"duration":"not-a-duration"}"#;
        let result = serde_json::from_str::<TestOptionalDuration>(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_compare_with_inner_serde() {
        use super::iso8601_duration_serde_tests::TestDuration;

        // Verify that Some(duration) produces the same results as the inner module
        let duration = Duration::days(3) + Duration::hours(5) + Duration::minutes(30);

        let inner_test = TestDuration { duration };
        let inner_json = serde_json::to_string(&inner_test).unwrap();

        // Using the option module with Some
        let option_test = TestOptionalDuration {
            duration: Some(duration),
        };
        let option_json = serde_json::to_string(&option_test).unwrap();

        // Extract the duration values from both JSON strings
        let inner_value: serde_json::Value = serde_json::from_str(&inner_json).unwrap();
        let option_value: serde_json::Value = serde_json::from_str(&option_json).unwrap();

        assert_eq!(
            inner_value["duration"], option_value["duration"],
            "Duration serialization should be identical"
        );
    }

    #[test]
    fn test_optional_field_behavior() {
        // Test that the field can be omitted entirely
        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct TestOptionalField {
            #[serde(with = "iso8601_option_duration_serde", default)]
            duration: Option<Duration>,
            other_field: String,
        }

        let json = r#"{"other_field":"test"}"#;
        let test: TestOptionalField = serde_json::from_str(json).unwrap();
        assert_eq!(test.duration, None);
        assert_eq!(test.other_field, "test");

        // Test that explicit null works
        let json = r#"{"duration":null,"other_field":"test"}"#;
        let test: TestOptionalField = serde_json::from_str(json).unwrap();
        assert_eq!(test.duration, None);
        assert_eq!(test.other_field, "test");

        // Test that a value works
        let json = r#"{"duration":"P1D","other_field":"test"}"#;
        let test: TestOptionalField = serde_json::from_str(json).unwrap();
        assert_eq!(test.duration, Some(Duration::days(1)));
        assert_eq!(test.other_field, "test");
    }
}
