use iceberg_ext::catalog::rest::ErrorModel;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Debug)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(untagged)]
pub enum Period {
    Days(#[cfg_attr(feature = "open-api", schema(inline))] NonZeroDays),
}

impl Period {
    #[must_use]
    pub fn with_days(days: NonZeroDays) -> Self {
        Self::Days(days)
    }
}

impl Default for Period {
    fn default() -> Self {
        Self::Days(NonZeroDays::default())
    }
}

#[derive(Clone, Copy, PartialEq, Debug, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
pub struct NonZeroDays {
    #[cfg_attr(feature = "open-api", schema(minimum = 1))]
    days: u16,
}

impl<'de> Deserialize<'de> for NonZeroDays {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper {
            days: u16,
        }

        let helper = Helper::deserialize(deserializer)?;
        if helper.days == 0 {
            Err(serde::de::Error::custom("Days must be greater than zero"))
        } else {
            Ok(NonZeroDays { days: helper.days })
        }
    }
}

impl Default for NonZeroDays {
    fn default() -> Self {
        Self { days: 1 }
    }
}

impl TryFrom<u16> for NonZeroDays {
    type Error = ErrorModel;

    fn try_from(value: u16) -> std::result::Result<Self, Self::Error> {
        if value == 0 {
            Err(ErrorModel::internal(
                "Days must be greater than zero",
                "InvalidPeriod",
                None,
            ))
        } else {
            Ok(NonZeroDays { days: value })
        }
    }
}

impl From<NonZeroDays> for u16 {
    fn from(value: NonZeroDays) -> Self {
        value.days
    }
}

impl From<NonZeroDays> for i32 {
    fn from(value: NonZeroDays) -> Self {
        i32::from(value.days)
    }
}

impl From<NonZeroDays> for i64 {
    fn from(value: NonZeroDays) -> Self {
        i64::from(value.days)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parsing_period_from_json() {
        let period_json = r#"{ "days": 7 }"#;
        let period: Period = serde_json::from_str(period_json).unwrap();
        let days = NonZeroDays::try_from(7).unwrap();
        assert_eq!(period, Period::with_days(days));
    }

    #[test]
    fn test_parsing_period_should_fail_from_json_with_zero_days() {
        let period_json = r#"{ "days": 0 }"#;
        let result = serde_json::from_str::<Period>(period_json);
        assert!(result.is_err());
    }

    #[test]
    fn test_parsing_period_should_fail_from_json_with_negative_days() {
        let period_json = r#"{ "days": -7 }"#;
        let result = serde_json::from_str::<Period>(period_json);
        assert!(result.is_err());
    }

    #[test]
    fn test_create_period_fails_with_zero_days() {
        let days_result = NonZeroDays::try_from(0);
        assert!(days_result.is_err());
    }
}
