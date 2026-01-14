use iceberg_ext::catalog::rest::ErrorModel;
use serde::{Deserialize, Deserializer, Serialize, de::Error};

use crate::api::Result;

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Default, Debug)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
pub struct Period {
    #[serde(flatten)]
    data: PeriodData,
}

impl Period {
    pub fn with_days(days: u16) -> Result<Self> {
        if days == 0 {
            return Err(ErrorModel::internal(
                "Invalid period",
                "days must be greater than zero",
                None,
            )
            .into());
        }
        Ok(Self {
            data: PeriodData::Days(days),
        })
    }

    pub fn data(&self) -> PeriodData {
        self.data
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Debug)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
pub enum PeriodData {
    #[serde(rename = "days", deserialize_with = "deserialize_non_zero_days")]
    Days(u16),
}

impl Default for PeriodData {
    fn default() -> Self {
        Self::Days(1)
    }
}

fn deserialize_non_zero_days<'de, D>(deserializer: D) -> Result<u16, D::Error>
where
    D: Deserializer<'de>,
{
    let days = u16::deserialize(deserializer)?;
    if days == 0 {
        return Err(D::Error::custom("days must be greater than zero"));
    }
    Ok(days)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parsing_period_from_json() {
        let period_json = r#"{ "days": 7 }"#;
        let period: Period = serde_json::from_str(period_json).unwrap();
        assert_eq!(period, Period::with_days(7).unwrap());
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
        let period = Period::with_days(0);
        assert!(period.is_err());
    }
}
