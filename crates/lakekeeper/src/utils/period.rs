use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Debug)]
pub enum Period {
    #[serde(rename = "days")]
    Days(i32),
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parsing_period_from_json() {
        let period_json = r#"{ "days": 7 }"#;
        let period: Period = serde_json::from_str(period_json).unwrap();
        assert_eq!(period, Period::Days(7));
    }
}
