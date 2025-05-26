use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetricResult {
    #[serde(rename = "unit")]
    pub unit: String,
    #[serde(rename = "value")]
    pub value: i64,
    #[serde(rename = "time-unit")]
    pub time_unit: String,
    #[serde(rename = "count")]
    pub count: i64,
    #[serde(rename = "total-duration")]
    pub total_duration: i64,
}
