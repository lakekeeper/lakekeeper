use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub struct CounterResult {
    pub unit: String,
    pub value: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub struct TimerResult {
    pub time_unit: String,
    pub count: i64,
    pub total_duration: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(untagged)]
pub enum MetricResult {
    Counter(CounterResult),
    Timer(TimerResult),
}

pub type Metrics = HashMap<String, MetricResult>;

// Placeholder for iceberg_ext::org::apache::iceberg::expressions::Expression
// Using serde_json::Value for now as discussed.
pub type Expression = serde_json::Value;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub struct ScanReport {
    pub report_type: String, // Should be "scan-report"
    pub table_name: String,
    pub snapshot_id: i64,
    pub filter: Expression,
    pub schema_id: i32,
    pub projected_field_ids: Vec<i32>,
    pub projected_field_names: Vec<String>,
    pub metrics: Metrics,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub struct CommitReport {
    pub report_type: String, // Should be "commit-report"
    pub table_name: String,
    pub snapshot_id: i64,
    pub sequence_number: i64,
    pub operation: String,
    pub metrics: Metrics,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(tag = "report-type", rename_all = "kebab-case")]
pub enum ReportMetricsRequest {
    Scan(ScanReport),
    Commit(CommitReport),
}

#[cfg(test)]
mod tests {
    use super::*; // Imports the types defined above (ReportMetricsRequest, ScanReport, etc.)
    use serde_json;

    #[test]
    fn test_deserialize_scan_report() {
        let json_string = r#"{
            "report-type": "scan-report",
            "table-name": "my_db.my_table",
            "snapshot-id": 1234567890123456789,
            "filter": {"type": "true"}, 
            "schema-id": 1,
            "projected-field-ids": [1, 2, 3],
            "projected-field-names": ["col1", "col2", "col3"],
            "metrics": {
                "total-planning-duration": {
                    "time-unit": "nanoseconds",
                    "count": 1,
                    "total-duration": 2644235116
                },
                "result-data-files": {
                    "unit": "count",
                    "value": 10
                }
            },
            "metadata": {
                "client-version": "iceberg-rust/0.1.0"
            }
        }"#;

        let result = serde_json::from_str::<ReportMetricsRequest>(json_string);
        assert!(result.is_ok(), "Failed to deserialize ScanReport: {:?}", result.err());

        let report_request = result.unwrap();
        match report_request {
            ReportMetricsRequest::Scan(report) => {
                assert_eq!(report.table_name, "my_db.my_table");
                assert_eq!(report.snapshot_id, 1234567890123456789);
                assert_eq!(report.filter, serde_json::json!({"type": "true"}));
                assert!(report.metadata.is_some());
                assert_eq!(report.metadata.unwrap().get("client-version").unwrap(), "iceberg-rust/0.1.0");
                // Check a metric
                match report.metrics.get("result-data-files") {
                    Some(MetricResult::Counter(counter)) => {
                        assert_eq!(counter.value, 10);
                    }
                    _ => panic!("Incorrect metric type or metric not found for result-data-files"),
                }
                match report.metrics.get("total-planning-duration") {
                    Some(MetricResult::Timer(timer)) => {
                        assert_eq!(timer.total_duration, 2644235116);
                    }
                    _ => panic!("Incorrect metric type or metric not found for total-planning-duration"),
                }
            }
            _ => panic!("Deserialized into incorrect ReportMetricsRequest variant"),
        }
    }

    #[test]
    fn test_deserialize_commit_report() {
        let json_string = r#"{
            "report-type": "commit-report",
            "table-name": "another_db.another_table",
            "snapshot-id": 9876543210987654321,
            "sequence-number": 123,
            "operation": "append",
            "metrics": {
                "added-data-files": {
                    "unit": "count",
                    "value": 5
                }
            },
            "metadata": null
        }"#;

        let result = serde_json::from_str::<ReportMetricsRequest>(json_string);
        assert!(result.is_ok(), "Failed to deserialize CommitReport: {:?}", result.err());

        let report_request = result.unwrap();
        match report_request {
            ReportMetricsRequest::Commit(report) => {
                assert_eq!(report.table_name, "another_db.another_table");
                assert_eq!(report.operation, "append");
                assert!(report.metadata.is_none());
                 match report.metrics.get("added-data-files") {
                    Some(MetricResult::Counter(counter)) => {
                        assert_eq!(counter.value, 5);
                    }
                    _ => panic!("Incorrect metric type or metric not found for added-data-files"),
                }
            }
            _ => panic!("Deserialized into incorrect ReportMetricsRequest variant"),
        }
    }
    
    #[test]
    fn test_deserialize_metric_result_variants() {
        let counter_json = r#"{"unit": "files", "value": 42}"#;
        let timer_json = r#"{"time-unit": "ms", "count": 5, "total-duration": 500}"#;

        let counter_result = serde_json::from_str::<MetricResult>(counter_json);
        assert!(counter_result.is_ok(), "Failed to deserialize CounterResult: {:?}", counter_result.err());
        match counter_result.unwrap() {
            MetricResult::Counter(c) => {
                assert_eq!(c.unit, "files");
                assert_eq!(c.value, 42);
            }
            _ => panic!("Failed to deserialize into CounterResult"),
        }

        let timer_result = serde_json::from_str::<MetricResult>(timer_json);
        assert!(timer_result.is_ok(), "Failed to deserialize TimerResult: {:?}", timer_result.err());
        match timer_result.unwrap() {
            MetricResult::Timer(t) => {
                assert_eq!(t.time_unit, "ms");
                assert_eq!(t.count, 5);
                assert_eq!(t.total_duration, 500);
            }
            _ => panic!("Failed to deserialize into TimerResult"),
        }
    }
}
