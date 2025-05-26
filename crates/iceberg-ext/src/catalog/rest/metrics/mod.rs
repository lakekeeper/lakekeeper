use crate::catalog::rest::metrics::expression::Expression;
use crate::catalog::rest::metrics::metric_result::MetricResult;

mod and_or_expression;
mod expression;
mod false_expression;
mod literal_expression;
mod metric_result;
mod not_expression;
mod set_expression;
mod term;
mod transform_term;
mod true_expression;
mod unary_expression;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "report-type", rename_all = "kebab-case")]
pub enum ReportMetricsRequest {
    ScanReport(ScanReport),
    CommitReport(CommitReport),
}

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct DatabaseTest {
    pub field: String,
}

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScanReport {
    #[serde(rename = "table-name")]
    pub table_name: String,
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    #[serde(rename = "filter")]
    pub filter: Box<Expression>,
    #[serde(rename = "schema-id")]
    pub schema_id: i32,
    #[serde(rename = "projected-field-ids")]
    pub projected_field_ids: Vec<i32>,
    #[serde(rename = "projected-field-names")]
    pub projected_field_names: Vec<String>,
    #[serde(rename = "metrics")]
    pub metrics: std::collections::HashMap<String, MetricResult>,
    #[serde(rename = "metadata", skip_serializing_if = "Option::is_none")]
    pub metadata: Option<std::collections::HashMap<String, String>>,
}

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct CommitReport {
    #[serde(rename = "table-name")]
    pub table_name: String,
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    #[serde(rename = "sequence-number")]
    pub sequence_number: i64,
    #[serde(rename = "operation")]
    pub operation: String,
    #[serde(rename = "metrics")]
    pub metrics: std::collections::HashMap<String, MetricResult>,
    #[serde(rename = "metadata", skip_serializing_if = "Option::is_none")]
    pub metadata: Option<std::collections::HashMap<String, String>>,
}
