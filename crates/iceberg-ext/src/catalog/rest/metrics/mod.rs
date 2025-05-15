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

// #[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
// pub struct MetricResult {
//     #[serde(rename = "unit")]
//     pub unit: String,
//     #[serde(rename = "value")]
//     pub value: i64,
//     #[serde(rename = "time-unit")]
//     pub time_unit: String,
//     #[serde(rename = "count")]
//     pub count: i64,
//     #[serde(rename = "total-duration")]
//     pub total_duration: i64,
// }
//
// #[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
// pub struct AndOrExpression {
//     #[serde(rename = "type")]
//     pub r#type: String,
//     #[serde(rename = "left")]
//     pub left: Box<Expression>,
//     #[serde(rename = "right")]
//     pub right: Box<Expression>,
// }
//
// #[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
// pub struct NotExpression {
//     #[serde(rename = "type")]
//     pub r#type: String,
//     #[serde(rename = "child")]
//     pub child: Box<Expression>,
// }
//
// #[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
// pub struct SetExpression {
//     #[serde(rename = "type")]
//     pub r#type: String,
//     #[serde(rename = "term")]
//     pub term: Box<Term>,
//     #[serde(rename = "values")]
//     pub values: Vec<serde_json::Value>,
// }
//
//
// #[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
// pub enum Type {
//     #[serde(rename = "transform")]
//     Transform,
// }
//
//
// #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
// #[serde(untagged)]
// pub enum Term {
//     Reference(String),
//     TransformTerm(Box<TransformTerm>),
// }
//
// #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
// #[serde(untagged)]
// pub enum Expression {
//     TrueExpression(Box<TrueExpression>),
//     FalseExpression(Box<FalseExpression>),
//     AndOrExpression(Box<AndOrExpression>),
//     NotExpression(Box<NotExpression>),
//     SetExpression(Box<models::SetExpression>),
//     LiteralExpression(Box<models::LiteralExpression>),
//     UnaryExpression(Box<models::UnaryExpression>),
// }
//
//
// #[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
// pub struct TrueExpression {
//     #[serde(rename = "type")]
//     pub r#type: String,
// }
//
// #[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
// pub struct FalseExpression {
//     #[serde(rename = "type")]
//     pub r#type: String,
// }
//
//
//
// impl Default for Expression {
//     fn default() -> Self {
//         Self::TrueExpression(Default::default())
//     }
// }
//
//
//
//
// impl Default for Term {
//     fn default() -> Self {
//         Self::Reference(Default::default())
//     }
// }
// ///
//
// impl Default for Type {
//     fn default() -> Type {
//         Self::Transform
//     }
// }
//
