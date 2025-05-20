use crate::catalog::rest::metrics::expression::Expression;
use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct AndOrExpression {
    #[serde(rename = "type")]
    pub r#type: String,
    #[serde(rename = "left")]
    pub left: Box<Expression>,
    #[serde(rename = "right")]
    pub right: Box<Expression>,
}
