use crate::catalog::rest::metrics::expression::Expression;
use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct NotExpression {
    #[serde(rename = "type")]
    pub r#type: String,
    #[serde(rename = "child")]
    pub child: Box<Expression>,
}

impl NotExpression {
    pub fn new(r#type: String, child: Expression) -> NotExpression {
        NotExpression {
            r#type,
            child: Box::new(child),
        }
    }
}
