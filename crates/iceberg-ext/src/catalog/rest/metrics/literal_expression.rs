use crate::catalog::rest::metrics::term::Term;
use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct LiteralExpression {
    #[serde(rename = "type")]
    pub r#type: String,
    #[serde(rename = "term")]
    pub term: Box<Term>,
    #[serde(rename = "value")]
    pub value: serde_json::Value,
}

impl LiteralExpression {
    pub fn new(r#type: String, term: Term, value: serde_json::Value) -> LiteralExpression {
        LiteralExpression {
            r#type,
            term: Box::new(term),
            value,
        }
    }
}
