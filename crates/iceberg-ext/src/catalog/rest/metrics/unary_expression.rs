use crate::catalog::rest::metrics::term::Term;
use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct UnaryExpression {
    #[serde(rename = "type")]
    pub r#type: String,
    #[serde(rename = "term")]
    pub term: Box<Term>,
    #[serde(rename = "value")]
    pub value: serde_json::Value,
}

impl UnaryExpression {
    pub fn new(r#type: String, term: Term, value: serde_json::Value) -> UnaryExpression {
        UnaryExpression {
            r#type,
            term: Box::new(term),
            value,
        }
    }
}
