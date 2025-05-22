use crate::catalog::rest::metrics::term::Term;
use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct SetExpression {
    #[serde(rename = "type")]
    pub r#type: String,
    #[serde(rename = "term")]
    pub term: Box<Term>,
    #[serde(rename = "values")]
    pub values: Vec<serde_json::Value>,
}

impl SetExpression {
    pub fn new(r#type: String, term: Term, values: Vec<serde_json::Value>) -> SetExpression {
        SetExpression {
            r#type,
            term: Box::new(term),
            values,
        }
    }
}
