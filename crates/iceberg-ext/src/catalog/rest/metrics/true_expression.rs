use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct TrueExpression {
    #[serde(rename = "type")]
    pub r#type: String,
}

impl TrueExpression {
    pub fn new(r#type: String) -> TrueExpression {
        TrueExpression { r#type }
    }
}
