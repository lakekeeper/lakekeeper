use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct FalseExpression {
    #[serde(rename = "type")]
    pub r#type: String,
}

impl FalseExpression {
    pub fn new(r#type: String) -> FalseExpression {
        FalseExpression { r#type }
    }
}
