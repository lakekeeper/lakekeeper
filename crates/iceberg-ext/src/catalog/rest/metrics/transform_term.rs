use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct TransformTerm {
    #[serde(rename = "type")]
    pub r#type: Type,
    #[serde(rename = "transform")]
    pub transform: String,
    #[serde(rename = "term")]
    pub term: String,
}

impl TransformTerm {
    pub fn new(r#type: Type, transform: String, term: String) -> TransformTerm {
        TransformTerm {
            r#type,
            transform,
            term,
        }
    }
}
///
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum Type {
    #[serde(rename = "transform")]
    Transform,
}

impl Default for Type {
    fn default() -> Type {
        Self::Transform
    }
}
