use crate::catalog::rest::metrics::transform_term::TransformTerm;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Term {
    Reference(String),
    TransformTerm(Box<TransformTerm>),
}

impl Default for Term {
    fn default() -> Self {
        Self::Reference(Default::default())
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
