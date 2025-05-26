use crate::catalog::rest::metrics::and_or_expression::AndOrExpression;
use crate::catalog::rest::metrics::false_expression::FalseExpression;
use crate::catalog::rest::metrics::literal_expression::LiteralExpression;
use crate::catalog::rest::metrics::not_expression::NotExpression;
use crate::catalog::rest::metrics::set_expression::SetExpression;
use crate::catalog::rest::metrics::true_expression::TrueExpression;
use crate::catalog::rest::metrics::unary_expression::UnaryExpression;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Expression {
    TrueExpression(Box<TrueExpression>),
    FalseExpression(Box<FalseExpression>),
    AndOrExpression(Box<AndOrExpression>),
    NotExpression(Box<NotExpression>),
    SetExpression(Box<SetExpression>),
    LiteralExpression(Box<LiteralExpression>),
    UnaryExpression(Box<UnaryExpression>),
}

impl Default for Expression {
    fn default() -> Self {
        Self::TrueExpression(Default::default())
    }
}
