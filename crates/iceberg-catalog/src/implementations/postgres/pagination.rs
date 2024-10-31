use chrono::Utc;
use iceberg_ext::catalog::rest::ErrorModel;
use std::fmt::Display;

#[derive(Debug)]
pub(crate) enum PaginateToken<T> {
    V1(V1PaginateToken<T>),
}

#[derive(Debug)]
pub(crate) struct V1PaginateToken<T> {
    pub(crate) created_at: chrono::DateTime<Utc>,
    pub(crate) id: T,
}

impl<T> Display for PaginateToken<T>
where
    T: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            PaginateToken::V1(V1PaginateToken { created_at, id }) => {
                format!("1&{}&{}", created_at.timestamp_micros(), id)
            }
        };
        write!(f, "{str}")
    }
}

impl<T, Z> TryFrom<&str> for PaginateToken<T>
where
    T: for<'a> TryFrom<&'a str, Error = Z> + Display,
    Z: std::error::Error + Send + Sync + 'static,
{
    type Error = ErrorModel;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let parts = s.split('&').collect::<Vec<_>>();

        match *parts.first().ok_or(parse_error(None))? {
            "1" => match &parts[1..] {
                &[ts, id] => {
                    let created_at = chrono::DateTime::from_timestamp_micros(
                        ts.parse().map_err(|e| parse_error(Some(Box::new(e))))?,
                    )
                    .ok_or(parse_error(None))?;
                    let id = id.try_into().map_err(|e| parse_error(Some(Box::new(e))))?;
                    Ok(PaginateToken::V1(V1PaginateToken { created_at, id }))
                }
                _ => Err(parse_error(None)),
            },
            _ => Err(parse_error(None)),
        }
    }
}

fn parse_error(e: Option<Box<dyn std::error::Error + Send + Sync + 'static>>) -> ErrorModel {
    ErrorModel::bad_request(
        "Invalid paginate token".to_string(),
        "PaginateTokenParseError".to_string(),
        e,
    )
}
