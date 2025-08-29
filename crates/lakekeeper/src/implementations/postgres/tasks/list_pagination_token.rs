use iceberg_ext::catalog::rest::ErrorModel;
use uuid::Uuid;

use crate::service::task_queue::TaskId;

pub(super) struct ListTasksPaginationToken {
    pub(super) task_id: TaskId,
    pub(super) attempt: i32,
}

impl std::fmt::Display for ListTasksPaginationToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.task_id, self.attempt)
    }
}

impl std::str::FromStr for ListTasksPaginationToken {
    type Err = ErrorModel;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split(':');
        let task_id = parts
            .next()
            .ok_or_else(|| {
                ErrorModel::bad_request(
                    "Invalid page token for list tasks",
                    "InvalidPageToken",
                    None,
                )
                .append_detail("Expected format: <task_id>:<attempt>")
            })
            .and_then(|s| {
                Uuid::parse_str(s).map_err(|e| {
                    ErrorModel::bad_request(
                        format!("Invalid UUID in page token: {e}"),
                        "InvalidPageToken",
                        None,
                    )
                })
            })?;
        let attempt = parts
            .next()
            .ok_or_else(|| {
                ErrorModel::bad_request(
                    "Invalid page token for list tasks",
                    "InvalidPageToken",
                    None,
                )
                .append_detail("Expected format: <task_id>:<attempt>")
            })
            .and_then(|s| {
                s.parse::<i32>().map_err(|e| {
                    ErrorModel::bad_request(
                        format!("Invalid attempt number in page token: {e}"),
                        "InvalidPageToken",
                        None,
                    )
                })
            })?;
        Ok(Self {
            task_id: task_id.into(),
            attempt,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_tasks_pagination_token_parse() {
        let token = ListTasksPaginationToken {
            task_id: Uuid::new_v4().into(),
            attempt: 3,
        };

        let token_str = token.to_string();
        let parsed_token = token_str.parse::<ListTasksPaginationToken>().unwrap();
        assert_eq!(token.task_id, parsed_token.task_id);
        assert_eq!(token.attempt, parsed_token.attempt);
    }
}
