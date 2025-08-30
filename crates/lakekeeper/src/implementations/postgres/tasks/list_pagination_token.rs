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
        let (task_str, rest) = s.split_once(':').ok_or_else(|| {
            ErrorModel::bad_request(
                "Invalid page token for list tasks",
                "InvalidPageToken",
                None,
            )
            .append_detail("Expected format: <task_id>:<attempt>")
        })?;

        // Disallow any additional ':' segments.
        if rest.contains(':') {
            return Err(ErrorModel::bad_request(
                "Invalid page token for list tasks",
                "InvalidPageToken",
                None,
            )
            .append_detail("Too many segments; expected exactly one ':'"));
        }

        let task_uuid = Uuid::parse_str(task_str).map_err(|e| {
            ErrorModel::bad_request(
                format!("Invalid UUID in page token: {e}"),
                "InvalidPageToken",
                None,
            )
        })?;
        let attempt = rest.parse::<i32>().map_err(|e| {
            ErrorModel::bad_request(
                format!("Invalid attempt number in page token: {e}"),
                "InvalidPageToken",
                None,
            )
        })?;
        if attempt < 0 {
            return Err(ErrorModel::bad_request(
                "Attempt must be non-negative",
                "InvalidPageToken",
                None,
            ));
        }

        Ok(Self {
            task_id: task_uuid.into(),
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

    #[test]
    fn test_token_rejects_extra_segments() {
        assert!("00000000-0000-0000-0000-000000000000:1:2"
            .parse::<ListTasksPaginationToken>()
            .is_err());
    }

    #[test]
    fn test_token_rejects_negative_attempt() {
        let id = Uuid::nil();
        let s = format!("{id}:-1");
        assert!(s.parse::<ListTasksPaginationToken>().is_err());
    }
}
