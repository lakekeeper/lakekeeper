use std::{
    num::NonZeroUsize,
    sync::{Arc, LazyLock},
};

use tokio::sync::Semaphore;

use crate::operations::error::{InternalError, RetryableErrorKind};

/// Parses that may run at once in this process: half the cores when it also serves the API, so
/// parsing cannot starve requests, all cores in a worker-only process.
static PARSE_SLOTS: LazyLock<Arc<Semaphore>> = LazyLock::new(|| {
    let cores = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
    Arc::new(Semaphore::new(parse_slots(
        cores,
        crate::CONFIG.serve_http_api,
    )))
});

fn parse_slots(cores: usize, serves_api: bool) -> usize {
    if serves_api {
        (cores / 2).max(1)
    } else {
        cores
    }
}

/// Runs CPU-bound `parse` of the file at `path` on the blocking pool once a parse slot is free.
///
/// The slot stays held until `parse` returns, even if the caller is dropped meanwhile. Never call
/// this from inside `parse`: a nested wait for a slot can deadlock.
///
/// # Errors
/// Returns a permanent [`InternalError`] if `parse` panicked, a temporary one if the runtime shut
/// down first.
pub async fn run_parse<R: Send + 'static>(
    path: &str,
    parse: impl FnOnce() -> R + Send + 'static,
) -> Result<R, InternalError> {
    let permit = Arc::clone(&PARSE_SLOTS)
        .acquire_owned()
        .await
        .map_err(|e| {
            InternalError::new(
                format!("Parse slots are closed: {e}"),
                RetryableErrorKind::Temporary,
            )
        })?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        parse()
    })
    .await
    .map_err(|e| {
        let retryable = if e.is_cancelled() {
            RetryableErrorKind::Temporary
        } else {
            RetryableErrorKind::Permanent
        };
        InternalError::new(format!("Parsing `{path}` failed: {e}"), retryable)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_process_that_serves_the_api_keeps_half_the_cores() {
        assert_eq!(parse_slots(8, true), 4);
        assert_eq!(parse_slots(3, true), 1);
        assert_eq!(parse_slots(1, true), 1);
        assert_eq!(parse_slots(8, false), 8);
    }

    #[tokio::test]
    async fn a_panicking_parse_is_a_permanent_error_naming_the_file() {
        let err = run_parse("s3://b/t/m.avro", || -> u8 { panic!("corrupt") })
            .await
            .unwrap_err();
        assert_eq!(err.retryable, RetryableErrorKind::Permanent);
        // The task id in the join error varies.
        let message = err.to_string();
        assert!(
            message.contains("Parsing `s3://b/t/m.avro` failed: task ")
                && message.ends_with(" panicked with message \"corrupt\""),
            "{message}"
        );
    }

    #[tokio::test]
    async fn returns_the_parse_result() {
        assert_eq!(run_parse("s3://b/t/m.avro", || 42).await.unwrap(), 42);
    }
}
