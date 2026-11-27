use std::{future::Future, ops::ControlFlow, time::Duration};

/// Runs `work` and calls `heartbeat` every `interval` until `work` completes, or until `heartbeat`
/// breaks. Returns the output of `work`, or `None` if `heartbeat` ended it.
pub(crate) async fn with_heartbeat<T, H>(
    work: impl Future<Output = T>,
    interval: Duration,
    mut heartbeat: impl FnMut() -> H,
) -> Option<T>
where
    H: Future<Output = ControlFlow<()>>,
{
    let mut work = std::pin::pin!(work);
    let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
    // The running heartbeat. `work` keeps running while it is pending.
    let mut beat = None;
    loop {
        tokio::select! {
            output = &mut work => return Some(output),
            _ = interval.tick(), if beat.is_none() => beat = Some(Box::pin(heartbeat())),
            flow = async { beat.as_mut().expect("the branch runs only with a heartbeat").await },
                if beat.is_some() =>
            {
                beat = None;
                if flow.is_break() {
                    return None;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;

    const INTERVAL: Duration = Duration::from_mins(1);

    #[tokio::test(start_paused = true)]
    async fn long_work_heartbeats_until_it_finishes() {
        let beats = Arc::new(AtomicUsize::new(0));
        let done = with_heartbeat(
            async {
                tokio::time::sleep(INTERVAL * 3 + Duration::from_secs(1)).await;
                "done"
            },
            INTERVAL,
            || {
                let beats = beats.clone();
                async move {
                    beats.fetch_add(1, Ordering::SeqCst);
                    ControlFlow::Continue(())
                }
            },
        )
        .await;

        assert_eq!(done, Some("done"));
        assert_eq!(beats.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn work_ends_when_its_heartbeat_says_so() {
        let start = tokio::time::Instant::now();
        let done = with_heartbeat(
            async {
                tokio::time::sleep(INTERVAL * 3).await;
                "done"
            },
            INTERVAL,
            || async { ControlFlow::Break(()) },
        )
        .await;

        assert_eq!(done, None);
        assert_eq!(start.elapsed(), INTERVAL);
    }

    #[tokio::test(start_paused = true)]
    async fn work_continues_while_its_heartbeat_is_pending() {
        let start = tokio::time::Instant::now();
        let done = tokio::time::timeout(
            INTERVAL * 10,
            with_heartbeat(
                async {
                    tokio::time::sleep(INTERVAL * 2).await;
                    "done"
                },
                INTERVAL,
                std::future::pending::<ControlFlow<()>>,
            ),
        )
        .await
        .expect("the work waited for its heartbeat");

        assert_eq!(done, Some("done"));
        assert_eq!(start.elapsed(), INTERVAL * 2);
    }
}
