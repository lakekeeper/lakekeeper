use std::time::Duration;

pub(crate) async fn retry_fn<T, Z, E>(f: impl Fn() -> T) -> crate::api::Result<Z, E>
where
    T: std::future::Future<Output = crate::api::Result<Z, E>>,
{
    tryhard::retry_fn(f)
        .retries(3)
        .exponential_backoff(Duration::from_millis(100))
        .max_delay(Duration::from_secs(1))
        .await
}
