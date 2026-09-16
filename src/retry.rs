use std::time::{Duration, Instant};

use tonic::{Code, Status};

use crate::error::{Error, Result};

const INITIAL_BACKOFF: Duration = Duration::from_millis(50);
const MAX_BACKOFF: Duration = Duration::from_secs(2);

pub(crate) fn transient(status: &Status) -> bool {
    matches!(
        status.code(),
        Code::Unavailable | Code::ResourceExhausted | Code::Aborted
    )
}

pub(crate) fn ready_transient(status: &Status) -> bool {
    transient(status) || status.code() == Code::DeadlineExceeded
}

pub(crate) async fn retry<T, F, Fut>(deadline: Instant, mut f: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<T, Status>>,
{
    retry_if(deadline, transient, &mut f).await
}

pub(crate) async fn retry_if<T, F, Fut>(
    deadline: Instant,
    is_transient: fn(&Status) -> bool,
    f: &mut F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<T, Status>>,
{
    let mut backoff = INITIAL_BACKOFF;
    let mut last: Option<Status> = None;
    loop {
        if Instant::now() >= deadline {
            return Err(last
                .map(rpc)
                .unwrap_or_else(|| Error::new("clusdr: request timeout")));
        }
        match f().await {
            Ok(value) => return Ok(value),
            Err(status) if is_transient(&status) => {
                last = Some(status);
            }
            Err(status) => return Err(rpc(status)),
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(last
                .map(rpc)
                .unwrap_or_else(|| Error::new("clusdr: request timeout")));
        }
        tokio::time::sleep(backoff.min(remaining)).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

pub(crate) fn rpc(status: Status) -> Error {
    Error::new(status.to_string())
}

pub(crate) fn remaining(deadline: Instant) -> Result<Duration> {
    let left = deadline.saturating_duration_since(Instant::now());
    if left.is_zero() {
        Err(Error::new("clusdr: request timeout"))
    } else {
        Ok(left)
    }
}

pub(crate) async fn sleep_ctx(cancel: &tokio_util::sync::CancellationToken, d: Duration) -> bool {
    tokio::select! {
        _ = cancel.cancelled() => false,
        _ = tokio::time::sleep(d) => true,
    }
}
