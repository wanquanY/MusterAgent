//! Opt-in timings for the durable dispatch owner; no query or request data.
use std::{future::Future, time::Instant};

pub(super) async fn observe<T>(stage: &'static str, operation: impl Future<Output = T>) -> T {
    let started = Instant::now();
    let value = operation.await;
    record(stage, started);
    value
}

fn record(stage: &'static str, started: Instant) {
    tracing::debug!(
        dispatch.prepare.stage = stage,
        dispatch.prepare.duration_us =
            u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
        "Durable dispatch preparation completed"
    );
}
