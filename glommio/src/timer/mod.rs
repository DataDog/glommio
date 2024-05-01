// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
//! glommio::timer is a module that provides timing related primitives.
mod timer_impl;

use std::{future::Future, time::Duration};
pub use timer_impl::{Timer, TimerActionOnce, TimerActionRepeat};

type Result<T> = crate::Result<T, ()>;

/// Sleep for some time on the current task. Explicit sleeps can introduce undesirable delays if not used correctly.
/// Consider using [crate::timer::timeout] instead if you are implementing timeout-like semantics or
/// [crate::timer::TimerActionOnce] if you need to schedule a future for some later date in the future without needing
/// to await.
///
/// ```
/// use glommio::{timer::sleep, LocalExecutor};
/// use std::time::Duration;
///
/// let ex = LocalExecutor::default();
///
/// ex.run(async {
///     sleep(Duration::from_millis(100)).await;
/// });
/// ```
pub async fn sleep(wait: std::time::Duration) {
    Timer::new(wait).await;
}

/// Executes a future with a specified timeout
///
/// Returns a `Result`, with `Ok` if the future ran to completion
/// or a [`GlommioError::TimedOut`] error if the timeout was reached
///
/// ```
/// # use glommio::{
/// #    timer::{timeout, Timer},
/// #    LocalExecutor,
/// # };
/// # use std::time::Duration;
/// # let ex = LocalExecutor::default();
/// # ex.run(async {
/// timeout(Duration::from_millis(1), async move {
///     // this future will wait for 100ms, but won't complete, as the timeout is 1ms
///     Timer::new(Duration::from_millis(100)).await;
///     Ok(())
/// })
/// .await;
/// # });
/// ```
///
/// [`GlommioError::TimedOut`]: crate::GlommioError::TimedOut
pub async fn timeout<F, T>(dur: Duration, f: F) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    timer_impl::Timeout::new(f, dur).await
}
