// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
//! glommio::timer is a module that provides timing related primitives.
mod timer_impl;

pub use timer_impl::{Timer, TimerActionOnce, TimerActionRepeat};

/// Sleep for some time.
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
