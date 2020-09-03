// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use crate::parking::Reactor;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

/// A timer that expires after a duration of time.
///
/// Timers are futures that output the [`Instant`] at which they fired.
///
/// # Examples
///
/// Sleep for 1 second:
///
/// ```
/// use async_io::Timer;
/// use std::time::Duration;
///
/// async fn sleep(dur: Duration) {
///     Timer::new(dur).await;
/// }
///
/// # futures_lite::future::block_on(async {
/// sleep(Duration::from_secs(1)).await;
/// # });
/// ```
#[derive(Debug)]
pub struct Timer {
    /// This timer's ID and last waker that polled it.
    ///
    /// When this field is set to `None`, this timer is not registered in the reactor.
    id_and_waker: Option<(usize, Waker)>,

    /// When this timer fires.
    when: Instant,
}

impl Timer {
    /// Creates a timer that expires after the given duration of time.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_io::Timer;
    /// use std::time::Duration;
    ///
    /// # futures_lite::future::block_on(async {
    /// Timer::new(Duration::from_secs(1)).await;
    /// # });
    /// ```
    pub fn new(dur: Duration) -> Timer {
        Timer {
            id_and_waker: None,
            when: Instant::now() + dur,
        }
    }

    /// Resets the timer to expire after the new duration of time.
    ///
    /// Note that resetting a timer is different from creating a new timer because
    /// [`reset()`][`Timer::reset()`] does not remove the waker associated with the task that is
    /// polling the timer.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_io::Timer;
    /// use std::time::Duration;
    ///
    /// # futures_lite::future::block_on(async {
    /// let mut t = Timer::new(Duration::from_secs(1));
    /// t.reset(Duration::from_millis(100));
    /// # });
    /// ```
    pub fn reset(&mut self, dur: Duration) {
        if let Some((id, _)) = self.id_and_waker.as_ref() {
            // Deregister the timer from the reactor.
            Reactor::get().remove_timer(self.when, *id);
        }

        // Update the timeout.
        self.when = Instant::now() + dur;

        if let Some((id, waker)) = self.id_and_waker.as_mut() {
            // Re-register the timer with the new timeout.
            *id = Reactor::get().insert_timer(self.when, waker);
        }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        if let Some((id, _)) = self.id_and_waker.take() {
            // Deregister the timer from the reactor.
            Reactor::get().remove_timer(self.when, id);
        }
    }
}

impl Future for Timer {
    type Output = Instant;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Check if the timer has already fired.
        if Instant::now() >= self.when {
            if let Some((id, _)) = self.id_and_waker.take() {
                // Deregister the timer from the reactor.
                Reactor::get().remove_timer(self.when, id);
            }
            Poll::Ready(self.when)
        } else {
            match &self.id_and_waker {
                None => {
                    // Register the timer in the reactor.
                    let id = Reactor::get().insert_timer(self.when, cx.waker());
                    self.id_and_waker = Some((id, cx.waker().clone()));
                }
                Some((id, w)) if !w.will_wake(cx.waker()) => {
                    // Deregister the timer from the reactor to remove the old waker.
                    Reactor::get().remove_timer(self.when, *id);

                    // Register the timer in the reactor with the new waker.
                    let id = Reactor::get().insert_timer(self.when, cx.waker());
                    self.id_and_waker = Some((id, cx.waker().clone()));
                }
                Some(_) => {}
            }
            Poll::Pending
        }
    }
}

#[test]
fn basic_timer_works() {
    test_executor!(
        async move {
            let now = Instant::now();
            Timer::new(Duration::from_millis(100)).await;
            assert!(now.elapsed().as_millis() >= 100)
        }
    );
}
