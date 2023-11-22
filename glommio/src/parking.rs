// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
//! Thread parking
//!
//! This module exposes a similar API as [`parking`][docs-parking]. However, it
//! is adapted to be used in a thread-per-core environment. Because there is a
//! single thread per reactor, there is no way to forcibly unpark a parked
//! reactor, so we don't expose an unpark function.
//!
//! Also, we expose a function called `poll_io` aside from park.
//!
//! `poll_io` will poll for I/O, but not ever sleep. This is useful when we know
//! there are more events and are just doing I/O, so we don't starve anybody.
//!
//! park() is different in that it may sleep if no new events arrive. It
//! essentially means: "I, the executor, have nothing else to do. If you don't
//! find work feel free to sleep"
//!
//! Executors may use this mechanism to go to sleep when idle and wake up when
//! more work is scheduled. By waking tasks blocked on I/O and then running
//! those tasks on the same thread, no thread context switch is necessary when
//! going between task execution and I/O.

use std::{
    fmt, io,
    panic::{RefUnwindSafe, UnwindSafe},
    rc::Rc,
    time::Duration,
};

/// Waits for a notification.
pub(crate) struct Parker {
    inner: Rc<Inner>,
}

impl UnwindSafe for Parker {}
impl RefUnwindSafe for Parker {}

impl Parker {
    /// Creates a new parker.
    pub(crate) fn new() -> Parker {
        // Ensure `Reactor` is initialized now to prevent it from being initialized in
        // `Drop`.
        Parker {
            inner: Rc::new(Inner {}),
        }
    }

    /// Blocks until notified and then goes back into sleeping state.
    pub(crate) fn park(&self) -> io::Result<bool> {
        self.inner.park(|| None)
    }

    /// Performs non-sleepable poll and installs a preemption timeout into the
    /// ring with `Duration`. A value of zero means we are not interested in
    /// installing a preemption timer. Tasks executing in the CPU right after
    /// this will be able to check if the timer has elapsed and yield the
    /// CPU if that is the case.
    pub(crate) fn poll_io(&self, timeout: impl Fn() -> Option<Duration>) -> io::Result<bool> {
        self.inner.park(timeout)
    }
}

impl Drop for Parker {
    fn drop(&mut self) {}
}

impl Default for Parker {
    fn default() -> Parker {
        Parker::new()
    }
}

impl fmt::Debug for Parker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("Parker { .. }")
    }
}
struct Inner {}

impl Inner {
    fn park(&self, timeout: impl Fn() -> Option<Duration>) -> io::Result<bool> {
        crate::executor().reactor().react(timeout)
    }
}
