// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2020 Datadog, Inc.
//
//! Similar to a [`std::sync::Barrier`] but provides [`Latch::cancel`] which a
//! failed thread can use to cancel the `Latch`.
//! [`Latch::wait`] and [`Latch::arrive_and_wait`] return a [`LatchState`] to
//! determine whether the state is `Ready` or `Canceled`.
//!
//! The implementation is intended for multi-threaded rather than task local
//! use.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Condvar, Mutex,
};

#[derive(Clone, Debug)]
pub(crate) struct Latch {
    inner: Arc<LatchInner>,
}

#[derive(Debug)]
struct LatchInner {
    count: AtomicUsize,
    state: Mutex<LatchState>,
    cv: Condvar,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LatchState {
    Pending,
    Ready,
    Canceled,
}

impl Latch {
    /// Create a new `Latch` with given count.
    pub fn new(count: usize) -> Self {
        let state = if 0 < count {
            LatchState::Pending
        } else {
            LatchState::Ready
        };

        let inner = LatchInner {
            count: AtomicUsize::new(count),
            state: Mutex::new(state),
            cv: Condvar::new(),
        };

        Self {
            inner: Arc::new(inner),
        }
    }

    /// If the counter's current value is greater than or equal to `n`, this
    /// method decrements the counter by 'n' and returns an `Ok` containing
    /// the previous value.  Otherwise, this method returns an `Err` with
    /// the previous value.  This method's behavior is independent of the
    /// `LatchState` (e.g. it may return `Ok(0)` even if the state is `Canceled`
    /// if `n == 0`).
    ///
    /// The method does not synchronize with other threads, so while setting the
    /// counter to 0 with this method (or checking its value with
    /// `count_down(0)`) is indicative that the state is either `Ready` or
    /// `Canceled`, other data may not yet be synchronized with other threads.
    pub fn count_down(&self, n: usize) -> Result<usize, usize> {
        #[allow(clippy::unnecessary_lazy_evaluations)]
        self.update(LatchState::Ready, |v| (v >= n).then(|| v - n))
    }

    /// Cancels the latch.  Other threads will no longer wait.  If this call
    /// caused a Cancellation, it returns `Ok` with the previous counter value.
    /// Otherwise, it returns an `Err` with the `LatchState`.
    ///
    /// The method does not synchronize with other threads.
    pub fn cancel(&self) -> Result<usize, LatchState> {
        self.update(LatchState::Canceled, |v| (v != 0).then_some(0))
            .map_err(|_| self.wait())
    }

    /// Wait for `Ready` or `Canceled`.  Synchronizes with other threads via an
    /// internal `Mutex`.
    #[must_use = "check if latch was canceled"]
    pub fn wait(&self) -> LatchState {
        *self
            .inner
            .cv
            .wait_while(self.lock(), |s| matches!(s, LatchState::Pending))
            .expect("unreachable: poisoned mutex")
    }

    /// Decrement the counter by one and wait for `Ready` or `Canceled`.
    /// Synchronizes with other threads via an internal `Mutex`.
    #[must_use = "check if latch was canceled"]
    pub fn arrive_and_wait(&self) -> LatchState {
        self.count_down(1).ok();
        self.wait()
    }

    /// Update the counter based on the provided closure `f` which receives as
    /// input the current value of the counter and should return `Some(new)`
    /// or `None` if no change should be made. The provided closure is
    /// called repeatedly until it either succeeds in updating the `Latch`'s
    /// internal atomic counter or returns `None`.  If the updated counter value
    /// is `0`, then `LatchState` will be changed to `state_if_zero`.  The
    /// `update` method returns an `Ok` of the previous counter value if the
    /// closure returned `Some(new)` or an `Err` of the unchanged value
    /// otherwise.
    fn update<F>(&self, state_if_zero: LatchState, mut f: F) -> Result<usize, usize>
    where
        F: FnMut(usize) -> Option<usize>,
    {
        let mut new = None;
        let f_observe = |cur| {
            new = f(cur);
            new
        };

        // the Mutex synchronizes, so using `Ordering::Relaxed` here
        let res = self
            .inner
            .count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, f_observe);

        if let Some(0) = new {
            self.set_state(state_if_zero);
        }

        res
    }

    fn set_state(&self, state: LatchState) {
        *self.lock() = state;
        self.inner.cv.notify_all();
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, LatchState> {
        self.inner
            .state
            .lock()
            .expect("unreachable: poisoned mutex")
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn cancel() {
        let n = 1 << 10;
        let cxl_ids = (n / 2..n + 1).step_by(n / 2 / 5).collect::<HashSet<_>>();
        assert_eq!(6, cxl_ids.len());

        let (w, a, t) = helper(cxl_ids, n);
        assert_eq!(n - 5 - 1, w.len());
        assert_eq!(1, a.len());
        assert_eq!(5, t.len());
        w.into_iter().for_each(|s| {
            assert_eq!(s, LatchState::Canceled);
        });
    }

    #[test]
    fn ready() {
        let n = 1 << 10;
        let cxl_ids = HashSet::new();
        assert_eq!(0, cxl_ids.len());

        let (w, a, t) = helper(cxl_ids, n);
        assert_eq!(n, w.len());
        assert_eq!(0, a.len());
        assert_eq!(0, t.len());
        w.into_iter().for_each(|s| {
            assert_eq!(s, LatchState::Ready);
        });
    }

    fn helper(
        cxl_ids: HashSet<usize>,
        count: usize,
    ) -> (Vec<LatchState>, Vec<usize>, Vec<LatchState>) {
        let latch = Latch::new(count);
        let cxl_ids = Arc::new(cxl_ids);
        let res = (0..count)
            .map(|id| {
                std::thread::spawn({
                    let l = Latch::clone(&latch);
                    let cxl_ids = Arc::clone(&cxl_ids);
                    move || {
                        if !cxl_ids.contains(&id) {
                            Ok(l.arrive_and_wait())
                        } else {
                            Err(l.cancel())
                        }
                    }
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().unwrap());

        let mut waits = Vec::new();
        let mut cxls = Vec::new();
        let mut cxl_attempts = Vec::new();
        for r in res {
            match r {
                Ok(w) => waits.push(w),
                Err(Ok(id)) => cxls.push(id),
                Err(Err(s)) => cxl_attempts.push(s),
            }
        }

        (waits, cxls, cxl_attempts)
    }
}
