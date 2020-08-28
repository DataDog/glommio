use core::fmt;
use core::future::Future;
use core::marker::{PhantomData, Unpin};
use core::pin::Pin;
use core::ptr::NonNull;
use core::task::{Context, Poll, Waker};

use crate::task::header::Header;
use crate::task::state::*;

/// A handle that awaits the result of a task.
///
/// This type is a future that resolves to an `Option<R>` where:
///
/// * `None` indicates the task has panicked or was canceled.
/// * `Some(result)` indicates the task has completed with `result` of type `R`.
pub struct JoinHandle<R, T> {
    /// A raw task pointer.
    pub(crate) raw_task: NonNull<()>,

    /// A marker capturing generic types `R` and `T`.
    pub(crate) _marker: PhantomData<(R, T)>,
}

impl<R, T> Unpin for JoinHandle<R, T> {}

impl<R, T> JoinHandle<R, T> {
    /// Cancels the task.
    ///
    /// If the task has already completed, calling this method will have no effect.
    ///
    /// When a task is canceled, its future will not be polled again.
    pub fn cancel(&self) {
        let ptr = self.raw_task.as_ptr();
        let mut header = ptr as *mut Header;

        unsafe {
            let state = (*header).state;

            // If the task has been completed or closed, it can't be canceled.
            if state & (COMPLETED | CLOSED) != 0 {
                return;
            }

            // If the task is not scheduled nor running, we'll need to schedule it.
            let new = if state & (SCHEDULED | RUNNING) == 0 {
                (state | SCHEDULED | CLOSED) + REFERENCE
            } else {
                state | CLOSED
            };

            // Mark the task as closed.
            (*header).state = new;
            // If the task is not scheduled nor running, schedule it one more time so
            // that its future gets dropped by the executor.
            if state & (SCHEDULED | RUNNING) == 0 {
                ((*header).vtable.schedule)(ptr);
            }

            // Notify the awaiter that the task has been closed.
            if state & AWAITER != 0 {
                (*header).notify(None);
            }
        }
    }

    /// Returns a reference to the tag stored inside the task.
    pub fn tag(&self) -> &T {
        let offset = Header::offset_tag::<T>();
        let ptr = self.raw_task.as_ptr();

        unsafe {
            let raw = (ptr as *mut u8).add(offset) as *const T;
            &*raw
        }
    }

    /// Returns a waker associated with the task.
    pub fn waker(&self) -> Waker {
        let ptr = self.raw_task.as_ptr();
        let header = ptr as *const Header;

        unsafe {
            let raw_waker = ((*header).vtable.clone_waker)(ptr);
            Waker::from_raw(raw_waker)
        }
    }
}

impl<R, T> Drop for JoinHandle<R, T> {
    fn drop(&mut self) {
        let ptr = self.raw_task.as_ptr();
        let mut header = ptr as *mut Header;

        // A place where the output will be stored in case it needs to be dropped.
        let mut output = None;
        unsafe {
            let state = (*header).state;

            // If the task has been completed but not yet closed, that means its output
            // must be dropped.
            if state & COMPLETED != 0 && state & CLOSED == 0 {
                // Mark the task as closed in order to grab its output.
                (*header).state |= CLOSED;
                // Read the output.
                output = Some((((*header).vtable.get_output)(ptr) as *mut R).read());
            } else {
                // If this is the last reference to the task and it's not closed, then
                // close it and schedule one more time so that its future gets dropped by
                // the executor.
                let new = if state & (!(REFERENCE - 1) | CLOSED) == 0 {
                    SCHEDULED | CLOSED | REFERENCE
                } else {
                    state & !HANDLE
                };

                // Unset the handle flag.
                (*header).state = new;
                // If this is the last reference to the task, we need to either
                // schedule dropping its future or destroy it.
                if state & !(REFERENCE - 1) == 0 {
                    if state & CLOSED == 0 {
                        ((*header).vtable.schedule)(ptr);
                    } else {
                        ((*header).vtable.destroy)(ptr);
                    }
                }
                (*header).state = SCHEDULED | REFERENCE;
            }
        }

        // Drop the output if it was taken out of the task.
        drop(output);
    }
}

impl<R, T> Future for JoinHandle<R, T> {
    type Output = Option<R>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let ptr = self.raw_task.as_ptr();
        let mut header = ptr as *mut Header;

        unsafe {
            let mut state = (*header).state;

            loop {
                // If the task has been closed, notify the awaiter and return `None`.
                if state & CLOSED != 0 {
                    // If the task is scheduled or running, we need to wait until its future is
                    // dropped.
                    if state & (SCHEDULED | RUNNING) != 0 {
                        // Replace the waker with one associated with the current task.
                        (*header).register(cx.waker());

                        // Reload the state after registering. It is possible changes occurred just
                        // before registration so we need to check for that.
                        state = (*header).state;

                        // If the task is still scheduled or running, we need to wait because its
                        // future is not dropped yet.
                        if state & (SCHEDULED | RUNNING) != 0 {
                            return Poll::Pending;
                        }
                    }

                    // Even though the awaiter is most likely the current task, it could also be
                    // another task.
                    (*header).notify(Some(cx.waker()));
                    return Poll::Ready(None);
                }

                // If the task is not completed, register the current task.
                if state & COMPLETED == 0 {
                    // Replace the waker with one associated with the current task.
                    (*header).register(cx.waker());

                    // Reload the state after registering. It is possible that the task became
                    // completed or closed just before registration so we need to check for that.
                    state = (*header).state;

                    // If the task has been closed, restart.
                    if state & CLOSED != 0 {
                        continue;
                    }

                    // If the task is still not completed, we're blocked on it.
                    if state & COMPLETED == 0 {
                        return Poll::Pending;
                    }
                }

                // Since the task is now completed, mark it as closed in order to grab its output.
                (*header).state |= CLOSED;
                // Notify the awaiter. Even though the awaiter is most likely the current
                // task, it could also be another task.
                if state & AWAITER != 0 {
                    (*header).notify(Some(cx.waker()));
                }

                // Take the output from the task.
                let output = ((*header).vtable.get_output)(ptr) as *mut R;
                return Poll::Ready(Some(output.read()));
            }
        }
    }
}

impl<R, T> fmt::Debug for JoinHandle<R, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ptr = self.raw_task.as_ptr();
        let header = ptr as *const Header;

        f.debug_struct("JoinHandle")
            .field("header", unsafe { &(*header) })
            .finish()
    }
}
