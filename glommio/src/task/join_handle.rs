// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use core::{
    fmt,
    future::Future,
    marker::{PhantomData, Unpin},
    pin::Pin,
    ptr::NonNull,
    task::{Context, Poll},
};

use crate::task::{header::Header, state::*};

/// A handle that awaits the result of a task.
///
/// This type is a future that resolves to an `Option<R>` where:
///
/// * `None` indicates the task has panicked or was canceled.
/// * `Some(result)` indicates the task has completed with `result` of type `R`.
pub struct JoinHandle<R> {
    /// A raw task pointer.
    pub(crate) raw_task: NonNull<()>,

    /// A marker capturing generic types `R`.
    pub(crate) _marker: PhantomData<R>,
}

impl<R> Unpin for JoinHandle<R> {}

impl<R> JoinHandle<R> {
    /// Cancels the task.
    ///
    /// If the task has already completed, calling this method will have no
    /// effect.
    ///
    /// When a task is canceled, its future will not be polled again.
    pub fn cancel(&self) {
        let ptr = self.raw_task.as_ptr();
        let header = ptr as *mut Header;

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

            if state & (SCHEDULED | RUNNING) == 0 {
                ((*header).vtable.schedule)(ptr);
            }

            // Notify the awaiter that the task has been closed.
            if state & AWAITER != 0 {
                (*header).notify(None);
            }
        }
    }
}

impl<R> Drop for JoinHandle<R> {
    fn drop(&mut self) {
        let ptr = self.raw_task.as_ptr();
        let header = ptr as *mut Header;

        // A place where the output will be stored in case it needs to be dropped.
        let mut output = None;

        unsafe {
            // Optimistically assume the `JoinHandle` is being dropped just after creating
            // the task. This is a common case so if the handle is not used, the
            // overhead of it is only one compare-exchange operation.
            if (*header).state == SCHEDULED | HANDLE | REFERENCE {
                (*header).state = SCHEDULED | REFERENCE;
                return;
            }

            let state = (*header).state;
            // If the task has been completed but not yet closed, that means its output
            // must be dropped.
            if state & COMPLETED != 0 && state & CLOSED == 0 {
                // Mark the task as closed in order to grab its output.
                (*header).state |= CLOSED;
                // Read the output.
                output = Some((((*header).vtable.get_output)(ptr) as *mut R).read());

                (*header).state &= !HANDLE;

                // If this is the last reference to the task, we need to destroy it.
                if state & !(REFERENCE - 1) == 0 {
                    ((*header).vtable.destroy)(ptr)
                }
            } else {
                // If this is the last reference to the task and it's not closed, then
                // close it and schedule one more time so that its future gets dropped by
                // the executor.
                let new = if state & (!(REFERENCE - 1) | CLOSED) == 0 {
                    SCHEDULED | CLOSED | REFERENCE
                } else {
                    state & !HANDLE
                };

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
            }
        }

        drop(output);
    }
}

impl<R> Future for JoinHandle<R> {
    type Output = Option<R>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let ptr = self.raw_task.as_ptr();
        let header = ptr as *mut Header;

        unsafe {
            let state = (*header).state;

            // If the task has been closed, notify the awaiter and return `None`.
            if state & CLOSED != 0 {
                // If the task is scheduled or running, we need to wait until its future is
                // dropped.
                if state & (SCHEDULED | RUNNING) != 0 {
                    // Replace the waker with one associated with the current task.
                    (*header).register(cx.waker());
                    return Poll::Pending;
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

                return Poll::Pending;
            }

            (*header).state |= CLOSED;

            // Notify the awaiter. Even though the awaiter is most likely the current
            // task, it could also be another task.
            if state & AWAITER != 0 {
                (*header).notify(Some(cx.waker()));
            }

            // Take the output from the task.
            let output = ((*header).vtable.get_output)(ptr) as *mut R;
            Poll::Ready(Some(output.read()))
        }
    }
}

impl<R> fmt::Debug for JoinHandle<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ptr = self.raw_task.as_ptr();
        let header = ptr as *const Header;

        f.debug_struct("JoinHandle")
            .field("header", unsafe { &(*header) })
            .finish()
    }
}
