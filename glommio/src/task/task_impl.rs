// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use core::{fmt, future::Future, marker::PhantomData, mem, ptr::NonNull};

#[cfg(feature = "debugging")]
use crate::task::debugging::TaskDebugger;
use crate::{
    dbg_context,
    task::{header::Header, raw::RawTask, state::*, JoinHandle},
};

use std::sync::atomic::Ordering;

/// Creates a new local task.
///
/// This constructor returns a [`Task`] reference that runs the future and a
/// [`JoinHandle`] that awaits its result.
///
/// When run, the task polls `future`. When woken up, it gets scheduled for
/// running by the `schedule` function.
///
/// [`Task`]: struct.Task.html
/// [`JoinHandle`]: struct.JoinHandle.html
pub(crate) fn spawn_local<F, R, S>(
    executor_id: usize,
    future: F,
    schedule: S,
    latency_matters: bool,
) -> (Task, JoinHandle<R>)
where
    F: Future<Output = R>,
    S: Fn(Task),
{
    // Allocate large futures on the heap.
    let raw_task = if mem::size_of::<F>() >= 2048 {
        let future = alloc::boxed::Box::pin(future);
        RawTask::<_, R, S>::allocate(future, schedule, executor_id, latency_matters)
    } else {
        RawTask::<_, R, S>::allocate(future, schedule, executor_id, latency_matters)
    };

    let task = Task { raw_task };
    let handle = JoinHandle {
        raw_task,
        _marker: PhantomData,
    };
    (task, handle)
}

/// A task reference that runs its future.
///
/// At any moment in time, there is at most one [`Task`] reference associated
/// with a particular task. Running consumes the [`Task`] reference and polls
/// its internal future. If the future is still pending after getting polled,
/// the [`Task`] reference simply won't exist until a [`Waker`] notifies the
/// task. If the future completes, its result becomes available to the
/// [`JoinHandle`].
///
/// When a task is woken up, its [`Task`] reference is recreated and passed to
/// the schedule function. In most executors, scheduling simply pushes the
/// [`Task`] reference into a queue of runnable tasks.
///
/// If the [`Task`] reference is dropped without getting run, the task is
/// automatically canceled. When canceled, the task won't be scheduled again
/// even if a [`Waker`] wakes it. It is possible for the [`JoinHandle`] to
/// cancel while the [`Task`] reference exists, in which case an attempt
/// to run the task won't do anything.
///
/// [`run()`]: struct.Task.html#method.run
/// [`JoinHandle`]: struct.JoinHandle.html
/// [`Task`]: struct.Task.html
/// [`Waker`]: https://doc.rust-lang.org/std/task/struct.Waker.html
pub struct Task {
    /// A pointer to the heap-allocated task.
    pub(crate) raw_task: NonNull<()>,
}

impl Task {
    /// Schedules the task.
    ///
    /// This is a convenience method that simply reschedules the task by passing
    /// it to its schedule function.
    ///
    /// If the task is canceled, this method won't do anything.
    pub(crate) fn schedule(self) {
        let ptr = self.raw_task.as_ptr();
        let header = ptr as *const Header;
        mem::forget(self);

        unsafe {
            ((*header).vtable.schedule)(ptr);
        }
    }

    /// Runs the task.
    ///
    /// Returns `true` if the task was woken while running, in which case it
    /// gets rescheduled at the end of this method invocation.
    ///
    /// This method polls the task's future. If the future completes, its result
    /// will become available to the [`JoinHandle`]. And if the future is
    /// still pending, the task will have to be woken up in order to be
    /// rescheduled and run again.
    ///
    /// If the task was canceled by a [`JoinHandle`] before it gets run, then
    /// this method won't do anything.
    ///
    /// It is possible that polling the future panics, in which case the panic
    /// will be propagated into the caller. It is advised that invocations
    /// of this method are wrapped inside [`catch_unwind`]. If a panic
    /// occurs, the task is automatically canceled.
    ///
    /// [`JoinHandle`]: struct.JoinHandle.html
    /// [`catch_unwind`]: https://doc.rust-lang.org/std/panic/fn.catch_unwind.html
    pub(crate) fn run(self) -> bool {
        let ptr = self.raw_task.as_ptr();
        dbg_context!(ptr, "run", {
            let header = ptr as *const Header;
            mem::forget(self);
            #[cfg(feature = "debugging")]
            TaskDebugger::set_current_task(ptr);
            unsafe { ((*header).vtable.run)(ptr) }
        })
    }

    pub(crate) fn run_right_away(self) -> bool {
        let ptr = self.raw_task.as_ptr();
        let header = ptr as *const Header;
        mem::forget(self);

        unsafe {
            let refs = (*header).references.fetch_add(1, Ordering::Relaxed);
            assert_ne!(refs, i16::MAX);
            ((*header).vtable.run)(ptr)
        }
    }
}

impl Drop for Task {
    fn drop(&mut self) {
        let ptr = self.raw_task.as_ptr();
        let header = ptr as *mut Header;

        unsafe {
            // Cancel the task.
            (*header).cancel();

            // Drop the future.
            ((*header).vtable.drop_future)(ptr);

            // Mark the task as unscheduled.
            (*header).state &= !SCHEDULED;

            // Notify the awaiter that the future has been dropped.
            (*header).notify(None);

            // Drop the task reference.
            ((*header).vtable.drop_task)(ptr);
        }
    }
}

impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ptr = self.raw_task.as_ptr();
        let header = ptr as *const Header;

        f.debug_struct("Task")
            .field("header", unsafe { &(*header) })
            .finish()
    }
}
