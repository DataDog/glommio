// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use core::fmt;
use core::future::Future;
use core::marker::PhantomData;
use core::mem;
use core::ptr::NonNull;
use core::sync::atomic::Ordering;
use core::task::Waker;

use crate::task::header::Header;
use crate::task::raw::RawTask;
use crate::task::state::*;
use crate::task::JoinHandle;

/// Creates a new local task.
///
/// This constructor returns a [`Task`] reference that runs the future and a [`JoinHandle`] that
/// awaits its result.
///
/// When run, the task polls `future`. When woken up, it gets scheduled for running by the
/// `schedule` function. Argument `tag` is an arbitrary piece of data stored inside the task.
///
/// [`Task`]: struct.Task.html
/// [`JoinHandle`]: struct.JoinHandle.html
pub(crate) fn spawn_local<F, R, S, T>(future: F, schedule: S, tag: T) -> (Task<T>, JoinHandle<R, T>)
where
    F: Future<Output = R> + 'static,
    R: 'static,
    S: Fn(Task<T>) + 'static,
    T: 'static,
{
    // Allocate large futures on the heap.
    let raw_task = if mem::size_of::<F>() >= 2048 {
        let future = alloc::boxed::Box::pin(future);
        RawTask::<_, R, S, T>::allocate(future, schedule, tag)
    } else {
        RawTask::<_, R, S, T>::allocate(future, schedule, tag)
    };

    let task = Task {
        raw_task,
        _marker: PhantomData,
    };
    let handle = JoinHandle {
        raw_task,
        _marker: PhantomData,
    };
    (task, handle)
}

/// A task reference that runs its future.
///
/// At any moment in time, there is at most one [`Task`] reference associated with a particular
/// task. Running consumes the [`Task`] reference and polls its internal future. If the future is
/// still pending after getting polled, the [`Task`] reference simply won't exist until a [`Waker`]
/// notifies the task. If the future completes, its result becomes available to the [`JoinHandle`].
///
/// When a task is woken up, its [`Task`] reference is recreated and passed to the schedule
/// function. In most executors, scheduling simply pushes the [`Task`] reference into a queue of
/// runnable tasks.
///
/// If the [`Task`] reference is dropped without getting run, the task is automatically canceled.
/// When canceled, the task won't be scheduled again even if a [`Waker`] wakes it. It is possible
/// for the [`JoinHandle`] to cancel while the [`Task`] reference exists, in which case an attempt
/// to run the task won't do anything.
///
/// [`run()`]: struct.Task.html#method.run
/// [`JoinHandle`]: struct.JoinHandle.html
/// [`Task`]: struct.Task.html
/// [`Waker`]: https://doc.rust-lang.org/std/task/struct.Waker.html
pub struct Task<T> {
    /// A pointer to the heap-allocated task.
    pub(crate) raw_task: NonNull<()>,

    /// A marker capturing the generic type `T`.
    pub(crate) _marker: PhantomData<T>,
}

unsafe impl<T> Send for Task<T> {}
unsafe impl<T> Sync for Task<T> {}

impl<T> Task<T> {
    /// Schedules the task.
    ///
    /// This is a convenience method that simply reschedules the task by passing it to its schedule
    /// function.
    ///
    /// If the task is canceled, this method won't do anything.
    pub fn schedule(self) {
        let ptr = self.raw_task.as_ptr();
        let header = ptr as *const Header;
        mem::forget(self);

        unsafe {
            ((*header).vtable.schedule)(ptr);
        }
    }

    /// Runs the task.
    ///
    /// Returns `true` if the task was woken while running, in which case it gets rescheduled at
    /// the end of this method invocation.
    ///
    /// This method polls the task's future. If the future completes, its result will become
    /// available to the [`JoinHandle`]. And if the future is still pending, the task will have to
    /// be woken up in order to be rescheduled and run again.
    ///
    /// If the task was canceled by a [`JoinHandle`] before it gets run, then this method won't do
    /// anything.
    ///
    /// It is possible that polling the future panics, in which case the panic will be propagated
    /// into the caller. It is advised that invocations of this method are wrapped inside
    /// [`catch_unwind`]. If a panic occurs, the task is automatically canceled.
    ///
    /// [`JoinHandle`]: struct.JoinHandle.html
    /// [`catch_unwind`]: https://doc.rust-lang.org/std/panic/fn.catch_unwind.html
    pub fn run(self) -> bool {
        let ptr = self.raw_task.as_ptr();
        let header = ptr as *const Header;
        mem::forget(self);

        unsafe { ((*header).vtable.run)(ptr) }
    }

    /// Cancels the task.
    ///
    /// When canceled, the task won't be scheduled again even if a [`Waker`] wakes it. An attempt
    /// to run it won't do anything.
    ///
    /// [`Waker`]: https://doc.rust-lang.org/std/task/struct.Waker.html
    pub fn cancel(&self) {
        let ptr = self.raw_task.as_ptr();
        let header = ptr as *const Header;

        unsafe {
            (*header).cancel();
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

    /// Converts this task into a raw pointer to the tag.
    pub fn into_raw(self) -> *const T {
        let offset = Header::offset_tag::<T>();
        let ptr = self.raw_task.as_ptr();
        mem::forget(self);

        unsafe { (ptr as *mut u8).add(offset) as *const T }
    }

    /// Converts a raw pointer to the tag into a task.
    ///
    /// This method should only be used with raw pointers returned from [`into_raw`].
    ///
    /// [`into_raw`]: #method.into_raw
    ///
    /// # Safety
    ///
    /// `ptr` must be non-null.
    pub unsafe fn from_raw(raw: *const T) -> Task<T> {
        let offset = Header::offset_tag::<T>();
        let ptr = (raw as *mut u8).sub(offset) as *mut ();

        Task {
            raw_task: NonNull::new_unchecked(ptr),
            _marker: PhantomData,
        }
    }

    /// Returns a waker associated with this task.
    pub fn waker(&self) -> Waker {
        let ptr = self.raw_task.as_ptr();
        let header = ptr as *const Header;

        unsafe {
            let raw_waker = ((*header).vtable.clone_waker)(ptr);
            Waker::from_raw(raw_waker)
        }
    }
}

impl<T> Drop for Task<T> {
    fn drop(&mut self) {
        let ptr = self.raw_task.as_ptr();
        let header = ptr as *const Header;

        unsafe {
            // Cancel the task.
            (*header).cancel();

            // Drop the future.
            ((*header).vtable.drop_future)(ptr);

            // Mark the task as unscheduled.
            let state = (*header).state.fetch_and(!SCHEDULED, Ordering::AcqRel);

            // Notify the awaiter that the future has been dropped.
            if state & AWAITER != 0 {
                (*header).notify(None);
            }

            // Drop the task reference.
            ((*header).vtable.drop_task)(ptr);
        }
    }
}

impl<T: fmt::Debug> fmt::Debug for Task<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ptr = self.raw_task.as_ptr();
        let header = ptr as *const Header;

        f.debug_struct("Task")
            .field("header", unsafe { &(*header) })
            .field("tag", self.tag())
            .finish()
    }
}
