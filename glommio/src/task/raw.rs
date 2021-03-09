// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use alloc::alloc::Layout;
use core::{
    future::Future,
    mem::{self, ManuallyDrop},
    pin::Pin,
    ptr::NonNull,
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
};

use crate::task::{
    header::Header,
    state::*,
    utils::{abort, abort_on_panic, extend},
    Task,
};
use std::thread::ThreadId;

thread_local!(static THREAD_ID : ThreadId = std::thread::current().id());

/// The vtable for a task.
pub(crate) struct TaskVTable {
    /// Schedules the task.
    pub(crate) schedule: unsafe fn(*const ()),

    /// Drops the future inside the task.
    pub(crate) drop_future: unsafe fn(*const ()),

    /// Returns a pointer to the output stored after completion.
    pub(crate) get_output: unsafe fn(*const ()) -> *const (),

    /// Drops the task.
    pub(crate) drop_task: unsafe fn(ptr: *const ()),

    /// Destroys the task.
    pub(crate) destroy: unsafe fn(*const ()),

    /// Runs the task.
    pub(crate) run: unsafe fn(*const ()) -> bool,
}

/// Memory layout of a task.
///
/// This struct contains the following information:
///
/// 1. How to allocate and deallocate the task.
/// 2. How to access the fields inside the task.
#[derive(Clone, Copy)]
pub(crate) struct TaskLayout {
    /// Memory layout of the whole task.
    pub(crate) layout: Layout,

    /// Offset into the task at which the schedule function is stored.
    pub(crate) offset_s: usize,

    /// Offset into the task at which the future is stored.
    pub(crate) offset_f: usize,

    /// Offset into the task at which the output is stored.
    pub(crate) offset_r: usize,
}

/// Raw pointers to the fields inside a task.
pub(crate) struct RawTask<F, R, S> {
    /// The task header.
    pub(crate) header: *const Header,

    /// The schedule function.
    pub(crate) schedule: *const S,

    /// The future.
    pub(crate) future: *mut F,

    /// The output of the future.
    pub(crate) output: *mut R,
}

impl<F, R, S> Copy for RawTask<F, R, S> {}

impl<F, R, S> Clone for RawTask<F, R, S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<F, R, S> RawTask<F, R, S>
where
    F: Future<Output = R>,
    S: Fn(Task),
{
    const RAW_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
        Self::clone_waker,
        Self::wake,
        Self::wake_by_ref,
        Self::drop_waker,
    );

    /// Allocates a task with the given `future` and `schedule` function.
    ///
    /// It is assumed that initially only the `Task` reference and the
    /// `JoinHandle` exist.
    pub(crate) fn allocate(future: F, schedule: S) -> NonNull<()> {
        // Compute the layout of the task for allocation. Abort if the computation
        // fails.
        let task_layout = abort_on_panic(Self::task_layout);

        unsafe {
            // Allocate enough space for the entire task.
            let raw_task = match NonNull::new(alloc::alloc::alloc(task_layout.layout) as *mut ()) {
                None => abort(),
                Some(p) => p,
            };

            let raw = Self::from_ptr(raw_task.as_ptr());

            // Write the header as the first field of the task.
            (raw.header as *mut Header).write(Header {
                thread_id: Self::thread_id(),
                state: SCHEDULED | HANDLE | REFERENCE,
                awaiter: None,
                vtable: &TaskVTable {
                    schedule: Self::schedule,
                    drop_future: Self::drop_future,
                    get_output: Self::get_output,
                    drop_task: Self::drop_task,
                    destroy: Self::destroy,
                    run: Self::run,
                },
            });

            // Write the schedule function as the third field of the task.
            (raw.schedule as *mut S).write(schedule);

            // Write the future as the fourth field of the task.
            raw.future.write(future);

            raw_task
        }
    }

    fn thread_id() -> ThreadId {
        THREAD_ID
            .try_with(|id| *id)
            .unwrap_or_else(|_e| std::thread::current().id())
    }

    /// Creates a `RawTask` from a raw task pointer.
    #[inline]
    pub(crate) fn from_ptr(ptr: *const ()) -> Self {
        let task_layout = Self::task_layout();
        let p = ptr as *const u8;

        unsafe {
            Self {
                header: p as *const Header,
                schedule: p.add(task_layout.offset_s) as *const S,
                future: p.add(task_layout.offset_f) as *mut F,
                output: p.add(task_layout.offset_r) as *mut R,
            }
        }
    }

    /// Returns the memory layout for a task.
    #[inline]
    fn task_layout() -> TaskLayout {
        // Compute the layouts for `Header`, `T`, `S`, `F`, and `R`.
        let layout_header = Layout::new::<Header>();
        let layout_s = Layout::new::<S>();
        let layout_f = Layout::new::<F>();
        let layout_r = Layout::new::<R>();

        // Compute the layout for `union { F, R }`.
        let size_union = layout_f.size().max(layout_r.size());
        let align_union = layout_f.align().max(layout_r.align());
        let layout_union = unsafe { Layout::from_size_align_unchecked(size_union, align_union) };

        // Compute the layout for `Header` followed by `T`, then `S`, and finally `union
        // { F, R }`.
        let layout = layout_header;
        let (layout, offset_s) = extend(layout, layout_s);
        let (layout, offset_union) = extend(layout, layout_union);
        let offset_f = offset_union;
        let offset_r = offset_union;

        TaskLayout {
            layout,
            offset_s,
            offset_f,
            offset_r,
        }
    }

    /// Wakes a waker.
    unsafe fn wake(ptr: *const ()) {
        // This is just an optimization. If the schedule function has captured
        // variables, then we'll do less reference counting if we wake the waker
        // by reference and then drop it.
        if mem::size_of::<S>() > 0 {
            Self::wake_by_ref(ptr);
            Self::drop_waker_reference(ptr);
            return;
        }

        let raw = Self::from_ptr(ptr);
        assert_eq!(
            Self::thread_id(),
            (*raw.header).thread_id,
            "Waker::wake is called outside of working thread. Waker instances can not be moved to \
             or work with multiple threads"
        );

        let state = (*raw.header).state;

        // If the task is completed or closed, it can't be woken up.
        if state & (COMPLETED | CLOSED) != 0 {
            // Drop the waker.
            Self::drop_waker_reference(ptr);
            return;
        }

        // If the task is already scheduled do nothing.
        if state & SCHEDULED != 0 {
            // Drop the waker.
            Self::drop_waker_reference(ptr);
        } else {
            // Mark the task as scheduled.
            (*(raw.header as *mut Header)).state = state | SCHEDULED;
            if state & RUNNING == 0 {
                // Schedule the task.
                Self::schedule(ptr);
            } else {
                // Drop the waker.
                Self::drop_waker_reference(ptr);
            }
        }
    }

    /// Wakes a waker by reference.
    unsafe fn wake_by_ref(ptr: *const ()) {
        let raw = Self::from_ptr(ptr);

        assert_eq!(
            Self::thread_id(),
            (*raw.header).thread_id,
            "Waker::wake_by_ref is called outside of working thread. Waker instances can not be \
             moved to or work with multiple threads"
        );

        let state = (*raw.header).state;

        // If the task is completed or closed, it can't be woken up.
        if state & (COMPLETED | CLOSED) != 0 {
            return;
        }

        // If the task is already scheduled, we just need to synchronize with the thread
        // that will run the task by "publishing" our current view of the
        // memory.
        if state & SCHEDULED == 0 {
            // If the task is not running, we can schedule right away.
            let new = if state & RUNNING == 0 {
                (state | SCHEDULED) + REFERENCE
            } else {
                state | SCHEDULED
            };

            // Mark the task as scheduled.
            (*(raw.header as *mut Header)).state = new;

            if state & RUNNING == 0 {
                // If the reference count overflowed, abort.
                if state > isize::max_value() as usize {
                    abort();
                }

                // Schedule the task. There is no need to call `Self::schedule(ptr)`
                // because the schedule function cannot be destroyed while the waker is
                // still alive.
                let task = Task {
                    raw_task: NonNull::new_unchecked(ptr as *mut ()),
                };

                (*raw.schedule)(task);
            }
        }
    }

    /// Clones a waker.
    unsafe fn clone_waker(ptr: *const ()) -> RawWaker {
        let raw = Self::from_ptr(ptr);

        assert_eq!(
            Self::thread_id(),
            (*raw.header).thread_id,
            "Waker::clone is called outside of working thread. Waker instances can not be moved \
             to or work with multiple threads"
        );

        Self::increment_references(&mut *(raw.header as *mut Header));

        RawWaker::new(ptr, &Self::RAW_WAKER_VTABLE)
    }

    #[inline]
    fn increment_references(header: &mut Header) {
        let state = header.state;
        header.state += REFERENCE;

        // If the reference count overflowed, abort.
        if state > isize::max_value() as usize {
            abort();
        }
    }

    /// Drops a waker.
    ///
    /// This function will decrement the reference count. If it drops down to
    /// zero, the associated join handle has been dropped too, and the task
    /// has not been completed, then it will get scheduled one more time so
    /// that its future gets dropped by the executor.
    #[inline]
    unsafe fn drop_waker(ptr: *const ()) {
        let header = ptr as *const Header;
        assert_eq!(
            Self::thread_id(),
            (*header).thread_id,
            "Waker::drop is called outside of working thread. Waker instances can not be moved to \
             or work with multiple threads"
        );

        <RawTask<F, R, S>>::drop_waker_reference(ptr)
    }

    #[inline]
    unsafe fn drop_waker_reference(ptr: *const ()) {
        let raw = Self::from_ptr(ptr);
        // Decrement the reference count.
        let new = (*raw.header).state - REFERENCE;
        (*(raw.header as *mut Header)).state = new;

        // If this was the last reference to the task and the `JoinHandle` has been
        // dropped too, then we need to decide how to destroy the task.
        if new & !(REFERENCE - 1) == 0 && new & HANDLE == 0 {
            if new & (COMPLETED | CLOSED) == 0 {
                // If the task was not completed nor closed, close it and schedule one more time
                // so that its future gets dropped by the executor.
                (*(raw.header as *mut Header)).state = SCHEDULED | CLOSED | REFERENCE;
                Self::schedule(ptr);
            } else {
                // Otherwise, destroy the task right away.
                Self::destroy(ptr);
            }
        }
    }

    /// Drops a task.
    ///
    /// This function will decrement the reference count. If it drops down to
    /// zero and the associated join handle has been dropped too, then the
    /// task gets destroyed.
    #[inline]
    unsafe fn drop_task(ptr: *const ()) {
        let raw = Self::from_ptr(ptr);

        // Decrement the reference count.
        let new = (*raw.header).state - REFERENCE;
        (*(raw.header as *mut Header)).state = new;

        // If this was the last reference to the task and the `JoinHandle` has been
        // dropped too, then destroy the task.
        if new & !(REFERENCE - 1) == 0 && new & HANDLE == 0 {
            Self::destroy(ptr);
        }
    }

    /// Schedules a task for running.
    ///
    /// This function doesn't modify the state of the task. It only passes the
    /// task reference to its schedule function.
    unsafe fn schedule(ptr: *const ()) {
        let raw = Self::from_ptr(ptr);

        struct Guard<'a, F, R, S>(&'a RawTask<F, R, S>)
        where
            F: Future<Output = R>,
            S: Fn(Task);

        impl<'a, F, R, S> Drop for Guard<'a, F, R, S>
        where
            F: Future<Output = R>,
            S: Fn(Task),
        {
            fn drop(&mut self) {
                let raw = self.0;
                let ptr = raw.header as *const ();

                unsafe {
                    RawTask::<F, R, S>::drop_waker_reference(ptr);
                }
            }
        }

        let guard;
        // Calling of schedule functions itself does not increment references,
        // if the schedule function has captured variables, increment references
        // so if task being dropped inside schedule function , function itself
        // will keep valid data till the end of execution.
        if mem::size_of::<S>() > 0 {
            Self::increment_references(&mut *(raw.header as *mut Header));
            guard = Some(Guard(&raw));
        } else {
            guard = None;
        }

        let task = Task {
            raw_task: NonNull::new_unchecked(ptr as *mut ()),
        };

        (*raw.schedule)(task);
        drop(guard);
    }

    /// Drops the future inside a task.
    #[inline]
    unsafe fn drop_future(ptr: *const ()) {
        let raw = Self::from_ptr(ptr);

        // We need a safeguard against panics because the destructor can panic.
        abort_on_panic(|| {
            raw.future.drop_in_place();
        })
    }

    /// Returns a pointer to the output inside a task.
    unsafe fn get_output(ptr: *const ()) -> *const () {
        let raw = Self::from_ptr(ptr);
        raw.output as *const ()
    }

    /// Cleans up task's resources and deallocates it.
    ///
    /// The schedule function will be dropped, and the task will then get
    /// deallocated. The task must be closed before this function is called.
    #[inline]
    unsafe fn destroy(ptr: *const ()) {
        let raw = Self::from_ptr(ptr);
        let task_layout = Self::task_layout();

        // We need a safeguard against panics because destructors can panic.
        abort_on_panic(|| {
            // Drop the schedule function.
            (raw.schedule as *mut S).drop_in_place();
        });

        // Finally, deallocate the memory reserved by the task.
        alloc::alloc::dealloc(ptr as *mut u8, task_layout.layout);
    }

    /// Runs a task.
    ///
    /// If polling its future panics, the task will be closed and the panic will
    /// be propagated into the caller.
    unsafe fn run(ptr: *const ()) -> bool {
        let raw = Self::from_ptr(ptr);

        let mut state = (*raw.header).state;

        // Update the task's state before polling its future.
        // If the task has already been closed, drop the task reference and return.
        if state & CLOSED != 0 {
            // Drop the future.
            Self::drop_future(ptr);

            // Mark the task as unscheduled.
            (*(raw.header as *mut Header)).state &= !SCHEDULED;

            // Notify the awaiter that the future has been dropped.
            if state & AWAITER != 0 {
                (*(raw.header as *mut Header)).notify(None);
            }

            // Drop the task reference.
            Self::drop_task(ptr);
            return false;
        }

        state = (state & !SCHEDULED) | RUNNING;
        (*(raw.header as *mut Header)).state = state;

        // Create a context from the raw task pointer and the vtable inside the its
        // header.
        let waker = ManuallyDrop::new(Waker::from_raw(RawWaker::new(ptr, &Self::RAW_WAKER_VTABLE)));
        let cx = &mut Context::from_waker(&waker);

        // Poll the inner future, but surround it with a guard that closes the task in
        // case polling panics.
        let guard = Guard(raw);
        let poll = <F as Future>::poll(Pin::new_unchecked(&mut *raw.future), cx);
        mem::forget(guard);

        //state could be updated after the coll to the poll
        state = (*raw.header).state;

        match poll {
            Poll::Ready(out) => {
                // Replace the future with its output.
                Self::drop_future(ptr);
                raw.output.write(out);

                // A place where the output will be stored in case it needs to be dropped.
                let mut output = None;

                // The task is now completed.
                // If the handle is dropped, we'll need to close it and drop the output.
                let new = if state & HANDLE == 0 {
                    (state & !RUNNING & !SCHEDULED) | COMPLETED | CLOSED
                } else {
                    (state & !RUNNING & !SCHEDULED) | COMPLETED
                };

                (*(raw.header as *mut Header)).state = new;

                // If the handle is dropped or if the task was closed while running,
                // now it's time to drop the output.
                if state & HANDLE == 0 || state & CLOSED != 0 {
                    // Read the output.
                    output = Some(raw.output.read());
                }

                // Notify the awaiter that the task has been completed.
                if state & AWAITER != 0 {
                    (*(raw.header as *mut Header)).notify(None);
                }

                // Drop the task reference.
                Self::drop_task(ptr);

                drop(output);
            }
            Poll::Pending => {
                // The task is still not completed.

                // If the task was closed while running, we'll need to unschedule in case it
                // was woken up and then destroy it.
                let new = if state & CLOSED != 0 {
                    state & !RUNNING & !SCHEDULED
                } else {
                    state & !RUNNING
                };

                if state & CLOSED != 0 {
                    Self::drop_future(ptr);
                }

                (*(raw.header as *mut Header)).state = new;

                // If the task was closed while running, we need to notify the awaiter.
                // If the task was woken up while running, we need to schedule it.
                // Otherwise, we just drop the task reference.
                if state & CLOSED != 0 {
                    // Notify the awaiter that the future has been dropped.
                    if state & AWAITER != 0 {
                        (*(raw.header as *mut Header)).notify(None);
                    }
                    // Drop the task reference.
                    Self::drop_task(ptr);
                } else if state & SCHEDULED != 0 {
                    // The thread that woke the task up didn't reschedule it because
                    // it was running so now it's our responsibility to do so.
                    Self::schedule(ptr);
                    return true;
                } else {
                    // Drop the task reference.
                    Self::drop_task(ptr);
                }
            }
        }

        return false;

        /// A guard that closes the task if polling its future panics.
        struct Guard<F, R, S>(RawTask<F, R, S>)
        where
            F: Future<Output = R>,
            S: Fn(Task);

        impl<F, R, S> Drop for Guard<F, R, S>
        where
            F: Future<Output = R>,
            S: Fn(Task),
        {
            fn drop(&mut self) {
                let raw = self.0;
                let ptr = raw.header as *const ();

                unsafe {
                    // Mark the task as not running and not scheduled.
                    (*(raw.header as *mut Header)).state =
                        ((*(raw.header)).state & !RUNNING & !SCHEDULED) | CLOSED;

                    // drop tasks future, and drop the task reference.

                    // The thread that closed the task didn't drop the future because it
                    // was running so now it's our responsibility to do so.
                    RawTask::<F, R, S>::drop_future(ptr);

                    // Notify the awaiter that the future has been dropped.
                    if (*raw.header).state & AWAITER != 0 {
                        (*(raw.header as *mut Header)).notify(None);
                    }

                    // Drop the task reference.
                    RawTask::<F, R, S>::drop_task(ptr);
                }
            }
        }
    }
}
