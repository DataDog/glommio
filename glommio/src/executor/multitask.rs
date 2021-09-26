// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
//! An executor for running async tasks.

#![forbid(unsafe_code)]
#![warn(missing_docs, missing_debug_implementations)]

use crate::{
    executor::{maybe_activate, TaskQueue},
    task::{task_impl, JoinHandle},
};
use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    future::Future,
    marker::PhantomData,
    panic::{RefUnwindSafe, UnwindSafe},
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};

/// A runnable future, ready for execution.
///
/// When a future is internally spawned using `task::spawn()` or
/// `task::spawn_local()`, we get back two values:
///
/// 1. an `task::Task<()>`, which we refer to as a `Runnable`
/// 2. an `task::JoinHandle<T, ()>`, which is wrapped inside a `Task<T>`
///
/// Once a `Runnable` is run, it "vanishes" and only reappears when its future
/// is woken. When it's woken up, its schedule function is called, which means
/// the `Runnable` gets pushed into a task queue in an executor.
pub(crate) type Runnable = task_impl::Task;

/// An identity fingerprint of a runnable task; useful to compare two tasks for
/// equality without keeping references
pub(crate) type RunnableId = task_impl::TaskId;

/// A spawned future.
///
/// Tasks are also futures themselves and yield the output of the spawned
/// future.
///
/// When a task is dropped, its gets canceled and won't be polled again. To
/// cancel a task a bit more gracefully and wait until it stops running, use the
/// [`cancel()`][Task::cancel()] method.
///
/// Tasks that panic get immediately canceled. Awaiting a canceled task also
/// causes a panic.
///
/// If a task panics, the panic will be thrown by the [`Ticker::tick()`]
/// invocation that polled it.
///
/// ```
#[must_use = "tasks get canceled when dropped, use `.detach()` to run them in the background"]
#[derive(Debug)]
pub(crate) struct Task<T>(Option<JoinHandle<T>>);

impl<T> Task<T> {
    /// Detaches the task to let it keep running in the background.
    pub(crate) fn detach(mut self) -> JoinHandle<T> {
        self.0.take().unwrap()
    }

    /// Cancels the task and waits for it to stop running.
    pub(crate) async fn cancel(self) -> Option<T> {
        let mut task = self;
        let handle = task.0.take().unwrap();
        handle.cancel();
        handle.await
    }
}

impl<T> Drop for Task<T> {
    fn drop(&mut self) {
        if let Some(handle) = &self.0 {
            handle.cancel();
        }
    }
}

impl<T> Future for Task<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0.as_mut().unwrap())
            .poll(cx)
            .map(|output| output.expect("task has failed"))
    }
}

#[derive(Debug)]
struct LocalQueue {
    queue: RefCell<VecDeque<Runnable>>,
}

impl LocalQueue {
    fn new() -> Self {
        LocalQueue {
            queue: RefCell::new(VecDeque::new()),
        }
    }

    pub(crate) fn push(&self, runnable: Runnable) {
        self.queue.borrow_mut().push_back(runnable);
    }

    pub(crate) fn pop(&self) -> Option<Runnable> {
        self.queue.borrow_mut().pop_front()
    }
}

const MAX_UNFAIR_SCHEDULING: usize = 256;

/// A single-threaded executor.
#[derive(Debug)]
pub(crate) struct LocalExecutor {
    local_queue: LocalQueue,

    /// run_next, if present is a runnable that was scheduled by the previous
    /// task and should be run next instead of what's in the local queue.
    /// This approach eliminates the (potentially large) scheduling latency
    /// that otherwise arises from adding the scheduled runnable to the end of
    /// the run queue.
    ///
    /// Another way to look at this is as a 1-element LIFO buffer in front of
    /// the fair (FIFO) task queue. The latter is an instrument for fairness
    /// while this optimizes for throughput.
    run_next: RefCell<Option<Runnable>>,
    /// last_popped is the last runnable return by this executor
    /// It is used to avoid placing the same task in run_next
    last_popped: RefCell<Option<RunnableId>>,

    /// run_next_count counts the number of throughput-oriented scheduling
    /// decision we have made in a row. It is used to inject some fairness into
    /// the mix.
    run_next_count: Cell<usize>,

    /// Make sure the type is `!Send` and `!Sync`.
    _marker: PhantomData<Rc<()>>,
}

impl UnwindSafe for LocalExecutor {}

impl RefUnwindSafe for LocalExecutor {}

impl LocalExecutor {
    /// Creates a new single-threaded executor.
    pub(crate) fn new() -> LocalExecutor {
        LocalExecutor {
            local_queue: LocalQueue::new(),
            run_next: RefCell::new(None),
            last_popped: RefCell::new(None),
            run_next_count: Cell::new(0),
            _marker: PhantomData,
        }
    }

    /// Spawns a thread-local future onto this executor.
    fn spawn<T>(
        &self,
        executor_id: usize,
        tq: Rc<RefCell<TaskQueue>>,
        future: impl Future<Output = T>,
    ) -> (Runnable, JoinHandle<T>) {
        let tq = Rc::downgrade(&tq);

        // The function that schedules a runnable task when it gets woken up.
        let schedule = move |runnable: Runnable| {
            let tq = tq.upgrade();

            if let Some(tq) = tq {
                tq.borrow().ex.push_task(runnable);
                maybe_activate(tq);
            }
        };

        // Create a task, push it into the queue by scheduling it, and return its `Task`
        // handle.
        task_impl::spawn_local(executor_id, future, schedule)
    }

    /// Creates a [`Task`] for a given future and runs it immediately
    pub(crate) fn spawn_and_run<T>(
        &self,
        executor_id: usize,
        tq: Rc<RefCell<TaskQueue>>,
        future: impl Future<Output = T>,
    ) -> Task<T> {
        let (runnable, handle) = self.spawn(executor_id, tq, future);
        runnable.run_right_away();
        Task(Some(handle))
    }

    /// Creates a [`Task`] for a given future and schedules it to run eventually
    pub(crate) fn spawn_and_schedule<T>(
        &self,
        executor_id: usize,
        tq: Rc<RefCell<TaskQueue>>,
        future: impl Future<Output = T>,
    ) -> Task<T> {
        let (runnable, handle) = self.spawn(executor_id, tq, future);
        runnable.schedule();
        Task(Some(handle))
    }

    /// Gets one task from the queue, if one exists.
    ///
    /// Returns an option rapping the task.
    pub(crate) fn get_task(&self) -> Option<Runnable> {
        let mut run_next = self.run_next.borrow_mut();

        let ret = match (
            run_next.take(),
            self.run_next_count.get() < MAX_UNFAIR_SCHEDULING,
        ) {
            (Some(runnable), true) => {
                // run the next task immediately
                self.run_next_count.replace(self.run_next_count.get() + 1);
                Some(runnable)
            }
            (Some(runnable), false) => {
                // pop from the queue instead
                self.run_next_count.replace(0);
                Some(if let Some(ret) = self.local_queue.pop() {
                    self.local_queue.push(runnable);
                    ret
                } else {
                    // there is nothing in the queue so run the priority runnable anyway
                    runnable
                })
            }
            (_, _) => {
                self.run_next_count.replace(0);
                self.local_queue.pop()
            }
        };

        self.last_popped.replace(ret.as_ref().map(RunnableId::from));
        ret
    }

    /// Returns true if this [`LocalExecutor`] has some scheduled work
    pub(crate) fn is_active(&self) -> bool {
        !(self.local_queue.queue.borrow().is_empty() && self.run_next.borrow().is_none())
    }

    /// Reset the state of this [`LocalExecutor`] to be fair wrt scheduling
    /// decisions
    pub(crate) fn make_fair(&self) {
        // empty the priority slot so that we pick a task from the queue next time
        let _ = self.last_popped.borrow_mut().take();
        if let Some(runnable) = self.run_next.borrow_mut().take() {
            self.local_queue.push(runnable)
        }
        self.run_next_count.replace(0);
    }

    /// Schedule a given task for execution on this LocalExecutor
    /// If next is false, runnable is added to the tail of the runnable queue.
    /// If next is true, runnable is set as the immediate next task to run.
    fn push_task(&self, runnable: Runnable) {
        // Check whether the task we try to schedule is the same that's currently
        // running, if any. If so then the task is yielding voluntarily so we
        // put it at the tail of the queue
        let reentrant = self
            .last_popped
            .borrow()
            .as_ref()
            .map_or(false, |last| *last == RunnableId::from(&runnable));

        // if run_next is already populated, kick the runnable out into the regular
        // queue
        let runnable = if !reentrant {
            self.run_next.borrow_mut().replace(runnable)
        } else {
            Some(runnable)
        };

        if let Some(runnable) = runnable {
            self.local_queue.push(runnable)
        }
    }
}
