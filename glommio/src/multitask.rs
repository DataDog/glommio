// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
//! An executor for running async tasks.

#![forbid(unsafe_code)]
#![warn(missing_docs, missing_debug_implementations)]

use crate::executor::{maybe_activate, TaskQueue};
use crate::task::task_impl;
use crate::task::JoinHandle;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::marker::PhantomData;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

/// A runnable future, ready for execution.
///
/// When a future is internally spawned using `task::spawn()` or `task::spawn_local()`,
/// we get back two values:
///
/// 1. an `task::Task<()>`, which we refer to as a `Runnable`
/// 2. an `task::JoinHandle<T, ()>`, which is wrapped inside a `Task<T>`
///
/// Once a `Runnable` is run, it "vanishes" and only reappears when its future is woken. When it's
/// woken up, its schedule function is called, which means the `Runnable` gets pushed into a task
/// queue in an executor.
pub(crate) type Runnable = task_impl::Task;

/// A spawned future.
///
/// Tasks are also futures themselves and yield the output of the spawned future.
///
/// When a task is dropped, its gets canceled and won't be polled again. To cancel a task a bit
/// more gracefully and wait until it stops running, use the [`cancel()`][Task::cancel()] method.
///
/// Tasks that panic get immediately canceled. Awaiting a canceled task also causes a panic.
///
/// If a task panics, the panic will be thrown by the [`Ticker::tick()`] invocation that polled it.
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

/// A single-threaded executor.
#[derive(Debug)]
pub(crate) struct LocalExecutor {
    local_queue: LocalQueue,

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
            _marker: PhantomData,
        }
    }

    /// Spawns a thread-local future onto this executor.
    pub(crate) fn spawn<T>(
        &self,
        tq: Rc<RefCell<TaskQueue>>,
        future: impl Future<Output = T>,
    ) -> Task<T> {
        let tq = Rc::downgrade(&tq);

        // The function that schedules a runnable task when it gets woken up.
        let schedule = move |runnable: Runnable| {
            let tq = tq.upgrade();

            if let Some(tq) = tq {
                {
                    let queue = tq.borrow();
                    queue.ex.local_queue.push(runnable);
                }
                maybe_activate(tq);
            }
        };

        // Create a task, push it into the queue by scheduling it, and return its `Task` handle.
        let (runnable, handle) = task_impl::spawn_local(future, schedule);
        runnable.schedule();
        Task(Some(handle))
    }

    /// Gets one task from the queue, if one exists.
    ///
    /// Returns an option rapping the task.
    pub(crate) fn get_task(&self) -> Option<Runnable> {
        self.local_queue.pop()
    }

    pub(crate) fn is_active(&self) -> bool {
        !self.local_queue.queue.borrow().is_empty()
    }
}
