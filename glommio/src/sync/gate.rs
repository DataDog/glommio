use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

use futures_lite::Future;

use crate::{
    channels::local_channel::{self, LocalSender},
    GlommioError,
    Local,
    ResourceType,
    Task,
    TaskQueueHandle,
};

#[derive(Debug)]
enum State {
    Closing(LocalSender<bool>),
    Closed,
    Open,
}

/// A visitor pass which could be acquired when entering a gate, and should be
/// released before the gate is closed.
#[derive(Debug)]
pub struct Pass {
    gate: Rc<GateInner>,
}

impl Drop for Pass {
    fn drop(&mut self) {
        self.gate.leave()
    }
}

/// Facility to achieve graceful shutdown by waiting for the dependent tasks
/// to complete.
#[derive(Clone, Debug)]
pub struct Gate {
    inner: Rc<GateInner>,
}

impl Gate {
    /// Create a new [`Gate`](crate::sync::Gate)
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            inner: Rc::new(GateInner {
                count: Cell::new(0),
                state: RefCell::new(State::Open),
            }),
        }
    }

    /// Get a visitor pass which will be waited to be released on closing
    pub fn enter(&self) -> Result<Pass, GlommioError<()>> {
        self.inner.enter()?;
        Ok(Pass {
            gate: self.inner.clone(),
        })
    }

    /// Spawn a task for which the gate will wait on closing into the current
    /// task queue.
    pub fn spawn<T: 'static>(
        &self,
        future: impl Future<Output = T> + 'static,
    ) -> Result<Task<T>, GlommioError<()>> {
        self.spawn_into(future, Local::current_task_queue())
    }

    /// Spawn a task for which the gate will wait on closing
    pub fn spawn_into<T: 'static>(
        &self,
        future: impl Future<Output = T> + 'static,
        handle: TaskQueueHandle,
    ) -> Result<Task<T>, GlommioError<()>> {
        let pass = self.enter()?;
        Task::<T>::local_into(
            async move {
                let result = future.await;
                drop(pass);
                result
            },
            handle,
        )
    }

    /// Close the gate, and wait for all spawned tasks to complete
    pub async fn close(&self) -> Result<(), GlommioError<()>> {
        self.inner.close().await
    }

    /// Whether the gate is open or not
    pub fn is_open(&self) -> bool {
        self.inner.is_open()
    }
}

#[derive(Debug)]
struct GateInner {
    count: Cell<usize>,
    state: RefCell<State>,
}

impl GateInner {
    pub fn try_enter(&self) -> bool {
        let open = self.is_open();
        if open {
            self.count.set(self.count.get() + 1);
        }
        open
    }

    pub fn enter(&self) -> Result<(), GlommioError<()>> {
        if !self.try_enter() {
            Err(GlommioError::Closed(ResourceType::Gate))
        } else {
            Ok(())
        }
    }

    pub fn leave(&self) {
        self.count.set(self.count.get() - 1);
        if self.count.get() == 0 && !self.is_open() {
            self.notify_closed()
        }
    }

    pub async fn close(&self) -> Result<(), GlommioError<()>> {
        if self.is_open() {
            if self.count.get() == 0 {
                *self.state.borrow_mut() = State::Closed;
            } else {
                let (sender, receiver) = local_channel::new_bounded(1);
                *self.state.borrow_mut() = State::Closing(sender);
                receiver.recv().await;
            }
            Ok(())
        } else {
            Err(GlommioError::Closed(ResourceType::Gate))
        }
    }

    pub fn is_open(&self) -> bool {
        matches!(*self.state.borrow(), State::Open)
    }

    pub fn notify_closed(&self) {
        if let State::Closing(sender) = self.state.replace(State::Closed) {
            sender.try_send(true).unwrap();
        } else {
            unreachable!("It should not happen!");
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{enclose, LocalExecutor};

    use super::*;
    use crate::sync::Semaphore;

    #[test]
    fn test_immediate_close() {
        LocalExecutor::default().run(async {
            let gate = Gate::new();
            assert!(gate.is_open());

            gate.close().await.unwrap();
            assert!(!gate.is_open());

            assert!(gate.spawn(async {}).is_err())
        })
    }

    #[test]
    fn test_future_close() {
        LocalExecutor::default().run(async {
            let gate = Gate::new();

            let nr_tasks = 5;
            let running_tasks = Rc::new(Semaphore::new(0));
            let tasks_to_complete = Rc::new(Semaphore::new(0));

            let spawn_task = |i| {
                enclose!((running_tasks, tasks_to_complete) async move {
                    running_tasks.signal(1);
                    println!("[Task {}] started, running tasks: {}", i, running_tasks.available());
                    tasks_to_complete.acquire(1).await.unwrap();
                    println!("[Task {}] complete, tasks to complete: {}", i, tasks_to_complete.available());
                })
            };

            for i in 0..nr_tasks {
                gate.spawn(spawn_task(i)).unwrap().detach();
            }

            println!("Main: waiting for {} tasks", nr_tasks);
            running_tasks.acquire(nr_tasks).await.unwrap();

            println!("Main: closing gate");
            let close_future =
                Task::<_>::local(enclose!((gate) async move { gate.close().await })).detach();
            Local::later().await;
            assert!(!gate.is_open());
            assert!(gate.spawn(async {}).is_err());

            tasks_to_complete.signal(nr_tasks);
            close_future.await.unwrap().unwrap();
            println!("Main: gate is closed");
            assert_eq!(tasks_to_complete.available(), 0);
        })
    }

    #[test]
    fn test_dropped_task() {
        LocalExecutor::default().run(async {
            let gate = Gate::new();
            let running = Rc::new(Cell::new(false));

            let task = gate
                .spawn(enclose!((running) async move {
                    running.set(true);
                }))
                .unwrap();

            drop(task);

            gate.close().await.unwrap();
            assert!(!running.get());
        })
    }
}
