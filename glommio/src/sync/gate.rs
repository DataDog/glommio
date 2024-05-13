use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

use futures_lite::Future;

use crate::{
    channels::local_channel::{self, LocalReceiver, LocalSender},
    GlommioError, ResourceType, Task, TaskQueueHandle,
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
        self.spawn_into(future, crate::executor().current_task_queue())
    }

    /// Spawn a task for which the gate will wait on closing
    pub fn spawn_into<T: 'static>(
        &self,
        future: impl Future<Output = T> + 'static,
        handle: TaskQueueHandle,
    ) -> Result<Task<T>, GlommioError<()>> {
        let pass = self.enter()?;
        crate::spawn_local_into(
            async move {
                let result = future.await;
                drop(pass);
                result
            },
            handle,
        )
    }

    /// Close the gate, and return a waiter for all spawned tasks to complete. If the gate is currently closing, the
    /// returned future will wait for it to close before returning a success. This is particularly useful if you might
    /// have a timeout on the close - the would otherwise be no safe way to retry & wait for remaining tasks to finish.
    ///
    /// NOTE: After this function returns, [is_open](Self::is_open) returns false and any subsequent attempts to acquire
    /// a pass will fail, even if you drop the future. The future will return an error if and only if the gate is
    /// already fully closed
    pub fn close(&self) -> impl Future<Output = Result<(), GlommioError<()>>> {
        self.inner.close()
    }

    /// Whether the gate is open or not.
    pub fn is_open(&self) -> bool {
        self.inner.is_open()
    }

    /// This returns true only if [Self::close] has been called and all spawned tasks are complete. If it returns false,
    /// you may call [Self::close] without it returning an error and it'll wait for all spawned tasks to complete.
    ///
    /// NOTE: multiple concurrent calls to [Self::close] may be a performance issue since each invocation to close will
    /// allocate some nominal amount of memory for the channel underneath.
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
}

type PreviousWaiter = Option<LocalSender<bool>>;
type CurrentClosure = LocalReceiver<bool>;

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

    async fn wait_for_closure(
        waiter: Result<Option<(CurrentClosure, PreviousWaiter)>, GlommioError<()>>,
    ) -> Result<(), GlommioError<()>> {
        if let Some((waiter, previous_closer)) = waiter? {
            waiter.recv().await;
            if let Some(previous_closer) = previous_closer {
                // Previous channel may be dropped so ignore the result.
                let _ = previous_closer.try_send(true);
            }
        }

        Ok(())
    }

    pub fn close(&self) -> impl Future<Output = Result<(), GlommioError<()>>> {
        match self.state.replace(State::Closed) {
            State::Open => {
                if self.count.get() != 0 {
                    let (sender, receiver) = local_channel::new_bounded(1);
                    self.state.replace(State::Closing(sender));
                    Self::wait_for_closure(Ok(Some((receiver, None))))
                } else {
                    Self::wait_for_closure(Ok(None))
                }
            }
            State::Closing(previous_closer) => {
                assert!(
                    self.count.get() != 0,
                    "If count is 0 then the state should have been marked as closed"
                );
                assert!(
                    !previous_closer.is_full(),
                    "Already notified that the gate is closed!"
                );

                let (sender, receiver) = local_channel::new_bounded(1);
                self.state.replace(State::Closing(sender));

                Self::wait_for_closure(Ok(Some((receiver, Some(previous_closer)))))
            }
            State::Closed => Self::wait_for_closure(Err(GlommioError::Closed(ResourceType::Gate))),
        }
    }

    pub fn is_open(&self) -> bool {
        matches!(*self.state.borrow(), State::Open)
    }

    pub fn is_closed(&self) -> bool {
        matches!(*self.state.borrow(), State::Closed)
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
    use super::*;
    use crate::sync::Semaphore;
    use crate::{enclose, timer::timeout, LocalExecutor};
    use futures::join;
    use std::time::Duration;

    #[test]
    fn test_immediate_close() {
        LocalExecutor::default().run(async {
            let gate = Gate::new();
            assert!(gate.is_open());

            gate.close().await.unwrap();
            assert!(!gate.is_open());

            assert!(gate.spawn(async {}).is_err());

            assert!(gate.close().await.is_err());
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
                    println!("[Task {i}] started, running tasks: {}", running_tasks.available());
                    tasks_to_complete.acquire(1).await.unwrap();
                    println!("[Task {i}] complete, tasks to complete: {}", tasks_to_complete.available());
                })
            };

            for i in 0..nr_tasks {
                gate.spawn(spawn_task(i)).unwrap().detach();
            }

            println!("Main: waiting for {nr_tasks} tasks");
            running_tasks.acquire(nr_tasks).await.unwrap();

            println!("Main: closing gate");
            let close_future =
                crate::spawn_local(enclose!((gate) async move { gate.close().await })).detach();
            crate::executor().yield_task_queue_now().await;
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

    #[test]
    fn test_concurrent_close() {
        LocalExecutor::default().run(async {
            let gate = &Gate::new();
            let gate_closures = &Semaphore::new(0);
            let closed = &RefCell::new(false);

            let pass = gate.enter().unwrap();

            join!(
                async {
                    gate_closures.signal(1);
                    gate.close().await.unwrap();
                    assert!(*closed.borrow());
                },
                async {
                    gate_closures.signal(1);
                    gate.close().await.unwrap();
                    assert!(*closed.borrow());
                },
                async {
                    gate_closures.acquire(2).await.unwrap();
                    drop(pass);
                    closed.replace(true);
                },
            );
        })
    }

    #[test]
    fn test_close_after_timed_out_close() {
        LocalExecutor::default().run(async {
            let gate = Gate::new();
            let gate = &gate;
            let gate_closed_once = Rc::new(Semaphore::new(0));
            let task_gate = gate_closed_once.clone();

            let _task = gate
                .spawn(async move {
                    task_gate.acquire(1).await.unwrap();
                })
                .unwrap();

            timeout(Duration::from_millis(1), async move {
                gate.close().await.unwrap();
                Ok(())
            })
            .await
            .expect_err("Should have timed out");

            assert!(
                !gate.is_closed(),
                "Should still be waiting for a task that hasn't finished"
            );

            gate_closed_once.signal(1);

            gate.close().await.unwrap();
        })
    }

    #[test]
    fn test_marked_closed_without_waiting() {
        LocalExecutor::default().run(async {
            let gate = Gate::new();
            // Even if task is immediately cancelled, the gate still closes.
            drop(gate.close());
            assert!(gate.is_closed());

            let gate = Gate::new();
            let pass = gate.enter().unwrap();
            // Even if task is cancelled, the gate is still marked as closing.
            drop(gate.close());
            assert!(!gate.is_open());
            assert!(!gate.is_closed());
            // Here we install a waiter after the aborted cancel.
            let wait_for_closure = gate.close();
            join!(
                async move {
                    drop(pass);
                },
                async move {
                    wait_for_closure.await.unwrap();
                }
            );
        })
    }
}
