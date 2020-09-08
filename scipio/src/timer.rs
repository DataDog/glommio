// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use crate::parking::Reactor;
use crate::task::JoinHandle;
use crate::{Local, QueueNotFoundError, Task, TaskQueueHandle};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

/// A timer that expires after a duration of time.
///
/// Timers are futures that output the [`Instant`] at which they fired.
/// Note that because of that, Timers always block the current task queue
/// in which they currently execute.
///
/// In most situations you will want to use [`TimerAction`]
///
/// # Examples
///
/// Sleep for 100 milliseconds:
///
/// ```
/// use scipio::{LocalExecutor,Timer};
/// use std::time::Duration;
///
/// async fn sleep(dur: Duration) {
///     Timer::new(dur).await;
/// }
///
/// let ex = LocalExecutor::new(None).expect("failed to create local executor");
///
/// ex.run(async {
///     sleep(Duration::from_millis(100)).await;
/// });
/// ```
/// [`TimerAction`]: struct.TimerAction
#[derive(Debug)]
pub struct Timer {
    /// This timer's ID and last waker that polled it.
    ///
    /// When this field is set to `None`, this timer is not registered in the reactor.
    id_and_waker: Option<(usize, Waker)>,

    /// When this timer fires.
    when: Instant,
}

impl Timer {
    /// Creates a timer that expires after the given duration of time.
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::Timer;
    /// use std::time::Duration;
    ///
    /// Timer::new(Duration::from_millis(100));
    /// ```
    pub fn new(dur: Duration) -> Timer {
        Timer {
            id_and_waker: None,
            when: Instant::now() + dur,
        }
    }

    /// Resets the timer to expire after the new duration of time.
    ///
    /// Note that resetting a timer is different from creating a new timer because
    /// [`reset()`][`Timer::reset()`] does not remove the waker associated with the task that is
    /// polling the timer.
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::Timer;
    /// use std::time::Duration;
    ///
    /// let mut t = Timer::new(Duration::from_secs(1));
    /// t.reset(Duration::from_millis(100));
    /// ```
    pub fn reset(&mut self, dur: Duration) {
        if let Some((id, _)) = self.id_and_waker.as_ref() {
            // Deregister the timer from the reactor.
            Reactor::get().remove_timer(self.when, *id);
        }

        // Update the timeout.
        self.when = Instant::now() + dur;

        if let Some((id, waker)) = self.id_and_waker.as_mut() {
            // Re-register the timer with the new timeout.
            *id = Reactor::get().insert_timer(self.when, waker);
        }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        if let Some((id, _)) = self.id_and_waker.take() {
            // Deregister the timer from the reactor.
            Reactor::get().remove_timer(self.when, id);
        }
    }
}

impl Future for Timer {
    type Output = Instant;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Check if the timer has already fired.
        if Instant::now() >= self.when {
            if let Some((id, _)) = self.id_and_waker.take() {
                // Deregister the timer from the reactor.
                Reactor::get().remove_timer(self.when, id);
            }
            Poll::Ready(self.when)
        } else {
            match &self.id_and_waker {
                None => {
                    // Register the timer in the reactor.
                    let id = Reactor::get().insert_timer(self.when, cx.waker());
                    self.id_and_waker = Some((id, cx.waker().clone()));
                }
                Some((id, w)) if !w.will_wake(cx.waker()) => {
                    // Deregister the timer from the reactor to remove the old waker.
                    Reactor::get().remove_timer(self.when, *id);

                    // Register the timer in the reactor with the new waker.
                    let id = Reactor::get().insert_timer(self.when, cx.waker());
                    self.id_and_waker = Some((id, cx.waker().clone()));
                }
                Some(_) => {}
            }
            Poll::Pending
        }
    }
}

/// The TimerAction struct provides an ergonomic way to fire an action at a
/// later point in time.
///
/// In practice [`Timer`] is hard to use because it will always block the
/// current task queue. This is rarely what one wants.
///
/// The TimerAction creates a timer in the background and executes an action
/// when the timer expires. It also provides a convenient way to cancel a timer.
///
/// [`Timer`]: struct.Timer
#[derive(Debug)]
pub struct TimerAction<T> {
    handle: JoinHandle<T, ()>,
}

// This is mainly a trick because we want the function we return in repeat()
// to be a duration, but Rust only allow us to restrict by trait. So we can't
// write T: Duration, but we can write T: DurationLike and implement DurationLike
// for Duration.
pub trait DurationLike {
    fn duration(self) -> Duration;
}
impl DurationLike for Duration {
    fn duration(self) -> Duration {
        self
    }
}

impl<T: 'static> TimerAction<T> {
    /// Creates a [`TimerAction`] that will execute the associated future once after some
    /// time is passed
    ///
    /// # Arguments
    ///
    /// * `when` a [`Duration`] that represents when to execute the action.
    /// * `action` a Future to be executed after `when` is elapsed.
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::{LocalExecutor, TimerAction};
    /// use std::time::Duration;
    ///
    /// LocalExecutor::spawn_executor("test", None, || async move {
    ///     let action = TimerAction::once_in(Duration::from_millis(100), async move {
    ///         println!("Executed once");
    ///     });
    ///     action.join().await;
    /// });
    /// ```
    /// [`Duration`]: https://doc.rust-lang.org/std/time/struct.Duration.html
    /// [`TimerAction`]: struct.TimerAction
    pub fn once_in(when: Duration, action: impl Future<Output = T> + 'static) -> TimerAction<T> {
        Self::once_in_into(when, action, Local::current_task_queue()).unwrap()
    }

    /// Creates a [`TimerAction`] that will execute the associated future once after some
    /// time is passed in a specific Task Queue
    ///
    /// # Arguments
    ///
    /// * `when` a [`Duration`] that represents when to execute the action.
    /// * `action` a Future to be executed after `when` is elapsed.
    /// * `tq` the [`TaskQueueHandle`] for the TaskQueue we want.
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::{LocalExecutor, TimerAction, Local, Latency};
    /// use std::time::Duration;
    ///
    /// LocalExecutor::spawn_executor("test", None, || async move {
    ///     let tq = Local::create_task_queue(1, Latency::NotImportant, "test");
    ///     let action = TimerAction::once_in_into(Duration::from_millis(100), async move {
    ///         println!("Executed once");
    ///     }, tq).unwrap();
    ///     action.join().await;
    /// });
    /// ```
    /// [`Duration`]: https://doc.rust-lang.org/std/time/struct.Duration.html
    /// [`TimerAction`]: struct.TimerAction
    /// [`TaskQueueHandle`]: struct.TaskQueueHandle
    pub fn once_in_into(
        when: Duration,
        action: impl Future<Output = T> + 'static,
        tq: TaskQueueHandle,
    ) -> Result<TimerAction<T>, QueueNotFoundError> {
        let task = Task::local_into(
            async move {
                Timer::new(when).await;
                action.await
            },
            tq,
        )?;

        Ok(TimerAction {
            handle: task.detach(),
        })
    }

    /// Creates a [`TimerAction`] that will execute the associated future once at a specific time
    ///
    /// # Arguments
    ///
    /// * `when` an [`Instant`] that represents when to execute the action.
    /// * `action` a Future to be executed at time `when`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::{LocalExecutor, TimerAction};
    /// use std::time::{Instant, Duration};
    ///
    /// LocalExecutor::spawn_executor("test", None, || async move {
    ///     let when = Instant::now().checked_add(Duration::from_millis(100)).unwrap();
    ///     let action = TimerAction::once_at(when, async move {
    ///         println!("Executed once");
    ///     });
    ///     action.join().await;
    /// });
    /// ```
    /// [`Instant`]: https://doc.rust-lang.org/std/time/struct.Instant.html
    /// [`TimerAction`]: struct.TimerAction
    pub fn once_at(when: Instant, action: impl Future<Output = T> + 'static) -> TimerAction<T> {
        Self::once_at_into(when, action, Local::current_task_queue()).unwrap()
    }

    /// Creates a [`TimerAction`] that will execute the associated future once at a specific time
    /// in a specific Task Queue.
    ///
    /// # Arguments
    ///
    /// * `when` an [`Instant`] that represents when to execute the action.
    /// * `action` a Future to be executed at time `when`.
    /// * `tq` the [`TaskQueueHandle`] for the TaskQueue we want.
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::{LocalExecutor, TimerAction, Local, Latency};
    /// use std::time::{Instant, Duration};
    ///
    /// LocalExecutor::spawn_executor("test", None, || async move {
    ///     let tq = Local::create_task_queue(1, Latency::NotImportant, "test");
    ///     let when = Instant::now().checked_add(Duration::from_millis(100)).unwrap();
    ///     let action = TimerAction::once_at_into(when, async move {
    ///         println!("Executed once");
    ///     }, tq).unwrap();
    ///     action.join().await;
    /// });
    /// ```
    /// [`Instant`]: https://doc.rust-lang.org/std/time/struct.Instant.html
    /// [`TimerAction`]: struct.TimerAction
    /// [`TaskQueueHandle`]: struct.TaskQueueHandle
    pub fn once_at_into(
        when: Instant,
        action: impl Future<Output = T> + 'static,
        tq: TaskQueueHandle,
    ) -> Result<TimerAction<T>, QueueNotFoundError> {
        let task = Task::local_into(
            async move {
                let now = Instant::now();
                if when > now {
                    let dur = when.duration_since(now);
                    Timer::new(dur).await;
                }
                action.await
            },
            tq,
        )?;

        Ok(TimerAction {
            handle: task.detach(),
        })
    }

    /// Creates a [`TimerAction`] that will execute the associated future repeatedly in a specific
    /// Task Queue until returns None
    ///
    /// # Arguments
    ///
    /// * `action_gen` a Future to be executed repeatedly. The Future's return value must be
    /// Option<Duration>. If [`Some`], It will execute again after Duration elapses. If `None`,
    /// it stops.
    /// * `tq` the [`TaskQueueHandle`] for the TaskQueue we want.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use scipio::{LocalExecutor, TimerAction, Latency, Local};
    /// use std::time::Duration;
    ///
    /// LocalExecutor::spawn_executor("test", None, || async move {
    ///     let tq = Local::create_task_queue(1, Latency::NotImportant, "test");
    ///     let action = TimerAction::repeat_into(|| async move {
    ///         println!("Execute this!");
    ///         Some(Duration::from_millis(100))
    ///     }, tq).unwrap();
    ///     action.join().await; // this never returns
    /// });
    /// ```
    /// [`Duration`]: https://doc.rust-lang.org/std/time/struct.Duration.html
    /// [`TimerAction`]: struct.TimerAction
    /// [`TaskQueueHandle`]: struct.TaskQueueHandle
    pub fn repeat_into<G, F>(
        action_gen: G,
        tq: TaskQueueHandle,
    ) -> Result<TimerAction<()>, QueueNotFoundError>
    where
        T: DurationLike,
        G: Fn() -> F + 'static,
        F: Future<Output = Option<T>> + 'static,
    {
        let task = Task::local_into(
            async move {
                loop {
                    if let Some(period) = action_gen().await {
                        Timer::new(period.duration()).await;
                    } else {
                        break;
                    }
                }
            },
            tq,
        )?;

        Ok(TimerAction {
            handle: task.detach(),
        })
    }

    /// Creates a [`TimerAction`] that will execute the associated future repeatedly until
    /// it returns None
    ///
    /// # Arguments
    ///
    /// * `action_gen` a Future to be executed repeatedly. The Future's return value must be
    /// Option<Duration>. If [`Some`], It will execute again after Duration elapses. If `None`,
    /// it stops.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use scipio::{LocalExecutor, TimerAction};
    /// use std::time::Duration;
    ///
    /// LocalExecutor::spawn_executor("test", None, || async move {
    ///     let action = TimerAction::repeat(|| async move {
    ///         println!("Execute this!");
    ///         Some(Duration::from_millis(100))
    ///     });
    ///     action.join().await; // this never returns
    /// });
    /// ```
    /// [`Duration`]: https://doc.rust-lang.org/std/time/struct.Duration.html
    /// [`TimerAction`]: struct.TimerAction
    pub fn repeat<G, F>(action_gen: G) -> TimerAction<()>
    where
        T: DurationLike,
        G: Fn() -> F + 'static,
        F: Future<Output = Option<T>> + 'static,
    {
        Self::repeat_into(action_gen, Local::current_task_queue()).unwrap()
    }

    /// Cancel an existing [`TimerAction`]
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::{LocalExecutor, TimerAction};
    /// use std::time::Duration;
    ///
    /// LocalExecutor::spawn_executor("test", None, || async move {
    ///     let action = TimerAction::once_in(Duration::from_millis(100), async move {
    ///         println!("Will not execute this");
    ///     });
    ///     action.cancel().await;
    /// });
    /// ```
    /// [`TimerAction`]: struct.TimerAction
    pub async fn cancel(self) {
        self.handle.cancel();
        self.handle.await;
    }

    /// Waits for a [`TimerAction`] to return
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::{LocalExecutor, TimerAction};
    /// use std::time::Duration;
    ///
    /// LocalExecutor::spawn_executor("test", None, || async move {
    ///     let action = TimerAction::once_in(Duration::from_millis(100), async move {
    ///         println!("Execute this in 100ms");
    ///     });
    ///     action.join().await;
    /// });
    /// ```
    /// [`TimerAction`]: struct.TimerAction
    pub async fn join(self) {
        self.handle.await;
    }
}

#[test]
fn basic_timer_works() {
    test_executor!(async move {
        let now = Instant::now();
        Timer::new(Duration::from_millis(100)).await;
        assert!(now.elapsed().as_millis() >= 100)
    });
}

#[test]
fn basic_timer_action_instant_works() {
    use std::cell::RefCell;
    use std::rc::Rc;
    make_shared_var_mut!(0, exec1, exec2);

    test_executor!(async move {
        let when = Instant::now()
            .checked_add(Duration::from_millis(50))
            .unwrap();
        let _ = TimerAction::once_at(when, async move {
            *(exec1.borrow_mut()) = 1;
        });

        Timer::new(Duration::from_millis(100)).await;
        assert_eq!(*(exec2.borrow()), 1);
    });
}

#[test]
fn basic_timer_action_instant_past_works() {
    use std::cell::RefCell;
    use std::rc::Rc;
    make_shared_var_mut!(0, exec1, exec2);

    test_executor!(async move {
        let when = Instant::now()
            .checked_sub(Duration::from_millis(50))
            .unwrap();
        let _ = TimerAction::once_at(when, async move {
            *(exec1.borrow_mut()) = 1;
        });

        Task::<()>::later().await;
        assert_eq!(*(exec2.borrow()), 1);
    });
}

#[test]
fn basic_timer_action_works() {
    use std::cell::RefCell;
    use std::rc::Rc;
    make_shared_var_mut!(0, exec1, exec2);

    test_executor!(async move {
        let _ = TimerAction::once_in(Duration::from_millis(50), async move {
            *(exec1.borrow_mut()) = 1;
        });

        Timer::new(Duration::from_millis(100)).await;
        assert_eq!(*(exec2.borrow()), 1);
    });
}

#[test]
fn basic_timer_action_cancel_works() {
    use std::cell::RefCell;
    use std::rc::Rc;
    make_shared_var_mut!(0, exec1, exec2);

    test_executor!(async move {
        let action = TimerAction::once_in(Duration::from_millis(50), async move {
            *(exec1.borrow_mut()) = 1;
        });
        // Force this to go into the task queue to make the test more
        // realistic
        Task::<()>::later().await;
        action.cancel().await;

        Timer::new(Duration::from_millis(100)).await;
        assert_eq!(*(exec2.borrow()), 0);
    });
}

#[test]
fn basic_timer_action_cancel_fails_if_fired() {
    use std::cell::RefCell;
    use std::rc::Rc;
    make_shared_var_mut!(0, exec1, exec2);

    test_executor!(async move {
        let action = TimerAction::once_in(Duration::from_millis(1), async move {
            *(exec1.borrow_mut()) = 1;
        });
        // Force this to go into the task queue to make the test more
        // realistic
        Timer::new(Duration::from_millis(10)).await;
        action.cancel().await;

        Timer::new(Duration::from_millis(90)).await;
        // too late, fired
        assert_eq!(*(exec2.borrow()), 1);
    });
}

#[test]
fn basic_timer_action_repeat_works() {
    use std::cell::RefCell;
    use std::rc::Rc;
    make_shared_var_mut!(0, exec1, exec2);

    test_executor!(async move {
        let _ = TimerAction::repeat(move || {
            let ex = exec1.clone();
            async move {
                *(ex.borrow_mut()) += 1;
                if (*ex.borrow()) == 10 {
                    return None;
                } else {
                    return Some(Duration::from_millis(5));
                }
            }
        });
        Timer::new(Duration::from_millis(100)).await;
        let value = *(exec2.borrow());
        assert!(value == 10);
    });
}

#[test]
fn basic_timer_action_cancellation_works() {
    use std::cell::RefCell;
    use std::rc::Rc;
    make_shared_var_mut!(0, exec1, exec2);

    test_executor!(async move {
        let action = TimerAction::repeat(move || {
            let ex = exec1.clone();
            async move {
                *(ex.borrow_mut()) += 1;
                Some(Duration::from_millis(10))
            }
        });
        Timer::new(Duration::from_millis(50)).await;
        action.cancel().await;
        let old_value = *(exec2.borrow());
        Timer::new(Duration::from_millis(50)).await;
        assert_eq!(*(exec2.borrow()), old_value);
    });
}
