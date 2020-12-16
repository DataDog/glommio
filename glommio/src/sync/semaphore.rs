// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use crate::error::GlommioError;
use crate::error::ResourceType;
use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use intrusive_collections::intrusive_adapter;
use intrusive_collections::{LinkedList, LinkedListLink};

type Result<T> = crate::error::Result<T, ()>;

#[derive(Debug)]
struct Waiter {
    node: Rc<WaiterNode>,
    sem_state: Rc<RefCell<SemaphoreState>>,
}

#[derive(Debug)]
struct WaiterNode {
    link: LinkedListLink,
    state: RefCell<WaiterState>,
}

#[derive(Debug)]
struct WaiterState {
    units: u64,
    waker: Option<Waker>,
}

intrusive_adapter!(WaiterAdapter = Rc<WaiterNode> : WaiterNode {link : LinkedListLink});

impl Waiter {
    fn new(units: u64, sem_state: Rc<RefCell<SemaphoreState>>) -> Waiter {
        Waiter {
            sem_state,
            node: Rc::new(WaiterNode {
                link: LinkedListLink::default(),
                state: RefCell::new(WaiterState { units, waker: None }),
            }),
        }
    }

    fn remove_from_waiting_queue(&self, sem_state: &mut SemaphoreState) {
        if self.node.link.is_linked() {
            let mut cursor = unsafe {
                sem_state
                    .waiters_list
                    .cursor_mut_from_ptr(self.node.as_ref())
            };

            if cursor.remove().is_none() {
                panic!("Waiter has to be linked into the list of waiting futures");
            }
        }
    }

    fn register_in_waiting_queue(&self, sem_state: &mut SemaphoreState, waker: Waker) {
        self.node.state.borrow_mut().waker = Some(waker);

        if self.node.link.is_linked() {
            return;
        }

        sem_state.waiters_list.push_front(self.node.clone());
    }
}

impl Future for Waiter {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut sem_state = self.sem_state.borrow_mut();

        let units = self.node.state.borrow().units;
        match sem_state.try_acquire(units) {
            Err(x) => {
                self.remove_from_waiting_queue(&mut sem_state);
                Poll::Ready(Err(x))
            }
            Ok(true) => {
                self.remove_from_waiting_queue(&mut sem_state);
                Poll::Ready(Ok(()))
            }
            Ok(false) => {
                self.register_in_waiting_queue(&mut sem_state, cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

#[derive(Debug)]
struct SemaphoreState {
    avail: u64,
    closed: bool,
    waiters_list: LinkedList<WaiterAdapter>,
}

impl SemaphoreState {
    fn new(avail: u64) -> Self {
        SemaphoreState {
            avail,
            closed: false,
            waiters_list: LinkedList::new(WaiterAdapter::new()),
        }
    }

    fn available(&self) -> u64 {
        self.avail
    }

    fn try_acquire(&mut self, units: u64) -> Result<bool> {
        if self.closed {
            return Err(GlommioError::Closed(ResourceType::Semaphore {
                requested: units,
                available: self.avail,
            }));
        }

        if self.avail >= units {
            self.avail -= units;
            return Ok(true);
        }
        Ok(false)
    }

    fn close(&mut self) {
        self.closed = true;

        for node in &self.waiters_list {
            if let Some(waker) = node.state.borrow_mut().waker.take() {
                waker.wake();
            } else {
                panic!("Future is linked into the waiting list without a waker")
            }
        }

        self.waiters_list.fast_clear();
    }

    fn signal(&mut self, units: u64) {
        self.avail += units;
    }
}

/// The permit is A RAII-friendly way to acquire semaphore resources.
///
/// Resources are held while the Permit is alive, and released when the
/// permit is dropped.
#[derive(Debug)]
#[must_use = "units are only held while the permit is alive. If unused then semaphore will immediately release units"]
pub struct Permit {
    units: u64,
    sem: Rc<RefCell<SemaphoreState>>,
}

impl Permit {
    fn new(units: u64, sem: Rc<RefCell<SemaphoreState>>) -> Permit {
        Permit { units, sem }
    }
}

fn process_wakes(sem: Rc<RefCell<SemaphoreState>>, units: u64) {
    let mut state = sem.borrow_mut();
    state.signal(units);

    let mut available_units = state.avail;

    let mut cursor = state.waiters_list.cursor_mut();
    cursor.move_next();

    //only tasks which will be able to proceed will be awaken
    while available_units > 0 {
        let mut waker = None;
        if let Some(node) = cursor.get() {
            let mut waiter_state = node.state.borrow_mut();

            if waiter_state.units <= available_units {
                let w = waiter_state.waker.take();

                if w.is_some() {
                    waker = w;
                } else {
                    panic!("Future was linked into the waiting list without a waker");
                }

                available_units -= waiter_state.units;
            }
        } else {
            break;
        }

        if let Some(waker) = waker {
            waker.wake();
            cursor.remove();
        } else {
            cursor.move_next();
        }
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        process_wakes(self.sem.clone(), self.units);
    }
}

/// An implementation of semaphore that doesn't use helper threads,
/// condition variables, and is friendly to single-threaded execution.
#[derive(Debug)]
pub struct Semaphore {
    state: Rc<RefCell<SemaphoreState>>,
}

impl Semaphore {
    /// Creates a new semaphore with the specified amount of units
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::sync::Semaphore;
    ///
    /// let _ = Semaphore::new(1);
    ///
    /// ```
    pub fn new(avail: u64) -> Semaphore {
        Semaphore {
            state: Rc::new(RefCell::new(SemaphoreState::new(avail))),
        }
    }

    /// Returns the amount of units currently available in this semaphore
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::sync::Semaphore;
    ///
    /// let sem = Semaphore::new(1);
    /// assert_eq!(sem.available(), 1);
    ///
    /// ```
    pub fn available(&self) -> u64 {
        self.state.borrow().available()
    }

    /// Suspends until a permit can be acquired with the specified amount of units.
    ///
    /// Returns Err() if the semaphore is closed during the wait.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::LocalExecutor;
    /// use glommio::sync::Semaphore;
    ///
    /// let sem = Semaphore::new(1);
    ///
    /// let ex = LocalExecutor::make_default();
    /// ex.run(async move {
    ///     {
    ///         let permit = sem.acquire_permit(1).await.unwrap();
    ///         // once it is dropped it can be acquired again
    ///         // going out of scope will drop
    ///     }
    ///     let _guard = sem.acquire_permit(1).await.unwrap();
    /// });
    /// ```
    pub async fn acquire_permit(&self, units: u64) -> Result<Permit> {
        self.acquire(units).await?;
        Ok(Permit::new(units, self.state.clone()))
    }

    /// Acquires the specified amount of units from this semaphore.
    ///
    /// The caller is then responsible to release units. Whenever possible,
    /// prefer acquire_permit().
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::LocalExecutor;
    /// use glommio::sync::Semaphore;
    ///
    /// let sem = Semaphore::new(1);
    ///
    /// let ex = LocalExecutor::make_default();
    /// ex.run(async move {
    ///     sem.acquire(1).await.unwrap();
    ///     sem.signal(1); // Has to be signaled explicitly. Be careful
    /// });
    /// ```
    pub async fn acquire(&self, units: u64) -> Result<()> {
        let waiter = {
            let mut state = self.state.borrow_mut();
            // Try acquiring first without paying the price to construct a waker.
            // If that fails then we construct a waker and wait on it.
            if state.waiters_list.is_empty() && state.try_acquire(units)? {
                return Ok(());
            }
            Waiter::new(units, self.state.clone())
        };

        waiter.await
    }

    /// Acquires the given number of units, if they are available, and
    /// returns immediately, with the value true,
    /// reducing the number of available units by the given amount.
    ///
    /// If insufficient units are available then this method will return
    /// immediately with the value `false` and the number of available
    /// permits is unchanged.
    ///
    /// This method does not suspend.
    ///
    /// The caller is then responsible to release units. Whenever possible,
    /// prefer [`try_acquire_permit`].
    ///
    /// # Errors
    ///
    /// If semaphore is closed `Err(io::Error(BrokenPipe))` will be returned.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::LocalExecutor;
    /// use glommio::sync::Semaphore;
    ///
    /// let sem = Semaphore::new(1);
    ///
    /// let ex = LocalExecutor::make_default();
    ///
    /// ex.run(async move {
    ///     assert!(sem.try_acquire(1).unwrap());
    ///     sem.signal(1); // Has to be signaled explicitly. Be careful
    /// });
    /// ```
    ///
    /// [`try_acquire_permit`]: Semaphore::try_acquire_permit
    pub fn try_acquire(&self, units: u64) -> Result<bool> {
        let mut state = self.state.borrow_mut();

        if state.waiters_list.is_empty() && state.try_acquire(units)? {
            return Ok(true);
        }

        Ok(false)
    }

    /// Acquires the given number of units, if they are available, and
    /// returns immediately, with the RAAI guard,
    /// reducing the number of available units by the given amount.
    ///
    /// If insufficient units are available then this method will return
    /// `Err` and the number of available permits is unchanged.
    ///
    /// This method does not suspend.
    ///
    /// # Errors
    ///  If semaphore is closed `Err(GlommioError::Closed(ResourceType::Semaphore { .. }))` will be returned.
    ///  If semaphore does not have sufficient amount of units
    ///  `
    ///  Err(GlommioError::WouldBlock(ResourceType::Semaphore {
    ///      requested: u64,
    ///      available: u64
    ///  }))
    ///  `
    ///  will be returned.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::LocalExecutor;
    /// use glommio::sync::Semaphore;
    ///
    /// let sem = Semaphore::new(1);
    ///
    /// let ex = LocalExecutor::make_default();
    ///
    /// ex.run(async move {
    ///   let permit = sem.acquire_permit(1).await.unwrap();
    ///         // once it is dropped it can be acquired again
    ///         // going out of scope will drop
    /// });
    /// ```
    pub fn try_acquire_permit(&self, units: u64) -> Result<Permit> {
        let mut state = self.state.borrow_mut();

        if state.waiters_list.is_empty() && state.try_acquire(units)? {
            return Ok(Permit::new(units, self.state.clone()));
        }
        Err(GlommioError::WouldBlock(ResourceType::Semaphore {
            requested: units,
            available: state.available(),
        }))
    }

    /// Signals the semaphore to release the specified amount of units.
    ///
    /// This needs to be paired with a call to acquire(). You should not
    /// call this if the units were acquired with acquire_permit().
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::LocalExecutor;
    /// use glommio::sync::Semaphore;
    ///
    /// let sem = Semaphore::new(0);
    ///
    /// let ex = LocalExecutor::make_default();
    /// ex.run(async move {
    ///     // Note that we can signal to expand to more units than the original capacity had.
    ///     sem.signal(1);
    ///     sem.acquire(1).await.unwrap();
    /// });
    /// ```
    pub fn signal(&self, units: u64) {
        process_wakes(self.state.clone(), units);
    }

    /// Closes the semaphore
    ///
    /// All existing waiters will return Err(), and no new waiters are allowed.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::LocalExecutor;
    /// use glommio::sync::Semaphore;
    ///
    /// let sem = Semaphore::new(0);
    ///
    /// let ex = LocalExecutor::make_default();
    /// ex.run(async move {
    ///     // Note that we can signal to expand to more units than the original capacity had.
    ///     sem.close();
    ///     if let Ok(_) = sem.acquire(1).await {
    ///         panic!("a closed semaphore should have errored");
    ///     }
    /// });
    /// ```
    pub fn close(&self) {
        let mut state = self.state.borrow_mut();
        state.close();
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::timer::Timer;
    use crate::{enclose, Local, LocalExecutor};

    use std::cell::Cell;
    use std::time::{Duration, Instant};

    #[test]
    fn semaphore_acquisition_for_zero_unit_works() {
        make_shared_var!(Semaphore::new(1), sem1);

        test_executor!(async move {
            sem1.acquire(0).await.unwrap();
        });
    }

    #[test]
    fn permit_raii_works() {
        test_executor!(async move {
            let sem = Rc::new(Semaphore::new(0));
            let exec = Rc::new(Cell::new(0));

            let t1 = Local::local(enclose! { (sem, exec) async move {
                exec.set(exec.get() + 1);
                let _g = sem.acquire_permit(1).await.unwrap();
            }});
            let t2 = Task::local(enclose! { (sem, exec) async move {
                exec.set(exec.get() + 1);
                let _g = sem.acquire_permit(1).await.unwrap();
            }});

            let t3 = Local::local(enclose! { (sem, exec) async move {
                exec.set(exec.get() + 1);
                let _g = sem.acquire_permit(1).await.unwrap();
            }});

            // Wait for all permits to try and acquire, then unleash the gates.
            while exec.get() != 3 {
                Local::later().await;
            }
            sem.signal(1);

            t3.await;
            t2.await;
            t1.await;
        });
    }

    #[test]
    fn explicit_signal_unblocks_waiting_semaphore() {
        make_shared_var!(Semaphore::new(0), sem1, sem2);
        make_shared_var_mut!(0, exec1, exec2);

        test_executor!(
            async move {
                {
                    wait_on_cond!(exec1, 1);
                    let _g = sem1.acquire_permit(1).await.unwrap();
                    update_cond!(exec1, 2);
                }
            },
            async move {
                update_cond!(exec2, 1);
                let _ = sem2.signal(1);
                wait_on_cond!(exec2, 2, 1);
            }
        );
    }

    #[test]
    fn explicit_signal_unblocks_many_wakers() {
        make_shared_var!(Semaphore::new(0), sem1, sem2, sem3);

        test_executor!(
            async move {
                sem1.acquire(1).await.unwrap();
            },
            async move {
                sem2.acquire(1).await.unwrap();
            },
            async move {
                sem3.signal(2);
            }
        );
    }

    #[test]
    fn broken_semaphore_returns_the_right_error() {
        test_executor!(async move {
            let sem = Semaphore::new(0);
            sem.close();
            match sem.acquire(0).await {
                Ok(_) => panic!("Should have failed"),
                Err(e) => match e {
                    GlommioError::Closed(ResourceType::Semaphore { .. }) => {}
                    _ => panic!("Wrong Error"),
                },
            }
        });
    }

    #[test]
    fn try_acquire_sufficient_units() {
        let sem = Semaphore::new(42);
        assert!(sem.try_acquire(24).unwrap());
    }

    #[test]
    fn try_acquire_permit_sufficient_units() {
        let sem = Semaphore::new(42);
        let _ = sem.try_acquire_permit(24).unwrap();
    }

    #[test]
    fn try_acquire_insufficient_units() {
        let sem = Semaphore::new(42);
        assert!(!sem.try_acquire(62).unwrap());
    }

    #[test]
    fn try_acquire_permit_insufficient_units() {
        let sem = Semaphore::new(42);
        let result = sem.try_acquire_permit(62);
        assert!(result.is_err());

        let err = result.err().unwrap();
        if !matches!(
            err,
            GlommioError::WouldBlock(ResourceType::Semaphore { .. })
        ) {
            panic!("Incorrect error type is returned from try_acquire_permit method");
        }
    }

    #[test]
    fn try_acquire_semaphore_is_closed() {
        let sem = Semaphore::new(42);
        sem.close();

        let result = sem.try_acquire(24);
        assert!(result.is_err());

        let err = result.err().unwrap();
        if !matches!(err, GlommioError::Closed(ResourceType::Semaphore { .. })) {
            panic!("Incorrect error type is returned from try_acquire method");
        }
    }

    #[test]
    fn try_acquire_permit_semaphore_is_closed() {
        let sem = Semaphore::new(42);
        sem.close();

        let result = sem.try_acquire_permit(24);
        assert!(result.is_err());

        let err = result.err().unwrap();
        if !matches!(err, GlommioError::Closed(ResourceType::Semaphore { .. })) {
            panic!("Incorrect error type is returned from try_acquire_permit method");
        }
    }

    #[test]
    #[should_panic]
    fn broken_semaphore_if_close_happens_first() {
        make_shared_var!(Semaphore::new(1), sem1, sem2);
        make_shared_var_mut!(0, exec1, exec2);

        test_executor!(
            async move {
                wait_on_cond!(exec1, 1);
                // even if try to acquire 0, which always succeed,
                // we should fail if it is closed.
                let _g = sem1.acquire_permit(0).await.unwrap();
            },
            async move {
                sem2.close();
                update_cond!(exec2, 1);
            }
        );
    }

    #[test]
    #[should_panic]
    fn broken_semaphore_if_acquire_happens_first() {
        // Notice how in this test, for the acquire to happen first, we
        // need to block on the acquisition. So the semaphore starts at 0
        make_shared_var!(Semaphore::new(0), sem1, sem2);
        make_shared_var_mut!(0, exec1, exec2);

        test_executor!(
            async move {
                update_cond!(exec1, 1);
                let _g = sem1.acquire_permit(1).await.unwrap();
            },
            async move {
                wait_on_cond!(exec2, 1);
                sem2.close();
            }
        );
    }

    #[test]
    fn semaphore_overflow() {
        let ex = LocalExecutor::make_default();
        let semaphore = Rc::new(Semaphore::new(0));
        let semaphore_c = semaphore.clone();

        ex.run(async move {
            Local::local(async move {
                for _ in 0..100 {
                    Timer::new(Duration::from_micros(100)).await;
                }

                let mut waiters_count = 0;
                for _ in &semaphore_c.state.borrow().waiters_list {
                    waiters_count += 1;
                }

                assert_eq!(1, waiters_count);

                semaphore_c.signal(1);
            })
            .detach();

            let _ = semaphore.acquire(1).await.unwrap();
        });
    }
}
