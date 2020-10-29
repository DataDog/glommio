// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use std::cell::RefCell;
use std::collections::hash_map::{Entry, HashMap};
use std::collections::VecDeque;
use std::future::Future;
use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
struct WaiterId(u64);

#[derive(Debug)]
struct Waiter {
    id: WaiterId,
    units: u64,
    sem_state: Rc<RefCell<State>>,
}

impl Future for Waiter {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.sem_state.borrow_mut();
        match state.try_acquire(self.units) {
            Err(x) => Poll::Ready(Err(x)),
            Ok(true) => Poll::Ready(Ok(())),
            Ok(false) => {
                state.add_waker(self.id, self.units, cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

#[derive(Debug)]
struct State {
    idgen: u64,
    avail: u64,
    virtual_consumed: u64,
    waiterset: HashMap<WaiterId, (u64, Waker)>,
    list: VecDeque<WaiterId>,
    closed: bool,
}

impl State {
    fn new(avail: u64) -> Self {
        State {
            avail,
            virtual_consumed: 0,
            list: VecDeque::new(),
            waiterset: HashMap::new(),
            closed: false,
            idgen: 0,
        }
    }

    fn available(&self) -> u64 {
        self.avail
    }

    fn new_waiter(&mut self, units: u64, state: Rc<RefCell<State>>) -> Waiter {
        self.idgen += 1;
        let id = self.idgen;
        Waiter::new(WaiterId(id), units, state)
    }

    fn add_waker(&mut self, id: WaiterId, units: u64, waker: Waker) {
        self.waiterset.insert(id, (units, waker));
        self.list.push_back(id);
    }

    fn try_acquire(&mut self, units: u64) -> Result<bool> {
        if self.closed {
            return Err(Error::new(ErrorKind::BrokenPipe, "Semaphore Broken"));
        }

        if self.avail >= units {
            self.avail -= units;
            return Ok(true);
        }
        Ok(false)
    }

    fn close(&mut self) {
        self.closed = true;
        for (_, (_, waiter)) in self.waiterset.drain() {
            waiter.wake();
        }
    }

    fn signal(&mut self, units: u64) {
        self.avail += units;
    }

    fn try_wake_one(&mut self) -> Option<Waker> {
        let id = *self.list.front()?;
        let waiterset_entry = match self.waiterset.entry(id) {
            Entry::Occupied(entry) => entry,
            Entry::Vacant(_) => unreachable!(),
        };
        let units = waiterset_entry.get().0;
        let expected_units = self.avail - self.virtual_consumed;
        if units <= expected_units {
            self.list.pop_front();
            let (units, waker) = waiterset_entry.remove();
            self.virtual_consumed += units;
            return Some(waker);
        }
        None
    }
}

impl Waiter {
    fn new(id: WaiterId, units: u64, sem_state: Rc<RefCell<State>>) -> Waiter {
        Waiter {
            id,
            units,
            sem_state,
        }
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
    sem: Rc<RefCell<State>>,
}

impl Permit {
    fn new(units: u64, sem: Rc<RefCell<State>>) -> Permit {
        Permit { units, sem }
    }
}

fn process_wakes(sem: Rc<RefCell<State>>, units: u64) {
    let mut state = sem.borrow_mut();
    state.signal(units);
    while let Some(waiter) = state.try_wake_one() {
        drop(state);
        waiter.wake();
        state = sem.borrow_mut();
    }
    state.virtual_consumed = 0;
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
    state: Rc<RefCell<State>>,
}

impl Semaphore {
    /// Creates a new semaphore with the specified amount of units
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::Semaphore;
    ///
    /// let _ = Semaphore::new(1);
    ///
    /// ```
    pub fn new(avail: u64) -> Semaphore {
        Semaphore {
            state: Rc::new(RefCell::new(State::new(avail))),
        }
    }

    /// Returns the amount of units currently available in this semaphore
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::Semaphore;
    ///
    /// let sem = Semaphore::new(1);
    /// assert_eq!(sem.available(), 1);
    ///
    /// ```
    pub fn available(&self) -> u64 {
        self.state.borrow().available()
    }

    /// Blocks until a permit can be acquired with the specified amount of units.
    ///
    /// Returns Err() if the semaphore is closed during the wait.
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::{LocalExecutor, Semaphore};
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
    /// The caller is then responsible to release it. Whenever possible,
    /// prefer acquire_permit().
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::{LocalExecutor, Semaphore};
    ///
    /// let sem = Semaphore::new(1);
    ///
    /// let ex = LocalExecutor::make_default();
    /// ex.run(async move {
    ///     sem.acquire(1).await.unwrap();
    ///     sem.signal(1); // Has to be signaled explicity. Be careful
    /// });
    /// ```
    pub async fn acquire(&self, units: u64) -> Result<()> {
        let mut state = self.state.borrow_mut();
        // Try acquiring first without paying the price to construct a waker.
        // If that fails then we construct a waker and wait on it.
        if state.list.is_empty() && state.try_acquire(units)? {
            return Ok(());
        }
        let waiter = state.new_waiter(units, self.state.clone());
        drop(state);
        waiter.await
    }

    /// Signals the semaphore to release the specified amount of units.
    ///
    /// This needs to be paired with a call to acquire(). You should not
    /// call this if the units were acquired with acquire_permit().
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::{LocalExecutor, Semaphore};
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
    /// use scipio::{LocalExecutor, Semaphore};
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
    use crate::{enclose, Local};
    use std::cell::Cell;
    use std::time::Instant;

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
                Err(e) => match e.kind() {
                    ErrorKind::BrokenPipe => {}
                    _ => panic!("Wrong Error"),
                },
            }
        });
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
}
