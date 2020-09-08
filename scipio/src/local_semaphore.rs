// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use futures::prelude::*;
use futures::task::{Context, Poll, Waker};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::rc::Rc;

struct Waiter {
    units: u64,
    woken: bool,
    waker: Option<Waker>,
}

impl Future for Waiter {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.woken {
            return Poll::Ready(());
        }
        self.waker = Some(cx.waker().clone());
        return Poll::Pending;
    }
}

#[derive(Debug)]
struct State {
    avail: u64,
    list: VecDeque<*mut Waiter>,
    closed: bool,
}

impl State {
    fn new(avail: u64) -> Self {
        State {
            avail,
            list: VecDeque::new(),
            closed: false,
        }
    }

    fn available(&self) -> u64 {
        self.avail
    }

    fn queue(&mut self, units: u64) -> Box<Waiter> {
        // FIXME: I should pin this
        let mut waiter = Box::new(Waiter::new(units));
        self.list.push_back(waiter.as_mut());
        waiter
    }

    fn try_acquire(&mut self, units: u64) -> Result<bool> {
        if self.closed == true {
            return Err(Error::new(ErrorKind::BrokenPipe, "Semaphore Broken"));
        }

        if self.list.is_empty() && self.avail >= units {
            self.avail -= units;
            return Ok(true);
        }
        return Ok(false);
    }

    fn close(&mut self) {
        self.closed = true;
        loop {
            let cont = match self.list.pop_front() {
                None => None,
                Some(waitref) => {
                    let waiter = unsafe { &mut *waitref };
                    Some(waiter.wake())
                }
            };
            if let None = cont {
                break;
            }
        }
    }

    fn signal(&mut self, units: u64) -> Option<*mut Waiter> {
        self.avail += units;

        if let Some(waitref) = self.list.front() {
            let waiter = *waitref;
            let w = unsafe { &mut *waiter };
            if w.units <= self.avail {
                self.list.pop_front();
                return Some(waiter);
            }
        }
        None
    }
}

impl Waiter {
    fn wake(&mut self) {
        if let Some(waker) = self.waker.take() {
            self.woken = true;
            waker.wake();
        }
    }

    fn new(units: u64) -> Waiter {
        Waiter {
            units,
            woken: false,
            waker: None,
        }
    }
}

/// The permit is A RAII-friendly way to acquire semaphore resources.
///
/// Resources are held while the Permit is alive, and released when the
/// permit is dropped.
#[derive(Debug)]
pub struct Permit {
    units: u64,
    sem: Rc<RefCell<State>>,
}

impl Permit {
    fn new(units: u64, sem: Rc<RefCell<State>>) -> Permit {
        Permit {
            units,
            sem: sem.clone(),
        }
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        let waker = self.sem.borrow_mut().signal(self.units);
        waker.and_then(|w| {
            let waiter = unsafe { &mut *w };
            Some(waiter.wake())
        });
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
    /// let ex = LocalExecutor::new(None).unwrap();
    /// ex.run(async move {
    ///     {
    ///         let permit = sem.acquire_permit(1).await.unwrap();
    ///         // once it is dropped it can be acquired again
    ///         // going out of scope will drop
    ///     }
    ///     let _ = sem.acquire_permit(1).await.unwrap();
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
    /// let ex = LocalExecutor::new(None).unwrap();
    /// ex.run(async move {
    ///     sem.acquire(1).await.unwrap();
    ///     sem.signal(1); // Has to be signaled explicity. Be careful
    /// });
    /// ```
    pub async fn acquire(&self, units: u64) -> Result<()> {
        loop {
            let mut state = self.state.borrow_mut();
            if state.try_acquire(units)? {
                return Ok(());
            }

            let waiter = state.queue(units);
            drop(state);
            waiter.await;
        }
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
    /// let ex = LocalExecutor::new(None).unwrap();
    /// ex.run(async move {
    ///     // Note that we can signal to expand to more units than the original capacity had.
    ///     sem.signal(1);
    ///     sem.acquire(1).await.unwrap();
    /// });
    /// ```
    pub fn signal(&self, units: u64) {
        let waker = self.state.borrow_mut().signal(units);
        waker.and_then(|w| {
            let waiter = unsafe { &mut *w };
            Some(waiter.wake())
        });
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
    /// let ex = LocalExecutor::new(None).unwrap();
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

#[test]
fn semaphore_acquisition_for_zero_unit_works() {
    make_shared_var!(Semaphore::new(1), sem1);

    test_executor!(async move {
        sem1.acquire(0).await.unwrap();
    });
}

#[test]
fn permit_raii_works() {
    use std::time::Instant;

    make_shared_var!(Semaphore::new(1), sem1, sem2);
    make_shared_var_mut!(0, exec1, exec2, exec3);

    test_executor!(
        async move {
            {
                let _ = sem1.acquire_permit(1).await.unwrap();
                update_cond!(exec1, 1);
            }
        },
        async move {
            wait_on_cond!(exec2, 1);
            // This statement will only execute if the permit was released successfully
            let _ = sem2.acquire_permit(1).await.unwrap();
            update_cond!(exec2, 2);
        },
        async move {
            // Busy loop yielding, waiting for the previous semaphore to return
            wait_on_cond!(exec3, 2, 1);
        }
    );
}

#[test]
fn explicit_signal_unblocks_waiting_semaphore() {
    use std::time::Instant;

    make_shared_var!(Semaphore::new(0), sem1, sem2);
    make_shared_var_mut!(0, exec1, exec2);

    test_executor!(
        async move {
            {
                wait_on_cond!(exec1, 1);
                let _ = sem1.acquire_permit(1).await.unwrap();
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
            let _ = sem1.acquire_permit(0).await.unwrap();
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
            let _ = sem1.acquire_permit(1).await.unwrap();
        },
        async move {
            wait_on_cond!(exec2, 1);
            sem2.close();
        }
    );
}
