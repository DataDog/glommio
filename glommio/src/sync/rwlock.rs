// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience

//! Read-write locks.
//!
//! Provides functionality similar to the ['std::sync::RwLock'] except that lock can not be poisoned.
//!
//! # Examples
//!
//! ```
//! use glommio::sync::RwLock;
//! use glommio::LocalExecutor;
//! let lock = RwLock::new(5);
//! let ex = LocalExecutor::make_default();
//!
//! ex.run( async move {
//!    // many reader locks can be held at once
//!    {
//!        let r1 = lock.read().await.unwrap();
//!        let r2 = lock.read().await.unwrap();
//!        assert_eq!(*r1, 5);
//!        assert_eq!(*r2, 5);
//!    } // read locks are dropped at this point
//!
//!    // only one write lock may be held, however
//!    {
//!        let mut w = lock.write().await.unwrap();
//!        *w += 1;
//!        assert_eq!(*w, 6);
//!    } // write lock is dropped here
//! });
//! ```
//!
use ahash::AHashMap;
use alloc::rc::Rc;
use core::fmt::Debug;
use std::cell::{Ref, RefCell, RefMut};
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::VecDeque;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::io;
use std::io::ErrorKind;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
struct WaiterId(u64);

/// A type alias for the result of a lock method which can be suspended.
pub type LockResult<T> = Result<T, LockClosedError>;

/// A type alias for the result of a non-suspending locking method.
pub type TryLockResult<T> = Result<T, TryLockError>;

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum WaiterKind {
    READER,
    WRITER,
}

///Error which indicates that RwLock is closed and can not be use
///to request lock access
pub struct LockClosedError;

impl Error for LockClosedError {}

impl Display for LockClosedError {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        f.write_str("lock already closed")
    }
}

impl Debug for LockClosedError {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        f.write_str("Closed")
    }
}

/// Error which indicates reason of the error returned by [`try_read`] and ['try_write'] methods
/// on ['RwLock']
///
/// [`try_read`]: struct.RwLock.html#method.try_read
/// [`try_write`]: struct.RwLock.html#method.try_write
pub enum TryLockError {
    ///Lock is closed and can not be used to request access to the lock
    Closed(LockClosedError),
    ///Requested access can not be granted till already hold access will be released
    WouldSuspend,
}

impl Display for TryLockError {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        match self {
            TryLockError::Closed(error) => Display::fmt(error, f),
            TryLockError::WouldSuspend => f.write_str("call would suspend execution"),
        }
    }
}

impl Debug for TryLockError {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        match self {
            TryLockError::Closed(error) => Debug::fmt(error, f),
            TryLockError::WouldSuspend => f.write_str("WouldSuspend"),
        }
    }
}

impl Error for TryLockError {}

impl From<LockClosedError> for TryLockError {
    fn from(error: LockClosedError) -> Self {
        TryLockError::Closed(error)
    }
}

impl From<LockClosedError> for io::Error {
    fn from(er: LockClosedError) -> Self {
        io::Error::new(ErrorKind::BrokenPipe, er)
    }
}

impl From<TryLockError> for io::Error {
    fn from(er: TryLockError) -> Self {
        match &er {
            TryLockError::Closed(_) => io::Error::new(ErrorKind::BrokenPipe, er),
            TryLockError::WouldSuspend => io::Error::new(ErrorKind::WouldBlock, er),
        }
    }
}

struct Waiter {
    kind: WaiterKind,
    id: WaiterId,
    rw: Rc<RefCell<State>>,
}

/// A reader-writer lock
///
/// This type of lock allows a number of readers or at most one writer at any
/// point in time. The write portion of this lock typically allows modification
/// of the underlying data (exclusive access) and the read portion of this lock
/// typically allows for read-only access (shared access).
///
/// An `RwLock` will allow any number of readers to acquire the
/// lock as long as a writer is not holding the lock.
///
/// The priority policy of the lock is based on FIFO policy. Fibers will be granted access in the
/// order in which access to the lock was requested.
///
/// Lock is not reentrant, yet. That means that two subsequent calls to request write access
/// to the lock will lead to dead lock problem.
///
/// The type parameter `T` represents the data that this lock protects. The RAII guards
/// returned from the locking methods implement [`Deref`] (and [`DerefMut`]
/// for the `write` methods) to allow access to the content of the lock.
///
///
/// # Examples
///
/// ```
///use glommio::sync::RwLock;
///use glommio::LocalExecutor;
///
/// let lock = RwLock::new(5);
/// let ex = LocalExecutor::make_default();
///
/// ex.run( async move {
///    // many reader locks can be held at once
///    {
///        let r1 = lock.read().await.unwrap();
///        let r2 = lock.read().await.unwrap();
///        assert_eq!(*r1, 5);
///        assert_eq!(*r2, 5);
///    } // read locks are dropped at this point
///
///    // only one write lock may be held, however
///    {
///        let mut w = lock.write().await.unwrap();
///        *w += 1;
///        assert_eq!(*w, 6);
///    } // write lock is dropped here
/// });
/// ```
///
#[derive(Debug)]
pub struct RwLock<T> {
    rw: Rc<RefCell<State>>,
    value: Rc<RefCell<T>>,
}

#[derive(Debug)]
struct State {
    id_gen: u64,
    //number of granted write access
    //there can be only single writer, but we use u32 type to support reentrancy fot the lock
    //in future
    writers: u32,
    //number of granted read accesses
    readers: u32,

    //number of queued requests to get write access
    queued_writers: u32,

    closed: bool,

    waiters_map: AHashMap<WaiterId, (WaiterKind, Waker)>,
    waiters: VecDeque<WaiterId>,
}

impl Waiter {
    fn new(id: WaiterId, kind: WaiterKind, rw: Rc<RefCell<State>>) -> Self {
        Waiter { id, kind, rw }
    }
}

impl Future for Waiter {
    type Output = LockResult<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.rw.borrow_mut();
        match self.kind {
            WaiterKind::WRITER => {
                if state.try_write()? {
                    Poll::Ready(Ok(()))
                } else {
                    state.add_waker(self.id, self.kind, cx.waker().clone());
                    Poll::Pending
                }
            }

            WaiterKind::READER => {
                if state.try_read()? {
                    Poll::Ready(Ok(()))
                } else {
                    state.add_waker(self.id, self.kind, cx.waker().clone());
                    Poll::Pending
                }
            }
        }
    }
}

impl State {
    fn new() -> Self {
        State {
            id_gen: 0,
            writers: 0,
            readers: 0,
            queued_writers: 0,
            closed: false,

            waiters: VecDeque::new(),
            waiters_map: AHashMap::new(),
        }
    }

    fn try_read(&mut self) -> LockResult<bool> {
        if self.closed {
            return Err(LockClosedError);
        }

        debug_assert!(!(self.readers > 0 && self.writers > 0));

        if self.writers == 0 {
            self.readers += 1;
            return Ok(true);
        }

        Ok(false)
    }

    fn try_write(&mut self) -> LockResult<bool> {
        if self.closed {
            return Err(LockClosedError);
        }

        debug_assert!(!(self.readers > 0 && self.writers > 0));

        if self.readers == 0 {
            self.writers += 1;
            return Ok(true);
        }

        Ok(false)
    }

    fn add_waker(&mut self, id: WaiterId, kind: WaiterKind, waker: Waker) {
        debug_assert!(!(self.readers > 0 && self.writers > 0));

        let entry = self.waiters_map.entry(id);
        if let Vacant(entry) = entry {
            entry.insert((kind, waker));
            self.waiters.push_back(id);

            if kind == WaiterKind::WRITER {
                self.queued_writers += 1;
            }
        }
    }
}

/// RAII structure used to release the shared read access of a lock when
/// dropped.
///
/// This structure is created by the [`read`] and [`try_read`] methods on
/// [`RwLock`].
///
/// [`read`]: RwLock::read
/// [`try_read`]: RwLock::try_read
#[derive(Debug)]
#[must_use = "if unused the RwLock will immediately unlock"]
pub struct RwLockReadGuard<'a, T> {
    rw: Rc<RefCell<State>>,
    value_ref: Ref<'a, T>,
}

impl<'a, T> Deref for RwLockReadGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        let state = (*self.rw).borrow();
        if state.closed {
            panic!("Related RwLock is already closed");
        }

        &self.value_ref
    }
}

impl<'a, T> Drop for RwLockReadGuard<'a, T> {
    fn drop(&mut self) {
        let mut state = self.rw.borrow_mut();

        if !state.closed {
            debug_assert!(state.readers > 0);
            state.readers -= 1;

            RwLock::<T>::wake_up_fibers(&mut state);
        }
    }
}

/// RAII structure used to release the exclusive write access of a lock when
/// dropped.
///
/// This structure is created by the [`write`] and [`try_write`] methods
/// on [`RwLock`].
///
/// [`write`]: RwLock::write
/// [`try_write`]: RwLock::try_write
#[must_use = "if unused the RwLock will immediately unlock"]
#[derive(Debug)]
pub struct RwLockWriteGuard<'a, T> {
    rw: Rc<RefCell<State>>,
    value_ref: RefMut<'a, T>,
}

impl<'a, T> Deref for RwLockWriteGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        let state = (*self.rw).borrow();

        if state.closed {
            panic!("Related RwLock is already closed");
        }

        &self.value_ref
    }
}

impl<'a, T> DerefMut for RwLockWriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let state = (*self.rw).borrow();

        if state.closed {
            panic!("Related RwLock is already closed");
        }

        &mut self.value_ref
    }
}

impl<'a, T> Drop for RwLockWriteGuard<'a, T> {
    fn drop(&mut self) {
        let mut state = self.rw.borrow_mut();

        if !state.closed {
            debug_assert!(state.writers > 0);
            state.writers -= 1;

            RwLock::<T>::wake_up_fibers(&mut state);
        }
    }
}

impl<T> RwLock<T> {
    /// Creates a new instance of an `RwLock<T>` which is unlocked.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::sync::RwLock;
    ///
    /// let lock = RwLock::new(5);
    /// ```
    pub fn new(value: T) -> Self {
        RwLock {
            rw: Rc::new(RefCell::new(State::new())),
            value: Rc::new(RefCell::new(value)),
        }
    }

    /// Returns a mutable reference to the underlying data.
    ///
    /// Since this call borrows the `RwLock` mutably, no actual locking needs to
    /// take place -- the mutable borrow statically guarantees no locks exist.
    ///
    /// # Errors
    ///
    /// This function will return an error if the RwLock is closed.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::sync::RwLock;
    /// use glommio::LocalExecutor;
    ///
    /// let mut lock = RwLock::new(0);
    /// let ex = LocalExecutor::make_default();
    ///
    /// ex.run(async move {
    ///     *lock.get_mut().unwrap() = 10;
    ///     assert_eq!(*lock.read().await.unwrap(), 10);
    /// });
    /// ```
    pub fn get_mut(&mut self) -> LockResult<&mut T> {
        let state = (*self.rw).borrow();
        if state.closed {
            return Err(LockClosedError);
        }

        Ok(unsafe { &mut (*self.value.as_ptr()) })
    }

    /// Locks this RwLock with shared read access, suspending the current fiber
    /// until lock can be acquired.
    ///
    /// The calling fiber will be suspended until there are no more writers which
    /// hold the lock. There may be other readers currently inside the lock when
    /// this method returns.
    ///
    /// Returns an RAII guard which will release this fiber's shared access
    /// once guard is dropped.
    ///
    /// # Errors
    ///
    /// This function will return an error if the RwLock is closed.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::sync::RwLock;
    /// use glommio::LocalExecutor;
    /// use std::rc::Rc;
    /// use futures::future::join;
    ///
    /// let lock = Rc::new(RwLock::new(1));
    /// let c_lock = lock.clone();
    ///
    /// let ex = LocalExecutor::make_default();
    ///
    /// let first_reader = ex.spawn(async move {
    ///     let n = lock.read().await.unwrap();
    ///     assert_eq!(*n, 1);
    /// });
    ///
    /// let second_reader = ex.spawn(async move {
    ///     let r = c_lock.read().await;
    ///     assert!(r.is_ok());
    /// });
    ///
    /// ex.run(async move {
    ///     join(first_reader, second_reader).await;
    /// });
    /// ```
    pub async fn read(&self) -> LockResult<RwLockReadGuard<'_, T>> {
        let mut state = self.rw.borrow_mut();
        let try_result = state.try_read()?;

        if try_result {
            return Ok(RwLockReadGuard {
                rw: self.rw.clone(),
                value_ref: self.value.borrow(),
            });
        }

        let waiter_id = state.id_gen;
        state.id_gen += 1;

        let waiter = Waiter::new(WaiterId(waiter_id), WaiterKind::READER, self.rw.clone());
        drop(state);

        waiter.await.map(|_| RwLockReadGuard {
            rw: self.rw.clone(),
            value_ref: self.value.borrow(),
        })
    }

    /// Locks this RwLock with exclusive write access, suspending the current
    /// finber until RwLock can be acquired.
    ///
    /// This function will not return while other writers or other readers
    /// currently have access to the lock.
    ///
    /// Returns an RAII guard which will drop the write access of this RwLock
    /// when dropped.
    ///
    /// # Errors
    ///
    /// This function will return an error if the RwLock is closed.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::sync::RwLock;
    /// use glommio::LocalExecutor;
    ///
    /// let lock = RwLock::new(1);
    /// let ex = LocalExecutor::make_default();
    ///
    /// ex.run(async move {
    ///     let mut n = lock.write().await.unwrap();
    ///     *n = 2;
    ///
    ///     assert!(lock.try_read().is_err());
    /// });
    /// ```
    pub async fn write(&self) -> LockResult<RwLockWriteGuard<'_, T>> {
        let mut state = self.rw.borrow_mut();
        let try_result = state.try_write()?;

        if try_result {
            return Ok(RwLockWriteGuard {
                rw: self.rw.clone(),
                value_ref: self.value.borrow_mut(),
            });
        }

        let waiter_id = state.id_gen;
        state.id_gen += 1;

        let waiter = Waiter::new(WaiterId(waiter_id), WaiterKind::WRITER, self.rw.clone());
        drop(state);

        waiter.await?;

        Ok(RwLockWriteGuard {
            rw: self.rw.clone(),
            value_ref: self.value.borrow_mut(),
        })
    }

    /// Attempts to acquire this RwLock with shared read access.
    ///
    /// If the access could not be granted at this time, then `Err` is returned.
    /// Otherwise, an RAII guard is returned which will release the shared access
    /// when guard is dropped.
    ///
    /// This function does not suspend.
    ///
    /// # Errors
    ///
    /// This function will return an error if the RwLock is closed.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::sync::RwLock;
    ///
    /// let lock = RwLock::new(1);
    ///
    /// match lock.try_read() {
    ///     Ok(n) => assert_eq!(*n, 1),
    ///     Err(_) => unreachable!(),
    /// };
    /// ```
    pub fn try_read(&self) -> TryLockResult<RwLockReadGuard<'_, T>> {
        let mut state = self.rw.borrow_mut();
        let try_result = state.try_read()?;

        if try_result {
            return Ok(RwLockReadGuard {
                rw: self.rw.clone(),
                value_ref: self.value.borrow(),
            });
        }

        Err(TryLockError::WouldSuspend)
    }

    /// Attempts to lock this RwLock with exclusive write access.
    ///
    /// If the lock could not be acquired at this time, then `Err` is returned.
    /// Otherwise, an RAII guard is returned which will release the lock when
    /// guard is dropped.
    ///
    /// This function does not suspend.
    ///
    /// # Errors
    ///
    /// This function will return an error if the RwLock is closed.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::sync::RwLock;
    /// use glommio::LocalExecutor;
    ///
    /// let lock = RwLock::new(1);
    /// let ex = LocalExecutor::make_default();
    ///
    /// ex.run(async move {
    ///   let n = lock.read().await.unwrap();
    ///   assert_eq!(*n, 1);
    ///
    ///   assert!(lock.try_write().is_err());
    /// });
    /// ```
    pub fn try_write(&self) -> TryLockResult<RwLockWriteGuard<'_, T>> {
        let mut state = self.rw.borrow_mut();
        let try_result = state.try_write()?;

        if try_result {
            return Ok(RwLockWriteGuard {
                rw: self.rw.clone(),
                value_ref: self.value.borrow_mut(),
            });
        }

        Err(TryLockError::WouldSuspend)
    }

    ///Indicates whether current RwLock is closed. Once lock is closed all subsequent calls
    ///to the methods which requests lock access will return `Err`.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::sync::RwLock;
    ///
    /// let lock = RwLock::new(());
    ///
    /// lock.close();
    ///
    /// assert!(lock.is_closed());
    /// ```
    pub fn is_closed(&self) -> bool {
        (*self.rw).borrow().closed
    }

    ///Closes current RwLock. Once lock is closed all being hold accesses will be released
    ///and all subsequent calls to the methods to request lock access will return `Err`.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::sync::RwLock;
    /// use glommio::{LocalExecutor, Local};
    /// use std::rc::Rc;
    /// use std::cell::RefCell;
    /// use glommio::sync::Semaphore;
    ///
    ///
    /// let lock = Rc::new(RwLock::new(()));
    /// let c_lock = lock.clone();
    ///
    /// let semaphore = Rc::new(Semaphore::new(0));
    /// let c_semaphore = semaphore.clone();
    ///
    /// let ex = LocalExecutor::make_default();
    ///
    /// let closer = ex.spawn(async move {
    ///      //await till read lock will be acquired
    ///      c_semaphore.acquire(1).await.unwrap();
    ///      c_lock.close();
    ///
    ///      assert!(c_lock.try_write().is_err());
    /// });
    ///
    /// let dead_locker = ex.spawn(async move {
    ///    let _r = lock.read().await.unwrap();
    ///
    ///    //allow another fiber close RwLock
    ///    semaphore.signal(1);
    ///
    ///    // this situation leads to deadlock unless lock is closed
    ///    let lock_result = lock.write().await;
    ///    assert!(lock_result.is_err());
    /// });
    ///
    /// ex.run(async move {
    ///     dead_locker.await;
    /// });
    /// ```
    pub fn close(&self) {
        let mut state = self.rw.borrow_mut();
        if state.closed {
            return;
        }
        state.writers = 0;
        state.readers = 0;

        state.closed = true;

        Self::wake_up_fibers(&mut state);
    }

    /// Consumes this `RwLock`, returning the underlying data.
    ///
    ///
    /// # Errors
    ///
    /// This function will return an error if the ReadWritreLock is closed.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::sync::RwLock;
    /// use glommio::LocalExecutor;
    ///
    /// let lock = RwLock::new(String::new());
    /// let ex = LocalExecutor::make_default();
    ///
    /// ex.run(async move {
    ///     {
    ///         let mut s = lock.write().await.unwrap();
    ///         *s = "modified".to_owned();
    ///     }
    ///
    ///     assert_eq!(lock.into_inner().unwrap(), "modified");
    /// });
    /// ```
    pub fn into_inner(self) -> LockResult<T> {
        let state = (*self.rw).borrow();
        if state.closed {
            return Err(LockClosedError {});
        }

        drop(state);

        self.close();

        let value = self.value.clone();
        drop(self);

        //it is safe operation because we own lock and because non of the guards can outlive lock
        Ok(Rc::try_unwrap(value)
            .ok()
            .expect("There are dangling references on lock's value")
            .into_inner())
    }

    fn wake_up_fibers(rw: &mut State) {
        //created with assumption in mind that waker will trigger delayed execution of fibers
        //such behaviour supports users intuition about fibers.

        //all fibers waked up in the fair order (in the order of acquiring of the ) if that matters.
        // That allows to avoid lock starvation as much as possible.
        if rw.readers == 0 && rw.writers == 0 {
            if rw.queued_writers == 0 {
                Self::wake_up_all_fibers(rw);
            } else {
                Self::wake_up_readers_and_first_writer(rw);
            }
        } else if rw.writers == 0 {
            if rw.queued_writers == 0 {
                Self::wake_up_all_fibers(rw);
            } else {
                Self::wake_up_all_readers_till_first_writer(rw);
            }
        }
    }

    fn wake_up_all_readers_till_first_writer(rw: &mut State) {
        loop {
            let waiter_id = rw.waiters.front();

            if let Some(waiter_id) = waiter_id {
                let entry = rw.waiters_map.entry(*waiter_id);
                match entry {
                    Occupied(entry) => {
                        let (waiter_kind, _) = entry.get();
                        if *waiter_kind == WaiterKind::WRITER {
                            break;
                        }

                        let (_, waker) = entry.remove();
                        waker.wake();

                        rw.waiters.pop_front().unwrap();
                    }
                    Vacant(_) => unreachable!(),
                }
            }
        }
    }
    fn wake_up_readers_and_first_writer(rw: &mut State) {
        loop {
            let waiter_id = rw.waiters.pop_front();
            if let Some(waiter_id) = waiter_id {
                let (waiter_kind, waker) = rw.waiters_map.remove(&waiter_id).unwrap();
                waker.wake();

                if waiter_kind == WaiterKind::WRITER {
                    rw.queued_writers -= 1;
                    break;
                }
            } else {
                break;
            }
        }
    }

    fn wake_up_all_fibers(rw: &mut State) {
        for (_, (_, waker)) in rw.waiters_map.drain() {
            waker.wake();
        }

        rw.waiters.clear();
    }
}

impl<T> Drop for RwLock<T> {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod test {
    use crate::sync::rwlock::LockClosedError;
    use crate::sync::rwlock::RwLock;
    use crate::sync::rwlock::TryLockError;

    use crate::sync::Semaphore;
    use crate::Local;
    use std::cell::RefCell;
    use std::rc::Rc;

    #[derive(Eq, PartialEq, Debug)]
    struct NonCopy(i32);

    #[test]
    fn test_smoke() {
        test_executor!(async move {
            let lock = RwLock::new(());
            drop(lock.read().await.unwrap());
            drop(lock.write().await.unwrap());
            drop((lock.read().await.unwrap(), lock.read().await.unwrap()));
            drop(lock.read().await.unwrap());
        });
    }

    #[test]
    fn test_frob() {
        test_executor!(async move {
            const N: u32 = 10;
            const M: usize = 1000;

            let r = Rc::new(RwLock::new(()));
            let mut futures = Vec::new();

            for _ in 0..N {
                let r = r.clone();

                let f = Task::local(async move {
                    for _ in 0..M {
                        if fastrand::u32(0..N) == 0 {
                            drop(r.write().await.unwrap());
                        } else {
                            drop(r.read().await.unwrap());
                        }
                    }
                });

                futures.push(f);
            }

            join_all(futures).await;
        });
    }

    #[test]
    fn test_close_wr() {
        test_executor!(async move {
            let rc = Rc::new(RwLock::new(1));
            let rc2 = rc.clone();

            Task::local(async move {
                let _lock = rc2.write().await.unwrap();
                rc2.close();
            })
            .await;

            assert!(rc.read().await.is_err());
        });
    }

    #[test]
    fn test_close_ww() {
        test_executor!(async move {
            let rc = Rc::new(RwLock::new(1));
            let rc2 = rc.clone();

            Task::local(async move {
                let _lock = rc2.write().await.unwrap();
                rc2.close();
            })
            .await;

            assert!(rc.write().await.is_err());
        });
    }

    #[test]
    fn test_close_rr() {
        test_executor!(async move {
            let rc = Rc::new(RwLock::new(1));
            let rc2 = rc.clone();

            Task::local(async move {
                let _lock = rc2.read().await.unwrap();
                rc2.close();
            })
            .await;

            assert!(rc.read().await.is_err());
        });
    }

    #[test]
    fn test_close_rw() {
        test_executor!(async move {
            let rc = Rc::new(RwLock::new(1));
            let rc2 = rc.clone();

            Task::local(async move {
                let _lock = rc2.read().await.unwrap();
                rc2.close();
            })
            .await;

            assert!(rc.write().await.is_err());
        });
    }

    #[test]
    fn test_global_lock() {
        test_executor!(async move {
            let rc = Rc::new(RwLock::new(0));
            let rc2 = rc.clone();

            let s = Rc::new(Semaphore::new(0));
            let s2 = s.clone();

            let mut fibers = Vec::new();
            fibers.push(Task::local(async move {
                let mut lock = rc2.write().await.unwrap();

                for _ in 0..10 {
                    Local::later().await;
                    let tmp = *lock;
                    *lock -= 1;
                    Local::later().await;
                    *lock = tmp + 1;
                }

                s2.signal(1);
            }));

            for _ in 0..5 {
                let rc3 = rc.clone();

                fibers.push(Task::local(async move {
                    let lock = rc3.read().await.unwrap();
                    assert!(*lock == 0 || *lock == 10);

                    Local::later().await;
                }));
            }

            join_all(fibers).await;
            s.acquire(1).await.unwrap();
            let lock = rc.read().await.unwrap();
            assert_eq!(*lock, 10);
        });
    }

    #[test]
    fn test_local_lock() {
        test_executor!(async move {
            let rc = Rc::new(RwLock::new(0));
            let rc2 = rc.clone();

            let s = Rc::new(Semaphore::new(0));
            let s2 = s.clone();

            let mut fibers = Vec::new();
            fibers.push(Task::local(async move {
                for _ in 0..10 {
                    let mut lock = rc2.write().await.unwrap();
                    let tmp = *lock;
                    *lock -= 1;

                    Local::later().await;

                    *lock = tmp + 1;
                }

                s2.signal(1);
            }));

            for _ in 0..5 {
                let rc3 = rc.clone();

                fibers.push(Task::local(async move {
                    let lock = rc3.read().await.unwrap();
                    assert!(*lock >= 0);

                    Local::later().await;
                }));
            }

            join_all(fibers).await;
            s.acquire(1).await.unwrap();
            let lock = rc.read().await.unwrap();
            assert_eq!(*lock, 10);
        });
    }

    #[test]
    fn test_ping_pong() {
        test_executor!(async move {
            const ITERATIONS: i32 = 10;

            let ball = Rc::new(RwLock::new(0));
            let ball2 = ball.clone();
            let ball3 = ball.clone();

            let mut fibers = Vec::new();

            let pinger = Task::local(async move {
                let mut prev = -1;
                loop {
                    //give a room for other fibers to participate
                    Local::later().await;

                    let mut lock = ball2.write().await.unwrap();
                    if *lock == ITERATIONS {
                        break;
                    }

                    if *lock % 2 == 0 {
                        *lock += 1;

                        if prev >= 0 {
                            assert_eq!(prev + 2, *lock);
                        }

                        prev = *lock;
                    }
                }
            });

            let ponger = Task::local(async move {
                let mut prev = -1;
                loop {
                    //give a room for other fibers to participate
                    Local::later().await;

                    let mut lock = ball3.write().await.unwrap();
                    if *lock == ITERATIONS {
                        break;
                    }

                    if *lock % 2 == 1 {
                        *lock += 1;

                        if prev >= 0 {
                            assert_eq!(prev + 2, *lock);
                        }

                        prev = *lock;
                    }
                }
            });

            fibers.push(pinger);
            fibers.push(ponger);

            for _ in 0..12 {
                let ball = ball.clone();
                let reader = Task::local(async move {
                    let mut prev = -1;
                    loop {
                        //give a room for other fibers to participate
                        Local::later().await;
                        let lock = ball.read().await.unwrap();

                        if *lock == ITERATIONS {
                            break;
                        }

                        assert!(prev <= *lock);
                        prev = *lock;
                    }
                });
                fibers.push(reader);
            }

            join_all(fibers).await;
        });
    }

    #[test]
    fn test_try_write() {
        test_executor!(async move {
            let lock = RwLock::new(());
            let read_guard = lock.read().await.unwrap();

            let write_result = lock.try_write();
            match write_result {
                Err(TryLockError::WouldSuspend) => (),
                Ok(_) => assert!(
                    false,
                    "try_write should not succeed while read_guard is in scope"
                ),
                Err(_) => assert!(false, "unexpected error"),
            }

            drop(read_guard);
        });
    }

    #[test]
    fn test_try_read() {
        test_executor!(async move {
            let lock = RwLock::new(());
            let read_guard = lock.write().await.unwrap();

            let write_result = lock.try_read();
            match write_result {
                Err(TryLockError::WouldSuspend) => (),
                Ok(_) => assert!(
                    false,
                    "try_read should not succeed while read_guard is in scope"
                ),
                Err(_) => assert!(false, "unexpected error"),
            }

            drop(read_guard);
        });
    }

    #[test]
    fn test_into_inner() {
        let lock = RwLock::new(NonCopy(10));
        assert_eq!(lock.into_inner().unwrap(), NonCopy(10));
    }

    #[test]
    fn test_into_inner_drop() {
        struct Foo(Rc<RefCell<usize>>);

        impl Drop for Foo {
            fn drop(&mut self) {
                *self.0.borrow_mut() += 1;
            }
        }

        let num_drop = Rc::new(RefCell::new(0));
        let lock = RwLock::new(Foo(num_drop.clone()));
        assert_eq!(*num_drop.borrow(), 0);

        {
            let _inner = lock.into_inner().unwrap();
            assert_eq!(*_inner.0.borrow(), 0);
        }

        assert_eq!(*num_drop.borrow(), 1);
    }

    #[test]
    fn test_into_inner_close() {
        let lock = RwLock::new(());
        lock.close();

        assert!(lock.is_closed());
        let into_inner_result = lock.into_inner();
        match into_inner_result {
            Err(LockClosedError) => (),
            Ok(_) => panic!("into_inner of closed lock is Ok"),
        }
    }

    #[test]
    fn test_get_mut() {
        let mut lock = RwLock::new(NonCopy(10));
        *lock.get_mut().unwrap() = NonCopy(20);
        assert_eq!(lock.into_inner().unwrap(), NonCopy(20));
    }

    #[test]
    fn test_get_mut_close() {
        let mut lock = RwLock::new(());
        lock.close();

        assert!(lock.is_closed());
        let get_mut_result = lock.get_mut();
        match get_mut_result {
            Err(LockClosedError) => (),
            Ok(_) => panic!("get_mut of closed lock is Ok"),
        }
    }
}
