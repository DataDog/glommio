// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience

//! Read-write locks.
//!
//! Provides functionality similar to the ['std::sync::RwLock'] except that lock
//! can not be poisoned.
//!
//! # Examples
//!
//! ```
//! use glommio::{sync::RwLock, LocalExecutor};
//! let lock = RwLock::new(5);
//! let ex = LocalExecutor::default();
//!
//! ex.run(async move {
//!     // many reader locks can be held at once
//!     {
//!         let r1 = lock.read().await.unwrap();
//!         let r2 = lock.read().await.unwrap();
//!         assert_eq!(*r1, 5);
//!         assert_eq!(*r2, 5);
//!     } // read locks are dropped at this point
//!
//!     // only one write lock may be held, however
//!     {
//!         let mut w = lock.write().await.unwrap();
//!         *w += 1;
//!         assert_eq!(*w, 6);
//!     } // write lock is dropped here
//! });
//! ```
use core::fmt::Debug;
use std::{
    cell::{Ref, RefCell, RefMut},
    future::Future,
    ops::{Deref, DerefMut},
    pin::Pin,
    task::{Context, Poll, Waker},
};

use intrusive_collections::{
    container_of,
    linked_list::LinkOps,
    offset_of,
    Adapter,
    LinkedList,
    LinkedListLink,
    PointerOps,
};

use crate::{GlommioError, ResourceType};
use std::{marker::PhantomPinned, ptr::NonNull};

/// A type alias for the result of a lock method which can be suspended.
pub type LockResult<T> = Result<T, GlommioError<()>>;

/// A type alias for the result of a non-suspending locking method.
pub type TryLockResult<T> = Result<T, GlommioError<()>>;

#[derive(Debug)]
struct Waiter<'a, T> {
    node: WaiterNode,
    rw: &'a RwLock<T>,
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum WaiterKind {
    Reader,
    Writer,
}

#[derive(Debug)]
struct WaiterNode {
    kind: WaiterKind,
    link: LinkedListLink,
    waker: RefCell<Option<Waker>>,

    // waiter node can not be `Unpin` so its pointer could be used inside intrusive
    // collection, it also can not outlive the container which is guaranteed by the
    // Waiter lifetime bound to the RwLock which is container of all Waiters.
    _p: PhantomPinned,
}

struct WaiterPointerOps;

unsafe impl PointerOps for WaiterPointerOps {
    type Value = WaiterNode;
    type Pointer = NonNull<WaiterNode>;

    unsafe fn from_raw(&self, value: *const Self::Value) -> Self::Pointer {
        NonNull::new(value as *mut Self::Value).expect("Pointer to the value can not be null")
    }

    fn into_raw(&self, ptr: Self::Pointer) -> *const Self::Value {
        ptr.as_ptr() as *const Self::Value
    }
}

struct WaiterAdapter {
    pointers_ops: WaiterPointerOps,
    link_ops: LinkOps,
}

impl WaiterAdapter {
    fn new() -> Self {
        WaiterAdapter {
            pointers_ops: WaiterPointerOps,
            link_ops: LinkOps,
        }
    }
}

/// Adapter which converts pointer to link to the pointer to the object which is
/// hold in collection and vice versa
unsafe impl Adapter for WaiterAdapter {
    type LinkOps = LinkOps;
    type PointerOps = WaiterPointerOps;

    unsafe fn get_value(
        &self,
        link: <Self::LinkOps as intrusive_collections::LinkOps>::LinkPtr,
    ) -> *const <Self::PointerOps as PointerOps>::Value {
        container_of!(link.as_ptr(), WaiterNode, link)
    }

    unsafe fn get_link(
        &self,
        value: *const <Self::PointerOps as PointerOps>::Value,
    ) -> <Self::LinkOps as intrusive_collections::LinkOps>::LinkPtr {
        if value.is_null() {
            panic!("Passed in pointer to the value can not be null");
        }

        let ptr = (value as *const u8).add(offset_of!(WaiterNode, link));
        //null check is performed above
        core::ptr::NonNull::new_unchecked(ptr as *mut _)
    }

    fn link_ops(&self) -> &Self::LinkOps {
        &self.link_ops
    }

    fn link_ops_mut(&mut self) -> &mut Self::LinkOps {
        &mut self.link_ops
    }

    fn pointer_ops(&self) -> &Self::PointerOps {
        &self.pointers_ops
    }
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
/// The priority policy of the lock is based on FIFO policy. Fibers will be
/// granted access in the order in which access to the lock was requested.
///
/// Lock is not reentrant, yet. That means that two subsequent calls to request
/// write access to the lock will lead to deadlock problem.
///
/// The type parameter `T` represents the data that this lock protects. The RAII
/// guards returned from the locking methods implement [`Deref`] (and
/// [`DerefMut`] for the `write` methods) to allow access to the content of the
/// lock.
///
///
/// # Examples
///
/// ```
/// use glommio::{sync::RwLock, LocalExecutor};
///
/// let lock = RwLock::new(5);
/// let ex = LocalExecutor::default();
///
/// ex.run(async move {
///     // many reader locks can be held at once
///     {
///         let r1 = lock.read().await.unwrap();
///         let r2 = lock.read().await.unwrap();
///         assert_eq!(*r1, 5);
///         assert_eq!(*r2, 5);
///     } // read locks are dropped at this point
///
///     // only one write lock may be held, however
///     {
///         let mut w = lock.write().await.unwrap();
///         *w += 1;
///         assert_eq!(*w, 6);
///     } // write lock is dropped here
/// });
/// ```
#[derive(Debug)]
pub struct RwLock<T> {
    state: RefCell<State>,
    // Option is needed only to implement into_inner method so that is absolutely safe
    // to unwrap it by ref. during the execution
    value: RefCell<Option<T>>,
}

#[derive(Debug)]
struct State {
    // Number of granted write access
    // There can be only single writer, but we use u32 type to support reentrancy fot the lock
    // in future
    writers: u32,
    // Number of granted read accesses
    readers: u32,

    // Number of queued requests to get write access
    queued_writers: u32,

    waiters_queue: LinkedList<WaiterAdapter>,
    closed: bool,
}

impl<'a, T> Waiter<'a, T> {
    fn new(kind: WaiterKind, rw: &'a RwLock<T>) -> Self {
        Waiter {
            rw,
            node: WaiterNode {
                kind,
                link: LinkedListLink::new(),
                waker: RefCell::new(None),
                _p: PhantomPinned,
            },
        }
    }

    fn remove_from_waiting_queue(node: Pin<&mut WaiterNode>, rw: &mut State) {
        if node.link.is_linked() {
            let mut cursor = unsafe {
                rw.waiters_queue
                    .cursor_mut_from_ptr(node.get_unchecked_mut())
            };

            if cursor.remove().is_none() {
                panic!("Waiter has to be linked into the list of waiting futures");
            }
        }
    }

    fn register_in_waiting_queue(
        node: Pin<&mut WaiterNode>,
        rw: &mut State,
        waker: Waker,
        kind: WaiterKind,
    ) {
        *node.waker.borrow_mut() = Some(waker);

        if node.link.is_linked() {
            return;
        }

        if kind == WaiterKind::Writer {
            rw.queued_writers += 1;
        }

        // It is safe to skip null check here because we use object reference
        rw.waiters_queue
            .push_back(unsafe { NonNull::new_unchecked(node.get_unchecked_mut()) });
    }
}

impl<'a, T> Future for Waiter<'a, T> {
    type Output = LockResult<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut rw = self.rw.state.borrow_mut();
        let future_mut = unsafe { self.get_unchecked_mut() };
        let pinned_node = unsafe { Pin::new_unchecked(&mut future_mut.node) };

        match pinned_node.kind {
            WaiterKind::Writer => {
                if rw.try_write()? {
                    Self::remove_from_waiting_queue(pinned_node, &mut rw);
                    Poll::Ready(Ok(()))
                } else {
                    Self::register_in_waiting_queue(
                        pinned_node,
                        &mut rw,
                        cx.waker().clone(),
                        WaiterKind::Writer,
                    );
                    Poll::Pending
                }
            }

            WaiterKind::Reader => {
                if rw.try_read()? {
                    Self::remove_from_waiting_queue(pinned_node, &mut rw);
                    Poll::Ready(Ok(()))
                } else {
                    Self::register_in_waiting_queue(
                        pinned_node,
                        &mut rw,
                        cx.waker().clone(),
                        WaiterKind::Reader,
                    );
                    Poll::Pending
                }
            }
        }
    }
}

impl<'a, T> Drop for Waiter<'a, T> {
    fn drop(&mut self) {
        if self.node.link.is_linked() {
            // If node is lined them future is already pinned
            let pinned_node = unsafe { Pin::new_unchecked(&mut self.node) };
            Self::remove_from_waiting_queue(pinned_node, &mut self.rw.state.borrow_mut())
        }
    }
}

impl State {
    fn new() -> Self {
        State {
            writers: 0,
            readers: 0,
            queued_writers: 0,
            closed: false,

            waiters_queue: LinkedList::new(WaiterAdapter::new()),
        }
    }

    fn try_read(&mut self) -> LockResult<bool> {
        if self.closed {
            return Err(GlommioError::Closed(ResourceType::RwLock));
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
            return Err(GlommioError::Closed(ResourceType::RwLock));
        }

        debug_assert!(!(self.readers > 0 && self.writers > 0));

        if self.readers == 0 && self.writers == 0 {
            self.writers += 1;
            return Ok(true);
        }

        Ok(false)
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
    rw: &'a RwLock<T>,
    value_ref: Ref<'a, Option<T>>,
}

impl<'a, T> Deref for RwLockReadGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.value_ref.as_ref().unwrap()
    }
}

impl<'a, T> Drop for RwLockReadGuard<'a, T> {
    fn drop(&mut self) {
        let mut state = self.rw.state.borrow_mut();

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
    rw: &'a RwLock<T>,
    value_ref: RefMut<'a, Option<T>>,
}

impl<'a, T> Deref for RwLockWriteGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.value_ref.as_ref().unwrap()
    }
}

impl<'a, T> DerefMut for RwLockWriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let state = (*self.rw).state.borrow();

        if state.closed {
            panic!("Related RwLock is already closed");
        }

        self.value_ref.as_mut().unwrap()
    }
}

impl<'a, T> Drop for RwLockWriteGuard<'a, T> {
    fn drop(&mut self) {
        let mut state = self.rw.state.borrow_mut();

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
            state: RefCell::new(State::new()),
            value: RefCell::new(Some(value)),
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
    /// use glommio::{sync::RwLock, LocalExecutor};
    ///
    /// let mut lock = RwLock::new(0);
    /// let ex = LocalExecutor::default();
    ///
    /// ex.run(async move {
    ///     *lock.get_mut().unwrap() = 10;
    ///     assert_eq!(*lock.read().await.unwrap(), 10);
    /// });
    /// ```
    pub fn get_mut(&mut self) -> LockResult<&mut T> {
        let state = self.state.borrow();
        if state.closed {
            return Err(GlommioError::Closed(ResourceType::RwLock));
        }

        Ok(unsafe { &mut *(self.value.borrow_mut().as_mut().unwrap() as *mut _) })
    }

    /// Locks this RwLock with shared read access, suspending the current fiber
    /// until lock can be acquired.
    ///
    /// The calling fiber will be suspended until there are no more writers
    /// which hold the lock. There may be other readers currently inside the
    /// lock when this method returns.
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
    /// use futures::future::join;
    /// use glommio::{sync::RwLock, LocalExecutor};
    /// use std::rc::Rc;
    ///
    /// let lock = Rc::new(RwLock::new(1));
    /// let c_lock = lock.clone();
    ///
    /// let ex = LocalExecutor::default();
    ///
    /// ex.run(async move {
    ///     let first_reader = glommio::spawn_local(async move {
    ///         let n = lock.read().await.unwrap();
    ///         assert_eq!(*n, 1);
    ///     })
    ///     .detach();
    ///
    ///     let second_reader = glommio::spawn_local(async move {
    ///         let r = c_lock.read().await;
    ///         assert!(r.is_ok());
    ///     })
    ///     .detach();
    ///
    ///     join(first_reader, second_reader).await;
    /// });
    /// ```
    pub async fn read(&self) -> LockResult<RwLockReadGuard<'_, T>> {
        let waiter = {
            let mut state = self.state.borrow_mut();
            let try_result = state.try_read()?;

            if try_result {
                return Ok(RwLockReadGuard {
                    rw: self,
                    value_ref: self.value.borrow(),
                });
            }

            Waiter::new(WaiterKind::Reader, self)
        };

        waiter.await.map(|_| RwLockReadGuard {
            rw: self,
            value_ref: self.value.borrow(),
        })
    }

    /// Locks this RwLock with exclusive write access, suspending the current
    /// task until RwLock can be acquired.
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
    /// use glommio::{sync::RwLock, LocalExecutor};
    ///
    /// let lock = RwLock::new(1);
    /// let ex = LocalExecutor::default();
    ///
    /// ex.run(async move {
    ///     let mut n = lock.write().await.unwrap();
    ///     *n = 2;
    ///
    ///     assert!(lock.try_read().is_err());
    /// });
    /// ```
    pub async fn write(&self) -> LockResult<RwLockWriteGuard<'_, T>> {
        let waiter = {
            let mut state = self.state.borrow_mut();
            let try_result = state.try_write()?;

            if try_result {
                return Ok(RwLockWriteGuard {
                    rw: self,
                    value_ref: self.value.borrow_mut(),
                });
            }

            Waiter::new(WaiterKind::Writer, self)
        };
        waiter.await?;

        Ok(RwLockWriteGuard {
            rw: self,
            value_ref: self.value.borrow_mut(),
        })
    }

    /// Attempts to acquire this RwLock with shared read access.
    ///
    /// If the access could not be granted at this time, then `Err` is returned.
    /// Otherwise, an RAII guard is returned which will release the shared
    /// access when guard is dropped.
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
        let mut state = self.state.borrow_mut();
        let try_result = state.try_read()?;

        if try_result {
            return Ok(RwLockReadGuard {
                rw: self,
                value_ref: self.value.borrow(),
            });
        }

        Err(GlommioError::WouldBlock(ResourceType::RwLock))
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
    /// use glommio::{sync::RwLock, LocalExecutor};
    ///
    /// let lock = RwLock::new(1);
    /// let ex = LocalExecutor::default();
    ///
    /// ex.run(async move {
    ///     let n = lock.read().await.unwrap();
    ///     assert_eq!(*n, 1);
    ///
    ///     assert!(lock.try_write().is_err());
    /// });
    /// ```
    pub fn try_write(&self) -> TryLockResult<RwLockWriteGuard<'_, T>> {
        let mut state = self.state.borrow_mut();
        let try_result = state.try_write()?;

        if try_result {
            return Ok(RwLockWriteGuard {
                rw: self,
                value_ref: self.value.borrow_mut(),
            });
        }

        Err(GlommioError::WouldBlock(ResourceType::RwLock))
    }

    /// Indicates whether current RwLock is closed. Once lock is closed all
    /// subsequent calls to the methods which requests lock access will
    /// return `Err`.
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
        self.state.borrow().closed
    }

    /// Closes current RwLock. Once lock is closed all being hold accesses will
    /// be released and all subsequent calls to the methods to request lock
    /// access will return `Err`.
    ///
    /// # Errors
    ///
    /// This function will return an error if RwLock is still hold by any
    /// reader(s) or writer
    ///
    /// # Examples
    ///
    ///```
    /// use glommio::{
    ///     sync::{RwLock, Semaphore},
    ///     LocalExecutor,
    /// };
    /// use std::{cell::RefCell, rc::Rc};
    ///
    /// let lock = Rc::new(RwLock::new(()));
    /// let c_lock = lock.clone();
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let lock = RwLock::new(());
    ///     let guard = lock.read().await.unwrap();
    ///     assert!(lock.close().is_err());
    ///
    ///     drop(guard);
    ///     lock.close().unwrap();
    ///
    ///     assert!(lock.read().await.is_err());
    /// });
    /// ```
    pub fn close(&self) -> LockResult<()> {
        let mut state = self.state.borrow_mut();
        if state.closed {
            return Ok(());
        }

        if state.writers > 0 || state.readers > 0 {
            return Err(GlommioError::CanNotBeClosed(
                ResourceType::RwLock,
                "Lock is still held by fiber(s)",
            ));
        }

        state.closed = true;

        Self::wake_up_fibers(&mut state);
        Ok(())
    }

    /// Consumes this [`RwLock`], returning the underlying data.
    ///
    ///
    /// # Errors
    ///
    /// This function will return an error if the [`RwLock`] is closed.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::{sync::RwLock, LocalExecutor};
    ///
    /// let lock = RwLock::new(String::new());
    /// let ex = LocalExecutor::default();
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
        let state = self.state.borrow();
        if state.closed {
            return Err(GlommioError::Closed(ResourceType::RwLock));
        }

        drop(state);

        self.close().unwrap();

        let value = self.value.borrow_mut().take().unwrap();
        Ok(value)
    }

    fn wake_up_fibers(rw: &mut State) {
        // Created with assumption in mind that waker will trigger delayed execution of
        // fibers such behaviour supports users intuition about tasks.

        // All tasks waked up in the fair order (in the order of acquiring of the lock)
        // if that matters. That allows to avoid lock starvation as much as
        // possible.
        if rw.readers == 0 && rw.writers == 0 {
            if rw.queued_writers == 0 {
                // Only readers are waiting in the queue and no one holding a lock
                // wake up all of them
                Self::wake_up_all_fibers(rw);
            } else {
                // There are some writers waiting into the queue so wake up all readers and
                // single writer no one holding the lock, so likely they will be
                // executed in order so all will have a chance to proceed
                Self::wake_up_readers_and_first_writer(rw);
            }
        } else if rw.writers == 0 {
            if rw.queued_writers == 0 {
                // Only readers in the waiting queue and some readers holding the lock
                // wake up all of them
                Self::wake_up_all_fibers(rw);
            } else {
                // There are both readers and writers in the queue
                // so only readers are awakened
                Self::wake_up_all_readers_till_first_writer(rw);
            }
        }
        // The only option left that some writers still holding the lock
        // so no reason to try to wake up anyone.
    }

    fn wake_up_all_readers_till_first_writer(rw: &mut State) {
        let mut cursor = rw.waiters_queue.front_mut();
        while !cursor.is_null() {
            {
                let node = unsafe { Pin::new_unchecked(cursor.get().unwrap()) };
                if node.kind == WaiterKind::Writer {
                    break;
                }

                let waker = node.waker.borrow_mut().take();
                if let Some(waker) = waker {
                    waker.wake();
                } else {
                    panic!("Future was linked in waiting list without an a waker");
                }
            }

            cursor.remove();
        }
    }

    fn wake_up_readers_and_first_writer(rw: &mut State) {
        let mut cursor = rw.waiters_queue.front_mut();
        // We need to remove writer from the list too,
        // so we use flag instead of execution of break
        let mut only_readers = true;

        while !cursor.is_null() && only_readers {
            {
                let node = unsafe { Pin::new_unchecked(cursor.get().unwrap()) };

                let waker = node.waker.borrow_mut().take();
                if let Some(waker) = waker {
                    waker.wake();
                } else {
                    panic!("Future was linked in waiting list without an a waker");
                }

                if node.kind == WaiterKind::Writer {
                    rw.queued_writers -= 1;
                    only_readers = false;
                }
            }

            cursor.remove();
        }
    }

    fn wake_up_all_fibers(rw: &mut State) {
        let mut cursor = rw.waiters_queue.front_mut();
        while !cursor.is_null() {
            let node = cursor.remove().unwrap();
            let waker = (unsafe { node.as_ref() }).waker.borrow_mut().take();
            if let Some(waker) = waker {
                waker.wake();
            } else {
                panic!("Future was linked in waiting list without an a waker");
            }
        }
    }
}

impl<T: Default> Default for RwLock<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> Drop for RwLock<T> {
    fn drop(&mut self) {
        //Lifetime annotation prohibits guards to outlive RwLock so such unwrap is
        // safe.
        self.close().unwrap();
        assert!(self.state.borrow().waiters_queue.is_empty());
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{sync::rwlock::RwLock, timer::Timer, LocalExecutor};
    use std::time::Duration;

    use crate::sync::Semaphore;
    use std::{cell::RefCell, rc::Rc};

    #[derive(Eq, PartialEq, Debug)]
    struct NonCopy(i32);

    #[test]
    fn test_smoke() {
        test_executor!(async move {
            let lock = RwLock::new(());
            drop(lock.read().await.unwrap());
            drop(lock.write().await.unwrap());
            #[allow(clippy::eval_order_dependence)]
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

                let f = crate::spawn_local(async move {
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
    fn test_close_w() {
        test_executor!(async move {
            let rc = Rc::new(RwLock::new(1));
            let rc2 = rc.clone();

            crate::spawn_local(async move {
                let _lock = rc2.write().await.unwrap();
                assert!(rc2.close().is_err());
            })
            .await;
        });
    }

    #[test]
    fn test_close_r() {
        test_executor!(async move {
            let rc = Rc::new(RwLock::new(1));
            let rc2 = rc.clone();

            crate::spawn_local(async move {
                let _lock = rc2.read().await.unwrap();
                assert!(rc2.close().is_err());
            })
            .await;
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
            fibers.push(crate::spawn_local(async move {
                let mut lock = rc2.write().await.unwrap();

                for _ in 0..10 {
                    crate::executor().yield_task_queue_now().await;
                    let tmp = *lock;
                    *lock -= 1;
                    crate::executor().yield_task_queue_now().await;
                    *lock = tmp + 1;
                }

                s2.signal(1);
            }));

            for _ in 0..5 {
                let rc3 = rc.clone();

                fibers.push(crate::spawn_local(async move {
                    let lock = rc3.read().await.unwrap();
                    assert!(*lock == 0 || *lock == 10);

                    crate::executor().yield_task_queue_now().await;
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
            fibers.push(crate::spawn_local(async move {
                for _ in 0..10 {
                    let mut lock = rc2.write().await.unwrap();
                    let tmp = *lock;
                    *lock -= 1;

                    crate::executor().yield_task_queue_now().await;

                    *lock = tmp + 1;
                }

                s2.signal(1);
            }));

            for _ in 0..5 {
                let rc3 = rc.clone();

                fibers.push(crate::spawn_local(async move {
                    let lock = rc3.read().await.unwrap();
                    assert!(*lock >= 0);

                    crate::executor().yield_task_queue_now().await;
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

            let pinger = crate::spawn_local(async move {
                let mut prev = -1;
                loop {
                    //give a room for other fibers to participate
                    crate::executor().yield_task_queue_now().await;

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

            let ponger = crate::spawn_local(async move {
                let mut prev = -1;
                loop {
                    //give a room for other fibers to participate
                    crate::executor().yield_task_queue_now().await;

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
                let reader = crate::spawn_local(async move {
                    let mut prev = -1;
                    loop {
                        //give a room for other fibers to participate
                        crate::executor().yield_task_queue_now().await;
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
                Err(GlommioError::WouldBlock(ResourceType::RwLock)) => (),
                Ok(_) => unreachable!("try_write should not succeed while read_guard is in scope"),
                Err(_) => unreachable!("unexpected error"),
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
                Err(GlommioError::WouldBlock(ResourceType::RwLock)) => (),
                Ok(_) => unreachable!("try_read should not succeed while read_guard is in scope"),
                Err(_) => unreachable!("unexpected error"),
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
        lock.close().unwrap();

        assert!(lock.is_closed());
        let into_inner_result = lock.into_inner();
        match into_inner_result {
            Err(_) => (),
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
        lock.close().unwrap();

        assert!(lock.is_closed());
        let get_mut_result = lock.get_mut();
        match get_mut_result {
            Err(_) => (),
            Ok(_) => panic!("get_mut of closed lock is Ok"),
        }
    }

    #[test]
    fn rwlock_overflow() {
        let ex = LocalExecutor::default();

        let lock = Rc::new(RwLock::new(()));
        let c_lock = lock.clone();

        let cond = Rc::new(RefCell::new(0));
        let c_cond = cond.clone();

        let semaphore = Rc::new(Semaphore::new(0));
        let c_semaphore = semaphore.clone();

        ex.run(async move {
            crate::spawn_local(async move {
                c_semaphore.acquire(1).await.unwrap();

                let _g = c_lock.read().await.unwrap();
                *c_cond.borrow_mut() = 1;

                wait_on_cond!(c_cond, 2);

                for _ in 0..100 {
                    Timer::new(Duration::from_micros(100)).await;
                }

                let mut waiters_count = 0;
                for _ in &c_lock.state.borrow().waiters_queue {
                    waiters_count += 1;
                }

                assert_eq!(waiters_count, 1);
            })
            .detach();

            semaphore.signal(1);
            wait_on_cond!(cond, 1);
            *cond.borrow_mut() = 2;
            let _ = lock.write().await.unwrap();
        })
    }

    #[test]
    fn rwlock_reentrant() {
        let ex = LocalExecutor::default();
        ex.run(async {
            let lock = RwLock::new(());
            let _guard = lock.write().await.unwrap();
            assert!(lock.try_write().is_err());
        });
    }
}
