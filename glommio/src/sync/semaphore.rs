// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use crate::error::{GlommioError, ResourceType};
use std::{
    cell::RefCell,
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use intrusive_collections::{
    container_of, linked_list::LinkOps, offset_of, Adapter, LinkedList, LinkedListLink, PointerOps,
};
use std::{marker::PhantomPinned, ptr::NonNull, rc::Rc};

type Result<T> = crate::error::Result<T, ()>;

#[derive(Debug)]
struct Waiter<'a> {
    node: WaiterNode,
    semaphore: &'a Semaphore,
}

#[derive(Debug)]
struct WaiterNode {
    link: LinkedListLink,
    units: u64,
    waker: RefCell<Option<Waker>>,

    // Waiter node can not be `Unpin` so its pointer could be used inside intrusive
    // collections, it also can not outlive the container which is guaranteed by the
    // Waiter lifetime bound to the Semaphore which is container of all Waiters.
    _p: PhantomPinned,
}

struct WaiterPointerOps;

unsafe impl PointerOps for WaiterPointerOps {
    type Value = WaiterNode;
    type Pointer = NonNull<WaiterNode>;

    unsafe fn from_raw(&self, value: *const Self::Value) -> Self::Pointer {
        NonNull::new(value as *mut Self::Value).expect("Passed in Pointer can not be null")
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
        // We call unchecked method because of safety check above
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

impl<'a> Waiter<'a> {
    fn new(units: u64, semaphore: &'a Semaphore) -> Waiter<'a> {
        Waiter {
            node: WaiterNode {
                link: LinkedListLink::new(),
                units,
                waker: RefCell::new(None),
                _p: PhantomPinned,
            },
            semaphore,
        }
    }

    fn remove_from_waiting_queue(
        waiter_node: Pin<&mut WaiterNode>,
        sem_state: &mut SemaphoreState,
    ) {
        if waiter_node.link.is_linked() {
            let mut cursor = unsafe {
                sem_state
                    .waiters_list
                    .cursor_mut_from_ptr(Pin::into_inner_unchecked(waiter_node) as *const _)
            };

            if cursor.remove().is_none() {
                panic!("Waiter has to be linked into the list of waiting futures");
            }
        }
    }

    fn register_in_waiting_queue(
        waiter_node: Pin<&mut WaiterNode>,
        sem_state: &mut SemaphoreState,
        waker: Waker,
    ) {
        *waiter_node.waker.borrow_mut() = Some(waker);

        if waiter_node.link.is_linked() {
            return;
        }

        sem_state.waiters_list.push_back(unsafe {
            // It is safe to use unchecked call here because we convert passed in reference
            // which can not be null
            NonNull::new_unchecked(Pin::into_inner_unchecked(waiter_node) as *mut _)
        });
    }
}

impl Drop for Waiter<'_> {
    fn drop(&mut self) {
        if self.node.link.is_linked() {
            // If node is linked that is for sure is pinned so that is safe
            // to make it pinned directly
            let waiter_node = unsafe { Pin::new_unchecked(&mut self.node) };
            Self::remove_from_waiting_queue(waiter_node, &mut self.semaphore.state.borrow_mut())
        }
    }
}

impl Future for Waiter<'_> {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut sem_state = self.semaphore.state.borrow_mut();
        let future_mut = unsafe { self.get_unchecked_mut() };

        let waiter_node = unsafe { Pin::new_unchecked(&mut future_mut.node) };

        let units = waiter_node.units;
        match sem_state.try_acquire(units) {
            Err(x) => {
                Self::remove_from_waiting_queue(waiter_node, &mut sem_state);
                Poll::Ready(Err(x))
            }
            Ok(true) => {
                Self::remove_from_waiting_queue(waiter_node, &mut sem_state);
                Poll::Ready(Ok(()))
            }
            Ok(false) => {
                Self::register_in_waiting_queue(waiter_node, &mut sem_state, cx.waker().clone());
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

        let mut cursor = self.waiters_list.front_mut();
        while !cursor.is_null() {
            let node = cursor.remove().unwrap();

            let node = unsafe { Pin::new_unchecked(&*node.as_ptr()) };
            let waker = node.waker.borrow_mut().take();
            if let Some(waker) = waker {
                waker.wake();
            } else {
                panic!("Future is linked into the waiting list without a waker");
            }
        }
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
#[must_use = "units are only held while the permit is alive. If unused then semaphore will \
              immediately release units"]
pub struct Permit<'a> {
    units: u64,
    sem: &'a Semaphore,
}

/// The static permit is A RAII-friendly way to acquire semaphore resources in
/// long-lived operations that require a static lifetime.
///
/// Resources are held while the Permit is alive, and released when the
/// permit is dropped.
#[derive(Debug)]
pub struct StaticPermit {
    units: u64,
    sem: Rc<Semaphore>,
}

impl<'a> Permit<'a> {
    fn new(units: u64, sem: &'a Semaphore) -> Permit<'a> {
        Permit { units, sem }
    }
}

impl StaticPermit {
    fn new(units: u64, sem: Rc<Semaphore>) -> StaticPermit {
        StaticPermit { units, sem }
    }

    /// Closes the underlying semaphore that originated this permit.
    pub fn close(&self) {
        self.sem.close()
    }
}

impl Drop for Permit<'_> {
    fn drop(&mut self) {
        process_wakes(self.sem, self.units);
    }
}

impl Drop for StaticPermit {
    fn drop(&mut self) {
        process_wakes(&self.sem, self.units);
    }
}

fn process_wakes(sem: &Semaphore, units: u64) {
    let mut state = sem.state.borrow_mut();
    state.signal(units);

    let mut available_units = state.avail;

    let mut cursor = state.waiters_list.front_mut();

    //only tasks which will be able to proceed will be awaken
    while available_units > 0 {
        let mut waker = None;
        if let Some(node) = cursor.get() {
            let node = unsafe { Pin::new_unchecked(node) };

            if node.units <= available_units {
                let w = node.waker.borrow_mut().take();

                if w.is_some() {
                    waker = w;
                } else {
                    panic!("Future was linked into the waiting list without a waker");
                }

                available_units -= node.units;
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

/// An implementation of semaphore that doesn't use helper threads,
/// condition variables, and is friendly to single-threaded execution.
#[derive(Debug)]
pub struct Semaphore {
    state: RefCell<SemaphoreState>,
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
    /// ```
    pub fn new(avail: u64) -> Semaphore {
        Semaphore {
            state: RefCell::new(SemaphoreState::new(avail)),
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
    /// ```
    pub fn available(&self) -> u64 {
        self.state.borrow().available()
    }

    /// Suspends until a permit can be acquired with the specified amount of
    /// units.
    ///
    /// Returns Err() if the semaphore is closed during the wait.
    ///
    /// Similar to [`acquire_static_permit`], except that it requires the permit
    /// never to be passed to contexts that require a static lifetime. As
    /// this is cheaper than [`acquire_static_permit`], it is useful in
    /// situations where the permit tracks an asynchronous operation whose
    /// lifetime is simple and well-defined.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::{sync::Semaphore, LocalExecutor};
    ///
    /// let sem = Semaphore::new(1);
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     {
    ///         let permit = sem.acquire_permit(1).await.unwrap();
    ///         // once it is dropped it can be acquired again
    ///         // going out of scope will drop
    ///     }
    ///     let _guard = sem.acquire_permit(1).await.unwrap();
    /// });
    /// ```
    ///
    /// [`acquire_static_permit`]: Semaphore::acquire_static_permit
    pub async fn acquire_permit(&self, units: u64) -> Result<Permit<'_>> {
        self.acquire(units).await?;
        Ok(Permit::new(units, self))
    }

    /// Suspends until a permit can be acquired with the specified amount of
    /// units.
    ///
    /// Returns Err() if the semaphore is closed during the wait.
    ///
    /// Similar to [`acquire_permit`], except that it requires the semaphore to
    /// be contained in an [`Rc`]. This is useful in situations where the
    /// permit tracks a long-lived asynchronous operation whose lifetime is
    /// complex.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::{sync::Semaphore, timer::sleep, LocalExecutor};
    /// use std::{rc::Rc, time::Duration};
    ///
    /// let sem = Rc::new(Semaphore::new(1));
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     {
    ///         let permit = sem.acquire_static_permit(1).await.unwrap();
    ///         glommio::spawn_local(async move {
    ///             let _guard = permit;
    ///             sleep(Duration::from_secs(1)).await;
    ///         })
    ///         .detach();
    ///         // once it is dropped it can be acquired again
    ///         // going out of scope will drop
    ///     }
    ///     let _guard = sem.acquire_permit(1).await.unwrap();
    /// });
    /// ```
    ///
    /// [`acquire_permit`]: Semaphore::acquire_permit
    /// [`Rc`]: https://doc.rust-lang.org/std/rc/struct.Rc.html
    pub async fn acquire_static_permit(self: &Rc<Self>, units: u64) -> Result<StaticPermit> {
        self.acquire(units).await?;
        Ok(StaticPermit::new(units, self.clone()))
    }

    /// Acquires the specified amount of units from this semaphore.
    ///
    /// The caller is then responsible to release units. Whenever possible,
    /// prefer acquire_permit().
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::{sync::Semaphore, LocalExecutor};
    ///
    /// let sem = Semaphore::new(1);
    ///
    /// let ex = LocalExecutor::default();
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
            Waiter::new(units, self)
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
    /// use glommio::{sync::Semaphore, LocalExecutor};
    ///
    /// let sem = Semaphore::new(1);
    ///
    /// let ex = LocalExecutor::default();
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
    /// The [`Permit`] is bound to the lifetime of the current reference of this
    /// semaphore. If you need it to live longer, consider using
    /// [`try_acquire_static_permit`].
    ///
    /// If insufficient units are available then this method will return
    /// `Err` and the number of available permits is unchanged.
    ///
    /// This method does not suspend.
    ///
    /// # Errors
    ///  If semaphore is closed
    /// `Err(GlommioError::Closed(ResourceType::Semaphore { .. }))` will be
    /// returned.  If semaphore does not have sufficient amount of units
    ///  ```ignore
    ///  Err(GlommioError::WouldBlock(ResourceType::Semaphore {
    ///      requested: u64,
    ///      available: u64
    ///  }))
    ///  ```
    ///  will be returned.
    ///
    /// # Examples
    ///
    /// ```
    /// # use glommio::{sync::Semaphore, LocalExecutor};
    /// # let ex = LocalExecutor::default();
    /// # ex.run(async move {
    /// let sem = Semaphore::new(1);
    /// let permit = sem.try_acquire_permit(1).unwrap();
    /// let permit2 = sem.try_acquire_permit(1).unwrap_err();
    /// # });
    /// ```
    /// [`Permit`]: Permit
    /// [`try_acquire_static_permit`]: Semaphore::try_acquire_static_permit
    pub fn try_acquire_permit(&self, units: u64) -> Result<Permit<'_>> {
        let mut state = self.state.borrow_mut();

        if state.waiters_list.is_empty() && state.try_acquire(units)? {
            return Ok(Permit::new(units, self));
        }
        Err(GlommioError::WouldBlock(ResourceType::Semaphore {
            requested: units,
            available: state.available(),
        }))
    }

    /// Acquires the given number of units, if they are available, and
    /// returns immediately, with the RAII guard,
    /// reducing the number of available units by the given amount.
    ///
    /// This function returns a [`StaticPermit`]
    /// and is suitable for `'static` contexts. If your lifetimes are simple,
    /// and you don't need the permit to outlive your context, consider using
    /// [`try_acquire_permit`] instead.
    ///
    /// If insufficient units are available then this method will return
    /// `Err` and the number of available permits is unchanged.
    ///
    /// This method does not suspend.
    ///
    /// # Errors
    ///  If semaphore is closed
    /// `Err(GlommioError::Closed(ResourceType::Semaphore { .. }))` will be
    /// returned.  If semaphore does not have sufficient amount of units
    ///  ```ignore
    ///  Err(GlommioError::WouldBlock(ResourceType::Semaphore {
    ///      requested: u64,
    ///      available: u64
    ///  }))
    ///  ```
    ///  will be returned.
    ///
    /// # Examples
    ///
    /// ```
    /// # use glommio::{sync::Semaphore, LocalExecutor};
    /// # use std::rc::Rc;
    /// # let ex = LocalExecutor::default();
    /// # ex.run(async move {
    /// let sem = Rc::new(Semaphore::new(1));
    /// let permit = sem.try_acquire_static_permit(1).unwrap();
    /// let permit2 = sem.try_acquire_static_permit(1).unwrap_err();
    /// # });
    /// ```
    /// [`StaticPermit`]: StaticPermit
    /// [`try_acquire_permit`]: Semaphore::try_acquire_permit
    pub fn try_acquire_static_permit(self: &Rc<Self>, units: u64) -> Result<StaticPermit> {
        let mut state = self.state.borrow_mut();

        if state.waiters_list.is_empty() && state.try_acquire(units)? {
            return Ok(StaticPermit::new(units, self.clone()));
        }
        Err(GlommioError::WouldBlock(ResourceType::Semaphore {
            requested: units,
            available: state.available(),
        }))
    }

    /// Signals the semaphore to release the specified amount of units.
    ///
    /// This needs to be paired with a call to [`Semaphore::acquire()`]. You
    /// should not call this if the units were acquired with
    /// [`Semaphore::acquire_permit()`].
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::{sync::Semaphore, LocalExecutor};
    ///
    /// let sem = Semaphore::new(0);
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     // Note that we can signal to expand to more units than the original capacity had.
    ///     sem.signal(1);
    ///     sem.acquire(1).await.unwrap();
    /// });
    /// ```
    pub fn signal(&self, units: u64) {
        process_wakes(self, units);
    }

    /// Closes the semaphore
    ///
    /// All existing waiters will return `Err()`, and no new waiters are
    /// allowed.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::{sync::Semaphore, LocalExecutor};
    ///
    /// let sem = Semaphore::new(0);
    ///
    /// let ex = LocalExecutor::default();
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

impl Drop for Semaphore {
    fn drop(&mut self) {
        assert!(self.state.borrow().waiters_list.is_empty());
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        enclose,
        timer::{sleep, Timer},
        LocalExecutor,
    };

    use futures_lite::future::or;
    use std::{
        cell::Cell,
        rc::Rc,
        time::{Duration, Instant},
    };

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

            let t1 = crate::spawn_local(enclose! { (sem, exec) async move {
                exec.set(exec.get() + 1);
                let _g = sem.acquire_permit(1).await.unwrap();
            }});
            let t2 = crate::spawn_local(enclose! { (sem, exec) async move {
                exec.set(exec.get() + 1);
                let _g = sem.acquire_permit(1).await.unwrap();
            }});

            // For this last one, use the static version. Move around and sleep a bit first
            let t3 = crate::spawn_local(enclose! { (sem, exec) async move {
                exec.set(exec.get() + 1);
                let g = sem.acquire_static_permit(1).await.unwrap();
                crate::spawn_local(async move {
                    let _g = g;
                    sleep(Duration::from_secs(1)).await;
                }).await
            }});

            // Wait for all permits to try and acquire, then unleash the gates.
            while exec.get() != 3 {
                crate::executor().yield_task_queue_now().await;
            }
            sem.signal(1);

            t3.await;
            t2.await;
            t1.await;
            sleep(Duration::from_secs(2)).await;
            assert_eq!(sem.available(), 1);
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
                sem2.signal(1);
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
    fn try_acquire_static_permit_sufficient_units() {
        let sem = Rc::new(Semaphore::new(42));
        let _ = sem.try_acquire_static_permit(24).unwrap();
    }

    #[test]
    fn try_acquire_static_permit_insufficient_units() {
        let sem = Rc::new(Semaphore::new(42));
        let result = sem.try_acquire_static_permit(62);
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
        let ex = LocalExecutor::default();
        let semaphore = Rc::new(Semaphore::new(0));
        let semaphore_c = semaphore.clone();

        ex.run(async move {
            crate::spawn_local(async move {
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

            semaphore.acquire(1).await.unwrap();
        });
    }

    #[test]
    fn semaphore_ensure_execution_order() {
        let ex = LocalExecutor::default();
        let semaphore = Rc::new(Semaphore::new(0));

        let semaphore_c1 = semaphore.clone();
        let semaphore_c2 = semaphore.clone();
        let semaphore_c3 = semaphore.clone();

        let state = Rc::new(RefCell::new(0));

        let state_c1 = state.clone();
        let state_c2 = state.clone();
        let state_c3 = state.clone();

        ex.run(async move {
            let t1 = crate::spawn_local(async move {
                *state_c1.borrow_mut() = 1;
                let _g = semaphore_c1.acquire_permit(1).await.unwrap();
                assert_eq!(*state_c1.borrow(), 3);
                *state_c1.borrow_mut() = 4;
            });

            let t2 = crate::spawn_local(async move {
                while *state_c2.borrow() != 1 {
                    crate::executor().yield_task_queue_now().await;
                }

                *state_c2.borrow_mut() = 2;
                let _g = semaphore_c2.acquire_permit(1).await.unwrap();
                assert_eq!(*state_c2.borrow(), 4);
                *state_c2.borrow_mut() = 5;
            });

            let t3 = crate::spawn_local(async move {
                while *state_c3.borrow() != 2 {
                    crate::executor().yield_task_queue_now().await;
                }

                *state_c3.borrow_mut() = 3;
                let _g = semaphore_c3.acquire_permit(1).await.unwrap();
                assert_eq!(*state_c3.borrow(), 5);
            });

            crate::spawn_local(async move {
                while *state.borrow() != 3 {
                    crate::executor().yield_task_queue_now().await;
                }

                semaphore.signal(1);
            })
            .detach();

            or(or(t1, t2), t3).await;
        });
    }
}
