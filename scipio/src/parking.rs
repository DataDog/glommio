// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
//! Thread parking and unparking.
//!
//! This module exposes the exact same API as [`parking`][docs-parking]. The only
//! difference is that [`Parker`] in this module will wait on epoll/kqueue/wepoll and wake tasks
//! blocked on I/O or timers.
//!
//! Executors may use this mechanism to go to sleep when idle and wake up when more work is
//! scheduled. By waking tasks blocked on I/O and then running those tasks on the same thread,
//! no thread context switch is necessary when going between task execution and I/O.
//!
//! You can treat this module as merely an optimization over the [`parking`][docs-parking] crate.
//!
//! [docs-parking]: https://docs.rs/parking

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::ffi::CString;
use std::fmt;
use std::io;
use std::mem;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::RawFd;
use std::panic::{self, RefUnwindSafe, UnwindSafe};
use std::path::Path;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::task::{Poll, Waker};
use std::time::{Duration, Instant};

use concurrent_queue::ConcurrentQueue;
use futures_lite::*;

use crate::sys;
use crate::sys::{DmaBuffer, Source, SourceType};
use crate::IoRequirements;

thread_local!(static LOCAL_REACTOR: Reactor = Reactor::new());

/// Creates a parker and an associated unparker.
///
/// # Examples
///
/// ```
/// use scipio::parking;
///
/// let (p, u) = parking::pair();
/// ```
pub fn pair() -> (Parker, Unparker) {
    let p = Parker::new();
    let u = p.unparker();
    (p, u)
}

/// Waits for a notification.
pub struct Parker {
    unparker: Unparker,
}

impl UnwindSafe for Parker {}
impl RefUnwindSafe for Parker {}

impl Parker {
    /// Creates a new parker.
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::parking::Parker;
    ///
    /// let p = Parker::new();
    /// ```
    ///
    pub fn new() -> Parker {
        // Ensure `Reactor` is initialized now to prevent it from being initialized in `Drop`.
        let parker = Parker {
            unparker: Unparker {
                inner: Rc::new(Inner {}),
            },
        };
        parker
    }

    /// Blocks until notified and then goes back into unnotified state.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use scipio::parking::Parker;
    ///
    /// let p = Parker::new();
    /// let u = p.unparker();
    ///
    /// // Notify the parker.
    /// u.unpark();
    ///
    /// // Wakes up immediately because the parker is notified.
    /// p.park();
    /// ```
    pub fn park(&self) {
        self.unparker.inner.park(None);
    }

    /// Performs non-sleepable pool and install a preempt timeout into the
    /// ring with `Duration`. A value of zero means we are not interested in
    /// installing a preempt timer. Tasks executing in the CPU right after this
    /// will be able to check if the timer has elapsed and yield the CPU if that
    /// is the case.
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::parking::Parker;
    /// use std::time::Duration;
    ///
    /// let p = Parker::new();
    ///
    /// // Poll for existing I/O, and set next preemption point to 500ms
    /// p.poll_io(Duration::from_millis(500));
    /// ```
    pub fn poll_io(&self, timeout: Duration) {
        self.unparker.inner.park(Some(timeout));
    }

    /// Blocks until notified and then goes back into unnotified state, or times out at `instant`.
    ///
    /// Returns `true` if notified before the deadline.
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::parking::Parker;
    /// use std::time::{Duration, Instant};
    ///
    /// let p = Parker::new();
    ///
    /// // Wait for a notification, or time out after 500 ms.
    /// p.park_deadline(Instant::now() + Duration::from_millis(500));
    /// ```
    pub fn park_deadline(&self, deadline: Instant) -> bool {
        self.unparker
            .inner
            .park(Some(deadline.saturating_duration_since(Instant::now())))
    }

    /// Notifies the parker.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use scipio::parking::Parker;
    ///
    /// let p = Parker::new();
    /// let u = p.unparker();
    ///
    /// // Notify the parker.
    /// u.unpark();
    ///
    /// // Wakes up immediately because the parker is notified.
    /// p.park();
    /// ```
    pub fn unpark(&self) {
        self.unparker.unpark()
    }

    /// Returns a handle for unparking.
    ///
    /// The returned [`Unparker`] can be cloned and shared among threads.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use scipio::parking::Parker;
    ///
    /// let p = Parker::new();
    /// let u = p.unparker();
    ///
    /// // Notify the parker.
    /// u.unpark();
    ///
    /// // Wakes up immediately because the parker is notified.
    /// p.park();
    /// ```
    pub fn unparker(&self) -> Unparker {
        self.unparker.clone()
    }
}

impl Drop for Parker {
    fn drop(&mut self) {}
}

impl Default for Parker {
    fn default() -> Parker {
        Parker::new()
    }
}

impl fmt::Debug for Parker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("Parker { .. }")
    }
}

/// Notifies a parker.
pub struct Unparker {
    inner: Rc<Inner>,
}

impl UnwindSafe for Unparker {}
impl RefUnwindSafe for Unparker {}

impl Unparker {
    /// Notifies the associated parker.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use scipio::parking::Parker;
    ///
    /// let p = Parker::new();
    /// let u = p.unparker();
    ///
    /// // Notify the parker.
    /// u.unpark();
    ///
    /// // Wakes up immediately because the parker is notified.
    /// p.park();
    /// ```
    pub fn unpark(&self) {
        self.inner.unpark()
    }
}

impl fmt::Debug for Unparker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("Unparker { .. }")
    }
}

impl Clone for Unparker {
    fn clone(&self) -> Unparker {
        Unparker {
            inner: self.inner.clone(),
        }
    }
}

struct Inner {}

impl Inner {
    fn park(&self, timeout: Option<Duration>) -> bool {
        // If the timeout is zero, then there is no need to actually block.
        // Process available I/O events.
        let reactor_lock = Reactor::get().lock();
        let _ = reactor_lock.react(timeout);
        return false;
    }

    // Because this is a Thread-per-core system it is impossible to unpark.
    // We can only be awaken by some external event, and that will wake up the
    // blocking system call in park().
    pub fn unpark(&self) {}
}

/// The reactor.
///
/// Every async I/O handle and every timer is registered here. Invocations of
/// [`run()`][`crate::run()`] poll the reactor to check for new events every now and then.
///
/// There is only one global instance of this type, accessible by [`Reactor::get()`].
pub(crate) struct Reactor {
    /// Raw bindings to epoll/kqueue/wepoll.
    sys: sys::Reactor,

    /// An ordered map of registered timers.
    ///
    /// Timers are in the order in which they fire. The `usize` in this type is a timer ID used to
    /// distinguish timers that fire at the same time. The `Waker` represents the task awaiting the
    /// timer.
    timers: RefCell<BTreeMap<(Instant, usize), Waker>>,

    /// A queue of timer operations (insert and remove).
    ///
    /// When inserting or removing a timer, we don't process it immediately - we just push it into
    /// this queue. Timers actually get processed when the queue fills up or the reactor is polled.
    timer_ops: ConcurrentQueue<TimerOp>,

    /// I/O Requirements of the task currently executing.
    current_io_requirements: RefCell<IoRequirements>,

    /// Whether there are events in the latency ring.
    ///
    /// There will be events if the head and tail of the CQ ring are different.
    /// liburing has an inline function in its header to do this, but it becomes
    /// a function call if I use through uring-sys. This is quite critical and already
    /// more expensive than it should be (see comments for need_preempt()), so implement
    /// this ourselves.
    ///
    /// Also we don't want to acquire these addresses (which are behind a refcell)
    /// every time. Acquire during initialization
    preempt_ptr_head: *const u32,
    preempt_ptr_tail: *const AtomicU32,
}

impl Reactor {
    fn new() -> Reactor {
        let sys = sys::Reactor::new().expect("cannot initialize I/O event notification");
        let (preempt_ptr_head, preempt_ptr_tail) = sys.preempt_pointers();
        Reactor {
            sys,
            timers: RefCell::new(BTreeMap::new()),
            timer_ops: ConcurrentQueue::bounded(1000),
            current_io_requirements: RefCell::new(IoRequirements::default()),
            preempt_ptr_head,
            preempt_ptr_tail: preempt_ptr_tail as _,
        }
    }

    pub(crate) fn get() -> &'static Reactor {
        unsafe {
            LOCAL_REACTOR.with(|r| {
                let rc = r as *const Reactor;
                &*rc
            })
        }
    }

    #[inline(always)]
    // FIXME: This is a bit less efficient than it needs, because the scoped thread local key
    // does lazy initialization. Every time we call into this, we are paying to test if this
    // is initialized. This is what I got from objdump:
    //
    // 0:    50                      push   %rax
    // 1:    ff 15 00 00 00 00       callq  *0x0(%rip)
    // 7:    48 85 c0                test   %rax,%rax
    // a:    74 17                   je     23  <== will call into the initialization routine
    // c:    48 8b 88 38 03 00 00    mov    0x338(%rax),%rcx <== address of the head
    // 13:   48 8b 80 40 03 00 00    mov    0x340(%rax),%rax <== address of the tail
    // 1a:   8b 00                   mov    (%rax),%eax
    // 1c:   3b 01                   cmp    (%rcx),%eax <== need preempt
    // 1e:   0f 95 c0                setne  %al
    // 21:   59                      pop    %rcx
    // 22:   c3                      retq
    // 23    <== initialization stuff
    //
    // Rust has a thread local feature that is under experimental so we can maybe switch to
    // that someday.
    //
    // We will prefer to use the stable compiler and pay that unfortunate price for now.
    #[inline(always)]
    pub(crate) fn need_preempt() -> bool {
        unsafe {
            LOCAL_REACTOR.with(|r| {
                let rc = &*(r as *const Reactor);
                *rc.preempt_ptr_head != (*rc.preempt_ptr_tail).load(Ordering::Acquire)
            })
        }
    }

    fn new_source(&self, raw: RawFd, stype: SourceType) -> Pin<Box<Source>> {
        let ioreq = self.current_io_requirements.borrow();
        sys::Source::new(*ioreq, raw, stype)
    }

    pub(crate) fn inform_io_requirements(&self, req: IoRequirements) {
        let mut ioreq = self.current_io_requirements.borrow_mut();
        *ioreq = req;
    }

    pub(crate) fn alloc_dma_buffer(&self, size: usize) -> DmaBuffer {
        self.sys.alloc_dma_buffer(size)
    }

    pub(crate) fn write_dma(
        &self,
        raw: RawFd,
        buf: &DmaBuffer,
        pos: u64,
        pollable: bool,
    ) -> Pin<Box<Source>> {
        let source = self.new_source(raw, SourceType::DmaWrite(pollable));
        self.sys.write_dma(&source.as_ref(), buf, pos);
        source
    }

    pub(crate) fn read_dma<'a>(
        &self,
        raw: RawFd,
        pos: u64,
        size: usize,
        pollable: bool,
    ) -> Pin<Box<Source>> {
        let source = self.new_source(raw, SourceType::DmaRead(pollable, None));
        self.sys.read_dma(&source.as_ref(), pos, size);
        source
    }

    pub(crate) fn fdatasync(&self, raw: RawFd) -> Pin<Box<Source>> {
        let source = self.new_source(raw, SourceType::FdataSync);
        self.sys.fdatasync(&source.as_ref());
        source
    }

    pub(crate) fn fallocate(
        &self,
        raw: RawFd,
        position: u64,
        size: u64,
        flags: libc::c_int,
    ) -> Pin<Box<Source>> {
        let source = self.new_source(raw, SourceType::Fallocate);
        self.sys.fallocate(&source.as_ref(), position, size, flags);
        source
    }

    pub(crate) fn close(&self, raw: RawFd) -> Pin<Box<Source>> {
        let source = self.new_source(raw, SourceType::Close);
        self.sys.close(&source.as_ref());
        source
    }

    pub(crate) fn statx(&self, raw: RawFd, path: &Path) -> Pin<Box<Source>> {
        let path = CString::new(path.as_os_str().as_bytes()).expect("path contained null!");

        let statx_buf = unsafe {
            let statx_buf = mem::MaybeUninit::<libc::statx>::zeroed();
            statx_buf.assume_init()
        };

        let source = self.new_source(
            raw,
            SourceType::Statx(path, Box::new(RefCell::new(statx_buf))),
        );
        self.sys.statx(&source);
        source
    }

    pub(crate) fn open_at(
        &self,
        dir: RawFd,
        path: &Path,
        flags: libc::c_int,
        mode: libc::c_int,
    ) -> Pin<Box<Source>> {
        let path = CString::new(path.as_os_str().as_bytes()).expect("path contained null!");

        let source = self.new_source(dir, SourceType::Open(path));
        self.sys.open_at(&source.as_ref(), flags, mode);
        source
    }

    pub(crate) fn insert_pollable_io(&self, raw: RawFd) -> io::Result<Pin<Box<Source>>> {
        let source = self.new_source(raw, SourceType::PollableFd);
        self.sys.insert(raw)?;
        Ok(source)
    }

    /*
    /// Deregisters an I/O source from the reactor.
    pub(crate) fn cancel_io(&self, source: &Source) {
        self.sys.cancel_io(source)
    }
    */

    /// Registers a timer in the reactor.
    ///
    /// Returns the inserted timer's ID.
    pub(crate) fn insert_timer(&self, when: Instant, waker: &Waker) -> usize {
        // Generate a new timer ID.
        static ID_GENERATOR: AtomicUsize = AtomicUsize::new(1);
        let id = ID_GENERATOR.fetch_add(1, Ordering::Relaxed);

        // Push an insert operation.
        while self
            .timer_ops
            .push(TimerOp::Insert(when, id, waker.clone()))
            .is_err()
        {
            // If the queue is full, drain it and try again.
            let mut timers = self.timers.borrow_mut();
            self.process_timer_ops(&mut timers);
        }

        id
    }

    /// Deregisters a timer from the reactor.
    pub(crate) fn remove_timer(&self, when: Instant, id: usize) {
        // Push a remove operation.
        while self.timer_ops.push(TimerOp::Remove(when, id)).is_err() {
            // If the queue is full, drain it and try again.
            let mut timers = self.timers.borrow_mut();
            self.process_timer_ops(&mut timers);
        }
    }

    /// Locks the reactor, potentially blocking if the lock is held by another thread.
    fn lock(&self) -> ReactorLock<'_> {
        let reactor = self;
        ReactorLock { reactor }
    }

    /// Processes ready timers and extends the list of wakers to wake.
    ///
    /// Returns the duration until the next timer before this method was called.
    fn process_timers(&self, wakers: &mut Vec<Waker>) -> Option<Duration> {
        let mut timers = self.timers.borrow_mut();
        self.process_timer_ops(&mut timers);

        let now = Instant::now();

        // Split timers into ready and pending timers.
        let pending = timers.split_off(&(now, 0));
        let ready = mem::replace(&mut *timers, pending);

        // Calculate the duration until the next event.
        let dur = if ready.is_empty() {
            // Duration until the next timer.
            timers
                .keys()
                .next()
                .map(|(when, _)| when.saturating_duration_since(now))
        } else {
            // Timers are about to fire right now.
            Some(Duration::from_secs(0))
        };

        // Drop the lock before waking.
        drop(timers);

        // Add wakers to the list.
        for (_, waker) in ready {
            wakers.push(waker);
        }

        dur
    }

    /// Processes queued timer operations.
    fn process_timer_ops(&self, timers: &mut BTreeMap<(Instant, usize), Waker>) {
        // Process only as much as fits into the queue, or else this loop could in theory run
        // forever.
        for _ in 0..self.timer_ops.capacity().unwrap() {
            match self.timer_ops.pop() {
                Ok(TimerOp::Insert(when, id, waker)) => {
                    timers.insert((when, id), waker);
                }
                Ok(TimerOp::Remove(when, id)) => {
                    timers.remove(&(when, id));
                }
                Err(_) => break,
            }
        }
    }
}

/// A lock on the reactor.
struct ReactorLock<'a> {
    reactor: &'a Reactor,
}

impl ReactorLock<'_> {
    /// Processes new events, blocking until the first event or the timeout.
    fn react(self, timeout: Option<Duration>) -> io::Result<()> {
        // FIXME: there must be a way to avoid this allocation
        // Indeed it just showed in a profiler. We can cap the number of
        // cqes produced, but this is used for timers as well. Need to
        // be more careful, but doable.
        let mut wakers = Vec::new();

        // Process ready timers.
        let next_timer = self.reactor.process_timers(&mut wakers);

        // Block on I/O events.
        let res = match self.reactor.sys.wait(&mut wakers, timeout, next_timer) {
            // We slept, so don't wait for the next loop to process timers
            Ok(true) => {
                self.reactor.process_timers(&mut wakers);
                Ok(())
            }

            // At least one I/O event occurred.
            Ok(_) => Ok(()),

            // The syscall was interrupted.
            Err(err) if err.kind() == io::ErrorKind::Interrupted => Ok(()),

            // An actual error occureed.
            Err(err) => Err(err),
        };

        // Wake up ready tasks.
        for waker in wakers {
            // Don't let a panicking waker blow everything up.
            let _ = panic::catch_unwind(|| waker.wake());
        }

        res
    }
}

/// A single timer operation.
enum TimerOp {
    Insert(Instant, usize, Waker),
    Remove(Instant, usize),
}

// FIXME: source should be partitioned in two, write_dma and read_dma should not be allowed
// in files that don't support it, and same for readable() writable()
impl Source {
    pub(crate) async fn collect_rw(&self) -> io::Result<usize> {
        future::poll_fn(|cx| {
            let mut w = self.wakers.borrow_mut();

            if let Some(result) = w.result.take() {
                return Poll::Ready(result);
            }

            w.waiters.push(cx.waker().clone());
            Poll::Pending
        })
        .await
    }

    /// Waits until the I/O source is readable.
    pub(crate) async fn readable(&self) -> io::Result<()> {
        future::poll_fn(|cx| {
            let mut w = self.wakers.borrow_mut();

            if let Some(_) = w.result.take() {
                return Poll::Ready(Ok(()));
            }

            Reactor::get().sys.interest(self, true, false);
            w.waiters.push(cx.waker().clone());
            Poll::Pending
        })
        .await
    }

    /// Waits until the I/O source is writable.
    pub(crate) async fn writable(&self) -> io::Result<()> {
        future::poll_fn(|cx| {
            let mut w = self.wakers.borrow_mut();

            if let Some(_) = w.result.take() {
                return Poll::Ready(Ok(()));
            }

            Reactor::get().sys.interest(self, false, true);
            w.waiters.push(cx.waker().clone());
            Poll::Pending
        })
        .await
    }
}
