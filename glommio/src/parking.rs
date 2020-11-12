// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
//! Thread parking
//!
//! This module exposes a similar API as [`parking`][docs-parking]. However it is adapted to be
//! used in a thread-per-core environment. Because there is a single thread per reactor, there
//! is no way to forceably unpark a parked reactor so we don't expose an unpark function.
//!
//! Also, we expose a function called poll_io aside from park.
//!
//! poll_io will poll for I/O, but not ever sleep. This is useful when we know there are more
//! events and are just doing I/O so we don't starve anybody.
//!
//! park() is different in that it may sleep if no new events arrive. It essentially means:
//! "I, the executor, have nothing else to do. If you don't find work feel free to sleep"
//!
//! Executors may use this mechanism to go to sleep when idle and wake up when more work is
//! scheduled. By waking tasks blocked on I/O and then running those tasks on the same thread,
//! no thread context switch is necessary when going between task execution and I/O.
//!

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::ffi::CString;
use std::fmt;
use std::io;
use std::mem;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::RawFd;
use std::panic::{self, RefUnwindSafe, UnwindSafe};
use std::path::Path;
use std::rc::Rc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Poll, Waker};
use std::time::{Duration, Instant};

use futures_lite::*;

use crate::sys;
use crate::sys::{DmaBuffer, IOBuffer, PollableStatus, Source, SourceType};
use crate::IoRequirements;

thread_local!(static LOCAL_REACTOR: Reactor = Reactor::new());

/// Waits for a notification.
pub(crate) struct Parker {
    inner: Rc<Inner>,
}

impl UnwindSafe for Parker {}
impl RefUnwindSafe for Parker {}

impl Parker {
    /// Creates a new parker.
    pub(crate) fn new() -> Parker {
        // Ensure `Reactor` is initialized now to prevent it from being initialized in `Drop`.
        Parker {
            inner: Rc::new(Inner {}),
        }
    }

    /// Blocks until notified and then goes back into unnotified state.
    pub(crate) fn park(&self) {
        self.inner.park(None);
    }

    /// Performs non-sleepable pool and install a preempt timeout into the
    /// ring with `Duration`. A value of zero means we are not interested in
    /// installing a preempt timer. Tasks executing in the CPU right after this
    /// will be able to check if the timer has elapsed and yield the CPU if that
    /// is the case.
    pub(crate) fn poll_io(&self, timeout: Duration) {
        self.inner.park(Some(timeout));
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

struct Inner {}

impl Inner {
    fn park(&self, timeout: Option<Duration>) -> bool {
        // If the timeout is zero, then there is no need to actually block.
        // Process available I/O events.
        let _ = Reactor::get().react(timeout);
        false
    }
}

struct Timers {
    timer_id: u64,
    timers_by_id: HashMap<u64, Instant>,

    /// An ordered map of registered timers.
    ///
    /// Timers are in the order in which they fire. The `usize` in this type is a timer ID used to
    /// distinguish timers that fire at the same time. The `Waker` represents the task awaiting the
    /// timer.
    timers: BTreeMap<(Instant, u64), Waker>,
}

impl Timers {
    fn new() -> Timers {
        Timers {
            timer_id: 0,
            timers_by_id: HashMap::new(),
            timers: BTreeMap::new(),
        }
    }

    fn new_id(&mut self) -> u64 {
        self.timer_id += 1;
        self.timer_id
    }

    fn remove(&mut self, id: u64) {
        if let Some(when) = self.timers_by_id.remove(&id) {
            self.timers.remove(&(when, id));
        }
    }

    fn insert(&mut self, id: u64, when: Instant, waker: Waker) {
        if let Some(when) = self.timers_by_id.get_mut(&id) {
            self.timers.remove(&(*when, id));
        }
        self.timers_by_id.insert(id, when);
        self.timers.insert((when, id), waker);
    }

    fn process_timers(&mut self, wakers: &mut Vec<Waker>) -> Option<Duration> {
        let now = Instant::now();

        // Split timers into ready and pending timers.
        let pending = self.timers.split_off(&(now, 0));
        let ready = mem::replace(&mut self.timers, pending);

        // Calculate the duration until the next event.
        let dur = if ready.is_empty() {
            // Duration until the next timer.
            self.timers
                .keys()
                .next()
                .map(|(when, _)| when.saturating_duration_since(now))
        } else {
            // Timers are about to fire right now.
            Some(Duration::from_secs(0))
        };
        // Add wakers to the list.
        for (_, waker) in ready {
            wakers.push(waker);
        }

        dur
    }
}

struct SharedChannels {
    id: u64,
    check_map: BTreeMap<u64, Box<dyn Fn() -> usize>>,
    wakers_map: BTreeMap<u64, VecDeque<Waker>>,
}

impl SharedChannels {
    fn new() -> SharedChannels {
        SharedChannels {
            id: 0,
            check_map: BTreeMap::new(),
            wakers_map: BTreeMap::new(),
        }
    }

    fn process_shared_channels(&mut self, wakers: &mut Vec<Waker>) -> usize {
        let current_wakers = std::mem::replace(&mut self.wakers_map, BTreeMap::new());
        let mut added = 0;
        for (id, mut pending) in current_wakers.into_iter() {
            let room = self.check_map.get(&id).unwrap()();
            let room = std::cmp::min(room, pending.len());
            for w in pending.drain(0..room) {
                added += 1;
                wakers.push(w);
            }
            if !pending.is_empty() {
                self.wakers_map.insert(id, pending);
            }
        }
        added
    }
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

    timers: RefCell<Timers>,

    shared_channels: RefCell<SharedChannels>,

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
            timers: RefCell::new(Timers::new()),
            shared_channels: RefCell::new(SharedChannels::new()),
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

    pub(crate) fn eventfd(&self) -> Arc<AtomicUsize> {
        self.sys.eventfd()
    }

    pub(crate) fn notify(&self, remote: RawFd) {
        sys::write_eventfd(remote);
    }

    fn new_source(&self, raw: RawFd, stype: SourceType) -> Source {
        let ioreq = self.current_io_requirements.borrow();
        sys::Source::new(*ioreq, raw, stype)
    }

    pub(crate) fn inform_io_requirements(&self, req: IoRequirements) {
        let mut ioreq = self.current_io_requirements.borrow_mut();
        *ioreq = req;
    }

    pub(crate) fn register_shared_channel<F>(&self, test_function: Box<F>) -> u64
    where
        F: Fn() -> usize + 'static,
    {
        let mut channels = self.shared_channels.borrow_mut();
        let id = channels.id;
        channels.id += 1;
        let ret = channels.check_map.insert(id, test_function);
        assert_eq!(ret.is_none(), true);
        id
    }

    pub(crate) fn add_shared_channel_waker(&self, id: u64, waker: Waker) {
        let mut channels = self.shared_channels.borrow_mut();
        let map = channels.wakers_map.entry(id).or_insert_with(VecDeque::new);
        map.push_back(waker);
    }

    pub(crate) fn alloc_dma_buffer(&self, size: usize) -> DmaBuffer {
        self.sys.alloc_dma_buffer(size)
    }

    pub(crate) fn write_dma(
        &self,
        raw: RawFd,
        buf: DmaBuffer,
        pos: u64,
        pollable: PollableStatus,
    ) -> Source {
        let source = self.new_source(raw, SourceType::Write(pollable, IOBuffer::Dma(buf)));
        self.sys.write_dma(&source, pos);
        source
    }

    pub(crate) fn write_buffered(&self, raw: RawFd, buf: Vec<u8>, pos: u64) -> Source {
        let source = self.new_source(
            raw,
            SourceType::Write(PollableStatus::NonPollable, IOBuffer::Buffered(buf)),
        );
        self.sys.write_buffered(&source, pos);
        source
    }

    pub(crate) fn read_dma(
        &self,
        raw: RawFd,
        pos: u64,
        size: usize,
        pollable: PollableStatus,
    ) -> Source {
        let source = self.new_source(raw, SourceType::Read(pollable, None));
        self.sys.read_dma(&source, pos, size);
        source
    }

    pub(crate) fn read_buffered(&self, raw: RawFd, pos: u64, size: usize) -> Source {
        let source = self.new_source(raw, SourceType::Read(PollableStatus::NonPollable, None));
        self.sys.read_buffered(&source, pos, size);
        source
    }

    pub(crate) fn fdatasync(&self, raw: RawFd) -> Source {
        let source = self.new_source(raw, SourceType::FdataSync);
        self.sys.fdatasync(&source);
        source
    }

    pub(crate) fn fallocate(
        &self,
        raw: RawFd,
        position: u64,
        size: u64,
        flags: libc::c_int,
    ) -> Source {
        let source = self.new_source(raw, SourceType::Fallocate);
        self.sys.fallocate(&source, position, size, flags);
        source
    }

    pub(crate) fn close(&self, raw: RawFd) -> Source {
        let source = self.new_source(raw, SourceType::Close);
        self.sys.close(&source);
        source
    }

    pub(crate) fn statx(&self, raw: RawFd, path: &Path) -> Source {
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
    ) -> Source {
        let path = CString::new(path.as_os_str().as_bytes()).expect("path contained null!");

        let source = self.new_source(dir, SourceType::Open(path));
        self.sys.open_at(&source, flags, mode);
        source
    }

    pub(crate) fn insert_pollable_io(&self, raw: RawFd) -> io::Result<Source> {
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
    /// Returns the registered timer's ID.
    pub(crate) fn register_timer(&self) -> u64 {
        let mut timers = self.timers.borrow_mut();
        timers.new_id()
    }

    /// Registers a timer in the reactor.
    ///
    /// Returns the inserted timer's ID.
    pub(crate) fn insert_timer(&self, id: u64, when: Instant, waker: &Waker) {
        let mut timers = self.timers.borrow_mut();
        timers.insert(id, when, waker.clone());
    }

    /// Deregisters a timer from the reactor.
    pub(crate) fn remove_timer(&self, id: u64) {
        let mut timers = self.timers.borrow_mut();
        timers.remove(id);
    }

    /// Processes ready timers and extends the list of wakers to wake.
    ///
    /// Returns the duration until the next timer before this method was called.
    fn process_timers(&self, wakers: &mut Vec<Waker>) -> Option<Duration> {
        let mut timers = self.timers.borrow_mut();
        timers.process_timers(wakers)
    }

    fn process_shared_channels(&self, wakers: &mut Vec<Waker>) -> usize {
        let mut channels = self.shared_channels.borrow_mut();
        channels.process_shared_channels(wakers)
    }

    /// Processes new events, blocking until the first event or the timeout.
    fn react(&self, timeout: Option<Duration>) -> io::Result<()> {
        // FIXME: there must be a way to avoid this allocation
        // Indeed it just showed in a profiler. We can cap the number of
        // cqes produced, but this is used for timers as well. Need to
        // be more careful, but doable.
        let mut wakers = Vec::new();

        // Process ready timers.
        let next_timer = self.process_timers(&mut wakers);
        self.process_shared_channels(&mut wakers);

        // Block on I/O events.
        let res = match self.sys.wait(&mut wakers, timeout, next_timer, |wakers| {
            self.process_shared_channels(wakers)
        }) {
            // Don't wait for the next loop to process timers or shared channels
            Ok(true) => {
                self.process_timers(&mut wakers);
                Ok(())
            }

            Ok(false) => Ok(()),

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

// FIXME: source should be partitioned in two, write_dma and read_dma should not be allowed
// in files that don't support it, and same for readable() writable()
impl Source {
    pub(crate) async fn collect_rw(&self) -> io::Result<usize> {
        future::poll_fn(|cx| {
            if let Some(result) = self.take_result() {
                return Poll::Ready(result);
            }

            self.add_waiter(cx.waker().clone());
            Poll::Pending
        })
        .await
    }

    /// Waits until the I/O source is readable.
    pub(crate) async fn readable(&self) -> io::Result<()> {
        future::poll_fn(|cx| {
            if self.take_result().is_some() {
                return Poll::Ready(Ok(()));
            }

            self.add_waiter(cx.waker().clone());
            Reactor::get().sys.interest(self, true, false);
            Poll::Pending
        })
        .await
    }

    /// Waits until the I/O source is writable.
    pub(crate) async fn writable(&self) -> io::Result<()> {
        future::poll_fn(|cx| {
            if self.take_result().is_some() {
                return Poll::Ready(Ok(()));
            }

            self.add_waiter(cx.waker().clone());
            Reactor::get().sys.interest(self, false, true);
            Poll::Pending
        })
        .await
    }
}
