// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
//! Thread parking
//!
//! This module exposes a similar API as [`parking`][docs-parking]. However it
//! is adapted to be used in a thread-per-core environment. Because there is a
//! single thread per reactor, there is no way to forceably unpark a parked
//! reactor so we don't expose an unpark function.
//!
//! Also, we expose a function called poll_io aside from park.
//!
//! poll_io will poll for I/O, but not ever sleep. This is useful when we know
//! there are more events and are just doing I/O so we don't starve anybody.
//!
//! park() is different in that it may sleep if no new events arrive. It
//! essentially means: "I, the executor, have nothing else to do. If you don't
//! find work feel free to sleep"
//!
//! Executors may use this mechanism to go to sleep when idle and wake up when
//! more work is scheduled. By waking tasks blocked on I/O and then running
//! those tasks on the same thread, no thread context switch is necessary when
//! going between task execution and I/O.

use crate::iou::sqe::SockAddrStorage;
use ahash::AHashMap;
use nix::sys::socket::{MsgFlags, SockAddr};
use std::{
    cell::RefCell,
    collections::{BTreeMap, VecDeque},
    ffi::CString,
    fmt,
    io,
    mem,
    os::unix::{ffi::OsStrExt, io::RawFd},
    panic::{RefUnwindSafe, UnwindSafe},
    path::Path,
    rc::Rc,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    task::Waker,
    time::{Duration, Instant},
};

use crate::{
    io::IoScheduler,
    sys,
    sys::{
        DirectIo,
        DmaBuffer,
        IoBuffer,
        PollableStatus,
        SleepNotifier,
        Source,
        SourceType,
        StatsCollectionFn,
    },
    IoRequirements,
    IoStats,
    Latency,
    Local,
    TaskQueueHandle,
};

/// Waits for a notification.
pub(crate) struct Parker {
    inner: Rc<Inner>,
}

impl UnwindSafe for Parker {}
impl RefUnwindSafe for Parker {}

impl Parker {
    /// Creates a new parker.
    pub(crate) fn new() -> Parker {
        // Ensure `Reactor` is initialized now to prevent it from being initialized in
        // `Drop`.
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

impl fmt::Debug for Reactor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("Reactor { .. }")
    }
}

struct Inner {}

impl Inner {
    fn park(&self, timeout: Option<Duration>) -> bool {
        // If the timeout is zero, then there is no need to actually block.
        // Process available I/O events.
        let _ = Local::get_reactor().react(timeout);
        false
    }
}

struct Timers {
    timer_id: u64,
    timers_by_id: AHashMap<u64, Instant>,

    /// An ordered map of registered timers.
    ///
    /// Timers are in the order in which they fire. The `usize` in this type is
    /// a timer ID used to distinguish timers that fire at the same time.
    /// The `Waker` represents the task awaiting the timer.
    timers: BTreeMap<(Instant, u64), Waker>,
}

impl Timers {
    fn new() -> Timers {
        Timers {
            timer_id: 0,
            timers_by_id: AHashMap::new(),
            timers: BTreeMap::new(),
        }
    }

    fn new_id(&mut self) -> u64 {
        self.timer_id += 1;
        self.timer_id
    }

    fn remove(&mut self, id: u64) -> Option<Waker> {
        if let Some(when) = self.timers_by_id.remove(&id) {
            return self.timers.remove(&(when, id));
        }

        None
    }

    fn insert(&mut self, id: u64, when: Instant, waker: Waker) {
        if let Some(when) = self.timers_by_id.get_mut(&id) {
            self.timers.remove(&(*when, id));
        }
        self.timers_by_id.insert(id, when);
        self.timers.insert((when, id), waker);
    }

    /// Return the duration until next event and the number of
    /// ready and woke timers.
    fn process_timers(&mut self) -> (Option<Duration>, usize) {
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

        let woke = ready.len();
        for (_, waker) in ready {
            wake!(waker);
        }

        (dur, woke)
    }
}

struct SharedChannels {
    id: u64,
    check_map: BTreeMap<u64, Box<dyn Fn() -> usize>>,
    wakers_map: BTreeMap<u64, VecDeque<Waker>>,
    connection_wakers: Vec<Waker>,
}

impl SharedChannels {
    fn new() -> SharedChannels {
        SharedChannels {
            id: 0,
            connection_wakers: Vec::new(),
            check_map: BTreeMap::new(),
            wakers_map: BTreeMap::new(),
        }
    }

    fn process_shared_channels(&mut self) -> usize {
        let mut woke = self.connection_wakers.len();
        for waker in self.connection_wakers.drain(..) {
            wake!(waker);
        }

        let current_wakers = mem::take(&mut self.wakers_map);
        for (id, mut pending) in current_wakers.into_iter() {
            let room = self.check_map.get(&id).unwrap()();
            let room = std::cmp::min(room, pending.len());
            for waker in pending.drain(0..room) {
                woke += 1;
                wake!(waker);
            }
            if !pending.is_empty() {
                self.wakers_map.insert(id, pending);
            }
        }
        woke
    }
}

/// The reactor.
///
/// Every async I/O handle and every timer is registered here. Invocations of
/// [`run()`][`crate::run()`] poll the reactor to check for new events every now
/// and then.
///
/// There is only one global instance of this type, accessible by
/// [`Local::get_reactor()`].
pub(crate) struct Reactor {
    /// Raw bindings to epoll/kqueue/wepoll.
    pub(crate) sys: sys::Reactor,

    timers: RefCell<Timers>,

    shared_channels: RefCell<SharedChannels>,

    io_scheduler: Rc<IoScheduler>,

    /// Whether there are events in the latency ring.
    ///
    /// There will be events if the head and tail of the CQ ring are different.
    /// liburing has an inline function in its header to do this, but it becomes
    /// a function call if I use through uring-sys. This is quite critical and
    /// already more expensive than it should be (see comments for
    /// need_preempt()), so implement this ourselves.
    ///
    /// Also we don't want to acquire these addresses (which are behind a
    /// refcell) every time. Acquire during initialization
    preempt_ptr_head: *const u32,
    preempt_ptr_tail: *const AtomicU32,
}

impl Reactor {
    pub(crate) fn new(notifier: Arc<SleepNotifier>, io_memory: usize) -> Reactor {
        let sys = sys::Reactor::new(notifier, io_memory)
            .expect("cannot initialize I/O event notification");
        let (preempt_ptr_head, preempt_ptr_tail) = sys.preempt_pointers();
        Reactor {
            sys,
            timers: RefCell::new(Timers::new()),
            shared_channels: RefCell::new(SharedChannels::new()),
            io_scheduler: Rc::new(IoScheduler::new()),
            preempt_ptr_head,
            preempt_ptr_tail: preempt_ptr_tail as _,
        }
    }

    pub(crate) fn io_stats(&self) -> IoStats {
        self.sys.io_stats()
    }

    pub(crate) fn task_queue_io_stats(&self, handle: &TaskQueueHandle) -> Option<IoStats> {
        self.sys.task_queue_io_stats(handle)
    }

    #[inline(always)]
    pub(crate) fn need_preempt(&self) -> bool {
        unsafe { *self.preempt_ptr_head != (*self.preempt_ptr_tail).load(Ordering::Acquire) }
    }

    pub(crate) fn id(&self) -> usize {
        self.sys.id()
    }

    pub(crate) fn notify(&self, remote: RawFd) {
        sys::write_eventfd(remote);
    }

    fn new_source(
        &self,
        raw: RawFd,
        stype: SourceType,
        stats_collection_fn: Option<StatsCollectionFn>,
    ) -> Source {
        sys::Source::new(
            self.io_scheduler.requirements(),
            raw,
            stype,
            stats_collection_fn,
            Some(Local::current_task_queue()),
        )
    }

    pub(crate) fn inform_io_requirements(&self, req: IoRequirements) {
        self.io_scheduler.inform_requirements(req);
    }

    pub(crate) fn register_shared_channel<F>(&self, test_function: Box<F>) -> u64
    where
        F: Fn() -> usize + 'static,
    {
        let mut channels = self.shared_channels.borrow_mut();
        let id = channels.id;
        channels.id += 1;
        let ret = channels.check_map.insert(id, test_function);
        assert!(ret.is_none());
        id
    }

    pub(crate) fn unregister_shared_channel(&self, id: u64) {
        let mut channels = self.shared_channels.borrow_mut();
        channels.wakers_map.remove(&id);
        channels.check_map.remove(&id);
    }

    pub(crate) fn add_shared_channel_connection_waker(&self, waker: Waker) {
        let mut channels = self.shared_channels.borrow_mut();
        channels.connection_wakers.push(waker);
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
        let source = self.new_source(
            raw,
            SourceType::Write(pollable, IoBuffer::Dma(buf)),
            Some(|result, stats| {
                if let Ok(result) = result {
                    stats.file_writes += 1;
                    stats.file_bytes_written += *result as u64;
                }
            }),
        );
        self.sys.write_dma(&source, pos);
        source
    }

    pub(crate) fn write_buffered(&self, raw: RawFd, buf: Vec<u8>, pos: u64) -> Source {
        let source = self.new_source(
            raw,
            SourceType::Write(
                PollableStatus::NonPollable(DirectIo::Disabled),
                IoBuffer::Buffered(buf),
            ),
            Some(|result, stats| {
                if let Ok(result) = result {
                    stats.file_buffered_writes += 1;
                    stats.file_buffered_bytes_written += *result as u64;
                }
            }),
        );
        self.sys.write_buffered(&source, pos);
        source
    }

    pub(crate) fn connect(&self, raw: RawFd, addr: SockAddr) -> Source {
        let source = self.new_source(raw, SourceType::Connect(addr), None);
        self.sys.connect(&source);
        source
    }

    pub(crate) fn connect_timeout(&self, raw: RawFd, addr: SockAddr, d: Duration) -> Source {
        let source = self.new_source(raw, SourceType::Connect(addr), None);
        source.set_timeout(d);
        self.sys.connect(&source);
        source
    }

    pub(crate) fn accept(&self, raw: RawFd) -> Source {
        let addr = SockAddrStorage::uninit();
        let source = self.new_source(raw, SourceType::Accept(addr), None);
        self.sys.accept(&source);
        source
    }

    pub(crate) fn rushed_send(
        &self,
        fd: RawFd,
        buf: DmaBuffer,
        timeout: Option<Duration>,
    ) -> io::Result<Source> {
        let source = self.new_source(fd, SourceType::SockSend(buf), None);
        if let Some(timeout) = timeout {
            source.set_timeout(timeout);
        }
        self.sys.send(&source, MsgFlags::empty());
        self.rush_dispatch(&source)?;
        Ok(source)
    }

    pub(crate) fn rushed_sendmsg(
        &self,
        fd: RawFd,
        buf: DmaBuffer,
        addr: nix::sys::socket::SockAddr,
        timeout: Option<Duration>,
    ) -> io::Result<Source> {
        let iov = libc::iovec {
            iov_base: buf.as_ptr() as *mut libc::c_void,
            iov_len: 1,
        };
        // note that the iov and addresses we have above are stack addresses. We will
        // leave it blank and the io_uring callee will fill that up
        let hdr = libc::msghdr {
            msg_name: std::ptr::null_mut(),
            msg_namelen: 0,
            msg_iov: std::ptr::null_mut(),
            msg_iovlen: 0,
            msg_control: std::ptr::null_mut(),
            msg_controllen: 0,
            msg_flags: 0,
        };

        let source = self.new_source(fd, SourceType::SockSendMsg(buf, iov, hdr, addr), None);
        if let Some(timeout) = timeout {
            source.set_timeout(timeout);
        }

        self.sys.sendmsg(&source, MsgFlags::empty());
        self.rush_dispatch(&source)?;
        Ok(source)
    }

    pub(crate) fn rushed_recvmsg(
        &self,
        fd: RawFd,
        size: usize,
        flags: MsgFlags,
        timeout: Option<Duration>,
    ) -> io::Result<Source> {
        let hdr = libc::msghdr {
            msg_name: std::ptr::null_mut(),
            msg_namelen: 0,
            msg_iov: std::ptr::null_mut(),
            msg_iovlen: 0,
            msg_control: std::ptr::null_mut(),
            msg_controllen: 0,
            msg_flags: 0,
        };
        let iov = libc::iovec {
            iov_base: std::ptr::null_mut(),
            iov_len: 0,
        };
        let source = self.new_source(
            fd,
            SourceType::SockRecvMsg(
                None,
                iov,
                hdr,
                std::mem::MaybeUninit::<nix::sys::socket::sockaddr_storage>::uninit(),
            ),
            None,
        );
        if let Some(timeout) = timeout {
            source.set_timeout(timeout);
        }
        self.sys.recvmsg(&source, size, flags);
        self.rush_dispatch(&source)?;
        Ok(source)
    }

    pub(crate) fn rushed_recv(
        &self,
        fd: RawFd,
        size: usize,
        timeout: Option<Duration>,
    ) -> io::Result<Source> {
        let source = self.new_source(fd, SourceType::SockRecv(None), None);
        if let Some(timeout) = timeout {
            source.set_timeout(timeout);
        }
        self.sys.recv(&source, size, MsgFlags::empty());
        self.rush_dispatch(&source)?;
        Ok(source)
    }

    pub(crate) fn recv(&self, fd: RawFd, size: usize, flags: MsgFlags) -> Source {
        let source = self.new_source(fd, SourceType::SockRecv(None), None);
        self.sys.recv(&source, size, flags);
        source
    }

    pub(crate) fn read_dma(
        &self,
        raw: RawFd,
        pos: u64,
        size: usize,
        pollable: PollableStatus,
    ) -> Source {
        let source = self.new_source(
            raw,
            SourceType::Read(pollable, None),
            Some(|result, stats| {
                if let Ok(result) = result {
                    stats.file_reads += 1;
                    stats.file_bytes_read += *result as u64;
                }
            }),
        );
        self.sys.read_dma(&source, pos, size);
        source
    }

    pub(crate) fn read_buffered(&self, raw: RawFd, pos: u64, size: usize) -> Source {
        let source = self.new_source(
            raw,
            SourceType::Read(PollableStatus::NonPollable(DirectIo::Disabled), None),
            Some(|result, stats| {
                if let Ok(result) = result {
                    stats.file_buffered_reads += 1;
                    stats.file_buffered_bytes_read += *result as u64;
                }
            }),
        );
        self.sys.read_buffered(&source, pos, size);
        source
    }

    pub(crate) fn fdatasync(&self, raw: RawFd) -> Source {
        let source = self.new_source(raw, SourceType::FdataSync, None);
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
        let source = self.new_source(raw, SourceType::Fallocate, None);
        self.sys.fallocate(&source, position, size, flags);
        source
    }

    pub(crate) fn close(&self, raw: RawFd) -> Source {
        let source = self.new_source(
            raw,
            SourceType::Close,
            Some(|result, stats| {
                if result.is_ok() {
                    stats.files_closed += 1
                }
            }),
        );
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
            None,
        );
        self.sys.statx(&source);
        source
    }

    pub(crate) fn open_at(
        &self,
        dir: RawFd,
        path: &Path,
        flags: libc::c_int,
        mode: libc::mode_t,
    ) -> Source {
        let path = CString::new(path.as_os_str().as_bytes()).expect("path contained null!");

        let source = self.new_source(
            dir,
            SourceType::Open(path),
            Some(|result, stats| {
                if result.is_ok() {
                    stats.files_opened += 1
                }
            }),
        );
        self.sys.open_at(&source, flags, mode);
        source
    }

    #[cfg(feature = "bench")]
    pub(crate) fn nop(&self) -> Source {
        let source = self.new_source(-1, SourceType::Noop, None);
        self.sys.nop(&source);
        source
    }

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
    pub(crate) fn insert_timer(&self, id: u64, when: Instant, waker: Waker) {
        let mut timers = self.timers.borrow_mut();
        timers.insert(id, when, waker);
    }

    /// Deregisters a timer from the reactor.
    pub(crate) fn remove_timer(&self, id: u64) -> Option<Waker> {
        let mut timers = self.timers.borrow_mut();
        timers.remove(id)
    }

    /// Processes ready timers and extends the list of wakers to wake.
    ///
    /// Returns the duration until the next timer before this method was called.
    fn process_timers(&self) -> (Option<Duration>, usize) {
        let mut timers = self.timers.borrow_mut();
        timers.process_timers()
    }

    fn process_shared_channels(&self) -> usize {
        let mut channels = self.shared_channels.borrow_mut();
        let mut processed = channels.process_shared_channels();
        while let Some(waker) = self.sys.foreign_notifiers() {
            processed += 1;
            wake!(waker);
        }
        processed
    }

    fn rush_dispatch(&self, source: &Source) -> io::Result<()> {
        self.sys.rush_dispatch(Some(source.latency_req()), &mut 0)?;
        Ok(())
    }

    /// polls for I/O, but does not change any timer registration.
    ///
    /// This doesn't ever sleep, and does not touch the preempt timer.
    pub(crate) fn spin_poll_io(&self) -> io::Result<bool> {
        let mut woke = 0;
        // any duration, just so we land in the latency ring
        self.sys
            .rush_dispatch(Some(Latency::Matters(Duration::from_secs(1))), &mut woke)?;
        self.sys
            .rush_dispatch(Some(Latency::NotImportant), &mut woke)?;
        self.sys.rush_dispatch(None, &mut woke)?;
        woke += self.process_timers().1;
        woke += self.process_shared_channels();

        Ok(woke > 0)
    }

    fn process_external_events(&self) -> (Option<Duration>, usize) {
        let (next_timer, mut woke) = self.process_timers();
        woke += self.process_shared_channels();
        (next_timer, woke)
    }

    /// Processes new events, blocking until the first event or the timeout.
    fn react(&self, timeout: Option<Duration>) -> io::Result<()> {
        // Process ready timers.
        let (next_timer, woke) = self.process_external_events();

        // Block on I/O events.
        match self
            .sys
            .wait(timeout, next_timer, woke, || self.process_shared_channels())
        {
            // Don't wait for the next loop to process timers or shared channels
            Ok(true) => {
                self.process_external_events();
                Ok(())
            }

            Ok(false) => Ok(()),

            // The syscall was interrupted.
            Err(err) if err.kind() == io::ErrorKind::Interrupted => Ok(()),

            // An actual error occureed.
            Err(err) => Err(err),
        }
    }

    pub(crate) fn io_scheduler(&self) -> &Rc<IoScheduler> {
        &self.io_scheduler
    }
}
