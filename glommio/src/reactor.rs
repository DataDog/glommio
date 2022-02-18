// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
//!

use std::{
    cell::RefCell,
    collections::BTreeMap,
    ffi::CString,
    fmt,
    future::Future,
    io,
    mem,
    os::unix::{ffi::OsStrExt, io::RawFd},
    path::Path,
    rc::Rc,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    task::Waker,
    time::{Duration, Instant},
};

use ahash::AHashMap;
use log::error;
use nix::sys::socket::{MsgFlags, SockAddr};
use smallvec::SmallVec;

use crate::{
    io::{FileScheduler, IoScheduler, ScheduledSource},
    iou::sqe::SockAddrStorage,
    sys::{
        self,
        common_flags,
        read_flags,
        sysfs,
        DirectIo,
        DmaBuffer,
        IoBuffer,
        PollableStatus,
        SleepNotifier,
        Source,
        SourceType,
        StatsCollection,
    },
    IoRequirements,
    IoStats,
    PoolPlacement,
    TaskQueueHandle,
};
use nix::poll::PollFlags;

type SharedChannelWakerChecker = (SmallVec<[Waker; 1]>, Option<Box<dyn Fn() -> usize>>);

struct SharedChannels {
    id: u64,
    wakers_map: BTreeMap<u64, SharedChannelWakerChecker>,
    connection_wakers: Vec<Waker>,
}

impl SharedChannels {
    fn new() -> SharedChannels {
        SharedChannels {
            id: 0,
            connection_wakers: Vec::new(),
            wakers_map: BTreeMap::new(),
        }
    }

    fn process_shared_channels(&mut self) -> usize {
        let mut woke = self.connection_wakers.len();
        for waker in self.connection_wakers.drain(..) {
            wake!(waker);
        }

        for (_, (pending, check)) in self.wakers_map.iter_mut() {
            if pending.is_empty() {
                continue;
            }
            let room = std::cmp::min(check.as_ref().unwrap()(), pending.len());
            for waker in pending.drain(0..room).rev() {
                woke += 1;
                wake!(waker);
            }
        }
        woke
    }
}

struct Timers {
    timer_id: u64,
    timers_by_id: AHashMap<u64, Instant>,

    /// An ordered map of registered timers.
    ///
    /// Timers are in the order in which they fire. The `usize` in this type is
    /// a timer ID used to distinguish timers that fire at the same time.
    /// The [`Waker`] represents the task awaiting the timer.
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
        let woke = ready.len();
        for (_, waker) in ready {
            wake!(waker);
        }

        // Calculate the duration until the next event.
        let next = self
            .timers
            .keys()
            .next()
            .map(|(when, _)| when.saturating_duration_since(now));
        (next, woke)
    }
}

/// The reactor.
///
/// Every async I/O handle and every timer is registered here. Invocations of
/// [`run()`][`crate::run()`] poll the reactor to periodically check for new
/// events
///
/// There is only one global instance of this type, accessible by
/// [`Local::get_reactor()`].
pub(crate) struct Reactor {
    /// Raw bindings to `epoll`/`kqueue`/`wepoll`.
    pub(crate) sys: sys::Reactor,

    timers: RefCell<Timers>,

    shared_channels: RefCell<SharedChannels>,

    io_scheduler: Rc<IoScheduler>,
    record_io_latencies: bool,

    /// Whether there are events in the latency ring.
    ///
    /// There will be events if the head and tail of the CQ ring are different.
    /// `liburing` has an inline function in its header to do this, but it
    /// becomes a function call if I use through `uring-sys`. This is quite
    /// critical and already more expensive than it should be (see comments
    /// for need_preempt()), so implement this ourselves.
    ///
    /// Also, we don't want to acquire these addresses (which are behind a
    /// refcell) every time. Acquire during initialization
    preempt_ptr_head: *const u32,
    preempt_ptr_tail: *const AtomicU32,
}

impl Reactor {
    pub(crate) fn new(
        notifier: Arc<SleepNotifier>,
        io_memory: usize,
        ring_depth: usize,
        record_io_latencies: bool,
        thread_pool_placement: PoolPlacement,
    ) -> io::Result<Reactor> {
        let sys = sys::Reactor::new(notifier, io_memory, ring_depth, thread_pool_placement)?;
        let (preempt_ptr_head, preempt_ptr_tail) = sys.preempt_pointers();
        Ok(Reactor {
            sys,
            timers: RefCell::new(Timers::new()),
            shared_channels: RefCell::new(SharedChannels::new()),
            io_scheduler: Rc::new(IoScheduler::new()),
            record_io_latencies,
            preempt_ptr_head,
            preempt_ptr_tail: preempt_ptr_tail as _,
        })
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

    pub(crate) fn ring_depth(&self) -> usize {
        self.sys.ring_depth()
    }

    fn new_source(
        &self,
        raw: RawFd,
        stype: SourceType,
        stats_collection: Option<StatsCollection>,
    ) -> Source {
        sys::Source::new(
            self.io_scheduler.requirements(),
            raw,
            stype,
            stats_collection,
            Some(crate::executor().current_task_queue()),
        )
    }

    pub(crate) fn inform_io_requirements(&self, req: IoRequirements) {
        self.io_scheduler.inform_requirements(req);
    }

    pub(crate) async fn probe_iopoll_support(
        &self,
        raw: RawFd,
        alignment: u64,
        major: usize,
        minor: usize,
        path: &Path,
    ) -> bool {
        match sysfs::BlockDevice::iopoll(major, minor) {
            None => {
                // This routine exercises the iopoll code path in the kernel and asserts that we
                // can successfully read something. If we can't and receive `ENOTSUP` then
                // the kernel doesn't support iopoll for this device.
                //
                // In a perfect world, we would issue a read of size 0 because we are not
                // interested in any data, just to check whether we _could_. Unfortunately it
                // seems we are not allowed to have nice things, and the kernel can
                // short-circuit its internal logic and return an early
                // "success" for reads of size 0. We are therefore forced to do
                // an actual read.

                let source =
                    self.new_source(raw, SourceType::Read(PollableStatus::Pollable, None), None);
                self.sys.read_dma(&source, 0, alignment as usize);
                let iopoll = if let Err(err) = source.collect_rw().await {
                    if let Some(libc::ENOTSUP) = err.raw_os_error() {
                        false
                    } else {
                        // The IO requests failed, but not because the poll ring doesn't work.
                        error!(
                            "got unexpected error when probing iopoll support for file {:?} (fd: \
                             {}) hosted on ({}, {}); the poll ring will be disabled for this \
                             device: {}",
                            path, raw, major, minor, err
                        );
                        false
                    }
                } else {
                    true
                };
                sysfs::BlockDevice::set_iopoll_support(major, minor, iopoll);
                iopoll
            }
            Some(iopoll) => iopoll,
        }
    }

    pub(crate) fn register_shared_channel<F>(&self, test_function: Box<F>) -> u64
    where
        F: Fn() -> usize + 'static,
    {
        let mut channels = self.shared_channels.borrow_mut();
        let id = channels.id;
        channels.id += 1;
        let ret = channels
            .wakers_map
            .insert(id, (Default::default(), Some(test_function)));
        assert!(ret.is_none());
        id
    }

    pub(crate) fn unregister_shared_channel(&self, id: u64) {
        let mut channels = self.shared_channels.borrow_mut();
        channels.wakers_map.remove(&id);
    }

    pub(crate) fn add_shared_channel_connection_waker(&self, waker: Waker) {
        let mut channels = self.shared_channels.borrow_mut();
        channels.connection_wakers.push(waker);
    }

    pub(crate) fn add_shared_channel_waker(&self, id: u64, waker: Waker) {
        let mut channels = self.shared_channels.borrow_mut();
        let map = channels
            .wakers_map
            .entry(id)
            .or_insert_with(|| (SmallVec::new(), None));

        map.0.push(waker);
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
        let stats = StatsCollection {
            fulfilled: Some(|result, stats, op_count| {
                if let Ok(result) = result {
                    stats.file_writes += op_count;
                    stats.file_bytes_written += *result as u64 * op_count;
                }
            }),
            reused: None,
            latency: None,
        };

        let source = self.new_source(
            raw,
            SourceType::Write(pollable, IoBuffer::Dma(buf)),
            Some(stats),
        );
        self.sys.write_dma(&source, pos);
        source
    }

    pub(crate) fn write_buffered(&self, raw: RawFd, buf: Vec<u8>, pos: u64) -> Source {
        let stats = StatsCollection {
            fulfilled: Some(|result, stats, op_count| {
                if let Ok(result) = result {
                    stats.file_buffered_writes += op_count;
                    stats.file_buffered_bytes_written += *result as u64 * op_count;
                }
            }),
            reused: None,
            latency: None,
        };

        let source = self.new_source(
            raw,
            SourceType::Write(
                PollableStatus::NonPollable(DirectIo::Disabled),
                IoBuffer::Buffered(buf),
            ),
            Some(stats),
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

    pub(crate) fn poll_read_ready(&self, fd: RawFd) -> Source {
        let source = self.new_source(fd, SourceType::PollAdd, None);
        self.sys.poll_ready(&source, common_flags() | read_flags());
        source
    }

    pub(crate) fn poll_write_ready(&self, fd: RawFd) -> Source {
        let source = self.new_source(fd, SourceType::PollAdd, None);
        self.sys
            .poll_ready(&source, common_flags() | PollFlags::POLLOUT);
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
        // Note that the iov and addresses we have above are stack addresses. We will
        // leave it blank and the `io_uring` callee will fill that up
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
        scheduler: Option<&FileScheduler>,
    ) -> ScheduledSource {
        let stats = StatsCollection {
            fulfilled: Some(|result, stats, op_count| {
                if let Ok(result) = result {
                    stats.file_reads += op_count;
                    stats.file_bytes_read += *result as u64 * op_count;
                }
            }),
            reused: Some(|result, stats, op_count| {
                if let Ok(result) = result {
                    stats.file_deduped_reads += op_count;
                    stats.file_deduped_bytes_read += *result as u64 * op_count;
                }
            }),
            latency: if self.record_io_latencies {
                Some(|pre_lat, io_lat, post_lat, stats| {
                    stats
                        .pre_reactor_io_scheduler_latency_us
                        .add(pre_lat.as_micros() as f64);
                    stats.io_latency_us.add(io_lat.as_micros() as f64);
                    stats
                        .post_reactor_io_scheduler_latency_us
                        .add(post_lat.as_micros() as f64)
                })
            } else {
                None
            },
        };

        let source = self.new_source(raw, SourceType::Read(pollable, None), Some(stats));

        if let Some(scheduler) = scheduler {
            if let Some(source) =
                scheduler.consume_scheduled(pos..pos + size as u64, Some(&self.sys))
            {
                source
            } else {
                self.sys.read_dma(&source, pos, size);
                scheduler.schedule(source, pos..pos + size as u64)
            }
        } else {
            self.sys.read_dma(&source, pos, size);
            ScheduledSource::new_raw(source, pos..pos + size as u64)
        }
    }

    pub(crate) fn read_buffered(
        &self,
        raw: RawFd,
        pos: u64,
        size: usize,
        scheduler: Option<&FileScheduler>,
    ) -> ScheduledSource {
        let stats = StatsCollection {
            fulfilled: Some(|result, stats, op_count| {
                if let Ok(result) = result {
                    stats.file_buffered_reads += op_count;
                    stats.file_buffered_bytes_read += *result as u64 * op_count;
                }
            }),
            reused: None,
            latency: if self.record_io_latencies {
                Some(|pre_lat, io_lat, post_lat, stats| {
                    stats
                        .pre_reactor_io_scheduler_latency_us
                        .add(pre_lat.as_micros() as f64);
                    stats.io_latency_us.add(io_lat.as_micros() as f64);
                    stats
                        .post_reactor_io_scheduler_latency_us
                        .add(post_lat.as_micros() as f64)
                })
            } else {
                None
            },
        };

        let source = self.new_source(
            raw,
            SourceType::Read(PollableStatus::NonPollable(DirectIo::Disabled), None),
            Some(stats),
        );

        if let Some(scheduler) = scheduler {
            if let Some(source) =
                scheduler.consume_scheduled(pos..pos + size as u64, Some(&self.sys))
            {
                source
            } else {
                self.sys.read_buffered(&source, pos, size);
                scheduler.schedule(source, pos..pos + size as u64)
            }
        } else {
            self.sys.read_buffered(&source, pos, size);
            ScheduledSource::new_raw(source, pos..pos + size as u64)
        }
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

    pub(crate) fn truncate(&self, raw: RawFd, size: u64) -> impl Future<Output = Source> {
        let source = self.new_source(raw, SourceType::Truncate, None);
        let waiter = self.sys.truncate(&source, size);

        async move {
            waiter.await;
            source
        }
    }

    pub(crate) fn rename<P, Q>(&self, old_path: P, new_path: Q) -> impl Future<Output = Source>
    where
        P: AsRef<Path>,
        Q: AsRef<Path>,
    {
        let source = self.new_source(
            -1,
            SourceType::Rename(old_path.as_ref().to_owned(), new_path.as_ref().to_owned()),
            None,
        );
        let waiter = self.sys.rename(&source);

        async move {
            waiter.await;
            source
        }
    }

    pub(crate) fn remove_file<P: AsRef<Path>>(&self, path: P) -> impl Future<Output = Source> {
        let source = self.new_source(-1, SourceType::Remove(path.as_ref().to_owned()), None);
        let waiter = self.sys.remove_file(&source);

        async move {
            waiter.await;
            source
        }
    }

    pub(crate) fn create_dir<P: AsRef<Path>>(
        &self,
        path: P,
        mode: libc::c_int,
    ) -> impl Future<Output = Source> {
        let source = self.new_source(-1, SourceType::CreateDir(path.as_ref().to_owned()), None);
        let waiter = self.sys.create_dir(&source, mode);

        async move {
            waiter.await;
            source
        }
    }

    pub(crate) fn run_blocking(
        &self,
        func: Box<dyn FnOnce() + Send + 'static>,
    ) -> impl Future<Output = Source> {
        let source = self.new_source(-1, SourceType::BlockingFn, None);
        let waiter = self.sys.run_blocking(&source, func);

        async move {
            waiter.await;
            source
        }
    }

    pub(crate) fn close(&self, raw: RawFd) -> Source {
        let source = self.new_source(
            raw,
            SourceType::Close,
            Some(StatsCollection {
                fulfilled: Some(|result, stats, op_count| {
                    if result.is_ok() {
                        stats.files_closed += op_count
                    }
                }),
                reused: None,
                latency: None,
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
            Some(StatsCollection {
                fulfilled: Some(|result, stats, op_count| {
                    if result.is_ok() {
                        stats.files_opened += op_count
                    }
                }),
                reused: None,
                latency: None,
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

    pub(crate) fn timer_exists(&self, id: &(Instant, u64)) -> bool {
        let timers = self.timers.borrow();
        timers.timers.contains_key(id)
    }

    /// Processes ready timers and extends the list of wakers to wake.
    ///
    /// Returns the duration until the next timer
    fn process_timers(&self) -> (Option<Duration>, usize) {
        let mut timers = self.timers.borrow_mut();
        timers.process_timers()
    }

    fn process_shared_channels(&self) -> usize {
        let mut channels = self.shared_channels.borrow_mut();
        let mut processed = channels.process_shared_channels();
        processed += self.sys.process_foreign_wakes();
        processed
    }

    pub(crate) fn process_shared_channels_by_id(&self, id: u64) -> usize {
        match self.shared_channels.borrow_mut().wakers_map.get_mut(&id) {
            Some(wakers) => {
                let processed = wakers.0.len();
                wakers.0.drain(..).for_each(|w| {
                    wake!(w);
                });
                processed
            }
            None => 0,
        }
    }

    pub(crate) fn rush_dispatch(&self, source: &Source) -> io::Result<()> {
        self.sys.rush_dispatch(source, &mut 0)
    }

    /// Polls for I/O, but does not change any timer registration.
    ///
    /// This doesn't ever sleep, and does not touch the preemption timer.
    pub(crate) fn spin_poll_io(&self) -> io::Result<bool> {
        let mut woke = 0;
        self.sys.poll_io(&mut woke)?;
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
    pub(crate) fn react(&self, timeout: impl Fn() -> Option<Duration>) -> io::Result<bool> {
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
                Ok(true)
            }

            Ok(false) => Ok(false),

            // An actual error occurred.
            Err(err) => Err(err),
        }
    }

    pub(crate) fn io_scheduler(&self) -> &Rc<IoScheduler> {
        &self.io_scheduler
    }
}

impl fmt::Debug for Reactor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("Reactor { .. }")
    }
}
