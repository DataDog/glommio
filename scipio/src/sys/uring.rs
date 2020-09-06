// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use nix::poll::PollFlags;
use rlimit::Resource;
use std::cell::{Ref, RefCell};
use std::collections::VecDeque;
use std::ffi::CStr;
use std::io;
use std::io::{Error, ErrorKind};
use std::os::unix::io::RawFd;
use std::rc::Rc;
use std::task::Waker;
use std::time::Duration;

use crate::free_list::{FreeList, Idx};
use crate::sys::posix_buffers::PosixDmaBuffer;
use crate::sys::{IOBuffer, InnerSource, LinkStatus, PollableStatus, Source, SourceType};
use crate::{IoRequirements, Latency};
use crate::iou::iou;
use crate::uring_sys::uring_sys;

use uring_sys::IoRingOp;

use super::{EnqueuedSource, TimeSpec64};

type DmaBuffer = PosixDmaBuffer;

pub(crate) fn add_flag(fd: RawFd, flag: libc::c_int) -> io::Result<()> {
    let flags = syscall!(fcntl(fd, libc::F_GETFL))?;
    syscall!(fcntl(fd, libc::F_SETFL, flags | flag))?;
    Ok(())
}

#[allow(dead_code)]
#[derive(Debug)]
enum UringOpDescriptor {
    PollAdd(PollFlags),
    PollRemove(*const u8),
    Cancel(u64),
    Write(*const u8, usize, u64),
    WriteFixed(*const u8, usize, u64, usize),
    ReadFixed(u64, usize),
    Read(u64, usize),
    Open(*const u8, libc::c_int, u32),
    Close,
    FDataSync,
    Fallocate(u64, u64, libc::c_int),
    Statx(*const u8, *mut libc::statx),
    Timeout(*const uring_sys::__kernel_timespec),
    TimeoutRemove(u64),
}

#[derive(Debug)]
struct UringDescriptor {
    fd: RawFd,
    user_data: u64,
    args: UringOpDescriptor,
}

fn check_supported_operations(ops: &[uring_sys::IoRingOp]) -> bool {
    unsafe {
        let probe = uring_sys::io_uring_get_probe();
        if probe.is_null() {
            panic!("Failed to register a probe. The most likely reason is that your kernel witnessed Romulus killing Remus (too old!!)");
        }

        let mut ret = true;
        for op in ops {
            let opint = *{ op as *const uring_sys::IoRingOp as *const libc::c_int };
            let sup = uring_sys::io_uring_opcode_supported(probe, opint) > 0;
            ret &= sup;
            if !sup {
                println!("Yo kernel is so old it was with Hannibal when he crossed the Alps! Missing {:?}", op);
            }
        }
        uring_sys::io_uring_free_probe(probe);
        if !ret {
            eprintln!("Your kernel is older than Caesar. Bye");
            std::process::exit(1);
        }
        ret
    }
}

static SCIPIO_URING_OPS: &[IoRingOp] = &[
    IoRingOp::IORING_OP_NOP,
    IoRingOp::IORING_OP_READV,
    IoRingOp::IORING_OP_WRITEV,
    IoRingOp::IORING_OP_FSYNC,
    IoRingOp::IORING_OP_READ_FIXED,
    IoRingOp::IORING_OP_WRITE_FIXED,
    IoRingOp::IORING_OP_POLL_ADD,
    IoRingOp::IORING_OP_POLL_REMOVE,
    IoRingOp::IORING_OP_SENDMSG,
    IoRingOp::IORING_OP_RECVMSG,
    IoRingOp::IORING_OP_TIMEOUT,
    IoRingOp::IORING_OP_TIMEOUT_REMOVE,
    IoRingOp::IORING_OP_ACCEPT,
    IoRingOp::IORING_OP_CONNECT,
    IoRingOp::IORING_OP_FALLOCATE,
    IoRingOp::IORING_OP_OPENAT,
    IoRingOp::IORING_OP_CLOSE,
    IoRingOp::IORING_OP_STATX,
    IoRingOp::IORING_OP_READ,
    IoRingOp::IORING_OP_WRITE,
    IoRingOp::IORING_OP_SEND,
    IoRingOp::IORING_OP_RECV,
];

lazy_static! {
    static ref IO_URING_RECENT_ENOUGH: bool = check_supported_operations(SCIPIO_URING_OPS);
}

fn fill_sqe<F>(sqe: &mut iou::SubmissionQueueEvent<'_>, op: &UringDescriptor, buffer_allocation: F)
where
    F: FnOnce(usize) -> Option<DmaBuffer>,
{
    let mut user_data = op.user_data;
    unsafe {
        match op.args {
            UringOpDescriptor::PollAdd(events) => {
                sqe.prep_poll_add(op.fd, events);
            }
            UringOpDescriptor::PollRemove(to_remove) => {
                user_data = 0;
                sqe.prep_poll_remove(to_remove as u64);
            }
            UringOpDescriptor::Cancel(to_remove) => {
                user_data = 0;
                sqe.prep_cancel(to_remove);
            }
            UringOpDescriptor::Write(ptr, len, pos) => {
                let buf = std::slice::from_raw_parts(ptr, len);
                sqe.prep_write(op.fd, buf, pos);
            }
            UringOpDescriptor::Read(pos, len) => {
                let mut buf = vec![0; len];
                sqe.prep_read(op.fd, &mut buf, pos);

                let src = peek_source(from_user_data(op.user_data));
                if let SourceType::Read(PollableStatus::NonPollable, slot) =
                    &mut *src.source_type.borrow_mut()
                {
                    *slot = Some(IOBuffer::Buffered(buf));
                } else {
                    panic!("Expected Read source type");
                };
            }
            UringOpDescriptor::Open(path, flags, mode) => {
                let path = CStr::from_ptr(path as _);
                sqe.prep_openat(
                    op.fd,
                    path,
                    flags as _,
                    iou::OpenMode::from_bits_truncate(mode),
                );
            }
            UringOpDescriptor::FDataSync => {
                sqe.prep_fsync(op.fd, iou::FsyncFlags::FSYNC_DATASYNC);
            }
            UringOpDescriptor::Fallocate(offset, size, flags) => {
                let flags = iou::FallocateFlags::from_bits_truncate(flags);
                sqe.prep_fallocate(op.fd, offset, size, flags);
            }
            UringOpDescriptor::Statx(path, statx_buf) => {
                let flags =
                    iou::StatxFlags::AT_STATX_SYNC_AS_STAT | iou::StatxFlags::AT_NO_AUTOMOUNT;
                let mode = iou::StatxMode::from_bits_truncate(0x7ff);

                let path = CStr::from_ptr(path as _);
                sqe.prep_statx(-1, path, flags, mode, &mut *statx_buf);
            }
            UringOpDescriptor::Timeout(timespec) => {
                sqe.prep_timeout(&*timespec);
            }
            UringOpDescriptor::TimeoutRemove(timer) => {
                sqe.prep_timeout_remove(timer as _);
            }
            UringOpDescriptor::Close => {
                sqe.prep_close(op.fd);
            }
            UringOpDescriptor::ReadFixed(pos, len) => {
                let mut buf = buffer_allocation(len).expect("Buffer allocation failed");
                //let slabidx = buf.slabidx;

                //sqe.prep_read_fixed(op.fd, buf.as_mut_bytes(), pos, slabidx);
                sqe.prep_read(op.fd, buf.as_bytes_mut(), pos);
                let src = peek_source(from_user_data(op.user_data));

                if let SourceType::Read(_, slot) = &mut *src.source_type.borrow_mut() {
                    *slot = Some(IOBuffer::Dma(buf));
                } else {
                    panic!("Expected DmaRead source type");
                };
            }

            UringOpDescriptor::WriteFixed(ptr, len, pos, _) => {
                let buf = std::slice::from_raw_parts(ptr, len);
                //sqe.prep_write_fixed(op.fd, buf, pos, buf_index);
                sqe.prep_write(op.fd, buf, pos);
            }
        }
    }

    sqe.set_user_data(user_data);
}

fn process_one_event<F>(
    cqe: Option<iou::CompletionQueueEvent>,
    try_process: F,
    wakers: &mut Vec<Waker>,
) -> Option<()>
where
    F: FnOnce(&InnerSource) -> Option<()>,
{
    if let Some(value) = cqe {
        // No user data is POLL_REMOVE or CANCEL, we won't process.
        if value.user_data() == 0 {
            return Some(());
        }

        let src = consume_source(from_user_data(value.user_data()));

        let result = value.result();
        let was_cancelled =
            matches!(&result, Err(err) if err.raw_os_error() == Some(libc::ECANCELED));

        if !was_cancelled && try_process(&*src).is_none() {
            let mut w = src.wakers.borrow_mut();
            w.result = Some(result);
            wakers.append(&mut w.waiters);
        }
        return Some(());
    }
    None
}

type SourceMap = FreeList<Rc<InnerSource>>;
pub(crate) type SourceId = Idx<Rc<InnerSource>>;
fn from_user_data(user_data: u64) -> SourceId {
    SourceId::from_raw((user_data - 1) as usize)
}
fn to_user_data(id: SourceId) -> u64 {
    id.to_raw() as u64 + 1
}

thread_local!(static SOURCE_MAP: RefCell<SourceMap> = Default::default());

fn add_source(source: &Source, queue: ReactorQueue) -> SourceId {
    SOURCE_MAP.with(|x| {
        let item = source.inner.clone();
        let id = x.borrow_mut().alloc(item);
        source.inner.enqueued.set(Some(EnqueuedSource {
            id,
            queue: queue.clone(),
        }));
        id
    })
}

fn peek_source(id: SourceId) -> Rc<InnerSource> {
    SOURCE_MAP.with(|x| Rc::clone(&x.borrow()[id]))
}

fn consume_source(id: SourceId) -> Rc<InnerSource> {
    SOURCE_MAP.with(|x| {
        let source = x.borrow_mut().dealloc(id);
        source.enqueued.set(None);
        source
    })
}

#[derive(Debug)]
pub(crate) struct UringQueueState {
    submissions: VecDeque<UringDescriptor>,
    cancellations: VecDeque<UringDescriptor>,
}

pub(crate) type ReactorQueue = Rc<RefCell<UringQueueState>>;

impl UringQueueState {
    fn with_capacity(cap: usize) -> ReactorQueue {
        Rc::new(RefCell::new(UringQueueState {
            submissions: VecDeque::with_capacity(cap),
            cancellations: VecDeque::new(),
        }))
    }

    fn cancel_request(&mut self, id: SourceId) {
        let found = self
            .submissions
            .iter()
            .position(|el| el.user_data == to_user_data(id));
        match found {
            Some(idx) => {
                self.submissions.remove(idx);
                // We never submitted the request, so it's safe to consume
                // source here -- kernel didn't see our buffers.
                consume_source(id);
            }
            // We are cancelling this request, but it is already submitted.
            // This means that the kernel might be using the buffers right
            // now, so we delay `consume_source` until we consume the
            // corresponding event from the completion queue.
            None => self.cancellations.push_back(UringDescriptor {
                args: UringOpDescriptor::Cancel(to_user_data(id)),
                fd: -1,
                user_data: 0,
            }),
        }
    }
}

trait UringCommon {
    fn submission_queue(&mut self) -> ReactorQueue;
    fn submit_sqes(&mut self) -> io::Result<usize>;
    fn needs_kernel_enter(&self, submitted: usize) -> bool;
    fn submit_one_event(&mut self, queue: &mut VecDeque<UringDescriptor>) -> Option<()>;
    fn consume_one_event(&mut self, wakers: &mut Vec<Waker>) -> Option<()>;
    fn name(&self) -> &'static str;

    fn consume_sqe_queue(&mut self, queue: &mut VecDeque<UringDescriptor>) -> io::Result<usize> {
        let mut sub = 0;
        loop {
            if self.submit_one_event(queue).is_none() {
                break;
            }
            sub += 1;
        }

        if self.needs_kernel_enter(sub) {
            return self.submit_sqes();
        }
        Ok(0)
    }

    fn consume_cancellation_queue(&mut self) -> io::Result<usize> {
        let q = self.submission_queue();
        let mut queue = q.borrow_mut();
        self.consume_sqe_queue(&mut queue.cancellations)
    }

    fn consume_submission_queue(&mut self) -> io::Result<usize> {
        let q = self.submission_queue();
        let mut queue = q.borrow_mut();
        self.consume_sqe_queue(&mut queue.submissions)
    }

    fn consume_completion_queue(&mut self, wakers: &mut Vec<Waker>) -> usize {
        let mut completed: usize = 0;
        loop {
            if self.consume_one_event(wakers).is_none() {
                break;
            }
            completed += 1;
        }
        completed
    }

    // It is important to process cancellations as soon as we see them,
    // which is why they go into a separate queue. The reason is that
    // cancellations can be racy if they are left to their own devices.
    //
    // Imagine that you have a write request to fd 3 and wants to cancel it.
    // But before the cancellation is run fd 3 gets closed and another file
    // is opened with the same fd.
    fn flush_cancellations(&mut self, wakers: &mut Vec<Waker>) {
        let mut cnt = 0;
        loop {
            if self.consume_cancellation_queue().is_ok() {
                break;
            }
            self.consume_completion_queue(wakers);
            cnt += 1;
            if cnt > 1_000_000 {
                panic!(
                    "i tried literally a million times but couldn't flush to the {} ring",
                    self.name()
                );
            }
        }
        self.consume_completion_queue(wakers);
    }
}

struct PollRing {
    ring: iou::IoUring,
    submission_queue: ReactorQueue,
    submitted: u64,
    completed: u64,
}

impl PollRing {
    fn new(size: usize) -> io::Result<Self> {
        let ring = iou::IoUring::new_with_flags(size as _, iou::SetupFlags::IOPOLL)?;

        Ok(PollRing {
            submitted: 0,
            completed: 0,
            ring,
            submission_queue: UringQueueState::with_capacity(size * 4),
        })
    }

    fn can_sleep(&self) -> bool {
        self.submitted == self.completed
    }

    pub(crate) fn alloc_dma_buffer(&mut self, size: usize) -> DmaBuffer {
        PosixDmaBuffer::new(size).expect("Buffer allocation failed")
    }
}

impl UringCommon for PollRing {
    fn name(&self) -> &'static str {
        "poll"
    }

    fn needs_kernel_enter(&self, _submitted: usize) -> bool {
        // if we submitted anything, we will have the submission count
        // differing from the completion count and can_sleep will be false.
        //
        // So only need to check for that.
        !self.can_sleep()
    }

    fn submission_queue(&mut self) -> ReactorQueue {
        self.submission_queue.clone()
    }

    fn submit_sqes(&mut self) -> io::Result<usize> {
        if self.submitted != self.completed {
            return self.ring.submit_sqes();
        }
        Ok(0)
    }

    fn consume_one_event(&mut self, wakers: &mut Vec<Waker>) -> Option<()> {
        process_one_event(self.ring.peek_for_cqe(), |_| None, wakers).map(|x| {
            self.completed += 1;
            x
        })
    }

    fn submit_one_event(&mut self, queue: &mut VecDeque<UringDescriptor>) -> Option<()> {
        if queue.is_empty() {
            return None;
        }

        //let buffers = self.buffers.clone();
        if let Some(mut sqe) = self.ring.next_sqe() {
            self.submitted += 1;
            let op = queue.pop_front().unwrap();
            fill_sqe(&mut sqe, &op, |size| {
                /* FIXME: uring registered buffers need more work...
                let b = buffers.clone();
                let arena = buffers[b.len() - 1].clone();
                arena.alloc_buffer(size)
                */
                PosixDmaBuffer::new(size)
            });
            return Some(());
        }
        None
    }
}

impl InnerSource {
    pub(crate) fn update_source_type(&self, source_type: SourceType) -> SourceType {
        std::mem::replace(&mut *self.source_type.borrow_mut(), source_type)
    }
}

impl Source {
    fn latency_req(&self) -> Latency {
        self.inner.io_requirements.latency_req
    }

    fn source_type(&self) -> Ref<'_, SourceType> {
        self.inner.source_type.borrow()
    }

    fn update_source_type(&self, source_type: SourceType) -> SourceType {
        self.inner.update_source_type(source_type)
    }

    pub(crate) fn extract_source_type(&self) -> SourceType {
        self.inner.update_source_type(SourceType::Invalid)
    }

    pub(crate) fn extract_dma_buffer(&mut self) -> DmaBuffer {
        let stype = self.extract_source_type();
        match stype {
            SourceType::Read(_, Some(IOBuffer::Dma(buffer))) => buffer,
            SourceType::Write(_, IOBuffer::Dma(buffer)) => buffer,
            x => panic!("Could not extract buffer. Source: {:?}", x),
        }
    }

    pub(crate) fn extract_buffer(&mut self) -> Vec<u8> {
        let stype = self.extract_source_type();
        match stype {
            SourceType::Read(_, Some(IOBuffer::Buffered(buffer))) => buffer,
            SourceType::Write(_, IOBuffer::Buffered(buffer)) => buffer,
            x => panic!("Could not extract buffer. Source: {:?}", x),
        }
    }

    pub(crate) fn take_result(&self) -> Option<io::Result<usize>> {
        let mut w = self.inner.wakers.borrow_mut();
        w.result.take()
    }
    pub(crate) fn add_waiter(&self, waker: Waker) {
        let mut w = self.inner.wakers.borrow_mut();
        w.waiters.push(waker);
    }

    pub(crate) fn raw(&self) -> RawFd {
        self.inner.raw
    }
}

impl Drop for Source {
    fn drop(&mut self) {
        if let Some(EnqueuedSource { id, queue }) = self.inner.enqueued.take() {
            queue.borrow_mut().cancel_request(id);
        }
    }
}

struct SleepableRing {
    ring: iou::IoUring,
    submission_queue: ReactorQueue,
    name: &'static str,
}

impl SleepableRing {
    fn new(size: usize, name: &'static str) -> io::Result<Self> {
        assert_eq!(*IO_URING_RECENT_ENOUGH, true);
        Ok(SleepableRing {
            //     ring: iou::IoUring::new_with_flags(size as _, iou::SetupFlags::IOPOLL)?,
            ring: iou::IoUring::new(size as _)?,
            submission_queue: UringQueueState::with_capacity(size * 4),
            name,
        })
    }

    fn ring_fd(&self) -> RawFd {
        self.ring.raw().ring_fd
    }

    fn cancel_preempt_timer(&mut self, source: &mut Source) {
        let source_id = match &*source.source_type() {
            SourceType::Timeout(None, _) => return, // not armed, do nothing
            SourceType::Timeout(Some(source_id), _) => *source_id,
            _ => panic!("Unexpected source type when linking rings"),
        };
        // armed, need to cancel first
        let op_remove = UringOpDescriptor::TimeoutRemove(source_id);
        source.update_source_type(SourceType::Timeout(None, TimeSpec64::default()));

        let q = self.submission_queue();
        let mut queue = q.borrow_mut();
        queue.cancellations.push_front(UringDescriptor {
            args: op_remove,
            fd: -1,
            user_data: 0,
        });
    }

    fn rearm_preempt_timer(&mut self, source: &mut Source, d: Duration) {
        let new_id = add_source(source, self.submission_queue.clone());
        source.update_source_type(SourceType::Timeout(
            Some(to_user_data(new_id)),
            TimeSpec64::from(d),
        ));
        let op = match &*source.source_type() {
            SourceType::Timeout(_, ts) => UringOpDescriptor::Timeout(&ts.raw as *const _),
            _ => unreachable!(),
        };

        // This assumes SQEs will be processed in the order they are
        // seen. Because remove does not do anything asynchronously
        // and is processed inline there is no need to link sqes.
        let q = self.submission_queue();
        let mut queue = q.borrow_mut();
        queue.submissions.push_front(UringDescriptor {
            args: op,
            fd: -1,
            user_data: to_user_data(new_id),
        });
        // No need to submit, the next ring enter will submit for us. Because
        // we just flushed and we got put in front of the queue we should get a SQE.
        // Still it would be nice to verify if we did.
    }

    fn sleep(&mut self, link: &mut Source) -> io::Result<usize> {
        let is_freestanding = match &*link.source_type() {
            SourceType::LinkRings(LinkStatus::Linked) => false, // nothing to do
            SourceType::LinkRings(LinkStatus::Freestanding) => true,
            _ => panic!("Unexpected source type when linking rings"),
        };

        if is_freestanding {
            if let Some(mut sqe) = self.ring.next_sqe() {
                link.update_source_type(SourceType::LinkRings(LinkStatus::Linked));

                let op = UringDescriptor {
                    fd: link.raw(),
                    user_data: to_user_data(add_source(link, self.submission_queue.clone())),
                    args: UringOpDescriptor::PollAdd(common_flags() | read_flags()),
                };
                fill_sqe(&mut sqe, &op, PosixDmaBuffer::new);
            }
        }

        self.ring.submit_sqes_and_wait(1)
    }
}

impl UringCommon for SleepableRing {
    fn name(&self) -> &'static str {
        self.name
    }

    fn needs_kernel_enter(&self, submitted: usize) -> bool {
        submitted > 0
    }

    fn submission_queue(&mut self) -> ReactorQueue {
        self.submission_queue.clone()
    }

    fn submit_sqes(&mut self) -> io::Result<usize> {
        self.ring.submit_sqes()
    }

    fn consume_one_event(&mut self, wakers: &mut Vec<Waker>) -> Option<()> {
        process_one_event(
            self.ring.peek_for_cqe(),
            |source| match &mut *source.source_type.borrow_mut() {
                SourceType::LinkRings(status @ LinkStatus::Linked) => {
                    *status = LinkStatus::Freestanding;
                    Some(())
                }
                SourceType::LinkRings(LinkStatus::Freestanding) => {
                    panic!("Impossible to have an event firing like this");
                }
                SourceType::Timeout(id @ Some(_), _) => {
                    *id = None;
                    Some(())
                }
                // This is actually possible: when the request is cancelled
                // the original source does complete, and the cancellation
                // would have marked us as false. Just ignore it.
                SourceType::Timeout(None, _) => None,
                _ => None,
            },
            wakers,
        )
    }

    fn submit_one_event(&mut self, queue: &mut VecDeque<UringDescriptor>) -> Option<()> {
        if queue.is_empty() {
            return None;
        }

        if let Some(mut sqe) = self.ring.next_sqe() {
            let op = queue.pop_front().unwrap();
            fill_sqe(&mut sqe, &op, PosixDmaBuffer::new);
            return Some(());
        }
        None
    }
}

pub struct Reactor {
    // FIXME: it is starting to feel we should clean this up to a Inner pattern
    main_ring: RefCell<SleepableRing>,
    latency_ring: RefCell<SleepableRing>,
    poll_ring: RefCell<PollRing>,

    link_rings_src: RefCell<Source>,

    timeout_src: RefCell<Source>,
}

fn common_flags() -> PollFlags {
    PollFlags::POLLERR | PollFlags::POLLHUP | PollFlags::POLLNVAL
}

/// Epoll flags for all possible readability events.
fn read_flags() -> PollFlags {
    PollFlags::POLLIN | PollFlags::POLLPRI
}

/// Epoll flags for all possible writability events.
fn write_flags() -> PollFlags {
    PollFlags::POLLOUT
}

macro_rules! consume_rings {
    (into $output:expr; $( $ring:expr ),+ ) => {{
        let mut consumed = 0;
        $(
            consumed += $ring.consume_completion_queue($output);
        )*
        consumed
    }}
}
macro_rules! flush_cancellations {
    (into $output:expr; $( $ring:expr ),+ ) => {{
        $(
            $ring.flush_cancellations($output);
        )*
    }}
}

macro_rules! flush_rings {
    ($( $ring:expr ),+ ) => {{
        $(
            $ring.consume_submission_queue()?;
        )*
        let ret : io::Result<()> = Ok(());
        ret
    }}
}

impl Reactor {
    pub(crate) fn new() -> io::Result<Reactor> {
        // Different threads have no business passing files around. Once you have
        // a file descriptor you can do unsafe operations on it, and if some other
        // thread happens to have the same fd, then this is no fun.
        //
        // In Seastar fds are passed around to the I/O Scheduler, but when the time
        // come for us to do the same I would prefer to mediate that through other,
        // safer interfaces like an Arc-like version of the DmaFile.
        //
        // We can't prohibit users from passing a file descriptor because at the end
        // of the day that's just an integer, but we can call unshare() to make sure
        // that threads of the same process do not have the same set of file descriptors.
        //
        // The damage is at least contained.
        syscall!(unshare(libc::CLONE_FILES | libc::CLONE_FS))?;

        const MIN_MEMLOCK_LIMIT: u64 = 512 * 1024;
        let (memlock_limit, _) = Resource::MEMLOCK.get()?;
        if memlock_limit < MIN_MEMLOCK_LIMIT {
            return Err(Error::new(
                ErrorKind::Other,
                format!(
                    "The memlock resource limit is too low: {} (recommended {})",
                    memlock_limit, MIN_MEMLOCK_LIMIT
                ),
            ));
        }
        let main_ring = SleepableRing::new(128, "main")?;
        let latency_ring = SleepableRing::new(128, "latency")?;
        let link_fd = latency_ring.ring_fd();
        let link_rings_src = Source::new(
            IoRequirements::default(),
            link_fd,
            SourceType::LinkRings(LinkStatus::Freestanding),
        );

        let timeout_src = Source::new(
            IoRequirements::default(),
            -1,
            SourceType::Timeout(None, TimeSpec64::default()),
        );

        Ok(Reactor {
            main_ring: RefCell::new(main_ring),
            latency_ring: RefCell::new(latency_ring),
            poll_ring: RefCell::new(PollRing::new(128)?),
            link_rings_src: RefCell::new(link_rings_src),
            timeout_src: RefCell::new(timeout_src),
        })
    }

    pub(crate) fn alloc_dma_buffer(&self, size: usize) -> DmaBuffer {
        let mut poll_ring = self.poll_ring.borrow_mut();
        poll_ring.alloc_dma_buffer(size)
    }

    pub(crate) fn interest(&self, source: &Source, read: bool, write: bool) {
        let mut flags = common_flags();
        if read {
            flags |= read_flags();
        }
        if write {
            flags |= write_flags();
        }

        self.queue_standard_request(source, UringOpDescriptor::PollAdd(flags));
    }

    pub(crate) fn write_dma(&self, source: &Source, pos: u64) {
        match &*source.source_type() {
            SourceType::Write(_, IOBuffer::Dma(buf)) => {
                let op = UringOpDescriptor::WriteFixed(buf.as_ptr(), buf.len(), pos, 0);
                self.queue_storage_io_request(source, op);
            }
            x => panic!("Unexpected source type for write: {:?}", x),
        }
    }

    pub(crate) fn write_buffered(&self, source: &Source, pos: u64) {
        match &*source.source_type() {
            SourceType::Write(PollableStatus::NonPollable, IOBuffer::Buffered(buf)) => {
                let op = UringOpDescriptor::Write(buf.as_ptr() as *const u8, buf.len(), pos);
                self.queue_standard_request(source, op);
            }
            x => panic!("Unexpected source type for write: {:?}", x),
        }
    }

    pub(crate) fn read_dma(&self, source: &Source, pos: u64, size: usize) {
        let op = UringOpDescriptor::ReadFixed(pos, size);
        self.queue_storage_io_request(source, op);
    }

    pub(crate) fn read_buffered(&self, source: &Source, pos: u64, size: usize) {
        let op = UringOpDescriptor::Read(pos, size);
        self.queue_standard_request(source, op);
    }

    pub(crate) fn fdatasync(&self, source: &Source) {
        self.queue_standard_request(source, UringOpDescriptor::FDataSync);
    }

    pub(crate) fn fallocate(&self, source: &Source, offset: u64, size: u64, flags: libc::c_int) {
        let op = UringOpDescriptor::Fallocate(offset, size, flags);
        self.queue_standard_request(source, op);
    }

    pub(crate) fn close(&self, source: &Source) {
        let op = UringOpDescriptor::Close;
        self.queue_standard_request(source, op);
    }

    pub(crate) fn statx(&self, source: &Source) {
        let op = match &*source.source_type() {
            SourceType::Statx(path, buf) => {
                let path = path.as_c_str().as_ptr();
                let buf = buf.as_ptr();
                UringOpDescriptor::Statx(path as _, buf)
            }
            _ => panic!("Unexpected source for statx operation"),
        };
        self.queue_standard_request(source, op);
    }

    pub(crate) fn open_at(&self, source: &Source, flags: libc::c_int, mode: libc::c_int) {
        let pathptr = match &*source.source_type() {
            SourceType::Open(cstring) => cstring.as_c_str().as_ptr(),
            _ => panic!("Wrong source type!"),
        };
        let op = UringOpDescriptor::Open(pathptr as _, flags, mode as _);
        self.queue_standard_request(source, op);
    }

    pub(crate) fn insert(&self, fd: RawFd) -> io::Result<()> {
        add_flag(fd, libc::O_NONBLOCK)
    }

    // We want to go to sleep but we can only go to sleep in one of the rings,
    // as we only have one thread. There are more than one sleepable rings, so
    // what we do is we take advantage of the fact that the ring's ring_fd is pollable
    // and register a POLL_ADD event into the ring we will wait on.
    //
    // We may not be able to register an SQE at this point, so we return an Error and
    // will just not sleep.
    fn link_rings_and_sleep(&self, ring: &mut SleepableRing) -> io::Result<()> {
        let mut link_rings = self.link_rings_src.borrow_mut();
        ring.sleep(&mut link_rings)?;
        Ok(())
    }

    pub(crate) fn wait(
        &self,
        wakers: &mut Vec<Waker>,
        timeout: Option<Duration>,
        timer_expiration: Option<Duration>,
    ) -> io::Result<bool> {
        let mut poll_ring = self.poll_ring.borrow_mut();
        let mut main_ring = self.main_ring.borrow_mut();
        let mut lat_ring = self.latency_ring.borrow_mut();

        let mut should_sleep = match timeout {
            None => true,
            Some(dur) => {
                let mut src = self.timeout_src.borrow_mut();
                lat_ring.cancel_preempt_timer(&mut src);
                flush_cancellations!(into wakers; lat_ring);
                lat_ring.rearm_preempt_timer(&mut src, dur);
                false
            }
        };
        flush_cancellations!(into wakers; main_ring, lat_ring, poll_ring);
        flush_rings!(main_ring, lat_ring, poll_ring)?;
        should_sleep &= poll_ring.can_sleep();

        if should_sleep {
            consume_rings!(into wakers; lat_ring, poll_ring, main_ring);
        }
        // If we generated any event so far, we can't sleep. Need to handle them.
        should_sleep &= wakers.is_empty();

        if should_sleep {
            // We are about to go to sleep. It's ok to sleep, but if there
            // is a timer set, we need to make sure we wake up to handle it.
            if let Some(dur) = timer_expiration {
                let mut src = self.timeout_src.borrow_mut();
                lat_ring.cancel_preempt_timer(&mut src);
                flush_cancellations!(into wakers; lat_ring);
                // Although we keep the SQE queue separate for cancellations
                // and submission the CQE queue is a single one. So when we
                // flushed cancellations it is possible that we generated an
                // event. (cancellations don't generate events)
                //
                // If we did, bail now.
                should_sleep &= wakers.is_empty();
                if should_sleep {
                    lat_ring.rearm_preempt_timer(&mut src, dur);
                    flush_rings!(lat_ring)?;
                }
            }
            if should_sleep {
                self.link_rings_and_sleep(&mut main_ring)?;
            }
        }

        consume_rings!(into wakers; lat_ring, poll_ring, main_ring);
        // A Note about need_preempt:
        //
        // If in the last call to consume_rings! some events completed, the tail and
        // head would have moved to match. So it does not matter that events were
        // generated after we registered the timer: since we consumed them here,
        // need_preempt() should be false at this point. As soon as the next event
        // in the preempt ring completes, though, then it will be true.
        Ok(should_sleep)
    }

    pub(crate) fn preempt_pointers(&self) -> (*const u32, *const u32) {
        let mut lat_ring = self.latency_ring.borrow_mut();
        let cq = &lat_ring.ring.raw_mut().cq;
        (cq.khead, cq.ktail)
    }

    fn queue_standard_request(&self, source: &Source, op: UringOpDescriptor) {
        let ring = match source.latency_req() {
            Latency::NotImportant => &self.main_ring,
            Latency::Matters(_) => &self.latency_ring,
        };
        queue_request_into_ring(ring, source, op)
    }

    fn queue_storage_io_request(&self, source: &Source, op: UringOpDescriptor) {
        let pollable = match &*source.source_type() {
            SourceType::Read(p, _) | SourceType::Write(p, _) => *p,
            _ => panic!("SourceType should declare if it supports poll operations"),
        };
        match pollable {
            PollableStatus::Pollable => queue_request_into_ring(&self.poll_ring, source, op),
            PollableStatus::NonPollable => queue_request_into_ring(&self.main_ring, source, op),
        }
    }
}

fn queue_request_into_ring(
    ring: &RefCell<impl UringCommon>,
    source: &Source,
    descriptor: UringOpDescriptor,
) {
    let q = ring.borrow_mut().submission_queue();
    let id = add_source(source, Rc::clone(&q));

    let mut queue = q.borrow_mut();
    queue.submissions.push_back(UringDescriptor {
        args: descriptor,
        fd: source.raw(),
        user_data: to_user_data(id),
    });
}

impl Drop for Reactor {
    fn drop(&mut self) {}
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;

    #[test]
    fn timeout_smoke_test() {
        let reactor = Reactor::new().unwrap();

        fn timeout_source(millis: u64) -> (Source, UringOpDescriptor) {
            let source = Source::new(
                IoRequirements::default(),
                -1,
                SourceType::Timeout(None, TimeSpec64::from(Duration::from_millis(millis))),
            );
            let op = match &*source.source_type() {
                SourceType::Timeout(_, ts) => UringOpDescriptor::Timeout(&ts.raw as *const _),
                _ => unreachable!(),
            };
            (source, op)
        }

        let (fast, op) = timeout_source(50);
        reactor.queue_standard_request(&fast, op);

        let (slow, op) = timeout_source(150);
        reactor.queue_standard_request(&slow, op);

        let (lethargic, op) = timeout_source(300);
        reactor.queue_standard_request(&lethargic, op);

        let start = Instant::now();
        let mut wakers = Vec::new();
        reactor.wait(&mut wakers, None, None).unwrap();
        let elapsed_ms = start.elapsed().as_millis();
        assert!(50 <= elapsed_ms && elapsed_ms < 100);

        drop(slow); // Cancel this one.

        let mut wakers = Vec::new();
        reactor.wait(&mut wakers, None, None).unwrap();
        let elapsed_ms = start.elapsed().as_millis();
        assert!(300 <= elapsed_ms && elapsed_ms < 350);
    }
}
