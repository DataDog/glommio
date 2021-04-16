// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use alloc::alloc::Layout;
use log::warn;
use nix::{
    fcntl::{FallocateFlags, OFlag},
    poll::PollFlags,
};
use rlimit::Resource;
use std::{
    cell::{Cell, Ref, RefCell, RefMut},
    collections::VecDeque,
    ffi::CStr,
    fmt,
    io,
    io::{Error, ErrorKind},
    os::unix::io::RawFd,
    panic,
    ptr,
    rc::Rc,
    sync::Arc,
    task::Waker,
    time::Duration,
};

use crate::{
    free_list::{FreeList, Idx},
    iou,
    iou::sqe::{FsyncFlags, SockAddrStorage, StatxFlags, StatxMode, SubmissionFlags, TimeoutFlags},
    sys::{
        self,
        dma_buffer::{BufferStorage, DmaBuffer},
        DirectIo,
        InnerSource,
        IoBuffer,
        PollableStatus,
        Source,
        SourceType,
    },
    uring_sys,
    IoRequirements,
    Latency,
};
use buddy_alloc::buddy_alloc::{BuddyAlloc, BuddyAllocParam};
use nix::sys::{
    socket::{MsgFlags, SockAddr, SockFlag},
    stat::Mode as OpenMode,
};

use crate::uring_sys::IoRingOp;

use super::{EnqueuedSource, TimeSpec64};

const MSG_ZEROCOPY: i32 = 0x4000000;

#[allow(dead_code)]
#[derive(Debug)]
enum UringOpDescriptor {
    PollAdd(PollFlags),
    PollRemove(*const u8),
    Cancel(u64),
    Write(*const u8, usize, u64),
    WriteFixed(*const u8, usize, u64, u32),
    ReadFixed(u64, usize),
    Read(u64, usize),
    Open(*const u8, libc::c_int, u32),
    Close,
    FDataSync,
    Connect(*const SockAddr),
    LinkTimeout(*const uring_sys::__kernel_timespec),
    Accept(*mut SockAddrStorage),
    Fallocate(u64, u64, libc::c_int),
    Statx(*const u8, *mut libc::statx),
    Timeout(*const uring_sys::__kernel_timespec),
    TimeoutRemove(u64),
    SockSend(*const u8, usize, i32),
    SockSendMsg(*mut libc::msghdr, i32),
    SockRecv(usize, i32),
    SockRecvMsg(usize, i32),
    Nop,
}

#[derive(Debug)]
struct UringDescriptor {
    fd: RawFd,
    flags: SubmissionFlags,
    user_data: u64,
    args: UringOpDescriptor,
}

pub(crate) struct UringBufferAllocator {
    data: ptr::NonNull<u8>,
    size: usize,
    allocator: RefCell<BuddyAlloc>,
    layout: Layout,
    uring_buffer_id: Cell<Option<u32>>,
}

impl fmt::Debug for UringBufferAllocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UringBufferAllocator")
            .field("data", &self.data)
            .finish()
    }
}

impl UringBufferAllocator {
    fn new(size: usize) -> Self {
        let layout = Layout::from_size_align(size, 4096).unwrap();
        let (data, allocator) = unsafe {
            let data = alloc::alloc::alloc(layout) as *mut u8;
            let data = std::ptr::NonNull::new(data).unwrap();
            let allocator = BuddyAlloc::new(BuddyAllocParam::new(
                data.as_ptr(),
                layout.size(),
                layout.align(),
            ));
            (data, RefCell::new(allocator))
        };

        UringBufferAllocator {
            data,
            size,
            allocator,
            layout,
            uring_buffer_id: Cell::new(None),
        }
    }

    fn activate_registered_buffers(&self, idx: u32) {
        self.uring_buffer_id.set(Some(idx))
    }

    fn free(&self, ptr: ptr::NonNull<u8>) {
        let mut allocator = self.allocator.borrow_mut();
        allocator.free(ptr.as_ptr() as *mut u8);
    }

    fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data.as_ptr(), self.size) }
    }

    fn new_buffer(self: &Rc<Self>, size: usize) -> Option<DmaBuffer> {
        let mut alloc = self.allocator.borrow_mut();
        match ptr::NonNull::new(alloc.malloc(size)) {
            Some(data) => {
                let ub = UringBuffer {
                    allocator: self.clone(),
                    data,
                    uring_buffer_id: self.uring_buffer_id.get(),
                };
                Some(DmaBuffer::with_storage(size, BufferStorage::Uring(ub)))
            }
            None => DmaBuffer::new(size),
        }
    }
}

impl Drop for UringBufferAllocator {
    fn drop(&mut self) {
        unsafe {
            alloc::alloc::dealloc(self.data.as_ptr(), self.layout);
        }
    }
}

pub(crate) struct UringBuffer {
    allocator: Rc<UringBufferAllocator>,
    data: ptr::NonNull<u8>,
    uring_buffer_id: Option<u32>,
}

impl fmt::Debug for UringBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UringBuffer")
            .field("data", &self.data)
            .field("uring_buffer_id", &self.uring_buffer_id)
            .finish()
    }
}

impl UringBuffer {
    pub(crate) fn as_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    pub(crate) fn as_mut_ptr(&mut self) -> *mut u8 {
        self.data.as_ptr()
    }

    pub(crate) fn uring_buffer_id(&self) -> Option<u32> {
        self.uring_buffer_id
    }
}

impl Drop for UringBuffer {
    fn drop(&mut self) {
        let ptr = self.data;
        self.allocator.free(ptr);
    }
}

fn check_supported_operations(ops: &[uring_sys::IoRingOp]) -> bool {
    unsafe {
        let probe = uring_sys::io_uring_get_probe();
        if probe.is_null() {
            panic!(
                "Failed to register a probe. The most likely reason is that your kernel witnessed \
                 Romulus killing Remus (too old!! kernel should be at least 5.8)"
            );
        }

        let mut ret = true;
        for op in ops {
            let opint = *{ op as *const uring_sys::IoRingOp as *const libc::c_int };
            let sup = uring_sys::io_uring_opcode_supported(probe, opint) > 0;
            ret &= sup;
            if !sup {
                println!(
                    "Yo kernel is so old it was with Hannibal when he crossed the Alps! Missing \
                     {:?}",
                    op
                );
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

static GLOMMIO_URING_OPS: &[IoRingOp] = &[
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
    IoRingOp::IORING_OP_LINK_TIMEOUT,
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
    static ref IO_URING_RECENT_ENOUGH: bool = check_supported_operations(GLOMMIO_URING_OPS);
}

fn fill_sqe<F>(sqe: &mut iou::SQE<'_>, op: &UringDescriptor, buffer_allocation: F)
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
                sqe.prep_cancel(to_remove, 0);
            }
            UringOpDescriptor::Write(ptr, len, pos) => {
                let buf = std::slice::from_raw_parts(ptr, len);
                sqe.prep_write(op.fd, buf, pos);
            }
            UringOpDescriptor::Read(pos, len) => {
                let mut buf = buffer_allocation(len).expect("Buffer allocation failed");
                sqe.prep_read(op.fd, buf.as_bytes_mut(), pos);

                let src = peek_source(from_user_data(op.user_data));
                if let SourceType::Read(PollableStatus::NonPollable(DirectIo::Disabled), slot) =
                    &mut *src.source_type.borrow_mut()
                {
                    *slot = Some(IoBuffer::Dma(buf));
                } else {
                    panic!("Expected Read source type");
                };
            }
            UringOpDescriptor::Open(path, flags, mode) => {
                let path = CStr::from_ptr(path as _);
                sqe.prep_openat(
                    op.fd,
                    path,
                    OFlag::from_bits_truncate(flags),
                    OpenMode::from_bits_truncate(mode),
                );
            }
            UringOpDescriptor::FDataSync => {
                sqe.prep_fsync(op.fd, FsyncFlags::FSYNC_DATASYNC);
            }
            UringOpDescriptor::Connect(addr) => {
                sqe.prep_connect(op.fd, &*addr);
            }

            UringOpDescriptor::LinkTimeout(timespec) => {
                sqe.prep_link_timeout(&*timespec);
            }

            UringOpDescriptor::Accept(addr) => {
                sqe.prep_accept(op.fd, Some(&mut *addr), SockFlag::SOCK_CLOEXEC);
            }

            UringOpDescriptor::Fallocate(offset, size, flags) => {
                let flags = FallocateFlags::from_bits_truncate(flags);
                sqe.prep_fallocate(op.fd, offset, size, flags);
            }
            UringOpDescriptor::Statx(path, statx_buf) => {
                let flags = StatxFlags::AT_STATX_SYNC_AS_STAT | StatxFlags::AT_NO_AUTOMOUNT;
                let mode = StatxMode::from_bits_truncate(0x7ff);

                let path = CStr::from_ptr(path as _);
                sqe.prep_statx(-1, path, flags, mode, &mut *statx_buf);
            }
            UringOpDescriptor::Timeout(timespec) => {
                sqe.prep_timeout(&*timespec, 0, TimeoutFlags::empty());
            }
            UringOpDescriptor::TimeoutRemove(timer) => {
                sqe.prep_timeout_remove(timer as _);
            }
            UringOpDescriptor::Close => {
                sqe.prep_close(op.fd);
            }
            UringOpDescriptor::ReadFixed(pos, len) => {
                let mut buf = buffer_allocation(len).expect("Buffer allocation failed");
                let src = peek_source(from_user_data(op.user_data));

                match &mut *src.source_type.borrow_mut() {
                    SourceType::Read(PollableStatus::NonPollable(DirectIo::Disabled), slot) => {
                        sqe.prep_read(op.fd, buf.as_bytes_mut(), pos);
                        *slot = Some(IoBuffer::Dma(buf));
                    }
                    SourceType::Read(_, slot) => {
                        match buf.uring_buffer_id() {
                            None => {
                                sqe.prep_read(op.fd, buf.as_bytes_mut(), pos);
                            }
                            Some(idx) => {
                                sqe.prep_read_fixed(op.fd, buf.as_bytes_mut(), pos, idx);
                            }
                        };
                        *slot = Some(IoBuffer::Dma(buf));
                    }
                    _ => unreachable!(),
                };
            }

            UringOpDescriptor::WriteFixed(ptr, len, pos, buf_index) => {
                let buf = std::slice::from_raw_parts(ptr, len);
                sqe.prep_write_fixed(op.fd, buf, pos, buf_index as _);
            }

            UringOpDescriptor::SockSend(ptr, len, flags) => {
                let buf = std::slice::from_raw_parts(ptr, len);
                sqe.prep_send(
                    op.fd,
                    buf,
                    MsgFlags::from_bits_unchecked(flags | MSG_ZEROCOPY),
                );
            }

            UringOpDescriptor::SockSendMsg(hdr, flags) => {
                sqe.prep_sendmsg(
                    op.fd,
                    hdr,
                    MsgFlags::from_bits_unchecked(flags | MSG_ZEROCOPY),
                );
            }

            UringOpDescriptor::SockRecv(len, flags) => {
                let mut buf = DmaBuffer::new(len).expect("failed to allocate buffer");
                sqe.prep_recv(
                    op.fd,
                    buf.as_bytes_mut(),
                    MsgFlags::from_bits_unchecked(flags),
                );

                let src = peek_source(from_user_data(op.user_data));
                match &mut *src.source_type.borrow_mut() {
                    SourceType::SockRecv(slot) => {
                        *slot = Some(buf);
                    }
                    _ => unreachable!(),
                };
            }

            UringOpDescriptor::SockRecvMsg(len, flags) => {
                let mut buf = DmaBuffer::new(len).expect("failed to allocate buffer");
                let src = peek_source(from_user_data(op.user_data));
                match &mut *src.source_type.borrow_mut() {
                    SourceType::SockRecvMsg(slot, iov, hdr, msg_name) => {
                        iov.iov_base = buf.as_mut_ptr() as *mut libc::c_void;
                        iov.iov_len = len;

                        let msg_namelen = std::mem::size_of::<nix::sys::socket::sockaddr_storage>()
                            as libc::socklen_t;
                        hdr.msg_name = msg_name.as_mut_ptr() as *mut libc::c_void;
                        hdr.msg_namelen = msg_namelen;
                        hdr.msg_iov = iov as *mut libc::iovec;
                        hdr.msg_iovlen = 1;

                        sqe.prep_recvmsg(
                            op.fd,
                            hdr as *mut libc::msghdr,
                            MsgFlags::from_bits_unchecked(flags),
                        );
                        *slot = Some(buf);
                    }
                    _ => unreachable!(),
                };
            }
            UringOpDescriptor::Nop => sqe.prep_nop(),
        }
        sqe.set_user_data(user_data);
        sqe.set_flags(op.flags);
    }
}

fn transmute_error(res: io::Result<u32>) -> io::Result<usize> {
    res.map(|x| x as usize) // iou standardized on u32, which is good for low level but for higher layers usize is
        // better
        .map_err(|x| {
            // Convert CANCELED to TimedOut. This will be the case for linked sqes with a
            // timeout, and if we wanted to be really strict we'd check. But if
            // the operation is truly cancelled no one will check the result,
            // and we have no other use case for cancel at the moment so keep it simple
            if let Some(libc::ECANCELED) = x.raw_os_error() {
                io::Error::from_raw_os_error(libc::ETIMEDOUT)
            } else {
                x
            }
        })
}

fn process_one_event<F>(cqe: Option<iou::CQE>, try_process: F) -> Option<bool>
where
    F: FnOnce(&InnerSource) -> Option<()>,
{
    if let Some(value) = cqe {
        // No user data is POLL_REMOVE or CANCEL, we won't process.
        if value.user_data() == 0 {
            return Some(false);
        }

        let src = consume_source(from_user_data(value.user_data()));

        let result = value.result();

        let mut woke = false;
        if try_process(&*src).is_none() {
            let mut w = src.wakers.borrow_mut();
            w.result = Some(transmute_error(result));
            if let Some(waiter) = w.waiter.take() {
                woke = true;
                wake!(waiter);
            }
        }
        return Some(woke);
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
                flags: SubmissionFlags::empty(),
                user_data: 0,
            }),
        }
    }
}

trait UringCommon {
    fn submission_queue(&mut self) -> ReactorQueue;
    fn submit_sqes(&mut self) -> io::Result<usize>;
    fn needs_kernel_enter(&self) -> bool;
    // None if it wasn't possible to acquire an sqe. Some(true) if it was possible
    // and there was something to dispatch. Some(false) if there was nothing to
    // dispatch
    fn submit_one_event(&mut self, queue: &mut VecDeque<UringDescriptor>) -> Option<bool>;
    /// Return `None` if no event is completed, `Some(true)` for a task is woke
    /// and `Some(false)` for not.
    fn consume_one_event(&mut self) -> Option<bool>;
    fn name(&self) -> &'static str;
    fn registrar(&self) -> iou::Registrar<'_>;

    fn consume_sqe_queue(
        &mut self,
        queue: &mut VecDeque<UringDescriptor>,
        mut dispatch: bool,
    ) -> io::Result<usize> {
        loop {
            match self.submit_one_event(queue) {
                None => {
                    dispatch = true;
                    break;
                }
                Some(true) => {}
                Some(false) => break,
            }
        }

        if dispatch && self.needs_kernel_enter() {
            self.submit_sqes()
        } else {
            Ok(0)
        }
    }

    // We will not dispatch the cancellation queue unless we need to.
    // Dispatches will come from the submission queue.
    fn consume_cancellation_queue(&mut self) -> io::Result<usize> {
        let q = self.submission_queue();
        let mut queue = q.borrow_mut();
        self.consume_sqe_queue(&mut queue.cancellations, false)
    }

    fn consume_submission_queue(&mut self) -> io::Result<usize> {
        let q = self.submission_queue();
        let mut queue = q.borrow_mut();
        self.consume_sqe_queue(&mut queue.submissions, true)
    }

    fn consume_completion_queue(&mut self, woke: &mut usize) -> usize {
        let mut completed = 0;
        loop {
            match self.consume_one_event() {
                None => break,
                Some(false) => completed += 1,
                Some(true) => {
                    completed += 1;
                    *woke += 1;
                }
            }
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
    fn flush_cancellations(&mut self, woke: &mut usize) {
        let mut cnt = 0;
        loop {
            if self.consume_cancellation_queue().is_ok() {
                break;
            }
            self.consume_completion_queue(woke);
            cnt += 1;
            if cnt > 1_000_000 {
                panic!(
                    "i tried literally a million times but couldn't flush to the {} ring",
                    self.name()
                );
            }
        }
        self.consume_completion_queue(woke);
    }
}

struct PollRing {
    ring: iou::IoUring,
    size: usize,
    submission_queue: ReactorQueue,
    submitted: u64,
    completed: u64,
    allocator: Rc<UringBufferAllocator>,
}

impl PollRing {
    fn new(size: usize, allocator: Rc<UringBufferAllocator>) -> io::Result<Self> {
        let ring = iou::IoUring::new_with_flags(
            size as _,
            iou::SetupFlags::IOPOLL,
            iou::SetupFeatures::empty(),
        )?;
        Ok(PollRing {
            submitted: 0,
            completed: 0,
            size,
            ring,
            submission_queue: UringQueueState::with_capacity(size * 4),
            allocator,
        })
    }

    fn can_sleep(&self) -> bool {
        self.submitted == self.completed
    }

    pub(crate) fn alloc_dma_buffer(&mut self, size: usize) -> DmaBuffer {
        self.allocator.new_buffer(size).unwrap()
    }
}

impl UringCommon for PollRing {
    fn name(&self) -> &'static str {
        "poll"
    }

    fn registrar(&self) -> iou::Registrar<'_> {
        self.ring.registrar()
    }

    fn needs_kernel_enter(&self) -> bool {
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
            let res = self.ring.submit_sqes()?;
            Ok(res as usize)
        } else {
            Ok(0)
        }
    }

    fn consume_one_event(&mut self) -> Option<bool> {
        process_one_event(self.ring.peek_for_cqe(), |_| None).map(|x| {
            self.completed += 1;
            x
        })
    }

    fn submit_one_event(&mut self, queue: &mut VecDeque<UringDescriptor>) -> Option<bool> {
        if queue.is_empty() {
            return Some(false);
        }

        // find position of first element without a link
        let chain_len = match queue.iter().position(|sqe| {
            !sqe.flags
                .intersects(SubmissionFlags::IO_LINK | SubmissionFlags::IO_HARDLINK)
        }) {
            Some(pos) => pos,
            None => panic!(
                "Unterminated SQE link chain: internal source prep bug, {:?}",
                queue
            ),
        };

        if chain_len + 1 > self.size {
            panic!(
                "Link chain length ({:?}) overflows submission queue bounds ({:?}): {:?}",
                chain_len + 1,
                self.size,
                queue
            );
        }

        if let Some(sqes) = self.ring.prepare_sqes(chain_len as u32 + 1) {
            for mut sqe in sqes {
                self.submitted += 1;
                let op = queue.pop_front().unwrap();
                let allocator = self.allocator.clone();
                fill_sqe(&mut sqe, &op, move |size| allocator.new_buffer(size));
            }
            Some(true)
        } else {
            None
        }
    }
}

impl InnerSource {
    pub(crate) fn update_source_type(&self, source_type: SourceType) -> SourceType {
        std::mem::replace(&mut *self.source_type.borrow_mut(), source_type)
    }
}

impl Source {
    pub(crate) fn set_timeout(&self, d: Duration) -> Option<Duration> {
        let mut t = self.inner.timeout.borrow_mut();
        let old = *t;
        *t = Some(TimeSpec64::from(d));
        old.map(Duration::from)
    }

    fn timeout_ref(&self) -> Ref<'_, Option<TimeSpec64>> {
        self.inner.timeout.borrow()
    }

    pub(crate) fn latency_req(&self) -> Latency {
        self.inner.io_requirements.latency_req
    }

    fn source_type(&self) -> Ref<'_, SourceType> {
        self.inner.source_type.borrow()
    }

    pub(crate) fn source_type_mut(&self) -> RefMut<'_, SourceType> {
        self.inner.source_type.borrow_mut()
    }

    pub(crate) fn extract_source_type(&self) -> SourceType {
        self.inner.update_source_type(SourceType::Invalid)
    }

    pub(crate) fn extract_dma_buffer(&mut self) -> DmaBuffer {
        let stype = self.extract_source_type();
        match stype {
            SourceType::Read(_, Some(IoBuffer::Dma(buffer))) => buffer,
            SourceType::Write(_, IoBuffer::Dma(buffer)) => buffer,
            x => panic!("Could not extract buffer. Source: {:?}", x),
        }
    }

    pub(crate) fn extract_buffer(&mut self) -> Vec<u8> {
        let stype = self.extract_source_type();
        match stype {
            SourceType::Read(_, Some(IoBuffer::Buffered(buffer))) => buffer,
            SourceType::Write(_, IoBuffer::Buffered(buffer)) => buffer,
            x => panic!("Could not extract buffer. Source: {:?}", x),
        }
    }

    pub(crate) fn take_result(&self) -> Option<io::Result<usize>> {
        let mut w = self.inner.wakers.borrow_mut();
        match w.result.take() {
            None => None,
            Some(x) => Some(x.map(|x| x as usize)),
        }
    }

    pub(crate) fn has_result(&self) -> bool {
        let w = self.inner.wakers.borrow();
        w.result.is_some()
    }

    pub(crate) fn add_waiter(&self, waker: Waker) {
        let mut w = self.inner.wakers.borrow_mut();
        w.waiter.replace(waker);
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
    size: usize,
    submission_queue: ReactorQueue,
    waiting_submission: usize,
    name: &'static str,
    allocator: Rc<UringBufferAllocator>,
}

impl SleepableRing {
    fn new(
        size: usize,
        name: &'static str,
        allocator: Rc<UringBufferAllocator>,
    ) -> io::Result<Self> {
        assert_eq!(*IO_URING_RECENT_ENOUGH, true);
        Ok(SleepableRing {
            ring: iou::IoUring::new(size as _)?,
            size,
            submission_queue: UringQueueState::with_capacity(size * 4),
            waiting_submission: 0,
            name,
            allocator,
        })
    }

    fn ring_fd(&self) -> RawFd {
        self.ring.raw().ring_fd
    }

    fn arm_timer(&mut self, d: Duration) -> Source {
        let source = Source::new(
            IoRequirements::default(),
            -1,
            SourceType::Timeout(TimeSpec64::from(d)),
        );
        let new_id = add_source(&source, self.submission_queue.clone());
        let op = match &*source.source_type() {
            SourceType::Timeout(ts) => UringOpDescriptor::Timeout(&ts.raw as *const _),
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
            flags: SubmissionFlags::empty(),
            user_data: to_user_data(new_id),
        });
        // No need to submit, the next ring enter will submit for us. Because
        // we just flushed and we got put in front of the queue we should get a SQE.
        // Still it would be nice to verify if we did.
        source
    }

    fn install_eventfd(&mut self, eventfd_src: &Source) -> bool {
        if let Some(mut sqe) = self.ring.prepare_sqe() {
            self.waiting_submission += 1;
            // Now must wait on the eventfd in case someone wants to wake us up.
            // If we can't then we can't sleep and will just bail immediately
            let op = UringDescriptor {
                fd: eventfd_src.raw(),
                flags: SubmissionFlags::empty(),
                user_data: to_user_data(add_source(eventfd_src, self.submission_queue.clone())),
                args: UringOpDescriptor::Read(0, 8),
            };
            let allocator = self.allocator.clone();
            fill_sqe(&mut sqe, &op, |size| allocator.new_buffer(size));
            true
        } else {
            false
        }
    }

    fn sleep(&mut self, link: &mut Source, eventfd_src: &Source) -> io::Result<usize> {
        if let Some(mut sqe) = self.ring.prepare_sqe() {
            self.waiting_submission += 1;

            let op = UringDescriptor {
                fd: link.raw(),
                flags: SubmissionFlags::empty(),
                user_data: to_user_data(add_source(link, self.submission_queue.clone())),
                args: UringOpDescriptor::PollAdd(common_flags() | read_flags()),
            };
            fill_sqe(&mut sqe, &op, DmaBuffer::new);
        } else {
            // Can't link rings because we ran out of CQEs. Just can't sleep.
            // Submit what we have, once we're out of here we'll consume them
            // and at some point will be able to sleep again.
            return self.ring.submit_sqes().map(|x| x as usize);
        }

        let res = eventfd_src.take_result();
        match res {
            None => {
                // We already have the eventfd registered and nobody woke us up so far.
                // Just proceed to sleep
                self.ring.submit_sqes_and_wait(1).map(|x| x as usize)
            }
            Some(res) => {
                if self.install_eventfd(eventfd_src) {
                    // Do not expect any failures reading from eventfd. This will panic if we
                    // failed.
                    res.unwrap();
                    // Now must wait on the eventfd in case someone wants to wake us up.
                    // If we can't then we can't sleep and will just bail immediately
                    self.ring.submit_sqes_and_wait(1).map(|x| x as usize)
                } else {
                    self.ring.submit_sqes().map(|x| x as usize)
                }
            }
        }
    }
}

impl UringCommon for SleepableRing {
    fn name(&self) -> &'static str {
        self.name
    }

    fn registrar(&self) -> iou::Registrar<'_> {
        self.ring.registrar()
    }

    fn needs_kernel_enter(&self) -> bool {
        self.waiting_submission > 0
    }

    fn submission_queue(&mut self) -> ReactorQueue {
        self.submission_queue.clone()
    }

    fn submit_sqes(&mut self) -> io::Result<usize> {
        let x = self.ring.submit_sqes()? as usize;
        self.waiting_submission -= x;
        Ok(x)
    }

    fn consume_one_event(&mut self) -> Option<bool> {
        process_one_event(self.ring.peek_for_cqe(), |source| {
            match &mut *source.source_type.borrow_mut() {
                SourceType::LinkRings => Some(()),
                _ => None,
            }
        })
    }

    fn submit_one_event(&mut self, queue: &mut VecDeque<UringDescriptor>) -> Option<bool> {
        if queue.is_empty() {
            return Some(false);
        }

        // find position of first element without a link
        let chain_len = match queue.iter().position(|sqe| {
            !sqe.flags
                .intersects(SubmissionFlags::IO_LINK | SubmissionFlags::IO_HARDLINK)
        }) {
            Some(pos) => pos,
            None => panic!(
                "Unterminated SQE link chain: internal source prep bug, {:?}",
                queue
            ),
        };

        if chain_len + 1 > self.size {
            panic!(
                "Link chain length ({:?}) overflows submission queue bounds ({:?}): {:?}",
                chain_len + 1,
                self.size,
                queue
            );
        }

        if let Some(sqes) = self.ring.prepare_sqes(chain_len as u32 + 1) {
            for mut sqe in sqes {
                self.waiting_submission += 1;
                let op = queue.pop_front().unwrap();
                let allocator = self.allocator.clone();
                fill_sqe(&mut sqe, &op, move |size| allocator.new_buffer(size));
            }
            Some(true)
        } else {
            None
        }
    }
}

pub(crate) struct Reactor {
    // FIXME: it is starting to feel we should clean this up to a Inner pattern
    main_ring: RefCell<SleepableRing>,
    latency_ring: RefCell<SleepableRing>,
    poll_ring: RefCell<PollRing>,

    timeout_src: Cell<Option<Source>>,

    link_fd: RawFd,

    // This keeps the eventfd alive. Drop will close it when we're done
    notifier: Arc<sys::SleepNotifier>,
    // This is the source used to handle the notifications into the ring
    eventfd_src: Source,
}

fn common_flags() -> PollFlags {
    PollFlags::POLLERR | PollFlags::POLLHUP | PollFlags::POLLNVAL
}

/// Epoll flags for all possible readability events.
fn read_flags() -> PollFlags {
    PollFlags::POLLIN | PollFlags::POLLPRI
}

macro_rules! consume_rings {
    (into $woke:expr; $( $ring:expr ),+ ) => {{
        let mut consumed = 0;
        $(
            consumed += $ring.consume_completion_queue($woke);
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

fn align_up(v: usize, align: usize) -> usize {
    (v + align - 1) & !(align - 1)
}

impl Reactor {
    pub(crate) fn new(
        notifier: Arc<sys::SleepNotifier>,
        mut io_memory: usize,
    ) -> io::Result<Reactor> {
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

        // always have at least some small amount of memory for the slab
        io_memory = std::cmp::max(align_up(io_memory, 4096), 65536);

        let allocator = Rc::new(UringBufferAllocator::new(io_memory));
        let registry = vec![allocator.as_bytes()];

        let mut main_ring = SleepableRing::new(128, "main", allocator.clone())?;
        let poll_ring = PollRing::new(128, allocator.clone())?;

        match main_ring.registrar().register_buffers_by_ref(&registry) {
            Err(x) => warn!(
                "Error: registering buffers in the main ring. Skipping{:#?}",
                x
            ),
            Ok(_) => match poll_ring.registrar().register_buffers_by_ref(&registry) {
                Err(x) => {
                    warn!(
                        "Error: registering buffers in the poll ring. Skipping{:#?}",
                        x
                    );
                    main_ring.registrar().unregister_buffers().unwrap();
                }
                Ok(_) => {
                    allocator.activate_registered_buffers(0);
                }
            },
        }

        let latency_ring = SleepableRing::new(128, "latency", allocator.clone())?;
        let link_fd = latency_ring.ring_fd();

        let eventfd_src = Source::new(
            IoRequirements::default(),
            notifier.eventfd_fd(),
            SourceType::Read(PollableStatus::NonPollable(DirectIo::Disabled), None),
        );
        assert_eq!(main_ring.install_eventfd(&eventfd_src), true);

        Ok(Reactor {
            main_ring: RefCell::new(main_ring),
            latency_ring: RefCell::new(latency_ring),
            poll_ring: RefCell::new(poll_ring),
            timeout_src: Cell::new(None),
            link_fd,
            notifier,
            eventfd_src,
        })
    }

    pub(crate) fn id(&self) -> usize {
        self.notifier.id()
    }

    pub(crate) fn foreign_notifiers(&self) -> Option<core::task::Waker> {
        self.notifier.get_foreign_notifier()
    }

    pub(crate) fn alloc_dma_buffer(&self, size: usize) -> DmaBuffer {
        let mut poll_ring = self.poll_ring.borrow_mut();
        poll_ring.alloc_dma_buffer(size)
    }

    pub(crate) fn write_dma(&self, source: &Source, pos: u64) {
        let op = match &*source.source_type() {
            SourceType::Write(
                PollableStatus::NonPollable(DirectIo::Disabled),
                IoBuffer::Dma(buf),
            ) => UringOpDescriptor::Write(buf.as_ptr(), buf.len(), pos),
            SourceType::Write(_, IoBuffer::Dma(buf)) => match buf.uring_buffer_id() {
                Some(id) => UringOpDescriptor::WriteFixed(buf.as_ptr(), buf.len(), pos, id),
                None => UringOpDescriptor::Write(buf.as_ptr(), buf.len(), pos),
            },
            x => panic!("Unexpected source type for write: {:?}", x),
        };
        self.queue_storage_io_request(source, op);
    }

    pub(crate) fn write_buffered(&self, source: &Source, pos: u64) {
        let op = match &*source.source_type() {
            SourceType::Write(
                PollableStatus::NonPollable(DirectIo::Disabled),
                IoBuffer::Buffered(buf),
            ) => UringOpDescriptor::Write(buf.as_ptr() as *const u8, buf.len(), pos),
            x => panic!("Unexpected source type for write: {:?}", x),
        };
        self.queue_standard_request(source, op);
    }

    pub(crate) fn read_dma(&self, source: &Source, pos: u64, size: usize) {
        let op = UringOpDescriptor::ReadFixed(pos, size);
        self.queue_storage_io_request(source, op);
    }

    pub(crate) fn read_buffered(&self, source: &Source, pos: u64, size: usize) {
        let op = UringOpDescriptor::Read(pos, size);
        self.queue_standard_request(source, op);
    }

    pub(crate) fn send(&self, source: &Source, flags: MsgFlags) {
        let op = match &*source.source_type() {
            SourceType::SockSend(buf) => {
                UringOpDescriptor::SockSend(buf.as_ptr() as *const u8, buf.len(), flags.bits())
            }
            _ => unreachable!(),
        };
        self.queue_standard_request(source, op);
    }

    pub(crate) fn sendmsg(&self, source: &Source, flags: MsgFlags) {
        let op = match &mut *source.source_type_mut() {
            SourceType::SockSendMsg(_, iov, hdr, addr) => {
                let (msg_name, msg_namelen) = addr.as_ffi_pair();
                let msg_name = msg_name as *const nix::sys::socket::sockaddr as *mut libc::c_void;

                hdr.msg_iov = iov as *mut libc::iovec;
                hdr.msg_iovlen = 1;
                hdr.msg_name = msg_name;
                hdr.msg_namelen = msg_namelen;

                UringOpDescriptor::SockSendMsg(hdr, flags.bits())
            }
            _ => unreachable!(),
        };
        self.queue_standard_request(source, op);
    }

    pub(crate) fn recv(&self, source: &Source, len: usize, flags: MsgFlags) {
        let op = UringOpDescriptor::SockRecv(len, flags.bits());
        self.queue_standard_request(source, op);
    }

    pub(crate) fn recvmsg(&self, source: &Source, len: usize, flags: MsgFlags) {
        let op = UringOpDescriptor::SockRecvMsg(len, flags.bits());
        self.queue_standard_request(source, op);
    }

    pub(crate) fn connect(&self, source: &Source) {
        let op = match &*source.source_type() {
            SourceType::Connect(addr) => UringOpDescriptor::Connect(addr as *const SockAddr),
            x => panic!("Unexpected source type for connect: {:?}", x),
        };
        self.queue_standard_request(source, op);
    }

    pub(crate) fn accept(&self, source: &Source) {
        let op = match &mut *source.source_type_mut() {
            SourceType::Accept(addr) => UringOpDescriptor::Accept(addr as *mut SockAddrStorage),
            x => panic!("Unexpected source type for accept: {:?}", x),
        };
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

    pub(crate) fn open_at(&self, source: &Source, flags: libc::c_int, mode: libc::mode_t) {
        let pathptr = match &*source.source_type() {
            SourceType::Open(cstring) => cstring.as_c_str().as_ptr(),
            _ => panic!("Wrong source type!"),
        };
        let op = UringOpDescriptor::Open(pathptr as _, flags, mode as _);
        self.queue_standard_request(source, op);
    }

    #[cfg(feature = "bench")]
    pub(crate) fn nop(&self, source: &Source) {
        self.queue_standard_request(source, UringOpDescriptor::Nop)
    }

    // io_uring can return EBUSY when the CQE queue is full and we try to push more
    // requests. This is fine: we just need to make sure that we don't sleep and
    // that we dont' failed rushed polls. So we just ignore this error
    fn busy_ok(x: std::io::Error) -> io::Result<usize> {
        match x.raw_os_error() {
            Some(libc::EBUSY) => Ok(0),
            Some(_) => Err(x),
            None => Err(x),
        }
    }

    // We want to go to sleep but we can only go to sleep in one of the rings,
    // as we only have one thread. There are more than one sleepable rings, so
    // what we do is we take advantage of the fact that the ring's ring_fd is
    // pollable and register a POLL_ADD event into the ring we will wait on.
    //
    // We may not be able to register an SQE at this point, so we return an Error
    // and will just not sleep.
    fn link_rings_and_sleep(
        &self,
        ring: &mut SleepableRing,
        eventfd_src: &Source,
    ) -> io::Result<()> {
        let mut link_rings = Source::new(
            IoRequirements::default(),
            self.link_fd,
            SourceType::LinkRings,
        );
        ring.sleep(&mut link_rings, eventfd_src)
            .or_else(Self::busy_ok)?;
        Ok(())
    }

    fn simple_poll(ring: &RefCell<dyn UringCommon>, woke: &mut usize) -> io::Result<()> {
        let mut ring = ring.borrow_mut();
        ring.consume_submission_queue().or_else(Self::busy_ok)?;
        ring.consume_completion_queue(woke);
        Ok(())
    }

    pub(crate) fn rush_dispatch(
        &self,
        latency: Option<Latency>,
        woke: &mut usize,
    ) -> io::Result<()> {
        match latency {
            None => Self::simple_poll(&self.poll_ring, woke),
            Some(Latency::NotImportant) => Self::simple_poll(&self.main_ring, woke),
            Some(Latency::Matters(_)) => Self::simple_poll(&self.latency_ring, woke),
        }
    }

    // This function can be passed two timers. Because they play different roles we
    // keep them separate instead of overloading the same parameter.
    //
    // * The first is the preempt timer. It is designed to take the current task
    //   queue out of the cpu. If nothing else fires in the latency ring the preempt
    //   timer will, making need_preempt return true. Currently we always install a
    //   preempt timer in the upper layers but from the point of view of the
    //   io_uring implementation it is optional: it is perfectly valid not to have
    //   one. Preempt timers are installed by Glommio executor runtime.
    //
    // * The second is the user timer. It is installed per a user request when the
    //   user creates a Timer (or TimerAction).
    //
    // At some level, those are both just timers and can be coalesced. And they
    // certainly are: if there is a user timer that needs to fire in 1ms and we
    // want the preempt_timer to also fire around 1ms, there is no need to
    // register two timers. At the end of the day, all that matters is that the
    // latency ring flares and that we leave the CPU. That is because unlike I/O, we
    // don't have one Source per timer, and parking.rs just keeps them on a wheel
    // and just tell us about what is the next expiration.
    //
    // However they are also different. The main source of difference is sleep and
    // wake behavior:
    //
    // * When there is no more work to do and we go to sleep, we do not want to
    //   register the preempt timer: it is designed to fire periodically to take us
    //   out of the CPU and if there is no task queue running, we don't want to wake
    //   up and spend power just for that. However if there is a user timer that
    //   needs to fire in the future we must register it. Otherwise we will sleep
    //   and never wake up.
    //
    // * The user timer point of expiration never changes. So once we register it we
    //   don't need to rearm it until it fires. But the preempt timer has to be
    //   rearmed every time. Moreover it needs to give every task queue a fair shot
    //   at running. So it needs to be rearmed as close as possible to the point
    //   where we *leave* this method. For instance: if we spin here for 3ms and the
    //   preempt timer is 10ms that would leave the next task queue just 7ms to run.
    pub(crate) fn wait<F>(
        &self,
        preempt_timer: Option<Duration>,
        user_timer: Option<Duration>,
        mut woke: usize,
        process_remote_channels: F,
    ) -> io::Result<bool>
    where
        F: Fn() -> usize,
    {
        let mut poll_ring = self.poll_ring.borrow_mut();
        let mut main_ring = self.main_ring.borrow_mut();
        let mut lat_ring = self.latency_ring.borrow_mut();

        // Cancel the old timer regardless of whether or not we can sleep:
        // if we won't sleep, we will register the new timer with its new
        // value.
        //
        // But if we will sleep, there might be a timer registered that needs
        // to be removed otherwise we'll wake up when it expires.
        drop(self.timeout_src.take());
        let mut should_sleep = match preempt_timer {
            None => true,
            Some(dur) => {
                self.timeout_src.set(Some(lat_ring.arm_timer(dur)));
                false
            }
        };

        // this will only dispatch if we run out of sqes. Which means until
        // flush_rings! nothing is really send to the kernel...
        flush_cancellations!(into &mut woke; main_ring, lat_ring, poll_ring);
        // ... which happens right here. If you ever reorder this code just
        // be careful about this dependency.
        flush_rings!(main_ring, lat_ring, poll_ring)?;
        consume_rings!(into &mut woke; lat_ring, poll_ring, main_ring);

        // If we generated any event so far, we can't sleep. Need to handle them.
        should_sleep &= (woke == 0) & poll_ring.can_sleep();

        if should_sleep {
            // We are about to go to sleep. It's ok to sleep, but if there
            // is a timer set, we need to make sure we wake up to handle it.
            if let Some(dur) = user_timer {
                self.timeout_src.set(Some(lat_ring.arm_timer(dur)));
                flush_rings!(lat_ring)?;
            }
            // From this moment on the remote executors are aware that we are sleeping
            // We have to sweep the remote channels function once more because since
            // last time until now it could be that something happened in a remote executor
            // that opened up room. If if did we bail on sleep and go process it.
            self.notifier.prepare_to_sleep();
            // See https://www.scylladb.com/2018/02/15/memory-barriers-seastar-linux/ for
            // details. This translates to sys_membarrier() /
            // MEMBARRIER_CMD_PRIVATE_EXPEDITED
            membarrier::heavy();
            if process_remote_channels() == 0 {
                self.link_rings_and_sleep(&mut main_ring, &self.eventfd_src)
                    .expect("some error");
                // woke up, so no need to notify us anymore.
                self.notifier.wake_up();
                // may have new cancellations related to the link ring fd.
                flush_cancellations!(into &mut 0; main_ring);
                flush_rings!(main_ring)?;
                consume_rings!(into &mut 0; lat_ring, poll_ring, main_ring);
            }
        }

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
        let cq = unsafe { &lat_ring.ring.raw_mut().cq };
        (cq.khead, cq.ktail)
    }

    // RAII-close asynchronously files that were not closed explicitly.
    // We can't do this through a Source, because the Source will be dropped when
    // the file is dropped.
    pub(crate) fn async_close(&self, fd: RawFd) {
        let q = self.main_ring.borrow_mut().submission_queue();
        let mut queue = q.borrow_mut();
        queue.submissions.push_back(UringDescriptor {
            args: UringOpDescriptor::Close,
            fd,
            flags: SubmissionFlags::empty(),
            user_data: 0,
        });
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
            PollableStatus::NonPollable(_) => queue_request_into_ring(&self.main_ring, source, op),
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

    let flags = match &*source.timeout_ref() {
        Some(_) => SubmissionFlags::IO_LINK,
        _ => SubmissionFlags::empty(),
    };

    let mut queue = q.borrow_mut();
    queue.submissions.push_back(UringDescriptor {
        args: descriptor,
        fd: source.raw(),
        flags,
        user_data: to_user_data(id),
    });

    if let Some(ref ts) = &*source.timeout_ref() {
        queue.submissions.push_back(UringDescriptor {
            args: UringOpDescriptor::LinkTimeout(&ts.raw as *const _),
            flags: SubmissionFlags::empty(),
            fd: -1,
            user_data: 0,
        });
    }
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
        let notifier = sys::new_sleep_notifier().unwrap();
        let reactor = Reactor::new(notifier, 0).unwrap();

        fn timeout_source(millis: u64) -> (Source, UringOpDescriptor) {
            let source = Source::new(
                IoRequirements::default(),
                -1,
                SourceType::Timeout(TimeSpec64::from(Duration::from_millis(millis))),
            );
            let op = match &*source.source_type() {
                SourceType::Timeout(ts) => UringOpDescriptor::Timeout(&ts.raw as *const _),
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
        reactor.wait(None, None, 0, || 0).unwrap();
        let elapsed_ms = start.elapsed().as_millis();
        assert!(50 <= elapsed_ms && elapsed_ms < 100);

        drop(slow); // Cancel this one.

        reactor.wait(None, None, 0, || 0).unwrap();
        let elapsed_ms = start.elapsed().as_millis();
        assert!(300 <= elapsed_ms && elapsed_ms < 350);
    }

    #[test]
    fn allocator() {
        let l = Layout::from_size_align(10 << 20, 4 << 10).unwrap();
        let (data, mut allocator) = unsafe {
            let data = alloc::alloc::alloc(l) as *mut u8;
            assert_eq!(data as usize & 4095, 0);
            let data = std::ptr::NonNull::new(data).unwrap();
            (
                data,
                BuddyAlloc::new(BuddyAllocParam::new(data.as_ptr(), l.size(), l.align())),
            )
        };
        let x = allocator.malloc(4096);
        assert_eq!(x as usize & 4095, 0);
        let x = allocator.malloc(1024);
        assert_eq!(x as usize & 4095, 0);
        let x = allocator.malloc(1);
        assert_eq!(x as usize & 4095, 0);
        unsafe { alloc::alloc::dealloc(data.as_ptr(), l) }
    }

    #[test]
    fn allocator_exhaustion() {
        // The allocator fails with a single page, because it needs extra metadata
        // space
        let al = Rc::new(UringBufferAllocator::new(8192));
        al.activate_registered_buffers(1234);
        let x = al.new_buffer(4096).unwrap();
        let y = al.new_buffer(4096).unwrap();

        match x.uring_buffer_id() {
            Some(x) => assert_eq!(x, 1234),
            None => panic!("Expected uring buffer"),
        }

        match y.uring_buffer_id() {
            Some(_) => panic!("Expected non-uring buffer"),
            None => {}
        }
        drop(x);
        drop(y);

        // memory is back, able to allocate again
        let x = al.new_buffer(4096).unwrap();
        match x.uring_buffer_id() {
            Some(x) => assert_eq!(x, 1234),
            None => panic!("Expected uring buffer"),
        }
        drop(x);
        // Allocation for an object that is too big fails
        let x = al.new_buffer(40960).unwrap();
        match x.uring_buffer_id() {
            Some(_) => panic!("Expected non-uring buffer"),
            None => {}
        }
    }

    #[test]
    fn sqe_link_chain() {
        let allocator = Rc::new(UringBufferAllocator::new(65536));
        let mut ring = SleepableRing::new(4, "main", allocator).unwrap();
        let q = ring.submission_queue();
        let mut queue = q.borrow_mut();

        // enqueue three nops. The second is soft-linked to the third
        for i in 0..3 {
            queue.submissions.push_back(UringDescriptor {
                args: UringOpDescriptor::Nop,
                fd: -1,
                flags: if i == 1 {
                    SubmissionFlags::IO_LINK
                } else {
                    SubmissionFlags::empty()
                },
                user_data: 0,
            });
        }

        // the first nop is unlinked, so we're only expecting one SQE
        ring.submit_one_event(&mut queue.submissions);
        assert_eq!(1, ring.submit_sqes().unwrap());
        assert_eq!(1, ring.consume_completion_queue(&mut 0));

        // the following nops are linked, so we expect two submissions and completions
        ring.submit_one_event(&mut queue.submissions);
        assert_eq!(2, ring.submit_sqes().unwrap());
        assert_eq!(2, ring.consume_completion_queue(&mut 0));
    }

    #[test]
    #[should_panic(expected = "Unterminated SQE link chain")]
    fn unterminated_sqe_link_chain() {
        let allocator = Rc::new(UringBufferAllocator::new(65536));
        let mut ring = SleepableRing::new(2, "main", allocator).unwrap();
        let q = ring.submission_queue();
        let mut queue = q.borrow_mut();

        queue.submissions.push_back(UringDescriptor {
            args: UringOpDescriptor::Close,
            fd: -1,
            flags: SubmissionFlags::IO_LINK,
            user_data: 0,
        });

        // If the link chain points outside of the queue, we panic
        ring.submit_one_event(&mut queue.submissions);
    }

    #[test]
    #[should_panic(expected = "Link chain length (3) overflows submission queue bounds (2)")]
    fn sqe_link_chain_overflow() {
        let allocator = Rc::new(UringBufferAllocator::new(65536));
        let mut ring = SleepableRing::new(2, "main", allocator).unwrap();
        let q = ring.submission_queue();
        let mut queue = q.borrow_mut();

        for _ in 0..2 {
            queue.submissions.push_back(UringDescriptor {
                args: UringOpDescriptor::Close,
                fd: -1,
                flags: SubmissionFlags::IO_LINK,
                user_data: 0,
            });
        }

        queue.submissions.push_back(UringDescriptor {
            args: UringOpDescriptor::Close,
            fd: -1,
            flags: SubmissionFlags::empty(),
            user_data: 0,
        });

        // If the link chain is longer than the io_uring submission queue, we panic
        ring.submit_one_event(&mut queue.submissions);
    }
}
