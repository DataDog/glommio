// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use nix::poll::PollFlags;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::convert::TryInto;
use std::ffi::CStr;
use std::io;
use std::os::unix::io::RawFd;
use std::pin::Pin;
use std::task::Waker;
use std::time::Duration;

use crate::sys::posix_buffers::PosixDmaBuffer;
use crate::sys::{Source, SourceType};
use crate::{IoRequirements, Latency};

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
    Cancel(*const u8),
    Write(*const u8, usize, u64),
    WriteFixed(*const u8, usize, u64, usize),
    ReadFixed(u64, usize),
    Read(*mut u8, usize, u64),
    Open(*const u8, libc::c_int, u32),
    Close,
    FDataSync,
    Fallocate(u64, u64, libc::c_int),
    Statx(*const u8, *mut libc::statx),
    Timeout(u64),
    TimeoutRemove(*const Source),
}

#[derive(Debug)]
struct UringDescriptor {
    fd: RawFd,
    user_data: u64,
    args: UringOpDescriptor,
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
            UringOpDescriptor::Cancel(_) => {
                user_data = 0;
                println!("Don't yet know how to cancel. NEed to teach iou");
                //sqe.prep_cancel(to_remove as u64);
            }
            UringOpDescriptor::Write(ptr, len, pos) => {
                let buf = std::slice::from_raw_parts(ptr, len);
                sqe.prep_write(op.fd, buf, pos);
            }
            UringOpDescriptor::Read(ptr, len, pos) => {
                let buf = std::slice::from_raw_parts_mut(ptr, len);
                sqe.prep_read(op.fd, buf, pos);
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
            UringOpDescriptor::Timeout(micros) => {
                let d = Duration::from_micros(micros);
                let timeout: _ = uring_sys::__kernel_timespec {
                    tv_sec: d.as_secs() as _,
                    tv_nsec: d.subsec_nanos() as _,
                };
                sqe.prep_timeout(&timeout);
            }
            UringOpDescriptor::TimeoutRemove(timer) => {
                sqe.prep_timeout_remove(timer as _);
            }
            UringOpDescriptor::Close => {
                sqe.prep_close(op.fd);
            }
            UringOpDescriptor::ReadFixed(pos, len) => {
                let buf = buffer_allocation(len).expect("Buffer allocation failed");
                //let slabidx = buf.slabidx;

                //sqe.prep_read_fixed(op.fd, buf.as_mut_bytes(), pos, slabidx);
                sqe.prep_read(op.fd, buf.as_mut_bytes(), pos);
                let source = &mut *(op.user_data as *mut Source);
                source.source_type = SourceType::DmaRead(Some(buf));
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

trait UringCommon {
    fn submission_queue(&mut self) -> &mut VecDeque<UringDescriptor>;
    fn submit_sqes(&mut self) -> io::Result<usize>;
    fn submit_one_event(&mut self) -> Option<()>;
    fn consume_one_event(&mut self, wakers: &mut Vec<Waker>) -> Option<()>;
    fn name(&self) -> &'static str;

    fn add_to_submission_queue(&mut self, source: &Source, descriptor: UringOpDescriptor) {
        self.submission_queue().push_back(UringDescriptor {
            args: descriptor,
            fd: source.raw,
            user_data: source as *const Source as _,
        });
    }

    fn consume_submission_queue(&mut self) -> io::Result<usize> {
        loop {
            if let None = self.submit_one_event() {
                break;
            }
        }

        self.submit_sqes()
    }

    fn consume_completion_queue(&mut self, wakers: &mut Vec<Waker>) -> usize {
        let mut completed: usize = 0;
        loop {
            if let None = self.consume_one_event(wakers) {
                break;
            }
            completed += 1;
        }
        completed
    }
}

struct PollRing {
    ring: iou::IoUring,
    submission_queue: VecDeque<UringDescriptor>,
    submitted: u64,
    completed: u64,
    //buffers  : Rc<Vec<Rc<UringSlab>>>,
}

impl PollRing {
    fn new(size: usize) -> io::Result<Self> {
        let ring = iou::IoUring::new_with_flags(size as _, iou::SetupFlags::IOPOLL)?;

        /*
        let mut bufvec = Vec::with_capacity(8);
        for (idx,sz) in [1, 4, 8, 16, 256].iter().enumerate() {
            bufvec.push(UringSlab::new(sz << 10, idx));
        }

        let registry : Vec<IoSlice<'_>> = bufvec
            .iter_mut()
            .map(|b| b.as_io_slice())
            .collect();

        ring.registrar().register_buffers(&registry)?;

        let mut buffers = Vec::with_capacity(8);
        for b in bufvec {
            buffers.push(Rc::new(b));
        }
        */

        Ok(PollRing {
            submitted: 0,
            completed: 0,
            ring,
            submission_queue: VecDeque::with_capacity(size * 4),
            //   buffers: Rc::new(buffers),
        })
    }

    fn can_sleep(&self) -> bool {
        return self.submitted == self.completed;
    }

    pub(crate) fn alloc_dma_buffer(&mut self, size: usize) -> DmaBuffer {
        /* FIXME: uring buffers need more work
        let arena = self.buffers[self.buffers.len() -1].clone();
        arena.alloc_buffer(size)
            .expect("There are no buffers available. Because we only allocate (readers, at least) at the very last minute, this should not have happened. And yet it did. Life sucks")
         */
        PosixDmaBuffer::new(size).expect("Buffer allocation failed")
    }
}

impl Drop for PollRing {
    fn drop(&mut self) {
        match self.ring.registrar().unregister_buffers() {
            Err(x) => eprintln!("Failed to unregister buffers!: {:?}", x),
            Ok(_) => {}
        }
    }
}

impl UringCommon for PollRing {
    fn name(&self) -> &'static str {
        "poll"
    }

    fn submission_queue(&mut self) -> &mut VecDeque<UringDescriptor> {
        &mut self.submission_queue
    }

    fn submit_sqes(&mut self) -> io::Result<usize> {
        if self.submitted != self.completed {
            return self.ring.submit_sqes();
        }
        Ok(0)
    }

    fn consume_one_event(&mut self, wakers: &mut Vec<Waker>) -> Option<()> {
        process_one_event(self.ring.peek_for_cqe(), |_| None, wakers).and_then(|x| {
            self.completed += 1;
            Some(x)
        })
    }

    fn submit_one_event(&mut self) -> Option<()> {
        if self.submission_queue.is_empty() {
            return None;
        }

        //let buffers = self.buffers.clone();
        if let Some(mut sqe) = self.ring.next_sqe() {
            self.submitted += 1;
            let op = self.submission_queue.pop_front().unwrap();
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

struct SleepableRing {
    ring: iou::IoUring,
    submission_queue: VecDeque<UringDescriptor>,
    name: &'static str,
}

impl SleepableRing {
    fn new(size: usize, name: &'static str) -> io::Result<Self> {
        Ok(SleepableRing {
            //     ring: iou::IoUring::new_with_flags(size as _, iou::SetupFlags::IOPOLL)?,
            ring: iou::IoUring::new(size as _)?,
            submission_queue: VecDeque::with_capacity(size * 4),
            name,
        })
    }

    fn ring_fd(&self) -> RawFd {
        self.ring.raw().ring_fd
    }

    pub(crate) fn rearm_preempt_timer(
        &mut self,
        source: &mut Pin<Box<Source>>,
        d: Duration,
    ) -> io::Result<()> {
        let src = source.as_ref().as_ptr() as *const Source;

        match source.source_type {
            SourceType::Timeout(false) => {} // not armed, do nothing
            SourceType::Timeout(true) => {
                // armed, need to cancel first
                let op_remove = UringOpDescriptor::TimeoutRemove(src);
                source
                    .as_mut()
                    .update_source_type(SourceType::Timeout(false));

                self.submission_queue().push_front(UringDescriptor {
                    args: op_remove,
                    fd: -1,
                    user_data: 0,
                });
                loop {
                    if self.consume_submission_queue().is_ok() {
                        break;
                    }
                }
            }
            _ => panic!("Unexpected source type when linking rings"),
        }

        if d.as_secs() != u64::MAX {
            let op = UringOpDescriptor::Timeout(d.as_micros().try_into().unwrap());
            source
                .as_mut()
                .update_source_type(SourceType::Timeout(true));

            // This assumes SQEs will be processed in the order they are
            // seen. Because remove does not do anything asynchronously
            // and is processed inline there is no need to link sqes.
            self.submission_queue().push_front(UringDescriptor {
                args: op,
                fd: -1,
                user_data: src as _,
            });
            // No need to submit, the next ring enter will submit for us. Because
            // we just flushed and we got put in front of the queue we should get a SQE.
            // Still it would be nice to verify if we did.
        }
        Ok(())
    }

    fn sleep(&mut self, link: &mut Pin<Box<Source>>) -> io::Result<usize> {
        match link.source_type {
            SourceType::LinkRings(true) => {} // nothing to do
            SourceType::LinkRings(false) => {
                if let Some(mut sqe) = self.ring.next_sqe() {
                    link.as_mut()
                        .update_source_type(SourceType::LinkRings(true));

                    let op = UringDescriptor {
                        fd: link.as_ref().raw,
                        user_data: link.as_ref().as_ptr() as *const Source as u64,
                        args: UringOpDescriptor::PollAdd(common_flags() | read_flags()),
                    };
                    fill_sqe(&mut sqe, &op, |_| None);
                }
            }
            _ => panic!("Unexpected source type when linking rings"),
        }

        self.ring.submit_sqes_and_wait(1)
    }
}

fn process_one_event<F>(
    cqe: Option<iou::CompletionQueueEvent>,
    try_process: F,
    wakers: &mut Vec<Waker>,
) -> Option<()>
where
    F: FnOnce(&mut Source) -> Option<()>,
{
    if let Some(value) = cqe {
        // No user data is POLL_REMOVE or CANCEL, we won't process.
        if value.user_data() == 0 {
            return Some(());
        }

        let source = unsafe {
            let s = value.user_data() as *mut Source;
            &mut *s
        };

        if let None = try_process(source) {
            let mut w = source.wakers.borrow_mut();
            w.result = Some(value.result());
            wakers.append(&mut w.waiters);
        }
        return Some(());
    }
    None
}

impl UringCommon for SleepableRing {
    fn name(&self) -> &'static str {
        self.name
    }

    fn submission_queue(&mut self) -> &mut VecDeque<UringDescriptor> {
        &mut self.submission_queue
    }

    fn submit_sqes(&mut self) -> io::Result<usize> {
        self.ring.submit_sqes()
    }

    fn consume_one_event(&mut self, wakers: &mut Vec<Waker>) -> Option<()> {
        process_one_event(
            self.ring.peek_for_cqe(),
            |source| match source.source_type {
                SourceType::LinkRings(true) => {
                    source.source_type = SourceType::LinkRings(false);
                    Some(())
                }
                SourceType::LinkRings(false) => {
                    panic!("Impossible to have an event firing like this");
                }
                SourceType::Timeout(true) => {
                    source.source_type = SourceType::Timeout(false);
                    Some(())
                }
                // This is actually possible: when the request is cancelled
                // the original source does complete, and the cancellation
                // would have marked us as false. Just ignore it.
                SourceType::Timeout(false) => None,
                _ => None,
            },
            wakers,
        )
    }

    fn submit_one_event(&mut self) -> Option<()> {
        if self.submission_queue.is_empty() {
            return None;
        }

        if let Some(mut sqe) = self.ring.next_sqe() {
            let op = self.submission_queue.pop_front().unwrap();
            fill_sqe(&mut sqe, &op, |_| None);
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
    link_rings_src: RefCell<Pin<Box<Source>>>,
    timeout_src: RefCell<Pin<Box<Source>>>,
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

macro_rules! flush_rings {
    ($( $ring:expr ),+ ) => {{
        $(
            $ring.consume_submission_queue()?;
        )*
        let ret : io::Result<()> = Ok(());
        ret
    }}
}

macro_rules! queue_request_into_ring {
    ($ring:expr, $source:ident, $op:expr) => {{
        $ring.borrow_mut().add_to_submission_queue($source, $op)
    }};
}

macro_rules! queue_storage_io_request {
    ($self:expr, $source:ident, $op:expr) => {{
        queue_request_into_ring!($self.poll_ring, $source, $op)
    }};
}

macro_rules! queue_standard_request {
    ($self:expr, $source:ident, $op:expr) => {{
        match $source.io_requirements.latency_req {
            Latency::NotImportant => queue_request_into_ring!($self.main_ring, $source, $op),
            Latency::Matters(_) => queue_request_into_ring!($self.latency_ring, $source, $op),
        }
    }};
}

impl Reactor {
    pub(crate) fn new() -> io::Result<Reactor> {
        let main_ring = SleepableRing::new(128, "main")?;
        let latency_ring = SleepableRing::new(128, "latency")?;
        let link_fd = latency_ring.ring_fd();

        Ok(Reactor {
            main_ring: RefCell::new(main_ring),
            latency_ring: RefCell::new(latency_ring),
            poll_ring: RefCell::new(PollRing::new(128)?),
            link_rings_src: RefCell::new(Source::new(
                IoRequirements::default(),
                link_fd,
                SourceType::LinkRings(false),
            )),
            timeout_src: RefCell::new(Source::new(
                IoRequirements::default(),
                -1,
                SourceType::Timeout(false),
            )),
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

        queue_standard_request!(self, source, UringOpDescriptor::PollAdd(flags));
    }

    pub(crate) fn write_dma(&self, source: &Source, buf: &DmaBuffer, pos: u64) {
        //        let op = UringOpDescriptor::WriteFixed(buf.as_ptr() as *const u8, buf.len(), pos, buf.slabidx);
        let op = UringOpDescriptor::WriteFixed(buf.as_ptr() as *const u8, buf.len(), pos, 0);
        queue_storage_io_request!(self, source, op);
    }

    pub(crate) fn read_dma(&self, source: &Source, pos: u64, size: usize) {
        let op = UringOpDescriptor::ReadFixed(pos, size);
        queue_storage_io_request!(self, source, op);
    }

    pub(crate) fn fdatasync(&self, source: &Source) {
        queue_standard_request!(self, source, UringOpDescriptor::FDataSync);
    }

    pub(crate) fn fallocate(&self, source: &Source, offset: u64, size: u64, flags: libc::c_int) {
        let op = UringOpDescriptor::Fallocate(offset, size, flags);
        queue_standard_request!(self, source, op);
    }

    pub(crate) fn close(&self, source: &Source) {
        let op = UringOpDescriptor::Close;
        queue_standard_request!(self, source, op);
    }

    pub(crate) fn statx(&self, source: &Source) {
        let op = match &source.source_type {
            SourceType::Statx(path, buf) => {
                let path = path.as_c_str().as_ptr();
                let buf = buf.as_ptr();
                UringOpDescriptor::Statx(path as _, buf)
            }
            _ => panic!("Unexpected source for statx operation"),
        };
        queue_standard_request!(self, source, op);
    }

    pub(crate) fn open_at(&self, source: &Source, flags: libc::c_int, mode: libc::c_int) {
        let pathptr = match &source.source_type {
            SourceType::Open(cstring) => cstring.as_c_str().as_ptr(),
            _ => panic!("Wrong source type!"),
        };
        let op = UringOpDescriptor::Open(pathptr as _, flags, mode as _);
        queue_standard_request!(self, source, op);
    }

    pub(crate) fn insert(&self, fd: RawFd) -> io::Result<()> {
        add_flag(fd, libc::O_NONBLOCK)
    }

    /*
    pub(crate) fn cancel_io(&self, source : &Source) {
        let source_ptr = source as *const Source;
        let op = match source.source_type {
            sys::SourceType::PollableFd => UringOpDescriptor::PollRemove(source_ptr as _),
            _ => UringOpDescriptor::Cancel(source_ptr as _),
        };
        self.add_to_submission_queue(source, op);
    }
    */

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
    ) -> io::Result<usize> {
        let mut poll_ring = self.poll_ring.borrow_mut();
        let mut main_ring = self.main_ring.borrow_mut();
        let mut lat_ring = self.latency_ring.borrow_mut();

        let mut should_sleep = match timeout {
            None => true,
            Some(dur) => {
                let mut src = self.timeout_src.borrow_mut();
                lat_ring.rearm_preempt_timer(&mut src, dur)?;
                false
            }
        };
        flush_rings!(main_ring, lat_ring, poll_ring)?;
        should_sleep &= poll_ring.can_sleep();

        let mut completed = 0;
        if should_sleep {
            completed += consume_rings!(into wakers; lat_ring, poll_ring, main_ring);
            if completed == 0 {
                self.link_rings_and_sleep(&mut main_ring)?;
            }
        }

        completed += consume_rings!(into wakers; lat_ring, poll_ring, main_ring);
        // A Note about need_preempt:
        //
        // If in the last call to consume_rings! some events completed, the tail and
        // head would have moved to match. So it does not matter that events were
        // generated after we registered the timer: since we consumed them here,
        // need_preempt() should be false at this point. As soon as the next event
        // in the preempt ring completes, though, then it will be true.
        Ok(completed)
    }

    pub(crate) fn preempt_pointers(&self) -> (*const u32, *const u32) {
        let mut lat_ring = self.latency_ring.borrow_mut();
        let cq = &lat_ring.ring.raw_mut().cq;
        (cq.khead, cq.ktail)
    }

    pub(crate) fn notify(&self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for Reactor {
    fn drop(&mut self) {}
}
