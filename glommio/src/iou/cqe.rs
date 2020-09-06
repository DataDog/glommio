use std::io;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ptr::{self, NonNull};

use crate::iou::iou::IoUring;
use crate::uring_sys::uring_sys;

macro_rules! resultify {
    ($ret:expr) => {{
        let ret = $ret;
        match ret >= 0 {
            true => Ok(ret as _),
            false => Err(std::io::Error::from_raw_os_error(-ret)),
        }
    }};
}

/// The queue of completed IO events.
///
/// Each element is a [`CompletionQueueEvent`](crate::cqe::CompletionQueueEvent).
///
/// Completion does not imply success. Completed events may be [timeouts](crate::cqe::CompletionQueueEvent::is_timeout).
pub struct CompletionQueue<'ring> {
    ring: NonNull<uring_sys::io_uring>,
    _marker: PhantomData<&'ring mut IoUring>,
}

impl<'ring> CompletionQueue<'ring> {
    pub(crate) fn new(ring: &'ring IoUring) -> CompletionQueue<'ring> {
        CompletionQueue {
            ring: NonNull::from(&ring.ring),
            _marker: PhantomData,
        }
    }

    pub fn peek_for_cqe(&mut self) -> Option<CompletionQueueEvent> {
        unsafe {
            let mut cqe = MaybeUninit::uninit();
            let count = uring_sys::io_uring_peek_batch_cqe(self.ring.as_ptr(), cqe.as_mut_ptr(), 1);
            if count > 0 {
                Some(CompletionQueueEvent::new(
                    self.ring,
                    &mut *cqe.assume_init(),
                ))
            } else {
                None
            }
        }
    }

    pub fn wait_for_cqe(&mut self) -> io::Result<CompletionQueueEvent> {
        self.wait_for_cqes(1)
    }

    pub fn wait_for_cqes(&mut self, count: usize) -> io::Result<CompletionQueueEvent> {
        unsafe {
            let mut cqe = MaybeUninit::uninit();

            let _: i32 = resultify!(uring_sys::io_uring_wait_cqes(
                self.ring.as_ptr(),
                cqe.as_mut_ptr(),
                count as _,
                ptr::null(),
                ptr::null(),
            ))?;

            Ok(CompletionQueueEvent::new(
                self.ring,
                &mut *cqe.assume_init(),
            ))
        }
    }
}

unsafe impl<'ring> Send for CompletionQueue<'ring> {}
unsafe impl<'ring> Sync for CompletionQueue<'ring> {}

/// A completed IO event.
pub struct CompletionQueueEvent {
    user_data: u64,
    res: i32,
    _flags: u32,
}

impl CompletionQueueEvent {
    pub fn from_raw(user_data: u64, res: i32, flags: u32) -> CompletionQueueEvent {
        CompletionQueueEvent {
            user_data,
            res,
            _flags: flags,
        }
    }

    pub(crate) fn new(
        ring: NonNull<uring_sys::io_uring>,
        cqe: &mut uring_sys::io_uring_cqe,
    ) -> CompletionQueueEvent {
        let user_data = cqe.user_data;
        let res = cqe.res;
        let flags = cqe.flags;

        unsafe {
            uring_sys::io_uring_cqe_seen(ring.as_ptr(), cqe);
        }

        CompletionQueueEvent::from_raw(user_data, res, flags)
    }

    /// Check whether this event is a timeout.
    /// ```compile_fail
    /// # use iou::{IoUring, SubmissionQueueEvent, CompletionQueueEvent};
    /// # fn main() -> std::io::Result<()> {
    /// # let mut ring = IoUring::new(2)?;
    /// # let mut sqe = ring.next_sqe().unwrap();
    /// #
    /// # // make a nop for testing
    /// # unsafe { sqe.prep_nop(); }
    /// # ring.submit_sqes()?;
    /// #
    /// # let mut cq_event;
    /// cq_event = ring.wait_for_cqe()?;
    /// # // rewrite to be a fake timeout
    /// # let cq_event = CompletionQueueEvent::from_raw(uring_sys::LIBURING_UDATA_TIMEOUT, 0, 0);
    /// assert!(cq_event.is_timeout());
    /// # Ok(())
    /// # }
    /// ```
    pub fn is_timeout(&self) -> bool {
        self.user_data == uring_sys::LIBURING_UDATA_TIMEOUT
    }

    pub fn user_data(&self) -> u64 {
        self.user_data as u64
    }

    pub fn result(&self) -> io::Result<usize> {
        resultify!(self.res)
    }
}

unsafe impl Send for CompletionQueueEvent {}
unsafe impl Sync for CompletionQueueEvent {}
