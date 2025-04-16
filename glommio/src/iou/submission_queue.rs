use std::{fmt, io, marker::PhantomData, ptr::NonNull, time::Duration};

use super::{resultify, IoUring, SQEs, SQE};
use crate::uring_sys;

/// The queue of pending IO events.
///
/// Each element is a [`SQE`](crate::sqe::SQE).
/// By default, events are processed in parallel after being submitted.
/// You can modify this behavior for specific events using event
/// [`SubmissionFlags`](crate::sqe::SubmissionFlags).
///
/// # Examples
/// Consider a read event that depends on a successful write beforehand.
///
/// We reify this relationship by using `IO_LINK` to link these events.
pub struct SubmissionQueue<'ring> {
    ring: NonNull<uring_sys::io_uring>,
    _marker: PhantomData<&'ring mut IoUring>,
}

impl<'ring> SubmissionQueue<'ring> {
    pub(crate) fn new(ring: &'ring IoUring) -> SubmissionQueue<'ring> {
        SubmissionQueue {
            ring: NonNull::from(&ring.ring),
            _marker: PhantomData,
        }
    }

    /// Returns new [`SQE`s](crate::sqe::SQE) until the queue size is reached.
    /// After that, will return `None`.
    pub fn prepare_sqe(&mut self) -> Option<SQE<'_>> {
        unsafe {
            let sqe = uring_sys::io_uring_get_sqe(self.ring.as_mut());
            if !sqe.is_null() {
                uring_sys::io_uring_prep_nop(sqe);
                Some(SQE::new(&mut *sqe))
            } else {
                None
            }
        }
    }

    /// Returns the next `count` [`SQE`]s which can be prepared to submit as an
    /// iterator.
    ///
    /// See the [`SQEs`] type for more information about how these multiple SQEs
    /// can be used.
    pub fn prepare_sqes(&mut self, count: u32) -> Option<SQEs<'_>> {
        unsafe {
            if self.space_left() >= count {
                Some(SQEs::new(self.ring.as_mut(), count))
            } else {
                None
            }
        }
    }

    /// Submit all events in the queue. Returns the number of submitted events.
    ///
    /// If this function encounters any IO errors an
    /// [`io::Error`](std::io::Result) variant is returned.
    pub fn submit(&mut self) -> io::Result<u32> {
        resultify(unsafe { uring_sys::io_uring_submit(self.ring.as_ptr()) })
    }

    pub fn submit_and_wait(&mut self, wait_for: u32) -> io::Result<u32> {
        resultify(unsafe { uring_sys::io_uring_submit_and_wait(self.ring.as_ptr(), wait_for as _) })
    }

    pub fn submit_and_wait_with_timeout(
        &mut self,
        wait_for: u32,
        duration: Duration,
    ) -> io::Result<u32> {
        let ts = uring_sys::__kernel_timespec {
            tv_sec: duration.as_secs() as _,
            tv_nsec: duration.subsec_nanos() as _,
        };

        loop {
            if let Some(mut sqe) = self.prepare_sqe() {
                sqe.clear();
                unsafe {
                    sqe.prep_timeout(&ts, 0, crate::iou::sqe::TimeoutFlags::empty());
                    sqe.set_user_data(uring_sys::LIBURING_UDATA_TIMEOUT);
                    return resultify(uring_sys::io_uring_submit_and_wait(
                        self.ring.as_ptr(),
                        wait_for as _,
                    ));
                }
            }

            self.submit()?;
        }
    }

    pub fn ready(&self) -> u32 {
        unsafe { uring_sys::io_uring_sq_ready(self.ring.as_ptr()) }
    }

    pub fn space_left(&self) -> u32 {
        unsafe { uring_sys::io_uring_sq_space_left(self.ring.as_ptr()) }
    }
}

impl fmt::Debug for SubmissionQueue<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let fd = unsafe { self.ring.as_ref().ring_fd };
        f.debug_struct(std::any::type_name::<Self>())
            .field("fd", &fd)
            .finish()
    }
}

unsafe impl Send for SubmissionQueue<'_> {}
unsafe impl Sync for SubmissionQueue<'_> {}
