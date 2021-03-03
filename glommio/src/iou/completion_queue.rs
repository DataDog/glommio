use std::fmt;
use std::io;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ptr::{self, NonNull};

use super::{resultify, CQEs, CQEsBlocking, IoUring, CQE};
use crate::uring_sys;

/// The queue of completed IO events.
///
/// Each element is a [`CQE`](crate::cqe::CQE).
///
/// Completion does not imply success. Completed events may be [timeouts](crate::cqe::CQE::is_iou_timeout).
pub struct CompletionQueue<'ring> {
    pub(crate) ring: NonNull<uring_sys::io_uring>,
    _marker: PhantomData<&'ring mut IoUring>,
}

impl<'ring> CompletionQueue<'ring> {
    pub(crate) fn new(ring: &'ring IoUring) -> CompletionQueue<'ring> {
        CompletionQueue {
            ring: NonNull::from(&ring.ring),
            _marker: PhantomData,
        }
    }

    /// Returns the next CQE if any are available.
    pub fn peek_for_cqe(&mut self) -> Option<CQE> {
        unsafe {
            let mut cqe = MaybeUninit::uninit();
            uring_sys::io_uring_peek_cqe(self.ring.as_ptr(), cqe.as_mut_ptr());
            let cqe = cqe.assume_init();
            if !cqe.is_null() {
                Some(CQE::new(self.ring, &mut *cqe))
            } else {
                None
            }
        }
    }

    /// Returns the next CQE, blocking the thread until one is ready if necessary.
    pub fn wait_for_cqe(&mut self) -> io::Result<CQE> {
        self.wait_for_cqes(1)
    }

    #[inline(always)]
    pub(crate) fn wait_for_cqes(&mut self, count: u32) -> io::Result<CQE> {
        let ring = self.ring;
        self.wait_inner(count).map(|cqe| CQE::new(ring, cqe))
    }

    /// Block the thread until at least `count` CQEs are ready.
    ///
    /// These CQEs can be processed using `peek_for_cqe` or the `cqes` iterator.
    pub fn wait(&mut self, count: u32) -> io::Result<()> {
        self.wait_inner(count).map(|_| ())
    }

    #[inline(always)]
    fn wait_inner(&mut self, count: u32) -> io::Result<&mut uring_sys::io_uring_cqe> {
        unsafe {
            let mut cqe = MaybeUninit::uninit();

            resultify(uring_sys::io_uring_wait_cqes(
                self.ring.as_ptr(),
                cqe.as_mut_ptr(),
                count as _,
                ptr::null(),
                ptr::null(),
            ))?;

            Ok(&mut *cqe.assume_init())
        }
    }

    /// Returns an iterator of ready CQEs.
    ///
    /// When there are no CQEs ready to process, the iterator will end. It will never
    /// block the thread to wait for CQEs to be completed.
    pub fn cqes(&mut self) -> CQEs<'_> {
        CQEs::new(self.ring)
    }

    /// Returns an iterator of ready CQEs, blocking when there are none ready.
    ///
    /// This iterator never ends. Whenever there are no CQEs ready, it will block
    /// the thread until at least `wait_for` CQEs are ready.
    pub fn cqes_blocking(&mut self, wait_for: u32) -> CQEsBlocking<'_> {
        CQEsBlocking::new(self.ring, wait_for)
    }

    pub fn ready(&self) -> u32 {
        unsafe { uring_sys::io_uring_cq_ready(self.ring.as_ptr()) }
    }

    pub fn eventfd_enabled(&self) -> bool {
        unsafe { uring_sys::io_uring_cq_eventfd_enabled(self.ring.as_ptr()) }
    }

    pub fn eventfd_toggle(&mut self, enabled: bool) -> io::Result<()> {
        resultify(unsafe { uring_sys::io_uring_cq_eventfd_toggle(self.ring.as_ptr(), enabled) })?;
        Ok(())
    }
}

impl fmt::Debug for CompletionQueue<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let fd = unsafe { self.ring.as_ref().ring_fd };
        f.debug_struct(std::any::type_name::<Self>())
            .field("fd", &fd)
            .finish()
    }
}

unsafe impl<'ring> Send for CompletionQueue<'ring> {}
unsafe impl<'ring> Sync for CompletionQueue<'ring> {}
