//! Idiomatic Rust bindings to liburing.
//!
//! This gives users an idiomatic Rust interface for interacting with the Linux kernel's `io_uring`
//! interface for async IO. Despite being idiomatic Rust, this interface is still very low level
//! and some fundamental operations remain unsafe.
//!
//! The core entry point to the API is the `IoUring` type, which manages an `io_uring` object for
//! interfacing with the kernel. Using this, users can submit IO events and wait for their
//! completion.
//!
//! It is also possible to "split" an `IoUring` instance into its constituent components - a
//! `SubmissionQueue`, a `CompletionQueue`, and a `Registrar` - in order to operate on them
//! separately without synchronization.
//!
//! # Submitting events
//!
//! You can prepare new IO events using the `SQE` type. Once an event has been
//! prepared, the next call to submit will submit that event. Eventually, those events will
//! complete, and that a `CQE` will appear on the completion queue indicating that
//! the event is complete.
//!
//! Preparing IO events is inherently unsafe, as you must guarantee that the buffers and file
//! descriptors used for that IO are alive long enough for the kernel to perform the IO operation
//! with them.
//!
//! # Timeouts
//!
//! Some APIs allow you to time out a call into the kernel. It's important to note how this works
//! with io_uring.
//!
//! A timeout is submitted as an additional IO event which completes after the specified time.
//! Therefore when you create a timeout, all that happens is that a completion event will appear
//! after that specified time. This also means that when processing completion events, you need to
//! be prepared for the possibility that the completion represents a timeout and not a normal IO
//! event (`CQE` has a method to check for this).
use crate::uring_sys;

/// Types related to completion queue events.
pub mod cqe;
/// Types related to submission queue events.
///
/// The most important types here are [`SQE`], which represents a single submission queue event,
/// and [`SQEs`], which represents a sequence of events that can be prepared at once.
///
/// Many of the types in this module are re-exported from the `nix` crate, and are used when
/// preparing [`SQE`]s associated with specific Linux system operations.
pub mod sqe;

mod completion_queue;
mod submission_queue;

mod probe;

pub mod registrar;

use std::fmt;
use std::io;
use std::mem::{self, MaybeUninit};
use std::os::unix::io::RawFd;
use std::ptr::{self, NonNull};
use std::time::Duration;

#[doc(inline)]
pub use cqe::{CQEs, CQEsBlocking, CQE};
#[doc(inline)]
pub use sqe::{SQEs, SQE};

pub use completion_queue::CompletionQueue;
pub use submission_queue::SubmissionQueue;

pub use probe::Probe;
#[doc(inline)]
pub use registrar::{Personality, Registrar};

bitflags::bitflags! {
    /// [`IoUring`] initialization flags for advanced use cases.
    ///
    pub struct SetupFlags: u32 {
        /// Poll the IO context instead of defaulting to interrupts.
        const IOPOLL    = 1 << 0;   /* io_context is polled */
        /// Assign a kernel thread to poll the submission queue. Requires elevated privileges to set.
        const SQPOLL    = 1 << 1;   /* SQ poll thread */
        /// Force the kernel thread created with `SQPOLL` to be bound to the CPU used by the
        /// `SubmissionQueue`. Requires `SQPOLL` set.
        const SQ_AFF    = 1 << 2;   /* sq_thread_cpu is valid */

        const CQSIZE    = 1 << 3;
        const CLAMP     = 1 << 4;
        const ATTACH_WQ = 1 << 5;
    }
}

bitflags::bitflags! {
    /// Advanced features that can be enabled when setting up an [`IoUring`] instance.
    pub struct SetupFeatures: u32 {
        const SINGLE_MMAP       = 1 << 0;
        const NODROP            = 1 << 1;
        const SUBMIT_STABLE     = 1 << 2;
        const RW_CUR_POS        = 1 << 3;
        const CUR_PERSONALITY   = 1 << 4;
        const FAST_POLL         = 1 << 5;
        const POLL_32BITS       = 1 << 6;
    }
}

/// The main interface to kernel IO using `io_uring`.
///
/// `IoUring` is a high-level wrapper around an [`io_uring`](uring_sys::io_uring) object.
///
/// `IoUring`s are constructed with a requested number of ring buffer entries and possibly a set of
/// [`SetupFlags`](SetupFlags). Allocations for `IoUring` are `memlocked` and will not be paged
/// out.
///
/// `IoUring`s can either be used directly, or split into separate parts and
/// operated on without synchronization.
pub struct IoUring {
    ring: uring_sys::io_uring,
}

impl IoUring {
    /// Creates a new `IoUring` without any setup flags. `IoUring`'s created using this method will
    /// use interrupt-driven IO.
    ///
    /// The number of entries must be in the range of 1..4096 (inclusive) and
    /// it's recommended to be a power of two.
    ///
    /// The underlying `SubmissionQueue` and `CompletionQueue` will each have this number of
    /// entries.
    pub fn new(entries: u32) -> io::Result<IoUring> {
        IoUring::new_with_flags(entries, SetupFlags::empty(), SetupFeatures::empty())
    }

    /// Creates a new `IoUring` using a set of `SetupFlags` and `SetupFeatures` for advanced
    /// use cases.
    pub fn new_with_flags(
        entries: u32,
        flags: SetupFlags,
        features: SetupFeatures,
    ) -> io::Result<IoUring> {
        unsafe {
            let mut params: uring_sys::io_uring_params = mem::zeroed();
            params.flags = flags.bits();
            params.features = features.bits();
            let mut ring = MaybeUninit::uninit();
            resultify(uring_sys::io_uring_queue_init_params(
                entries as _,
                ring.as_mut_ptr(),
                &mut params,
            ))?;
            Ok(IoUring {
                ring: ring.assume_init(),
            })
        }
    }

    /// Returns the `SubmissionQueue` part of the `IoUring`.
    pub fn sq(&mut self) -> SubmissionQueue<'_> {
        SubmissionQueue::new(&*self)
    }

    /// Returns the `CompletionQueue` part of the `IoUring`.
    pub fn cq(&mut self) -> CompletionQueue<'_> {
        CompletionQueue::new(&*self)
    }

    /// Returns the `Registrar` part of the `IoUring`.
    pub fn registrar(&self) -> Registrar<'_> {
        Registrar::new(self)
    }

    /// Returns the three constituent parts of the `IoUring`.
    pub fn queues(&mut self) -> (SubmissionQueue<'_>, CompletionQueue<'_>, Registrar<'_>) {
        (
            SubmissionQueue::new(&*self),
            CompletionQueue::new(&*self),
            Registrar::new(&*self),
        )
    }

    pub fn probe(&mut self) -> io::Result<Probe> {
        Probe::for_ring(&mut self.ring)
    }

    /// Returns the next [`SQE`] which can be prepared to submit.
    pub fn prepare_sqe(&mut self) -> Option<SQE<'_>> {
        unsafe { submission_queue::prepare_sqe(&mut self.ring) }
    }

    /// Returns the next `count` [`SQE`]s which can be prepared to submit as an iterator.
    ///
    /// See the [`SQEs`] type for more information about how these multiple SQEs can be used.
    pub fn prepare_sqes(&mut self, count: u32) -> Option<SQEs<'_>> {
        unsafe { submission_queue::prepare_sqes(&mut self.ring.sq, count) }
    }

    /// Submit all prepared [`SQE`]s to the kernel.
    pub fn submit_sqes(&mut self) -> io::Result<u32> {
        self.sq().submit()
    }

    /// Submit all prepared [`SQE`]s to the kernel and wait until at least `wait_for` events have
    /// completed.
    pub fn submit_sqes_and_wait(&mut self, wait_for: u32) -> io::Result<u32> {
        self.sq().submit_and_wait(wait_for)
    }

    /// Submit all prepared [`SQE`]s to the kernel and wait until at least `wait_for` events have
    /// completed or `duration` has passed.
    pub fn submit_sqes_and_wait_with_timeout(
        &mut self,
        wait_for: u32,
        duration: Duration,
    ) -> io::Result<u32> {
        self.sq().submit_and_wait_with_timeout(wait_for, duration)
    }

    /// Peek for any [`CQE`] that is already completed, without blocking. This will consume that
    /// CQE.
    pub fn peek_for_cqe(&mut self) -> Option<CQE> {
        unsafe {
            let mut cqe = MaybeUninit::uninit();
            let count = uring_sys::io_uring_peek_batch_cqe(&mut self.ring, cqe.as_mut_ptr(), 1);

            if count > 0 {
                Some(CQE::new(NonNull::from(&self.ring), &mut *cqe.assume_init()))
            } else {
                None
            }
        }
    }

    /// Block until at least one [`CQE`] is completed. This will consume that CQE.
    pub fn wait_for_cqe(&mut self) -> io::Result<CQE> {
        let ring = NonNull::from(&self.ring);
        self.inner_wait_for_cqes(1, ptr::null())
            .map(|cqe| CQE::new(ring, cqe))
    }

    /// Block until a [`CQE`] is ready or timeout.
    pub fn wait_for_cqe_with_timeout(&mut self, duration: Duration) -> io::Result<CQE> {
        let ts = uring_sys::__kernel_timespec {
            tv_sec: duration.as_secs() as _,
            tv_nsec: duration.subsec_nanos() as _,
        };

        let ring = NonNull::from(&self.ring);
        self.inner_wait_for_cqes(1, &ts)
            .map(|cqe| CQE::new(ring, cqe))
    }

    /// Returns an iterator of [`CQE`]s which are ready from the kernel.
    pub fn cqes(&mut self) -> CQEs<'_> {
        CQEs::new(NonNull::from(&mut self.ring))
    }

    /// Returns an iterator of [`CQE`]s which will block when there are no CQEs ready. It will
    /// block until at least `count` are ready, and then continue iterating.
    ///
    /// This iterator will never be exhausted; every time it runs out of CQEs it will block the
    /// thread and wait for more to be ready.
    pub fn cqes_blocking(&mut self, count: u32) -> CQEsBlocking<'_> {
        CQEsBlocking::new(NonNull::from(&mut self.ring), count)
    }

    /// Wait until `count` [`CQE`]s are ready, without submitting any events.
    pub fn wait_for_cqes(&mut self, count: u32) -> io::Result<()> {
        self.inner_wait_for_cqes(count as _, ptr::null())
            .map(|_| ())
    }

    fn inner_wait_for_cqes(
        &mut self,
        count: u32,
        ts: *const uring_sys::__kernel_timespec,
    ) -> io::Result<&mut uring_sys::io_uring_cqe> {
        unsafe {
            let mut cqe = MaybeUninit::uninit();

            resultify(uring_sys::io_uring_wait_cqes(
                &mut self.ring,
                cqe.as_mut_ptr(),
                count,
                ts,
                ptr::null(),
            ))?;

            Ok(&mut *cqe.assume_init())
        }
    }

    pub fn raw(&self) -> &uring_sys::io_uring {
        &self.ring
    }

    pub unsafe fn raw_mut(&mut self) -> &mut uring_sys::io_uring {
        &mut self.ring
    }

    pub fn cq_ready(&mut self) -> u32 {
        self.cq().ready()
    }

    pub fn sq_ready(&mut self) -> u32 {
        self.sq().ready()
    }

    pub fn sq_space_left(&mut self) -> u32 {
        self.sq().space_left()
    }

    pub fn cq_eventfd_enabled(&mut self) -> bool {
        self.cq().eventfd_enabled()
    }

    pub fn cq_eventfd_toggle(&mut self, enabled: bool) -> io::Result<()> {
        self.cq().eventfd_toggle(enabled)
    }

    pub fn raw_fd(&self) -> RawFd {
        self.ring.ring_fd
    }
}

impl fmt::Debug for IoUring {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(std::any::type_name::<Self>())
            .field("fd", &self.ring.ring_fd)
            .finish()
    }
}

impl Drop for IoUring {
    fn drop(&mut self) {
        unsafe { uring_sys::io_uring_queue_exit(&mut self.ring) };
    }
}

unsafe impl Send for IoUring {}
unsafe impl Sync for IoUring {}

fn resultify(x: i32) -> io::Result<u32> {
    match x >= 0 {
        true => Ok(x as u32),
        false => Err(io::Error::from_raw_os_error(-x)),
    }
}

#[cfg(test)]
mod tests {
    use super::resultify;

    #[test]
    fn test_resultify() {
        let side_effect = |i, effect: &mut _| -> i32 {
            *effect += 1;
            return i;
        };

        let mut calls = 0;
        let ret = resultify(side_effect(0, &mut calls));
        assert!(match ret {
            Ok(0) => true,
            _ => false,
        });
        assert_eq!(calls, 1);

        calls = 0;
        let ret = resultify(side_effect(1, &mut calls));
        assert!(match ret {
            Ok(1) => true,
            _ => false,
        });
        assert_eq!(calls, 1);

        calls = 0;
        let ret = resultify(side_effect(-1, &mut calls));
        assert!(match ret {
            Err(e) if e.raw_os_error() == Some(1) => true,
            _ => false,
        });
        assert_eq!(calls, 1);
    }
}
