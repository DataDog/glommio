// code imported from https://github.com/jeehoonkang/membarrier-rs (seems unmaintained)
#[allow(unused_macros)]
macro_rules! fatal_assert {
    ($cond:expr) => {
        if !$cond {
            #[allow(unused_unsafe)]
            unsafe {
                libc::abort();
            }
        }
    };
}

use core::sync::atomic;

/// A choice between three strategies for process-wide barrier on Linux.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Strategy {
    /// Use the `membarrier` system call.
    Membarrier,
    /// Use the `mprotect`-based trick.
    Mprotect,
    /// Use `SeqCst` fences.
    Fallback,
}

lazy_static! {
    /// The right strategy to use on the current machine.
    static ref STRATEGY: Strategy = {
        if syscall::is_supported() {
            Strategy::Membarrier
        } else if mprotect::is_supported() {
            Strategy::Mprotect
        } else {
            Strategy::Fallback
        }
    };
}

mod syscall {
    /// Commands for the membarrier system call.
    ///
    /// # Caveat
    ///
    /// We're defining it here because, unfortunately, the `libc` crate
    /// currently doesn't expose `membarrier_cmd` for us. You can find the
    /// numbers in the [Linux source code](https://github.com/torvalds/linux/blob/master/include/uapi/linux/membarrier.h).
    ///
    /// This enum should really be `#[repr(libc::c_int)]`, but Rust currently
    /// doesn't allow it.
    #[repr(i32)]
    #[allow(dead_code, non_camel_case_types)]
    enum membarrier_cmd {
        MEMBARRIER_CMD_QUERY = 0,
        MEMBARRIER_CMD_GLOBAL = (1 << 0),
        MEMBARRIER_CMD_GLOBAL_EXPEDITED = (1 << 1),
        MEMBARRIER_CMD_REGISTER_GLOBAL_EXPEDITED = (1 << 2),
        MEMBARRIER_CMD_PRIVATE_EXPEDITED = (1 << 3),
        MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED = (1 << 4),
        MEMBARRIER_CMD_PRIVATE_EXPEDITED_SYNC_CORE = (1 << 5),
        MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED_SYNC_CORE = (1 << 6),
    }

    /// Call the `sys_membarrier` system call.
    #[inline]
    fn sys_membarrier(cmd: membarrier_cmd) -> libc::c_long {
        unsafe { libc::syscall(libc::SYS_membarrier, cmd as libc::c_int, 0 as libc::c_int) }
    }

    /// Returns `true` if the `sys_membarrier` call is available.
    pub fn is_supported() -> bool {
        // Queries which membarrier commands are supported. Checks if private expedited
        // membarrier is supported.
        let ret = sys_membarrier(membarrier_cmd::MEMBARRIER_CMD_QUERY);
        if ret < 0
            || ret & membarrier_cmd::MEMBARRIER_CMD_PRIVATE_EXPEDITED as libc::c_long == 0
            || ret & membarrier_cmd::MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED as libc::c_long == 0
        {
            return false;
        }

        // Registers the current process as a user of private expedited membarrier.
        if sys_membarrier(membarrier_cmd::MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED) < 0 {
            return false;
        }

        true
    }

    /// Executes a heavy `sys_membarrier`-based barrier.
    #[inline]
    pub fn barrier() {
        fatal_assert!(sys_membarrier(membarrier_cmd::MEMBARRIER_CMD_PRIVATE_EXPEDITED) >= 0);
    }
}

mod mprotect {
    use core::{cell::UnsafeCell, mem, ptr, sync::atomic};

    struct Barrier {
        lock: UnsafeCell<libc::pthread_mutex_t>,
        page: u64,
        page_size: libc::size_t,
    }

    unsafe impl Sync for Barrier {}

    impl Barrier {
        /// Issues a process-wide barrier by changing access protections of a
        /// single mmap-ed page. This method is not as fast as the
        /// `sys_membarrier()` call, but works very similarly.
        #[inline]
        fn barrier(&self) {
            let page = self.page as *mut libc::c_void;

            unsafe {
                // Lock the mutex.
                fatal_assert!(libc::pthread_mutex_lock(self.lock.get()) == 0);

                // Set the page access protections to read + write.
                fatal_assert!(
                    libc::mprotect(page, self.page_size, libc::PROT_READ | libc::PROT_WRITE,) == 0
                );

                // Ensure that the page is dirty before we change the protection so that we
                // prevent the OS from skipping the global TLB flush.
                let atomic_usize = &*(page as *const atomic::AtomicUsize);
                atomic_usize.fetch_add(1, atomic::Ordering::SeqCst);

                // Set the page access protections to none.
                //
                // Changing a page protection from read + write to none causes the OS to issue
                // an interrupt to flush TLBs on all processors. This also results in flushing
                // the processor buffers.
                fatal_assert!(libc::mprotect(page, self.page_size, libc::PROT_NONE) == 0);

                // Unlock the mutex.
                fatal_assert!(libc::pthread_mutex_unlock(self.lock.get()) == 0);
            }
        }
    }

    lazy_static! {
        /// An alternative solution to `sys_membarrier` that works on older Linux kernels and
        /// x86/x86-64 systems.
        static ref BARRIER: Barrier = {
            unsafe {
                // Find out the page size on the current system.
                let page_size = libc::sysconf(libc::_SC_PAGESIZE);
                fatal_assert!(page_size > 0);
                let page_size = page_size as libc::size_t;

                // Create a dummy page.
                let page = libc::mmap(
                    ptr::null_mut(),
                    page_size,
                    libc::PROT_NONE,
                    libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                    -1 as libc::c_int,
                    0 as libc::off_t,
                );
                fatal_assert!(page != libc::MAP_FAILED);
                fatal_assert!(page as libc::size_t % page_size == 0);

                // Locking the page ensures that it stays in memory during the two mprotect
                // calls in `Barrier::barrier()`. If the page was unmapped between those calls,
                // they would not have the expected effect of generating IPI.
                libc::mlock(page, page_size as libc::size_t);

                // Initialize the mutex.
                let lock = UnsafeCell::new(libc::PTHREAD_MUTEX_INITIALIZER);
                let mut attr = mem::MaybeUninit::<libc::pthread_mutexattr_t>::uninit();
                fatal_assert!(libc::pthread_mutexattr_init(attr.as_mut_ptr()) == 0);
                let mut attr = attr.assume_init();
                fatal_assert!(
                    libc::pthread_mutexattr_settype(&mut attr, libc::PTHREAD_MUTEX_NORMAL) == 0
                );
                fatal_assert!(libc::pthread_mutex_init(lock.get(), &attr) == 0);
                fatal_assert!(libc::pthread_mutexattr_destroy(&mut attr) == 0);

                let page = page as u64;

                Barrier { lock, page, page_size }
            }
        };
    }

    /// Returns `true` if the `mprotect`-based trick is supported.
    pub fn is_supported() -> bool {
        cfg!(target_arch = "x86") || cfg!(target_arch = "x86_64")
    }

    /// Executes a heavy `mprotect`-based barrier.
    #[inline]
    pub fn barrier() {
        BARRIER.barrier();
    }
}

/// Issues a light memory barrier for fast path.
///
/// It issues a compiler fence, which disallows compiler optimizations across
/// itself. It incurs basically no costs in run-time.
#[inline]
#[allow(dead_code)]
pub(crate) fn light() {
    use self::Strategy::*;
    match *STRATEGY {
        Membarrier | Mprotect => atomic::compiler_fence(atomic::Ordering::SeqCst),
        Fallback => atomic::fence(atomic::Ordering::SeqCst),
    }
}

/// Issues a heavy memory barrier for slow path.
///
/// It issues a private expedited membarrier using the `sys_membarrier()` system
/// call, if supported; otherwise, it falls back to `mprotect()`-based
/// process-wide memory barrier.
#[inline]
#[allow(dead_code)]
pub(crate) fn heavy() {
    use self::Strategy::*;
    match *STRATEGY {
        Membarrier => syscall::barrier(),
        Mprotect => mprotect::barrier(),
        Fallback => atomic::fence(atomic::Ordering::SeqCst),
    }
}

/// The membarrier strategy is expensive to initialize. Expose that initialization so
/// that [crate::executor::early_init] can call it (it also gets intentionally called
/// by [crate::executor::LocalExecutor::new] in case the user didn't).
///
/// I believe the cost of registration got worse sometime after Linux 6.6 because tests
/// in my project that do a sleep on a timer as the first thing started taking >30ms
/// after upgrading to 6.8 whereas before they were mostly succeeding < 30ms (it was
/// my hacky attempt at dealing with setting a timeout on how long a timer could take
/// before I dug into the problem once 6.8 made it unbearable & waiting up to 100ms
/// for a 10ms test seemed silly & was hard to write assertions around).
#[inline]
pub(crate) fn initialize_strategy() {
    let _ = *STRATEGY;
}
