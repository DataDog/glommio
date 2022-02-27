// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020
// Datadog, Inc.
//
//! # Glommio - asynchronous thread per core applications in Rust.
//!
//! ## What is Glommio
//!
//! Glommio is a library providing a safe Rust interface for asynchronous,
//! thread-local I/O, based on the linux `io_uring` interface and Rust's `async`
//! support. Glommio also provides support for pinning threads to CPUs, allowing
//! thread-per-core applications in Rust.
//!
//! This library depends on linux's `io_uring` interface, so this is Linux-only,
//! with a kernel version 5.8 or newer recommended.
//!
//! This library provides abstractions for timers, file I/O and networking plus
//! support for multiple-queues and an internal scheduler, all without using
//! helper threads.
//!
//! A more detailed exposition of Glommio's architecture is [available in this
//! blog post](https://www.datadoghq.com/blog/engineering/introducing-glommio/)
//!
//! ### Rust `async`
//!
//! Using Glommio is not hard if you are familiar with rust async. All you have
//! to do is:
//!
//! ```
//! use glommio::LocalExecutorBuilder;
//! LocalExecutorBuilder::default()
//!     .spawn(|| async move {
//!         // your code here
//!     })
//!     .unwrap();
//! ```
//!
//! ### Pinned threads
//!
//! Although pinned threads are not required for use of glommio, by creating N
//! executors and binding each to a specific CPU one can use this crate to
//! implement a thread-per-core system where context switches essentially never
//! happen, allowing much higher efficiency.
//!
//! You can easily bind an executor to a CPU by adjusting the
//! LocalExecutorBuilder in the example above:
//!
//! ```
//! /// This will now never leave CPU 0
//! use glommio::{LocalExecutorBuilder, Placement};
//! LocalExecutorBuilder::new(Placement::Fixed(0))
//!     .spawn(|| async move {
//!         // your code here
//!     })
//!     .unwrap();
//! ```
//!
//! Note that you can only have one executor per thread, so if you need more
//! executors, you will have to create more threads. A more ergonomic interface
//! for that is planned but not yet available.
//!
//! ### Scheduling
//!
//! For a Thread-per-core system to work well, it is paramount that some form of
//! scheduling can happen within the thread. Traditional applications use many
//! threads to divide the many aspects of its workload and rely on the operating
//! system and runtime to schedule these threads fairly and switch between these
//! as necessary. For a thread-per-core system, each thread must handle its
//! own scheduling at the application level.
//!
//! Glommio provides extensive abstractions for handling scheduling, allowing
//! multiple tasks to proceed on the same thread. Task scheduling can be handled
//! broadly through static shares, or more dynamically through the use of
//! controllers:
//!
//! ```
//! use glommio::{executor, Latency, LocalExecutorBuilder, Placement, Shares};
//!
//! LocalExecutorBuilder::new(Placement::Fixed(0))
//!     .spawn(|| async move {
//!         let tq1 =
//!             executor().create_task_queue(Shares::Static(2), Latency::NotImportant, "test1");
//!         let tq2 =
//!             executor().create_task_queue(Shares::Static(1), Latency::NotImportant, "test2");
//!         let t1 = glommio::spawn_local_into(
//!             async move {
//!                 // your code here
//!             },
//!             tq1,
//!         )
//!         .unwrap();
//!         let t2 = glommio::spawn_local_into(
//!             async move {
//!                 // your code here
//!             },
//!             tq2,
//!         )
//!         .unwrap();
//!
//!         t1.await;
//!         t2.await;
//!     })
//!     .unwrap();
//! ```
//!
//! This example creates two task queues: `tq1` has 2 shares, `tq2` has 1 share.
//! This means that if both want to use the CPU to its maximum, `tq1` will have
//! `1/3` of the CPU time `(1 / (1 + 2))` and `tq2` will have `2/3` of the CPU
//! time. Those shares are dynamic and can be changed at any time. Notice that
//! this scheduling method doesn't prevent either `tq1` no `tq2` from using 100%
//! of CPU time at times in which they are the only task queue running: the
//! shares are only considered when multiple queues need to run.
//!
//! ## Direct I/O
//!
//! Glommio makes Direct I/O a first-class citizen, although Buffered I/O is
//! present as well for situations where it may make sense.
//!
//! This rides the trend of devices getting faster over the years and tries to
//! bridge the software gap between fast devices, and fast storage applications.
//! You can read more about it [in this article](https://itnext.io/modern-storage-is-plenty-fast-it-is-the-apis-that-are-bad-6a68319fbc1a)
//!
//! ## Controlled processes
//!
//! Glommio ships with embedded controllers. You can read more about them in the
//! [Controllers](controllers) module documentation. Controllers allow one to
//! automatically adjust the scheduler shares to control how fast a particular
//! process should happen given a user-provided criteria.
//!
//! For a real-life application of such technology I recommend reading [this
//! post](https://www.scylladb.com/2018/06/12/scylla-leverages-control-theory/) from Glauber.
//!
//! ## Prior work
//!
//! This work is heavily inspired (with some code respectfully imported) by the
//! great work by Stjepan Glavina, in particular the following crates:
//!
//! * [async-io](https://github.com/stjepang/async-io)
//! * [async-task](https://github.com/stjepang/async-task)
//! * [async-executor](https://github.com/stjepang/async-executor)
//!
//! Aside from Stjepan's work, this is also inspired greatly by the [Seastar](http://seastar.io)
//! Framework for C++ that powers I/O intensive systems that are pushing the
//! performance envelope, like [ScyllaDB](https://www.scylladb.com/).
//!
//! ## Why is this its own crate?
//!
//! Cooperative Thread-per-core is a very specific programming model. Because
//! only one task is executing per thread, the programmer never needs any
//! locking to be held. Atomic operations are therefore rare, delegated to only
//! a handful of corner case tasks.
//!
//! As atomic operations are costlier than their non-atomic counterparts, this
//! improves efficiency by itself. However, it comes with the added benefits
//! that context switches are virtually non-existent (they only occur for kernel
//! threads and interrupts) and no time is ever wasted in waiting on locks.
//!
//! ## Why is this a single monolith instead of many crates
//!
//! Take as an example the [async-io](https://github.com/stjepang/async-io) crate. It has `park()`
//! and `unpark()` methods. One can `park()` the current executor, and a helper
//! thread will unpark it. This allows one to effectively use that crate with
//! very little need for anything else for the simpler cases. Combined with
//! synchronization primitives like `Condvar`, and other thread-pool based
//! future crates, it excels in conjunction with others, but it is useful on its
//! own.
//!
//! Now contrast that to the equivalent bits in this crate: once you `park()`
//! the thread, you can't unpark it. I/O never gets dispatched without explicit
//! calling into the reactor, which makes for a very weird programming model,
//! and it is very hard to integrate with the outside world since most external
//! I/O related crates have threads that sooner or later will require [`Send`] +
//! [`Sync`].
//!
//! A single crate is a way to minimize friction.
//!
//! ## `io_uring`
//!
//! This crate depends heavily on Linux's `io_uring`. The reactor will register
//! 3 rings per CPU:
//!
//!  * *Main ring*: The main ring, as its name implies, is where most operations
//!    will be placed. Once the reactor is parked, it only returns if the main
//!    ring has events to report.
//!
//!  * *Latency ring*: Operations that are latency sensitive can be put in the
//!    latency ring. The crate has a function called `yield_if_needed()` that
//!    efficiently checks if there are events pending in the latency ring.
//!    Because this crate uses `cooperative` programming, tasks run until they
//!    either complete or decide to yield, which means they can run for a very
//!    long time before tasks that are latency sensitive have a chance to run.
//!    Every time you fire a long-running operation (usually a loop) it is good
//!    practice to check [`yield_if_needed()`] periodically (for example after x
//!    iterations of the loop). In particular, a when a new priority class is
//!    registered, one can specify if it contains latency sensitive tasks or
//!    not. And if the queue is marked as latency sensitive, the Latency enum
//!    takes a duration parameter that determines for how long other tasks can
//!    run even if there are no external events (by registering a timer with the
//!    io_uring). If no runnable tasks in the system are latency sensitive, this
//!    timer is not registered. Because `io_uring` allows for polling in the
//!    ring file descriptor, it is safe to `park()` even if work is present in
//!    the latency ring: before going to sleep, the latency ring's file
//!    descriptor is registered with the main ring and any events it sees will
//!    also wake up the main ring.
//!
//!  * *Poll ring*: Read and write operations on NVMe devices are put in the
//!    poll ring. The poll ring does not rely on interrupts so the system has to
//!    keep constantly polling if there is any pending work. By not relying on
//!    interrupts we can be even more efficient with I/O in high IOPS scenarios
//!
//! ## Before using Glommio
//!
//! Please note Glommio requires at least 512 KiB of locked memory for
//! `io_uring` to work. You can increase the `memlock` resource limit (rlimit)
//! as follows:
//!
//! ```sh
//! $ vi /etc/security/limits.conf
//! *    hard    memlock        512
//! *    soft    memlock        512
//! ```
//!
//! To make the new limits effective, you need to log in to the machine again.
//! You can verify that the limits are updated by running the following:
//!
//! ```sh
//! $ ulimit -l
//! 512
//! ```
//!
//! ## Current limitations
//!
//! Due to our immediate needs which are a lot narrower, we make the following
//! design assumptions:
//!
//!  - NVMe. While other storage types may work, the general assumptions made in
//!    here are based on the characteristics of NVMe storage. This allows us to
//!    use io uring's poll ring for reads and writes which are interrupt free.
//!    This also assumes that one is running either `XFS` or `Ext4` (an
//!    assumption that `Seastar` also makes).
//!
//!  - A corollary to the above is that the CPUs are likely to be the
//!    bottleneck, so this crate has a CPU scheduler but lacks an I/O scheduler.
//!    That, however, would be a welcome addition.
//!
//!  - A recent (at least 5.8) kernel is no impediment, as long as a fully
//!    functional I/O uring is present. In fact, we require a kernel so recent
//!    that it doesn't even exist: operations like `mkdir, ftruncate`, etc.
//!    which are not present in today's (5.8) `io_uring` are simply synchronous,
//!    and we'll live with the pain in the hopes that Linux will eventually add
//!    support for them.
//!
//! ## Missing features
//!
//! There are many. In particular:
//!
//! * Memory allocator: memory allocation is a big source of contention for
//!   thread per core systems. A shard-aware allocator would be crucial for
//!   achieving good performance in allocation-heavy workloads.
//!
//! * As mentioned, an I/O Scheduler.
//!
//! ## Examples
//!
//! Connect to `example.com:80`, or time out after 10 seconds:
//!
//! ```
//! use futures_lite::{future::FutureExt, io};
//! use glommio::{net::TcpStream, timer::Timer, LocalExecutor};
//!
//! use std::time::Duration;
//!
//! let local_ex = LocalExecutor::default();
//! local_ex.run(async {
//!     let timeout = async {
//!         Timer::new(Duration::from_secs(10)).await;
//!         Err(io::Error::new(io::ErrorKind::TimedOut, "").into())
//!     };
//!     let stream = TcpStream::connect("::80").or(timeout).await?;
//!
//!     // Read or write from stream
//!
//!     std::io::Result::Ok(())
//! });
//! ```
#![warn(missing_docs, missing_debug_implementations, rust_2018_idioms)]
#![cfg_attr(doc, deny(rustdoc::broken_intra_doc_links))]
#![cfg_attr(feature = "native-tls", feature(thread_local))]

#[macro_use]
extern crate nix;
extern crate alloc;
#[macro_use]
extern crate lazy_static;
#[macro_use(defer)]
extern crate scopeguard;

/// Call [`Waker::wake()`] and log to `error` if panicked.
macro_rules! wake {
    ($waker:expr $(,)?) => {
        use log::error;

        if let Err(x) = std::panic::catch_unwind(|| $waker.wake()) {
            error!("Panic while calling waker! {:?}", x);
        }
    };
}

mod free_list;

#[allow(clippy::redundant_slicing)]
#[allow(dead_code)]
#[allow(clippy::upper_case_acronyms)]
mod iou;
mod parking;
mod reactor;
mod sys;
pub mod task;

#[allow(dead_code)]
#[allow(clippy::upper_case_acronyms)]
mod uring_sys;

#[cfg(feature = "bench")]
#[doc(hidden)]
pub mod nop;

/// Unwraps a Result to Poll<T>: if error returns right away.
///
/// Usage is similar to `future_lite::ready!`
macro_rules! poll_err {
    ($e:expr $(,)?) => {
        match $e {
            Ok(t) => t,
            Err(x) => return std::task::Poll::Ready(Err(x)),
        }
    };
}

/// Unwraps an Option to Poll<T>: if Some returns right away.
///
/// Usage is similar to `future_lite::ready!`
#[allow(unused)]
macro_rules! poll_some {
    ($e:expr $(,)?) => {
        match $e {
            Some(t) => return std::task::Poll::Ready(t),
            None => {}
        }
    };
}

#[macro_export]
/// Converts a Nix error into a native ErrorKind
macro_rules! to_io_error {
    ($e:expr) => {
        match $e {
            nix::errno::Errno::EACCES => io::Error::from(io::ErrorKind::PermissionDenied),
            nix::errno::Errno::EADDRINUSE => io::Error::from(io::ErrorKind::AddrInUse),
            nix::errno::Errno::EADDRNOTAVAIL => io::Error::from(io::ErrorKind::AddrNotAvailable),
            nix::errno::Errno::EAGAIN => io::Error::from(io::ErrorKind::WouldBlock),
            nix::errno::Errno::ECONNABORTED => io::Error::from(io::ErrorKind::ConnectionAborted),
            nix::errno::Errno::ECONNREFUSED => io::Error::from(io::ErrorKind::ConnectionRefused),
            nix::errno::Errno::ECONNRESET => io::Error::from(io::ErrorKind::ConnectionReset),
            nix::errno::Errno::EINTR => io::Error::from(io::ErrorKind::Interrupted),
            nix::errno::Errno::EINVAL => io::Error::from(io::ErrorKind::InvalidInput),
            nix::errno::Errno::ENAMETOOLONG => io::Error::from(io::ErrorKind::InvalidInput),
            nix::errno::Errno::ENOENT => io::Error::from(io::ErrorKind::NotFound),
            nix::errno::Errno::ENOTCONN => io::Error::from(io::ErrorKind::NotConnected),
            nix::errno::Errno::ENOTEMPTY => io::Error::from(io::ErrorKind::AlreadyExists),
            nix::errno::Errno::EPERM => io::Error::from(io::ErrorKind::PermissionDenied),
            nix::errno::Errno::ETIMEDOUT => io::Error::from(io::ErrorKind::TimedOut),
            _ => io::Error::from(io::ErrorKind::Other),
        }
    };
}

#[cfg(test)]
macro_rules! test_executor {
    ($( $fut:expr ),+ ) => {
    use futures::future::join_all;

    let local_ex = crate::executor::LocalExecutorBuilder::new(crate::executor::Placement::Unbound)
            .record_io_latencies(true)
            .make()
            .unwrap();
    local_ex.run(async move {
        let mut joins = Vec::new();
        $(
            joins.push(crate::spawn_local($fut));
        )*
        join_all(joins).await;
    });
    }
}

/// Wait for a variable to acquire a specific value.
/// The variable is expected to be a Rc<RefCell>
///
/// Alternatively it is possible to pass a timeout in seconds
/// (through an Instant object)
///
/// Updates to the variable gating the condition can be done (if convenient)
/// through update_cond!() (below)
///
/// Mostly useful for tests.
#[cfg(test)]
macro_rules! wait_on_cond {
    ($var:expr, $val:expr) => {
        loop {
            if *($var.borrow()) == $val {
                break;
            }
            crate::executor().yield_task_queue_now().await;
        }
    };
    ($var:expr, $val:expr, $instantval:expr) => {
        let start = Instant::now();
        loop {
            if *($var.borrow()) == $val {
                break;
            }

            if start.elapsed().as_secs() > $instantval {
                panic!("test timed out");
            }
            crate::executor().yield_task_queue_now().await;
        }
    };
}

#[cfg(test)]
macro_rules! update_cond {
    ($cond:expr, $val:expr) => {
        *($cond.borrow_mut()) = $val;
    };
}

#[cfg(test)]
macro_rules! make_shared_var {
    ($var:expr, $( $name:ident ),+ ) => {
        let local_name = Rc::new($var);
        $( let $name = local_name.clone(); )*
    }
}

#[cfg(test)]
macro_rules! make_shared_var_mut {
    ($var:expr, $( $name:ident ),+ ) => {
        let local_name = Rc::new(RefCell::new($var));
        $( let $name = local_name.clone(); )*
    }
}

mod byte_slice_ext;
pub mod channels;
pub mod controllers;
mod error;
mod executor;
pub mod io;
pub mod net;
mod shares;
pub mod sync;
pub mod timer;

use crate::reactor::Reactor;
pub use crate::{
    byte_slice_ext::{ByteSliceExt, ByteSliceMutExt},
    error::{
        BuilderErrorKind,
        ExecutorErrorKind,
        GlommioError,
        QueueErrorKind,
        ReactorErrorKind,
        ResourceType,
        Result,
    },
    executor::{
        allocate_dma_buffer,
        allocate_dma_buffer_global,
        executor,
        spawn_local,
        spawn_local_into,
        spawn_scoped_local,
        spawn_scoped_local_into,
        stall::{DefaultStallDetectionHandler, StallDetectionHandler},
        yield_if_needed,
        CpuSet,
        ExecutorJoinHandle,
        ExecutorProxy,
        ExecutorStats,
        LocalExecutor,
        LocalExecutorBuilder,
        LocalExecutorPoolBuilder,
        Placement,
        PoolPlacement,
        PoolThreadHandles,
        ScopedTask,
        Task,
        TaskQueueHandle,
        TaskQueueStats,
    },
    shares::{Shares, SharesManager},
    sys::hardware_topology::CpuLocation,
};
pub use enclose::enclose;
pub use scopeguard::defer;
use sketches_ddsketch::DDSketch;
use std::{
    fmt::{Debug, Formatter},
    iter::Sum,
    time::Duration,
};

/// Provides common imports that almost all Glommio applications will need
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{
        error::GlommioError,
        executor,
        spawn_local,
        spawn_local_into,
        yield_if_needed,
        ByteSliceExt,
        ByteSliceMutExt,
        ExecutorProxy,
        IoStats,
        Latency,
        LocalExecutor,
        LocalExecutorBuilder,
        LocalExecutorPoolBuilder,
        Placement,
        PoolPlacement,
        PoolThreadHandles,
        RingIoStats,
        Shares,
        TaskQueueHandle,
    };
}

/// An attribute of a [`TaskQueue`], passed during its creation.
///
/// This tells the executor whether tasks in this class are latency
/// sensitive. Latency sensitive tasks will be placed in their own I/O ring,
/// and tasks in background classes can cooperatively preempt themselves in
/// the faces of pending events for latency classes.
///
/// [`TaskQueue`]: struct.TaskQueueHandle.html
#[derive(Clone, Copy, Debug)]
pub enum Latency {
    /// Tasks marked as `Latency::Matters` will cooperatively signal to other
    /// tasks that they should preempt often. The `Duration` argument
    /// contributes to the rate of preemption of the scheduler.
    Matters(Duration),

    /// Tasks marked as `Latency::NotImportant` will not signal to other tasks
    /// that they should preempt often
    NotImportant,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct IoRequirements {
    latency_req: Latency,
    _io_handle: usize,
}

impl Default for IoRequirements {
    fn default() -> Self {
        Self {
            latency_req: Latency::NotImportant,
            _io_handle: 0,
        }
    }
}

impl IoRequirements {
    fn new(latency: Latency, handle: usize) -> Self {
        Self {
            latency_req: latency,
            _io_handle: handle,
        }
    }
}

/// Stores information about IO performed in a specific ring
#[derive(Clone)]
pub struct RingIoStats {
    // Counters
    pub(crate) files_opened: u64,
    pub(crate) files_closed: u64,
    pub(crate) file_reads: u64,
    pub(crate) file_bytes_read: u64,
    pub(crate) file_buffered_reads: u64,
    pub(crate) file_buffered_bytes_read: u64,
    pub(crate) file_deduped_reads: u64,
    pub(crate) file_deduped_bytes_read: u64,
    pub(crate) file_writes: u64,
    pub(crate) file_bytes_written: u64,
    pub(crate) file_buffered_writes: u64,
    pub(crate) file_buffered_bytes_written: u64,

    // Distributions
    pub(crate) pre_reactor_io_scheduler_latency_us: sketches_ddsketch::DDSketch,
    pub(crate) io_latency_us: sketches_ddsketch::DDSketch,
    pub(crate) post_reactor_io_scheduler_latency_us: sketches_ddsketch::DDSketch,
}

impl Default for RingIoStats {
    fn default() -> Self {
        Self {
            files_opened: 0,
            files_closed: 0,
            file_reads: 0,
            file_bytes_read: 0,
            file_buffered_reads: 0,
            file_buffered_bytes_read: 0,
            file_deduped_reads: 0,
            file_deduped_bytes_read: 0,
            file_writes: 0,
            file_bytes_written: 0,
            file_buffered_writes: 0,
            file_buffered_bytes_written: 0,
            pre_reactor_io_scheduler_latency_us: sketches_ddsketch::DDSketch::new(
                sketches_ddsketch::Config::new(0.01, 2048, 1.0e-9),
            ),
            io_latency_us: sketches_ddsketch::DDSketch::new(sketches_ddsketch::Config::new(
                0.01, 2048, 1.0e-9,
            )),
            post_reactor_io_scheduler_latency_us: sketches_ddsketch::DDSketch::new(
                sketches_ddsketch::Config::new(0.01, 2048, 1.0e-9),
            ),
        }
    }
}

impl Debug for RingIoStats {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RingIoStats")
            .field("files_opened", &self.files_opened)
            .field("files_closed", &self.files_closed)
            .field("file_reads", &self.file_reads)
            .field("file_bytes_read", &self.file_bytes_read)
            .field("file_buffered_reads", &self.file_buffered_reads)
            .field("file_buffered_bytes_read", &self.file_buffered_bytes_read)
            .field("file_deduped_reads", &self.file_deduped_reads)
            .field("file_deduped_bytes_read", &self.file_deduped_bytes_read)
            .field("file_writes", &self.file_writes)
            .field("file_bytes_written", &self.file_bytes_written)
            .field("file_buffered_writes", &self.file_buffered_writes)
            .field(
                "file_buffered_bytes_written",
                &self.file_buffered_bytes_written,
            )
            .finish_non_exhaustive()
    }
}

impl RingIoStats {
    /// The total amount of files opened in this executor so far.
    ///
    /// [`files_opened`] - [`files_closed`] gives the current open files count
    ///
    /// [`files_opened`]: RingIoStats::files_opened
    /// [`files_closed`]: RingIoStats::files_closed
    pub fn files_opened(&self) -> u64 {
        self.files_opened
    }

    /// The total amount of files closed in this executor so far.
    ///
    /// [`files_opened`] - [`files_closed`] gives the current open files count
    ///
    /// [`files_opened`]: RingIoStats::files_opened
    /// [`files_closed`]: RingIoStats::files_closed
    pub fn files_closed(&self) -> u64 {
        self.files_opened
    }

    /// File read IO stats
    ///
    /// Returns the number of individual read ops as well as bytes read
    pub fn file_reads(&self) -> (u64, u64) {
        (self.file_reads, self.file_bytes_read)
    }

    /// File read IO stats (deduplicated)
    ///
    /// Returns the number of reads that fed from another preexisting buffer
    pub fn file_deduped_reads(&self) -> (u64, u64) {
        (self.file_deduped_reads, self.file_deduped_bytes_read)
    }

    /// Buffered file read IO stats
    ///
    /// Returns the number of individual buffered read ops as well as bytes read
    pub fn file_buffered_reads(&self) -> (u64, u64) {
        (self.file_buffered_reads, self.file_buffered_bytes_read)
    }

    /// File write IO stats
    ///
    /// Returns the number of individual write ops as well as bytes written
    pub fn file_writes(&self) -> (u64, u64) {
        (self.file_writes, self.file_bytes_written)
    }

    /// Buffered file write IO stats
    ///
    /// Returns the number of individual buffered write ops as well as bytes
    /// written
    pub fn file_buffered_writes(&self) -> (u64, u64) {
        (self.file_buffered_writes, self.file_buffered_bytes_written)
    }

    /// The pre-reactor IO scheduler latency
    ///
    /// Returns a distribution of measures tracking the time between the moment
    /// an IO operation was queued up and the moment it was submitted to the
    /// kernel
    pub fn pre_reactor_io_scheduler_latency_us(&self) -> &DDSketch {
        &self.pre_reactor_io_scheduler_latency_us
    }

    /// The IO latency
    ///
    /// Returns a distribution of measures tracking the time sources spent in
    /// the ring
    pub fn io_latency_us(&self) -> &DDSketch {
        &self.io_latency_us
    }

    /// The post-reactor IO scheduler latency
    ///
    /// Returns a distribution of measures tracking the time between the moment
    /// an IO operation was marked as fulfilled by the reactor and when the
    /// result was consumed by the application code.
    pub fn post_reactor_io_scheduler_latency_us(&self) -> &DDSketch {
        &self.post_reactor_io_scheduler_latency_us
    }
}

impl<'a> Sum<&'a RingIoStats> for RingIoStats {
    fn sum<I: Iterator<Item = &'a RingIoStats>>(iter: I) -> Self {
        iter.fold(RingIoStats::default(), |mut a, b| {
            a.files_opened += b.files_opened;
            a.files_closed += b.files_closed;
            a.file_reads += b.file_reads;
            a.file_bytes_read += b.file_bytes_read;
            a.file_buffered_reads += b.file_buffered_reads;
            a.file_buffered_bytes_read += b.file_buffered_bytes_read;
            a.file_deduped_reads += b.file_deduped_reads;
            a.file_deduped_bytes_read += b.file_deduped_bytes_read;
            a.file_writes += b.file_writes;
            a.file_bytes_written += b.file_bytes_written;
            a.file_buffered_writes += b.file_buffered_writes;
            a.file_buffered_bytes_written += b.file_buffered_bytes_written;
            a.pre_reactor_io_scheduler_latency_us
                .merge(&b.pre_reactor_io_scheduler_latency_us)
                .unwrap();
            a.io_latency_us.merge(&b.io_latency_us).unwrap();
            a.post_reactor_io_scheduler_latency_us
                .merge(&b.post_reactor_io_scheduler_latency_us)
                .unwrap();
            a
        })
    }
}

/// Stores information about IO
#[derive(Debug)]
pub struct IoStats {
    /// The IO stats of the main ring
    pub main_ring: RingIoStats,
    /// The IO stats of the latency ring
    pub latency_ring: RingIoStats,
    /// The IO stats of the poll ring
    pub poll_ring: RingIoStats,
}

impl IoStats {
    fn new(main_ring: RingIoStats, latency_ring: RingIoStats, poll_ring: RingIoStats) -> IoStats {
        IoStats {
            main_ring,
            latency_ring,
            poll_ring,
        }
    }

    /// Combine stats from all rings
    pub fn all_rings(&self) -> RingIoStats {
        [&self.main_ring, &self.latency_ring, &self.poll_ring]
            .iter()
            .copied()
            .sum()
    }
}

#[cfg(test)]
pub(crate) mod test_utils {
    use super::*;
    use nix::sys::statfs::*;
    use std::path::{Path, PathBuf};
    use tracing::{debug, error, info, trace, warn};
    use tracing_subscriber::EnvFilter;

    #[derive(Copy, Clone)]
    pub(crate) enum TestDirectoryKind {
        TempFs,
        PollMedia,
        NonPollMedia,
    }

    pub(crate) struct TestDirectory {
        pub(crate) path: PathBuf,
        pub(crate) kind: TestDirectoryKind,
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    pub(crate) fn make_test_directories(test_name: &str) -> std::vec::Vec<TestDirectory> {
        let mut vec = Vec::new();

        // Glommio currently only supports NVMe-backed volumes formatted with XFS or
        // EXT4. We therefore let the user decide what directory glommio should
        // use to host the unit tests in. For more information regarding this
        // limitation, see the README
        match std::env::var("GLOMMIO_TEST_POLLIO_ROOTDIR") {
            Err(_) => {
                eprintln!(
                    "Glommio currently only supports NVMe-backed volumes formatted with XFS or \
                     EXT4. To run poll io-related tests, please set GLOMMIO_TEST_POLLIO_ROOTDIR \
                     to a NVMe-backed directory path in your environment.\nPoll io tests will not \
                     run."
                );
            }
            Ok(path) => {
                for p in path.split(',') {
                    vec.push(make_poll_test_directory(p, test_name));
                }
            }
        };

        vec.push(make_tmp_test_directory(test_name));
        vec
    }

    pub(crate) fn make_poll_test_directory<P: AsRef<Path>>(
        path: P,
        test_name: &str,
    ) -> TestDirectory {
        let mut dir = path.as_ref().to_owned();
        std::assert!(dir.exists());

        dir.push(test_name);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        TestDirectory {
            path: dir,
            kind: TestDirectoryKind::PollMedia,
        }
    }

    pub(crate) fn make_tmp_test_directory(test_name: &str) -> TestDirectory {
        let mut dir = std::env::temp_dir();
        dir.push(test_name);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let buf = statfs(&dir).unwrap();
        let fstype = buf.filesystem_type();

        let kind = if fstype == TMPFS_MAGIC {
            TestDirectoryKind::TempFs
        } else {
            TestDirectoryKind::NonPollMedia
        };
        TestDirectory { path: dir, kind }
    }

    #[test]
    #[allow(unused_must_use)]
    fn test_tracing_init() {
        tracing_subscriber::fmt::fmt()
            .with_env_filter(EnvFilter::from_env("GLOMMIO_TRACE"))
            .try_init();

        info!("Started tracing..");
        debug!("Started tracing..");
        warn!("Started tracing..");
        trace!("Started tracing..");
        error!("Started tracing..");
    }
}
