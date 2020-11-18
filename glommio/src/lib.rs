// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020
// Datadog, Inc.
//
//! # Glommio - asynchronous thread per core applications in Rust.
//!
//! ## Attention
//!
//! This crate was previously named Scipio but was renamed after a trademark dispute. We are
//! removing this message soon but it is now here to avoid confusion.
//!
//! ## What is Glommio
//!
//! Glommio is a library providing a safe Rust interface for asynchronous, thread-local I/O, based
//! on the linux `io_uring` interface and Rust's `async` support. Glommio also provides support for
//! pinning threads to CPUs, allowing thread-per-core applications in Rust.
//!
//! This library depends on linux's `io_uring` interface, so this is Linux-only, with a kernel
//! version 5.8 or newer recommended.
//!
//! This library provides abstractions for timers, file I/O and networking plus support for
//! multiple-queues and an internal scheduler, all without using helper threads.
//!
//! ### Rust `async`
//!
//! Using Glommio is not hard if you are familiar with rust async. All you have to do is:
//!
//! ```
//!     use glommio::LocalExecutorBuilder;
//!     LocalExecutorBuilder::new().spawn(|| async move {
//!         // your code here
//!     }).unwrap();
//! ```
//!
//! ### Pinned threads
//!
//! Although pinned threads are not required for use of glommio, by creating N executors and binding
//! each to a specific CPU one can use this crate to implement a thread-per-core system where
//! context switches essentially never happen, allowing much higher efficiency.
//!
//! You can easily bind an executor to a CPU by adjusting the LocalExecutorBuilder in the example
//! above:
//!
//! ```
//!     /// This will now never leave CPU 0
//!     use glommio::LocalExecutorBuilder;
//!     LocalExecutorBuilder::new().pin_to_cpu(0).spawn(|| async move {
//!         // your code here
//!     }).unwrap();
//! ```
//!
//! Note that you can only have one executor per thread, so if you need more executors, you will
//! have to create more threads. A more ergonomic interface for that is planned but not yet
//! available.
//!
//! ### Scheduling
//!
//! For a Thread-per-core system to work well, it is paramount that some form of scheduling can
//! happen within the thread. Traditional applications use many threads to divide the many aspects
//! of its workload and rely on the operating system and runtime to schedule these threads fairly
//! and switch between these as necessary. For a thread-per-core system, each thread must handle its
//! own scheduling at the application level.
//!
//! Glommio provides extensive abstractions for handling scheduling, allowing multiple tasks to
//! proceed on the same thread. Task scheduling can be handled broadly through static shares, or
//! more dynamically through the use of controllers:
//!
//! ```
//!     use glommio::{Local, LocalExecutorBuilder, Shares, Latency};
//!
//!     LocalExecutorBuilder::new().pin_to_cpu(0).spawn(|| async move {
//!         let tq1 = Local::create_task_queue(Shares::Static(2), Latency::NotImportant, "test1");
//!         let tq2 = Local::create_task_queue(Shares::Static(1), Latency::NotImportant, "test2");
//!         let t1 = Local::local_into(async move {
//!             // your code here
//!         }, tq1).unwrap().detach();
//!         let t2 = Local::local_into(async move {
//!             // your code here
//!         }, tq2).unwrap().detach();
//!
//!         t1.await;
//!         t2.await;
//!     }).unwrap();
//! ```
//!
//! This example creates two task queues: `tq1` has 2 shares, `tq2` has 1 share. This means that if
//! both want to use the CPU to its maximum, `tq1` will have `1/3` of the CPU time `(1 / (1 + 2))`
//! and `tq2` will have `2/3` of the CPU time. Those shares are dynamic and can be changed at any
//! time. Notice that this scheduling method doesn't prevent either `tq1` no `tq2` from using 100%
//! of CPU time at times in which they are the only task queue running: the shares are only
//! considered when multiple queues need to run.
//!
//! ## Controlled processes
//!
//! Glommio ships with embedded controllers. You can read more about them in the
//! [Controllers](controllers) module documentation. Controllers allow one to automatically adjust
//! the scheduler shares to control how fast a particular process should happen given a
//! user-provided criteria.
//!
//! For a real-life application of such technology I recommend reading [this
//! post](https://www.scylladb.com/2018/06/12/scylla-leverages-control-theory/) from Glauber.
//!
//! ## Prior work
//!
//! This work is heavily inspired (with some code respectfully imported) by the great work by
//! Stjepan Glavina, in particular the following crates:
//!
//! * [async-io](https://github.com/stjepang/async-io)
//! * [async-task](https://github.com/stjepang/async-task)
//! * [async-executor](https://github.com/stjepang/async-executor)
//! * [multitask](https://github.com/stjepang/async-multitask)
//!
//! Aside from Stjepan's work, this is also inspired greatly by the [Seastar](http://seastar.io)
//! Framework for C++ that powers I/O intensive systems that are pushing the performance envelope,
//! like [ScyllaDB](https://www.scylladb.com/).
//!
//! ## Why is this its own crate?
//!
//! Cooperative Thread-per-core is a very specific programming model. Because only one task is
//! executing per thread, the programmer never needs any locking to be held. Atomic operations are
//! therefore rare, delegated to only a handful of corner case tasks.
//!
//! As atomic operations are costlier than their non-atomic counterparts, this improves efficiency
//! by itself. However it comes with the added benefits that context switches are virtually
//! non-existent (they only occur for kernel threads and interrupts) and no time is ever wasted in
//! waiting on locks.
//!
//! ## Why is this a single monolith instead of many crates
//!
//! Take as an example the [async-io](https://github.com/stjepang/async-io) crate. It has `park()`
//! and `unpark()` methods. One can `park()` the current executor, and a helper thread will unpark
//! it. This allows one to effectively use that crate with very little need for anything else for
//! the simpler cases. Combined with synchronization primitives like `Condvar`, and other
//! thread-pool based future crates, it excels in conjunction with others but it is useful on its
//! own.
//!
//! Now contrast that to the equivalent bits in this crate: once you `park()` the thread, you can't
//! unpark it. I/O never gets dispatched without explicit calling into the reactor, which makes for
//! a very weird programming model and it is very hard to integrate with the outside world since
//! most external I/O related crates have threads that sooner or later will require `Send + Sync`.
//!
//! A single crate is a way to minimize friction.
//!
//! ## `io_uring`
//!
//! This crate depends heavily on Linux's `io_uring`. The reactor will register 3 rings per CPU:
//!
//!  * *Main ring*: The main ring, as its name implies, is where most operations will be placed.
//!    Once the reactor is parked, it only returns if the main ring has events to report.
//!
//!  * *Latency ring*: Operations that are latency sensitive can be put in the latency ring. The
//!    crate has a function called `yield_if_needed()` that efficiently checks if there are events
//!    pending in the latency ring. Because this crate uses `cooperative` programming, tasks run
//!    until they either complete or decide to yield, which means they can run for a very long time
//!    before tasks that are latency sensitive have a chance to run. Every time you fire a
//!    long-running operation (usually a loop) it is good practice to check `yield_if_needed()`
//!    periodically (for example after x iterations of the loop). In particular, a when a new
//!    priority class is registered, one can specify if it contains latency sensitive tasks or not.
//!    And if the queue is marked as latency sensitive, the Latency enum takes a duration parameter
//!    that determines for how long other tasks can run even if there are no external events (by
//!    registering a timer with the io_uring). If no runnable tasks in the system are latency
//!    sensitive, this timer is not registered. Because `io_uring` allows for polling in the ring
//!    file descriptor, it is safe to `park()` even if work is present in the latency ring: before
//!    going to sleep, the latency ring's file descriptor is registered with the main ring and any
//!    events it sees will also wake up the main ring.
//!
//!  * *Poll ring*: Read and write operations on NVMe devices are put in the poll ring. The poll
//!    ring does not rely on interrupts so the system has to keep constantly polling if there is any
//!    pending work. By not relying on interrupts we can be even more efficient with I/O in high
//!    IOPS scenarios
//!
//! ## Before using Glommio
//!
//! Please note Glommio requires at least 512 KiB of locked memory for `io_uring` to work. You can
//! increase the `memlock` resource limit (rlimit) as follows:
//!
//! ```sh
//! $ vi /etc/security/limits.conf
//! *    hard    memlock        512
//! *    soft    memlock        512
//! ```
//!
//! To make the new limits effective, you need to login to the machine again. You can verify that
//! the limits are updated by running the following:
//!
//! ```sh
//! $ ulimit -l
//! 512
//! ```
//!
//! ## Current limitations
//!
//! Due to our immediate needs which are a lot narrower, we make the following design assumptions:
//!
//!  - NVMe. While other storage types may work, the general assumptions made in here are based on
//!    the characteristics of NVMe storage. This allows us to use io uring's poll ring for reads and
//!    writes which are interrupt free. This also assumes that one is running either `XFS` or `Ext4`
//!    (an assumption that Seastar also makes).
//!
//!  - A corollary to the above is that the CPUs are likely to be the bottleneck, so this crate has
//!    a CPU scheduler but lacks an I/O scheduler. That, however, would be a welcome addition.
//!
//!  - A recent kernel is no impediment, as long as a fully functional I/O uring is present. In
//!    fact, we require a kernel so recent that it doesn't event exist: operations like `mkdir,
//!    ftruncate`, etc which are not present in today's (5.8) `io_uring` are simply synchronous and
//!    we'll live with the pain in the hopes that Linux will eventually add support for them.
//!
//! ## Missing features
//!
//! There are many. In particular:
//!
//! * Memory allocator: memory allocation is a big source of contention for thread per core systems.
//!   A shard-aware allocator would be crucial for achieving good performance in allocation-heavy
//!   workloads.
//!
//! * As mentioned, an I/O Scheduler.
//!
//! * The networking code uses `poll + rw`. This is essentially so we could get started sooner by
//!   reusing code from [async-io](https://github.com/stjepang/async-io) but we really should be
//!   using uring's native interface for that
//!
//! * Visibility: the crate exposes no metrics on its internals, and that should change ASAP.
//!
//!
//! ## Examples
//!
//! Connect to `example.com:80`, or time out after 10 seconds:
//!
//! ```
//! use glommio::{Async, LocalExecutor};
//! use glommio::timer::Timer;
//! use futures_lite::{future::FutureExt, io};
//!
//! use std::net::{TcpStream, ToSocketAddrs};
//! use std::time::Duration;
//!
//! let local_ex = LocalExecutor::make_default();
//! local_ex.run(async {
//!     let addr = "::80".to_socket_addrs()?.next().unwrap();
//!
//!     let timeout = async {
//!         Timer::new(Duration::from_secs(10)).await;
//!         Err(io::ErrorKind::TimedOut.into())
//!     };
//!     let stream = Async::<TcpStream>::connect(addr).or(timeout).await?;
//!
//!     // Read or write from stream
//!
//!     std::io::Result::Ok(())
//! });
//! ```
#![warn(missing_docs, missing_debug_implementations, rust_2018_idioms)]
#[macro_use]
extern crate nix;
extern crate alloc;
#[macro_use]
extern crate lazy_static;
#[macro_use(defer)]
extern crate scopeguard;

use crate::parking::Reactor;
use std::fmt::Debug;
use std::time::Duration;

mod free_list;
mod parking;
mod sys;
pub mod task;

#[cfg(test)]
macro_rules! test_executor {
    ($( $fut:expr ),+ ) => {
    use crate::executor::{LocalExecutor, Task};
    use futures::future::join_all;

    let local_ex = LocalExecutor::make_default();
    local_ex.run(async move {
        let mut joins = Vec::new();
        $(
            joins.push(Task::local($fut));
        )*
        join_all(joins).await;
    });
    }
}

// Wait for a variable to acquire a specific value.
// The variable is expected to be a Rc<RefCell>
//
// Alternatively it is possible to pass a timeout in seconds
// (through an Instant object)
//
// Updates to the variable gating the condition can be done (if convenient)
// through update_cond!() (below)
//
// Mostly useful for tests.
#[cfg(test)]
macro_rules! wait_on_cond {
    ($var:expr, $val:expr) => {
        loop {
            if *($var.borrow()) == $val {
                break;
            }
            Task::<()>::later().await;
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
            Task::<()>::later().await;
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

pub mod channels;
pub mod controllers;
mod error;
mod executor;
pub mod io;
mod multitask;
mod networking;
mod pollable;
mod shares;
pub mod sync;
pub mod timer;

pub use crate::executor::{
    ExecutorStats, LocalExecutor, LocalExecutorBuilder, QueueNotFoundError, Task, TaskQueueHandle,
    TaskQueueStats,
};
pub use crate::networking::*;
pub use crate::pollable::Async;
pub use crate::shares::{Shares, SharesManager};
pub use enclose::enclose;
pub use scopeguard::defer;

/// Provides common imports that almost all Glommio applications will need
pub mod prelude {
    pub use crate::{Latency, Local, LocalExecutor, LocalExecutorBuilder, Shares, TaskQueueHandle};
}

/// Local is an ergonomic way to access the local executor.
/// The local is executed through a Task type, but the Task type has a type
/// parameter consisting of the return type of the future encapsulated by this
/// task.
///
/// However for associated functions without a self parameter, like `local()` and
/// `local_into()`, the type is always `()` and Rust is not able to elide.
///
/// Writing `Task::<()>::function()` works, but it is not very ergonomic.
pub type Local = Task<()>;

/// An attribute of a [`TaskQueue`], passed during its creation.
///
/// This tells the executor whether or not tasks in this class are latency
/// sensitive. Latency sensitive tasks will be placed in their own I/O ring,
/// and tasks in background classes can cooperatively preempt themselves in
/// the faces of pending events for latency classes.
///
/// [`TaskQueue`]: struct.TaskQueueHandle.html
#[derive(Clone, Copy, Debug)]
pub enum Latency {
    /// Tasks marked as `Latency::Matters` will cooperatively signal to other
    /// tasks that they should preempt often
    Matters(Duration),

    /// Tasks marked as `Latency::NotImportant` will not signal to other tasks
    /// that they should preempt often
    NotImportant,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct IoRequirements {
    latency_req: Latency,
    io_handle: usize,
}

impl Default for IoRequirements {
    fn default() -> Self {
        Self {
            latency_req: Latency::NotImportant,
            io_handle: 0,
        }
    }
}

impl IoRequirements {
    fn new(latency: Latency, handle: usize) -> Self {
        Self {
            latency_req: latency,
            io_handle: handle,
        }
    }
}
