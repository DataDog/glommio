// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
//! Scipio - asynchronous thread per core applications in Rust.
//!
//! Makes heavy use of io_uring so this is Linux-only. 5.8 or
//! newer is recommended.
//!
//! This library provides abstractions for timers, file I/O and
//! networking plus support for multiple-queues and an internal
//! scheduler. All without using threads.
//!
//! # Examples
//!
//! Connect to `example.com:80`, or time out after 10 seconds.
//!
//! ```
//! use scipio::{Async, LocalExecutor};
//! use scipio::timer::Timer;
//! use futures_lite::{future::FutureExt, io};
//!
//! use std::net::{TcpStream, ToSocketAddrs};
//! use std::time::Duration;
//!
//! let local_ex = LocalExecutor::make_default();
//! local_ex.run(async {
//!     let addr = "::80".to_socket_addrs()?.next().unwrap();
//!
//!     let stream = Async::<TcpStream>::connect(addr).or(async {
//!         Timer::new(Duration::from_secs(10)).await;
//!         Err(io::ErrorKind::TimedOut.into())
//!     })
//!     .await?;
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
mod iou;
mod uring_sys;
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
mod semaphore;
mod shares;
pub mod timer;

pub use crate::executor::{
    ExecutorStats, LocalExecutor, LocalExecutorBuilder, QueueNotFoundError, Task, TaskQueueHandle,
    TaskQueueStats,
};
pub use crate::networking::*;
pub use crate::pollable::Async;
pub use crate::semaphore::Semaphore;
pub use crate::shares::{Shares, SharesManager};
pub use enclose::enclose;
pub use scopeguard::defer;

/// Local is an ergonomic way to access the local executor.
/// The local is executed through a Task type, but the Task type has a type
/// parameter consisting of the return type of the future encapsulated by this
/// task.
///
/// However for associated functions without a self parameter, like local() and
/// local_into(), the type is always () and Rust is not able to elide.
///
/// Writing Task::<()>::function() works, but it is not very ergonomic.
pub type Local = Task<()>;

/// An attribute of a TaskQueue, passed during its creation.
///
/// This tells the executor whether or not tasks in this class are latency
/// sensitive. Latency sensitive tasks will be placed in their own I/O ring,
/// and tasks in background classes can cooperatively preempt themselves in
/// the faces of pending events for latency classes.
#[derive(Clone, Copy, Debug)]
pub enum Latency {
    /// Tasks marked as Latency::Matters will cooperatively signal to other tasks that the should
    /// preempt often
    Matters(Duration),

    /// Tasks marked as Latency::NotImportant will not signal to other tasks that the should
    /// preempt often
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
