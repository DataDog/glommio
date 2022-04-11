// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
//! Async executor.
//!
//! This crate offers two kinds of executors: single-threaded and
//! multi-threaded.
//!
//! # Examples
//!
//! Run four single-threaded executors concurrently:
//!
//! ```
//! use glommio::{
//!     timer::Timer,
//!     LocalExecutor,
//!     LocalExecutorBuilder,
//!     LocalExecutorPoolBuilder,
//!     PoolPlacement,
//! };
//!
//! LocalExecutorPoolBuilder::new(PoolPlacement::Unbound(4))
//!     .on_all_shards(move || async {
//!         Timer::new(std::time::Duration::from_millis(100)).await;
//!         println!("Hello world!");
//!     })
//!     .expect("failed to spawn local executors")
//!     .join_all();
//! ```

#![warn(missing_docs, missing_debug_implementations)]

use crate::{
    error::BuilderErrorKind,
    executor::stall::StallDetector,
    io::DmaBuffer,
    parking,
    reactor,
    sys,
    task::{self, waker_fn::dummy_waker},
    GlommioError,
    IoRequirements,
    IoStats,
    Latency,
    Reactor,
    Shares,
};
use ahash::AHashMap;
use futures_lite::pin;
use latch::{Latch, LatchState};
use log::warn;
pub use placement::{CpuSet, Placement, PoolPlacement};
use std::{
    cell::RefCell,
    collections::{hash_map::Entry, BinaryHeap},
    fmt,
    future::Future,
    io,
    marker::PhantomData,
    mem::MaybeUninit,
    ops::{Deref, DerefMut},
    pin::Pin,
    rc::Rc,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    thread::{Builder, JoinHandle},
    time::{Duration, Instant},
};
use tracing::trace;

mod latch;
mod multitask;
mod placement;
pub mod stall;

pub(crate) const DEFAULT_EXECUTOR_NAME: &str = "unnamed";
pub(crate) const DEFAULT_PREEMPT_TIMER: Duration = Duration::from_millis(100);
pub(crate) const DEFAULT_IO_MEMORY: usize = 10 << 20;
pub(crate) const DEFAULT_RING_SUBMISSION_DEPTH: usize = 128;

/// Result type alias that removes the need to specify a type parameter
/// that's only valid in the channel variants of the error. Otherwise, it
/// might be confused with the error (`E`) that a result usually has in
/// the second type parameter.
type Result<T> = crate::Result<T, ()>;

#[cfg(feature = "native-tls")]
#[thread_local]
static mut LOCAL_EX: *const LocalExecutor = std::ptr::null();

#[cfg(not(feature = "native-tls"))]
scoped_tls::scoped_thread_local!(static LOCAL_EX: LocalExecutor);

/// Returns a proxy struct to the [`LocalExecutor`]
#[inline(always)]
pub fn executor() -> ExecutorProxy {
    ExecutorProxy {}
}

pub(crate) fn executor_id() -> Option<usize> {
    #[cfg(not(feature = "native-tls"))]
    {
        if LOCAL_EX.is_set() {
            Some(LOCAL_EX.with(|ex| ex.id))
        } else {
            None
        }
    }

    #[cfg(feature = "native-tls")]
    unsafe {
        LOCAL_EX.as_ref().map(|ex| ex.id)
    }
}

#[derive(Default, Debug, Copy, Clone, Eq, PartialEq, Hash)]
/// An opaque handle indicating in which queue a group of tasks will execute.
/// Tasks in the same group will execute in FIFO order but no guarantee is made
/// about ordering on different task queues.
pub struct TaskQueueHandle {
    index: usize,
}

impl TaskQueueHandle {
    /// Returns a numeric ID that uniquely identifies this Task queue
    pub fn index(&self) -> usize {
        self.index
    }
}

#[derive(Debug)]
pub(crate) struct TaskQueue {
    pub(crate) ex: Rc<multitask::LocalExecutor>,
    active: bool,
    shares: Shares,
    vruntime: u64,
    io_requirements: IoRequirements,
    name: String,
    last_adjustment: Instant,
    // for dynamic shares classes
    yielded: bool,
    stats: TaskQueueStats,
}

// Impl a custom order so we use a min-heap
impl Ord for TaskQueue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.vruntime.cmp(&self.vruntime)
    }
}

impl PartialOrd for TaskQueue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(other.vruntime.cmp(&self.vruntime))
    }
}

impl PartialEq for TaskQueue {
    fn eq(&self, other: &Self) -> bool {
        self.vruntime == other.vruntime
    }
}

impl Eq for TaskQueue {}

impl TaskQueue {
    fn new<S>(
        index: TaskQueueHandle,
        name: S,
        shares: Shares,
        ioreq: IoRequirements,
    ) -> Rc<RefCell<Self>>
    where
        S: Into<String>,
    {
        Rc::new(RefCell::new(TaskQueue {
            ex: Rc::new(multitask::LocalExecutor::new()),
            active: false,
            stats: TaskQueueStats::new(index, shares.reciprocal_shares()),
            shares,
            vruntime: 0,
            io_requirements: ioreq,
            name: name.into(),
            last_adjustment: Instant::now(),
            yielded: false,
        }))
    }

    fn is_active(&self) -> bool {
        self.active
    }

    fn get_task(&mut self) -> Option<multitask::Runnable> {
        self.ex.get_task()
    }

    fn yielded(&self) -> bool {
        self.yielded
    }

    fn prepare_to_run(&mut self, now: Instant) {
        self.yielded = false;
        if let Shares::Dynamic(bm) = &self.shares {
            if now.saturating_duration_since(self.last_adjustment) > bm.adjustment_period() {
                self.last_adjustment = now;
                self.stats.reciprocal_shares = self.shares.reciprocal_shares();
            }
        }
    }

    fn account_vruntime(&mut self, delta: Duration) -> Option<u64> {
        let delta_scaled = (self.stats.reciprocal_shares * (delta.as_nanos() as u64)) >> 12;
        self.stats.runtime += delta;
        self.stats.queue_selected += 1;
        self.active = self.ex.is_active();

        let vruntime = self.vruntime.checked_add(delta_scaled);
        if let Some(x) = vruntime {
            self.vruntime = x;
        }
        vruntime
    }
}

pub(crate) fn bind_to_cpu_set(cpus: impl IntoIterator<Item = usize>) -> Result<()> {
    let mut cpuset = nix::sched::CpuSet::new();
    for cpu in cpus {
        cpuset.set(cpu).map_err(|e| to_io_error!(e))?;
    }
    let pid = nix::unistd::Pid::from_raw(0);
    nix::sched::sched_setaffinity(pid, &cpuset).map_err(|e| Into::into(to_io_error!(e)))
}

// Dealing with references would imply getting a Rc, RefCells, and all of that
// Stats should be copied Infrequently, and if you have enough stats to fill a
// Kb with data from a single source, maybe you should rethink your life
// choices.
#[derive(Debug, Copy, Clone, Default)]
/// Allows information about the current state of this executor to be consumed
/// by applications.
pub struct ExecutorStats {
    executor_runtime: Duration,
    // total_runtime include poll_io time, exclude spin loop time
    total_runtime: Duration,
    scheduler_runs: u64,
    tasks_executed: u64,
}

impl ExecutorStats {
    fn new() -> Self {
        Self {
            executor_runtime: Duration::from_nanos(0),
            total_runtime: Duration::from_nanos(0),
            scheduler_runs: 0,
            tasks_executed: 0,
        }
    }

    /// The total amount of runtime in this executor so far.
    ///
    /// This is especially important for spinning executors, since the amount of
    /// CPU time you will see in the operating system will be a far cry from
    /// the CPU time it actually spent executing. Sleeping or Spinning are
    /// not accounted here
    pub fn executor_runtime(&self) -> Duration {
        self.executor_runtime
    }

    /// The total amount of runtime in this executor, plus poll io time
    pub fn total_runtime(&self) -> Duration {
        self.total_runtime
    }

    /// Returns the amount of times the scheduler loop was called. Glommio
    /// scheduler selects a task queue to run and runs many tasks in that
    /// task queue. This number corresponds to the amount of times was
    /// called upon to select a new queue.
    pub fn scheduler_runs(&self) -> u64 {
        self.scheduler_runs
    }

    /// Returns the amount of tasks executed in the system, over all queues.
    pub fn tasks_executed(&self) -> u64 {
        self.tasks_executed
    }
}

#[derive(Debug, Copy, Clone)]
/// Allows information about the current state of a particular task queue to be
/// consumed by applications.
pub struct TaskQueueStats {
    index: TaskQueueHandle,
    // so we can easily produce a handle
    reciprocal_shares: u64,
    queue_selected: u64,
    runtime: Duration,
}

impl TaskQueueStats {
    fn new(index: TaskQueueHandle, reciprocal_shares: u64) -> Self {
        Self {
            index,
            reciprocal_shares,
            runtime: Duration::from_nanos(0),
            queue_selected: 0,
        }
    }

    /// Returns a numeric ID that uniquely identifies this Task queue
    pub fn index(&self) -> TaskQueueHandle {
        self.index
    }

    /// Returns the current number of shares in this task queue.
    ///
    /// If the task queue is configured to use static shares this will never
    /// change. If the task queue is configured to use dynamic shares, this
    /// returns a sample of the shares values the last time the scheduler
    /// ran.
    pub fn current_shares(&self) -> usize {
        ((1u64 << 22) / self.reciprocal_shares) as usize
    }

    /// Returns the accumulated runtime this task queue had received since the
    /// beginning of its execution
    pub fn runtime(&self) -> Duration {
        self.runtime
    }

    /// Returns the number of times this queue was selected to be executed. In
    /// conjunction with the runtime, you can extract an average of the
    /// amount of time this queue tends to run for
    pub fn queue_selected(&self) -> u64 {
        self.queue_selected
    }

    pub(crate) fn take(&mut self) -> Self {
        std::mem::replace(
            self,
            Self {
                index: self.index,
                reciprocal_shares: self.reciprocal_shares,
                queue_selected: Default::default(),
                runtime: Default::default(),
            },
        )
    }
}

#[derive(Debug)]
struct ExecutorQueues {
    active_executors: BinaryHeap<Rc<RefCell<TaskQueue>>>,
    available_executors: AHashMap<usize, Rc<RefCell<TaskQueue>>>,
    active_executing: Option<Rc<RefCell<TaskQueue>>>,
    executor_index: usize,
    default_vruntime: u64,
    preempt_timer_duration: Duration,
    default_preempt_timer_duration: Duration,
    spin_before_park: Option<Duration>,
    stats: ExecutorStats,
}

impl ExecutorQueues {
    fn new(preempt_timer_duration: Duration, spin_before_park: Option<Duration>) -> Self {
        ExecutorQueues {
            active_executors: BinaryHeap::new(),
            available_executors: AHashMap::new(),
            active_executing: None,
            executor_index: 1, // 0 is the default
            default_vruntime: 0,
            preempt_timer_duration,
            default_preempt_timer_duration: preempt_timer_duration,
            spin_before_park,
            stats: ExecutorStats::new(),
        }
    }

    fn reevaluate_preempt_timer(&mut self) {
        self.preempt_timer_duration = self
            .active_executors
            .iter()
            .map(|tq| match tq.borrow().io_requirements.latency_req {
                Latency::NotImportant => self.default_preempt_timer_duration,
                Latency::Matters(d) => d,
            })
            .min()
            .unwrap_or(self.default_preempt_timer_duration)
    }

    fn maybe_activate(&mut self, queue: Rc<RefCell<TaskQueue>>) {
        let mut state = queue.borrow_mut();
        if !state.is_active() {
            state.vruntime = self.default_vruntime + 1;
            state.active = true;
            drop(state);
            self.active_executors.push(queue);
            self.reevaluate_preempt_timer();
        }
    }
}

/// A wrapper around a [`std::thread::JoinHandle`]
#[derive(Debug)]
pub struct ExecutorJoinHandle<T: Send + 'static>(JoinHandle<Result<T>>);

impl<T: Send + 'static> ExecutorJoinHandle<T> {
    /// See [`std::thread::JoinHandle::thread()`]
    #[must_use]
    pub fn thread(&self) -> &std::thread::Thread {
        self.0.thread()
    }

    /// See [`std::thread::JoinHandle::join()`]
    pub fn join(self) -> Result<T> {
        match self.0.join() {
            Err(err) => Err(GlommioError::BuilderError(BuilderErrorKind::ThreadPanic(
                err,
            ))),
            Ok(Err(err)) => Err(err),
            Ok(Ok(res)) => Ok(res),
        }
    }
}

/// A factory that can be used to configure and create a [`LocalExecutor`].
///
/// Methods can be chained on it in order to configure it.
///
/// The [`spawn`] method will take ownership of the builder and create a
/// `Result` to the [`LocalExecutor`] handle with the given configuration.
///
/// The [`LocalExecutor::default`] free function uses a Builder with default
/// configuration and unwraps its return value.
///
/// You may want to use [`LocalExecutorBuilder::spawn`] instead of
/// [`LocalExecutor::default`], when you want to recover from a failure to
/// launch a thread. The [`LocalExecutor::default`] function will panic where
/// the Builder method will return a `io::Result`.
///
/// # Examples
///
/// ```
/// use glommio::LocalExecutorBuilder;
///
/// let builder = LocalExecutorBuilder::default();
/// let ex = builder.make().unwrap();
/// ```
///
/// [`LocalExecutor`]: struct.LocalExecutor.html
///
/// [`LocalExecutor::default`]: struct.LocalExecutor.html#method.default
///
/// [`LocalExecutorBuilder::spawn`]:
/// struct.LocalExecutorBuilder.html#method.spawn
///
/// [`spawn`]: struct.LocalExecutorBuilder.html#method.spawn
#[derive(Debug)]
pub struct LocalExecutorBuilder {
    /// The placement policy for the [`LocalExecutor`] to create
    placement: Placement,
    /// Spin for duration before parking a reactor
    spin_before_park: Option<Duration>,
    /// A name for the thread-to-be (if any), for identification in panic
    /// messages
    name: String,
    /// Amount of memory to reserve for storage I/O. This will be preallocated
    /// and registered with io_uring. It is still possible to use more than
    /// that, but it will come from the standard allocator and performance
    /// will suffer. Defaults to 10 MiB.
    io_memory: usize,
    /// The depth of the IO rings to create. This influences the level of IO
    /// concurrency. A higher ring depth allows a shard to submit a
    /// greater number of IO requests to the kernel at once.
    ring_depth: usize,
    /// How often to yield to other task queues
    preempt_timer_duration: Duration,
    /// Whether to record the latencies of individual IO requests
    record_io_latencies: bool,
    /// The placement policy of the blocking thread pool
    /// Defaults to one thread using the same placement strategy as the host
    /// executor
    blocking_thread_pool_placement: PoolPlacement,
    /// Whether to detect stalls in unyielding tasks.
    /// [`stall::DefaultStallDetectionHandler`] installs a signal handler for
    /// [`nix::libc::SIGUSR1`], so is disabled by default.
    detect_stalls: Option<Box<dyn stall::StallDetectionHandler + 'static>>,
}

impl LocalExecutorBuilder {
    /// Generates the base configuration for spawning a [`LocalExecutor`], from
    /// which configuration methods can be chained.
    /// The method's only argument is the [`Placement`] policy by which the
    /// [`LocalExecutor`] is bound to the machine's hardware topology. i.e.
    /// how many and which CPUs to use.
    pub fn new(placement: Placement) -> LocalExecutorBuilder {
        LocalExecutorBuilder {
            placement: placement.clone(),
            spin_before_park: None,
            name: String::from(DEFAULT_EXECUTOR_NAME),
            io_memory: DEFAULT_IO_MEMORY,
            ring_depth: DEFAULT_RING_SUBMISSION_DEPTH,
            preempt_timer_duration: DEFAULT_PREEMPT_TIMER,
            record_io_latencies: false,
            blocking_thread_pool_placement: PoolPlacement::from(placement),
            detect_stalls: None,
        }
    }

    /// Spin for duration before parking a reactor
    #[must_use = "The builder must be built to be useful"]
    pub fn spin_before_park(mut self, spin: Duration) -> LocalExecutorBuilder {
        self.spin_before_park = Some(spin);
        self
    }

    /// Names the thread-to-be. Currently, the name is used for identification
    /// only in panic messages.
    #[must_use = "The builder must be built to be useful"]
    pub fn name(mut self, name: &str) -> LocalExecutorBuilder {
        self.name = String::from(name);
        self
    }

    /// Amount of memory to reserve for storage I/O. This will be preallocated
    /// and registered with io_uring. It is still possible to use more than
    /// that, but it will come from the standard allocator and performance
    /// will suffer.
    ///
    /// The system will always try to allocate at least 64 kiB for I/O memory,
    /// and the default is 10 MiB.
    #[must_use = "The builder must be built to be useful"]
    pub fn io_memory(mut self, io_memory: usize) -> LocalExecutorBuilder {
        self.io_memory = io_memory;
        self
    }

    /// The depth of the IO rings to create. This influences the level of IO
    /// concurrency. A higher ring depth allows a shard to submit a
    /// greater number of IO requests to the kernel at once.
    ///
    /// Values above zero are valid and the default is 128.
    #[must_use = "The builder must be built to be useful"]
    pub fn ring_depth(mut self, ring_depth: usize) -> LocalExecutorBuilder {
        assert!(ring_depth > 0, "ring depth should be strictly positive");
        self.ring_depth = ring_depth;
        self
    }

    /// How often [`need_preempt`] will return true by default.
    ///
    /// Lower values mean task queues will switch execution more often, which
    /// can help latency but harm throughput. When individual task queues
    /// are present, this value can still be dynamically lowered through the
    /// [`Latency`] setting.
    ///
    /// Default is 100ms.
    ///
    /// [`need_preempt`]: ExecutorProxy::need_preempt
    /// [`Latency`]: crate::Latency
    #[must_use = "The builder must be built to be useful"]
    pub fn preempt_timer(mut self, dur: Duration) -> LocalExecutorBuilder {
        self.preempt_timer_duration = dur;
        self
    }

    /// Whether to record the latencies of individual IO requests as part of the
    /// IO stats. Recording latency can be expensive. Disabled by default.
    #[must_use = "The builder must be built to be useful"]
    pub fn record_io_latencies(mut self, enabled: bool) -> LocalExecutorBuilder {
        self.record_io_latencies = enabled;
        self
    }

    /// The placement policy of the blocking thread pool.
    /// Defaults to one thread using the same placement strategy as the host
    /// executor.
    #[must_use = "The builder must be built to be useful"]
    pub fn blocking_thread_pool_placement(
        mut self,
        placement: PoolPlacement,
    ) -> LocalExecutorBuilder {
        self.blocking_thread_pool_placement = placement;
        self
    }

    /// Whether to detect stalls in unyielding tasks.
    /// [`stall::DefaultStallDetectionHandler`] installs a signal handler for
    /// [`nix::libc::SIGUSR1`], so is disabled by default.
    /// # Examples
    ///
    /// ```
    /// use glommio::{DefaultStallDetectionHandler, LocalExecutorBuilder};
    ///
    /// let local_ex = LocalExecutorBuilder::default()
    ///     .detect_stalls(Some(Box::new(DefaultStallDetectionHandler {})))
    ///     .make()
    ///     .unwrap();
    /// ```
    #[must_use = "The builder must be built to be useful"]
    pub fn detect_stalls(
        mut self,
        handler: Option<Box<dyn stall::StallDetectionHandler + 'static>>,
    ) -> Self {
        self.detect_stalls = handler;
        self
    }

    /// Make a new [`LocalExecutor`] by taking ownership of the Builder, and
    /// returns a [`Result`](crate::Result) to the executor.
    /// # Examples
    ///
    /// ```
    /// use glommio::LocalExecutorBuilder;
    ///
    /// let local_ex = LocalExecutorBuilder::default().make().unwrap();
    /// ```
    pub fn make(self) -> Result<LocalExecutor> {
        let notifier = sys::new_sleep_notifier()?;
        let mut cpu_set_gen = placement::CpuSetGenerator::one(self.placement)?;
        let mut le = LocalExecutor::new(
            notifier,
            cpu_set_gen.next().cpu_binding(),
            LocalExecutorConfig {
                io_memory: self.io_memory,
                ring_depth: self.ring_depth,
                preempt_timer: self.preempt_timer_duration,
                record_io_latencies: self.record_io_latencies,
                spin_before_park: self.spin_before_park,
                thread_pool_placement: self.blocking_thread_pool_placement,
                detect_stalls: self.detect_stalls,
            },
        )?;
        le.init();
        Ok(le)
    }

    /// Spawn a new [`LocalExecutor`] in a new thread with a given task.
    ///
    /// This `spawn` function is an ergonomic shortcut for calling
    /// `std::thread::spawn`, [`LocalExecutorBuilder::make`] in the spawned
    /// thread, and then [`LocalExecutor::run`]. This `spawn` function takes
    /// ownership of a [`LocalExecutorBuilder`] with the configuration for
    /// the [`LocalExecutor`], spawns that executor in a new thread, and starts
    /// the task given by `fut_gen()` in that thread.
    ///
    /// The indirection of `fut_gen()` here (instead of taking a `Future`)
    /// allows for futures that may not be `Send`-able once started. As this
    /// executor is thread-local it can guarantee that the futures will not
    /// be Sent once started.
    ///
    /// # Panics
    ///
    /// The newly spawned thread panics if creating the executor fails. If you
    /// need more fine-grained error handling consider initializing those
    /// entities manually.
    ///
    /// # Example
    ///
    /// ```
    /// use glommio::LocalExecutorBuilder;
    ///
    /// let handle = LocalExecutorBuilder::default()
    ///     .spawn(|| async move {
    ///         println!("hello");
    ///     })
    ///     .unwrap();
    ///
    /// handle.join().unwrap();
    /// ```
    ///
    /// [`LocalExecutor`]: struct.LocalExecutor.html
    ///
    /// [`LocalExecutorBuilder`]: struct.LocalExecutorBuilder.html
    ///
    /// [`LocalExecutorBuilder::make`]:
    /// struct.LocalExecutorBuilder.html#method.make
    ///
    /// [`LocalExecutor::run`]:struct.LocalExecutor.html#method.run
    #[must_use = "This spawns an executor on a thread, so you may need to call \
                  `JoinHandle::join()` to keep the main thread alive"]
    pub fn spawn<G, F, T>(self, fut_gen: G) -> Result<ExecutorJoinHandle<T>>
    where
        G: FnOnce() -> F + Send + 'static,
        F: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        let notifier = sys::new_sleep_notifier()?;
        let name = format!("{}-{}", self.name, notifier.id());
        let mut cpu_set_gen = placement::CpuSetGenerator::one(self.placement)?;
        let io_memory = self.io_memory;
        let ring_depth = self.ring_depth;
        let preempt_timer_duration = self.preempt_timer_duration;
        let spin_before_park = self.spin_before_park;
        let detect_stalls = self.detect_stalls;
        let record_io_latencies = self.record_io_latencies;
        let blocking_thread_pool_placement = self.blocking_thread_pool_placement;

        Builder::new()
            .name(name)
            .spawn(move || {
                let mut le = LocalExecutor::new(
                    notifier,
                    cpu_set_gen.next().cpu_binding(),
                    LocalExecutorConfig {
                        io_memory,
                        ring_depth,
                        preempt_timer: preempt_timer_duration,
                        record_io_latencies,
                        spin_before_park,
                        thread_pool_placement: blocking_thread_pool_placement,
                        detect_stalls,
                    },
                )?;
                le.init();
                le.run(async move { Ok(fut_gen().await) })
            })
            .map_err(Into::into)
            .map(ExecutorJoinHandle)
    }
}

impl Default for LocalExecutorBuilder {
    fn default() -> Self {
        Self::new(Placement::Unbound)
    }
}

/// A factory to configure and create a pool of [`LocalExecutor`]s.
///
/// Configuration methods apply their settings to all [`LocalExecutor`]s in the
/// pool unless otherwise specified.  Methods can be chained on the builder in
/// order to configure it.  The [`Self::on_all_shards`] method will take
/// ownership of the builder and create a [`PoolThreadHandles`] struct which can
/// be used to join the executor threads.
///
/// # Example
///
/// ```
/// use glommio::{LocalExecutorPoolBuilder, PoolPlacement};
///
/// let handles = LocalExecutorPoolBuilder::new(PoolPlacement::Unbound(4))
///     .on_all_shards(|| async move {
///         let id = glommio::executor().id();
///         println!("hello from executor {}", id);
///     })
///     .unwrap();
///
/// handles.join_all();
/// ```
pub struct LocalExecutorPoolBuilder {
    /// Spin for duration before parking a reactor
    spin_before_park: Option<Duration>,
    /// A name for the thread-to-be (if any), for identification in panic
    /// messages. Each executor in the pool will use this name followed by
    /// a hyphen and numeric id (e.g. `myname-1`).
    name: String,
    /// Amount of memory to reserve for storage I/O. This will be preallocated
    /// and registered with io_uring. It is still possible to use more than
    /// that, but it will come from the standard allocator and performance
    /// will suffer. Defaults to 10 MiB.
    io_memory: usize,
    /// The depth of the IO rings to create. This influences the level of IO
    /// concurrency. A higher ring depth allows a shard to submit a
    /// greater number of IO requests to the kernel at once.
    ring_depth: usize,
    /// How often to yield to other task queues
    preempt_timer_duration: Duration,
    /// Indicates a policy by which [`LocalExecutor`]s are bound to CPUs.
    placement: PoolPlacement,
    /// Whether to record the latencies of individual IO requests
    record_io_latencies: bool,
    /// The placement policy of the blocking thread pools. Each executor has
    /// its own pool. Defaults to 1 thread per pool, bound using the same
    /// placement strategy as its host executor
    blocking_thread_pool_placement: PoolPlacement,
    /// Factory function to generate the stall detection handler.
    /// [`DefaultStallDetectionHandler installs`] a signal handler for
    /// [`nix::libc::SIGUSR1`], so is disabled by default.
    handler_gen: Option<Box<dyn Fn() -> Box<dyn stall::StallDetectionHandler + 'static>>>,
}

impl fmt::Debug for LocalExecutorPoolBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalExecutorPoolBuilder")
            .field("spin_before_park", &self.spin_before_park)
            .field("name", &self.name)
            .field("io_memory", &self.io_memory)
            .field("ring_depth", &self.ring_depth)
            .field("preempt_timer_duration", &self.preempt_timer_duration)
            .field("record_io_latencies", &self.record_io_latencies)
            .field(
                "blocking_thread_pool_placement",
                &self.blocking_thread_pool_placement,
            )
            .finish_non_exhaustive()
    }
}

impl LocalExecutorPoolBuilder {
    /// Generates the base configuration for spawning a pool of
    /// [`LocalExecutor`]s, from which configuration methods can be chained.
    /// The method's only argument is the [`PoolPlacement`] policy by which
    /// [`LocalExecutor`]s are bound to the machine's hardware topology. i.e.
    /// how many and which CPUs to use.
    pub fn new(placement: PoolPlacement) -> Self {
        Self {
            spin_before_park: None,
            name: String::from(DEFAULT_EXECUTOR_NAME),
            io_memory: DEFAULT_IO_MEMORY,
            ring_depth: DEFAULT_RING_SUBMISSION_DEPTH,
            preempt_timer_duration: DEFAULT_PREEMPT_TIMER,
            placement: placement.clone(),
            record_io_latencies: false,
            blocking_thread_pool_placement: placement.shrink_to(1),
            handler_gen: None,
        }
    }

    /// Please see documentation under
    /// [`LocalExecutorBuilder::spin_before_park`] for details.  The setting
    /// is applied to all executors in the pool.
    #[must_use = "The builder must be built to be useful"]
    pub fn spin_before_park(mut self, spin: Duration) -> Self {
        self.spin_before_park = Some(spin);
        self
    }

    /// Please see documentation under [`LocalExecutorBuilder::name`] for
    /// details. The setting is applied to all executors in the pool. Note
    /// that when a thread is spawned, the `name` is combined with a hyphen
    /// and numeric id (e.g. `myname-1`) such that each thread has a unique
    /// name.
    #[must_use = "The builder must be built to be useful"]
    pub fn name(mut self, name: &str) -> Self {
        self.name = String::from(name);
        self
    }

    /// Please see documentation under [`LocalExecutorBuilder::io_memory`] for
    /// details.  The setting is applied to all executors in the pool.
    #[must_use = "The builder must be built to be useful"]
    pub fn io_memory(mut self, io_memory: usize) -> Self {
        self.io_memory = io_memory;
        self
    }

    /// Please see documentation under [`LocalExecutorBuilder::ring_depth`] for
    /// details.  The setting is applied to all executors in the pool.
    #[must_use = "The builder must be built to be useful"]
    pub fn ring_depth(mut self, ring_depth: usize) -> Self {
        assert!(ring_depth > 0, "ring depth should be strictly positive");
        self.ring_depth = ring_depth;
        self
    }

    /// Please see documentation under [`LocalExecutorBuilder::preempt_timer`]
    /// for details.  The setting is applied to all executors in the pool.
    #[must_use = "The builder must be built to be useful"]
    pub fn preempt_timer(mut self, dur: Duration) -> Self {
        self.preempt_timer_duration = dur;
        self
    }

    /// Whether to record the latencies of individual IO requests as part of the
    /// IO stats. Recording latency can be expensive. Disabled by default.
    #[must_use = "The builder must be built to be useful"]
    pub fn record_io_latencies(mut self, enabled: bool) -> Self {
        self.record_io_latencies = enabled;
        self
    }

    /// The placement policy of the blocking thread pool.
    /// Defaults to one thread using the same placement strategy as the host
    /// executor.
    #[must_use = "The builder must be built to be useful"]
    pub fn blocking_thread_pool_placement(mut self, placement: PoolPlacement) -> Self {
        self.blocking_thread_pool_placement = placement;
        self
    }

    /// Whether to detect stalls in unyielding tasks.
    /// This method takes a closure of `handler_gen`, which will be called on
    /// each new thread to generate the stall detection handler to be used in
    /// that executor. [`stall::DefaultStallDetectionHandler`] installs a signal
    /// handler for [`nix::libc::SIGUSR1`], so is disabled by default.
    /// # Examples
    ///
    /// ```
    /// use glommio::{
    ///     timer::Timer,
    ///     DefaultStallDetectionHandler,
    ///     LocalExecutorPoolBuilder,
    ///     PoolPlacement,
    /// };
    ///
    /// let local_ex = LocalExecutorPoolBuilder::new(PoolPlacement::Unbound(4))
    ///     .detect_stalls(Some(Box::new(|| Box::new(DefaultStallDetectionHandler {}))))
    ///     .on_all_shards(move || async {
    ///         Timer::new(std::time::Duration::from_millis(100)).await;
    ///         println!("Hello world!");
    ///     })
    ///     .expect("failed to spawn local executors")
    ///     .join_all();
    /// ```
    #[must_use = "The builder must be built to be useful"]
    pub fn detect_stalls(
        mut self,
        handler_gen: Option<Box<dyn Fn() -> Box<dyn stall::StallDetectionHandler + 'static>>>,
    ) -> Self {
        self.handler_gen = handler_gen;
        self
    }

    /// Spawn a pool of [`LocalExecutor`]s in a new thread according to the
    /// [`PoolPlacement`] policy, which is `Unbound` by default.
    ///
    /// This method is the pool equivalent of [`LocalExecutorBuilder::spawn`].
    ///
    /// The method takes a closure `fut_gen` which will be called on each new
    /// thread to obtain the [`Future`] to be executed there.
    ///
    /// # Panics
    ///
    /// The newly spawned thread panics if creating the executor fails. If you
    /// need more fine-grained error handling consider initializing those
    /// entities manually.
    #[must_use = "This spawns executors on multiple threads; threads may fail to spawn or you may \
                  need to call `PoolThreadHandles::join_all()` to keep the main thread alive"]
    pub fn on_all_shards<G, F, T>(self, fut_gen: G) -> Result<PoolThreadHandles<T>>
    where
        G: FnOnce() -> F + Clone + Send + 'static,
        F: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        let mut handles = PoolThreadHandles::new();
        let nr_shards = self.placement.executor_count();
        let mut cpu_set_gen = placement::CpuSetGenerator::pool(self.placement.clone())?;
        let latch = Latch::new(nr_shards);

        for _ in 0..nr_shards {
            match self.spawn_thread(&mut cpu_set_gen, &latch, fut_gen.clone()) {
                Ok(handle) => handles.push(handle),
                Err(err) => {
                    handles.join_all();
                    return Err(err);
                }
            }
        }

        Ok(handles)
    }

    /// Spawns a thread
    fn spawn_thread<G, F, T>(
        &self,
        cpu_set_gen: &mut placement::CpuSetGenerator,
        latch: &Latch,
        fut_gen: G,
    ) -> Result<JoinHandle<Result<T>>>
    where
        G: FnOnce() -> F + Clone + Send + 'static,
        F: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        // NOTE: `self.placement` was `std::mem::take`en in `Self::on_all_shards`; you
        // should no longer rely on its value at this point
        let cpu_binding = cpu_set_gen.next().cpu_binding();
        let notifier = sys::new_sleep_notifier()?;
        let name = format!("{}-{}", &self.name, notifier.id());
        let handle = Builder::new().name(name).spawn({
            let io_memory = self.io_memory;
            let ring_depth = self.ring_depth;
            let preempt_timer_duration = self.preempt_timer_duration;
            let spin_before_park = self.spin_before_park;
            let record_io_latencies = self.record_io_latencies;
            let blocking_thread_pool_placement = self.blocking_thread_pool_placement.clone();
            let detect_stalls = self.handler_gen.as_ref().map(|x| (*x.deref())());
            let latch = Latch::clone(latch);

            move || {
                // only allow the thread to create the `LocalExecutor` if all other threads that
                // are supposed to be created by the pool builder were successfully spawned
                if latch.arrive_and_wait() == LatchState::Ready {
                    let mut le = LocalExecutor::new(
                        notifier,
                        cpu_binding,
                        LocalExecutorConfig {
                            io_memory,
                            ring_depth,
                            preempt_timer: preempt_timer_duration,
                            record_io_latencies,
                            spin_before_park,
                            thread_pool_placement: blocking_thread_pool_placement,
                            detect_stalls,
                        },
                    )?;
                    le.init();
                    le.run(async move { Ok(fut_gen().await) })
                } else {
                    // this `Err` isn't visible to the user; the pool builder directly returns an
                    // `Err` from the `std::thread::Builder`
                    Err(io::Error::new(io::ErrorKind::Other, "spawn failed").into())
                }
            }
        });

        match handle {
            Ok(h) => Ok(h),
            Err(e) => {
                // The `std::thread::Builder` was unable to spawn the thread and retuned an
                // `Err`, so we notify other threads to let them know they
                // should not proceed with constructing their `LocalExecutor`s
                latch.cancel().expect("unreachable: latch was ready");

                Err(e.into())
            }
        }
    }
}

/// Holds a collection of [`JoinHandle`]s.
///
/// This struct is returned by [`LocalExecutorPoolBuilder::on_all_shards`].
#[derive(Debug)]
pub struct PoolThreadHandles<T> {
    handles: Vec<JoinHandle<Result<T>>>,
}

impl<T> PoolThreadHandles<T> {
    fn new() -> Self {
        Self {
            handles: Vec::new(),
        }
    }

    fn push(&mut self, handle: JoinHandle<Result<T>>) {
        self.handles.push(handle)
    }

    /// Obtain a reference to the `JoinHandle`s.
    pub fn handles(&self) -> &Vec<JoinHandle<Result<T>>> {
        &self.handles
    }

    /// Calls [`JoinHandle::join`] on all handles.
    pub fn join_all(self) -> Vec<Result<T>> {
        self.handles
            .into_iter()
            .map(|h| {
                match h.join() {
                    Ok(ok @ Ok(_)) => ok,
                    // this variant is unreachable since `Err` is only returned from a thread if
                    // another thread failed to spawn; `LocalExecutorPoolBuilder::on_all_shards`
                    // returns an immediate `Err` if any thread fails to spawn, so
                    // `PoolThreadHandles` would never be created
                    Ok(err @ Err(_)) => err,
                    Err(e) => Err(GlommioError::BuilderError(BuilderErrorKind::ThreadPanic(e))),
                }
            })
            .collect::<Vec<_>>()
    }
}

pub(crate) fn maybe_activate(tq: Rc<RefCell<TaskQueue>>) {
    #[cfg(not(feature = "native-tls"))]
    LOCAL_EX.with(|local_ex| {
        let mut queues = local_ex.queues.borrow_mut();
        queues.maybe_activate(tq);
    });

    #[cfg(feature = "native-tls")]
    unsafe {
        let mut queues = LOCAL_EX
            .as_ref()
            .expect("this thread doesn't have a LocalExecutor running")
            .queues
            .borrow_mut();
        queues.maybe_activate(tq);
    };
}

pub struct LocalExecutorConfig {
    pub io_memory: usize,
    pub ring_depth: usize,
    pub preempt_timer: Duration,
    pub record_io_latencies: bool,
    pub spin_before_park: Option<Duration>,
    pub thread_pool_placement: PoolPlacement,
    pub detect_stalls: Option<Box<dyn stall::StallDetectionHandler + 'static>>,
}

/// Single-threaded executor.
///
/// The executor can only be run on the thread that created it.
///
/// # Examples
///
/// ```
/// use glommio::LocalExecutor;
///
/// let local_ex = LocalExecutor::default();
///
/// local_ex.run(async {
///     println!("Hello world!");
/// });
/// ```
///
/// In many cases, use of [`LocalExecutorBuilder`] will provide more
/// configuration options and more ergonomic methods. See
/// [`LocalExecutorBuilder::spawn`] for examples.
///
/// [`LocalExecutorBuilder`]: struct.LocalExecutorBuilder.html
///
/// [`LocalExecutorBuilder::spawn`]:
/// struct.LocalExecutorBuilder.html#method.spawn
#[derive(Debug)]
pub struct LocalExecutor {
    queues: Rc<RefCell<ExecutorQueues>>,
    parker: parking::Parker,
    id: usize,
    reactor: Rc<reactor::Reactor>,
    stall_detector: RefCell<Option<StallDetector>>,
}

impl LocalExecutor {
    fn get_reactor(&self) -> Rc<Reactor> {
        self.reactor.clone()
    }

    fn init(&mut self) {
        let io_requirements = IoRequirements::new(Latency::NotImportant, 0);
        self.queues.borrow_mut().available_executors.insert(
            0,
            TaskQueue::new(
                Default::default(),
                "default",
                Shares::Static(1000),
                io_requirements,
            ),
        );
    }

    fn new(
        notifier: Arc<sys::SleepNotifier>,
        cpu_binding: Option<impl IntoIterator<Item = usize>>,
        mut config: LocalExecutorConfig,
    ) -> Result<LocalExecutor> {
        // Linux's default memory policy is "local allocation" which allocates memory
        // on the NUMA node containing the CPU where the allocation takes place.
        // Hence, we bind to a CPU in the provided CPU set before allocating any
        // memory for the `LocalExecutor`, thereby allowing any access to these
        // data structures to occur on a local NUMA node (nevertheless, for some
        // `Placement` variants a CPU set could span multiple NUMA nodes).
        // For additional information see:
        // https://www.kernel.org/doc/html/latest/admin-guide/mm/numa_memory_policy.html
        match cpu_binding {
            Some(cpu_set) => bind_to_cpu_set(cpu_set)?,
            None => config.spin_before_park = None,
        }
        let p = parking::Parker::new();
        let queues = ExecutorQueues::new(config.preempt_timer, config.spin_before_park);
        let id = notifier.id();
        trace!(id = id, "Creating executor");
        Ok(LocalExecutor {
            queues: Rc::new(RefCell::new(queues)),
            parker: p,
            id,
            reactor: Rc::new(reactor::Reactor::new(
                notifier,
                config.io_memory,
                config.ring_depth,
                config.record_io_latencies,
                config.thread_pool_placement,
            )?),
            stall_detector: RefCell::new(
                config
                    .detect_stalls
                    .map(|x| StallDetector::new(id, x))
                    .transpose()?,
            ),
        })
    }

    /// Enable or disable task stall detection at runtime
    ///
    /// # Examples
    /// ```
    /// use glommio::{DefaultStallDetectionHandler, LocalExecutor};
    ///
    /// let local_ex =
    ///     LocalExecutor::default().detect_stalls(Some(Box::new(DefaultStallDetectionHandler {})));
    /// ```
    pub fn detect_stalls(
        &self,
        handler: Option<Box<dyn stall::StallDetectionHandler + 'static>>,
    ) -> Result<()> {
        self.stall_detector.replace(
            handler
                .map(|x| StallDetector::new(self.id, x))
                .transpose()?,
        );
        Ok(())
    }

    /// Returns a unique identifier for this Executor.
    ///
    /// # Examples
    /// ```
    /// use glommio::LocalExecutor;
    ///
    /// let local_ex = LocalExecutor::default();
    /// println!("My ID: {}", local_ex.id());
    /// ```
    pub fn id(&self) -> usize {
        self.id
    }

    fn create_task_queue<S>(&self, shares: Shares, latency: Latency, name: S) -> TaskQueueHandle
    where
        S: Into<String>,
    {
        let index = {
            let mut ex = self.queues.borrow_mut();
            let index = ex.executor_index;
            ex.executor_index += 1;
            index
        };

        let io_requirements = IoRequirements::new(latency, index);
        let tq = TaskQueue::new(TaskQueueHandle { index }, name, shares, io_requirements);

        self.queues
            .borrow_mut()
            .available_executors
            .insert(index, tq);
        TaskQueueHandle { index }
    }

    /// Removes a task queue.
    ///
    /// The task queue cannot be removed if there are still pending tasks.
    pub fn remove_task_queue(&self, handle: TaskQueueHandle) -> Result<()> {
        let mut queues = self.queues.borrow_mut();

        let queue_entry = queues.available_executors.entry(handle.index);
        if let Entry::Occupied(entry) = queue_entry {
            let tq = entry.get();
            if tq.borrow().is_active() {
                return Err(GlommioError::queue_still_active(handle.index));
            }

            entry.remove();
            return Ok(());
        }
        Err(GlommioError::queue_not_found(handle.index))
    }

    fn get_queue(&self, handle: &TaskQueueHandle) -> Option<Rc<RefCell<TaskQueue>>> {
        self.queues
            .borrow()
            .available_executors
            .get(&handle.index)
            .cloned()
    }

    fn current_task_queue(&self) -> TaskQueueHandle {
        self.queues
            .borrow()
            .active_executing
            .as_ref()
            .unwrap()
            .borrow()
            .stats
            .index
    }

    fn mark_me_for_yield(&self) {
        let queues = self.queues.borrow();
        let mut me = queues.active_executing.as_ref().unwrap().borrow_mut();
        me.yielded = true;
    }

    fn spawn<T>(&self, future: impl Future<Output = T>) -> multitask::Task<T> {
        let tq = self
            .queues
            .borrow()
            .active_executing
            .clone() // this clone is cheap because we clone an `Option<Rc<_>>`
            .or_else(|| self.get_queue(&TaskQueueHandle { index: 0 }))
            .unwrap();

        let id = self.id;
        let ex = tq.borrow().ex.clone();
        ex.spawn_and_run(id, tq, future)
    }

    fn spawn_into<T, F>(&self, future: F, handle: TaskQueueHandle) -> Result<multitask::Task<T>>
    where
        F: Future<Output = T>,
    {
        let tq = self
            .get_queue(&handle)
            .ok_or_else(|| GlommioError::queue_not_found(handle.index))?;
        let ex = tq.borrow().ex.clone();
        let id = self.id;

        // can't run right away, because we need to cross into a different task queue
        Ok(ex.spawn_and_schedule(id, tq, future))
    }

    fn preempt_timer_duration(&self) -> Duration {
        self.queues.borrow().preempt_timer_duration
    }

    fn spin_before_park(&self) -> Option<Duration> {
        self.queues.borrow().spin_before_park
    }

    #[inline(always)]
    pub(crate) fn need_preempt(&self) -> bool {
        self.reactor.need_preempt()
    }

    fn run_task_queues(&self) -> bool {
        let mut ran = false;
        loop {
            self.reactor.sys.install_eventfd();
            if self.need_preempt() {
                break;
            }
            if !self.run_one_task_queue() {
                return false;
            } else {
                ran = true;
            }
        }
        ran
    }

    fn run_one_task_queue(&self) -> bool {
        let mut tq = self.queues.borrow_mut();
        let candidate = tq.active_executors.pop();
        tq.stats.scheduler_runs += 1;

        match candidate {
            Some(queue) => {
                tq.active_executing = Some(queue.clone());
                drop(tq);

                let time = {
                    let now = Instant::now();
                    let mut queue_ref = queue.borrow_mut();
                    queue_ref.prepare_to_run(now);
                    self.reactor
                        .inform_io_requirements(queue_ref.io_requirements);
                    now
                };

                let (runtime, tasks_executed_this_loop) = {
                    let detector = self.stall_detector.borrow();
                    let guard = detector.as_ref().map(|x| {
                        let queue = queue.borrow_mut();
                        x.enter_task_queue(
                            queue.stats.index,
                            queue.name.clone(),
                            time,
                            self.preempt_timer_duration(),
                        )
                    });

                    let mut tasks_executed_this_loop = 0;
                    loop {
                        let mut queue_ref = queue.borrow_mut();
                        if self.need_preempt() || queue_ref.yielded() {
                            break;
                        }

                        if let Some(r) = queue_ref.get_task() {
                            drop(queue_ref);
                            r.run();
                            tasks_executed_this_loop += 1;
                        } else {
                            break;
                        }
                    }
                    let elapsed = time.elapsed();
                    drop(guard);
                    (elapsed, tasks_executed_this_loop)
                };

                let (need_repush, vruntime) = {
                    let mut state = queue.borrow_mut();
                    let last_vruntime = state.account_vruntime(runtime);
                    (state.is_active(), last_vruntime)
                };

                let mut tq = self.queues.borrow_mut();
                tq.active_executing = None;
                tq.stats.executor_runtime += runtime;
                tq.stats.tasks_executed += tasks_executed_this_loop;
                let vruntime = match vruntime {
                    Some(x) => x,
                    None => {
                        for queue in tq.available_executors.values() {
                            let mut q = queue.borrow_mut();
                            q.vruntime = 0;
                        }
                        0
                    }
                };

                if need_repush {
                    tq.active_executors.push(queue);
                } else {
                    tq.reevaluate_preempt_timer();
                }

                // Compute the smallest vruntime out of all the active task queues
                // This value is used to set the vruntime of deactivated task queues when they
                // are woken up.
                tq.default_vruntime = tq
                    .active_executors
                    .iter()
                    .map(|x| x.borrow().vruntime)
                    .min()
                    .unwrap_or(vruntime);

                true
            }
            None => false,
        }
    }

    /// Runs the executor until the given future completes.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::{LocalExecutor, Task};
    ///
    /// let local_ex = LocalExecutor::default();
    ///
    /// let res = local_ex.run(async {
    ///     let task = glommio::spawn_local(async { 1 + 2 });
    ///     task.await * 2
    /// });
    ///
    /// assert_eq!(res, 6);
    /// ```
    pub fn run<T>(&self, future: impl Future<Output = T>) -> T {
        let run = |this: &Self| {
            // this waker is never exposed in the public interface and is only used to check
            // whether the task's `JoinHandle` is `Ready`
            let waker = dummy_waker();
            let cx = &mut Context::from_waker(&waker);

            let spin_before_park = self.spin_before_park().unwrap_or_default();

            let future = this
                .spawn_into(async move { future.await }, TaskQueueHandle::default())
                .unwrap()
                .detach();
            pin!(future);

            let mut pre_time = Instant::now();
            loop {
                if let Poll::Ready(t) = future.as_mut().poll(cx) {
                    // can't be canceled, and join handle is None only upon
                    // cancellation or panic. So in case of panic this just propagates
                    let cur_time = Instant::now();
                    this.queues.borrow_mut().stats.total_runtime += cur_time - pre_time;
                    break t.unwrap();
                }

                // We want to do I/O before we call run_task_queues,
                // for the benefit of the latency ring. If there are pending
                // requests that are latency sensitive we want them out of the
                // ring ASAP (before we run the task queues). We will also use
                // the opportunity to install the timer.
                this.parker
                    .poll_io(|| Some(this.preempt_timer_duration()))
                    .expect("Failed to poll io! This is actually pretty bad!");

                // run user code
                let run = this.run_task_queues();

                // account for runtime and poll/sleep if possible
                let cur_time = Instant::now();
                this.queues.borrow_mut().stats.total_runtime += cur_time - pre_time;
                pre_time = cur_time;
                if !run {
                    if let Poll::Ready(t) = future.as_mut().poll(cx) {
                        // It may be that we just became ready now that the task queue
                        // is exhausted. But if we sleep (park) we'll never know so we
                        // test again here. We can't test *just* here because the main
                        // future is probably the one setting up the task queues and etc.
                        break t.unwrap();
                    } else {
                        while !this.reactor.spin_poll_io().unwrap() {
                            if pre_time.elapsed() > spin_before_park {
                                this.parker
                                    .park()
                                    .expect("Failed to park! This is actually pretty bad!");
                                break;
                            }
                        }
                        // reset the timer for deduct spin loop time
                        pre_time = Instant::now();
                    }
                }
            }
        };

        #[cfg(not(feature = "native-tls"))]
        {
            assert!(
                !LOCAL_EX.is_set(),
                "There is already an LocalExecutor running on this thread"
            );
            LOCAL_EX.set(self, || run(self))
        }

        #[cfg(feature = "native-tls")]
        unsafe {
            assert!(
                LOCAL_EX.is_null(),
                "There is already an LocalExecutor running on this thread"
            );

            defer!(LOCAL_EX = std::ptr::null());
            LOCAL_EX = self as *const Self;
            run(self)
        }
    }
}

/// Spawns a single-threaded executor with default settings on the current
/// thread.
///
/// This will create a executor using default parameters of
/// `LocalExecutorBuilder`, if you want to further customize it, use this API
/// instead.
///
/// # Panics
///
/// Panics if creating the executor fails; use `LocalExecutorBuilder::make` to
/// recover from such errors.
///
/// # Examples
///
/// ```
/// use glommio::LocalExecutor;
///
/// let local_ex = LocalExecutor::default();
/// ```
impl Default for LocalExecutor {
    fn default() -> Self {
        LocalExecutorBuilder::new(Placement::Unbound)
            .make()
            .unwrap()
    }
}

/// A spawned future that can be detached
///
/// Tasks are also futures themselves and yield the output of the spawned
/// future.
///
/// When a task is dropped, its gets canceled and won't be polled again. To
/// cancel a task a bit more gracefully and wait until it stops running, use the
/// [`cancel()`][`Task::cancel()`] method.
///
/// Tasks that panic get immediately canceled. Awaiting a canceled task also
/// causes a panic.
///
/// # Examples
///
/// ```
/// # use glommio::{LocalExecutor, Task};
/// #
/// # let ex = LocalExecutor::default();
/// #
/// # ex.run(async {
/// let task = glommio::spawn_local(async {
///     println!("Hello from a task!");
///     1 + 2
/// });
///
/// assert_eq!(task.await, 3);
/// # });
/// ```
/// Note that there is no guarantee of ordering when reasoning about when a
/// task runs, as that is an implementation detail.
///
/// In particular, acquiring a borrow and holding across a task spawning may
/// sometimes work but panic depending on scheduling decisions, so it is still
/// illegal.
///
///
/// ```no_run
/// # use glommio::{LocalExecutor, Task};
/// # use std::rc::Rc;
/// # use std::cell::RefCell;
/// #
/// # let ex = LocalExecutor::default();
/// #
/// # ex.run(async {
/// let example = Rc::new(RefCell::new(0));
/// let exclone = example.clone();
///
/// let mut ex_mut = example.borrow_mut();
/// *ex_mut = 1;
///
/// let task = glommio::spawn_local(async move {
///     let ex = exclone.borrow();
///     println!("Current value: {}", ex);
/// });
///
/// // This is fine if `task` executes after the current task, but will panic if
/// // preempts the current task and executes first. This is therefore invalid.
/// *ex_mut = 2;
/// drop(ex_mut);
///
/// task.await;
/// # });
/// ```
#[must_use = "tasks get canceled when dropped, use `.detach()` to run them in the background"]
#[derive(Debug)]
pub struct Task<T>(multitask::Task<T>);

impl<T> Task<T> {
    /// Detaches the task to let it keep running in the background.
    ///
    /// # Examples
    ///
    /// ```
    /// use futures_lite::future;
    /// use glommio::{timer::Timer, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async {
    ///     glommio::spawn_local(async {
    ///         loop {
    ///             println!("I'm a background task looping forever.");
    ///             glommio::executor().yield_task_queue_now().await;
    ///         }
    ///     })
    ///     .detach();
    ///     Timer::new(std::time::Duration::from_micros(100)).await;
    /// })
    /// ```
    pub fn detach(self) -> task::JoinHandle<T> {
        self.0.detach()
    }

    /// Cancels the task and waits for it to stop running.
    ///
    /// Returns the task's output if it was completed just before it got
    /// canceled, or [`None`] if it didn't complete.
    ///
    /// While it's possible to simply drop the [`Task`] to cancel it, this is a
    /// cleaner way of canceling because it also waits for the task to stop
    /// running.
    ///
    /// # Examples
    ///
    /// ```
    /// use futures_lite::future;
    /// use glommio::LocalExecutor;
    ///
    /// let ex = LocalExecutor::default();
    ///
    /// ex.run(async {
    ///     let task = glommio::spawn_local(async {
    ///         loop {
    ///             println!("Even though I'm in an infinite loop, you can still cancel me!");
    ///             future::yield_now().await;
    ///         }
    ///     });
    ///
    ///     task.cancel().await;
    /// });
    /// ```
    pub async fn cancel(self) -> Option<T> {
        self.0.cancel().await
    }
}

impl<T> Future for Task<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}

/// A spawned future that cannot be detached, and has a predictable lifetime.
///
/// Because their lifetimes are bounded, you don't need to make sure that data
/// you pass to the `ScopedTask` is `'static`, which can be cheaper (no need to
/// reference count). If you, however, would like to `.detach` this task and
/// have it run in the background, consider using [`Task`] instead.
///
/// Tasks are also futures themselves and yield the output of the spawned
/// future.
///
/// When a task is dropped, its gets canceled and won't be polled again. To
/// cancel a task a bit more gracefully and wait until it stops running, use the
/// [`cancel()`][`ScopedTask::cancel()`] method.
///
/// Tasks that panic get immediately canceled. Awaiting a canceled task also
/// causes a panic.
///
/// # Safety
///
/// `ScopedTask` is safe to use so long as it is guaranteed to be either awaited
/// or dropped. Rust does not guarantee that destructors will be called, and if
/// they are not, `ScopedTask`s can be kept alive after the scope is terminated.
///
/// Typically, the only situations in which `drop` is not executed are:
///
/// * If you manually choose not to, with [`std::mem::forget`] or
///   [`ManuallyDrop`].
/// * If cyclic reference counts prevents the task from being destroyed.
///
/// If you believe any of the above situations are present (the first one is,
/// of course, considerably easier to spot), avoid using the `ScopedTask`.
///
/// # Examples
///
/// ```
/// use glommio::LocalExecutor;
///
/// let ex = LocalExecutor::default();
///
/// ex.run(async {
///     let a = 2;
///     let task = unsafe {
///         glommio::spawn_scoped_local(async {
///             println!("Hello from a task!");
///             1 + a // this is a reference, and it works just fine
///         })
///     };
///
///     assert_eq!(task.await, 3);
/// });
/// ```
/// The usual borrow checker rules apply. A [`ScopedTask`] can acquire a mutable
/// reference to a variable just fine:
///
/// ```
/// # use glommio::{LocalExecutor};
/// #
/// # let ex = LocalExecutor::default();
/// # ex.run(async {
/// let mut a = 2;
/// let task = unsafe {
///     glommio::spawn_scoped_local(async {
///         a = 3;
///     })
/// };
/// task.await;
/// assert_eq!(a, 3);
/// # });
/// ```
///
/// But until the task completes, the reference is mutably held, so we can no
/// longer immutably reference it:
///
/// ```compile_fail
/// # use glommio::LocalExecutor;
/// #
/// # let ex = LocalExecutor::default();
/// # ex.run(async {
/// let mut a = 2;
/// let task = unsafe {
///     glommio::scoped_local(async {
///         a = 3;
///     })
/// };
/// assert_eq!(a, 3); // task hasn't completed yet!
/// task.await;
/// # });
/// ```
///
/// You can still use [`Cell`] and [`RefCell`] normally to work around this.
/// Just keep in mind that there is no guarantee of ordering for execution of
/// tasks, and if the task has not yet finished the value may or may not have
/// changed (as with any interior mutability)
///
/// ```
/// # use glommio::{LocalExecutor};
/// # use std::cell::Cell;
/// #
/// # let ex = LocalExecutor::default();
/// # ex.run(async {
/// let a = Cell::new(2);
/// let task = unsafe {
///     glommio::spawn_scoped_local(async {
///         a.set(3);
///     })
/// };
///
/// assert!(a.get() == 3 || a.get() == 2); // impossible to know if it will be 2 or 3
/// task.await;
/// assert_eq!(a.get(), 3); // The task finished now.
/// //
/// # });
/// ```
///
/// The following code, however, will access invalid memory as drop is never
/// executed
///
/// ```no_run
/// # use glommio::{LocalExecutor};
/// # use std::cell::Cell;
/// #
/// # let ex = LocalExecutor::default();
/// # ex.run(async {
/// {
///     let a = &mut "mayhem";
///     let task = unsafe {
///         glommio::spawn_scoped_local(async {
///             *a = "doom";
///         })
///     };
///     std::mem::forget(task);
/// }
/// # });
/// ```

/// [`Task`]: crate::Task
/// [`Cell`]: std::cell::Cell
/// [`RefCell`]: std::cell::RefCell
/// [`std::mem::forget`]: std::mem::forget
/// [`ManuallyDrop`]: std::mem::ManuallyDrop
#[must_use = "scoped tasks get canceled when dropped, use a standard Task and `.detach()` to run \
              them in the background"]
#[derive(Debug)]
pub struct ScopedTask<'a, T>(multitask::Task<T>, PhantomData<&'a T>);

impl<'a, T> ScopedTask<'a, T> {
    /// Cancels the task and waits for it to stop running.
    ///
    /// Returns the task's output if it was completed just before it got
    /// canceled, or [`None`] if it didn't complete.
    ///
    /// While it's possible to simply drop the [`ScopedTask`] to cancel it, this
    /// is a cleaner way of canceling because it also waits for the task to
    /// stop running.
    ///
    /// # Examples
    ///
    /// ```
    /// use futures_lite::future;
    /// use glommio::LocalExecutor;
    ///
    /// let ex = LocalExecutor::default();
    ///
    /// ex.run(async {
    ///     let task = unsafe {
    ///         glommio::spawn_scoped_local(async {
    ///             loop {
    ///                 println!("Even though I'm in an infinite loop, you can still cancel me!");
    ///                 future::yield_now().await;
    ///             }
    ///         })
    ///     };
    ///
    ///     task.cancel().await;
    /// });
    /// ```
    pub async fn cancel(self) -> Option<T> {
        self.0.cancel().await
    }
}

impl<'a, T> Future for ScopedTask<'a, T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}

/// Conditionally yields the current task queue. The scheduler may then
/// process other task queues according to their latency requirements.
/// If a call to this function results in the current queue to yield,
/// then the calling task is moved to the back of the yielded task
/// queue.
///
/// Under which condition this function yield is an implementation detail
/// subject to change, but it will always be somehow related to the latency
/// guarantees that the task queues want to uphold in their
/// `Latency::Matters` parameter (or `Latency::NotImportant`).
///
/// This function is the central mechanism of task cooperation in Glommio
/// and should be preferred over unconditional yielding methods like
/// [`ExecutorProxy::yield_now`] and
/// [`ExecutorProxy::yield_task_queue_now`].
#[inline(always)]
pub async fn yield_if_needed() {
    executor().yield_if_needed().await
}

/// Spawns a task onto the current single-threaded executor.
///
/// If called from a [`LocalExecutor`], the task is spawned on it.
/// Otherwise, this method panics.
///
/// Note that there is no guarantee of when the spawned task is scheduled.
/// The current task can continue its execution or be preempted by the
/// newly spawned task immediately. See the documentation for the
/// top-level [`Task`] for examples.
///
/// Proxy to [`ExecutorProxy::spawn_local`]
///
/// # Examples
///
/// ```
/// use glommio::{LocalExecutor, Task};
///
/// let local_ex = LocalExecutor::default();
///
/// local_ex.run(async {
///     let task = glommio::spawn_local(async { 1 + 2 });
///     assert_eq!(task.await, 3);
/// });
/// ```
pub fn spawn_local<T>(future: impl Future<Output = T> + 'static) -> Task<T>
where
    T: 'static,
{
    executor().spawn_local(future)
}

/// Allocates a buffer that is suitable for using to write to Direct Memory
/// Access File (DMA). Please note that this implementation uses embedded buddy
/// allocator to speed up allocation of the memory chunks, but the same
/// allocator is used to server memory needed to write/read data from `uring` so
/// probably that is not good idea to keep allocated memory for a long time.
/// If you want to keep allocated buffer for a long time please use
/// ['crate::allocate_dma_buffer_global'] instead.
/// Be careful when you use this buffer with DMA file, size and position of the
/// buffer should be properly aligned to the block size of the device where the
/// file is located
///
/// * `size` size of the requested buffer in bytes
///
/// [`DmaFile`]: crate::io::DmaFile
pub fn allocate_dma_buffer(size: usize) -> DmaBuffer {
    executor().reactor().alloc_dma_buffer(size)
}

/// Allocates a buffer that is suitable for using to write to Direct Memory
/// Access File (DMA). If you do not plan to keep allocated buffer for a long
/// time please use ['crate::allocate_dma_buffer'] instead.
/// Be careful when you use this buffer with DMA file, size and position of the
/// buffer should be properly aligned to the block size of the device where the
/// file is located
///
/// * `size` size of the requested buffer in bytes
///
/// [`DmaFile`]: crate::io::DmaFile
pub fn allocate_dma_buffer_global(size: usize) -> DmaBuffer {
    DmaBuffer::new(size).unwrap()
}

/// Spawns a task onto the current single-threaded executor, in a particular
/// task queue
///
/// If called from a [`LocalExecutor`], the task is spawned on it.
/// Otherwise, this method panics.
///
/// Note that there is no guarantee of when the spawned task is scheduled.
/// The current task can continue its execution or be preempted by the
/// newly spawned task immediately. See the documentation for the
/// top-level [`Task`] for examples.
///
/// Proxy to [`ExecutorProxy::spawn_local_into`]
///
/// # Examples
///
/// ```
/// # use glommio::{ LocalExecutor, Shares, Task};
///
/// # let local_ex = LocalExecutor::default();
/// # local_ex.run(async {
/// let handle = glommio::executor().create_task_queue(
///     Shares::default(),
///     glommio::Latency::NotImportant,
///     "test_queue",
/// );
/// let task = glommio::spawn_local_into(async { 1 + 2 }, handle).expect("failed to spawn task");
/// assert_eq!(task.await, 3);
/// # });
/// ```
pub fn spawn_local_into<T>(
    future: impl Future<Output = T> + 'static,
    handle: TaskQueueHandle,
) -> Result<Task<T>>
where
    T: 'static,
{
    executor().spawn_local_into(future, handle)
}

/// Spawns a task onto the current single-threaded executor.
///
/// If called from a [`LocalExecutor`], the task is spawned on it.
///
/// Otherwise, this method panics.
///
/// Proxy to [`ExecutorProxy::spawn_scoped_local`]
///
/// # Safety
///
/// `ScopedTask` depends on `drop` running or `.await` being called for
/// safety. See the struct [`ScopedTask`] for details.
///
/// # Examples
///
/// ```
/// use glommio::LocalExecutor;
///
/// let local_ex = LocalExecutor::default();
///
/// local_ex.run(async {
///     let non_static = 2;
///     let task = unsafe { glommio::spawn_scoped_local(async { 1 + non_static }) };
///     assert_eq!(task.await, 3);
/// });
/// ```
pub unsafe fn spawn_scoped_local<'a, T>(future: impl Future<Output = T> + 'a) -> ScopedTask<'a, T> {
    executor().spawn_scoped_local(future)
}

/// Spawns a task onto the current single-threaded executor, in a particular
/// task queue
///
/// If called from a [`LocalExecutor`], the task is spawned on it.
///
/// Otherwise, this method panics.
///
/// Proxy to [`ExecutorProxy::spawn_scoped_local_into`]
///
/// # Safety
///
/// `ScopedTask` depends on `drop` running or `.await` being called for
/// safety. See the struct [`ScopedTask`] for details.
///
/// # Examples
///
/// ```
/// use glommio::{LocalExecutor, Shares};
///
/// let local_ex = LocalExecutor::default();
/// local_ex.run(async {
///     let handle = glommio::executor().create_task_queue(
///         Shares::default(),
///         glommio::Latency::NotImportant,
///         "test_queue",
///     );
///     let non_static = 2;
///     let task = unsafe {
///         glommio::spawn_scoped_local_into(async { 1 + non_static }, handle)
///             .expect("failed to spawn task")
///     };
///     assert_eq!(task.await, 3);
/// })
/// ```
pub unsafe fn spawn_scoped_local_into<'a, T>(
    future: impl Future<Output = T> + 'a,
    handle: TaskQueueHandle,
) -> Result<ScopedTask<'a, T>> {
    executor().spawn_scoped_local_into(future, handle)
}

/// A proxy struct to the underlying [`LocalExecutor`]. It is accessible from
/// anywhere within a Glommio context using [`executor()`].
#[derive(Debug)]
pub struct ExecutorProxy {}

impl ExecutorProxy {
    /// Checks if this task has run for too long and need to be preempted. This
    /// is useful for situations where we can't call .await, for instance,
    /// if a [`RefMut`] is held. If this tests true, then the user is
    /// responsible for making any preparations necessary for calling .await
    /// and doing it themselves.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::LocalExecutorBuilder;
    ///
    /// let ex = LocalExecutorBuilder::default()
    ///     .spawn(|| async {
    ///         loop {
    ///             if glommio::executor().need_preempt() {
    ///                 break;
    ///             }
    ///         }
    ///     })
    ///     .unwrap();
    ///
    /// ex.join().unwrap();
    /// ```
    ///
    /// [`RefMut`]: https://doc.rust-lang.org/std/cell/struct.RefMut.html
    #[inline(always)]
    pub fn need_preempt(&self) -> bool {
        #[cfg(not(feature = "native-tls"))]
        return LOCAL_EX.with(|local_ex| local_ex.need_preempt());

        #[cfg(feature = "native-tls")]
        return unsafe {
            LOCAL_EX
                .as_ref()
                .map(|ex| ex.need_preempt())
                .unwrap_or_default()
        };
    }

    /// Conditionally yields the current task queue. The scheduler may then
    /// process other task queues according to their latency requirements.
    /// If a call to this function results in the current queue to yield,
    /// then the calling task is moved to the back of the yielded task
    /// queue.
    ///
    /// Under which condition this function yield is an implementation detail
    /// subject to change, but it will always be somehow related to the latency
    /// guarantees that the task queues want to uphold in their
    /// `Latency::Matters` parameter (or `Latency::NotImportant`).
    ///
    /// This function is the central mechanism of task cooperation in Glommio
    /// and should be preferred over unconditional yielding methods like
    /// [`ExecutorProxy::yield_now`] and
    /// [`ExecutorProxy::yield_task_queue_now`].
    #[inline(always)]
    pub async fn yield_if_needed(&self) {
        #[cfg(not(feature = "native-tls"))]
        {
            let need_yield = if LOCAL_EX.is_set() {
                LOCAL_EX.with(|local_ex| {
                    if local_ex.need_preempt() {
                        local_ex.mark_me_for_yield();
                        true
                    } else {
                        false
                    }
                })
            } else {
                // We are not in a glommio context
                false
            };

            if need_yield {
                futures_lite::future::yield_now().await;
            }
        }

        #[cfg(feature = "native-tls")]
        unsafe {
            if self.need_preempt() {
                (*LOCAL_EX).mark_me_for_yield();
                futures_lite::future::yield_now().await;
            }
        }
    }

    /// Unconditionally yields the current task and forces the scheduler
    /// to poll another task within the current task queue.
    /// Calling this wakes the current task and returns [`Poll::Pending`] once.
    ///
    /// Unless you know you need to yield right now, using
    /// [`ExecutorProxy::yield_if_needed`] instead is the better choice.
    #[inline(always)]
    pub async fn yield_now(&self) {
        futures_lite::future::yield_now().await
    }

    /// Unconditionally yields the current task queue and forces the scheduler
    /// to poll another queue. Use [`ExecutorProxy::yield_now`] to yield within
    /// a queue.
    ///
    /// Unless you know you need to yield right now, using
    /// [`ExecutorProxy::yield_if_needed`] instead is the better choice.
    #[inline(always)]
    pub async fn yield_task_queue_now(&self) {
        #[cfg(not(feature = "native-tls"))]
        {
            if LOCAL_EX.is_set() {
                LOCAL_EX.with(|local_ex| {
                    local_ex.mark_me_for_yield();
                })
            }
            futures_lite::future::yield_now().await;
        }

        #[cfg(feature = "native-tls")]
        {
            if let Some(local_ex) = unsafe { LOCAL_EX.as_ref() } {
                local_ex.mark_me_for_yield();
            }
            futures_lite::future::yield_now().await;
        }
    }

    #[inline(always)]
    pub(crate) fn reactor(&self) -> Rc<reactor::Reactor> {
        #[cfg(not(feature = "native-tls"))]
        return LOCAL_EX.with(|local_ex| local_ex.get_reactor());

        #[cfg(feature = "native-tls")]
        return unsafe {
            LOCAL_EX
                .as_ref()
                .expect("this thread doesn't have a LocalExecutor running")
                .get_reactor()
        };
    }

    /// Returns the id of the current executor
    ///
    /// If called from a [`LocalExecutor`], returns the id of the executor.
    ///
    /// Otherwise, this method panics.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::{LocalExecutor, Task};
    ///
    /// let local_ex = LocalExecutor::default();
    ///
    /// local_ex.run(async {
    ///     println!("my ID: {}", glommio::executor().id());
    /// });
    /// ```
    pub fn id(&self) -> usize {
        #[cfg(not(feature = "native-tls"))]
        return LOCAL_EX.with(|local_ex| local_ex.id());

        #[cfg(feature = "native-tls")]
        return unsafe {
            LOCAL_EX
                .as_ref()
                .expect("this thread doesn't have a LocalExecutor running")
                .id()
        };
    }

    /// Creates a new task queue, with a given latency hint and the provided
    /// name
    ///
    /// Each task queue is scheduled based on the [`Shares`] and [`Latency`]
    /// system, and tasks within a queue will be scheduled in serial.
    ///
    /// Returns an opaque handle that can later be used to launch tasks into
    /// that queue with [`local_into`].
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::{Latency, LocalExecutor, Shares};
    /// use std::time::Duration;
    ///
    /// let local_ex = LocalExecutor::default();
    /// local_ex.run(async move {
    ///     let task_queue = glommio::executor().create_task_queue(
    ///         Shares::default(),
    ///         Latency::Matters(Duration::from_secs(1)),
    ///         "my_tq",
    ///     );
    ///     let task = glommio::spawn_local_into(
    ///         async {
    ///             println!("Hello world");
    ///         },
    ///         task_queue,
    ///     )
    ///     .expect("failed to spawn task");
    /// });
    /// ```
    ///
    /// [`local_into`]: crate::spawn_local_into
    /// [`Shares`]: enum.Shares.html
    /// [`Latency`]: enum.Latency.html
    pub fn create_task_queue(
        &self,
        shares: Shares,
        latency: Latency,
        name: &str,
    ) -> TaskQueueHandle {
        #[cfg(not(feature = "native-tls"))]
        return LOCAL_EX.with(|local_ex| local_ex.create_task_queue(shares, latency, name));

        #[cfg(feature = "native-tls")]
        return unsafe {
            LOCAL_EX
                .as_ref()
                .expect("this thread doesn't have a LocalExecutor running")
                .create_task_queue(shares, latency, name)
        };
    }

    /// Returns the [`TaskQueueHandle`] that represents the TaskQueue currently
    /// running. This can be passed directly into [`crate::spawn_local_into`].
    /// This must be run from a task that was generated through
    /// [`crate::spawn_local`] or [`crate::spawn_local_into`]
    ///
    /// # Examples
    /// ```
    /// use glommio::{Latency, LocalExecutor, LocalExecutorBuilder, Shares};
    ///
    /// let ex = LocalExecutorBuilder::default()
    ///     .spawn(|| async move {
    ///         let original_tq = glommio::executor().current_task_queue();
    ///         let new_tq = glommio::executor().create_task_queue(
    ///             Shares::default(),
    ///             Latency::NotImportant,
    ///             "test",
    ///         );
    ///
    ///         let task = glommio::spawn_local_into(
    ///             async move {
    ///                 glommio::spawn_local_into(
    ///                     async move {
    ///                         assert_eq!(glommio::executor().current_task_queue(), original_tq);
    ///                     },
    ///                     original_tq,
    ///                 )
    ///                 .unwrap();
    ///             },
    ///             new_tq,
    ///         )
    ///         .unwrap();
    ///         task.await;
    ///     })
    ///     .unwrap();
    ///
    /// ex.join().unwrap();
    /// ```
    pub fn current_task_queue(&self) -> TaskQueueHandle {
        #[cfg(not(feature = "native-tls"))]
        return LOCAL_EX.with(|local_ex| local_ex.current_task_queue());

        #[cfg(feature = "native-tls")]
        return unsafe {
            LOCAL_EX
                .as_ref()
                .expect("this thread doesn't have a LocalExecutor running")
                .current_task_queue()
        };
    }

    /// Returns a [`Result`] with its `Ok` value wrapping a [`TaskQueueStats`]
    /// or a [`GlommioError`] of type `[QueueErrorKind`] if there is no task
    /// queue with this handle
    ///
    /// # Examples
    /// ```
    /// use glommio::{Latency, LocalExecutorBuilder, Shares};
    ///
    /// let ex = LocalExecutorBuilder::default()
    ///     .spawn(|| async move {
    ///         let new_tq = glommio::executor().create_task_queue(
    ///             Shares::default(),
    ///             Latency::NotImportant,
    ///             "test",
    ///         );
    ///         println!(
    ///             "Stats for test: {:?}",
    ///             glommio::executor().task_queue_stats(new_tq).unwrap()
    ///         );
    ///     })
    ///     .unwrap();
    ///
    /// ex.join().unwrap();
    /// ```
    ///
    /// [`ExecutorStats`]: struct.ExecutorStats.html
    /// [`GlommioError`]: crate::error::GlommioError
    /// [`QueueErrorKind`]: crate::error::QueueErrorKind
    /// [`Result`]: https://doc.rust-lang.org/std/result/enum.Result.html
    pub fn task_queue_stats(&self, handle: TaskQueueHandle) -> Result<TaskQueueStats> {
        #[cfg(not(feature = "native-tls"))]
        return LOCAL_EX.with(|local_ex| match local_ex.get_queue(&handle) {
            Some(x) => Ok(x.borrow_mut().stats.take()),
            None => Err(GlommioError::queue_not_found(handle.index)),
        });

        #[cfg(feature = "native-tls")]
        return match unsafe {
            LOCAL_EX
                .as_ref()
                .expect("this thread doesn't have a LocalExecutor running")
                .get_queue(&handle)
        } {
            Some(x) => Ok(x.borrow_mut().stats.take()),
            None => Err(GlommioError::queue_not_found(handle.index)),
        };
    }

    /// Returns a collection of [`TaskQueueStats`] with information about all
    /// task queues in the system
    ///
    /// The collection can be anything that implements [`Extend`] and it is
    /// initially passed by the user, so they can control how allocations are
    /// done.
    ///
    /// # Examples
    /// ```
    /// use glommio::{executor, Latency, LocalExecutorBuilder, Shares};
    ///
    /// let ex = LocalExecutorBuilder::default()
    ///     .spawn(|| async move {
    ///         let new_tq = glommio::executor().create_task_queue(
    ///             Shares::default(),
    ///             Latency::NotImportant,
    ///             "test",
    ///         );
    ///         let v = Vec::new();
    ///         println!(
    ///             "Stats for all queues: {:?}",
    ///             glommio::executor().all_task_queue_stats(v)
    ///         );
    ///     })
    ///     .unwrap();
    ///
    /// ex.join().unwrap();
    /// ```
    ///
    /// [`ExecutorStats`]: struct.ExecutorStats.html
    /// [`Result`]: https://doc.rust-lang.org/std/result/enum.Result.html
    /// [`Extend`]: https://doc.rust-lang.org/std/iter/trait.Extend.html
    pub fn all_task_queue_stats<V>(&self, mut output: V) -> V
    where
        V: Extend<TaskQueueStats>,
    {
        #[cfg(not(feature = "native-tls"))]
        LOCAL_EX.with(|local_ex| {
            output.extend(
                local_ex
                    .queues
                    .borrow()
                    .available_executors
                    .values()
                    .map(|x| x.borrow_mut().stats.take()),
            );
        });

        #[cfg(feature = "native-tls")]
        output.extend(unsafe {
            LOCAL_EX
                .as_ref()
                .expect("this thread doesn't have a LocalExecutor running")
                .queues
                .borrow()
                .available_executors
                .values()
                .map(|x| x.borrow_mut().stats.take())
        });

        output
    }

    /// Returns a [`ExecutorStats`] struct with information about this Executor
    ///
    /// # Examples:
    ///
    /// ```
    /// use glommio::{executor, LocalExecutorBuilder};
    ///
    /// let ex = LocalExecutorBuilder::default()
    ///     .spawn(|| async move {
    ///         println!(
    ///             "Stats for executor: {:?}",
    ///             glommio::executor().executor_stats()
    ///         );
    ///     })
    ///     .unwrap();
    ///
    /// ex.join().unwrap();
    /// ```
    ///
    /// [`ExecutorStats`]: struct.ExecutorStats.html
    pub fn executor_stats(&self) -> ExecutorStats {
        #[cfg(not(feature = "native-tls"))]
        return LOCAL_EX.with(|local_ex| std::mem::take(&mut local_ex.queues.borrow_mut().stats));

        #[cfg(feature = "native-tls")]
        return std::mem::take(unsafe {
            &mut LOCAL_EX
                .as_ref()
                .expect("this thread doesn't have a LocalExecutor running")
                .queues
                .borrow_mut()
                .stats
        });
    }

    /// Returns an [`IoStats`] struct with information about IO performed by
    /// this executor's reactor
    ///
    /// # Examples:
    ///
    /// ```
    /// use glommio::LocalExecutorBuilder;
    ///
    /// let ex = LocalExecutorBuilder::default()
    ///     .spawn(|| async move {
    ///         println!("Stats for executor: {:?}", glommio::executor().io_stats());
    ///     })
    ///     .unwrap();
    ///
    /// ex.join().unwrap();
    /// ```
    ///
    /// [`IoStats`]: crate::IoStats
    pub fn io_stats(&self) -> IoStats {
        #[cfg(not(feature = "native-tls"))]
        return LOCAL_EX.with(|local_ex| local_ex.get_reactor().io_stats());

        #[cfg(feature = "native-tls")]
        return unsafe {
            LOCAL_EX
                .as_ref()
                .expect("this thread doesn't have a LocalExecutor running")
                .get_reactor()
                .io_stats()
        };
    }

    /// Returns an [`IoStats`] struct with information about IO performed from
    /// the provided TaskQueue by this executor's reactor
    ///
    /// # Examples:
    ///
    /// ```
    /// use glommio::{Latency, LocalExecutorBuilder, Shares};
    ///
    /// let ex = LocalExecutorBuilder::default()
    ///     .spawn(|| async move {
    ///         let new_tq = glommio::executor().create_task_queue(
    ///             Shares::default(),
    ///             Latency::NotImportant,
    ///             "test",
    ///         );
    ///         println!(
    ///             "Stats for executor: {:?}",
    ///             glommio::executor().task_queue_io_stats(new_tq)
    ///         );
    ///     })
    ///     .unwrap();
    ///
    /// ex.join().unwrap();
    /// ```
    ///
    /// [`IoStats`]: crate::IoStats
    pub fn task_queue_io_stats(&self, handle: TaskQueueHandle) -> Result<IoStats> {
        #[cfg(not(feature = "native-tls"))]
        return LOCAL_EX.with(|local_ex| {
            match local_ex.get_reactor().task_queue_io_stats(&handle) {
                Some(x) => Ok(x),
                None => Err(GlommioError::queue_not_found(handle.index)),
            }
        });

        #[cfg(feature = "native-tls")]
        return match unsafe {
            LOCAL_EX
                .as_ref()
                .expect("this thread doesn't have a LocalExecutor running")
                .get_reactor()
                .task_queue_io_stats(&handle)
        } {
            Some(x) => Ok(x),
            None => Err(GlommioError::queue_not_found(handle.index)),
        };
    }

    /// Spawns a task onto the current single-threaded executor.
    ///
    /// If called from a [`LocalExecutor`], the task is spawned on it.
    /// Otherwise, this method panics.
    ///
    /// Note that there is no guarantee of when the spawned task is scheduled.
    /// The current task can continue its execution or be preempted by the
    /// newly spawned task immediately. See the documentation for the
    /// top-level [`Task`] for examples.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::{LocalExecutor, Task};
    ///
    /// let local_ex = LocalExecutor::default();
    ///
    /// local_ex.run(async {
    ///     let task = glommio::executor().spawn_local(async { 1 + 2 });
    ///     assert_eq!(task.await, 3);
    /// });
    /// ```
    pub fn spawn_local<T>(&self, future: impl Future<Output = T> + 'static) -> Task<T>
    where
        T: 'static,
    {
        #[cfg(not(feature = "native-tls"))]
        return LOCAL_EX.with(|local_ex| Task::<T>(local_ex.spawn(future)));

        #[cfg(feature = "native-tls")]
        return Task::<T>(unsafe {
            LOCAL_EX
                .as_ref()
                .expect("this thread doesn't have a LocalExecutor running")
                .spawn(future)
        });
    }

    /// Spawns a task onto the current single-threaded executor, in a particular
    /// task queue
    ///
    /// If called from a [`LocalExecutor`], the task is spawned on it.
    /// Otherwise, this method panics.
    ///
    /// Note that there is no guarantee of when the spawned task is scheduled.
    /// The current task can continue its execution or be preempted by the
    /// newly spawned task immediately. See the documentation for the
    /// top-level [`Task`] for examples.
    ///
    /// # Examples
    ///
    /// ```
    /// # use glommio::{LocalExecutor, Shares, Task};
    ///
    /// # let local_ex = LocalExecutor::default();
    /// # local_ex.run(async {
    /// let handle = glommio::executor().create_task_queue(
    ///     Shares::default(),
    ///     glommio::Latency::NotImportant,
    ///     "test_queue",
    /// );
    /// let task = glommio::executor()
    ///     .spawn_local_into(async { 1 + 2 }, handle)
    ///     .expect("failed to spawn task");
    /// assert_eq!(task.await, 3);
    /// # });
    /// ```
    pub fn spawn_local_into<T>(
        &self,
        future: impl Future<Output = T> + 'static,
        handle: TaskQueueHandle,
    ) -> Result<Task<T>>
    where
        T: 'static,
    {
        #[cfg(not(feature = "native-tls"))]
        return LOCAL_EX.with(|local_ex| local_ex.spawn_into(future, handle).map(Task::<T>));

        #[cfg(feature = "native-tls")]
        return unsafe {
            LOCAL_EX
                .as_ref()
                .expect("this thread doesn't have a LocalExecutor running")
                .spawn_into(future, handle)
        }
        .map(Task::<T>);
    }

    /// Spawns a task onto the current single-threaded executor.
    ///
    /// If called from a [`LocalExecutor`], the task is spawned on it.
    ///
    /// Otherwise, this method panics.
    ///
    /// # Safety
    ///
    /// `ScopedTask` depends on `drop` running or `.await` being called for
    /// safety. See the struct [`ScopedTask`] for details.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::LocalExecutor;
    ///
    /// let local_ex = LocalExecutor::default();
    ///
    /// local_ex.run(async {
    ///     let non_static = 2;
    ///     let task = unsafe { glommio::executor().spawn_scoped_local(async { 1 + non_static }) };
    ///     assert_eq!(task.await, 3);
    /// });
    /// ```
    pub unsafe fn spawn_scoped_local<'a, T>(
        &self,
        future: impl Future<Output = T> + 'a,
    ) -> ScopedTask<'a, T> {
        #[cfg(not(feature = "native-tls"))]
        return LOCAL_EX.with(|local_ex| ScopedTask::<'a, T>(local_ex.spawn(future), PhantomData));

        #[cfg(feature = "native-tls")]
        return ScopedTask::<'a, T>(
            LOCAL_EX
                .as_ref()
                .expect("this thread doesn't have a LocalExecutor running")
                .spawn(future),
            PhantomData,
        );
    }

    /// Spawns a task onto the current single-threaded executor, in a particular
    /// task queue
    ///
    /// If called from a [`LocalExecutor`], the task is spawned on it.
    ///
    /// Otherwise, this method panics.
    ///
    /// # Safety
    ///
    /// `ScopedTask` depends on `drop` running or `.await` being called for
    /// safety. See the struct [`ScopedTask`] for details.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::{LocalExecutor, Shares};
    ///
    /// let local_ex = LocalExecutor::default();
    /// local_ex.run(async {
    ///     let handle = glommio::executor().create_task_queue(
    ///         Shares::default(),
    ///         glommio::Latency::NotImportant,
    ///         "test_queue",
    ///     );
    ///     let non_static = 2;
    ///     let task = unsafe {
    ///         glommio::executor()
    ///             .spawn_scoped_local_into(async { 1 + non_static }, handle)
    ///             .expect("failed to spawn task")
    ///     };
    ///     assert_eq!(task.await, 3);
    /// })
    /// ```
    pub unsafe fn spawn_scoped_local_into<'a, T>(
        &self,
        future: impl Future<Output = T> + 'a,
        handle: TaskQueueHandle,
    ) -> Result<ScopedTask<'a, T>> {
        #[cfg(not(feature = "native-tls"))]
        return LOCAL_EX.with(|local_ex| {
            local_ex
                .spawn_into(future, handle)
                .map(|x| ScopedTask::<'a, T>(x, PhantomData))
        });

        #[cfg(feature = "native-tls")]
        return LOCAL_EX
            .as_ref()
            .expect("this thread doesn't have a LocalExecutor running")
            .spawn_into(future, handle)
            .map(|x| ScopedTask::<'a, T>(x, PhantomData));
    }

    /// Spawns a blocking task into a background thread where blocking is
    /// acceptable.
    ///
    /// Glommio depends on cooperation from tasks in order to drive IO and meet
    /// latency requirements. Unyielding tasks are detrimental to the
    /// performance of the overall system, not just to the performance of
    /// the one stalling task.
    ///
    /// `spawn_blocking` is there as a last resort when a blocking task needs to
    /// be executed and cannot be made cooperative. Examples are:
    /// * Expensive syscalls that cannot use `io_uring`, such as `mmap`
    ///   (especially with `MAP_POPULATE`)
    /// * Calls to synchronous third-party code (compression, encoding, etc.)
    ///
    /// # Note
    ///
    /// *This method is not meant to be a way to achieve compute parallelism.*
    /// Distributing work across executors is the better way to achieve that.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::{LocalExecutor, Task};
    /// use std::time::Duration;
    ///
    /// let local_ex = LocalExecutor::default();
    ///
    /// local_ex.run(async {
    ///     let task = glommio::executor()
    ///         .spawn_blocking(|| {
    ///             std::thread::sleep(Duration::from_millis(100));
    ///         })
    ///         .await;
    /// });
    /// ```
    pub fn spawn_blocking<F, R>(&self, func: F) -> impl Future<Output = R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let result = Arc::new(Mutex::new(MaybeUninit::<R>::uninit()));
        let f_inner = enclose::enclose!((result) move || {result.lock().unwrap().write(func());});

        #[cfg(not(feature = "native-tls"))]
        let waiter =
            LOCAL_EX.with(move |local_ex| local_ex.reactor.run_blocking(Box::new(f_inner)));

        #[cfg(feature = "native-tls")]
        let waiter = unsafe {
            LOCAL_EX
                .as_ref()
                .expect("this thread doesn't have a LocalExecutor running")
                .reactor
                .run_blocking(Box::new(f_inner))
        };

        async move {
            let source = waiter.await;
            assert!(source.collect_rw().await.is_ok());
            unsafe {
                let res_arc = Arc::try_unwrap(result).expect("leak");
                let ret = std::mem::replace(
                    &mut *res_arc.lock().unwrap().deref_mut(),
                    MaybeUninit::<R>::uninit(),
                )
                .assume_init();
                ret
            }
        }
    }
}

#[cfg(test)]
mod test {
    use core::mem::MaybeUninit;
    use std::{
        cell::Cell,
        collections::HashMap,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
            Mutex,
        },
        task::Waker,
    };

    use futures::{
        future::{join, join_all, poll_fn},
        join,
    };

    use crate::{
        enclose,
        timer::{self, sleep, Timer},
        SharesManager,
    };

    use super::*;

    #[test]
    fn create_and_destroy_executor() {
        let mut var = Rc::new(RefCell::new(0));
        let local_ex = LocalExecutor::default();
        let varclone = var.clone();
        local_ex.run(async move {
            let mut m = varclone.borrow_mut();
            *m += 10;
        });

        let v = Rc::get_mut(&mut var).unwrap();
        let v = v.replace(0);
        assert_eq!(v, 10);
    }

    #[test]
    fn create_fail_to_bind() {
        // If you have a system with 4 billion CPUs let me know and I will
        // update this test.
        if LocalExecutorBuilder::new(Placement::Fixed(usize::MAX))
            .make()
            .is_ok()
        {
            unreachable!("Should have failed");
        }
    }

    #[test]
    fn bind_to_cpu_set_range() {
        // libc supports cpu ids up to 1023 and will use the intersection of values
        // specified by the cpu mask and those present on the system
        // https://man7.org/linux/man-pages/man2/sched_setaffinity.2.html#NOTES
        assert!(bind_to_cpu_set(vec![0, 1, 2, 3]).is_ok());
        assert!(bind_to_cpu_set(0..1024).is_ok());
        assert!(bind_to_cpu_set(0..1025).is_err());
    }

    #[test]
    fn create_and_bind() {
        if let Err(x) = LocalExecutorBuilder::new(Placement::Fixed(0)).make() {
            panic!("got error {:?}", x);
        }
    }

    #[test]
    #[should_panic]
    fn spawn_without_executor() {
        let _ = LocalExecutor::default();
        let _ = crate::spawn_local(async move {});
    }

    #[test]
    fn invalid_task_queue() {
        let local_ex = LocalExecutor::default();
        local_ex.run(async {
            let task = crate::spawn_local_into(
                async move {
                    unreachable!("Should not have executed this");
                },
                TaskQueueHandle { index: 1 },
            );

            if task.is_ok() {
                unreachable!("Should have failed");
            }
        });
    }

    #[test]
    fn ten_yielding_queues() {
        let local_ex = LocalExecutor::default();

        // 0 -> no one
        // 1 -> t1
        // 2 -> t2...
        let executed_last = Rc::new(RefCell::new(0));
        local_ex.run(async {
            let mut joins = Vec::with_capacity(10);
            for id in 1..11 {
                let exec = executed_last.clone();
                joins.push(crate::spawn_local(async move {
                    for _ in 0..10_000 {
                        let mut last = exec.borrow_mut();
                        assert_ne!(id, *last);
                        *last = id;
                        drop(last);
                        crate::executor().yield_task_queue_now().await;
                    }
                }));
            }
            futures::future::join_all(joins).await;
        });
    }

    #[test]
    fn task_with_latency_requirements() {
        let local_ex = LocalExecutor::default();

        local_ex.run(async {
            let not_latency = crate::executor().create_task_queue(
                Shares::default(),
                Latency::NotImportant,
                "test",
            );
            let latency = crate::executor().create_task_queue(
                Shares::default(),
                Latency::Matters(Duration::from_millis(2)),
                "testlat",
            );

            let nolat_started = Rc::new(RefCell::new(false));
            let lat_status = Rc::new(RefCell::new(false));

            // Loop until need_preempt is set. It is set to 2ms, but because this is a test
            // and can be running overcommited or in whichever shared infrastructure, we'll
            // allow the timer to fire in up to 1s. If it didn't fire in 1s, that's broken.
            let nolat = local_ex
                .spawn_into(
                    crate::enclose! { (nolat_started, lat_status)
                        async move {
                            *(nolat_started.borrow_mut()) = true;

                            let start = Instant::now();
                            // Now busy loop and make sure that we yield when we have too.
                            loop {
                                if *(lat_status.borrow()) {
                                    break; // Success!
                                }
                                if start.elapsed().as_secs() > 1 {
                                    panic!("Never received preempt signal");
                                }
                                crate::yield_if_needed().await;
                            }
                        }
                    },
                    not_latency,
                )
                .unwrap();

            let lat = local_ex
                .spawn_into(
                    crate::enclose! { (nolat_started, lat_status)
                        async move {
                            // In case we are executed first, yield to the the other task
                            loop {
                                if !(*(nolat_started.borrow())) {
                                    crate::executor().yield_task_queue_now().await;
                                } else {
                                    break;
                                }
                            }
                            *(lat_status.borrow_mut()) = true;
                        }
                    },
                    latency,
                )
                .unwrap();

            futures::join!(nolat, lat);
        });
    }

    #[test]
    fn current_task_queue_matches() {
        let local_ex = LocalExecutor::default();
        local_ex.run(async {
            let tq1 = crate::executor().create_task_queue(
                Shares::default(),
                Latency::NotImportant,
                "test1",
            );
            let tq2 = crate::executor().create_task_queue(
                Shares::default(),
                Latency::NotImportant,
                "test2",
            );

            let id1 = tq1.index;
            let id2 = tq2.index;
            let j0 = crate::spawn_local(async {
                assert_eq!(crate::executor().current_task_queue().index, 0);
            });
            let j1 = crate::spawn_local_into(
                async move {
                    assert_eq!(crate::executor().current_task_queue().index, id1);
                },
                tq1,
            )
            .unwrap();
            let j2 = crate::spawn_local_into(
                async move {
                    assert_eq!(crate::executor().current_task_queue().index, id2);
                },
                tq2,
            )
            .unwrap();
            futures::join!(j0, j1, j2);
        })
    }

    #[test]
    fn task_optimized_for_throughput() {
        let local_ex = LocalExecutor::default();

        local_ex.run(async {
            let tq1 = crate::executor().create_task_queue(
                Shares::default(),
                Latency::NotImportant,
                "test",
            );
            let tq2 = crate::executor().create_task_queue(
                Shares::default(),
                Latency::NotImportant,
                "testlat",
            );

            let first_started = Rc::new(RefCell::new(0));
            let second_status = Rc::new(RefCell::new(0));

            let first = local_ex
                .spawn_into(
                    crate::enclose! { (first_started, second_status)
                        async move {
                            let start = Instant::now();
                            // Now busy loop and make sure that we yield when we have too.
                            loop {
                                let mut count = first_started.borrow_mut();
                                *count += 1;

                                if start.elapsed().as_millis() >= 99 {
                                    break;
                                }

                                if *count < *(second_status.borrow()) {
                                    panic!("I was preempted but should not have been");
                                }
                                drop(count);
                                crate::yield_if_needed().await;
                            }
                        }
                    },
                    tq1,
                )
                .unwrap();

            let second = local_ex
                .spawn_into(
                    crate::enclose! { (first_started, second_status)
                        async move {
                            // In case we are executed first, yield to the the other task
                            loop {
                                let mut count = second_status.borrow_mut();
                                *count += 1;
                                if *count >= *(first_started.borrow()) {
                                    drop(count);
                                    crate::executor().yield_task_queue_now().await;
                                } else {
                                    break;
                                }
                            }
                        }
                    },
                    tq2,
                )
                .unwrap();

            futures::join!(first, second);
        });
    }

    #[test]
    fn test_detach() {
        let ex = LocalExecutor::default();

        ex.run(async {
            crate::spawn_local(async {
                loop {
                    crate::executor().yield_task_queue_now().await;
                }
            })
            .detach();

            Timer::new(Duration::from_micros(100)).await;
        });
    }

    /// As far as impl From<libc::timeval> for Duration is not allowed.
    fn from_timeval(v: libc::timeval) -> Duration {
        Duration::from_secs(v.tv_sec as u64) + Duration::from_micros(v.tv_usec as u64)
    }

    fn getrusage() -> Duration {
        let mut s0 = MaybeUninit::<libc::rusage>::uninit();
        let err = unsafe { libc::getrusage(libc::RUSAGE_THREAD, s0.as_mut_ptr()) };
        if err != 0 {
            panic!("getrusage error = {}", err);
        }
        let usage = unsafe { s0.assume_init() };
        from_timeval(usage.ru_utime) + from_timeval(usage.ru_stime)
    }

    #[test]
    fn test_no_spin() {
        let ex = LocalExecutor::default();
        let task_queue = ex.create_task_queue(
            Shares::default(),
            Latency::Matters(Duration::from_millis(10)),
            "my_tq",
        );
        let start = getrusage();
        ex.run(async {
            crate::spawn_local_into(
                async { timer::sleep(Duration::from_secs(1)).await },
                task_queue,
            )
            .expect("failed to spawn task")
            .await;
        });

        assert!(
            getrusage() - start < Duration::from_millis(10),
            "expected user time on LE is less than 10 millisecond"
        );
    }

    #[test]
    fn test_spin() {
        let dur = Duration::from_secs(1);
        let ex0 = LocalExecutorBuilder::default().make().unwrap();
        ex0.run(async {
            let ex0_ru_start = getrusage();
            timer::sleep(dur).await;
            let ex0_ru_finish = getrusage();

            assert!(
                ex0_ru_finish - ex0_ru_start < Duration::from_millis(10),
                "expected user time on LE0 is less than 10 millisecond"
            );
        });

        let ex = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spin_before_park(Duration::from_millis(100))
            .make()
            .unwrap();

        ex.run(async {
            let ex_ru_start = getrusage();
            timer::sleep(dur).await;
            let ex_ru_finish = getrusage();

            // 100 ms may have passed without us running for 100ms in case
            // there are other threads. Need to be a bit more relaxed
            assert!(
                ex_ru_finish - ex_ru_start >= Duration::from_millis(50),
                "expected user time on LE is much greater than 50 millisecond",
            );
        });
    }

    #[test]
    fn test_runtime_stats() {
        let dur = Duration::from_secs(2);
        let ex0 = LocalExecutorBuilder::default().make().unwrap();
        ex0.run(async {
            assert!(
                crate::executor().executor_stats().total_runtime() < Duration::from_nanos(10),
                "expected runtime on LE {:#?} is less than 10 ns",
                crate::executor().executor_stats().total_runtime()
            );

            let now = Instant::now();
            while now.elapsed().as_millis() < 200 {}
            crate::executor().yield_task_queue_now().await;
            assert!(
                crate::executor().executor_stats().total_runtime() >= Duration::from_millis(200),
                "expected runtime on LE0 {:#?} is greater than 200 ms",
                crate::executor().executor_stats().total_runtime()
            );

            timer::sleep(dur).await;
            assert!(
                crate::executor().executor_stats().total_runtime() < Duration::from_millis(400),
                "expected runtime on LE0 {:#?} is not greater than 400 ms",
                crate::executor().executor_stats().total_runtime()
            );
        });

        let ex = LocalExecutorBuilder::new(Placement::Fixed(0))
            // ensure entire sleep should spin
            .spin_before_park(Duration::from_secs(5))
            .make()
            .unwrap();
        ex.run(async {
            crate::spawn_local(async move {
                assert!(
                    crate::executor().executor_stats().total_runtime() < Duration::from_nanos(10),
                    "expected runtime on LE {:#?} is less than 10 ns",
                    crate::executor().executor_stats().total_runtime()
                );

                let now = Instant::now();
                while now.elapsed().as_millis() < 200 {}
                crate::executor().yield_task_queue_now().await;
                assert!(
                    crate::executor().executor_stats().total_runtime()
                        >= Duration::from_millis(200),
                    "expected runtime on LE {:#?} is greater than 200 ms",
                    crate::executor().executor_stats().total_runtime()
                );
                timer::sleep(dur).await;
                assert!(
                    crate::executor().executor_stats().total_runtime() < Duration::from_millis(400),
                    "expected runtime on LE {:#?} is not greater than 400 ms",
                    crate::executor().executor_stats().total_runtime()
                );
            })
            .await;
        });
    }

    // Spin for 2ms and then yield. How many shares we have should control how many
    // quantas we manage to execute.
    async fn work_quanta() {
        let now = Instant::now();
        while now.elapsed().as_millis() < 2 {}
        crate::executor().yield_task_queue_now().await;
    }

    macro_rules! test_static_shares {
        ( $s1:expr, $s2:expr, $work:block ) => {
            let local_ex = LocalExecutor::default();

            local_ex.run(async {
                // Run a latency queue, otherwise a queue will run for too long uninterrupted
                // and we'd have to run this test for a very long time for things to equalize.
                let tq1 = crate::executor().create_task_queue(
                    Shares::Static($s1),
                    Latency::Matters(Duration::from_millis(1)),
                    "test_1",
                );
                let tq2 = crate::executor().create_task_queue(
                    Shares::Static($s2),
                    Latency::Matters(Duration::from_millis(1)),
                    "test_2",
                );

                let tq1_count = Rc::new(Cell::new(0));
                let tq2_count = Rc::new(Cell::new(0));
                let now = Instant::now();

                let t1 = crate::spawn_local_into(
                    enclose! { (tq1_count, now) async move {
                        while now.elapsed().as_secs() < 5 {
                            $work;
                            tq1_count.replace(tq1_count.get() + 1);
                        }
                    }},
                    tq1,
                )
                .unwrap();

                let t2 = crate::spawn_local_into(
                    enclose! { (tq2_count, now ) async move {
                        while now.elapsed().as_secs() < 5 {
                            $work;
                            tq2_count.replace(tq2_count.get() + 1);
                        }
                    }},
                    tq2,
                )
                .unwrap();

                join!(t1, t2);

                let expected_ratio = $s2 as f64 / (($s2 + $s1) as f64);
                let actual_ratio =
                    tq2_count.get() as f64 / ((tq1_count.get() + tq2_count.get()) as f64);

                // Be gentle: we don't know if we're running against other threads, under which
                // conditions, etc
                assert!((expected_ratio - actual_ratio).abs() < 0.1);
            });
        };
    }

    #[test]
    fn test_shares_high_disparity_fat_task() {
        test_static_shares!(1000, 10, { work_quanta().await });
    }

    #[test]
    fn test_shares_low_disparity_fat_task() {
        test_static_shares!(1000, 1000, { work_quanta().await });
    }

    #[test]
    fn test_allocate_dma_buffer() {
        LocalExecutor::default().run(async {
            let mut buffer = crate::allocate_dma_buffer(42);
            assert_eq!(buffer.len(), 42);
            buffer.as_bytes_mut()[0] = 12;
            buffer.as_bytes_mut()[12] = 13;
            assert_eq!(buffer.as_bytes_mut().len(), 42);
            assert_eq!(buffer.as_bytes()[0], 12);
            assert_eq!(buffer.as_bytes()[12], 13);
        });
    }

    #[test]
    fn test_allocate_dma_buffer_global() {
        LocalExecutor::default().run(async {
            let mut buffer = crate::allocate_dma_buffer_global(42);
            assert_eq!(buffer.len(), 42);
            assert_eq!(buffer.len(), 42);
            buffer.as_bytes_mut()[0] = 12;
            buffer.as_bytes_mut()[12] = 13;
            assert_eq!(buffer.as_bytes_mut().len(), 42);
            assert_eq!(buffer.as_bytes()[0], 12);
            assert_eq!(buffer.as_bytes()[12], 13);
        });
    }

    struct DynamicSharesTest {
        shares: Cell<usize>,
    }

    impl DynamicSharesTest {
        fn new() -> Rc<Self> {
            Rc::new(Self {
                shares: Cell::new(0),
            })
        }
        fn tick(&self, millis: u64) {
            if millis < 1000 {
                self.shares.replace(1);
            } else {
                self.shares.replace(1000);
            }
        }
    }

    impl SharesManager for DynamicSharesTest {
        fn shares(&self) -> usize {
            self.shares.get()
        }

        fn adjustment_period(&self) -> Duration {
            Duration::from_millis(1)
        }
    }

    #[test]
    fn test_dynamic_shares() {
        let local_ex = LocalExecutor::default();

        local_ex.run(async {
            let bm = DynamicSharesTest::new();
            // Reference task queue.
            let tq1 = crate::executor().create_task_queue(
                Shares::Static(1000),
                Latency::Matters(Duration::from_millis(1)),
                "test_1",
            );
            let tq2 = crate::executor().create_task_queue(
                Shares::Dynamic(bm.clone()),
                Latency::Matters(Duration::from_millis(1)),
                "test_2",
            );

            let tq1_count = Rc::new(RefCell::new(vec![0, 0]));
            let tq2_count = Rc::new(RefCell::new(vec![0, 0]));
            let now = Instant::now();

            let t1 = crate::spawn_local_into(
                enclose! { (tq1_count, now) async move {
                    loop {
                        let secs = now.elapsed().as_secs();
                        if secs >= 2 {
                            break;
                        }
                        (*tq1_count.borrow_mut())[secs as usize] += 1;
                        crate::executor().yield_task_queue_now().await;
                    }
                }},
                tq1,
            )
            .unwrap();

            let t2 = crate::spawn_local_into(
                enclose! { (tq2_count, now, bm) async move {
                    loop {
                        let elapsed = now.elapsed();
                        let secs = elapsed.as_secs();
                        if secs >= 2 {
                            break;
                        }
                        bm.tick(elapsed.as_millis() as u64);
                        (*tq2_count.borrow_mut())[secs as usize] += 1;
                        crate::executor().yield_task_queue_now().await;
                    }
                }},
                tq2,
            )
            .unwrap();

            join!(t1, t2);
            // Keep this very simple because every new processor, every load condition, will
            // yield different results. All we want to validate is: for a large
            // part of the first two seconds shares were very low, we should
            // have received very low ratio. On the second half we should have
            // accumulated much more. Real numbers are likely much higher than
            // the targets, but those targets are safe.
            let ratios: Vec<f64> = tq1_count
                .borrow()
                .iter()
                .zip(tq2_count.borrow().iter())
                .map(|(x, y)| *y as f64 / *x as f64)
                .collect();
            assert!(ratios[1] > ratios[0]);
            assert!(ratios[0] < 0.25);
            assert!(ratios[1] > 0.50);
        });
    }

    #[test]
    fn multiple_spawn() {
        // Issue 241
        LocalExecutor::default().run(async {
            crate::spawn_local(async {}).detach().await;
            // In issue 241, the presence of the second detached waiter caused
            // the program to hang.
            crate::spawn_local(async {}).detach().await;
        });
    }

    #[test]
    #[should_panic(expected = "Message!")]
    fn panic_is_not_list() {
        LocalExecutor::default().run(async { panic!("Message!") });
    }

    struct TestFuture {
        w: Arc<Mutex<Option<Waker>>>,
    }

    impl Future for TestFuture {
        type Output = ();
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let mut w = self.w.lock().unwrap();
            match w.take() {
                Some(_) => Poll::Ready(()),
                None => {
                    *w = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        }
    }

    #[test]
    fn cross_executor_wake_by_ref() {
        let w = Arc::new(Mutex::new(None));
        let t = w.clone();

        let fut = TestFuture { w };

        let ex1 = LocalExecutorBuilder::default()
            .spawn(|| async move {
                fut.await;
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::default()
            .spawn(|| async move {
                loop {
                    sleep(Duration::from_secs(1)).await;
                    let w = t.lock().unwrap();
                    if let Some(ref x) = *w {
                        x.wake_by_ref();
                        return;
                    }
                }
            })
            .unwrap();

        ex1.join().unwrap();
        ex2.join().unwrap();
    }

    #[test]
    fn cross_executor_wake_by_value() {
        let w = Arc::new(Mutex::new(None));
        let t = w.clone();

        let fut = TestFuture { w };

        let ex1 = LocalExecutorBuilder::default()
            .spawn(|| async move {
                fut.await;
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::default()
            .spawn(|| async move {
                loop {
                    sleep(Duration::from_secs(1)).await;
                    let w = t.lock().unwrap();
                    if let Some(x) = w.clone() {
                        x.wake();
                        return;
                    }
                }
            })
            .unwrap();

        ex1.join().unwrap();
        ex2.join().unwrap();
    }

    // Wakes up the waker in a remote executor
    #[test]
    fn cross_executor_wake_with_join_handle() {
        let w = Arc::new(Mutex::new(None));
        let t = w.clone();

        let fut = TestFuture { w };

        let ex1 = LocalExecutorBuilder::default()
            .spawn(|| async move {
                let x = crate::spawn_local(fut).detach();
                x.await;
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::default()
            .spawn(|| async move {
                loop {
                    sleep(Duration::from_secs(1)).await;
                    let w = t.lock().unwrap();
                    if let Some(x) = w.clone() {
                        x.wake();
                        return;
                    }
                }
            })
            .unwrap();

        ex1.join().unwrap();
        ex2.join().unwrap();
    }

    // The other side won't be alive to get the notification. We should still
    // survive.
    #[test]
    fn cross_executor_wake_early_drop() {
        let w = Arc::new(Mutex::new(None));
        let t = w.clone();

        let fut = TestFuture { w };

        let ex1 = LocalExecutorBuilder::default()
            .spawn(|| async move {
                let _drop = futures_lite::future::poll_once(fut).await;
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::default()
            .spawn(|| async move {
                loop {
                    sleep(Duration::from_secs(1)).await;
                    let w = t.lock().unwrap();
                    if let Some(ref x) = *w {
                        x.wake_by_ref();
                        return;
                    }
                }
            })
            .unwrap();

        ex1.join().unwrap();
        ex2.join().unwrap();
    }

    // The other side won't be alive to get the notification and even worse, we hold
    // a waker that we notify after the first executor is surely dead. We should
    // still survive.
    #[test]
    fn cross_executor_wake_hold_waker() {
        let w = Arc::new(Mutex::new(None));
        let t = w.clone();

        let fut = TestFuture { w };

        let ex1 = LocalExecutorBuilder::default()
            .spawn(|| async move {
                let _drop = futures_lite::future::poll_once(fut).await;
            })
            .unwrap();
        ex1.join().unwrap();

        let ex2 = LocalExecutorBuilder::default()
            .spawn(|| async move {
                let w = t.lock().unwrap().clone().unwrap();
                w.wake_by_ref();
            })
            .unwrap();

        ex2.join().unwrap();
    }

    #[test]
    fn executor_pool_builder() {
        let nr_cpus = 4;

        let count = Arc::new(AtomicUsize::new(0));
        let handles = LocalExecutorPoolBuilder::new(PoolPlacement::Unbound(nr_cpus))
            .on_all_shards({
                let count = Arc::clone(&count);
                || async move { count.fetch_add(1, Ordering::Relaxed) }
            })
            .unwrap();

        let _: std::thread::ThreadId = handles.handles[0].thread().id();

        assert_eq!(nr_cpus, handles.handles().iter().count());

        let mut fut_output = handles
            .join_all()
            .into_iter()
            .map(Result::unwrap)
            .collect::<Vec<_>>();

        fut_output.sort_unstable();

        assert_eq!(fut_output, (0..nr_cpus).into_iter().collect::<Vec<_>>());

        assert_eq!(nr_cpus, count.load(Ordering::Relaxed));
    }

    #[test]
    fn executor_invalid_executor_count() {
        assert!(
            LocalExecutorPoolBuilder::new(PoolPlacement::Unbound(0))
                .on_all_shards(|| async move {})
                .is_err()
        );
    }

    #[test]
    fn executor_pool_builder_placements() {
        let cpu_set = CpuSet::online().unwrap();
        assert!(!cpu_set.is_empty());

        for nn in 1..2 {
            let nr_execs = nn * cpu_set.len();
            let mut placements = vec![
                PoolPlacement::Unbound(nr_execs),
                PoolPlacement::Fenced(nr_execs, cpu_set.clone()),
                PoolPlacement::MaxSpread(nr_execs, None),
                PoolPlacement::MaxSpread(nr_execs, Some(cpu_set.clone())),
                PoolPlacement::MaxPack(nr_execs, None),
                PoolPlacement::MaxPack(nr_execs, Some(cpu_set.clone())),
            ];

            for pp in placements.drain(..) {
                let ids = Arc::new(Mutex::new(HashMap::new()));
                let cpus = Arc::new(Mutex::new(HashMap::new()));
                let cpu_hard_bind =
                    !matches!(pp, PoolPlacement::Unbound(_) | PoolPlacement::Fenced(_, _));

                let handles = LocalExecutorPoolBuilder::new(pp)
                    .on_all_shards({
                        let ids = Arc::clone(&ids);
                        let cpus = Arc::clone(&cpus);
                        || async move {
                            ids.lock()
                                .unwrap()
                                .entry(crate::executor().id())
                                .and_modify(|e| *e += 1)
                                .or_insert(1);

                            let pid = nix::unistd::Pid::from_raw(0);
                            let cpu = nix::sched::sched_getaffinity(pid).unwrap();
                            cpus.lock()
                                .unwrap()
                                .entry(cpu)
                                .and_modify(|e| *e += 1)
                                .or_insert(1);
                        }
                    })
                    .unwrap();

                assert_eq!(nr_execs, handles.handles().len());
                handles
                    .join_all()
                    .into_iter()
                    .for_each(|r| assert!(r.is_ok()));

                assert_eq!(nr_execs, ids.lock().unwrap().len());
                ids.lock().unwrap().values().for_each(|v| assert_eq!(*v, 1));

                if cpu_hard_bind {
                    assert_eq!(nr_execs, cpus.lock().unwrap().len());
                    cpus.lock()
                        .unwrap()
                        .values()
                        .for_each(|v| assert_eq!(*v, nn));
                }
            }
        }
    }

    #[test]
    fn executor_pool_builder_shards_limit() {
        let cpu_set = CpuSet::online().unwrap();
        assert!(!cpu_set.is_empty());

        // test: confirm that we can always get shards up to the # of cpus
        {
            let mut placements = vec![
                (false, PoolPlacement::Unbound(cpu_set.len())),
                (false, PoolPlacement::Fenced(cpu_set.len(), cpu_set.clone())),
                (true, PoolPlacement::MaxSpread(cpu_set.len(), None)),
                (
                    true,
                    PoolPlacement::MaxSpread(cpu_set.len(), Some(cpu_set.clone())),
                ),
                (true, PoolPlacement::MaxPack(cpu_set.len(), None)),
                (
                    true,
                    PoolPlacement::MaxPack(cpu_set.len(), Some(cpu_set.clone())),
                ),
            ];

            for (_shard_limited, p) in placements.drain(..) {
                LocalExecutorPoolBuilder::new(p)
                    .on_all_shards(|| async move {})
                    .unwrap()
                    .join_all();
            }
        }

        // test: confirm that some placements fail when shards are # of cpus + 1
        {
            let mut placements = vec![
                (false, PoolPlacement::Unbound(1 + cpu_set.len())),
                (
                    false,
                    PoolPlacement::Fenced(1 + cpu_set.len(), cpu_set.clone()),
                ),
                (true, PoolPlacement::MaxSpread(1 + cpu_set.len(), None)),
                (
                    true,
                    PoolPlacement::MaxSpread(1 + cpu_set.len(), Some(cpu_set.clone())),
                ),
                (true, PoolPlacement::MaxPack(1 + cpu_set.len(), None)),
                (
                    true,
                    PoolPlacement::MaxPack(1 + cpu_set.len(), Some(cpu_set)),
                ),
            ];

            for (shard_limited, p) in placements.drain(..) {
                match LocalExecutorPoolBuilder::new(p).on_all_shards(|| async move {}) {
                    Ok(handles) => {
                        handles.join_all();
                        assert!(!shard_limited);
                    }
                    Err(_) => assert!(shard_limited),
                }
            }
        }
    }

    #[test]
    fn scoped_task() {
        LocalExecutor::default().run(async {
            let mut a = 1;
            unsafe {
                crate::spawn_scoped_local(async {
                    a = 2;
                })
                .await;
            }
            crate::executor().yield_task_queue_now().await;
            assert_eq!(a, 2);

            let mut a = 1;
            let do_later = unsafe {
                crate::spawn_scoped_local(async {
                    a = 2;
                })
            };

            crate::executor().yield_task_queue_now().await;
            do_later.await;
            assert_eq!(a, 2);
        });
    }

    #[test]
    fn executor_pool_builder_thread_panic() {
        let nr_execs = 8;
        let res = LocalExecutorPoolBuilder::new(PoolPlacement::Unbound(nr_execs))
            .on_all_shards(|| async move { panic!("join handle will be Err") })
            .unwrap()
            .join_all();

        assert_eq!(nr_execs, res.len());
        assert!(res.into_iter().all(|r| r.is_err()));
    }

    #[test]
    fn executor_pool_builder_return_values() {
        let nr_execs = 8;
        let x = Arc::new(AtomicUsize::new(0));
        let mut values = LocalExecutorPoolBuilder::new(PoolPlacement::Unbound(nr_execs))
            .on_all_shards(|| async move { x.fetch_add(1, Ordering::Relaxed) })
            .unwrap()
            .join_all()
            .into_iter()
            .map(Result::unwrap)
            .collect::<Vec<_>>();

        values.sort_unstable();
        assert_eq!(values, (0..nr_execs).into_iter().collect::<Vec<_>>());
    }

    #[test]
    fn executor_pool_builder_spawn_cancel() {
        let nr_shards = 8;
        let builder = LocalExecutorPoolBuilder::new(PoolPlacement::Unbound(nr_shards));
        let nr_exectuted = Arc::new(AtomicUsize::new(0));

        let fut_gen = {
            let nr_exectuted = Arc::clone(&nr_exectuted);
            || async move {
                nr_exectuted.fetch_add(1, Ordering::Relaxed);
                unreachable!("should not execute")
            }
        };

        let mut handles = PoolThreadHandles::new();
        let mut cpu_set_gen = placement::CpuSetGenerator::pool(builder.placement.clone()).unwrap();
        let latch = Latch::new(builder.placement.executor_count());

        let ii_cxl = 2;
        for ii in 0..builder.placement.executor_count() {
            if ii == nr_shards - ii_cxl {
                std::thread::sleep(std::time::Duration::from_millis(100));
                assert!(ii_cxl <= latch.cancel().unwrap());
            }
            match builder.spawn_thread(&mut cpu_set_gen, &latch, fut_gen.clone()) {
                Ok(handle) => handles.push(handle),
                Err(_) => break,
            }
        }

        assert_eq!(0, nr_exectuted.load(Ordering::Relaxed));
        assert_eq!(nr_shards, handles.handles.len());
        handles.join_all().into_iter().for_each(|s| {
            assert!(format!("{}", s.unwrap_err()).contains("spawn failed"));
        });
    }

    #[should_panic]
    #[test]
    fn executor_inception() {
        LocalExecutor::default().run(async {
            LocalExecutor::default().run(async {});
        });
    }

    enum TaskState {
        Pending(Option<Waker>),
        Ready,
    }

    // following four tests are regression ones for https://github.com/DataDog/glommio/issues/379.
    // here we test against task reference count underflow
    // test includes two scenarios, with join handles and with sleep, for each case
    // we test both, wake and wake_by_ref
    #[test]
    fn wake_by_ref_refcount_underflow_with_join_handle() {
        LocalExecutor::default().run(async {
            let slot: Rc<RefCell<TaskState>> = Rc::new(RefCell::new(TaskState::Pending(None)));
            let cloned_slot = slot.clone();
            let jh = crate::spawn_local(async move {
                // first task, places waker of self into slot, when polled checks for result, if
                // it's ready, returns Ready, otherwise return Pending
                poll_fn::<(), _>(|cx| {
                    let current = &mut *cloned_slot.borrow_mut();
                    match current {
                        TaskState::Pending(maybe_waker) => match maybe_waker {
                            Some(_) => unreachable!(),
                            None => {
                                *current = TaskState::Pending(Some(cx.waker().clone()));
                                Poll::Pending
                            }
                        },
                        TaskState::Ready => Poll::Ready(()),
                    }
                })
                .await;
            })
            .detach();
            let jh2 = crate::spawn_local(async move {
                // second task, checks slot for first task waker, wakes it by ref, and then it
                // is dropped.
                let current = &mut *slot.borrow_mut();
                match current {
                    TaskState::Pending(maybe_waker) => {
                        let waker = maybe_waker.take().unwrap();
                        waker.wake_by_ref();
                        *current = TaskState::Ready; // <-- waker dropped here, refcount is zero
                    }
                    TaskState::Ready => unreachable!(), // task cannot be ready at this time
                }
            })
            .detach();
            join_all(vec![jh, jh2]).await;
        });
    }

    #[test]
    fn wake_by_ref_refcount_underflow_with_sleep() {
        LocalExecutor::default().run(async {
            let slot: Rc<RefCell<TaskState>> = Rc::new(RefCell::new(TaskState::Pending(None)));
            let cloned_slot = slot.clone();
            crate::spawn_local(async move {
                poll_fn::<(), _>(|cx| {
                    let current = &mut *cloned_slot.borrow_mut();
                    match current {
                        TaskState::Pending(maybe_waker) => match maybe_waker {
                            Some(_) => unreachable!(),
                            None => {
                                *current = TaskState::Pending(Some(cx.waker().clone()));
                                Poll::Pending
                            }
                        },
                        TaskState::Ready => Poll::Ready(()),
                    }
                })
                .await;
            })
            .detach();
            crate::spawn_local(async move {
                let current = &mut *slot.borrow_mut();
                match current {
                    TaskState::Pending(maybe_waker) => {
                        let waker = maybe_waker.take().unwrap();
                        waker.wake_by_ref();
                        *current = TaskState::Ready;
                    }
                    TaskState::Ready => unreachable!(),
                }
            })
            .detach();
            timer::sleep(Duration::from_millis(1)).await;
        });
    }

    #[test]
    fn wake_refcount_underflow_with_join_handle() {
        LocalExecutor::default().run(async {
            let slot: Rc<RefCell<TaskState>> = Rc::new(RefCell::new(TaskState::Pending(None)));
            let cloned_slot = slot.clone();
            let jh = crate::spawn_local(async move {
                poll_fn::<(), _>(|cx| {
                    let current = &mut *cloned_slot.borrow_mut();
                    match current {
                        TaskState::Pending(maybe_waker) => match maybe_waker {
                            Some(_) => unreachable!(),
                            None => {
                                *current = TaskState::Pending(Some(cx.waker().clone()));
                                Poll::Pending
                            }
                        },
                        TaskState::Ready => Poll::Ready(()),
                    }
                })
                .await;
            })
            .detach();
            let jh2 = crate::spawn_local(async move {
                let current = &mut *slot.borrow_mut();
                match current {
                    TaskState::Pending(maybe_waker) => {
                        let waker = maybe_waker.take().unwrap();
                        waker.wake();
                        *current = TaskState::Ready;
                    }
                    TaskState::Ready => unreachable!(),
                }
            })
            .detach();
            join_all(vec![jh, jh2]).await;
        });
    }

    #[test]
    fn wake_refcount_underflow_with_sleep() {
        LocalExecutor::default().run(async {
            let slot: Rc<RefCell<TaskState>> = Rc::new(RefCell::new(TaskState::Pending(None)));
            let cloned_slot = slot.clone();
            crate::spawn_local(async move {
                poll_fn::<(), _>(|cx| {
                    let current = &mut *cloned_slot.borrow_mut();
                    match current {
                        TaskState::Pending(maybe_waker) => match maybe_waker {
                            Some(_) => unreachable!(),
                            None => {
                                *current = TaskState::Pending(Some(cx.waker().clone()));
                                Poll::Pending
                            }
                        },
                        TaskState::Ready => Poll::Ready(()),
                    }
                })
                .await;
            })
            .detach();
            crate::spawn_local(async move {
                let current = &mut *slot.borrow_mut();
                match current {
                    TaskState::Pending(maybe_waker) => {
                        let waker = maybe_waker.take().unwrap();
                        waker.wake();
                        *current = TaskState::Ready;
                    }
                    TaskState::Ready => unreachable!(),
                }
            })
            .detach();
            timer::sleep(Duration::from_millis(1)).await;
        });
    }

    #[test]
    fn blocking_function() {
        LocalExecutor::default().run(async {
            let started = Instant::now();

            let blocking = executor().spawn_blocking(enclose!((started) move || {
                let now = Instant::now();
                while now.elapsed() < Duration::from_millis(100) {}
                started.elapsed()
            }));
            let coop = enclose!((started) async move {
                let now = Instant::now();
                while now.elapsed() < Duration::from_millis(100) {
                    yield_if_needed().await;
                }
                started.elapsed()
            });

            let (blocking, coop) = join(blocking, coop).await;

            assert!(blocking.as_millis() >= 100 && blocking.as_millis() < 150);
            assert!(coop.as_millis() >= 100 && coop.as_millis() < 150);
        });
    }

    #[test]
    fn blocking_function_parallelism() {
        LocalExecutorBuilder::new(Placement::Unbound)
            .blocking_thread_pool_placement(PoolPlacement::Unbound(4))
            .spawn(|| async {
                let started = Instant::now();
                let mut blocking = vec![];

                for _ in 0..5 {
                    blocking.push(executor().spawn_blocking(enclose!((started) move || {
                        let now = Instant::now();
                        while now.elapsed() < Duration::from_millis(100) {}
                        started.elapsed()
                    })));
                }

                // we created 5 blocking jobs each taking 100ms but our thread pool only has 4
                // threads. SWe expect one of those jobs to take twice as long as the others.

                let mut ts = join_all(blocking.into_iter()).await;
                assert_eq!(ts.len(), 5);

                ts.sort_unstable();
                for ts in ts.iter().take(4) {
                    assert!(ts.as_millis() >= 100 && ts.as_millis() < 150);
                }
                assert!(ts[4].as_millis() >= 200 && ts[4].as_millis() < 250);
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn blocking_pool_invalid_placement() {
        let ret = LocalExecutorBuilder::new(Placement::Unbound)
            .blocking_thread_pool_placement(PoolPlacement::Unbound(0))
            .spawn(|| async {})
            .unwrap()
            .join();
        assert!(ret.is_err());
    }

    #[test]
    fn local_executor_unset() {
        LocalExecutor::default().run(async {});

        #[cfg(not(feature = "native-tls"))]
        assert!(!LOCAL_EX.is_set());

        #[cfg(feature = "native-tls")]
        assert!(unsafe { LOCAL_EX.is_null() });
    }

    #[test]
    fn local_executor_unset_when_panic() {
        let res = std::panic::catch_unwind(|| {
            LocalExecutor::default().run(async {
                panic!("uh oh!");
            });
        });
        assert!(res.is_err());

        #[cfg(not(feature = "native-tls"))]
        assert!(!LOCAL_EX.is_set());

        #[cfg(feature = "native-tls")]
        assert!(unsafe { LOCAL_EX.is_null() });
    }
}
