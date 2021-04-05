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
//! use glommio::{timer::Timer, LocalExecutor, LocalExecutorBuilder};
//!
//! for i in 0..4 {
//!     std::thread::spawn(move || {
//!         let builder = LocalExecutorBuilder::new().pin_to_cpu(i);
//!         let local_ex = builder.make().expect("failed to spawn local executor");
//!         local_ex.run(async {
//!             Timer::new(std::time::Duration::from_millis(100)).await;
//!             println!("Hello world!");
//!         });
//!     });
//! }
//! ```

#![warn(missing_docs, missing_debug_implementations)]

use std::{
    cell::RefCell,
    collections::{hash_map::Entry, BinaryHeap},
    future::Future,
    io,
    pin::Pin,
    rc::Rc,
    sync::Arc,
    task::{Context, Poll},
    thread::{Builder, JoinHandle},
    time::{Duration, Instant},
};

use futures_lite::pin;
use scoped_tls::scoped_thread_local;

use crate::{
    multitask,
    parking,
    sys,
    task::{self, waker_fn::waker_fn},
    GlommioError,
    IoRequirements,
    Latency,
    Reactor,
    Shares,
};
use ahash::AHashMap;

/// Result type alias that removes the need to specify a type parameter
/// that's only valid in the channel variants of the error. Otherwise it
/// might be confused with the error (`E`) that a result usually has in
/// the second type parameter.
type Result<T> = crate::Result<T, ()>;

scoped_thread_local!(static LOCAL_EX: LocalExecutor);

pub(crate) fn executor_id() -> Option<usize> {
    if LOCAL_EX.is_set() {
        Some(LOCAL_EX.with(|ex| ex.id))
    } else {
        None
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
/// An opaque handle indicating in which queue a group of tasks will execute.
/// Tasks in the same group will execute in FIFO order but no guarantee is made
/// about ordering on different task queues.
pub struct TaskQueueHandle {
    index: usize,
}

impl Default for TaskQueueHandle {
    fn default() -> Self {
        TaskQueueHandle { index: 0 }
    }
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
    fn new<S>(index: usize, name: S, shares: Shares, ioreq: IoRequirements) -> Rc<RefCell<Self>>
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

macro_rules! to_io_error {
    ($error:expr) => {{
        match $error {
            Ok(x) => Ok(x),
            Err(nix::Error::Sys(_)) => Err(io::Error::last_os_error()),
            Err(nix::Error::InvalidUtf8) => Err(io::Error::new(io::ErrorKind::InvalidInput, "")),
            Err(nix::Error::InvalidPath) => Err(io::Error::new(io::ErrorKind::InvalidInput, "")),
            Err(nix::Error::UnsupportedOperation) => Err(io::Error::new(io::ErrorKind::Other, "")),
        }
    }};
}

fn bind_to_cpu(cpu: usize) -> Result<()> {
    let mut cpuset = nix::sched::CpuSet::new();
    to_io_error!(&cpuset.set(cpu as usize))?;
    let pid = nix::unistd::Pid::from_raw(0);
    to_io_error!(nix::sched::sched_setaffinity(pid, &cpuset)).map_err(Into::into)
}

// Dealing with references would imply getting an Rc, RefCells, and all of that
// Stats should be copied unfrequently, and if you have enough stats to fill a
// Kb of data from a single source, maybe you should rethink your life choices.
#[derive(Debug, Copy, Clone)]
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
    index: usize,
    // so we can easily produce a handle
    reciprocal_shares: u64,
    queue_selected: u64,
    runtime: Duration,
}

impl TaskQueueStats {
    fn new(index: usize, reciprocal_shares: u64) -> Self {
        Self {
            index,
            reciprocal_shares,
            runtime: Duration::from_nanos(0),
            queue_selected: 0,
        }
    }

    /// Returns a numeric ID that uniquely identifies this Task queue
    pub fn index(&self) -> usize {
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
    /// amount of time this queue tends to runs for
    pub fn queue_selected(&self) -> u64 {
        self.queue_selected
    }
}

#[derive(Debug)]
struct ExecutorQueues {
    active_executors: BinaryHeap<Rc<RefCell<TaskQueue>>>,
    available_executors: AHashMap<usize, Rc<RefCell<TaskQueue>>>,
    active_executing: Option<Rc<RefCell<TaskQueue>>>,
    executor_index: usize,
    last_vruntime: u64,
    preempt_timer_duration: Duration,
    default_preempt_timer_duration: Duration,
    spin_before_park: Option<Duration>,
    stats: ExecutorStats,
}

impl ExecutorQueues {
    fn new(preempt_timer_duration: Duration) -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(ExecutorQueues {
            active_executors: BinaryHeap::new(),
            available_executors: AHashMap::new(),
            active_executing: None,
            executor_index: 1, // 0 is the default
            last_vruntime: 0,
            preempt_timer_duration,
            default_preempt_timer_duration: preempt_timer_duration,
            spin_before_park: None,
            stats: ExecutorStats::new(),
        }))
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
            state.vruntime = self.last_vruntime;
            state.active = true;
            drop(state);
            self.active_executors.push(queue);
            self.reevaluate_preempt_timer();
        }
    }
}

/// [`LocalExecutor`] factory, which can be used in order to configure the
/// properties of a new [`LocalExecutor`].
///
/// Methods can be chained on it in order to configure it.
///
/// The [`spawn`] method will take ownership of the builder and create an
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
/// let builder = LocalExecutorBuilder::new();
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
    // The id of a CPU to bind the current (or yet to be created) thread
    binding: Option<usize>,
    /// Spin for duration before parking a reactor
    spin_before_park: Option<Duration>,
    /// A name for the thread-to-be (if any), for identification in panic
    /// messages
    name: String,
    /// Amount of memory to reserve for storage I/O. This will be preallocated
    /// and registered with io_uring. It is still possible to use more than
    /// that but it will come from the standard allocator and performance
    /// will suffer. Defaults to 10MB.
    io_memory: usize,
    /// How often to yield to other task queues
    preempt_timer_duration: Duration,
}

impl LocalExecutorBuilder {
    /// Generates the base configuration for spawning a [`LocalExecutor`], from
    /// which configuration methods can be chained.
    pub fn new() -> LocalExecutorBuilder {
        LocalExecutorBuilder {
            binding: None,
            spin_before_park: None,
            name: String::from("unnamed"),
            io_memory: 10 << 20,
            preempt_timer_duration: Duration::from_millis(100),
        }
    }

    /// Sets the new executor's affinity to the provided CPU
    pub fn pin_to_cpu(mut self, cpu: usize) -> LocalExecutorBuilder {
        self.binding = Some(cpu);
        self
    }

    /// Spin for duration before parking a reactor
    pub fn spin_before_park(mut self, spin: Duration) -> LocalExecutorBuilder {
        self.spin_before_park = Some(spin);
        self
    }

    /// Names the thread-to-be. Currently the name is used for identification
    /// only in panic messages.
    pub fn name(mut self, name: &str) -> LocalExecutorBuilder {
        self.name = String::from(name);
        self
    }

    /// Amount of memory to reserve for storage I/O. This will be preallocated
    /// and registered with io_uring. It is still possible to use more than
    /// that but it will come from the standard allocator and performance
    /// will suffer.
    ///
    /// The system will always try to allocate at least 64kB for I/O memory, and
    /// the default is 10MB.
    pub fn io_memory(mut self, io_memory: usize) -> LocalExecutorBuilder {
        self.io_memory = io_memory;
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
    /// [`need_preempt`]: Task::need_preempt
    /// [`Latency`]: crate::Latency
    pub fn preempt_timer(mut self, dur: Duration) -> LocalExecutorBuilder {
        self.preempt_timer_duration = dur;
        self
    }

    /// Make a new [`LocalExecutor`] by taking ownership of the Builder, and
    /// returns a [`Result`](crate::Result) to the executor.
    /// # Examples
    ///
    /// ```
    /// use glommio::LocalExecutorBuilder;
    ///
    /// let local_ex = LocalExecutorBuilder::new().make().unwrap();
    /// ```
    pub fn make(self) -> Result<LocalExecutor> {
        let notifier = sys::new_sleep_notifier()?;
        let mut le = LocalExecutor::new(notifier, self.io_memory, self.preempt_timer_duration);
        if let Some(cpu) = self.binding {
            le.bind_to_cpu(cpu)?;
            le.queues.borrow_mut().spin_before_park = self.spin_before_park;
        }
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
    /// executor is thread-local, it can guarantee that the futures will not
    /// be Sent once started.
    ///
    /// # Panics
    ///
    /// This function panics if creating the thread or the executor fails. If
    /// you need more fine-grained error handling consider initializing
    /// those entities manually.
    ///
    /// # Example
    ///
    /// ```
    /// use glommio::LocalExecutorBuilder;
    ///
    /// let handle = LocalExecutorBuilder::new()
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
    #[must_use = "This spawns an executor on a thread, so you must acquire its handle and then \
                  join() to keep it alive"]
    pub fn spawn<G, F, T>(self, fut_gen: G) -> Result<JoinHandle<()>>
    where
        G: FnOnce() -> F + std::marker::Send + 'static,
        F: Future<Output = T> + 'static,
    {
        let notifier = sys::new_sleep_notifier()?;
        let name = format!("{}-{}", self.name, notifier.id());

        Builder::new()
            .name(name)
            .spawn(move || {
                let mut le =
                    LocalExecutor::new(notifier, self.io_memory, self.preempt_timer_duration);
                if let Some(cpu) = self.binding {
                    le.bind_to_cpu(cpu).unwrap();
                    le.queues.borrow_mut().spin_before_park = self.spin_before_park;
                }
                le.init();
                le.run(async move {
                    fut_gen().await;
                })
            })
            .map_err(Into::into)
    }
}

impl Default for LocalExecutorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) fn maybe_activate(tq: Rc<RefCell<TaskQueue>>) {
    LOCAL_EX.with(|local_ex| {
        let mut queues = local_ex.queues.borrow_mut();
        queues.maybe_activate(tq)
    })
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
    reactor: Rc<parking::Reactor>,
}

impl LocalExecutor {
    fn get_reactor(&self) -> Rc<Reactor> {
        self.reactor.clone()
    }

    fn bind_to_cpu(&self, cpu: usize) -> Result<()> {
        bind_to_cpu(cpu)
    }

    fn init(&mut self) {
        let io_requirements = IoRequirements::new(Latency::NotImportant, 0);
        self.queues.borrow_mut().available_executors.insert(
            0,
            TaskQueue::new(0, "default", Shares::Static(1000), io_requirements),
        );
    }

    fn new(
        notifier: Arc<sys::SleepNotifier>,
        io_memory: usize,
        preempt_timer: Duration,
    ) -> LocalExecutor {
        let p = parking::Parker::new();
        LocalExecutor {
            queues: ExecutorQueues::new(preempt_timer),
            parker: p,
            id: notifier.id(),
            reactor: Rc::new(parking::Reactor::new(notifier, io_memory)),
        }
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
        let tq = TaskQueue::new(index, name, shares, io_requirements);

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
        TaskQueueHandle {
            index: self
                .queues
                .borrow()
                .active_executing
                .as_ref()
                .unwrap()
                .borrow()
                .stats
                .index,
        }
    }

    fn mark_me_for_yield(&self) {
        let queues = self.queues.borrow();
        let mut me = queues.active_executing.as_ref().unwrap().borrow_mut();
        me.yielded = true;
    }

    fn spawn<T>(&self, future: impl Future<Output = T>) -> Task<T> {
        let tq = self
            .queues
            .borrow()
            .active_executing
            .clone() // this clone is cheap because we clone an `Option<Rc<_>>`
            .or_else(|| self.get_queue(&TaskQueueHandle { index: 0 }))
            .unwrap();

        let id = self.id;
        let ex = tq.borrow().ex.clone();
        Task(ex.spawn(id, tq, future))
    }

    fn spawn_into<T, F>(&self, future: F, handle: TaskQueueHandle) -> Result<Task<T>>
    where
        F: Future<Output = T>,
    {
        let tq = self
            .get_queue(&handle)
            .ok_or_else(|| GlommioError::queue_not_found(handle.index))?;
        let ex = tq.borrow().ex.clone();
        let id = self.id;

        Ok(Task(ex.spawn(id, tq, future)))
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
        while !self.need_preempt() {
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

                let runtime = time.elapsed();

                let (need_repush, last_vruntime) = {
                    let mut state = queue.borrow_mut();
                    let last_vruntime = state.account_vruntime(runtime);
                    (state.is_active(), last_vruntime)
                };

                let mut tq = self.queues.borrow_mut();
                tq.active_executing = None;
                tq.stats.executor_runtime += runtime;
                tq.stats.tasks_executed += tasks_executed_this_loop;

                tq.last_vruntime = match last_vruntime {
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
    ///     let task = Task::<usize>::local(async { 1 + 2 });
    ///     task.await * 2
    /// });
    ///
    /// assert_eq!(res, 6);
    /// ```
    pub fn run<T>(&self, future: impl Future<Output = T>) -> T {
        let waker = waker_fn(|| {});
        let cx = &mut Context::from_waker(&waker);

        let spin_before_park = self.spin_before_park().unwrap_or_default();

        LOCAL_EX.set(self, || {
            let future = self.spawn(async move { future.await }).detach();
            pin!(future);

            let mut pre_time = Instant::now();
            loop {
                if let Poll::Ready(t) = future.as_mut().poll(cx) {
                    // can't be canceled, and join handle is None only upon
                    // cancellation or panic. So in case of panic this just propagates
                    let cur_time = Instant::now();
                    self.queues.borrow_mut().stats.total_runtime += cur_time - pre_time;
                    break t.unwrap();
                }

                // We want to do I/O before we call run_task_queues,
                // for the benefit of the latency ring. If there are pending
                // requests that are latency sensitive we want them out of the
                // ring ASAP (before we run the task queues). We will also use
                // the opportunity to install the timer.
                let duration = self.preempt_timer_duration();
                self.parker.poll_io(duration);
                let run = self.run_task_queues();
                let cur_time = Instant::now();
                self.queues.borrow_mut().stats.total_runtime += cur_time - pre_time;
                pre_time = cur_time;
                if !run {
                    if let Poll::Ready(t) = future.as_mut().poll(cx) {
                        // It may be that we just became ready now that the task queue
                        // is exhausted. But if we sleep (park) we'll never know so we
                        // test again here. We can't test *just* here because the main
                        // future is probably the one setting up the task queues and etc.
                        break t.unwrap();
                    } else {
                        while !self.reactor.spin_poll_io().unwrap() {
                            if pre_time.elapsed() > spin_before_park {
                                self.parker.park();
                                break;
                            }
                        }
                        // reset the timer for deduct spin loop time
                        pre_time = Instant::now();
                    }
                }
            }
        })
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
        LocalExecutorBuilder::new().make().unwrap()
    }
}

/// A spawned future.
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
/// use glommio::{LocalExecutor, Task};
///
/// let ex = LocalExecutor::default();
///
/// ex.run(async {
///     let task = Task::local(async {
///         println!("Hello from a task!");
///         1 + 2
///     });
///
///     assert_eq!(task.await, 3);
/// });
/// ```
#[must_use = "tasks get canceled when dropped, use `.detach()` to run them in the background"]
#[derive(Debug)]
pub struct Task<T>(multitask::Task<T>);

impl<T> Task<T> {
    /// Spawns a task onto the current single-threaded executor.
    ///
    /// If called from a [`LocalExecutor`], the task is spawned on it.
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
    ///     let task = Task::local(async { 1 + 2 });
    ///     assert_eq!(task.await, 3);
    /// });
    /// ```
    pub fn local(future: impl Future<Output = T> + 'static) -> Task<T>
    where
        T: 'static,
    {
        LOCAL_EX.with(|local_ex| local_ex.spawn(future))
    }

    /// Unconditionally yields the current task, moving it back to the end of
    /// its queue. It is not possible to yield futures that are not spawn'd,
    /// as they don't have a task associated with them.
    pub async fn later() {
        Self::cond_yield(|_| true).await
    }

    async fn cond_yield<F>(cond: F)
    where
        F: FnOnce(&LocalExecutor) -> bool,
    {
        let need_yield = LOCAL_EX.with(|local_ex| {
            if cond(local_ex) {
                local_ex.mark_me_for_yield();
                true
            } else {
                false
            }
        });

        if need_yield {
            futures_lite::future::yield_now().await;
        }
    }

    /// checks if this task has ran for too long and need to be preempted. This
    /// is useful for situations where we can't call .await, for instance,
    /// if a [`RefMut`] is held. If this tests true, then the user is
    /// responsible for making any preparations necessary for calling .await
    /// and doing it themselves.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::{Local, LocalExecutorBuilder};
    ///
    /// let ex = LocalExecutorBuilder::new()
    ///     .spawn(|| async {
    ///         loop {
    ///             if Local::need_preempt() {
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
    // FIXME: This is a bit less efficient than it needs, because the scoped thread
    // local key does lazy initialization. Every time we call into this, we are
    // paying to test if this is initialized. This is what I got from objdump:
    //
    // 0:    50                      push   %rax
    // 1:    ff 15 00 00 00 00       callq  *0x0(%rip)
    // 7:    48 85 c0                test   %rax,%rax
    // a:    74 17                   je     23  <== will call into the
    // initialization routine c:    48 8b 88 38 03 00 00    mov
    // 0x338(%rax),%rcx <== address of the head 13:   48 8b 80 40 03 00 00
    // mov    0x340(%rax),%rax <== address of the tail 1a:   8b 00
    // mov    (%rax),%eax 1c:   3b 01                   cmp    (%rcx),%eax <==
    // need preempt 1e:   0f 95 c0                setne  %al
    // 21:   59                      pop    %rcx
    // 22:   c3                      retq
    // 23    <== initialization stuff
    //
    // Rust has a thread local feature that is under experimental so we can maybe
    // switch to that someday.
    //
    // We will prefer to use the stable compiler and pay that unfortunate price for
    // now.
    pub fn need_preempt() -> bool {
        LOCAL_EX.with(|local_ex| local_ex.need_preempt())
    }

    /// Conditionally yields the current task, moving it back to the end of its
    /// queue, if the task has run for too long
    #[inline]
    pub async fn yield_if_needed() {
        Self::cond_yield(|local_ex| local_ex.need_preempt()).await;
    }

    #[inline]
    pub(crate) fn get_reactor() -> Rc<parking::Reactor> {
        LOCAL_EX.with(|local_ex| local_ex.get_reactor())
    }

    /// Spawns a task onto the current single-threaded executor, in a particular
    /// task queue
    ///
    /// If called from a [`LocalExecutor`], the task is spawned on it.
    ///
    /// Otherwise, this method panics.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::{Local, LocalExecutor, Shares, Task};
    ///
    /// let local_ex = LocalExecutor::default();
    /// local_ex.run(async {
    ///     let handle = Local::create_task_queue(
    ///         Shares::default(),
    ///         glommio::Latency::NotImportant,
    ///         "test_queue",
    ///     );
    ///     let task =
    ///         Task::<usize>::local_into(async { 1 + 2 }, handle).expect("failed to spawn task");
    ///     assert_eq!(task.await, 3);
    /// })
    /// ```
    pub fn local_into(
        future: impl Future<Output = T> + 'static,
        handle: TaskQueueHandle,
    ) -> Result<Task<T>>
    where
        T: 'static,
    {
        LOCAL_EX.with(|local_ex| local_ex.spawn_into(future, handle))
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
    ///     println!("my ID: {}", Task::<()>::id());
    /// });
    /// ```
    pub fn id() -> usize
    where
        T: 'static,
    {
        LOCAL_EX.with(|local_ex| local_ex.id())
    }

    /// Detaches the task to let it keep running in the background.
    ///
    /// # Examples
    ///
    /// ```
    /// use futures_lite::future;
    /// use glommio::{timer::Timer, Local, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async {
    ///     Local::local(async {
    ///         loop {
    ///             println!("I'm a background task looping forever.");
    ///             Local::later().await;
    ///         }
    ///     })
    ///     .detach();
    ///     Timer::new(std::time::Duration::from_micros(100)).await;
    /// })
    /// ```
    pub fn detach(self) -> task::JoinHandle<T> {
        self.0.detach()
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
    /// use glommio::{Latency, Local, LocalExecutor, Shares};
    /// use std::time::Duration;
    ///
    /// let local_ex = LocalExecutor::default();
    /// local_ex.run(async move {
    ///     let task_queue = Local::create_task_queue(
    ///         Shares::default(),
    ///         Latency::Matters(Duration::from_secs(1)),
    ///         "my_tq",
    ///     );
    ///     let task = Local::local_into(
    ///         async {
    ///             println!("Hello world");
    ///         },
    ///         task_queue,
    ///     )
    ///     .expect("failed to spawn task");
    /// });
    /// ```
    ///
    /// [`local_into`]: Task::local_into
    /// [`Shares`]: enum.Shares.html
    /// [`Latency`]: enum.Latency.html
    pub fn create_task_queue(shares: Shares, latency: Latency, name: &str) -> TaskQueueHandle {
        LOCAL_EX.with(|local_ex| local_ex.create_task_queue(shares, latency, name))
    }

    /// Returns the [`TaskQueueHandle`] that represents the TaskQueue currently
    /// running. This can be passed directly into [`Task::local_into`]. This
    /// must be run from a task that was generated through [`Task::local`]
    /// or [`Task::local_into`]
    ///
    /// # Examples
    /// ```
    /// use glommio::{Latency, Local, LocalExecutor, LocalExecutorBuilder, Shares};
    ///
    /// let ex = LocalExecutorBuilder::new()
    ///     .spawn(|| async move {
    ///         let original_tq = Local::current_task_queue();
    ///         let new_tq = Local::create_task_queue(Shares::default(), Latency::NotImportant, "test");
    ///
    ///         let task = Local::local_into(
    ///             async move {
    ///                 Local::local_into(
    ///                     async move {
    ///                         assert_eq!(Local::current_task_queue(), original_tq);
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
    pub fn current_task_queue() -> TaskQueueHandle {
        LOCAL_EX.with(|local_ex| local_ex.current_task_queue())
    }

    /// Returns a [`Result`] with its `Ok` value wrapping a [`TaskQueueStats`]
    /// or a [`GlommioError`] of type `[QueueErrorKind`] if there is no task
    /// queue with this handle
    ///
    /// # Examples
    /// ```
    /// use glommio::{Latency, Local, LocalExecutorBuilder, Shares};
    ///
    /// let ex = LocalExecutorBuilder::new()
    ///     .spawn(|| async move {
    ///         let new_tq = Local::create_task_queue(Shares::default(), Latency::NotImportant, "test");
    ///         println!(
    ///             "Stats for test: {:?}",
    ///             Local::task_queue_stats(new_tq).unwrap()
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
    pub fn task_queue_stats(handle: TaskQueueHandle) -> Result<TaskQueueStats> {
        LOCAL_EX.with(|local_ex| match local_ex.get_queue(&handle) {
            Some(x) => Ok(x.borrow().stats),
            None => Err(GlommioError::queue_not_found(handle.index)),
        })
    }

    /// Returns a collection of [`TaskQueueStats`] with information about all
    /// task queues in the system
    ///
    /// The collection can be anything that implements [`Extend`] and it is
    /// initially passed by the user so they can control how allocations are
    /// done.
    ///
    /// # Examples
    /// ```
    /// use glommio::{Latency, Local, LocalExecutorBuilder, Shares};
    ///
    /// let ex = LocalExecutorBuilder::new()
    ///     .spawn(|| async move {
    ///         let new_tq = Local::create_task_queue(Shares::default(), Latency::NotImportant, "test");
    ///         let v = Vec::new();
    ///         println!("Stats for all queues: {:?}", Local::all_task_queue_stats(v));
    ///     })
    ///     .unwrap();
    ///
    /// ex.join().unwrap();
    /// ```
    ///
    /// [`ExecutorStats`]: struct.ExecutorStats.html
    /// [`Result`]: https://doc.rust-lang.org/std/result/enum.Result.html
    /// [`Extend`]: https://doc.rust-lang.org/std/iter/trait.Extend.html
    pub fn all_task_queue_stats<V>(mut output: V) -> V
    where
        V: Extend<TaskQueueStats>,
    {
        LOCAL_EX.with(|local_ex| {
            let tq = local_ex.queues.borrow();
            output.extend(tq.available_executors.values().map(|x| x.borrow().stats));
            output
        })
    }

    /// Returns a [`ExecutorStats`] struct with information about this Executor
    ///
    /// # Examples:
    ///
    /// ```
    /// use glommio::{Local, LocalExecutorBuilder};
    ///
    /// let ex = LocalExecutorBuilder::new()
    ///     .spawn(|| async move {
    ///         println!("Stats for executor: {:?}", Local::executor_stats());
    ///     })
    ///     .unwrap();
    ///
    /// ex.join().unwrap();
    /// ```
    ///
    /// [`ExecutorStats`]: struct.ExecutorStats.html
    pub fn executor_stats() -> ExecutorStats {
        LOCAL_EX.with(|local_ex| local_ex.queues.borrow().stats)
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
    /// use glommio::{Local, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    ///
    /// ex.run(async {
    ///     let task = Local::local(async {
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        enclose,
        timer::{self, sleep, Timer},
        Local,
        SharesManager,
    };
    use core::mem::MaybeUninit;
    use futures::join;
    use std::{
        cell::Cell,
        sync::{Arc, Mutex},
        task::Waker,
    };

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
        if let Ok(_) = LocalExecutorBuilder::new().pin_to_cpu(usize::MAX).make() {
            panic!("Should have failed");
        }
    }

    #[test]
    fn create_and_bind() {
        if let Err(x) = LocalExecutorBuilder::new().pin_to_cpu(0).make() {
            panic!("got error {:?}", x);
        }
    }

    #[test]
    #[should_panic]
    fn spawn_without_executor() {
        let _ = LocalExecutor::default();
        let _ = Task::local(async move {});
    }

    #[test]
    fn invalid_task_queue() {
        let local_ex = LocalExecutor::default();
        local_ex.run(async {
            let task = Task::local_into(
                async move {
                    panic!("Should not have executed this");
                },
                TaskQueueHandle { index: 1 },
            );

            if let Ok(_) = task {
                panic!("Should have failed");
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
                joins.push(Task::local(async move {
                    for _ in 0..10_000 {
                        let mut last = exec.borrow_mut();
                        assert_ne!(id, *last);
                        *last = id;
                        drop(last);
                        Local::later().await;
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
            let not_latency =
                Local::create_task_queue(Shares::default(), Latency::NotImportant, "test");
            let latency = Local::create_task_queue(
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
                                Local::yield_if_needed().await;
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
                                if *(nolat_started.borrow()) == false {
                                    Local::later().await;
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
            let tq1 = Local::create_task_queue(Shares::default(), Latency::NotImportant, "test1");
            let tq2 = Local::create_task_queue(Shares::default(), Latency::NotImportant, "test2");

            let id1 = tq1.index;
            let id2 = tq2.index;
            let j0 = Local::local(async {
                assert_eq!(Local::current_task_queue().index, 0);
            });
            let j1 = Local::local_into(
                async move {
                    assert_eq!(Local::current_task_queue().index, id1);
                },
                tq1,
            )
            .unwrap();
            let j2 = Local::local_into(
                async move {
                    assert_eq!(Local::current_task_queue().index, id2);
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
            let tq1 = Local::create_task_queue(Shares::default(), Latency::NotImportant, "test");
            let tq2 = Local::create_task_queue(Shares::default(), Latency::NotImportant, "testlat");

            let first_started = Rc::new(RefCell::new(false));
            let second_status = Rc::new(RefCell::new(false));

            let first = local_ex
                .spawn_into(
                    crate::enclose! { (first_started, second_status)
                        async move {
                            *(first_started.borrow_mut()) = true;

                            let start = Instant::now();
                            // Now busy loop and make sure that we yield when we have too.
                            loop {
                                if start.elapsed().as_millis() >= 99 {
                                    break;
                                }

                                if *(second_status.borrow()) {
                                    panic!("I was preempted but should not have been");
                                }
                                Local::yield_if_needed().await;
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
                                if *(first_started.borrow()) == false {
                                    Local::later().await;
                                } else {
                                    break;
                                }
                            }
                            *(second_status.borrow_mut()) = true;
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
            Local::local(async {
                loop {
                    Local::later().await;
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

    fn getrusage() -> libc::rusage {
        let mut s0 = MaybeUninit::<libc::rusage>::uninit();
        let err = unsafe { libc::getrusage(libc::RUSAGE_THREAD, s0.as_mut_ptr()) };
        if err != 0 {
            panic!("getrusage error = {}", err);
        }
        unsafe { s0.assume_init() }
    }

    fn getrusage_utime() -> Duration {
        from_timeval(getrusage().ru_utime)
    }

    #[test]
    fn test_no_spin() {
        let ex = LocalExecutor::default();
        let task_queue = ex.create_task_queue(
            Shares::default(),
            Latency::Matters(Duration::from_millis(10)),
            "my_tq",
        );
        let start = getrusage_utime();
        ex.run(async {
            Local::local_into(
                async { timer::sleep(Duration::from_secs(1)).await },
                task_queue,
            )
            .expect("failed to spawn task")
            .await;
        });

        assert!(
            getrusage_utime() - start < Duration::from_millis(2),
            "expected user time on LE is less than 2 millisecond"
        );
    }

    #[test]
    fn test_spin() {
        let dur = Duration::from_secs(1);
        let ex0 = LocalExecutorBuilder::new().make().unwrap();
        let ex0_ru_start = getrusage_utime();
        ex0.run(async { timer::sleep(dur).await });
        let ex0_ru_finish = getrusage_utime();

        let ex = LocalExecutorBuilder::new()
            .pin_to_cpu(0)
            .spin_before_park(Duration::from_millis(100))
            .make()
            .unwrap();
        let ex_ru_start = getrusage_utime();
        ex.run(async {
            Local::local(async move { timer::sleep(dur).await }).await;
        });
        let ex_ru_finish = getrusage_utime();

        assert!(
            ex0_ru_finish - ex0_ru_start < Duration::from_millis(10),
            "expected user time on LE0 is less than 10 millisecond"
        );
        // 100 ms may have passed without us running for 100ms in case
        // there are other threads. Need to be a bit more relaxed
        assert!(
            ex_ru_finish - ex_ru_start >= Duration::from_millis(50),
            "expected user time on LE is much greater than 50 millisecond"
        );
    }

    #[test]
    fn test_runtime_stats() {
        let dur = Duration::from_secs(2);
        let ex0 = LocalExecutorBuilder::new().make().unwrap();
        ex0.run(async {
            assert!(
                Local::executor_stats().total_runtime() < Duration::from_nanos(10),
                "expected runtime on LE {:#?} is less than 10 ns",
                Local::executor_stats().total_runtime()
            );

            let now = Instant::now();
            while now.elapsed().as_millis() < 200 {}
            Local::later().await;
            assert!(
                Local::executor_stats().total_runtime() >= Duration::from_millis(200),
                "expected runtime on LE0 {:#?} is greater than 200 ms",
                Local::executor_stats().total_runtime()
            );

            timer::sleep(dur).await;
            assert!(
                Local::executor_stats().total_runtime() < Duration::from_millis(400),
                "expected runtime on LE0 {:#?} is not greater than 400 ms",
                Local::executor_stats().total_runtime()
            );
        });

        let ex = LocalExecutorBuilder::new()
            .pin_to_cpu(0)
            // ensure entire sleep should spin
            .spin_before_park(Duration::from_secs(5))
            .make()
            .unwrap();
        ex.run(async {
            Local::local(async move {
                assert!(
                    Local::executor_stats().total_runtime() < Duration::from_nanos(10),
                    "expected runtime on LE {:#?} is less than 10 ns",
                    Local::executor_stats().total_runtime()
                );

                let now = Instant::now();
                while now.elapsed().as_millis() < 200 {}
                Local::later().await;
                assert!(
                    Local::executor_stats().total_runtime() >= Duration::from_millis(200),
                    "expected runtime on LE {:#?} is greater than 200 ms",
                    Local::executor_stats().total_runtime()
                );
                timer::sleep(dur).await;
                assert!(
                    Local::executor_stats().total_runtime() < Duration::from_millis(400),
                    "expected runtime on LE {:#?} is not greater than 400 ms",
                    Local::executor_stats().total_runtime()
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
        Local::later().await;
    }

    macro_rules! test_static_shares {
        ( $s1:expr, $s2:expr, $work:block ) => {
            let local_ex = LocalExecutor::default();

            local_ex.run(async {
                // Run a latency queue, otherwise a queue will run for too long uninterrupted
                // and we'd have to run this test for a very long time for things to equalize.
                let tq1 = Local::create_task_queue(
                    Shares::Static($s1),
                    Latency::Matters(Duration::from_millis(1)),
                    "test_1",
                );
                let tq2 = Local::create_task_queue(
                    Shares::Static($s2),
                    Latency::Matters(Duration::from_millis(1)),
                    "test_2",
                );

                let tq1_count = Rc::new(Cell::new(0));
                let tq2_count = Rc::new(Cell::new(0));
                let now = Instant::now();

                let t1 = Local::local_into(
                    enclose! { (tq1_count, now) async move {
                        while now.elapsed().as_secs() < 5 {
                            $work;
                            tq1_count.replace(tq1_count.get() + 1);
                        }
                    }},
                    tq1,
                )
                .unwrap();

                let t2 = Local::local_into(
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
            let tq1 = Local::create_task_queue(
                Shares::Static(1000),
                Latency::Matters(Duration::from_millis(1)),
                "test_1",
            );
            let tq2 = Local::create_task_queue(
                Shares::Dynamic(bm.clone()),
                Latency::Matters(Duration::from_millis(1)),
                "test_2",
            );

            let tq1_count = Rc::new(RefCell::new(vec![0, 0]));
            let tq2_count = Rc::new(RefCell::new(vec![0, 0]));
            let now = Instant::now();

            let t1 = Local::local_into(
                enclose! { (tq1_count, now) async move {
                    loop {
                        let secs = now.elapsed().as_secs();
                        if secs >= 2 {
                            break;
                        }
                        (*tq1_count.borrow_mut())[secs as usize] += 1;
                        Local::later().await;
                    }
                }},
                tq1,
            )
            .unwrap();

            let t2 = Local::local_into(
                enclose! { (tq2_count, now, bm) async move {
                    loop {
                        let elapsed = now.elapsed();
                        let secs = elapsed.as_secs();
                        if secs >= 2 {
                            break;
                        }
                        bm.tick(elapsed.as_millis() as u64);
                        (*tq2_count.borrow_mut())[secs as usize] += 1;
                        Local::later().await;
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
            Local::local(async {}).detach().await;
            // In issue 241, the presence of the second detached waiter caused
            // the program to hang.
            Local::local(async {}).detach().await;
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

        let ex1 = LocalExecutorBuilder::new()
            .spawn(|| async move {
                fut.await;
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::new()
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

        let ex1 = LocalExecutorBuilder::new()
            .spawn(|| async move {
                fut.await;
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::new()
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

        let ex1 = LocalExecutorBuilder::new()
            .spawn(|| async move {
                let x = Local::local(fut).detach();
                x.await;
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::new()
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

        let ex1 = LocalExecutorBuilder::new()
            .spawn(|| async move {
                let _drop = futures_lite::future::poll_once(fut).await;
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::new()
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
}
