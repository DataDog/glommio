// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
//! Async executor.
//!
//! This crate offers two kinds of executors: single-threaded and multi-threaded.
//!
//! # Examples
//!
//! Run four single-threaded executors concurrently:
//!
//! ```
//! use scipio::{LocalExecutor, LocalExecutorBuilder};
//! use scipio::timer::Timer;
//!
//! for i in 0..4 {
//!     std::thread::spawn(move || {
//!         let builder = LocalExecutorBuilder::new()
//!                                             .pin_to_cpu(i);
//!         let local_ex = builder.make().expect("failed to spawn local executor");
//!         local_ex.run(async {
//!             Timer::new(std::time::Duration::from_millis(100)).await;
//!             println!("Hello world!");
//!         });
//!     });
//! }
//! ```

#![warn(missing_docs, missing_debug_implementations)]

use std::cell::RefCell;
use std::collections::{BinaryHeap, HashMap};
use std::fmt;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::thread::{Builder, JoinHandle};
use std::time::{Duration, Instant};

use futures_lite::pin;
use scoped_tls::scoped_thread_local;

use crate::multitask;
use crate::parking;
use crate::task::{self, waker_fn::waker_fn};
use crate::Local;
use crate::Reactor;
use crate::{IoRequirements, Latency};

static EXECUTOR_ID: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Clone)]
/// Error thrown when a Task Queue is not found.
pub struct QueueNotFoundError {
    index: usize,
}

impl QueueNotFoundError {
    fn new(h: TaskQueueHandle) -> Self {
        QueueNotFoundError { index: h.index }
    }
}
impl std::error::Error for QueueNotFoundError {}

impl fmt::Display for QueueNotFoundError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid queue index: {}", self.index)
    }
}

#[derive(Debug, Clone)]
/// Error thrown when a Task Queue is still active and one attempts to remove it
pub struct QueueStillActiveError {
    index: usize,
}
impl std::error::Error for QueueStillActiveError {}

impl QueueStillActiveError {
    fn new(h: TaskQueueHandle) -> Self {
        QueueStillActiveError { index: h.index }
    }
}

impl fmt::Display for QueueStillActiveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "queue with index {} is still active, but tried to remove",
            self.index
        )
    }
}

scoped_thread_local!(static LOCAL_EX: LocalExecutor);

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
/// An opaque handler indicating in which queue a group of tasks will execute.
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
    /// Sets the number of shares used for a particular TaskQueue
    pub fn set_task_queue_shares(&self, shares: usize) -> Result<(), QueueNotFoundError> {
        if LOCAL_EX.is_set() {
            LOCAL_EX.with(|local_ex| local_ex.set_task_queue_shares(*self, shares))
        } else {
            panic!("`Task::local()` must be called from a `LocalExecutor`")
        }
    }

    /// Gets the number of shares used for a particular TaskQueue
    pub fn get_task_queue_shares(&self) -> Result<usize, QueueNotFoundError> {
        if LOCAL_EX.is_set() {
            LOCAL_EX.with(|local_ex| local_ex.get_task_queue_shares(*self))
        } else {
            panic!("`Task::local()` must be called from a `LocalExecutor`")
        }
    }
}

#[derive(Debug)]
struct TaskQueue {
    ex: Rc<multitask::LocalExecutor>,
    active: bool,
    shares: usize,
    reciprocal_shares: u64,
    runtime: u64,
    vruntime: u64,
    io_requirements: IoRequirements,
    name: &'static str,
    index: usize, // so we can easily produce a handle
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
    fn new<F>(
        index: usize,
        name: &'static str,
        shares: usize,
        ioreq: IoRequirements,
        notify: F,
    ) -> Rc<RefCell<Self>>
    where
        F: Fn() + 'static,
    {
        let mut tq = TaskQueue {
            ex: Rc::new(multitask::LocalExecutor::new(notify)),
            active: false,
            shares: 0,
            reciprocal_shares: 0,
            runtime: 0,
            vruntime: 0,
            io_requirements: ioreq,
            name,
            index,
        };
        tq.set_shares(shares);
        Rc::new(RefCell::new(tq))
    }

    fn is_active(&self) -> bool {
        self.active
    }

    fn get_task(&mut self) -> Option<multitask::Runnable> {
        let r = self.ex.get_task();
        self.active = r.is_some();
        r
    }

    fn set_shares(&mut self, shares: usize) {
        self.shares = std::cmp::max(shares, 1);
        self.reciprocal_shares = (1u64 << 22) / (self.shares as u64);
    }

    fn account_vruntime(&mut self, delta: Duration) -> u64 {
        // Overflow shouldn't happen but if it does (in case a task gets stalled), account as 0
        // otherwise it will never run again.
        let delta_scaled = self
            .reciprocal_shares
            .checked_mul(delta.as_micros() as u64)
            .unwrap_or(0)
            >> 12;
        self.vruntime += delta_scaled;
        self.runtime += delta.as_micros() as u64;

        //println!("Ran task for {} us, adding {} of vruntime (shares = {})", delta.as_micros(), delta_scaled, self.shares);
        self.vruntime
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

fn bind_to_cpu(cpu: usize) -> io::Result<()> {
    let mut cpuset = nix::sched::CpuSet::new();
    to_io_error!(&cpuset.set(cpu as usize))?;
    let pid = nix::unistd::Pid::from_raw(0);
    to_io_error!(nix::sched::sched_setaffinity(pid, &cpuset))
}

#[derive(Debug)]
struct ExecutorQueues {
    active_executors: BinaryHeap<Rc<RefCell<TaskQueue>>>,
    available_executors: HashMap<usize, Rc<RefCell<TaskQueue>>>,
    active_executing: Option<Rc<RefCell<TaskQueue>>>,
    default_executor: TaskQueueHandle,
    executor_index: usize,
    last_vruntime: u64,
    preempt_timer_duration: Duration,
    spin_before_park: Option<Duration>,
}

impl ExecutorQueues {
    fn new() -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(ExecutorQueues {
            active_executors: BinaryHeap::new(),
            available_executors: HashMap::new(),
            active_executing: None,
            default_executor: TaskQueueHandle::default(),
            executor_index: 1, // 0 is the default
            last_vruntime: 0,
            preempt_timer_duration: Duration::from_secs(1),
            spin_before_park: None,
        }))
    }

    fn reevaluate_preempt_timer(&mut self) {
        self.preempt_timer_duration = self
            .active_executors
            .iter()
            .map(|tq| match tq.borrow().io_requirements.latency_req {
                Latency::NotImportant => Duration::from_secs(1),
                Latency::Matters(d) => d,
            })
            .min()
            .unwrap_or_else(|| Duration::from_secs(1))
    }
    fn maybe_activate(&mut self, index: usize) {
        let queue = self
            .available_executors
            .get(&index)
            .expect("Trying to activate invalid queue! Index")
            .clone();

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

/// LocalExecutor factory, which can be used in order to configure the properties of a new
/// `LocalExecutor`.
///
/// Methods can be chained on it in order to configure it.
///
/// The `spawn` method will take ownership of the builder and create an
/// `io::Result` to the `LocalExecutor` handle with the given configuration.
///
/// The `LocalExecutor::make_default` free function uses a Builder with default configuration and
/// unwraps its return value.
///
/// You may want to use `LocalExecutorBuilder::spawn` instead of `LocalExecutor::make_default`,
/// when you want to recover from a failure to launch a thread, indeed the free function will panic
/// where the Builder method will return a `io::Result`.
///
/// # Examples
///
/// ```
/// use scipio::LocalExecutorBuilder;
///
/// let builder = LocalExecutorBuilder::new();
/// let ex = builder.make().unwrap();
/// ```
#[derive(Debug)]
pub struct LocalExecutorBuilder {
    // The id of a CPU to bind the current (or yet to be created) thread
    binding: Option<usize>,
    /// Spin for duration before parking a reactor
    spin_before_park: Option<Duration>,
    /// A name for the thread-to-be (if any), for identification in panic messages
    name: String,
}

impl LocalExecutorBuilder {
    /// Generates the base configuration for spawning a [`LocalExecutor`], from which configuration
    /// methods can be chained.
    pub fn new() -> LocalExecutorBuilder {
        LocalExecutorBuilder {
            binding: None,
            spin_before_park: None,
            name: String::from("unnamed"),
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

    /// Make a new [`LocalExecutor`] by taking ownership of the Builder, and returns an
    /// [`io::Result`] to the executor.
    /// # Examples
    ///
    /// ```
    /// use scipio::LocalExecutorBuilder;
    ///
    /// let local_ex = LocalExecutorBuilder::new().make().unwrap();
    /// ```
    pub fn make(self) -> io::Result<LocalExecutor> {
        let mut le = LocalExecutor::new(EXECUTOR_ID.fetch_add(1, Ordering::Relaxed));
        if let Some(cpu) = self.binding {
            le.bind_to_cpu(cpu)?;
            le.queues.borrow_mut().spin_before_park = self.spin_before_park;
        }
        match le.init() {
            Ok(_) => Ok(le),
            Err(e) => Err(e),
        }
    }

    /// Spawns a new [`LocalExecutor`] in a new thread by taking ownership of the Builder,
    /// and returns an io::Result to its JoinHandle.
    ///
    /// This is a more ergonomic way to create a thread and then run an executor inside it
    /// This function panics if creating the thread or the executor fails. If you need more
    /// fine-grained error handling consider initializing those entities manually.
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::LocalExecutorBuilder;
    ///
    /// let handle = LocalExecutorBuilder::new().spawn(|| async move {
    ///     println!("hello");
    /// }).unwrap();
    ///
    /// handle.join().unwrap();
    /// ```
    #[must_use = "This spawns an executor on a thread, so you must acquire its handle and then join() to keep it alive"]
    pub fn spawn<G, F, T>(self, fut_gen: G) -> io::Result<JoinHandle<()>>
    where
        G: FnOnce() -> F + std::marker::Send + 'static,
        F: Future<Output = T> + 'static,
    {
        let id = EXECUTOR_ID.fetch_add(1, Ordering::Relaxed);
        let name = format!("{}-{}", self.name, id);

        Builder::new().name(name).spawn(move || {
            let mut le = LocalExecutor::new(id);
            if let Some(cpu) = self.binding {
                le.bind_to_cpu(cpu).unwrap();
            }
            le.init().unwrap();
            le.run(async move {
                let task = Task::local(async move {
                    fut_gen().await;
                });
                task.await;
            })
        })
    }
}

impl Default for LocalExecutorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Single-threaded executor.
///
/// The executor can only be run on the thread that created it.
///
/// # Examples
///
/// ```
/// use scipio::LocalExecutor;
///
/// let local_ex = LocalExecutor::make_default();
///
/// local_ex.run(async {
///     println!("Hello world!");
/// });
/// ```
#[derive(Debug)]
pub struct LocalExecutor {
    queues: Rc<RefCell<ExecutorQueues>>,
    parker: parking::Parker,
    id: usize,
}

impl LocalExecutor {
    fn bind_to_cpu(&self, cpu: usize) -> io::Result<()> {
        bind_to_cpu(cpu)
    }

    fn init(&mut self) -> io::Result<()> {
        let queues = self.queues.clone();
        let index = 0;

        let io_requirements = IoRequirements::new(Latency::NotImportant, 0);
        self.queues.borrow_mut().available_executors.insert(
            0,
            TaskQueue::new(0, "default", 1000, io_requirements, move || {
                let mut queues = queues.borrow_mut();
                queues.maybe_activate(index);
            }),
        );
        Ok(())
    }

    /// Spawns a single-threaded executor with default settings on the current thread.
    ///
    /// This will create a executor using default parameters of `LocalExecutorBuilder`, if you
    /// want to further customize it, use this API instead.
    ///
    /// # Panics
    ///
    /// Panics if creating the executor fails; use `LocalExecutorBuilder::make` to recover
    /// from such errors.
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::LocalExecutor;
    ///
    /// let local_ex = LocalExecutor::make_default();
    /// ```
    pub fn make_default() -> LocalExecutor {
        LocalExecutorBuilder::new().make().unwrap()
    }

    fn new(id: usize) -> LocalExecutor {
        let p = parking::Parker::new();
        LocalExecutor {
            queues: ExecutorQueues::new(),
            parker: p,
            id,
        }
    }

    /// Returns a unique identifier for this Executor.
    ///
    /// # Examples
    /// ```
    /// use scipio::LocalExecutor;
    ///
    /// let local_ex = LocalExecutor::make_default();
    /// println!("My ID: {}", local_ex.id());
    /// ```
    pub fn id(&self) -> usize {
        self.id
    }

    /// Creates a task queue in the executor.
    ///
    /// Returns an opaque handler that can later be used to launch tasks into that queue with spawn_into
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use scipio::{LocalExecutor, Latency};
    ///
    /// let local_ex = LocalExecutor::make_default();
    ///
    /// let task_queue = local_ex.create_task_queue(1000, Latency::Matters(Duration::from_secs(1)), "my_tq");
    /// let task = local_ex.spawn_into(async {
    ///     println!("Hello world");
    /// }, task_queue).expect("failed to spawn task");
    /// ```
    pub fn create_task_queue(
        &self,
        shares: usize,
        latency: Latency,
        name: &'static str,
    ) -> TaskQueueHandle {
        let queues = self.queues.clone();
        let index = {
            let mut ex = queues.borrow_mut();
            let index = ex.executor_index;
            ex.executor_index += 1;
            index
        };

        let io_requirements = IoRequirements::new(latency, index);
        let tq = TaskQueue::new(index, name, shares, io_requirements, move || {
            let mut queues = queues.borrow_mut();
            queues.maybe_activate(index);
        });

        self.queues
            .borrow_mut()
            .available_executors
            .insert(index, tq);
        TaskQueueHandle { index }
    }

    /// Removes a task queue.
    ///
    /// The task queue cannot be removed if there are still pending tasks.
    pub fn remove_task_queue(
        &self,
        handle: TaskQueueHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut queues = self.queues.borrow_mut();

        if let Some(tq) = queues.available_executors.get(&handle.index) {
            if tq.borrow().is_active() {
                return Err(Box::new(QueueStillActiveError::new(handle)));
            }
            queues
                .available_executors
                .remove(&handle.index)
                .expect("test already done");
            return Ok(());
        }
        Err(Box::new(QueueNotFoundError::new(handle)))
    }

    fn get_queue(&self, handle: &TaskQueueHandle) -> Option<Rc<RefCell<TaskQueue>>> {
        self.queues
            .borrow()
            .available_executors
            .get(&handle.index)
            .cloned()
    }

    fn get_executor(&self, handle: &TaskQueueHandle) -> Option<Rc<multitask::LocalExecutor>> {
        self.get_queue(handle).map(|x| x.borrow().ex.clone())
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
                .index,
        }
    }

    /// Sets the number of shares used for a particular TaskQueue
    pub fn set_task_queue_shares(
        &self,
        handle: TaskQueueHandle,
        shares: usize,
    ) -> Result<(), QueueNotFoundError> {
        self.get_queue(&handle)
            .map(|tq| tq.borrow_mut().set_shares(shares))
            .ok_or_else(|| QueueNotFoundError::new(handle))
    }

    /// Gets the number of shares used for a particular TaskQueue
    pub fn get_task_queue_shares(
        &self,
        handle: TaskQueueHandle,
    ) -> Result<usize, QueueNotFoundError> {
        self.get_queue(&handle)
            .map(|tq| tq.borrow().shares)
            .ok_or_else(|| QueueNotFoundError::new(handle))
    }

    /// Spawns a task onto the executor.
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::LocalExecutor;
    ///
    /// let local_ex = LocalExecutor::make_default();
    ///
    /// let task = local_ex.spawn(async {
    ///     println!("Hello world");
    /// });
    /// ```
    pub fn spawn<T: 'static>(&self, future: impl Future<Output = T> + 'static) -> Task<T> {
        let ex = self
            .queues
            .borrow()
            .active_executing
            .as_ref()
            .map(|x| x.borrow().ex.clone())
            .or_else(|| self.get_executor(&TaskQueueHandle { index: 0 }))
            .unwrap();
        Task(ex.spawn(future))
    }

    /// Spawns a task onto the executor, to be run at a particular task queue indicated by the
    /// TaskQueueHandle
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::LocalExecutor;
    ///
    /// let local_ex = LocalExecutor::make_default();
    /// let handle = local_ex.create_task_queue(1000, scipio::Latency::NotImportant, "test_queue");
    ///
    /// let task = local_ex.spawn_into(async {
    ///     println!("Hello world");
    /// }, handle).expect("failed to spawn task");
    /// ```
    pub fn spawn_into<T, F>(
        &self,
        future: F,
        handle: TaskQueueHandle,
    ) -> Result<Task<T>, QueueNotFoundError>
    where
        T: 'static,
        F: Future<Output = T> + 'static,
    {
        self.get_executor(&handle)
            .map(|ex| Task(ex.spawn(future)))
            .ok_or_else(|| QueueNotFoundError::new(handle))
    }

    fn preempt_timer_duration(&self) -> Duration {
        self.queues.borrow().preempt_timer_duration
    }

    fn spin_before_park(&self) -> Option<Duration> {
        self.queues.borrow().spin_before_park
    }

    fn run_one_task_queue(&self) -> bool {
        let mut tq = self.queues.borrow_mut();
        let candidate = tq.active_executors.pop();

        match candidate {
            Some(queue) => {
                tq.active_executing = Some(queue.clone());
                drop(tq);

                let time = Instant::now();
                loop {
                    if Reactor::need_preempt() {
                        break;
                    }
                    let mut queue_ref = queue.borrow_mut();
                    if let Some(r) = queue_ref.get_task() {
                        Reactor::get().inform_io_requirements(queue_ref.io_requirements);
                        drop(queue_ref);
                        r.run();
                    } else {
                        break;
                    }
                }

                let (need_repush, last_vruntime) = {
                    let mut state = queue.borrow_mut();
                    let last_vruntime = state.account_vruntime(time.elapsed());
                    (state.is_active(), last_vruntime)
                };

                let mut tq = self.queues.borrow_mut();
                tq.active_executing = None;
                tq.last_vruntime = last_vruntime;

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
    /// use scipio::LocalExecutor;
    ///
    /// let local_ex = LocalExecutor::make_default();
    ///
    /// let task = local_ex.spawn(async { 1 + 2 });
    /// let res = local_ex.run(async { task.await * 2 });
    ///
    /// assert_eq!(res, 6);
    /// ```
    pub fn run<T>(&self, future: impl Future<Output = T>) -> T {
        pin!(future);

        let waker = waker_fn(|| {});
        let cx = &mut Context::from_waker(&waker);

        let spin_before_park = self.spin_before_park().unwrap_or_default();
        let spin = spin_before_park.as_nanos() > 0;
        let mut spin_since: Option<Instant> = None;

        LOCAL_EX.set(self, || loop {
            if let Poll::Ready(t) = future.as_mut().poll(cx) {
                break t;
            }

            // We want to do I/O before we call run_one_task queue,
            // for the benefit of the latency ring. If there are pending
            // requests that are latency sensitive we want them out of the
            // ring ASAP (before we run the task queues). We will also use
            // the opportunity to install the timer.
            let duration = self.preempt_timer_duration();
            self.parker.poll_io(duration);
            if !self.run_one_task_queue() {
                if spin {
                    if let Some(t) = spin_since {
                        if t.elapsed() < spin_before_park {
                            continue;
                        }
                        spin_since = None
                    } else {
                        spin_since = Some(Instant::now());
                        continue;
                    }
                }

                self.parker.park();
            } else {
                spin_since = None
            }
        })
    }
}

/// A spawned future.
///
/// Tasks are also futures themselves and yield the output of the spawned future.
///
/// When a task is dropped, its gets canceled and won't be polled again. To cancel a task a bit
/// more gracefully and wait until it stops running, use the [`cancel()`][`Task::cancel()`] method.
///
/// Tasks that panic get immediately canceled. Awaiting a canceled task also causes a panic.
///
/// # Examples
///
/// ```
/// use scipio::{LocalExecutor, Task};
///
/// let ex = LocalExecutor::make_default();
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
    /// use scipio::{LocalExecutor, Task};
    ///
    /// let local_ex = LocalExecutor::make_default();
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
        if LOCAL_EX.is_set() {
            LOCAL_EX.with(|local_ex| local_ex.spawn(future))
        } else {
            panic!("`Task::local()` must be called from a `LocalExecutor`")
        }
    }

    /// Unconditionally yields the current task, moving it back to the end of its queue.
    /// It is not possible to yield futures that are not spawn'd, as they don't have a task
    /// associated with them.
    pub async fn later() {
        struct Yield {
            done: Option<()>,
        }

        // Returns pending once, so we are taken away from the execution queue.
        // The next time returns Ready, so we can proceed. We don't want to pay
        // the cost of calling schedule functions
        impl Future for Yield {
            type Output = ();

            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                match self.done.take() {
                    Some(_) => {
                        cx.waker().clone().wake();
                        Poll::Pending
                    }
                    None => Poll::Ready(()),
                }
            }
        }
        let y = Yield { done: Some(()) };
        y.await;
    }

    /// checks if this task has ran for too long and need to be preempted. This is useful for
    /// situations where we can't call .await, for instance, if a [`RefMut`] is held. If this
    /// tests true, then the user is responsible for making any preparations necessary for
    /// calling .await and doing it themselves.
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::{LocalExecutorBuilder, Local};
    ///
    /// let ex = LocalExecutorBuilder::new().spawn(|| async {
    ///     loop {
    ///         if Local::need_preempt() {
    ///             break;
    ///         }
    ///     }
    /// }).unwrap();
    ///
    /// ex.join().unwrap();
    /// ```
    ///
    /// [`RefMut`]: https://doc.rust-lang.org/std/cell/struct.RefMut.html
    #[inline]
    pub fn need_preempt() -> bool {
        Reactor::need_preempt()
    }

    /// Conditionally yields the current task, moving it back to the end of its queue, if the task
    /// has run for too long
    #[inline]
    pub async fn yield_if_needed() {
        if Reactor::need_preempt() {
            Local::later().await;
        }
    }

    /// Spawns a task onto the current single-threaded executor, in a particular task queue
    ///
    /// If called from a [`LocalExecutor`], the task is spawned on it.
    ///
    /// Otherwise, this method panics.
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::{LocalExecutor, Task};
    ///
    /// let local_ex = LocalExecutor::make_default();
    /// let handle = local_ex.create_task_queue(1000, scipio::Latency::NotImportant, "test_queue");
    ///
    /// local_ex.spawn_into(async {
    ///     let task = Task::local(async { 1 + 2 });
    ///     assert_eq!(task.await, 3);
    /// }, handle).expect("failed to spawn task");
    /// ```
    pub fn local_into(
        future: impl Future<Output = T> + 'static,
        handle: TaskQueueHandle,
    ) -> Result<Task<T>, QueueNotFoundError>
    where
        T: 'static,
    {
        if LOCAL_EX.is_set() {
            LOCAL_EX.with(|local_ex| local_ex.spawn_into(future, handle))
        } else {
            panic!("`Task::local()` must be called from a `LocalExecutor`")
        }
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
    /// use scipio::{LocalExecutor, Task};
    ///
    /// let local_ex = LocalExecutor::make_default();
    ///
    /// local_ex.run(async {
    ///     println!("my ID: {}", Task::<()>::id());
    /// });
    /// ```
    pub fn id() -> usize
    where
        T: 'static,
    {
        if LOCAL_EX.is_set() {
            LOCAL_EX.with(|local_ex| local_ex.id())
        } else {
            panic!("`Task::id()` must be called from a `LocalExecutor`")
        }
    }

    /// Detaches the task to let it keep running in the background.
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::{LocalExecutor, Task};
    /// use scipio::timer::Timer;
    /// use futures_lite::future;
    ///
    /// let ex = LocalExecutor::make_default();
    ///
    /// ex.spawn(async {
    ///     loop {
    ///         println!("I'm a background task looping forever.");
    ///         Task::<()>::later().await;
    ///     }
    /// })
    /// .detach();
    ///
    /// ex.run(async { Timer::new(std::time::Duration::from_micros(100)).await; });
    /// ```
    pub fn detach(self) -> task::JoinHandle<T, ()> {
        self.0.detach()
    }

    /// Creates a new task queue, with a given latency hint and the provided name
    pub fn create_task_queue(
        shares: usize,
        latency: Latency,
        name: &'static str,
    ) -> TaskQueueHandle {
        if LOCAL_EX.is_set() {
            LOCAL_EX.with(|local_ex| local_ex.create_task_queue(shares, latency, name))
        } else {
            panic!("`Task::create_task_queue()` must be called from a `LocalExecutor`")
        }
    }

    /// Returns the [`TaskQueueHandle`] that represents the TaskQueue currently running.
    /// This can be passed directly into [`Task::local_into`]. This must be run from a task that
    /// was generated through [`Task::local`] or [`Task::local_into`]
    ///
    /// # Examples
    /// ```
    /// use scipio::{LocalExecutor, Local, Latency, LocalExecutorBuilder};
    ///
    /// let ex = LocalExecutorBuilder::new().spawn(|| async move {
    ///     let original_tq = Local::current_task_queue();
    ///     let new_tq = Local::create_task_queue(1000, Latency::NotImportant, "test");
    ///
    ///     let task = Local::local_into(async move {
    ///         Local::local_into(async move {
    ///             assert_eq!(Local::current_task_queue(), original_tq);
    ///         }, original_tq).unwrap();
    ///     }, new_tq).unwrap();
    ///     task.await;
    /// }).unwrap();
    ///
    /// ex.join().unwrap();
    /// ```
    pub fn current_task_queue() -> TaskQueueHandle {
        if LOCAL_EX.is_set() {
            LOCAL_EX.with(|local_ex| local_ex.current_task_queue())
        } else {
            panic!("`Task::current_task_queue()` must be called from a `LocalExecutor`")
        }
    }

    /// Cancels the task and waits for it to stop running.
    ///
    /// Returns the task's output if it was completed just before it got canceled, or [`None`] if
    /// it didn't complete.
    ///
    /// While it's possible to simply drop the [`Task`] to cancel it, this is a cleaner way of
    /// canceling because it also waits for the task to stop running.
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::{LocalExecutor, Task};
    /// use futures_lite::future;
    ///
    /// let ex = LocalExecutor::make_default();
    ///
    /// let task = ex.spawn(async {
    ///     loop {
    ///         println!("Even though I'm in an infinite loop, you can still cancel me!");
    ///         future::yield_now().await;
    ///     }
    /// });
    ///
    /// ex.run(async {
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
    use crate::timer::{self, Timer};
    use core::mem::MaybeUninit;

    #[test]
    fn create_and_destroy_executor() {
        let mut var = Rc::new(RefCell::new(0));
        let local_ex = LocalExecutor::make_default();

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
        let _ = LocalExecutor::make_default();
        let _ = Task::local(async move {});
    }

    #[test]
    fn invalid_task_queue() {
        let local_ex = LocalExecutor::make_default();
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
        let local_ex = LocalExecutor::make_default();

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
        let local_ex = LocalExecutor::make_default();

        local_ex.run(async {
            let not_latency = Local::create_task_queue(1, Latency::NotImportant, "test");
            let latency =
                Local::create_task_queue(1, Latency::Matters(Duration::from_millis(2)), "testlat");

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
        let local_ex = LocalExecutor::make_default();
        local_ex.run(async {
            let tq1 = Local::create_task_queue(1, Latency::NotImportant, "test1");
            let tq2 = Local::create_task_queue(1, Latency::NotImportant, "test2");

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
        let local_ex = LocalExecutor::make_default();

        local_ex.run(async {
            let tq1 = Local::create_task_queue(1, Latency::NotImportant, "test");
            let tq2 = Local::create_task_queue(1, Latency::NotImportant, "testlat");

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
                                if *(second_status.borrow()) {
                                    panic!("I was preempted but should not have been");
                                }
                                if start.elapsed().as_millis() > 200 {
                                    break;
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
        let ex = LocalExecutor::make_default();

        ex.spawn(async {
            loop {
                //   println!("I'm a background task looping forever.");
                Local::later().await;
            }
        })
        .detach();

        ex.run(async {
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
        let ex = LocalExecutor::make_default();
        let task_queue =
            ex.create_task_queue(1000, Latency::Matters(Duration::from_millis(10)), "my_tq");
        let start = getrusage_utime();
        let task = ex
            .spawn_into(
                async { timer::sleep(Duration::from_secs(1)).await },
                task_queue,
            )
            .expect("failed to spawn task");
        ex.run(async { task.await });

        assert!(
            getrusage_utime() - start < Duration::from_millis(1),
            "expected user time on LE is less than 1 millisecond"
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
            .spin_before_park(Duration::from_millis(10))
            .make()
            .unwrap();
        let ex_ru_start = getrusage_utime();
        let task = ex.spawn(async move { timer::sleep(dur).await });
        ex.run(async { task.await });
        let ex_ru_finish = getrusage_utime();

        assert!(
            ex0_ru_finish - ex0_ru_start < Duration::from_millis(1),
            "expected user time on LE0 is less than 1 millisecond"
        );
        assert!(
            ex_ru_finish - ex_ru_start >= Duration::from_millis(1),
            "expected user time on LE is much greater than 1 millisecond"
        );
    }
}
