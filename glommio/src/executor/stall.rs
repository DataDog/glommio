// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2022 Datadog, Inc.
//

use crate::executor::TaskQueueHandle;
use nix::sys;
use std::{
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

pub struct StallDetection<'a> {
    executor: usize,
    queue_handle: TaskQueueHandle,
    queue_name: &'a str,
    trace: backtrace::Backtrace,
    budget: Duration,
    overage: Duration,
}

impl fmt::Debug for StallDetection<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StallDetection")
            .field("executor", &self.executor)
            .field("queue_handle", &self.queue_handle)
            .field("queue_name", &self.queue_name)
            .field("trace", &self.trace)
            .field("budget", &self.budget)
            .field("overage", &self.overage)
            .finish()
    }
}

impl fmt::Display for StallDetection<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[stall-detector -- executor {}] task queue {} went over-budget: {:#?} (budget: \
             {:#?}). Backtrace: {:#?}",
            self.executor, self.queue_name, self.overage, self.budget, self.trace,
        )
    }
}

/// Trait describing what signal to use to trigger stall detection,
/// how far past expected execution time to trigger a stall,
/// and how to handle a stall detection once triggered.
pub trait StallDetectionHandler: std::fmt::Debug + Send + Sync {
    /// How far past the preemption timer should qualify as a stall
    /// If None is returned, don't use the stall detector for this task queue.
    fn high_water_mark(
        &self,
        queue_handle: TaskQueueHandle,
        max_expected_runtime: Duration,
    ) -> Option<Duration>;

    /// What signal number to use; see values in libc::SIG*
    fn signal(&self) -> u8;

    /// Handler called when a task exceeds its budget
    fn stall(&self, detection: StallDetection<'_>);
}

/// Default settings for signal number, high water mark and stall handler.
/// By default, the high water mark to consider a task queue stalled is set to
/// 10% of the expected run time. The default handler will log a stack trace of
/// the currently executing task queue. The default signal number is SIGUSR1.
#[derive(Debug)]
pub struct DefaultStallDetectionHandler {}

impl StallDetectionHandler for DefaultStallDetectionHandler {
    /// The default high water mark is 10% of the preemption time,
    /// capped at 10ms.
    fn high_water_mark(
        &self,
        _queue_handle: TaskQueueHandle,
        max_expected_runtime: Duration,
    ) -> Option<Duration> {
        // We consider a queue to be stalling the system if it failed to yield in due
        // time. For a given maximum expected runtime, we allow a margin of error f 10%
        // (and an absolute minimum of 10ms) after which we record a stacktrace. i.e. a
        // task queue has should return shortly after `need_preempt()` returns
        // true or the stall detector triggers. For example::
        // * If a task queue has a preempt timer of 100ms the the stall detector
        // triggers if it doesn't yield after running for 110ms.
        // * If a task queue has a preempt timer of 5ms the the stall detector
        // triggers if it doesn't yield after running for 15ms.
        Some(
            Duration::from_millis((max_expected_runtime.as_millis() as f64 * 0.1) as u64)
                .max(Duration::from_millis(10)),
        )
    }

    /// The default signal is SIGUSR1.
    fn signal(&self) -> u8 {
        nix::libc::SIGUSR1 as u8
    }

    /// The default stall reporting mechanism is to log a warning.
    fn stall(&self, detection: StallDetection<'_>) {
        log::warn!("{}", detection);
    }
}

#[derive(Debug)]
pub(crate) struct StallDetector {
    timer: Arc<sys::timerfd::TimerFd>,
    stall_handler: Box<dyn StallDetectionHandler + 'static>,
    timer_handler: Option<JoinHandle<()>>,
    id: usize,
    terminated: Arc<AtomicBool>,
    // NOTE: we don't use signal_hook::low_level::channel as backtraces
    // have too many elements
    pub(crate) tx: crossbeam::channel::Sender<backtrace::BacktraceFrame>,
    pub(crate) rx: crossbeam::channel::Receiver<backtrace::BacktraceFrame>,
}

impl StallDetector {
    pub(crate) fn new(
        executor_id: usize,
        stall_handler: Box<dyn StallDetectionHandler + 'static>,
    ) -> std::io::Result<StallDetector> {
        let timer = Arc::new(
            sys::timerfd::TimerFd::new(
                sys::timerfd::ClockId::CLOCK_MONOTONIC,
                sys::timerfd::TimerFlags::empty(),
            )
            .map_err(std::io::Error::from)?,
        );
        let tid = unsafe { nix::libc::pthread_self() };
        let terminated = Arc::new(AtomicBool::new(false));
        let sig = stall_handler.signal();
        let timer_handler = std::thread::spawn(enclose::enclose! { (terminated, timer) move || {
            while timer.wait().is_ok() {
                if terminated.load(Ordering::Relaxed) {
                    return
                }
                unsafe { nix::libc::pthread_kill(tid, sig.into()) };
            }
        }});
        let (tx, rx) = crossbeam::channel::bounded(1 << 10);

        Ok(Self {
            timer,
            timer_handler: Some(timer_handler),
            stall_handler,
            id: executor_id,
            terminated,
            tx,
            rx,
        })
    }

    pub(crate) fn enter_task_queue(
        &self,
        queue_handle: TaskQueueHandle,
        queue_name: String,
        start: Instant,
        max_expected_runtime: Duration,
    ) -> Option<StallDetectorGuard<'_>> {
        self.stall_handler
            .high_water_mark(queue_handle, max_expected_runtime)
            .map(|hwm| {
                StallDetectorGuard::new(
                    self,
                    queue_handle,
                    queue_name,
                    start,
                    max_expected_runtime.saturating_add(hwm),
                )
                .expect("Unable to create StallDetectorGuard, giving up")
            })
    }

    pub(crate) fn arm(&self, threshold: Duration) -> nix::Result<()> {
        self.timer.set(
            sys::timerfd::Expiration::OneShot(sys::time::TimeSpec::from(threshold)),
            sys::timerfd::TimerSetTimeFlags::empty(),
        )
    }

    pub(crate) fn disarm(&self) -> nix::Result<()> {
        self.timer.unset()
    }
}

impl Drop for StallDetector {
    fn drop(&mut self) {
        let timer_handler = self.timer_handler.take().unwrap();
        self.terminated.store(true, Ordering::Relaxed);

        self.timer
            .set(
                sys::timerfd::Expiration::Interval(sys::time::TimeSpec::from(
                    Duration::from_millis(1),
                )),
                sys::timerfd::TimerSetTimeFlags::empty(),
            )
            .expect("failed wake the timer for termination");

        let _ = timer_handler.join();
    }
}

pub(crate) struct StallDetectorGuard<'detector> {
    detector: &'detector StallDetector,
    queue_handle: TaskQueueHandle,
    queue_name: String,
    start: Instant,
    threshold: Duration,
}

impl<'detector> StallDetectorGuard<'detector> {
    fn new(
        detector: &'detector StallDetector,
        queue_handle: TaskQueueHandle,
        queue_name: String,
        start: Instant,
        threshold: Duration,
    ) -> nix::Result<Self> {
        detector
            .arm(threshold)
            .expect("Unable to arm stall detector, giving up");
        Ok(Self {
            detector,
            queue_handle,
            queue_name,
            start,
            threshold,
        })
    }
}

impl<'detector> Drop for StallDetectorGuard<'detector> {
    fn drop(&mut self) {
        let _ = self.detector.disarm();

        let mut frames = vec![];
        while let Ok(frame) = self.detector.rx.try_recv() {
            frames.push(frame);
        }
        let mut strace = backtrace::Backtrace::from(frames);

        if strace.frames().is_empty() {
            return;
        }

        let elapsed = self.start.elapsed();
        strace.resolve();
        self.detector.stall_handler.stall(StallDetection {
            executor: self.detector.id,
            queue_name: &self.queue_name,
            queue_handle: self.queue_handle,
            trace: strace,
            budget: self.threshold,
            overage: elapsed.saturating_sub(self.threshold),
        });
    }
}

#[cfg(test)]
mod test {
    use crate::{
        executor::{
            stall::{StallDetection, StallDetectionHandler},
            TaskQueueHandle,
        },
        timer::sleep,
        LocalExecutorBuilder,
    };
    use std::{
        sync::{Arc, RwLock},
        thread,
        time::Duration,
    };

    #[allow(dead_code)]
    #[derive(Debug)]
    pub struct TestStallDetection {
        executor: usize,
        queue_handle: TaskQueueHandle,
        queue_name: String,
        trace: backtrace::Backtrace,
        budget: Duration,
        overage: Duration,
    }

    #[derive(Debug)]
    struct InnerTestHandler {
        detections: Vec<TestStallDetection>,
    }

    #[derive(Clone, Debug)]
    struct TestHandler {
        inner: Arc<RwLock<InnerTestHandler>>,
        signal: u8,
    }

    impl TestHandler {
        fn new(signal: u8) -> Self {
            TestHandler {
                inner: Arc::new(RwLock::new(InnerTestHandler {
                    detections: Vec::new(),
                })),
                signal,
            }
        }
    }

    impl StallDetectionHandler for TestHandler {
        fn high_water_mark(
            &self,
            _queue_handle: TaskQueueHandle,
            max_expected_runtime: Duration,
        ) -> Option<Duration> {
            Some(
                Duration::from_millis((max_expected_runtime.as_millis() as f64 * 0.1) as u64)
                    .max(Duration::from_millis(10)),
            )
        }

        fn signal(&self) -> u8 {
            self.signal
        }

        fn stall(&self, detection: StallDetection<'_>) {
            let mut inner = self.inner.write().unwrap();
            inner.detections.push(TestStallDetection {
                executor: detection.executor,
                queue_handle: detection.queue_handle,
                queue_name: detection.queue_name.to_owned(),
                trace: detection.trace,
                budget: detection.budget,
                overage: detection.overage,
            });
        }
    }

    #[test]
    fn executor_stall_detector() {
        let stall_handler = TestHandler::new(nix::libc::SIGUSR1 as u8);
        LocalExecutorBuilder::default()
            .detect_stalls(Some(Box::new(stall_handler.clone())))
            .preempt_timer(Duration::from_millis(50))
            .make()
            .unwrap()
            .run(async {
                // will trigger the stall detector because we go over budget
                thread::sleep(Duration::from_millis(100));

                let exec = crate::executor();
                assert!(stall_handler.inner.read().unwrap().detections.is_empty());

                exec.yield_task_queue_now().await; // yield the queue

                assert!(
                    stall_handler
                        .inner
                        .write()
                        .unwrap()
                        .detections
                        .pop()
                        .is_some()
                );

                // no stall because < 50ms of un-cooperativeness
                thread::sleep(Duration::from_millis(40));

                assert!(stall_handler.inner.read().unwrap().detections.is_empty());

                exec.yield_task_queue_now().await; // yield the queue

                // no stall because a timer yields internally
                sleep(Duration::from_millis(100)).await;

                exec.yield_task_queue_now().await; // yield the queue

                assert!(stall_handler.inner.read().unwrap().detections.is_empty());

                // trigger one last time
                thread::sleep(Duration::from_millis(100));

                exec.yield_task_queue_now().await; // yield the queue

                assert!(
                    stall_handler
                        .inner
                        .write()
                        .unwrap()
                        .detections
                        .pop()
                        .is_some()
                );

                // Make sure nothing else was reported
                exec.yield_task_queue_now().await; // yield the queue
                assert!(stall_handler.inner.read().unwrap().detections.is_empty());
            });
    }

    #[test]
    fn stall_detector_correct_signal_handler() {
        let mut build_handlers: Vec<(TestHandler, LocalExecutorBuilder)> = Vec::with_capacity(10);
        for i in 1..11 {
            let handler = TestHandler::new(nix::libc::SIGUSR1 as u8);
            let tname = format!("exec{}", i);
            let builder = LocalExecutorBuilder::default()
                .name(&tname)
                .detect_stalls(Some(Box::new(handler.clone())))
                .preempt_timer(Duration::from_millis(50));
            build_handlers.push((handler, builder));
        }
        let mut handles = Vec::with_capacity(10);
        for (handler, builder) in build_handlers.drain(..) {
            let join_handle = builder.spawn(move || async move {
                let exec = crate::executor();
                // will trigger the stall detector because we go over budget
                thread::sleep(Duration::from_millis(100));

                assert!(handler.inner.read().unwrap().detections.is_empty());

                exec.yield_task_queue_now().await; // yield the queue

                let detection = handler.inner.write().unwrap().detections.pop();
                assert!(detection.is_some());
                assert_eq!(detection.unwrap().executor, exec.id())
            });
            handles.push(join_handle.unwrap());
        }
        for handle in handles.drain(..) {
            handle.join().unwrap();
        }
    }

    #[test]
    fn stall_detector_multiple_signals() {
        let mut signals = vec![nix::libc::SIGALRM as u8, nix::libc::SIGUSR1 as u8, nix::libc::SIGUSR2 as u8];
        let mut build_handlers: Vec<(TestHandler, LocalExecutorBuilder)> = Vec::with_capacity(signals.len());
        let mut handles = Vec::with_capacity(signals.len());
        for (i, signal) in signals.drain(..).enumerate() {
            let handler = TestHandler::new(signal);
            let tname = format!("exec{}", i);
            let builder = LocalExecutorBuilder::default()
                .name(&tname)
                .detect_stalls(Some(Box::new(handler.clone())))
                .preempt_timer(Duration::from_millis(50));
            build_handlers.push((handler, builder));
        }
        for (handler, builder) in build_handlers.drain(..) {
            let join_handle = builder.spawn(move || async move {
                let exec = crate::executor();
                // will trigger the stall detector because we go over budget
                thread::sleep(Duration::from_millis(100));

                assert!(handler.inner.read().unwrap().detections.is_empty());

                exec.yield_task_queue_now().await; // yield the queue

                let detection = handler.inner.write().unwrap().detections.pop();
                assert!(detection.is_some());
                assert_eq!(detection.unwrap().executor, exec.id())
            });
            handles.push(join_handle.unwrap());
        }
        for handle in handles.drain(..) {
            handle.join().unwrap();
        }
    }
}
