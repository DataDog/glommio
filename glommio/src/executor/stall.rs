// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021 Datadog, Inc.
//
#![allow(dead_code)]

use nix::sys;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

#[derive(Debug)]
pub(crate) struct StallDetector {
    timer: Arc<sys::timerfd::TimerFd>,
    handler: Option<JoinHandle<()>>,
    id: usize,
    terminated: Arc<AtomicBool>,
    //NOTE: we don't use signal_hook::low_level::channel as backtraces
    //      have too many elements
    pub(crate) tx: crossbeam::channel::Sender<backtrace::BacktraceFrame>,
    pub(crate) rx: crossbeam::channel::Receiver<backtrace::BacktraceFrame>,
}

impl StallDetector {
    pub(crate) fn new(executor_id: usize) -> std::io::Result<StallDetector> {
        let timer = Arc::new(
            sys::timerfd::TimerFd::new(
                sys::timerfd::ClockId::CLOCK_MONOTONIC,
                sys::timerfd::TimerFlags::empty(),
            )
            .map_err(std::io::Error::from)?,
        );
        let tid = unsafe { nix::libc::pthread_self() };
        let terminated = Arc::new(AtomicBool::new(false));
        let timer_handler = std::thread::spawn(enclose::enclose! { (terminated, timer) move || {
            while timer.wait().is_ok() {
                if terminated.load(Ordering::Relaxed) {
                    return
                }
                unsafe { nix::libc::pthread_kill(tid, nix::libc::SIGUSR1) };
            }
        }});
        let (tx, rx) = crossbeam::channel::bounded(1 << 10);

        Ok(Self {
            timer,
            handler: Some(timer_handler),
            id: executor_id,
            terminated,
            tx,
            rx,
        })
    }

    pub(crate) fn enter_task_queue(
        &self,
        queue_name: String,
        start: Instant,
        max_expected_runtime: Duration,
    ) -> nix::Result<StallDetectorGuard<'_>> {
        // We consider a queue to be stalling the system if it failed to yield in due
        // time. For a given maximum expected runtime, we allow a margin of error f 10%
        // (and an absolute minimum of 10ms) after which we record a stacktrace. i.e. a
        // task queue has should return shortly after `need_preempt()` returns
        // true or the stall detector triggers. For example::
        // * If a task queue has a preempt timer of 100ms the the stall detector
        // triggers if it doesn't yield after running for 110ms.
        // * If a task queue has a preempt timer of 100ms the the stall detector
        // triggers if it doesn't yield after running for 15ms.

        let error_margin =
            Duration::from_millis((max_expected_runtime.as_millis() as f64 * 0.1) as u64)
                .max(Duration::from_millis(10));
        StallDetectorGuard::new(
            self,
            queue_name,
            start,
            max_expected_runtime.saturating_add(error_margin),
        )
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
        let timer_handler = self.handler.take().unwrap();
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
    queue_name: String,
    start: Instant,
    threshold: Duration,
}

impl<'detector> StallDetectorGuard<'detector> {
    fn new(
        detector: &'detector StallDetector,
        queue_name: String,
        start: Instant,
        threshold: Duration,
    ) -> nix::Result<Self> {
        detector.disarm().unwrap();
        detector.arm(threshold).unwrap();
        Ok(Self {
            detector,
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
        log::warn!(
            "[stall-detector -- executor {}] task queue {} went over-budget: {:#?} (budget: \
             {:#?}). Backtrace: {:#?}",
            self.detector.id,
            &self.queue_name,
            elapsed,
            self.threshold,
            strace,
        );
    }
}

#[cfg(test)]
mod test {
    use crate::{timer::sleep, LocalExecutorBuilder};
    use logtest::Logger;
    use std::time::{Duration, Instant};

    enum ExpectedLog {
        Expected(&'static str),
        NotExpected(&'static str),
    }

    fn search_logs_for(logger: &mut Logger, expected: ExpectedLog) -> bool {
        let mut found = false;
        while let Some(event) = logger.pop() {
            match expected {
                ExpectedLog::Expected(str) => found |= event.args().contains(str),
                ExpectedLog::NotExpected(str) => found |= event.args().contains(str),
            }
        }

        match expected {
            ExpectedLog::Expected(_) => found,
            ExpectedLog::NotExpected(_) => !found,
        }
    }

    #[test]
    fn executor_stall_detector() {
        LocalExecutorBuilder::default()
            .detect_stalls(true)
            .preempt_timer(Duration::from_millis(50))
            .make()
            .unwrap()
            .run(async {
                let mut logger = Logger::start();
                let now = Instant::now();

                // will trigger the stall detector because we go over budget
                while now.elapsed() < Duration::from_millis(100) {}

                assert!(search_logs_for(
                    &mut logger,
                    ExpectedLog::NotExpected("task queue default went over-budget"),
                ));

                crate::executor().yield_task_queue_now().await; // yield the queue

                assert!(search_logs_for(
                    &mut logger,
                    ExpectedLog::Expected("task queue default went over-budget")
                ));

                // no stall because < 50ms of un-cooperativeness
                let now = Instant::now();
                while now.elapsed() < Duration::from_millis(40) {}

                assert!(search_logs_for(
                    &mut logger,
                    ExpectedLog::NotExpected("task queue default went over-budget"),
                ));

                crate::executor().yield_task_queue_now().await; // yield the queue

                // no stall because a timer yields internally
                sleep(Duration::from_millis(100)).await;

                crate::executor().yield_task_queue_now().await; // yield the queue

                assert!(search_logs_for(
                    &mut logger,
                    ExpectedLog::NotExpected("task queue default went over-budget"),
                ));

                // trigger one last time
                let now = Instant::now();
                while now.elapsed() < Duration::from_millis(100) {}

                crate::executor().yield_task_queue_now().await; // yield the queue

                assert!(search_logs_for(
                    &mut logger,
                    ExpectedLog::Expected("task queue default went over-budget")
                ));
            });
    }
}
