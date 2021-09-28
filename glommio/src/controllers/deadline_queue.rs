// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.

use crate::{
    channels::local_channel::{self, LocalReceiver, LocalSender},
    controllers::ControllerStatus,
    enclose,
    task,
    Latency,
    Shares,
    SharesManager,
    TaskQueueHandle,
};
use futures_lite::StreamExt;
use log::{trace, warn};
use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    fmt,
    future::Future,
    io,
    pin::Pin,
    rc::Rc,
    time::{Duration, Instant},
};

/// Items going into the [`DeadlineQueue`] must implement this trait.
///
/// It allows the [`DeadlineQueue`] to understand the progress and expectations
/// of processing this item
///
/// [`DeadlineQueue`]: struct.DeadlineQueue.html
pub trait DeadlineSource {
    /// What type is returned by the [`action`] method
    ///
    /// [`action`]: trait.DeadlineSource.html#tymethod.action
    type Output;

    /// Returns a [`Duration`] indicating when we would like this operation to
    /// complete.
    ///
    /// It is calculated from the point of Queueing, not from the point in which
    /// the operation starts.
    fn expected_duration(&self) -> Duration;

    /// The action to execute. Usually your struct will implement an async
    /// function that you want to see completed at a particular deadline,
    /// and then the implementation of this would be:
    ///
    /// ```ignore
    /// fn action(&self) -> Pin<Box<dyn Future<Output = io::Result<Duration>> + 'static>> {
    ///    Box::pin(self.my_action())
    /// }
    /// ```
    fn action(self: Rc<Self>) -> Pin<Box<dyn Future<Output = Self::Output> + 'static>>;

    /// The total amount of units to be processed.
    ///
    /// This could be anything you want:
    /// * If you are flushing a file, this could indicate the size in bytes of
    ///   the buffer
    /// * If you are scanning an array, this could be the number of elements.
    ///
    /// As long as this quantity is consistent with [`processed_units`] the
    /// controllers should work.
    ///
    /// This need not be static. On the contrary: as you are filling a new
    /// buffer you can already add it to the queue and increase its total
    /// units as the buffer is written to. This can lead to smoother
    /// operation as opposed to just adding a lot of units at once.
    ///
    /// [`processed_units`]: trait.DeadlineSource.html#tymethod.processed_units
    fn total_units(&self) -> u64;

    /// The amount of units that were already processed.
    ///
    /// The units should match the quantities specified in [`total_units`].
    /// The more often the system is made aware of processed units, the smoother
    /// the controller will be.
    ///
    /// For example, you can buffer all updates and just inform that
    /// processed_units == total_units at the end of the process, but then
    /// the controller would be a step function.
    ///
    /// [`total_units`]: trait.DeadlineSource.html#tymethod.total_units
    fn processed_units(&self) -> u64;
}

impl<T> fmt::Debug for dyn DeadlineSource<Output = T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DeadlineSource processed {} out of {}",
            self.processed_units(),
            self.total_units()
        )
    }
}

#[derive(Debug)]
/// Allows the priority of the [`DeadlineQueue`] to be temporarily bumped.
///
/// The priority is bumped for as long as this object is alive. This is useful
/// in situations where, despite having a deadline we may never want the
/// priority to fall too low.
///
/// This could be because a user started watching the process, a shutdown
/// sequence was initiated, etc.
///
/// [`DeadlineQueue`]: struct.DeadlineQueue.html
pub struct PriorityBump<T> {
    queue: Rc<InnerQueue<T>>,
}

impl<T> PriorityBump<T> {
    fn new(queue: Rc<InnerQueue<T>>) -> PriorityBump<T> {
        queue.min_shares.set(250);
        PriorityBump { queue }
    }
}

impl<T> Drop for PriorityBump<T> {
    fn drop(&mut self) {
        self.queue.min_shares.set(1);
    }
}

type QueueItem<T> = Rc<dyn DeadlineSource<Output = T>>;

#[derive(Debug)]
struct InnerQueue<T> {
    queue: RefCell<VecDeque<(Instant, QueueItem<T>)>>,
    last_admitted: Cell<Instant>,

    last_adjusted: Cell<Instant>,
    last_shares: Cell<usize>,
    accumulated_error: Cell<f64>,
    adjustment_period: Duration,
    last_error: Cell<f64>,
    min_shares: Cell<usize>,

    state: Cell<ControllerStatus>,
}

impl<T> SharesManager for InnerQueue<T> {
    /// PI controller for shares.
    ///
    /// We are not dealing with the derivative constant here: it is too risky
    /// given the generic nature of the processes under control.
    ///
    /// The variable we are controlling is the speed at which the units are
    /// processed. Also, because we don't know what units are and different
    /// items in the queue may have different magnitudes we need to work
    /// with normalized units.
    ///
    /// The error is the difference between our effective speed and the desired
    /// speed:
    ///
    ///    e(t) = units_expected/delta_t -  units_processed/ delta_t,
    ///
    ///  and because we are normalizing:
    ///
    ///    e(t) = 1/delta_t * (1 - units_processed / units_expected)
    ///
    ///  The controller output is then:
    ///
    ///    u(t) = Kp * e(t) + Ki + Integral{0, t} e(t)
    ///
    ///  There are a couple of practical problems with that.
    ///
    ///  * The first is that the output of the controller would be zero if we
    ///    there are no error
    ///  * The second is that our integral term would accumulate errors that may
    ///    not be comparable as we accumulate artifacts of the delta_t
    ///    calculation (as we'll never in practice keep delta_t constant)
    ///
    ///  The way we'll solve this is by expressing an alternate error E(T) which
    /// is  essentially the integral of e(t) in time:
    ///
    ///    E(t) = 1 - units_processed / units_expected.
    ///
    ///  That is easy to compute, as it is essentially the total count of units
    /// for  all items in the queue, both processed and expected.
    ///
    ///  We can now express our output function as the derivative of U(t), the
    /// output  function for the integral of the error:
    ///
    ///    u(t) = d(U(t)) / dt = Kp * dE(t) /dt + Ki * E(t)
    ///
    ///  Now we are calculating how many shares should be added or removed to
    /// the  last output, and not how much the shares should be. It also
    /// eliminates any  dependency on time when calculating the error which
    /// increases resiliency.
    fn shares(&self) -> usize {
        if let ControllerStatus::Disabled(shares) = self.state.get() {
            return shares;
        }

        let queue = self.queue.borrow();
        let mut expected = 0.0;
        let mut processed = 0.0;
        let now = Instant::now();

        for (exp, source) in queue.iter() {
            let remaining_time = exp.saturating_duration_since(now);
            trace!(
                "Remaining time for this source: {:#?}, total_units {}",
                remaining_time,
                source.total_units()
            );
            let time_fraction =
                1.0 - (remaining_time.as_secs_f64() / source.expected_duration().as_secs_f64());
            if remaining_time.as_nanos() == 0 && now.saturating_duration_since(*exp).as_secs() > 5 {
                // already too late, bump it up hard
                self.last_shares.set(1000);
                return 1000;
            }
            expected += source.total_units() as f64 * time_fraction;
            processed += source.processed_units() as f64;
        }

        // so little time has passed we can't really make any useful prediction
        if expected < 0.01 {
            return self.last_shares.get();
        }

        let error = 1.0 - processed / expected;
        let accumulated_error = self.accumulated_error.get();
        let acc = accumulated_error + error;
        self.accumulated_error.set(acc);
        let delta_error = error - self.last_error.get();
        self.last_error.set(error);

        // How did we pick our constants:
        //  * As with any stable PI controller we want the bulk of our gain to come from
        //    P.
        //  * As we normalize the maximum error to 1 we know that the gain should be at
        //    most 1000
        //  * physically, Ki can be expressed as Kp / Tau where Tau is a time constant,
        //    roughly equivalent to how many periods need to pass for the integral term
        //    to generate the same gain as the proportional term. We set that to 6 so
        //    the controller is not too sluggish, which is around 1.5 seconds on the
        //    default 250ms adjustment period.
        //
        //  We can then write X + X/6 = 1000, and solving for X we have the constants
        // below
        let kp = 850.0;
        let ki = kp / 6.0;
        let dshares = ki * error + kp * delta_error;

        let mut shares = (dshares + self.last_shares.get() as f64) as isize;
        shares = std::cmp::min(shares, 1000);
        shares = std::cmp::max(shares, self.min_shares.get() as isize);
        let shares = shares as usize;

        trace!(
            "processed: {}. expected: {} error: {}, delta_error {} , kp term {}, ki term {}, \
             shares: {}",
            processed,
            expected,
            error,
            delta_error,
            ki * error,
            kp * delta_error,
            shares
        );
        self.last_shares.set(shares);
        shares
    }

    fn adjustment_period(&self) -> Duration {
        self.adjustment_period
    }
}

impl<T> InnerQueue<T> {
    fn new(adjustment_period: Duration) -> Self {
        let now = Instant::now();
        Self {
            queue: RefCell::new(VecDeque::new()),
            last_admitted: Cell::new(now),
            last_adjusted: Cell::new(now),
            last_shares: Cell::new(1),
            accumulated_error: Cell::new(0.0),
            adjustment_period,
            last_error: Cell::new(0.0),
            min_shares: Cell::new(1),
            state: Cell::new(ControllerStatus::Enabled),
        }
    }

    fn admit(&self, source: Rc<dyn DeadlineSource<Output = T>>) -> io::Result<()> {
        let expiration = Instant::now()
            .checked_add(source.expected_duration())
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        expiration
            .checked_duration_since(self.last_admitted.get())
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        self.last_admitted.set(expiration);
        let mut queue = self.queue.borrow_mut();
        queue.push_back((expiration, source.clone()));
        Ok(())
    }
}

#[derive(Debug)]
/// Glommio's scheduler is based on [`Shares`]: the more shares, the more
/// resources the task will receive.
///
/// There are situations however in which we don't want shares to grow too high:
/// for instance, a background process that is flushing a file to storage. If it
/// were to run at full speed, it would rob us of precious resources that we'd
/// rather dedicate to the rest of the application.
///
/// However, we don't want it to run too slowly either, as it may never
/// complete.
///
/// The "right amount" of shares is not even application dependent: it is time
/// dependent! As the load of the system changes, what is "too high" or "too
/// low" changes too.
///
/// The `DeadlineQueue` uses a feedback loop controller, not unlike the ones in
/// car's cruise controls to dynamically and automatically adjust shares so that
/// the process is slowed down using as few resources as possible but still
/// finishes before its deadline.
///
/// For example, you may want to flush a file and would like it to finish in
/// 10min because you have an agent that copies files every 10 minutes. You name
/// it!
///
/// Controlling processes is tricky, and you should keep some things in mind for
/// best results:
///
/// * Control loops have a set time. It takes a while for the system to
///   stabilize so this is better suited for long processes (Deadline is much
///   higher than the adjustment period)
/// * Control loops add overhead, so setting the adjustment period too low may
///   not be the best way to make sure that the deadline is much higher than the
///   adjustment period =)
/// * Control loops have *dead time*. In control theory, dead time is the time
///   that passes between the application of the control decision and its
///   effects being seen. In our case, the scheduler may not schedule us for a
///   long time, especially if the shares are low.
/// * Control loops work better the faster and smoother the response is. Let's
///   use an example flushing a file: you may be moving data to the file
///   internal buffers, but they are not *flushed* to the media. When the data
///   is finally flushed a giant bubble is inserted into the control loop. The
///   characteristics of the system will radically change. Contract that for
///   instance with O_DIRECT files, where writing to the file means writing to
///   the media: smooth and fast feedback!
///
///   The moral of the story is:
///    * do not use this with buffered files or other buffered sinks where the
///      real physical response is delayed
///    * do not set the adjustment period too low
///    * do not use this very short-lived processes.
///
/// To calculate the speed of the process, the needs of all elements in the
/// queue are considered.
///
/// Let's say, for instance, that you queue items A, B and C, each with 10,000
/// units finishing respectively in 1, 2 and 3 minutes. From the point of view
/// of the system, all units from A and B need to be flushed before C can start
/// so that is taken into account: the system needs to set its speed to 10,000
/// points per minute so that the entire queue is flushed in 3 minutes.
pub struct DeadlineQueue<T> {
    tq: TaskQueueHandle,
    sender: LocalSender<Rc<dyn DeadlineSource<Output = T>>>,
    responder: LocalReceiver<T>,
    handle: task::join_handle::JoinHandle<()>,
    queue: Rc<InnerQueue<T>>,
}

impl<T: 'static> DeadlineQueue<T> {
    /// Creates a new `DeadlineQueue` with a given `name` and `adjustment
    /// period`
    ///
    /// Internally the `DeadlineQueue` spawns a new task queue with dynamic
    /// shares in which it will execute the tasks that were registered.
    ///
    /// # Examples
    /// ```
    /// use glommio::{controllers::DeadlineQueue, LocalExecutor};
    /// use std::time::Duration;
    ///
    /// let ex = LocalExecutor::default();
    ///
    /// ex.run(async {
    ///     DeadlineQueue::<usize>::new("example", Duration::from_millis(250));
    /// });
    /// ```
    pub fn new(name: &'static str, adjustment_period: Duration) -> DeadlineQueue<T> {
        let queue = Rc::new(InnerQueue::new(adjustment_period));
        // We could always dispatch into the latency queue, but since that effectively
        // means putting requests in a different io_uring we'll avoid doing that
        // unless the process is very sensitive. Because of dead time it
        // wouldn't be a bad idea to even have a minimum here. But we'll just
        // document it and leave it to the user.
        let lat = {
            if adjustment_period < Duration::from_millis(100) {
                Latency::Matters(adjustment_period)
            } else {
                Latency::NotImportant
            }
        };

        let tq = crate::executor().create_task_queue(Shares::Dynamic(queue.clone()), lat, name);
        let (sender, receiver): (LocalSender<QueueItem<T>>, LocalReceiver<QueueItem<T>>) =
            local_channel::new_bounded(1);

        let (response_sender, responder): (LocalSender<T>, LocalReceiver<T>) =
            local_channel::new_bounded(1);

        let queue_weak = Rc::downgrade(&queue);

        let handle = crate::spawn_local_into(
            enclose! { (queue_weak) async move {
                let response = Rc::new(response_sender);
                let mut stream = receiver.stream();
                while let Some(request) = stream.next().await {
                    let res = request.action().await;
                    // now that we have executed the action, pop it from
                    // the queue. We must do it regardless of whether we succeeded.
                    //
                    // It is legal for the queue not to be present anymore in
                    // case the DeadlineQueue itself was destroyed after the action
                    // was done. But if it does exist we expect to be able to pop
                    // the element: so we unwrap() pop_front().
                    //
                    // Note that because this is a queue with concurrency of one we
                    // can safely pop_front(). If we ever do this with higher concurrency
                    // (very unlikely), then we need to move to a more complex data structure.
                    if let Some(queue) = queue_weak.upgrade() {
                        let mut queue = queue.queue.borrow_mut();
                        queue.pop_front().unwrap();
                    }
                    if response.send(res).await.is_err() {
                        warn!("receiver channel broken!");
                        break;
                    }
                }
            }},
            tq,
        )
        .unwrap()
        .detach();

        DeadlineQueue {
            tq,
            sender,
            responder,
            handle,
            queue,
        }
    }

    /// Pushes a new [`DeadlineSource`] into this queue
    ///
    /// Returns an [`io::Result`] wrapping the result of the operation.
    ///
    /// # Examples:
    ///
    /// ```
    /// use futures_lite::{future::ready, Future};
    /// use glommio::{
    ///     controllers::{DeadlineQueue, DeadlineSource},
    ///     LocalExecutor,
    /// };
    /// use std::{io, pin::Pin, rc::Rc, time::Duration};
    ///
    /// struct Example {}
    ///
    /// impl DeadlineSource for Example {
    ///     type Output = usize;
    ///
    ///     fn expected_duration(&self) -> Duration {
    ///         Duration::from_secs(1)
    ///     }
    ///
    ///     fn action(self: Rc<Self>) -> Pin<Box<dyn Future<Output = Self::Output> + 'static>> {
    ///         Box::pin(ready(1))
    ///     }
    ///
    ///     fn total_units(&self) -> u64 {
    ///         1
    ///     }
    ///
    ///     fn processed_units(&self) -> u64 {
    ///         1
    ///     }
    /// }
    ///
    /// let ex = LocalExecutor::default();
    ///
    /// ex.run(async {
    ///     let mut queue = DeadlineQueue::new("example", Duration::from_millis(250));
    ///     let res = queue.push_work(Rc::new(Example {})).await;
    ///     assert_eq!(res.unwrap(), 1);
    /// });
    /// ```
    pub async fn push_work(&self, source: Rc<dyn DeadlineSource<Output = T>>) -> io::Result<T> {
        self.queue.admit(source.clone())?;
        self.sender.send(source.clone()).await?;
        self.responder.recv().await.ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, "no response from response channel")
        })
    }

    /// Returns the TaskQueueHandle associated with this controller
    pub fn task_queue(&self) -> TaskQueueHandle {
        self.tq
    }

    /// Temporarily bumps the priority of this DeadlineQueue
    ///
    /// The bump happens by making sure that the shares never fall below
    /// a minimum (250). If the output of the controller is already higher than
    /// that then this has no effect.
    pub fn bump_priority(&self) -> PriorityBump<T> {
        PriorityBump::new(self.queue.clone())
    }

    /// Disables the controller.
    ///
    /// Instead, the process being controlled will now have static shares
    /// defined by the `shares` argument (between 1 and 1000).
    pub fn disable(&self, mut shares: usize) {
        shares = std::cmp::min(shares, 1000);
        shares = std::cmp::max(shares, 1);
        self.queue.state.set(ControllerStatus::Disabled(shares));
    }

    /// Enables the controller.
    ///
    /// This is a no-op if the controller was already enabled. If it had been
    /// manually disabled then it moves back to automatic mode.
    pub fn enable(&self) {
        self.queue.state.set(ControllerStatus::Enabled);
    }

    /// Queries the controller for its status.
    ///
    /// The possible statuses are defined by the [`ControllerStatus`] enum
    ///
    /// [`ControllerStatus`]: enum.ControllerStatus.html
    pub fn status(&self) -> ControllerStatus {
        self.queue.state.get()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        enclose,
        timer::{Timer, TimerActionRepeat},
    };

    struct DeadlineSourceTest {
        duration: Duration,
        total_units: usize,
        processed_units: Cell<usize>,
        drop_guarantee: Rc<Cell<bool>>,
    }

    impl DeadlineSourceTest {
        fn new(duration: Duration, total_units: usize) -> Rc<Self> {
            Rc::new(Self {
                duration,
                total_units,
                processed_units: Cell::new(0),
                drop_guarantee: Rc::new(Cell::new(false)),
            })
        }
        async fn wait(self: Rc<Self>) -> usize {
            Timer::new(self.duration).await;
            0
        }
    }

    impl Drop for DeadlineSourceTest {
        fn drop(&mut self) {
            self.drop_guarantee.set(true);
        }
    }

    impl DeadlineSource for DeadlineSourceTest {
        type Output = usize;

        fn expected_duration(&self) -> Duration {
            self.duration
        }

        fn action(self: Rc<Self>) -> Pin<Box<dyn Future<Output = Self::Output> + 'static>> {
            Box::pin(self.wait())
        }

        fn total_units(&self) -> u64 {
            self.total_units as _
        }

        fn processed_units(&self) -> u64 {
            self.processed_units.get() as _
        }
    }

    #[test]
    fn deadline_queue_does_not_accept_non_monotonic_durations() {
        test_executor!(async move {
            let queue = Rc::new(DeadlineQueue::new("example", Duration::from_millis(1)));
            let tq = crate::spawn_local(enclose! { (queue) async move {
                let test = DeadlineSourceTest::new(Duration::from_secs(2_u64), 1);
                let res = queue.push_work(test).await.unwrap();
                assert_eq!(res, 0);
            }})
            .detach();

            Timer::new(Duration::from_millis(1)).await;
            let test = DeadlineSourceTest::new(Duration::from_secs(1_u64), 1);
            match queue.push_work(test).await {
                Err(x) => assert_eq!(x.kind(), io::ErrorKind::InvalidInput),
                Ok(_) => panic!("should have failed"),
            }
            tq.await;
        });
    }

    #[test]
    fn deadline_queue_behaves_well_with_zero_total_units() {
        test_executor!(async move {
            let queue = Rc::new(DeadlineQueue::<usize>::new(
                "example",
                Duration::from_millis(1),
            ));
            let test = DeadlineSourceTest::new(Duration::from_secs(1_u64), 0);
            let res = queue.push_work(test).await.unwrap();
            assert_eq!(res, 0);
        });
    }

    #[test]
    fn deadline_queue_successfully_drops_item_when_done() {
        test_executor!(async move {
            let queue = Rc::new(DeadlineQueue::<usize>::new(
                "example",
                Duration::from_millis(1),
            ));
            let test = DeadlineSourceTest::new(Duration::from_secs(1_u64), 0);
            let drop_happens = test.drop_guarantee.clone();
            let res = queue.push_work(test).await.unwrap();
            assert_eq!(res, 0);
            assert!(drop_happens.get());
        });
    }

    #[test]
    fn deadline_queue_behaves_well_if_we_process_too_much() {
        test_executor!(async move {
            let queue = Rc::new(DeadlineQueue::<usize>::new(
                "example",
                Duration::from_millis(1),
            ));
            let test = DeadlineSourceTest::new(Duration::from_secs(1_u64), 1000);
            let tq = crate::spawn_local(enclose! { (queue, test) async move {
                let res = queue.push_work(test).await.unwrap();
                assert_eq!(res, 0);
            }})
            .detach();

            Timer::new(Duration::from_millis(2)).await;
            test.processed_units.set(1000 * 1000);
            Timer::new(Duration::from_millis(2)).await;
            assert_eq!(queue.queue.shares(), 1);
            tq.await;
        });
    }

    #[test]
    fn deadline_queue_shares_increase_if_we_dont_process() {
        test_executor!(async move {
            let queue = Rc::new(DeadlineQueue::<usize>::new(
                "example",
                Duration::from_millis(1),
            ));
            let test = DeadlineSourceTest::new(Duration::from_secs(1_u64), 1000);
            let tq = crate::spawn_local(enclose! { (queue, test) async move {
                let res = queue.push_work(test).await.unwrap();
                assert_eq!(res, 0);
            }})
            .detach();

            Timer::new(Duration::from_millis(900)).await;
            assert!(queue.queue.shares() > 800);
            tq.await;
        });
    }

    #[test]
    fn deadline_queue_shares_ok_if_we_process_smoothly() {
        test_executor!(async move {
            let queue = Rc::new(DeadlineQueue::<usize>::new(
                "example",
                Duration::from_millis(10),
            ));
            let test = DeadlineSourceTest::new(Duration::from_secs(1_u64), 1000);
            let tq = crate::spawn_local(enclose! { (queue, test) async move {
                let res = queue.push_work(test).await.unwrap();
                assert_eq!(res, 0);
            }})
            .detach();

            let start = Instant::now();
            let last_shares = Rc::new(Cell::new(0));
            let action = TimerActionRepeat::repeat(move || {
                enclose! { (queue, test, last_shares) async move {
                    let elapsed = start.elapsed().as_millis();
                    let shares = queue.queue.shares();
                    let old = last_shares.replace(shares) as isize;
                    if elapsed > 500 && elapsed < 850 {
                        let diff = old - shares as isize;
                        assert!(diff.abs() < 200, "Found diff: {}", diff);
                    }
                    if test.processed_units.replace(elapsed as usize) < 1000 {
                        Some(Duration::from_millis(50))
                    } else {
                        None
                    }
                }}
            });
            tq.await;
            action.join().await;
        });
    }

    #[test]
    fn deadline_queue_second_queued_item_increases_slope() {
        test_executor!(async move {
            let queue = Rc::new(DeadlineQueue::new("example", Duration::from_millis(1)));
            let tq = crate::spawn_local(enclose! { (queue) async move {
                let test = DeadlineSourceTest::new(Duration::from_secs(1_u64), 1);
                let res = queue.push_work(test).await.unwrap();
                assert_eq!(res, 0);
            }})
            .detach();

            Timer::new(Duration::from_millis(2)).await;
            let shares_first = queue.queue.shares();
            let tq2 = crate::spawn_local(enclose! { (queue) async move {
                let test = DeadlineSourceTest::new(Duration::from_secs(1_u64), 1000);
                let res = queue.push_work(test).await.unwrap();
                assert_eq!(res, 0);
            }})
            .detach();

            Timer::new(Duration::from_millis(2)).await;
            let shares_second = queue.queue.shares();
            // The second element that we push should rush the first.
            assert!(shares_second >= shares_first * 20);
            tq.await;
            tq2.await;
        });
    }
}
