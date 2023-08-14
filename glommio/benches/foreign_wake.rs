use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    rc::Rc,
    time::{Duration, Instant},
};

use futures_lite::future::yield_now;
use hdrhistogram::Histogram;
use tokio::{
    runtime::{Builder, Handle},
    sync::{mpsc, oneshot},
};

use glommio::{enclose, prelude::*};

#[derive(Debug, Eq, PartialEq, Hash)]
enum Metric {
    // Time spent staying in the sending channel
    Glommio2Tokio,

    // Time between taken from the sending channel
    // and the start of request processing
    TokioSchedule,

    // Time used by request processing
    TokioProcess,

    // Time spent staying in the responding channel
    Tokio2Glommio,
}

struct Metrics(HashMap<Metric, Histogram<u64>>);

impl Metrics {
    fn new() -> Self {
        let mut metrics = HashMap::new();
        metrics.insert(Metric::Glommio2Tokio, Histogram::new(3).unwrap());
        metrics.insert(Metric::TokioSchedule, Histogram::new(3).unwrap());
        metrics.insert(Metric::TokioProcess, Histogram::new(3).unwrap());
        metrics.insert(Metric::Tokio2Glommio, Histogram::new(3).unwrap());
        Self(metrics)
    }

    fn record(&mut self, timeline: Timeline) {
        for (metric, delay) in timeline.delays {
            self.0
                .get_mut(&metric)
                .unwrap()
                .record(delay.as_micros() as _)
                .unwrap();
        }
    }

    fn metric(&self, metric: Metric) -> &Histogram<u64> {
        self.0.get(&metric).unwrap()
    }

    fn print(&self) {
        println!(
            "{:14}  {:>6} {:>6} {:>6} {:>6} {:>6}",
            "Label", "Min", "Mean", "P99", "P99.9", "Max"
        );
        Self::print_histogram("glommio->tokio", self.metric(Metric::Glommio2Tokio));
        Self::print_histogram("tokio schedule", self.metric(Metric::TokioSchedule));
        Self::print_histogram("tokio process", self.metric(Metric::TokioProcess));
        Self::print_histogram("tokio->glommio", self.metric(Metric::Tokio2Glommio));
    }

    fn print_histogram(label: &str, h: &Histogram<u64>) {
        println!(
            "{:14}: {:6.3} {:6.3} {:6.3} {:6.3} {:6.3}",
            label,
            h.min() as f64 / 1000.0,
            h.mean() / 1000.0,
            h.value_at_percentile(99.0) as f64 / 1000.0,
            h.value_at_percentile(99.9) as f64 / 1000.0,
            h.max() as f64 / 1000.0
        );
    }
}

#[derive(Debug)]
struct Timeline {
    last_timestamp: Instant,
    delays: HashMap<Metric, Duration>,
}

impl Timeline {
    fn new() -> Self {
        Self {
            last_timestamp: Instant::now(),
            delays: HashMap::new(),
        }
    }

    fn record(&mut self, metric: Metric) {
        let start = self.last_timestamp;
        self.last_timestamp = Instant::now();
        self.delays.insert(metric, self.last_timestamp - start);
    }
}

fn main() {
    test_latency(Latency::NotImportant);
    test_latency(Latency::Matters(Duration::from_millis(100)));
}

fn test_latency(latency_req: Latency) {
    println!();
    println!("Latency requirement: {latency_req:?}");

    let runtime = Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let (sender, mut receiver) = mpsc::unbounded_channel::<(Timeline, oneshot::Sender<Timeline>)>();

    runtime.spawn(async move {
        let handle = Handle::current();
        while let Some((mut timeline, sender)) = receiver.recv().await {
            timeline.record(Metric::Glommio2Tokio);
            handle.spawn(async move {
                timeline.record(Metric::TokioSchedule);
                tokio::time::sleep(Duration::from_micros(fastrand::u64(500..1000))).await;
                timeline.record(Metric::TokioProcess);
                sender.send(timeline).unwrap();
            });
        }
    });

    LocalExecutorBuilder::default()
        .spawn(move || async move {
            let tq = glommio::executor().create_task_queue(
                Shares::Static(1000),
                latency_req,
                "test_queue",
            );

            glommio::spawn_local_into(
                async move {
                    let mut remaining = 100_000;
                    let pending_requests = Rc::new(Cell::new(0));
                    let metrics = Rc::new(RefCell::new(Metrics::new()));

                    while remaining > 0 {
                        while pending_requests.get() < 1000 {
                            for _ in 0..10 {
                                remaining -= 1;
                                let (response_sender, response_receiver) = oneshot::channel();
                                sender.send((Timeline::new(), response_sender)).unwrap();
                                pending_requests.set(pending_requests.get() + 1);

                                spawn_local(enclose!((pending_requests, metrics) async move {
                                    let mut timeline = response_receiver.await.unwrap();
                                    pending_requests.set(pending_requests.get() - 1);
                                    timeline.record(Metric::Tokio2Glommio);
                                    metrics.borrow_mut().record(timeline);
                                }))
                                .detach();
                            }
                            yield_if_needed().await;
                        }
                        yield_now().await;
                    }

                    while pending_requests.get() > 0 {
                        glommio::timer::sleep(Duration::from_millis(10)).await;
                    }
                    metrics.borrow().print();
                },
                tq,
            )
            .unwrap()
            .await;
        })
        .unwrap()
        .join()
        .unwrap();
}
