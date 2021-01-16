//! Benchmark the noop performance of Glommio. This is a stress test for the executor/reactor
//! infrastructure.

use glommio::{LocalExecutorBuilder, Task};
use std::fmt;
use std::time::{Duration, Instant};

struct Bench {
    num_tasks: u64,
    num_events: u64,
}

const BENCH_RUNS: &'static [Bench] = &[
    Bench {
        num_tasks: 100,
        num_events: 10_000_000,
    },
    Bench {
        num_tasks: 1_000,
        num_events: 10_000_000,
    },
    Bench {
        num_tasks: 10_000,
        num_events: 10_000_000,
    },
    Bench {
        num_tasks: 100_000,
        num_events: 10_000_000,
    },
    Bench {
        num_tasks: 1_000_000,
        num_events: 10_000_000,
    },
];

struct Measurement {
    total_events: u64,
    total_tasks: u64,
    duration: Duration,
}

impl fmt::Display for Measurement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            r"completed submission of {} noop events across {} tasks, {} per task
            duration: {:?}",
            self.total_events,
            self.total_tasks,
            self.total_events / self.total_tasks,
            self.duration
        )
    }
}

fn main() {
    let mut measurements = vec![];
    let num_bench_runs = 1;
    for bench in BENCH_RUNS {
        for _ in 0..num_bench_runs {
            let ex = LocalExecutorBuilder::new().pin_to_cpu(0).make().unwrap();
            let measurement = ex.run(run_bench_tasks(bench.num_tasks, bench.num_events));

            println!("{}", measurement);
            measurements.push(measurement);
        }

        let sum = measurements
            .iter()
            .fold(Duration::from_secs(0), |acc, v| acc + v.duration);
        let average = sum / num_bench_runs;
        println!("average bench duration: {:?}\n", average);
        measurements.clear();
    }
}

async fn run_bench_tasks(num_tasks: u64, num_events: u64) -> Measurement {
    let num_ops_per_task = num_events / num_tasks;
    let start_time = Instant::now();
    let mut handles = vec![];
    for _ in 0..num_tasks {
        let handle = Task::local(async move {
            let submitter = glommio::nop::NopSubmitter::new();
            for _ in 0..num_ops_per_task {
                submitter.run_nop().await.unwrap();
            }
        });
        handles.push(handle)
    }
    for handle in handles {
        handle.await;
    }
    let end_time = Instant::now();
    Measurement {
        total_events: num_events,
        total_tasks: num_tasks,
        duration: end_time - start_time,
    }
}
