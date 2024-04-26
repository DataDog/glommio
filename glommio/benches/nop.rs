//! Benchmark the performance of the submission ring by timing tasks which
//! submit and wait on noop requests.
use glommio::{LocalExecutorBuilder, Placement};
use std::{
    fmt,
    time::{Duration, Instant},
};

struct Bench {
    num_tasks: u32,
    num_events: u32,
}

const BENCH_RUNS: &[Bench] = &[
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
    total_events: u32,
    total_tasks: u32,
    average_task_duration: Duration,
}

impl fmt::Display for Measurement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            r"Completed submission of {} noop requests across {} tasks, {} per task.
            Average task duration: {:?}",
            self.total_events,
            self.total_tasks,
            self.total_events / self.total_tasks,
            self.average_task_duration
        )
    }
}

fn main() {
    let mut measurements = vec![];
    let num_bench_runs = 5;
    for bench in BENCH_RUNS {
        for _ in 0..num_bench_runs {
            let ex = LocalExecutorBuilder::new(Placement::Fixed(0))
                .make()
                .unwrap();
            let measurement = ex.run(run_bench_tasks(bench.num_tasks, bench.num_events));

            println!("{measurement}");
            measurements.push(measurement);
        }

        let sum = measurements.iter().fold(Duration::from_secs(0), |acc, v| {
            acc + v.average_task_duration
        });
        let average = sum / num_bench_runs;
        println!("Average task duration across {num_bench_runs} runs: {average:?}\n");
        measurements.clear();
    }
}

async fn run_bench_tasks(num_tasks: u32, num_events: u32) -> Measurement {
    let num_ops_per_task = num_events / num_tasks;

    let mut handles = vec![];
    for _ in 0..num_tasks {
        let handle = glommio::spawn_local(async move {
            let start_time = Instant::now();
            let submitter = glommio::nop::NopSubmitter::new();
            for _ in 0..num_ops_per_task {
                submitter.run_nop().await.unwrap();
            }
            start_time.elapsed()
        });
        handles.push(handle)
    }

    let mut task_duration_sum = Duration::from_secs(0);
    for handle in handles {
        let task_duration = handle.await;
        task_duration_sum += task_duration;
    }
    let average_task_duration = task_duration_sum / num_tasks as u32;
    Measurement {
        total_events: num_events,
        total_tasks: num_tasks,
        average_task_duration,
    }
}
