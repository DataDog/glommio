use glommio::prelude::*;
use std::time::{Duration, Instant};

fn bench_need_preempt() {
    let local_ex = LocalExecutorBuilder::new(Placement::Fixed(0))
        .preempt_timer(Duration::from_secs(10))
        .spawn(|| async move {
            let mut runs = 0;
            let t = Instant::now();
            while runs < 1_000_000 {
                let _ = crate::executor().need_preempt();
                runs += 1;
            }

            println!(
                "cost of checking for need_preempt:\t {:#?} (ran {} times)",
                t.elapsed() / 1_000_000,
                1_000_000
            );
        })
        .unwrap();

    local_ex.join().unwrap();
}

fn bench_yield_if_needed() {
    let local_ex = LocalExecutorBuilder::new(Placement::Fixed(0))
        .preempt_timer(Duration::from_secs(10))
        .spawn(|| async move {
            let mut runs = 0;
            let t = Instant::now();
            while runs < 1_000_000 {
                crate::yield_if_needed().await;
                runs += 1;
            }

            println!(
                "cost of calling yield_if_needed:\t {:#?} (ran {} times)",
                t.elapsed() / 1_000_000,
                1_000_000
            );
        })
        .unwrap();

    local_ex.join().unwrap();
}

fn main() {
    bench_need_preempt();
    bench_yield_if_needed();
}
