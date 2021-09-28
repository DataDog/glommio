use glommio::{enclose, prelude::*};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

fn main() {
    let runs: u32 = 100;
    let vec = Arc::new(Mutex::new(Vec::with_capacity(10)));
    for _ in 0..runs {
        let t = Instant::now();
        let local_ex = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(enclose! { (vec) move || async move {
                let mut v = vec.lock().unwrap();
                v.push(t.elapsed());
            }})
            .unwrap();
        local_ex.join().unwrap();
    }
    let v = vec.lock().unwrap();
    println!(
        "cost to spawn an executor : {:#?}",
        v.iter().fold(Duration::from_nanos(0), |acc, x| acc
            .checked_add(*x)
            .unwrap())
            / runs
    );

    let t = Instant::now();
    for _ in 0..runs {
        let local_ex = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(move || async move {})
            .unwrap();
        local_ex.join().unwrap();
    }
    println!(
        "cost to spawn and join an executor : {:#?}",
        t.elapsed() / runs
    );

    let local_ex = LocalExecutorBuilder::new(Placement::Fixed(0))
        .spawn(|| async move {
            let runs: u32 = 10_000_000;
            let t = Instant::now();
            for _ in 0..runs {
                crate::spawn_local(async {}).await;
            }
            println!("cost to run task no task queue: {:#?}", t.elapsed() / runs);
        })
        .unwrap();
    local_ex.join().unwrap();

    let local_ex = LocalExecutorBuilder::new(Placement::Fixed(0))
        .spawn(|| async move {
            let runs: u32 = 10_000_000;
            let tq1 = crate::executor().create_task_queue(
                Shares::Static(1000),
                Latency::NotImportant,
                "tq1",
            );
            let t = Instant::now();
            for _ in 0..runs {
                crate::spawn_local_into(async {}, tq1).unwrap().await;
            }
            println!("cost to run task in task queue: {:#?}", t.elapsed() / runs);
        })
        .unwrap();
    local_ex.join().unwrap();
}
