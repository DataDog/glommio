use glommio::{enclose, prelude::*, sync::Semaphore};
use std::{cell::Cell, rc::Rc, time::Instant};

fn main() {
    let local_ex = LocalExecutorBuilder::new(Placement::Fixed(0))
        .spawn(|| async move {
            let runs: u32 = 10_000_000;
            let s = Rc::new(Semaphore::new(10_000_000));
            let t = Instant::now();
            for _ in 0..runs {
                s.acquire(1).await.unwrap();
            }
            println!(
                "cost of acquiring uncontended semaphore: {:#?}",
                t.elapsed() / runs
            );
        })
        .unwrap();
    local_ex.join().unwrap();

    let local_ex = LocalExecutorBuilder::new(Placement::Fixed(0))
        .spawn(|| async move {
            let runs: u32 = 10_000_000;
            let s = Rc::new(Semaphore::new(0));
            let acquisitions = Rc::new(Cell::new(0));

            let signals = crate::spawn_local(enclose! { (acquisitions, s) async move {
                let mut expected : u32 = 0;
                while expected != runs {
                    while expected != acquisitions.get() {
                        crate::executor().yield_task_queue_now().await;
                    }
                    s.signal(1);
                    expected += 1;
                }
            }})
            .detach();

            let t = Instant::now();
            for i in 0..runs {
                acquisitions.set(i);
                s.acquire(1).await.unwrap();
            }
            println!(
                "cost of acquiring contended semaphore: {:#?}",
                t.elapsed() / runs
            );
            signals.await;
        })
        .unwrap();
    local_ex.join().unwrap();
}
