use futures::join;
use glommio::prelude::*;
use std::{
    cell::RefCell,
    rc::Rc,
    time::{Duration, Instant},
};

fn main() {
    // Glommio is a cooperative thread per core system so once you start processing
    // a future it will run it to completion. This is not great for latency, and
    // may be outright wrong if you have tasks that may spin forever before
    // returning, like a long-lived server.
    //
    // Applications using Glommio are then expected to be well-behaved and
    // explicitly yield control if they are going to do something that may take
    // too long (that is usually a loop!)
    //
    // There are two ways of yielding control:
    //
    //  * Local::yield_if_needed(), which will yield if the task has run for too
    //    long. What "too long" means is an implementation detail, but it will be
    //    always somehow related to the latency guarantees that the task queues want
    //    to uphold in their `Latency::Matters` parameter (or
    //    Latency::NotImportant).
    //
    //  * Local::later(), which will yield immediately (execute the rest of the
    //    function later).
    //
    // Because yield_if_needed() returns a future that has to be .awaited, it cannot
    // be used in situations where .await is illegal. For instance, if we are
    // holding a borrow. For those, one can call need_preempt() which will tell
    // you if yielding is needed, and then explicitly yield with later().
    let handle = LocalExecutorBuilder::new()
        .spawn(|| async move {
            let tq1 = Local::create_task_queue(
                Shares::default(),
                Latency::Matters(Duration::from_millis(10)),
                "tq1",
            );
            let tq2 = Local::create_task_queue(
                Shares::default(),
                Latency::Matters(Duration::from_millis(10)),
                "tq1",
            );
            let shared_value = Rc::new(RefCell::new(0u64));

            let value = shared_value.clone();
            let j2 = Local::local_into(
                async move {
                    let start = Instant::now();
                    let mut lap = start;
                    while start.elapsed().as_millis() < 50 {
                        Local::yield_if_needed().await;
                        if lap.elapsed().as_millis() > 1 {
                            lap = Instant::now();
                            println!("tq1: 1ms");
                        }
                    }
                    println!("tq1: Final value of v: {}", *(value.borrow()));
                },
                tq1,
            )
            .unwrap();

            let value = shared_value.clone();
            let j1 = Local::local_into(
                async move {
                    let start = Instant::now();
                    let mut lap = start;
                    while start.elapsed().as_millis() < 50 {
                        let mut v = value.borrow_mut();
                        if Local::need_preempt() {
                            drop(v);
                            Local::later().await;
                        } else {
                            *v += 1;
                        }
                        if lap.elapsed().as_millis() > 1 {
                            lap = Instant::now();
                            println!("tq2: 1ms");
                        }
                    }
                    println!("tq2: Final value of v: {}", *(value.borrow()));
                },
                tq2,
            )
            .unwrap();

            join!(j1, j2);
        })
        .unwrap();
    handle.join().unwrap();
}
