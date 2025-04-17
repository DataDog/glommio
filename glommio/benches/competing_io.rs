use futures::future::{join, join_all};
use futures_lite::AsyncWriteExt;
use glommio::{
    enclose,
    io::{ImmutableFile, ImmutableFileBuilder},
    Latency, LocalExecutorBuilder, Placement, Shares,
};
use rand::Rng;
use std::{
    cell::{Cell, RefCell},
    path::PathBuf,
    rc::Rc,
    time::{Duration, Instant},
};

fn main() {
    let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).preempt_timer(Duration::from_millis(10))
        .spawn(|| async move {
            let filename = match std::env::var("GLOMMIO_TEST_POLLIO_ROOTDIR") {
                Ok(path) => {
                    let mut path = PathBuf::from(path);
                    path.push("benchfile");
                    path
                }
                Err(_) => panic!("please set 'GLOMMIO_TEST_POLLIO_ROOTDIR'"),
            };

            {
                let _ = std::fs::remove_file(&filename);
                let mut sink = ImmutableFileBuilder::new(&filename)
                    .with_buffer_size(512 << 10)
                    .with_pre_allocation(Some(64 << 20))
                    .build_sink()
                    .await
                    .unwrap();

                let contents = vec![1; 512 << 10];
                for _ in 0..(64 << 20) / contents.len() {
                    sink.write_all(&contents).await.unwrap();
                }
                let file = sink.seal().await.unwrap();

                let lat_tq = glommio::executor().create_task_queue(
                    Shares::Static(100),
                    Latency::NotImportant,
                    "lat",
                );
                let throughput_tq = glommio::executor().create_task_queue(
                    Shares::Static(100),
                    Latency::NotImportant,
                    "throughput",
                );
                let gate = Rc::new(Cell::new(true));

                const IO_TO_PERFORM: usize = 1_000_000;

                for x in 0..4 {
                    gate.set(true);
                    let t1 = glommio::executor()
                        .spawn_local_into(
                            enclose!((gate)
                            async move {
                                let now = Instant::now();
                                let mut counter = 0u64;
                                // simulate a very CPU intensive but highly cooperative task
                                while gate.get() {
                                    counter += 1;
                                    glommio::yield_if_needed().await;
                                }
                                println!("[measured] CPU task ran at {} Mops/s", (counter as f64 / now.elapsed().as_secs_f64()) as u64 / 1_000_000);
                            }),
                            throughput_tq,
                        )
                        .unwrap();

                    let t2 = glommio::executor()
                        .spawn_local_into(
                            enclose!((gate, file)
                            async move {
                                run_io(&format!("iteration: {x}"), &file, IO_TO_PERFORM, 4096).await;
                                gate.replace(false);
                            }),
                            lat_tq,
                        )
                        .unwrap();
                    join(t1, t2).await;
                }
            };
        })
        .unwrap();
    handle.join().unwrap();
}

async fn run_io(name: &str, file: &ImmutableFile, count: usize, size: usize) {
    let blocks = file.file_size() / size as u64 - 1;
    let hist = Rc::new(RefCell::new(
        hdrhistogram::Histogram::<u64>::new(4).unwrap(),
    ));
    let started_at = Instant::now();

    let tasks: Vec<_> = (0..2 << 10)
        .map(|_| {
            let file = file.clone();
            let hist = hist.clone();
            glommio::spawn_local(async move {
                let mut rand = rand::rng();
                for _ in 0..count / (2 << 10) {
                    let now = Instant::now();
                    file.read_at(rand.random_range(0..blocks) * size as u64, size)
                        .await
                        .unwrap();
                    hist.borrow_mut()
                        .record(now.elapsed().as_micros() as u64)
                        .unwrap();
                }
            })
        })
        .collect();

    join_all(tasks).await;

    let hist = Rc::try_unwrap(hist).unwrap().into_inner();

    println!("\n --- {name} ---");
    println!(
        "performed {}k read IO at {}k IOPS (took {:.2}s)",
        count / 1_000,
        ((count as f64 / started_at.elapsed().as_secs_f64()) / 1_000f64) as usize,
        started_at.elapsed().as_secs_f64()
    );
    println!(
        "[measured] lat (end-to-end):\t\t\t\t\t\tmin: {: >4}us\tp50: {: >4}us\tp99: {: \
         >4}us\tp99.9: {: >4}us\tp99.99: {: >4}us\tp99.999: {: >4}us\t max: {: >4}us",
        Duration::from_micros(hist.min()).as_micros(),
        Duration::from_micros(hist.value_at_quantile(0.5)).as_micros(),
        Duration::from_micros(hist.value_at_quantile(0.99)).as_micros(),
        Duration::from_micros(hist.value_at_quantile(0.999)).as_micros(),
        Duration::from_micros(hist.value_at_quantile(0.9999)).as_micros(),
        Duration::from_micros(hist.value_at_quantile(0.99999)).as_micros(),
        Duration::from_micros(hist.max()).as_micros(),
    );
}
