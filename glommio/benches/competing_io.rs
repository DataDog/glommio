use futures::future::join;
use futures_lite::{stream, AsyncWriteExt, StreamExt};
use glommio::{
    enclose,
    io::{ImmutableFile, ImmutableFileBuilder},
    Latency,
    LocalExecutorBuilder,
    Placement,
    Shares,
};
use rand::Rng;
use std::{
    cell::Cell,
    path::PathBuf,
    rc::Rc,
    time::{Duration, Instant},
};

fn main() {
    let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
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
                    Shares::Static(1000),
                    Latency::Matters(Duration::from_millis(1)),
                    "lat",
                );
                let throughput_tq = glommio::executor().create_task_queue(
                    Shares::Static(1000),
                    Latency::NotImportant,
                    "throughput",
                );
                let gate = Rc::new(Cell::new(true));

                let t1 = glommio::executor()
                    .spawn_local_into(
                        enclose!((gate)
                        async move {
                            // simulate a very CPU intensive but highly cooperative task
                            while gate.get() {
                                glommio::yield_if_needed().await;
                            }
                        }),
                        throughput_tq,
                    )
                    .unwrap();

                let t2 = glommio::executor()
                    .spawn_local_into(
                        enclose!((gate)
                        async move {
                            for x in 0..4 {
                                run_io(&format!("iteration: {}", x), &file, 1_000_000, 4096).await;
                            }
                            gate.replace(false);
                        }),
                        lat_tq,
                    )
                    .unwrap();

                join(t1, t2).await;
            };
        })
        .unwrap();
    handle.join().unwrap();
}

async fn run_io(name: &str, file: &ImmutableFile, count: usize, size: usize) {
    let mut rand = rand::thread_rng();
    let blocks = file.file_size() / size as u64 - 1;
    let mut hist = hdrhistogram::Histogram::<u64>::new(4).unwrap();
    let started_at = Instant::now();

    file.read_many(
        stream::repeat_with(|| IOVec {
            vec: (rand.gen_range(0..blocks) * size as u64, size),
            at: Instant::now(),
        })
        .take(count),
        0,
        Some(0),
    )
    .for_each(|res| {
        let (io, _) = res.unwrap();
        hist.record(io.at.elapsed().as_micros() as u64).unwrap();
    })
    .await;

    assert_eq!(hist.len() as usize, count);

    println!("\n --- {} ---", name);
    println!(
        "performed {} read IO at {} IOPS (took {:.2}s)",
        count,
        (count as f64 / started_at.elapsed().as_secs_f64()) as usize,
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

struct IOVec<T: glommio::io::IoVec> {
    vec: T,
    at: Instant,
}

impl<T: glommio::io::IoVec> glommio::io::IoVec for IOVec<T> {
    fn pos(&self) -> u64 {
        self.vec.pos()
    }

    fn size(&self) -> usize {
        self.vec.size()
    }
}
