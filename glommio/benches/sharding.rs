use std::time::{Duration, Instant};

use futures_lite::{future::ready, stream::repeat, FutureExt, StreamExt};

use glommio::{
    channels::{
        channel_mesh::MeshBuilder,
        sharding::{Handler, HandlerResult, Sharded},
    },
    enclose,
    prelude::*,
};

fn main() {
    type Msg = i32;

    let nr_shards = 2;

    fn get_shard_for(_msg: &Msg, _nr_shards: usize) -> usize {
        1
    }

    #[derive(Clone)]
    struct RequestHandler {
        nr_shards: usize,
    };

    impl Handler<i32> for RequestHandler {
        fn handle(&self, _msg: Msg, _src_shard: usize, _cur_shard: usize) -> HandlerResult {
            ready(()).boxed_local()
        }
    }

    let mesh = MeshBuilder::full(nr_shards, 1024);

    let n = 400_000_000;

    let shards = (0..nr_shards).map(|i| {
        LocalExecutorBuilder::new()
            .pin_to_cpu(i)
            .spin_before_park(Duration::from_millis(10))
            .spawn(enclose!((mesh) move || async move {
                let handler = RequestHandler { nr_shards };
                let mut sharded = Sharded::new(mesh, get_shard_for, handler).await.unwrap();
                if sharded.shard_id() == 0 {
                    sharded.handle(repeat(1).take(n)).unwrap();
                }
                sharded.close().await;
            }))
    });

    let t = Instant::now();
    for s in shards.collect::<Vec<_>>() {
        s.unwrap().join().unwrap();
    }
    println!(
        "elapsed: {:?}, average cost: {:?}",
        t.elapsed(),
        t.elapsed() / n as u32
    );
}
