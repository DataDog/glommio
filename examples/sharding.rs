use futures_lite::{future::ready, stream::repeat_with, FutureExt, StreamExt};

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

    fn get_shard_for(msg: &Msg, nr_shards: usize) -> usize {
        *msg as usize % nr_shards
    }

    #[derive(Clone)]
    struct RequestHandler {
        nr_shards: usize,
    }

    impl Handler<i32> for RequestHandler {
        fn handle(&self, msg: Msg, _src_shard: usize, cur_shard: usize) -> HandlerResult {
            println!("shard {cur_shard} received {msg}");
            assert_eq!(get_shard_for(&msg, self.nr_shards), cur_shard);
            ready(()).boxed_local()
        }
    }

    let mesh = MeshBuilder::full(nr_shards, 1024);

    let shards = (0..nr_shards).map(|_| {
        LocalExecutorBuilder::default().spawn(enclose!((mesh) move || async move {
            let handler = RequestHandler { nr_shards };
            let mut sharded = Sharded::new(mesh, get_shard_for, handler).await.unwrap();
            let me = sharded.shard_id();
            let messages = repeat_with(|| fastrand::i32(0..100)).take(10).inspect(move |x| println!("shard {me} generated {x}"));
            sharded.handle(messages).unwrap();
            sharded.close().await;
        }))
    });

    for s in shards.collect::<Vec<_>>() {
        s.unwrap().join().unwrap();
    }
}
