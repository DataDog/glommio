use futures_lite::{
    future::try_zip,
    io::{copy, split},
};

use glommio::{
    net::{TcpListener, TcpStream},
    prelude::*,
};
use std::time::Duration;
use tokio::runtime::{Builder, Handle};

fn main() {
    let threads = 1;
    let upstream_port = 6379;
    let glommio_port = 8080;
    let tokio_port = 8090;

    LocalExecutorPoolBuilder::new(threads)
        .spin_before_park(Duration::from_millis(10))
        .on_all_shards(move || async move {
            let listener = TcpListener::bind(("0.0.0.0", glommio_port)).unwrap();

            while let Ok(downstream) = listener.accept().await {
                spawn_local(async move {
                    let upstream = TcpStream::connect(("0.0.0.0", upstream_port))
                        .await
                        .unwrap();

                    let (down_read, down_write) = split(downstream);
                    let (up_read, up_write) = split(upstream);

                    let upstreaming = copy(down_read, up_write);
                    let downstreaming = copy(up_read, down_write);

                    try_zip(upstreaming, downstreaming).await.ok();
                })
                .detach();
            }
        })
        .unwrap();

    Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_io()
        .build()
        .unwrap()
        .block_on(async move {
            use tokio::{
                io::copy,
                net::{TcpListener, TcpStream},
            };

            let listener = TcpListener::bind(("0.0.0.0", tokio_port)).await.unwrap();

            let handle = Handle::current();
            while let Ok((mut downstream, _)) = listener.accept().await {
                handle.spawn(async move {
                    let mut upstream = TcpStream::connect(("0.0.0.0", upstream_port))
                        .await
                        .unwrap();

                    let (mut down_read, mut down_write) = downstream.split();
                    let (mut up_read, mut up_write) = upstream.split();

                    let upstreaming = copy(&mut down_read, &mut up_write);
                    let downstreaming = copy(&mut up_read, &mut down_write);

                    try_zip(upstreaming, downstreaming).await.unwrap();
                });
            }
        });
}
