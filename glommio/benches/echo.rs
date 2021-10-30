use std::time::Instant;

use futures_lite::{
    future::block_on,
    io::{copy, split},
};
use glommio::{net::TcpListener, prelude::*};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    runtime::{Builder, Handle, Runtime},
    sync::oneshot,
};

const CONCURRENCY: usize = 50;
const REQUESTS: usize = 2000000;
const REQUEST_SIZE: usize = 64;

fn main() {
    let glommio_server_port = 9100;
    let tokio_server_port = 9200;

    let runtime = Builder::new_multi_thread()
        // client runs in worker thread
        .worker_threads(1)
        // tokio server runs in blocking thread
        .max_blocking_threads(1)
        .enable_io()
        .build()
        .unwrap();

    start_glommio_server(glommio_server_port);
    start_tokio_server(&runtime, tokio_server_port);

    start_clients(&runtime, "Tokio", tokio_server_port);
    start_clients(&runtime, "Glommio", glommio_server_port);
    runtime.shutdown_background();
}

fn start_glommio_server(port: u16) {
    let (ready_send, ready_recv) = oneshot::channel();

    LocalExecutorBuilder::new()
        .spawn(move || async move {
            let listener = TcpListener::bind(("0.0.0.0", port)).unwrap();
            ready_send.send(()).unwrap();

            while let Ok(stream) = listener.accept().await {
                spawn_local(async move {
                    let (inbound, outbound) = split(stream);
                    copy(inbound, outbound).await.ok();
                })
                .detach();
            }
        })
        .unwrap();

    block_on(ready_recv).unwrap();
}

fn start_tokio_server(runtime: &Runtime, port: u16) {
    use tokio::{io::copy, net::TcpListener};

    let (ready_send, ready_recv) = oneshot::channel();

    runtime.spawn_blocking(move || {
        Builder::new_current_thread()
            .enable_io()
            .build()
            .unwrap()
            .block_on(async move {
                let listener = TcpListener::bind(("0.0.0.0", port)).await.unwrap();
                ready_send.send(()).unwrap();

                let handle = Handle::current();
                while let Ok((mut stream, _)) = listener.accept().await {
                    handle.spawn(async move {
                        let (mut inbound, mut outbound) = stream.split();
                        copy(&mut inbound, &mut outbound).await.ok()
                    });
                }
            });
    });

    block_on(ready_recv).unwrap();
}

fn start_clients(runtime: &Runtime, label: &str, port: u16) {
    use tokio::net::TcpStream;

    let start = Instant::now();
    let mut loaders = vec![];
    for _ in 0..CONCURRENCY {
        loaders.push(runtime.spawn(async move {
            let mut stream = TcpStream::connect(("0.0.0.0", port)).await.unwrap();
            let mut buf = vec![0; REQUEST_SIZE];
            for _ in 0..REQUESTS / CONCURRENCY {
                stream.write_all(buf.as_slice()).await.unwrap();
                stream.read(buf.as_mut_slice()).await.unwrap();
            }
        }));
    }

    for ld in loaders {
        block_on(ld).unwrap();
    }
    println!("{}: {:.2?}", label, start.elapsed());
}
