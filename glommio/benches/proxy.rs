use std::time::Instant;

use futures_lite::{
    future::{block_on, try_zip},
    io::{copy, split},
};
use glommio::{
    net::{TcpListener, TcpStream},
    prelude::*,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    runtime::{Builder, Handle, Runtime},
    sync::oneshot,
};

#[derive(Copy, Clone)]
struct TestCfg {
    clients: usize,
    requests: usize,
    msg_size: usize,
    short_lived: bool,
}

impl TestCfg {
    fn new(clients: usize, requests: usize, msg_size: usize, short_lived: bool) -> Self {
        Self {
            clients,
            requests,
            msg_size,
            short_lived,
        }
    }
}

fn main() {
    let server_port = 9000;
    let glommio_proxy_port = 9100;
    let tokio_proxy_port = 9200;

    let runtime = Builder::new_multi_thread()
        // server and loader run in worker threads
        .worker_threads(2)
        // tokio proxy run in blocking thread
        .max_blocking_threads(1)
        .enable_io()
        .build()
        .unwrap();

    start_server(&runtime, server_port);
    start_glommio_proxy(glommio_proxy_port, server_port);
    start_tokio_proxy(&runtime, tokio_proxy_port, server_port);

    let mut test_cfgs = vec![];
    for sz in [128, 512, 1000] {
        for c in [1, 50, 150, 300, 500, 1000] {
            test_cfgs.push(TestCfg::new(c, 500_000, sz, false));
        }
    }

    for cfg in test_cfgs {
        let qps = start_loaders(&runtime, "Tokio", tokio_proxy_port, cfg, None);
        start_loaders(&runtime, "Glommio", glommio_proxy_port, cfg, Some(qps));
        println!("--")
    }
    runtime.shutdown_background();
}

fn start_server(runtime: &Runtime, port: u16) {
    let (ready_send, ready_recv) = oneshot::channel();

    runtime.spawn(async move {
        use tokio::{io::copy, net::TcpListener};

        let listener = TcpListener::bind(("0.0.0.0", port)).await.unwrap();
        ready_send.send(()).unwrap();
        while let Ok((mut stream, _)) = listener.accept().await {
            Handle::current().spawn(async move {
                let (mut read, mut write) = stream.split();
                copy(&mut read, &mut write).await.ok();
            });
        }
    });

    block_on(ready_recv).unwrap();
}

fn start_glommio_proxy(port: u16, server_port: u16) {
    let (ready_send, ready_recv) = oneshot::channel();

    LocalExecutorBuilder::default()
        .spawn(move || async move {
            let listener = TcpListener::bind(("0.0.0.0", port)).unwrap();
            ready_send.send(()).unwrap();

            while let Ok(downstream) = listener.accept().await {
                spawn_local(async move {
                    let upstream = TcpStream::connect(("0.0.0.0", server_port)).await.unwrap();

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

    block_on(ready_recv).unwrap();
}

fn start_tokio_proxy(runtime: &Runtime, port: u16, server_port: u16) {
    let (ready_send, ready_recv) = oneshot::channel();

    runtime.spawn_blocking(move || {
        block_on(async move {
            use tokio::{
                io::copy,
                net::{TcpListener, TcpStream},
            };

            Builder::new_current_thread()
                .enable_io()
                .build()
                .unwrap()
                .block_on(async move {
                    let listener = TcpListener::bind(("0.0.0.0", port)).await.unwrap();
                    ready_send.send(()).unwrap();

                    let handle = Handle::current();
                    while let Ok((mut downstream, _)) = listener.accept().await {
                        handle.spawn(async move {
                            let mut upstream =
                                TcpStream::connect(("0.0.0.0", server_port)).await.unwrap();

                            let (mut down_read, mut down_write) = downstream.split();
                            let (mut up_read, mut up_write) = upstream.split();

                            let upstreaming = copy(&mut down_read, &mut up_write);
                            let downstreaming = copy(&mut up_read, &mut down_write);

                            try_zip(upstreaming, downstreaming).await.ok();
                        });
                    }
                });
        })
    });

    block_on(ready_recv).unwrap();
}

fn start_loaders(runtime: &Runtime, label: &str, port: u16, cfg: TestCfg, cmp: Option<f64>) -> f64 {
    use tokio::net::TcpStream;

    let start = Instant::now();
    let mut loaders = vec![];

    for _ in 0..cfg.clients {
        loaders.push(runtime.spawn(async move {
            let mut stream = TcpStream::connect(("0.0.0.0", port)).await.unwrap();
            let mut buf = vec![0; cfg.msg_size];
            for _ in 0..cfg.requests / cfg.clients {
                if cfg.short_lived {
                    stream = TcpStream::connect(("0.0.0.0", port)).await.unwrap();
                }
                stream.write_all(buf.as_slice()).await.unwrap();
                stream.read_exact(buf.as_mut_slice()).await.unwrap();
            }
        }));
    }

    for ld in loaders {
        block_on(ld).unwrap();
    }

    let elapsed = start.elapsed();
    let qps = cfg.requests as f64 / elapsed.as_secs_f64();
    println!(
        "{}-lived connections, {} {}-byte messages, {} clients, {:7}: {:.2?} ({:.2} req/s, {:.1}%)",
        if cfg.short_lived { "short" } else { "long" },
        cfg.requests,
        cfg.msg_size,
        cfg.clients,
        label,
        elapsed,
        qps,
        cmp.map(|x| qps / x * 100.0).unwrap_or(100.0),
    );
    qps
}
