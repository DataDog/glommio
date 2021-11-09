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
    let glommio_server_port: u16 = 9100;
    let tokio_server_port: u16 = 9200;

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

    let mut test_cfgs = vec![];
    for sz in [128, 512, 1000] {
        for c in [1, 50, 150, 300, 500, 1000] {
            test_cfgs.push(TestCfg::new(c, 1_000_000, sz, false));
        }
    }

    for cfg in test_cfgs {
        let qps = start_clients(&runtime, "Tokio", tokio_server_port, cfg, None);
        start_clients(&runtime, "Glommio", glommio_server_port, cfg, Some(qps));
        println!("--")
    }
    runtime.shutdown_background();
}

fn start_glommio_server(port: u16) {
    let (ready_send, ready_recv) = oneshot::channel();

    LocalExecutorBuilder::default()
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

fn start_clients(runtime: &Runtime, label: &str, port: u16, cfg: TestCfg, cmp: Option<f64>) -> f64 {
    use tokio::net::TcpStream;

    let start = Instant::now();
    let mut loaders = vec![];

    for _ in 0..cfg.clients {
        loaders.push(runtime.spawn(async move {
            let mut buf = vec![0; cfg.msg_size];
            let mut stream = TcpStream::connect(("0.0.0.0", port)).await.unwrap();
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
