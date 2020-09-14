// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use futures::future::join_all;
use futures::io::{copy, AsyncReadExt, AsyncWriteExt};
use scipio::{Async, Local, LocalExecutorBuilder};
use std::io::Result;
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::rc::Rc;
use std::time::Instant;

async fn server(conns: usize) {
    let listener = Rc::new(Async::<TcpListener>::bind(([127, 0, 0, 1], 10000)).unwrap());
    println!(
        "Server Listening on {} on {} connections",
        listener.get_ref().local_addr().unwrap(),
        conns
    );

    // After we are already listening, we will spawn the client.
    // Not only this will guarantee that we are listening on the port already (so no need
    // for sleep or retry), but it also demonstrates how a more complex application may not
    // necessarily spawn all executors at once running symmetrical code.
    let client_handle = LocalExecutorBuilder::new()
        .pin_to_cpu(2)
        .spawn_thread("client", move || async move {
            client(conns).await;
        })
        .unwrap();

    let mut servers = vec![];
    for _ in 0..conns {
        let l = listener.clone();
        servers.push(Local::local(async move {
            let (stream, _) = l.accept().await.unwrap();
            loop {
                let x = copy(&stream, &mut &stream).await.unwrap();
                if x == 0 {
                    break;
                }
            }
        }));
    }
    join_all(servers).await;
    client_handle.join().unwrap();
}

async fn client(clients: usize) {
    let msgs: usize = 300_000;
    let msg_per_client = msgs / clients;

    let now = Instant::now();
    let mut tasks = vec![];
    for _ in 0..clients {
        tasks.push(Local::local(async move {
            let addr = "127.0.0.1:10000".to_socket_addrs().unwrap().next().unwrap();
            let mut stream = Async::<TcpStream>::connect(addr).await.unwrap();
            let mut buf = [0u8; 4096];

            for _ in 0..msg_per_client {
                stream.write(b"a").await.unwrap();
                stream.read(&mut buf).await.unwrap();
            }
            stream.flush().await.unwrap();
            stream.close().await.unwrap();
        }));
    }
    join_all(tasks).await;
    let delta = now.elapsed();
    println!(
        "Sent {} messages in {} ms. {:.2} msg/s",
        msgs,
        delta.as_millis(),
        msgs as f64 / delta.as_secs_f64()
    );
}

fn main() -> Result<()> {
    // Skip CPU0 because that is commonly used to host interrupts. That depends on
    // system configuration and most modern systems will balance it, but that it is
    // still common enough that it is worth excluding it in this benchmark
    let builder = LocalExecutorBuilder::new().pin_to_cpu(1);
    let server_handle = builder.spawn_thread("server", || async move {
        // If you try `top` during the execution of the first batch, you
        // will see that the CPUs should not be at 100%. A single connection will
        // not be enough to extract all the performance available in the cores.
        server(1).await;
        // This should drive the CPU utilization to 100%.
        // Asynchronous execution needs parallelism to thrive!
        server(5).await;
    })?;

    server_handle.join().unwrap();
    Ok(())
}
