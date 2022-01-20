// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use glommio::{
    net::{TcpListener, TcpStream},
    prelude::*,
};
use std::{io::Result, rc::Rc, time::Instant};

async fn server(conns: usize) -> Result<()> {
    let listener = Rc::new(TcpListener::bind("127.0.0.1:10000").unwrap());
    println!(
        "Server Listening on {} on {} connections",
        listener.local_addr()?,
        conns
    );

    // After we are already listening, we will spawn the client.
    // Not only this will guarantee that we are listening on the port already (so no
    // need for sleep or retry), but it also demonstrates how a more complex
    // application may not necessarily spawn all executors at once running
    // symmetrical code.
    let client_handle = LocalExecutorBuilder::new(Placement::Fixed(2))
        .name("client")
        .spawn(move || async move { client(conns).await })?;

    let mut servers = vec![];
    for _ in 0..conns {
        let l = listener.clone();
        servers.push(
            spawn_local(async move {
                let mut stream = l.accept().await.unwrap();
                loop {
                    let mut buf = [0u8; 1];
                    let b = stream.read(&mut buf).await?;
                    if b == 0 {
                        break;
                    } else {
                        stream.write(&buf).await?;
                    }
                }
                Result::Ok(())
            })
            .detach(),
        );
    }

    for s in servers {
        s.await.unwrap()?;
    }

    client_handle.join().unwrap().unwrap();
    Ok(())
}

async fn client(clients: usize) -> Result<()> {
    let msgs: usize = 300_000;
    let msg_per_client = msgs / clients;

    let now = Instant::now();
    let mut tasks = vec![];
    for _ in 0..clients {
        tasks.push(crate::spawn_local(async move {
            let mut stream = TcpStream::connect("127.0.0.1:10000").await.unwrap();
            for _ in 0..msg_per_client {
                stream.write(b"a").await?;
                let mut buf = [0u8; 1];
                stream.read(&mut buf).await?;
                assert_eq!(&buf, b"a");
            }
            stream.flush().await?;
            stream.close().await
        }));
    }
    for c in tasks {
        c.await?;
    }

    let delta = now.elapsed();
    println!(
        "Sent {} messages in {} ms. {:.2} msg/s",
        msgs,
        delta.as_millis(),
        msgs as f64 / delta.as_secs_f64()
    );
    Ok(())
}

fn main() -> Result<()> {
    // Skip CPU0 because that is commonly used to host interrupts. That depends on
    // system configuration and most modern systems will balance it, but that it is
    // still common enough that it is worth excluding it in this benchmark
    let builder = LocalExecutorBuilder::new(Placement::Fixed(1));
    let server_handle = builder.name("server").spawn(|| async move {
        // If you try `top` during the execution of the first batch, you
        // will see that the CPUs should not be at 100%. A single connection will
        // not be enough to extract all the performance available in the cores.
        server(1).await?;
        // This should drive the CPU utilization to 100%.
        // Asynchronous execution needs parallelism to thrive!
        server(5).await
    })?;

    // Congrats for getting to the end of this example!
    //
    // Now can you adapt it, so it uses multiple executors and all CPUs in your
    // system?
    server_handle.join().unwrap().unwrap();
    Ok(())
}
