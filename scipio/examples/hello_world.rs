// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use futures::future::join_all;
use scipio::{Local, LocalExecutor};
use std::io::Result;

async fn hello() {
    let mut tasks = vec![];
    for t in 0..5 {
        tasks.push(Local::local(async move {
            println!("{}: Hello {} ...", Local::id(), t);
            Local::later().await;
            println!("{}: ... {} World!", Local::id(), t);
        }));
    }
    join_all(tasks).await;
}

fn main() -> Result<()> {
    // There are two ways to create an executor, demonstrated in this example.
    //
    // We can create it in the current thread, and run it separately later...
    let ex = LocalExecutor::new(Some(0))?;

    // Or we can spawn a new thread with an executor inside.
    let handle = LocalExecutor::spawn_executor("hello", Some(1), async move {
        hello().await;
    })?;

    // If you create the executor manually, you have to run it like so.
    //
    // spawn_new() is the preferred way to create an executor!
    ex.run(async move {
        hello().await;
    });

    // The newly spawned executor runs on a thread, so we need to join on
    // its handle so we can wait for it to finish
    handle.join().unwrap();
    Ok(())
}
