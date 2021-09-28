// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use glommio::{defer, timer::TimerActionOnce, LocalExecutorBuilder};
use std::time::Duration;

fn main() {
    defer! {
        println!("Executor is done!");
    }

    let handle = LocalExecutorBuilder::default()
        .spawn(|| async move {
            defer! {
                println!("This will print after the timer");
            }

            println!("This will print first");
            let task = TimerActionOnce::do_in(Duration::from_secs(1), async move {
                println!("This will print after one second");
            });
            task.join().await;
        })
        .unwrap();
    handle.join().unwrap();
}
