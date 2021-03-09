use glommio::{channels::shared_channel, prelude::*};
use std::{
    sync::mpsc::sync_channel,
    time::{Duration, Instant},
};

fn test_spsc(capacity: usize) {
    let runs: u32 = 10_000_000;
    let (sender, receiver) = shared_channel::new_bounded(capacity);

    let sender = LocalExecutorBuilder::new()
        .pin_to_cpu(0)
        .spin_before_park(Duration::from_millis(10))
        .spawn(move || async move {
            let sender = sender.connect().await;
            let t = Instant::now();
            for _ in 0..runs {
                sender.send(1).await.unwrap();
            }
            println!(
                "cost of sending shared channel {:#?}, capacity: {}",
                t.elapsed() / runs,
                capacity
            );
            drop(sender);
        })
        .unwrap();

    let receiver = LocalExecutorBuilder::new()
        .spin_before_park(Duration::from_millis(10))
        .pin_to_cpu(1)
        .spawn(move || async move {
            let receiver = receiver.connect().await;
            let t = Instant::now();
            for _ in 0..runs {
                receiver.recv().await.unwrap();
            }
            println!(
                "cost of receiving shared channel: {:#?}, capacity {}",
                t.elapsed() / runs,
                capacity
            );
        })
        .unwrap();

    sender.join().unwrap();
    receiver.join().unwrap();
}

fn test_rust_std(capacity: usize) {
    let runs: u32 = 10_000_000;
    let (sender, receiver) = sync_channel::<u8>(capacity);

    let sender = LocalExecutorBuilder::new()
        .pin_to_cpu(0)
        .spin_before_park(Duration::from_millis(10))
        .spawn(move || async move {
            let t = Instant::now();
            for _ in 0..runs {
                sender.send(1).unwrap();
            }
            println!(
                "cost of sending rust standard channel {:#?}, capacity: {}",
                t.elapsed() / runs,
                capacity
            );
            drop(sender);
        })
        .unwrap();

    let receiver = LocalExecutorBuilder::new()
        .spin_before_park(Duration::from_millis(10))
        .pin_to_cpu(1)
        .spawn(move || async move {
            let t = Instant::now();
            for _ in 0..runs {
                receiver.recv().unwrap();
            }
            println!(
                "cost of receiving rust standard channel: {:#?}, capacity {}",
                t.elapsed() / runs,
                capacity
            );
        })
        .unwrap();

    sender.join().unwrap();
    receiver.join().unwrap();
}

fn main() {
    test_rust_std(1024);
    test_spsc(1024);
}
