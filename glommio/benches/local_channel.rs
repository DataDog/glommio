use futures_lite::stream::StreamExt;
use glommio::{channels::local_channel, prelude::*};
use std::time::Instant;

fn main() {
    let local_ex = LocalExecutorBuilder::new(Placement::Fixed(0))
        .spawn(|| async move {
            let runs: u32 = 10_000_000;
            let (sender, receiver) = local_channel::new_bounded(1);
            let receiver = crate::spawn_local(async move {
                receiver.stream().fold(0, |acc, x| acc + x).await;
            })
            .detach();

            let t = Instant::now();
            for _ in 0..runs {
                sender.send(1).await.unwrap();
            }
            println!(
                "cost of sending contended local channel: {:#?}",
                t.elapsed() / runs
            );
            drop(sender);
            receiver.await;
        })
        .unwrap();
    local_ex.join().unwrap();

    let local_ex = LocalExecutorBuilder::new(Placement::Fixed(0))
        .spawn(|| async move {
            let runs: u32 = 10_000_000;
            let (sender, receiver) = local_channel::new_bounded(10_000_000);
            let receiver = crate::spawn_local(async move {
                receiver.stream().fold(0, |acc, x| acc + x).await;
            })
            .detach();

            let t = Instant::now();
            for _ in 0..runs {
                sender.send(1).await.unwrap();
            }
            println!(
                "cost of sending uncontended local channel: {:#?}",
                t.elapsed() / runs
            );
            drop(sender);
            receiver.await;
        })
        .unwrap();
    local_ex.join().unwrap();
}
