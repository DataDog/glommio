use glommio::{channels::shared_channel, enclose, prelude::*};
use rand::Rng;
use std::{sync::mpsc::sync_channel, time::Instant};

fn test_spsc(capacity: usize) {
    let runs: u32 = 10_000_000;
    let (sender, receiver) = shared_channel::new_bounded(capacity);

    let sender = LocalExecutorBuilder::new(Placement::Fixed(0))
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

    let receiver = LocalExecutorBuilder::new(Placement::Fixed(1))
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

    let sender = LocalExecutorBuilder::new(Placement::Fixed(0))
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

    let receiver = LocalExecutorBuilder::new(Placement::Fixed(1))
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

fn test_tokio_mpsc(capacity: usize) {
    let runs: u32 = 10_000_000;
    let (sender, mut receiver) = tokio::sync::mpsc::channel(capacity);

    let sender = LocalExecutorBuilder::new(Placement::Fixed(0))
        .spawn(move || async move {
            let t = Instant::now();
            for _ in 0..runs {
                sender.send(1).await.unwrap();
            }
            println!(
                "cost of sending (tokio) shared channel {:#?}, capacity: {}",
                t.elapsed() / runs,
                capacity
            );
            drop(sender);
        })
        .unwrap();

    let receiver = LocalExecutorBuilder::new(Placement::Fixed(1))
        .spawn(move || async move {
            let t = Instant::now();
            for _ in 0..runs {
                receiver.recv().await.unwrap();
            }
            println!(
                "cost of receiving (tokio) shared channel: {:#?}, capacity {}",
                t.elapsed() / runs,
                capacity
            );
        })
        .unwrap();

    sender.join().unwrap();
    receiver.join().unwrap();
}

fn test_mesh_mpmc(capacity: usize, peers: usize) {
    let runs: u32 = 10_000_000;

    let mesh = glommio::channels::channel_mesh::MeshBuilder::full(peers, capacity);
    let t = Instant::now();
    LocalExecutorPoolBuilder::new(PoolPlacement::MaxSpread(peers, None))
        .on_all_shards(enclose! {(mesh) move || async move {
            let (sender, mut receiver) = mesh.join().await.unwrap();

            let sender_task = async move {
                let mut rng = rand::rng();
                for _ in 0..runs {
                    let mut peer = rng.random_range(0..sender.nr_consumers());
                    if peer == sender.producer_id().unwrap() && peer == 0 {
                        peer += 1;
                    } else if peer == sender.producer_id().unwrap() {
                        peer-=1;
                    }

                    sender.send_to(peer, 1).await.unwrap();
                }

                drop(sender);
            };

            let mut recvs = vec![];
            for (_, recv) in receiver.streams() {
                recvs.push(crate::spawn_local(async move {
                    while recv.recv().await.is_some(){};
                }));
            }

            sender_task.await;
            for x in recvs {
                x.await;
            }
        }})
        .unwrap()
        .join_all();

    println!(
        "cost of mesh message shared channel: {:#?}, peers {}, capacity {}",
        t.elapsed() / runs * peers as u32,
        peers,
        capacity,
    );
}

fn main() {
    test_rust_std(1024);
    test_rust_std(1);
    test_spsc(1024);
    test_spsc(1);
    test_tokio_mpsc(1024);
    test_tokio_mpsc(1);
    test_mesh_mpmc(1024, 8);
    test_mesh_mpmc(1, 8);
}
