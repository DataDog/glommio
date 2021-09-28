use glommio::{channels::shared_channel, net::UdpSocket, prelude::*};
use std::time::Instant;

fn main() {
    let runs: u32 = 100_000;

    let (server_sender, server_receiver) = shared_channel::new_bounded(1);
    let (client_sender, client_receiver) = shared_channel::new_bounded(1);

    let server_ex = LocalExecutorBuilder::new(Placement::Fixed(0))
        .spawn(move || async move {
            let addr_sender = server_sender.connect().await;
            let addr_receiver = client_receiver.connect().await;

            let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
            addr_sender
                .try_send(receiver.local_addr().unwrap())
                .unwrap();

            let client_addr = addr_receiver.recv().await.unwrap();
            receiver.connect(client_addr).await.unwrap();

            loop {
                let mut buf = [0u8; 1];
                let read = receiver.recv(&mut buf).await.unwrap();
                if read == 0 {
                    break;
                }
                receiver.send(&buf).await.unwrap();
            }

            loop {
                let mut buf = [0u8; 1];
                let read = receiver.recv(&mut buf).await.unwrap();
                if read == 0 {
                    break;
                }
                receiver.send_to(&buf, client_addr).await.unwrap();
            }
        })
        .unwrap();

    let client_ex = LocalExecutorBuilder::new(Placement::Fixed(1))
        .spawn(move || async move {
            let addr_receiver = server_receiver.connect().await;
            let addr_sender = client_sender.connect().await;

            let addr = addr_receiver.recv().await.unwrap();
            let client = UdpSocket::bind("127.0.0.1:0").unwrap();
            client.connect(addr).await.unwrap();
            addr_sender.try_send(client.local_addr().unwrap()).unwrap();

            let t = Instant::now();
            for _ in 0..runs {
                assert_eq!(1, client.send(&[65u8; 1]).await.unwrap());
                let mut buf = [0u8; 1];
                assert_eq!(1, client.recv(&mut buf).await.unwrap());
            }
            println!(
                "cost of a byte-sized round trip over connected socket {:#?}",
                t.elapsed() / runs
            );
            client.send(&[]).await.unwrap();

            let t = Instant::now();
            for _ in 0..runs {
                assert_eq!(1, client.send_to(&[65u8; 1], addr).await.unwrap());
                let mut buf = [0u8; 1];
                let (sz, _) = client.recv_from(&mut buf).await.unwrap();
                assert_eq!(1, sz);
            }
            println!(
                "cost of a byte-sized round trip over sendto/recvfrom {:#?}",
                t.elapsed() / runs
            );
            client.send(&[]).await.unwrap();
        })
        .unwrap();

    client_ex.join().unwrap();
    server_ex.join().unwrap();
}
