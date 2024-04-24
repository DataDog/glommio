use futures_lite::{
    io::{AsyncReadExt, AsyncWriteExt},
    stream::StreamExt,
};
use glommio::{
    enclose,
    net::{TcpListener, TcpStream},
    LocalExecutorBuilder, Placement,
};
use std::{
    cell::Cell,
    rc::Rc,
    sync::{Arc, Mutex},
    time::Instant,
};

fn main() {
    let runs: u32 = 100_000;
    let coord = Arc::new(Mutex::new(0));

    let server = coord.clone();
    let _server_ex = LocalExecutorBuilder::new(Placement::Fixed(0))
        .spawn(move || async move {
            let bw = glommio::spawn_local(async move {
                let mut handles = Vec::new();
                let listener = TcpListener::bind("127.0.0.1:8002").unwrap();
                for _ in 0..101 {
                    let mut stream = listener.accept().await.unwrap();
                    handles.push(
                        glommio::spawn_local(async move {
                            let mut byte = vec![0u8; 8192];
                            loop {
                                let read = stream.read(&mut byte).await.unwrap();
                                if read == 0 {
                                    break;
                                }
                            }
                        })
                        .detach(),
                    );
                }
                for h in handles {
                    h.await.unwrap();
                }
            })
            .detach();

            let rr = glommio::spawn_local(async move {
                let listener = TcpListener::bind("127.0.0.1:8001").unwrap();
                let mut stream = listener.accept().await.unwrap();

                loop {
                    let mut byte = [0u8; 1];
                    let read = stream.read(&mut byte).await.unwrap();
                    if read == 0 {
                        break;
                    }
                    assert_eq!(1, stream.write(&byte).await.unwrap());
                }
            })
            .detach();

            let listener = TcpListener::bind("127.0.0.1:8000").unwrap();
            *(server.lock().unwrap()) = 1;
            listener
                .incoming()
                .try_for_each(|addr| addr.map(|_| ()))
                .await
                .unwrap();

            rr.await.unwrap();
            bw.await.unwrap();
        })
        .unwrap();

    while *(coord.lock().unwrap()) != 1 {}

    let client_ex = LocalExecutorBuilder::new(Placement::Fixed(1))
        .spawn(move || async move {
            let t = Instant::now();
            for _ in 0..runs {
                let _stream = TcpStream::connect("127.0.0.1:8000").await.unwrap();
            }
            println!(
                "cost to establish a connection (serial connect) {:#?}",
                t.elapsed() / runs
            );

            let t = Instant::now();
            let mut handles = Vec::with_capacity(runs as usize);
            for _ in 0..100 {
                handles.push(
                    glommio::spawn_local(async move {
                        for _ in 0..(runs / 100) {
                            TcpStream::connect("127.0.0.1:8000").await.unwrap();
                        }
                    })
                    .detach(),
                );
            }

            for handle in handles {
                handle.await.unwrap();
            }
            println!(
                "cost to establish a connection (parallel connect) {:#?}",
                t.elapsed() / runs
            );

            let mut stream = TcpStream::connect("127.0.0.1:8001").await.unwrap();
            let t = Instant::now();
            for _ in 0..runs {
                let mut byte = [65u8; 1];
                assert_eq!(1, stream.write(&byte).await.unwrap());
                assert_eq!(1, stream.read(&mut byte).await.unwrap());
            }
            println!("cost of a byte-sized round trip {:#?}", t.elapsed() / runs);

            let mut stream = TcpStream::connect("127.0.0.1:8002").await.unwrap();
            let t = Instant::now();
            let mut total: f64 = 0.0;
            let byte = [65u8; 8192];
            while t.elapsed().as_secs() < 5 {
                let wr = stream.write(&byte).await.unwrap();
                total += wr as f64;
            }
            total /= 1_000_000_000.0;

            println!(
                "Bandwidth of sending through a single socket: {:.2} Gbps/s",
                (8.0 * total) / t.elapsed().as_secs_f64()
            );

            let mut handles = Vec::new();
            let t = Instant::now();
            let total = Rc::new(Cell::new(0.0));
            for _ in 0..100 {
                handles.push(
                    glommio::spawn_local(enclose! { (total) async move {
                        let t = Instant::now();
                        let mut stream = TcpStream::connect("127.0.0.1:8002").await.unwrap();
                        let byte = vec![65u8; 8192];
                        while t.elapsed().as_secs() < 5 {
                            let wr = stream.write(&byte).await.unwrap();
                            total.set(total.get() + wr as f64);
                        }
                    }})
                    .detach(),
                );
            }
            for handle in handles {
                handle.await.unwrap();
            }
            let total = total.get() / 1_000_000_000.0;

            println!(
                "Bandwidth of sending through 100 sockets: {:.2} Gbps/s",
                (8.0 * total) / t.elapsed().as_secs_f64()
            );
        })
        .unwrap();

    client_ex.join().unwrap();
}
