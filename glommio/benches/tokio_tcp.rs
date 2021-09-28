use enclose::enclose;
use std::{
    io,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::sleep,
};

#[tokio::main]
async fn main() -> io::Result<()> {
    let runs: u32 = 100_000;

    // Test the speed of establishing a connection, :9000
    tokio::spawn(async move {
        let listener = TcpListener::bind("127.0.0.1:9000").await.unwrap();
        loop {
            let _ = listener.accept().await.unwrap();
        }
    });

    // Test the speed of a round-trip, :9001
    tokio::spawn(async move {
        let listener = TcpListener::bind("127.0.0.1:9001").await.unwrap();
        let (mut stream, _) = listener.accept().await.unwrap();
        loop {
            let mut byte = [0u8; 1];
            let read = stream.read(&mut byte).await.unwrap();
            if read == 0 {
                break;
            }
            assert_eq!(1, stream.write(&byte).await.unwrap());
        }
    });

    // Test bandwidth, server side act as a sink, many connections allowed
    tokio::spawn(async move {
        let listener = TcpListener::bind("127.0.0.1:9002").await.unwrap();
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let mut byte = [0u8; 8192];
                loop {
                    let read = stream.read(&mut byte).await.unwrap();
                    if read == 0 {
                        break;
                    }
                }
            });
        }
    });

    sleep(Duration::from_secs(1)).await;
    // ------- client side -------- //

    // Connection establishment
    let t = Instant::now();
    for _ in 0..runs {
        let _stream = TcpStream::connect("127.0.0.1:9000").await.unwrap();
    }
    println!("cost to establish a connection {:#?}", t.elapsed() / runs);

    // round trips
    let mut stream = TcpStream::connect("127.0.0.1:9001").await?;
    let t = Instant::now();
    for _ in 0..runs {
        let mut byte = [65u8; 1];
        assert_eq!(1, stream.write(&byte).await.unwrap());
        assert_eq!(1, stream.read(&mut byte).await.unwrap());
    }
    println!("cost of a byte-sized round trip {:#?}", t.elapsed() / runs);

    // bandwidth
    let mut stream = TcpStream::connect("127.0.0.1:9002").await.unwrap();
    let t = Instant::now();
    let byte = vec![65u8; 8192];
    let mut total: f64 = 0.0;
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
    let total = Arc::new(AtomicUsize::new(0));
    for _ in 0..100 {
        handles.push(tokio::spawn(enclose! { (total) async move {
            let t = Instant::now();
            let mut stream = TcpStream::connect("127.0.0.1:9002").await.unwrap();
            let byte = vec![65u8; 8192];
            while t.elapsed().as_secs() < 5 {
                let wr = stream.write(&byte).await.unwrap();
                total.fetch_add(wr, Ordering::Relaxed);
            }
        }}));
    }

    for handle in handles {
        handle.await.unwrap();
    }
    let total = total.load(Ordering::Relaxed) as f64 / 1_000_000_000.0;

    println!(
        "Bandwidth of sending through 100 sockets: {:.2} Gbps/s",
        (8.0 * total) / t.elapsed().as_secs_f64()
    );
    Ok(())
}
