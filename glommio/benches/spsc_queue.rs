use glommio::channels::spsc_queue;
use std::time::Instant;

fn test_spsc(capacity: usize) {
    const RUNS: u32 = 10 * 1000 * 1000;
    let (sender, receiver) = spsc_queue::make::<u64>(1024);
    let consumer = std::thread::spawn(move || {
        let t = Instant::now();
        for _ in 0..RUNS {
            let mut opt: Option<u64>;
            while {
                opt = receiver.try_pop();
                opt.is_none()
            } {}
        }
        println!(
            "cost of receiving {:#?}, capacity {}",
            t.elapsed() / RUNS,
            capacity,
        );
    });
    let t = Instant::now();
    for i in 0..RUNS {
        while sender.try_push(i as u64).is_some() {}
    }
    println!(
        "cost of sending {:#?}, capacity {}",
        t.elapsed() / RUNS,
        capacity
    );
    consumer.join().unwrap();
}

fn main() {
    test_spsc(1024);
}
