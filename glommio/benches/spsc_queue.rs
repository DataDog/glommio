use glommio::channels::spsc_queue;
use std::hint::{black_box, spin_loop};
use std::thread;
use std::time::Instant;

const RUNS: usize = 10_000_000;

fn pin_to_cpu(cpu: usize) {
    use std::mem::MaybeUninit;

    unsafe {
        let mut set = MaybeUninit::<libc::cpu_set_t>::zeroed().assume_init();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu, &mut set);

        let ret = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
        assert_eq!(ret, 0, "sched_setaffinity failed");
    }
}

fn bench_spsc(capacity: usize, cpu_producer: usize, cpu_consumer: usize) {
    let (sender, receiver) = spsc_queue::make::<u32>(capacity);

    let consumer = thread::spawn(move || {
        pin_to_cpu(cpu_consumer);

        let start = Instant::now();
        for _ in 0..RUNS {
            loop {
                if black_box(receiver.try_pop()).is_some() {
                    break;
                }
                spin_loop();
            }
        }
        start.elapsed()
    });

    // Producer timing
    pin_to_cpu(cpu_producer);

    let start = Instant::now();
    for i in 0..RUNS {
        loop {
            if black_box(sender.try_push(i as u32)).is_none() {
                break;
            }
            spin_loop();
        }
    }
    let prod_elapsed = start.elapsed();
    let cons_elapsed = consumer.join().unwrap();

    let prod_ns = prod_elapsed.as_nanos() as f64 / RUNS as f64;
    let cons_ns = cons_elapsed.as_nanos() as f64 / RUNS as f64;

    let prod_kops = (1e9 / prod_ns) / 1e3;
    let cons_kops = (1e9 / cons_ns) / 1e3;

    println!(
        "Cap {:>6} | Prod {:>8.2} ns/op ({:>10.2} KOPS) | Cons {:>8.2} ns/op ({:>10.2} KOPS)",
        capacity, prod_ns, prod_kops, cons_ns, cons_kops
    );
}

fn main() {
    let pairs = &[(0usize, 1usize), (0usize, 2usize)];

    for &(cpu_p, cpu_c) in pairs {
        println!("CPU {}->{}", cpu_p, cpu_c);
        for &capacity in &[1, 16, 1024, 4096, 10_000] {
            bench_spsc(capacity, cpu_p, cpu_c);
        }
        println!("--");
    }
}
