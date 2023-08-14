use futures_lite::io::AsyncBufReadExt;
use glommio::{
    controllers::{DeadlineQueue, DeadlineSource},
    io::stdin,
    prelude::*,
};
use std::{
    cell::Cell,
    future::Future,
    pin::Pin,
    rc::Rc,
    time::{Duration, Instant},
};
use yansi::Paint;

fn burn_cpu(dur: Duration) {
    let now = Instant::now();
    while now.elapsed() < dur {}
}

struct IntWriter {
    deadline: Duration,
    start: Instant,
    count_target: usize,
    count: Cell<usize>,
    next_print: Cell<Duration>,
    count_at_last_print: Cell<usize>,

    last_tq_runtime: Cell<Duration>,
    last_ex_runtime: Cell<Duration>,
}

impl IntWriter {
    fn new(count_target: usize, deadline: Duration) -> Rc<IntWriter> {
        Rc::new(IntWriter {
            start: Instant::now(),
            deadline,
            count_target,
            count: Cell::new(0),
            next_print: Cell::new(Duration::from_secs(1)),
            count_at_last_print: Cell::new(0),
            last_tq_runtime: Cell::new(Duration::from_nanos(0)),
            last_ex_runtime: Cell::new(Duration::from_nanos(0)),
        })
    }

    async fn write_int(self: Rc<Self>) -> Duration {
        let my_handle = glommio::executor().current_task_queue();

        loop {
            let me = self.count.get();
            let elapsed = self.start.elapsed();
            if me >= self.count_target {
                return elapsed;
            }
            self.count.set(me + 1);
            if elapsed > self.next_print.get() {
                let tq_stats = glommio::executor().task_queue_stats(my_handle).unwrap();

                let tq_runtime = tq_stats.runtime();
                let tq_delta = tq_runtime - self.last_tq_runtime.get();
                let ex_runtime = glommio::executor().executor_stats().total_runtime();
                let ex_delta = ex_runtime - self.last_ex_runtime.get();

                let ratio = self.count.get() as f64 / self.count_target as f64 * 100.0;
                let intratio = self.count.get() - self.count_at_last_print.get();

                let cpuratio = 100.0 * tq_delta.as_secs_f64() / ex_delta.as_secs_f64();

                println!(
                    "{}: Wrote {} ({}%), {:.0} int/s, scheduler shares: {} , {:.2} % CPU",
                    Paint::blue(format!("{}s", elapsed.as_secs())),
                    self.count.get(),
                    Paint::new(format!("{:.0}", ratio)).bold(),
                    intratio,
                    Paint::new(tq_stats.current_shares().to_string()).bold(),
                    cpuratio
                );
                self.next_print
                    .set(self.next_print.get() + Duration::from_secs(1));
                self.count_at_last_print.set(self.count.get());
                self.last_tq_runtime.set(tq_runtime);
                self.last_ex_runtime.set(ex_runtime);
            }

            burn_cpu(Duration::from_micros(500));
            glommio::executor().yield_task_queue_now().await;
        }
    }
}

impl DeadlineSource for IntWriter {
    type Output = Duration;

    fn expected_duration(&self) -> Duration {
        self.deadline
    }

    fn action(self: Rc<Self>) -> Pin<Box<dyn Future<Output = Duration> + 'static>> {
        Box::pin(self.write_int())
    }

    fn total_units(&self) -> u64 {
        self.count_target as _
    }

    fn processed_units(&self) -> u64 {
        self.count.get() as _
    }
}

fn competing_cpu_hog(
    stop: Rc<Cell<bool>>,
    cpuhog_tq: TaskQueueHandle,
) -> glommio::task::JoinHandle<()> {
    glommio::spawn_local_into(
        async move {
            while !stop.get() {
                burn_cpu(Duration::from_micros(500));
                glommio::executor().yield_task_queue_now().await;
            }
        },
        cpuhog_tq,
    )
    .unwrap()
    .detach()
}

async fn static_writer(how_many: usize, shares: usize, cpuhog_tq: TaskQueueHandle) -> Duration {
    let name = format!("shares-{shares}");
    let tq =
        glommio::executor().create_task_queue(Shares::Static(shares), Latency::NotImportant, &name);

    let stop = Rc::new(Cell::new(false));
    let hog = competing_cpu_hog(stop.clone(), cpuhog_tq);

    let writer = glommio::spawn_local_into(
        async move {
            // Last parameter is bogus outside the queue, but we're just reusing the same
            // writer
            let test = IntWriter::new(how_many, Duration::from_secs(0));
            test.write_int().await
        },
        tq,
    )
    .unwrap()
    .detach();

    let res = writer.await.unwrap();
    stop.set(true);
    hog.await.unwrap();
    res
}

async fn read_int() -> Result<usize, <usize as std::str::FromStr>::Err> {
    let mut buffer = String::new();
    stdin().read_line(&mut buffer).await.unwrap();
    let buf = buffer.trim();
    buf.parse::<usize>()
}

fn main() {
    let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
        .spawn(|| async move {
            let cpuhog_tq = glommio::executor().create_task_queue(
                Shares::Static(1000),
                Latency::NotImportant,
                "cpuhog",
            );

            println!(
                "{}",
                Paint::new("Welcome to the Deadline Writer example").bold()
            );
            println!(
                "In this example we will write a sequence of integers to a variable, busy looping \
                 for 500us after each write"
            );
            println!(
                "While we do that, another CPU hog will be running constantly in a different \
                 TaskQueue"
            );
            println!(
                "For {} results, this test is pinned to your CPU0. Make sure nothing else of \
                 significance is running there. You should be able to see it at 100% at all times!",
                Paint::new("best").bold()
            );

            println!("\n\nPlease tell me how many integers you would like to write");
            let to_write = read_int().await.unwrap();
            println!(
                "Ok, now let's write {} integers with both the writer and the CPU hog having the \
                 same priority",
                Paint::blue(to_write.to_string())
            );
            let dur = static_writer(to_write, 1000, cpuhog_tq).await;
            println!(
                "Finished writing in {}",
                Paint::green(format!("{dur:#.0?}"))
            );
            println!(
                "This was using {} shares, and short of reducing the priority of the CPU hog. {}",
                Paint::green("1000"),
                Paint::new("This is as fast as we can do!").bold()
            );
            println!(
                "With {} shares, this would have taken approximately {}",
                Paint::green("100"),
                Paint::green(format!("{:#.1?}", dur * 10))
            );
            println!(
                "With {} shares, this would have taken approximately {}. {}.",
                Paint::green("1"),
                Paint::green(format!("{:#.1?}", dur * 1000)),
                Paint::new("Can't go any slower than that!").bold()
            );

            println!(
                "\n\nLet's try the controlled process. How long would you like it to take? \
                 (seconds)"
            );
            println!(
                "Keep in mind that very short processes will be inherently unstable because of \
                 the time the controller needs to adapt"
            );
            let mut duration = read_int().await.unwrap();

            loop {
                let stop = Rc::new(Cell::new(false));
                let hog = competing_cpu_hog(stop.clone(), cpuhog_tq);
                glommio::executor().yield_task_queue_now().await;

                let deadline = DeadlineQueue::new("example", Duration::from_millis(250));
                let test = IntWriter::new(to_write, Duration::from_secs(duration as u64));
                let dur = deadline.push_work(test).await.unwrap();
                println!(
                    "Finished writing in {}",
                    Paint::green(format!("{dur:#.2?}"))
                );
                stop.set(true);
                hog.await.unwrap();
                println!(
                    "If you want to try again tell me how long it should take this time, or press \
                     some non-number to exit"
                );
                duration = match read_int().await {
                    Ok(num) => num,
                    Err(_) => break,
                }
            }
        })
        .unwrap();

    handle.join().unwrap();
}
