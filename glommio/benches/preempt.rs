use glommio::prelude::*;
use std::time::Instant;

fn main() {
    let local_ex = LocalExecutorBuilder::new()
        .pin_to_cpu(0)
        .spawn(|| async move {
            let mut runs = 0;
            let t = Instant::now();
            while !Local::need_preempt() {
                runs += 1;
            }

            println!(
                "cost of checking for need_preempt: {:#?}",
                t.elapsed() / runs,
            );
        })
        .unwrap();

    local_ex.join().unwrap();
}
