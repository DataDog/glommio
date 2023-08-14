use std::rc::Rc;

use glommio::{
    enclose,
    prelude::*,
    sync::{Gate, Semaphore},
};

fn main() {
    LocalExecutor::default().run(async {
        let gate = Gate::new();

        let nr_tasks = 5;
        let running_tasks = Rc::new(Semaphore::new(0));
        let tasks_to_complete = Rc::new(Semaphore::new(0));

        for i in 0..nr_tasks {
            gate.spawn(enclose!((running_tasks, tasks_to_complete) async move {
                running_tasks.signal(1);
                println!("[Task {i}] started, running tasks: {}", running_tasks.available());
                tasks_to_complete.acquire(1).await.unwrap();
            }))
            .unwrap()
            .detach();
        }

        println!("Main: waiting for {nr_tasks} tasks");
        running_tasks.acquire(nr_tasks).await.unwrap();

        println!("Main: closing gate");
        let close_future =
            crate::spawn_local(enclose!((gate) async move { gate.close().await })).detach();

        tasks_to_complete.signal(nr_tasks);
        close_future.await.unwrap().unwrap();
        println!("Main: gate is closed");
    })
}
