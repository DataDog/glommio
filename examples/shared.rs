// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use scipio::{enclose, Local, LocalExecutor, Shared};

fn main() {
    let ex = LocalExecutor::make_default();
    ex.run(async move {
        let shared = Shared::new(0u64);

        Local::local(enclose! { (shared)
            async move {
            shared.do_with(|x| *x = 42 );
        }})
        .await;
        println!("The spawned task set my value to {}", shared.inner());
    });
}
