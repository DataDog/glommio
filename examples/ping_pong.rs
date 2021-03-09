// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use glommio::{enclose, Local, LocalExecutor};
use std::{cell::RefCell, rc::Rc};

fn main() {
    let ex = LocalExecutor::default();
    let left = Rc::new(RefCell::new(false));
    let right = Rc::new(RefCell::new(false));

    ex.run(async {
        // Nice and short way to say a closure needs to capture vars clones.
        let first = Local::local(enclose! { (left, right)
            async move {
                loop {
                    if *(right.borrow()) {
                        println!("left");
                        *(left.borrow_mut()) = true;
                        println!("reset");
                        *(right.borrow_mut()) = true
                    }
                    Local::yield_if_needed().await;

                }
            }
        })
        .detach();

        // What would you write if there were no enclose! macro.
        let second = Local::local(|_left: Rc<RefCell<bool>>, right: Rc<RefCell<bool>>| -> _ {
            async move {
                loop {
                    if *(right.borrow()) == false {
                        println!("right");
                        *(right.borrow_mut()) = true
                    }
                    Local::yield_if_needed().await;
                }
            }
        }(left.clone(), right.clone()))
        .detach();

        futures::join!(first, second);
    });
}
