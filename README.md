# scipio

[![CircleCI](https://circleci.com/gh/DataDog/scipio.svg?style=svg)](https://circleci.com/gh/DataDog/scipio)

Scipio (pronounced skip-iow) is a Cooperative Thread-per-Core crate for
Rust & Linux based on `io_uring`. Like other rust asynchronous crates it allows
one to write asynchronous code that takes advantage of rust async/await, but
unlike its counterparts it doesn't use helper threads anywhere.

Using Scipio is not hard if you are familiar with rust async. All you have to do is:

```rust
    let ex = LocalExecutor::new(None).unwrap();
    ex.run(async {
        /// your code here
    });
```

Although this is not forced upon the user, by creating N executors and binding
each to a specific CPU one can use this crate to implement a thread-per-core
system where context switches essentially never happen, achieving maximum efficiency.

You can easily bind an executor to a CPU by adjusting the parameter to `new` in the
example above:

```rust
    /// This will now never leave CPU 0
    let ex = LocalExecutor::new(Some(0)).unwrap();
    ex.run(async {
        /// your code here
    });
```

Note that you can only have one executor per thread, so if you need more executor,
you will have to create more threads (we do consider providing helper code for that soon)

For a Thread-per-core-system to work well, it is paramount that some form of scheduling
can happen within the thread. A traditional application would use many threads to divide
the many aspects of its workload but that is a luxury that a Thread-per-Core application doesn't have.

However what looks like a shortcoming, is actually an advantage: you can create many independent
task queues inside each of your executors:

```rust
    let ex = LocalExecutor::new(Some(0)).unwrap();
    ex.run(async {
        let tq1 = Task::<()>::create_task_queue(2, Latency::NotImportant, "test1");
        let tq2 = Task::<()>::create_task_queue(1, Latency::NotImportant, "test2");
            let t1 = Task::local_into(async move {
            // your code here
                }, tq1);
            let t2 = Task::local_into(async move {
            // your code here
                }, tq2);
        join!(t1, t2);
    });

```

This example creates two task queues: tq1 has 1 share, tq2 has 1 share. This means
that if both want to use the CPU to its maximum, tq1 will have 1/3 of the CPU time
(1 / (1 + 2)) and tq2 will have 2/3 of the CPU time. Those shares are dynamic and
can be changed at any time.

## What does scipio mean?

This crate is named after Publius Cornelius Scipio, who defeated Hannibal Barca
at the Battle of Zama, ending the Second Punic War.  I actually like Hannibal a
bit more (just a bit), but Scipio ends with IO, which makes for a nicer name
for an I/O oriented framework.

## Prior work

This work is heavily inspired (with some code respectfully imported) by
the great work by Stjepan Glavina, in particular the following crates:

* [async-io](https://github.com/stjepang/async-io)
* [async-task](https://github.com/stjepang/async-task)
* [async-executor](https://github.com/stjepang/async-executor)
* [multitask](https://github.com/stjepang/async-multitask)

## Why is this its own crate?

Cooperative Thread-per-core is a very specific programming model.
Because only one task is executing per thread, the programmer never
needs any locking to be held. Atomic operations are therefore rare,
delegated to only a handful of corner case tasks.

As atomic operations are costlier than their non-atomic counterparts,
this improves efficiency by itself. However it comes with the added
benefits that context switches are virtually non-existent (they only
occur for kernel threads and interrupts) and no time is ever wasted
in waiting on locks.

## Why is this a single monolith instead of many crates

Take as an example the
[async-io](https://github.com/stjepang/async-io)  crate. It has a
`park()` and `unpark()` methods. One can `park()` the current executor,
and a helper thread will unpark it. This allows one to effectively use
that crate with very little need for anything else for the simpler
cases. Combined with synchronization primitives like `Condvar`, and
other thread-pool based future crates, it excels in conjunction with
others but it is useful on its own.

Now contrast that to the equivalent bits in this crate: once you
`park()` the thread, you can't unpark it. I/O never gets dispatched
without explicit calling into the reactor, which makes for a very weird
programming model and it is very hard to integrate with the outside
world since most external I/O related crates have threads that sooner
or later will require `Send + Sync`.

A single crate is a way to minimize friction.

## io_uring

This crate depends heavily on Linux's `io_uring`. The reactor will
register 3 rings per CPU:

 * *Main ring*: The main ring, as its name implies, is where most
   operations will be placed. Once the reactor is parked, it only
   returns if the main ring has events to report.

 * *Latency ring*: Operations that are latency sensitive can be
   put in the latency ring. The crate has a function called
   `yield_if_needed()`
   that efficiently checks if there are events pending in the latency
   ring. Because this crate uses `cooperative` programming, tasks
   run until they either complete or decide to yield, which means they
   can run for a very long time before tasks that are latency sensitive
   have a chance to run. Every time you fire a long-running operation
   (usually a loop) it is good practice to check `yield_if_needed()`
   periodically (for example after x iterations of the loop).

   In particular, a when a new priority class is registered, one can
   specify if it contains latency sensitive tasks or not. And if the
   queue is marked as latency sensitive, the Latency enum takes a
   duration parameter that determines for how long other tasks can run
   even if there are no external events (by registering a timer with
   the io_uring). If no runnable tasks in the system are latency sensitive,
   this timer is not registered.

   Because `io_uring` allows for polling in the ring file descriptor,
   it is safe to `park()` even if work is present in the latency ring:
   before going to sleep, the latency ring's file descriptor is
   registered with the main ring and any events it sees will also wake
   up the main ring.

 * *Poll ring*: Read and write operations on NVMe devices are put in the
   poll ring. The poll ring does not rely on interrupts so the system
   has to keep constantly polling if there is any pending work. By
   not relying on interrupts we can be even more efficient with I/O in
   high IOPS scenarios

Please note Scipio requires at least 256 KiB of locked memory for `io_uring`
to work. You can increase the `memlock` resource limit (rlimit) as follows:

```
$ vi /etc/security/limits.conf
*	hard	memlock		512
*	soft	memlock		512
```

To make the new limits effective, you need to login to the machine
again. You can verify that the limits are updated by running the
following:

```
$ ulimit -l
512
```

## Additional inspiration

Aside from Stjepan's work, this is also inspired greatly by the
[Seastar](http://seastar.io) Framework for C++ that powers I/O intensive
systems that are pushing the performance envelope, like ScyllaDB.

However due to my immediate needs which are a lot narrower, we make
the following design assumptions:

 - NVMe. Supports for any other storage type is not even considered.
   This allow us to use io uring's poll ring for reads and writes which
   are interrupt free. This also assumes that one is running either XFS
   or Ext4 (an assumption that Seastar also makes)

 - A corollary to the above is that the CPUs are likely to be the
   bottleneck, so this crate has a CPU scheduler but lacks an I/O
   scheduler. That, however, would be a welcome addition.

 - A recent kernel is no impediment, so a fully functional I/O uring is
   present. In fact, we require a kernel so recent that it doesn't event
   exist: operations like `mkdir, ftruncate`, etc which are not present
   in today's (5.8) `io_uring` are simply synchronous and we'll live
   with the pain in the hopes that Linux will eventually add support for
   them.

## Missing features

There are many. In particular:

* There is no yet cross-shard communication nor ergonomic primitives to
  allow for cross-shard services. This allows one to implement simple,
  independent sharded systems but would need to happen before more
  complex work can be built on top of this crate.

* As mentioned, an I/O Scheduler.

* The networking code uses `poll + rw`. This is essentially so I could
  get started sooner by reusing code from [async-io](https://github.com/stjepang/async-io)
  but we really should be using uring's native interface for that

* Visibility: the crate exposes no metrics on its internals, and
  that should change ASAP.

## License

Licensed under either of

 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

#### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
