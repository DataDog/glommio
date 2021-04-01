# glommio

[![docs.rs](https://docs.rs/mio/badge.svg)](https://docs.rs/glommio/latest/glommio/) [![CircleCI](https://circleci.com/gh/DataDog/glommio.svg?style=svg)](https://circleci.com/gh/DataDog/glommio)

## Join our Zulip community!

If you are interested in Glommio consider joining our [Zulip](https://glommio.zulipchat.com) community.
Come tell us about exciting applications you are building, ask for help,
or really just chat with friends

## What is Glommio?


Glommio (pronounced glo-mee-jow or |glomjəʊ|) is a Cooperative Thread-per-Core crate for
Rust & Linux based on `io_uring`. Like other rust asynchronous crates it allows
one to write asynchronous code that takes advantage of rust `async`/`await`, but
unlike its counterparts it doesn't use helper threads anywhere.

Using Glommio is not hard if you are familiar with rust async. All you have to do is:

```rust
    use glommio::prelude::*;
    LocalExecutorBuilder::new().spawn(|| async move {
        /// your code here
    }).unwrap();
```

Please note Glommio requires at least 512 KiB of locked memory for `io_uring` to work. You can
increase the `memlock` resource limit (rlimit) as follows:

```sh
$ vi /etc/security/limits.conf
*    hard    memlock        512
*    soft    memlock        512
```

 To make the new limits effective, you need to login to the machine again. You can verify that
 the limits are updated by running the following:

```sh
$ ulimit -l
512
```

For more details check out our [docs
page](https://docs.rs/glommio/latest/glommio/) and an [introductory
article](https://www.datadoghq.com/blog/engineering/introducing-glommio/)

## Status

Glommio is still considered an alpha release. The main reasons are:

* The existing API is still evolving
* There are still some uses of unsafe that can be avoided
* There are features that are critical for a good thread per core system
  that are not implemented yet. The top one being:
  * per-shard memory allocator.

Want to help bring us to production status sooner? PRs are welcome!

## Current limitations

Due to our immediate needs which are a lot narrower, we make the following design assumptions:

- NVMe. While other storage types may work, the general assumptions made in
  here are based on the characteristics of NVMe storage. This allows us to
  use io uring's poll ring for reads and writes which are interrupt free.
  This also assumes that one is running either `XFS` or `Ext4` (an
  assumption that Seastar also makes).
  
- A corollary to the above is that the CPUs are likely to be the
  bottleneck, so this crate has a CPU scheduler but lacks an I/O scheduler.
  That, however, would be a welcome addition.
  
- A recent(at least 5.8) kernel is no impediment, as long as a fully functional I/O uring
  is present. In fact, we require a kernel so recent that it doesn't even
  exist: operations like `mkdir, ftruncate`, etc which are not present in
  today's (5.8) `io_uring` are simply synchronous and we'll live with the
  pain in the hopes that Linux will eventually add support for them. 
  
## Missing features

There are many. In particular:

* Memory allocator: memory allocation is a big source of contention for
  thread per core systems. A shard-aware allocator would be crucial for
  achieving good performance in allocation-heavy workloads.
  
* As mentioned, an I/O Scheduler.
  
* Visibility: the crate exposes no metrics on its internals, and that should
  change ASAP.
  
## License

Licensed under either of

 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
