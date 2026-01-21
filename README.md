# glommio

[![crates.io](https://img.shields.io/crates/v/glommio)](https://crates.io/crates/glommio)
[![docs.rs](https://docs.rs/glommio/badge.svg)](https://docs.rs/glommio/latest/glommio/)
![license](https://img.shields.io/crates/l/glommio)
[![project chat](https://img.shields.io/badge/zulip-join_chat-brightgreen.svg)](https://glommio.zulipchat.com)
[![CI](https://github.com/DataDog/glommio/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/DataDog/glommio/actions/workflows/ci.yml)

## Join our Zulip community!

If you are interested in Glommio, consider joining our [Zulip](https://glommio.zulipchat.com) community. Tell us about
exciting applications you are building, ask for help, or just chat with friends 😃

## What is Glommio?

Glommio (pronounced glo-mee-jow or |glomjəʊ|) is a Cooperative Thread-per-Core crate for Rust & Linux based
on `io_uring`. Like other rust asynchronous crates, it allows one to write asynchronous code that takes advantage of
rust `async`/`await`, but unlike its counterparts, it doesn't use helper threads anywhere.

Using Glommio is not hard if you are familiar with rust async. All you have to do is:

```rust
use glommio::prelude::*;

LocalExecutorBuilder::default().spawn(|| async move {
    /// your async code here
})
.expect("failed to spawn local executor")
.join();
```

For more details check out our [docs page](https://docs.rs/glommio/latest/glommio/) and
an [introductory article.](https://www.datadoghq.com/blog/engineering/introducing-glommio/)

## Supported Rust Versions

Glommio is built against the latest stable release. The minimum supported version is 1.92. The current Glommio version
is not guaranteed to build on Rust versions earlier than the minimum supported version.

## Supported Linux kernels

Glommio requires a kernel with a recent enough `io_uring` support, at least current enough to run discovery probes. The
minimum version at this time is 5.8.

Please also note Glommio requires at least 512 KiB of locked memory for `io_uring` to work. You can increase the
`memlock` resource limit (rlimit) as follows:

```sh
$ vi /etc/security/limits.conf
*    hard    memlock        512
*    soft    memlock        512
```

> Please note that 512 KiB is the minimum needed to spawn a single executor. Spawning multiple executors may require you
> to raise the limit accordingly.

To make the new limits effective, you need to log in to the machine again. You can verify that the limits are updated by
running the following:

```sh
$ ulimit -l
512
```

## Contributing

See [Contributing.](CONTRIBUTING.md)

## License

Licensed under either of

* Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
* MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
