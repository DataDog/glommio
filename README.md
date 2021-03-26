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

## License

Licensed under either of

 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
