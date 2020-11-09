# glommio 

**ATTENTION** If you are confused between this and Scipio, this
project was previously called Scipio but had to be very unfortunately
renamed due to a trademark dispute. This disclaimer will self-destruct
as soon as people have enough time to switch over. We would also like to
make sure to clarify that this doesn't change our opinion on Scipio
Africanus being such a great and underrated general that deserved a lot
better from Rome than what we got.


[![CircleCI](https://circleci.com/gh/DataDog/glommio.svg?style=svg)](https://circleci.com/gh/DataDog/glommio)

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

For more details check out our [docs
page](https://docs.rs/glommio/0.2.0-alpha/glommio/)

## Status

Glommio is still considered an alpha release. The main reasons are:

* The existing API is still evolving
* There are still some uses of unsafe that can be avoided
* There are features that are critical for a good thread per core system
  that are not implemented yet. The top two are:
  * communication channels between executors so we can pass `Send` data.
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
