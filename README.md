# scipio

[![CircleCI](https://circleci.com/gh/DataDog/scipio.svg?style=svg)](https://circleci.com/gh/DataDog/scipio)

###  _"Contextvm Svitchae Delenda Est"_

>    -- Scipio Africanus after defeating Hannibal in the Battle of Zama (*)

<sub>(*) That really happened. You weren't there so you can't prove Scipio never said that</sub>

## Join our Zulip community!

If you are interested in Scipio consider joining our [Zulip](https://scipio.zulipchat.com) community.
Come tell us about exciting applications you are building, ask for help,
or really just chat with friends

## What is Scipio?

Scipio (pronounced skip-io or |skɪpjəʊ|) is a Cooperative Thread-per-Core crate for
Rust & Linux based on `io_uring`. Like other rust asynchronous crates it allows
one to write asynchronous code that takes advantage of rust `async`/`await`, but
unlike its counterparts it doesn't use helper threads anywhere.

Using Scipio is not hard if you are familiar with rust async. All you have to do is:

```rust
    use scipio::LocalExecutorBuilder;
    LocalExecutorBuilder::new().spawn(|| async move {
        /// your code here
    }).unwrap();
```

For more details check out our [docs
page](https://docs.rs/crate/scipio/0.1.0-alpha)

## Status

Scipio is still considered an alpha release. The main reasons are:

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
