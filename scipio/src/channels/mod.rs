// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
//! scipio::channels is a module that provides scipio channel-like abstractions.
//!

/// Allow data to be transmitted across two tasks in the same shard.
///
/// Asynchronous code is rarely useful if it is serialized. In practice,
/// you will want to spawn asynchronous tasks or in the case of Scipio which
/// support multi-queued execution have a dedicated [`TaskQueue`] for each group
/// of tasks with their own priority.
///
/// The `local_channel` is an abstraction similar to the [`channel`] that can be
/// used for this purpose
///
/// The communication between sender and receiver is broken when one of them goes
/// out of scope. They however behave differently:
///
/// * The [`LocalReceiver`] never sees an error, as it is implemented as a stream
///   interface compatible with [`StreamExt`]. When the sender is no longer available the receiver's call to [`next`] will return [`None`].
/// * The [`LocalSender`] will return a [`ChannelError`] encapsulating a [`BrokenPipe`] error if it tries to [`push`] into
///   a channel that no longer has a receiver.
///
/// # Examples
///
/// ```
/// use scipio::{LocalExecutor, Local, Latency, Shares};
/// use scipio::channels::local_channel;
/// use futures_lite::stream::StreamExt;
///
/// let ex = LocalExecutor::make_default();
/// ex.run(async move {
///     let task_queue =
///         Local::create_task_queue(
///             Shares::default(), Latency::NotImportant, "example");
///
///     let (sender, mut receiver) = local_channel::new_unbounded();
///     let h = Local::local_into(async move {
///         assert_eq!(receiver.next().await.unwrap(), 0);
///     }, task_queue).unwrap().detach();
///     sender.try_send(0);
///     drop(sender);
///     h.await;
/// });
/// ```
/// [`TaskQueue`]: ../../struct.TaskQueueHandle.html
/// [`LocalSender`]: struct.LocalSender.html
/// [`LocalReceiver`]: struct.LocalReceiver.html
/// [`ChannelError`]: struct.ChannelError.html
/// [`push`]: struct.LocalSender.html#method.push
/// [`None`]: https://doc.rust-lang.org/std/option/enum.Option.html#variant.None
/// [`BrokenPipe`]: https://doc.rust-lang.org/std/io/enum.ErrorKind.html#variant.BrokenPipe
/// [`channel`]: https://doc.rust-lang.org/std/sync/mpsc/fn.channel.html
/// [`next`]: https://docs.rs/futures-lite/1.11.1/futures_lite/stream/trait.StreamExt.html#method.next
/// [`StreamExt`]: https://docs.rs/futures-lite/1.11.1/futures_lite/stream/trait.StreamExt.html
pub mod local_channel;

#[derive(Debug)]
/// Establishes the capacity of this channel.
///
/// The same enum is used for all Scipio channels.
enum ChannelCapacity {
    Unbounded,
    Bounded(usize),
}
