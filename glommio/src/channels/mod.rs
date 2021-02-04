// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
//! glommio::channels is a module that provides glommio channel-like abstractions.
//!
mod spsc_queue;

/// Allow data to be transmitted across two tasks in the same shard.
///
/// Asynchronous code is rarely useful if it is serialized. In practice,
/// you will want to spawn asynchronous tasks or in the case of Glommio which
/// support multi-queued execution have a dedicated [`TaskQueue`] for each group
/// of tasks with their own priority.
///
/// The `local_channel` is an abstraction similar to the [`channel`] that can be
/// used for this purpose
///
/// The communication between sender and receiver is broken when one of them goes
/// out of scope. They however behave differently:
///
/// * The [`LocalReceiver`] never sees an error, as it provides a stream
///   interface compatible with [`StreamExt`]. When the sender is no longer available the receiver's call to [`next`] will return [`None`].
/// * The [`LocalSender`] will return a [`GlommioError::Closed(..)`](crate::GlommioError::Closed) if it tries to [`send`] into
///   a channel that no longer has a receiver.
///
/// # Examples
///
/// ```
/// use glommio::{LocalExecutor, Local, Latency, Shares};
/// use glommio::channels::local_channel;
/// use futures_lite::stream::StreamExt;
///
/// let ex = LocalExecutor::default();
/// ex.run(async move {
///     let task_queue =
///         Local::create_task_queue(
///             Shares::default(), Latency::NotImportant, "example");
///
///     let (sender, mut receiver) = local_channel::new_unbounded();
///     let h = Local::local_into(async move {
///         assert_eq!(receiver.stream().next().await.unwrap(), 0);
///     }, task_queue).unwrap().detach();
///     sender.try_send(0);
///     drop(sender);
///     h.await;
/// });
/// ```
/// [`TaskQueue`]: ../../struct.TaskQueueHandle.html
/// [`LocalSender`]: struct.LocalSender.html
/// [`LocalReceiver`]: struct.LocalReceiver.html
/// [`GlommioError`]: ../struct.GlommioError.html
/// [`send`]: struct.LocalSender.html#method.send
/// [`None`]: https://doc.rust-lang.org/std/option/enum.Option.html#variant.None
/// [`channel`]: https://doc.rust-lang.org/std/sync/mpsc/fn.channel.html
/// [`next`]: https://docs.rs/futures-lite/1.11.1/futures_lite/stream/trait.StreamExt.html#method.next
/// [`StreamExt`]: https://docs.rs/futures-lite/1.11.1/futures_lite/stream/trait.StreamExt.html
pub mod local_channel;

/// Allow data to be transmitted across two tasks in the different executors.
///
/// Most useful thread-per-core applications will heavily process data locally
/// to their executor, but at times inter-executor communication is unavoidable
/// (if you never communicate inter-executors, you may consider just using independent
/// processes!)
///
/// Whenever you need to send data across two executors you can use the `shared_channel`.
/// This channel is a fully lockless single-producer-single-consumer channel, unlike the standard
/// library [`channel`], which is multi-producer and need locks for synchronization. That means
/// that to wire N executors to each other you will have to create O(N^2) channels.
///
/// The channels are also not bidirectional, so for full bidirectional communication you
/// will need pairs of channels.
///
/// True to the spirit of our library the `shared_channel` is not created or wired
/// automatically among executors. You can have executors that work as sinks and never
/// respond back, other executors that are not really connected to anybody, cliques of
/// executors, you name it.
///
/// However, note that connecting a channel will involve a blocking operation as we need to
/// synchronize and exchange information between peers. That can block the executor, although in
/// practice that will only happen if it races with the creation of a new executor. Because of
/// that, The best performance pattern when using shared channels is to create both the
/// executors and channels early, connect the channels as soon as they start, and keep them alive.
///
/// Data that goes into the channels need to be [`Send`], as well as the channels themselves
/// (otherwise you would not be able to really pass them along to the actual executors). But to
/// prevent accidental use from multiple producers, the channel endpoints have to be `connected` before
/// using: Connecting consumes the endpoint, transforming a [`SharedSender`] into a [`ConnectedSender`] and
/// a [`SharedReceiver`] into a [`ConnectedReceiver`] so although you can pass one of the ends of the channel
/// to multiple executors, you can only connect in one of them. The connected channel itself is not
/// [`Send`] nor [`Sync`] so once connected they are stuck in an executor.
///
/// The communication between sender and receiver is broken when one of them goes
/// out of scope. They however behave differently:
///
/// * The [`ConnectedReceiver`] never sees an error, as it is implemented as a stream
///   interface compatible with [`StreamExt`]. When the sender is no longer available the receiver's call to [`next`] will return [`None`].
/// * The [`ConnectedSender`] will return a [`GlommioError::Closed(..)`](crate::GlommioError::Closed) if it tries to [`send`] into
///   a channel that no longer has a receiver.
///
/// # Examples
///
/// ```
/// use glommio::{LocalExecutorBuilder, Local};
/// use glommio::channels::shared_channel;
///
/// // creates both ends of the channel. This is done outside the executors
/// // and we will not pass the sender to ex1 and the receiver to ex2
/// let (sender, receiver) = shared_channel::new_bounded(1);
///
/// let ex1 = LocalExecutorBuilder::new()
///     .spawn(move || async move {
///         // Before using we have to connect. Connecting this endpoint
///         // binds it this executor as the connected endpoint is not Send.
///         let sender = sender.connect().await;
///         // Channel has room for 1 element so this will always succeed
///         sender.try_send(100).unwrap();
///     })
///     .unwrap();
///
/// let ex2 = LocalExecutorBuilder::new()
///     .spawn(move || async move {
///         // much like the sender, the receiver also needs to be connected
///         let receiver = receiver.connect().await;
///         let x = receiver.recv().await.unwrap();
///         assert_eq!(x, 100);
///     })
///     .unwrap();
///
/// ex1.join().unwrap();
/// ex2.join().unwrap();
/// ```
/// [`GlommioError`]: ../struct.GlommioError.html
/// [`ConnectedSender`]: struct.ConnectedSender.html
/// [`ConnectedReceiver`]: struct.ConnectedReceiver.html
/// [`SharedSender`]: struct.SharedSender.html
/// [`SharedReceiver`]: struct.SharedReceiver.html
/// [`Send`]: https://doc.rust-lang.org/std/marker/trait.Send.html
/// [`Sync`]: https://doc.rust-lang.org/std/marker/trait.Sync.html
/// [`send`]: struct.ConnectedSender.html#method.send
/// [`None`]: https://doc.rust-lang.org/std/option/enum.Option.html#variant.None
/// [`channel`]: https://doc.rust-lang.org/std/sync/mpsc/fn.channel.html
/// [`next`]: https://docs.rs/futures-lite/1.11.1/futures_lite/stream/trait.StreamExt.html#method.next
/// [`StreamExt`]: https://docs.rs/futures-lite/1.11.1/futures_lite/stream/trait.StreamExt.html
pub mod shared_channel;

/// A channel mesh consists of a group of participating executors (peers) and the channels between
/// each pair of them.
///
/// Within a mesh, peers are identified by unique peer ids, which are used to specify the
/// source/destination which messages are sent to/received from. Peer ids are assigned when the last
/// peer joins by the indexes to the list of all peers, which is sorted by their executor ids. So
/// multiple meshes can be built over the same set of peers, and the peer ids are guaranteed to be
/// identical across meshes for each peer.
///
/// # Examples
///
/// ```
/// use glommio::enclose;
/// use glommio::prelude::*;
/// use glommio::channels::channel_mesh::MeshBuilder;
///
/// let mesh_builder = MeshBuilder::new(nr_peers, channel_size);
///
/// let executors = (0..nr_executors).map(|_| {
///     LocalExecutorBuilder::new().spawn(enclose!((mesh_builder) move || async move {
///         let (sender, receiver) = mesh_builder.join().await;
///         Local::local(async move {
///             for peer in 0..sender.nr_peers() {
///                 sender.send_to(peer, (sender.id(), peer)).await.unwrap();
///             }
///         }).detach();
///
///         for peer in 0..receiver.nr_peers() {
///             assert_eq!((peer, receiver.id()), receiver.recv_from(peer).await.unwrap());
///         }
///     }))
/// });
///
/// for ex in executors.collect::<Vec<_>>() {
///     ex.unwrap().join().unwrap();
/// }
/// ```
pub mod channel_mesh;

use std::fmt::Debug;

#[derive(Debug)]
/// Establishes the capacity of this channel.
///
/// The same enum is used for all Glommio channels.
enum ChannelCapacity {
    Unbounded,
    Bounded(usize),
}
