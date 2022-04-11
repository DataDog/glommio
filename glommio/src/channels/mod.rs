// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
//! glommio::channels is a module that provides glommio channel-like
//! abstractions.

/// A single-producer, single-consumer lock-free queue, allowing two threads
/// to efficiently communicate.
pub mod spsc_queue;

/// Allow data to be transmitted across two tasks in the same shard.
///
/// Asynchronous code is rarely useful if it is serialized. In practice,
/// you will want to spawn asynchronous tasks or in the case of Glommio which
/// support multi-queued execution have a dedicated [`TaskQueue`] for each group
/// of tasks with their own priority.
///
/// The `local_channel` is an abstraction similar to the [`channel`] that can be
/// used for this purpose.
///
/// The communication between sender and receiver is broken when one of them
/// goes out of scope. They however behave differently:
///
/// * The [`LocalReceiver`] never sees an error, as it provides a stream
///   interface compatible with [`StreamExt`]. When the sender is no longer
///   available the receiver's call to [`next`] will return [`None`].
/// * The [`LocalSender`] will return a
///   [`GlommioError::Closed(..)`](crate::GlommioError::Closed) if it tries to
///   [`send`] into a channel that no longer has a receiver.
///
/// # Examples
///
/// ```
/// use futures_lite::stream::StreamExt;
/// use glommio::{channels::local_channel, Latency, LocalExecutor, Shares};
///
/// let ex = LocalExecutor::default();
/// ex.run(async move {
///     let task_queue = glommio::executor().create_task_queue(
///         Shares::default(),
///         Latency::NotImportant,
///         "example",
///     );
///
///     let (sender, mut receiver) = local_channel::new_unbounded();
///     let h = glommio::spawn_local_into(
///         async move {
///             assert_eq!(receiver.stream().next().await.unwrap(), 0);
///         },
///         task_queue,
///     )
///     .unwrap()
///     .detach();
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

/// Allow data to be transmitted across two tasks in different executors.
///
/// Most useful thread-per-core applications will heavily process data locally
/// to their executor, but at times inter-executor communication is unavoidable
/// (if you never communicate inter-executors, you may consider just using
/// independent processes!)
///
/// Whenever you need to send data across two executors you can use the
/// `shared_channel`. This channel is a fully lockless
/// single-producer-single-consumer channel, unlike the standard
/// library [`channel`], which is multi-producer and need locks for
/// synchronization. That means that to wire N executors to each other you will
/// have to create O(N^2) channels.
///
/// The channels are also not bidirectional, so for full bidirectional
/// communication you will need pairs of channels.
///
/// True to the spirit of our library the `shared_channel` is not created or
/// wired automatically among executors. You can have executors that work as
/// sinks and never respond back, other executors that are not really connected
/// to anybody, cliques of executors, you name it.
///
/// However, note that connecting a channel will involve a blocking operation as
/// we need to synchronize and exchange information between peers. That can
/// block the executor, although in practice that will only happen if it races
/// with the creation of a new executor. Because of that, the best performance
/// pattern when using shared channels is to create both the executors and
/// channels early, connect the channels as soon as they start, and keep them
/// alive.
///
/// Data that goes into the channels need to be [`Send`], as well as the
/// channels themselves (otherwise you would not be able to really pass them
/// along to the actual executors). But to prevent accidental use from multiple
/// producers, the channel endpoints have to be `connected` before
/// using: Connecting consumes the endpoint, transforming a [`SharedSender`]
/// into a [`ConnectedSender`] and a [`SharedReceiver`] into a
/// [`ConnectedReceiver`] so although you can pass one of the ends of the
/// channel to multiple executors, you can only connect in one of them. The
/// connected channel itself is not [`Send`] nor [`Sync`] so once connected they
/// are stuck in an executor.
///
/// The communication between sender and receiver is broken when one of them
/// goes out of scope. They however behave differently:
///
/// * The [`ConnectedReceiver`] never sees an error, as it is implemented as a
///   stream interface compatible with [`StreamExt`]. When the sender is no
///   longer available the receiver's call to [`next`] will return [`None`].
/// * The [`ConnectedSender`] will return a
///   [`GlommioError::Closed(..)`](crate::GlommioError::Closed) if it tries to
///   [`send`] into a channel that no longer has a receiver.
///
/// # Examples
///
/// ```
/// use glommio::{channels::shared_channel, LocalExecutorBuilder};
///
/// // Creates both ends of the channel. This is done outside the executors
/// // and we will then pass the sender to ex1 and the receiver to ex2
/// let (sender, receiver) = shared_channel::new_bounded(1);
///
/// let ex1 = LocalExecutorBuilder::default()
///     .spawn(move || async move {
///         // Before using we have to connect. Connecting this endpoint
///         // binds it to this executor as the connected endpoint is not Send.
///         let sender = sender.connect().await;
///         // Channel has room for 1 element so this will always succeed
///         sender.try_send(100).unwrap();
///     })
///     .unwrap();
///
/// let ex2 = LocalExecutorBuilder::default()
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

/// A mesh-like structure to connect a set of executors
///
/// A channel mesh consists of a group of participating executors (peers) and
/// the channels between selected pairs of them. Two kinds of meshes are
/// supported depending on the peer-pair selection criteria: full mesh and
/// partial mesh.
///
/// With full mesh, every pair of distinct peers are selected, so messages can
/// be sent to every other peer. Full mesh is useful for building kinds of
/// sharding. With partial mesh, peers are assigned a role of either producer or
/// consumer when they join the mesh, and channels are created from producers to
/// consumers.
///
/// Within a mesh, peers are identified by unique peer ids, which are used to
/// specify the source/destination which messages are sent to/received from.
/// Peer ids are determined when the last peer joins the mesh by the indexes to
/// the list of all peers sorted by their executor ids. In this way, multiple
/// meshes can be built over the same set of peers, and the peer ids are
/// guaranteed to be identical across meshes for each peer. This invariance
/// makes it possible to cooperate with multiple meshes.
///
/// # Examples
///
/// Full mesh
/// ```
/// use glommio::enclose;
/// use glommio::prelude::*;
/// use glommio::channels::channel_mesh::MeshBuilder;
///
/// let nr_peers = 5;
/// let channel_size = 100;
/// let mesh_builder = MeshBuilder::full(nr_peers, channel_size);
///
/// let executors = (0..nr_peers).map(|_| {
///     LocalExecutorBuilder::default().spawn(enclose!((mesh_builder) move || async move {
///         let (sender, receiver) = mesh_builder.join().await.unwrap();
///         glommio::spawn_local(async move {
///             for peer in 0..sender.nr_consumers() {
///                 if peer != sender.peer_id() {
///                     sender.send_to(peer, (sender.peer_id(), peer)).await.unwrap();
///                 }
///             }
///         }).detach();
///
///         for peer in 0..receiver.nr_producers() {
///             if peer != receiver.peer_id() {
///                 assert_eq!((peer, receiver.peer_id()), receiver.recv_from(peer).await.unwrap().unwrap());
///             }
///         }
///     }))
/// });
///
/// for ex in executors.collect::<Vec<_>>() {
///     ex.unwrap().join().unwrap();
/// }
/// ```
///
/// Partial mesh
/// ```
/// use glommio::enclose;
/// use glommio::prelude::*;
/// use glommio::channels::channel_mesh::{MeshBuilder, Role};
///
/// let nr_producers = 2;
/// let nr_consumers = 3;
/// let channel_size = 100;
/// let mesh_builder = MeshBuilder::partial(nr_producers + nr_consumers, channel_size);
///
/// let producers = (0..nr_producers).map(|i| {
///     LocalExecutorBuilder::default().spawn(enclose!((mesh_builder) move || async move {
///         let (sender, receiver) = mesh_builder.join(Role::Producer).await.unwrap();
///         assert_eq!(nr_consumers, sender.nr_consumers());
///         assert_eq!(Some(i), sender.producer_id());
///         assert_eq!(0, receiver.nr_producers());
///         assert_eq!(None, receiver.consumer_id());
///
///         for consumer_id in 0..sender.nr_consumers() {
///             sender.send_to(consumer_id, (sender.producer_id().unwrap(), consumer_id)).await.unwrap();
///         }
///     }))
/// });
///
/// let consumers = (0..nr_consumers).map(|i| {
///     LocalExecutorBuilder::default().spawn(enclose!((mesh_builder) move || async move {
///         let (sender, receiver) = mesh_builder.join(Role::Consumer).await.unwrap();
///         assert_eq!(0, sender.nr_consumers());
///         assert_eq!(None, sender.producer_id());
///         assert_eq!(nr_producers, receiver.nr_producers());
///         assert_eq!(Some(i), receiver.consumer_id());
///
///         for producer_id in 0..receiver.nr_producers() {
///             assert_eq!((producer_id, receiver.consumer_id().unwrap()), receiver.recv_from(producer_id).await.unwrap().unwrap());
///         }
///     }))
/// });
///
/// for ex in producers.chain(consumers).collect::<Vec<_>>() {
///     ex.unwrap().join().unwrap();
/// }
/// ```
pub mod channel_mesh;

/// Sharding utilities built on top of full mesh.
///
/// Examples
///
/// ```
/// use futures_lite::future::ready;
/// use futures_lite::stream::repeat_with;
/// use futures_lite::{FutureExt, StreamExt};
/// use glommio::enclose;
/// use glommio::prelude::*;
/// use glommio::channels::channel_mesh::MeshBuilder;
/// use glommio::channels::sharding::{Handler, Sharded, HandlerResult};
///
/// type Msg = i32;
///
/// let nr_shards = 2;
///
/// fn get_shard_for(msg: &Msg, nr_shards: usize) -> usize {
///     *msg as usize % nr_shards
/// }
///
/// #[derive(Clone)]
/// struct RequestHandler {
///     nr_shards: usize,
/// };
///
/// impl Handler<i32> for RequestHandler {
///     fn handle(&self, msg: Msg, _src_shard: usize, cur_shard: usize) -> HandlerResult {
///         println!("shard {} received {}", cur_shard, msg);
///         assert_eq!(get_shard_for(&msg, self.nr_shards), cur_shard);
///         ready(()).boxed_local()
///     }
/// }
///
/// let mesh = MeshBuilder::full(nr_shards, 1024);
///
/// let shards = (0..nr_shards).map(|_| {
///     LocalExecutorBuilder::default().spawn(enclose!((mesh) move || async move {
///         let handler = RequestHandler { nr_shards };
///         let mut sharded = Sharded::new(mesh, get_shard_for, handler).await.unwrap();
///         let me = sharded.shard_id();
///         let messages = repeat_with(|| fastrand::i32(0..10)).take(1000).inspect(move |x| println!("shard {} generated {}", me, x));
///         sharded.handle(messages).unwrap();
///         sharded.close().await;
///     }))
/// });
///
/// for s in shards.collect::<Vec<_>>() {
///     s.unwrap().join().unwrap();
/// }
/// ```
pub mod sharding;

use std::fmt::Debug;

#[derive(Debug)]
/// Establishes the capacity of this channel.
///
/// The same enum is used for all Glommio channels.
enum ChannelCapacity {
    Unbounded,
    Bounded(usize),
}
