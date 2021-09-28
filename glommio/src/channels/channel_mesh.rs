// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use std::{
    cell::Cell,
    fmt::{self, Debug, Formatter},
    io::{Error, ErrorKind},
};

use std::sync::{Arc, RwLock};

use crate::{
    channels::shared_channel::{self, *},
    GlommioError,
    Result,
};

/// Sender side
#[derive(Debug)]
pub struct Senders<T: Send> {
    peer_id: usize,
    producer_id: Option<usize>,
    senders: Vec<Option<ConnectedSender<T>>>,
}

impl<T: Send> Senders<T> {
    /// Index of the local executor in the list of all peers
    pub fn peer_id(&self) -> usize {
        self.peer_id
    }

    /// Index of the local executor in the list of producers
    pub fn producer_id(&self) -> Option<usize> {
        self.producer_id
    }

    /// Number of peers to which messages can be sent.
    pub fn nr_consumers(&self) -> usize {
        self.senders.len()
    }

    /// Send a message to the idx-th consumer
    ///
    /// It returns a [`GlommioError::IoError`] encapsulating a [`InvalidInput`]
    /// if the idx is out of the range of available senders, or the sender
    /// is a placeholder in the case of full mesh.
    ///
    /// See [`ConnectedSender.send`] for how the underlying sender works.
    ///
    /// [`GlommioError::IoError`]: crate::GlommioError::IoError
    /// [`InvalidInput`]: https://doc.rust-lang.org/std/io/enum.ErrorKind.html#variant.InvalidInput
    /// [`ConnectedSender.send`]:
    /// crate::channels::shared_channel::ConnectedSender::send
    pub async fn send_to(&self, idx: usize, msg: T) -> Result<(), T> {
        match self.senders.get(idx) {
            Some(Some(consumer)) => consumer.send(msg).await,
            _ => {
                let msg = if idx < self.nr_consumers() {
                    "Local message should not be sent via channel mesh".into()
                } else {
                    format!("Shard {} is invalid in the channel mesh", idx)
                };
                Err(GlommioError::IoError(Error::new(
                    ErrorKind::InvalidInput,
                    msg,
                )))
            }
        }
    }

    /// Send a message to the idx-th consumer
    ///
    /// It returns a [`GlommioError::IoError`] encapsulating a [`InvalidInput`]
    /// if the idx is out of the range of available senders, or the sender
    /// is a placeholder in the case of full mesh.
    ///
    /// See [`ConnectedSender.try_send`] for how the underlying sender works.
    ///
    /// [`GlommioError::IoError`]: crate::GlommioError::IoError
    /// [`InvalidInput`]: https://doc.rust-lang.org/std/io/enum.ErrorKind.html#variant.InvalidInput
    /// [`ConnectedSender.try_send`]:
    /// crate::channels::shared_channel::ConnectedSender::try_send
    pub fn try_send_to(&self, idx: usize, msg: T) -> Result<(), T> {
        match self.senders.get(idx) {
            Some(Some(consumer)) => consumer.try_send(msg),
            _ => Err(GlommioError::IoError(Error::new(
                ErrorKind::InvalidInput,
                "Local message should not be sent via channel mesh",
            ))),
        }
    }

    /// Close the senders
    pub fn close(&self) {
        for sender in self.senders.iter().flatten() {
            sender.close();
        }
    }
}

/// Receiver side
#[derive(Debug)]
pub struct Receivers<T: Send> {
    peer_id: usize,
    consumer_id: Option<usize>,
    receivers: Vec<Option<ConnectedReceiver<T>>>,
}

impl<T: Send> Receivers<T> {
    /// Index of the local executor in the list of all peers
    pub fn peer_id(&self) -> usize {
        self.peer_id
    }

    /// Index of the local executor in the list of consumers.
    pub fn consumer_id(&self) -> Option<usize> {
        self.consumer_id
    }

    /// Number of peers from which message can be received.
    pub fn nr_producers(&self) -> usize {
        self.receivers.len()
    }

    /// Receive a message from the idx-th producer
    ///
    /// It returns a [`GlommioError::IoError`] encapsulating a [`InvalidInput`]
    /// if the idx is out of the range of available receivers, or the
    /// receiver is a placeholder in the case of full mesh.
    ///
    /// See [`ConnectedReceiver.recv`] for how the underlying sender works.
    ///
    /// [`GlommioError::IoError`]: crate::GlommioError::IoError
    /// [`InvalidInput`]: https://doc.rust-lang.org/std/io/enum.ErrorKind.html#variant.InvalidInput
    /// [`ConnectedReceiver.recv`]:
    /// crate::channels::shared_channel::ConnectedReceiver::recv
    pub async fn recv_from(&self, idx: usize) -> Result<Option<T>, ()> {
        match self.receivers.get(idx) {
            Some(Some(producer)) => Ok(producer.recv().await),
            _ => Err(GlommioError::IoError(Error::new(
                ErrorKind::InvalidInput,
                "Local message should not be received from channel mesh",
            ))),
        }
    }

    /// Returns a vec of [`ConnectedReceiver`]s with the id of their upstream
    /// producers.
    ///
    /// [`ConnectedReceiver`]: ../shared_channel/struct.ConnectedReceiver.html
    pub fn streams(&mut self) -> Vec<(usize, ConnectedReceiver<T>)> {
        self.receivers
            .iter_mut()
            .enumerate()
            .flat_map(|(idx, recv)| recv.take().map(|recv| (idx, recv)))
            .collect()
    }
}

struct Peer {
    executor_id: usize,
    notifier: Option<SharedSender<bool>>,
    role: Role,
}

impl Peer {
    fn new(sender: Option<SharedSender<bool>>, role: Role) -> Self {
        Self {
            executor_id: crate::executor().id(),
            notifier: sender,
            role,
        }
    }
}

type SharedChannel<T> = (
    Cell<Option<SharedSender<T>>>,
    Cell<Option<SharedReceiver<T>>>,
);

type SharedChannels<T> = Vec<Vec<SharedChannel<T>>>;

/// The role an executor plays in the mesh
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Role {
    /// The executor produces message
    Producer,
    /// The executor consumes message
    Consumer,
    /// The executor produces and consumes message
    Both,
}

impl Role {
    fn is_producer(&self) -> bool {
        Self::Consumer.ne(self)
    }

    fn is_consumer(&self) -> bool {
        Self::Producer.ne(self)
    }
}

/// An adapter for MeshBuilder
pub trait MeshAdapter: Clone {
    /// Determine whether a channel should be created between a pair of peers
    /// considering their roles.
    fn connect(&self, from: &Role, to: &Role) -> bool;

    /// Determine whether we are building a full mesh, when placeholders should
    /// be inserted into the list senders/receivers
    fn is_full(&self) -> bool;
}

/// Adapter for full mesh
#[derive(Clone, Debug)]
pub struct Full;

impl MeshAdapter for Full {
    fn connect(&self, _: &Role, _: &Role) -> bool {
        true
    }

    fn is_full(&self) -> bool {
        true
    }
}

/// Alias for full mesh builder
pub type FullMesh<T> = MeshBuilder<T, Full>;

/// Adapter for partial mesh
#[derive(Clone, Debug)]
pub struct Partial;

impl MeshAdapter for Partial {
    fn connect(&self, from: &Role, to: &Role) -> bool {
        matches!((from, to), (Role::Producer, Role::Consumer))
    }

    fn is_full(&self) -> bool {
        false
    }
}

/// Alias for partial mesh builder
pub type PartialMesh<T> = MeshBuilder<T, Partial>;

/// A builder for channel mesh
pub struct MeshBuilder<T: Send, A: MeshAdapter> {
    nr_peers: usize,
    channel_size: usize,
    peers: Arc<RwLock<Vec<Peer>>>,
    channels: Arc<SharedChannels<T>>,
    adapter: A,
}

unsafe impl<T: Send, A: MeshAdapter> Send for MeshBuilder<T, A> {}

impl<T: Send, A: MeshAdapter> Clone for MeshBuilder<T, A> {
    fn clone(&self) -> Self {
        Self {
            nr_peers: self.nr_peers,
            channel_size: self.channel_size,
            peers: self.peers.clone(),
            channels: self.channels.clone(),
            adapter: self.adapter.clone(),
        }
    }
}

impl<T: Send, A: MeshAdapter> Debug for MeshBuilder<T, A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!(
            "MeshBuilder {{ nr_peers: {} }}",
            self.nr_peers
        ))
    }
}

impl<T: 'static + Send> MeshBuilder<T, Full> {
    /// Create a full mesh builder.
    pub fn full(nr_peers: usize, channel_size: usize) -> Self {
        Self::new(nr_peers, channel_size, Full)
    }

    /// Join a full mesh
    pub async fn join(self) -> Result<(Senders<T>, Receivers<T>), ()> {
        self.join_with(Role::Both).await
    }
}

impl<T: 'static + Send> MeshBuilder<T, Partial> {
    /// Create a partial mesh builder.
    pub fn partial(nr_peers: usize, channel_size: usize) -> Self {
        Self::new(nr_peers, channel_size, Partial)
    }

    /// Join a partial mesh
    pub async fn join(self, role: Role) -> Result<(Senders<T>, Receivers<T>), ()> {
        self.join_with(role).await
    }
}

impl<T: 'static + Send, A: MeshAdapter> MeshBuilder<T, A> {
    fn new(nr_peers: usize, channel_size: usize, adapter: A) -> Self {
        MeshBuilder {
            nr_peers,
            channel_size,
            peers: Arc::new(RwLock::new(Vec::new())),
            channels: Arc::new(Self::placeholder(nr_peers)),
            adapter,
        }
    }

    /// Returns number of all peers in the mesh
    pub fn nr_peers(&self) -> usize {
        self.nr_peers
    }

    fn placeholder(nr_peers: usize) -> SharedChannels<T> {
        (0..nr_peers)
            .map(|_| {
                (0..nr_peers)
                    .map(|_| (Cell::new(None), Cell::new(None)))
                    .collect()
            })
            .collect()
    }

    fn register(&self, role: Role) -> Result<RegisterResult<bool>, ()> {
        let mut peers = self.peers.write().unwrap();

        if peers.len() == self.nr_peers {
            return Err(GlommioError::IoError(Error::new(
                ErrorKind::Other,
                "The channel mesh is full.",
            )));
        }

        let index = peers
            .binary_search_by(|n| n.executor_id.cmp(&crate::executor().id()))
            .expect_err("Should not join a mesh more than once.");

        if peers.len() == self.nr_peers - 1 {
            peers.insert(index, Peer::new(None, role));

            for (idx_from, from) in peers.iter().enumerate() {
                for (idx_to, to) in peers.iter().enumerate() {
                    let channel = &self.channels[idx_from][idx_to];
                    if idx_from != idx_to && self.adapter.connect(&from.role, &to.role) {
                        let (sender, receiver) = shared_channel::new_bounded(self.channel_size);
                        channel.0.set(Some(sender));
                        channel.1.set(Some(receiver));
                    }
                }
            }

            let peers: Vec<_> = peers
                .iter_mut()
                .map(|notifier| notifier.notifier.take())
                .flatten()
                .collect();

            Ok(RegisterResult::NotificationSenders(peers))
        } else {
            let (sender, receiver) = shared_channel::new_bounded(1);
            peers.insert(index, Peer::new(Some(sender), role));
            Ok(RegisterResult::NotificationReceiver(receiver))
        }
    }

    async fn join_with(self, role: Role) -> Result<(Senders<T>, Receivers<T>), ()> {
        match Self::register(&self, role)? {
            RegisterResult::NotificationReceiver(receiver) => {
                receiver.connect().await.recv().await.unwrap();
            }
            RegisterResult::NotificationSenders(peers) => {
                for peer in peers {
                    peer.connect().await.send(true).await.unwrap();
                }
            }
        }

        let (peer_id, role_id) = {
            let peers = self.peers.read().unwrap();
            let peer_id = peers
                .binary_search_by(|n| n.executor_id.cmp(&crate::executor().id()))
                .unwrap();
            let role_id = peers
                .iter()
                .take(peer_id)
                .filter(|r| r.role.eq(&role))
                .count();
            (peer_id, role_id)
        };

        let producer_id = if role.is_producer() {
            Some(role_id)
        } else {
            None
        };

        let consumer_id = if role.is_consumer() {
            Some(role_id)
        } else {
            None
        };

        let mut senders = Vec::with_capacity(self.nr_peers);
        let mut receivers = Vec::with_capacity(self.nr_peers);

        for i in 0..self.nr_peers {
            let sender = self.channels[peer_id][i].0.take();
            let receiver = self.channels[i][peer_id].1.take();

            let sender = sender.map(|sender| crate::spawn_local(sender.connect()).detach());
            let receiver = receiver.map(|receiver| crate::spawn_local(receiver.connect()).detach());

            match sender {
                None => {
                    if self.adapter.is_full() {
                        senders.push(None)
                    }
                }
                Some(sender) => senders.push(Some(sender.await.unwrap())),
            }

            match receiver {
                None => {
                    if self.adapter.is_full() {
                        receivers.push(None)
                    }
                }
                Some(receiver) => receivers.push(Some(receiver.await.unwrap())),
            }
        }

        Ok((
            Senders {
                peer_id,
                producer_id,
                senders,
            },
            Receivers {
                peer_id,
                consumer_id,
                receivers,
            },
        ))
    }
}

enum RegisterResult<T: Send> {
    NotificationReceiver(SharedReceiver<T>),
    NotificationSenders(Vec<SharedSender<T>>),
}

#[cfg(test)]
mod tests {
    use futures::future;

    use crate::{enclose, prelude::*};

    use super::*;

    #[test]
    fn test_channel_mesh() {
        do_test_channel_mesh(0, 0, 0);
        do_test_channel_mesh(1, 1, 1);
        do_test_channel_mesh(10, 10, 10);
    }

    #[test]
    fn test_peer_id_invariance() {
        let nr_peers = 10;
        let channel_size = 100;
        let builder1 = MeshBuilder::<i32, _>::full(nr_peers, channel_size);
        let builder2 = MeshBuilder::<usize, _>::full(nr_peers, channel_size);

        let executors = (0..nr_peers).map(|_| {
            LocalExecutorBuilder::default().spawn(
                enclose!((builder1, builder2) move || async move {
                    let (sender1, receiver1) = builder1.join().await.unwrap();
                    assert_eq!(sender1.peer_id(), receiver1.peer_id());

                    let (sender2, receiver2) = builder2.join().await.unwrap();
                    assert_eq!(sender2.peer_id(), receiver2.peer_id());

                    assert_eq!(sender1.peer_id(), sender2.peer_id());
                    assert_eq!(receiver1.peer_id(), receiver2.peer_id());
                }),
            )
        });

        for ex in executors.collect::<Vec<_>>() {
            ex.unwrap().join().unwrap();
        }
    }

    #[test]
    #[should_panic]
    #[allow(unused_must_use)]
    fn test_join_more_than_once() {
        let mesh_builder = MeshBuilder::<u32, _>::full(2, 1);

        LocalExecutor::default().run(enclose!((mesh_builder) async move {
            future::join(mesh_builder.clone().join(), mesh_builder.join()).await;
        }));
    }

    #[test]
    #[should_panic]
    #[allow(unused_must_use)]
    fn test_join_more_than_capacity() {
        do_test_channel_mesh(1, 1, 2);
    }

    fn do_test_channel_mesh(nr_peers: usize, channel_size: usize, nr_executors: usize) {
        let mesh_builder = MeshBuilder::full(nr_peers, channel_size);

        let executors = (0..nr_executors).map(|_| {
            LocalExecutorBuilder::default().spawn(enclose!((mesh_builder) move || async move {
                let (sender, receiver) = mesh_builder.join().await.unwrap();
                assert_eq!(nr_peers, sender.nr_consumers());
                assert_eq!(sender.peer_id, sender.producer_id.unwrap());
                assert_eq!(nr_peers, receiver.nr_producers());
                assert_eq!(receiver.peer_id, receiver.consumer_id.unwrap());

                crate::spawn_local(async move {
                    for peer in 0..sender.nr_consumers() {
                        if peer != sender.peer_id() {
                            sender.send_to(peer, (sender.peer_id(), peer)).await.unwrap();
                        }
                    }
                }).detach();

                for peer in 0..receiver.nr_producers() {
                    if peer != receiver.peer_id() {
                        assert_eq!((peer, receiver.peer_id()), receiver.recv_from(peer).await.unwrap().unwrap());
                    }
                }
            }))
        });

        for ex in executors.collect::<Vec<_>>() {
            ex.unwrap().join().unwrap();
        }
    }

    #[test]
    fn test_partial_mesh() {
        let nr_producers = 2;
        let nr_consumers = 3;
        let mesh_builder = MeshBuilder::partial(5, 100);

        let producers = (0..nr_producers).map(|i| {
            LocalExecutorBuilder::default().spawn(enclose!((mesh_builder) move || async move {
                let (sender, receiver) = mesh_builder.join(Role::Producer).await.unwrap();
                assert_eq!(nr_consumers, sender.nr_consumers());
                assert_eq!(Some(i), sender.producer_id());
                assert_eq!(0, receiver.nr_producers());
                assert_eq!(None, receiver.consumer_id());

                for consumer_id in 0..sender.nr_consumers() {
                    sender.send_to(consumer_id, (sender.producer_id().unwrap(), consumer_id)).await.unwrap();
                }
            }))
        });

        let consumers = (0..nr_consumers).map(|i| {
            LocalExecutorBuilder::default().spawn(enclose!((mesh_builder) move || async move {
                let (sender, receiver) = mesh_builder.join(Role::Consumer).await.unwrap();
                assert_eq!(0, sender.nr_consumers());
                assert_eq!(None, sender.producer_id());
                assert_eq!(nr_producers, receiver.nr_producers());
                assert_eq!(Some(i), receiver.consumer_id());

                for producer_id in 0..receiver.nr_producers() {
                    assert_eq!((producer_id, receiver.consumer_id().unwrap()), receiver.recv_from(producer_id).await.unwrap().unwrap());
                }
            }))
        });

        for ex in producers.chain(consumers).collect::<Vec<_>>() {
            ex.unwrap().join().unwrap();
        }
    }
}
