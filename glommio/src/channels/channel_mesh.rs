// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use std::{
    fmt::{self, Debug, Formatter},
    io::{Error, ErrorKind},
    sync::{Arc, Mutex},
};

use crate::{
    channels::shared_channel::{self, *},
    GlommioError, Result,
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
                    format!("Shard {idx} is invalid in the channel mesh")
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
    role: Role,
}

impl Peer {
    fn new(role: Role) -> Self {
        Self {
            executor_id: crate::executor().id(),
            role,
        }
    }
}

/// The role an executor plays in the mesh
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

struct JoinState<T: Send> {
    peers: Vec<Peer>,
    ready: bool,
    senders: Vec<Vec<Option<SharedSender<T>>>>,
    receivers: Vec<Vec<Option<SharedReceiver<T>>>>,
}

impl<T: Send> JoinState<T> {
    fn new(nr_peers: usize) -> Self {
        let mut senders: Vec<Vec<Option<SharedSender<T>>>> = Vec::with_capacity(nr_peers);
        let mut receivers: Vec<Vec<Option<SharedReceiver<T>>>> = Vec::with_capacity(nr_peers);

        for _ in 0..nr_peers {
            senders.push((0..nr_peers).map(|_| None).collect());
            receivers.push((0..nr_peers).map(|_| None).collect());
        }

        Self {
            peers: Vec::new(),
            ready: false,
            senders,
            receivers,
        }
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
    state: Arc<Mutex<JoinState<T>>>,
    adapter: A,
}

impl<T: Send, A: MeshAdapter> Clone for MeshBuilder<T, A> {
    fn clone(&self) -> Self {
        Self {
            nr_peers: self.nr_peers,
            channel_size: self.channel_size,
            state: self.state.clone(),
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
            state: Arc::new(Mutex::new(JoinState::new(nr_peers))),
            adapter,
        }
    }

    /// Returns number of all peers in the mesh
    pub fn nr_peers(&self) -> usize {
        self.nr_peers
    }

    async fn register(&self, role: Role) -> Result<(usize, usize), ()> {
        let (is_last, exec_id) = {
            let mut state = self.state.lock().unwrap();

            if state.peers.len() == self.nr_peers {
                return Err(GlommioError::IoError(Error::other(
                    "The channel mesh is full.",
                )));
            }

            let exec_id = crate::executor().id();
            let index = state
                .peers
                .binary_search_by(|n| n.executor_id.cmp(&exec_id))
                .expect_err("Should not join a mesh more than once.");

            state.peers.insert(index, Peer::new(role));

            (state.peers.len() == self.nr_peers, exec_id)
        };

        if is_last {
            // If this was the last joiner, build the channels
            let mut state = self.state.lock().unwrap();
            if !state.ready {
                let n = state.peers.len();
                for from_idx in 0..n {
                    for to_idx in 0..n {
                        if from_idx == to_idx {
                            continue;
                        }

                        let from_role = state.peers[from_idx].role;
                        let to_role = state.peers[to_idx].role;

                        if self.adapter.connect(&from_role, &to_role) {
                            let (tx, rx) = shared_channel::new_bounded(self.channel_size);
                            state.senders[from_idx][to_idx] = Some(tx);
                            state.receivers[from_idx][to_idx] = Some(rx);
                        }
                    }
                }

                state.ready = true;
            }
        } else {
            // Wait until last joiner finishes building
            loop {
                let ready = {
                    let state = self.state.lock().unwrap();
                    state.ready
                };

                if ready {
                    break;
                }

                futures_lite::future::yield_now().await;
            }
        }

        let state = self.state.lock().unwrap();
        // At this point st.ready == true and st.peers is stable.

        let peer_id = state
            .peers
            .binary_search_by(|p| p.executor_id.cmp(&exec_id))
            .unwrap();

        let role_id = state.peers[..peer_id]
            .iter()
            .filter(|p| p.role == role)
            .count();

        Ok((peer_id, role_id))
    }

    async fn join_with(self, role: Role) -> Result<(Senders<T>, Receivers<T>), ()> {
        let (peer_id, role_id) = self.register(role).await?;

        // Extract Senders and Receivers for this peer
        let (mut senders, mut receivers) = {
            let mut st = self.state.lock().unwrap();

            let mut row = Vec::with_capacity(self.nr_peers);
            let mut col = Vec::with_capacity(self.nr_peers);

            for i in 0..self.nr_peers {
                row.push(st.senders[peer_id][i].take());
                col.push(st.receivers[i][peer_id].take());
            }

            (row, col)
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

        let mut senders_vec = Vec::with_capacity(self.nr_peers);
        let mut receivers_vec = Vec::with_capacity(self.nr_peers);

        for i in 0..self.nr_peers {
            let sender = senders[i].take();
            let receiver = receivers[i].take();

            let sender = sender.map(|s| crate::spawn_local(s.connect()).detach());
            let receiver = receiver.map(|r| crate::spawn_local(r.connect()).detach());

            match sender {
                None => {
                    if self.adapter.is_full() {
                        senders_vec.push(None)
                    }
                }
                Some(h) => senders_vec.push(Some(h.await.unwrap())),
            }

            match receiver {
                None => {
                    if self.adapter.is_full() {
                        receivers_vec.push(None)
                    }
                }
                Some(h) => receivers_vec.push(Some(h.await.unwrap())),
            }
        }

        Ok((
            Senders {
                peer_id,
                producer_id,
                senders: senders_vec,
            },
            Receivers {
                peer_id,
                consumer_id,
                receivers: receivers_vec,
            },
        ))
    }
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
