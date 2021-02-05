// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use std::cell::Cell;
use std::fmt::{self, Debug, Formatter};
use std::io::{Error, ErrorKind};
use std::iter::repeat_with;
use std::sync::{Arc, RwLock};

use crate::channels::shared_channel::{self, *};
use crate::{GlommioError, Local, Result, Task};

#[derive(Debug)]
struct Senders<T: Send + Copy> {
    peer_id: usize,
    senders: Vec<Option<ConnectedSender<T>>>,
}

impl<T: Send + Copy> Senders<T> {
    pub fn id(&self) -> usize {
        self.peer_id
    }

    pub fn nr_peers(&self) -> usize {
        self.senders.len()
    }

    pub async fn send_to(&self, peer_id: usize, msg: T) -> Result<(), T> {
        if peer_id == self.peer_id {
            return Err(GlommioError::IoError(Error::new(
                ErrorKind::InvalidInput,
                "Local message should not be sent via channel mesh",
            )));
        }
        self.senders[peer_id].as_ref().unwrap().send(msg).await
    }

    pub fn try_send_to(&self, peer_id: usize, msg: T) -> Result<(), T> {
        if peer_id == self.peer_id {
            return Err(GlommioError::IoError(Error::new(
                ErrorKind::InvalidInput,
                "Local message should not be sent via channel mesh",
            )));
        }
        self.senders[peer_id].as_ref().unwrap().try_send(msg)
    }
}

#[derive(Debug)]
struct Receivers<T: Send + Copy> {
    peer_id: usize,
    receivers: Vec<Option<ConnectedReceiver<T>>>,
}

impl<T: Send + Copy> Receivers<T> {
    pub fn id(&self) -> usize {
        self.peer_id
    }

    pub fn nr_peers(&self) -> usize {
        self.receivers.len()
    }

    pub async fn recv_from(&self, peer_id: usize) -> Result<Option<T>, ()> {
        if peer_id == self.peer_id {
            return Err(GlommioError::IoError(Error::new(
                ErrorKind::InvalidInput,
                "Local message should not be received from channel mesh",
            )));
        }
        Ok(self.receivers[peer_id].as_ref().unwrap().recv().await)
    }
}

struct Notifier {
    executor_id: usize,
    sender: Option<SharedSender<bool>>,
}

impl Notifier {
    fn new(sender: Option<SharedSender<bool>>) -> Self {
        Self {
            executor_id: Local::id(),
            sender,
        }
    }
}

type SharedChannel<T> = (
    Cell<Option<SharedSender<T>>>,
    Cell<Option<SharedReceiver<T>>>,
);

type SharedChannels<T> = Vec<Vec<SharedChannel<T>>>;

#[derive(Clone)]
pub struct MeshBuilder<T: Send + Copy> {
    nr_peers: usize,
    notifiers: Arc<RwLock<Vec<Notifier>>>,
    channels: Arc<SharedChannels<T>>,
}

unsafe impl<T: Send + Copy> Send for MeshBuilder<T> {}

impl<T: Send + Copy> Debug for MeshBuilder<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!(
            "MeshBuilder {{ nr_peers: {} }}",
            self.nr_peers
        ))
    }
}

impl<T: 'static + Send + Copy> MeshBuilder<T> {
    pub fn new(nr_peers: usize, channel_size: usize) -> Self {
        MeshBuilder {
            nr_peers,
            notifiers: Arc::new(RwLock::new(Vec::new())),
            channels: Arc::new(Self::build_channels(nr_peers, channel_size)),
        }
    }

    fn build_channels(nr_peers: usize, channel_size: usize) -> SharedChannels<T> {
        let mut channels: Vec<_> = repeat_with(Vec::new).take(nr_peers).collect();
        for (i, vec) in channels.iter_mut().enumerate() {
            vec.resize_with(i, || Self::build_channel(channel_size));
            vec.resize_with(i + 1, || (Cell::new(None), Cell::new(None)));
            vec.resize_with(nr_peers, || Self::build_channel(channel_size));
        }
        channels
    }

    fn build_channel(channel_size: usize) -> SharedChannel<T> {
        let (sender, receiver) = shared_channel::new_bounded(channel_size);
        (Cell::new(Some(sender)), Cell::new(Some(receiver)))
    }

    fn register(&self) -> Result<RegisterResult<bool>, ()> {
        let mut notifiers = self.notifiers.write().unwrap();

        if notifiers.len() == self.nr_peers {
            return Err(GlommioError::IoError(Error::new(
                ErrorKind::Other,
                "The channel mesh is full.",
            )));
        }

        let index = notifiers
            .binary_search_by(|n| n.executor_id.cmp(&Local::id()))
            .expect_err("Should not join a mesh more than once.");

        if notifiers.len() == self.nr_peers - 1 {
            notifiers.insert(index, Notifier::new(None));

            let peers: Vec<_> = notifiers
                .iter_mut()
                .map(|notifier| notifier.sender.take())
                .flatten()
                .collect();

            Ok(RegisterResult::NotificationSenders(peers))
        } else {
            let (sender, receiver) = shared_channel::new_bounded(1);
            notifiers.insert(index, Notifier::new(Some(sender)));
            Ok(RegisterResult::NotificationReceiver(receiver))
        }
    }

    async fn join(self) -> Result<(Senders<T>, Receivers<T>), ()> {
        match Self::register(&self)? {
            RegisterResult::NotificationReceiver(receiver) => {
                receiver.connect().await.recv().await.unwrap();
            }
            RegisterResult::NotificationSenders(peers) => {
                for peer in peers {
                    peer.connect().await.send(true).await.unwrap();
                }
            }
        }

        let peer_id = {
            let notifiers = self.notifiers.read().unwrap();

            notifiers
                .binary_search_by(|n| n.executor_id.cmp(&Local::id()))
                .unwrap()
        };

        let mut senders = Vec::with_capacity(self.nr_peers);
        let mut receivers = Vec::with_capacity(self.nr_peers);

        for i in 0..self.nr_peers {
            if peer_id == i {
                senders.push(None);
                receivers.push(None);
            } else {
                let sender = self.channels[peer_id][i].0.take().unwrap();
                let sender = Task::<_>::local(sender.connect()).detach();

                let receiver = self.channels[i][peer_id].1.take().unwrap();
                let receiver = Task::<_>::local(receiver.connect()).detach();

                senders.push(Some(sender.await.unwrap()));
                receivers.push(Some(receiver.await.unwrap()));
            }
        }

        Ok((
            Senders { peer_id, senders },
            Receivers { peer_id, receivers },
        ))
    }
}

enum RegisterResult<T: Send + Copy> {
    NotificationReceiver(SharedReceiver<T>),
    NotificationSenders(Vec<SharedSender<T>>),
}

#[cfg(test)]
mod tests {
    use futures::future;

    use crate::enclose;
    use crate::prelude::*;

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
        let builder1 = MeshBuilder::<i32>::new(nr_peers, channel_size);
        let builder2 = MeshBuilder::<usize>::new(nr_peers, channel_size);

        let executors = (0..nr_peers).map(|_| {
            LocalExecutorBuilder::new().spawn(enclose!((builder1, builder2) move || async move {
                let (sender1, receiver1) = builder1.join().await.unwrap();
                assert_eq!(sender1.id(), receiver1.id());

                let (sender2, receiver2) = builder2.join().await.unwrap();
                assert_eq!(sender2.id(), receiver2.id());

                assert_eq!(sender1.id(), sender2.id());
                assert_eq!(receiver1.id(), receiver2.id());
            }))
        });

        for ex in executors.collect::<Vec<_>>() {
            ex.unwrap().join().unwrap();
        }
    }

    #[test]
    #[should_panic]
    fn test_join_more_than_once() {
        let mesh_builder = MeshBuilder::<u32>::new(2, 1);

        LocalExecutor::default().run(enclose!((mesh_builder) async move {
            future::join(mesh_builder.clone().join(), mesh_builder.join()).await;
        }));
    }

    #[test]
    #[should_panic]
    fn test_join_more_than_capacity() {
        do_test_channel_mesh(1, 1, 2);
    }

    fn do_test_channel_mesh(nr_peers: usize, channel_size: usize, nr_executors: usize) {
        let mesh_builder = MeshBuilder::new(nr_peers, channel_size);

        let executors = (0..nr_executors).map(|_| {
            LocalExecutorBuilder::new().spawn(enclose!((mesh_builder) move || async move {
                let (sender, receiver) = mesh_builder.join().await.unwrap();
                Local::local(async move {
                    for peer in 0..sender.nr_peers() {
                        if peer != sender.id() {
                            sender.send_to(peer, (sender.id(), peer)).await.unwrap();
                        }
                    }
                }).detach();

                for peer in 0..receiver.nr_peers() {
                    if peer != receiver.id() {
                        assert_eq!((peer, receiver.id()), receiver.recv_from(peer).await.unwrap().unwrap());
                    }
                }
            }))
        });

        for ex in executors.collect::<Vec<_>>() {
            ex.unwrap().join().unwrap();
        }
    }
}
