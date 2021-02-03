// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use std::cell::Cell;
use std::fmt::{self, Debug, Formatter};
use std::result::Result as StdResult;
use std::sync::{Arc, RwLock};

use futures::future;

use crate::channels::local_channel::{self, *};
use crate::channels::shared_channel::{self, *};
use crate::{Local, Result};

#[derive(Debug)]
pub enum Sender<T: Send + Copy> {
    Local(LocalSender<T>),
    Connected(ConnectedSender<T>),
}

impl<T: Send + Copy> Sender<T> {
    pub async fn send(&self, item: T) -> Result<(), T> {
        match self {
            Self::Local(sender) => sender.send(item).await,
            Self::Connected(sender) => sender.send(item).await,
        }
    }

    pub fn try_send(&self, item: T) -> Result<(), T> {
        match self {
            Self::Local(sender) => sender.try_send(item),
            Self::Connected(sender) => sender.try_send(item),
        }
    }
}

enum SenderProvider<T: Send + Copy> {
    Local(LocalSender<T>),
    Shared(SharedSender<T>),
    Taken,
}

impl<T: Send + Copy> Default for SenderProvider<T> {
    fn default() -> Self {
        Self::Taken
    }
}

impl<T: Send + Copy + 'static> SenderProvider<T> {
    async fn get(self) -> Sender<T> {
        match self {
            Self::Local(sender) => Sender::Local(sender),
            Self::Shared(sender) => Sender::Connected(sender.connect().await),
            Self::Taken => unreachable!(),
        }
    }
}

#[derive(Debug)]
pub enum Receiver<T: Send + Copy> {
    Local(LocalReceiver<T>),
    Connected(ConnectedReceiver<T>),
}

impl<T: Send + Copy> Receiver<T> {
    pub async fn recv(&self) -> Option<T> {
        match self {
            Self::Local(receiver) => receiver.recv().await,
            Self::Connected(receiver) => receiver.recv().await,
        }
    }
}

enum ReceiverProvider<T: Send + Copy> {
    Local(LocalReceiver<T>),
    Shared(SharedReceiver<T>),
    Taken,
}

impl<T: Send + Copy> Default for ReceiverProvider<T> {
    fn default() -> Self {
        Self::Taken
    }
}

impl<T: Send + Copy + 'static> ReceiverProvider<T> {
    async fn get(self) -> Receiver<T> {
        match self {
            Self::Local(receiver) => Receiver::Local(receiver),
            Self::Shared(receiver) => Receiver::Connected(receiver.connect().await),
            Self::Taken => unreachable!(),
        }
    }
}

#[derive(Debug)]
struct Senders<T: Send + Copy> {
    id: usize,
    senders: Vec<Sender<T>>,
}

impl<T: Send + Copy> Senders<T> {
    pub fn id(&self) -> usize {
        self.id
    }

    pub fn nr_peers(&self) -> usize {
        self.senders.len()
    }

    pub async fn send_to(&self, peer_id: usize, msg: T) -> Result<(), T> {
        self.senders[peer_id].send(msg).await
    }

    pub fn try_send_to(&self, peer_id: usize, msg: T) -> Result<(), T> {
        self.senders[peer_id].try_send(msg)
    }
}

#[derive(Debug)]
struct Receivers<T: Send + Copy> {
    id: usize,
    receivers: Vec<Receiver<T>>,
}

impl<T: Send + Copy> Receivers<T> {
    pub fn id(&self) -> usize {
        self.id
    }

    pub fn nr_peers(&self) -> usize {
        self.receivers.len()
    }

    pub async fn recv_from(&self, peer_id: usize) -> Option<T> {
        self.receivers[peer_id].recv().await
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

type ChannelProvider<T> = (Cell<SenderProvider<T>>, Cell<ReceiverProvider<T>>);

type ChannelProviders<T> = Vec<Vec<ChannelProvider<T>>>;

#[derive(Clone)]
pub struct MeshBuilder<T: Send + Copy> {
    nr_peers: usize,
    notifiers: Arc<RwLock<Vec<Notifier>>>,
    providers: Arc<Vec<Vec<ChannelProvider<T>>>>,
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
            providers: Arc::new(Self::build_providers(nr_peers, channel_size)),
        }
    }

    fn build_providers(nr_peers: usize, channel_size: usize) -> ChannelProviders<T> {
        (0..nr_peers)
            .map(|i| {
                (0..nr_peers)
                    .map(|j| Self::build_provider(i, j, channel_size))
                    .collect()
            })
            .collect()
    }

    fn build_provider(from: usize, to: usize, channel_size: usize) -> ChannelProvider<T> {
        if from == to {
            let (sender, receiver) = local_channel::new_bounded(channel_size.max(1));
            let sender_provider = SenderProvider::Local(sender);
            let receiver_provider = ReceiverProvider::Local(receiver);
            (Cell::new(sender_provider), Cell::new(receiver_provider))
        } else {
            let (sender, receiver) = shared_channel::new_bounded(channel_size);
            let sender_provider = SenderProvider::Shared(sender);
            let receiver_provider = ReceiverProvider::Shared(receiver);
            (Cell::new(sender_provider), Cell::new(receiver_provider))
        }
    }

    fn register(&self) -> StdResult<SharedReceiver<bool>, Vec<SharedSender<bool>>> {
        let mut notifiers = self.notifiers.write().unwrap();

        if notifiers.len() == self.nr_peers {
            panic!("The channel mesh is full.");
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

            Err(peers)
        } else {
            let (sender, receiver) = shared_channel::new_bounded(1);
            notifiers.insert(index, Notifier::new(Some(sender)));
            Ok(receiver)
        }
    }

    async fn join(self) -> (Senders<T>, Receivers<T>) {
        match Self::register(&self) {
            Ok(receiver) => {
                receiver.connect().await.recv().await.unwrap();
            }
            Err(peers) => {
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

        let providers = self.providers.as_ref();

        let mut senders = Senders {
            id: peer_id,
            senders: Vec::with_capacity(self.nr_peers),
        };
        let mut receivers = Receivers {
            id: peer_id,
            receivers: Vec::with_capacity(self.nr_peers),
        };

        for i in 0..self.nr_peers {
            let sender = providers[peer_id][i].0.take();
            let receiver = providers[i][peer_id].1.take();
            let (sender, receiver) = future::join(sender.get(), receiver.get()).await;
            senders.senders.push(sender);
            receivers.receivers.push(receiver);
        }

        (senders, receivers)
    }
}

#[cfg(test)]
mod tests {
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
                let (sender, receiver) = mesh_builder.join().await;
                Local::local(async move {
                    for peer in 0..sender.nr_peers() {
                        sender.send_to(peer, (sender.id(), peer)).await.unwrap();
                    }
                }).detach();

                for peer in 0..receiver.nr_peers() {
                    assert_eq!((peer, receiver.id()), receiver.recv_from(peer).await.unwrap());
                }
            }))
        });

        for ex in executors.collect::<Vec<_>>() {
            ex.unwrap().join().unwrap();
        }
    }
}
