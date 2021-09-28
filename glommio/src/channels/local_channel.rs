// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use crate::{channels::ChannelCapacity, GlommioError, ResourceType};
use futures_lite::{stream::Stream, Future};
use std::{
    cell::RefCell,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};

use intrusive_collections::{
    container_of,
    linked_list::LinkOps,
    offset_of,
    Adapter,
    LinkedList,
    LinkedListLink,
    PointerOps,
};

use std::{collections::VecDeque, marker::PhantomPinned, ptr::NonNull};

#[derive(Debug)]
/// Send endpoint to the `local_channel`
pub struct LocalSender<T> {
    channel: LocalChannel<T>,
}

#[derive(Debug)]
/// Receive endpoint to the `local_channel`
///
/// The `LocalReceiver` provides an interface compatible with [`StreamExt`] and
/// will keep yielding elements until the sender is destroyed.
///
/// # Examples
///
/// ```
/// use futures_lite::StreamExt;
/// use glommio::{channels::local_channel, LocalExecutor};
///
/// let ex = LocalExecutor::default();
/// ex.run(async move {
///     let (sender, mut receiver) = local_channel::new_unbounded();
///
///     let h = glommio::spawn_local(async move {
///         let sum = receiver.stream().fold(0, |acc, x| acc + x).await;
///         assert_eq!(sum, 45);
///     })
///     .detach();
///
///     for i in 0..10 {
///         sender.try_send(i);
///     }
///     drop(sender);
///     h.await;
/// });
/// ```
///
/// [`StreamExt`]: https://docs.rs/futures/0.3.6/futures/stream/trait.StreamExt.html
pub struct LocalReceiver<T> {
    channel: LocalChannel<T>,
    node: WaiterNode,
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
enum WaiterKind {
    Sender,
    Receiver,
}

#[derive(Debug)]
struct Waiter<'a, T, F> {
    node: WaiterNode,
    channel: &'a LocalChannel<T>,

    poll_fn: F,
}

#[derive(Debug)]
enum PollResult<T> {
    Pending(WaiterKind),
    Ready(T),
}

impl<'a, T, F, R> Waiter<'a, T, F>
where
    F: FnMut() -> PollResult<R>,
{
    fn new(poll_fn: F, channel: &'a LocalChannel<T>) -> Self {
        Waiter {
            poll_fn,
            channel,

            node: WaiterNode {
                waker: RefCell::new(None),
                link: LinkedListLink::new(),
                kind: RefCell::new(None),

                _p: PhantomPinned,
            },
        }
    }
}

impl<'a, T, F, R> Future for Waiter<'a, T, F>
where
    F: FnMut() -> PollResult<R>,
{
    type Output = R;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let future_mut = unsafe { self.get_unchecked_mut() };
        let pinned_node = unsafe { Pin::new_unchecked(&mut future_mut.node) };

        let result = (future_mut.poll_fn)();
        match result {
            PollResult::Pending(kind) => {
                *pinned_node.kind.borrow_mut() = Some(kind);
                *pinned_node.waker.borrow_mut() = Some(cx.waker().clone());

                register_into_waiting_queue(
                    pinned_node,
                    &mut future_mut.channel.state.borrow_mut(),
                );
                Poll::Pending
            }
            PollResult::Ready(result) => {
                remove_from_the_waiting_queue(
                    pinned_node,
                    &mut future_mut.channel.state.borrow_mut(),
                );
                Poll::Ready(result)
            }
        }
    }
}

fn register_into_waiting_queue<T>(node: Pin<&mut WaiterNode>, state: &mut State<T>) {
    if node.link.is_linked() {
        return;
    }

    let kind = node.kind.borrow().expect("Unknown part of the channel");
    match kind {
        WaiterKind::Sender => {
            state
                .send_waiters
                .as_mut()
                .expect("There should be active sender instance for the channel")
                //it is safe to use unchecked call here because we convert from reference
                .push_back(unsafe { NonNull::new_unchecked(node.get_unchecked_mut()) });
        }
        WaiterKind::Receiver => {
            state
                .recv_waiters
                .as_mut()
                .expect("There should be active receiver instance for the channel")
                //it is safe to use unchecked call here because we convert from reference
                .push_back(unsafe { NonNull::new_unchecked(node.get_unchecked_mut()) });
        }
    }
}

fn remove_from_the_waiting_queue<T>(node: Pin<&mut WaiterNode>, state: &mut State<T>) {
    if !node.link.is_linked() {
        return;
    }

    let kind = node.kind.borrow().expect("Unknown part of the channel");
    let mut cursor = match kind {
        WaiterKind::Sender => unsafe {
            state
                .send_waiters
                .as_mut()
                .expect("There should be active sender instance for the channel")
                .cursor_mut_from_ptr(node.get_unchecked_mut())
        },
        WaiterKind::Receiver => unsafe {
            state
                .recv_waiters
                .as_mut()
                .expect("There should be active receiver instance for the channel")
                .cursor_mut_from_ptr(node.get_unchecked_mut())
        },
    };

    cursor
        .remove()
        .expect("Future has to be queue into the waiting queue");
}

impl<'a, T, F> Drop for Waiter<'a, T, F> {
    fn drop(&mut self) {
        if self.node.link.is_linked() {
            //if future is linked into the waiting queue then it is already pinned
            let pinned_node = unsafe { Pin::new_unchecked(&mut self.node) };
            let kind = pinned_node
                .kind
                .borrow()
                .expect("If Future is queued type of the queue has to be specified");

            let mut state = self.channel.state.borrow_mut();
            let waiters = match kind {
                WaiterKind::Sender => state
                    .send_waiters
                    .as_mut()
                    .expect("Waiting queue of senders can not be empty"),
                WaiterKind::Receiver => state
                    .recv_waiters
                    .as_mut()
                    .expect("Waiting queue of receivers can not be empty"),
            };

            let mut cursor =
                unsafe { waiters.cursor_mut_from_ptr(pinned_node.get_unchecked_mut()) };
            cursor
                .remove()
                .expect("Future has to be linked into the waiting queue");
        }
    }
}

#[derive(Debug)]
struct WaiterNode {
    waker: RefCell<Option<Waker>>,
    link: LinkedListLink,
    kind: RefCell<Option<WaiterKind>>,

    _p: PhantomPinned,
}

struct WaiterPointerOps;

unsafe impl PointerOps for WaiterPointerOps {
    type Value = WaiterNode;
    type Pointer = NonNull<WaiterNode>;

    unsafe fn from_raw(&self, value: *const Self::Value) -> Self::Pointer {
        NonNull::new(value as *mut Self::Value).expect("Pointer to the value can not be null")
    }

    fn into_raw(&self, ptr: Self::Pointer) -> *const Self::Value {
        ptr.as_ptr() as *const Self::Value
    }
}

struct WaiterAdapter {
    pointers_ops: WaiterPointerOps,
    link_ops: LinkOps,
}

impl WaiterAdapter {
    pub const NEW: Self = WaiterAdapter {
        pointers_ops: WaiterPointerOps,
        link_ops: LinkOps,
    };
}

unsafe impl Adapter for WaiterAdapter {
    type LinkOps = LinkOps;
    type PointerOps = WaiterPointerOps;

    unsafe fn get_value(
        &self,
        link: <Self::LinkOps as intrusive_collections::LinkOps>::LinkPtr,
    ) -> *const <Self::PointerOps as PointerOps>::Value {
        container_of!(link.as_ptr(), WaiterNode, link)
    }

    unsafe fn get_link(
        &self,
        value: *const <Self::PointerOps as PointerOps>::Value,
    ) -> <Self::LinkOps as intrusive_collections::LinkOps>::LinkPtr {
        if value.is_null() {
            panic!("Value pointer can not be null");
        }
        let ptr = (value as *const u8).add(offset_of!(WaiterNode, link));
        core::ptr::NonNull::new_unchecked(ptr as *mut _)
    }

    fn link_ops(&self) -> &Self::LinkOps {
        &self.link_ops
    }

    fn link_ops_mut(&mut self) -> &mut Self::LinkOps {
        &mut self.link_ops
    }

    fn pointer_ops(&self) -> &Self::PointerOps {
        &self.pointers_ops
    }
}

#[derive(Debug)]
struct State<T> {
    capacity: ChannelCapacity,
    channel: VecDeque<T>,
    recv_waiters: Option<LinkedList<WaiterAdapter>>,
    send_waiters: Option<LinkedList<WaiterAdapter>>,
}

impl<T> State<T> {
    fn push(&mut self, item: T) -> Result<Option<Waker>, GlommioError<T>> {
        if self.recv_waiters.is_none() {
            Err(GlommioError::Closed(ResourceType::Channel(item)))
        } else if self.is_full() {
            Err(GlommioError::WouldBlock(ResourceType::Channel(item)))
        } else {
            self.channel.push_back(item);

            Ok(self.recv_waiters.as_mut().and_then(|x| {
                x.pop_front().map(|n| {
                    unsafe { n.as_ref() }
                        .waker
                        .borrow_mut()
                        .take()
                        .expect("Future was added to the waiting queue without a waker")
                })
            }))
        }
    }

    fn is_full(&self) -> bool {
        match self.capacity {
            ChannelCapacity::Unbounded => false,
            ChannelCapacity::Bounded(x) => self.channel.len() >= x,
        }
    }

    fn wait_for_room(&mut self) -> PollResult<()> {
        if !self.is_full() {
            PollResult::Ready(())
        } else {
            PollResult::Pending(WaiterKind::Sender)
        }
    }

    fn recv_one(&mut self) -> PollResult<Option<(T, Option<Waker>)>> {
        match self.channel.pop_front() {
            Some(item) => PollResult::Ready(Some((
                item,
                self.send_waiters.as_mut().and_then(|x| {
                    x.pop_front()
                        .and_then(|node| unsafe { node.as_ref() }.waker.borrow_mut().take())
                }),
            ))),
            None => {
                if self.send_waiters.is_some() {
                    PollResult::Pending(WaiterKind::Receiver)
                } else {
                    PollResult::Ready(None)
                }
            }
        }
    }
}

#[derive(Debug)]
struct LocalChannel<T> {
    state: Rc<RefCell<State<T>>>,
}

impl<T> Clone for LocalChannel<T> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

impl<T> Drop for LocalChannel<T> {
    fn drop(&mut self) {
        assert!(
            self.state.borrow().recv_waiters.is_none()
                || self
                    .state
                    .borrow()
                    .recv_waiters
                    .as_ref()
                    .unwrap()
                    .is_empty()
        );
        assert!(
            self.state.borrow().send_waiters.is_none()
                || self
                    .state
                    .borrow()
                    .send_waiters
                    .as_ref()
                    .unwrap()
                    .is_empty()
        );
    }
}

impl<T> LocalChannel<T> {
    #[allow(clippy::new_ret_no_self)]
    fn new(capacity: ChannelCapacity) -> (LocalSender<T>, LocalReceiver<T>) {
        let channel = match capacity {
            ChannelCapacity::Unbounded => VecDeque::new(),
            ChannelCapacity::Bounded(x) => VecDeque::with_capacity(x),
        };

        let channel = LocalChannel {
            state: Rc::new(RefCell::new(State {
                capacity,
                channel,
                send_waiters: Some(LinkedList::new(WaiterAdapter::NEW)),
                recv_waiters: Some(LinkedList::new(WaiterAdapter::NEW)),
            })),
        };

        (
            LocalSender {
                channel: channel.clone(),
            },
            LocalReceiver {
                channel,
                node: WaiterNode {
                    waker: RefCell::new(None),
                    link: LinkedListLink::new(),
                    kind: RefCell::new(None),

                    _p: PhantomPinned,
                },
            },
        )
    }
}

/// Creates a new `local_channel` with unbounded capacity
///
/// # Examples
/// ```
/// use futures_lite::StreamExt;
/// use glommio::{channels::local_channel, LocalExecutor};
///
/// let ex = LocalExecutor::default();
/// ex.run(async move {
///     let (sender, receiver) = local_channel::new_unbounded();
///     let h = glommio::spawn_local(async move {
///         assert_eq!(receiver.stream().next().await.unwrap(), 0);
///     })
///     .detach();
///     sender.try_send(0);
///     drop(sender);
///     h.await;
/// });
/// ```
pub fn new_unbounded<T>() -> (LocalSender<T>, LocalReceiver<T>) {
    LocalChannel::new(ChannelCapacity::Unbounded)
}

/// Creates a new `local_channel` with capacity limited to the `size` argument
///
/// # Examples
/// ```
/// use futures_lite::StreamExt;
/// use glommio::{channels::local_channel, LocalExecutor};
///
/// let ex = LocalExecutor::default();
/// ex.run(async move {
///     let (sender, receiver) = local_channel::new_bounded(1);
///     assert_eq!(sender.is_full(), false);
///     sender.try_send(0);
///     assert_eq!(sender.is_full(), true);
///     receiver.stream().next().await.unwrap();
///     assert_eq!(sender.is_full(), false);
/// });
/// ```
pub fn new_bounded<T>(size: usize) -> (LocalSender<T>, LocalReceiver<T>) {
    LocalChannel::new(ChannelCapacity::Bounded(size))
}

#[allow(clippy::len_without_is_empty)]
impl<T> LocalSender<T> {
    /// Sends data into this channel.
    ///
    /// It returns a [`GlommioError::Closed`] encapsulating a [`BrokenPipe`] if
    /// the receiver is destroyed. It returns a [`GlommioError::WouldBlock`]
    /// encapsulating a [`WouldBlock`] if this is a bounded channel that has no
    /// more capacity
    ///
    /// # Examples
    /// ```
    /// use futures_lite::StreamExt;
    /// use glommio::{channels::local_channel, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let (sender, receiver) = local_channel::new_bounded(1);
    ///     sender.try_send(0);
    ///     sender.try_send(0).unwrap_err(); // no more capacity
    ///     receiver.stream().next().await.unwrap(); // now we have capacity again
    ///     drop(receiver); // but because the receiver is destroyed send will err
    ///     sender.try_send(0).unwrap_err();
    /// });
    /// ```
    ///
    /// [`BrokenPipe`]: https://doc.rust-lang.org/std/io/enum.ErrorKind.html#variant.BrokenPipe
    /// [`WouldBlock`]: https://doc.rust-lang.org/std/io/enum.ErrorKind.html#variant.WouldBlock
    /// [`Other`]: https://doc.rust-lang.org/std/io/enum.ErrorKind.html#variant.Other
    /// [`io::Error`]: https://doc.rust-lang.org/std/io/struct.Error.html
    pub fn try_send(&self, item: T) -> Result<(), GlommioError<T>> {
        if let Some(w) = self.channel.state.borrow_mut().push(item)? {
            w.wake();
        }
        Ok(())
    }

    /// Sends data into this channel when it is ready to receive it
    ///
    /// For an unbounded channel this is just a more expensive version of
    /// [`try_send`]. Prefer to use [`try_send`] instead.
    ///
    /// For a bounded channel this will push to the channel when the channel is
    /// ready to receive data.
    ///
    /// # Examples
    /// ```
    /// use glommio::{channels::local_channel, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let (sender, receiver) = local_channel::new_bounded(1);
    ///     sender.send(0).await.unwrap();
    ///     drop(receiver);
    /// });
    /// ```
    ///
    /// [`try_send`]: struct.LocalSender.html#method.try_send
    pub async fn send(&self, item: T) -> Result<(), GlommioError<T>> {
        Waiter::new(|| self.wait_for_room(), &self.channel).await;
        self.try_send(item)
    }

    /// Checks if there is room to send data in this channel
    ///
    /// # Examples
    /// ```
    /// use glommio::{channels::local_channel, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let (sender, receiver) = local_channel::new_bounded(1);
    ///     assert_eq!(sender.is_full(), false);
    ///     sender.try_send(0);
    ///     assert_eq!(sender.is_full(), true);
    ///     drop(receiver);
    /// });
    /// ```
    #[inline]
    pub fn is_full(&self) -> bool {
        self.channel.state.borrow().is_full()
    }

    /// Returns the number of items still queued in this channel
    ///
    /// # Examples
    /// ```
    /// use futures_lite::StreamExt;
    /// use glommio::{channels::local_channel, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let (sender, mut receiver) = local_channel::new_unbounded();
    ///     sender.try_send(0);
    ///     sender.try_send(0);
    ///     assert_eq!(sender.len(), 2);
    ///     receiver.stream().next().await.unwrap();
    ///     assert_eq!(sender.len(), 1);
    ///     drop(receiver);
    /// });
    /// ```
    #[inline]
    pub fn len(&self) -> usize {
        self.channel.state.borrow().channel.len()
    }

    fn wait_for_room(&self) -> PollResult<()> {
        // NOTE: it is important that the borrow is dropped
        // if Poll::Pending is returned
        self.channel.state.borrow_mut().wait_for_room()
    }
}

fn wake_up_all(ws: &mut Option<LinkedList<WaiterAdapter>>) {
    if let Some(ref mut waiters) = ws {
        // we assume here that wakes don't try to acquire a borrow on
        // the channel.state
        let mut cursor = waiters.front_mut();
        while !cursor.is_null() {
            {
                let node = unsafe {
                    Pin::new_unchecked(cursor.get().expect("Waiter queue can not be empty"))
                };
                node.waker
                    .borrow_mut()
                    .take()
                    .expect("Future can not be queued without waker")
                    .wake();
            }

            cursor.remove().expect("Waiter queue can not be empty");
        }
    }
}

impl<T> Drop for LocalSender<T> {
    fn drop(&mut self) {
        let mut state = self.channel.state.borrow_mut();
        // Will not wake up senders, but we are dropping the sender so nobody
        // wants to wake up anyway.
        state.send_waiters.take();
        wake_up_all(&mut state.recv_waiters);
    }
}

impl<T> Drop for LocalReceiver<T> {
    fn drop(&mut self) {
        let mut state = self.channel.state.borrow_mut();
        state.recv_waiters.take();
        wake_up_all(&mut state.send_waiters);
    }
}

struct ChannelStream<'a, T> {
    channel: &'a LocalChannel<T>,
    //if do not use Box stream becomes !Unpin which will force users to pin it
    //while using StreamExt which is not user friendly. Creation of stream is relatively
    //rare operation and happens once during the routine so we can afford to perform
    //memory allocation here
    node: Pin<Box<WaiterNode>>,
}

impl<'a, T> ChannelStream<'a, T> {
    fn new(channel: &'a LocalChannel<T>) -> Self {
        ChannelStream {
            channel,
            node: Box::pin(WaiterNode {
                waker: RefCell::new(None),
                link: LinkedListLink::new(),
                kind: RefCell::new(None),

                _p: PhantomPinned,
            }),
        }
    }
}

impl<'a, T> Stream for ChannelStream<'a, T> {
    type Item = T;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = self.channel.state.borrow_mut().recv_one();

        let this = unsafe { self.get_unchecked_mut() };

        match result {
            PollResult::Pending(kind) => {
                *this.node.waker.borrow_mut() = Some(cx.waker().clone());
                *this.node.kind.borrow_mut() = Some(kind);

                register_into_waiting_queue(
                    this.node.as_mut(),
                    &mut this.channel.state.borrow_mut(),
                );

                Poll::Pending
            }
            PollResult::Ready(result) => {
                remove_from_the_waiting_queue(
                    this.node.as_mut(),
                    &mut this.channel.state.borrow_mut(),
                );

                Poll::Ready(result.map(|(ret, mw)| {
                    if let Some(waker) = mw {
                        waker.wake();
                    }

                    ret
                }))
            }
        }
    }
}

impl<'a, T> Drop for ChannelStream<'a, T> {
    fn drop(&mut self) {
        remove_from_the_waiting_queue(self.node.as_mut(), &mut self.channel.state.borrow_mut());
    }
}

impl<T> LocalReceiver<T> {
    /// Receives data from this channel
    ///
    /// If the sender is no longer available it returns [`None`]. Otherwise,
    /// block until an item is available and returns it wrapped in [`Some`]
    ///
    /// Notice that this is also available as a Stream. Whether to consume from
    /// a stream or `recv` is up to the application. The biggest difference
    /// is that [`StreamExt`]'s [`next`] method takes a mutable reference to
    /// self. If the LocalReceiver is, say, behind an [`Rc`] it may be more
    /// ergonomic to `recv`.
    ///
    /// # Examples
    /// ```
    /// use glommio::{channels::local_channel, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let (sender, receiver) = local_channel::new_bounded(1);
    ///     sender.send(0).await.unwrap();
    ///     let x = receiver.recv().await.unwrap();
    ///     assert_eq!(x, 0);
    /// });
    /// ```
    ///
    /// [`None`]: https://doc.rust-lang.org/std/option/enum.Option.html#variant.None
    /// [`Some`]: https://doc.rust-lang.org/std/option/enum.Option.html#variant.Some
    /// [`StreamExt`]: https://docs.rs/futures-lite/1.11.2/futures_lite/stream/index.html
    /// [`next`]: https://docs.rs/futures-lite/1.11.2/futures_lite/stream/trait.StreamExt.html#method.next
    /// [`Rc`]: https://doc.rust-lang.org/std/rc/struct.Rc.html
    pub async fn recv(&self) -> Option<T> {
        Waiter::new(|| self.recv_one(), &self.channel).await
    }

    /// Converts receiver into the ['Stream'] instance.
    /// Each ['Stream'] instance may handle only single receiver and can not be
    /// shared between ['Tasks'].
    pub fn stream(&self) -> impl Stream<Item = T> + '_ {
        ChannelStream::new(&self.channel)
    }

    fn recv_one(&self) -> PollResult<Option<T>> {
        let result = self.channel.state.borrow_mut().recv_one();
        match result {
            PollResult::Pending(kind) => PollResult::Pending(kind),
            PollResult::Ready(opt) => PollResult::Ready(opt.map(|(ret, mw)| {
                if let Some(w) = mw {
                    w.wake();
                }

                ret
            })),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::enclose;
    use futures_lite::stream::{self, StreamExt};

    #[test]
    fn producer_consumer() {
        test_executor!(async move {
            let (sender, receiver) = new_unbounded();

            let handle = crate::spawn_local(async move {
                let sum = receiver.stream().fold(0, |acc, x| acc + x).await;
                assert_eq!(sum, 10);
            })
            .detach();

            for _ in 0..10 {
                sender.try_send(1).unwrap();
            }
            drop(sender);
            handle.await;
        });
    }

    #[test]
    fn producer_parallel_consumer() {
        use futures::{future, stream::StreamExt};

        test_executor!(async move {
            let (sender, receiver) = new_unbounded();

            let handle = crate::spawn_local(async move {
                let mut sum = 0;
                receiver
                    .stream()
                    .for_each_concurrent(1000, |x| {
                        sum += x;
                        future::ready(())
                    })
                    .await;
                assert_eq!(sum, 10);
            })
            .detach();

            for _ in 0..10 {
                sender.try_send(1).unwrap();
            }
            drop(sender);

            handle.await;
        });
    }

    #[test]
    fn producer_receiver_serialized() {
        test_executor!(async move {
            let (sender, receiver) = new_unbounded();

            for _ in 0..10 {
                sender.try_send(1).unwrap();
            }
            drop(sender);

            let handle = crate::spawn_local(async move {
                let sum = receiver.stream().fold(0, |acc, x| acc + x).await;
                assert_eq!(sum, 10);
            })
            .detach();

            handle.await;
        });
    }

    #[test]
    fn producer_early_drop_receiver() {
        test_executor!(async move {
            let (sender, receiver) = new_unbounded();

            let handle = crate::spawn_local(async move {
                let sum = receiver.stream().take(3).fold(0, |acc, x| acc + x).await;
                assert_eq!(sum, 3);
            })
            .detach();

            loop {
                match sender.try_send(1) {
                    Ok(_) => crate::executor().yield_task_queue_now().await,
                    err => {
                        matches!(err, Err(GlommioError::WouldBlock(ResourceType::Channel(1))));
                        break;
                    }
                }
            }
            handle.await;
        });
    }

    #[test]
    fn producer_bounded_early_error() {
        test_executor!(async move {
            let (sender, receiver) = new_bounded(1);
            sender.try_send(0).unwrap();
            sender.try_send(0).unwrap_err();
            drop(receiver);
        });
    }

    #[test]
    fn producer_bounded_has_capacity() {
        test_executor!(async move {
            let (sender, receiver) = new_bounded(1);
            assert!(!sender.is_full());
            sender.try_send(0).unwrap();
            assert!(sender.is_full());
            drop(receiver);
        });
    }

    #[test]
    fn producer_bounded_ping_pong() {
        test_executor!(async move {
            let (sender, receiver) = new_bounded(1);

            let handle = crate::spawn_local(async move {
                let sum = receiver.stream().fold(0, |acc, x| acc + x).await;
                assert_eq!(sum, 10);
            })
            .detach();

            for _ in 0..10 {
                sender.send(1).await.unwrap();
            }
            drop(sender);
            handle.await;
        });
    }

    #[test]
    fn producer_bounded_previously_blocked_still_errors_out() {
        test_executor!(async move {
            let (sender, receiver) = new_bounded(1);

            let s = crate::spawn_local(async move {
                sender.try_send(0).unwrap();
                sender.send(0).await.unwrap_err();
            })
            .detach();

            let r = crate::spawn_local(async move {
                receiver.stream().next().await.unwrap();
                drop(receiver);
            })
            .detach();
            r.await;
            s.await;
        });
    }

    #[test]
    fn non_stream_receiver() {
        test_executor!(async move {
            let (sender, receiver) = new_bounded(1);

            let s = crate::spawn_local(async move {
                sender.try_send(0).unwrap();
                sender.send(0).await.unwrap_err();
            })
            .detach();

            let r = crate::spawn_local(async move {
                receiver.recv().await.unwrap();
                drop(receiver);
            })
            .detach();
            r.await;
            s.await;
        });
    }

    #[test]
    fn multiple_task_receivers() {
        test_executor!(async move {
            let (sender, receiver) = new_bounded(1);
            let receiver = Rc::new(receiver);

            let mut ret = Vec::new();
            for _ in 0..10 {
                ret.push(crate::spawn_local(enclose! {(receiver) async move {
                    receiver.recv().await.unwrap();
                }}));
            }

            for _ in 0..10 {
                sender.send(0).await.unwrap();
            }

            let recvd = stream::iter(ret).then(|f| f).count().await;
            assert_eq!(recvd, 10);
        });
    }
}
