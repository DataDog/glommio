use std::cell::Cell;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

const CACHELINE_LEN: usize = 64;

const fn cacheline_pad(used: usize) -> usize {
    CACHELINE_LEN / std::mem::size_of::<usize>() - used
}

/// The internal memory buffer used by the queue.
///
/// Buffer holds a pointer to allocated memory which represents the bounded
/// ring buffer, as well as a head and tail atomicUsize which the producer and consumer
/// use to track location in the ring.
#[repr(C)]
pub(crate) struct Buffer<T: Copy> {
    buffer_storage: Arc<Vec<Cell<T>>>,

    /// The bounded size as specified by the user.  If the queue reaches capacity, it will block
    /// until values are poppped off.
    capacity: usize,

    /// The allocated size of the ring buffer, in terms of number of values (not physical memory).
    /// This will be the next power of two larger than `capacity`
    allocated_size: usize,
    _padding1: [usize; cacheline_pad(3)],

    /// Consumer cacheline:

    /// Index position of the current head
    head: AtomicUsize,
    shadow_tail: Cell<usize>,
    /// Id == 0 : never connected
    /// Id == usize::MAX: disconnected
    producer_id: AtomicUsize,
    _padding2: [usize; cacheline_pad(3)],

    /// Producer cacheline:

    /// Index position of current tail
    tail: AtomicUsize,
    shadow_head: Cell<usize>,
    /// Id == 0 : never connected
    /// Id == usize::MAX: disconnected
    consumer_id: AtomicUsize,
    _padding3: [usize; cacheline_pad(3)],
}

impl<T: Copy> fmt::Debug for Buffer<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Relaxed);
        let shead = self.shadow_head.get();
        let stail = self.shadow_tail.get();
        let id_to_str = |id| match id {
            0 => "not connected".into(),
            usize::MAX => "disconnected".into(),
            x => format!("{}", x),
        };

        let consumer_id = id_to_str(self.consumer_id.load(Ordering::Relaxed));
        let producer_id = id_to_str(self.producer_id.load(Ordering::Relaxed));

        f.debug_struct("SPSC Buffer")
            .field("capacity:", &self.capacity)
            .field("allocated_size:", &self.allocated_size)
            .field("consumer_head:", &head)
            .field("shadow_head:", &shead)
            .field("producer_tail:", &tail)
            .field("shadow_tail:", &stail)
            .field("consumer_id:", &consumer_id)
            .field("producer_id:", &producer_id)
            .finish()
    }
}

unsafe impl<T: Sync + Copy> Sync for Buffer<T> {}

/// A handle to the queue which allows consuming values from the buffer
#[derive(Clone)]
pub(crate) struct Consumer<T: Copy> {
    pub(crate) buffer: Arc<Buffer<T>>,
}

/// A handle to the queue which allows adding values onto the buffer
#[derive(Clone)]
pub(crate) struct Producer<T: Copy> {
    pub(crate) buffer: Arc<Buffer<T>>,
}

impl<T: Copy> fmt::Debug for Consumer<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Consumer {:?}", self.buffer)
    }
}

impl<T: Copy> fmt::Debug for Producer<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Producer {:?}", self.buffer)
    }
}

unsafe impl<T: Send + Copy> Send for Consumer<T> {}
unsafe impl<T: Send + Copy> Send for Producer<T> {}

impl<T: Copy> Buffer<T> {
    /// Attempt to pop a value off the buffer.
    ///
    /// If the buffer is empty, this method will not block.  Instead, it will return `None`
    /// signifying the buffer was empty.  The caller may then decide what to do next (e.g. spin-wait,
    /// sleep, process something else, etc)
    fn try_pop(&self) -> Option<T> {
        let current_head = self.head.load(Ordering::Relaxed);

        if current_head == self.shadow_tail.get() {
            self.shadow_tail.set(self.tail.load(Ordering::Acquire));
            if current_head == self.shadow_tail.get() {
                return None;
            }
        }

        let index = current_head & (self.allocated_size - 1);
        let v = self.buffer_storage[index].get();
        self.head
            .store(current_head.wrapping_add(1), Ordering::Release);
        Some(v)
    }

    /// Attempt to push a value onto the buffer.
    ///
    /// If the buffer is full, this method will not block.  Instead, it will return `Some(v)`, where
    /// `v` was the value attempting to be pushed onto the buffer.  If the value was successfully
    /// pushed onto the buffer, `None` will be returned signifying success.
    fn try_push(&self, v: T) -> Option<T> {
        if self.consumer_disconnected() {
            return Some(v);
        }
        let current_tail = self.tail.load(Ordering::Relaxed);

        if self.shadow_head.get() + self.capacity <= current_tail {
            self.shadow_head.set(self.head.load(Ordering::Relaxed));
            if self.shadow_head.get() + self.capacity <= current_tail {
                return Some(v);
            }
        }

        let index = current_tail & (self.allocated_size - 1);
        self.buffer_storage[index].set(v);
        self.tail
            .store(current_tail.wrapping_add(1), Ordering::Release);
        None
    }

    /// Disconnects the consumer, and returns whether or not it was already disconnected
    pub(crate) fn disconnect_consumer(&self) -> bool {
        self.consumer_id.swap(usize::MAX, Ordering::Release) == usize::MAX
    }

    /// Disconnects the consumer, and returns whether or not it was already disconnected
    pub(crate) fn disconnect_producer(&self) -> bool {
        self.producer_id.swap(usize::MAX, Ordering::Release) == usize::MAX
    }

    /// Disconnects the consumer, and returns whether or not it was already disconnected
    pub(crate) fn producer_disconnected(&self) -> bool {
        self.producer_id.load(Ordering::Acquire) == usize::MAX
    }

    /// Disconnects the consumer, and returns whether or not it was already disconnected
    pub(crate) fn consumer_disconnected(&self) -> bool {
        self.consumer_id.load(Ordering::Acquire) == usize::MAX
    }

    /// Returns the current size of the queue
    ///
    /// This value represents the current size of the queue.  This value can be from 0-`capacity`
    /// inclusive.
    pub(crate) fn size(&self) -> usize {
        self.tail.load(Ordering::Acquire) - self.head.load(Ordering::Acquire)
    }
}

/// Handles deallocation of heap memory when the buffer is dropped
impl<T: Copy> Drop for Buffer<T> {
    fn drop(&mut self) {
        // Pop the rest of the values off the queue.  By moving them into this scope,
        // we implicitly call their destructor
        while self.try_pop().is_some() {}
        // We don't want to run any destructors here, because we didn't run
        // any of the constructors through the vector. And whatever object was
        // in fact still alive we popped above.
        unsafe {
            match Arc::get_mut(&mut self.buffer_storage) {
                Some(storage) => storage.set_len(0),
                None => unreachable!(),
            }
        }
    }
}

pub(crate) fn make<T: Copy>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    let buffer_storage = allocate_buffer(capacity);

    let arc = Arc::new(Buffer {
        buffer_storage,
        capacity,
        allocated_size: capacity.next_power_of_two(),
        _padding1: [0; cacheline_pad(3)],
        _padding2: [0; cacheline_pad(3)],
        _padding3: [0; cacheline_pad(3)],

        head: AtomicUsize::new(0),
        shadow_tail: Cell::new(0),
        producer_id: AtomicUsize::new(0),

        tail: AtomicUsize::new(0),
        shadow_head: Cell::new(0),
        consumer_id: AtomicUsize::new(0),
    });

    (
        Producer {
            buffer: arc.clone(),
        },
        Consumer { buffer: arc },
    )
}

fn allocate_buffer<T: Copy>(capacity: usize) -> Arc<Vec<Cell<T>>> {
    let size = capacity.next_power_of_two();
    let mut vec = Vec::with_capacity(size);
    unsafe {
        vec.set_len(size);
    }
    Arc::new(vec)
}

pub(crate) trait BufferHalf {
    type Item: Copy;

    fn buffer(&self) -> &Buffer<Self::Item>;
    fn connect(&self, id: usize);
    fn peer_id(&self) -> usize;

    /// Returns the total capacity of this queue
    ///
    /// This value represents the total capacity of the queue when it is full.  It does not
    /// represent the current usage.  For that, call `size()`.
    fn capacity(&self) -> usize {
        self.buffer().capacity
    }

    /// Returns the current size of the queue
    ///
    /// This value represents the current size of the queue.  This value can be from 0-`capacity`
    /// inclusive.
    fn size(&self) -> usize {
        self.buffer().size()
    }
}

impl<T: Copy> BufferHalf for Producer<T> {
    type Item = T;
    fn buffer(&self) -> &Buffer<T> {
        &*self.buffer
    }

    fn connect(&self, id: usize) {
        assert_ne!(id, 0);
        assert_ne!(id, usize::MAX);
        (*self.buffer).producer_id.store(id, Ordering::Relaxed);
    }

    fn peer_id(&self) -> usize {
        (*self.buffer).consumer_id.load(Ordering::Relaxed)
    }
}

impl<T: Copy> Producer<T> {
    /// Attempt to push a value onto the buffer.
    ///
    /// This method does not block.  If the queue is not full, the value will be added to the
    /// queue and the method will return `None`, signifying success.  If the queue is full,
    /// this method will return `Some(v)``, where `v` is your original value.
    pub(crate) fn try_push(&self, v: T) -> Option<T> {
        (*self.buffer).try_push(v)
    }

    /// Disconnects the producer, signaling to the consumer that no new values are going to be
    /// produced.
    ///
    /// Returns the buffer status before the disconnect
    pub(crate) fn disconnect(&self) -> bool {
        (*self.buffer).disconnect_producer()
    }

    pub(crate) fn consumer_disconnected(&self) -> bool {
        (*self.buffer).consumer_disconnected()
    }

    /// Returns the available space in the queue
    ///
    /// This value represents the number of items that can be pushed onto the queue before it
    /// becomes full.
    pub(crate) fn free_space(&self) -> usize {
        self.capacity() - self.size()
    }
}

impl<T: Copy> BufferHalf for Consumer<T> {
    type Item = T;
    fn buffer(&self) -> &Buffer<T> {
        &(*self.buffer)
    }

    fn connect(&self, id: usize) {
        assert_ne!(id, usize::MAX);
        assert_ne!(id, 0);
        (*self.buffer).consumer_id.store(id, Ordering::Relaxed);
    }

    fn peer_id(&self) -> usize {
        (*self.buffer).producer_id.load(Ordering::Relaxed)
    }
}

impl<T: Copy> Consumer<T> {
    /// Disconnects the consumer, signaling to the producer that no new values are going to be
    /// consumed. After this is done, any attempt on the producer to try_push should fail
    ///
    /// Returns the buffer status before the disconnect
    pub(crate) fn disconnect(&self) -> bool {
        (*self.buffer).disconnect_consumer()
    }

    pub(crate) fn producer_disconnected(&self) -> bool {
        (*self.buffer).producer_disconnected()
    }

    /// Attempt to pop a value off the queue.
    ///
    /// This method does not block.  If the queue is empty, the method will return `None`.  If
    /// there is a value available, the method will return `Some(v)`, where `v` is the value
    /// being popped off the queue.
    pub(crate) fn try_pop(&self) -> Option<T> {
        (*self.buffer).try_pop()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_buffer_size() {
        assert_eq!(::std::mem::size_of::<Buffer<()>>(), 3 * CACHELINE_LEN);
    }

    #[test]
    fn test_try_push() {
        let (p, _) = super::make(10);

        for i in 0..10 {
            p.try_push(i);
            assert!(p.capacity() == 10);
            assert!(p.size() == i + 1);
        }

        match p.try_push(10) {
            Some(v) => {
                assert!(v == 10);
            }
            None => assert!(false, "Queue should not have accepted another write!"),
        }
    }

    #[test]
    fn test_try_poll() {
        let (p, c) = super::make(10);

        match c.try_pop() {
            Some(_) => assert!(false, "Queue was empty but a value was read!"),
            None => {}
        }

        p.try_push(123);

        match c.try_pop() {
            Some(v) => assert!(v == 123),
            None => assert!(false, "Queue was not empty but poll() returned nothing!"),
        }

        match c.try_pop() {
            Some(_) => assert!(false, "Queue was empty but a value was read!"),
            None => {}
        }
    }

    #[test]
    fn test_threaded() {
        let (p, c) = super::make(500);

        thread::spawn(move || {
            for i in 0..100000 {
                loop {
                    if let None = p.try_push(i) {
                        break;
                    }
                }
            }
        });

        for i in 0..100000 {
            loop {
                if let Some(t) = c.try_pop() {
                    assert!(t == i);
                    break;
                }
            }
        }
    }
}
