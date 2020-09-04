// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use crate::local_semaphore::Semaphore;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::io::Result;

/// The AsyncDeque is similar to the standard library's VecDeque.
///
/// The main difference is that when one pops from this queue (either
/// front or back), the queue will not return an option if it is empty,
/// but rather asynchronously wait for new elements to be produced.
#[derive(Debug)]
pub struct AsyncDeque<T> {
    deque: RefCell<VecDeque<T>>,
    sem: Semaphore,
}

impl<T> AsyncDeque<T> {
    /// Creates a new queue
    pub fn new() -> Self {
        AsyncDeque {
            deque: RefCell::new(VecDeque::new()),
            sem: Semaphore::new(0),
        }
    }

    /// Closes the current queue. All waiters will return Err(), and
    /// no new waiters are accepted.
    pub fn close(&self) {
        self.sem.close();
    }

    /// Creates a new queue with the specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        AsyncDeque {
            deque: RefCell::new(VecDeque::with_capacity(capacity)),
            sem: Semaphore::new(0),
        }
    }

    /// returns the amount of elements currently in the queue
    pub fn len(&self) -> usize {
        self.deque.borrow().len()
    }

    /// pushes an element to the front of the queue
    pub fn push_front(&self, el: T) {
        self.deque.borrow_mut().push_front(el);
        self.sem.signal(1);
    }

    /// pushes an element to the back of the queue
    pub fn push_back(&self, el: T) {
        self.deque.borrow_mut().push_front(el);
        self.sem.signal(1);
    }

    /// Pops the back of the queue.
    ///
    /// If the queue is empty, this blocks until an element can be pop'd.
    ///
    /// This method returns Ok(T) unless the collection is closed.
    pub async fn pop_back(&self) -> Result<T> {
        self.sem.acquire(1).await?;
        Ok(self.deque.borrow_mut().pop_back().unwrap())
    }

    /// Pops the front of the queue.
    ///
    /// If the queue is empty, this blocks until an element can be pop'd.
    ///
    /// This method returns Ok(T) unless the collection is closed.
    pub async fn pop_front(&self) -> Result<T> {
        self.sem.acquire(1).await?;
        Ok(self.deque.borrow_mut().pop_front().unwrap())
    }
}
