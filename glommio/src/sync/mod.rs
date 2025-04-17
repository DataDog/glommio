// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience

//! Set of synchronization primitives.
//!
//! This crate provides a set of synchronization primitives which are optimized
//! to be used inside of fibers which are driven by single-thread bounded
//! executor.
//!
//! Following primitives are provided.
//!
//! 1. Semaphore - A counting semaphore. Semaphore maintains a set of permits.
//!    Each call to ['acquire_permit'] suspends fiber if necessary until a permit
//!    is available, and then takes it.
//!    Each call to ['signal'] adds a permit, potentially releasing a suspended
//!    acquirer. There is also ['try_acquire'] method which fails if semaphore
//!    lacks of permits requested without suspending the fiber.
//!
//! 2. RwLock - Implementation of read-write lock optimized for single-thread
//!    bounded executor. All methods of RwLock have the same meaning as the methods
//!    of [`std::sync::RwLock`]. With exception that RwLock can not be poisoned but
//!    can be closed.

mod gate;
mod rwlock;
mod semaphore;

pub use self::{gate::*, rwlock::*, semaphore::*};
