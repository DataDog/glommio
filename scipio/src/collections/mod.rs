// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
//! scipio::collections is a module that provides scipio compatible collection.
//!
//! # Example
//!
//! Deque:
//!
//! ```
//! use scipio::collections::Deque;
//! use scipio::LocalExecutor;
//!
//! let ad : Deque<usize> = Deque::with_capacity(10);
//! let ex = LocalExecutor::spawn_default();
//!
//! ex.run(async move {
//!     ad.push_front(1);
//!     ad.push_front(2);
//!     let res = ad.pop_front().await.unwrap();
//!     assert_eq!(res, 2);
//! });
//! ```
mod dequeue;

pub use self::dequeue::*;
