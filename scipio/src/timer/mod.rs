// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
//! scipio::timer is a module that provides timing related primitives.
mod timer_impl;

pub use timer_impl::{Timer, TimerActionOnce, TimerActionRepeat};
