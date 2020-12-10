// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
//! This module provide glommio's networking support.
mod tcp_socket;
pub use self::tcp_socket::{AcceptedTcpStream, TcpListener, TcpStream};
