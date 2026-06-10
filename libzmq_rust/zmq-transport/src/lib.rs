//! # zmq-transport
//!
//! Transport layer for ZeroMQ — TCP, IPC, and inproc transports.
//!
//! Handles all network I/O using Tokio async I/O. Provides connect/bind/accept
//! primitives for each transport type and integrates with `zmq-core` via
//! the Sans-I/O protocol boundary.

#![allow(dead_code)]

pub mod endpoint;
pub mod inproc;
pub mod ipc;
pub mod tcp;

pub use endpoint::Endpoint;
pub use inproc::InprocRegistry;
