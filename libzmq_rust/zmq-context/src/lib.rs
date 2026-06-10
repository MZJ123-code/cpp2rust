//! # zmq-context
//!
//! Public API for ZeroMQ — the Rust equivalent of `zmq.h`.
//!
//! Provides `ZContext` (global state), `ZSocket` (socket handle),
//! socket options, and monitoring.

#![allow(dead_code)]

pub mod context;
pub mod monitor;
pub mod options;
pub mod socket;

pub use context::ZContext;
pub use socket::ZSocket;
