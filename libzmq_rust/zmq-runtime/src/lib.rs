//! # zmq-runtime
//!
//! Async runtime abstraction for ZeroMQ.
//!
//! Provides the I/O event loop (reactor), cross-platform I/O multiplexing
//! (via `mio`), cross-thread signaling, and thread pool management.

#![allow(dead_code)]

pub mod reactor;
pub mod poller;
pub mod signaler;
pub mod thread_pool;
