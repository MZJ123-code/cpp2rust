//! # zmq-core
//!
//! Sans-I/O ZMTP protocol core — pure protocol logic with zero I/O dependencies.
//!
//! This crate implements the ZeroMQ Message Transport Protocol (ZMTP) as a
//! pure state machine. It takes bytes in, produces structured events out.
//! It does NOT perform any network I/O, async operations, or thread management.
//!
//! ## Architecture
//!
//! ```text
//! &[u8] → [ZmqDecoder] → Vec<ZmqEvent>    (protocol parsing)
//! ZmqCommand → [ZmqEncoder] → Vec<u8>      (protocol serialization)
//! ```
//!
//! This design enables:
//! - Reuse across any async runtime (Tokio, async-std, etc.)
//! - Pure functional testing (byte input → expected event sequence)
//! - Fuzz testing without network setup

#![allow(dead_code)]
#![allow(unused_imports)]

pub mod constants;
pub mod error;
pub mod message;
pub mod socket_type;

pub mod codec;
pub mod data_structures;
pub mod security;
pub mod socket;

pub mod engine;
pub mod mailbox;
pub mod pipe;
pub mod session;

// Re-export commonly used types
pub use constants::*;
pub use error::ZmqError;
pub use message::{Payload, ZmqMessage};
pub use socket_type::SocketType;
