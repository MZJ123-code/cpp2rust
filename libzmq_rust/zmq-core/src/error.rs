//! Error types for zmq-core.
//!
//! `ZmqError` provides detailed, categorised errors for all ZeroMQ operations.
//! Each variant maps to a specific failure domain (protocol, codec, network,
//! security, state), enabling precise error handling and recovery decisions.
//!
//! # Error categories
//!
//! | Variant | Domain | Recoverable |
//! |---------|--------|-------------|
//! | `Protocol` / `Codec` | ZMTP wire protocol | Usually not |
//! | `Network` | Transport layer | Sometimes (retry) |
//! | `Security` | Authentication / encryption | Depends on context |
//! | `InvalidState` | Socket state machine | No (programming error) |
//! | `BufferFull` / `WouldBlock` | Flow control | Yes (retry later) |
//! | `NoPeer` | No connections | Yes (wait for peer) |
//! | `ContextTerminated` | Shutdown | No (cleanup) |

use thiserror::Error;

/// Primary error type for all ZeroMQ operations.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum ZmqError {
    /// Invalid endpoint format
    #[error("invalid endpoint: {0}")]
    InvalidEndpoint(String),

    /// Protocol error (ZMTP wire format violation)
    #[error("protocol error: {0}")]
    Protocol(String),

    /// Codec error (encode/decode failure)
    #[error("codec error: {0}")]
    Codec(String),

    /// Network-level error
    #[error("network error: {0}")]
    Network(String),

    /// Security/authentication error
    #[error("security error: {0}")]
    Security(String),

    /// Buffer full (send would block / HWM reached)
    #[error("buffer full")]
    BufferFull,

    /// Operation would block (non-blocking mode)
    #[error("resource temporarily unavailable")]
    WouldBlock,

    /// No peers connected
    #[error("no peer available")]
    NoPeer,

    /// Socket is in wrong state for this operation
    #[error("invalid state: {0}")]
    InvalidState(&'static str),

    /// Operation not supported by this socket type
    #[error("operation not supported: {0}")]
    NotSupported(&'static str),

    /// Context has been terminated
    #[error("context was terminated")]
    ContextTerminated,

    /// Socket has been closed
    #[error("socket closed")]
    SocketClosed,

    /// Invalid argument
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// Option not supported
    #[error("option not supported: {0}")]
    OptionNotSupported(String),

    /// Internal error (should not happen)
    #[error("internal error: {0}")]
    Internal(String),

    /// Message was truncated
    #[error("message truncated")]
    MessageTruncated,

    /// No message available (non-blocking recv)
    #[error("no message available")]
    NoMessage,

    /// Address already in use
    #[error("address already in use")]
    AddressInUse,

    /// Connection refused
    #[error("connection refused")]
    ConnectionRefused,

    /// Connection reset by peer
    #[error("connection reset by peer")]
    ConnectionReset,

    /// Timeout
    #[error("operation timed out")]
    Timeout,

    /// Host unreachable
    #[error("host unreachable")]
    HostUnreachable,
}

/// Result type alias for ZeroMQ operations.
pub type ZmqResult<T> = Result<T, ZmqError>;
