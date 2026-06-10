//! Security mechanism trait for ZMTP handshake.
//!
//! Mechanisms handle the security handshake that happens after the greeting
//! exchange and before READY commands.
//!
//! Standard mechanisms:
//! - NULL (no security)
//! - PLAIN (username/password)
//! - CURVE (Curve25519 encryption)

use crate::error::{ZmqError, ZmqResult};

/// Security mechanism identifier (encoded in greeting byte 11).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecurityMechanism {
    Null = 0,
    Plain = 1,
    Curve = 2,
}

/// Result of processing handshake data.
pub enum MechanismResult {
    /// Handshake successful, optionally with authenticated user ID.
    Success { user_id: Option<String> },
    /// More handshake data is needed; contains bytes to send to peer.
    NeedMore(Vec<u8>),
    /// Handshake failed.
    Error(ZmqError),
}

/// Trait for ZMTP security mechanisms.
///
/// Each mechanism handles its own handshake exchange after the greeting
/// is exchanged and before the READY command is sent.
pub trait Mechanism: Send + Sync {
    /// The mechanism type identifier.
    fn mechanism_type(&self) -> SecurityMechanism;

    /// Whether the handshake is complete on our side.
    fn is_handshake_complete(&self) -> bool;

    /// Process incoming handshake data from the peer.
    /// Returns the result of this processing step.
    fn process_handshake(&mut self, data: &[u8]) -> ZmqResult<MechanismResult>;

    /// Get the next handshake data to send to the peer (if any).
    fn next_handshake_output(&mut self) -> Option<Vec<u8>>;

    /// Get the authenticated user ID (if the mechanism provides one).
    fn user_id(&self) -> Option<&str> {
        None
    }
}
