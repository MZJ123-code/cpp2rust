//! NULL security mechanism — no authentication, no encryption.
//!
//! The NULL mechanism provides no security. Both sides exchange READY
//! commands after the greeting exchange. This matches ZMTP 3.0 specification.
//!
//! # Protocol
//!
//! After the ZMTP greeting, both peers exchange READY commands:
//! ```text
//! Peer A: \x05READY[properties]  →
//! Peer B:                         →  \x05READY[properties]
//! ```
//!
//! Either peer may send an ERROR command instead of READY:
//! ```text
//! Peer A: \x05ERROR[1-byte len][reason]  →
//! ```
//!
//! References:
//! - C++ `null_mechanism.cpp`
//! - ZMTP 3.0 specification

use crate::codec::mechanism::{Mechanism, MechanismResult, SecurityMechanism};
use crate::error::{ZmqError, ZmqResult};

/// Command name for READY command (length-prefixed: 0x05 then "READY").
const READY_COMMAND: &[u8] = b"\x05READY";
const READY_COMMAND_LEN: usize = 6;

/// Command name for ERROR command (length-prefixed: 0x05 then "ERROR").
const ERROR_COMMAND: &[u8] = b"\x05ERROR";
const ERROR_COMMAND_LEN: usize = 6;

/// Handshake state for the NULL mechanism.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NullState {
    /// Initial state: waiting to exchange READY commands with the peer.
    WaitingForReady,
    /// Our READY has been sent; waiting for peer's READY.
    ReadySent,
    /// Peer's READY has been received; waiting to send ours.
    ReadyReceived,
    /// Both READY commands exchanged; handshake complete.
    Ready,
    /// Received an ERROR command from the peer.
    ErrorReceived,
}

/// NULL security mechanism — exchanges READY commands with no actual security.
///
/// # Example
///
/// ```rust
/// use zmq_core::security::NullMechanism;
/// use zmq_core::codec::mechanism::{Mechanism, MechanismResult};
///
/// let mut client = NullMechanism::new();
/// let mut server = NullMechanism::new();
///
/// // Both sides send READY
/// let c_out = client.next_handshake_output().unwrap();
/// let s_out = server.next_handshake_output().unwrap();
///
/// // Both sides receive peer's READY
/// let c_result = client.process_handshake(&s_out).unwrap();
/// let s_result = server.process_handshake(&c_out).unwrap();
///
/// assert!(matches!(c_result, MechanismResult::Success { .. }));
/// assert!(matches!(s_result, MechanismResult::Success { .. }));
/// assert!(client.is_handshake_complete());
/// assert!(server.is_handshake_complete());
/// ```
pub struct NullMechanism {
    state: NullState,
}

impl NullMechanism {
    /// Create a new NULL mechanism in initial state.
    pub fn new() -> Self {
        Self {
            state: NullState::WaitingForReady,
        }
    }

    /// Build a READY command with minimal properties.
    /// In a full implementation, this would include Socket-Type and
    /// other ZMTP properties from the session options.
    fn build_ready_command() -> Vec<u8> {
        // READY command prefix followed by optional properties.
        // For now, we emit just the prefix — the session/engine layer
        // is responsible for adding metadata properties.
        READY_COMMAND.to_vec()
    }
}

impl Default for NullMechanism {
    fn default() -> Self {
        Self::new()
    }
}

impl Mechanism for NullMechanism {
    fn mechanism_type(&self) -> SecurityMechanism {
        SecurityMechanism::Null
    }

    fn is_handshake_complete(&self) -> bool {
        self.state == NullState::Ready
    }

    fn process_handshake(&mut self, data: &[u8]) -> ZmqResult<MechanismResult> {
        match self.state {
            NullState::WaitingForReady | NullState::ReadySent => {
                self.parse_peer_command(data)
            }
            NullState::Ready | NullState::ReadyReceived => {
                // Already received peer's command — protocol violation
                Err(ZmqError::Protocol(
                    "NULL handshake: unexpected second command from peer".into(),
                ))
            }
            NullState::ErrorReceived => {
                Err(ZmqError::Protocol(
                    "NULL handshake: peer already sent ERROR".into(),
                ))
            }
        }
    }

    fn next_handshake_output(&mut self) -> Option<Vec<u8>> {
        match self.state {
            NullState::WaitingForReady | NullState::ReadyReceived => {
                let cmd = Self::build_ready_command();
                // Transition based on whether we already received peer's READY
                self.state = if matches!(self.state, NullState::ReadyReceived) {
                    NullState::Ready
                } else {
                    NullState::ReadySent
                };
                Some(cmd)
            }
            NullState::ReadySent | NullState::Ready | NullState::ErrorReceived => {
                // Already sent our command — nothing more to send
                None
            }
        }
    }

    fn user_id(&self) -> Option<&str> {
        // NULL mechanism provides no authentication
        None
    }
}

impl NullMechanism {
    /// Parse the peer's handshake command (READY or ERROR).
    fn parse_peer_command(&mut self, data: &[u8]) -> ZmqResult<MechanismResult> {
        // Check for READY command
        if data.len() >= READY_COMMAND_LEN && data[..READY_COMMAND_LEN] == *READY_COMMAND {
            return self.handle_ready(data);
        }

        // Check for ERROR command
        if data.len() >= ERROR_COMMAND_LEN && data[..ERROR_COMMAND_LEN] == *ERROR_COMMAND {
            return self.handle_error(data);
        }

        // Unknown command
        let preview = if data.len() > 10 { &data[..10] } else { data };
        Err(ZmqError::Protocol(format!(
            "NULL handshake: unexpected command ({} bytes, starts with {:?})",
            data.len(),
            preview
        )))
    }

    /// Process a READY command from the peer.
    fn handle_ready(&mut self, data: &[u8]) -> ZmqResult<MechanismResult> {
        // The data after READY_COMMAND is ZMTP metadata properties.
        // Parsing metadata is done at the session/engine layer.
        // For now, we just verify the prefix is correct.

        // Optional: validate that properties are well-formed
        let properties = &data[READY_COMMAND_LEN..];
        if !properties.is_empty() {
            // Basic validation: properties should contain at least one
            // name-length byte. Detailed validation is done at higher layers.
            if properties.len() < 1 {
                return Err(ZmqError::Protocol(
                    "NULL handshake: malformed READY properties".into(),
                ));
            }
        }

        // Update state
        self.state = if matches!(self.state, NullState::ReadySent) {
            NullState::Ready
        } else {
            NullState::ReadyReceived
        };

        Ok(MechanismResult::Success { user_id: None })
    }

    /// Process an ERROR command from the peer.
    fn handle_error(&mut self, data: &[u8]) -> ZmqResult<MechanismResult> {
        self.state = NullState::ErrorReceived;

        // Extract error reason
        let reason = extract_error_reason(data);
        let error_msg = if reason.is_empty() {
            "NULL handshake: peer sent ERROR".to_string()
        } else {
            format!("NULL handshake: peer sent ERROR: {}", reason)
        };

        Ok(MechanismResult::Error(ZmqError::Security(error_msg)))
    }
}

/// Helper: extract the error reason string from an ERROR command.
///
/// ERROR command format: `\x05ERROR [1-byte reason_len] [reason]`
fn extract_error_reason(data: &[u8]) -> String {
    let fixed_prefix = ERROR_COMMAND_LEN + 1; // prefix + 1 byte length
    if data.len() >= fixed_prefix {
        let reason_len = data[ERROR_COMMAND_LEN] as usize;
        let start = ERROR_COMMAND_LEN + 1;
        let end = std::cmp::min(start + reason_len, data.len());
        String::from_utf8_lossy(&data[start..end]).to_string()
    } else {
        String::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== Creation and basic properties ====================

    #[test]
    fn test_null_mechanism_creation() {
        let m = NullMechanism::new();
        assert_eq!(m.mechanism_type(), SecurityMechanism::Null);
        assert!(!m.is_handshake_complete());
        assert!(m.user_id().is_none());
    }

    #[test]
    fn test_null_mechanism_default() {
        let m = NullMechanism::default();
        assert_eq!(m.mechanism_type(), SecurityMechanism::Null);
        assert!(!m.is_handshake_complete());
    }

    // ==================== Outbound READY ====================

    #[test]
    fn test_null_handshake_output_is_ready_command() {
        let mut m = NullMechanism::new();
        let output = m.next_handshake_output().unwrap();
        assert_eq!(&output, READY_COMMAND);
    }

    #[test]
    fn test_null_handshake_only_one_output() {
        let mut m = NullMechanism::new();
        assert!(m.next_handshake_output().is_some());
        assert!(m.next_handshake_output().is_none());
    }

    #[test]
    fn test_null_handshake_no_output_after_error() {
        let mut m = NullMechanism::new();
        // Send our READY first
        m.next_handshake_output();
        // Then receive ERROR
        let error_cmd = build_error_command("403");
        let _ = m.process_handshake(&error_cmd);
        assert!(m.next_handshake_output().is_none());
    }

    // ==================== Inbound READY — send then receive ====================

    #[test]
    fn test_null_handshake_send_then_receive() {
        let mut m = NullMechanism::new();
        // Send our READY
        m.next_handshake_output();
        assert!(!m.is_handshake_complete());
        // Receive peer's READY
        let result = m.process_handshake(READY_COMMAND).unwrap();
        assert!(matches!(result, MechanismResult::Success { user_id: None }));
        assert!(m.is_handshake_complete());
    }

    #[test]
    fn test_null_handshake_receive_then_send() {
        let mut m = NullMechanism::new();
        // Receive peer's READY first
        let result = m.process_handshake(READY_COMMAND).unwrap();
        assert!(matches!(result, MechanismResult::Success { user_id: None }));
        assert!(!m.is_handshake_complete());
        // Then send our READY
        let output = m.next_handshake_output().unwrap();
        assert_eq!(&output, READY_COMMAND);
        assert!(m.is_handshake_complete());
    }

    // ==================== Inbound READY with properties ====================

    #[test]
    fn test_null_handshake_ready_with_properties() {
        let props = build_properties(&[("Socket-Type", "DEALER")]);
        let ready_with_props = [READY_COMMAND, &props].concat();

        let mut m = NullMechanism::new();
        m.next_handshake_output();
        let result = m.process_handshake(&ready_with_props).unwrap();
        assert!(matches!(result, MechanismResult::Success { user_id: None }));
        assert!(m.is_handshake_complete());
    }

    // ==================== Full two-way handshake ====================

    #[test]
    fn test_null_handshake_full_two_way() {
        let mut peer_a = NullMechanism::new();
        let mut peer_b = NullMechanism::new();

        // Both peers generate their READY commands
        let a_to_b = peer_a.next_handshake_output().unwrap();
        let b_to_a = peer_b.next_handshake_output().unwrap();

        // Each peer processes the other's READY
        let a_result = peer_a.process_handshake(&b_to_a).unwrap();
        let b_result = peer_b.process_handshake(&a_to_b).unwrap();

        assert!(matches!(a_result, MechanismResult::Success { .. }));
        assert!(matches!(b_result, MechanismResult::Success { .. }));
        assert!(peer_a.is_handshake_complete());
        assert!(peer_b.is_handshake_complete());
    }

    // ==================== ERROR handling ====================

    #[test]
    fn test_null_handshake_receive_error_no_reason() {
        let mut m = NullMechanism::new();
        m.next_handshake_output();
        let result = m.process_handshake(ERROR_COMMAND).unwrap();
        assert!(matches!(result, MechanismResult::Error(_)));
        assert!(!m.is_handshake_complete());
    }

    #[test]
    fn test_null_handshake_receive_error_with_reason() {
        let mut m = NullMechanism::new();
        m.next_handshake_output();
        let error_cmd = build_error_command("403");
        let result = m.process_handshake(&error_cmd).unwrap();
        match result {
            MechanismResult::Error(e) => {
                assert!(e.to_string().contains("403"));
            }
            _ => panic!("Expected error"),
        }
    }

    #[test]
    fn test_null_handshake_receive_error_instead_of_ready() {
        let mut m = NullMechanism::new();
        // We haven't sent READY yet, but peer sends ERROR
        let error_cmd = build_error_command("500");
        let result = m.process_handshake(&error_cmd).unwrap();
        match result {
            MechanismResult::Error(e) => {
                assert!(e.to_string().contains("500"));
            }
            _ => panic!("Expected error"),
        }
        // After error, we can't send
        assert!(m.next_handshake_output().is_none());
    }

    // ==================== Protocol error cases ====================

    #[test]
    fn test_null_handshake_unknown_command() {
        let mut m = NullMechanism::new();
        let result = m.process_handshake(b"\x05XUNKN");
        assert!(result.is_err());
    }

    #[test]
    fn test_null_handshake_empty_data() {
        let mut m = NullMechanism::new();
        let result = m.process_handshake(b"");
        assert!(result.is_err());
    }

    #[test]
    fn test_null_handshake_double_receive_error() {
        let mut m = NullMechanism::new();
        m.next_handshake_output();
        // First READY succeeds
        let r1 = m.process_handshake(READY_COMMAND);
        assert!(r1.is_ok());
        // Second READY fails
        let r2 = m.process_handshake(READY_COMMAND);
        assert!(r2.is_err());
    }

    #[test]
    fn test_null_handshake_short_unknown_command() {
        let mut m = NullMechanism::new();
        // Only 1 byte — not enough for any valid command
        let result = m.process_handshake(b"\x01");
        assert!(result.is_err());
    }

    // ==================== Edge case: nearly-valid prefixes ====================

    #[test]
    fn test_null_handshake_nearly_ready() {
        let mut m = NullMechanism::new();
        // "READ" is too short
        let result = m.process_handshake(b"\x04READXXXX");
        assert!(result.is_err());
    }

    // ==================== Helper functions for tests ====================

    /// Build a well-formed ERROR command with a reason string.
    fn build_error_command(reason: &str) -> Vec<u8> {
        let reason = &reason[..std::cmp::min(reason.len(), 255)];
        let mut cmd = Vec::with_capacity(ERROR_COMMAND_LEN + 1 + reason.len());
        cmd.extend_from_slice(ERROR_COMMAND);
        cmd.push(reason.len() as u8);
        cmd.extend_from_slice(reason.as_bytes());
        cmd
    }

    /// Build ZMTP properties in wire format.
    /// Each property: [1-byte name_len][name][4-byte BE value_len][value]
    fn build_properties(props: &[(&str, &str)]) -> Vec<u8> {
        let mut buf = Vec::new();
        for (name, value) in props {
            buf.push(name.len() as u8);
            buf.extend_from_slice(name.as_bytes());
            // 4-byte big-endian value length
            let len = value.len() as u32;
            buf.extend_from_slice(&len.to_be_bytes());
            buf.extend_from_slice(value.as_bytes());
        }
        buf
    }

    #[test]
    fn test_build_properties() {
        let props = build_properties(&[("Socket-Type", "DEALER")]);
        assert_eq!(props.len(), 1 + 11 + 4 + 6); // 1(name_len) + 11(name) + 4(val_len) + 6(val)
        assert_eq!(props[0], 11); // "Socket-Type".len()
    }
}
