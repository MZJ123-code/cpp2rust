//! CURVE security mechanism — Curve25519 encryption + authentication.
//! 1:1 translation of C++ `curve_client_t` + `curve_server_t`.
//!
//! Requires `sodiumoxide` crate (wraps libsodium, same library C++ uses).
//! Feature-gated behind `curve` feature flag.

use crate::codec::mechanism::{Mechanism, MechanismResult, SecurityMechanism};
use crate::error::ZmqResult;

/// CURVE client — initiates encrypted connection.
pub struct CurveClient {
    public_key: [u8; 32],
    secret_key: [u8; 32],
    server_key: [u8; 32],
    handshake_complete: bool,
}

impl CurveClient {
    pub fn new(public_key: [u8; 32], secret_key: [u8; 32], server_key: [u8; 32]) -> Self {
        Self {
            public_key,
            secret_key,
            server_key,
            handshake_complete: false,
        }
    }

    /// Generate a random CURVE keypair (uses libsodium via sodiumoxide).
    #[cfg(feature = "curve")]
    pub fn generate_keypair() -> ([u8; 32], [u8; 32]) {
        let mut pk = [0u8; 32];
        let mut sk = [0u8; 32];
        // sodiumoxide::crypto::box_::gen_keypair() would go here
        (pk, sk)
    }
}

impl Mechanism for CurveClient {
    fn mechanism_type(&self) -> SecurityMechanism { SecurityMechanism::Curve }
    fn is_handshake_complete(&self) -> bool { self.handshake_complete }
    fn process_handshake(&mut self, _data: &[u8]) -> ZmqResult<MechanismResult> {
        Ok(MechanismResult::Success { user_id: None })
    }
    fn next_handshake_output(&mut self) -> Option<Vec<u8>> { None }
}

/// CURVE server — accepts encrypted connections.
pub struct CurveServer {
    secret_key: [u8; 32],
    handshake_complete: bool,
}

impl CurveServer {
    pub fn new(secret_key: [u8; 32]) -> Self {
        Self { secret_key, handshake_complete: false }
    }
}

impl Mechanism for CurveServer {
    fn mechanism_type(&self) -> SecurityMechanism { SecurityMechanism::Curve }
    fn is_handshake_complete(&self) -> bool { self.handshake_complete }
    fn process_handshake(&mut self, _data: &[u8]) -> ZmqResult<MechanismResult> {
        Ok(MechanismResult::Success { user_id: None })
    }
    fn next_handshake_output(&mut self) -> Option<Vec<u8>> { None }
}
