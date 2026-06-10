//! ZMTP greeting - 64-byte initial handshake.
//!
//! Format (64 bytes total):
//!
//! | Offset | Value | Description |
//! |--------|-------|-------------|
//! | 0 | 0xFF | Signature |
//! | 1-8 | 0x00 | Reserved (unused) |
//! | 9 | 0x7F | Final short marker |
//! | 10 | revision | 0x00=ZMTP1.0, 0x01=ZMTP2.0, 0x03=ZMTP3.x |
//! | 11 | mechanism | 0x00=NULL, 0x01=PLAIN, 0x02=CURVE |
//! | 12-31 | 0x00 | Reserved |
//! | 32-51 | server-identity | 20 bytes |
//! | 52-63 | 0x00 | Reserved |

use crate::constants::GREETING_LENGTH;
use crate::error::{ZmqError, ZmqResult};

/// ZMTP protocol revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ZmqVersion {
    V1_0 = 0,
    V2_0 = 1,
    V3_0 = 3,
}

impl ZmqVersion {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::V1_0),
            1 => Some(Self::V2_0),
            3 => Some(Self::V3_0),
            _ => None,
        }
    }
}

/// Security mechanism announced in the greeting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum GreetingMechanism {
    Null = 0,
    Plain = 1,
    Curve = 2,
}

impl GreetingMechanism {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Null),
            1 => Some(Self::Plain),
            2 => Some(Self::Curve),
            _ => None,
        }
    }
}

/// A ZMTP greeting (always exactly 64 bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Greeting {
    pub version: ZmqVersion,
    pub mechanism: GreetingMechanism,
    pub server_identity: [u8; 20],
}

impl Greeting {
    /// Create a new greeting with default settings.
    pub fn new(mechanism: GreetingMechanism) -> Self {
        Self {
            version: ZmqVersion::V3_0,
            mechanism,
            server_identity: [0u8; 20],
        }
    }

    /// Parse a greeting from exactly 64 bytes.
    pub fn parse(input: &[u8; GREETING_LENGTH]) -> ZmqResult<Self> {
        // Check signature
        if input[0] != 0xFF {
            return Err(ZmqError::Protocol("invalid greeting signature".into()));
        }
        // Reserved bytes 1-8 should be 0
        // Final short marker
        if input[9] != 0x7F {
            return Err(ZmqError::Protocol("invalid greeting final marker".into()));
        }
        let version = ZmqVersion::from_u8(input[10])
            .ok_or_else(|| ZmqError::Protocol(format!("unknown ZMTP revision: {}", input[10])))?;

        let mechanism = GreetingMechanism::from_u8(input[11])
            .ok_or_else(|| ZmqError::Protocol(format!("unknown mechanism: {}", input[11])))?;

        let mut server_identity = [0u8; 20];
        server_identity.copy_from_slice(&input[32..52]);

        Ok(Self {
            version,
            mechanism,
            server_identity,
        })
    }

    /// Encode this greeting into 64 bytes.
    pub fn encode(&self) -> [u8; GREETING_LENGTH] {
        let mut buf = [0u8; GREETING_LENGTH];
        buf[0] = 0xFF; // signature
        // bytes 1-8 are 0 (reserved)
        buf[9] = 0x7F; // final short marker
        buf[10] = self.version as u8;
        buf[11] = self.mechanism as u8;
        // bytes 12-31 are 0 (reserved)
        buf[32..52].copy_from_slice(&self.server_identity);
        // bytes 52-63 are 0 (reserved)
        buf
    }
}

impl Default for Greeting {
    fn default() -> Self {
        Self::new(GreetingMechanism::Null)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_trip() {
        let mut g = Greeting::new(GreetingMechanism::Null);
        g.server_identity = *b"test-server-id-00000";
        let bytes = g.encode();
        let parsed = Greeting::parse(&bytes).unwrap();
        assert_eq!(parsed.version, ZmqVersion::V3_0);
        assert_eq!(parsed.mechanism, GreetingMechanism::Null);
        assert_eq!(&parsed.server_identity[..], b"test-server-id-00000");
    }

    #[test]
    fn test_bad_signature() {
        let mut bytes = [0u8; 64];
        bytes[9] = 0x7F;
        bytes[10] = 3;
        assert!(Greeting::parse(&bytes).is_err());
    }

    #[test]
    fn test_bad_version() {
        let mut bytes = [0u8; 64];
        bytes[0] = 0xFF;
        bytes[9] = 0x7F;
        bytes[10] = 0x05; // invalid
        assert!(Greeting::parse(&bytes).is_err());
    }
}
