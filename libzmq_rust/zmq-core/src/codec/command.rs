//! ZMTP protocol commands (wire-level, not internal thread commands).
//!
//! ZMTP commands are sent as command frames (flag bit 2 set).
//! Command format:
//! ```text
//! [name_len: 1 byte] [name: name_len bytes] [data: remaining bytes]
//! ```
//!
//! Standard commands:
//! - READY (0x04): connection handshake complete
//! - SUBSCRIBE (0x05): subscription request
//! - CANCEL (0x06): cancel subscription
//! - PING (0x08): heartbeat ping
//! - PONG (0x09): heartbeat pong
//! - DISCONNECT (0x10): graceful disconnect

use bytes::{Bytes, BytesMut, BufMut};
use crate::error::{ZmqError, ZmqResult};

/// ZMTP command names (wire protocol constants).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CommandName {
    Hello = 0x01,
    Hiccup = 0x02,
    Ready = 0x04,
    Subscribe = 0x05,
    Cancel = 0x06,
    Ping = 0x08,
    Pong = 0x09,
    Disconnect = 0x10,
}

impl CommandName {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0x01 => Some(Self::Hello),
            0x02 => Some(Self::Hiccup),
            0x04 => Some(Self::Ready),
            0x05 => Some(Self::Subscribe),
            0x06 => Some(Self::Cancel),
            0x08 => Some(Self::Ping),
            0x09 => Some(Self::Pong),
            0x10 => Some(Self::Disconnect),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Hello => "HELLO",
            Self::Hiccup => "HICCUP",
            Self::Ready => "READY",
            Self::Subscribe => "SUBSCRIBE",
            Self::Cancel => "CANCEL",
            Self::Ping => "PING",
            Self::Pong => "PONG",
            Self::Disconnect => "DISCONNECT",
        }
    }

    pub fn name_bytes(&self) -> &'static [u8] {
        self.as_str().as_bytes()
    }

    /// Parse command name from raw bytes (used in decoding).
    pub fn from_name_bytes(bytes: &[u8]) -> ZmqResult<Self> {
        match bytes.len() {
            5 if bytes == b"HELLO" => Ok(Self::Hello),
            6 if bytes == b"HICCUP" => Ok(Self::Hiccup),
            5 if bytes == b"READY" => Ok(Self::Ready),
            9 if bytes == b"SUBSCRIBE" => Ok(Self::Subscribe),
            6 if bytes == b"CANCEL" => Ok(Self::Cancel),
            4 if bytes == b"PING" => Ok(Self::Ping),
            4 if bytes == b"PONG" => Ok(Self::Pong),
            10 if bytes == b"DISCONNECT" => Ok(Self::Disconnect),
            _ => Err(ZmqError::Codec(format!(
                "unknown command: {:?}",
                String::from_utf8_lossy(bytes)
            ))),
        }
    }
}

/// A ZMTP protocol command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Command {
    pub name: CommandName,
    pub data: Bytes,
}

impl Command {
    /// Create a HELLO command with welcome message data.
    pub fn hello(data: &[u8]) -> Self {
        Self {
            name: CommandName::Hello,
            data: Bytes::copy_from_slice(data),
        }
    }

    /// Create a HICCUP command (notify peer about subscription changes).
    pub fn hiccup() -> Self {
        Self {
            name: CommandName::Hiccup,
            data: Bytes::new(),
        }
    }

    /// Create a READY command with socket type metadata.
    /// Format: "Socket-type\0\0\0" + properties
    pub fn ready(socket_type: &str) -> Self {
        let mut data = BytesMut::new();
        data.put_slice(socket_type.as_bytes());
        // Pad to at least 3 null bytes
        data.put_u8(0);
        data.put_u8(0);
        data.put_u8(0);
        Self {
            name: CommandName::Ready,
            data: data.freeze(),
        }
    }

    /// Create a SUBSCRIBE command with the subscription prefix.
    pub fn subscribe(prefix: &[u8]) -> Self {
        Self {
            name: CommandName::Subscribe,
            data: Bytes::copy_from_slice(prefix),
        }
    }

    /// Create a CANCEL command with the subscription prefix.
    pub fn cancel(prefix: &[u8]) -> Self {
        Self {
            name: CommandName::Cancel,
            data: Bytes::copy_from_slice(prefix),
        }
    }

    /// Create a PING command (can include context for PONG reply).
    pub fn ping(context: &[u8]) -> Self {
        Self {
            name: CommandName::Ping,
            data: Bytes::copy_from_slice(context),
        }
    }

    /// Create a PONG command (echoes PING context).
    pub fn pong(context: &[u8]) -> Self {
        Self {
            name: CommandName::Pong,
            data: Bytes::copy_from_slice(context),
        }
    }

    /// Create a DISCONNECT command.
    pub fn disconnect() -> Self {
        Self {
            name: CommandName::Disconnect,
            data: Bytes::new(),
        }
    }

    /// Encode this command into wire bytes.
    pub fn encode(&self) -> Bytes {
        let name = self.name.name_bytes();
        let mut buf = BytesMut::with_capacity(1 + name.len() + self.data.len());
        buf.put_u8(name.len() as u8);
        buf.put_slice(name);
        buf.put_slice(&self.data);
        buf.freeze()
    }

    /// Decode a command from wire bytes.
    pub fn decode(buf: &[u8]) -> ZmqResult<(Self, usize)> {
        if buf.is_empty() {
            return Err(ZmqError::Codec("empty command buffer".into()));
        }
        let name_len = buf[0] as usize;
        if buf.len() < 1 + name_len {
            return Err(ZmqError::Codec("command name truncated".into()));
        }
        let name_slice = &buf[1..1 + name_len];
        let name = CommandName::from_name_bytes(name_slice)?;
        let consumed = 1 + name_len + buf[1 + name_len..].len(); // remaining is data
        let data = Bytes::copy_from_slice(&buf[1 + name_len..consumed]);
        Ok((Self { name, data }, consumed))
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ready_round_trip() {
        let cmd = Command::ready("DEALER");
        let bytes = cmd.encode();
        let (decoded, _) = Command::decode(&bytes).unwrap();
        assert_eq!(decoded.name, CommandName::Ready);
    }

    #[test]
    fn test_subscribe_round_trip() {
        let cmd = Command::subscribe(b"topic");
        let bytes = cmd.encode();
        let (decoded, _) = Command::decode(&bytes).unwrap();
        assert_eq!(decoded.name, CommandName::Subscribe);
        assert_eq!(decoded.data.as_ref(), b"topic");
    }

    #[test]
    fn test_ping_pong() {
        let ping = Command::ping(b"ctx");
        let bytes = ping.encode();
        let (decoded, _) = Command::decode(&bytes).unwrap();
        assert_eq!(decoded.name, CommandName::Ping);
        let pong = Command::pong(&decoded.data);
        assert_eq!(pong.name, CommandName::Pong);
    }

    #[test]
    fn test_unknown_command() {
        let bytes = vec![3, b'X', b'X', b'X'];
        assert!(Command::decode(&bytes).is_err());
    }
}
