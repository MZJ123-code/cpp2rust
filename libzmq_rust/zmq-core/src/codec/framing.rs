//! ZMTP wire-level framing.
//!
//! ZMTP uses two frame formats:
//!
//! **Short frame** (payload < 255 bytes):
//! ```text
//! [1 byte: flags] [1 byte: size] [size bytes: payload]
//! ```
//!
//! **Long frame** (payload >= 255 bytes):
//! ```text
//! [1 byte: flags] [8 bytes: size (big-endian)] [size bytes: payload]
//! ```
//!
//! Flags byte layout:
//! ```text
//! bit 0: MORE (1 = more frames follow)
//! bit 1: LONG (1 = long frame, 0 = short frame)
//! bit 2: COMMAND (1 = command frame)
//! bits 3-7: reserved (0)
//! ```

use bytes::{BufMut, Bytes, BytesMut};
use crate::error::{ZmqError, ZmqResult};

/// ZMTP frame flags.
pub const FLAG_MORE: u8 = 1;
pub const FLAG_LONG: u8 = 2;
pub const FLAG_COMMAND: u8 = 4;

/// Threshold: payloads < 255 bytes use short frame format.
pub const SHORT_FRAME_MAX_SIZE: usize = 254;

/// A decoded ZMTP frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// Whether more frames follow in this message.
    pub more: bool,
    /// Whether this is a ZMTP command frame.
    pub command: bool,
    /// Frame payload bytes.
    pub data: Bytes,
}

impl Frame {
    /// Create a message frame.
    pub fn message(data: Bytes, more: bool) -> Self {
        Self { more, command: false, data }
    }

    /// Create a command frame.
    pub fn command(data: Bytes) -> Self {
        Self { more: false, command: true, data }
    }

    /// Encode this frame into wire bytes.
    pub fn encode(&self) -> Bytes {
        let mut flags: u8 = 0;
        if self.more {
            flags |= FLAG_MORE;
        }
        if self.command {
            flags |= FLAG_COMMAND;
        }

        let payload_len = self.data.len();
        if payload_len <= SHORT_FRAME_MAX_SIZE {
            // Short frame
            let mut buf = BytesMut::with_capacity(2 + payload_len);
            buf.put_u8(flags); // no LONG flag
            buf.put_u8(payload_len as u8);
            buf.put_slice(&self.data);
            buf.freeze()
        } else {
            // Long frame
            flags |= FLAG_LONG;
            let mut buf = BytesMut::with_capacity(9 + payload_len);
            buf.put_u8(flags);
            buf.put_u64(payload_len as u64);
            buf.put_slice(&self.data);
            buf.freeze()
        }
    }

    /// Decode a frame from wire bytes. Returns `(frame, bytes_consumed)`.
    pub fn decode(buf: &[u8]) -> ZmqResult<(Self, usize)> {
        if buf.is_empty() {
            return Err(ZmqError::Codec("empty frame buffer".into()));
        }

        let flags = buf[0];
        let more = (flags & FLAG_MORE) != 0;
        let long = (flags & FLAG_LONG) != 0;
        let command = (flags & FLAG_COMMAND) != 0;

        let (payload_len, header_size): (usize, usize) = if long {
            if buf.len() < 9 {
                return Err(ZmqError::Codec("long frame truncated".into()));
            }
            let size = u64::from_be_bytes(buf[1..9].try_into().unwrap()) as usize;
            (size, 9)
        } else {
            if buf.len() < 2 {
                return Err(ZmqError::Codec("short frame truncated".into()));
            }
            (buf[1] as usize, 2)
        };

        let total = header_size + payload_len;
        if buf.len() < total {
            return Err(ZmqError::Codec(format!(
                "frame payload truncated: need {} have {}",
                payload_len,
                buf.len().saturating_sub(header_size)
            )));
        }

        let data = Bytes::copy_from_slice(&buf[header_size..total]);
        Ok((Self { more, command, data }, total))
    }

    /// Minimum bytes needed to decode the header (for sizing reads).
    pub fn header_size(buf: &[u8]) -> ZmqResult<usize> {
        if buf.is_empty() {
            return Err(ZmqError::Codec("empty buffer".into()));
        }
        let long = (buf[0] & FLAG_LONG) != 0;
        Ok(if long { 9 } else { 2 })
    }

    /// Parse the payload size from header bytes.
    pub fn payload_size(buf: &[u8]) -> ZmqResult<usize> {
        if buf.is_empty() {
            return Err(ZmqError::Codec("empty buffer".into()));
        }
        let long = (buf[0] & FLAG_LONG) != 0;
        if long {
            if buf.len() < 9 {
                return Err(ZmqError::Codec("long frame header truncated".into()));
            }
            Ok(u64::from_be_bytes(buf[1..9].try_into().unwrap()) as usize)
        } else {
            if buf.len() < 2 {
                return Err(ZmqError::Codec("short frame header truncated".into()));
            }
            Ok(buf[1] as usize)
        }
    }

    /// Total frame size (header + payload).
    pub fn total_size(buf: &[u8]) -> ZmqResult<usize> {
        Ok(Self::header_size(buf)? + Self::payload_size(buf)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_short_frame_round_trip() {
        let frame = Frame::message(Bytes::from("hello"), false);
        let bytes = frame.encode();
        let (decoded, consumed) = Frame::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded.data.as_ref(), b"hello");
        assert!(!decoded.more);
        assert!(!decoded.command);
    }

    #[test]
    fn test_more_flag() {
        let frame = Frame::message(Bytes::from("part1"), true);
        let bytes = frame.encode();
        let (decoded, _) = Frame::decode(&bytes).unwrap();
        assert!(decoded.more);
    }

    #[test]
    fn test_command_flag() {
        let frame = Frame::command(Bytes::from("READY"));
        let bytes = frame.encode();
        let (decoded, _) = Frame::decode(&bytes).unwrap();
        assert!(decoded.command);
    }

    #[test]
    fn test_long_frame() {
        let data = vec![0xAAu8; 300]; // > 254 bytes triggers long frame
        let frame = Frame::message(Bytes::from(data), false);
        let bytes = frame.encode();
        assert!(bytes[0] & FLAG_LONG != 0);
        let (decoded, consumed) = Frame::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded.data.len(), 300);
    }
}
