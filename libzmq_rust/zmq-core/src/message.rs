//! ZeroMQ message — multi-frame message with routing metadata.
//!
//! Replaces C++ `msg_t`. Key optimization: VSM (Very Small Message) —
//! payloads ≤ 30 bytes are stored inline (stack allocation, zero heap cost).
//! Larger payloads use `Bytes` for reference-counted zero-copy.
//!
//! Matching C++ `msg_t` which is fixed at 64 bytes with inline VSM.

use bytes::Bytes;
use std::collections::VecDeque;

/// Maximum VSM size — payloads ≤ this are stored inline without heap allocation.
pub const ZMQ_MAX_VSM_SIZE: usize = 30;

/// Frame payload — either inline (VSM) for small data, or heap-allocated.
#[derive(Debug, Clone)]
pub enum Payload {
    /// Inline storage: data[..len] is valid, 0 ≤ len ≤ 30
    Vsm([u8; ZMQ_MAX_VSM_SIZE], u8),
    /// Heap-allocated reference-counted buffer (zero-copy)
    Shared(Bytes),
}

impl Payload {
    pub fn from_slice(data: &[u8]) -> Self {
        if data.len() <= ZMQ_MAX_VSM_SIZE {
            let mut buf = [0u8; ZMQ_MAX_VSM_SIZE];
            buf[..data.len()].copy_from_slice(data);
            Payload::Vsm(buf, data.len() as u8)
        } else {
            Payload::Shared(Bytes::copy_from_slice(data))
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Payload::Vsm(_, len) => *len as usize,
            Payload::Shared(b) => b.len(),
        }
    }

    pub fn is_empty(&self) -> bool { self.len() == 0 }

    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Payload::Vsm(data, len) => &data[..*len as usize],
            Payload::Shared(b) => b.as_ref(),
        }
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
}

impl Default for Payload {
    fn default() -> Self { Payload::Vsm([0u8; ZMQ_MAX_VSM_SIZE], 0) }
}

impl From<&[u8]> for Payload {
    fn from(data: &[u8]) -> Self { Self::from_slice(data) }
}

impl From<Vec<u8>> for Payload {
    fn from(data: Vec<u8>) -> Self {
        if data.len() <= ZMQ_MAX_VSM_SIZE {
            Self::from_slice(&data)
        } else {
            Payload::Shared(Bytes::from(data))
        }
    }
}

impl From<Bytes> for Payload {
    fn from(b: Bytes) -> Self {
        if b.len() <= ZMQ_MAX_VSM_SIZE {
            Self::from_slice(&b)
        } else {
            Payload::Shared(b)
        }
    }
}

/// A ZeroMQ message, consisting of one or more frames.
#[derive(Debug, Clone, Default)]
pub struct ZmqMessage {
    frames: VecDeque<Payload>,
    routing_id: Option<u32>,
    group: Option<String>,
    flags: MessageFlags,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct MessageFlags {
    pub more: bool,
    pub command: bool,
    pub truncated: bool,
    pub zero_copy: bool,
}

impl ZmqMessage {
    pub fn new() -> Self { Self::default() }

    pub fn from_slice(data: &[u8]) -> Self {
        let mut msg = Self::new();
        msg.push_back(Payload::from_slice(data));
        msg
    }

    pub fn from_bytes(data: Bytes) -> Self {
        let mut msg = Self::new();
        msg.push_back(Payload::from(data));
        msg
    }

    pub fn from_parts(parts: &[&[u8]]) -> Self {
        let mut msg = Self::new();
        for part in parts { msg.push_back(Payload::from_slice(part)); }
        msg
    }

    pub fn with_capacity(nframes: usize) -> Self {
        Self { frames: VecDeque::with_capacity(nframes), ..Default::default() }
    }

    pub fn push_back(&mut self, data: Payload) { self.frames.push_back(data); }
    pub fn push_front(&mut self, data: Payload) { self.frames.push_front(data); }
    pub fn pop_front(&mut self) -> Option<Payload> { self.frames.pop_front() }
    pub fn pop_back(&mut self) -> Option<Payload> { self.frames.pop_back() }
    pub fn first(&self) -> Option<&Payload> { self.frames.front() }
    pub fn last(&self) -> Option<&Payload> { self.frames.back() }
    pub fn frame_count(&self) -> usize { self.frames.len() }
    pub fn is_empty(&self) -> bool { self.frames.is_empty() }
    pub fn total_size(&self) -> usize { self.frames.iter().map(|p| p.len()).sum() }
    pub fn size(&self) -> usize { self.total_size() }

    pub fn data(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.total_size());
        for frame in &self.frames { buf.extend_from_slice(frame.as_bytes()); }
        buf
    }

    /// Iterate over frame data as &[u8] (for compatibility with old Bytes API).
    pub fn frame_bytes_iter(&self) -> impl Iterator<Item = &[u8]> {
        self.frames.iter().map(|p| p.as_bytes())
    }

    // Routing
    pub fn routing_id(&self) -> Option<u32> { self.routing_id }
    pub fn set_routing_id(&mut self, id: u32) { self.routing_id = Some(id); }
    pub fn clear_routing_id(&mut self) { self.routing_id = None; }
    pub fn group(&self) -> Option<&str> { self.group.as_deref() }
    pub fn set_group(&mut self, group: String) { self.group = Some(group); }
    pub fn clear_group(&mut self) { self.group = None; }

    // Flags
    pub fn more(&self) -> bool { self.flags.more }
    pub fn set_more(&mut self, more: bool) { self.flags.more = more; }
    pub fn is_last(&self) -> bool { !self.flags.more }
    pub fn is_command(&self) -> bool { self.flags.command }
    pub fn set_command(&mut self, command: bool) { self.flags.command = command; }
}

// Convert a &str directly into a VSM message (no allocation for ≤30B strings)
impl From<&str> for ZmqMessage {
    fn from(s: &str) -> Self { Self::from_slice(s.as_bytes()) }
}

impl From<String> for ZmqMessage {
    fn from(s: String) -> Self { Self::from_bytes(Bytes::from(s)) }
}

impl From<&[u8]> for ZmqMessage {
    fn from(data: &[u8]) -> Self { Self::from_slice(data) }
}

impl From<Vec<u8>> for ZmqMessage {
    fn from(data: Vec<u8>) -> Self {
        if data.len() <= ZMQ_MAX_VSM_SIZE {
            Self::from_slice(&data)
        } else {
            Self::from_bytes(Bytes::from(data))
        }
    }
}

impl From<Bytes> for ZmqMessage {
    fn from(data: Bytes) -> Self { Self::from_bytes(data) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_message_is_empty() {
        let msg = ZmqMessage::new();
        assert!(msg.is_empty());
        assert_eq!(msg.frame_count(), 0);
    }

    #[test]
    fn test_single_frame_message() {
        let msg = ZmqMessage::from_slice(b"hello");
        assert_eq!(msg.frame_count(), 1);
        assert_eq!(msg.size(), 5);
        assert_eq!(&msg.data()[..], b"hello");
    }

    #[test]
    fn test_multi_part_message() {
        let msg = ZmqMessage::from_parts(&[b"part1", b"part2", b"part3"]);
        assert_eq!(msg.frame_count(), 3);
    }

    #[test]
    fn test_push_pop_frames() {
        let mut msg = ZmqMessage::new();
        msg.push_back(Payload::from_slice(b"first"));
        msg.push_back(Payload::from_slice(b"second"));
        assert_eq!(msg.frame_count(), 2);
        assert_eq!(msg.pop_front().unwrap().as_bytes(), b"first");
        assert_eq!(msg.pop_front().unwrap().as_bytes(), b"second");
        assert!(msg.is_empty());
    }

    #[test]
    fn test_routing_id() {
        let mut msg = ZmqMessage::from_slice(b"hello");
        assert_eq!(msg.routing_id(), None);
        msg.set_routing_id(42);
        assert_eq!(msg.routing_id(), Some(42));
    }

    #[test]
    fn test_total_size() {
        let msg = ZmqMessage::from_parts(&[b"abc", b"def", b"ghi"]);
        assert_eq!(msg.total_size(), 9);
    }

    #[test]
    fn test_vsm_boundary() {
        // Exactly 30 bytes — should be VSM (no heap alloc)
        let data = vec![b'x'; 30];
        let msg = ZmqMessage::from_slice(&data);
        assert_eq!(msg.size(), 30);

        // 31 bytes — should be heap-allocated
        let data = vec![b'x'; 31];
        let msg = ZmqMessage::from_slice(&data);
        assert_eq!(msg.size(), 31);
    }
}
