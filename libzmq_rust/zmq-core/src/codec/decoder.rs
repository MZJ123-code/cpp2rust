//! ZMTP decoder — converts wire bytes into structured events.
//!
//! The decoder is a Sans-I/O state machine: you feed it raw bytes,
//! and it produces a sequence of `ZmqEvent` values.
//! No I/O happens inside this module.

use bytes::Bytes;
use crate::error::{ZmqError, ZmqResult};
use crate::message::{Payload, ZmqMessage};

use super::command::Command;
use super::command::CommandName;
use super::framing::Frame;
use super::greeting::Greeting;

/// Events produced by the decoder.
#[derive(Debug, Clone)]
pub enum ZmqEvent {
    /// A ZMTP greeting was received.
    GreetingReceived(Greeting),
    /// A READY command was received (handshake complete).
    ReadyReceived {
        socket_type: String,
    },
    /// A complete message was received.
    MessageReceived(ZmqMessage),
    /// A SUBSCRIBE command was received (XPUB/XSUB).
    SubscribeReceived(Vec<u8>),
    /// A CANCEL command was received (XPUB/XSUB).
    CancelReceived(Vec<u8>),
    /// A PING heartbeat was received.
    PingReceived(Vec<u8>),
    /// A PONG heartbeat was received.
    PongReceived(Vec<u8>),
    /// A HELLO command was received (ZMTP 3.x welcome message).
    HelloReceived(ZmqMessage),
    /// A HICCUP command was received (subscription state update).
    HiccupReceived,
    /// A DISCONNECT command was received.
    DisconnectReceived,
    /// The peer sent a handshake error.
    Error(ZmqError),
}

/// Decoder state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Waiting for the initial 64-byte greeting
    WaitingForGreeting,
    /// Handshake in progress (waiting for READY or command)
    Handshaking,
    /// Reading a frame header
    ReadingFrameHeader,
    /// Reading frame payload
    ReadingFramePayload(usize), // payload_size
    /// Reading command data
    ReadingCommandPayload(usize), // cmd_data_size
    /// Active state — processing messages
    Active,
}

/// Sans-I/O ZMTP decoder.
pub struct ZmqDecoder {
    state: State,
    /// Buffer for accumulating partial data
    buffer: Vec<u8>,
    /// Multi-part message being assembled
    pending_message: Option<ZmqMessage>,
    /// Whether the handshake is complete
    ready_received: bool,
    /// Current frame flags (temporary)
    current_flags: u8,
}

impl ZmqDecoder {
    pub fn new() -> Self {
        Self {
            state: State::WaitingForGreeting,
            buffer: Vec::new(),
            pending_message: None,
            ready_received: false,
            current_flags: 0,
        }
    }

    /// Feed raw bytes into the decoder. Returns a list of decoded events.
    pub fn decode(&mut self, data: &[u8]) -> ZmqResult<Vec<ZmqEvent>> {
        self.buffer.extend_from_slice(data);
        let mut events = Vec::new();

        loop {
            match self.state {
                State::WaitingForGreeting => {
                    if self.buffer.len() < 64 {
                        break;
                    }
                    let greeting_bytes: [u8; 64] = self.buffer[..64].try_into().unwrap();
                    let greeting = Greeting::parse(&greeting_bytes)?;
                    self.buffer.drain(..64);
                    events.push(ZmqEvent::GreetingReceived(greeting));
                    self.state = State::Handshaking;
                }

                State::Handshaking => {
                    if self.buffer.is_empty() {
                        break;
                    }
                    // Try to decode the next frame (should be READY or error)
                    if let Some(event) = self.decode_next_frame(&mut events)? {
                        // Got a frame — if it's a command, process it
                        if let Some(cmd_event) = event {
                            let is_ready = matches!(cmd_event, ZmqEvent::ReadyReceived { .. });
                            events.push(cmd_event);
                            if is_ready {
                                self.ready_received = true;
                                self.state = State::Active;
                            }
                        }
                    } else {
                        break; // need more data
                    }
                }

                State::Active => {
                    if self.buffer.is_empty() {
                        break;
                    }
                    if let Some(event) = self.decode_next_frame(&mut events)? {
                        match event {
                            Some(ZmqEvent::ReadyReceived { .. }) => {
                                // Another READY? unusual but accept
                                events.push(ZmqEvent::ReadyReceived {
                                    socket_type: String::new(),
                                });
                            }
                            Some(e) => events.push(e),
                            None => {
                                if let Some(msg) = &mut self.pending_message {
                                    if !msg.more() {
                                        let complete = std::mem::take(&mut self.pending_message).unwrap();
                                        events.push(ZmqEvent::MessageReceived(complete));
                                    }
                                }
                            }
                        }
                    } else {
                        break;
                    }
                }

                _ => break,
            }
        }

        Ok(events)
    }

    /// Try to decode one frame from the buffer.
    /// Returns `Ok(Some(event))` for command events, `Ok(None)` for message frames,
    /// or `Err` if decoding fails. Returns `Ok(None)` without consuming if
    /// more data is needed.
    fn decode_next_frame(&mut self, _events: &mut Vec<ZmqEvent>) -> ZmqResult<Option<Option<ZmqEvent>>> {
        // Need at least 2 bytes for short frame header
        if self.buffer.len() < 2 {
            return Ok(None::<Option<ZmqEvent>>);
        }

        // Get frame size
        let header_size = Frame::header_size(&self.buffer)?;
        if self.buffer.len() < header_size {
            return Ok(None::<Option<ZmqEvent>>);
        }

        let payload_size = Frame::payload_size(&self.buffer)?;
        let total_size = header_size + payload_size;
        if self.buffer.len() < total_size {
            return Ok(None::<Option<ZmqEvent>>);
        }

        // Decode the full frame
        let (frame, consumed) = Frame::decode(&self.buffer)?;
        self.buffer.drain(..consumed);

        if frame.command {
            // Decode command
            let (cmd, _) = Command::decode(&frame.data)?;
            self.current_flags = if frame.more { super::framing::FLAG_MORE } else { 0 };
            Ok(Some(Some(self.cmd_to_event(cmd)?)))
        } else {
            // Message frame
            if self.pending_message.is_none() {
                self.pending_message = Some(ZmqMessage::new());
            }
            if let Some(msg) = &mut self.pending_message {
                msg.push_back(Payload::from(frame.data));
                msg.set_more(frame.more);
                if !frame.more {
                    let complete = std::mem::take(&mut self.pending_message).unwrap();
                    return Ok(Some(Some(ZmqEvent::MessageReceived(complete))));
                }
            }
            Ok(Some(None))
        }
    }

    fn cmd_to_event(&self, cmd: Command) -> ZmqResult<ZmqEvent> {
        match cmd.name {
            CommandName::Hello => {
                let msg = ZmqMessage::from_bytes(cmd.data);
                Ok(ZmqEvent::HelloReceived(msg))
            }
            CommandName::Hiccup => {
                Ok(ZmqEvent::HiccupReceived)
            }
            CommandName::Ready => {
                let socket_type = String::from_utf8_lossy(&cmd.data)
                    .trim_end_matches('\0')
                    .to_string();
                Ok(ZmqEvent::ReadyReceived { socket_type })
            }
            CommandName::Subscribe => {
                Ok(ZmqEvent::SubscribeReceived(cmd.data.to_vec()))
            }
            CommandName::Cancel => {
                Ok(ZmqEvent::CancelReceived(cmd.data.to_vec()))
            }
            CommandName::Ping => {
                Ok(ZmqEvent::PingReceived(cmd.data.to_vec()))
            }
            CommandName::Pong => {
                Ok(ZmqEvent::PongReceived(cmd.data.to_vec()))
            }
            CommandName::Disconnect => {
                Ok(ZmqEvent::DisconnectReceived)
            }
        }
    }

    /// Has the handshake completed?
    pub fn is_ready(&self) -> bool {
        self.ready_received
    }

    /// Reset for a new connection.
    pub fn reset(&mut self) {
        self.state = State::WaitingForGreeting;
        self.buffer.clear();
        self.pending_message = None;
        self.ready_received = false;
        self.current_flags = 0;
    }
}

impl Default for ZmqDecoder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::encoder::{EncoderEvent, ZmqEncoder};
    use super::super::greeting::Greeting;

    #[test]
    fn test_decode_greeting() {
        let mut dec = ZmqDecoder::new();
        let greeting = Greeting::default();
        let events = dec.decode(&greeting.encode()).unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], ZmqEvent::GreetingReceived(_)));
    }

    #[test]
    fn test_decode_ready() {
        let mut dec = ZmqDecoder::new();
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&Greeting::default().encode());
        // READY command must be wrapped in a command frame
        let ready_cmd = Command::ready("DEALER");
        let ready_frame = super::super::framing::Frame::command(ready_cmd.encode());
        bytes.extend_from_slice(&ready_frame.encode());

        let events = dec.decode(&bytes).unwrap();
        assert_eq!(events.len(), 2); // Greeting + Ready
        assert!(matches!(events[1], ZmqEvent::ReadyReceived { .. }));
        assert!(dec.is_ready());
    }

    #[test]
    fn test_partial_data_buffering() {
        let mut dec = ZmqDecoder::new();
        let greeting_bytes = Greeting::default().encode();
        // Feed only half
        let events = dec.decode(&greeting_bytes[..32]).unwrap();
        assert!(events.is_empty());
        // Feed the rest
        let events = dec.decode(&greeting_bytes[32..]).unwrap();
        assert_eq!(events.len(), 1);
    }
}
