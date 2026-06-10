//! ZMTP encoder — converts events/commands into wire bytes.
//!
//! The encoder is a Sans-I/O state machine: it takes abstract commands
//! and produces `Vec<u8>` output ready to be written to a socket.
//! No I/O happens inside this module.

use bytes::Bytes;
use crate::error::{ZmqError, ZmqResult};

use super::command::{Command, CommandName};
use super::framing::Frame;
use super::greeting::Greeting;

/// Events that can be encoded.
#[derive(Debug, Clone)]
pub enum EncoderEvent {
    /// Send the ZMTP greeting
    Greeting(Greeting),
    /// Send a protocol command
    Command(Command),
    /// Send a message frame
    Message(Bytes, bool), // (data, more)
    /// End of message
    MessageEnd,
}

/// Sans-I/O ZMTP encoder.
pub struct ZmqEncoder {
    /// Pending output bytes
    output: Vec<u8>,
    /// Whether the greeting has been sent
    greeting_sent: bool,
    /// Whether the handshake is complete (READY sent)
    ready_sent: bool,
}

impl ZmqEncoder {
    pub fn new() -> Self {
        Self {
            output: Vec::new(),
            greeting_sent: false,
            ready_sent: false,
        }
    }

    /// Encode an event and append to the output buffer.
    pub fn encode(&mut self, event: EncoderEvent) -> ZmqResult<()> {
        match event {
            EncoderEvent::Greeting(greeting) => {
                if self.greeting_sent {
                    return Err(ZmqError::Protocol("greeting already sent".into()));
                }
                self.output.extend_from_slice(&greeting.encode());
                self.greeting_sent = true;
            }
            EncoderEvent::Command(cmd) => {
                if cmd.name == CommandName::Ready {
                    self.ready_sent = true;
                }
                let frame = Frame::command(cmd.encode());
                self.output.extend_from_slice(&frame.encode());
            }
            EncoderEvent::Message(data, more) => {
                let frame = Frame::message(data, more);
                self.output.extend_from_slice(&frame.encode());
            }
            EncoderEvent::MessageEnd => {
                // No output; just marks logical end of multi-part message
            }
        }
        Ok(())
    }

    /// Take all pending output bytes, clearing the internal buffer.
    pub fn take_output(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.output)
    }

    /// Whether there is pending output to send.
    pub fn has_output(&self) -> bool {
        !self.output.is_empty()
    }

    /// Has the handshake completed (READY sent)?
    pub fn is_ready(&self) -> bool {
        self.ready_sent
    }

    /// Reset for a new connection.
    pub fn reset(&mut self) {
        self.output.clear();
        self.greeting_sent = false;
        self.ready_sent = false;
    }
}

impl Default for ZmqEncoder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_greeting_encoding() {
        let mut enc = ZmqEncoder::new();
        let greeting = Greeting::default();
        enc.encode(EncoderEvent::Greeting(greeting.clone())).unwrap();
        let output = enc.take_output();
        assert_eq!(output.len(), 64);
        assert_eq!(output[0], 0xFF);
        assert_eq!(output[9], 0x7F);
    }

    #[test]
    fn test_ready_command() {
        let mut enc = ZmqEncoder::new();
        enc.encode(EncoderEvent::Greeting(Greeting::default())).unwrap();
        let _greeting_bytes = enc.take_output();
        enc.encode(EncoderEvent::Command(Command::ready("DEALER"))).unwrap();
        assert!(enc.is_ready());
    }

    #[test]
    fn test_double_greeting_fails() {
        let mut enc = ZmqEncoder::new();
        enc.encode(EncoderEvent::Greeting(Greeting::default())).unwrap();
        assert!(enc.encode(EncoderEvent::Greeting(Greeting::default())).is_err());
    }
}
