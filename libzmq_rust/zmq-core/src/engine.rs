//! Stream engine — bridges transport I/O and ZMTP protocol.
//!
//! 1:1 translation of C++ `zmtp_engine_t` + `stream_engine_base_t`.
//!
//! A `ZmtpEngine` consumes raw bytes from the network and produces
//! protocol events via the decoder. It also encodes commands/messages
//! into bytes ready for network transmission.

use crate::codec::command::Command;
use crate::codec::decoder::{ZmqDecoder, ZmqEvent};
use crate::codec::encoder::{EncoderEvent, ZmqEncoder};
use crate::codec::greeting::{Greeting, GreetingMechanism};
use crate::error::{ZmqError, ZmqResult};
use crate::message::ZmqMessage;

/// Engine lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineState {
    /// Initial state, greeting not yet sent
    WaitingForGreeting,
    /// Greeting sent, waiting for peer greeting
    Handshaking,
    /// READY exchanged, normal operation
    Active,
    /// Engine encountered an error
    Error,
    /// Engine stopped
    Stopped,
}

/// The ZMTP protocol engine — Sans-I/O core connected to transport.
///
/// Usage pattern:
/// 1. Call `next_output()` to get bytes to send over the transport
/// 2. Feed received bytes to `process_input()` to get decoded events
/// 3. Use `send_message()` / `send_command()` to queue outgoing data
pub struct ZmtpEngine {
    decoder: ZmqDecoder,
    encoder: ZmqEncoder,
    state: EngineState,
    /// Whether we initiated the connection (client side)
    is_client: bool,
    /// Socket type name for READY command
    socket_type: String,
    /// Whether a READY has been received from the peer
    peer_ready: bool,
    /// Whether we have sent READY
    self_ready_sent: bool,
    /// Whether we have sent our greeting
    greeting_sent: bool,
    /// Whether we have received peer's greeting
    greeting_received: bool,
}

impl ZmtpEngine {
    /// Create a new engine.
    ///
    /// `is_client` — true if we initiated the connection (we send greeting first).
    /// `socket_type` — the ZMTP socket type name for the READY command.
    pub fn new(is_client: bool, socket_type: &str) -> Self {
        Self {
            decoder: ZmqDecoder::new(),
            encoder: ZmqEncoder::new(),
            state: EngineState::WaitingForGreeting,
            is_client,
            socket_type: socket_type.to_string(),
            peer_ready: false,
            self_ready_sent: false,
            greeting_sent: false,
            greeting_received: false,
        }
    }

    /// Process incoming bytes from the network.
    /// Returns a list of decoded events (messages, commands, etc.)
    pub fn process_input(&mut self, buf: &[u8]) -> ZmqResult<Vec<ZmqEvent>> {
        let events = self.decoder.decode(buf)?;
        for event in &events {
            match event {
                ZmqEvent::GreetingReceived(_) => {
                    self.greeting_received = true;
                    if self.state == EngineState::WaitingForGreeting {
                        self.state = EngineState::Handshaking;
                    }
                    // Server generates greeting upon receiving client's greeting
                    if !self.is_client && !self.greeting_sent {
                        let greeting = Greeting::new(GreetingMechanism::Null);
                        self.encoder.encode(EncoderEvent::Greeting(greeting))?;
                        self.greeting_sent = true;
                    }
                }
                ZmqEvent::ReadyReceived { .. } => {
                    self.peer_ready = true;
                    if self.self_ready_sent {
                        self.state = EngineState::Active;
                    }
                }
                _ => {}
            }
        }
        Ok(events)
    }

    /// Get the next bytes to send over the network.
    /// Returns `None` if there's nothing to send.
    pub fn next_output(&mut self) -> Option<Vec<u8>> {
        // Client: send greeting first
        if self.is_client && !self.greeting_sent && !self.encoder.has_output() {
            let greeting = Greeting::new(GreetingMechanism::Null);
            self.encoder
                .encode(EncoderEvent::Greeting(greeting))
                .ok();
            self.greeting_sent = true;
            self.state = EngineState::Handshaking;
        }

        // Send READY when: we received peer's greeting AND sent our greeting
        // (client sends READY after greeting exchange)
        // (server sends READY after receiving client's READY via decoder.is_ready())
        let should_send_ready = !self.self_ready_sent
            && self.greeting_sent
            && self.greeting_received
            && !self.encoder.has_output();

        if should_send_ready {
            let ready_cmd = Command::ready(&self.socket_type);
            self.encoder
                .encode(EncoderEvent::Command(ready_cmd))
                .ok();
            self.self_ready_sent = true;
            if self.peer_ready {
                self.state = EngineState::Active;
            }
        }

        if self.encoder.has_output() {
            Some(self.encoder.take_output())
        } else {
            None
        }
    }

    /// Queue a message for sending.
    pub fn send_message(&mut self, data: &[u8], more: bool) {
        let _ = self.encoder.encode(EncoderEvent::Message(bytes::Bytes::copy_from_slice(data), more));
    }

    /// Queue a ZMTP command for sending.
    pub fn send_command(&mut self, cmd: Command) {
        let _ = self.encoder.encode(EncoderEvent::Command(cmd));
    }

    /// Whether the engine is ready for normal message exchange.
    pub fn is_ready(&self) -> bool {
        self.state == EngineState::Active
    }

    /// Whether the engine has stopped.
    pub fn is_stopped(&self) -> bool {
        self.state == EngineState::Stopped || self.state == EngineState::Error
    }

    /// Get the current state.
    pub fn state(&self) -> EngineState {
        self.state
    }

    /// Stop the engine.
    pub fn stop(&mut self) {
        // Send DISCONNECT command
        self.send_command(Command::disconnect());
        self.state = EngineState::Stopped;
    }

    /// Mark the engine as errored.
    pub fn set_error(&mut self, _err: ZmqError) {
        self.state = EngineState::Error;
    }

    /// Reset the engine for reuse.
    pub fn reset(&mut self, is_client: bool, socket_type: &str) {
        self.decoder.reset();
        self.encoder.reset();
        self.state = EngineState::WaitingForGreeting;
        self.is_client = is_client;
        self.socket_type = socket_type.to_string();
        self.peer_ready = false;
        self.self_ready_sent = false;
        self.greeting_sent = false;
        self.greeting_received = false;
    }
}

// ─── Tests ───────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::command::Command;
    use crate::codec::greeting::{Greeting, GreetingMechanism};

    /// Helper: create a pair of engines and simulate the greeting+ready handshake.
    fn handshake_pair() -> (ZmtpEngine, ZmtpEngine) {
        let mut client = ZmtpEngine::new(true, "DEALER");
        let mut server = ZmtpEngine::new(false, "ROUTER");

        // Client → Greeting → Server
        let client_out1 = client.next_output().unwrap();
        let events = server.process_input(&client_out1).unwrap();
        assert!(events.iter().any(|e| matches!(e, ZmqEvent::GreetingReceived(_))));

        // Server → Greeting → Client
        let server_out1 = server.next_output().unwrap();
        let events = client.process_input(&server_out1).unwrap();
        assert!(events.iter().any(|e| matches!(e, ZmqEvent::GreetingReceived(_))));

        // Client → READY → Server
        let client_out2 = client.next_output().unwrap();
        let events = server.process_input(&client_out2).unwrap();
        assert!(events.iter().any(|e| matches!(e, ZmqEvent::ReadyReceived { .. })));

        // Server → READY → Client
        let server_out2 = server.next_output().unwrap();
        let events = client.process_input(&server_out2).unwrap();
        assert!(events.iter().any(|e| matches!(e, ZmqEvent::ReadyReceived { .. })));

        (client, server)
    }

    #[test]
    fn test_handshake() {
        let (client, server) = handshake_pair();
        assert!(client.is_ready());
        assert!(server.is_ready());
    }

    #[test]
    fn test_message_exchange() {
        let (mut client, mut server) = handshake_pair();

        // Client sends a message
        client.send_message(b"ping", false);
        let wire_bytes = client.next_output().unwrap();

        // Server receives
        let events = server.process_input(&wire_bytes).unwrap();
        let msg_event = events
            .iter()
            .find(|e| matches!(e, ZmqEvent::MessageReceived(_)));
        assert!(msg_event.is_some());

        if let ZmqEvent::MessageReceived(msg) = msg_event.unwrap() {
            assert_eq!(msg.data(), b"ping");
        }
    }
}
