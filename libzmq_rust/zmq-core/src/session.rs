//! Session — manages the lifecycle of a single peer connection.
//!
//! 1:1 translation of C++ `session_base_t`.
//!
//! A Session is created when a Socket connects to or accepts a peer.
//! It holds the ZMTP engine and manages the handshake and message forwarding
//! between the transport and the socket via Pipes.

use std::sync::Arc;

use crate::engine::{EngineState, ZmtpEngine};
use crate::error::{ZmqError, ZmqResult};
use crate::message::ZmqMessage;
use crate::pipe::Pipe;

/// Session state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    /// Initial state, connection in progress
    Connecting,
    /// ZMTP handshake (greeting + READY exchange)
    Handshaking,
    /// Fully connected and exchanging messages
    Active,
    /// Disconnecting gracefully
    Disconnecting,
    /// Session terminated
    Terminated,
}

/// Manages one peer connection.
pub struct Session {
    /// Outbound pipe (Session → Socket): messages from network to application
    out_pipe: Arc<Pipe>,
    /// Inbound pipe (Socket → Session): messages from application to network
    in_pipe: Arc<Pipe>,
    /// The ZMTP protocol engine
    engine: ZmtpEngine,
    /// Current session state
    state: SessionState,
    /// Whether we initiated the connection
    is_client: bool,
}

impl Session {
    /// Create a new session.
    pub fn new(
        out_pipe: Arc<Pipe>,
        in_pipe: Arc<Pipe>,
        is_client: bool,
        socket_type: &str,
    ) -> Self {
        Self {
            out_pipe,
            in_pipe,
            engine: ZmtpEngine::new(is_client, socket_type),
            state: SessionState::Connecting,
            is_client,
        }
    }

    /// Feed network bytes into the engine and process resulting events.
    pub fn process_input(&mut self, buf: &[u8]) -> ZmqResult<()> {
        let events = self.engine.process_input(buf)?;

        for event in events {
            match event {
                crate::codec::decoder::ZmqEvent::GreetingReceived(_) => {
                    self.state = SessionState::Handshaking;
                }
                crate::codec::decoder::ZmqEvent::ReadyReceived { .. } => {
                    if self.engine.is_ready() {
                        self.state = SessionState::Active;
                    }
                }
                crate::codec::decoder::ZmqEvent::MessageReceived(msg) => {
                    // Forward to socket via out_pipe
                    self.out_pipe.write_to_socket(msg, false);
                    self.out_pipe.flush_to_socket();
                }
                crate::codec::decoder::ZmqEvent::SubscribeReceived(prefix) => {
                    // Forward subscription to socket
                    let mut msg = ZmqMessage::from_slice(&prefix);
                    msg.set_more(false);
                    self.out_pipe.write_to_socket(msg, false);
                    self.out_pipe.flush_to_socket();
                }
                crate::codec::decoder::ZmqEvent::CancelReceived(prefix) => {
                    let mut msg = ZmqMessage::from_slice(&prefix);
                    msg.set_more(false);
                    self.out_pipe.write_to_socket(msg, false);
                    self.out_pipe.flush_to_socket();
                }
                crate::codec::decoder::ZmqEvent::HelloReceived(msg) => {
                    // Forward HELLO (welcome message) to socket
                    self.out_pipe.write_to_socket(msg, false);
                    self.out_pipe.flush_to_socket();
                }
                crate::codec::decoder::ZmqEvent::HiccupReceived => {
                    // Notify socket that subscriptions may have changed
                    // Socket should re-send subscriptions
                    self.out_pipe.write_to_socket(
                        ZmqMessage::from_slice(b"HICCUP"),
                        false,
                    );
                    self.out_pipe.flush_to_socket();
                }
                crate::codec::decoder::ZmqEvent::PingReceived(_) => {
                    // Auto-PONG
                    self.engine
                        .send_command(crate::codec::command::Command::pong(&[]));
                }
                crate::codec::decoder::ZmqEvent::PongReceived(_) => {
                    // PONG received — no action needed (caller tracks heartbeat)
                }
                crate::codec::decoder::ZmqEvent::DisconnectReceived => {
                    self.state = SessionState::Disconnecting;
                }
                crate::codec::decoder::ZmqEvent::Error(e) => {
                    self.state = SessionState::Terminated;
                    return Err(e);
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Pull messages from the inbound pipe (Socket → Session) and queue them
    /// for sending via the engine. Returns the wire bytes to send, if any.
    pub fn pull_and_encode(&mut self) -> Option<Vec<u8>> {
        // Check for pending messages from socket
        while self.in_pipe.check_read_from_socket() {
            if let Some(msg) = self.in_pipe.read_from_socket() {
                for (i, frame_data) in msg.frame_bytes_iter().enumerate() {
                    let is_last = i == msg.frame_count() - 1;
                    self.engine.send_message(frame_data, !is_last);
                }
            } else {
                break;
            }
        }
        self.engine.next_output()
    }

    /// Get the engine's next output bytes without pulling from pipes.
    pub fn engine_output(&mut self) -> Option<Vec<u8>> {
        self.engine.next_output()
    }

    /// Get the current session state (derived from engine).
    pub fn state(&self) -> SessionState {
        if self.engine.is_ready() {
            SessionState::Active
        } else if self.engine.is_stopped() {
            SessionState::Terminated
        } else if self.state == SessionState::Connecting {
            SessionState::Connecting
        } else {
            SessionState::Handshaking
        }
    }

    /// Whether the session is active.
    pub fn is_active(&self) -> bool {
        self.engine.is_ready()
    }

    /// Whether the session is terminated.
    pub fn is_terminated(&self) -> bool {
        self.state == SessionState::Terminated
    }

    /// Start graceful shutdown.
    pub fn disconnect(&mut self) {
        self.state = SessionState::Disconnecting;
        self.engine.stop();
    }

    /// Force termination.
    pub fn terminate(&mut self) {
        self.state = SessionState::Terminated;
        self.out_pipe.terminate();
        self.in_pipe.terminate();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_session_handshake() {
        let (out_pipe, _) = Pipe::new_pair(1);
        let (in_pipe, _) = Pipe::new_pair(2);

        let mut client = Session::new(out_pipe, in_pipe, true, "DEALER");
        assert_eq!(client.state(), SessionState::Connecting);

        // Get client greeting
        let greeting_bytes = client.engine_output().unwrap();
        assert!(!greeting_bytes.is_empty());

        // Create server session and feed the greeting
        let (s_out, _) = Pipe::new_pair(3);
        let (s_in, _) = Pipe::new_pair(4);
        let mut server = Session::new(s_out, s_in, false, "ROUTER");
        server.process_input(&greeting_bytes).unwrap();
        let server_greeting = server.engine_output().unwrap();

        // Feed server greeting to client
        client.process_input(&server_greeting).unwrap();
        let client_ready = client.engine_output().unwrap();

        // Feed client READY to server
        server.process_input(&client_ready).unwrap();
        let server_ready = server.engine_output().unwrap();

        // Feed server READY to client
        client.process_input(&server_ready).unwrap();

        assert!(client.is_active());
        assert!(server.is_active());
    }
}
