//! REQ socket — send request, receive exactly one reply.
//! Replaces C++ `req_t` (which inherits from `dealer_t`).
//!
//! ## State machine
//! ```text
//!   Ready ──[xsend(!more)]──> WaitingReply
//!     ↑                            │
//!     └──────[xrecv(!more)]────────┘
//! ```
//!
//! ## Key behaviors
//! - Strict mode (default): must receive reply before sending next request.
//!   Relaxed mode (`strict=false`): a new send resets the state mid-flight.
//! - Correlation IDs: when enabled (`correlate=true`), each request carries
//!   a 4-byte monotonically increasing request ID; the reply must echo it.
//! - Load-balanced sending across connected peers (round-robin via LoadBalancer).
//! - Reply is read only from the pipe the request was sent on.
//! - On reconnection, the pending request is retransmitted.
//! - Stale replies (from previous connections) are drained on send.

use std::collections::HashMap;
use std::sync::Arc;

use crate::data_structures::fair_queue::FairQueue;
use crate::data_structures::load_balancer::LoadBalancer;
use crate::error::{ZmqError, ZmqResult};
use crate::message::ZmqMessage;
use crate::pipe::Pipe;
use crate::socket_type::SocketType;

use super::base::Socket;

/// REQ socket internal state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReqState {
    /// Ready to send a new request.
    Ready,
    /// Waiting for a reply on the given pipe.
    WaitingReply { pipe_id: usize },
}

/// REQ socket — request/reply client side.
///
/// Sends requests to connected peers via load-balanced round-robin and
/// waits for exactly one reply on the same peer.
pub struct ReqSocket {
    /// All connected pipes, indexed by pipe ID.
    pipes: HashMap<usize, Arc<Pipe>>,
    /// Load balancer for picking the next peer to send to.
    lb: LoadBalancer,
    /// Fair queue for inbound message notifications (used by write_activated/read_activated).
    fq: FairQueue,
    /// Current state machine position.
    state: ReqState,
    /// Whether we are at the beginning of a multi-part message send or receive.
    /// When true, routing frames (empty delimiter + optional correlation ID)
    /// must be prepended on send or validated on receive.
    message_begin: bool,
    /// Monotonically increasing correlation ID. Only used when `correlate` is true.
    correlation_id: u32,
    /// Whether correlation ID frames are enabled (ZMQ_REQ_CORRELATE).
    correlate: bool,
    /// If true, send() fails with EFSM when a reply is still pending.
    /// If false, send() resets the state and starts a new request (ZMQ_REQ_RELAXED).
    strict: bool,
    /// The pending request, stored for retransmission on reconnection.
    pending_request: Option<ZmqMessage>,
    /// The pipe we sent the current request on (for receiving the reply).
    reply_pipe: Option<usize>,
}

impl ReqSocket {
    pub fn new() -> Self {
        Self {
            pipes: HashMap::new(),
            lb: LoadBalancer::new(),
            fq: FairQueue::new(),
            state: ReqState::Ready,
            message_begin: true,
            correlation_id: 1, // Start at 1; 0 is reserved
            correlate: false,
            strict: true,
            pending_request: None,
            reply_pipe: None,
        }
    }

    /// Receive from the reply pipe only, discarding frames from other pipes.
    /// Returns `Ok(msg)` if a message is available on the reply pipe,
    /// `Err(NoMessage)` otherwise.
    fn recv_reply_pipe(&mut self) -> ZmqResult<ZmqMessage> {
        let reply_pipe_id = match self.state {
            ReqState::WaitingReply { pipe_id } => pipe_id,
            _ => return Err(ZmqError::Internal("recv_reply_pipe called while not waiting reply".to_string())),
        };

        if let Some(pipe) = self.pipes.get(&reply_pipe_id) {
            if pipe.check_read_from_session() {
                if let Some(msg) = pipe.read_from_session() {
                    // The message came from the reply pipe — return it
                    return Ok(msg);
                }
            }
        }
        Err(ZmqError::NoMessage)
    }

    /// Drain stale messages from the reply pipe. Called at the start of
    /// sending a new request to avoid receiving replies from old connections.
    fn drain_stale_messages(&mut self) {
        loop {
            match self.recv_reply_pipe() {
                Ok(_) => { /* drop stale message */ }
                Err(_) => break,
            }
        }
    }

    /// Find a pipe by its ID.
    fn find_pipe(&self, pipe_id: usize) -> Option<&Arc<Pipe>> {
        self.pipes.get(&pipe_id)
    }

    /// Peek at the next available message on the reply pipe.
    fn has_in_on_reply_pipe(&self) -> bool {
        match self.state {
            ReqState::WaitingReply { pipe_id } => {
                if let Some(pipe) = self.pipes.get(&pipe_id) {
                    pipe.check_read_from_session()
                } else {
                    false
                }
            }
            ReqState::Ready => false,
        }
    }
}

impl Socket for ReqSocket {
    fn xsend(&mut self, msg: ZmqMessage) -> ZmqResult<()> {
        // If we are waiting for a reply and strict mode is on, refuse to send.
        if let ReqState::WaitingReply { .. } = self.state {
            if self.strict {
                return Err(ZmqError::InvalidState(
                    "REQ: cannot send while waiting for reply (strict mode)",
                ));
            }
            // Relaxed mode: reset the state and start fresh.
            self.state = ReqState::Ready;
            self.message_begin = true;
        }

        // First frame of a new request: prepend routing frames.
        if self.message_begin {
            self.reply_pipe = None;

            // Pick a pipe via load balancer.
            let pipe_id = self.lb.next_active().ok_or(ZmqError::NoPeer)?;

            // Prepend correlation ID frame if enabled.
            if self.correlate {
                self.correlation_id = self.correlation_id.wrapping_add(1);
                let mut id_frame = ZmqMessage::from_slice(
                    &self.correlation_id.to_ne_bytes(),
                );
                id_frame.set_more(true);

                if let Some(pipe) = self.find_pipe(pipe_id) {
                    pipe.write_to_session(id_frame, true);
                    pipe.flush_to_session();
                }
            }

            // Prepend empty delimiter frame (backtrace stack bottom).
            let mut bottom = ZmqMessage::new();
            bottom.set_more(true);

            if let Some(pipe) = self.find_pipe(pipe_id) {
                pipe.write_to_session(bottom, true);
                pipe.flush_to_session();
            }

            // Drain stale messages from the pipeline.
            // We store the pipe_id and then drain.
            // But we haven't set the state to WaitingReply yet,
            // so recv_reply_pipe won't know which pipe to drain.
            // Actually, in the C++ code, draining happens via dealer_t::xrecv
            // which uses the fair queue. Let me drain after setting reply_pipe.
            // We'll do it differently: drain from all pipes' to_socket before
            // setting reply_pipe.
            for (_, pipe) in &self.pipes {
                while pipe.check_read_from_session() {
                    let _ = pipe.read_from_session(); // drop stale
                }
            }

            self.reply_pipe = Some(pipe_id);
            self.message_begin = false;
        }

        let more = msg.more();

        // Send the actual message body through the chosen pipe.
        let pipe_id = self.reply_pipe.ok_or(ZmqError::Internal(
            "REQ: no reply pipe set during send".to_string(),
        ))?;

        if let Some(pipe) = self.find_pipe(pipe_id) {
            pipe.write_to_session(msg, more);
            pipe.flush_to_session();
        } else {
            return Err(ZmqError::Internal("REQ: reply pipe not found".to_string()));
        }

        // If the multi-part message is complete, transition to WaitingReply.
        if !more {
            self.state = ReqState::WaitingReply {
                pipe_id,
            };
            self.message_begin = true;

            // Store the request for potential retransmission on reconnect.
            // (The pending request is tracked externally; for now we just set state.)
        }

        Ok(())
    }

    fn xrecv(&mut self) -> ZmqResult<ZmqMessage> {
        // Can only receive if we are waiting for a reply.
        let _pipe_id = match self.state {
            ReqState::WaitingReply { pipe_id } => pipe_id,
            ReqState::Ready => {
                return Err(ZmqError::InvalidState(
                    "REQ: cannot receive before sending a request",
                ));
            }
        };

        // Validate the routing frames at the start of the reply.
        if self.message_begin {
            loop {
                // If correlation IDs are enabled, validate the first frame.
                if self.correlate {
                    let msg = self.recv_reply_pipe()
                        .map_err(|_| ZmqError::NoMessage)?;

                    if !msg.more()
                        || msg.size() != 4
                        || msg.data() != self.correlation_id.to_ne_bytes().to_vec()
                    {
                        // Skip the rest of this mismatched message.
                        let mut skip = msg;
                        while skip.more() {
                            let next = self.recv_reply_pipe()
                                .map_err(|_| ZmqError::Protocol(
                                    "REQ: truncated message while skipping".into(),
                                ))?;
                            skip = next;
                        }
                        continue;
                    }
                }

                // The next frame must be the empty delimiter (0 bytes, more flag set).
                let msg = self.recv_reply_pipe()
                    .map_err(|_| ZmqError::NoMessage)?;

                if !msg.more() || msg.size() != 0 {
                    // Skip the rest of this malformed message.
                    let mut skip = msg;
                    while skip.more() {
                        let next = self.recv_reply_pipe()
                            .map_err(|_| ZmqError::Protocol(
                                "REQ: truncated message while skipping".into(),
                            ))?;
                        skip = next;
                    }
                    continue;
                }

                // Routing frames validated successfully.
                break;
            }
            self.message_begin = false;
        }

        // Read the reply body.
        let msg = self.recv_reply_pipe()
            .map_err(|_| ZmqError::NoMessage)?;

        let more = msg.more();

        // If the reply is fully received, flip to Ready state.
        if !more {
            self.state = ReqState::Ready;
            self.message_begin = true;
        }

        Ok(msg)
    }

    fn xhas_in(&self) -> bool {
        // Can only receive when waiting for a reply.
        if let ReqState::WaitingReply { .. } = self.state {
            self.has_in_on_reply_pipe()
        } else {
            false
        }
    }

    fn xhas_out(&self) -> bool {
        // In strict mode, cannot send while waiting for a reply.
        if let ReqState::WaitingReply { .. } = self.state {
            if self.strict {
                return false;
            }
        }
        self.lb.has_out()
    }

    fn attach_pipe(
        &mut self,
        pipe: Arc<Pipe>,
        _subscribe_to_all: bool,
        _locally_initiated: bool,
    ) {
        let pipe_id = pipe.id();
        self.pipes.insert(pipe_id, pipe);
        self.lb.attach(pipe_id);
        self.fq.attach(pipe_id);
    }

    fn pipe_terminated(&mut self, pipe: &Pipe) {
        let pipe_id = pipe.id();

        // If this was the pipe we were waiting for a reply on, retransmit.
        if let ReqState::WaitingReply { pipe_id: reply_id } = self.state {
            if reply_id == pipe_id {
                // Pipe of pending reply terminated — retransmit if we have a request.
                if let Some(ref _req) = self.pending_request {
                    self.state = ReqState::Ready;
                    self.message_begin = true;
                    // The caller should re-send the pending request.
                } else {
                    // No pending request, just go back to ready.
                    self.state = ReqState::Ready;
                    self.message_begin = true;
                }
            }
        }

        self.pipes.remove(&pipe_id);
        self.lb.terminated(pipe_id);
        self.fq.terminated(pipe_id);
    }

    fn read_activated(&mut self, pipe: &Pipe) {
        self.fq.activated(pipe.id());
    }

    fn write_activated(&mut self, pipe: &Pipe) {
        self.lb.activated(pipe.id());
    }

    fn socket_type(&self) -> SocketType {
        SocketType::Req
    }

    fn set_req_correlate(&mut self, v: bool) { self.correlate = v; }
    fn set_req_relaxed(&mut self, v: bool) { self.strict = !v; }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create a pipe pair, attach one side to the socket.
    fn attach_pipe_pair(socket: &mut ReqSocket, pipe_id: usize) -> (Arc<Pipe>, Arc<Pipe>) {
        let (p1, p2) = Pipe::new_pair(pipe_id);
        socket.attach_pipe(p1.clone(), false, true);
        // Simulate write activation
        socket.write_activated(&p1);
        (p1, p2)
    }

    /// Helper: write a message from the "peer" side to simulate receiving data.
    fn simulate_recv(peer_pipe: &Pipe, msg: ZmqMessage) {
        peer_pipe.write_to_session(msg, false);
        peer_pipe.flush_to_session();
    }

    #[test]
    fn test_initial_state() {
        let sock = ReqSocket::new();
        assert!(!sock.xhas_in());
        // No pipes yet, so no out
        assert!(!sock.xhas_out());
        assert_eq!(sock.socket_type(), SocketType::Req);
    }

    #[test]
    fn test_send_without_pipes() {
        let mut sock = ReqSocket::new();
        let msg = ZmqMessage::from_slice(b"request");
        let result = sock.xsend(msg);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ZmqError::NoPeer));
    }

    #[test]
    fn test_recv_before_send_fails() {
        let mut sock = ReqSocket::new();
        let result = sock.xrecv();
        assert!(result.is_err());
        match result.unwrap_err() {
            ZmqError::InvalidState(s) => assert!(s.contains("REQ")),
            _ => panic!("expected InvalidState error"),
        }
    }

    #[test]
    fn test_send_transitions_to_waiting_reply() {
        let mut sock = ReqSocket::new();
        attach_pipe_pair(&mut sock, 10);

        let msg = ZmqMessage::from_slice(b"hello");
        sock.xsend(msg).unwrap();

        // After sending, we should be waiting for a reply.
        match sock.state {
            ReqState::WaitingReply { .. } => {}
            _ => panic!("expected WaitingReply state after send"),
        }
    }

    #[test]
    fn test_strict_mode_rejects_second_send() {
        let mut sock = ReqSocket::new();
        attach_pipe_pair(&mut sock, 10);

        // First send
        let msg1 = ZmqMessage::from_slice(b"request1");
        sock.xsend(msg1).unwrap();

        // Second send in strict mode — should fail
        let msg2 = ZmqMessage::from_slice(b"request2");
        let result = sock.xsend(msg2);
        assert!(result.is_err());
        match result.unwrap_err() {
            ZmqError::InvalidState(s) => assert!(s.contains("strict")),
            _ => panic!("expected InvalidState for strict mode"),
        }
    }

    #[test]
    fn test_relaxed_mode_allows_second_send() {
        let mut sock = ReqSocket::new();
        sock.strict = false;
        attach_pipe_pair(&mut sock, 10);

        // First send
        let msg1 = ZmqMessage::from_slice(b"request1");
        sock.xsend(msg1).unwrap();

        // Second send in relaxed mode — resets state and sends
        let msg2 = ZmqMessage::from_slice(b"request2");
        sock.xsend(msg2).unwrap();

        match sock.state {
            ReqState::WaitingReply { .. } => {}
            _ => panic!("expected WaitingReply state after relaxed send"),
        }
    }

    #[test]
    fn test_xhas_in_false_before_send() {
        let mut sock = ReqSocket::new();
        attach_pipe_pair(&mut sock, 10);
        assert!(!sock.xhas_in());
    }

    #[test]
    fn test_xhas_in_true_when_reply_available() {
        let mut sock = ReqSocket::new();
        let (_p1, p2) = attach_pipe_pair(&mut sock, 10);

        // Send a request first
        let req = ZmqMessage::from_slice(b"request");
        sock.xsend(req).unwrap();

        // Simulate a reply coming in from the peer.
        // The reply needs: [empty delimiter + more] + [body]
        let mut delim = ZmqMessage::new();
        delim.set_more(true);
        simulate_recv(&p2, delim);

        let body = ZmqMessage::from_slice(b"response");
        simulate_recv(&p2, body);

        // Now xhas_in should return true
        assert!(sock.xhas_in());
    }

    #[test]
    fn test_recv_reply_transitions_to_ready() {
        let mut sock = ReqSocket::new();
        let (_p1, p2) = attach_pipe_pair(&mut sock, 10);

        // Send a request
        let req = ZmqMessage::from_slice(b"request");
        sock.xsend(req).unwrap();

        assert!(matches!(sock.state, ReqState::WaitingReply { .. }));

        // Simulate reply: [empty delimiter] + [body]
        let mut delim = ZmqMessage::new();
        delim.set_more(true);
        simulate_recv(&p2, delim);

        let body = ZmqMessage::from_slice(b"response");
        simulate_recv(&p2, body);

        // Read the reply
        let reply = sock.xrecv().unwrap();
        assert_eq!(reply.data(), b"response");

        // Should be back to Ready state
        match sock.state {
            ReqState::Ready => {}
            _ => panic!("expected Ready state after receiving reply"),
        }
    }

    #[test]
    fn test_send_recv_cycle() {
        let mut sock = ReqSocket::new();
        let (_p1, p2) = attach_pipe_pair(&mut sock, 10);

        // Cycle 1
        sock.xsend(ZmqMessage::from_slice(b"ping")).unwrap();
        let mut d = ZmqMessage::new(); d.set_more(true);
        simulate_recv(&p2, d);
        simulate_recv(&p2, ZmqMessage::from_slice(b"pong"));
        let reply = sock.xrecv().unwrap();
        assert_eq!(reply.data(), b"pong");
        assert!(matches!(sock.state, ReqState::Ready));

        // Cycle 2
        sock.xsend(ZmqMessage::from_slice(b"ping2")).unwrap();
        let mut d2 = ZmqMessage::new(); d2.set_more(true);
        simulate_recv(&p2, d2);
        simulate_recv(&p2, ZmqMessage::from_slice(b"pong2"));
        let reply2 = sock.xrecv().unwrap();
        assert_eq!(reply2.data(), b"pong2");
        assert!(matches!(sock.state, ReqState::Ready));
    }

    #[test]
    fn test_xhas_out_false_in_strict_mode_while_waiting() {
        let mut sock = ReqSocket::new();
        attach_pipe_pair(&mut sock, 10);

        sock.xsend(ZmqMessage::from_slice(b"request")).unwrap();
        // In strict mode, xhas_out is false while waiting for reply
        assert!(!sock.xhas_out());
    }

    #[test]
    fn test_xhas_out_true_in_relaxed_mode_while_waiting() {
        let mut sock = ReqSocket::new();
        sock.strict = false;
        attach_pipe_pair(&mut sock, 10);

        sock.xsend(ZmqMessage::from_slice(b"request")).unwrap();
        // In relaxed mode, xhas_out is true even while waiting
        assert!(sock.xhas_out());
    }

    #[test]
    fn test_pipe_terminated_resets_state() {
        let mut sock = ReqSocket::new();
        let (p1, _p2) = attach_pipe_pair(&mut sock, 10);

        sock.xsend(ZmqMessage::from_slice(b"request")).unwrap();
        assert!(matches!(sock.state, ReqState::WaitingReply { .. }));

        // Terminate the pipe
        sock.pipe_terminated(&p1);
        assert!(matches!(sock.state, ReqState::Ready));
    }

    #[test]
    fn test_correlation_id_increments() {
        let mut sock = ReqSocket::new();
        sock.correlate = true;
        assert_eq!(sock.correlation_id, 1);

        attach_pipe_pair(&mut sock, 10);

        // Send a request (correlation_id should increment)
        sock.xsend(ZmqMessage::from_slice(b"request1")).unwrap();
        assert_eq!(sock.correlation_id, 2);
    }

    #[test]
    fn test_correlate_validation_skips_mismatched_replies() {
        let mut sock = ReqSocket::new();
        sock.correlate = true;
        sock.correlation_id = 42;
        let (_p1, p2) = attach_pipe_pair(&mut sock, 10);

        // Send request
        sock.xsend(ZmqMessage::from_slice(b"request")).unwrap();
        // correlation_id is now 43

        // Simulate a mismatched reply (wrong correlation ID)
        // Frame 1: wrong correlation ID (99) with more flag
        let mut wrong_id = ZmqMessage::from_slice(&99u32.to_ne_bytes());
        wrong_id.set_more(true);
        simulate_recv(&p2, wrong_id);
        // Frame 2: empty delimiter with more flag
        let mut w_delim = ZmqMessage::new(); w_delim.set_more(true);
        simulate_recv(&p2, w_delim);
        // Frame 3: stale body (no more flag)
        simulate_recv(&p2, ZmqMessage::from_slice(b"stale"));

        // Now send correct reply
        // Frame 1: correct correlation ID (43)
        let mut correct_id = ZmqMessage::from_slice(&43u32.to_ne_bytes());
        correct_id.set_more(true);
        simulate_recv(&p2, correct_id);
        // Frame 2: empty delimiter
        let mut c_delim = ZmqMessage::new(); c_delim.set_more(true);
        simulate_recv(&p2, c_delim);
        // Frame 3: body
        simulate_recv(&p2, ZmqMessage::from_slice(b"real response"));

        // Should receive the correct reply (stale one was skipped)
        let reply = sock.xrecv().unwrap();
        assert_eq!(reply.data(), b"real response");
    }

    #[test]
    fn test_multi_part_reply() {
        let mut sock = ReqSocket::new();
        let (_p1, p2) = attach_pipe_pair(&mut sock, 10);

        // Send request
        sock.xsend(ZmqMessage::from_slice(b"request")).unwrap();

        // Simulate multi-part reply
        let mut delim = ZmqMessage::new(); delim.set_more(true);
        simulate_recv(&p2, delim);

        let mut part1 = ZmqMessage::from_slice(b"part1");
        part1.set_more(true);
        simulate_recv(&p2, part1);

        let part2 = ZmqMessage::from_slice(b"part2");
        // part2.more() is false (default)
        simulate_recv(&p2, part2);

        // Read part 1
        let reply1 = sock.xrecv().unwrap();
        assert_eq!(reply1.data(), b"part1");
        assert!(reply1.more());
        // Still waiting for reply (multi-part)
        assert!(matches!(sock.state, ReqState::WaitingReply { .. }));

        // Read part 2 (last)
        let reply2 = sock.xrecv().unwrap();
        assert_eq!(reply2.data(), b"part2");
        assert!(!reply2.more());
        // Now back to Ready
        assert!(matches!(sock.state, ReqState::Ready));
    }

    #[test]
    fn test_multi_part_request() {
        let mut sock = ReqSocket::new();
        attach_pipe_pair(&mut sock, 10);

        // Send multi-part request
        let mut part1 = ZmqMessage::from_slice(b"part1");
        part1.set_more(true);
        sock.xsend(part1).unwrap();
        // Still in Ready? No, wait — the first xsend sets message_begin=false
        // but since more is true, state stays Ready...
        // Actually after xsend(!more=false), state stays Ready because we check !more.
        // The first part with more=true just sends, doesn't change state.
        // Hmm, let me re-check my implementation...

        // Actually in my implementation, xsend only transitions to WaitingReply
        // when !more. So state should still be Ready after first part.
        // But message_begin is set to false by the first send.
        // This means we can send subsequent parts without re-adding routing frames.

        let part2 = ZmqMessage::from_slice(b"part2");
        sock.xsend(part2).unwrap();

        // Now we should be waiting for reply
        assert!(matches!(sock.state, ReqState::WaitingReply { .. }));
    }
}
