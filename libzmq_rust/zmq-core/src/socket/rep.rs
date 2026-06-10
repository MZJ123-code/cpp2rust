//! REP socket — receive request, send exactly one reply.
//! Replaces C++ `rep_t` (which inherits from `router_t`).
//!
//! ## State machine
//! ```text
//!   ReceivingRequest ──[xrecv(!more)]──> SendingReply
//!         ↑                                  │
//!         └──────────[xsend(!more)]──────────┘
//! ```
//!
//! ## Key behaviors
//! - Fair-queued reception: incoming requests are round-robined across all
//!   connected peers so no one peer starves.
//! - Routing prefix preservation: the routing frames (backtrace stack) from
//!   the incoming request are copied to the reply pipe so the response is
//!   routed back to the correct peer.
//! - Strict pairing: each request gets exactly one reply; must receive
//!   before sending.

use std::collections::HashMap;
use std::sync::Arc;

use crate::data_structures::fair_queue::FairQueue;
use crate::error::{ZmqError, ZmqResult};
use crate::message::ZmqMessage;
use crate::pipe::Pipe;
use crate::socket_type::SocketType;

use super::base::Socket;

/// REP socket internal state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RepState {
    /// Waiting to receive a request.
    ReceivingRequest,
    /// Received a complete request, ready to send a reply.
    SendingReply,
}

/// REP socket — request/reply server side.
///
/// Receives requests from connected peers via fair-queued round-robin and
/// sends exactly one reply back to the requesting peer.
pub struct RepSocket {
    /// All connected pipes, indexed by pipe ID.
    pipes: HashMap<usize, Arc<Pipe>>,
    /// Fair queue for round-robining across peers with pending data.
    fq: FairQueue,
    /// Current state machine position.
    state: RepState,
    /// Whether we are at the beginning of receiving a request.
    /// When true, the next frames we read are routing frames (backtrace stack)
    /// and must be copied to the reply pipe.
    request_begin: bool,
    /// Whether we are at the beginning of sending a reply.
    /// When true, the routing prefix needs to be written first.
    reply_begin: bool,
    /// The pipe we are currently receiving from / sending to.
    current_peer: Option<usize>,
    /// Stored routing prefix frames (copied from the incoming request).
    /// Written back to the pipe during reply send.
    routing_prefix: Vec<ZmqMessage>,
}

impl RepSocket {
    pub fn new() -> Self {
        Self {
            pipes: HashMap::new(),
            fq: FairQueue::new(),
            state: RepState::ReceivingRequest,
            request_begin: true,
            reply_begin: true,
            current_peer: None,
            routing_prefix: Vec::new(),
        }
    }

    /// Get the next message from the next active pipe in the fair queue.
    fn recv_from_fq(&mut self) -> ZmqResult<(usize, ZmqMessage)> {
        let pipe_id = self.fq.next_active().ok_or(ZmqError::NoMessage)?;

        if let Some(pipe) = self.pipes.get(&pipe_id) {
            if pipe.check_read_from_session() {
                if let Some(msg) = pipe.read_from_session() {
                    // If no more data on this pipe, mark it as inactive
                    if !pipe.check_read_from_session() {
                        self.fq.deactivated(pipe_id);
                    }
                    return Ok((pipe_id, msg));
                }
            }
            // Pipe was active but has no data — deactivate it
            self.fq.deactivated(pipe_id);
        }

        Err(ZmqError::NoMessage)
    }

    /// Recv from a specific pipe (used once we've established which peer
    /// we're receiving from).
    fn recv_from_pipe(&mut self, pipe_id: usize) -> ZmqResult<ZmqMessage> {
        if let Some(pipe) = self.pipes.get(&pipe_id) {
            if pipe.check_read_from_session() {
                if let Some(msg) = pipe.read_from_session() {
                    if !pipe.check_read_from_session() {
                        self.fq.deactivated(pipe_id);
                    }
                    return Ok(msg);
                }
            }
        }
        Err(ZmqError::NoMessage)
    }
}

impl Socket for RepSocket {
    fn xsend(&mut self, msg: ZmqMessage) -> ZmqResult<()> {
        // Must be in SendingReply state to send.
        if self.state != RepState::SendingReply {
            return Err(ZmqError::InvalidState(
                "REP: cannot send reply before receiving a request",
            ));
        }

        let peer_id = self.current_peer
            .ok_or(ZmqError::Internal("REP: no current peer during send".to_string()))?;

        let pipe = self.pipes.get(&peer_id)
            .ok_or(ZmqError::Internal("REP: current peer pipe not found".to_string()))?;

        let more = msg.more();

        // On the first send call, prepend the stored routing prefix.
        if self.reply_begin {
            for prefix_frame in &self.routing_prefix {
                pipe.write_to_session(prefix_frame.clone(), true);
            }
            pipe.flush_to_session();
            self.reply_begin = false;
        }

        // Send the reply body.
        pipe.write_to_session(msg, more);
        pipe.flush_to_session();

        // If the reply is complete, flip back to receiving state.
        if !more {
            self.state = RepState::ReceivingRequest;
            self.request_begin = true;
            self.reply_begin = true;
            self.current_peer = None;
            self.routing_prefix.clear();
        }

        Ok(())
    }

    fn xrecv(&mut self) -> ZmqResult<ZmqMessage> {
        // If we are in the middle of sending a reply, cannot receive.
        if self.state == RepState::SendingReply {
            return Err(ZmqError::InvalidState(
                "REP: cannot receive while sending reply",
            ));
        }

        // At the beginning of a request: read and copy the routing frames
        // (backtrace stack) so the reply can be routed back to the same peer.
        if self.request_begin {
            // Clear any previous routing prefix.
            self.routing_prefix.clear();

            // Read the first routing frame from the fair queue to determine
            // which peer we're serving.
            let (pipe_id, first_frame) =
                self.recv_from_fq().map_err(|_| ZmqError::NoMessage)?;
            self.current_peer = Some(pipe_id);

            // Process the first frame
            if first_frame.more() {
                let is_bottom = first_frame.size() == 0;
                self.routing_prefix.push(first_frame.clone());
                if is_bottom {
                    // Just a delimiter — no more routing frames.
                    self.request_begin = false;
                    // Fall through to read body
                }
            } else {
                return Err(ZmqError::Protocol(
                    "REP: malformed routing prefix — frame without more flag".into(),
                ));
            }

            // Read remaining routing frames from the SAME pipe
            // (not from fair queue — that would round-robin to a different peer)
            while self.request_begin {
                let frame = self.recv_from_pipe(pipe_id)
                    .map_err(|_| ZmqError::NoMessage)?;

                if frame.more() {
                    let is_bottom = frame.size() == 0;
                    self.routing_prefix.push(frame.clone());
                    if is_bottom {
                        self.request_begin = false;
                        // Fall through to read the body.
                    }
                } else {
                    return Err(ZmqError::Protocol(
                        "REP: malformed routing prefix — frame without more flag".into(),
                    ));
                }
            }
        }

        // Read the actual request body from the current peer.
        let peer_id = self.current_peer
            .ok_or(ZmqError::Internal("REP: no current peer during recv".to_string()))?;

        let msg = self.recv_from_pipe(peer_id)
            .map_err(|_| ZmqError::NoMessage)?;

        let more = msg.more();

        // If the request is fully received, flip to reply-sending state.
        if !more {
            self.state = RepState::SendingReply;
            self.request_begin = true;
        }

        Ok(msg)
    }

    fn xhas_in(&self) -> bool {
        // Can only receive when not sending a reply.
        if self.state == RepState::SendingReply {
            return false;
        }
        self.fq.has_in()
    }

    fn xhas_out(&self) -> bool {
        // Can only send when we have a reply to send.
        if self.state != RepState::SendingReply {
            return false;
        }
        // Check if the current peer's pipe is still active.
        if let Some(peer_id) = self.current_peer {
            if let Some(pipe) = self.pipes.get(&peer_id) {
                return pipe.is_active();
            }
        }
        false
    }

    fn attach_pipe(
        &mut self,
        pipe: Arc<Pipe>,
        _subscribe_to_all: bool,
        _locally_initiated: bool,
    ) {
        let pipe_id = pipe.id();
        self.pipes.insert(pipe_id, pipe);
        self.fq.attach(pipe_id);
    }

    fn pipe_terminated(&mut self, pipe: &Pipe) {
        let pipe_id = pipe.id();

        // If the terminated pipe was the current peer, reset state.
        if self.current_peer == Some(pipe_id) {
            self.current_peer = None;
            self.routing_prefix.clear();
            self.state = RepState::ReceivingRequest;
            self.request_begin = true;
            self.reply_begin = true;
        }

        self.pipes.remove(&pipe_id);
        self.fq.terminated(pipe_id);
    }

    fn read_activated(&mut self, pipe: &Pipe) {
        self.fq.activated(pipe.id());
    }

    fn write_activated(&mut self, _pipe: &Pipe) {
        // REP doesn't need write activation tracking for now.
        // The reply pipe is always available once selected.
    }

    fn socket_type(&self) -> SocketType {
        SocketType::Rep
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Generate unique pipe IDs for tests.
    static NEXT_PIPE_ID: AtomicUsize = AtomicUsize::new(100);

    fn next_pipe_id() -> usize {
        NEXT_PIPE_ID.fetch_add(2, Ordering::Relaxed)
    }

    /// Create a single pipe and attach it to the socket.
    /// Returns a reference to the same pipe that the socket holds.
    fn attach_single_pipe(socket: &mut RepSocket) -> Arc<Pipe> {
        let pid = next_pipe_id();
        let pipe = Pipe::new_pair(pid).0;
        socket.attach_pipe(pipe.clone(), false, true);
        pipe
    }

    /// Write data to the socket-side pipe's `to_socket` queue,
    /// simulating data arriving from the network via the session.
    /// The socket reads this data via `read_from_session()`.
    fn push_to_socket(pipe: &Pipe, msg: ZmqMessage) {
        pipe.write_to_socket(msg, false);
        pipe.flush_to_socket();
    }

    /// Read data from the socket-side pipe's `to_session` queue,
    /// simulating the session consuming data sent by the socket.
    /// The socket writes data via `write_to_session()`.
    fn pop_from_session(pipe: &Pipe) -> Option<ZmqMessage> {
        if pipe.check_read_from_socket() {
            pipe.read_from_socket()
        } else {
            None
        }
    }

    /// Helper: push a full request to the socket pipe.
    /// Format: [routing_frame, empty_delimiter, body]
    fn push_request(socket: &mut RepSocket, pipe: &Pipe, routing_prefix: &[u8], body: &[u8]) {
        // Activate fair queue
        socket.read_activated(pipe);

        // Routing frame (peer identity)
        let mut routing = ZmqMessage::from_slice(routing_prefix);
        routing.set_more(true);
        push_to_socket(pipe, routing);

        // Empty delimiter
        let mut delim = ZmqMessage::new();
        delim.set_more(true);
        push_to_socket(pipe, delim);

        // Body
        push_to_socket(pipe, ZmqMessage::from_slice(body));
    }

    #[test]
    fn test_initial_state() {
        let sock = RepSocket::new();
        assert!(!sock.xhas_in());
        assert!(!sock.xhas_out());
        assert_eq!(sock.socket_type(), SocketType::Rep);
    }

    #[test]
    fn test_send_before_recv_fails() {
        let mut sock = RepSocket::new();
        let msg = ZmqMessage::from_slice(b"reply");
        let result = sock.xsend(msg);
        assert!(result.is_err());
        match result.unwrap_err() {
            ZmqError::InvalidState(s) => assert!(s.contains("REP")),
            _ => panic!("expected InvalidState error"),
        }
    }

    #[test]
    fn test_recv_from_empty_fq() {
        let mut sock = RepSocket::new();
        let result = sock.xrecv();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ZmqError::NoMessage));
    }

    #[test]
    fn test_recv_receives_body_after_routing() {
        let mut sock = RepSocket::new();
        let pipe = attach_single_pipe(&mut sock);
        push_request(&mut sock, &pipe, b"peer1", b"hello");

        let msg = sock.xrecv().unwrap();
        assert_eq!(msg.data(), b"hello");
        assert!(matches!(sock.state, RepState::SendingReply));
    }

    #[test]
    fn test_recv_transitions_to_sending_reply() {
        let mut sock = RepSocket::new();
        let pipe = attach_single_pipe(&mut sock);
        push_request(&mut sock, &pipe, b"peer1", b"request");

        let msg = sock.xrecv().unwrap();
        assert_eq!(msg.data(), b"request");
        assert!(!msg.more());
        assert_eq!(sock.state, RepState::SendingReply);
        assert!(sock.xhas_out());
        assert!(!sock.xhas_in());
    }

    #[test]
    fn test_send_reply_transitions_back_to_recv() {
        let mut sock = RepSocket::new();
        let pipe = attach_single_pipe(&mut sock);
        push_request(&mut sock, &pipe, b"peer1", b"request");

        let request = sock.xrecv().unwrap();
        assert_eq!(request.data(), b"request");

        sock.xsend(ZmqMessage::from_slice(b"response")).unwrap();
        assert_eq!(sock.state, RepState::ReceivingRequest);
        assert!(!sock.xhas_out());
    }

    #[test]
    fn test_recv_recv_send_cycle() {
        let mut sock = RepSocket::new();
        let pipe = attach_single_pipe(&mut sock);

        // Cycle 1
        push_request(&mut sock, &pipe, b"peer1", b"req1");
        let req1 = sock.xrecv().unwrap();
        assert_eq!(req1.data(), b"req1");
        sock.xsend(ZmqMessage::from_slice(b"rep1")).unwrap();

        // Cycle 2
        push_request(&mut sock, &pipe, b"peer1", b"req2");
        let req2 = sock.xrecv().unwrap();
        assert_eq!(req2.data(), b"req2");
        sock.xsend(ZmqMessage::from_slice(b"rep2")).unwrap();
        assert_eq!(sock.state, RepState::ReceivingRequest);
    }

    #[test]
    fn test_cannot_recv_while_sending() {
        let mut sock = RepSocket::new();
        let pipe = attach_single_pipe(&mut sock);
        push_request(&mut sock, &pipe, b"peer1", b"request");
        sock.xrecv().unwrap(); // Now in SendingReply

        let result = sock.xrecv();
        assert!(result.is_err());
        match result.unwrap_err() {
            ZmqError::InvalidState(s) => assert!(s.contains("cannot receive")),
            _ => panic!("expected InvalidState error"),
        }
    }

    #[test]
    fn test_cannot_send_while_receiving() {
        let mut sock = RepSocket::new();
        attach_single_pipe(&mut sock);

        let result = sock.xsend(ZmqMessage::from_slice(b"reply"));
        assert!(result.is_err());
        match result.unwrap_err() {
            ZmqError::InvalidState(s) => assert!(s.contains("cannot send")),
            _ => panic!("expected InvalidState error"),
        }
    }

    #[test]
    fn test_routing_prefix_stored() {
        let mut sock = RepSocket::new();
        let pipe = attach_single_pipe(&mut sock);
        push_request(&mut sock, &pipe, b"peer42", b"request");

        let _request = sock.xrecv().unwrap();

        assert_eq!(sock.routing_prefix.len(), 2);
        assert_eq!(sock.routing_prefix[0].data(), b"peer42");
        assert_eq!(sock.routing_prefix[1].size(), 0);
        assert!(sock.routing_prefix[1].more());
    }

    #[test]
    fn test_reply_routed_correctly() {
        let mut sock = RepSocket::new();
        let pipe = attach_single_pipe(&mut sock);
        push_request(&mut sock, &pipe, b"peerX", b"ping");

        let req = sock.xrecv().unwrap();
        assert_eq!(req.data(), b"ping");

        sock.xsend(ZmqMessage::from_slice(b"pong")).unwrap();

        // Read from pipe's to_session (what the session would see)
        let routing_frame = pop_from_session(&pipe).unwrap();
        assert_eq!(routing_frame.data(), b"peerX");
        assert!(routing_frame.more());

        let delim = pop_from_session(&pipe).unwrap();
        assert_eq!(delim.size(), 0);
        assert!(delim.more());

        let body = pop_from_session(&pipe).unwrap();
        assert_eq!(body.data(), b"pong");
        assert!(!body.more());
    }

    #[test]
    fn test_pipe_terminated_resets_state() {
        let mut sock = RepSocket::new();
        let pipe = attach_single_pipe(&mut sock);
        push_request(&mut sock, &pipe, b"peer1", b"request");
        sock.xrecv().unwrap();
        assert_eq!(sock.state, RepState::SendingReply);

        sock.pipe_terminated(&pipe);
        assert_eq!(sock.state, RepState::ReceivingRequest);
        assert!(sock.current_peer.is_none());
        assert!(sock.routing_prefix.is_empty());
    }

    #[test]
    fn test_xhas_in_out_state_dependent() {
        let mut sock = RepSocket::new();
        let pipe = attach_single_pipe(&mut sock);

        assert!(!sock.xhas_in());
        assert!(!sock.xhas_out());

        push_request(&mut sock, &pipe, b"peer1", b"hello");
        assert!(sock.xhas_in());

        sock.xrecv().unwrap();
        assert!(!sock.xhas_in());
        assert!(sock.xhas_out());

        sock.xsend(ZmqMessage::from_slice(b"world")).unwrap();
        assert!(!sock.xhas_out());
    }

    #[test]
    fn test_multi_part_request() {
        let mut sock = RepSocket::new();
        let pipe = attach_single_pipe(&mut sock);

        // Routing prefix
        sock.read_activated(&pipe);
        let mut routing = ZmqMessage::from_slice(b"peerA");
        routing.set_more(true);
        push_to_socket(&pipe, routing);

        let mut delim = ZmqMessage::new();
        delim.set_more(true);
        push_to_socket(&pipe, delim);

        // Multi-part body
        let mut part1 = ZmqMessage::from_slice(b"part1");
        part1.set_more(true);
        push_to_socket(&pipe, part1);

        let part2 = ZmqMessage::from_slice(b"part2");
        push_to_socket(&pipe, part2);

        let r1 = sock.xrecv().unwrap();
        assert_eq!(r1.data(), b"part1");
        assert!(r1.more());
        assert_eq!(sock.state, RepState::ReceivingRequest);

        let r2 = sock.xrecv().unwrap();
        assert_eq!(r2.data(), b"part2");
        assert!(!r2.more());
        assert_eq!(sock.state, RepState::SendingReply);
    }

    #[test]
    fn test_multi_part_reply() {
        let mut sock = RepSocket::new();
        let pipe = attach_single_pipe(&mut sock);
        push_request(&mut sock, &pipe, b"peerB", b"request");
        sock.xrecv().unwrap();

        // Multi-part reply
        let mut part1 = ZmqMessage::from_slice(b"reply_part1");
        part1.set_more(true);
        sock.xsend(part1).unwrap();
        assert_eq!(sock.state, RepState::SendingReply);

        let part2 = ZmqMessage::from_slice(b"reply_part2");
        sock.xsend(part2).unwrap();
        assert_eq!(sock.state, RepState::ReceivingRequest);

        // Verify reply was sent with prefix
        let _rid = pop_from_session(&pipe).unwrap(); // routing frame
        let _d = pop_from_session(&pipe).unwrap(); // delimiter
        let rp1 = pop_from_session(&pipe).unwrap();
        assert_eq!(rp1.data(), b"reply_part1");
        assert!(rp1.more());
        let rp2 = pop_from_session(&pipe).unwrap();
        assert_eq!(rp2.data(), b"reply_part2");
        assert!(!rp2.more());
    }

    #[test]
    fn test_fair_queue_round_robins_peers() {
        let mut sock = RepSocket::new();
        let pipe_a = attach_single_pipe(&mut sock);
        let pipe_b = attach_single_pipe(&mut sock);

        push_request(&mut sock, &pipe_a, b"peerA", b"reqA");
        push_request(&mut sock, &pipe_b, b"peerB", b"reqB");

        let req1 = sock.xrecv().unwrap();
        assert_eq!(req1.data(), b"reqA");
        sock.xsend(ZmqMessage::from_slice(b"repA")).unwrap();

        let req2 = sock.xrecv().unwrap();
        assert_eq!(req2.data(), b"reqB");
        sock.xsend(ZmqMessage::from_slice(b"repB")).unwrap();

        assert_eq!(sock.state, RepState::ReceivingRequest);
    }

    #[test]
    fn test_empty_routing_prefix() {
        let mut sock = RepSocket::new();
        let pipe = attach_single_pipe(&mut sock);

        // Only empty delimiter as routing prefix
        sock.read_activated(&pipe);
        let mut delim = ZmqMessage::new();
        delim.set_more(true);
        push_to_socket(&pipe, delim);

        push_to_socket(&pipe, ZmqMessage::from_slice(b"minimal"));

        let req = sock.xrecv().unwrap();
        assert_eq!(req.data(), b"minimal");

        assert_eq!(sock.routing_prefix.len(), 1);
        assert_eq!(sock.routing_prefix[0].size(), 0);
    }

    #[test]
    fn test_recv_without_data() {
        let mut sock = RepSocket::new();
        attach_single_pipe(&mut sock);

        let result = sock.xrecv();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ZmqError::NoMessage));
    }
}
