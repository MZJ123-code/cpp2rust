//! PEER socket — bidirectional peer-to-peer with routing (draft API).
//!
//! Replaces C++ `peer_t`. Extends SERVER socket behavior.
//! Can both send and receive from any connected peer, using routing IDs.
//! Supports dynamic peer connections.

use std::collections::HashMap;
use std::sync::Arc;

use crate::data_structures::fair_queue::FairQueue;
use crate::error::{ZmqError, ZmqResult};
use crate::message::ZmqMessage;
use crate::pipe::Pipe;
use crate::socket_type::SocketType;
use super::base::Socket;

/// PEER socket — bidirectional peer-to-peer with routing IDs.
pub struct PeerSocket {
    /// Fair queue for inbound pipes.
    fq: FairQueue,
    /// Outbound pipes indexed by routing ID (pipe, active).
    out_pipes: HashMap<u32, (Arc<Pipe>, bool)>,
    /// Next routing ID to assign.
    next_routing_id: u32,
    /// Last routing ID assigned (for connect_peer tracking).
    last_routing_id: u32,
}

impl PeerSocket {
    pub fn new() -> Self {
        Self {
            fq: FairQueue::new(),
            out_pipes: HashMap::new(),
            next_routing_id: 1, // never zero
            last_routing_id: 0,
        }
    }
}

impl Socket for PeerSocket {
    fn xsend(&mut self, msg: ZmqMessage) -> ZmqResult<()> {
        // PEER sockets do not allow multipart data
        if msg.more() {
            return Err(ZmqError::InvalidState("PEER: multipart not allowed"));
        }

        let routing_id = msg.routing_id().ok_or(
            ZmqError::InvalidArgument("PEER: message has no routing ID".into()),
        )?;

        let (pipe, active) = self
            .out_pipes
            .get_mut(&routing_id)
            .ok_or(ZmqError::HostUnreachable)?;

        if !pipe.is_active() || !pipe.check_read_from_socket() {
            *active = false;
            return Err(ZmqError::WouldBlock);
        }

        pipe.write_to_session(msg, false);
        pipe.flush_to_session();
        Ok(())
    }

    fn xrecv(&mut self) -> ZmqResult<ZmqMessage> {
        if !self.fq.has_in() {
            return Err(ZmqError::NoMessage);
        }
        // In full impl: read from fq, drop multipart, set routing_id
        self.fq.next_active().ok_or(ZmqError::NoMessage)?;
        Err(ZmqError::NoMessage)
    }

    fn xhas_in(&self) -> bool {
        self.fq.has_in()
    }

    fn xhas_out(&self) -> bool {
        true
    }

    fn attach_pipe(&mut self, pipe: Arc<Pipe>, _subscribe_to_all: bool, _locally_initiated: bool) {
        let mut routing_id = self.next_routing_id;
        self.next_routing_id = self.next_routing_id.wrapping_add(1);
        if self.next_routing_id == 0 {
            self.next_routing_id = 1;
        }
        if routing_id == 0 {
            routing_id = self.next_routing_id;
            self.next_routing_id = self.next_routing_id.wrapping_add(1);
        }

        self.last_routing_id = routing_id;
        self.out_pipes.insert(routing_id, (Arc::clone(&pipe), true));
        self.fq.attach(pipe.id());
    }

    fn pipe_terminated(&mut self, pipe: &Pipe) {
        self.out_pipes.retain(|_rid, (p, _active)| p.id() != pipe.id());
        self.fq.terminated(pipe.id());
    }

    fn read_activated(&mut self, pipe: &Pipe) {
        self.fq.activated(pipe.id());
    }

    fn write_activated(&mut self, pipe: &Pipe) {
        for (_rid, (p, active)) in self.out_pipes.iter_mut() {
            if p.id() == pipe.id() {
                *active = true;
                break;
            }
        }
    }

    fn socket_type(&self) -> SocketType {
        SocketType::Peer
    }
}
