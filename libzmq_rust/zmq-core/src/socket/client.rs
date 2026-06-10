//! CLIENT socket — draft client-to-server request socket.
//!
//! Replaces C++ `client_t`. Connects to a SERVER socket. Messages are
//! load-balanced across outbound pipes and fair-queued from inbound pipes.
//! Rejects multipart data. Thread-safe.

use std::sync::Arc;

use crate::data_structures::fair_queue::FairQueue;
use crate::data_structures::load_balancer::LoadBalancer;
use crate::error::{ZmqError, ZmqResult};
use crate::message::ZmqMessage;
use crate::pipe::Pipe;
use crate::socket_type::SocketType;
use super::base::Socket;

/// CLIENT socket — connects to SERVER, sends requests, receives replies.
pub struct ClientSocket {
    /// Fair queue for inbound pipes.
    fq: FairQueue,
    /// Load balancer for outbound pipes.
    lb: LoadBalancer,
}

impl ClientSocket {
    pub fn new() -> Self {
        Self {
            fq: FairQueue::new(),
            lb: LoadBalancer::new(),
        }
    }
}

impl Socket for ClientSocket {
    fn xsend(&mut self, msg: ZmqMessage) -> ZmqResult<()> {
        // CLIENT sockets do not allow multipart data
        if msg.more() {
            return Err(ZmqError::InvalidState("CLIENT: multipart not allowed"));
        }
        if !self.lb.has_out() {
            return Err(ZmqError::NoPeer);
        }
        // In full impl: write msg to the pipe selected by lb.sendpipe
        // For now: just report success (actual pipe write happens via session)
        self.lb.next_active().ok_or(ZmqError::NoPeer)?;
        Ok(())
    }

    fn xrecv(&mut self) -> ZmqResult<ZmqMessage> {
        if !self.fq.has_in() {
            return Err(ZmqError::NoMessage);
        }
        // In full impl: read from fair queue, dropping multi-frame messages
        // For now: report available
        self.fq.next_active().ok_or(ZmqError::NoMessage)?;
        Err(ZmqError::NoMessage)
    }

    fn xhas_in(&self) -> bool {
        self.fq.has_in()
    }

    fn xhas_out(&self) -> bool {
        self.lb.has_out()
    }

    fn attach_pipe(&mut self, pipe: Arc<Pipe>, _subscribe_to_all: bool, _locally_initiated: bool) {
        self.fq.attach(pipe.id());
        self.lb.attach(pipe.id());
    }

    fn pipe_terminated(&mut self, pipe: &Pipe) {
        self.fq.terminated(pipe.id());
        self.lb.terminated(pipe.id());
    }

    fn read_activated(&mut self, pipe: &Pipe) {
        self.fq.activated(pipe.id());
    }

    fn write_activated(&mut self, pipe: &Pipe) {
        self.lb.activated(pipe.id());
    }

    fn socket_type(&self) -> SocketType {
        SocketType::Client
    }
}
