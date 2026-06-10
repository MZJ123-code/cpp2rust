//! SCATTER socket — distributes messages round-robin (draft API).
//!
//! Replaces C++ `scatter_t`. Load-balances outbound messages across all
//! attached pipes. Like PUSH but with multipart rejection.

use std::sync::Arc;

use crate::data_structures::load_balancer::LoadBalancer;
use crate::error::{ZmqError, ZmqResult};
use crate::message::ZmqMessage;
use crate::pipe::Pipe;
use crate::socket_type::SocketType;
use super::base::Socket;

/// SCATTER socket — load-balanced outbound distributor.
pub struct ScatterSocket {
    lb: LoadBalancer,
}

impl ScatterSocket {
    pub fn new() -> Self {
        Self {
            lb: LoadBalancer::new(),
        }
    }
}

impl Socket for ScatterSocket {
    fn xsend(&mut self, msg: ZmqMessage) -> ZmqResult<()> {
        // SCATTER sockets do not allow multipart data
        if msg.more() {
            return Err(ZmqError::InvalidState("SCATTER: multipart not allowed"));
        }
        if !self.lb.has_out() {
            return Err(ZmqError::NoPeer);
        }
        // In full impl: write msg to lb.next_active pipe
        self.lb.next_active().ok_or(ZmqError::NoPeer)?;
        Ok(())
    }

    fn xrecv(&mut self) -> ZmqResult<ZmqMessage> {
        // SCATTER cannot receive
        Err(ZmqError::NotSupported("SCATTER"))
    }

    fn xhas_in(&self) -> bool {
        false
    }

    fn xhas_out(&self) -> bool {
        self.lb.has_out()
    }

    fn attach_pipe(&mut self, pipe: Arc<Pipe>, _subscribe_to_all: bool, _locally_initiated: bool) {
        self.lb.attach(pipe.id());
    }

    fn pipe_terminated(&mut self, pipe: &Pipe) {
        self.lb.terminated(pipe.id());
    }

    fn read_activated(&mut self, _pipe: &Pipe) {
        // SCATTER only sends
    }

    fn write_activated(&mut self, pipe: &Pipe) {
        self.lb.activated(pipe.id());
    }

    fn socket_type(&self) -> SocketType {
        SocketType::Scatter
    }
}
