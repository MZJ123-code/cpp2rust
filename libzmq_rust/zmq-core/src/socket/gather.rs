//! GATHER socket — collects messages from multiple peers (draft API).
//!
//! Replaces C++ `gather_t`. Fair-queues inbound messages from all attached
//! pipes. Drops multi-frame messages. Like PULL but with multipart rejection.

use std::sync::Arc;

use crate::data_structures::fair_queue::FairQueue;
use crate::error::{ZmqError, ZmqResult};
use crate::message::ZmqMessage;
use crate::pipe::Pipe;
use crate::socket_type::SocketType;
use super::base::Socket;

/// GATHER socket — fair-queue inbound collector.
pub struct GatherSocket {
    fq: FairQueue,
}

impl GatherSocket {
    pub fn new() -> Self {
        Self {
            fq: FairQueue::new(),
        }
    }
}

impl Socket for GatherSocket {
    fn xsend(&mut self, _msg: ZmqMessage) -> ZmqResult<()> {
        // GATHER cannot send
        Err(ZmqError::NotSupported("GATHER"))
    }

    fn xrecv(&mut self) -> ZmqResult<ZmqMessage> {
        if !self.fq.has_in() {
            return Err(ZmqError::NoMessage);
        }
        // In full impl: read from fq, dropping multi-frame messages
        self.fq.next_active().ok_or(ZmqError::NoMessage)?;
        Err(ZmqError::NoMessage)
    }

    fn xhas_in(&self) -> bool {
        self.fq.has_in()
    }

    fn xhas_out(&self) -> bool {
        false
    }

    fn attach_pipe(&mut self, pipe: Arc<Pipe>, _subscribe_to_all: bool, _locally_initiated: bool) {
        self.fq.attach(pipe.id());
    }

    fn pipe_terminated(&mut self, pipe: &Pipe) {
        self.fq.terminated(pipe.id());
    }

    fn read_activated(&mut self, pipe: &Pipe) {
        self.fq.activated(pipe.id());
    }

    fn write_activated(&mut self, _pipe: &Pipe) {
        // GATHER only receives
    }

    fn socket_type(&self) -> SocketType {
        SocketType::Gather
    }
}
