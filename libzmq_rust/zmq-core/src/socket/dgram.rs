//! DGRAM socket — unreliable datagram-style messaging (draft API).
//!
//! Replaces C++ `dgram_t`. Single-peer raw socket. Messages are two-part:
//! first frame is the peer routing ID, second frame is the payload.
//! No ordering guarantees. Fire-and-forget.

use std::sync::Arc;

use crate::error::{ZmqError, ZmqResult};
use crate::message::ZmqMessage;
use crate::pipe::Pipe;
use crate::socket_type::SocketType;
use super::base::Socket;

/// DGRAM socket — single-peer, raw, two-part messages.
pub struct DgramSocket {
    pipe: Option<Arc<Pipe>>,
    /// If true, the next send frame is expected to be the peer routing ID.
    more_out: bool,
}

impl DgramSocket {
    pub fn new() -> Self {
        Self {
            pipe: None,
            more_out: false,
        }
    }
}

impl Socket for DgramSocket {
    fn xsend(&mut self, msg: ZmqMessage) -> ZmqResult<()> {
        let pipe = self.pipe.as_ref().ok_or(ZmqError::NoPeer)?;

        if !self.more_out {
            // First part must be the routing ID, and must have MORE set
            if !msg.more() {
                return Err(ZmqError::InvalidState(
                    "DGRAM: first part must be routing ID with MORE",
                ));
            }
        } else {
            // Second part must NOT have MORE (dgram is two-part only)
            if msg.more() {
                return Err(ZmqError::InvalidState(
                    "DGRAM: only two-part messages allowed",
                ));
            }
        }

        if !pipe.is_active() {
            return Err(ZmqError::NoPeer);
        }

        pipe.write_to_session(msg, false);
        if !self.more_out {
            // After routing ID frame
            self.more_out = true;
        } else {
            // After body: flush and reset
            pipe.flush_to_session();
            self.more_out = false;
        }
        Ok(())
    }

    fn xrecv(&mut self) -> ZmqResult<ZmqMessage> {
        let pipe = self.pipe.as_ref().ok_or(ZmqError::NoMessage)?;

        if !pipe.check_read_from_session() {
            return Err(ZmqError::NoMessage);
        }
        pipe.read_from_session().ok_or(ZmqError::NoMessage)
    }

    fn xhas_in(&self) -> bool {
        self.pipe.as_ref().map(|p| p.check_read_from_session()).unwrap_or(false)
    }

    fn xhas_out(&self) -> bool {
        self.pipe.as_ref().map(|p| p.is_active()).unwrap_or(false)
    }

    fn attach_pipe(&mut self, pipe: Arc<Pipe>, _subscribe_to_all: bool, _locally_initiated: bool) {
        // Only one peer allowed — reject additional connections
        if self.pipe.is_none() {
            self.pipe = Some(pipe);
        } else {
            pipe.terminate();
        }
    }

    fn pipe_terminated(&mut self, pipe: &Pipe) {
        if self.pipe.as_ref().map(|p| p.id() == pipe.id()).unwrap_or(false) {
            self.pipe = None;
        }
    }

    fn read_activated(&mut self, _pipe: &Pipe) {
        // Single pipe — no activation tracking
    }

    fn write_activated(&mut self, _pipe: &Pipe) {
        // Single pipe — no activation tracking
    }

    fn socket_type(&self) -> SocketType {
        SocketType::Dgram
    }
}
