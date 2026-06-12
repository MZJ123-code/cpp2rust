//! CHANNEL socket — bidirectional peer-to-peer channel (draft API).
//!
//! Replaces C++ `channel_t`. Similar to PAIR but with peer-specific routing.
//! Only one peer connection is allowed. Rejects additional connection
//! attempts. Drops multi-frame messages on receive.

use std::sync::Arc;

use crate::error::{ZmqError, ZmqResult};
use crate::message::ZmqMessage;
use crate::pipe::Pipe;
use crate::socket_type::SocketType;
use super::base::Socket;

/// CHANNEL socket — bidirectional, single-peer.
pub struct ChannelSocket {
    pipe: Option<Arc<Pipe>>,
}

impl ChannelSocket {
    pub fn new() -> Self {
        Self { pipe: None }
    }
}

impl Socket for ChannelSocket {
    fn xsend(&mut self, msg: ZmqMessage) -> ZmqResult<()> {
        // CHANNEL sockets do not allow multipart data
        if msg.more() {
            return Err(ZmqError::InvalidState("CHANNEL: multipart not allowed"));
        }
        match &self.pipe {
            Some(pipe) if pipe.is_active() => {
                pipe.write_to_session(msg, false);
                pipe.flush_to_session();
                Ok(())
            }
            _ => Err(ZmqError::NoPeer),
        }
    }

    fn xrecv(&mut self) -> ZmqResult<ZmqMessage> {
        let pipe = self.pipe.as_ref().ok_or(ZmqError::NoMessage)?;

        // Read and drop multi-frame messages
        loop {
            if !pipe.check_read_from_session() {
                return Err(ZmqError::NoMessage);
            }
            let msg = pipe.read_from_session().ok_or(ZmqError::NoMessage)?;
            if !msg.more() {
                return Ok(msg);
            }
            // Drop multi-frame messages (C++ channel drops all frames with more flag)
            while pipe.check_read_from_session() {
                let inner = pipe.read_from_session().ok_or(ZmqError::NoMessage)?;
                if !inner.more() {
                    break;
                }
            }
        }
    }

    fn xhas_in(&self) -> bool {
        self.pipe.as_ref().map(|p| p.check_read_from_session()).unwrap_or(false)
    }

    fn xhas_out(&self) -> bool {
        self.pipe.as_ref().map(|p| p.is_active()).unwrap_or(false)
    }

    fn attach_pipe(&mut self, pipe: Arc<Pipe>, _subscribe_to_all: bool, _locally_initiated: bool) {
        // Only allow one peer — reject additional connection attempts
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
        // Single pipe — no activation tracking needed
    }

    fn write_activated(&mut self, _pipe: &Pipe) {
        // Single pipe — no activation tracking needed
    }

    fn socket_type(&self) -> SocketType {
        SocketType::Channel
    }
}
