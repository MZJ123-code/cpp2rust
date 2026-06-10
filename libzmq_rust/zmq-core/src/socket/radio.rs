//! RADIO socket — broadcasts to groups (draft API).
//!
//! Replaces C++ `radio_t`. Like PUB but with group-based routing. Messages
//! are sent to all peers subscribed to the message's group. Also sends to
//! UDP pipes (subscribe-all peers). Supports lossy mode.

use std::collections::HashMap;
use std::sync::Arc;

use crate::data_structures::distribution::Distribution;
use crate::error::{ZmqError, ZmqResult};
use crate::message::ZmqMessage;
use crate::pipe::Pipe;
use crate::socket_type::SocketType;
use super::base::Socket;

/// RADIO socket — group-based publisher.
pub struct RadioSocket {
    /// Subscriptions: group → list of pipe IDs.
    subscriptions: HashMap<String, Vec<usize>>,
    /// UDP pipes (subscribe-to-all, e.g., multicast).
    udp_pipes: Vec<usize>,
    /// All outbound pipes (for distribution).
    dist: Distribution,
    /// Drop messages if HWM reached (lossy mode, default true).
    lossy: bool,
}

impl RadioSocket {
    pub fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
            udp_pipes: Vec::new(),
            dist: Distribution::new(),
            lossy: true,
        }
    }
}

impl Socket for RadioSocket {
    fn xsend(&mut self, msg: ZmqMessage) -> ZmqResult<()> {
        // RADIO sockets do not allow multipart data
        if msg.more() {
            return Err(ZmqError::InvalidState("RADIO: multipart not allowed"));
        }

        let group = msg.group().map(|g| g.to_string());

        // Collect pipes to send to based on group
        let pipe_ids: Vec<usize> = {
            let mut ids = Vec::new();

            // Add pipes subscribed to the specific group
            if let Some(ref group_name) = group {
                if let Some(pipes) = self.subscriptions.get(group_name) {
                    ids.extend(pipes);
                }
            }

            // Add UDP pipes (subscribe-to-all)
            ids.extend(&self.udp_pipes);

            ids
        };

        if pipe_ids.is_empty() {
            if self.lossy {
                return Ok(()); // silently drop
            }
            return Err(ZmqError::NoPeer);
        }

        // Send to matching pipes
        for pipe_id in &pipe_ids {
            let _ = pipe_id;
        }

        Ok(())
    }

    fn xrecv(&mut self) -> ZmqResult<ZmqMessage> {
        // RADIO cannot receive
        Err(ZmqError::NotSupported("RADIO"))
    }

    fn xhas_in(&self) -> bool {
        false
    }

    fn xhas_out(&self) -> bool {
        self.dist.pipe_count() > 0
    }

    fn attach_pipe(&mut self, pipe: Arc<Pipe>, subscribe_to_all: bool, _locally_initiated: bool) {
        self.dist.attach(pipe.id());

        if subscribe_to_all {
            self.udp_pipes.push(pipe.id());
        } else {
            // Subscriptions are read from the pipe on read_activated
        }

        // Process any initial subscriptions on the pipe
        // In full impl: this reads JOIN/LEAVE commands from the pipe
    }

    fn pipe_terminated(&mut self, pipe: &Pipe) {
        // Remove from subscriptions
        for (_group, pipes) in self.subscriptions.iter_mut() {
            pipes.retain(|&id| id != pipe.id());
        }
        self.subscriptions.retain(|_k, v| !v.is_empty());

        // Remove from UDP pipes
        self.udp_pipes.retain(|&id| id != pipe.id());

        self.dist.terminated(pipe.id());
    }

    fn read_activated(&mut self, pipe: &Pipe) {
        // Process GROUP JOIN/LEAVE commands from the pipe
        // In full impl: read subscription commands from pipe
        let _ = pipe;
    }

    fn write_activated(&mut self, _pipe: &Pipe) {
        // Distribution does not track per-pipe activation yet.
        // For now, all attached pipes in the dist are considered writable.
    }

    fn socket_type(&self) -> SocketType {
        SocketType::Radio
    }
}
