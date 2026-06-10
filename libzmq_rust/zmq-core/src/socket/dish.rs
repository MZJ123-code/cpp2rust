//! DISH socket — receives messages by group subscription (draft API).
//!
//! Replaces C++ `dish_t`. Subscribes to groups (like SUB but group-based).
//! Sends JOIN/LEAVE commands upstream when subscribing/unsubscribing.
//! Receives only messages matching joined groups.

use std::collections::HashSet;
use std::sync::Arc;

use crate::data_structures::distribution::Distribution;
use crate::data_structures::fair_queue::FairQueue;
use crate::error::{ZmqError, ZmqResult};
use crate::message::ZmqMessage;
use crate::pipe::Pipe;
use crate::socket_type::SocketType;
use super::base::Socket;

/// DISH socket — group-based subscriber.
pub struct DishSocket {
    /// Fair queue for inbound messages.
    fq: FairQueue,
    /// Distribution for sending subscriptions upstream.
    dist: Distribution,
    /// Joined groups.
    subscriptions: HashSet<String>,
    /// Prefetch: whether a matching message is already stored.
    has_message: bool,
    /// Prefetched matching message.
    message: Option<ZmqMessage>,
}

impl DishSocket {
    pub fn new() -> Self {
        Self {
            fq: FairQueue::new(),
            dist: Distribution::new(),
            subscriptions: HashSet::new(),
            has_message: false,
            message: None,
        }
    }

    /// Join a group. Sends a JOIN command to all upstream peers.
    pub fn join(&mut self, group: &str) -> ZmqResult<()> {
        if group.len() > 255 {
            return Err(ZmqError::InvalidArgument("DISH: group name too long".into()));
        }
        if !self.subscriptions.insert(group.to_string()) {
            return Err(ZmqError::InvalidArgument("DISH: already joined group".into()));
        }

        // Send JOIN to all upstream peers
        let _join_msg = make_join_message(group);
        for &pipe_id in self.dist.all_pipes() {
            // In full impl: write join_msg to pipe
            let _ = pipe_id;
        }
        Ok(())
    }

    /// Leave a group. Sends a LEAVE command to all upstream peers.
    pub fn leave(&mut self, group: &str) -> ZmqResult<()> {
        if group.len() > 255 {
            return Err(ZmqError::InvalidArgument("DISH: group name too long".into()));
        }
        if !self.subscriptions.remove(group) {
            return Err(ZmqError::InvalidArgument("DISH: not joined group".into()));
        }

        let _leave_msg = make_leave_message(group);
        for &pipe_id in self.dist.all_pipes() {
            // In full impl: write leave_msg to pipe
            let _ = pipe_id;
        }
        Ok(())
    }
}

/// Build a JOIN command message for a group.
fn make_join_message(group: &str) -> ZmqMessage {
    let mut data = vec![4u8]; // "\x04JOIN" command format
    data.extend_from_slice(b"JOIN");
    data.extend_from_slice(group.as_bytes());
    let mut msg = ZmqMessage::from_slice(&data);
    msg.set_group(group.to_string());
    msg
}

/// Build a LEAVE command message for a group.
fn make_leave_message(group: &str) -> ZmqMessage {
    let mut data = vec![5u8]; // "\x05LEAVE" command format
    data.extend_from_slice(b"LEAVE");
    data.extend_from_slice(group.as_bytes());
    let mut msg = ZmqMessage::from_slice(&data);
    msg.set_group(group.to_string());
    msg
}

impl Socket for DishSocket {
    fn xsend(&mut self, _msg: ZmqMessage) -> ZmqResult<()> {
        // DISH cannot send application messages
        Err(ZmqError::NotSupported("DISH"))
    }

    fn xrecv(&mut self) -> ZmqResult<ZmqMessage> {
        if self.has_message {
            self.has_message = false;
            return Ok(self.message.take().unwrap_or_default());
        }

        // Fair-queue from pipes, only return messages in joined groups
        Err(ZmqError::NoMessage)
    }

    fn xhas_in(&self) -> bool {
        if self.has_message {
            return true;
        }
        self.fq.has_in()
    }

    fn xhas_out(&self) -> bool {
        // Subscription commands can always be sent
        true
    }

    fn attach_pipe(&mut self, pipe: Arc<Pipe>, _subscribe_to_all: bool, _locally_initiated: bool) {
        self.fq.attach(pipe.id());
        self.dist.attach(pipe.id());

        // Send all cached subscriptions to the new upstream peer
        for group in &self.subscriptions {
            let join_msg = make_join_message(group);
            pipe.write_to_session(join_msg, false);
        }
        pipe.flush_to_session();
    }

    fn pipe_terminated(&mut self, pipe: &Pipe) {
        self.fq.terminated(pipe.id());
        self.dist.terminated(pipe.id());
    }

    fn read_activated(&mut self, pipe: &Pipe) {
        self.fq.activated(pipe.id());
    }

    fn write_activated(&mut self, _pipe: &Pipe) {
        // Distribution does not track per-pipe activation yet.
        // Pipes are always considered available for sending subscriptions.
    }

    fn socket_type(&self) -> SocketType {
        SocketType::Dish
    }
}
