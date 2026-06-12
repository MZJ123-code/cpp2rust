//! SUB socket — subscribe to matching messages. Replaces C++ `sub_t`.
use std::sync::Arc;
use crate::error::{ZmqError, ZmqResult};
use crate::message::ZmqMessage;
use crate::pipe::Pipe;
use crate::socket_type::SocketType;
use super::base::Socket;

pub struct SubSocket { pipes: Vec<Arc<Pipe>>, pub subscriptions: Vec<Vec<u8>> }

impl SubSocket { pub fn new() -> Self { Self { pipes: Vec::new(), subscriptions: Vec::new() } } }

impl SubSocket {
    /// Check if a message data matches any of our subscriptions.
    fn matches_subscription(&self, msg: &ZmqMessage) -> bool {
        let data = msg.data();
        self.subscriptions.iter().any(|prefix| data.starts_with(prefix))
    }
}

impl Socket for SubSocket {
    fn xsend(&mut self, _msg: ZmqMessage) -> ZmqResult<()> { Err(ZmqError::NotSupported("SUB")) }
    fn xrecv(&mut self) -> ZmqResult<ZmqMessage> {
        for pipe in &self.pipes {
            if pipe.is_active() && pipe.check_read_from_socket() {
                let msg = pipe.read_from_socket().ok_or(ZmqError::NoMessage)?;
                // Check subscription matching
                if self.subscriptions.is_empty() || self.matches_subscription(&msg) {
                    return Ok(msg);
                }
                // Message doesn't match — skip and continue
            }
        }
        Err(ZmqError::NoMessage)
    }
    fn xhas_in(&self) -> bool {
        self.pipes.iter().any(|p| p.is_active() && p.check_read_from_socket())
    }
    fn xhas_out(&self) -> bool { false }
    fn attach_pipe(&mut self, p: Arc<Pipe>, sub_all: bool, _li: bool) {
        if sub_all { self.subscriptions.push(Vec::new()); }
        self.pipes.push(p);
    }
    fn pipe_terminated(&mut self, p: &Pipe) { self.pipes.retain(|x| x.id() != p.id()); }
    fn read_activated(&mut self, _p: &Pipe) {}
    fn write_activated(&mut self, _p: &Pipe) {}
    fn socket_type(&self) -> SocketType { SocketType::Sub }
    fn set_subscriptions(&mut self, subs: &[Vec<u8>]) {
        self.subscriptions = subs.to_vec();
    }
}
