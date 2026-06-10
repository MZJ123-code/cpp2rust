//! PUB socket — publish to all subscribers. Replaces C++ `pub_t`.
use std::sync::Arc;
use crate::error::{ZmqError, ZmqResult};
use crate::message::ZmqMessage;
use crate::pipe::Pipe;
use crate::socket_type::SocketType;
use super::base::Socket;

pub struct PubSocket { pipes: Vec<Arc<Pipe>> }

impl PubSocket { pub fn new() -> Self { Self { pipes: Vec::new() } } }

impl Socket for PubSocket {
    fn xsend(&mut self, msg: ZmqMessage) -> ZmqResult<()> {
        if self.pipes.is_empty() { return Err(ZmqError::NoPeer); }
        for pipe in &self.pipes {
            if pipe.is_active() {
                pipe.write_to_session(msg.clone(), false);
                pipe.flush_to_session();
            }
        }
        Ok(())
    }
    fn xrecv(&mut self) -> ZmqResult<ZmqMessage> { Err(ZmqError::NotSupported("PUB")) }
    fn xhas_in(&self) -> bool { false }
    fn xhas_out(&self) -> bool { !self.pipes.is_empty() }
    fn attach_pipe(&mut self, p: Arc<Pipe>, _sa: bool, _li: bool) { self.pipes.push(p); }
    fn pipe_terminated(&mut self, p: &Pipe) { self.pipes.retain(|x| x.id() != p.id()); }
    fn read_activated(&mut self, _p: &Pipe) {}
    fn write_activated(&mut self, _p: &Pipe) {}
    fn socket_type(&self) -> SocketType { SocketType::Pub }
}
